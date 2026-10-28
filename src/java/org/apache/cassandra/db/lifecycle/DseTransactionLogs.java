/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.lifecycle;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.TimeUUID;

/**
 * DSE 6 compatibility: the names and the remove records of the transaction logs written while the node, or a tool,
 * writes the sstables DSE 6 reads ({@code internode_compatibility_mode: DSE_6}), so that DSE 6, after a rollback,
 * completes at its start a transaction this software left unfinished: the inputs of a committed compaction whose
 * deletion a drain dropped (run against DSE), or the new files of a flush a crash interrupted (inferred from the same
 * records, not run).
 * <p>
 * Runs of DSE 6.9.26 and DSE 6.8.65: DSE takes a file for a transaction log only with the name of the 4.0 line,
 * {@code <version>_txn_<type>_<id>.log}, and then completes it from its records as this software does (a committed
 * compaction's input deleted, its output kept, the log deleted); it ignores the name with the sstable format,
 * {@code <format>-<version>_txn_<type>_<id>.log}, which this software gives a log of any format but {@code big}
 * otherwise, and then loads the inputs next to the output and leaves the log; it refuses to start, with
 * {@code java.lang.IllegalArgumentException: Invalid fileName for operation type: <type>}, on a 4.0-line name whose
 * type it does not know; and it refuses to start on a remove record whose update time is not the latest modification
 * time of all the sstable's files, the stats file included, which this software leaves out otherwise (see
 * {@link LogRecord#make(LogRecord.Type, java.util.List, int, String)}). So the name has no format, the type is this
 * software's when DSE accepts it, otherwise {@code compaction}, and the remove records include the stats file. The type
 * is a label: the records alone say which files a start deletes; this software reads the type back only to name the
 * transaction, and DSE completed, in those runs, a committed compaction log with records, and a committed log without
 * records of each of the types it accepts.
 * <p>
 * Once the flag is dropped, a remove record left by this mode is accepted at the next start through the legacy branch
 * of {@link LogFile#verifyRecord} (with a WARN), as long as its time is still the latest modification time of the
 * sstable's files; a record whose sstable had its stats file written again after the record, which this mode accepts
 * ({@link #isDse6RemoveRecord}), is then refused as any log whose files changed. Drop the flag on a drained node.
 */
final class DseTransactionLogs
{
    private static final Logger logger = LoggerFactory.getLogger(DseTransactionLogs.class);

    /**
     * The operation types of this software, as they appear in a log name, that DSE 6.9.26 and DSE 6.8.65 accept too.
     * Literal on purpose: a type of this software whose name changes no longer matches, and is written as
     * {@code compaction}.
     */
    static final Set<String> DSE_OPERATION_TYPES = ImmutableSet.of("anticompactionafterrepair",
                                                                   "cleanup",
                                                                   "compaction",
                                                                   "countercachesave",
                                                                   "flush",
                                                                   "indexsummaryredistribution",
                                                                   "keycachesave",
                                                                   "relocatesstablestocorrectdisk",
                                                                   "removedeleteddata",
                                                                   "restore",
                                                                   "rowcachesave",
                                                                   "scrub",
                                                                   "secondaryindexbuild",
                                                                   "stream",
                                                                   "tombstonecompaction",
                                                                   "unknowncompactiontype",
                                                                   "upgradesstables",
                                                                   "validation",
                                                                   "verify",
                                                                   "viewbuild",
                                                                   "write");

    /** The type written in place of one DSE 6 does not accept; DSE accepts it. */
    private static final String DSE_FALLBACK_TYPE = "compaction";

    private DseTransactionLogs()
    {
    }

    /** {@code <version>_txn_<type>_<id>.log}, with {@link #dseOperationType} for the type. */
    static String fileName(Version version, OperationType type, TimeUUID id)
    {
        return StringUtils.join(version.toString(), LogFile.SEP,
                                "txn", LogFile.SEP,
                                dseOperationType(type), LogFile.SEP,
                                id.toString(), LogFile.EXT);
    }

    /**
     * Whether a remove record whose time is not that of the sstable's files without the stats file is one written in
     * this mode ({@link LogRecord#make(LogRecord.Type, org.apache.cassandra.io.sstable.SSTable)}): its time is the
     * latest of the files with the stats file (temporary files left out, as when it was written); or it lies between
     * the latest time of the other files and the stats file's, the stats file having been the newest file when the
     * record was written, and written again since (a level or repairedAt change racing with a shutdown, the reason
     * CASSANDRA-18119 left the stats file out of the records). Any other time is refused, as files restored after the
     * record would give.
     */
    static boolean isDse6RemoveRecord(LogRecord record, List<File> existingFiles)
    {
        long others = 0;
        long stats = 0;
        for (File file : existingFiles)
        {
            if (file.name().endsWith(Descriptor.TMP_EXT))
                continue;
            if (Descriptor.componentFromFile(file) == SSTableFormat.Components.STATS)
                stats = Math.max(stats, file.lastModified());
            else
                others = Math.max(others, file.lastModified());
        }
        long time = LogFile.truncateMillis(record.updateTime);
        if (time == LogFile.truncateMillis(Math.max(others, stats)))
        {
            logger.debug("Found a log record {} with updateTime based on the stats file, as DSE 6, and this software in DSE 6 " +
                         "compatibility mode, write it", record);
            return true;
        }
        if (LogFile.truncateMillis(others) < time && time < LogFile.truncateMillis(stats))
        {
            logger.debug("Found a log record {} with updateTime based on the stats file, which was written again since", record);
            return true;
        }
        return false;
    }

    /** The file name of {@code type} if DSE 6 accepts it in a log name, otherwise {@code compaction}. */
    static String dseOperationType(OperationType type)
    {
        return DSE_OPERATION_TYPES.contains(type.fileName) ? type.fileName : DSE_FALLBACK_TYPE;
    }
}
