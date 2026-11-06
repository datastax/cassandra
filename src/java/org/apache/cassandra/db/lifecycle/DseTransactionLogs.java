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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
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
 * ({@link #isDseRemoveRecord}), is then refused as any log whose files changed. Drop the flag on a drained node.
 * <p>
 * In {@code internode_compatibility_mode: DSE_5_1}, for the node and for the tools that read it, the logs are named
 * and their remove records written as DSE 5.1.51 reads them, by its source. Its logs share some rules with DSE 6's: the
 * name of the 4.0 line, whose first two characters it does not read; a type it does not know refuses its start with the
 * same exception; the update time of a remove record taken over every file of the sstable, the stats file included.
 * Three differ: it knows 20 types, those of {@link #DSE_OPERATION_TYPES} without {@code restore}
 * ({@link #DSE_51_OPERATION_TYPES}); a record names the sstable by its path without the trailing component separator,
 * where this software records its URI ({@link #recordPath}); and it compares the update time of a remove record with
 * that of the files exactly, at the second resolution of the Java 8 it runs on ({@link #removeRecordTime}). The start
 * finds the files of a record naming a path by its URI ({@link #uriOfRecordPath}), so that it checks a log DSE 5.1
 * left, or one the mode wrote, as it checks its own. Once the flag is dropped such a record is no longer matched to its
 * files: its log is completed without the check of the update time, as any log whose records name paths, unless its
 * last record is corrupt, which then refuses it (its files being counted as none).
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

    /**
     * The operation types of this software, as they appear in a log name, that DSE 5.1.51 accepts too: those of
     * {@link #DSE_OPERATION_TYPES} but {@code restore}. Literal on purpose, as that table.
     */
    static final Set<String> DSE_51_OPERATION_TYPES = ImmutableSet.of("anticompactionafterrepair",
                                                                      "cleanup",
                                                                      "compaction",
                                                                      "countercachesave",
                                                                      "flush",
                                                                      "indexsummaryredistribution",
                                                                      "keycachesave",
                                                                      "relocatesstablestocorrectdisk",
                                                                      "removedeleteddata",
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

    /** The type written in place of one DSE does not accept; DSE 6 and DSE 5.1 accept it. */
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
     * this mode, {@code DSE_6} or {@code DSE_5_1}
     * ({@link LogRecord#make(LogRecord.Type, org.apache.cassandra.io.sstable.SSTable)}): its time is the
     * latest of the files with the stats file (temporary files left out, as when it was written); or it lies between
     * the latest time of the other files and the stats file's, the stats file having been the newest file when the
     * record was written, and written again since (a level or repairedAt change racing with a shutdown, the reason
     * CASSANDRA-18119 left the stats file out of the records). Any other time is refused, as files restored after the
     * record would give.
     */
    static boolean isDseRemoveRecord(LogRecord record, List<File> existingFiles)
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
            String source = DatabaseDescriptor.isDse51SSTableOutput() ? "DSE 5.1" : "DSE 6";
            logger.debug("Found a log record {} with updateTime based on the stats file, as {}, and this software in " +
                         "{} compatibility mode, write it", record, source, source);
            return true;
        }
        if (LogFile.truncateMillis(others) < time && time < LogFile.truncateMillis(stats))
        {
            logger.debug("Found a log record {} with updateTime based on the stats file, which was written again since", record);
            return true;
        }
        return false;
    }

    /**
     * The path a record names an sstable by, from the URI this software builds for it ({@code file:} and the absolute
     * path, ending with the component separator): in {@code DSE_5_1} the absolute path without the separator, as DSE
     * 5.1.51 writes it, by its source (it writes the canonical path, and finds the files of a record by listing its
     * directory, so an absolute path reads the same); otherwise, or for a URI of another file system, the URI as it is.
     * The prefix still matches the files of that sstable only: a big sstable is named
     * {@code <version>-<generation>-big-<component>}.
     */
    static String recordPath(String uri)
    {
        if (!DatabaseDescriptor.isDse51SSTableOutput() || !uri.startsWith("file:"))
            return uri;
        String path = PathUtils.getPath(uri).toString();
        return path.endsWith(String.valueOf(Component.SEPARATOR)) ? path.substring(0, path.length() - 1) : path;
    }

    /**
     * The update time of a remove record: in {@code DSE_5_1} cut to the second, otherwise as it is. DSE 5.1.51 compares
     * it, by its source, with the latest modification time of the sstable's files as it reads them, exactly; it runs on
     * Java 8, whose {@code File.lastModified} has a resolution of a second on Linux. This software compares both times
     * cut to the second.
     */
    static long removeRecordTime(long updateTime)
    {
        return DatabaseDescriptor.isDse51SSTableOutput() ? LogFile.truncateMillis(updateTime) : updateTime;
    }

    /**
     * The URI of the sstable a record names by {@code path}, by which {@link LogRecord#getExistingFiles(java.util.Set)}
     * finds its files: this software records the URI, DSE 5.1.51, by its source, the absolute path, which this returns
     * as a URI. A URI gives back the same URI, normalised.
     */
    static String uriOfRecordPath(String path)
    {
        return PathUtils.getPath(path).toUri().toString();
    }

    /**
     * The file name of {@code type} if the DSE release of the mode accepts it in a log name
     * ({@link #DSE_OPERATION_TYPES} for DSE 6, {@link #DSE_51_OPERATION_TYPES} in {@code DSE_5_1}), otherwise
     * {@code compaction}.
     */
    static String dseOperationType(OperationType type)
    {
        Set<String> accepted = DatabaseDescriptor.isDse51SSTableOutput() ? DSE_51_OPERATION_TYPES : DSE_OPERATION_TYPES;
        return accepted.contains(type.fileName) ? type.fileName : DSE_FALLBACK_TYPE;
    }
}
