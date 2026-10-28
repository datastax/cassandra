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

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.TimeUUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In {@code internode_compatibility_mode: DSE_6} the transaction logs carry the name, and the remove records the update
 * time, that DSE 6 completes at its start after a rollback ({@link DseTransactionLogs}), and this software still
 * reads them back, in that mode and after the flag drop; with {@code NONE} both are unchanged.
 */
public class DseTransactionLogsTest
{
    private static final String KEYSPACE = "dse_transaction_logs_test";
    /** The trie version DSE 6 reads, and that the node writes in DSE_6 mode. */
    private static final Version BTI_BB = new BtiFormat(null).getVersion("bb");
    private static final long SECOND = 1000;
    private static final int STATS_WRITTEN_AGAIN = 1;
    private static final int OTHER_FILES_RESTORED = 2;
    private static final int TEMPORARY_FILE_NEWER = 3;
    private static final int STATS_OLDER = 4;
    private static final String AS_DSE_6_MODE_WRITES_IT = "with updateTime based on the stats file, as DSE 6, and this software in DSE 6 " +
                                                          "compatibility mode, write it";
    private static final String WRITTEN_AGAIN_SINCE = "with updateTime based on the stats file, which was written again since";

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        MockSchema.cleanup();
    }

    @After
    public void resetMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void theNameHasNoFormatAndATypeDse6KnowsOnlyInDse6Mode()
    {
        TimeUUID id = TimeUUID.Generator.nextTimeUUID();
        assertThat(LogFile.fileName(BTI_BB, OperationType.MAJOR_COMPACTION, id)).isEqualTo("bti-bb_txn_majorcompaction_" + id + ".log");
        assertThat(LogFile.fileName(BTI_BB, OperationType.FLUSH, id)).isEqualTo("bti-bb_txn_flush_" + id + ".log");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(LogFile.fileName(BTI_BB, OperationType.FLUSH, id)).isEqualTo("bb_txn_flush_" + id + ".log");
        assertThat(LogFile.fileName(BTI_BB, OperationType.COMPACTION, id)).isEqualTo("bb_txn_compaction_" + id + ".log");
        // types DSE 6 refuses to start on are written as compaction
        assertThat(LogFile.fileName(BTI_BB, OperationType.MAJOR_COMPACTION, id)).isEqualTo("bb_txn_compaction_" + id + ".log");
        assertThat(LogFile.fileName(BTI_BB, OperationType.TRUNCATE_TABLE, id)).isEqualTo("bb_txn_compaction_" + id + ".log");
    }

    @Test
    public void everyOperationTypeGetsANameDse6AcceptsAndThisSoftwareReadsBack()
    {
        // the types DSE 6.9.26 and DSE 6.8.65 accept in a log name, and those they refuse to start on
        assertThat(DseTransactionLogs.DSE_OPERATION_TYPES)
        .containsExactlyInAnyOrder("anticompactionafterrepair", "cleanup", "compaction", "countercachesave", "flush",
                                   "indexsummaryredistribution", "keycachesave", "relocatesstablestocorrectdisk",
                                   "removedeleteddata", "restore", "rowcachesave", "scrub", "secondaryindexbuild", "stream",
                                   "tombstonecompaction", "unknowncompactiontype", "upgradesstables", "validation", "verify",
                                   "viewbuild", "write");
        Set<String> refusedByDse = Set.of("cancelalloperations", "local-onlysstablediscard",
                                          "local-onlysstableloadingduringnodeinitialization", "majorcompaction",
                                          "regionbootstrap", "regiondecommission", "regionrepair", "remotecompaction",
                                          "remotereload", "remotereloadforrepair", "removeunreadablesstables", "tabledropped",
                                          "tabletruncated");
        // every type of this software is one or the other
        Set<String> fileNames = Arrays.stream(OperationType.values()).map(type -> type.fileName).collect(Collectors.toSet());
        assertThat(Sets.union(DseTransactionLogs.DSE_OPERATION_TYPES, refusedByDse)).isEqualTo(fileNames);
        assertThat(Sets.intersection(DseTransactionLogs.DSE_OPERATION_TYPES, refusedByDse)).isEmpty();

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        TimeUUID id = TimeUUID.Generator.nextTimeUUID();
        for (OperationType type : OperationType.values())
        {
            String name = LogFile.fileName(BTI_BB, type, id);
            Matcher matcher = LogFile.FILE_REGEX.matcher(name);
            assertThat(matcher.matches()).as(name).isTrue();
            assertThat(matcher.group(1)).as(name).isEqualTo("bb_");
            assertThat(matcher.group(2)).as(name).isEqualTo(refusedByDse.contains(type.fileName) ? "compaction" : type.fileName);
            assertThat(OperationType.fromFileName(matcher.group(2))).as(name)
                                                                     .isEqualTo(refusedByDse.contains(type.fileName) ? OperationType.COMPACTION : type);
        }
    }

    @Test
    public void aLogIsShownWithItsNameOnDiskInDse6Mode()
    {
        // a log written without the mode (or by a build before this one), read in DSE_6 mode: its lines name the file
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader output = MockSchema.sstable(1, 128, cfs);
        LogTransaction txn = new LogTransaction(OperationType.MAJOR_COMPACTION, LifecycleTransaction.newId());
        txn.trackNew(output);
        File log = txn.logFiles().get(0);
        assertThat(log.name()).contains("_txn_majorcompaction_");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try (LogFile readBack = LogFile.make(log))
        {
            assertThat(readBack.toString()).startsWith('[' + log.name() + " in ");
        }
        assertThat(txn.txnFile().toString()).startsWith('[' + log.name() + " in ");

        txn.txnFile().commit();
        assertThat(txn.complete(null)).isNull();
    }

    @Test
    public void theRemoveRecordTakesTheStatsFileButNoTemporaryFileOnlyInDse6Mode()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader input = MockSchema.sstable(1, 128, cfs);
        long others = latestModificationOfAllButTheStatsFile(input);
        File stats = statsFile(input, others + 5 * SECOND);
        File temporary = stats.parent().resolve(stats.name() + Descriptor.TMP_EXT);
        assertThat(temporary.createFileIfNotExists()).isTrue();
        assertThat(temporary.trySetLastModified(others + 10 * SECOND)).isTrue();

        LogRecord noneRemove = LogRecord.make(LogRecord.Type.REMOVE, input);
        LogRecord noneAdd = LogRecord.make(LogRecord.Type.ADD, input);
        assertThat(noneRemove.updateTime).isEqualTo(others);

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        LogRecord remove = LogRecord.make(LogRecord.Type.REMOVE, input);
        // the stats file's time, not the temporary file's, which counts in the files no more than without the mode
        assertThat(remove.updateTime).isEqualTo(stats.lastModified());
        assertThat(remove.numFiles).isEqualTo(noneRemove.numFiles);
        // the add records do not change (their update time is not recorded)
        assertThat(LogRecord.make(LogRecord.Type.ADD, input)).isEqualTo(noneAdd);
    }

    @Test
    public void aCommittedTransactionWrittenInDse6ModeIsCompletedAtTheNextStart() throws IOException
    {
        // the input's stats file written again after its other files, before the transaction (as a level change does)
        List<ILoggingEvent> logged = startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.DSE_6, 0, true);
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.DEBUG && e.getFormattedMessage().contains(AS_DSE_6_MODE_WRITES_IT));
        assertThat(logged).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    @Test
    public void aCommittedTransactionWrittenInDse6ModeIsCompletedAtTheNextStartAfterTheFlagDrop() throws IOException
    {
        List<ILoggingEvent> logged = startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.NONE, 0, true);
        // the warning this software has always given for a record with the stats file's time, and no DSE 6 line
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.WARN &&
                                         e.getFormattedMessage().startsWith("Found a legacy log record ") &&
                                         e.getFormattedMessage().endsWith(" with updateTime based on the stats file, ignoring to allow startup to continue"));
        assertThat(logged).noneMatch(e -> e.getFormattedMessage().contains(AS_DSE_6_MODE_WRITES_IT) ||
                                          e.getFormattedMessage().contains(WRITTEN_AGAIN_SINCE));
    }

    @Test
    public void aCommittedTransactionWrittenWithoutTheModeIsCompletedInDse6Mode() throws IOException
    {
        // written without the stats file's time (NONE, or a build before DSE 6 compatibility mode wrote it)
        List<ILoggingEvent> logged = startOnACommittedMajorCompaction(InternodeCompatibilityMode.NONE, InternodeCompatibilityMode.DSE_6, 0, true);
        assertThat(logged).noneMatch(e -> e.getFormattedMessage().contains("with updateTime based on the stats file"));
    }

    @Test
    public void aStatsFileWrittenAgainAfterTheRecordDoesNotStopTheStartInDse6Mode() throws IOException
    {
        // the stats file was the newest file when the record took its time, and is written again before the start
        List<ILoggingEvent> logged = startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.DSE_6,
                                                                      STATS_WRITTEN_AGAIN, true);
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.DEBUG && e.getFormattedMessage().contains(WRITTEN_AGAIN_SINCE));
        assertThat(logged).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    @Test
    public void aStatsFileWrittenAgainAfterTheRecordStopsTheStartAfterTheFlagDrop() throws IOException
    {
        // the trade-off of the stats file's time: this software's check without the mode is unchanged, and refuses the
        // record, as DSE 6 does; started again in DSE_6, the node accepts and completes it
        startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.NONE, STATS_WRITTEN_AGAIN, false);
    }

    @Test
    public void filesRestoredAfterTheRecordStillStopTheStartInDse6Mode() throws IOException
    {
        // the check against files restored from a backup holds: the input's other files are newer than the record
        startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.DSE_6, OTHER_FILES_RESTORED, false);
    }

    @Test
    public void aStatsFileOlderThanTheRecordStopsTheStartInDse6Mode() throws IOException
    {
        // a record newer than every file of the sstable, the stats file included (an older copy of it put back)
        startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.DSE_6, STATS_OLDER, false);
    }

    @Test
    public void aTemporaryFileNewerThanTheRecordDoesNotStopTheStartInDse6Mode() throws IOException
    {
        // a temporary stats file left by a crash, newer than the record: left out, as when the record was written
        List<ILoggingEvent> logged = startOnACommittedMajorCompaction(InternodeCompatibilityMode.DSE_6, InternodeCompatibilityMode.DSE_6,
                                                                      TEMPORARY_FILE_NEWER, true);
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.DEBUG && e.getFormattedMessage().contains(AS_DSE_6_MODE_WRITES_IT));
        assertThat(logged).noneMatch(e -> e.getFormattedMessage().contains(WRITTEN_AGAIN_SINCE));
    }

    /**
     * A major compaction committed in {@code writeMode} whose input's deletion never ran, as a stop that drops the tidy
     * leaves it, the input's stats file written 5 s after its other files before the transaction (as a level change
     * does), then the start of this software, in {@code startMode}, on the table directory. Before the start, when
     * {@code change} is {@link #STATS_WRITTEN_AGAIN}, the stats file is written again 5 s later still; when it is
     * {@link #OTHER_FILES_RESTORED}, the other files of the input are 10 s newer than the record; when it is
     * {@link #TEMPORARY_FILE_NEWER}, a temporary stats file 15 s newer than the record is left next to the input; when it
     * is {@link #STATS_OLDER}, the stats file is put back 3 s older than the record. If {@code completes}, the start
     * deletes the input and the log, and keeps the output; otherwise it refuses the log and keeps them all.
     *
     * @return what {@link LogFile} and {@link DseTransactionLogs} logged during the start
     */
    private static List<ILoggingEvent> startOnACommittedMajorCompaction(InternodeCompatibilityMode writeMode, InternodeCompatibilityMode startMode,
                                                                       int change, boolean completes) throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(writeMode);
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        // MockSchema has already released the readers' own references
        SSTableReader input = MockSchema.sstable(1, 128, cfs);
        SSTableReader output = MockSchema.sstable(2, 128, cfs);
        File directory = input.descriptor.directory;
        long others = latestModificationOfAllButTheStatsFile(input);
        File stats = statsFile(input, others + 5 * SECOND);

        LogTransaction txn = new LogTransaction(OperationType.MAJOR_COMPACTION, LifecycleTransaction.newId());
        txn.trackNew(output);
        AbstractLogTransaction.ReaderTidier tidier = txn.obsoleted(input);
        txn.txnFile().commit();
        List<File> logs = txn.logFiles();
        assertThat(logs).hasSize(1);
        // the name: the version alone in DSE_6 mode (that of the selected format, which the unit tests do not switch to
        // DSE's bb, and which a test target can choose), and the type DSE 6 accepts; the format too without the mode,
        // unless it is big
        Version version = DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion();
        String prefix = writeMode.isDse6() || BigFormat.is(version.format) ? version.toString() : version.toFormatAndVersionString();
        String type = writeMode.isDse6() ? "compaction" : "majorcompaction";
        assertThat(logs.get(0).name()).isEqualTo(prefix + "_txn_" + type + '_' + txn.id() + ".log");
        // the remove record's update time: in DSE 6 compatibility mode the stats file's, the latest of all the input's
        // files, as DSE 6 computes it; otherwise the latest of the others
        assertThat(Files.readAllLines(logs.get(0).toPath())).extracting(LogRecord::make)
                                                            .filteredOn(r -> r.type == LogRecord.Type.REMOVE)
                                                            .extracting(r -> r.updateTime)
                                                            .containsExactly(writeMode.isDse6() ? stats.lastModified() : others);
        Set<File> inputFiles = LogTransactionTest.getAllFilePaths(input);
        inputFiles.add(stats);
        Set<File> outputFiles = LogTransactionTest.getAllFilePaths(output);
        if (change == STATS_WRITTEN_AGAIN)
            assertThat(stats.trySetLastModified(others + 10 * SECOND)).isTrue();
        if (change == OTHER_FILES_RESTORED)
            for (File file : inputFiles)
                if (!file.equals(stats))
                    assertThat(file.trySetLastModified(others + 15 * SECOND)).isTrue();
        if (change == TEMPORARY_FILE_NEWER)
        {
            File temporary = stats.parent().resolve(stats.name() + Descriptor.TMP_EXT);
            assertThat(temporary.createFileIfNotExists()).isTrue();
            assertThat(temporary.trySetLastModified(others + 20 * SECOND)).isTrue();
            inputFiles.add(temporary);
        }
        if (change == STATS_OLDER)
            assertThat(stats.trySetLastModified(others + 2 * SECOND)).isTrue();

        // what the start of this software does in each table directory
        DatabaseDescriptor.setInternodeCompatibilityMode(startMode);
        List<Logger> loggers = List.of((Logger) LoggerFactory.getLogger(LogFile.class), (Logger) LoggerFactory.getLogger(DseTransactionLogs.class));
        List<Level> levels = loggers.stream().map(Logger::getLevel).collect(Collectors.toList());
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        for (Logger logger : loggers)
        {
            logger.addAppender(appender);
            logger.setLevel(Level.DEBUG);
        }
        try
        {
            assertThat(LogTransaction.removeUnfinishedLeftovers(List.of(directory))).isEqualTo(completes);
        }
        finally
        {
            for (int i = 0; i < loggers.size(); i++)
            {
                loggers.get(i).detachAppender(appender);
                loggers.get(i).setLevel(levels.get(i));
            }
        }
        if (completes)
            assertThat(inputFiles).noneMatch(File::exists);
        else
            assertThat(inputFiles).allMatch(File::exists);
        assertThat(outputFiles).allMatch(File::exists);
        assertThat(logs.get(0).exists()).isEqualTo(!completes);

        // ends the transaction, as LogTransactionTest does, to avoid leak reports
        tidier.commit();
        assertThat(txn.complete(null)).isNull();
        return appender.list;
    }

    private static long latestModificationOfAllButTheStatsFile(SSTableReader sstable)
    {
        File stats = sstable.descriptor.fileFor(Components.STATS);
        return LogRecord.getExistingFiles(sstable.descriptor.baseFileUri() + Component.SEPARATOR)
                        .stream()
                        .filter(f -> !f.equals(stats))
                        .mapToLong(File::lastModified)
                        .max()
                        .getAsLong();
    }

    /** The sstable's stats file (created if MockSchema made none), with the given modification time. */
    private static File statsFile(SSTableReader sstable, long modified)
    {
        File stats = sstable.descriptor.fileFor(Components.STATS);
        stats.createFileIfNotExists();
        assertThat(stats.trySetLastModified(modified)).isTrue();
        return stats;
    }
}
