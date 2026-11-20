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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assume;
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
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
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

import static org.apache.cassandra.config.InternodeCompatibilityMode.DSE_5_1;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * In {@code internode_compatibility_mode: DSE_6} the transaction logs carry the name, and the remove records the update
 * time, that DSE 6 completes at its start after a rollback ({@link DseTransactionLogs}), and this software still
 * reads them back, in that mode and after the flag drop; with {@code NONE} both are unchanged. In {@code DSE_5_1} the
 * same holds with the types DSE 5.1 knows, and the logs DSE 5.1.51 wrote ({@link Dse51Fixtures#transactionLog}),
 * moved into a directory of the test, are completed or rolled back.
 */
public class DseTransactionLogsTest
{
    private static final String KEYSPACE = "dse_transaction_logs_test";
    /** The trie version DSE 6 reads, and that the node writes in DSE_6 mode. */
    private static final Version BTI_BB = new BtiFormat(null).getVersion("bb");
    /** The big version the node writes in DSE_5_1 mode. */
    private static final Version BIG_NB = new BigFormat(null).getVersion("nb");
    private static final long SECOND = 1000;
    private static final int STATS_WRITTEN_AGAIN = 1;
    private static final int OTHER_FILES_RESTORED = 2;
    private static final int TEMPORARY_FILE_NEWER = 3;
    private static final int STATS_OLDER = 4;
    private static final int DATA_NEWEST = 5;
    private static final int DATA_NEWER = 6;
    private static final String AS_DSE_6_MODE_WRITES_IT = "with updateTime based on the stats file, as DSE 6, and this software in DSE 6 " +
                                                          "compatibility mode, write it";
    private static final String AS_DSE_51_MODE_WRITES_IT = "with updateTime based on the stats file, as DSE 5.1, and " +
                                                           "this software in DSE 5.1 compatibility mode, write it";
    /** A record of a log DSE 5.1.51 wrote: its type, path, update time, number of files and checksum. */
    private static final Pattern DSE_51_RECORD =
        Pattern.compile("(ADD|REMOVE|COMMIT):\\[(.*),(\\d+),(\\d+)\\]\\[(\\d+)\\]");
    /** The components of the big {@code me} sstables of the logs DSE 5.1.51 wrote, as the fixtures' sstables have. */
    private static final List<String> ME_COMPONENTS = List.of("CompressionInfo.db", "Data.db", "Digest.crc32",
                                                              "Filter.db", "Index.db", "Statistics.db", "Summary.db",
                                                              "TOC.txt");
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

    /** The types DSE 5.1.51 knows are those DSE 6 knows but {@code restore}, which DSE_5_1 writes as compaction. */
    @Test
    public void theNameHasNoFormatAndATypeDse51KnowsInDse51Mode()
    {
        assertThat(Sets.difference(DseTransactionLogs.DSE_OPERATION_TYPES, DseTransactionLogs.DSE_51_OPERATION_TYPES))
        .containsExactly("restore");
        assertThat(DseTransactionLogs.DSE_51_OPERATION_TYPES).hasSize(20)
                                                              .isSubsetOf(DseTransactionLogs.DSE_OPERATION_TYPES);

        TimeUUID id = TimeUUID.Generator.nextTimeUUID();
        DatabaseDescriptor.setInternodeCompatibilityMode(DSE_5_1);
        assertThat(LogFile.fileName(BIG_NB, OperationType.FLUSH, id)).isEqualTo("nb_txn_flush_" + id + ".log");
        assertThat(LogFile.fileName(BTI_BB, OperationType.FLUSH, id)).isEqualTo("bb_txn_flush_" + id + ".log");
        assertThat(LogFile.fileName(BIG_NB, OperationType.RESTORE, id)).isEqualTo("nb_txn_compaction_" + id + ".log");
        assertThat(LogFile.fileName(BIG_NB, OperationType.MAJOR_COMPACTION, id)).isEqualTo("nb_txn_compaction_" + id +
                                                                                           ".log");
        for (OperationType type : OperationType.values())
        {
            Matcher matcher = LogFile.FILE_REGEX.matcher(LogFile.fileName(BIG_NB, type, id));
            assertThat(matcher.matches()).isTrue();
            assertThat(DseTransactionLogs.DSE_51_OPERATION_TYPES).contains(matcher.group(2));
        }

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(LogFile.fileName(BIG_NB, OperationType.RESTORE, id)).isEqualTo("nb_txn_restore_" + id + ".log");
    }

    @Test
    public void aCommittedTransactionWrittenInDse51ModeIsCompletedAtTheNextStart() throws IOException
    {
        List<ILoggingEvent> logged = startOnACommittedMajorCompactionWrittenInDse51(DSE_5_1, 0, true);
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.DEBUG &&
                                         e.getFormattedMessage().contains(AS_DSE_51_MODE_WRITES_IT));
        assertThat(logged).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    @Test
    public void aStatsFileWrittenAgainAfterTheRecordDoesNotStopTheStartInDse51Mode() throws IOException
    {
        List<ILoggingEvent> logged = startOnACommittedMajorCompactionWrittenInDse51(DSE_5_1, STATS_WRITTEN_AGAIN, true);
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.DEBUG &&
                                         e.getFormattedMessage().contains(WRITTEN_AGAIN_SINCE));
    }

    @Test
    public void filesRestoredAfterTheRecordStillStopTheStartInDse51Mode() throws IOException
    {
        startOnACommittedMajorCompactionWrittenInDse51(DSE_5_1, OTHER_FILES_RESTORED, false);
    }

    /**
     * The transaction logs DSE 5.1.51 wrote ({@link Dse51Fixtures#transactionLog}: a compaction of four sstables into
     * one, as it ran and once committed) are made of the records built here ({@link #dse51Record}), line for line,
     * checksums included: each record {@code TYPE:[path,update time,number of files][checksum]}, the path of the
     * sstable without the trailing component separator, the update time 0 but in a remove record, the checksum the
     * CRC32 of the path, the type, the update time as two ints (low half first) and the number of files. This software
     * parses each as a valid record. The update times of the remove records are in milliseconds, as the Java 8 of the
     * capture read the files' times, and the mode writes them so
     * ({@link #theRemoveRecordsTheModeWritesCarryTheCapturedTimes}).
     */
    @Test
    public void theRecordsOfTheLogsDse51WroteAreTheOnesBuiltHere() throws IOException
    {
        List<String> committed = Files.readAllLines(DseFixtures.dse51().transactionLog(true));
        List<String> inProgress = Files.readAllLines(DseFixtures.dse51().transactionLog(false));
        assertThat(committed).hasSize(6);
        assertThat(inProgress).containsExactly(committed.get(0));
        assertThat(committed).extracting(line -> line.substring(0, line.indexOf(':')))
                             .containsExactly("ADD", "REMOVE", "REMOVE", "REMOVE", "REMOVE", "COMMIT");
        for (String line : committed)
        {
            Matcher matcher = DSE_51_RECORD.matcher(line);
            assertThat(matcher.matches()).as(line).isTrue();
            String type = matcher.group(1);
            String path = matcher.group(2);
            long updateTime = Long.parseLong(matcher.group(3));
            int files = Integer.parseInt(matcher.group(4));
            assertThat(line).isEqualTo(dse51Record(type, path, updateTime, files));

            LogRecord record = LogRecord.make(line);
            assertThat(record.isValid()).as(line).isTrue();
            assertThat(record.type.name()).isEqualTo(type);
            assertThat(record.checksum).as(line).isEqualTo(record.computeChecksum());
            assertThat(record.updateTime).isEqualTo(updateTime);
            assertThat(record.numFiles).isEqualTo(files);
            if (record.type.hasFile())
            {
                assertThat(record.absolutePath()).isEqualTo(path).doesNotEndWith(String.valueOf(Component.SEPARATOR));
                assertThat(files).as(line).isEqualTo(ME_COMPONENTS.size());
            }
            if (record.type != LogRecord.Type.REMOVE)
                assertThat(updateTime).as(line).isZero();
        }
        assertThat(dse51RemoveTimes()).hasSize(4).noneMatch(time -> time % SECOND == 0);
    }

    /**
     * The remove record the mode writes for an sstable whose files carry one of the update times of the remove records
     * DSE 5.1.51 wrote carries that time, to the millisecond, as DSE 5.1.51 wrote it: the line built here
     * ({@link #dse51Record}) with the path of the sstable and the number of its files.
     */
    @Test
    public void theRemoveRecordsTheModeWritesCarryTheCapturedTimes() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(DSE_5_1);
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        int generation = 1;
        for (long time : dse51RemoveTimes())
        {
            SSTableReader sstable = MockSchema.sstable(generation++, 128, cfs);
            sstable.descriptor.fileFor(Components.STATS).createFileIfNotExists();
            List<File> files = LogRecord.getExistingFiles(sstable.descriptor.baseFileUri() + Component.SEPARATOR);
            for (File file : files)
                setLastModified(file, time);

            LogTransaction txn = new LogTransaction(OperationType.COMPACTION, LifecycleTransaction.newId());
            AbstractLogTransaction.ReaderTidier tidier = txn.obsoleted(sstable);
            Throwable aborted;
            try
            {
                List<String> written = Files.readAllLines(txn.logFiles().get(0).toPath());
                assertThat(written).hasSize(1);
                LogRecord record = LogRecord.make(written.get(0));
                assertThat(record.type).isEqualTo(LogRecord.Type.REMOVE);
                assertThat(record.updateTime).isEqualTo(time);
                assertThat(record.numFiles).isEqualTo(files.size());
                assertThat(record.absolutePath()).isEqualTo(sstable.descriptor.baseFile().absolutePath());
                assertThat(written.get(0)).isEqualTo(dse51Record("REMOVE",
                                                                 sstable.descriptor.baseFile().absolutePath(), time,
                                                                 files.size()));
            }
            finally
            {
                aborted = tidier.abort(null);
                txn.abort();
            }
            assertThat(aborted).isNull();
        }
    }

    /** The update times of the remove records of the committed log DSE 5.1.51 wrote, in their order. */
    private static List<Long> dse51RemoveTimes() throws IOException
    {
        List<Long> removeTimes = new ArrayList<>();
        for (String line : Files.readAllLines(DseFixtures.dse51().transactionLog(true)))
        {
            Matcher matcher = DSE_51_RECORD.matcher(line);
            assertThat(matcher.matches()).as(line).isTrue();
            if (matcher.group(1).equals(LogRecord.Type.REMOVE.name()))
                removeTimes.add(Long.parseLong(matcher.group(3)));
        }
        return removeTimes;
    }

    /**
     * The committed log DSE 5.1.51 wrote, moved into a directory of this test with files for its sstables
     * ({@link #startOnTheLogDse51Wrote}): the start of this software in {@code DSE_5_1} checks its records against
     * the files, through the rule of the remove records the mode writes, at their millisecond times, and completes it:
     * the input deleted, the output kept, the log deleted.
     */
    @Test
    public void aCommittedLogDse51WroteIsCompletedInDse51Mode() throws IOException
    {
        List<ILoggingEvent> logged = startOnTheLogDse51Wrote(true, 0, DSE_5_1, true);
        // read as a record of the mode, not through the legacy branch, which takes it too with a WARN
        assertThat(logged).filteredOn(e -> e.getLevel() == Level.DEBUG &&
                                           e.getFormattedMessage().contains(AS_DSE_51_MODE_WRITES_IT))
                          .hasSize(4);
        assertThat(logged).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    /**
     * The same log, the input's files restored after the records: the start in {@code DSE_5_1} checks the records of
     * DSE 5.1, which name the absolute path of the sstables, against the files, and refuses the log.
     */
    @Test
    public void aLogDse51WroteWhoseFilesChangedStopsTheStartInDse51Mode() throws IOException
    {
        List<ILoggingEvent> logged = startOnTheLogDse51Wrote(true, OTHER_FILES_RESTORED, DSE_5_1, false);
        // refused for the update time of its first remove record, not for a checksum or a line that does not parse
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.ERROR &&
                                         e.getFormattedMessage().contains("Unexpected files detected for sstable " +
                                                                          "[me-1-big"));
        assertThat(logged).noneMatch(e -> e.getFormattedMessage().contains("checksum") ||
                                          e.getFormattedMessage().contains("Failed to parse"));
    }

    /**
     * The same committed log, a data file of each input the newest file, at the record's time: the start in
     * {@code DSE_5_1} matches the record to the files without the rule of the stats file, and completes it.
     */
    @Test
    public void aCommittedLogDse51WroteWhoseDataFileIsTheNewestIsCompletedInDse51Mode() throws IOException
    {
        List<ILoggingEvent> logged = startOnTheLogDse51Wrote(true, DATA_NEWEST, DSE_5_1, true);
        assertThat(logged).noneMatch(e -> e.getFormattedMessage().contains("updateTime"));
        assertThat(logged).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    /**
     * The same committed log, a data file of each input newer than the record's time by a second: the start in
     * {@code DSE_5_1} matches the record to the files, finds their time changed, and refuses the log.
     */
    @Test
    public void aCommittedLogDse51WroteWhoseDataFileIsNewerThanTheRecordStopsTheStartInDse51Mode() throws IOException
    {
        List<ILoggingEvent> logged = startOnTheLogDse51Wrote(true, DATA_NEWER, DSE_5_1, false);
        assertThat(logged).anyMatch(e -> e.getLevel() == Level.ERROR &&
                                         e.getFormattedMessage().contains("Unexpected files detected for sstable " +
                                                                          "[me-1-big"));
        assertThat(logged).noneMatch(e -> e.getFormattedMessage().contains("checksum") ||
                                          e.getFormattedMessage().contains("Failed to parse"));
    }

    /**
     * Without {@code DSE_5_1} the records of the same log, which name absolute paths, are not matched to their files:
     * the log is completed without the check of their update time, its files changed or not, as before this mode.
     */
    @Test
    public void aLogDse51WroteIsCompletedUncheckedInTheOtherModes() throws IOException
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            if (mode.isDse51())
                continue;
            for (int change : new int[]{ 0, OTHER_FILES_RESTORED })
            {
                List<ILoggingEvent> logged = startOnTheLogDse51Wrote(true, change, mode, true);
                assertThat(logged).as("%s", mode).noneMatch(e -> e.getFormattedMessage().contains("updateTime"));
            }
        }
    }

    /**
     * The log DSE 5.1.51 left while its compaction ran, its add record only: the start of this software rolls the
     * compaction back in every mode, the output deleted, the input kept, the log deleted.
     */
    @Test
    public void aLogDse51LeftWhileItsCompactionRanIsRolledBackInEveryMode() throws IOException
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            assertThat(startOnTheLogDse51Wrote(false, 0, mode, true)).as("%s", mode)
                                                                       .noneMatch(e -> e.getLevel() == Level.WARN);
    }

    /**
     * In DSE_5_1 a record names its sstable by the path without the trailing component separator, as DSE 5.1 does;
     * otherwise by its URI.
     */
    @Test
    public void theRecordsNameTheAbsolutePathOfTheSSTableInDse51Mode()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = MockSchema.sstable(1, 128, cfs);
        String uri = sstable.descriptor.baseFileUri() + Component.SEPARATOR;
        assertThat(uri).startsWith("file:");
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            String expected = mode.isDse51() ? sstable.descriptor.baseFile().absolutePath() : uri;
            for (LogRecord.Type type : List.of(LogRecord.Type.ADD, LogRecord.Type.REMOVE))
                assertThat(LogRecord.make(type, sstable).absolutePath()).as("%s %s", mode, type).isEqualTo(expected);
        }
    }

    /**
     * After the flag drop, a committed compaction written in DSE_5_1 is completed without the check of its records,
     * which name absolute paths (see {@link #aLogDse51WroteIsCompletedUncheckedInTheOtherModes}).
     */
    @Test
    public void aCommittedTransactionWrittenInDse51ModeIsCompletedAtTheNextStartAfterTheFlagDrop() throws IOException
    {
        InternodeCompatibilityMode none = InternodeCompatibilityMode.NONE;
        List<ILoggingEvent> logged = startOnACommittedMajorCompactionWrittenInDse51(none, 0, true);
        assertThat(logged).noneMatch(e -> e.getLevel() == Level.WARN);
    }

    /**
     * The transaction log DSE 5.1.51 wrote ({@code committed} or as its compaction ran), moved, as the fixtures' README
     * says, into a directory of this test, and the start of this software in {@code startMode} on that directory. Every
     * sstable the committed log names gets the components of a big {@code me} sstable there, empty: the input ones (its
     * remove records) modified so that their latest modification time is the record's update time, which is all the
     * capture tells of them: the test chooses the stats file as the newest, the others a second older, as DSE writes
     * the stats file last, or with {@link #DATA_NEWEST} the data file, the others, the stats file among them, a second
     * older; with {@link #DATA_NEWER} the data file is a second newer than the record instead; with
     * {@link #OTHER_FILES_RESTORED} every file but the stats file is then made 10 s newer than the record. The records
     * name the sstables of the directory, their checksums computed again, their update times kept; the records this
     * software builds from those files, the stats file included, are the lines of the moved log (but with
     * {@link #DATA_NEWER}). The start completes the log when {@code completes} (a committed one: the input deleted, the
     * output kept; one in progress: the output deleted, the input kept), otherwise it keeps every file. Returns what
     * {@link #loggedWhile} collects.
     */
    private static List<ILoggingEvent> startOnTheLogDse51Wrote(boolean committed, int change,
                                                                InternodeCompatibilityMode startMode,
                                                                boolean completes) throws IOException
    {
        File directory = new File(Files.createTempDirectory("dse51txn"));
        try
        {
            Map<String, String> moved = new HashMap<>();
            List<File> input = new ArrayList<>();
            List<File> output = new ArrayList<>();
            for (String line : Files.readAllLines(DseFixtures.dse51().transactionLog(true)))
            {
                Matcher matcher = DSE_51_RECORD.matcher(line);
                assertThat(matcher.matches()).as(line).isTrue();
                String type = matcher.group(1);
                long updateTime = Long.parseLong(matcher.group(3));
                int files = Integer.parseInt(matcher.group(4));
                if (type.equals("COMMIT"))
                {
                    moved.put(line, line);
                    continue;
                }
                String path = directory.resolve(new File(matcher.group(2)).name()).absolutePath();
                String record = dse51Record(type, path, updateTime, files);
                List<File> sstable = sstableFiles(path);
                if (type.equals("REMOVE"))
                {
                    String newest = change == DATA_NEWEST || change == DATA_NEWER ? "-Data.db" : "-Statistics.db";
                    long newestTime = change == DATA_NEWER ? updateTime + SECOND : updateTime;
                    for (File file : sstable)
                        setLastModified(file, file.name().endsWith(newest) ? newestTime : updateTime - SECOND);
                    LogRecord built = LogRecord.make(LogRecord.Type.REMOVE, sstable, files, path, true);
                    if (change != DATA_NEWER)
                        assertThat(built.raw).isEqualTo(record);
                    input.addAll(sstable);
                }
                else
                {
                    assertThat(LogRecord.make(LogRecord.Type.ADD, sstable, files, path).raw).isEqualTo(record);
                    output.addAll(sstable);
                }
                moved.put(line, record);
            }
            assertThat(output).hasSize(ME_COMPONENTS.size());
            assertThat(input).hasSize(4 * ME_COMPONENTS.size());
            Path captured = DseFixtures.dse51().transactionLog(committed);
            List<String> lines = new ArrayList<>();
            for (String line : Files.readAllLines(captured))
                lines.add(Objects.requireNonNull(moved.get(line), line));
            File log = directory.resolve(captured.getFileName().toString());
            Files.write(log.toPath(), lines);
            if (change == OTHER_FILES_RESTORED)
                for (File file : input)
                    if (!file.name().endsWith("-Statistics.db"))
                        setLastModified(file, file.lastModified() + 11 * SECOND);

            DatabaseDescriptor.setInternodeCompatibilityMode(startMode);
            List<File> directories = List.of(directory);
            List<ILoggingEvent> logged = loggedWhile(() -> {
                assertThat(LogTransaction.removeUnfinishedLeftovers(directories)).isEqualTo(completes);
            });
            List<File> deleted = committed ? input : output;
            List<File> kept = committed ? output : input;
            if (completes)
                assertThat(deleted).noneMatch(File::exists);
            else
                assertThat(deleted).allMatch(File::exists);
            assertThat(kept).allMatch(File::exists);
            assertThat(log.exists()).isEqualTo(!completes);
            return logged;
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    /**
     * Sets the modification time of {@code file}, skipping the test where the file system does not keep the
     * milliseconds of the time, which the remove records of the logs DSE 5.1.51 wrote hold.
     */
    private static void setLastModified(File file, long time)
    {
        assertThat(file.trySetLastModified(time)).isTrue();
        Assume.assumeTrue("the file system keeps the milliseconds of a modification time", file.lastModified() == time);
    }

    /**
     * Runs {@code body} and returns what {@link LogFile}, {@link DseTransactionLogs} and {@link LogTransaction} logged,
     * DEBUG included.
     */
    private static List<ILoggingEvent> loggedWhile(Runnable body)
    {
        List<Logger> loggers = List.of((Logger) LoggerFactory.getLogger(LogFile.class),
                                       (Logger) LoggerFactory.getLogger(DseTransactionLogs.class),
                                       (Logger) LoggerFactory.getLogger(LogTransaction.class));
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
            body.run();
        }
        finally
        {
            for (int i = 0; i < loggers.size(); i++)
            {
                loggers.get(i).detachAppender(appender);
                loggers.get(i).setLevel(levels.get(i));
            }
        }
        return appender.list;
    }

    /**
     * A record of a transaction log as DSE 5.1.51 writes it, by its source, built byte by byte: the lines of the logs
     * DSE 5.1.51 wrote ({@link #theRecordsOfTheLogsDse51WroteAreTheOnesBuiltHere}).
     */
    private static String dse51Record(String type, String path, long updateTime, int files)
    {
        CRC32 crc = new CRC32();
        crc.update(path.getBytes(StandardCharsets.UTF_8));
        crc.update(type.getBytes(StandardCharsets.UTF_8));
        crc.update(ByteBuffer.allocate(12).putInt((int) updateTime).putInt((int) (updateTime >>> 32)).putInt(files)
                             .array());
        return type + ":[" + path + ',' + updateTime + ',' + files + "][" + (crc.getValue() & Long.MAX_VALUE) + ']';
    }

    /** The components of the big {@code me} sstable {@code path} names, without the separator, as empty files. */
    private static List<File> sstableFiles(String path)
    {
        List<File> files = new ArrayList<>();
        for (String component : ME_COMPONENTS)
        {
            File file = new File(path + Component.SEPARATOR + component);
            assertThat(file.createFileIfNotExists()).isTrue();
            files.add(file);
        }
        return files;
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

    /**
     * As {@link #startOnACommittedMajorCompaction}, the transaction written in {@code DSE_5_1} and the start in
     * {@code start}: the log is named with the version alone and the type DSE 5.1 accepts, its records name the
     * sstables by their path without the trailing component separator, as DSE 5.1 writes them, and the remove record's
     * update time is the stats file's, to the millisecond. Only the changes {@link #STATS_WRITTEN_AGAIN} and
     * {@link #OTHER_FILES_RESTORED} are made here.
     */
    private static List<ILoggingEvent> startOnACommittedMajorCompactionWrittenInDse51(InternodeCompatibilityMode start,
                                                                                    int change, boolean completes)
    throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(DSE_5_1);
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader input = MockSchema.sstable(1, 128, cfs);
        SSTableReader output = MockSchema.sstable(2, 128, cfs);
        File directory = input.descriptor.directory;
        long others = latestModificationOfAllButTheStatsFile(input);
        // a time that is not on a whole second, which the record keeps
        File stats = statsFile(input, LogFile.truncateMillis(others) + 5 * SECOND + 123);

        LogTransaction txn = new LogTransaction(OperationType.MAJOR_COMPACTION, LifecycleTransaction.newId());
        txn.trackNew(output);
        AbstractLogTransaction.ReaderTidier tidier = txn.obsoleted(input);
        txn.txnFile().commit();
        List<File> logs = txn.logFiles();
        assertThat(logs).hasSize(1);
        Version version = DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion();
        assertThat(logs.get(0).name()).isEqualTo(version + "_txn_compaction_" + txn.id() + ".log");
        List<LogRecord> records = Files.readAllLines(logs.get(0).toPath()).stream().map(LogRecord::make)
                                       .collect(Collectors.toList());
        assertThat(records).filteredOn(r -> r.type.hasFile())
                           .extracting(LogRecord::absolutePath)
                           .containsExactlyInAnyOrder(output.descriptor.baseFile().absolutePath(),
                                                      input.descriptor.baseFile().absolutePath());
        assertThat(records).filteredOn(r -> r.type == LogRecord.Type.REMOVE)
                           .extracting(r -> r.updateTime)
                           .containsExactly(stats.lastModified());
        // each line as DSE 5.1 writes it, checksum included; DSE 5.1 finds the files of a record by its path followed
        // by the separator, which names the files of that sstable only
        for (LogRecord record : records)
        {
            if (!record.type.hasFile())
                continue;
            assertThat(record.raw).isEqualTo(dse51Record(record.type.name(), record.absolutePath(), record.updateTime,
                                                         record.numFiles));
            String prefix = new File(record.absolutePath()).name() + Component.SEPARATOR;
            SSTableReader sstable = record.type == LogRecord.Type.REMOVE ? input : output;
            String sstablePrefix = sstable.descriptor.baseFile().name() + Component.SEPARATOR;
            assertThat(prefix).isEqualTo(sstablePrefix);
            assertThat(directory.tryList((dir, name) -> name.startsWith(prefix))).extracting(File::name)
                                                                                .contains(sstablePrefix + "Data.db")
                                                                                .hasSize(record.numFiles);
        }
        Set<File> inputFiles = LogTransactionTest.getAllFilePaths(input);
        inputFiles.add(stats);
        Set<File> outputFiles = LogTransactionTest.getAllFilePaths(output);
        if (change == STATS_WRITTEN_AGAIN)
            assertThat(stats.trySetLastModified(others + 10 * SECOND)).isTrue();
        if (change == OTHER_FILES_RESTORED)
            for (File file : inputFiles)
                if (!file.equals(stats))
                    assertThat(file.trySetLastModified(others + 15 * SECOND)).isTrue();

        DatabaseDescriptor.setInternodeCompatibilityMode(start);
        List<ILoggingEvent> logged = loggedWhile(() -> {
            assertThat(LogTransaction.removeUnfinishedLeftovers(List.of(directory))).isEqualTo(completes);
        });
        if (completes)
            assertThat(inputFiles).noneMatch(File::exists);
        else
            assertThat(inputFiles).allMatch(File::exists);
        assertThat(outputFiles).allMatch(File::exists);
        assertThat(logs.get(0).exists()).isEqualTo(!completes);

        tidier.commit();
        assertThat(txn.complete(null)).isNull();
        return logged;
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
