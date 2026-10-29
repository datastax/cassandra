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

package org.apache.cassandra.dsecompat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A former DSE node that was not drained before the switch (a crash, a killed process) leaves the active
 * commit log segment behind, in DSE's segment version 680. This software replays it: the segment version
 * maps to the DSE 6.8 messaging version, whose mutation layout the partition update serializer reads. The
 * replay does not depend on {@code internode_compatibility_mode}: each case runs with the modes a former DSE
 * node uses ({@code DSE_6}, storage {@code HCD_1}) and with the defaults, and gets the same result.
 * <p>
 * The fixture segments hold the inserts and deletions of the collection table {@code test_ks_fixture.t}
 * (see {@code test/data/dse-compat/README.md}; the 6.9.26 segment holds them twice, from two runs of the fixture
 * workload), the DSE node's own writes to system tables, and mutations of DSE-only tables and of the test cluster's
 * other keyspaces, which do not exist here and are skipped as unknown tables. The replay at the start keeps
 * such a segment, and replays it again at the next start (shown here with {@code NONE}), unless {@code DSE_6} mode
 * moves it aside. What a real start adds (the startup checks letting version 680 through, a restart after the move)
 * is not exercised here: this test drives the replay directly.
 */
public class DseCommitLogReplayTest extends CQLTester
{
    private static final String FIXTURE_KEYSPACE = DseFixtures.FIXTURE_KEYSPACE;
    /** The 200 inserts and three deletions of the fixture data set, each in its own mutation. */
    private static final int FIXTURE_MUTATIONS = DseFixtures.FIXTURE_MUTATIONS_PER_RUN;
    /** The 6.9.26 segment holds the fixture writes of two runs of the workload: the same values, later timestamps. */
    private static final int FIXTURE_MUTATIONS_6_9_26 = 2 * FIXTURE_MUTATIONS;

    /**
     * The tables of this software the 6.8.65 segment writes to, with the number of partition updates: the
     * fixture table, its schema rows, and DSE's routine writes to local system tables. The DSE-only tables
     * ({@code system_distributed.nodesync_status} and {@code system.nodesync_checkpoints}, whose ids DSE derives from
     * the keyspace and table names)
     * and {@code system_auth} (not loaded by this test server; a real node replays the three
     * {@code role_permissions} updates) are unknown here.
     */
    private static final Map<String, Integer> KNOWN_TABLES_6_8_65 = knownTables6865();

    /**
     * Same for the 6.9.26 segment, which also holds other schema changes, batchlog updates and prepared statements.
     * Unknown here: the DSE-only tables, DSE's own schema tables ({@code system_schema.edges},
     * {@code vertices} and {@code hidden_columns}), {@code system_auth}, and three tables of the test cluster's other keyspaces.
     */
    private static final Map<String, Integer> KNOWN_TABLES_6_9_26 = knownTables6926();

    /**
     * The replayer logs every skipped mutation of an unknown table at DEBUG, some 170000 lines per replay of the
     * fixture segment, which the test output (DEBUG on the console) would hold in memory for every replay of the class.
     */
    private static final Logger REPLAYER_LOGGER = (Logger) LoggerFactory.getLogger(CommitLogReplayer.class);
    private static Level replayerLevel;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void quietReplayer()
    {
        replayerLevel = REPLAYER_LOGGER.getLevel();
        REPLAYER_LOGGER.setLevel(Level.INFO);
    }

    @AfterClass
    public static void restoreReplayerLogLevel()
    {
        REPLAYER_LOGGER.setLevel(replayerLevel);
    }

    @After
    public void dropFixtureKeyspace() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS " + FIXTURE_KEYSPACE);
    }

    @Test
    public void segmentVersion680MapsToTheDse68MessagingVersion()
    {
        CommitLogDescriptor descriptor = CommitLogDescriptor.fromFileName("CommitLog-680-1790248906231.log");
        assertThat(descriptor.getMessagingVersion()).isEqualTo(MessagingService.VERSION_DSE_68);
        assertThat(descriptor.id).isEqualTo(1790248906231L);
    }

    @Test
    public void theSegmentOfDse6865WritesOnlyTheseTables() throws Throwable
    {
        Map<String, Integer> known = new TreeMap<>();
        Map<TableId, Integer> unknown = new HashMap<>();
        readSegment(DseVersion.DSE_6_8_65, known, unknown);

        assertThat(known).isEqualTo(KNOWN_TABLES_6_8_65);
        assertThat(unknown).containsEntry(TableId.fromUUID(DseFixtures.NODESYNC_STATUS_TABLE_ID), 119452)
                           .containsEntry(TableId.fromUUID(DseFixtures.ROLE_PERMISSIONS_TABLE_ID), 3)
                           .containsEntry(TableId.fromUUID(DseFixtures.NODESYNC_CHECKPOINTS_TABLE_ID), 47697)
                           .hasSize(3);
    }

    @Test
    public void theSegmentOfDse6926WritesOnlyTheseTables() throws Throwable
    {
        Map<String, Integer> known = new TreeMap<>();
        Map<TableId, Integer> unknown = new HashMap<>();
        readSegment(DseVersion.DSE_6_9_26, known, unknown);

        assertThat(known).isEqualTo(KNOWN_TABLES_6_9_26);
        assertThat(unknown).containsEntry(TableId.fromUUID(DseFixtures.NODESYNC_STATUS_TABLE_ID), 350)
                           .containsEntry(TableId.fromUUID(DseFixtures.ROLE_PERMISSIONS_TABLE_ID), 3)
                           .containsEntry(TableId.fromUUID(DseFixtures.NODESYNC_CHECKPOINTS_TABLE_ID), 175)
                           .containsEntry(TableId.fromString("0fb31699-813e-336f-be0a-81c75670cbcc"), 2)   // system_schema.edges
                           .containsEntry(TableId.fromString("17d1d931-9590-371b-ae20-ff78d3c9ca0c"), 2)   // system_schema.vertices
                           .containsEntry(TableId.fromString("2461e8b2-8f6a-31a1-ac72-30b9cd9c6ca5"), 2)   // system_schema.hidden_columns
                           // three tables of the test cluster's other keyspaces, created before the fixture keyspace
                           .containsEntry(TableId.fromString("1dcbe004-bb0f-11f1-91d0-b3b5fc586fca"), 1)
                           .containsEntry(TableId.fromString("1e795e11-bb0f-11f1-91d0-b3b5fc586fca"), 8)
                           .containsEntry(TableId.fromString("246701b0-bb0f-11f1-91d0-b3b5fc586fca"), 12)
                           .hasSize(9);
    }

    /**
     * Reads the segment of a version with the fixture table created, counting the partition updates by table:
     * those of tables known here by name, those of unknown tables by id.
     */
    private void readSegment(DseVersion version, Map<String, Integer> known, Map<TableId, Integer> unknown) throws Throwable
    {
        createFixtureSchema(version);
        Path segment = DseFixtures.unpackCommitLogSegment(version, folder.newFolder().toPath());

        new CommitLogReader().readCommitLogSegment(new CommitLogReadHandler()
        {
            public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
            {
                throw exception;
            }

            public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
            {
                throw exception;
            }

            public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
            {
                for (PartitionUpdate update : m.getPartitionUpdates())
                    known.merge(update.metadata().toString(), 1, Integer::sum);
            }

            public void handleInvalidMutation(TableId id)
            {
                unknown.merge(id, 1, Integer::sum);
            }
        }, new File(segment), false);
    }

    @Test
    public void replaysTheSegmentOfDse6865InDse6Mode() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1, () -> replayAndVerify(DseVersion.DSE_6_8_65));
    }

    @Test
    public void replaysTheSegmentOfDse6865WithTheDefaultModes() throws Throwable
    {
        withModes(InternodeCompatibilityMode.NONE, StorageCompatibilityMode.NONE, () -> replayAndVerify(DseVersion.DSE_6_8_65));
    }

    @Test
    public void replaysTheSegmentOfDse6926InDse6Mode() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1, () -> replayAndVerify(DseVersion.DSE_6_9_26));
    }

    @Test
    public void replaysTheSegmentOfDse6926WithTheDefaultModes() throws Throwable
    {
        withModes(InternodeCompatibilityMode.NONE, StorageCompatibilityMode.NONE, () -> replayAndVerify(DseVersion.DSE_6_9_26));
    }

    @Test
    public void dse6ModeMovesTheReplayedDse6865SegmentAside() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1, () -> assertMovedAside(replayAsAtStartup()));
    }

    @Test
    public void dse6ModeMovesTheReplayedDse6926SegmentAside() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1,
                  () -> assertMovedAside(replay(DseVersion.DSE_6_9_26, ColumnFamilyStore.FlushReason.STARTUP)));
    }

    @Test
    public void noneModeKeepsThe6926SegmentWithUnknownTableMutations() throws Throwable
    {
        withModes(InternodeCompatibilityMode.NONE, StorageCompatibilityMode.NONE,
                  () -> assertKept(replay(DseVersion.DSE_6_9_26, ColumnFamilyStore.FlushReason.STARTUP)));
    }

    private static void assertMovedAside(Path segment)
    {
        assertThat(Files.exists(segment)).isFalse();
        assertThat(Files.exists(segment.resolveSibling(segment.getFileName() + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX))).isTrue();
    }

    private static void assertKept(Path segment)
    {
        assertThat(Files.exists(segment)).isTrue();
        assertThat(Files.exists(segment.resolveSibling(segment.getFileName() + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX))).isFalse();
    }

    /**
     * The reader flags only the fixture segment for skipped mutations of unknown tables; a header-only DSE segment (as
     * a freshly allocated segment is at a crash) is not flagged, and is moved aside all the same.
     */
    @Test
    public void dse6ModeMovesAsideEveryDseSegmentOfTheStartupReplay() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1, () -> {
            createFixtureSchema(DseVersion.DSE_6_8_65);
            Path segment = DseFixtures.unpackCommitLogSegment(DseVersion.DSE_6_8_65, folder.newFolder().toPath());
            Path next = emptyDseSegment(segment.getParent(), CommitLogDescriptor.fromFileName(segment.getFileName().toString()).id + 1);

            Map<Keyspace, Integer> replayed = CommitLog.instance.recoverSegmentsOnDiskNoArchive(ColumnFamilyStore.FlushReason.STARTUP,
                                                                                                new File[]{ new File(segment), new File(next) });
            assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(FIXTURE_MUTATIONS);
            try (Stream<Path> files = Files.list(segment.getParent()))
            {
                assertThat(files.map(p -> p.getFileName().toString()))
                .containsExactlyInAnyOrder(segment.getFileName() + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX,
                                           next.getFileName() + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX);
            }
        });
    }

    /**
     * A failed rename leaves the segment to the usual handling, which keeps the fixture segment (mutations of unknown
     * tables were skipped in it): it would be replayed again at the next start, as the WARN says.
     */
    @Test
    public void dse6ModeKeepsTheFlaggedSegmentWhenTheRenameFails() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1, () -> {
            createFixtureSchema(DseVersion.DSE_6_8_65);
            Path segment = DseFixtures.unpackCommitLogSegment(DseVersion.DSE_6_8_65, folder.newFolder().toPath());
            long size = Files.size(segment);
            blockRename(segment);

            List<String> warnings = replayAtStartupCollectingWarnings(segment, FIXTURE_MUTATIONS);

            assertThat(Files.size(segment)).isEqualTo(size);
            assertThat(warnings).anyMatch(w -> w.startsWith("Unable to move the DSE commit log segment " + segment) &&
                                               w.contains("which by default keeps it: it will be replayed again at the next start"));
        });
    }

    /** A failed rename of a segment the reader did not flag leaves it to the usual handling, which deletes it. */
    @Test
    public void dse6ModeDeletesAnUnflaggedSegmentWhenTheRenameFails() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1, () -> {
            Path segment = emptyDseSegment(folder.newFolder().toPath(), 1790248906232L);
            blockRename(segment);

            List<String> warnings = replayAtStartupCollectingWarnings(segment, null);

            assertThat(Files.exists(segment)).isFalse();
            assertThat(warnings).anyMatch(w -> w.startsWith("Unable to move the DSE commit log segment " + segment) &&
                                               w.contains("which by default deletes it"));
        });
    }

    /** A non-empty directory where the renamed file would go makes the rename fail. */
    private static void blockRename(Path segment) throws IOException
    {
        Path blocker = Files.createDirectories(segment.resolveSibling(segment.getFileName() + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX));
        Files.createFile(blocker.resolve("x"));
    }

    /**
     * Replays the segment through the start-up path, asserts the updates replayed into the fixture keyspace (none when
     * {@code expectedFixtureUpdates} is null) and returns the WARN messages of {@link CommitLog}.
     */
    private static List<String> replayAtStartupCollectingWarnings(Path segment, Integer expectedFixtureUpdates) throws IOException
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CommitLog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            Map<Keyspace, Integer> replayed = CommitLog.instance.recoverSegmentsOnDiskNoArchive(ColumnFamilyStore.FlushReason.STARTUP,
                                                                                                new File[]{ new File(segment) });
            if (expectedFixtureUpdates == null)
                assertThat(replayed).isEmpty();
            else
                assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(expectedFixtureUpdates);
            return appender.list.stream()
                                .filter(event -> event.getLevel() == Level.WARN)
                                .map(ILoggingEvent::getFormattedMessage)
                                .collect(Collectors.toList());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    public void dse6ModeMovesTheSegmentAsideOnlyAtTheStartupReplay() throws Throwable
    {
        withModes(InternodeCompatibilityMode.DSE_6, StorageCompatibilityMode.HCD_1,
                  () -> assertKept(replay(DseVersion.DSE_6_8_65, ColumnFamilyStore.FlushReason.REMOTE_REPLAY)));
    }

    @Test
    public void noneModeKeepsThe6865SegmentWithUnknownTableMutations() throws Throwable
    {
        withModes(InternodeCompatibilityMode.NONE, StorageCompatibilityMode.NONE, () -> assertKept(replayAsAtStartup()));
    }

    /**
     * The hazard the rename of {@code DSE_6} mode prevents: the segment kept by the usual handling (mutations of unknown
     * tables were skipped) is replayed again at the next start, and brings back a partition whose deletion was purged
     * since by a compaction (gc_grace_seconds 0), as nothing on disk shadows the replayed rows any more.
     */
    @Test
    public void aKeptSegmentReplayedAgainResurrectsAPurgedDeletion() throws Throwable
    {
        // the storage mode of the test configuration: the system tables (compaction_history, written by the compaction)
        // were created with its layout at start-up
        withModes(InternodeCompatibilityMode.NONE, DatabaseDescriptor.getStorageCompatibilityMode(), () -> {
            Path segment = replayAsAtStartup();
            String t = FIXTURE_KEYSPACE + ".t";
            execute("ALTER TABLE " + t + " WITH gc_grace_seconds = 0");
            execute("DELETE FROM " + t + " WHERE id = 25");
            flush(FIXTURE_KEYSPACE, "t");
            // the tombstone's deletion time must be in the past for gc_grace_seconds 0 to purge it: it is the server's
            // clock in seconds at the deletion, which CQL cannot set, hence the wait of a little over a second
            Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
            compact(FIXTURE_KEYSPACE, "t");
            ColumnFamilyStore cfs = Keyspace.open(FIXTURE_KEYSPACE).getColumnFamilyStore("t");
            assertThat(cfs.getLiveSSTables()).as("one sstable after the major compaction").hasSize(1);
            assertThat(partitionOnDisk(cfs, 25)).as("partition 25 and its tombstone purged").isFalse();
            assertRows(execute("SELECT count(*) FROM " + t), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR - 2));

            // the next start: the kept segment is in the commit log directory and replayed again
            Map<Keyspace, Integer> replayed = CommitLog.instance.recoverSegmentsOnDiskNoArchive(ColumnFamilyStore.FlushReason.STARTUP,
                                                                                                new File[]{ new File(segment) });
            assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(FIXTURE_MUTATIONS);
            assertRows(execute("SELECT count(*) FROM " + t), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
            assertRows(execute("SELECT id, ck, v FROM " + t + " WHERE id = 25"), row(25, "ck1", "val49"), row(25, "ck2", "val50"));
        });
    }

    /**
     * A DSE segment holding only its header (as a freshly allocated segment is at a crash): skipped as empty by the
     * replay, and never flagged for skipped mutations.
     */
    private static Path emptyDseSegment(Path directory, long id) throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.VERSION_DSE_68, id, null, new EncryptionContext());
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        buffer.putInt(0).putInt(0); // no sync marker: the end of the log
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Files.write(directory.resolve(descriptor.fileName()), bytes);
    }

    /** Whether a live sstable of the table holds anything (data or tombstone) for the partition. */
    private static boolean partitionOnDisk(ColumnFamilyStore cfs, int id)
    {
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(id));
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        if (partition.partitionKey().equals(key))
                            return true;
                    }
                }
            }
        }
        return false;
    }

    /** Replays the 6.8.65 segment through the start-up path, which also disposes of the replayed segment. */
    private Path replayAsAtStartup() throws Throwable
    {
        return replay(DseVersion.DSE_6_8_65, ColumnFamilyStore.FlushReason.STARTUP);
    }

    /** Replays the segment of a version through the path of a start ({@code STARTUP}) or of a remote replay. */
    private Path replay(DseVersion version, ColumnFamilyStore.FlushReason flushReason) throws Throwable
    {
        createFixtureSchema(version);
        Path segment = DseFixtures.unpackCommitLogSegment(version, folder.newFolder().toPath());
        Map<Keyspace, Integer> replayed = CommitLog.instance.recoverSegmentsOnDiskNoArchive(flushReason, new File[]{ new File(segment) });
        assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(fixtureMutations(version));
        return segment;
    }

    private static int fixtureMutations(DseVersion version)
    {
        switch (version)
        {
            case DSE_6_8_65:
                return FIXTURE_MUTATIONS;
            case DSE_6_9_26:
                return FIXTURE_MUTATIONS_6_9_26;
            default:
                throw new AssertionError("no commit log segment of " + version);
        }
    }

    private interface Body
    {
        void run() throws Throwable;
    }

    private static void withModes(InternodeCompatibilityMode internode, StorageCompatibilityMode storage, Body body) throws Throwable
    {
        InternodeCompatibilityMode previousInternode = DatabaseDescriptor.getInternodeCompatibilityMode();
        StorageCompatibilityMode previousStorage = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(internode);
        DatabaseDescriptor.setStorageCompatibilityMode(storage);
        try
        {
            body.run();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(previousInternode);
            DatabaseDescriptor.setStorageCompatibilityMode(previousStorage);
        }
    }

    private void replayAndVerify(DseVersion version) throws Throwable
    {
        createFixtureSchema(version);

        Path segment = DseFixtures.unpackCommitLogSegment(version, folder.newFolder().toPath());
        assertThat(CommitLogDescriptor.fromFileName(segment.getFileName().toString()).getMessagingVersion())
        .isEqualTo(MessagingService.VERSION_DSE_68);

        Map<Keyspace, Integer> replayed = CommitLog.instance.recoverFiles(ColumnFamilyStore.FlushReason.STARTUP, new File(segment));

        assertThat(replayed.keySet().stream().map(Keyspace::getName))
        .containsExactlyInAnyOrder("system", "system_schema", FIXTURE_KEYSPACE);
        // 200 inserted rows plus the three deletions, each in its own mutation (twice in the 6.9.26 segment)
        assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(fixtureMutations(version));
        // after the replay of the 6.9.26 segment, with its six batchlog updates (theSegmentOfDse6926WritesOnlyTheseTables),
        // no batch is left in system.batches for the batchlog to replay; the 6.8.65 segment has none
        if (version == DseVersion.DSE_6_9_26)
            assertRows(execute("SELECT count(*) FROM system.batches"), row(0L));

        // the data set of the fixtures README: 100 partitions of two rows, partition 1 deleted, cell v of (2, ck1)
        // deleted, the clustering range ck1..ck2 of partition 3 deleted (its static row survives): count(*) counts the
        // live regular rows plus one row for the partition left with only its static row
        String t = FIXTURE_KEYSPACE + ".t";
        assertRows(execute("SELECT count(*) FROM " + t), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
        assertRows(execute("SELECT id, ck FROM " + t + " WHERE id = 1"));
        assertRows(execute("SELECT id, ck, s FROM " + t + " WHERE id = 3"), row(3, null, 30));
        assertRows(execute("SELECT id, ck, v, n, s FROM " + t + " WHERE id = 2"),
                   row(2, "ck1", null, 300L, 20),
                   row(2, "ck2", "val4", 400L, 20));
        assertRows(execute("SELECT id, ck, fu, l, m, n, s, tp, u, v FROM " + t + " WHERE id = 25 AND ck = 'ck2'"),
                   row(25, "ck2",
                       userType("street", "Street25", "zip", 100025),
                       list(25, 26, 27),
                       map("m", 25),
                       5000L,
                       250,
                       tuple(25, "tuple25"),
                       userType("street", "Avenue25", "zip", 200025),
                       "val50"));
        assertRows(execute("SELECT id, ck FROM " + t + " WHERE v = 'val50' ALLOW FILTERING"), row(25, "ck2"));
        assertRows(execute("SELECT id, ck, v, n, s FROM " + t + " WHERE id = 100"),
                   row(100, "ck1", "val199", 19900L, 1000),
                   row(100, "ck2", "val200", 20000L, 1000));
    }

    /**
     * The schema the fixture segment was written with, created before its replay, with
     * {@code cassandra.internode_compatibility_mode.allow_ddl}, which the tests of the mode set around their schema
     * changes, set for the duration.
     */
    private void createFixtureSchema(DseVersion version) throws Throwable
    {
        try (WithProperties allowDdl = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            for (String statement : DseFixtures.fixtureSchema(version))
                execute(statement);
        }
    }

    private static Map<String, Integer> knownTables6926()
    {
        Map<String, Integer> tables = new TreeMap<>();
        tables.put(FIXTURE_KEYSPACE + ".t", FIXTURE_MUTATIONS_6_9_26);
        // the schema rows DSE wrote while the segment was active, those of the fixture keyspace among them
        tables.put("system_schema.aggregates", 2);
        tables.put("system_schema.columns", 5);
        tables.put("system_schema.dropped_columns", 3);
        tables.put("system_schema.functions", 2);
        tables.put("system_schema.indexes", 3);
        tables.put("system_schema.keyspaces", 8);
        tables.put("system_schema.tables", 6);
        tables.put("system_schema.triggers", 2);
        tables.put("system_schema.types", 3);
        tables.put("system_schema.views", 2);
        // node-local tables: statistics, the index build record, batchlog entries and prepared statements
        tables.put("system.\"IndexInfo\"", 1);
        tables.put("system.batches", 6);
        tables.put("system.compaction_history", 12);
        tables.put("system.prepared_statements", 9);
        tables.put("system.size_estimates", 965);
        tables.put("system.sstable_activity", 516);
        return tables;
    }

    private static Map<String, Integer> knownTables6865()
    {
        Map<String, Integer> tables = new TreeMap<>();
        tables.put(FIXTURE_KEYSPACE + ".t", FIXTURE_MUTATIONS);
        // the schema rows of the fixture keyspace, type, table and SAI index, as DSE wrote them
        tables.put("system_schema.keyspaces", 4);
        tables.put("system_schema.tables", 2);
        tables.put("system_schema.columns", 1);
        tables.put("system_schema.indexes", 1);
        tables.put("system_schema.types", 1);
        // node-local statistics and the index build record; nothing in system.local, peers, paxos or batches
        tables.put("system.\"IndexInfo\"", 1);
        tables.put("system.compaction_history", 5);
        tables.put("system.size_estimates", 2376);
        tables.put("system.sstable_activity", 548);
        return tables;
    }
}
