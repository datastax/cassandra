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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.db.view.DseViewWireFormat;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMKiller;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * is not exercised here, where the test drives the replay directly: the in-JVM {@code Dse6RollbackArtefactsTest}
 * covers it. No DSE-written segment holds the update of a materialized view: the tests of such updates replay a segment
 * of version 680 built here ({@code dseSegment}), holding an update built in DSE's layout.
 * <p>
 * DSE 5.1 writes segments of version 6, the Cassandra 3.x layout (the header, the sync markers and the entries as in a
 * segment of this software, the mutations at messaging version 11, laid out as at 10 and 12), which the replay reads
 * at messaging version 10. The segments DSE 5.1.51 wrote with entries (the active one,
 * {@link Dse51Fixtures#commitLogSegmentGz()}, and the older one of {@link Dse51Fixtures#killedCommitLogDirectory()},
 * which continues it) hold, beside the collection table's mutations, the writes of DSE's endpoint state persister
 * to {@code system.local} and {@code system.peers}, which name the columns DSE 5.1 adds to those tables
 * ({@link Dse51SystemTables}): their headers are checked against the one built here, and their replay in each mode
 * is tested. The tests of what the fixtures do not hold or what is not checked on them (a mutation naming another
 * unknown column, the sstable flushed from the replayed persister's writes, a replay without the mode that completes)
 * replay a segment of version 6 built here ({@code segment}), holding mutations this software serializes at version
 * 12, those of the persister serialized from the definitions of those tables with the five columns
 * ({@code persisterMutations}, {@link Dse51SystemTables#withDse51Columns}).
 */
public class DseCommitLogReplayTest extends CQLTester
{
    private static final String FIXTURE_KEYSPACE = DseFixtures.FIXTURE_KEYSPACE;
    /** The 200 inserts and three deletions of the fixture data set, each in its own mutation. */
    private static final int FIXTURE_MUTATIONS = DseFixtures.FIXTURE_MUTATIONS_PER_RUN;
    /** The 6.9.26 segment holds the fixture writes of two runs of the workload: the same values, later timestamps. */
    private static final int FIXTURE_MUTATIONS_6_9_26 = 2 * FIXTURE_MUTATIONS;
    /** The table {@code dse51Segment} writes to, in the fixture keyspace, which each test drops. */
    private static final String DSE_51_TABLE = FIXTURE_KEYSPACE + ".dse51";
    /** The rows {@code dse51Segment} writes to {@link #DSE_51_TABLE}. */
    private static final int DSE_51_ROWS = 3;

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
    /** The logger of the WARN saying why a view update DSE stored is not translated. */
    private static final Logger TRANSLATION_LOGGER = (Logger) LoggerFactory.getLogger(DseViewWireFormat.class);
    /** The logger of the INFO line of the mutations naming the columns DSE 5.1 adds to system.local and peers. */
    private static final Logger READER_LOGGER = (Logger) LoggerFactory.getLogger(CommitLogReader.class);
    /** The mutations naming the five columns in the active segment of the DSE 5.1.51 fixtures. */
    private static final int DSE_51_ACTIVE_SEGMENT_PERSISTER_MUTATIONS = 146;
    /**
     * The same in the older segment of the killed DSE 5.1.51 node, which continues the active one: its mutations, and
     * the 20 the persister wrote for both peers after node 3 restarted.
     */
    private static final int DSE_51_KILLED_SEGMENT_PERSISTER_MUTATIONS =
        DSE_51_ACTIVE_SEGMENT_PERSISTER_MUTATIONS + 20;
    /**
     * The first mutation the replay of the segments DSE 5.1.51 wrote stops on without the mode: a write of DSE's
     * endpoint state persister.
     */
    private static final String FIRST_UNKNOWN_COLUMN_OF_THE_DSE_51_SEGMENTS =
        "Unknown column dse_version during deserialization of system.local";
    /** The peer of the writes of DSE 5.1's endpoint state persister ({@code persisterMutations}). */
    private static final InetAddress DSE_51_PEER = InetAddresses.forString("10.0.51.2");
    private static final UUID DSE_51_HOST_ID = UUID.fromString("5a5a5a5a-0000-4000-8000-000000005151");
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
        // the rows the replays of DSE 5.1's segments write, also when a test fails before it truncates them
        execute("TRUNCATE system.peers");
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
     * In {@code DSE_6}, the update of a materialized view in a DSE segment, which names DSE's hidden column of the view, is
     * replayed when the catalog of DSE's hidden columns lists that column for the view, as a batch or a hint DSE stored:
     * the rows get the liveness of their hidden cells, so the entry DSE wrote dead (a hidden tombstone) stays dead.
     */
    @Test
    public void aViewUpdateOfADseSegmentIsTranslatedInDse6Mode() throws Throwable
    {
        TableMetadata view = createFixtureView();
        Path segment = dseSegment(folder.newFolder().toPath(), DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(view, "x")));
        DseViewHiddenColumnCatalog before = DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(
            Map.of(Pair.create(FIXTURE_KEYSPACE, view.name), List.of(new HiddenColumn(".v", "regular", true, "text")))));
        try
        {
            withModes(InternodeCompatibilityMode.DSE_6, DatabaseDescriptor.getStorageCompatibilityMode(), () -> {
                Map<Keyspace, Integer> replayed = CommitLog.instance.recoverSegmentsOnDiskNoArchive(ColumnFamilyStore.FlushReason.STARTUP,
                                                                                                    new File[]{ new File(segment) });
                assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(1);
            });
        }
        finally
        {
            DseViewHiddenColumnCatalog.setInstance(before);
        }
        assertRows(execute("SELECT v, id, ck FROM " + view + " WHERE v = 'x'"), row("x", 1, "a"));
    }

    /**
     * The update of a materialized view in a DSE segment fails its replay as before, as a mutation naming a column unknown
     * here, when the translation of the hidden column is not certain (in {@code DSE_6}, the catalog of DSE's hidden columns
     * lists no hidden column for the view; a WARN says why) or not made (without the mode). The replay errors are ignored
     * here, as with {@code -Dcassandra.commitlog.ignorereplayerrors=true}, rather than stop the replay; the copy of the
     * mutation the replay saves is deleted.
     */
    @Test
    public void aViewUpdateOfADseSegmentFailsItsReplayWhenItsTranslationIsUncertain() throws Throwable
    {
        TableMetadata view = createFixtureView();
        ByteBuffer update = DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(view, "x"));
        DseViewHiddenColumnCatalog before = DseViewHiddenColumnCatalog.setInstance(new DseViewHiddenColumnCatalog(Map.of()));
        try (WithProperties ignoreErrors = new WithProperties().set(CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS, true))
        {
            for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            {
                Path segment = dseSegment(folder.newFolder().toPath(), update);
                ListAppender<ILoggingEvent> appender = new ListAppender<>();
                appender.start();
                REPLAYER_LOGGER.addAppender(appender);
                ListAppender<ILoggingEvent> translation = new ListAppender<>();
                translation.start();
                TRANSLATION_LOGGER.addAppender(translation);
                try
                {
                    withModes(mode, DatabaseDescriptor.getStorageCompatibilityMode(), () -> {
                        Map<Keyspace, Integer> replayed = CommitLog.instance.recoverSegmentsOnDiskNoArchive(ColumnFamilyStore.FlushReason.STARTUP,
                                                                                                            new File[]{ new File(segment) });
                        assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).as("mutations replayed in %s", mode).isNull();
                    });
                }
                finally
                {
                    REPLAYER_LOGGER.detachAppender(appender);
                    appender.stop();
                    TRANSLATION_LOGGER.detachAppender(translation);
                    translation.stop();
                }
                List<String> errors = appender.list.stream()
                                                   .filter(event -> event.getLevel() == Level.ERROR && event.getFormattedMessage().equals("Ignoring commit log replay error"))
                                                   .map(event -> event.getThrowableProxy().getMessage())
                                                   .collect(Collectors.toList());
                assertThat(errors).as("replay errors in %s", mode).singleElement().asString()
                                  .startsWith("Unexpected error deserializing mutation; saved to ")
                                  .endsWith("Exception follows: java.lang.RuntimeException: Unknown column .v during deserialization of " + view);
                String saved = errors.get(0).substring("Unexpected error deserializing mutation; saved to ".length());
                Files.delete(Path.of(saved.substring(0, saved.indexOf(".  "))));
                if (mode == InternodeCompatibilityMode.DSE_6)
                    assertThat(translation.list).singleElement().satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.WARN);
                        assertThat(event.getFormattedMessage()).contains("names the column .v of materialized view " + view + ", which is not translated: " +
                                                                         "DSE's system_schema.hidden_columns on this node lists no hidden column for that view.");
                    });
                else
                    assertThat(translation.list).isEmpty();
            }
        }
        finally
        {
            DseViewHiddenColumnCatalog.setInstance(before);
        }
        assertRows(execute("SELECT * FROM " + view));
    }

    /**
     * The view {@code t_by_v} of the 6.8.65 fixture table, keyed by {@code (v, id, ck)}. The keyspace system_distributed,
     * where the build of a view is recorded, is created here: the tests run without the network, which creates it otherwise.
     */
    private TableMetadata createFixtureView() throws Throwable
    {
        Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(SystemDistributedKeyspace.metadata(), SystemDistributedKeyspace.GENERATION));
        createFixtureSchema(DseVersion.DSE_6_8_65);
        execute("CREATE MATERIALIZED VIEW " + FIXTURE_KEYSPACE + ".t_by_v AS SELECT id, ck, v FROM " + FIXTURE_KEYSPACE + ".t " +
                "WHERE v IS NOT NULL AND id IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, id, ck)");
        return Schema.instance.getTableMetadata(FIXTURE_KEYSPACE, "t_by_v");
    }

    /**
     * A DSE segment (version 680) holding one mutation, framed as a commit log segment frames it: the header, a sync marker
     * (the position of the next one and the CRC32 of the segment id and its own position), the mutation (its size, the
     * CRC32 of the size, the mutation, the CRC32 of the size and the mutation), then an empty sync marker, the end of the
     * log. A fresh segment id, above those of the segments of this test server.
     */
    private static Path dseSegment(Path directory, ByteBuffer mutation) throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.VERSION_DSE_68, CommitLog.instance.getCurrentPosition().segmentId + 1000,
                                                                 null, new EncryptionContext());
        ByteBuffer buffer = ByteBuffer.allocate(4096 + mutation.remaining());
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        int markerPosition = buffer.position();
        int end = markerPosition + 8 + 4 + 4 + mutation.remaining() + 4;
        CRC32 crc = new CRC32();
        FBUtilities.updateChecksumInt(crc, (int) (descriptor.id & 0xFFFFFFFFL));
        FBUtilities.updateChecksumInt(crc, (int) (descriptor.id >>> 32));
        FBUtilities.updateChecksumInt(crc, markerPosition);
        buffer.putInt(end).putInt((int) crc.getValue());
        crc.reset();
        FBUtilities.updateChecksumInt(crc, mutation.remaining());
        buffer.putInt(mutation.remaining()).putInt((int) crc.getValue());
        crc.update(mutation.duplicate());
        buffer.put(mutation.duplicate()).putInt((int) crc.getValue());
        assertThat(buffer.position()).isEqualTo(end);
        buffer.putInt(0).putInt(0);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Files.write(directory.resolve(descriptor.fileName()), bytes);
    }

    /**
     * A segment of {@code version} holding {@code mutations} in one sync section, framed as {@link #dseSegment} frames
     * its mutation: the header, a sync marker, each mutation with its size and CRC32s, then an empty sync marker. The
     * segments of DSE 5.1 (version 6) are built with it.
     */
    private static Path segment(Path directory, int version, List<ByteBuffer> mutations) throws IOException
    {
        long id = CommitLog.instance.getCurrentPosition().segmentId + 1000;
        CommitLogDescriptor descriptor = new CommitLogDescriptor(version, id, null, new EncryptionContext());
        int entries = 0;
        for (ByteBuffer mutation : mutations)
            entries += 4 + 4 + mutation.remaining() + 4;
        ByteBuffer buffer = ByteBuffer.allocate(4096 + entries);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        int markerPosition = buffer.position();
        int end = markerPosition + 8 + entries;
        CRC32 crc = new CRC32();
        FBUtilities.updateChecksumInt(crc, (int) (descriptor.id & 0xFFFFFFFFL));
        FBUtilities.updateChecksumInt(crc, (int) (descriptor.id >>> 32));
        FBUtilities.updateChecksumInt(crc, markerPosition);
        buffer.putInt(end).putInt((int) crc.getValue());
        for (ByteBuffer mutation : mutations)
        {
            crc.reset();
            FBUtilities.updateChecksumInt(crc, mutation.remaining());
            buffer.putInt(mutation.remaining()).putInt((int) crc.getValue());
            crc.update(mutation.duplicate());
            buffer.put(mutation.duplicate()).putInt((int) crc.getValue());
        }
        assertThat(buffer.position()).isEqualTo(end);
        buffer.putInt(0).putInt(0);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Files.write(directory.resolve(descriptor.fileName()), bytes);
    }

    @Test
    public void segmentVersion6MapsToMessagingVersion10() throws IOException
    {
        String name = DseFixtures.dse51().commitLogSegmentGz().getFileName().toString().replaceFirst("\\.gz$", "");
        assertThat(name).startsWith("CommitLog-" + Dse51Fixtures.COMMIT_LOG_VERSION + '-');
        CommitLogDescriptor descriptor = CommitLogDescriptor.fromFileName(name);
        assertThat(descriptor.getMessagingVersion()).isEqualTo(MessagingService.VERSION_30);
    }

    /**
     * The header of the segments of version 6 built here ({@code segment}) is the one DSE 5.1.51 writes
     * ({@link #dse51Header}), which the segments DSE 5.1.51 wrote start with
     * ({@link #theHeaderOfTheSegmentsDse51WroteIsTheOneBuiltHere}).
     */
    @Test
    public void theHeaderOfAVersion6SegmentIsTheOneDse51Writes() throws Throwable
    {
        Path segment = segment(folder.newFolder().toPath(), CommitLogDescriptor.VERSION_30, List.of());
        long id = CommitLogDescriptor.fromFileName(segment.getFileName().toString()).id;
        byte[] expected = dse51Header(id);
        byte[] bytes = Files.readAllBytes(segment);
        assertThat(Arrays.copyOf(bytes, expected.length)).isEqualTo(expected);
    }

    /**
     * The segments DSE 5.1.51 wrote (the active segment of node 1, and both segments of that node killed without a
     * drain) start with the header built here, byte for byte, with the id of their name, and this software reads that
     * header as one of a segment of version 6.
     */
    @Test
    public void theHeaderOfTheSegmentsDse51WroteIsTheOneBuiltHere() throws Throwable
    {
        Dse51Fixtures dse51 = DseFixtures.dse51();
        List<Path> segments = new ArrayList<>(dse51.unpackCommitLogSegments(dse51.commitLogSegmentGz().getParent(),
                                                                            folder.newFolder().toPath()));
        segments.addAll(dse51.unpackCommitLogSegments(dse51.killedCommitLogDirectory(), folder.newFolder().toPath()));
        assertThat(segments).hasSize(3);
        for (Path segment : segments)
        {
            long id = CommitLogDescriptor.fromFileName(segment.getFileName().toString()).id;
            byte[] expected = dse51Header(id);
            byte[] bytes = Files.readAllBytes(segment);
            assertThat(Arrays.copyOf(bytes, expected.length)).as(segment.toString()).isEqualTo(expected);
            CommitLogDescriptor header = CommitLogDescriptor.fromHeader(new File(segment), new EncryptionContext());
            assertThat(header).as(segment.toString()).isNotNull();
            // the name of a descriptor holds its version and its id
            assertThat(header.fileName()).as(segment.toString()).isEqualTo(segment.getFileName().toString())
                                         .startsWith("CommitLog-" + Dse51Fixtures.COMMIT_LOG_VERSION + '-');
            assertThat(header.getMessagingVersion()).as(segment.toString()).isEqualTo(MessagingService.VERSION_30);
        }
    }

    /**
     * The header of a segment of version 6 with the id {@code id}, as DSE 5.1.51 writes it, by its source: the version
     * (an int), the id (a long), the length of the parameters (an unsigned short), the parameters (a JSON map, empty),
     * then the CRC32 of the version, of the id as two ints (low half first), of the length as an int and of the
     * parameters, all big endian. Built byte by byte, apart from this software's writer.
     */
    private static byte[] dse51Header(long id)
    {
        byte[] parameters = "{}".getBytes(StandardCharsets.UTF_8);
        ByteBuffer expected = ByteBuffer.allocate(4 + 8 + 2 + parameters.length + 4);
        expected.putInt(6).putLong(id).putShort((short) parameters.length).put(parameters);
        CRC32 crc = new CRC32();
        crc.update(ByteBuffer.allocate(16).putInt(6).putInt((int) id).putInt((int) (id >>> 32))
                             .putInt(parameters.length).array());
        crc.update(parameters);
        expected.putInt((int) crc.getValue());
        return expected.array();
    }

    /**
     * In {@code DSE_5_1}, the active segment of a DSE 5.1.51 node (version 6) read at the start is replayed: the
     * mutations of the collection table give back the fixture data set, those of tables unknown here (DSE 5.1's own,
     * and those of the cluster's other keyspaces) are skipped, those of {@code system.local} and {@code system.peers}
     * naming the columns DSE 5.1 adds are applied without them, counted by one INFO line; the segment is then renamed
     * so that it is not replayed again.
     */
    @Test
    public void dse51ModeReplaysADse51SegmentAndMovesItAside() throws Throwable
    {
        Path segment = replayTheActiveDse51Segment();
        assertMovedAside(segment);
        assertFixtureDataSet();
        assertDse51Peers();
        assertTheLocalRowIsThisServers();
    }

    /**
     * In {@code DSE_5_1}, the segments of a DSE 5.1.51 node killed without a drain after a peer's restart are replayed
     * at the start and moved aside: the older one holds, in its tail, the writes of DSE's endpoint state persister to
     * {@code system.peers} for both peers after node 3 restarted, which are applied without the columns DSE 5.1 adds
     * (node 3's row is written later than by the active segment alone), counted by its INFO line; the newer one holds
     * its header only.
     */
    @Test
    public void dse51ModeReplaysTheSegmentsOfAKilledDse51Node() throws Throwable
    {
        replayTheActiveDse51Segment();
        long beforeTheRestart = latestWriteOfTheRowOf(3);
        execute("TRUNCATE system.peers");
        prepareTheReplayOfDse51Segments();
        Dse51Fixtures dse51 = DseFixtures.dse51();
        List<Path> segments = dse51.unpackCommitLogSegments(dse51.killedCommitLogDirectory(),
                                                            folder.newFolder().toPath());
        assertThat(segments).hasSize(2);
        // the newer segment: its header only, then the zeros of the preallocated file, no sync marker
        byte[] header = dse51Header(CommitLogDescriptor.fromFileName(segments.get(1).getFileName().toString()).id);
        byte[] newer = Files.readAllBytes(segments.get(1));
        assertThat(Arrays.copyOf(newer, header.length)).isEqualTo(header);
        assertThat(Arrays.copyOfRange(newer, header.length, newer.length)).containsOnly((byte) 0);
        File[] files = segments.stream().map(File::new).toArray(File[]::new);
        withDse51SSTableFormat(() -> {
            withModes(InternodeCompatibilityMode.DSE_5_1, StorageCompatibilityMode.HCD_1, () -> {
                List<String> info = new ArrayList<>();
                List<String> warnings = new ArrayList<>();
                Logger commitLogLogger = (Logger) LoggerFactory.getLogger(CommitLog.class);
                Map<Keyspace, Integer> replayed = collecting(READER_LOGGER, Level.INFO, info, () -> {
                    return collecting(commitLogLogger, Level.WARN, warnings, () -> {
                        ColumnFamilyStore.FlushReason startup = ColumnFamilyStore.FlushReason.STARTUP;
                        return CommitLog.instance.recoverSegmentsOnDiskNoArchive(startup, files);
                    });
                });
                for (Path segment : segments)
                    assertThat(warnings).anyMatch(w -> w.startsWith("Commit log segment " + segment + " was " +
                                                                    "written by DataStax Enterprise 5.1 and read " +
                                                                    "at the start"));
                assertThat(replayed.get(Keyspace.open(FIXTURE_KEYSPACE))).isEqualTo(FIXTURE_MUTATIONS);
                assertThat(info).filteredOn(line -> line.startsWith("DSE 5.1 compatibility mode: mutations of"))
                                .containsExactly(dse51ColumnsInfo(segments.get(0),
                                                                  DSE_51_KILLED_SEGMENT_PERSISTER_MUTATIONS));
                for (Path segment : segments)
                    assertMovedAside(segment);
            });
            assertFixtureDataSet();
            assertThat(latestWriteOfTheRowOf(3)).isGreaterThan(beforeTheRestart);
            assertDse51Peers();
            assertTheLocalRowIsThisServers();
        });
    }

    /**
     * In the other modes the replay of the segments of the killed DSE 5.1.51 node stops at the first write of DSE's
     * endpoint state persister, on the unknown column, as before: the JVM is killed (here a killer that only records
     * it) and the replay stops (here with the {@code stop_commit} policy), the segments kept. The copy of the mutation
     * the replay saves is deleted, and the mutations handed to the mutation stage before the stop are waited for.
     */
    @Test
    public void theOtherModesStopTheReplayOfTheSegmentsOfAKilledDse51NodeAsBefore() throws Throwable
    {
        prepareTheReplayOfDse51Segments();
        String savedPrefix = "Unexpected error deserializing mutation; saved to ";
        Config.CommitFailurePolicy policy = DatabaseDescriptor.getCommitFailurePolicy();
        DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop_commit);
        KillerForTests killer = new KillerForTests();
        JVMKiller previousKiller = JVMStabilityInspector.replaceKiller(killer);
        try
        {
            for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            {
                if (mode.isDse51())
                    continue;
                killer.reset();
                Dse51Fixtures dse51 = DseFixtures.dse51();
                List<Path> segments = dse51.unpackCommitLogSegments(dse51.killedCommitLogDirectory(),
                                                                    folder.newFolder().toPath());
                File[] files = segments.stream().map(File::new).toArray(File[]::new);
                // NONE with the default storage mode, as a node that dropped the flag
                StorageCompatibilityMode storage = mode == InternodeCompatibilityMode.NONE
                                                   ? StorageCompatibilityMode.NONE
                                                   : StorageCompatibilityMode.HCD_1;
                withModes(mode, storage, () -> {
                    ColumnFamilyStore.FlushReason startup = ColumnFamilyStore.FlushReason.STARTUP;
                    assertThatThrownBy(() -> CommitLog.instance.recoverSegmentsOnDiskNoArchive(startup, files))
                    .as("%s", mode)
                    .isInstanceOf(CommitLogReplayer.CommitLogReplayException.class)
                    .hasMessageStartingWith(savedPrefix)
                    .hasMessageEndingWith("Exception follows: java.lang.RuntimeException: " +
                                          FIRST_UNKNOWN_COLUMN_OF_THE_DSE_51_SEGMENTS)
                    .satisfies(e -> {
                        String saved = e.getMessage().substring(savedPrefix.length());
                        Files.delete(Path.of(saved.substring(0, saved.indexOf(".  "))));
                    });
                });
                awaitTheMutationStage();
                assertThat(killer.wasKilled()).as("%s", mode).isTrue();
                for (Path segment : segments)
                    assertKept(segment);
                assertTheLocalRowIsThisServers();
                execute("TRUNCATE system.peers");
                SystemKeyspace.removeTruncationRecord(SystemKeyspace.LegacyPeers.id);
            }
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(previousKiller);
            DatabaseDescriptor.setCommitFailurePolicy(policy);
            execute("TRUNCATE system.peers");
        }
    }

    /**
     * Replays the active segment of the DSE 5.1.51 fixtures in {@code DSE_5_1} through the start-up path, the
     * collection table created, and returns it: its fixture mutations replayed, its WARN of a DSE 5.1 segment and its
     * INFO line of the mutations naming the five logged.
     */
    private Path replayTheActiveDse51Segment() throws Throwable
    {
        prepareTheReplayOfDse51Segments();
        Path segment = Iterables.getOnlyElement(DseFixtures.dse51().unpackCommitLogSegments(
            DseFixtures.dse51().commitLogSegmentGz().getParent(), folder.newFolder().toPath()));
        withDse51SSTableFormat(() -> {
            withModes(InternodeCompatibilityMode.DSE_5_1, StorageCompatibilityMode.HCD_1, () -> {
                List<String> info = new ArrayList<>();
                List<String> warnings = new ArrayList<>();
                collecting(READER_LOGGER, Level.INFO, info, () -> {
                    warnings.addAll(replayAtStartupCollectingWarnings(segment, FIXTURE_MUTATIONS));
                    return Map.of();
                });
                assertThat(warnings).anyMatch(w -> w.startsWith("Commit log segment " + segment + " was written by " +
                                                                "DataStax Enterprise 5.1 and read at the start"));
                assertThat(info).contains(dse51ColumnsInfo(segment, DSE_51_ACTIVE_SEGMENT_PERSISTER_MUTATIONS));
            });
        });
        return segment;
    }

    /**
     * Creates the collection table with its id on the DSE 5.1.51 cluster, and removes the truncation record of
     * {@code system.peers}, which a TRUNCATE of an earlier test leaves at a position of this server's commit log, after
     * every position of the DSE segments: the replay would skip their mutations of that table. The replay of the DSE
     * segments also writes node 1's cells to {@code system.local}, older than this server's, which win; the cells of
     * DSE 5.1's local row that this server never writes ({@code thrift_version}, read as a dropped column, and the
     * five, replayed without their values) add nothing any test of the class reads.
     */
    private void prepareTheReplayOfDse51Segments() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS " + FIXTURE_KEYSPACE);
        for (String statement : DseFixtures.dse51().fixtureSchema())
            execute(statement);
        SystemKeyspace.removeTruncationRecord(SystemKeyspace.LegacyPeers.id);
    }

    /**
     * Waits until the mutation stage has applied every mutation the replay handed to it: until it reads idle twice in a
     * row, 10 ms apart.
     */
    private static void awaitTheMutationStage()
    {
        ExecutorPlus mutations = Stage.MUTATION.executor();
        long deadline = Clock.Global.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        // idle at two reads in a row, so that a task taken from the queue but not yet running is not missed
        int idleReads = 0;
        while (idleReads < 2)
        {
            boolean idle = mutations.getActiveTaskCount() == 0 && mutations.getPendingTaskCount() == 0;
            idleReads = idle ? idleReads + 1 : 0;
            assertThat(Clock.Global.nanoTime()).as("the mutation stage still busy").isLessThan(deadline);
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
    }

    /** The INFO line of the replay of {@code segment} in {@code DSE_5_1} for its {@code mutations} naming the five. */
    private static String dse51ColumnsInfo(Path segment, int mutations)
    {
        return "DSE 5.1 compatibility mode: mutations of system.local or system.peers naming the columns " +
               "[dse_version, graph, server_id, workload, workloads] DSE 5.1 adds to those tables, read in " + segment +
               ": " + mutations + "; those not yet in an sstable are replayed without the values of those columns, " +
               "which this software reads as dropped columns";
    }

    /**
     * Both peers of DSE 5.1.51's three-node cluster, nodes 2 and 3, are in the replayed {@code system.peers}, with
     * their data center, rack, host id and release, as DSE's persister wrote them; then {@code system.peers} is
     * truncated.
     */
    private void assertDse51Peers() throws Throwable
    {
        try
        {
            for (int node = 2; node <= Dse51Fixtures.CLUSTER_NODES; node++)
            {
                assertRows(execute("SELECT data_center, rack, host_id, release_version FROM system.peers " +
                                   "WHERE peer = ?", InetAddresses.forString("127.0.51.1" + node)),
                           row("Cassandra", "rack1", Dse51Fixtures.HOST_IDS.get(node - 1),
                               Dse51Fixtures.RELEASE_VERSION));
            }
        }
        finally
        {
            execute("TRUNCATE system.peers");
        }
    }

    /** The latest write time, in microseconds, of the cells of node {@code node}'s row of {@code system.peers}. */
    private long latestWriteOfTheRowOf(int node) throws Throwable
    {
        UntypedResultSet rows = execute("SELECT WRITETIME(data_center) AS dc, WRITETIME(rack) AS rack, " +
                                        "WRITETIME(host_id) AS host, WRITETIME(release_version) AS release, " +
                                        "WRITETIME(rpc_address) AS rpc, WRITETIME(schema_version) AS schema_time " +
                                        "FROM system.peers WHERE peer = ?",
                                        InetAddresses.forString("127.0.51.1" + node));
        assertThat(rows.size()).isEqualTo(1);
        long latest = Long.MIN_VALUE;
        for (String column : List.of("dc", "rack", "host", "release", "rpc", "schema_time"))
            if (rows.one().has(column))
                latest = Math.max(latest, rows.one().getLong(column));
        assertThat(latest).isPositive();
        return latest;
    }

    /** The row {@code local} of {@code system.local} keeps this server's host id and cluster name. */
    private void assertTheLocalRowIsThisServers() throws Throwable
    {
        assertRows(execute("SELECT host_id, cluster_name FROM system.local WHERE key = 'local'"),
                   row(SystemKeyspace.getLocalHostId(), DatabaseDescriptor.getClusterName()));
    }

    /** Runs {@code body} with the big format selected, as in {@code DSE_5_1}, which the reader of the five reads. */
    private static void withDse51SSTableFormat(Body body) throws Throwable
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(BigFormat.getInstance());
        try
        {
            body.run();
        }
        finally
        {
            DatabaseDescriptor.setSelectedSSTableFormat(format);
        }
    }

    /**
     * Without the mode, a generated segment of version 6 ({@code dse51Segment}) is replayed and, its mutations of an
     * unknown table skipped, kept. It is generated: the replay of the segments DSE 5.1.51 wrote stops without the mode
     * ({@link #theOtherModesStopTheReplayOfTheSegmentsOfAKilledDse51NodeAsBefore}).
     */
    @Test
    public void noneModeKeepsADse51SegmentWithUnknownTableMutations() throws Throwable
    {
        Path segment = dse51Segment();
        withModes(InternodeCompatibilityMode.NONE, StorageCompatibilityMode.NONE, () -> {
            List<String> warnings = replayAtStartupCollectingWarnings(segment, DSE_51_ROWS);
            assertThat(warnings).noneMatch(w -> w.contains("was written by DataStax Enterprise"));
        });
        assertKept(segment);
        assertRows(execute("SELECT count(*) FROM " + DSE_51_TABLE), row((long) DSE_51_ROWS));
    }

    /**
     * A segment of version 6 holding a mutation of each of {@link #DSE_51_ROWS} rows of {@link #DSE_51_TABLE}, created
     * here, and one of a table unknown here; the mutations are serialized at version 12, laid out as at 10 and 11.
     */
    private Path dse51Segment() throws Throwable
    {
        execute("CREATE KEYSPACE " + FIXTURE_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', " +
                "'replication_factor': 1}");
        execute("CREATE TABLE " + DSE_51_TABLE + " (id int PRIMARY KEY, v text)");
        TableMetadata metadata = Schema.instance.getTableMetadata(FIXTURE_KEYSPACE, "dse51");
        TableMetadata unknown = TableMetadata.builder(FIXTURE_KEYSPACE, "dse51_only")
                                             .id(TableId.generate())
                                             .addPartitionKeyColumn("id", Int32Type.instance)
                                             .addRegularColumn("v", UTF8Type.instance)
                                             .build();
        List<ByteBuffer> mutations = new ArrayList<>();
        long timestamp = FBUtilities.timestampMicros();
        for (int id = 0; id < DSE_51_ROWS; id++)
        {
            Mutation mutation = new RowUpdateBuilder(metadata, timestamp, id).add("v", "v" + id).build();
            mutations.add(DseFixtures.mutation40(mutation));
        }
        mutations.add(DseFixtures.mutation40(new RowUpdateBuilder(unknown, timestamp, 0).add("v", "x").build()));
        return segment(folder.newFolder().toPath(), CommitLogDescriptor.VERSION_30, mutations);
    }

    /**
     * In {@code DSE_5_1}, the writes of DSE 5.1's endpoint state persister to {@code system.peers} and
     * {@code system.local}, which name the columns DSE 5.1 adds to those tables, are replayed without the values of
     * those columns, the other cells of their mutations applied, with one INFO line for the segment; the segment is
     * then moved aside.
     */
    @Test
    public void dse51ModeReplaysThePersisterWritesWithoutTheColumnsDse51Adds() throws Throwable
    {
        Path segment = segment(folder.newFolder().toPath(), CommitLogDescriptor.VERSION_30, persisterMutations());
        try
        {
            // the format the mode selects, which the reader of the five columns reads
            withDse51SSTableFormat(() -> {
                withModes(InternodeCompatibilityMode.DSE_5_1, StorageCompatibilityMode.HCD_1, () -> {
                    List<String> info = new ArrayList<>();
                    Map<Keyspace, Integer> replayed = replayCollecting(segment, READER_LOGGER, Level.INFO, info, null);
                    assertThat(replayed.get(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME))).isEqualTo(3);
                    assertThat(info).contains("DSE 5.1 compatibility mode: mutations of system.local or system.peers " +
                                              "naming the columns [dse_version, graph, server_id, workload, " +
                                              "workloads] DSE 5.1 adds to those tables, read in " + segment + ": 3; " +
                                              "those not yet in an sstable are replayed without the values of those " +
                                              "columns, which this software reads as dropped columns");
                });
                assertMovedAside(segment);
                // the replayed rows hold no value of the five columns, as the sstable written from them shows
                ColumnFamilyStore peers = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                                  .getColumnFamilyStore(SystemKeyspace.LEGACY_PEERS);
                peers.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                TableMetadata withDse51Columns = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
                List<Row> rows = new ArrayList<>();
                for (SSTableReader sstable : peers.getLiveSSTables())
                {
                    for (PartitionUpdate update : Dse51SystemTables.partitions(sstable, withDse51Columns))
                        rows.add(update.getRow(Clustering.EMPTY));
                }
                assertThat(rows).isNotEmpty().doesNotContainNull();
                for (Row row : rows)
                {
                    for (ColumnMetadata column : withDse51Columns.regularColumns())
                    {
                        if (Dse51SystemTables.COLUMNS.containsKey(column.name.toString()))
                            assertThat(row.getColumnData(column)).as("%s", column).isNull();
                    }
                }
                assertRows(execute("SELECT data_center, rack, host_id, release_version FROM system.peers " +
                                   "WHERE peer = ?", DSE_51_PEER),
                           row("dc1", "r1", DSE_51_HOST_ID, "3.11.3.5151"));
                assertRows(execute("SELECT cluster_name FROM system.local WHERE key = 'local'"),
                           row(DatabaseDescriptor.getClusterName()));
            });
        }
        finally
        {
            execute("TRUNCATE system.peers");
        }
    }

    /**
     * In the other modes the replay of such a write fails on the unknown column, as before; here the replay errors
     * are ignored, as with {@code -Dcassandra.commitlog.ignorereplayerrors=true}, rather than stop the replay, and the
     * copy of the mutation the replay saves is deleted. The same for a mutation of {@code system.peers} naming a
     * column unknown here that DSE 5.1 does not add, in every mode.
     */
    @Test
    public void theOtherModesFailTheReplayOfThePersisterWritesAsBefore() throws Throwable
    {
        TableMetadata withUnknown = SystemKeyspace.LegacyPeers.unbuild()
                                                              .droppedColumns(Map.of())
                                                              .addRegularColumn("foo", UTF8Type.instance)
                                                              .build();
        Mutation unknownColumn = new RowUpdateBuilder(withUnknown, FBUtilities.timestampMicros(), DSE_51_PEER)
                                 .add("foo", "x")
                                 .build();
        ByteBuffer unknown = DseFixtures.mutation40(unknownColumn);
        String savedPrefix = "Unexpected error deserializing mutation; saved to ";
        CassandraRelevantProperties ignore = CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
        try (WithProperties ignoreErrors = new WithProperties().set(ignore, true))
        {
            for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            {
                List<String> expected = new ArrayList<>();
                List<ByteBuffer> mutations = new ArrayList<>();
                if (mode != InternodeCompatibilityMode.DSE_5_1)
                {
                    mutations.addAll(persisterMutations());
                    expected.add("Unknown column dse_version during deserialization of system.peers");
                    expected.add("Unknown column dse_version during deserialization of system.peers");
                    expected.add("Unknown column workloads during deserialization of system.local");
                }
                mutations.add(unknown);
                expected.add("Unknown column foo during deserialization of system.peers");
                Path segment = segment(folder.newFolder().toPath(), CommitLogDescriptor.VERSION_30, mutations);
                List<String> errors = new ArrayList<>();
                List<String> info = new ArrayList<>();
                withModes(mode, DatabaseDescriptor.getStorageCompatibilityMode(), () -> {
                    Map<Keyspace, Integer> replayed = collecting(READER_LOGGER, Level.INFO, info,
                                                                 () -> replayCollecting(segment, REPLAYER_LOGGER,
                                                                                        Level.ERROR, null, errors));
                    assertThat(replayed.get(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)))
                    .as("mutations replayed in %s", mode)
                    .isNull();
                });
                assertThat(info).as("INFO lines in %s", mode).noneMatch(line -> line.startsWith("DSE 5.1"));
                assertThat(errors).as("replay errors in %s", mode).hasSize(expected.size());
                for (int i = 0; i < expected.size(); i++)
                {
                    assertThat(errors.get(i)).startsWith(savedPrefix)
                                             .endsWith("Exception follows: java.lang.RuntimeException: " +
                                                       expected.get(i));
                    String saved = errors.get(i).substring(savedPrefix.length());
                    Files.delete(Path.of(saved.substring(0, saved.indexOf(".  "))));
                }
                assertRows(execute("SELECT peer FROM system.peers WHERE peer = ?", DSE_51_PEER));
            }
        }
    }

    /**
     * In {@code DSE_5_1}, without {@code -Dcassandra.commitlog.ignorereplayerrors=true}, a mutation of
     * {@code system.peers} naming a column unknown here that DSE 5.1 does not add stops the start: the JVM is killed
     * (here a killer that only records it) and the replay stops (here with the {@code stop_commit} policy, which does
     * not stop the transports of the test server). The copy of the mutation the replay saves is deleted.
     */
    @Test
    public void dse51ModeStopsTheReplayOnAnotherUnknownColumnOfSystemPeers() throws Throwable
    {
        TableMetadata withUnknown = SystemKeyspace.LegacyPeers.unbuild()
                                                              .droppedColumns(Map.of())
                                                              .addRegularColumn("foo", UTF8Type.instance)
                                                              .build();
        Mutation unknownColumn = new RowUpdateBuilder(withUnknown, FBUtilities.timestampMicros(), DSE_51_PEER)
                                 .add("foo", "x")
                                 .build();
        Path segment = segment(folder.newFolder().toPath(), CommitLogDescriptor.VERSION_30,
                               List.of(DseFixtures.mutation40(unknownColumn)));
        File[] segments = { new File(segment) };
        String savedPrefix = "Unexpected error deserializing mutation; saved to ";
        Config.CommitFailurePolicy policy = DatabaseDescriptor.getCommitFailurePolicy();
        DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop_commit);
        KillerForTests killer = new KillerForTests();
        JVMKiller previousKiller = JVMStabilityInspector.replaceKiller(killer);
        try
        {
            withModes(InternodeCompatibilityMode.DSE_5_1, StorageCompatibilityMode.HCD_1, () -> {
                ColumnFamilyStore.FlushReason startup = ColumnFamilyStore.FlushReason.STARTUP;
                assertThatThrownBy(() -> CommitLog.instance.recoverSegmentsOnDiskNoArchive(startup, segments))
                .isInstanceOf(CommitLogReplayer.CommitLogReplayException.class)
                .hasMessageStartingWith(savedPrefix)
                .hasMessageEndingWith("Exception follows: java.lang.RuntimeException: " +
                                      "Unknown column foo during deserialization of system.peers")
                .satisfies(e -> {
                    String saved = e.getMessage().substring(savedPrefix.length());
                    Files.delete(Path.of(saved.substring(0, saved.indexOf(".  "))));
                });
            });
            assertThat(killer.wasKilled()).isTrue();
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(previousKiller);
            DatabaseDescriptor.setCommitFailurePolicy(policy);
        }
        assertRows(execute("SELECT peer FROM system.peers WHERE peer = ?", DSE_51_PEER));
    }

    /**
     * Writes of DSE 5.1's endpoint state persister, serialized at version 12, laid out as at 10 and 11: a mutation
     * of the row of {@link #DSE_51_PEER} in {@code system.peers} naming the five columns DSE 5.1 adds and some of its
     * own (the persister writes one column at a time, other writers the others: this mutation holds both), one naming
     * only {@code dse_version}, as the persister writes it, and one of the row {@code local} of {@code system.local}
     * naming only {@code workloads}, at timestamp 1, below that of every write of this test server.
     */
    private static List<ByteBuffer> persisterMutations() throws IOException
    {
        TableMetadata peers = Dse51SystemTables.withDse51Columns(SystemKeyspace.LegacyPeers);
        TableMetadata local = Dse51SystemTables.withDse51Columns(SystemKeyspace.Local);
        long timestamp = FBUtilities.timestampMicros();
        return List.of(DseFixtures.mutation40(new RowUpdateBuilder(peers, timestamp, DSE_51_PEER)
                                              .add("data_center", "dc1")
                                              .add("rack", "r1")
                                              .add("host_id", DSE_51_HOST_ID)
                                              .add("release_version", "3.11.3.5151")
                                              .add("workload", "Cassandra")
                                              .add("workloads", Set.of("Cassandra"))
                                              .add("graph", false)
                                              .add("dse_version", "5.1.51")
                                              .add("server_id", "02-42-AC-11-00-02")
                                              .build()),
                       DseFixtures.mutation40(new RowUpdateBuilder(peers, timestamp + 1, DSE_51_PEER)
                                              .add("dse_version", "5.1.51")
                                              .build()),
                       DseFixtures.mutation40(new RowUpdateBuilder(local, 1, SystemKeyspace.LOCAL)
                                              .add("workloads", Set.of("Cassandra"))
                                              .build()));
    }

    /**
     * Replays {@code segment} through the start-up path, collecting the events of {@code logger} at {@code level}:
     * their messages into {@code messages} and the messages of their exceptions into {@code exceptions}, each when
     * not null.
     */
    private static Map<Keyspace, Integer> replayCollecting(Path segment, Logger logger, Level level,
                                                           List<String> messages, List<String> exceptions)
    throws IOException
    {
        return collecting(logger, level, messages, exceptions,
                          () -> CommitLog.instance.recoverSegmentsOnDiskNoArchive(ColumnFamilyStore.FlushReason.STARTUP,
                                                                                  new File[]{ new File(segment) }));
    }

    private interface Replay
    {
        Map<Keyspace, Integer> run() throws IOException;
    }

    /** Runs {@code replay}, collecting the messages of {@code logger} at {@code level} into {@code messages}. */
    private static Map<Keyspace, Integer> collecting(Logger logger, Level level, List<String> messages, Replay replay)
    throws IOException
    {
        return collecting(logger, level, messages, null, replay);
    }

    private static Map<Keyspace, Integer> collecting(Logger logger, Level level, List<String> messages,
                                                     List<String> exceptions, Replay replay) throws IOException
    {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            return replay.run();
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
            for (ILoggingEvent event : appender.list)
            {
                if (event.getLevel() != level)
                    continue;
                if (messages != null)
                    messages.add(event.getFormattedMessage());
                if (exceptions != null && event.getThrowableProxy() != null)
                    exceptions.add(event.getThrowableProxy().getMessage());
            }
        }
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

        assertFixtureDataSet();
    }

    /**
     * The data set of the fixtures README: 100 partitions of two rows, partition 1 deleted, cell v of (2, ck1) deleted,
     * the clustering range ck1..ck2 of partition 3 deleted (its static row survives): count(*) counts the live regular
     * rows plus one row for the partition left with only its static row.
     */
    private void assertFixtureDataSet() throws Throwable
    {
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
     * The schema the fixture segment was written with, created before its replay: in {@code DSE_6} mode the schema is
     * frozen ({@code DseUpgradeGuards}), so {@code cassandra.internode_compatibility_mode.allow_ddl} allows the schema
     * changes for the duration.
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
