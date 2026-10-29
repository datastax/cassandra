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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.PaxosRow;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.net.DseCompat.isUnknownStoredDseVersion;
import static org.apache.cassandra.net.DseCompat.storedDecodingVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The Paxos state DSE 6.8.65 left in {@code system.paxos} of a node of its test cluster
 * ({@link DseFixtures#paxosRows()}), and the {@code system.paxos} sstable DSE 6.9.26 left on a node of another test
 * cluster ({@link DseFixtures#dse6926PaxosSSTableDirectory()}), are read by this software, in both
 * {@code internode_compatibility_mode}s: the rows outlive the mode. Paxos v2 runs in {@code NONE} only: the DSE 6
 * peers of a node in {@code DSE_6} have Paxos v1 only, so such a node keeps {@code paxos_variant: v1}.
 */
public class DsePaxosRowsTest extends CQLTester
{
    /** The keyspace of the table of DSE 6.9.26's sstable of system.paxos, beside the 6.8.65 fixture's {@code lab.lwt}. */
    private static final String DSE_6_9_26_KEYSPACE = "dse_6926";
    /** When the DSE 6.9.26 sstable is read: 2026-10-03T12:00:00Z, after its last commit and before its first cell expires. */
    private static final long DSE_6_9_26_READ_AT = 1791028800L;
    /** The gc_grace_seconds of that table here: not DSE's TTL, so that the test tells which of the two expires a commit. */
    private static final int DSE_6_9_26_GC_GRACE_SECONDS = 432000;

    private InternodeCompatibilityMode modeBefore;
    private Config.PaxosVariant variantBefore;

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Before
    public void createDseTables() throws Throwable
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        variantBefore = DatabaseDescriptor.getPaxosVariant();
        for (String statement : DseFixtures.paxosSchema(1))
            schemaChange(statement.replaceFirst("^CREATE (KEYSPACE|TYPE|TABLE) ", "CREATE $1 IF NOT EXISTS "));
        execute("TRUNCATE lab.lwt");
        execute("TRUNCATE bbtest.t");
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS).truncateBlocking();
        // the Paxos state of recent keys is cached in memory, above system.paxos
        PaxosState.unsafeReset();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setPaxosVariant(variantBefore);
        PaxosState.unsafeReset();
    }

    @Test
    public void decodingVersions()
    {
        // every messaging version of this software, and whatever else lies below DSE's range, is decoded as it is
        for (MessagingService.Version version : MessagingService.Version.values())
        {
            assertFalse(isUnknownStoredDseVersion(version.value));
            assertEquals(version.value, storedDecodingVersion(version.value));
        }
        for (int version : new int[]{ Integer.MIN_VALUE, -1, 0, 255 })
        {
            assertFalse(isUnknownStoredDseVersion(version));
            assertEquals(version, storedDecodingVersion(version));
        }
        // DSE 6.0 and 6.0.3 (WRITES at OSS_30), then 6.8.0 to 6.9.7 and later (WRITES at DSE_68)
        assertEquals(MessagingService.VERSION_40, storedDecodingVersion(256));
        assertEquals(MessagingService.VERSION_40, storedDecodingVersion(512));
        for (int version : new int[]{ 768, 1024, 1280, 1536 })
            assertEquals(MessagingService.VERSION_DSE_68, storedDecodingVersion(version));
        for (int version : new int[]{ 256, 512, 768, 1024, 1280, 1536 })
            assertFalse(isUnknownStoredDseVersion(version));
        assertEquals(DseFixtures.PAXOS_DSE_6_8_65_VERSION, DseCompat.DSE_681_VERSION);
        assertEquals(DseFixtures.PAXOS_DSE_6_9_26_VERSION, DseCompat.DSE_697_VERSION);
        for (int version : new int[]{ 257, 1025, 1792, DseFixtures.PAXOS_UNKNOWN_DSE_VERSION, Integer.MAX_VALUE })
        {
            assertTrue(isUnknownStoredDseVersion(version));
            try
            {
                storedDecodingVersion(version);
                fail("decoded messaging version " + version);
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Not a DSE 6 messaging version: " + version, e.getMessage());
            }
        }
    }

    /**
     * The empty-update check of the uncommitted tracker reads the flags past DSE's maximum timestamp; it depends on the
     * stored version only, not on the mode.
     */
    @Test
    public void isEmptyAtDse68() throws Throwable
    {
        PaxosRow emptyRound = firstRow(r -> r.proposalBallot != null);
        PaxosRow lwt = firstRow(r -> r.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID));
        DecoratedKey emptyKey = decorate(emptyRound);
        DecoratedKey lwtKey = decorate(lwt);
        DeserializationHelper.Flag local = DeserializationHelper.Flag.LOCAL;
        int dse68 = MessagingService.VERSION_DSE_68;

        assertTrue(PartitionUpdate.PartitionUpdateSerializer.isEmpty(emptyRound.proposal.duplicate(), local, emptyKey, dse68));
        assertFalse(PartitionUpdate.PartitionUpdateSerializer.isEmpty(lwt.mostRecentCommit.duplicate(), local, lwtKey, dse68));
        // the second node's form of the same update: the commit time as maximum timestamp instead of Long.MIN_VALUE
        ByteBuffer withTimestamp = ByteBuffer.allocate(emptyRound.proposal.remaining()).put(emptyRound.proposal.duplicate());
        withTimestamp.putLong(16, Ballot.fromUuid(emptyRound.mostRecentCommitAt).unixMicros()).flip();
        assertTrue(PartitionUpdate.PartitionUpdateSerializer.isEmpty(withTimestamp, local, emptyKey, dse68));
        assertTrue(PartitionUpdate.fromBytes(withTimestamp.duplicate(), dse68).isEmpty());
        // read as this software's own layout, the empty DSE update looks non-empty: the failure observed with DSE 6.8.65
        // before the fix
        assertFalse(PartitionUpdate.PartitionUpdateSerializer.isEmpty(emptyRound.proposal.duplicate(), local, emptyKey, MessagingService.current_version));
        // the table id and the maximum timestamp only
        ByteBuffer truncated = lwt.mostRecentCommit.duplicate().limit(24);
        try
        {
            PartitionUpdate.PartitionUpdateSerializer.isEmpty(truncated, local, lwtKey, dse68);
            fail("read the flags of a 24-byte update");
        }
        catch (EOFException e)
        {
            // expected: nothing past the maximum timestamp
        }
    }

    /**
     * Every ballot a DSE coordinator wrote ends in 0 (tenths of a microsecond), which this software reads as no serial
     * consistency, so that Paxos repair skips the rounds DSE coordinated; the ones ending in 2, SERIAL, are the rounds the
     * third node of the test cluster coordinated when it already ran this software.
     */
    @Test
    public void ballotFlagsOfTheFixture() throws Throwable
    {
        int fromUpgraded = 0, fromDse = 0;
        for (PaxosRow row : DseFixtures.paxosRows())
        {
            boolean upgradedCoordinator = row.cfId.equals(DseFixtures.PAXOS_COLLECTION_TABLE_ID)
                                          && !isEmptyUpdate(row.mostRecentCommit, row.key())
                                          && row.casBatchValue().equals("cas-dse-node-3.lab");
            assertEquals(upgradedCoordinator ? Ballot.Flag.GLOBAL : Ballot.Flag.NONE, Ballot.fromUuid(row.mostRecentCommitAt).flag());
            if (row.proposalBallot != null)
                // the empty proposals the serial reads of the third node left after a DSE commit
                assertEquals(Ballot.Flag.GLOBAL, Ballot.fromUuid(row.proposalBallot).flag());
            if (upgradedCoordinator)
                fromUpgraded++;
            else
                fromDse++;
        }
        assertEquals(3, fromUpgraded);
        assertEquals(115, fromDse);
    }

    @Test
    public void loadsDse68PaxosStateInNone() throws Throwable
    {
        loadsDsePaxosState(InternodeCompatibilityMode.NONE, DseCompat.DSE_681_VERSION, false);
    }

    @Test
    public void loadsDse68PaxosStateInDse6() throws Throwable
    {
        loadsDsePaxosState(InternodeCompatibilityMode.DSE_6, DseCompat.DSE_681_VERSION, false);
    }

    /**
     * DSE 6.9.7 and later store 1536; their WRITES version is DSE_68 as for 6.8 (DSE's version matrix), so the
     * 6.8.65 bytes stand for theirs: this checks the mapping on the 6.8.65 rows, {@link #loadsDse6926PaxosSSTable} on rows DSE
     * 6.9.26 wrote.
     */
    @Test
    public void loadsDse697TaggedPaxosState() throws Throwable
    {
        loadsDsePaxosState(InternodeCompatibilityMode.NONE, DseCompat.DSE_697_VERSION, false);
    }

    /** DSE 6.0 stores 256 and writes the update at OSS_30: the same bytes without the maximum timestamp. */
    @Test
    public void loadsDse60TaggedPaxosState() throws Throwable
    {
        loadsDsePaxosState(InternodeCompatibilityMode.NONE, DseCompat.DSE_60_VERSION, true);
    }

    @Test
    public void loadsDse6926PaxosSSTableInNone() throws Throwable
    {
        loadsDse6926PaxosSSTable(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void loadsDse6926PaxosSSTableInDse6() throws Throwable
    {
        loadsDse6926PaxosSSTable(InternodeCompatibilityMode.DSE_6);
    }

    /**
     * The {@code system.paxos} sstable DSE 6.9.26 left on a node of its test cluster ({@link DseFixtures#dse6926PaxosSSTableDirectory()}),
     * imported as DSE left it: every row is tagged 1536, the messaging version of DSE 6.9.7 and later, and its commit
     * decodes with DSE's WRITES layout DSE_68, as {@link #loadsDse697TaggedPaxosState} assumes from the 6.8.65 bytes. The
     * rows are committed: DSE nulls the proposal at commit and leaves {@code proposal_version}. The whole table is read at
     * {@link #DSE_6_9_26_READ_AT}; then each row an hour after its commit and once DSE's TTL has passed. The table's
     * {@code gc_grace_seconds} is set to {@link #DSE_6_9_26_GC_GRACE_SECONDS}, apart from DSE's TTL, to tell the TTL of
     * DSE's cells from the expiry this software derives for a commit.
     */
    private void loadsDse6926PaxosSSTable(InternodeCompatibilityMode mode) throws Throwable
    {
        // the schema and the import first, then the mode, as on a node upgraded in place
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + DSE_6_9_26_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TABLE IF NOT EXISTS " + DSE_6_9_26_KEYSPACE + ".lwt (id int PRIMARY KEY, v int) WITH ID = " +
                     DseFixtures.PAXOS_DSE_6_9_26_LWT_TABLE_ID + " AND gc_grace_seconds = " + DSE_6_9_26_GC_GRACE_SECONDS);
        TableMetadata metadata = Schema.instance.getTableMetadata(TableId.fromUUID(DseFixtures.PAXOS_DSE_6_9_26_LWT_TABLE_ID));
        assertEquals(DSE_6_9_26_GC_GRACE_SECONDS, metadata.params.gcGraceSeconds);
        // the expiry asserted below is the one of these modes (the test configuration leaves the default, legacy)
        assertTrue(EnumSet.of(Config.PaxosStatePurging.legacy, Config.PaxosStatePurging.gc_grace).contains(DatabaseDescriptor.paxosStatePurging()));
        UUID cfId = DseFixtures.PAXOS_DSE_6_9_26_LWT_TABLE_ID;
        ColumnFamilyStore paxos = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS);
        // the cells expire in October 2026: no compaction may purge them while the test reads them at an earlier time
        boolean autoCompactionWasDisabled = paxos.isAutoCompactionDisabled();
        paxos.disableAutoCompaction();
        try
        {
            importDse6926PaxosSSTable(paxos);
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);

            List<Integer> ids = new ArrayList<>();
            for (UntypedResultSet.Row row : QueryProcessor.executeInternalWithNowInSec("SELECT * FROM system.paxos", DSE_6_9_26_READ_AT))
            {
                ByteBuffer rowKey = row.getBytes("row_key");
                int id = ByteBufferUtil.toInt(rowKey);
                ids.add(id);
                assertEquals(cfId, row.getUUID("cf_id"));
                assertEquals(DseFixtures.PAXOS_DSE_6_9_26_VERSION, row.getInt("most_recent_commit_version"));
                assertEquals(DseFixtures.PAXOS_DSE_6_9_26_VERSION, row.getInt("proposal_version"));
                assertFalse(row.has("proposal"));
                assertFalse(row.has("proposal_ballot"));
                assertFalse(row.has("in_progress_read_ballot"));
                Ballot committedAt = Ballot.fromUuid(row.getUUID("most_recent_commit_at"));
                assertEquals(committedAt, Ballot.fromUuid(row.getUUID("in_progress_ballot")));
                // a DSE coordinator leaves the serial consistency digit of its ballots at 0 (ballotFlagsOfTheFixture)
                assertEquals(Ballot.Flag.NONE, committedAt.flag());

                DecoratedKey key = metadata.partitioner.decorateKey(rowKey);
                long committedAtInSec = TimeUnit.MICROSECONDS.toSeconds(committedAt.unixMicros());
                // DSE wrote every cell at the time of the ballot, with its TTL
                Row paxosRow = paxosRow(key, cfId, committedAtInSec + 3600);
                int cells = 0;
                for (Cell<?> cell : paxosRow.cells())
                {
                    assertEquals(committedAt.unixMicros(), cell.timestamp());
                    assertTrue(cell.isExpiring());
                    assertEquals(DseFixtures.PAXOS_DSE_TTL_SECONDS, cell.ttl());
                    // DSE counts the TTL from its clock when it writes, the ballot's second or the next one
                    long expiresAt = cell.localDeletionTime() - DseFixtures.PAXOS_DSE_TTL_SECONDS;
                    assertTrue(cell.toString(), expiresAt == committedAtInSec || expiresAt == committedAtInSec + 1);
                    cells++;
                }
                assertEquals(5, cells);

                PaxosState.Snapshot state = SystemKeyspace.loadPaxosState(key, metadata, committedAtInSec + 3600);
                assertTrue(state.committed instanceof Commit.CommittedWithTTL);
                assertEquals(committedAt, state.committed.ballot);
                // this software derives the expiry of a commit from the table's gc_grace_seconds, not from the cell's TTL
                assertEquals(committedAtInSec + DSE_6_9_26_GC_GRACE_SECONDS, ((Commit.CommittedWithTTL) state.committed).localDeletionTime);
                assertEquals(key, state.committed.update.partitionKey());
                assertEquals(metadata.id, state.committed.update.metadata().id);
                assertNull(state.accepted);
                assertEquals(Collections.singletonList(dse6926CommittedValue(id)), committedValues(state.committed.update));

                // what the uncommitted tracker and Paxos repair see: nothing in progress (a commit after the proposal: the
                // tracker decodes no update here, so this checks the ballots and not the 1536 mapping)
                PaxosKeyState keyState = PaxosRows.getCommitState(key, paxosRow, null);
                assertNotNull(keyState);
                assertTrue(keyState.committed);
                assertEquals(committedAt, keyState.ballot);

                // after the table's gc_grace_seconds and before DSE's TTL the commit is still returned: loadPaxosState compares
                // the raw 100-ns timestamp of the ballot with a purge bound in microseconds, so it never purges here
                // (upstream since CASSANDRA-19617; a fix of it makes this assertion fail, on purpose)
                state = SystemKeyspace.loadPaxosState(key, metadata, committedAtInSec + DSE_6_9_26_GC_GRACE_SECONDS + 1);
                assertEquals(committedAt, state.committed.ballot);

                // two seconds after DSE's TTL the cells are gone, and so is the state
                long expired = committedAtInSec + DseFixtures.PAXOS_DSE_TTL_SECONDS + 2;
                assertTrue(QueryProcessor.executeInternalWithNowInSec("SELECT * FROM system.paxos WHERE row_key = ? AND cf_id = ?",
                                                                      expired, rowKey, cfId).isEmpty());
                state = SystemKeyspace.loadPaxosState(key, metadata, expired);
                assertEquals(Ballot.none(), state.committed.ballot);
                assertNull(state.accepted);
            }
            // the 100 rows of the test cluster's data set and the three keys of the capture
            List<Integer> expected = new ArrayList<>();
            for (int id = 0; id < DseFixtures.PAXOS_DSE_6_9_26_ROWS - 3; id++)
                expected.add(id);
            expected.addAll(Arrays.asList(9001, 9002, 9003));
            Collections.sort(ids);
            assertEquals(expected, ids);
        }
        finally
        {
            // the imported sstable must not outlive the test (its cells are expired after October 2026)
            paxos.truncateBlocking();
            if (!autoCompactionWasDisabled)
                paxos.enableAutoCompaction();
        }
    }

    /** Copies the DSE 6.9.26 sstable of system.paxos and imports the copy into {@code paxos}, as an operator imports sstables. */
    private static void importDse6926PaxosSSTable(ColumnFamilyStore paxos) throws IOException
    {
        Path root = Files.createTempDirectory("dse-6.9.26-paxos");
        try
        {
            Path directory = root.resolve(SchemaConstants.SYSTEM_KEYSPACE_NAME).resolve(SystemKeyspace.PAXOS + '-' + paxos.metadata.id.toHexString());
            Files.createDirectories(directory);
            for (Path component : DseFixtures.dse6926PaxosSSTableComponents())
                Files.copy(component, directory.resolve(component.getFileName()));
            boolean resetLevel = false, clearRepaired = false, verifySSTables = false, verifyTokens = false, invalidateCaches = false,
                    extendedVerify = false, copyData = false, failOnMissingIndex = false, validateIndexChecksum = false;
            List<String> failed = paxos.importNewSSTables(Collections.singleton(directory.toString()), resetLevel, clearRepaired, verifySSTables,
                                                          verifyTokens, invalidateCaches, extendedVerify, copyData, failOnMissingIndex,
                                                          validateIndexChecksum);
            assertEquals(Collections.emptyList(), failed);
            assertEquals(1, paxos.getLiveSSTables().size());
        }
        finally
        {
            FileUtils.deleteRecursive(new File(root));
        }
    }

    /**
     * The row the commit of a key of the DSE 6.9.26 sstable wrote, as {@code =<v>}: the data set of the test cluster ran
     * {@code UPDATE lab.lwt SET v = 2 WHERE id = <id> IF v = 1} on ids 0 to 99, the capture
     * {@code INSERT … VALUES (9001, 1)}, {@code (9002, 2)} and {@code (9003, 3)} … {@code IF NOT EXISTS}.
     */
    private static String dse6926CommittedValue(int id)
    {
        return "=" + (id < 100 ? 2 : id - 9000);
    }

    private void loadsDsePaxosState(InternodeCompatibilityMode mode, int storedVersion, boolean withoutMaxTimestamp) throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        List<PaxosRow> rows = DseFixtures.paxosRows();
        assertEquals(118, rows.size());
        int lwtRows = 0, committedValues = 0, emptyProposals = 0;
        for (PaxosRow fixture : rows)
        {
            PaxosRow row = withoutMaxTimestamp ? fixture.withoutMaxTimestamp() : fixture;
            savePaxosRow(row, storedVersion);
            TableMetadata metadata = Schema.instance.getTableMetadata(TableId.fromUUID(row.cfId));
            DecoratedKey key = decorate(row);
            // the state as a node read it on the day the fixture was taken, an hour after the last commit
            long nowInSec = TimeUnit.MICROSECONDS.toSeconds(Ballot.fromUuid(row.mostRecentCommitAt).unixMicros()) + 3600;
            PaxosState.Snapshot state = SystemKeyspace.loadPaxosState(key, metadata, nowInSec);

            assertTrue(state.committed instanceof Commit.CommittedWithTTL);
            assertEquals(Ballot.fromUuid(row.mostRecentCommitAt), state.committed.ballot);
            assertEquals(key, state.committed.update.partitionKey());
            assertEquals(metadata.id, state.committed.update.metadata().id);
            boolean emptyCommit = isEmptyUpdate(fixture.mostRecentCommit, row.key());
            assertEquals(emptyCommit, state.committed.update.isEmpty());
            if (!emptyCommit)
            {
                assertEquals(values(fixture), committedValues(state.committed.update));
                committedValues++;
            }

            if (row.proposalBallot != null)
            {
                // the rounds that changed nothing: an empty proposal accepted after an empty commit
                assertNotNull(state.accepted);
                assertTrue(state.accepted instanceof Commit.AcceptedWithTTL);
                assertEquals(Ballot.fromUuid(row.proposalBallot), state.accepted.ballot);
                assertTrue(state.accepted.update.isEmpty());
                emptyProposals++;
            }
            else
            {
                assertNull(state.accepted);
            }

            // what the uncommitted tracker and Paxos repair see: nothing in progress
            PaxosKeyState keyState = PaxosRows.getCommitState(key, paxosRow(key, row.cfId), null);
            assertNotNull(keyState);
            assertTrue(ByteBufferUtil.bytesToHex(row.rowKey) + " is not committed", keyState.committed);
            assertEquals(Ballot.fromUuid(row.proposalBallot != null ? row.proposalBallot : row.mostRecentCommitAt), keyState.ballot);
            lwtRows += row.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID) ? 1 : 0;
        }
        assertEquals(100, lwtRows);
        assertEquals(109, committedValues);
        assertEquals(9, emptyProposals);
    }

    @Test
    public void unknownDseVersionIsRefusedInNone() throws Throwable
    {
        unknownDseVersionIsRefused(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void unknownDseVersionIsRefusedInDse6() throws Throwable
    {
        unknownDseVersionIsRefused(InternodeCompatibilityMode.DSE_6);
    }

    private void unknownDseVersionIsRefused(InternodeCompatibilityMode mode) throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        PaxosRow lwt = firstRecentRow(r -> r.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID));
        PaxosRow emptyRound = firstRecentRow(r -> r.proposalBallot != null);
        for (PaxosRow row : new PaxosRow[]{ lwt, emptyRound })
        {
            savePaxosRow(row, DseFixtures.PAXOS_UNKNOWN_DSE_VERSION);
            TableMetadata metadata = Schema.instance.getTableMetadata(TableId.fromUUID(row.cfId));
            try
            {
                SystemKeyspace.loadPaxosState(decorate(row), metadata, FBUtilities.nowInSeconds());
                fail("decoded a Paxos row of messaging version " + DseFixtures.PAXOS_UNKNOWN_DSE_VERSION);
            }
            catch (IllegalStateException e)
            {
                // the proposal is decoded first, when there is one
                String column = row.proposalBallot == null ? "most_recent_commit" : "proposal";
                assertEquals("Cannot decode system.paxos." + column + " of table " + row.cfId + ", row_key 0x" +
                             ByteBufferUtil.bytesToHex(row.rowKey) + ": it was stored with messaging version " +
                             DseFixtures.PAXOS_UNKNOWN_DSE_VERSION + ", which is not a " +
                             "DSE 6 version (256, 512, 768, 1024, 1280 or 1536). The lightweight transactions of that partition, " +
                             "and its Paxos repair for a round with a serial consistency, fail until the row expires, with the TTL " +
                             "DSE gave it from the table's gc_grace_seconds, or a superuser deletes it from system.paxos on each replica",
                             e.getMessage());
            }
        }
        // the form without the partition key, for the projects built on it, refuses the proposal the same way
        DecoratedKey emptyRoundKey = decorate(emptyRound);
        try
        {
            PaxosRows.getAccepted(paxosRow(emptyRoundKey, emptyRound.cfId), 0, 0);
            fail("decoded a Paxos proposal of messaging version " + DseFixtures.PAXOS_UNKNOWN_DSE_VERSION);
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage(), e.getMessage().startsWith("Cannot decode system.paxos.proposal of table " + emptyRound.cfId +
                                                                 ", row_key (not known here): it was stored with messaging version " +
                                                                 DseFixtures.PAXOS_UNKNOWN_DSE_VERSION + ", "));
        }
        // a proposal of an unknown version counts as in progress, so that it is repaired, and refused there
        DecoratedKey key = decorate(emptyRound);
        PaxosKeyState keyState = PaxosRows.getCommitState(key, paxosRow(key, emptyRound.cfId), null);
        assertNotNull(keyState);
        assertFalse(keyState.committed);
        assertEquals(Ballot.fromUuid(emptyRound.proposalBallot), keyState.ballot);

        // the remedy of the error text, on rows already flushed as DSE left them: an operator deletes each row above the
        // timestamps of its cells; the deleted row has no state to track (the flush of system.paxos that used to fail on it
        // is covered by DsePaxosStateTest: this test does not start the uncommitted tracker), and lightweight transactions
        // work again
        ColumnFamilyStore paxos = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS);
        paxos.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        for (PaxosRow row : new PaxosRow[]{ lwt, emptyRound })
        {
            DseFixtures.Statement deletion = DseFixtures.paxosRowDeletion(row, writetimes(row));
            executeNet(deletion.cql, deletion.values);
        }
        paxos.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertNull(PaxosRows.getCommitState(key, paxosRow(key, emptyRound.cfId), null));
        PaxosState.unsafeReset();
        execute("INSERT INTO lab.lwt (id, v) VALUES (?, 2)", lwt.key());
        assertTrue(executeNet("UPDATE lab.lwt SET v = 3 WHERE id = ? IF v = 2", lwt.key()).wasApplied());
        assertEquals(3, serial("SELECT v FROM lab.lwt WHERE id = " + lwt.key()).one().getInt("v"));
        assertTrue(serial("SELECT * FROM bbtest.t WHERE id = " + emptyRound.key()).isExhausted());
    }

    @Test
    public void lwtOnDsePaxosStatePaxosV1InNone() throws Throwable
    {
        lwtOnDsePaxosState(InternodeCompatibilityMode.NONE, Config.PaxosVariant.v1);
    }

    @Test
    public void lwtOnDsePaxosStatePaxosV1InDse6() throws Throwable
    {
        lwtOnDsePaxosState(InternodeCompatibilityMode.DSE_6, Config.PaxosVariant.v1);
    }

    @Test
    public void lwtOnDsePaxosStatePaxosV2InNone() throws Throwable
    {
        lwtOnDsePaxosState(InternodeCompatibilityMode.NONE, Config.PaxosVariant.v2);
    }

    private void lwtOnDsePaxosState(InternodeCompatibilityMode mode, Config.PaxosVariant variant) throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        DatabaseDescriptor.setPaxosVariant(variant);
        PaxosRow lwt = firstRecentRow(r -> r.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID));
        PaxosRow emptyRound = firstRecentRow(r -> r.proposalBallot != null);
        savePaxosRow(lwt, DseCompat.DSE_681_VERSION);
        savePaxosRow(emptyRound, DseCompat.DSE_681_VERSION);
        execute("INSERT INTO lab.lwt (id, v) VALUES (?, 2)", lwt.key());

        assertEquals(2, serial("SELECT v FROM lab.lwt WHERE id = " + lwt.key()).one().getInt("v"));
        assertTrue(executeNet("UPDATE lab.lwt SET v = 3 WHERE id = ? IF v = 2", lwt.key()).wasApplied());
        assertEquals(3, serial("SELECT v FROM lab.lwt WHERE id = " + lwt.key()).one().getInt("v"));
        assertTrue(serial("SELECT * FROM bbtest.t WHERE id = " + emptyRound.key()).isExhausted());
        assertTrue(executeNet("INSERT INTO bbtest.t (id, ck, v) VALUES (?, 'u1', 'x') IF NOT EXISTS", emptyRound.key()).wasApplied());

        // what this software wrote back carries its own version, never DSE's: the commit, and the empty proposal a serial
        // read leaves after it (CASSANDRA-12126) where there is one
        int ownVersion = StorageCompatibilityMode.current().storageMessagingVersion();
        for (PaxosRow row : new PaxosRow[]{ lwt, emptyRound })
        {
            UntypedResultSet.Row stored = execute("SELECT proposal, proposal_version, most_recent_commit_version FROM system.paxos " +
                                                  "WHERE row_key = ? AND cf_id = ?", row.rowKey, row.cfId).one();
            assertEquals(ownVersion, stored.getInt("most_recent_commit_version"));
            assertEquals(stored.has("proposal"), stored.has("proposal_version"));
            if (stored.has("proposal_version"))
                assertEquals(ownVersion, stored.getInt("proposal_version"));
        }
    }

    @Test
    public void inProgressDseProposalPaxosV1InNone() throws Throwable
    {
        inProgressDseProposal(InternodeCompatibilityMode.NONE, Config.PaxosVariant.v1);
    }

    @Test
    public void inProgressDseProposalPaxosV1InDse6() throws Throwable
    {
        inProgressDseProposal(InternodeCompatibilityMode.DSE_6, Config.PaxosVariant.v1);
    }

    @Test
    public void inProgressDseProposalPaxosV2InNone() throws Throwable
    {
        inProgressDseProposal(InternodeCompatibilityMode.NONE, Config.PaxosVariant.v2);
    }

    /**
     * A DSE proposal accepted but never committed (DSE nulls the proposal at commit, so the fixture has none; this one is
     * a committed update of the fixture saved as a proposal): it is in progress for the uncommitted tracker, and a serial
     * read completes the round with DSE's update.
     */
    private void inProgressDseProposal(InternodeCompatibilityMode mode, Config.PaxosVariant variant) throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        DatabaseDescriptor.setPaxosVariant(variant);
        PaxosRow committed = firstRecentRow(r -> r.cfId.equals(DseFixtures.PAXOS_COLLECTION_TABLE_ID) && !isEmptyUpdate(r.mostRecentCommit, r.key()));
        DseFixtures.Statement proposal = committed.saveCommitAsAcceptedProposal(DseCompat.DSE_681_VERSION);
        QueryProcessor.executeInternal(proposal.cql, proposal.values);
        DecoratedKey key = decorate(committed);

        PaxosKeyState keyState = PaxosRows.getCommitState(key, paxosRow(key, committed.cfId), null);
        assertNotNull(keyState);
        assertFalse(keyState.committed);
        assertEquals(Ballot.fromUuid(committed.mostRecentCommitAt), keyState.ballot);
        // what Paxos repair is given to complete
        List<DecoratedKey> uncommitted = new ArrayList<>();
        try (CloseableIterator<UncommittedPaxosKey> keys = PaxosState.uncommittedTracker().uncommittedKeyIterator(TableId.fromUUID(committed.cfId), null))
        {
            keys.forEachRemaining(k -> uncommitted.add(k.getKey()));
        }
        assertEquals(Collections.singletonList(key), uncommitted);
        PaxosState.Snapshot state = SystemKeyspace.loadPaxosState(key, Schema.instance.getTableMetadata(TableId.fromUUID(committed.cfId)),
                                                                  FBUtilities.nowInSeconds());
        assertNotNull(state.accepted);
        assertEquals(values(committed), committedValues(state.accepted.update));

        List<String> read = new ArrayList<>();
        for (com.datastax.driver.core.Row row : serial("SELECT ck, v FROM bbtest.t WHERE id = " + committed.key()))
            read.add(row.getString("ck") + '=' + row.getString("v"));
        assertEquals(values(committed), read);
        assertEquals(values(committed).size(), execute("SELECT * FROM bbtest.t WHERE id = ?", committed.key()).size());
    }

    /** The write timestamps of the cells of a row of system.paxos, as the remedy reads them first. */
    private long[] writetimes(PaxosRow row) throws Throwable
    {
        UntypedResultSet.Row stored = execute("SELECT writetime(in_progress_ballot) AS a, writetime(in_progress_read_ballot) AS b, " +
                                              "writetime(proposal_ballot) AS c, writetime(proposal_version) AS d, " +
                                              "writetime(most_recent_commit_at) AS e " +
                                              "FROM system.paxos WHERE row_key = ? AND cf_id = ?", row.rowKey, row.cfId).one();
        long[] writetimes = new long[5];
        int i = 0;
        for (String column : new String[]{ "a", "b", "c", "d", "e" })
            writetimes[i++] = stored.has(column) ? stored.getLong(column) : Long.MIN_VALUE;
        return writetimes;
    }

    private ResultSet serial(String query)
    {
        return executeNet(new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.SERIAL));
    }

    private static PaxosRow firstRow(Predicate<PaxosRow> filter) throws Exception
    {
        return DseFixtures.paxosRows().stream().filter(filter).findFirst().orElseThrow(AssertionError::new);
    }

    /** The same row with its ballots an hour old, for the Paxos of this software, which purges older state. */
    private static PaxosRow firstRecentRow(Predicate<PaxosRow> filter) throws Exception
    {
        return DseFixtures.paxosRowsOfAnHourAgo().stream().filter(filter).findFirst().orElseThrow(AssertionError::new);
    }

    private static DecoratedKey decorate(PaxosRow row)
    {
        return Schema.instance.getTableMetadata(TableId.fromUUID(row.cfId)).partitioner.decorateKey(row.rowKey);
    }

    /**
     * The rows a fixture commit wrote, as {@code <ck>=<v>}: {@code =2} for lab.lwt's {@code UPDATE … SET v = 2} (no
     * clustering), the rows u1 and u3 of a conditional batch of bbtest.t with {@code v = 'cas-<coordinator>'}.
     */
    private static List<String> values(PaxosRow row)
    {
        List<String> values = new ArrayList<>();
        if (row.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID))
        {
            values.add("=2");
        }
        else
        {
            values.add("u1=" + row.casBatchValue());
            values.add("u3=" + row.casBatchValue());
        }
        return values;
    }

    private static List<String> committedValues(PartitionUpdate update) throws Exception
    {
        List<String> values = new ArrayList<>();
        TableMetadata metadata = update.metadata();
        // the lwt table of either fixture (lab.lwt, dse_6926.lwt) has no clustering; bbtest.t, the collection table, has ck
        boolean lwt = metadata.clusteringColumns().isEmpty();
        try (UnfilteredRowIterator partition = update.unfilteredIterator())
        {
            while (partition.hasNext())
            {
                Row row = (Row) partition.next();
                ByteBuffer v = row.getCell(metadata.getColumn(ByteBufferUtil.bytes("v"))).buffer();
                values.add(lwt ? "=" + ByteBufferUtil.toInt(v)
                               : ByteBufferUtil.string(row.clustering().bufferAt(0)) + '=' + ByteBufferUtil.string(v));
            }
        }
        return values;
    }

    /** Writes a fixture row as DSE wrote it, see {@link PaxosRow#saveStatements(int)}. */
    private static void savePaxosRow(PaxosRow row, int storedVersion)
    {
        for (DseFixtures.Statement statement : row.saveStatements(storedVersion))
            QueryProcessor.executeInternal(statement.cql, statement.values);
    }

    private static Row paxosRow(DecoratedKey key, UUID cfId)
    {
        return paxosRow(key, cfId, FBUtilities.nowInSeconds());
    }

    /** The row of system.paxos of a key, as read at {@code nowInSec}. */
    private static Row paxosRow(DecoratedKey key, UUID cfId, long nowInSec)
    {
        TableMetadata paxos = Schema.instance.getTableMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS);
        DecoratedKey paxosKey = paxos.partitioner.decorateKey(key.getKey());
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(paxos, nowInSec, paxosKey,
                                                                               new BufferClustering(UUIDType.instance.decompose(cfId)));
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller);
             UnfilteredRowIterator partition = Iterators.getOnlyElement(partitions))
        {
            return (Row) Iterators.getOnlyElement(partition);
        }
    }

    /** Whether DSE's serialization of an update (table id, maximum timestamp, key, flags) has the IS_EMPTY flag. */
    private static boolean isEmptyUpdate(ByteBuffer dseUpdate, int key)
    {
        ByteBuffer b = dseUpdate.duplicate();
        b.position(b.position() + 16 + 8);
        assertEquals(4, b.get());
        assertEquals(key, b.getInt());
        return (b.get() & 0x01) != 0;
    }
}
