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

package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.db.view.DseViewWireFormat;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The batches DSE 6 left in {@code system.batches} of a node before its upgrade are replayed by this software, in both
 * {@code internode_compatibility_mode}s (the rows outlive the mode): DSE stores its own messaging version in the
 * {@code version} column (1024 on 6.8.1+, 1536 on 6.9.7+) and the mutations in the layout of its WRITES version DSE_68,
 * here the mutations of the DSE 6.8.65 and 6.9.26 hints fixtures. A batch whose mutations cannot be decoded is kept in
 * the batchlog and blocks neither the replay cycle nor the batches after it.
 */
@RunWith(BMUnitRunner.class)
public class DseBatchlogReplayTest extends CQLTester
{
    private static final String KS = DseFixtures.FIXTURE_KEYSPACE;
    /** The table of the 6.8.65 fixture (its id), the one of the 6.9.26 fixture, and the two the expected rows go to. */
    private static final String T68 = "t";
    private static final String T69 = "t69";
    private static final String EXPECTED68 = "expected68";
    private static final String EXPECTED69 = "expected69";
    /** DSE's hidden column of a view of the fixture table keyed by {@code v}, as system_schema.hidden_columns lists it. */
    private static final HiddenColumn DSE_V = new HiddenColumn(".v", "regular", true, "text");
    private static final String COLUMNS = "(id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, n bigint, s int static, " +
                                          "tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck))";

    private InternodeCompatibilityMode modeBefore;
    /** The mode a test runs in, which decides the expected end of the ERROR about a kept batch. */
    private InternodeCompatibilityMode mode = InternodeCompatibilityMode.NONE;

    /**
     * The keyspace system_distributed, where the build and the drop of a materialized view are recorded: the tests run
     * without the network, which creates it otherwise, and each test drops the fixture keyspace with its views.
     */
    @BeforeClass
    public static void createSystemDistributedKeyspace()
    {
        Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(SystemDistributedKeyspace.metadata(), SystemDistributedKeyspace.GENERATION));
    }

    @Before
    public void createFixtureTables() throws Throwable
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // the replay needs a ring: this node, owning every token
        TokenMetadata tokens = StorageService.instance.getTokenMetadata();
        if (tokens.getSizeOfAllEndpoints() == 0)
        {
            tokens.updateNormalToken(Util.token("A"), FBUtilities.getBroadcastAddressAndPort());
            tokens.updateHostId(UUID.randomUUID(), FBUtilities.getBroadcastAddressAndPort());
        }
        // a fresh keyspace for every test: a truncation would make the replay skip the mutations of the older batches
        schemaChange("DROP KEYSPACE IF EXISTS " + KS);
        for (String statement : DseFixtures.fixtureSchema(DseVersion.DSE_6_8_65))
            schemaChange(statement);
        schemaChange("CREATE TABLE " + KS + '.' + T69 + ' ' + COLUMNS + " WITH ID = " + DseFixtures.fixtureTableId(DseVersion.DSE_6_9_26));
        schemaChange("CREATE TABLE " + KS + '.' + EXPECTED68 + ' ' + COLUMNS);
        schemaChange("CREATE TABLE " + KS + '.' + EXPECTED69 + ' ' + COLUMNS);
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).truncateBlocking();
        // the batches an earlier test kept are gone with the truncation: the next WARN forgets them
        BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
        BatchlogManager.instance.forceBatchlogReplay();
        assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
        // the batches of a test are an hour old: an earlier test's replay moved the start of the scan past them
        BatchlogManager.instance.resetLastReplayedUuid();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void replaysDseBatchesInNoneMode() throws Throwable
    {
        replaysDseBatches(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void replaysDseBatchesInDse6Mode() throws Throwable
    {
        replaysDseBatches(InternodeCompatibilityMode.DSE_6);
    }

    @Test
    public void undecodableBatchesAreKeptAndBlockNothingInNoneMode() throws Throwable
    {
        undecodableBatchesAreKeptAndBlockNothing(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void undecodableBatchesAreKeptAndBlockNothingInDse6Mode() throws Throwable
    {
        undecodableBatchesAreKeptAndBlockNothing(InternodeCompatibilityMode.DSE_6);
    }

    @Test
    public void dseBatchOfAnUnknownTableIsDroppedInNoneMode() throws Throwable
    {
        dseBatchOfAnUnknownTableIsDropped(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void dseBatchOfAnUnknownTableIsDroppedInDse6Mode() throws Throwable
    {
        dseBatchOfAnUnknownTableIsDropped(InternodeCompatibilityMode.DSE_6);
    }

    /**
     * A DSE batch writing to a table this software does not have (DSE's {@code system_distributed.nodesync_status}) is
     * dropped with the warning the replay logs for a batch of a dropped table: the existing policy for a mutation of an
     * unknown table, which the DSE versions do not change; the batch after it is replayed.
     */
    private void dseBatchOfAnUnknownTableIsDropped(InternodeCompatibilityMode mode) throws Throwable
    {
        useMode(mode);
        List<ByteBuffer> nodesync = nodesyncMutations();

        TimeUUID unknownTable = batchId(0);
        TimeUUID good = batchId(1);
        storeBatch(unknownTable, storedVersion(DseVersion.DSE_6_8_65), nodesync);
        storeBatch(good, storedVersion(DseVersion.DSE_6_8_65), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));

        long replaysBefore = StorageMetrics.batchlogReplays.getCount();
        long hintedReplaysBefore = StorageMetrics.hintedBatchlogReplays.getCount();
        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(Collections.emptySet(), storedBatchIds());
            assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
            assertReplayed(DseVersion.DSE_6_8_65);
            // the replay meters count the good batch only: the skipped one never reaches them
            assertReplayMeters(replaysBefore, hintedReplaysBefore, 1);
            assertEquals(Collections.emptyList(), messages(appender.list, Level.ERROR));
            assertEquals(Collections.singletonList("Skipped batch replay of " + unknownTable + " (stored by DSE 6 with messaging version " +
                                                   storedVersion(DseVersion.DSE_6_8_65) + "; its mutations, none of which is replayed, " +
                                                   "write to unknown table " + DseFixtures.NODESYNC_STATUS_TABLE_ID + ", the first table " +
                                                   "of each mutation) due to Couldn't find table with id " +
                                                   DseFixtures.NODESYNC_STATUS_TABLE_ID + ". If a table was just created, this is " +
                                                   "likely due to the schemanot being fully propagated.  Please wait for schema " +
                                                   "agreement on table creation."),
                         messages(appender.list, Level.WARN).stream().filter(m -> m.startsWith("Skipped")).collect(Collectors.toList()));
        });
    }

    @Test
    public void pre40BatchOfAnUnknownTableIsDroppedInNoneMode() throws Throwable
    {
        pre40BatchOfAnUnknownTableIsDropped(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void pre40BatchOfAnUnknownTableIsDroppedInDse6Mode() throws Throwable
    {
        pre40BatchOfAnUnknownTableIsDropped(InternodeCompatibilityMode.DSE_6);
    }

    /**
     * A batch stored with a pre-4.0 version whose mutation writes to a table this software does not have is dropped, as
     * at the versions of this software: the table id is read before anything of the pre-4.0 layout.
     */
    private void pre40BatchOfAnUnknownTableIsDropped(InternodeCompatibilityMode mode) throws Throwable
    {
        useMode(mode);
        TimeUUID unknownTable = batchId(0);
        storeBatch(unknownTable, MessagingService.VERSION_3014, nodesyncMutations());

        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(Collections.emptySet(), storedBatchIds());
            assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
            assertEquals(Collections.emptyList(), messages(appender.list, Level.ERROR));
            assertEquals(Collections.singletonList("Skipped batch replay of " + unknownTable + " due to Couldn't find table with id " +
                                                   DseFixtures.NODESYNC_STATUS_TABLE_ID + ". If a table was just created, this is " +
                                                   "likely due to the schemanot being fully propagated.  Please wait for schema " +
                                                   "agreement on table creation."),
                         messages(appender.list, Level.WARN).stream().filter(m -> m.startsWith("Skipped")).collect(Collectors.toList()));
        });
    }

    /**
     * DSE 6.0 and 6.0.3 store 256 and 512 and write their mutations at OSS_30, the layout of DSE_68 without the maximum
     * timestamp after the table id: the 6.8.65 mutations without it stand for theirs, and are replayed.
     */
    @Test
    public void replaysDse60TaggedBatches() throws Throwable
    {
        List<ByteBuffer> oss30 = new ArrayList<>();
        for (ByteBuffer dse68 : DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65))
            oss30.add(withoutMaxTimestamp(dse68));
        long replayedBefore = BatchlogManager.instance.getTotalBatchesReplayed();
        storeBatch(batchId(0), DseCompat.DSE_60_VERSION, oss30);
        storeBatch(batchId(1), DseCompat.DSE_603_VERSION, oss30);

        BatchlogManager.instance.forceBatchlogReplay();

        assertEquals(Collections.emptySet(), storedBatchIds());
        assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
        assertEquals(2, BatchlogManager.instance.getTotalBatchesReplayed() - replayedBefore);
        assertReplayed(DseVersion.DSE_6_8_65);
    }

    /**
     * A DSE batch whose mutation names a column the table does not have (as the hidden column of a DSE materialized view
     * does) is kept, and none of its mutations is applied: the replay applies all the mutations of a batch or none.
     */
    @Test
    public void dseBatchNamingAnUnknownColumnIsKept() throws Throwable
    {
        // the fixture table without its column v
        schemaChange("DROP KEYSPACE " + KS);
        for (String statement : DseFixtures.fixtureSchema(DseVersion.DSE_6_8_65))
        {
            String withoutV = statement.replace(", v text,", ",");
            assertEquals(statement, statement.startsWith("CREATE TABLE "), !withoutV.equals(statement));
            schemaChange(withoutV);
        }
        String tableT = KS + '.' + T68 + " (" + DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65) + ')';
        TimeUUID id = batchId(0);
        storeBatch(id, storedVersion(DseVersion.DSE_6_8_65), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));

        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(Collections.singleton(id), storedBatchIds());
            assertEquals(Collections.singleton(id), BatchlogManager.instance.undecodableBatchIds());
            assertEmpty(execute("SELECT * FROM " + KS + '.' + T68));
            List<ILoggingEvent> errors = events(appender.list, Level.ERROR);
            assertEquals(errors.toString(), 1, errors.size());
            assertError(errors.get(0), id, storedVersion(DseVersion.DSE_6_8_65),
                        "java.lang.RuntimeException: Unknown column v during deserialization of " + KS + '.' + T68 + ". ", tableT, true);
        });
    }

    /** A decoding that fails with an AssertionError keeps the batch, as a RuntimeException does. */
    @Test
    @BMRule(name = "FailTheDecodingOfAMutation",
            targetClass = "org.apache.cassandra.db.Mutation$MutationSerializer",
            targetMethod = "deserialize",
            targetLocation = "AT ENTRY",
            action = "throw new java.lang.AssertionError()")
    public void batchFailingWithAnAssertionErrorIsKept() throws Throwable
    {
        String tableT = KS + '.' + T68 + " (" + DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65) + ')';
        TimeUUID id = batchId(0);
        storeBatch(id, storedVersion(DseVersion.DSE_6_8_65), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));

        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(Collections.singleton(id), storedBatchIds());
            assertEquals(Collections.singleton(id), BatchlogManager.instance.undecodableBatchIds());
            List<ILoggingEvent> errors = events(appender.list, Level.ERROR);
            assertEquals(errors.toString(), 1, errors.size());
            assertError(errors.get(0), id, storedVersion(DseVersion.DSE_6_8_65), "java.lang.AssertionError. ", tableT, true);
        });
    }

    /**
     * When the check of the kept batches against {@code system.batches} fails, they stay listed, deleted or not, and the
     * replay cycle is unharmed: the batch after them is replayed.
     */
    @Test
    @BMRule(name = "FailTheCheckOfTheKeptBatches",
            targetClass = "org.apache.cassandra.cql3.QueryProcessor",
            targetMethod = "executeInternal(String, Object[])",
            targetLocation = "AT ENTRY",
            condition = "$1.equals(\"SELECT id FROM system.batches WHERE id = ?\")",
            action = "throw new java.lang.RuntimeException(\"injected\")")
    public void aFailedCheckOfTheKeptBatchesLeavesThemListed() throws Throwable
    {
        TimeUUID kept = batchId(0);
        storeBatch(kept, DseFixtures.UNKNOWN_DSE_VERSION, DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));
        BatchlogManager.instance.forceBatchlogReplay();
        assertEquals(Collections.singleton(kept), BatchlogManager.instance.undecodableBatchIds());
        // the operator's remedy, which the failed check does not see, and a batch for the next cycle
        execute("DELETE FROM system.batches WHERE id = ?", kept);
        storeBatch(batchId(1), storedVersion(DseVersion.DSE_6_8_65), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));

        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.resetLastReplayedUuid();
            BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(Collections.emptySet(), storedBatchIds());
            assertReplayed(DseVersion.DSE_6_8_65);
            assertEquals(Collections.singleton(kept), BatchlogManager.instance.undecodableBatchIds());
            List<String> warnings = messages(appender.list, Level.WARN);
            assertTrue(warnings.toString(), warnings.contains("Failed to check whether the undecodable batches are still in system.batches"));
            List<String> listed = undecodableWarnings(appender.list);
            assertEquals(listed.toString(), 1, listed.size());
            assertTrue(listed.get(0), listed.get(0).startsWith("1 batches of system.batches cannot be decoded and are kept in the batchlog, " +
                                                               "not replayed: " + kept + " (version " + DseFixtures.UNKNOWN_DSE_VERSION + "). "));
        });
    }

    /**
     * Asserts that since the given counts {@code batchlogReplays} was marked {@code replays} times and
     * {@code hintedBatchlogReplays} never.
     */
    private static void assertReplayMeters(long replaysBefore, long hintedReplaysBefore, int replays)
    {
        assertEquals(replays, StorageMetrics.batchlogReplays.getCount() - replaysBefore);
        assertEquals(0, StorageMetrics.hintedBatchlogReplays.getCount() - hintedReplaysBefore);
    }

    /**
     * A batch DSE 6.8.65 stored (version 1024) and one DSE 6.9.26 stored (version 1536) are replayed: their mutations are
     * applied with the values DSE wrote, the batches are removed from the batchlog and counted as replayed.
     */
    private void replaysDseBatches(InternodeCompatibilityMode mode) throws Throwable
    {
        useMode(mode);
        assertEquals(DseCompat.DSE_681_VERSION, storedVersion(DseVersion.DSE_6_8_65));
        assertEquals(DseCompat.DSE_697_VERSION, storedVersion(DseVersion.DSE_6_9_26));

        long replayedBefore = BatchlogManager.instance.getTotalBatchesReplayed();
        long replaysBefore = StorageMetrics.batchlogReplays.getCount();
        long hintedReplaysBefore = StorageMetrics.hintedBatchlogReplays.getCount();
        storeBatch(batchId(0), storedVersion(DseVersion.DSE_6_8_65), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));
        storeBatch(batchId(1), storedVersion(DseVersion.DSE_6_9_26), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_9_26));

        BatchlogManager.instance.forceBatchlogReplay();

        assertEquals(Collections.emptySet(), storedBatchIds());
        assertEquals(2, BatchlogManager.instance.getTotalBatchesReplayed() - replayedBefore);
        // each replayed DSE batch is marked once, in the meter of the batches that wrote no hint (the replica is up)
        assertReplayMeters(replaysBefore, hintedReplaysBefore, 2);
        assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
        assertReplayed(DseVersion.DSE_6_8_65);
        assertReplayed(DseVersion.DSE_6_9_26);
    }

    /**
     * Ahead of a good DSE batch, by their ids, the batchlog holds a batch with a version of DSE's range that is none of
     * DSE 6 (its bytes are fine), and five whose bytes cannot be decoded: DSE's bytes tagged with the current version of
     * this software (decoded with this software's own layout, as the replay decoded DSE's batches before this change), a
     * corrupt blob (a length out of range), a blob cut short, a blob cut right after its table id tagged with the pre-4.0
     * version 11, and a second corrupt blob. The
     * good batch is replayed, removed and counted; the other six are logged once each at ERROR (the stack trace of a
     * decoding failure once per exception class), kept in the batchlog, not counted, listed by a WARN once its interval
     * has passed, met again at the next start of the node, and forgotten, at the next WARN, once an operator deletes them.
     * Once none is kept, the stack trace of a decoding failure is logged again.
     */
    private void undecodableBatchesAreKeptAndBlockNothing(InternodeCompatibilityMode mode) throws Throwable
    {
        useMode(mode);
        List<ByteBuffer> dse68 = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65);
        String tableT = KS + '.' + T68 + " (" + DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65) + ')';

        TimeUUID unknownVersion = batchId(0);
        TimeUUID wrongVersion = batchId(1);
        TimeUUID corrupt = batchId(2);
        TimeUUID cut = batchId(3);
        TimeUUID cutPre40 = batchId(4);
        TimeUUID corruptAgain = batchId(5);
        TimeUUID good = batchId(6);
        storeBatch(unknownVersion, DseFixtures.UNKNOWN_DSE_VERSION, dse68);
        storeBatch(wrongVersion, MessagingService.current_version, dse68);
        storeBatch(corrupt, storedVersion(DseVersion.DSE_6_8_65), Collections.singletonList(corruptMutation()));
        storeBatch(cut, storedVersion(DseVersion.DSE_6_8_65), Collections.singletonList(cutMutation()));
        storeBatch(cutPre40, MessagingService.VERSION_3014, Collections.singletonList(cutAfterTableId()));
        storeBatch(corruptAgain, storedVersion(DseVersion.DSE_6_8_65), Collections.singletonList(corruptMutation()));
        storeBatch(good, storedVersion(DseVersion.DSE_6_8_65), dse68);
        List<TimeUUID> kept = Arrays.asList(unknownVersion, wrongVersion, corrupt, cut, cutPre40, corruptAgain);
        HashSet<TimeUUID> undecodable = new HashSet<>(kept);

        long replayedBefore = BatchlogManager.instance.getTotalBatchesReplayed();
        long replaysBefore = StorageMetrics.batchlogReplays.getCount();
        long hintedReplaysBefore = StorageMetrics.hintedBatchlogReplays.getCount();
        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(undecodable, storedBatchIds());
            assertEquals(undecodable, BatchlogManager.instance.undecodableBatchIds());
            assertEquals(1, BatchlogManager.instance.getTotalBatchesReplayed() - replayedBefore);
            // the replay meters count the good batch only: a kept batch is not replayed
            assertReplayMeters(replaysBefore, hintedReplaysBefore, 1);
            assertReplayed(DseVersion.DSE_6_8_65);
            List<ILoggingEvent> errors = events(appender.list, Level.ERROR);
            assertEquals(errors.toString(), 6, errors.size());
            assertEquals("Batch " + unknownVersion + " of system.batches, stored with messaging version " + DseFixtures.UNKNOWN_DSE_VERSION +
                         ", cannot be decoded: it is in DSE's range of messaging versions but not a DSE 6 version (256, 512, 768, " +
                         "1024, 1280 or 1536). It is kept in the batchlog and not replayed; the other batches are replayed as usual. " +
                         "Its mutations may be missing on some of their replicas; they write to " + tableT + " (the first table of " +
                         "each mutation). The batch is tried again at the next start of this node; if it is older than the " +
                         "gc_grace_seconds of its tables, it would not be replayed even if it could be decoded. To drop it, save it " +
                         "(SELECT mutations, version FROM system.batches WHERE id = " + unknownVersion + "), delete it on this node " +
                         "(DELETE FROM system.batches WHERE id = " + unknownVersion + ") and repair the tables it writes to" +
                         repairWhen(),
                         errors.get(0).getFormattedMessage());
            // no stack trace for an unknown version, the decoding failure for the others, once per exception class
            assertNull(errors.get(0).getThrowableProxy());
            assertError(errors.get(1), wrongVersion, MessagingService.current_version, "java.lang.IllegalStateException: Unknown synthetic column . ",
                        tableT, true);
            assertError(errors.get(2), corrupt, storedVersion(DseVersion.DSE_6_8_65),
                        "org.apache.cassandra.utils.vint.VIntCoding$VIntOutOfRangeException: 34224994295 is out of range for a 32-bit integer. ",
                        tableT, true);
            assertError(errors.get(3), cut, storedVersion(DseVersion.DSE_6_8_65), "java.io.EOFException. ", tableT, true);
            // a pre-4.0 version is none of this software's, whose batches are dropped on an IOException: the batch is kept
            assertError(errors.get(4), cutPre40, MessagingService.VERSION_3014, "java.io.EOFException. ", tableT, false);
            assertError(errors.get(5), corruptAgain, storedVersion(DseVersion.DSE_6_8_65),
                        "org.apache.cassandra.utils.vint.VIntCoding$VIntOutOfRangeException: 34224994295 is out of range for a 32-bit integer. ",
                        tableT, false);
            // the WARN listing them waits for its interval
            assertEquals(Collections.emptyList(), undecodableWarnings(appender.list));

            // a later cycle of the same run neither replays nor logs them again, but for the WARN once its interval is over
            appender.list.clear();
            BatchlogManager.instance.resetLastReplayedUuid();
            BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
            BatchlogManager.instance.forceBatchlogReplay();
            assertEquals(undecodable, storedBatchIds());
            assertEquals(Collections.emptyList(), events(appender.list, Level.ERROR));
            List<String> warnings = undecodableWarnings(appender.list);
            assertEquals(warnings.toString(), 1, warnings.size());
            assertTrue(warnings.get(0), warnings.get(0).startsWith("6 batches of system.batches cannot be decoded and are kept in the batchlog, " +
                                                                   "not replayed: "));
            assertTrue(warnings.get(0), warnings.get(0).endsWith(". See the errors logged when they were first met"));
            for (TimeUUID id : kept)
                assertTrue(warnings.get(0), warnings.get(0).contains(id + " (version "));
            assertTrue(warnings.get(0), warnings.get(0).contains(unknownVersion + " (version " + DseFixtures.UNKNOWN_DSE_VERSION + ')'));
            // and not again before the next interval
            appender.list.clear();
            BatchlogManager.instance.forceBatchlogReplay();
            assertEquals(Collections.emptyList(), undecodableWarnings(appender.list));
            // nor do the later cycles mark the meters for them
            assertReplayMeters(replaysBefore, hintedReplaysBefore, 1);

            // the next start of the node scans the batchlog from the first batch: the batches are logged and kept again
            BatchlogManager restarted = new BatchlogManager();
            try
            {
                restarted.performInitialReplay();
                assertEquals(undecodable, storedBatchIds());
                assertEquals(undecodable, restarted.undecodableBatchIds());
                assertEquals(6, events(appender.list, Level.ERROR).size());
                // the meters are node-wide: the new scan does not mark them for the kept batches either
                assertReplayMeters(replaysBefore, hintedReplaysBefore, 1);
            }
            finally
            {
                restarted.shutdownAndWait(1, TimeUnit.MINUTES);
            }
        });

        // the operator's remedy: the deleted batches are forgotten at the next WARN
        execute("DELETE FROM system.batches WHERE id = ?", unknownVersion);
        execute("DELETE FROM system.batches WHERE id = ?", corrupt);
        execute("DELETE FROM system.batches WHERE id = ?", cut);
        execute("DELETE FROM system.batches WHERE id = ?", cutPre40);
        execute("DELETE FROM system.batches WHERE id = ?", corruptAgain);
        BatchlogManager.instance.forceBatchlogReplay();
        assertEquals(undecodable, BatchlogManager.instance.undecodableBatchIds());
        BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
        BatchlogManager.instance.forceBatchlogReplay();
        assertEquals(Collections.singleton(wrongVersion), storedBatchIds());
        assertEquals(Collections.singleton(wrongVersion), BatchlogManager.instance.undecodableBatchIds());
        execute("DELETE FROM system.batches WHERE id = ?", wrongVersion);
        BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
        BatchlogManager.instance.forceBatchlogReplay();
        assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
        // a batch deleted by the operator is forgotten, not counted as replayed
        assertReplayMeters(replaysBefore, hintedReplaysBefore, 1);

        // no batch is kept any more: the next one of an exception class already met logs its stack trace again
        TimeUUID corruptLater = batchId(7);
        storeBatch(corruptLater, storedVersion(DseVersion.DSE_6_8_65), Collections.singletonList(corruptMutation()));
        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.resetLastReplayedUuid();
            BatchlogManager.instance.forceBatchlogReplay();
            List<ILoggingEvent> errors = events(appender.list, Level.ERROR);
            assertEquals(errors.toString(), 1, errors.size());
            assertError(errors.get(0), corruptLater, storedVersion(DseVersion.DSE_6_8_65),
                        "org.apache.cassandra.utils.vint.VIntCoding$VIntOutOfRangeException: 34224994295 is out of range for a 32-bit integer. ",
                        tableT, true);
            assertReplayMeters(replaysBefore, hintedReplaysBefore, 1);
        });
        execute("DELETE FROM system.batches WHERE id = ?", corruptLater);
    }

    /**
     * A row with no version, or an empty one, is kept like an undecodable batch; a row with no mutations (an
     * empty list) has nothing to replay and is removed and counted, as a batch whose mutations all expired, with nothing
     * logged, whatever its version.
     */
    @Test
    public void batchesWithoutVersionOrMutations() throws Throwable
    {
        TimeUUID noVersion = batchId(0);
        TimeUUID noMutations = batchId(1);
        TimeUUID nothingAtAll = batchId(2);
        TimeUUID noMutationsUnknownVersion = batchId(3);
        execute("INSERT INTO system.batches (id, mutations) VALUES (?, ?)", noVersion, DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));
        execute("INSERT INTO system.batches (id, version) VALUES (?, ?)", noMutations, MessagingService.current_version);
        // a row with no cell but its primary key
        execute("INSERT INTO system.batches (id) VALUES (?)", nothingAtAll);
        execute("INSERT INTO system.batches (id, version) VALUES (?, ?)", noMutationsUnknownVersion, DseFixtures.UNKNOWN_DSE_VERSION);
        // an empty version value, which CQL accepts for an int column
        TimeUUID emptyVersion = batchId(4);
        execute("INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, blobAsInt(0x))", emptyVersion,
                DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));
        Set<TimeUUID> kept = new HashSet<>(Arrays.asList(noVersion, emptyVersion));

        long replayedBefore = BatchlogManager.instance.getTotalBatchesReplayed();
        try
        {
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();

                assertEquals(kept, storedBatchIds());
                assertEquals(kept, BatchlogManager.instance.undecodableBatchIds());
                assertEquals(3, BatchlogManager.instance.getTotalBatchesReplayed() - replayedBefore);
                List<String> errors = messages(appender.list, Level.ERROR);
                assertEquals(errors.toString(), 2, errors.size());
                for (TimeUUID id : new TimeUUID[]{ noVersion, emptyVersion })
                    assertTrue(errors.toString(), errors.stream().anyMatch(e -> e.startsWith("Batch " + id + " of system.batches, stored without a " +
                                                                                             "messaging version, cannot be decoded: its mutations " +
                                                                                             "cannot be decoded without it. ")));
                assertEquals(Collections.emptyList(), messages(appender.list, Level.WARN));

                // the WARN lists the row without a version as such
                BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
                BatchlogManager.instance.forceBatchlogReplay();
                List<String> warnings = undecodableWarnings(appender.list);
                assertEquals(warnings.toString(), 1, warnings.size());
                assertTrue(warnings.get(0), warnings.get(0).startsWith("2 batches of system.batches cannot be decoded and are kept in the " +
                                                                       "batchlog, not replayed: "));
                for (TimeUUID id : kept)
                    assertTrue(warnings.get(0), warnings.get(0).contains(id + " (no version)"));
            });
        }
        finally
        {
            for (TimeUUID id : kept)
                execute("DELETE FROM system.batches WHERE id = ?", id);
        }
    }

    @Test
    public void ownBatchCutShortIsDroppedAsBeforeInNoneMode() throws Throwable
    {
        ownBatchCutShortIsDroppedAsBefore(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void ownBatchCutShortIsDroppedAsBeforeInDse6Mode() throws Throwable
    {
        ownBatchCutShortIsDroppedAsBefore(InternodeCompatibilityMode.DSE_6);
    }

    /**
     * A batch of this software (a version below 256) whose mutation fails to decode with an IOException is dropped with
     * the warning the replay always logged for it; the batch after it is replayed.
     */
    private void ownBatchCutShortIsDroppedAsBefore(InternodeCompatibilityMode mode) throws Throwable
    {
        useMode(mode);
        TimeUUID cut = batchId(0);
        storeBatch(cut, MessagingService.current_version, Collections.singletonList(ownCutMutation()));
        storeBatch(batchId(1), storedVersion(DseVersion.DSE_6_8_65), DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65));

        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(Collections.emptySet(), storedBatchIds());
            assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
            assertReplayed(DseVersion.DSE_6_8_65);
            assertEquals(Collections.emptyList(), messages(appender.list, Level.ERROR));
            List<String> skipped = messages(appender.list, Level.WARN).stream().filter(m -> m.startsWith("Skipped")).collect(Collectors.toList());
            assertEquals(skipped.toString(), 1, skipped.size());
            assertTrue(skipped.get(0), skipped.get(0).startsWith("Skipped batch replay of " + cut + " due to "));
            assertFalse(skipped.get(0), skipped.get(0).contains("DSE"));
        });
    }

    /**
     * The WARN lists at most ten batches, then how many more there are; the first ten kept batches are logged at ERROR each,
     * then one more ERROR says that the others are logged at DEBUG only.
     */
    @Test
    public void warningListsTenBatches() throws Throwable
    {
        List<TimeUUID> ids = new ArrayList<>();
        for (int n = 0; n < 11; n++)
        {
            ids.add(batchId(n));
            storeBatch(ids.get(n), DseFixtures.UNKNOWN_DSE_VERSION, Collections.singletonList(ByteBuffer.wrap(new byte[]{ 1 })));
        }
        try
        {
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();
                assertEquals(new HashSet<>(ids), BatchlogManager.instance.undecodableBatchIds());
                List<String> errors = messages(appender.list, Level.ERROR);
                assertEquals(errors.toString(), 11, errors.size());
                // in the order of the scan, by token
                Set<String> logged = new HashSet<>();
                for (int n = 0; n < 10; n++)
                    logged.add(errors.get(n).substring(0, errors.get(n).indexOf(" of system.batches, ")));
                assertEquals(errors.toString(), 10, logged.size());
                for (String batch : logged)
                    assertTrue(batch, ids.stream().anyMatch(id -> batch.equals("Batch " + id)));
                assertEquals("More batches of system.batches cannot be decoded than the 10 logged above: they are kept in the " +
                             "batchlog and not replayed as well, each logged at DEBUG only; the periodic warning counts and lists them",
                             errors.get(10));
                BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
                BatchlogManager.instance.forceBatchlogReplay();
                List<String> warnings = undecodableWarnings(appender.list);
                assertEquals(warnings.toString(), 1, warnings.size());
                assertTrue(warnings.get(0), warnings.get(0).startsWith("11 batches of system.batches cannot be decoded"));
                assertTrue(warnings.get(0), warnings.get(0).contains(" and 1 more. "));
                assertEquals(warnings.get(0), 10, warnings.get(0).split(" \\(version " + DseFixtures.UNKNOWN_DSE_VERSION + "\\)").length - 1);
            });
        }
        finally
        {
            for (TimeUUID id : ids)
                execute("DELETE FROM system.batches WHERE id = ?", id);
            BatchlogManager.instance.expireUndecodableBatchesWarnInterval();
            BatchlogManager.instance.forceBatchlogReplay();
        }
        assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
    }

    /** {@link BatchlogManager#describeTables} names the table of each mutation it can read the id of, and no more. */
    @Test
    public void describeTables() throws Throwable
    {
        ByteBuffer dse = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65).get(0);
        UUID unknown = DseFixtures.NODESYNC_STATUS_TABLE_ID;
        List<ByteBuffer> nodesync = new ArrayList<>();
        for (byte[] hint : DseFixtures.hintsRecords(DseVersion.DSE_6_8_65))
            if (DseFixtures.hintTableId(hint).equals(unknown))
                nodesync.add(DseFixtures.hintMutation(hint));
        assertEquals(KS + '.' + T68 + " (" + DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65) + "), unknown table " + unknown +
                     ", a mutation too short or corrupt to name its table",
                     BatchlogManager.describeTables(Arrays.asList(dse, dse.duplicate(), nodesync.get(0), ByteBuffer.wrap(new byte[]{ 1, 2, 3 }))));
        assertEquals("no mutation", BatchlogManager.describeTables(Collections.emptyList()));
    }

    /**
     * {@link DseFixtures#dse68Mutation}, which the tests of the view updates DSE stored build them with, writes the bytes DSE
     * wrote: each mutation of the hints fixtures, decoded and written again, is the same bytes.
     */
    @Test
    public void dse68MutationWritesTheBytesOfDse() throws Throwable
    {
        for (DseVersion version : new DseVersion[]{ DseVersion.DSE_6_8_65, DseVersion.DSE_6_9_26 })
        {
            for (ByteBuffer dse : DseFixtures.fixtureHintMutations(version))
            {
                Mutation decoded;
                try (DataInputBuffer in = new DataInputBuffer(dse.duplicate(), true))
                {
                    decoded = Mutation.serializer.deserialize(in, MessagingService.VERSION_DSE_68);
                }
                assertEquals(ByteBufferUtil.bytesToHex(dse), ByteBufferUtil.bytesToHex(DseFixtures.dse68Mutation(decoded)));
            }
        }
    }

    /**
     * In {@code DSE_6}, a DSE batch holding the update of a materialized view, which names DSE's hidden column of the view,
     * is replayed when the catalog of DSE's hidden columns lists that column for the view: the rows get the liveness of
     * their hidden cells, so the entry DSE wrote dead (a hidden tombstone) stays dead, and the batch is removed.
     */
    @Test
    public void dseBatchOfAViewIsReplayedInDse6Mode() throws Throwable
    {
        String viewName = "t_by_v";
        createFixtureView(viewName, T68);
        useMode(InternodeCompatibilityMode.DSE_6);
        TableMetadata view = Schema.instance.getTableMetadata(KS, viewName);
        TimeUUID id = batchId(0);
        storeBatch(id, storedVersion(DseVersion.DSE_6_8_65), Collections.singletonList(DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(view, "x"))));
        long replayedBefore = BatchlogManager.instance.getTotalBatchesReplayed();

        withCatalog(Collections.singletonMap(Pair.create(KS, viewName), Collections.singletonList(DSE_V)), () ->
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();

                assertEquals(Collections.emptySet(), storedBatchIds());
                assertEquals(Collections.emptySet(), BatchlogManager.instance.undecodableBatchIds());
                assertEquals(1, BatchlogManager.instance.getTotalBatchesReplayed() - replayedBefore);
                assertEquals(Collections.emptyList(), messages(appender.list, Level.ERROR));
                assertRows(execute("SELECT v, id, ck FROM " + KS + '.' + viewName + " WHERE v = 'x'"), row("x", 1, "a"));
            }));
    }

    /**
     * Without the mode, a DSE batch holding the update of a materialized view is kept, as before, with the advice for a
     * view: the hidden column of the view is not translated, and the catalog of DSE's hidden columns is not read.
     */
    @Test
    public void dseBatchOfAViewIsKeptInNoneMode() throws Throwable
    {
        String viewName = "t_by_v";
        createFixtureView(viewName, T68);
        useMode(InternodeCompatibilityMode.NONE);
        TableMetadata view = Schema.instance.getTableMetadata(KS, viewName);
        TimeUUID id = batchId(0);
        storeBatch(id, storedVersion(DseVersion.DSE_6_8_65), Collections.singletonList(DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(view, "x"))));

        withCatalog(null, () -> {
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();

                assertKeptViewBatch(appender.list, id, view);
            });
            assertNull("the catalog was not read", DseViewHiddenColumnCatalog.setInstance(null));
        });
    }

    /**
     * In {@code DSE_6}, a DSE batch holding the update of a materialized view is kept, as before, when the translation of
     * the hidden column is not certain (the catalog of DSE's hidden columns lists no hidden column for the view, or cannot
     * be read), or when the batch is DSE 6.0's (views with hidden columns came with DSE 6.7); a WARN says why the column is
     * not translated.
     */
    @Test
    public void dseBatchOfAViewIsKeptWhenItsTranslationIsUncertain() throws Throwable
    {
        createFixtureView("unlisted_by_v", T68);
        createFixtureView("unreadable_by_v", T68);
        createFixtureView("dse60_by_v", T68);
        useMode(InternodeCompatibilityMode.DSE_6);
        TableMetadata unlisted = Schema.instance.getTableMetadata(KS, "unlisted_by_v");
        TableMetadata unreadable = Schema.instance.getTableMetadata(KS, "unreadable_by_v");
        TableMetadata dse60 = Schema.instance.getTableMetadata(KS, "dse60_by_v");
        int version = storedVersion(DseVersion.DSE_6_8_65);

        TimeUUID unlistedId = batchId(0);
        storeBatch(unlistedId, version, Collections.singletonList(DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(unlisted, "x"))));
        withCatalog(Collections.emptyMap(), () -> withLogAppender(DseViewWireFormat.class, warnings -> {
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();

                assertKeptViewBatch(appender.list, unlistedId, unlisted);
            });
            assertTrue(warnings.list.toString(), messages(warnings.list, Level.WARN).stream().anyMatch(m -> m.contains(
            "names the column .v of materialized view " + unlisted + ", which is not translated: DSE's system_schema.hidden_columns on this " +
            "node lists no hidden column for that view.")));
        }));

        TimeUUID unreadableId = batchId(1);
        // the scan of the next replay starts from the first batch again: the batches kept before are not decoded again
        BatchlogManager.instance.resetLastReplayedUuid();
        storeBatch(unreadableId, version, Collections.singletonList(DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(unreadable, "x"))));
        withCatalog(null, () -> withLogAppender(DseViewWireFormat.class, warnings -> {
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();

                assertKeptViewBatch(appender.list, unreadableId, unreadable);
            });
            assertTrue(warnings.list.toString(), messages(warnings.list, Level.WARN).stream().anyMatch(m -> m.contains(
            "names the column .v of materialized view " + unreadable + ", which is not translated: DSE 6 compatibility mode: cannot read " +
            "DSE's system_schema.hidden_columns from the data directories of this node")));
        }));

        // DSE 6.0's layout: the update without the maximum timestamp DSE_68 writes after the table id
        TimeUUID dse60Id = batchId(2);
        // the scan of the next replay starts from the first batch again: the batches kept before are not decoded again
        BatchlogManager.instance.resetLastReplayedUuid();
        storeBatch(dse60Id, DseCompat.DSE_60_VERSION, Collections.singletonList(withoutMaxTimestamp(DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(dse60, "x")))));
        withCatalog(Collections.singletonMap(Pair.create(KS, "dse60_by_v"), Collections.singletonList(DSE_V)), () ->
            withLogAppender(BatchlogManager.class, appender -> {
                BatchlogManager.instance.forceBatchlogReplay();

                List<ILoggingEvent> errors = events(appender.list, Level.ERROR);
                assertEquals(errors.toString(), 1, errors.size());
                assertTrue(errors.get(0).getFormattedMessage(),
                           errors.get(0).getFormattedMessage().startsWith("Batch " + dse60Id + " of system.batches, stored with messaging version " +
                                                                          DseCompat.DSE_60_VERSION + ", cannot be decoded: java.lang.RuntimeException: " +
                                                                          "Unknown column .v during deserialization of " + dse60 + ". "));
            }));
        assertEquals(new HashSet<>(Arrays.asList(unlistedId, unreadableId, dse60Id)), storedBatchIds());
        assertEquals(new HashSet<>(Arrays.asList(unlistedId, unreadableId, dse60Id)), BatchlogManager.instance.undecodableBatchIds());
        for (String viewName : new String[]{ "unlisted_by_v", "unreadable_by_v", "dse60_by_v" })
            assertEmpty(execute("SELECT * FROM " + KS + '.' + viewName));
    }

    /**
     * The batch {@code id}, holding the view update of {@link DseFixtures#dseViewUpdate} for {@code view}, is kept with one ERROR naming
     * the hidden column as a column unknown here and advising to repair or rebuild the view; none of its rows is written.
     */
    private void assertKeptViewBatch(List<ILoggingEvent> events, TimeUUID id, TableMetadata view) throws Throwable
    {
        assertTrue(storedBatchIds().contains(id));
        assertTrue(BatchlogManager.instance.undecodableBatchIds().contains(id));
        List<String> errors = messages(events, Level.ERROR);
        assertEquals(errors.toString(), 1, errors.size());
        assertTrue(errors.get(0), errors.get(0).startsWith("Batch " + id + " of system.batches, stored with messaging version " +
                                                           storedVersion(DseVersion.DSE_6_8_65) + ", cannot be decoded: java.lang.RuntimeException: " +
                                                           "Unknown column .v during deserialization of " + view + ". "));
        assertTrue(errors.get(0), errors.get(0).endsWith("(DELETE FROM system.batches WHERE id = " + id + ") and " +
                                                         viewAdvice(T68, view.name) + repairWhen()));
        assertEmpty(execute("SELECT * FROM " + view));
    }

    /**
     * Runs {@code body} with the catalog of DSE's hidden columns listing {@code hiddenColumns}, or with none read yet when
     * {@code hiddenColumns} is null (this test's data directories do not hold DSE's table, so reading it fails), then
     * restores the catalog.
     */
    private static void withCatalog(Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns, ThrowingRunnable body) throws Throwable
    {
        DseViewHiddenColumnCatalog before = DseViewHiddenColumnCatalog.setInstance(hiddenColumns == null ? null : new DseViewHiddenColumnCatalog(hiddenColumns));
        try
        {
            body.run();
        }
        finally
        {
            DseViewHiddenColumnCatalog.setInstance(before);
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    @Test
    public void keptBatchOfAViewAdvisesToRepairTheViewInNoneMode() throws Throwable
    {
        keptBatchOfAViewAdvisesToRepairTheView(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void keptBatchOfAViewAdvisesToRepairTheViewInDse6Mode() throws Throwable
    {
        keptBatchOfAViewAdvisesToRepairTheView(InternodeCompatibilityMode.DSE_6);
    }

    /**
     * A kept batch whose mutations write to a materialized view (the batchlog holds the view updates of base writes): the
     * ERROR advises to repair the view, to repair its base table or to rebuild the view, and to repair the other tables
     * the batch writes to, each with the timing of the mode.
     */
    private void keptBatchOfAViewAdvisesToRepairTheView(InternodeCompatibilityMode mode) throws Throwable
    {
        // the schema first, as for the fixture tables: the mode is entered once it is in place
        String viewName = "t_by_v";
        createFixtureView(viewName, T68);
        useMode(mode);
        TableMetadata view = Schema.instance.getTableMetadata(KS, viewName);
        String tableT = KS + '.' + T68 + " (" + DseFixtures.fixtureTableId(DseVersion.DSE_6_8_65) + ')';
        String tableView = KS + '.' + viewName + " (" + view.id + ')';

        TimeUUID viewOnly = batchId(0);
        TimeUUID viewAndTable = batchId(1);
        storeBatch(viewOnly, storedVersion(DseVersion.DSE_6_8_65), Arrays.asList(corruptMutation(view.id), corruptMutation(view.id)));
        storeBatch(viewAndTable, storedVersion(DseVersion.DSE_6_8_65), Arrays.asList(corruptMutation(view.id), corruptMutation()));
        withLogAppender(BatchlogManager.class, appender -> {
            BatchlogManager.instance.forceBatchlogReplay();

            assertEquals(new HashSet<>(Arrays.asList(viewOnly, viewAndTable)), BatchlogManager.instance.undecodableBatchIds());
            List<String> errors = messages(appender.list, Level.ERROR);
            assertEquals(errors.toString(), 2, errors.size());
            assertTrue(errors.get(0), errors.get(0).contains(" they write to " + tableView + " (the first table of each mutation). "));
            assertTrue(errors.get(0), errors.get(0).endsWith("(DELETE FROM system.batches WHERE id = " + viewOnly + ") and " +
                                                             viewAdvice(T68, viewName) + repairWhen()));
            assertTrue(errors.get(1), errors.get(1).contains(" they write to " + tableView + ", " + tableT + " (the first table of each mutation). "));
            assertTrue(errors.get(1), errors.get(1).endsWith("(DELETE FROM system.batches WHERE id = " + viewAndTable + ") and repair the other " +
                                                             "tables it writes to" + repairWhen() + "; " + viewAdvice(T68, viewName) + repairWhen()));
        });
    }

    /**
     * {@link BatchlogManager#describeRepair} names each view among the first tables of the mutations, with its base table,
     * once, and the other tables as a whole: a base table with a view, a view no longer in the schema and a mutation that
     * does not name its table are among the other tables. Each clause ends with the timing given.
     */
    @Test
    public void describeRepair() throws Throwable
    {
        createFixtureView("t_by_v", T68);
        createFixtureView("t69_by_v", T69);
        createFixtureView("dropped_by_v", T69);
        TableId droppedId = Schema.instance.getTableMetadata(KS, "dropped_by_v").id;
        schemaChange("DROP MATERIALIZED VIEW " + KS + ".dropped_by_v");
        ByteBuffer view = corruptMutation(Schema.instance.getTableMetadata(KS, "t_by_v").id);
        ByteBuffer view69 = corruptMutation(Schema.instance.getTableMetadata(KS, "t69_by_v").id);
        ByteBuffer droppedView = corruptMutation(droppedId);
        // the fixture table, the base table of t_by_v
        ByteBuffer table = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65).get(0);
        ByteBuffer tooShort = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        String when = " (once)";

        assertEquals("repair the tables it writes to", BatchlogManager.describeRepair(Collections.emptyList(), ""));
        assertEquals("repair the tables it writes to" + when, BatchlogManager.describeRepair(Arrays.asList(table, tooShort, droppedView), when));
        assertEquals(viewAdvice(T68, "t_by_v"), BatchlogManager.describeRepair(Arrays.asList(view, view.duplicate()), ""));
        assertEquals("repair the other tables it writes to" + when + "; " + viewAdvice(T68, "t_by_v") + when + "; " + viewAdvice(T69, "t69_by_v") + when,
                     BatchlogManager.describeRepair(Arrays.asList(view, tooShort, view69), when));
    }

    /** The advice of the ERROR of a kept batch for the view {@code view} of the fixture table {@code base}, without its timing. */
    private static String viewAdvice(String base, String view)
    {
        return "repair the view " + KS + '.' + view + " on every node (the view replicas that miss its updates are usually other nodes), " +
               "which restores the lost view rows that another replica of the view holds, or repair its base table " + KS + '.' + base +
               ", which restores the lost view rows only where the replicas of the base table differ, or rebuild the view (drop it and " +
               "create it again, with materialized_views_enabled: true), which restores all the view rows";
    }

    /**
     * Creates the materialized view {@code view} of the table {@code base} of the fixture keyspace, keyed by {@code v}. The
     * keyspace system_distributed, where the build and the drop of a view are recorded, is created by the class setup.
     */
    private void createFixtureView(String view, String base) throws Throwable
    {
        schemaChange("CREATE MATERIALIZED VIEW " + KS + '.' + view + " AS SELECT id, ck, v FROM " + KS + '.' + base +
                     " WHERE v IS NOT NULL AND id IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, id, ck)");
    }

    private void assertError(ILoggingEvent error, TimeUUID id, int version, String reason, String tables, boolean traced)
    {
        String message = error.getFormattedMessage();
        assertTrue(message, message.startsWith("Batch " + id + " of system.batches, stored with messaging version " + version +
                                               ", cannot be decoded: " + reason));
        assertTrue(message, message.contains(". Its mutations may be missing on some of their replicas; they write to " + tables +
                                             " (the first table of each mutation). "));
        assertTrue(message, message.endsWith("(DELETE FROM system.batches WHERE id = " + id + ") and repair the tables it writes to" +
                                             repairWhen()));
        assertEquals("the stack trace of the decoding failure of " + id, traced, error.getThrowableProxy() != null);
    }

    /** The end of the ERROR about a kept batch: in {@code DSE_6} the upgrade is in progress, and the repair waits for its end. */
    private String repairWhen()
    {
        return mode == InternodeCompatibilityMode.DSE_6 ? " (once the DSE 6 upgrade is finished)" : "";
    }

    /** Runs the rest of the test in {@code mode}. */
    private void useMode(InternodeCompatibilityMode mode)
    {
        this.mode = mode;
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
    }

    /** The events of the batchlog at {@code level} among {@code events}. */
    private static List<ILoggingEvent> events(List<ILoggingEvent> events, Level level)
    {
        return events.stream().filter(e -> e.getLevel() == level).collect(Collectors.toList());
    }

    /** The messages of the batchlog at {@code level} among {@code events}. */
    private static List<String> messages(List<ILoggingEvent> events, Level level)
    {
        return events(events, level).stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    /** The WARN lines listing the undecodable batches among {@code events}. */
    private static List<String> undecodableWarnings(List<ILoggingEvent> events)
    {
        return messages(events, Level.WARN).stream().filter(m -> m.contains("batches of system.batches cannot be decoded")).collect(Collectors.toList());
    }

    /** The messaging version a DSE node of {@code version} stores in {@code system.batches}: its relative version shifted by 8. */
    private static int storedVersion(DseVersion version)
    {
        return version.relativeMessagingVersion << 8;
    }

    /**
     * The id of the {@code n}-th batch of a test, old enough for the replay to take it (older than the batchlog timeout)
     * and young enough not to be expired by the gc grace seconds of the fixture table; the ids sort in {@code n} order.
     */
    private static TimeUUID batchId(int n)
    {
        long millis = FBUtilities.now().toEpochMilli() - BatchlogManager.getBatchlogTimeout() - TimeUnit.HOURS.toMillis(1);
        return atUnixMillis(millis + n, 0);
    }

    /** Writes a batch into {@code system.batches} as the node of {@code version} wrote it: the version next to the mutations. */
    private void storeBatch(TimeUUID id, int version, List<ByteBuffer> mutations) throws Throwable
    {
        execute("INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?) USING TIMESTAMP ?",
                id, mutations, version, id.unix(MILLISECONDS) * 1000);
    }

    private Set<TimeUUID> storedBatchIds() throws Throwable
    {
        Set<TimeUUID> ids = new HashSet<>();
        execute("SELECT id FROM system.batches").forEach(row -> ids.add(row.getTimeUUID("id")));
        return ids;
    }

    /** The mutations of the DSE 6.8.65 hints fixture that write to DSE's {@code system_distributed.nodesync_status}. */
    private static List<ByteBuffer> nodesyncMutations() throws IOException
    {
        List<ByteBuffer> nodesync = new ArrayList<>();
        for (byte[] hint : DseFixtures.hintsRecords(DseVersion.DSE_6_8_65))
            if (DseFixtures.hintTableId(hint).equals(DseFixtures.NODESYNC_STATUS_TABLE_ID))
                nodesync.add(DseFixtures.hintMutation(hint));
        assertFalse("nodesync hints in the 6.8.65 fixture", nodesync.isEmpty());
        return nodesync;
    }

    /** A mutation of the fixture in DSE's DSE_68 layout, without the maximum timestamp after its table id: the OSS_30 layout. */
    private static ByteBuffer withoutMaxTimestamp(ByteBuffer dse68)
    {
        byte[] bytes = new byte[dse68.remaining()];
        dse68.duplicate().get(bytes);
        assertEquals("partition updates of a fixture mutation", 1, bytes[0]);
        // one partition update (1 byte) and its table id (16 bytes), then the maximum timestamp (8 bytes) only DSE_68 writes
        byte[] oss30 = new byte[bytes.length - 8];
        System.arraycopy(bytes, 0, oss30, 0, 1 + 16);
        System.arraycopy(bytes, 1 + 16 + 8, oss30, 1 + 16, bytes.length - (1 + 16 + 8));
        return ByteBuffer.wrap(oss30);
    }

    /**
     * A mutation of the fixture table whose bytes past the table id and DSE's maximum timestamp are corrupt: the length
     * of the partition key, an unsigned vint, does not fit in an int.
     */
    private static ByteBuffer corruptMutation() throws IOException
    {
        ByteBuffer good = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65).get(0);
        byte[] bytes = new byte[good.remaining()];
        good.duplicate().get(bytes);
        // one partition update (1 byte), the table id (16 bytes), the maximum timestamp (8 bytes), then the update
        for (int i = 1 + 16 + 8; i < bytes.length; i++)
            bytes[i] = (byte) 0xF7;
        return ByteBuffer.wrap(bytes);
    }

    /** {@link #corruptMutation()}, writing to the table {@code tableId} instead of the fixture table. */
    private static ByteBuffer corruptMutation(TableId tableId) throws IOException
    {
        ByteBuffer corrupt = corruptMutation();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            tableId.serialize(out);
            // after the number of partition updates (1 byte)
            ByteBuffer id = out.asNewBuffer();
            for (int i = 0; i < id.remaining(); i++)
                corrupt.put(corrupt.position() + 1 + i, id.get(id.position() + i));
        }
        return corrupt;
    }

    /** A mutation of the fixture table, in this software's layout, cut short after its table id and 8 more bytes. */
    private static ByteBuffer ownCutMutation() throws IOException
    {
        Mutation decoded;
        try (DataInputBuffer in = new DataInputBuffer(DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65).get(0), true))
        {
            decoded = Mutation.serializer.deserialize(in, MessagingService.VERSION_DSE_68);
        }
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(decoded, out, MessagingService.current_version);
            return ByteBuffer.wrap(Arrays.copyOf(out.toByteArray(), 1 + 16 + 8));
        }
    }

    /** The number of partition updates and the table id of a mutation of the fixture table, and nothing after them. */
    private static ByteBuffer cutAfterTableId() throws IOException
    {
        ByteBuffer cut = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65).get(0).duplicate();
        cut.limit(cut.position() + 1 + 16);
        return cut.slice();
    }

    /** A mutation of the fixture table, in DSE's layout, cut short in its partition header: the decoding runs out of bytes. */
    private static ByteBuffer cutMutation() throws IOException
    {
        ByteBuffer good = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65).get(0);
        ByteBuffer cut = good.duplicate();
        cut.limit(cut.position() + 1 + 16 + 8 + 8);
        return cut.slice();
    }

    /**
     * The table of the fixture of {@code version} holds what its mutations write: the rows of the same mutations, decoded
     * here and applied to a table of the same columns without the batchlog.
     */
    private void assertReplayed(DseVersion version) throws Throwable
    {
        String table = version == DseVersion.DSE_6_8_65 ? T68 : T69;
        String expected = version == DseVersion.DSE_6_8_65 ? EXPECTED68 : EXPECTED69;
        UUID expectedId = Schema.instance.getTableMetadata(KS, expected).id.asUUID();
        for (ByteBuffer dse : DseFixtures.fixtureHintMutations(version))
            retarget(dse, expectedId).apply();

        Object[][] rows = getRows(execute("SELECT * FROM " + KS + '.' + expected));
        assertTrue("rows of the fixture of " + version, rows.length > 0);
        assertRows(execute("SELECT * FROM " + KS + '.' + table), rows);

        // what the fixture data set wrote (test/data/dse-compat/README.md): the 200 inserts and the three deletions
        assertRows(execute("SELECT count(*) FROM " + KS + '.' + table), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
        assertRows(execute("SELECT l, m, n, s, tp, v FROM " + KS + '.' + table + " WHERE id = 25 AND ck = 'ck2'"),
                   row(list(25, 26, 27), map("m", 25), 5000L, 250, tuple(25, "tuple25"), "val50"));
        assertRows(execute("SELECT fu.street, u.street FROM " + KS + '.' + table + " WHERE id = 25 AND ck = 'ck2'"),
                   row("Street25", "Avenue25"));
        assertRows(execute("SELECT v FROM " + KS + '.' + table + " WHERE id = 2 AND ck = 'ck1'"), row((String) null));
        assertEmpty(execute("SELECT * FROM " + KS + '.' + table + " WHERE id = 1"));
        assertRows(execute("SELECT ck, s FROM " + KS + '.' + table + " WHERE id = 3"), row(null, 30));
    }

    /** A mutation of the fixture, decoded with DSE's layout, as a mutation of the table {@code tableId}. */
    private static Mutation retarget(ByteBuffer dse, UUID tableId) throws IOException
    {
        Mutation decoded;
        try (DataInputBuffer in = new DataInputBuffer(dse.duplicate(), true))
        {
            decoded = Mutation.serializer.deserialize(in, MessagingService.VERSION_DSE_68);
        }
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(decoded, out, MessagingService.current_version);
            byte[] bytes = out.toByteArray();
            assertEquals("partition updates of a fixture mutation", 1, bytes[0]);
            ByteBuffer.wrap(bytes, 1, 16).putLong(tableId.getMostSignificantBits()).putLong(tableId.getLeastSignificantBits());
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                return Mutation.serializer.deserialize(in, MessagingService.current_version);
            }
        }
    }
}
