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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class BatchlogManagerInterceptorTest
{
    static
    {
        // Must be set before BatchlogManagerInterceptor is initialized, as the instance field
        // is resolved once at class-init; unit tests run in their own forked JVM, so nothing
        // has touched the interface yet.
        CassandraRelevantProperties.CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS.setString(TestInterceptor.class.getName());
    }

    // Initializes the interface (freezing its instance field) right after the property is set
    // above, so no test-method ordering can first touch it while the property is cleared.
    private static final BatchlogManagerInterceptor frozenInstance = BatchlogManagerInterceptor.instance;

    private static final String KEYSPACE1 = "BatchlogManagerInterceptorTest1";
    private static final String KEYSPACE2 = "BatchlogManagerInterceptorTest2";
    private static final String CF_STANDARD1 = "Standard1";

    static PartitionerSwitcher sw;

    public static class TestInterceptor implements BatchlogManagerInterceptor
    {
        static final List<TimeUUID> observedBatchIds = new CopyOnWriteArrayList<>();
        static final List<Long> observedWrittenAt = new CopyOnWriteArrayList<>();
        static final List<List<Mutation>> observedBatches = new CopyOnWriteArrayList<>();
        static volatile boolean failCallbacks = false;

        @Override
        public void onReplayedBatch(TimeUUID batchId, long writtenAt, List<Mutation> mutations)
        {
            observedBatchIds.add(batchId);
            observedWrittenAt.add(writtenAt);
            observedBatches.add(new ArrayList<>(mutations));
            if (failCallbacks)
                throw new RuntimeException("simulated interceptor failure");
        }

        static void reset()
        {
            observedBatchIds.clear();
            observedWrittenAt.clear();
            observedBatches.clear();
            failCallbacks = false;
        }
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        sw = Util.switchPartitioner(Murmur3Partitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1, 1, BytesType.instance));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1, 1, BytesType.instance));
    }

    @AfterClass
    public static void cleanup()
    {
        sw.close();
    }

    @Before
    public void setUp() throws Exception
    {
        // start from a clean ring: some tests add a second (down) endpoint
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        InetAddressAndPort localhost = InetAddressAndPort.getByName("127.0.0.1");
        metadata.updateNormalToken(Util.token("A"), localhost);
        metadata.updateHostId(UUID.randomUUID(), localhost);
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).truncateBlocking();
        TestInterceptor.reset();
    }

    /** Stores (and flushes) a batch of 3 mutations on the given table, old enough to be replayed. */
    private static TimeUUID storeReplayableBatch(TableMetadata cfm, int key)
    {
        List<Mutation> mutations = new ArrayList<>(3);
        for (int j = 0; j < 3; j++)
        {
            mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(key))
                          .clustering("name" + j)
                          .add("val", "val" + j)
                          .build());
        }
        long timestamp = currentTimeMillis() - BatchlogManager.getBatchlogTimeout();
        TimeUUID batchId = atUnixMillis(timestamp, 0);
        BatchlogManager.store(Batch.createLocal(batchId, timestamp * 1000, mutations));
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        return batchId;
    }

    /** Registers a second, never-gossiped (hence seen as down) node owning part of the ring. */
    private static void registerDownRemoteNode() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        metadata.updateNormalToken(Util.token("B"), remote);
        metadata.updateHostId(UUID.randomUUID(), remote);
    }

    @Test
    public void testCustomInterceptorConstruction()
    {
        assertSame(TestInterceptor.class, frozenInstance.getClass());
        assertSame(TestInterceptor.class, BatchlogManagerInterceptor.instance.getClass());
        assertSame(TestInterceptor.class, BatchlogManagerInterceptor.getCustomOrDefault().getClass());
    }

    @Test
    public void testDefaultIsNoop()
    {
        String key = CassandraRelevantProperties.CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS.getKey();
        String saved = System.clearProperty(key);
        try
        {
            assertSame(BatchlogManagerInterceptor.Noop.class, BatchlogManagerInterceptor.getCustomOrDefault().getClass());
        }
        finally
        {
            CassandraRelevantProperties.CUSTOM_BATCHLOG_MANAGER_INTERCEPTOR_CLASS.setString(saved);
        }
    }

    @Test
    public void testInterceptorObservesReplayedMutations() throws Exception
    {
        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata();

        // 2 replayable batches of 3 mutations each, plus 1 batch that is not old enough to be replayed.
        Set<TimeUUID> replayableIds = new HashSet<>();
        for (int i = 0; i < 3; i++)
        {
            List<Mutation> mutations = new ArrayList<>(3);
            for (int j = 0; j < 3; j++)
            {
                mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(i))
                              .clustering("name" + j)
                              .add("val", "val" + j)
                              .build());
            }

            boolean replayable = i < 2;
            long timestamp = replayable
                           ? (currentTimeMillis() - BatchlogManager.getBatchlogTimeout())
                           : (currentTimeMillis() + BatchlogManager.getBatchlogTimeout());
            TimeUUID id = atUnixMillis(timestamp, i);
            if (replayable)
                replayableIds.add(id);
            BatchlogManager.store(Batch.createLocal(id, timestamp * 1000, mutations));
        }

        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        BatchlogManager.instance.startBatchlogReplay().get();

        // 2 batches observed, each through a single callback carrying all of its 3 mutations; the
        // not-yet-replayable batch must not be observed.
        assertEquals(2, TestInterceptor.observedBatches.size());
        assertEquals(replayableIds, new HashSet<>(TestInterceptor.observedBatchIds));
        for (int k = 0; k < TestInterceptor.observedBatches.size(); k++)
        {
            List<Mutation> batchMutations = TestInterceptor.observedBatches.get(k);
            assertEquals(3, batchMutations.size());
            for (Mutation mutation : batchMutations)
                assertEquals(KEYSPACE1, mutation.getKeyspaceName());
            assertEquals(TestInterceptor.observedBatchIds.get(k).unix(MILLISECONDS),
                         (long) TestInterceptor.observedWrittenAt.get(k));
        }

        // The interceptor runs after the mutations completed: the replayed rows are readable.
        for (int i = 0; i < 2; i++)
        {
            UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = int_as_blob(%d)", KEYSPACE1, CF_STANDARD1, i));
            assertNotNull(result);
            assertEquals(3, result.size());
        }
    }

    @Test
    public void testThrowingInterceptorForcesRetry() throws Exception
    {
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();
        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata();
        storeReplayableBatch(cfm, 1234);

        TestInterceptor.failCallbacks = true;
        BatchlogManager.instance.startBatchlogReplay().get();

        // The interceptor was invoked and threw: the batch's mutations were applied, but the
        // batch must be kept in the batchlog and not counted as replayed, so it is retried.
        assertTrue(TestInterceptor.observedBatches.size() >= 1);
        assertEquals(1, BatchlogManager.instance.countAllBatches());
        assertEquals(1, BatchlogManager.instance.countBatchesAwaitingInterceptor());
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);
        UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = int_as_blob(%d)", KEYSPACE1, CF_STANDARD1, 1234));
        assertNotNull(result);
        assertEquals(3, result.size());

        int observedBeforeRetry = TestInterceptor.observedBatches.size();
        TestInterceptor.failCallbacks = false;
        BatchlogManager.instance.startBatchlogReplay().get();

        // The retry notifies the interceptor of the batch, with its complete mutation list, again
        // (their delivery is not repeated: it completed in the first cycle), and this time the
        // batch is removed and counted as replayed.
        assertEquals(observedBeforeRetry + 1, TestInterceptor.observedBatches.size());
        assertEquals(3, TestInterceptor.observedBatches.get(TestInterceptor.observedBatches.size() - 1).size());
        assertEquals(0, BatchlogManager.instance.countAllBatches());
        assertEquals(0, BatchlogManager.instance.countBatchesAwaitingInterceptor());
        assertEquals(1, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);
    }

    /**
     * A batch awaiting its interceptor can be removed from the batchlog by another path (e.g. the
     * write path removing a batch that completed on the coordinator). The retained entry must then
     * be forgotten, without further callbacks.
     */
    @Test
    public void testBatchRemovedWhileAwaitingInterceptorIsForgotten() throws Exception
    {
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();
        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata();
        TimeUUID batchId = storeReplayableBatch(cfm, 4321);

        TestInterceptor.failCallbacks = true;
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(1, BatchlogManager.instance.countBatchesAwaitingInterceptor());

        // the batch is removed from the batchlog behind the replayer's back
        BatchlogManager.remove(batchId);

        int observedBeforeCleanup = TestInterceptor.observedBatches.size();
        TestInterceptor.failCallbacks = false;
        BatchlogManager.instance.startBatchlogReplay().get();

        // The retained entry is forgotten without invoking the interceptor again; the batch still
        // counts as replayed, as its mutations did complete in the first cycle.
        assertEquals(0, BatchlogManager.instance.countBatchesAwaitingInterceptor());
        assertEquals(observedBeforeCleanup, TestInterceptor.observedBatches.size());
        assertEquals(0, BatchlogManager.instance.countAllBatches());
        assertEquals(1, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);
    }

    /**
     * A failing interceptor must not make the node re-deliver the batch's mutations on every
     * replay cycle: in particular, hints for a down replica must be written once, not once per
     * retry cycle, or they would accumulate for as long as the interceptor keeps failing.
     */
    @Test
    public void testFailingInterceptorRetriesCallbackWithoutRewritingHints() throws Exception
    {
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();

        // KEYSPACE2 has RF=2, so the down node is a replica of every mutation and replaying the
        // batch writes one hint per mutation for it.
        registerDownRemoteNode();
        TableMetadata cfm = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1).metadata();
        storeReplayableBatch(cfm, 5678);

        long initialHints = StorageMetrics.totalHints.getCount();

        // First cycle: the mutations are applied locally and hinted to the down replica; the
        // interceptor throws, so the batch is retained.
        TestInterceptor.failCallbacks = true;
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(3, StorageMetrics.totalHints.getCount() - initialHints);
        assertEquals(1, BatchlogManager.instance.countAllBatches());
        assertEquals(1, BatchlogManager.instance.countBatchesAwaitingInterceptor());
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);
        int observedAfterFirstCycle = TestInterceptor.observedBatches.size();
        assertTrue(observedAfterFirstCycle >= 1);

        // Second cycle, interceptor still failing: only the callback is retried; the mutations are
        // not re-delivered, so no new hints are written for the down replica.
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(3, StorageMetrics.totalHints.getCount() - initialHints);
        assertEquals(1, BatchlogManager.instance.countAllBatches());
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);
        assertTrue(TestInterceptor.observedBatches.size() > observedAfterFirstCycle);

        // Third cycle, interceptor recovered: the callback completes with the batch's complete
        // mutation list, the batch is removed and counted as replayed, and still no new hints
        // were written.
        int observedBeforeSuccess = TestInterceptor.observedBatches.size();
        TestInterceptor.failCallbacks = false;
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(3, StorageMetrics.totalHints.getCount() - initialHints);
        assertEquals(observedBeforeSuccess + 1, TestInterceptor.observedBatches.size());
        assertEquals(3, TestInterceptor.observedBatches.get(TestInterceptor.observedBatches.size() - 1).size());
        assertEquals(0, BatchlogManager.instance.countAllBatches());
        assertEquals(0, BatchlogManager.instance.countBatchesAwaitingInterceptor());
        assertEquals(1, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        // The mutations were applied locally (once, in the first cycle) and are readable.
        UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = int_as_blob(%d)", KEYSPACE2, CF_STANDARD1, 5678));
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    /**
     * If a replay cycle aborts before advancing the scan lower bound, the next scan covers the rows
     * of batches awaiting their interceptor again: the scan must skip them (they are handled by the
     * callback-only retry), or their mutations would be re-delivered and their hints duplicated.
     */
    @Test
    public void testScanSkipsBatchesAwaitingInterceptor() throws Exception
    {
        registerDownRemoteNode();
        TableMetadata cfm = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1).metadata();
        storeReplayableBatch(cfm, 9012);

        long initialHints = StorageMetrics.totalHints.getCount();
        TestInterceptor.failCallbacks = true;
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(3, StorageMetrics.totalHints.getCount() - initialHints);
        assertEquals(1, BatchlogManager.instance.countBatchesAwaitingInterceptor());

        // Simulate a cycle that aborted before advancing the scan bound, so that the next scan
        // covers the retained batch's row again: the batch must not be replayed a second time.
        BatchlogManager.instance.resetLastReplayedUuid();
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(3, StorageMetrics.totalHints.getCount() - initialHints);
        assertEquals(1, BatchlogManager.instance.countAllBatches());
        assertEquals(1, BatchlogManager.instance.countBatchesAwaitingInterceptor());

        // Once the interceptor recovers, the batch is removed, still without new hints.
        TestInterceptor.failCallbacks = false;
        BatchlogManager.instance.startBatchlogReplay().get();
        assertEquals(3, StorageMetrics.totalHints.getCount() - initialHints);
        assertEquals(0, BatchlogManager.instance.countAllBatches());
        assertEquals(0, BatchlogManager.instance.countBatchesAwaitingInterceptor());
    }
}
