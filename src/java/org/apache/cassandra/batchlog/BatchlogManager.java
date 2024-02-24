/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.*;
import java.util.concurrent.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteOptions;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.UUIDGen;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;

public class BatchlogManager implements BatchlogManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 10 * 1000; // milliseconds
    static final int DEFAULT_PAGE_SIZE = 128;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final long BATCHLOG_REPLAY_TIMEOUT = Long.getLong("cassandra.batchlog.replay_timeout_in_ms", DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS) * 2);

    // number of replicas required to store batchlog for atomicity, at most 2
    private static final int REQUIRED_BATCHLOG_REPLICA_COUNT = Math.min(2, Integer.getInteger("dse.batchlog.required_replica_count", 2));

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile UUID lastReplayedUuid = UUIDGen.minTimeUUID(0);

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorService batchlogTasks;

    private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

    public BatchlogManager()
    {
        ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        batchlogTasks = executor;
    }

    public void start()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                             StorageService.RING_DELAY_MILLIS,
                                             REPLAY_INTERVAL,
                                             MILLISECONDS);
    }

    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, batchlogTasks);
    }

    public static void remove(UUID id)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches,
                                                         UUIDType.instance.decompose(id),
                                                         FBUtilities.timestampMicros(),
                                                         FBUtilities.nowInSeconds()))
            .apply();
    }

    public static void store(Batch batch)
    {
        /**
         * by default writes are durable, see
         * {@link org.apache.cassandra.schema.KeyspaceParams#DEFAULT_DURABLE_WRITES}
         */
        store(batch, WriteOptions.DEFAULT);
    }

    public static void store(Batch batch, WriteOptions writeOptions)
    {
        List<ByteBuffer> mutations = new ArrayList<>(batch.encodedMutations.size() + batch.decodedMutations.size());
        mutations.addAll(batch.encodedMutations);

        for (Mutation mutation : batch.decodedMutations)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(mutation, buffer, MessagingService.current_version);
                mutations.add(buffer.buffer());
            }
            catch (IOException e)
            {
                // shouldn't happen
                throw new AssertionError(e);
            }
        }

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(SystemKeyspace.Batches, batch.id);
        builder.row()
               .timestamp(batch.creationTime)
               .add("version", MessagingService.current_version)
               .appendAll("mutations", mutations);

        builder.buildAsMutation().apply(writeOptions);
    }

    @VisibleForTesting
    public int countAllBatches()
    {
        String query = String.format("SELECT count(*) FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES);
        UntypedResultSet results = executeInternal(query);
        if (results == null || results.isEmpty())
            return 0;

        return (int) results.one().getLong("count");
    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed;
    }

    public void forceBatchlogReplay() throws Exception
    {
        startBatchlogReplay().get();
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(this::replayFailedBatches);
    }

    void performInitialReplay() throws InterruptedException, ExecutionException
    {
        // Invokes initial replay. Used for testing only.
        batchlogTasks.submit(this::replayFailedBatches).get();
    }

    private void replayFailedBatches()
    {
        logger.trace("Started replayFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount <= 0)
        {
            logger.trace("Replay cancelled as there are no peers in the ring.");
            return;
        }
        setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKB());

        UUID limitUuid = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());
        ColumnFamilyStore store = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES);
        int pageSize = calculatePageSize(store);
        // There cannot be any live content where token(id) <= token(lastReplayedUuid) as every processed batch is
        // deleted, but the tombstoned content may still be present in the tables. To avoid walking over it we specify
        // token(id) > token(lastReplayedUuid) as part of the query.
        String query = String.format("SELECT id, mutations, version FROM %s.%s WHERE token(id) > token(?) AND token(id) <= token(?)",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BATCHES);
        UntypedResultSet batches = executeInternalWithPaging(query, PageSize.inRows(pageSize), lastReplayedUuid, limitUuid);
        processBatchlogEntries(batches, pageSize, rateLimiter);
        lastReplayedUuid = limitUuid;
        logger.trace("Finished replayFailedBatches");
    }

    /**
     * Sets the rate for the current rate limiter. When {@code throttleInKB} is 0, this sets the rate to
     * {@link Double#MAX_VALUE} bytes per second.
     *
     * @param throttleInKB throughput to set in KB per second
     */
    public void setRate(final int throttleInKB)
    {
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount > 0)
        {
            int endpointThrottleInKB = throttleInKB / endpointsCount;
            double throughput = endpointThrottleInKB == 0 ? Double.MAX_VALUE : endpointThrottleInKB * 1024.0;
            if (rateLimiter.getRate() != throughput)
            {
                logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", throttleInKB, endpointThrottleInKB);
                rateLimiter.setRate(throughput);
            }
        }
    }

    // read less rows (batches) per page if they are very large
    static int calculatePageSize(ColumnFamilyStore store)
    {
        double averageRowSize = store.getMeanPartitionSize();
        if (averageRowSize <= 0)
            return DEFAULT_PAGE_SIZE;

        return (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, 4 * 1024 * 1024 / averageRowSize));
    }

    private void processBatchlogEntries(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter)
    {
        int positionInPage = 0;
        ArrayList<ReplayingBatch> unfinishedBatches = new ArrayList<>(pageSize);

        Set<UUID> hintedNodes = new HashSet<>();
        Set<UUID> replayedBatches = new HashSet<>();
        Exception caughtException = null;
        int skipped = 0;

        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
        for (UntypedResultSet.Row row : batches)
        {
            UUID id = row.getUUID("id");
            int version = row.getInt("version");
            try
            {
                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
                if (batch.replay(rateLimiter, hintedNodes) > 0)
                {
                    unfinishedBatches.add(batch);
                }
                else
                {
                    remove(id); // no write mutations were sent (either expired or all CFs involved truncated).
                    ++totalBatchesReplayed;
                }
            }
            catch (IOException e)
            {
                logger.warn("Skipped batch replay of {} due to {}", id, e.getMessage());
                caughtException = e;
                remove(id);
                ++skipped;
            }

            if (++positionInPage == pageSize)
            {
                // We have reached the end of a batch. To avoid keeping more than a page of mutations in memory,
                // finish processing the page before requesting the next row.
                finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
                positionInPage = 0;
            }
        }

        // finalize the incomplete last page of batches
        if (positionInPage > 0)
            finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);

        if (caughtException != null)
            logger.warn(String.format("Encountered %d unexpected exceptions while sending out batches", skipped), caughtException);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        HintsService.instance.flushAndFsyncBlockingly(hintedNodes);

        // once all generated hints are fsynced, actually delete the batches
        replayedBatches.forEach(BatchlogManager::remove);
    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<UUID> hintedNodes, Set<UUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish(hintedNodes);
            replayedBatches.add(batch.id);
        }

        totalBatchesReplayed += batches.size();
        batches.clear();
    }

    public static long getBatchlogTimeout()
    {
        return BATCHLOG_REPLAY_TIMEOUT; // enough time for the actual write + BM removal mutation
    }

    private static class ReplayingBatch
    {
        private final UUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private final int replayedBytes;

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers;

        ReplayingBatch(UUID id, int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.mutations = new ArrayList<>(serializedMutations.size());
            this.replayedBytes = addMutations(version, serializedMutations);
        }

        public int replay(RateLimiter rateLimiter, Set<UUID> hintedNodes) throws IOException
        {
            logger.trace("Replaying batch {}", id);

            if (mutations.isEmpty())
                return 0;

            int gcgs = gcgs(mutations);
            if (MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return 0;

            replayHandlers = sendReplays(mutations, writtenAt, hintedNodes);

            rateLimiter.acquire(replayedBytes); // acquire afterwards, to not mess up ttl calculation.

            return replayHandlers.size();
        }

        public void finish(Set<UUID> hintedNodes)
        {
            for (int i = 0; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException|WriteFailureException e)
                {
                    logger.trace("Failed replaying a batched mutation to a node, will write a hint");
                    logger.trace("Failure was : {}", e.getMessage());
                    // writing hints for the rest to hints, starting from i
                    writeHintsForUndeliveredEndpoints(i, hintedNodes);
                    return;
                }
            }
        }

        private int addMutations(int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            int ret = 0;
            for (ByteBuffer serializedMutation : serializedMutations)
            {
                ret += serializedMutation.remaining();
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    addMutation(Mutation.serializer.deserialize(in, version));
                }
            }

            return ret;
        }

        // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
        // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
        // truncated.
        private void addMutation(Mutation mutation)
        {
            for (TableId tableId : mutation.getTableIds())
                if (writtenAt <= Nodes.local().getTruncatedAt(tableId))
                    mutation = mutation.without(tableId);

            if (!mutation.isEmpty())
                mutations.add(mutation);
        }

        private void writeHintsForUndeliveredEndpoints(int startFrom, Set<UUID> hintedNodes)
        {
            int gcgs = gcgs(mutations);

            // expired
            if (MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            Set<UUID> nodesToHint = new HashSet<>();
            for (int i = startFrom; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                Mutation undeliveredMutation = mutations.get(i);

                if (handler != null)
                {
                    for (InetAddressAndPort address : handler.undelivered)
                    {
                        UUID hostId = StorageService.instance.getHostIdForEndpoint(address);
                        if (null != hostId)
                            nodesToHint.add(hostId);
                    }
                    if (!nodesToHint.isEmpty())
                        HintsService.instance.write(nodesToHint, Hint.create(undeliveredMutation, writtenAt));
                    hintedNodes.addAll(nodesToHint);
                    nodesToHint.clear();
                }
            }
        }

        private static List<ReplayWriteResponseHandler<Mutation>> sendReplays(List<Mutation> mutations,
                                                                              long writtenAt,
                                                                              Set<UUID> hintedNodes)
        {
            List<ReplayWriteResponseHandler<Mutation>> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteResponseHandler<Mutation> handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
                handlers.add(handler);
            }
            return handlers;
        }

        /**
         * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
         * when a replica is down or a write request times out.
         *
         * @return direct delivery handler to wait on
         */
        private static ReplayWriteResponseHandler<Mutation> sendSingleReplayMutation(final Mutation mutation,
                                                                                     long writtenAt,
                                                                                     Set<UUID> hintedNodes)
        {
            String ks = mutation.getKeyspaceName();
            Keyspace keyspace = Keyspace.open(ks);
            Token tk = mutation.key().getToken();

            // TODO: this logic could do with revisiting at some point, as it is unclear what its rationale is
            // we perform a local write, ignoring errors and inline in this thread (potentially slowing replay down)
            // effectively bumping CL for locally owned writes and also potentially stalling log replay if an error occurs
            // once we decide how it should work, it can also probably be simplified, and avoid constructing a ReplicaPlan directly
            ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(keyspace, tk);
            Replicas.temporaryAssertFull(liveAndDown.all()); // TODO in CASSANDRA-14549

            Replica selfReplica = liveAndDown.all().selfIfPresent();
            if (selfReplica != null)
                mutation.apply(WriteOptions.FOR_BATCH_REPLAY);

            ReplicaLayout.ForTokenWrite liveRemoteOnly = liveAndDown.filter(
                    r -> IFailureDetector.isReplicaAlive.test(r) && r != selfReplica);

            for (Replica replica : liveAndDown.all())
            {
                if (replica == selfReplica || liveRemoteOnly.all().contains(replica))
                    continue;

                UUID hostId = StorageService.instance.getHostIdForEndpoint(replica.endpoint());
                if (null != hostId)
                {
                    HintsService.instance.write(hostId, Hint.create(mutation, writtenAt));
                    hintedNodes.add(hostId);
                }
            }

            ReplicaPlan.ForTokenWrite replicaPlan = new ReplicaPlan.ForTokenWrite(keyspace, liveAndDown.replicationStrategy(),
                    ConsistencyLevel.ONE, liveRemoteOnly.pending(), liveRemoteOnly.all(), liveRemoteOnly.all(), liveRemoteOnly.all());
            ReplayWriteResponseHandler<Mutation> handler = new ReplayWriteResponseHandler<>(replicaPlan, System.nanoTime());
            Message<Mutation> message = Message.outWithFlag(MUTATION_REQ, mutation, MessageFlag.CALL_BACK_ON_FAILURE);
            for (Replica replica : liveRemoteOnly.all())
                MessagingService.instance().sendWriteWithCallback(message, replica, handler, false);
            return handler;
        }

        private static int gcgs(Collection<Mutation> mutations)
        {
            int gcgs = Integer.MAX_VALUE;
            for (Mutation mutation : mutations)
                gcgs = Math.min(gcgs, mutation.smallestGCGS());
            return gcgs;
        }

        /**
         * A wrapper of WriteResponseHandler that stores the addresses of the endpoints from
         * which we did not receive a successful response.
         */
        private static class ReplayWriteResponseHandler<T> extends WriteResponseHandler<T>
        {
            private final Set<InetAddressAndPort> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<>());

            ReplayWriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan, long queryStartNanoTime)
            {
                super(replicaPlan, null, WriteType.UNLOGGED_BATCH, queryStartNanoTime);
                Iterables.addAll(undelivered, replicaPlan.contacts().endpoints());
            }

            @Override
            public int blockFor()
            {
                return this.replicaPlan.contacts().size();
            }

            @Override
            public void onResponse(Message<T> m)
            {
                boolean removed = undelivered.remove(m == null ? FBUtilities.getBroadcastAddressAndPort() : m.from());
                assert removed;
                super.onResponse(m);
            }
        }
    }

    /**
     * There are three strategies to select the "best" batchlog store candidates. See the descriptions
     * of the strategies in {@link Config.BatchlogEndpointStrategy}.
     *
     *
     * @param consistencyLevel
     * @param localRack the name of the local rack
     * @param localDcEndpoints the endpoints as a multimap of rack to endpoints.
     *                  Implementation note: this one comes directly from
     *                  {@link TokenMetadata.Topology#getDatacenterRacks() TokenMetadata.Topology.getDatacenterRacks().get(LOCAL_DC)}.
     * @return list of endpoints to store the batchlog
     */
    public static Collection<InetAddressAndPort> filterEndpoints(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddressAndPort> localDcEndpoints)
    {
        EndpointFilter filter = endpointFilter(consistencyLevel, localRack, localDcEndpoints);
        return filter.filter();
    }

    @VisibleForTesting
    static EndpointFilter endpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddressAndPort> localDcEndpoints)
    {
        return DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch
                && DatabaseDescriptor.isDynamicEndpointSnitch()
                ? new DynamicEndpointFilter(consistencyLevel, localRack, localDcEndpoints)
                : new RandomEndpointFilter(consistencyLevel, localRack, localDcEndpoints);
    }

    /**
     * Picks endpoints for batchlög storage according to {@link org.apache.cassandra.locator.DynamicEndpointSnitch}.
     *
     * Unlike the default ({@link RandomEndpointFilter}) implementation, this one allows batchlog replicas in
     * the local rack.
     *
     * The implementation picks the fastest endpoint of one rack, the 2nd fastest node from another rack,
     * until {@link #REQUIRED_BATCHLOG_REPLICA_COUNT} endpoints have been collected. Endpoints in the same
     * rack are avoided.
     *
     * It can either try to prevent the local rack {@link Config.BatchlogEndpointStrategy#dynamic_remote}
     * or not ({@link Config.BatchlogEndpointStrategy#dynamic}).
     *
     * The tradeoff here is performance over availability (local rack is eligible for batchlog storage).
     */
    public static class DynamicEndpointFilter extends EndpointFilter
    {
        DynamicEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddressAndPort> endpoints)
        {
            super(consistencyLevel, localRack, endpoints);
        }

        public Collection<InetAddressAndPort> filter()
        {
            Collection<InetAddressAndPort> allEndpoints = endpoints.values();
            int endpointCount = allEndpoints.size();
            if (endpointCount <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return checkFewEndpoints(allEndpoints, endpointCount);

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddressAndPort> validated = validatedNodes(endpointCount);

            // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
            // This step is mandatory _before_ we filter the local rack.
            int numValidated = validated.size();
            if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return notEnoughAvailableEndpoints(validated);

            if (!DatabaseDescriptor.getBatchlogEndpointStrategy().allowLocalRack)
            {
                filterLocalRack(validated);

                // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
                // This is a mandatory step - otherwise the for-loop below could loop forever, if
                // REQUIRED_BATCHLOG_REPLICA_COUNT can't be achieved.
                numValidated = validated.size();
                if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                    return notEnoughAvailableEndpoints(validated);
            }

            // sort _all_ nodes to pick the best racks
            List<InetAddressAndPort> sorted = reorder(validated.values());

            List<InetAddressAndPort> result = new ArrayList<>(REQUIRED_BATCHLOG_REPLICA_COUNT);
            Set<String> racks = new HashSet<>();

            while (result.size() < REQUIRED_BATCHLOG_REPLICA_COUNT)
            {
                for (InetAddressAndPort endpoint : sorted)
                {
                    if (result.size() == REQUIRED_BATCHLOG_REPLICA_COUNT)
                        break;

                    if (racks.isEmpty())
                        racks.addAll(validated.keySet());

                    String rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);
                    if (!racks.remove(rack))
                        continue;
                    if (result.contains(endpoint))
                        continue;

                    result.add(endpoint);
                }
            }

            return result;
        }

        List<InetAddressAndPort> reorder(Collection<InetAddressAndPort> endpoints)
        {
            EndpointsForRange endpointsForRange = SystemReplicas.getSystemReplicas(endpoints);
            Endpoints sorted = DatabaseDescriptor.getEndpointSnitch().sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), endpointsForRange);
            return sorted.endpointList();
        }
    }

    /**
     * This is the default endpoint-filter implementation for logged batches.
     *
     * It picks random endpoints from random racks. Endpoints from non-local racks are preferred.
     * Also prefers to pick endpoints from as many different racks as possible.
     *
     * The tradeoff here is performance over availability (local rack is eligible for batchlog storage).
     */
    public static class RandomEndpointFilter extends EndpointFilter
    {
        RandomEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddressAndPort> endpoints)
        {
            super(consistencyLevel, localRack, endpoints);
        }

        public Collection<InetAddressAndPort> filter()
        {
            Collection<InetAddressAndPort> allEndpoints = endpoints.values();
            int endpointCount = allEndpoints.size();
            if (endpointCount <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return checkFewEndpoints(allEndpoints, endpointCount);

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddressAndPort> validated = validatedNodes(endpointCount);

            // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
            // This step is mandatory _before_ we filter the local rack.
            int numValidated = validated.size();
            if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                return notEnoughAvailableEndpoints(validated);

            if (!DatabaseDescriptor.getBatchlogEndpointStrategy().allowLocalRack)
            {
                filterLocalRack(validated);

                // Return all validated endpoints, if we cannot achieve REQUIRED_BATCHLOG_REPLICA_COUNT.
                // This is a mandatory step - otherwise the for-loop below could loop forever, if
                // REQUIRED_BATCHLOG_REPLICA_COUNT can't be achieved.
                numValidated = validated.size();
                if (numValidated <= REQUIRED_BATCHLOG_REPLICA_COUNT)
                    return notEnoughAvailableEndpoints(validated);
            }

            // Randomize the racks - all we need is the collections of the endpoints per rack.
            List<Collection<InetAddressAndPort>> rackNodes = new ArrayList<>(validated.asMap().values());
            Collections.shuffle(rackNodes, ThreadLocalRandom.current());

            // Now iterate over the racks (one after each other) and randomly pick one node
            // from each rack. Repeat that until we've reached REQUIRED_BATCHLOG_REPLICA_COUNT.
            List<InetAddressAndPort> result = new ArrayList<>(REQUIRED_BATCHLOG_REPLICA_COUNT);
            for (int i = 0; result.size() < REQUIRED_BATCHLOG_REPLICA_COUNT; )
            {
                // cast to List is safe in this case, because it's an ArrayListMultimap
                List<InetAddressAndPort> singleRack = (List) rackNodes.get(i);
                InetAddressAndPort endpoint = singleRack.get(ThreadLocalRandom.current().nextInt(singleRack.size()));
                if (result.contains(endpoint))
                    continue;

                result.add(endpoint);

                i++;
                if (i == rackNodes.size())
                    i = 0;
            }

            return result;
        }
    }

    public static abstract class EndpointFilter
    {
        final ConsistencyLevel consistencyLevel;
        final String localRack;
        final Multimap<String, InetAddressAndPort> endpoints;

        @VisibleForTesting
        EndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddressAndPort> endpoints)
        {
            this.consistencyLevel = batchlogConsistencyLevel(consistencyLevel);
            this.localRack = localRack;
            this.endpoints = endpoints;
        }

        static ConsistencyLevel batchlogConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return ConsistencyLevel.ANY;
            return REQUIRED_BATCHLOG_REPLICA_COUNT == 2 ? ConsistencyLevel.TWO : ConsistencyLevel.ONE;
        }

        /**
         * Generate the list of endpoints for batchlog hosting. If possible these will be
         * {@code REQUIRED_BATCHLOG_REPLICA_COUNT} nodes from different racks.
         */
        public abstract Collection<InetAddressAndPort> filter();

        ListMultimap<String, InetAddressAndPort> validatedNodes(int endpointCount)
        {
            int rackCount = endpoints.keySet().size();

            ListMultimap<String, InetAddressAndPort> validated = ArrayListMultimap.create(rackCount, endpointCount / rackCount);
            for (Map.Entry<String, InetAddressAndPort> entry : endpoints.entries())
                if (isValid(entry.getValue()))
                    validated.put(entry.getKey(), entry.getValue());

            return validated;
        }

        void filterLocalRack(ListMultimap<String, InetAddressAndPort> validated)
        {
            // If there are at least REQUIRED_BATCHLOG_REPLICA_COUNT nodes in _other_ racks,
            // then exclude the local rack
            if (validated.size() - validated.get(localRack).size() >= REQUIRED_BATCHLOG_REPLICA_COUNT)
                validated.removeAll(localRack);
        }

        /**
         * Called when there are not enough endpoints <em>commissioned</em>
         * to achieve {@link #REQUIRED_BATCHLOG_REPLICA_COUNT}.
         */
        Collection<InetAddressAndPort> checkFewEndpoints(Collection<InetAddressAndPort> allEndpoints, int totalEndpointCount)
        {
            int available = 0;
            for (InetAddressAndPort ep : allEndpoints)
                if (isAlive(ep))
                    available++;

            allEndpoints = maybeThrowUnavailableException(allEndpoints, totalEndpointCount, available);

            return allEndpoints;
        }

        /**
         * Called when there are not enough commissioned endpoints <em>available</em>
         * to achieve {@link #REQUIRED_BATCHLOG_REPLICA_COUNT}.
         */
        Collection<InetAddressAndPort> notEnoughAvailableEndpoints(ListMultimap<String, InetAddressAndPort> validated)
        {
            Collection<InetAddressAndPort> validatedEndpoints = validated.values();
            int validatedCount = validatedEndpoints.size();

            validatedEndpoints = maybeThrowUnavailableException(validatedEndpoints, REQUIRED_BATCHLOG_REPLICA_COUNT, validatedCount);

            return validatedEndpoints;
        }

        Collection<InetAddressAndPort> maybeThrowUnavailableException(Collection<InetAddressAndPort> endpoints,
                                                                      int totalEndpointCount,
                                                                      int available)
        {
            // This is exactly what the pre DB-1367 code does (one exception):
            // - If there are no available nodes AND batchlog-consistency == ANY, then use
            //   the local node as the batchlog endpoint.
            // - If there are no available nodes AND batchlog-consistency, then ...
            //   return *NO* batchlog endpoints, causing the batchlog write to timeout
            //   (waiting for noting - nothing can trigger the condition in the write handler).
            //   Changed to immediately throw an UnavailableException.
            // - If there are less batchlog endpoint candidates available than required,
            //   just use those (despite the "usual" guarantee of two batchlog-replicas).
            // - The batchlog-consistency is not respected (beside the CL ANY check
            //   if there are no available endpoints).
            // - Batchlog endpoints are always chosen from the local DC. The exact racks
            //   and endpoints depend on the strategy. The "random_remote" strategy is what's
            //   been in since forever.

            if (available == 0)
            {
                if (consistencyLevel == ConsistencyLevel.ANY)
                    return Collections.singleton(FBUtilities.getBroadcastAddressAndPort());

                // New/changed since DB-1367: we immediately throw an UnavailableException here instead
                // of letting the batchlog write unnecessarily timeout.
                throw new UnavailableException("Cannot achieve consistency level " + consistencyLevel
                                               + " for batchlog in local DC, required:" + totalEndpointCount
                                               + ", available:" + available,
                                               consistencyLevel,
                                               totalEndpointCount,
                                               available);
            }

            return endpoints;
        }

        @VisibleForTesting
        protected boolean isValid(InetAddressAndPort input)
        {
            return !input.equals(getCoordinator()) && isAlive(input);
        }

        @VisibleForTesting
        protected boolean isAlive(InetAddressAndPort input)
        {
            return FailureDetector.instance.isAlive(input);
        }

        @VisibleForTesting
        protected InetAddressAndPort getCoordinator()
        {
            return FBUtilities.getBroadcastAddressAndPort();
        }
    }
}
