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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.BATCHLOG_REPLAY_TIMEOUT_IN_MS;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class BatchlogManager implements BatchlogManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 10 * 1000; // milliseconds
    static final int DEFAULT_PAGE_SIZE = 128;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final long BATCHLOG_REPLAY_TIMEOUT = BATCHLOG_REPLAY_TIMEOUT_IN_MS.getLong(DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS) * 2);

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile TimeUUID lastReplayedUuid = TimeUUID.minAtUnixMillis(0);

    // Batches whose mutations have completed (applied locally, acknowledged by remote replicas, or
    // hinted) in an earlier replay cycle but whose interceptor callback failed, mapped to the hosts
    // hinted for them. On later cycles only the callback is retried, via a point read of the batch:
    // the mutations are not re-sent and no new hints are written for them, and the hinted hosts are
    // fsynced again before the batch is finally deleted. Only accessed by the replay thread. Lost
    // on restart, in which case the batch (whose row sits behind lastReplayedUuid, itself reset by
    // the restart) is replayed from scratch, which the at-least-once contract already tolerates.
    private final Map<TimeUUID, Set<UUID>> batchesAwaitingInterceptor = new HashMap<>();

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorPlus batchlogTasks;

    private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

    public BatchlogManager()
    {
        batchlogTasks = executorFactory().scheduled(false, "BatchlogTasks");
    }

    public void start()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        // Resolve the interceptor eagerly: a misconfigured custom class fails the node at
        // startup rather than at the first replay, and operators can see what is installed.
        BatchlogManagerInterceptor interceptor = BatchlogManagerInterceptor.instance;
        logger.info("Batchlog interceptor: {}", interceptor.getClass().getName());

        batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                             StorageService.RING_DELAY_MILLIS,
                                             REPLAY_INTERVAL,
                                             MILLISECONDS);
    }

    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, batchlogTasks);
    }

    public static void remove(TimeUUID id)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches,
                                                         id.toBytes(),
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
        int storageVersion = StorageCompatibilityMode.current().storageMessagingVersion();
        List<ByteBuffer> mutations = new ArrayList<>(batch.encodedMutations.size() + batch.decodedMutations.size());
        mutations.addAll(batch.encodedMutations);

        for (Mutation mutation : batch.decodedMutations)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(mutation, buffer, storageVersion);
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
               .add("version", storageVersion)
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

    @VisibleForTesting
    int countBatchesAwaitingInterceptor()
    {
        return batchesAwaitingInterceptor.size();
    }

    /**
     * Simulates the effect of a replay cycle that aborted before advancing the scan lower bound:
     * the next scan covers already-processed rows again, including those of batches awaiting their
     * interceptor.
     */
    @VisibleForTesting
    void resetLastReplayedUuid()
    {
        lastReplayedUuid = TimeUUID.minAtUnixMillis(0);
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
        setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKiB());

        TimeUUID limitUuid = TimeUUID.maxAtUnixMillis(currentTimeMillis() - getBatchlogTimeout());
        ColumnFamilyStore store = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES);
        int pageSize = calculatePageSize(store);
        // The only live content where token(id) <= token(lastReplayedUuid) are batches retained because
        // their interceptor callback failed, and those are retried through point reads, not through this
        // scan; every other processed batch is deleted, but the tombstoned content may still be present
        // in the tables. To avoid walking over it we specify token(id) > token(lastReplayedUuid) as part
        // of the query.
        String query = String.format("SELECT id, mutations, version FROM %s.%s WHERE token(id) > token(?) AND token(id) <= token(?)",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BATCHES);
        UntypedResultSet batches = executeInternalWithPaging(query, PageSize.inRows(pageSize), lastReplayedUuid, limitUuid);
        processBatchlogEntries(batches, pageSize, rateLimiter);
        lastReplayedUuid = limitUuid;
        if (!batchesAwaitingInterceptor.isEmpty())
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "{} batches whose mutations were replayed are retained in the batchlog awaiting successful interceptor callbacks",
                             batchesAwaitingInterceptor.size());
        logger.trace("Finished replayFailedBatches");
    }

    /**
     * Sets the rate for the current rate limiter. When {@code throttleInKB} is 0, this sets the rate to
     * {@link Double#MAX_VALUE} bytes per second.
     *
     * @param throttleInKB throughput to set in KiB per second
     */
    public void setRate(final int throttleInKB)
    {
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount > 0)
        {
            int endpointThrottleInKiB = throttleInKB / endpointsCount;
            double throughput = endpointThrottleInKiB == 0 ? Double.MAX_VALUE : endpointThrottleInKiB * 1024.0;
            if (rateLimiter.getRate() != throughput)
            {
                logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", throttleInKB, endpointThrottleInKiB);
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
        Set<TimeUUID> replayedBatches = new HashSet<>();
        Exception caughtException = null;
        int skipped = 0;

        // Snapshot before the scan: only batches memoized by EARLIER cycles get their callbacks
        // retried in this cycle. A callback failing during this cycle's scan is retried on the
        // next cycle, per the interceptor contract, not immediately.
        List<Map.Entry<TimeUUID, Set<UUID>>> awaitingInterceptorRetry = new ArrayList<>(batchesAwaitingInterceptor.entrySet());

        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
        for (UntypedResultSet.Row row : batches)
        {
            TimeUUID id = row.getTimeUUID("id");
            int version = row.getInt("version");

            // If a cycle aborts before advancing lastReplayedUuid, the next scan still covers the
            // batches it left awaiting their interceptor; those are handled by
            // retryBatchesAwaitingInterceptor later in this cycle, so don't replay them here.
            if (batchesAwaitingInterceptor.containsKey(id))
                continue;

            try
            {
                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
                if (batch.replay(rateLimiter) > 0)
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

        // Retry the callbacks of batches retained by earlier cycles. This runs after the scan, so
        // replay of new content is not starved by retries, and before the hint fsync barrier below,
        // which the deferred deletion of the retried batches must share.
        retryBatchesAwaitingInterceptor(awaitingInterceptorRetry, rateLimiter, hintedNodes, replayedBatches);

        if (caughtException != null)
            logger.warn(String.format("Encountered %d unexpected exceptions while sending out batches", skipped), caughtException);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        HintsService.instance.flushAndFsyncBlockingly(hintedNodes);

        // Once all generated hints are fsynced, actually delete the batches. A batch awaiting its
        // interceptor is forgotten right when its row is deleted (and only counts as replayed now):
        // had the cycle aborted before its deletion, the batch must go through the callback-only
        // retry again — not through a full (mutation re-sending, hint re-writing) replay, and
        // without being counted twice.
        for (TimeUUID id : replayedBatches)
        {
            remove(id);
            if (batchesAwaitingInterceptor.remove(id) != null)
                ++totalBatchesReplayed;
        }
    }

    /**
     * Retries the interceptor callbacks of batches whose mutations completed in an earlier cycle.
     * Batches are looked up individually — their rows sit behind {@code lastReplayedUuid}, outside
     * the ranges scanned by {@link #replayFailedBatches} — and their mutations are not re-delivered:
     * on callback success the batch joins {@code replayedBatches}, its previously hinted hosts join
     * {@code hintedNodes} so the hints are fsynced (again) before the caller deletes the batch, and
     * the memoized entry itself is dropped (and the batch counted as replayed) by the caller once
     * the deletion has happened. Re-reads are throttled by the replay rate limiter.
     */
    private void retryBatchesAwaitingInterceptor(List<Map.Entry<TimeUUID, Set<UUID>>> awaitingRetry,
                                                 RateLimiter rateLimiter,
                                                 Set<UUID> hintedNodes,
                                                 Set<TimeUUID> replayedBatches)
    {
        if (awaitingRetry.isEmpty())
            return;

        String query = String.format("SELECT id, mutations, version FROM %s.%s WHERE id = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BATCHES);
        for (Map.Entry<TimeUUID, Set<UUID>> entry : awaitingRetry)
        {
            TimeUUID id = entry.getKey();
            try
            {
                UntypedResultSet result = executeInternal(query, id);
                if (result == null || result.isEmpty())
                {
                    // The batch was removed from the batchlog by another path (e.g. its coordinator
                    // completing the batch): there is no row left to retry the callbacks from.
                    logger.warn("Batch {} was removed from the batchlog before its interceptor callbacks completed; dropping them", id);
                    batchesAwaitingInterceptor.remove(id);
                    ++totalBatchesReplayed;
                    continue;
                }

                UntypedResultSet.Row row = result.one();
                ReplayingBatch batch = new ReplayingBatch(id, row.getInt("version"), row.getList("mutations", BytesType.instance));
                rateLimiter.acquire(batch.replayedBytes);
                if (tryNotifyInterceptor(batch))
                {
                    hintedNodes.addAll(entry.getValue());
                    replayedBatches.add(id);
                }
            }
            catch (IOException e)
            {
                logger.error("Batch {} was replayed but can no longer be deserialized; its remaining interceptor callbacks are dropped", id, e);
                // let the deletion share the hint fsync barrier below, like any other replayed batch
                hintedNodes.addAll(entry.getValue());
                replayedBatches.add(id);
            }
            catch (Throwable t)
            {
                // Failing to read one batch back (e.g. a transient local read error) must wedge
                // neither the other retries nor the scan of new batchlog content: keep the entry
                // and let a later cycle retry the read. Deliberately, an error we cannot classify
                // is never grounds for deleting the batch (a corrupt row that keeps failing with
                // a RuntimeException is retried, and warned about, indefinitely).
                JVMStabilityInspector.inspectThrowable(t);
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                 "Failed to read batch {} back for its interceptor retry; it will be retried", id, t);
            }
        }
    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<UUID> hintedNodes, Set<TimeUUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish();
            hintedNodes.addAll(batch.hintedNodes());
            if (tryNotifyInterceptor(batch))
            {
                replayedBatches.add(batch.id);
                ++totalBatchesReplayed;
            }
            else
            {
                // Don't mark the batch replayed: its batchlog row survives. Its mutations have
                // completed though, so remember that (along with the hosts hinted for it) and let
                // later cycles retry the callback alone instead of replaying the whole batch
                // (and writing another round of hints).
                batchesAwaitingInterceptor.put(batch.id, batch.hintedNodes());
            }
        }

        batches.clear();
    }

    /**
     * Invokes {@link BatchlogManagerInterceptor#instance} with the batch and all of its mutations.
     *
     * @return true if the callback completed, false if the interceptor threw and the batch must be
     *         retained in the batchlog so the callback is retried on the next replay cycle
     */
    private static boolean tryNotifyInterceptor(ReplayingBatch batch)
    {
        try
        {
            batch.notifyInterceptor();
            return true;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "Batchlog interceptor failed for batch {}; the callback will be retried", batch.id, t);
            return false;
        }
    }

    public static long getBatchlogTimeout()
    {
        return BATCHLOG_REPLAY_TIMEOUT; // enough time for the actual write + BM removal mutation
    }

    private static class ReplayingBatch
    {
        private final TimeUUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private final int replayedBytes;
        private final Set<UUID> hintedNodes = new HashSet<>();

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers;

        ReplayingBatch(TimeUUID id, int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            this.id = id;
            this.writtenAt = id.unix(MILLISECONDS);
            this.mutations = new ArrayList<>(serializedMutations.size());
            this.replayedBytes = addMutations(version, serializedMutations);
        }

        /**
         * @return the hosts hinted while replaying this batch, both for replicas that were down when
         *         the mutations were sent and for replicas that did not acknowledge them in time
         */
        Set<UUID> hintedNodes()
        {
            return hintedNodes;
        }

        public int replay(RateLimiter rateLimiter) throws IOException
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

        public void finish()
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
                    writeHintsForUndeliveredEndpoints(i);
                    return;
                }
            }
        }

        /**
         * Notifies {@link BatchlogManagerInterceptor#instance} of the replayed batch, with all of
         * its mutations. Exceptions propagate to the caller, which keeps the batch in the batchlog
         * so its recovery is retried.
         */
        void notifyInterceptor()
        {
            BatchlogManagerInterceptor.instance.onReplayedBatch(id, writtenAt, Collections.unmodifiableList(mutations));
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
                if (writtenAt <= SystemKeyspace.getTruncatedAt(tableId))
                    mutation = mutation.without(tableId);

            if (!mutation.isEmpty())
                mutations.add(mutation);
        }

        private void writeHintsForUndeliveredEndpoints(int startFrom)
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

            ReplicaPlan.ForWrite replicaPlan = new ReplicaPlan.ForWrite(keyspace, liveAndDown.replicationStrategy(),
                                                                        ConsistencyLevel.ONE, liveRemoteOnly.pending(), liveRemoteOnly.all(), liveRemoteOnly.all(), liveRemoteOnly.all());
            ReplayWriteResponseHandler<Mutation> handler = new ReplayWriteResponseHandler<>(replicaPlan, mutation, Dispatcher.RequestTime.forImmediateExecution());
            Message<Mutation> message = Message.outWithFlag(MUTATION_REQ, mutation, MessageFlag.CALL_BACK_ON_FAILURE);
            for (Replica replica : liveRemoteOnly.all())
                MessagingService.instance().sendWriteWithCallback(message, replica, handler);
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

            // TODO: should we be hinting here, since presumably batch log will retry? Maintaining historical behaviour for the moment.
            ReplayWriteResponseHandler(ReplicaPlan.ForWrite replicaPlan, Supplier<Mutation> hintOnFailure, Dispatcher.RequestTime requestTime)
            {
                super(replicaPlan, null, WriteType.UNLOGGED_BATCH, hintOnFailure, requestTime);
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
}
