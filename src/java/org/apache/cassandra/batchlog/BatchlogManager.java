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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import org.apache.cassandra.exceptions.UnknownTableException;
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
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
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
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

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

    // Batches whose mutations cannot be decoded (a corrupt row, a messaging version this software does not know, a row DSE
    // 6 left in the table with a layout this software cannot read), mapped to the version they were stored with (null when
    // the row has none). They are kept in the batchlog, never deleted by the replay: the row may be the only complete copy
    // of a batch whose mutations reached only some replicas, and a later build may decode it (the scan starts from the
    // first batch again at every start of the node). They are not decoded again before that: the decoding is a function of
    // the stored bytes and of the schema only, so it fails the same way while the schema does not change, and a schema
    // change that would let it succeed (a column added, for example) is picked up at the next start of the node (a failure
    // to read the row itself is not one of these: it fails the scan, which aborts the cycle, as before). Each is logged at
    // ERROR once per start of the node, up to UNDECODABLE_ERROR_MAX_BATCHES of them while the set is not empty, the others
    // at DEBUG; while any remains a WARN counts and lists them every UNDECODABLE_WARN_INTERVAL_NANOS, the first one an
    // interval after the first batch was kept; the rows an operator deleted are forgotten then. Only accessed by the replay
    // thread, but for the test accessor, called once a replay has completed.
    private final Map<TimeUUID, Integer> undecodableBatches = new HashMap<>();
    private static final long UNDECODABLE_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(10);
    // the most ids the WARN lists
    private static final int UNDECODABLE_WARN_MAX_IDS = 10;
    // the most kept batches logged at ERROR, each, since the set of kept batches was last empty
    private static final int UNDECODABLE_ERROR_MAX_BATCHES = 10;
    private long nextUndecodableWarnNanos;
    // the kept batches logged at ERROR since the set of kept batches was last empty
    private int undecodableErrorsLogged;
    // the exception classes whose stack trace an ERROR about a kept batch carried since the set of kept batches was last empty:
    // the trace is logged for the first batch of each class only
    private final Set<Class<?>> tracedUndecodableCauses = new HashSet<>();

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

    public long getHintedBatchesReplayed()
    {
        return StorageMetrics.hintedBatchlogReplays.getCount();
    }

    @VisibleForTesting
    int countBatchesAwaitingInterceptor()
    {
        return batchesAwaitingInterceptor.size();
    }

    /** The ids of the batches kept in the batchlog because their mutations cannot be decoded, in no particular order. */
    @VisibleForTesting
    Set<TimeUUID> undecodableBatchIds()
    {
        return new HashSet<>(undecodableBatches.keySet());
    }

    /** Lets the next replay cycle log the WARN listing the undecodable batches without waiting for its interval. */
    @VisibleForTesting
    void expireUndecodableBatchesWarnInterval()
    {
        nextUndecodableWarnNanos = nanoTime();
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
        // scan, and batches kept because they cannot be decoded, which are not retried before the next
        // start; every other processed batch is deleted, but the tombstoned content may still be present
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
        warnAboutUndecodableBatches();
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

            // If a cycle aborts before advancing lastReplayedUuid, the next scan still covers the
            // batches it left awaiting their interceptor; those are handled by
            // retryBatchesAwaitingInterceptor later in this cycle, so don't replay them here.
            if (batchesAwaitingInterceptor.containsKey(id))
                continue;

            // A scan that covers an undecodable batch again (see above) neither decodes nor logs it again.
            if (undecodableBatches.containsKey(id))
                continue;

            Integer version = storedVersion(row);
            // a batch without mutations (an empty list reads as null) has nothing to replay and is removed below
            List<ByteBuffer> serializedMutations = row.has("mutations") ? row.getList("mutations", BytesType.instance) : Collections.emptyList();
            try
            {
                if (serializedMutations.isEmpty())
                {
                    // nothing to replay, whatever the version: as for a batch of this software without mutations below
                    remove(id);
                    ++totalBatchesReplayed;
                }
                else
                {
                    if (version == null)
                        throw new UndecodableBatchException("its mutations cannot be decoded without it", null);
                    ReplayingBatch batch = new ReplayingBatch(id, version, serializedMutations);
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
            }
            catch (IOException e)
            {
                // version is not null here; a batch DSE 6 stored gets here only for a table this software does not have
                if (version >= DseCompat.DSE_60_VERSION)
                    logger.warn("Skipped batch replay of {} (stored by DSE 6 with messaging version {}; its mutations, none of which " +
                                "is replayed, write to {}, the first table of each mutation) due to {}",
                                id, version, describeTables(serializedMutations), e.getMessage());
                else
                    logger.warn("Skipped batch replay of {} due to {}", id, e.getMessage());
                caughtException = e;
                remove(id);
                ++skipped;
            }
            catch (UndecodableBatchException e)
            {
                keepUndecodableBatch(id, version, serializedMutations, e);
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
     * Keeps a batch whose mutations cannot be decoded in the batchlog, so that it blocks neither this replay cycle nor the
     * replay of the batches after it, and logs it at ERROR: the caller skips the batches already kept, so this happens the
     * first time a scan meets it after the node started. Past {@link #UNDECODABLE_ERROR_MAX_BATCHES} kept batches one
     * more ERROR says that the others are logged at DEBUG only, so that many corrupt rows do not flood the log.
     */
    private void keepUndecodableBatch(TimeUUID id, Integer version, List<ByteBuffer> serializedMutations, UndecodableBatchException e)
    {
        if (undecodableBatches.isEmpty())
        {
            nextUndecodableWarnNanos = nanoTime() + UNDECODABLE_WARN_INTERVAL_NANOS;
            tracedUndecodableCauses.clear();
            undecodableErrorsLogged = 0;
        }
        undecodableBatches.put(id, version);

        // the decoding failure, if any, with its stack trace the first time its class is met; an unknown or missing version
        // has none
        Throwable cause = e.getCause();
        // in DSE_6 the upgrade is in progress: the tables are repaired once it is finished
        String stored = version == null ? "stored without a messaging version" : "stored with messaging version " + version;
        String repairWhen = DatabaseDescriptor.isDse6CompatibilityMode() ? " (once the DSE 6 upgrade is finished)" : "";
        String message = "Batch {} of system.batches, {}, cannot be decoded: {}. It is kept in the batchlog and not replayed; " +
                         "the other batches are replayed as usual. Its mutations may be missing on some of their replicas; they " +
                         "write to {} (the first table of each mutation). The batch is tried again at the next start of this node; " +
                         "if it is older than the gc_grace_seconds of its tables, it would not be replayed even if it could be " +
                         "decoded. To drop it, save it (SELECT mutations, version FROM system.batches WHERE id = {}), delete it on " +
                         "this node (DELETE FROM system.batches WHERE id = {}) and {}";
        if (undecodableErrorsLogged < UNDECODABLE_ERROR_MAX_BATCHES)
        {
            ++undecodableErrorsLogged;
            logger.error(message, id, stored, e.getMessage(), describeTables(serializedMutations), id, id, describeRepair(serializedMutations, repairWhen),
                         cause != null && tracedUndecodableCauses.add(cause.getClass()) ? cause : null);
            return;
        }
        if (undecodableErrorsLogged == UNDECODABLE_ERROR_MAX_BATCHES)
        {
            ++undecodableErrorsLogged;
            logger.error("More batches of system.batches cannot be decoded than the {} logged above: they are kept in the batchlog " +
                         "and not replayed as well, each logged at DEBUG only; the periodic warning counts and lists them",
                         UNDECODABLE_ERROR_MAX_BATCHES);
        }
        if (logger.isDebugEnabled())
            logger.debug(message, id, stored, e.getMessage(), describeTables(serializedMutations), id, id, describeRepair(serializedMutations, repairWhen),
                         cause);
    }

    /**
     * The messaging version a row of {@code system.batches} was stored with, or null when it has none: a version cell that
     * does not hold a 4-byte int (an empty value, which CQL accepts for an int) is read as no version.
     */
    private static Integer storedVersion(UntypedResultSet.Row row)
    {
        ByteBuffer versionBytes = row.getBytes("version");
        return versionBytes != null && versionBytes.remaining() == Integer.BYTES ? ByteBufferUtil.toInt(versionBytes) : null;
    }

    /**
     * The tables the first partition update of each serialized mutation writes to, best effort, for the operator: in every
     * layout a mutation starts with the number of its partition updates (an unsigned vint) and the table id of the first.
     */
    @VisibleForTesting
    static String describeTables(List<ByteBuffer> serializedMutations)
    {
        Set<String> tables = new LinkedHashSet<>();
        for (ByteBuffer serializedMutation : serializedMutations)
        {
            try
            {
                TableId tableId = firstTableId(serializedMutation);
                TableMetadata table = Schema.instance.getTableMetadata(tableId);
                tables.add(table == null ? "unknown table " + tableId : table.keyspace + '.' + table.name + " (" + tableId + ')');
            }
            catch (IOException | RuntimeException e)
            {
                tables.add("a mutation too short or corrupt to name its table");
            }
        }
        return tables.isEmpty() ? "no mutation" : String.join(", ", tables);
    }

    /**
     * What to repair once a kept batch is dropped, for the operator, from the same first table of each serialized mutation
     * as {@link #describeTables}, each clause followed by {@code repairWhen}: the tables it writes to, and for a
     * materialized view among them (the batchlog holds the view updates of base writes) a repair of the view or of its
     * base table, or a rebuild of the view, in that order. The batchlog holds the view updates for a paired view replica
     * that is usually another node (all of them while this node starts, joins or moves), so the view is repaired on every
     * node, not only on this one. The base write usually reached every base replica, each of which sends its view
     * updates to its paired view replica, so the lost view update is usually held by another view replica: a repair of
     * the view restores it, in which case a repair of the base table finds no difference. A repair of the base table
     * streams the base rows its replicas disagree on, which write their view updates again, so it restores only the view
     * rows of those base rows; a rebuild of the view (drop and create it again) restores all the view rows.
     */
    @VisibleForTesting
    static String describeRepair(List<ByteBuffer> serializedMutations, String repairWhen)
    {
        Set<String> views = new LinkedHashSet<>();
        boolean otherTables = false;
        for (ByteBuffer serializedMutation : serializedMutations)
        {
            ViewMetadata view = null;
            try
            {
                TableMetadata table = Schema.instance.getTableMetadata(firstTableId(serializedMutation));
                if (table != null && table.isView())
                    view = Schema.instance.getView(table.keyspace, table.name);
            }
            catch (IOException | RuntimeException e)
            {
                // a mutation that does not name its table: among the tables it writes to
            }
            if (view == null)
                otherTables = true;
            else
                views.add("repair the view " + view.keyspace() + '.' + view.name() + " on every node (the view replicas that miss its " +
                          "updates are usually other nodes), which restores the lost view rows that another replica of the view holds, or " +
                          "repair its base table " + view.keyspace() + '.' + view.baseTableName + ", which restores the lost view rows " +
                          "only where the replicas of the base table differ, or rebuild the view (drop it and create it again, with " +
                          "materialized_views_enabled: true), which restores all the view rows" + repairWhen);
        }
        List<String> advice = new ArrayList<>();
        if (otherTables || views.isEmpty())
            advice.add((views.isEmpty() ? "repair the tables it writes to" : "repair the other tables it writes to") + repairWhen);
        advice.addAll(views);
        return String.join("; ", advice);
    }

    /** The id of the table the first partition update of {@code serializedMutation} writes to, in every layout. */
    private static TableId firstTableId(ByteBuffer serializedMutation) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
        {
            // the number of partition updates, then the table id of the first
            in.readUnsignedVInt();
            return TableId.deserialize(in);
        }
    }

    /**
     * Every {@link #UNDECODABLE_WARN_INTERVAL_NANOS} while batches are kept: forgets those whose rows are gone (deleted by
     * an operator), with a point read each, and lists the others in a WARN, since the condition does not go away by
     * itself. A failure of the point reads leaves the batches listed and the cycle unharmed.
     */
    private void warnAboutUndecodableBatches()
    {
        if (undecodableBatches.isEmpty() || nanoTime() - nextUndecodableWarnNanos < 0)
            return;

        nextUndecodableWarnNanos = nanoTime() + UNDECODABLE_WARN_INTERVAL_NANOS;
        String query = String.format("SELECT id FROM %s.%s WHERE id = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES);
        try
        {
            undecodableBatches.keySet().removeIf(id -> {
                UntypedResultSet result = executeInternal(query, id);
                return result == null || result.isEmpty();
            });
        }
        catch (RuntimeException e)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 10, TimeUnit.MINUTES,
                             "Failed to check whether the undecodable batches are still in system.batches", e);
        }
        if (undecodableBatches.isEmpty())
        {
            tracedUndecodableCauses.clear();
            return;
        }

        String listed = undecodableBatches.entrySet().stream()
                                          .limit(UNDECODABLE_WARN_MAX_IDS)
                                          .map(entry -> entry.getKey() + (entry.getValue() == null ? " (no version)" : " (version " + entry.getValue() + ')'))
                                          .collect(Collectors.joining(", "));
        if (undecodableBatches.size() > UNDECODABLE_WARN_MAX_IDS)
            listed += " and " + (undecodableBatches.size() - UNDECODABLE_WARN_MAX_IDS) + " more";
        logger.warn("{} batches of system.batches cannot be decoded and are kept in the batchlog, not replayed: {}. See the errors " +
                    "logged when they were first met", undecodableBatches.size(), listed);
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
                Integer version = storedVersion(row);
                if (version == null)
                    throw new UndecodableBatchException("its mutations cannot be decoded without a messaging version", null);
                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
                rateLimiter.acquire(batch.replayedBytes);
                if (tryNotifyInterceptor(batch))
                {
                    hintedNodes.addAll(entry.getValue());
                    replayedBatches.add(id);
                }
            }
            catch (IOException e)
            {
                dropInterceptorCallbacks(entry, hintedNodes, replayedBatches, e);
            }
            catch (UndecodableBatchException e)
            {
                // the row decoded in an earlier cycle and changed since: classified as before batches could be kept, an
                // IOException (in a batch DSE 6 stored) drops the callbacks, anything else is retried as below
                if (e.getCause() instanceof IOException)
                    dropInterceptorCallbacks(entry, hintedNodes, replayedBatches, e.getCause());
                else
                    retryInterceptorRead(id, e.getCause() == null ? e : e.getCause());
            }
            catch (Throwable t)
            {
                retryInterceptorRead(id, t);
            }
        }
    }

    private void dropInterceptorCallbacks(Map.Entry<TimeUUID, Set<UUID>> entry, Set<UUID> hintedNodes, Set<TimeUUID> replayedBatches, Throwable e)
    {
        logger.error("Batch {} was replayed but can no longer be deserialized; its remaining interceptor callbacks are dropped", entry.getKey(), e);
        // let the deletion share the hint fsync barrier below, like any other replayed batch
        hintedNodes.addAll(entry.getValue());
        replayedBatches.add(entry.getKey());
    }

    private static void retryInterceptorRead(TimeUUID id, Throwable t)
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

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<UUID> hintedNodes, Set<TimeUUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish();
            Set<UUID> batchHintedNodes = batch.hintedNodes();
            // Metered now, even if the interceptor fails below: the retry path re-sends no mutations and writes
            // no hints, so the batch would otherwise never be accounted for.
            if (batchHintedNodes.isEmpty())
                StorageMetrics.batchlogReplays.mark();
            else
                StorageMetrics.hintedBatchlogReplays.mark();
            hintedNodes.addAll(batchHintedNodes);
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
                batchesAwaitingInterceptor.put(batch.id, batchHintedNodes);
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

    /** The mutations of a batch cannot be decoded: the batch is kept in the batchlog, see {@link #undecodableBatches}. */
    private static final class UndecodableBatchException extends Exception
    {
        UndecodableBatchException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    private static class ReplayingBatch
    {
        private final TimeUUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private final int replayedBytes;
        private final Set<UUID> hintedNodes = new HashSet<>();

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers;

        /**
         * @throws IOException if a mutation writes to a table that does not exist ({@link UnknownTableException}: dropped, or
         * one of DSE's own), or if a mutation of a batch of this software (stored at {@link MessagingService#VERSION_40} up
         * to {@link DseCompat#DSE_60_VERSION}, excluded) fails to decode with an IOException: the replay drops the batch, as
         * it always did
         * @throws UndecodableBatchException if the stored version is unknown, or a mutation fails to decode with an
         * IOException in a batch stored at another version, or with a RuntimeException or an AssertionError: the batch is
         * kept
         */
        ReplayingBatch(TimeUUID id, int version, List<ByteBuffer> serializedMutations) throws IOException, UndecodableBatchException
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

        private int addMutations(int version, List<ByteBuffer> serializedMutations) throws IOException, UndecodableBatchException
        {
            // a batch DSE 6 left in the table carries DSE's messaging version, in every mode: the rows outlive the mode
            if (DseCompat.isUnknownStoredDseVersion(version))
                throw new UndecodableBatchException("it is in DSE's range of messaging versions but not a DSE 6 version (" +
                                                    DseCompat.DSE_STORED_VERSIONS_TEXT + ')', null);
            int decodingVersion = DseCompat.storedDecodingVersion(version);

            int ret = 0;
            for (ByteBuffer serializedMutation : serializedMutations)
            {
                ret += serializedMutation.remaining();
                Mutation mutation;
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    mutation = Mutation.serializer.deserialize(in, decodingVersion);
                }
                catch (UnknownTableException e)
                {
                    throw e;
                }
                catch (IOException e)
                {
                    // the batches of this software and of HCD (stored at VERSION_40 or later) keep the drop of the IOException
                    // path; a batch a pre-4.0 node or DSE 6 stored that is cut short or corrupt is kept like the ones below
                    if (version >= MessagingService.VERSION_40 && version < DseCompat.DSE_60_VERSION)
                        throw e;
                    throw new UndecodableBatchException(e.toString(), e);
                }
                catch (RuntimeException | AssertionError e)
                {
                    // a corrupt row (a length out of range), or bytes of a layout this software does not read: the batch
                    // may be the only complete copy of its mutations, and it must not abort the replay cycle
                    throw new UndecodableBatchException(e.toString(), e);
                }
                addMutation(mutation);
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
