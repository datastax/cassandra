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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationCallback;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TruncateRequest;
import org.apache.cassandra.db.WriteOptions;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.ViewUtils;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.metrics.CASClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.SensorsFactory;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.PaxosUtils;
import org.apache.cassandra.service.paxos.PrepareCallback;
import org.apache.cassandra.service.paxos.ProposeCallback;
import org.apache.cassandra.service.reads.AbstractReadExecutor;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.range.RangeCommands;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.BATCH_STORE_REQ;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.TRUNCATE_REQ;
import static org.apache.cassandra.service.BatchlogResponseHandler.BatchlogCleanup;
import static org.apache.cassandra.service.paxos.PrepareVerbHandler.doPrepare;
import static org.apache.cassandra.service.paxos.ProposeVerbHandler.doPropose;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private static final Mutator mutator = MutatorProvider.instance;

    private static final boolean useDynamicSnitchForCounterLeader = CassandraRelevantProperties.USE_DYNAMIC_SNITCH_FOR_COUNTER_LEADER.getBoolean();

    public static class DefaultMutator implements Mutator
    {
        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime)
        {
            return defaultMutateCounter(cm, localDataCenter, queryStartNanoTime);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounterOnLeader(CounterMutation mutation,
                                                                             String localDataCenter,
                                                                             StorageProxy.WritePerformer performer,
                                                                             Runnable callback,
                                                                             long queryStartNanoTime)
        {
            return performWrite(mutation, mutation.consistency(), localDataCenter, performer, callback, WriteType.COUNTER, queryStartNanoTime);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateStandard(Mutation mutation, ConsistencyLevel consistencyLevel, String localDataCenter, WritePerformer standardWritePerformer, Runnable callback, WriteType writeType, long queryStartNanoTime)
        {
            return performWrite(mutation, consistencyLevel, localDataCenter, standardWritePerformer, callback, writeType, queryStartNanoTime);
        }

        @Override
        public AbstractWriteResponseHandler<Commit> mutatePaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, long queryStartNanoTime)
        {
            return defaultCommitPaxos(proposal, consistencyLevel, allowHints, queryStartNanoTime);
        }

        @Override
        public void mutateAtomically(Collection<Mutation> mutations, ConsistencyLevel consistencyLevel, boolean requireQuorumForRemove, long queryStartNanoTime, ClientRequestsMetrics metrics, ClientState clientState) throws UnavailableException, OverloadedException, WriteTimeoutException
        {
            Tracing.trace("Determining replicas for atomic batch");
            long startTime = System.nanoTime();

            QueryInfoTracker.WriteTracker writeTracker = StorageProxy.queryTracker().onWrite(clientState, true, mutations, consistencyLevel);

            // Request sensors are utilized to track usages from replicas serving atomic batch request
            RequestSensors sensors = SensorsFactory.instance.createRequestSensors(mutations.stream().map(IMutation::getKeyspaceName).toArray(String[]::new));
            ExecutorLocals locals = ExecutorLocals.create(sensors);
            ExecutorLocals.set(locals);

            if (mutations.stream().anyMatch(mutation -> Keyspace.open(mutation.getKeyspaceName()).getReplicationStrategy().hasTransientReplicas()))
                throw new AssertionError("Logged batches are unsupported with transient replication");

            try
            {

                // If we are requiring quorum nodes for removal, we upgrade consistency level to QUORUM unless we already
                // require ALL, or EACH_QUORUM. This is so that *at least* QUORUM nodes see the update.
                ConsistencyLevel batchConsistencyLevel = requireQuorumForRemove
                                                         ? ConsistencyLevel.QUORUM
                                                         : consistencyLevel;

                switch (consistencyLevel)
                {
                    case ALL:
                    case EACH_QUORUM:
                        batchConsistencyLevel = consistencyLevel;
                }

                String keyspace = mutations.iterator().next().getKeyspaceName();
                ReplicaPlan.ForTokenWrite replicaPlan = ReplicaPlans.forBatchlogWrite(batchConsistencyLevel == ConsistencyLevel.ANY, false, keyspace);

                final UUID batchUUID = UUIDGen.getTimeUUID();

                BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(mutations.size(), () -> clearBatchlog(keyspace, replicaPlan, batchUUID));

                List<StorageProxy.WriteResponseHandlerWrapper> wrappers = wrapBatchResponseHandlers(mutations, consistencyLevel, batchConsistencyLevel, cleanup, queryStartNanoTime, sensors);

                // persist batchlog before writing batched mutations
                persistBatchlog(mutations, queryStartNanoTime, replicaPlan, batchUUID);

                // now actually perform the writes
                asyncWriteBatchedMutations(wrappers);

                // wait for batched mutations to complete
                for (StorageProxy.WriteResponseHandlerWrapper wrapper : wrappers)
                    wrapper.handler.get();

                writeTracker.onDone();
            }
            catch (UnavailableException e)
            {
                metrics.writeMetrics.unavailables.mark();
                metrics.writeMetricsForLevel(consistencyLevel).unavailables.mark();
                Tracing.trace("Unavailable");
                writeTracker.onError(e);
                throw e;
            }
            catch (WriteTimeoutException e)
            {
                metrics.writeMetrics.timeouts.mark();
                metrics.writeMetricsForLevel(consistencyLevel).timeouts.mark();
                Tracing.trace("Write timeout; received {} of {} required replies", e.received, e.blockFor);
                writeTracker.onError(e);
                throw e;
            }
            catch (WriteFailureException e)
            {
                metrics.writeMetrics.failures.mark();
                metrics.writeMetricsForLevel(consistencyLevel).failures.mark();
                Tracing.trace("Write failure; received {} of {} required replies", e.received, e.blockFor);
                writeTracker.onError(e);
                throw e;
            }
            finally
            {
                long endTime = System.nanoTime();
                long latency = endTime - startTime;
                metrics.writeMetrics.executionTimeMetrics.addNano(latency);
                metrics.writeMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
                metrics.writeMetricsForLevel(consistencyLevel).executionTimeMetrics.addNano(latency);
                metrics.writeMetricsForLevel(consistencyLevel).serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
                StorageProxy.updateCoordinatorWriteLatencyTableMetric(mutations, latency);
            }
        }

        @Override
        public void clearBatchlog(String keyspace, ReplicaPlan.ForTokenWrite replicaPlan, UUID batchUUID)
        {
            StorageProxy.asyncRemoveFromBatchlog(replicaPlan, batchUUID);
        }

        @Override
        public void persistBatchlog(Collection<Mutation> mutations, long queryStartNanoTime, ReplicaPlan.ForTokenWrite replicaPlan, UUID batchUUID)
        {
            // write to the batchlog
            StorageProxy.syncWriteToBatchlog(mutations, replicaPlan, batchUUID, queryStartNanoTime);
        }

        private List<WriteResponseHandlerWrapper> wrapBatchResponseHandlers(Collection<Mutation> mutations,
                                                                              ConsistencyLevel consistencyLevel,
                                                                              ConsistencyLevel batchConsistencyLevel,
                                                                              BatchlogResponseHandler.BatchlogCleanup cleanup,
                                                                              long queryStartNanoTime,
                                                                              RequestSensors sensors)
        {
            List<StorageProxy.WriteResponseHandlerWrapper> wrappers = new ArrayList<>(mutations.size());

            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
            for (Mutation mutation : mutations)
            {
                // register the sensors for the mutation before the actual write is performed
                for (PartitionUpdate pu: mutation.getPartitionUpdates())
                {
                    if (pu.metadata().isIndex()) continue;
                    sensors.registerSensor(Context.from(pu.metadata()), Type.WRITE_BYTES);
                }
                StorageProxy.WriteResponseHandlerWrapper wrapper = StorageProxy.wrapBatchResponseHandler(mutation,
                                                                                                         consistencyLevel,
                                                                                                         batchConsistencyLevel,
                                                                                                         WriteType.BATCH,
                                                                                                         cleanup,
                                                                                                         queryStartNanoTime);
                // exit early if we can't fulfill the CL at this time.
                wrappers.add(wrapper);
            }

            return wrappers;
        }

        private void asyncWriteBatchedMutations(List<StorageProxy.WriteResponseHandlerWrapper> wrappers)
        throws WriteTimeoutException, OverloadedException
        {
            String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
            StorageProxy.asyncWriteBatchedMutations(wrappers, localDataCenter, Stage.MUTATION);
        }
    }

    private static volatile int maxHintsInProgress = 128 * FBUtilities.getAvailableProcessors();
    private static final CacheLoader<InetAddressAndPort, AtomicInteger> hintsInProgress = new CacheLoader<InetAddressAndPort, AtomicInteger>()
    {
        public AtomicInteger load(InetAddressAndPort inetAddress)
        {
            return new AtomicInteger(0);
        }
    };

    private static volatile QueryInfoTracker queryInfoTracker = QueryInfoTracker.NOOP;

    private static final String DISABLE_SERIAL_READ_LINEARIZABILITY_KEY = "cassandra.unsafe.disable-serial-reads-linearizability";
    private static final boolean disableSerialReadLinearizability =
        Boolean.parseBoolean(System.getProperty(DISABLE_SERIAL_READ_LINEARIZABILITY_KEY, "false"));

    private StorageProxy()
    {
    }

    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
        HintsService.instance.registerMBean();
        PaxosUtils.instance.registerMBean();

        standardWritePerformer = (mutation, targets, responseHandler, localDataCenter) ->
        {
            assert mutation instanceof Mutation;
            sendToHintedReplicas((Mutation) mutation, targets, responseHandler, localDataCenter, Stage.MUTATION);
            mutator.onAppliedMutation(mutation);
        };

        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the COUNTER_MUTATION stage
         * but on the latter case, the verb handler already run on the COUNTER_MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = (mutation, targets, responseHandler, localDataCenter) ->
        {
            EndpointsForToken selected = targets.contacts().withoutSelf();
            Replicas.temporaryAssertFull(selected); // TODO CASSANDRA-14548
            counterWriteTask(mutation, targets.withContact(selected), responseHandler, localDataCenter).run();
        };

        counterWriteOnCoordinatorPerformer = (mutation, targets, responseHandler, localDataCenter) ->
        {
            EndpointsForToken selected = targets.contacts().withoutSelf();
            Replicas.temporaryAssertFull(selected); // TODO CASSANDRA-14548
            Stage.COUNTER_MUTATION.execute(counterWriteTask(mutation, targets.withContact(selected), responseHandler, localDataCenter));
        };

        ReadRepairMetrics.init();

        if (disableSerialReadLinearizability)
        {
            logger.warn("This node was started with -D{}. SERIAL (and LOCAL_SERIAL) reads coordinated by this node " +
                        "will not offer linearizability (see CASSANDRA-12126 for details on what this mean) with " +
                        "respect to other SERIAL operations. Please note that, with this flag, SERIAL reads will be " +
                        "slower than QUORUM reads, yet offer no more guarantee. This flag should only be used in " +
                        "the restricted case of upgrading from a pre-CASSANDRA-12126 version, and only if you " +
                        "understand the tradeoff.", DISABLE_SERIAL_READ_LINEARIZABILITY_KEY);
        }
    }

    /**
     * Registers the provided query info tracker
     *
     * <p>Note that only 1 query tracker can be registered at a time, so the provided tracker will unconditionally
     * replace the currently registered tracker.
     *
     * @param tracker the tracker to register.
     */
    public void registerQueryTracker(QueryInfoTracker tracker) {
        Objects.requireNonNull(tracker);
        queryInfoTracker = tracker;
    }

    public static QueryInfoTracker queryTracker() {
        return queryInfoTracker;
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas respond, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @param keyspaceName the keyspace for the CAS
     * @param cfName the column family for the CAS
     * @param key the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static RowIterator cas(String keyspaceName,
                                  String cfName,
                                  DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit,
                                  QueryState state,
                                  int nowInSeconds,
                                  long queryStartNanoTime)
    throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException, CasWriteUnknownResultException
    {
        final long startTimeForMetrics = System.nanoTime();
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(keyspaceName);
        TableMetadata metadata = Schema.instance.validateTable(keyspaceName, cfName);
        QueryInfoTracker.LWTWriteTracker lwtTracker = queryTracker().onLWTWrite(state.getClientState(),
                                                                                metadata,
                                                                                key,
                                                                                consistencyForPaxos,
                                                                                consistencyForCommit);
        // Request sensors are utilized to track usages from replicas serving a cas request
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(keyspaceName);
        Context context = Context.from(metadata);
        sensors.registerSensor(context, Type.WRITE_BYTES); // track user table + paxos table write bytes
        sensors.registerSensor(context, Type.READ_BYTES); // track user table + paxos table read bytes
        ExecutorLocals locals = ExecutorLocals.create(sensors);
        ExecutorLocals.set(locals);
        try
        {
            consistencyForPaxos.validateForCas(keyspaceName, state);
            consistencyForCommit.validateForCasCommit(Keyspace.open(keyspaceName).getReplicationStrategy(), keyspaceName, state);

            Supplier<Pair<PartitionUpdate, RowIterator>> updateProposer = () ->
            {
                long startTimeNanos = System.nanoTime();
                try
                {
                    // read the current values and check they validate the conditions
                    Tracing.trace("Reading existing values for CAS precondition");
                    SinglePartitionReadCommand readCommand = (SinglePartitionReadCommand) request.readCommand(nowInSeconds);
                    ConsistencyLevel readConsistency = consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;

                    FilteredPartition current;

                    try (RowIterator rowIter = readOne(readCommand, readConsistency, queryStartNanoTime, lwtTracker))
                    {
                        current = FilteredPartition.create(rowIter);
                    }

                    if (!request.appliesTo(current))
                    {
                        Tracing.trace("CAS precondition does not match current values {}", current);
                        lwtTracker.onNotApplied();
                        lwtTracker.onDone();
                        metrics.casWriteMetrics.conditionNotMet.inc();
                        return Pair.create(PartitionUpdate.emptyUpdate(metadata, key), current.rowIterator());
                    }

                    // Create the desired updates
                    PartitionUpdate updates = request.makeUpdates(current, state);
                    lwtTracker.onApplied(updates);
                    lwtTracker.onDone();

                    long size = updates.dataSize();
                    metrics.casWriteMetrics.mutationSize.update(size);
                    metrics.writeMetricsForLevel(consistencyForPaxos).mutationSize.update(size);

                    // Apply triggers to cas updates. A consideration here is that
                    // triggers emit Mutations, and so a given trigger implementation
                    // may generate mutations for partitions other than the one this
                    // paxos round is scoped for. In this case, TriggerExecutor will
                    // validate that the generated mutations are targetted at the same
                    // partition as the initial updates and reject (via an
                    // InvalidRequestException) any which aren't.
                    updates = TriggerExecutor.instance.execute(updates);

                    return Pair.create(updates, null);
                }
                finally
                {
                    metrics.casWriteMetrics.createProposalLatency.addNano(System.nanoTime() - startTimeNanos);
                }
            };

            return doPaxos(metadata,
                           key,
                           consistencyForPaxos,
                           consistencyForCommit,
                           consistencyForCommit,
                           state,
                           queryStartNanoTime,
                           metrics.casWriteMetrics,
                           updateProposer,
                           false);

        }
        catch (CasWriteUnknownResultException e)
        {
            metrics.casWriteMetrics.unknownResult.mark();
            lwtTracker.onError(e);
            throw e;
        }
        catch (CasWriteTimeoutException wte)
        {
            metrics.casWriteMetrics.timeouts.mark();
            metrics.writeMetricsForLevel(consistencyForPaxos).timeouts.mark();
            lwtTracker.onError(wte);
            throw new CasWriteTimeoutException(wte.writeType, wte.consistency, wte.received, wte.blockFor, wte.contentions);
        }
        catch (ReadTimeoutException e)
        {
            metrics.casWriteMetrics.timeouts.mark();
            metrics.writeMetricsForLevel(consistencyForPaxos).timeouts.mark();
            lwtTracker.onError(e);
            throw e;
        }
        catch (WriteFailureException | ReadFailureException e)
        {
            metrics.casWriteMetrics.failures.mark();
            metrics.writeMetricsForLevel(consistencyForPaxos).failures.mark();
            lwtTracker.onError(e);
            throw e;
        }
        catch (UnavailableException e)
        {
            metrics.casWriteMetrics.unavailables.mark();
            metrics.writeMetricsForLevel(consistencyForPaxos).unavailables.mark();
            lwtTracker.onError(e);
            throw e;
        }
        finally
        {
            final long endTime = System.nanoTime();
            final long latency = endTime - startTimeForMetrics;
            metrics.casWriteMetrics.executionTimeMetrics.addNano(latency);
            metrics.casWriteMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            metrics.writeMetricsForLevel(consistencyForPaxos).executionTimeMetrics.addNano(latency);
            metrics.writeMetricsForLevel(consistencyForPaxos).serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
        }
    }

    private static void recordCasContention(TableMetadata table,
                                            DecoratedKey key,
                                            CASClientRequestMetrics casMetrics,
                                            int contentions)
    {
        if (contentions == 0)
            return;

        casMetrics.contention.update(contentions);
        Keyspace.open(table.keyspace)
                .getColumnFamilyStore(table.name)
                .metric
                .topCasPartitionContention
                .addSample(key.getKey(), contentions);
    }

    /**
     * Performs the Paxos rounds for a given proposal, retrying when preempted until the timeout.
     *
     * <p>The main 'configurable' of this method is the {@code createUpdateProposal} method: it is called by the method
     * once a ballot has been successfully 'prepared' to generate the update to 'propose' (and commit if the proposal is
     * successful). That method also generates the result that the whole method will return. Note that due to retrying,
     * this method may be called multiple times and does not have to return the same results.
     *
     * @param metadata the table to update with Paxos.
     * @param key the partition updated.
     * @param consistencyForPaxos the serial consistency of the operation (either {@link ConsistencyLevel#SERIAL} or
     *     {@link ConsistencyLevel#LOCAL_SERIAL}).
     * @param consistencyForReplayCommits the consistency for the commit phase of "replayed" in-progress operations.
     * @param consistencyForCommit the consistency for the commit phase of _this_ operation update.
     * @param queryState the query state.
     * @param queryStartNanoTime the nano time for the start of the query this is part of. This is the base time for
     *     timeouts.
     * @param casMetrics the metrics to update for this operation.
     * @param createUpdateProposal method called after a successful 'prepare' phase to obtain 1) the actual update of
     *     this operation and 2) the result that the whole method should return. This can return {@code null} in the
     *     special where, after having "prepared" (and thus potentially replayed in-progress upgdates), we don't want
     *     to propose anything (the whole method then return {@code null}).
     * @param skipCommitConsistencyValidation whether to skip {@link ConsistencyLevel#validateForCasCommit} for commit consistency
     * @return the second element of the pair returned by {@code createUpdateProposal} (for the last call of that method
     *     if that method is called multiple times due to retries).
     */
    private static RowIterator doPaxos(TableMetadata metadata,
                                       DecoratedKey key,
                                       ConsistencyLevel consistencyForPaxos,
                                       ConsistencyLevel consistencyForReplayCommits,
                                       ConsistencyLevel consistencyForCommit,
                                       QueryState queryState,
                                       long queryStartNanoTime,
                                       CASClientRequestMetrics casMetrics,
                                       Supplier<Pair<PartitionUpdate, RowIterator>> createUpdateProposal,
                                       boolean skipCommitConsistencyValidation)
    throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        int contentions = 0;
        Keyspace keyspace = Keyspace.open(metadata.keyspace);
        AbstractReplicationStrategy latestRs = keyspace.getReplicationStrategy();
        try
        {
            consistencyForPaxos.validateForCas(metadata.keyspace, queryState);
            consistencyForReplayCommits.validateForCasCommit(latestRs, metadata.keyspace, queryState);
            if (!skipCommitConsistencyValidation)
                consistencyForCommit.validateForCasCommit(latestRs, metadata.keyspace, queryState);

            long timeoutNanos = DatabaseDescriptor.getCasContentionTimeout(NANOSECONDS);
            while (System.nanoTime() - queryStartNanoTime < timeoutNanos)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                ReplicaPlan.ForPaxosWrite replicaPlan = ReplicaPlans.forPaxos(keyspace, key, consistencyForPaxos);
                latestRs = replicaPlan.replicationStrategy();
                PaxosBallotAndContention pair = beginAndRepairPaxos(queryStartNanoTime,
                                                                    key,
                                                                    metadata,
                                                                    replicaPlan,
                                                                    consistencyForPaxos,
                                                                    consistencyForReplayCommits,
                                                                    casMetrics, queryState.getClientState()
                );

                final UUID ballot = pair.ballot;
                contentions += pair.contentions;

                Pair<PartitionUpdate, RowIterator> proposalPair = createUpdateProposal.get();
                // See method javadoc: null here is code for "stop here and return null".
                if (proposalPair == null)
                    return null;

                Commit proposal = Commit.newProposal(ballot, proposalPair.left);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                if (proposePaxos(proposal, replicaPlan, true, queryStartNanoTime, casMetrics))
                {
                    // We skip committing accepted updates when they are empty. This is an optimization which works
                    // because we also skip replaying those same empty update in beginAndRepairPaxos (see the longer
                    // comment there). As empty update are somewhat common (serial reads and non-applying CAS propose
                    // them), this is worth bothering.
                    if (!proposal.update.isEmpty())
                        commitPaxos(proposal, consistencyForCommit, true, queryStartNanoTime, casMetrics);
                    RowIterator result = proposalPair.right;
                    if (result != null)
                        Tracing.trace("CAS did not apply");
                    else
                        Tracing.trace("CAS applied successfully");
                    return result;
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                PaxosUtils.applyPaxosContentionBackoff(casMetrics);
                // continue to retry
            }
        }
        catch (CasWriteTimeoutException e)
        {
            // Might be thrown by beginRepairAndPaxos. In that case, any contention that happened within the method and
            // led up to the timeout was not accounted in our local 'contentions' variable and we add it now so it the
            // contention recorded in the finally is correct.
            contentions += e.contentions;
            throw e;
        }
        catch (WriteTimeoutException e)
        {
            // Might be thrown by proposePaxos or commitPaxos
            throw new CasWriteTimeoutException(e.writeType, e.consistency, e.received, e.blockFor, contentions);
        }
        finally
        {
            recordCasContention(metadata, key, casMetrics, contentions);
        }

        throw new CasWriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(latestRs), contentions);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static PaxosBallotAndContention beginAndRepairPaxos(long queryStartNanoTime,
                                                                DecoratedKey key,
                                                                TableMetadata metadata,
                                                                ReplicaPlan.ForPaxosWrite paxosPlan,
                                                                ConsistencyLevel consistencyForPaxos,
                                                                ConsistencyLevel consistencyForCommit,
                                                                CASClientRequestMetrics casMetrics,
                                                                ClientState state)
    throws WriteTimeoutException, WriteFailureException
    {
        long timeoutNanos = DatabaseDescriptor.getCasContentionTimeout(NANOSECONDS);

        PrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - queryStartNanoTime < timeoutNanos)
        {
            // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
            // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
            // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
            // out-of-order (#7801).
            long minTimestampMicrosToUse = summary == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(summary.mostRecentInProgressCommit.ballot);
            long ballotMicros = state.getTimestampForPaxos(minTimestampMicrosToUse);
            // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled concurrently by the same coordinator. But we still
            // need ballots to be unique for each proposal so we have to use getRandomTimeUUIDFromMicros.
            UUID ballot = UUIDGen.getRandomTimeUUIDFromMicros(ballotMicros);

            // prepare
            try
            {
                Tracing.trace("Preparing {}", ballot);
                Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
                summary = preparePaxos(toPrepare, paxosPlan, queryStartNanoTime, casMetrics);
                if (!summary.promised)
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    contentions++;
                    // sleep a random amount to give the other proposer a chance to finish
                    PaxosUtils.applyPaxosContentionBackoff(casMetrics);
                    continue;
                }

                Commit inProgress = summary.mostRecentInProgressCommit;
                Commit mostRecent = summary.mostRecentCommit;

                // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
                // needs to be completed, so do it.
                // One special case we make is for update that are empty (which are proposed by serial reads and
                // non-applying CAS). While we could handle those as any other updates, we can optimize this somewhat by
                // neither committing those empty updates, nor replaying in-progress ones. The reasoning is this: as the
                // update is empty, we have nothing to apply to storage in the commit phase, so the only reason to commit
                // would be to update the MRC. However, if we skip replaying those empty updates, then we don't need to
                // update the MRC for following updates to make progress (that is, if we didn't had the empty update skip
                // below _but_ skipped updating the MRC on empty updates, then we'd be stuck always proposing that same
                // empty update). And the reason skipping that replay is safe is that when an operation tries to propose
                // an empty value, there can be only 2 cases:
                //  1) the propose succeed, meaning a quorum of nodes accept it, in which case we are guaranteed no earlier
                //     pending operation can ever be replayed (which is what we want to guarantee with the empty update).
                //  2) the propose does not succeed. But then the operation proposing the empty update will not succeed
                //     either (it will retry or ultimately timeout), and we're actually ok if earlier pending operation gets
                //     replayed in that case.
                // Tl;dr, it is safe to skip committing empty updates _as long as_ we also skip replying them below. And
                // doing is more efficient, so we do so.
                if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
                {
                    Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                    casMetrics.unfinishedCommit.inc();
                    Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update);
                    if (proposePaxos(refreshedInProgress, paxosPlan, false, queryStartNanoTime, casMetrics))
                    {
                        commitPaxos(refreshedInProgress, consistencyForCommit, false, queryStartNanoTime, casMetrics);
                    }
                    else
                    {
                        Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                        // sleep a random amount to give the other proposer a chance to finish
                        contentions++;
                        PaxosUtils.applyPaxosContentionBackoff(casMetrics);
                    }
                    continue;
                }

                // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
                // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
                // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
                // mean we lost messages), we pro-actively "repair" those nodes, and retry.
                int nowInSec = Ints.checkedCast(TimeUnit.MICROSECONDS.toSeconds(ballotMicros));
                Iterable<InetAddressAndPort> missingMRC = summary.replicasMissingMostRecentCommit(metadata, nowInSec);
                int missingMRCSize = Iterables.size(missingMRC);
                if (missingMRCSize > 0)
                {
                    Tracing.trace("Repairing replicas that missed the most recent commit");
                    casMetrics.missingMostRecentCommit.inc(missingMRCSize);
                    sendCommit(mostRecent, missingMRC);
                    // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                    // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                    // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                    // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                    continue;
                }

                return new PaxosBallotAndContention(ballot, contentions);
            }
            catch (WriteTimeoutException e)
            {
                // We're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                throw new CasWriteTimeoutException(WriteType.CAS, e.consistency, e.received, e.blockFor, contentions);
            }
        }

        throw new CasWriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(paxosPlan.replicationStrategy()), contentions);
    }

    /**
     * Unlike commitPaxos, this does not wait for replies
     */
    private static void sendCommit(Commit commit, Iterable<InetAddressAndPort> replicas)
    {
        Message<Commit> message = Message.out(PAXOS_COMMIT_REQ, commit);
        for (InetAddressAndPort target : replicas)
            MessagingService.instance().send(message, target);
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, ReplicaPlan.ForPaxosWrite replicaPlan, long queryStartNanoTime,
                                                CASClientRequestMetrics casMetrics) throws WriteTimeoutException
    {
        long startTimeNanos = System.nanoTime();
        try
        {
            PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(), toPrepare.update.metadata(), replicaPlan.requiredParticipants(), replicaPlan.consistencyLevel(), queryStartNanoTime);
            Message<Commit> message = Message.out(PAXOS_PREPARE_REQ, toPrepare);
            for (Replica replica : replicaPlan.contacts())
            {
                if (replica.isSelf())
                {
                    PAXOS_PREPARE_REQ.stage.execute(() -> {
                        try
                        {
                            callback.onResponse(message.responseWith(doPrepare(toPrepare)));
                        }
                        catch (Exception ex)
                        {
                            logger.error("Failed paxos prepare locally", ex);
                        }
                    });
                }
                else
                {
                    MessagingService.instance().sendWithCallback(message, replica.endpoint(), callback);
                }
            }
            callback.await();
            return callback;
        }
        finally
        {
            casMetrics.prepareLatency.addNano(System.nanoTime() - startTimeNanos);
        }
    }

    /**
     * Propose the {@param proposal} accoding to the {@param replicaPlan}.
     * When {@param backoffIfPartial} is true, the proposer backs off when seeing the proposal being accepted by some but not a quorum.
     * The result of the cooresponding CAS in uncertain as the accepted proposal may or may not be spread to other nodes in later rounds.
     */
    private static boolean proposePaxos(Commit proposal, ReplicaPlan.ForPaxosWrite replicaPlan, boolean backoffIfPartial,
                                        long queryStartNanoTime, CASClientRequestMetrics casMetrics)
    throws WriteTimeoutException, CasWriteUnknownResultException
    {
        long startTimeNanos = System.nanoTime();
        try
        {
            ProposeCallback callback = new ProposeCallback(proposal.update.metadata(), replicaPlan.contacts().size(), replicaPlan.requiredParticipants(), !backoffIfPartial, replicaPlan.consistencyLevel(), queryStartNanoTime);
            Message<Commit> message = Message.out(PAXOS_PROPOSE_REQ, proposal);
            for (Replica replica : replicaPlan.contacts())
            {
                if (replica.isSelf())
                {
                    PAXOS_PROPOSE_REQ.stage.execute(() -> {
                        try
                        {
                            Message<Boolean> response = message.responseWith(doPropose(proposal));
                            callback.onResponse(response);
                        }
                        catch (Exception ex)
                        {
                            logger.error("Failed paxos propose locally", ex);
                        }
                    });
                }
                else
                {
                    MessagingService.instance().sendWithCallback(message, replica.endpoint(), callback);
                }
            }
            callback.await();

            if (callback.isSuccessful())
                return true;

            if (backoffIfPartial && !callback.isFullyRefused())
                throw new CasWriteUnknownResultException(replicaPlan.consistencyLevel(), callback.getAcceptCount(), replicaPlan.requiredParticipants());
        }
        finally
        {
            casMetrics.proposeLatency.addNano(System.nanoTime() - startTimeNanos);
        }

        return false;
    }

    @Nullable
    private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, long queryStartNanoTime,
                                    CASClientRequestMetrics casMetrics) throws WriteTimeoutException
    {
        long startTimeNanos = System.nanoTime();
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        AbstractWriteResponseHandler<Commit> responseHandler = mutator.mutatePaxos(proposal, consistencyLevel, allowHints, queryStartNanoTime);
        if (shouldBlock && responseHandler != null)
        {
            try
            {
                responseHandler.get();
            }
            finally
            {
                casMetrics.commitLatency.addNano(System.nanoTime() - startTimeNanos);
            }
        }
    }

    @Nullable
    private static AbstractWriteResponseHandler<Commit> defaultCommitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, long queryStartNanoTime) throws WriteTimeoutException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().keyspace);

        Token tk = proposal.update.partitionKey().getToken();

        AbstractWriteResponseHandler<Commit> responseHandler = null;
        // NOTE: this ReplicaPlan is a lie, this usage of ReplicaPlan could do with being clarified - the selected() collection is essentially (I think) never used
        ReplicaPlan.ForTokenWrite replicaPlan = ReplicaPlans.forWrite(keyspace, consistencyLevel, tk, ReplicaPlans.writeAll);
        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = replicaPlan.replicationStrategy();
            responseHandler = rs.getWriteResponseHandler(replicaPlan, null, WriteType.SIMPLE, queryStartNanoTime);
        }

        Message<Commit> message = Message.outWithFlag(PAXOS_COMMIT_REQ, proposal, MessageFlag.CALL_BACK_ON_FAILURE);
        for (Replica replica : replicaPlan.liveAndDown())
        {
            InetAddressAndPort destination = replica.endpoint();
            checkHintOverload(replica);

            if (replicaPlan.isAlive(replica))
            {
                if (shouldBlock)
                {
                    if (replica.isSelf())
                        commitPaxosLocal(replica, message, responseHandler);
                    else
                        MessagingService.instance().sendWriteWithCallback(message, replica, responseHandler, allowHints && shouldHint(replica));
                }
                else
                {
                    MessagingService.instance().send(message, destination);
                }
            }
            else
            {
                if (responseHandler != null)
                {
                    responseHandler.expired();
                }
                if (allowHints && shouldHint(replica))
                {
                    submitHint(proposal.makeMutation(), replica, null);
                }
            }
        }

        return responseHandler;
    }

    /**
     * Commit a PAXOS task locally, and if the task times out rather then submitting a real hint
     * submit a fake one that executes immediately on the mutation stage, but generates the necessary backpressure
     * signal for hints
     */
    private static void commitPaxosLocal(Replica localReplica, final Message<Commit> message, final AbstractWriteResponseHandler<?> responseHandler)
    {
        PAXOS_COMMIT_REQ.stage.maybeExecuteImmediately(new LocalMutationRunnable(localReplica)
        {
            public void runMayThrow()
            {
                try
                {
                    PaxosState.commit(message.payload, p -> mutator.onAppliedProposal(p));
                    if (responseHandler != null)
                        responseHandler.onResponse(null);
                }
                catch (Exception ex)
                {
                    if (!(ex instanceof WriteTimeoutException))
                        logger.error("Failed to apply paxos commit locally : ", ex);
                    responseHandler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.forException(ex));
                }
            }

            @Override
            protected Verb verb()
            {
                return PAXOS_COMMIT_REQ;
            }
        });
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistencyLevel the consistency level for the operation
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static void mutate(List<? extends IMutation> mutations,
                              ConsistencyLevel consistencyLevel,
                              long queryStartNanoTime,
                              ClientRequestsMetrics metrics,
                              ClientState state)
    throws UnavailableException, OverloadedException, WriteTimeoutException, WriteFailureException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();

        QueryInfoTracker.WriteTracker writeTracker = queryTracker().onWrite(state, false, mutations, consistencyLevel);

        // Request sensors are utilized to track usages from replicas serving a write request
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(mutations.stream().map(IMutation::getKeyspaceName).toArray(String[]::new));
        ExecutorLocals locals = ExecutorLocals.create(sensors);
        ExecutorLocals.set(locals);

        long startTime = System.nanoTime();

        List<AbstractWriteResponseHandler<IMutation>> responseHandlers = new ArrayList<>(mutations.size());
        WriteType plainWriteType = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;

        try
        {
            for (IMutation mutation : mutations)
            {
                // register the sensors for the mutation before the actual write is performed
                for (PartitionUpdate pu: mutation.getPartitionUpdates())
                {
                    if (pu.metadata().isIndex()) continue;
                    sensors.registerSensor(Context.from(pu.metadata()), Type.WRITE_BYTES);
                }

                if (mutation instanceof CounterMutation)
                    responseHandlers.add(mutateCounter((CounterMutation)mutation, localDataCenter, queryStartNanoTime));
                else
                    responseHandlers.add(mutator.mutateStandard((Mutation)mutation, consistencyLevel, localDataCenter, standardWritePerformer, null, plainWriteType, queryStartNanoTime));
            }

            // upgrade to full quorum any failed cheap quorums
            for (int i = 0 ; i < mutations.size() ; ++i)
            {
                if (!(mutations.get(i) instanceof CounterMutation) && mutator instanceof DefaultMutator) // at the moment, only non-counter writes support cheap quorums
                    responseHandlers.get(i).maybeTryAdditionalReplicas(mutations.get(i), standardWritePerformer, localDataCenter);
            }

            // wait for writes.  throws TimeoutException if necessary
            for (AbstractWriteResponseHandler<IMutation> responseHandler : responseHandlers)
                responseHandler.get();

            writeTracker.onDone();
        }
        catch (WriteTimeoutException|WriteFailureException ex)
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
            {
                hintMutations(mutations);
            }
            else
            {
                if (ex instanceof WriteFailureException)
                {
                    metrics.writeMetrics.failures.mark();
                    metrics.writeMetricsForLevel(consistencyLevel).failures.mark();
                    WriteFailureException fe = (WriteFailureException)ex;
                    Tracing.trace("Write failure; received {} of {} required replies, failed {} requests",
                                  fe.received, fe.blockFor, fe.failureReasonByEndpoint.size());
                }
                else
                {
                    metrics.writeMetrics.timeouts.mark();
                    metrics.writeMetricsForLevel(consistencyLevel).timeouts.mark();
                    WriteTimeoutException te = (WriteTimeoutException)ex;
                    Tracing.trace("Write timeout; received {} of {} required replies", te.received, te.blockFor);
                }
                writeTracker.onError(ex);
                throw ex;
            }
        }
        catch (UnavailableException e)
        {
            metrics.writeMetrics.unavailables.mark();
            metrics.writeMetricsForLevel(consistencyLevel).unavailables.mark();
            Tracing.trace("Unavailable");
            writeTracker.onError(e);
            throw e;
        }
        catch (OverloadedException e)
        {
            metrics.writeMetrics.unavailables.mark();
            metrics.writeMetricsForLevel(consistencyLevel).unavailables.mark();
            Tracing.trace("Overloaded");
            writeTracker.onError(e);
            throw e;
        }
        finally
        {
            long endTime = System.nanoTime();
            long latency = endTime - startTime;
            metrics.writeMetrics.executionTimeMetrics.addNano(latency);
            metrics.writeMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            metrics.writeMetricsForLevel(consistencyLevel).executionTimeMetrics.addNano(latency);
            metrics.writeMetricsForLevel(consistencyLevel).serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            updateCoordinatorWriteLatencyTableMetric(mutations, latency);
        }
    }

    /**
     * Hint all the mutations (except counters, which can't be safely retried).  This means
     * we'll re-hint any successful ones; doesn't seem worth it to track individual success
     * just for this unusual case.
     *
     * Only used for CL.ANY
     *
     * @param mutations the mutations that require hints
     */
    private static void hintMutations(Collection<? extends IMutation> mutations)
    {
        for (IMutation mutation : mutations)
            if (!(mutation instanceof CounterMutation))
                hintMutation((Mutation) mutation);

        Tracing.trace("Wrote hints to satisfy CL.ANY after no replicas acknowledged the write");
    }

    private static void hintMutation(Mutation mutation)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();

        // local writes can timeout, but cannot be dropped (see LocalMutationRunnable and CASSANDRA-6510),
        // so there is no need to hint or retry.
        EndpointsForToken replicasToHint = ReplicaLayout.forTokenWriteLiveAndDown(Keyspace.open(keyspaceName), token)
                .all()
                .filter(StorageProxy::shouldHint);

        submitHint(mutation, replicasToHint, null);
    }

    public boolean appliesLocally(Mutation mutation)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();

        return ReplicaLayout.forTokenWriteLiveAndDown(Keyspace.open(keyspaceName), token)
                .all().endpoints().contains(local);
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param writeOptions describes desired write properties
     * @param baseComplete time from epoch in ms that the local base mutation was(or will be) completed
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static void mutateMV(ByteBuffer dataKey, Collection<Mutation> mutations, WriteOptions writeOptions, AtomicLong baseComplete, long queryStartNanoTime)
            throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();

        long startTime = System.nanoTime();
        String ks = mutations.iterator().next().getKeyspaceName();
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(ks);

        try
        {
            // if we haven't joined the ring, write everything to batchlog because paired replicas may be stale
            final UUID batchUUID = UUIDGen.getTimeUUID();

            if (StorageService.instance.isStarting() || StorageService.instance.isJoining() || StorageService.instance.isMoving())
            {
                BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(),
                        mutations), writeOptions);
            }
            else
            {
                List<WriteResponseHandlerWrapper> wrappers = new ArrayList<>(mutations.size());
                //non-local mutations rely on the base mutation commit-log entry for eventual consistency
                Set<Mutation> nonLocalMutations = new HashSet<>(mutations);
                Token baseToken = StorageService.instance.getTokenMetadataForKeyspace(ks).partitioner.getToken(dataKey);

                ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

                //Since the base -> view replication is 1:1 we only need to store the BL locally
                ReplicaPlan.ForTokenWrite replicaPlan = ReplicaPlans.forLocalBatchlogWrite();
                BatchlogCleanup cleanup = new BatchlogCleanup(mutations.size(),
                                                              () -> asyncRemoveFromBatchlog(replicaPlan, batchUUID));

                // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
                for (Mutation mutation : mutations)
                {
                    String keyspaceName = mutation.getKeyspaceName();
                    Token tk = mutation.key().getToken();
                    AbstractReplicationStrategy replicationStrategy = Keyspace.open(keyspaceName).getReplicationStrategy();
                    Optional<Replica> pairedEndpoint = ViewUtils.getViewNaturalEndpoint(replicationStrategy, baseToken, tk);
                    EndpointsForToken pendingReplicas = StorageService.instance.getTokenMetadataForKeyspace(keyspaceName).pendingEndpointsForToken(tk, keyspaceName);

                    // if there are no paired endpoints there are probably range movements going on, so we write to the local batchlog to replay later
                    if (!pairedEndpoint.isPresent())
                    {
                        if (pendingReplicas.isEmpty())
                            logger.warn("Received base materialized view mutation for key {} that does not belong " +
                                        "to this node. There is probably a range movement happening (move or decommission)," +
                                        "but this node hasn't updated its ring metadata yet. Adding mutation to " +
                                        "local batchlog to be replayed later.",
                                        mutation.key());
                        continue;
                    }

                    // When local node is the endpoint we can just apply the mutation locally,
                    // unless there are pending endpoints, in which case we want to do an ordinary
                    // write so the view mutation is sent to the pending endpoint
                    if (pairedEndpoint.get().isSelf() && StorageService.instance.isJoined()
                        && pendingReplicas.isEmpty())
                    {
                        try
                        {
                            mutation.apply(writeOptions);
                            nonLocalMutations.remove(mutation);
                            // won't trigger cleanup
                            cleanup.decrement();
                        }
                        catch (Exception exc)
                        {
                            logger.error("Error applying local view update: Mutation (keyspace {}, tables {}, partition key {})",
                                         mutation.getKeyspaceName(), mutation.getTableIds(), mutation.key());
                            throw exc;
                        }
                    }
                    else
                    {
                        ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWrite(replicationStrategy,
                                                                                              EndpointsForToken.of(tk, pairedEndpoint.get()),
                                                                                              pendingReplicas);
                        wrappers.add(wrapViewBatchResponseHandler(mutation,
                                                                  consistencyLevel,
                                                                  consistencyLevel,
                                                                  liveAndDown,
                                                                  baseComplete,
                                                                  WriteType.BATCH,
                                                                  cleanup,
                                                                  queryStartNanoTime,
                                                                  metrics));
                    }
                }

                // Apply to local batchlog memtable in this thread
                if (!nonLocalMutations.isEmpty())
                    BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), nonLocalMutations), writeOptions);

                // Perform remote writes
                if (!wrappers.isEmpty())
                    asyncWriteBatchedMutations(wrappers, localDataCenter, Stage.VIEW_MUTATION);
            }
        }
        finally
        {
            final long endTime = System.nanoTime();
            metrics.viewWriteMetrics.executionTimeMetrics.addNano(endTime - startTime);
            metrics.viewWriteMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mutateWithTriggers(List<? extends IMutation> mutations,
                                          ConsistencyLevel consistencyLevel,
                                          boolean mutateAtomically,
                                          long queryStartNanoTime,
                                          ClientState state)
    throws WriteTimeoutException, WriteFailureException, UnavailableException, OverloadedException, InvalidRequestException
    {
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);
        String keyspaceName = mutations.iterator().next().getKeyspaceName();
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(keyspaceName);

        boolean updatesView = Keyspace.open(mutations.iterator().next().getKeyspaceName())
                              .viewManager
                              .updatesAffectView(mutations, true);

        long size = IMutation.dataSize(mutations);
        metrics.writeMetrics.mutationSize.update(size);
        metrics.writeMetricsForLevel(consistencyLevel).mutationSize.update(size);

        if (augmented != null)
            mutateAtomically(augmented, consistencyLevel, updatesView, queryStartNanoTime, metrics, state);
        else
        {
            if (mutateAtomically || updatesView)
                mutateAtomically((Collection<Mutation>) mutations, consistencyLevel, updatesView, queryStartNanoTime, metrics, state);
            else
                mutate(mutations, consistencyLevel, queryStartNanoTime, metrics, state);
        }
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     *      write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param mutations the Mutations to be applied across the replicas
     * @param consistencyLevel the consistency level for the operation
     * @param requireQuorumForRemove at least a quorum of nodes will see update before deleting batchlog
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static void mutateAtomically(Collection<Mutation> mutations,
                                        ConsistencyLevel consistencyLevel,
                                        boolean requireQuorumForRemove,
                                        long queryStartNanoTime,
                                        ClientRequestsMetrics metrics,
                                        ClientState clientState)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        mutator.mutateAtomically(mutations, consistencyLevel, requireQuorumForRemove, queryStartNanoTime, metrics, clientState);
    }

    public static void updateCoordinatorWriteLatencyTableMetric(Collection<? extends IMutation> mutations, long latency)
    {
        if (null == mutations)
        {
            return;
        }

        try
        {
            //We could potentially pass a callback into performWrite. And add callback provision for mutateCounter or mutateAtomically (sendToHintedEndPoints)
            //However, Trade off between write metric per CF accuracy vs performance hit due to callbacks. Similar issue exists with CoordinatorReadLatency metric.
            mutations.stream()
                     .flatMap(m -> m.getTableIds().stream().map(tableId -> Keyspace.open(m.getKeyspaceName()).getColumnFamilyStore(tableId)))
                     .distinct()
                     .forEach(store -> store.metric.coordinatorWriteLatency.update(latency, TimeUnit.NANOSECONDS));
        }
        catch (Exception ex)
        {
            logger.warn("Exception occurred updating coordinatorWriteLatency metric", ex);
        }
    }

    private static void syncWriteToBatchlog(Collection<Mutation> mutations, ReplicaPlan.ForTokenWrite replicaPlan, UUID uuid, long queryStartNanoTime)
    throws WriteTimeoutException, WriteFailureException
    {
        WriteResponseHandler<?> handler = new WriteResponseHandler(replicaPlan,
                                                                   WriteType.BATCH_LOG,
                                                                   queryStartNanoTime);

        Batch batch = Batch.createLocal(uuid, FBUtilities.timestampMicros(), mutations);
        Message<Batch> message = Message.out(BATCH_STORE_REQ, batch);
        for (Replica replica : replicaPlan.liveAndDown())
        {
            logger.trace("Sending batchlog store request {} to {} for {} mutations", batch.id, replica, batch.size());

            if (replica.isSelf())
                performLocally(Stage.MUTATION, replica, () -> BatchlogManager.store(batch), handler);
            else
                MessagingService.instance().sendWithCallback(message, replica.endpoint(), handler);
        }
        handler.get();
    }

    protected static void asyncRemoveFromBatchlog(ReplicaPlan.ForTokenWrite replicaPlan, UUID uuid)
    {
        Message<UUID> message = Message.out(Verb.BATCH_REMOVE_REQ, uuid);
        for (Replica target : replicaPlan.contacts())
        {
            if (logger.isTraceEnabled())
                logger.trace("Sending batchlog remove request {} to {}", uuid, target);

            if (target.isSelf())
                performLocally(Stage.MUTATION, target, () -> BatchlogManager.remove(uuid));
            else
                MessagingService.instance().send(message, target.endpoint());
        }
    }

    private static void asyncWriteBatchedMutations(List<WriteResponseHandlerWrapper> wrappers, String localDataCenter, Stage stage)
    {
        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Replicas.temporaryAssertFull(wrapper.handler.replicaPlan.liveAndDown());  // TODO: CASSANDRA-14549
            ReplicaPlan.ForTokenWrite replicas = wrapper.handler.replicaPlan.withContact(wrapper.handler.replicaPlan.liveAndDown());

            try
            {
                sendToHintedReplicas(wrapper.mutation, replicas, wrapper.handler, localDataCenter, stage);
            }
            catch (OverloadedException | WriteTimeoutException e)
            {
                wrapper.handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.forException(e));
            }
        }
    }

    /**
     * Perform the write of a mutation given a WritePerformer.
     * Gather the list of write endpoints, apply locally and/or forward the mutation to
     * said write endpoint (deletaged to the actual WritePerformer) and wait for the
     * responses based on consistency level.
     *
     * @param mutation the mutation to be applied
     * @param consistencyLevel the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     * @param callback an optional callback to be run if and when the write is
     * @param queryStartNanoTime the value of System.nanoTime() when the query started to be processed
     */
    public static AbstractWriteResponseHandler<IMutation> performWrite(IMutation mutation,
                                                                       ConsistencyLevel consistencyLevel,
                                                                       String localDataCenter,
                                                                       WritePerformer performer,
                                                                       Runnable callback,
                                                                       WriteType writeType,
                                                                       long queryStartNanoTime)
    {
        AbstractWriteResponseHandler<IMutation> responseHandler = getWriteResponseHandler(mutation, consistencyLevel, callback, writeType, queryStartNanoTime);
        performer.apply(mutation, responseHandler.replicaPlan(), responseHandler, localDataCenter);
        return responseHandler;
    }

    // same as performWrites except does not initiate writes (but does perform availability checks).
    public static WriteResponseHandlerWrapper wrapBatchResponseHandler(Mutation mutation,
                                                                       ConsistencyLevel consistencyLevel,
                                                                       ConsistencyLevel batchConsistencyLevel,
                                                                       WriteType writeType,
                                                                       BatchlogResponseHandler.BatchlogCleanup cleanup,
                                                                       long queryStartNanoTime)
    {
        AbstractWriteResponseHandler<IMutation> writeHandler = StorageProxy.getWriteResponseHandler(mutation, consistencyLevel,null,  writeType, queryStartNanoTime);
        int batchlogBlockFor = batchConsistencyLevel.blockFor(writeHandler.replicaPlan().replicationStrategy());
        BatchlogResponseHandler<IMutation> batchHandler = new BatchlogResponseHandler<>(writeHandler, batchlogBlockFor, cleanup, queryStartNanoTime);
        return new WriteResponseHandlerWrapper(batchHandler, mutation);
    }

    public static AbstractWriteResponseHandler<IMutation> getWriteResponseHandler(IMutation mutation,
                                                                                  ConsistencyLevel consistencyLevel,
                                                                                  @Nullable Runnable callback,
                                                                                  WriteType writeType,
                                                                                  long queryStartNanoTime)
    {
        Keyspace keyspace = mutation.getKeyspace();
        Token tk = mutation.key().getToken();

        ReplicaPlan.ForTokenWrite replicaPlan = ReplicaPlans.forWrite(keyspace, consistencyLevel, tk, ReplicaPlans.writeNormal);
        AbstractReplicationStrategy rs = replicaPlan.replicationStrategy();
        AbstractWriteResponseHandler<IMutation> responseHandler = rs.getWriteResponseHandler(replicaPlan, callback, writeType, queryStartNanoTime);
        if (callback instanceof CounterMutationCallback)
        {
            ((CounterMutationCallback) callback).setReplicaCount(replicaPlan.contacts().size());
        }
        return responseHandler;
    }

    /**
     * Same as performWrites except does not initiate writes (but does perform availability checks).
     * Keeps track of ViewWriteMetrics
     */
    private static WriteResponseHandlerWrapper wrapViewBatchResponseHandler(Mutation mutation,
                                                                            ConsistencyLevel consistencyLevel,
                                                                            ConsistencyLevel batchConsistencyLevel,
                                                                            ReplicaLayout.ForTokenWrite liveAndDown,
                                                                            AtomicLong baseComplete,
                                                                            WriteType writeType,
                                                                            BatchlogCleanup cleanup,
                                                                            long queryStartNanoTime,
                                                                            ClientRequestsMetrics metrics)
    {
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        ReplicaPlan.ForTokenWrite replicaPlan = ReplicaPlans.forWrite(keyspace, consistencyLevel, liveAndDown, ReplicaPlans.writeAll);
        AbstractReplicationStrategy replicationStrategy = replicaPlan.replicationStrategy();
        AbstractWriteResponseHandler<IMutation> writeHandler = replicationStrategy.getWriteResponseHandler(replicaPlan, () -> {
            long delay = Math.max(0, System.currentTimeMillis() - baseComplete.get());
            metrics.viewWriteMetrics.viewWriteLatency.update(delay, MILLISECONDS);
        }, writeType, queryStartNanoTime);
        BatchlogResponseHandler<IMutation> batchHandler = new ViewWriteMetricsWrapped(writeHandler, batchConsistencyLevel.blockFor(replicationStrategy), cleanup, queryStartNanoTime, metrics);
        return new WriteResponseHandlerWrapper(batchHandler, mutation);
    }

    // used by atomic_batch_mutate to decouple availability check from the write itself, caches consistency level and endpoints.
    public static class WriteResponseHandlerWrapper
    {
        public final BatchlogResponseHandler<IMutation> handler;
        public final Mutation mutation;

        public WriteResponseHandlerWrapper(BatchlogResponseHandler<IMutation> handler, Mutation mutation)
        {
            this.handler = handler;
            this.mutation = mutation;
        }
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     * <pre>
     * {@code
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * }
     * </pre>
     *
     * @throws OverloadedException if the hints cannot be written/enqueued
     */
    public static void sendToHintedReplicas(final Mutation mutation,
                                            ReplicaPlan.ForTokenWrite plan,
                                            AbstractWriteResponseHandler<IMutation> responseHandler,
                                            String localDataCenter,
                                            Stage stage)
    throws OverloadedException
    {
        // this dc replicas:
        Collection<Replica> localDc = null;
        // extra-datacenter replicas, grouped by dc
        Map<String, Collection<Replica>> dcGroups = null;
        // only need to create a Message for non-local writes
        Message<Mutation> message = null;

        boolean insertLocal = false;
        Replica localReplica = null;
        Collection<Replica> endpointsToHint = null;

        List<InetAddressAndPort> backPressureHosts = null;

        // For performance, Mutation caches serialized buffers that are computed lazily in serializedBuffer(). That
        // computation is not synchronized however and we will potentially call that method concurrently for each
        // dispatched message (not that concurrent calls to serializedBuffer() are "unsafe" per se, just that they
        // may result in multiple computations, making the caching optimization moot). So forcing the serialization
        // here to make sure it's already cached/computed when it's concurrently used later.
        // Side note: we have one cached buffers for each used EncodingVersion and this only pre-compute the one for
        // the current version, but it's just an optimization and we're ok not optimizing for mixed-version clusters.
        Mutation.serializer.prepareSerializedBuffer(mutation, MessagingService.current_version);

        for (Replica destination : plan.contacts())
        {
            checkHintOverload(destination);

            if (plan.isAlive(destination))
            {
                if (destination.isSelf())
                {
                    insertLocal = true;
                    localReplica = destination;
                }
                else
                {
                    // belongs on a different server
                    if (message == null)
                        message = Message.outWithFlag(MUTATION_REQ, mutation, MessageFlag.CALL_BACK_ON_FAILURE);

                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);

                    // direct writes to local DC or old Cassandra versions
                    // (1.1 knows how to forward old-style String message IDs; updated to int in 2.0)
                    if (localDataCenter.equals(dc))
                    {
                        if (localDc == null)
                            localDc = new ArrayList<>(plan.contacts().size());

                        localDc.add(destination);
                    }
                    else
                    {
                        if (dcGroups == null)
                            dcGroups = new HashMap<>();

                        Collection<Replica> messages = dcGroups.get(dc);
                        if (messages == null)
                            messages = dcGroups.computeIfAbsent(dc, (v) -> new ArrayList<>(3)); // most DCs will have <= 3 replicas

                        messages.add(destination);
                    }

                    if (backPressureHosts == null)
                        backPressureHosts = new ArrayList<>(plan.contacts().size());

                    backPressureHosts.add(destination.endpoint());
                }
            }
            else
            {
                //Immediately mark the response as expired since the request will not be sent
                responseHandler.expired();
                if (shouldHint(destination))
                {
                    if (endpointsToHint == null)
                        endpointsToHint = new ArrayList<>();

                    endpointsToHint.add(destination);
                }
            }
        }

        if (endpointsToHint != null)
            submitHint(mutation, EndpointsForToken.copyOf(mutation.key().getToken(), endpointsToHint), responseHandler);

        if (insertLocal)
        {
            Preconditions.checkNotNull(localReplica);
            performLocally(stage, localReplica, mutation::apply, responseHandler);
        }

        if (localDc != null)
        {
            for (Replica destination : localDc)
                MessagingService.instance().sendWriteWithCallback(message, destination, responseHandler, true);
        }
        if (dcGroups != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            for (Collection<Replica> dcTargets : dcGroups.values())
                sendMessagesToNonlocalDC(message, EndpointsForToken.copyOf(mutation.key().getToken(), dcTargets), responseHandler);
        }
    }

    private static void checkHintOverload(Replica destination)
    {
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        if (StorageMetrics.totalHintsInProgress.getCount() > maxHintsInProgress
                && (getHintsInProgressFor(destination.endpoint()).get() > 0 && shouldHint(destination)))
        {
            throw new OverloadedException("Too many in flight hints: " + StorageMetrics.totalHintsInProgress.getCount() +
                                          " destination: " + destination +
                                          " destination hints: " + getHintsInProgressFor(destination.endpoint()).get());
        }
    }

    /*
     * Send the message to the first replica of targets, and have it forward the message to others in its DC
     */
    private static void sendMessagesToNonlocalDC(Message<? extends IMutation> message,
                                                 EndpointsForToken targets,
                                                 AbstractWriteResponseHandler<IMutation> handler)
    {
        final Replica target;

        if (targets.size() > 1)
        {
            target = targets.get(ThreadLocalRandom.current().nextInt(0, targets.size()));
            EndpointsForToken forwardToReplicas = targets.filter(r -> r != target, targets.size());

            for (Replica replica : forwardToReplicas)
            {
                MessagingService.instance().callbacks.addWithExpiration(handler, message, replica, handler.replicaPlan.consistencyLevel(), true);
                logger.trace("Adding FWD message to {}@{}", message.id(), replica);
            }

            // starting with 4.0, use the same message id for all replicas
            long[] messageIds = new long[forwardToReplicas.size()];
            Arrays.fill(messageIds, message.id());

            message = message.withForwardTo(new ForwardingInfo(forwardToReplicas.endpointList(), messageIds));
        }
        else
        {
            target = targets.get(0);
        }

        MessagingService.instance().sendWriteWithCallback(message, target, handler, true);
        logger.trace("Sending message to {}@{}", message.id(), target);
    }

    private static void performLocally(Stage stage, Replica localReplica, final Runnable runnable)
    {
        stage.maybeExecuteImmediately(new LocalMutationRunnable(localReplica)
        {
            public void runMayThrow()
            {
                try
                {
                    runnable.run();
                }
                catch (Exception ex)
                {
                    logger.error("Failed to apply mutation locally : ", ex);
                }
            }

            @Override
            protected Verb verb()
            {
                return Verb.MUTATION_REQ;
            }
        });
    }

    private static void performLocally(Stage stage, Replica localReplica, final Runnable runnable, final RequestCallback<?> handler)
    {
        stage.maybeExecuteImmediately(new LocalMutationRunnable(localReplica)
        {
            public void runMayThrow()
            {
                try
                {
                    runnable.run();
                    handler.onResponse(null);
                }
                catch (Exception ex)
                {
                    if (!(ex instanceof WriteTimeoutException))
                        logger.error("Failed to apply mutation locally : ", ex);
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.forException(ex));
                }
            }

            @Override
            protected Verb verb()
            {
                return Verb.MUTATION_REQ;
            }
        });
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime) throws UnavailableException, OverloadedException
    {
        return mutator.mutateCounter(cm, localDataCenter, queryStartNanoTime);
    }

    private static AbstractWriteResponseHandler<IMutation> defaultMutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime) throws UnavailableException, OverloadedException
    {
        Replica replica = findSuitableReplica(cm.getKeyspaceName(), cm.key(), localDataCenter, cm.consistency());
        if (replica.isSelf())
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter, queryStartNanoTime);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String keyspaceName = cm.getKeyspaceName();
            Keyspace keyspace = Keyspace.open(keyspaceName);
            Token tk = cm.key().getToken();

            // we build this ONLY to perform the sufficiency check that happens on construction
            ReplicaPlans.forWrite(keyspace, cm.consistency(), tk, ReplicaPlans.writeAll);

            // Forward the actual update to the chosen leader replica
            AbstractWriteResponseHandler<IMutation> responseHandler = new WriteResponseHandler<>(ReplicaPlans.forForwardingCounterWrite(keyspace, tk, replica), WriteType.COUNTER, queryStartNanoTime);

            Tracing.trace("Enqueuing counter update to {}", replica);
            Message message = Message.outWithFlag(Verb.COUNTER_MUTATION_REQ, cm, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().sendWriteWithCallback(message, replica, responseHandler, false);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     */
    private static Replica findSuitableReplica(String keyspaceName, DecoratedKey key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        EndpointsForToken replicas = replicationStrategy.getNaturalReplicasForToken(key);

        // CASSANDRA-13043: filter out those endpoints not live yet, maybe because still bootstrapping
        // We have a keyspace, so filter by affinity too
        replicas = replicas.filter(IFailureDetector.isReplicaAlive)
                           .filter(snitch.filterByAffinityForReads(keyspace.getName()));

        // Counter leader involves local read, coordinator will prioritize faster replica if configured
        boolean endpointsSorted = false;
        if (DatabaseDescriptor.isDynamicEndpointSnitch() && useDynamicSnitchForCounterLeader)
        {
            replicas = snitch.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
            endpointsSorted = true;
        }

        // TODO have a way to compute the consistency level
        if (replicas.isEmpty())
            throw UnavailableException.create(cl, cl.blockFor(replicationStrategy), 0);

        List<Replica> localReplicas = new ArrayList<>(replicas.size());

        for (Replica replica : replicas)
            if (snitch.getDatacenter(replica).equals(localDataCenter))
                localReplicas.add(replica);

        if (localReplicas.isEmpty())
        {
            // If the consistency required is local then we should not involve other DCs
            if (cl.isDatacenterLocal())
                throw UnavailableException.create(cl, cl.blockFor(replicationStrategy), 0);

            // No endpoint in local DC, pick the closest endpoint according to the snitch
            if (!endpointsSorted)
                replicas = snitch.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
            return replicas.get(0);
        }

        // if it's ordered, exclude the slowest one and pick randomly from the rest to avoid overloading single replica
        int replicasToPick = localReplicas.size();
        if (endpointsSorted && localReplicas.size() > 1)
            replicasToPick--;
        return localReplicas.get(ThreadLocalRandom.current().nextInt(replicasToPick));
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static AbstractWriteResponseHandler<IMutation> applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, Runnable callback, long queryStartNanoTime)
    throws UnavailableException, OverloadedException
    {
        return mutator.mutateCounterOnLeader(cm, localDataCenter, counterWritePerformer, callback, queryStartNanoTime);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static AbstractWriteResponseHandler<IMutation> applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter, long queryStartNanoTime)
    throws UnavailableException, OverloadedException
    {
        return mutator.mutateCounterOnLeader(cm, localDataCenter, counterWriteOnCoordinatorPerformer, null, queryStartNanoTime);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final ReplicaPlan.ForTokenWrite replicaPlan,
                                             final AbstractWriteResponseHandler<IMutation> responseHandler,
                                             final String localDataCenter)
    {
        return new DroppableRunnable(Verb.COUNTER_MUTATION_REQ)
        {
            @Override
            public void runMayThrow() throws OverloadedException, WriteTimeoutException
            {
                assert mutation instanceof CounterMutation;

                Mutation result = ((CounterMutation) mutation).applyCounterMutation();
                responseHandler.onResponse(null);
                mutator.onAppliedCounter(result, responseHandler);
                sendToHintedReplicas(result, replicaPlan, responseHandler, localDataCenter, Stage.COUNTER_MUTATION);
            }
        };
    }

    private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!SchemaConstants.isLocalSystemKeyspace(cmd.metadata().keyspace))
                return false;
        return true;
    }

    public static RowIterator readOne(SinglePartitionReadCommand command,
                                      ConsistencyLevel consistencyLevel,
                                      long queryStartNanoTime,
                                      QueryInfoTracker.ReadTracker readTracker)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return readOne(command, consistencyLevel, null, queryStartNanoTime, readTracker);
    }

    public static RowIterator readOne(SinglePartitionReadCommand command,
                                      ConsistencyLevel consistencyLevel,
                                      QueryState queryState,
                                      long queryStartNanoTime,
                                      QueryInfoTracker.ReadTracker readTracker)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return PartitionIterators.getOnlyElement(read(SinglePartitionReadCommand.Group.one(command),
                                                      consistencyLevel,
                                                      queryState,
                                                      queryStartNanoTime,
                                                      readTracker),
                                                 command);
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group,
                                         ConsistencyLevel consistencyLevel,
                                         QueryState queryState,
                                         long queryStartNanoTime)
    {
        QueryInfoTracker.ReadTracker readTracker = StorageProxy.queryTracker().onRead(queryState.getClientState(),
                                                                                      group.metadata(),
                                                                                      group.queries,
                                                                                      consistencyLevel);
        // Request sensors are utilized to track usages from replicas serving a read request
        RequestSensors requestSensors = SensorsFactory.instance.createRequestSensors(group.metadata().keyspace);
        Context context = Context.from(group.metadata());
        requestSensors.registerSensor(context, Type.READ_BYTES);
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);
        PartitionIterator partitions = read(group, consistencyLevel, queryState, queryStartNanoTime, readTracker);
        partitions = PartitionIterators.filteredRowTrackingIterator(partitions, readTracker::onFilteredPartition, readTracker::onFilteredRow, readTracker::onFilteredRow);
        return PartitionIterators.doOnClose(partitions, readTracker::onDone);
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static PartitionIterator read(SinglePartitionReadCommand.Group group,
                                         ConsistencyLevel consistencyLevel,
                                         QueryState queryState,
                                         long queryStartNanoTime,
                                         QueryInfoTracker.ReadTracker readTracker)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(group.metadata().keyspace);
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(group.metadata());

        if (!cfs.isReadyToServeData() && !systemKeyspaceQuery(group.queries))
        {
            metrics.readMetrics.unavailables.mark();
            metrics.readMetricsForLevel(consistencyLevel).unavailables.mark();
            throw new IsBootstrappingException();
        }

        return consistencyLevel.isSerialConsistency()
             ? readWithPaxos(group, consistencyLevel, queryState, queryStartNanoTime, readTracker)
             : readRegular(group, consistencyLevel, queryStartNanoTime, readTracker);
    }

    /**
     * Performs a read for paxos reads and paxos writes (cas method)
      */
    private static PartitionIterator readWithPaxos(SinglePartitionReadCommand.Group group,
                                                   ConsistencyLevel consistencyLevel,
                                                   QueryState queryState,
                                                   long queryStartNanoTime,
                                                   QueryInfoTracker.ReadTracker readTracker)
    throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        assert queryState != null;
        if (group.queries.size() > 1)
            throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");

        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(group.metadata().keyspace);
        long start = System.nanoTime();
        SinglePartitionReadCommand command = group.queries.get(0);
        TableMetadata metadata = command.metadata();
        DecoratedKey key = command.partitionKey();
        // calculate the blockFor before repair any paxos round to avoid RS being altered in between.
        int blockForRead = consistencyLevel.blockFor(Keyspace.open(metadata.keyspace).getReplicationStrategy());

        PartitionIterator result = null;
        try
        {
            final ConsistencyLevel consistencyForReplayCommitsOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL
                                                                        ? ConsistencyLevel.LOCAL_QUORUM
                                                                        : ConsistencyLevel.QUORUM;

            try
            {
                // Commit an empty update to make sure all in-progress updates that should be finished first is, _and_
                // that no other in-progress can get resurrected.
                Supplier<Pair<PartitionUpdate, RowIterator>> updateProposer =
                    disableSerialReadLinearizability
                    ? () -> null
                    : () -> Pair.create(PartitionUpdate.emptyUpdate(metadata, key), null);
                // When replaying, we commit at quorum/local quorum, as we want to be sure the following read (done at
                // quorum/local_quorum) sees any replayed updates. Our own update is however empty, and those don't even
                // get committed due to an optimiation described in doPaxos/beingRepairAndPaxos, so the commit
                // consistency is irrelevant (we use ANY just to emphasis that we don't wait on our commit).
                doPaxos(metadata,
                        key,
                        consistencyLevel,
                        consistencyForReplayCommitsOrFetch,
                        ConsistencyLevel.ANY,
                        queryState,
                        start,
                        metrics.casReadMetrics,
                        updateProposer,
                        true); // skip guardrail for ANY which is blocked by CNDB
            }
            catch (WriteTimeoutException e)
            {
                throw new ReadTimeoutException(consistencyLevel, 0, blockForRead, false);
            }
            catch (WriteFailureException e)
            {
                throw new ReadFailureException(consistencyLevel, e.received, e.blockFor, false, e.failureReasonByEndpoint);
            }

            result = fetchRows(group.queries, consistencyForReplayCommitsOrFetch, queryStartNanoTime, readTracker);
        }
        catch (CasWriteUnknownResultException e)
        {
            metrics.casReadMetrics.unknownResult.mark();
            readTracker.onError(e);
            throw e;
        }
        catch (UnavailableException e)
        {
            metrics.readMetrics.unavailables.mark();
            metrics.casReadMetrics.unavailables.mark();
            metrics.readMetricsForLevel(consistencyLevel).unavailables.mark();
            readTracker.onError(e);
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            metrics.readMetrics.timeouts.mark();
            metrics.casReadMetrics.timeouts.mark();
            metrics.readMetricsForLevel(consistencyLevel).timeouts.mark();
            readTracker.onError(e);
            throw e;
        }
        catch (ReadFailureException e)
        {
            metrics.readMetrics.failures.mark();
            metrics.casReadMetrics.failures.mark();
            metrics.readMetricsForLevel(consistencyLevel).failures.mark();
            readTracker.onError(e);
            throw e;
        }
        finally
        {
            long endTime = System.nanoTime();
            long latency = endTime - start;
            metrics.readMetrics.executionTimeMetrics.addNano(latency);
            metrics.readMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            metrics.casReadMetrics.executionTimeMetrics.addNano(latency);
            metrics.casReadMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            metrics.readMetricsForLevel(consistencyLevel).executionTimeMetrics.addNano(latency);
            metrics.readMetricsForLevel(consistencyLevel).serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }
        return result;
    }

    @SuppressWarnings("resource")
    private static PartitionIterator readRegular(SinglePartitionReadCommand.Group group,
                                                 ConsistencyLevel consistencyLevel,
                                                 long queryStartNanoTime,
                                                 QueryInfoTracker.ReadTracker readTracker)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        ClientRequestsMetrics metrics = ClientRequestsMetricsProvider.instance.metrics(group.metadata().keyspace);
        long start = System.nanoTime();
        try
        {
            PartitionIterator result = fetchRows(group.queries, consistencyLevel, queryStartNanoTime, readTracker);
            // Note that the only difference between the command in a group must be the partition key on which
            // they applied.
            boolean enforceStrictLiveness = group.queries.get(0).metadata().enforceStrictLiveness();
            // If we have more than one command, then despite each read command honoring the limit, the total result
            // might not honor it and so we should enforce it
            if (group.queries.size() > 1)
                result = group.limits().filter(result, group.nowInSec(), group.selectsFullPartition(), enforceStrictLiveness);
            return result;
        }
        catch (UnavailableException e)
        {
            metrics.readMetrics.unavailables.mark();
            metrics.readMetricsForLevel(consistencyLevel).unavailables.mark();
            readTracker.onError(e);
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            metrics.readMetrics.timeouts.mark();
            metrics.readMetricsForLevel(consistencyLevel).timeouts.mark();
            readTracker.onError(e);
            throw e;
        }
        catch (ReadFailureException e)
        {
            metrics.readMetrics.failures.mark();
            metrics.readMetricsForLevel(consistencyLevel).failures.mark();
            readTracker.onError(e);
            throw e;
        }
        finally
        {
            long endTime = System.nanoTime();
            long latency = endTime - start;
            metrics.readMetrics.executionTimeMetrics.addNano(latency);
            metrics.readMetrics.serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            metrics.readMetricsForLevel(consistencyLevel).executionTimeMetrics.addNano(latency);
            metrics.readMetricsForLevel(consistencyLevel).serviceTimeMetrics.addNano(endTime - queryStartNanoTime);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : group.queries)
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }
    }

    public static PartitionIterator concatAndBlockOnRepair(List<PartitionIterator> iterators, List<ReadRepair<?, ?>> repairs)
    {
        PartitionIterator concatenated = PartitionIterators.concat(iterators);

        if (repairs.isEmpty())
            return concatenated;

        return new PartitionIterator()
        {
            public void close()
            {
                concatenated.close();
                repairs.forEach(ReadRepair::maybeSendAdditionalWrites);
                repairs.forEach(ReadRepair::awaitWrites);
            }

            public boolean hasNext()
            {
                return concatenated.hasNext();
            }

            public RowIterator next()
            {
                return concatenated.next();
            }
        };
    }

    /**
     * This function executes local and remote reads, and blocks for the results:
     *
     * 1. Get the replica locations, sorted by response time according to the snitch
     * 2. Send a data request to the closest replica, and digest requests to either
     *    a) all the replicas, if read repair is enabled
     *    b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     * 3. Wait for a response from R replicas
     * 4. If the digests (if any) match the data return the data
     * 5. else carry out read repair by getting data from all the nodes.
     */
    private static PartitionIterator fetchRows(List<SinglePartitionReadCommand> commands,
                                               ConsistencyLevel consistencyLevel,
                                               long queryStartNanoTime,
                                               QueryInfoTracker.ReadTracker readTracker)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        int cmdCount = commands.size();

        AbstractReadExecutor[] reads = new AbstractReadExecutor[cmdCount];

        // Get the replica locations, sorted by response time according to the snitch, and create a read executor
        // for type of speculation we'll use in this read
        for (int i=0; i<cmdCount; i++)
        {
            reads[i] = AbstractReadExecutor.getReadExecutor(commands.get(i), consistencyLevel, queryStartNanoTime, readTracker);
        }

        // sends a data request to the closest replica, and a digest request to the others. If we have a speculating
        // read executor, we'll only send read requests to enough replicas to satisfy the consistency level
        for (int i=0; i<cmdCount; i++)
        {
            reads[i].executeAsync();
        }

        // if we have a speculating read executor, and it looks like we may not receive a response from the initial
        // set of replicas we sent messages to, speculatively send an additional messages to an un-contacted replica
        for (int i=0; i<cmdCount; i++)
        {
            reads[i].maybeTryAdditionalReplicas();
        }

        // wait for enough responses to meet the consistency level. If there's a digest mismatch, begin the read
        // repair process by sending full data reads to all replicas we received responses from.
        for (int i=0; i<cmdCount; i++)
        {
            reads[i].awaitResponses();
        }

        // read repair - if it looks like we may not receive enough full data responses to meet CL, send
        // an additional request to any remaining replicas we haven't contacted (if there are any)
        for (int i=0; i<cmdCount; i++)
        {
            reads[i].maybeSendAdditionalDataRequests();
        }

        // read repair - block on full data responses
        for (int i=0; i<cmdCount; i++)
        {
            reads[i].awaitReadRepair();
        }

        // if we didn't do a read repair, return the contents of the data response, if we did do a read
        // repair, merge the full data reads
        List<PartitionIterator> results = new ArrayList<>(cmdCount);
        List<ReadRepair<?, ?>> repairs = new ArrayList<>(cmdCount);
        for (int i=0; i<cmdCount; i++)
        {
            results.add(reads[i].getResult());
            repairs.add(reads[i].getReadRepair());
        }

        // if we did a read repair, assemble repair mutation and block on them
        return concatAndBlockOnRepair(results, repairs);
    }

    public static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback handler;
        private final boolean trackRepairedStatus;

        public LocalReadRunnable(ReadCommand command, ReadCallback handler)
        {
            this(command, handler, false);
        }

        public LocalReadRunnable(ReadCommand command, ReadCallback handler, boolean trackRepairedStatus)
        {
            super(Verb.READ_REQ);
            this.command = command;
            this.handler = handler;
            this.trackRepairedStatus = trackRepairedStatus;
        }

        protected void runMayThrow()
        {
            try
            {
                command.setMonitoringTime(approxCreationTimeNanos, false, verb.expiresAfterNanos(), DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

                ReadResponse response;
                try (ReadExecutionController controller = command.executionController(trackRepairedStatus);
                     UnfilteredPartitionIterator iterator = command.executeLocally(controller))
                {
                    response = command.createResponse(iterator, controller.getRepairedDataInfo());
                }

                if (command.complete())
                {
                    handler.response(response);
                }
                else
                {
                    MessagingService.instance().metrics.recordSelfDroppedMessage(verb, MonotonicClock.approxTime.now() - approxCreationTimeNanos, NANOSECONDS);
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
                }

                MessagingService.instance().latencySubscribers.add(FBUtilities.getBroadcastAddressAndPort(), MonotonicClock.approxTime.now() - approxCreationTimeNanos, NANOSECONDS);
            }
            catch (Throwable t)
            {
                if (t instanceof TombstoneOverwhelmingException)
                {
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
                    logger.error(t.getMessage());
                }
                else
                {
                    handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.UNKNOWN);
                    throw t;
                }
            }
        }
    }

    public static PartitionIterator getRangeSlice(PartitionRangeReadCommand command,
                                                  ConsistencyLevel consistencyLevel,
                                                  long queryStartNanoTime,
                                                  ClientState clientState)
    {
        QueryInfoTracker.ReadTracker readTracker = queryTracker().onRangeRead(clientState,
                                                                              command.metadata(),
                                                                              command,
                                                                              consistencyLevel);
        // Request sensors are utilized to track usages from replicas serving a range request
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(command.metadata().keyspace);
        Context context = Context.from(command);
        sensors.registerSensor(context, Type.READ_BYTES);
        ExecutorLocals locals = ExecutorLocals.create(sensors);
        ExecutorLocals.set(locals);

        PartitionIterator partitions = RangeCommands.partitions(command, consistencyLevel, queryStartNanoTime, readTracker);
        partitions = PartitionIterators.filteredRowTrackingIterator(partitions, readTracker::onFilteredPartition, readTracker::onFilteredRow, readTracker::onFilteredRow);

        return PartitionIterators.doOnClose(partitions, readTracker::onDone);
    }

    public Map<String, List<String>> getSchemaVersions()
    {
        return describeSchemaVersions(false);
    }

    public Map<String, List<String>> getSchemaVersionsWithPort()
    {
        return describeSchemaVersions(true);
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions(boolean withPort)
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddressAndPort, UUID> versions = new ConcurrentHashMap<>();
        final Set<InetAddressAndPort> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        RequestCallback<UUID> cb = message ->
        {
            // record the response from the remote node.
            versions.put(message.from(), message.payload);
            latch.countDown();
        };
        // an empty message acts as a request to the SchemaVersionVerbHandler.
        Message message = Message.out(Verb.SCHEMA_VERSION_REQ, noPayload);
        for (InetAddressAndPort endpoint : liveHosts)
            MessagingService.instance().sendWithCallback(message, endpoint, cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(NANOSECONDS), NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddressAndPort> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddressAndPort host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress(withPort));
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        synchronized (StorageService.instance)
        {
            if (b)
                StorageService.instance.checkServiceAllowedToStart("hinted handoff");

            DatabaseDescriptor.setHintedHandoffEnabled(b);
        }
    }

    public void enableHintsForDC(String dc)
    {
        DatabaseDescriptor.enableHintsForDC(dc);
    }

    public void disableHintsForDC(String dc)
    {
        DatabaseDescriptor.disableHintsForDC(dc);
    }

    public Set<String> getHintedHandoffDisabledDCs()
    {
        return DatabaseDescriptor.hintedHandoffDisabledDCs();
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(Replica replica)
    {
        if (!DatabaseDescriptor.hintedHandoffEnabled())
            return false;
        if (replica.isTransient() || replica.isSelf())
            return false;

        Set<String> disabledDCs = DatabaseDescriptor.hintedHandoffDisabledDCs();
        if (!disabledDCs.isEmpty())
        {
            final String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(replica);
            if (disabledDCs.contains(dc))
            {
                Tracing.trace("Not hinting {} since its data center {} has been disabled {}", replica, dc, disabledDCs);
                return false;
            }
        }
        boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(replica.endpoint()) > DatabaseDescriptor.getMaxHintWindow();
        if (hintWindowExpired)
        {
            HintsService.instance.metrics.incrPastWindow(replica.endpoint());
            Tracing.trace("Not hinting {} which has been down {} ms", replica, Gossiper.instance.getEndpointDowntime(replica.endpoint()));
            return false;
        }
        if (HintsService.instance.exceedsMaxHintsSize())
        {
            Tracing.trace("Not hinting {} which has reached the max hints size of {} bytes on disk. "
                          + "The current hints size on disk is {} bytes",
                          replica.endpoint(),
                          DatabaseDescriptor.getMaxTotalHintsSize(),
                          HintsService.instance.getTotalHintsSize());
            return false;
        }
        return true;
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down.
     * @throws TimeoutException If the truncate operation doesn't complete within the truncation timeout limit.
     * @throws TruncateException If the truncate operation fails on some replica.
     */
    public void truncateBlocking(String keyspace, String cfname)
    throws UnavailableException, TimeoutException, TruncateException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);
        if (isAnyStorageHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw UnavailableException.create(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddressAndPort> allEndpoints = StorageService.instance.getLiveRingMembers(true);
        truncateBlocking(allEndpoints, keyspace, cfname);
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname.
     * This method sends truncate requests and waits for the answers. It assumes taht all endpoints
     * are live. This is either enforced by {@link StorageProxy#truncateBlocking(String, String)} or by the CNDB
     * override.
     *
     * @param allEndpoints All endpoints where to send truncate requests.
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down (all nodes need to be up to perform
     *                              a truncate operation).
     * @throws TimeoutException If the truncate operation doesn't complete within the truncation timeout limit.
     * @throws TruncateException If the truncate operation fails on some replica.
     */
    public void truncateBlocking(Set<InetAddressAndPort> allEndpoints, String keyspace, String cfname)
    throws UnavailableException, TimeoutException, TruncateException
    {
        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        Tracing.trace("Enqueuing truncate messages to hosts {}", allEndpoints);
        Message<TruncateRequest> message = Message.out(TRUNCATE_REQ, new TruncateRequest(keyspace, cfname));
        for (InetAddressAndPort endpoint : allEndpoints)
            MessagingService.instance().sendWithCallback(message, endpoint, responseHandler);

        // Wait for all
        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            Tracing.trace("Timed out");
            throw e;
        }
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyStorageHostDown()
    {
        return !Gossiper.instance.getUnreachableTokenOwners().isEmpty();
    }

    public interface WritePerformer
    {
        public void apply(IMutation mutation,
                          ReplicaPlan.ForTokenWrite targets,
                          AbstractWriteResponseHandler<IMutation> responseHandler,
                          String localDataCenter) throws OverloadedException;
    }

    /**
     * This class captures metrics for views writes.
     */
    private static class ViewWriteMetricsWrapped extends BatchlogResponseHandler<IMutation>
    {
        ClientRequestsMetrics metrics;

        public ViewWriteMetricsWrapped(AbstractWriteResponseHandler<IMutation> writeHandler, int i, BatchlogCleanup cleanup, long queryStartNanoTime, ClientRequestsMetrics metrics)
        {
            super(writeHandler, i, cleanup, queryStartNanoTime);
            this.metrics = metrics;
            metrics.viewWriteMetrics.viewReplicasAttempted.inc(candidateReplicaCount());
        }

        public void onResponse(Message<IMutation> msg)
        {
            super.onResponse(msg);
            metrics.viewWriteMetrics.viewReplicasSuccess.inc();
        }
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        final long approxCreationTimeNanos;
        final Verb verb;

        public DroppableRunnable(Verb verb)
        {
            this.approxCreationTimeNanos = MonotonicClock.approxTime.now();
            this.verb = verb;
        }

        public final void run()
        {
            long approxCurrentTimeNanos = MonotonicClock.approxTime.now();
            long expirationTimeNanos = verb.expiresAtNanos(approxCreationTimeNanos);
            if (approxCurrentTimeNanos > expirationTimeNanos)
            {
                long timeTakenNanos = approxCurrentTimeNanos - approxCreationTimeNanos;
                MessagingService.instance().metrics.recordSelfDroppedMessage(verb, timeTakenNanos, NANOSECONDS);
                return;
            }
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long approxCreationTimeNanos = MonotonicClock.approxTime.now();

        private final Replica localReplica;

        LocalMutationRunnable(Replica localReplica)
        {
            this.localReplica = localReplica;
        }

        public final void run()
        {
            final Verb verb = verb();
            long nowNanos = MonotonicClock.approxTime.now();
            long expirationTimeNanos = verb.expiresAtNanos(approxCreationTimeNanos);
            if (nowNanos > expirationTimeNanos)
            {
                long timeTakenNanos = nowNanos - approxCreationTimeNanos;
                MessagingService.instance().metrics.recordSelfDroppedMessage(Verb.MUTATION_REQ, timeTakenNanos, NANOSECONDS);

                HintRunnable runnable = new HintRunnable(EndpointsForToken.of(localReplica.range().right, localReplica))
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected Verb verb();
        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final EndpointsForToken targets;

        protected HintRunnable(EndpointsForToken targets)
        {
            this.targets = targets;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                StorageMetrics.totalHintsInProgress.dec(targets.size());
                for (InetAddressAndPort target : targets.endpoints())
                    getHintsInProgressFor(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return StorageMetrics.totalHints.getCount();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return (int) StorageMetrics.totalHintsInProgress.getCount();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    private static AtomicInteger getHintsInProgressFor(InetAddressAndPort destination)
    {
        try
        {
            return hintsInProgress.load(destination);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static Future<Void> submitHint(Mutation mutation, Replica target, AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        return submitHint(mutation, EndpointsForToken.of(target.range().right, target), responseHandler);
    }

    public static Future<Void> submitHint(Mutation mutation,
                                          EndpointsForToken targets,
                                          AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        Replicas.assertFull(targets); // hints should not be written for transient replicas
        HintRunnable runnable = new HintRunnable(targets)
        {
            public void runMayThrow()
            {
                Set<InetAddressAndPort> validTargets = new HashSet<>(targets.size());
                Set<UUID> hostIds = new HashSet<>(targets.size());
                for (InetAddressAndPort target : targets.endpoints())
                {
                    UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
                    if (hostId != null)
                    {
                        hostIds.add(hostId);
                        validTargets.add(target);
                    }
                    else
                        logger.debug("Discarding hint for endpoint not part of ring: {}", target);
                }
                logger.trace("Adding hints for {}", validTargets);
                HintsService.instance.write(hostIds, Hint.create(mutation, System.currentTimeMillis()));
                validTargets.forEach(HintsService.instance.metrics::incrCreatedHints);
                // Notify the handler only for CL == ANY
                if (responseHandler != null && responseHandler.replicaPlan.consistencyLevel() == ConsistencyLevel.ANY)
                    responseHandler.onResponse(null);
            }
        };

        return submitHint(runnable);
    }

    private static Future<Void> submitHint(HintRunnable runnable)
    {
        StorageMetrics.totalHintsInProgress.inc(runnable.targets.size());
        for (Replica target : runnable.targets)
            getHintsInProgressFor(target.endpoint()).incrementAndGet();
        return (Future<Void>) Stage.MUTATION.submit(runnable);
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(MILLISECONDS); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(MILLISECONDS); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCounterWriteRpcTimeout() { return DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS); }
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis); }

    public Long getCasContentionTimeout() { return DatabaseDescriptor.getCasContentionTimeout(MILLISECONDS); }
    public void setCasContentionTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(MILLISECONDS); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(MILLISECONDS); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }

    public Long getNativeTransportMaxConcurrentConnections() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnections(); }
    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnections(nativeTransportMaxConcurrentConnections); }

    public Long getNativeTransportMaxConcurrentConnectionsPerIp() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp(); }
    public void setNativeTransportMaxConcurrentConnectionsPerIp(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnectionsPerIp(nativeTransportMaxConcurrentConnections); }

    public void reloadTriggerClasses() { TriggerExecutor.instance.reloadClasses(); }

    public long getReadRepairAttempted()
    {
        return ReadRepairMetrics.attempted.getCount();
    }

    public long getReadRepairRepairedBlocking()
    {
        return ReadRepairMetrics.repairedBlocking.getCount();
    }

    public long getReadRepairRepairedBackground()
    {
        return ReadRepairMetrics.repairedBackground.getCount();
    }

    public int getNumberOfTables()
    {
        return Schema.instance.getNumberOfTables();
    }

    public String getIdealConsistencyLevel()
    {
        return Objects.toString(DatabaseDescriptor.getIdealConsistencyLevel(), "");
    }

    public String setIdealConsistencyLevel(String cl)
    {
        ConsistencyLevel original = DatabaseDescriptor.getIdealConsistencyLevel();
        ConsistencyLevel newCL = ConsistencyLevel.valueOf(cl.trim().toUpperCase());
        DatabaseDescriptor.setIdealConsistencyLevel(newCL);
        return String.format("Updating ideal consistency level new value: %s old value %s", newCL, original.toString());
    }

    @Override
    public int getNonIndexMemtableFlushPeriodInSeconds()
    {
        return CassandraRelevantProperties.FLUSH_PERIOD_IN_MILLIS.getInt();
    }

    @Override
    public void setNonIndexMemtableFlushPeriodInSeconds(int flushPeriodInSeconds)
    {
        CassandraRelevantProperties.FLUSH_PERIOD_IN_MILLIS.setInt(flushPeriodInSeconds);
    }

    @Override
    public int getVectorIndexMemtableFlushPeriodInSecond()
    {
        return CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.getInt();
    }

    @Override
    public void setVectorMemtableFlushPeriodInSecond(int flushPeriodInSecond)
    {
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.setInt(flushPeriodInSecond);
    }

    @Override
    public int getNonVectorIndexMemtableFlushPeriodInSecond()
    {
        return CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_PERIOD_IN_MILLIS.getInt();
    }

    @Override
    public void setNonVectorMemtableFlushPeriodInSecond(int flushPeriodInSecond)
    {
        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_PERIOD_IN_MILLIS.setInt(flushPeriodInSecond);
    }

    @Override
    public int getVectorIndexMemtableFlushMaxRows()
    {
        return CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.getInt();
    }

    @Override
    public void setVectorMemtableFlushMaxRows(int threshold)
    {
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.setInt(threshold);
    }

    @Override
    public int getNonVectorIndexMemtableFlushMaxRows()
    {
        return CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.getInt();
    }

    @Override
    public void setNonVectorMemtableFlushPeriodMaxRows(int threshold)
    {
        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.setInt(threshold);
    }

    @Deprecated
    public int getOtcBacklogExpirationInterval() {
        return 0;
    }

    @Deprecated
    public void setOtcBacklogExpirationInterval(int intervalInMillis) { }

    @Override
    public void enableRepairedDataTrackingForRangeReads()
    {
        DatabaseDescriptor.setRepairedDataTrackingForRangeReadsEnabled(true);
    }

    @Override
    public void disableRepairedDataTrackingForRangeReads()
    {
        DatabaseDescriptor.setRepairedDataTrackingForRangeReadsEnabled(false);
    }

    @Override
    public boolean getRepairedDataTrackingEnabledForRangeReads()
    {
        return DatabaseDescriptor.getRepairedDataTrackingForRangeReadsEnabled();
    }

    @Override
    public void enableRepairedDataTrackingForPartitionReads()
    {
        DatabaseDescriptor.setRepairedDataTrackingForPartitionReadsEnabled(true);
    }

    @Override
    public void disableRepairedDataTrackingForPartitionReads()
    {
        DatabaseDescriptor.setRepairedDataTrackingForPartitionReadsEnabled(false);
    }

    @Override
    public boolean getRepairedDataTrackingEnabledForPartitionReads()
    {
        return DatabaseDescriptor.getRepairedDataTrackingForPartitionReadsEnabled();
    }

    @Override
    public void enableReportingUnconfirmedRepairedDataMismatches()
    {
        DatabaseDescriptor.reportUnconfirmedRepairedDataMismatches(true);
    }

    @Override
    public void disableReportingUnconfirmedRepairedDataMismatches()
    {
       DatabaseDescriptor.reportUnconfirmedRepairedDataMismatches(false);
    }

    @Override
    public boolean getReportingUnconfirmedRepairedDataMismatchesEnabled()
    {
        return DatabaseDescriptor.reportUnconfirmedRepairedDataMismatches();
    }

    @Override
    public boolean getSnapshotOnRepairedDataMismatchEnabled()
    {
        return DatabaseDescriptor.snapshotOnRepairedDataMismatch();
    }

    @Override
    public void enableSnapshotOnRepairedDataMismatch()
    {
        DatabaseDescriptor.setSnapshotOnRepairedDataMismatch(true);
    }

    @Override
    public void disableSnapshotOnRepairedDataMismatch()
    {
        DatabaseDescriptor.setSnapshotOnRepairedDataMismatch(false);
    }

    static class PaxosBallotAndContention
    {
        final UUID ballot;
        final int contentions;

        PaxosBallotAndContention(UUID ballot, int contentions)
        {
            this.ballot = ballot;
            this.contentions = contentions;
        }

        @Override
        public final int hashCode()
        {
            int hashCode = 31 + (ballot == null ? 0 : ballot.hashCode());
            return 31 * hashCode * this.contentions;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof PaxosBallotAndContention))
                return false;
            PaxosBallotAndContention that = (PaxosBallotAndContention)o;
            // handles nulls properly
            return Objects.equals(ballot, that.ballot) && contentions == that.contentions;
        }
    }

    @Override
    public boolean getSnapshotOnDuplicateRowDetectionEnabled()
    {
        return DatabaseDescriptor.snapshotOnDuplicateRowDetection();
    }

    @Override
    public void enableSnapshotOnDuplicateRowDetection()
    {
        DatabaseDescriptor.setSnapshotOnDuplicateRowDetection(true);
    }

    @Override
    public void disableSnapshotOnDuplicateRowDetection()
    {
        DatabaseDescriptor.setSnapshotOnDuplicateRowDetection(false);
    }

    @Override
    public boolean getCheckForDuplicateRowsDuringReads()
    {
        return DatabaseDescriptor.checkForDuplicateRowsDuringReads();
    }

    @Override
    public void enableCheckForDuplicateRowsDuringReads()
    {
        DatabaseDescriptor.setCheckForDuplicateRowsDuringReads(true);
    }

    @Override
    public void disableCheckForDuplicateRowsDuringReads()
    {
        DatabaseDescriptor.setCheckForDuplicateRowsDuringReads(false);
    }

    @Override
    public boolean getCheckForDuplicateRowsDuringCompaction()
    {
        return DatabaseDescriptor.checkForDuplicateRowsDuringCompaction();
    }

    @Override
    public void enableCheckForDuplicateRowsDuringCompaction()
    {
        DatabaseDescriptor.setCheckForDuplicateRowsDuringCompaction(true);
    }

    @Override
    public void disableCheckForDuplicateRowsDuringCompaction()
    {
        DatabaseDescriptor.setCheckForDuplicateRowsDuringCompaction(false);
    }
}
