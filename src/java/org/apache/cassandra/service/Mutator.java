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

import java.util.Collection;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Facilitates mutations for counters, simple inserts, unlogged batches and LWTs.
 * Used on the coordinator.
 * <br/>
 * The implementations may choose how and where to send the mutations.
 * <br/>
 * An instance of this interface implementation must be obtained via {@link MutatorProvider#instance}.
 */
public interface Mutator
{
    /**
     * Where a Paxos commit dispatch originated. Passed to {@link #onCasCommit}.
     */
    enum CasCommitOrigin
    {
        /**
         * The coordinator's own CAS operation reached the commit phase: the proposal carries the
         * update this coordinator built and successfully proposed. At most one per
         * {@link #mutateCas} invocation.
         */
        CLIENT_OPERATION,
        /**
         * The coordinator witnessed another proposer's accepted-but-uncommitted round and is
         * completing it on their behalf. The payload is the foreign in-progress update, not
         * anything this coordinator's client asked for; a SERIAL/LOCAL_SERIAL read can trigger it.
         */
        REPAIR_IN_PROGRESS,
        /**
         * Re-transmission of an already-committed value to replicas that have not witnessed it
         * (most-recent-commit refresh). Carries no new decision; the value was agreed earlier.
         */
        REFRESH_COMMITTED
    }

    /**
     * Used for handling the given {@code mutations} as a logged batch.
     */
    void mutateAtomically(Collection<Mutation> mutations,
                          ConsistencyLevel consistencyLevel,
                          boolean requireQuorumForRemove,
                          Dispatcher.RequestTime requestTime,
                          ClientRequestsMetrics metrics,
                          ClientState clientState)
    throws UnavailableException, OverloadedException, WriteTimeoutException;

    /**
     * Used for handling counter mutations on the coordinator level:
     * - if coordinator is a replica, it will apply the counter mutation locally and forward the applied mutation to other counter replica
     * - if coordinator is not a replica, it will forward the counter mutation to a counter leader which is a replica
     */
    AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, Dispatcher.RequestTime requestTime);

    /**
     * Used for handling counter mutations on the counter leader level
     */
    AbstractWriteResponseHandler<IMutation> mutateCounterOnLeader(CounterMutation mutation,
                                                                  String localDataCenter,
                                                                  StorageProxy.WritePerformer performer,
                                                                  Runnable callback,
                                                                  Dispatcher.RequestTime requestTime);

    /**
     * Used for standard inserts and unlogged batchs.
     */
    AbstractWriteResponseHandler<IMutation> mutateStandard(Mutation mutation,
                                                           ConsistencyLevel consistencyLevel,
                                                           String localDataCenter,
                                                           StorageProxy.WritePerformer writePerformer,
                                                           Runnable callback,
                                                           WriteType writeType,
                                                           Dispatcher.RequestTime requestTime);

    /**
     * Used for LWT mutation at the last (COMMIT) phase of Paxos.
     */
    @Nullable
    AbstractWriteResponseHandler<Commit> mutatePaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, Dispatcher.RequestTime requestTime);

    /**
     * Used for handling a whole CAS (LWT) operation: a single conditional statement or a
     * conditional batch. This is the operation-level Paxos entry point, called exactly once per
     * client operation regardless of the configured {@code paxos_variant} (the default
     * implementation dispatches to the v2 engine or the legacy v1 flow), of internal
     * prepare/propose retries under contention, and of the outcome.
     * <p>
     * Completion is the return or throw of this call, on the calling thread:
     * <ul>
     *   <li>returns {@code null}: the update applied;</li>
     *   <li>returns a {@link RowIterator}: the condition did not match (payload = current values);</li>
     *   <li>throws {@link CasWriteUnknownResultException}: the update's fate is unknown (a partial
     *       propose under the v1 engine). NOTE: the v2 engine does not throw this exception — its
     *       fate-unknown cases (a proposal superseded with possible side effects, or a partially
     *       accepted propose that times out) surface as plain timeout/failure exceptions with no
     *       distinguishing marker, so on v2 a propose-phase timeout must be treated as
     *       possibly-durable: a minority-accepted value can still be completed by a later repair;</li>
     *   <li>throws after {@link #onCasCommit} fired with {@link CasCommitOrigin#CLIENT_OPERATION}:
     *       the update was agreed by a quorum and its commit dispatched but not acknowledged in
     *       time. An agreed value is decided: it WILL be completed (by the dispatched commit, or
     *       by any later operation or repair touching the partition) even though this operation
     *       reported failure to the client;</li>
     *   <li>throws otherwise: this coordinator dispatched no commit (but see the v2 caveat above
     *       for propose-phase timeouts).</li>
     * </ul>
     */
    default RowIterator mutateCas(TableMetadata metadata,
                                  DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit,
                                  ClientState clientState,
                                  long nowInSeconds,
                                  Dispatcher.RequestTime requestTime)
    throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException,
           InvalidRequestException, CasWriteUnknownResultException
    {
        return Paxos.useV2()
               ? Paxos.cas(key, request, consistencyForPaxos, consistencyForCommit, clientState)
               : StorageProxy.legacyCas(metadata.keyspace, metadata.name, key, request, consistencyForPaxos,
                                        consistencyForCommit, clientState, nowInSeconds, requestTime);
    }

    /**
     * Callback invoked when a Paxos commit is DISPATCHED by this coordinator, under either Paxos
     * engine (v1 or v2). Dispatched, not acknowledged: replicas may still miss it, in which case a
     * later operation re-issues it (reported as {@link CasCommitOrigin#REFRESH_COMMITTED}).
     * <p>
     * Multiplicity and threading: {@link CasCommitOrigin#CLIENT_OPERATION} fires at most once per
     * {@link #mutateCas} call, inside that call, on the SAME thread, before the commit is
     * dispatched/awaited — a wrapper can correlate the two without inspecting ballots (both
     * engines invoke the whole operation synchronously on the request thread; any future
     * refactoring of the engines must preserve this for CLIENT_OPERATION). The repair origins
     * carry payloads this coordinator never originated and can fire any number of times, from CAS
     * writes and SERIAL/LOCAL_SERIAL reads (on the request thread, while completing in-progress
     * rounds they witness) and from background Paxos repair (on ARBITRARY threads, including
     * messaging/response threads) — implementations must not assume request-thread context for
     * repair origins.
     * <p>
     * {@code consistencyLevel} is the consistency the commit is performed at for
     * {@link CasCommitOrigin#CLIENT_OPERATION} and the v1/PaxosRepair repair sites; for the v2
     * engine's internal repair sites (where the commit piggybacks on the prepare exchange and has
     * no commit consistency of its own) it is the consensus consistency (SERIAL/LOCAL_SERIAL) of
     * the operation that triggered the repair.
     * <p>
     * Empty proposals (serial reads, non-applying CAS) are never committed by either engine, so
     * they never produce this callback. Low-level re-transmissions inside the v2 prepare exchange
     * ({@code PaxosPrepareRefresh}) are transport details and are not reported.
     * <p>
     * Implementations should not throw and must not block. Throwing is contained by the caller
     * ({@link MutatorProvider#notifyCasCommit}): the exception is logged and ignored, never
     * failing the operation, read or repair dispatching the commit.
     */
    default void onCasCommit(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin)
    {
        // no-op
    }

    /**
     * Callback invoked AFTER a Paxos commit dispatched by this coordinator has been ACKNOWLEDGED by
     * a {@code consistencyLevel} quorum of replicas — i.e. that many replicas have applied the
     * committed {@code PartitionUpdate} to their base table and replied. Unlike {@link #onCasCommit}
     * (which fires <em>before</em> the commit is dispatched, for every <em>decided</em> value), this
     * fires only once the value is durably visible: a read at {@code consistencyLevel} (or stronger)
     * issued after this callback returns will observe the committed value. Note that if
     * {@code consistencyLevel} is {@code ONE}, reading at ONE or QUORUM may still not return the
     * committed value if the acknowledging replica is not the one serving the read.
     * <p>
     * Relationship to {@link #onCasCommit}: every delivery of this callback is preceded by a
     * matching {@link #onCasCommit} for the same ballot, and it fires only on the success path (the
     * commit was acknowledged). Multiplicity mirrors {@link #onCasCommit}: exactly one per
     * {@link CasCommitOrigin#CLIENT_OPERATION}; the repair/refresh origins may fire any number of
     * times (once per completed round, across retries and repair cycles). If a commit times out or
     * fails (the value is decided and will be completed later by a repair, but no quorum ack was
     * received), {@link #onCasCommit} still fired but this callback does NOT — absence of this
     * callback after an {@link #onCasCommit} means "commit not confirmed", not "not decided".
     * <p>
     * Coverage — this applied callback is delivered for:
     * <ul>
     *   <li>{@link CasCommitOrigin#CLIENT_OPERATION} under both paxos variants (v1 {@code doPaxos}
     *       after the blocking commit, v2 {@code Paxos.cas} after the commit await): fires on the
     *       request thread, inside {@link #mutateCas}, after the CLIENT_OPERATION {@link #onCasCommit};</li>
     *   <li>{@link CasCommitOrigin#REPAIR_IN_PROGRESS} completed by the v1 engine
     *       ({@code beginAndRepairPaxos}, on the triggering operation's request thread) and by
     *       background {@code PaxosRepair} (on an arbitrary repair thread);</li>
     *   <li>{@link CasCommitOrigin#REFRESH_COMMITTED} re-transmitted by background {@code PaxosRepair}.</li>
     * </ul>
     * It is NOT delivered (only the dispatched {@link #onCasCommit} is) for the v1
     * {@link CasCommitOrigin#REFRESH_COMMITTED} site (an asynchronous fire-and-forget
     * {@code sendCommit} with no awaited ack) nor for the v2 engine's {@code begin()}-path
     * {@link CasCommitOrigin#REFRESH_COMMITTED}/{@link CasCommitOrigin#REPAIR_IN_PROGRESS} sites
     * (where the commit piggybacks on the following prepare and has no separable ack). For those,
     * a deferred {@code LOCAL_SERIAL}/{@code SERIAL} read issued from {@link #onCasCommit} is
     * self-correcting and will observe the value regardless.
     * <p>
     * Threading and containment mirror {@link #onCasCommit}: implementations should not throw and
     * must not block; a thrown exception is logged and ignored by
     * {@link MutatorProvider#notifyCasCommitApplied} and never fails the operation, read or repair.
     */
    default void onCasCommitApplied(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin)
    {
        // no-op
    }

    /**
     * Used to persist the given batch of mutations. Usually invoked as part of
     * {@link #mutateAtomically(Collection, ConsistencyLevel, boolean, long, ClientRequestsMetrics, ClientState)}.
     */
    void persistBatchlog(Collection<Mutation> mutations, Dispatcher.RequestTime requestTime, ReplicaPlan.ForWrite replicaPlan, TimeUUID batchUUID);

    /**
     * Used to clear the given batch id. Usually invoked as part of
     * {@link #mutateAtomically(Collection, ConsistencyLevel, boolean, long, ClientRequestsMetrics, ClientState)}.
     */
    void clearBatchlog(String keyspace, Dispatcher.RequestTime requestTime, ReplicaPlan.ForWrite replicaPlan, TimeUUID batchUUID);

    /**
     * Callback invoked when the given {@code mutation} is localy applied.
     */
    default void onAppliedMutation(IMutation mutation)
    {
        // no-op
    }

    /**
     * Callback invoked when the given {@code counter} is localy applied.
     */
    default void onAppliedCounter(IMutation counter, AbstractWriteResponseHandler<IMutation> handler)
    {
        // no-op
    }

    /**
     * Callback invoked when the given {@code proposal} is localy committed.
     */
    default void onAppliedProposal(Commit proposal)
    {
        // no-op
    }
}