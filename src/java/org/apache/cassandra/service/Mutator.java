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
     * Terminal outcome of a commit previously announced via {@link #onCasCommit}. Passed to
     * {@link #onCasCommitCompleted}. {@link #APPLIED} and {@link #CONFIRMED_BY_PREPARE} are the two
     * success outcomes (the value is durably visible at {@code consistencyLevel}); {@link #SUPERSEDED}
     * and {@link #UNCONFIRMED} mean this coordinator did NOT confirm the commit — but the value is
     * still <em>decided</em> and may become durable via another proposer or a background repair, so
     * they are "not confirmed here", never "rolled back" (Paxos never un-decides an agreed value).
     */
    enum CasCommitOutcome
    {
        /**
         * Acknowledged by a {@code consistencyForCommit} quorum via a standalone, separately-awaited
         * commit (the strongest confirmation). A read at {@code consistencyLevel} (or stronger) issued
         * after this callback observes the value. Delivered for CLIENT_OPERATION under both engines,
         * the v1 in-progress repair, and background Paxos repair.
         */
        APPLIED,
        /**
         * Inferred-applied: the commit was fused into the following prepare (the v2 {@code begin()}
         * "commit-and-prepare" optimization used to finish another proposer's round) and that prepare
         * reached a {@code consistencyForConsensus} (serial) promise-quorum. Every replica applies the
         * fused commit before answering the prepare, so a promise-quorum implies the commit reached
         * that same quorum. Slightly weaker than {@link #APPLIED} (serial-quorum, inferred rather than
         * a directly-awaited commit ack), but still means the value is durably visible.
         */
        CONFIRMED_BY_PREPARE,
        /**
         * Decided but NOT confirmed by this coordinator: a higher ballot pre-empted the fused
         * commit-and-prepare before a quorum was reached. Another proposer is finishing the round.
         */
        SUPERSEDED,
        /**
         * Decided but NOT confirmed: the commit (or the fused prepare) timed out or failed before a
         * quorum acknowledged it. The value may still be completed later by a repair.
         */
        UNCONFIRMED
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
     * TERMINAL completion for a commit previously announced via {@link #onCasCommit}: fires once the
     * fate of that commit is known, carrying the {@link CasCommitOutcome outcome} (success or not).
     * Where wired (see coverage below) it fires EXACTLY ONCE per {@link #onCasCommit} for the same
     * ballot, letting an implementation that opened an operation on {@link #onCasCommit} close it out
     * — pairing every announced commit with a success ({@link CasCommitOutcome#APPLIED} /
     * {@link CasCommitOutcome#CONFIRMED_BY_PREPARE}) or a not-confirmed
     * ({@link CasCommitOutcome#SUPERSEDED} / {@link CasCommitOutcome#UNCONFIRMED}) terminal.
     * <p>
     * Unlike {@link #onCasCommit} (which fires <em>before</em> dispatch, for every decided value),
     * this fires once the commit's fate is settled. A success outcome means the value is durably
     * visible: a read at {@code consistencyLevel} (or stronger) issued after this returns observes it
     * (with the usual caveat that at {@code ONE} the acknowledging replica may not be the one serving
     * a later read). A not-confirmed outcome does NOT mean "not decided" (see {@link CasCommitOutcome});
     * the standard response is a deferred {@code LOCAL_SERIAL}/{@code SERIAL} read, which is
     * self-correcting and will observe the value if/when it becomes durable.
     * <p>
     * Coverage — a terminal is delivered for:
     * <ul>
     *   <li>{@link CasCommitOrigin#CLIENT_OPERATION} under both engines (v1 {@code doPaxos}, v2
     *       {@code Paxos.cas}): {@link CasCommitOutcome#APPLIED} when the awaited commit is
     *       acknowledged, else {@link CasCommitOutcome#UNCONFIRMED} — on the request thread, inside
     *       {@link #mutateCas}, after the CLIENT_OPERATION {@link #onCasCommit};</li>
     *   <li>the v2 {@code begin()}-path {@link CasCommitOrigin#REPAIR_IN_PROGRESS} and
     *       {@link CasCommitOrigin#REFRESH_COMMITTED} sites (commit fused into the following prepare):
     *       {@link CasCommitOutcome#CONFIRMED_BY_PREPARE} when that prepare reaches a promise-quorum,
     *       {@link CasCommitOutcome#SUPERSEDED} if pre-empted, else {@link CasCommitOutcome#UNCONFIRMED};</li>
     *   <li>the v1 in-progress {@link CasCommitOrigin#REPAIR_IN_PROGRESS} repair
     *       ({@code beginAndRepairPaxos}): {@link CasCommitOutcome#APPLIED} or
     *       {@link CasCommitOutcome#UNCONFIRMED};</li>
     *   <li>background {@code PaxosRepair} ({@link CasCommitOrigin#REPAIR_IN_PROGRESS} /
     *       {@link CasCommitOrigin#REFRESH_COMMITTED}): {@link CasCommitOutcome#APPLIED} only — this
     *       background state machine retries on failure rather than delivering a negative terminal,
     *       so a non-success outcome is not reported (best-effort, on an arbitrary repair thread).</li>
     * </ul>
     * NO terminal is delivered (only the dispatched {@link #onCasCommit} is) for the v1
     * {@link CasCommitOrigin#REFRESH_COMMITTED} fire-and-forget {@code sendCommit} (no awaited ack),
     * nor for any commit performed at {@code consistencyForCommit == ANY} (which does not block for a
     * replica ack). For those, rely on a deferred serial read.
     * <p>
     * Threading and containment mirror {@link #onCasCommit}: implementations should not throw and
     * must not block; a thrown exception is logged and ignored by
     * {@link MutatorProvider#notifyCasCommitCompleted} and never fails the operation, read or repair.
     */
    default void onCasCommitCompleted(Commit committed, ConsistencyLevel consistencyLevel,
                                      CasCommitOrigin origin, CasCommitOutcome outcome)
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