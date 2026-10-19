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

package org.apache.cassandra.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.Util.dk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the operation-level CAS surface of the {@link Mutator} SPI: {@link Mutator#mutateCas}
 * and {@link Mutator#onCasCommit}, under both {@code paxos_variant} v1 and v2, on a single-node
 * (RF=1) in-process cluster.
 *
 * The custom mutator is installed the same way a real deployment does it (the
 * {@code cassandra.custom_mutator_class} system property, read once at {@link MutatorProvider}
 * class-init), so the static block below must run before any server class is touched.
 */
public class MutatorCasTest
{
    static
    {
        CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS.setString(RecordingMutator.class.getName());
    }

    private static final String KEYSPACE = "mutator_cas_test";
    private static final String KEYSPACE_RF2 = "mutator_cas_test_rf2";
    private static final String TABLE = "table_cas";

    /** Monotonic stamp so tests can assert relative ordering of dispatched vs applied callbacks. */
    static final AtomicInteger sequence = new AtomicInteger();

    /** One record per {@link Mutator#onCasCommit} / {@link Mutator#onCasCommitCompleted} callback. */
    static final class CommitRecord
    {
        final Commit commit;
        final ConsistencyLevel consistencyLevel;
        final Mutator.CasCommitOrigin origin;
        final Mutator.CasCommitOutcome outcome; // null for onCasCommit (dispatch) records
        final Thread thread;
        final int seq;

        CommitRecord(Commit commit, ConsistencyLevel consistencyLevel, Mutator.CasCommitOrigin origin)
        {
            this(commit, consistencyLevel, origin, null);
        }

        CommitRecord(Commit commit, ConsistencyLevel consistencyLevel, Mutator.CasCommitOrigin origin,
                     Mutator.CasCommitOutcome outcome)
        {
            this.commit = commit;
            this.consistencyLevel = consistencyLevel;
            this.origin = origin;
            this.outcome = outcome;
            this.thread = Thread.currentThread();
            this.seq = sequence.incrementAndGet();
        }

        boolean isSuccess()
        {
            return outcome == Mutator.CasCommitOutcome.APPLIED || outcome == Mutator.CasCommitOutcome.CONFIRMED_BY_PREPARE;
        }
    }

    /**
     * Wraps the default engine dispatch ({@code Mutator.super.mutateCas}) exactly the way a real
     * custom Mutator would, recording begin/completion and every commit callback.
     */
    public static class RecordingMutator implements Mutator
    {
        static final AtomicInteger casBegun = new AtomicInteger();
        static final AtomicInteger casCompleted = new AtomicInteger();
        static final AtomicInteger mutatePaxosCalls = new AtomicInteger();
        static final List<CommitRecord> commits = new CopyOnWriteArrayList<>();
        static final List<CommitRecord> completed = new CopyOnWriteArrayList<>();

        /**
         * When non-null, {@link #mutatePaxos} returns a handler whose {@code get()} throws this instead
         * of dispatching a real commit -- lets a test drive the commit-phase failure path in doPaxos.
         */
        static volatile RuntimeException commitFailure;

        /**
         * When non-null, {@link #onCasCommit} throws this (after recording) for callbacks whose origin
         * matches {@link #onCasCommitFailureOrigin} (all origins when that is null) -- lets a test
         * drive the veto and containment paths of {@link MutatorProvider#notifyCasCommit}.
         */
        static volatile RuntimeException onCasCommitFailure;
        static volatile Mutator.CasCommitOrigin onCasCommitFailureOrigin;

        private final Mutator delegate = new StorageProxy.DefaultMutator();

        static void reset()
        {
            casBegun.set(0);
            casCompleted.set(0);
            mutatePaxosCalls.set(0);
            commits.clear();
            completed.clear();
            commitFailure = null;
            onCasCommitFailure = null;
            onCasCommitFailureOrigin = null;
            sequence.set(0);
        }

        static List<CommitRecord> commitsWithOrigin(Mutator.CasCommitOrigin origin)
        {
            return withOrigin(commits, origin);
        }

        static List<CommitRecord> completedWithOrigin(Mutator.CasCommitOrigin origin)
        {
            return withOrigin(completed, origin);
        }

        private static List<CommitRecord> withOrigin(List<CommitRecord> records, Mutator.CasCommitOrigin origin)
        {
            List<CommitRecord> result = new CopyOnWriteArrayList<>();
            for (CommitRecord r : records)
                if (r.origin == origin)
                    result.add(r);
            return result;
        }

        @Override
        public RowIterator mutateCas(TableMetadata metadata,
                                     DecoratedKey key,
                                     CASRequest request,
                                     ConsistencyLevel consistencyForPaxos,
                                     ConsistencyLevel consistencyForCommit,
                                     ClientState clientState,
                                     long nowInSeconds,
                                     Dispatcher.RequestTime requestTime)
        {
            casBegun.incrementAndGet();
            try
            {
                return Mutator.super.mutateCas(metadata, key, request, consistencyForPaxos, consistencyForCommit,
                                               clientState, nowInSeconds, requestTime);
            }
            finally
            {
                casCompleted.incrementAndGet();
            }
        }

        @Override
        public void onCasCommit(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin)
        {
            commits.add(new CommitRecord(committed, consistencyLevel, origin));
            RuntimeException failure = onCasCommitFailure;
            if (failure != null && (onCasCommitFailureOrigin == null || onCasCommitFailureOrigin == origin))
                throw failure;
        }

        @Override
        public void onCasCommitCompleted(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin, CasCommitOutcome outcome)
        {
            completed.add(new CommitRecord(committed, consistencyLevel, origin, outcome));
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateStandard(Mutation mutation,
                                                                      ConsistencyLevel consistencyLevel,
                                                                      String localDataCenter,
                                                                      StorageProxy.WritePerformer writePerformer,
                                                                      Runnable callback,
                                                                      WriteType writeType,
                                                                      Dispatcher.RequestTime requestTime)
        {
            return delegate.mutateStandard(mutation, consistencyLevel, localDataCenter, writePerformer, callback,
                                           writeType, requestTime);
        }

        @Override
        public void mutateAtomically(Collection<Mutation> mutations,
                                     ConsistencyLevel consistencyLevel,
                                     boolean requireQuorumForRemove,
                                     Dispatcher.RequestTime requestTime,
                                     ClientRequestsMetrics metrics,
                                     ClientState clientState)
        {
            delegate.mutateAtomically(mutations, consistencyLevel, requireQuorumForRemove, requestTime, metrics,
                                      clientState);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter,
                                                                     Dispatcher.RequestTime requestTime)
        {
            return delegate.mutateCounter(cm, localDataCenter, requestTime);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounterOnLeader(CounterMutation mutation,
                                                                             String localDataCenter,
                                                                             StorageProxy.WritePerformer performer,
                                                                             Runnable callback,
                                                                             Dispatcher.RequestTime requestTime)
        {
            return delegate.mutateCounterOnLeader(mutation, localDataCenter, performer, callback, requestTime);
        }

        @Nullable
        @Override
        public AbstractWriteResponseHandler<Commit> mutatePaxos(Commit proposal, ConsistencyLevel consistencyLevel,
                                                                boolean allowHints, Dispatcher.RequestTime requestTime)
        {
            // Counted to pin backward compatibility: implementations that rely on the legacy v1
            // commit-transport hook must keep receiving it unchanged through the new dispatch.
            mutatePaxosCalls.incrementAndGet();
            RuntimeException failure = commitFailure;
            if (failure != null)
                return throwingCommitHandler(failure);
            return delegate.mutatePaxos(proposal, consistencyLevel, allowHints, requestTime);
        }

        /**
         * A commit response handler whose {@code get()} fails with {@code failure} -- simulating a
         * commit that could not be confirmed (replica failure, timeout, interruption). Only
         * {@code get()} is exercised by commitPaxos, so the other members are inert.
         */
        private static AbstractWriteResponseHandler<Commit> throwingCommitHandler(RuntimeException failure)
        {
            return new AbstractWriteResponseHandler<Commit>(null, null, WriteType.SIMPLE, null,
                                                            Dispatcher.RequestTime.forImmediateExecution())
            {
                @Override
                public void get()
                {
                    throw failure;
                }

                @Override
                public int ackCount()
                {
                    return 0;
                }

                @Override
                public void onResponse(Message<Commit> msg)
                {
                }
            };
        }

        @Override
        public void persistBatchlog(Collection<Mutation> mutations, Dispatcher.RequestTime requestTime,
                                    ReplicaPlan.ForWrite replicaPlan, TimeUUID batchUUID)
        {
            delegate.persistBatchlog(mutations, requestTime, replicaPlan, batchUUID);
        }

        @Override
        public void clearBatchlog(String keyspace, Dispatcher.RequestTime requestTime,
                                  ReplicaPlan.ForWrite replicaPlan, TimeUUID batchUUID)
        {
            delegate.clearBatchlog(keyspace, requestTime, replicaPlan, batchUUID);
        }
    }

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        // PaxosRepair's query phase has no execute-on-self shortcut (unlike the prepare/propose/commit
        // paths): it always goes through messaging, so loopback delivery needs a live listener.
        MessagingService.instance().listen();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        SchemaLoader.createKeyspace(KEYSPACE_RF2,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE_RF2, TABLE));

        Token token = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes(1));
        StorageService.instance.getTokenMetadata().updateNormalToken(token, FBUtilities.getBroadcastAddressAndPort());

        // The whole point of the test: the SPI singleton must be our recording implementation.
        assertThat(MutatorProvider.instance).isInstanceOf(RecordingMutator.class);
    }

    /**
     * Paxos.setPaxosVariant, NOT DatabaseDescriptor.setPaxosVariant: useV2() reads Paxos's own
     * volatile snapshot, which only the former updates (the latter just sets the config field).
     */
    private static void setPaxosVariant(Config.PaxosVariant variant)
    {
        org.apache.cassandra.service.paxos.Paxos.setPaxosVariant(variant);
    }

    @Before
    public void resetRecorder()
    {
        RecordingMutator.reset();
    }

    /**
     * A CAS on {@code (KEYSPACE, TABLE, key)} whose condition is "the partition is empty" and whose
     * update inserts one row {@code (name='r1', val=value)}.
     */
    private static CASRequest ifEmptyInsert(TableMetadata metadata, DecoratedKey key, String value)
    {
        return new CASRequest()
        {
            @Override
            public SinglePartitionReadCommand readCommand(long nowInSec)
            {
                return SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSec, key);
            }

            @Override
            public boolean appliesTo(FilteredPartition current)
            {
                return current.rowCount() == 0;
            }

            @Override
            public PartitionUpdate makeUpdates(FilteredPartition current, ClientState clientState, Ballot ballot)
            {
                return insertRow(metadata, key, value, ballot.unixMicros());
            }
        };
    }

    private static PartitionUpdate insertRow(TableMetadata metadata, DecoratedKey key, String value, long timestampMicros)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key);
        builder.timestamp(timestampMicros);
        builder.row("r1").add("val", value);
        return builder.build();
    }

    private static RowIterator cas(String keyspace, DecoratedKey key, CASRequest request)
    {
        return StorageProxy.cas(keyspace,
                                TABLE,
                                key,
                                request,
                                ConsistencyLevel.SERIAL,
                                ConsistencyLevel.QUORUM,
                                ClientState.forInternalCalls(),
                                FBUtilities.nowInSeconds(),
                                Dispatcher.RequestTime.forImmediateExecution());
    }

    private void appliedAndNotApplied(Config.PaxosVariant variant, String keyName) throws Exception
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        // Applied: empty partition matches the condition.
        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "v-" + variant)))
        {
            assertThat(result).as("applied CAS returns null").isNull();
        }
        assertThat(RecordingMutator.casBegun.get()).isEqualTo(1);
        assertThat(RecordingMutator.casCompleted.get()).isEqualTo(1);
        List<CommitRecord> clientCommits = RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCommits).as("exactly one CLIENT_OPERATION commit for an applied CAS").hasSize(1);
        CommitRecord commit = clientCommits.get(0);
        assertThat(commit.commit.update.partitionKey()).isEqualTo(key);
        assertThat(commit.commit.update.isEmpty()).isFalse();
        assertThat(commit.consistencyLevel).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(commit.thread)
            .as("CLIENT_OPERATION must fire on the mutateCas caller thread (the documented correlation contract)")
            .isSameAs(Thread.currentThread());
        // The terminal completion fires once, after the dispatched one, on the same (request) thread,
        // once the commit is acknowledged at the commit CL -- under both paxos variants, with the APPLIED
        // outcome (a standalone, separately-awaited commit).
        List<CommitRecord> clientCompleted = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCompleted).as("exactly one CLIENT_OPERATION completion callback for an applied CAS").hasSize(1);
        CommitRecord applied = clientCompleted.get(0);
        assertThat(applied.outcome)
            .as("an acknowledged client commit completes as APPLIED").isEqualTo(Mutator.CasCommitOutcome.APPLIED);
        assertThat(applied.commit.update.partitionKey()).isEqualTo(key);
        assertThat(applied.commit.update.isEmpty()).isFalse();
        assertThat(applied.consistencyLevel).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(applied.thread)
            .as("onCasCommitCompleted fires on the mutateCas caller thread for CLIENT_OPERATION")
            .isSameAs(Thread.currentThread());
        assertThat(applied.seq)
            .as("onCasCommitCompleted fires AFTER the dispatched onCasCommit")
            .isGreaterThan(commit.seq);
        // Backward compatibility: moving the engine dispatch into Mutator.mutateCas must not
        // change when the legacy hook fires -- existing implementations overriding mutatePaxos
        // still see exactly one call per applied v1 CAS (from commitPaxos), and still none
        // under v2 (which never invoked it).
        assertThat(RecordingMutator.mutatePaxosCalls.get())
            .isEqualTo(variant == Config.PaxosVariant.v1 ? 1 : 0);

        // Not applied: the row just written violates the "partition is empty" condition. No new
        // commit may be dispatched for this operation (empty proposals are never committed).
        RecordingMutator.reset();
        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "unused")))
        {
            assertThat(result).as("non-applying CAS returns the current values").isNotNull();
        }
        assertThat(RecordingMutator.casBegun.get()).isEqualTo(1);
        assertThat(RecordingMutator.casCompleted.get()).isEqualTo(1);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION))
            .as("a non-applying CAS must not dispatch a CLIENT_OPERATION commit").isEmpty();
        assertThat(RecordingMutator.completed)
            .as("a non-applying CAS commits nothing, so no completion callback fires").isEmpty();
        assertThat(RecordingMutator.mutatePaxosCalls.get())
            .as("a non-applying CAS commits nothing, so the legacy hook must not fire either")
            .isZero();
    }

    @Test
    public void appliedAndNotAppliedV1() throws Exception
    {
        appliedAndNotApplied(Config.PaxosVariant.v1, "cas_v1");
    }

    @Test
    public void appliedAndNotAppliedV2() throws Exception
    {
        appliedAndNotApplied(Config.PaxosVariant.v2, "cas_v2");
    }

    /**
     * v1 only: v1's ReplicaPlans.forPaxos sizes the required paxos participants from the
     * REPLICATION FACTOR (RF=2 -> quorum 2), so a single live node is unavailable. The v2 engine
     * sizes its consensus quorum from the actual electorate, and a single-node ring with RF=2 has
     * an electorate of one -- the operation legitimately succeeds, so this scenario cannot
     * reproduce under v2 here (v2's exceptional completion is covered by
     * {@link #throwingConditionCompletesExceptionallyV2}).
     */
    @Test
    public void unavailableCompletesExceptionallyV1()
    {
        setPaxosVariant(Config.PaxosVariant.v1);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE_RF2, TABLE);
        DecoratedKey key = dk("cas_unavailable_v1");

        // RF=2 with a single live node: SERIAL needs 2 promises, the operation is unavailable.
        assertThatThrownBy(() -> cas(KEYSPACE_RF2, key, ifEmptyInsert(metadata, key, "unused")))
            .isInstanceOf(UnavailableException.class);

        assertThat(RecordingMutator.casBegun.get()).isEqualTo(1);
        assertThat(RecordingMutator.casCompleted.get()).as("completion also fires on failure").isEqualTo(1);
        assertThat(RecordingMutator.commits).as("no commit was dispatched").isEmpty();
        assertThat(RecordingMutator.completed).as("no commit dispatched, so none completed").isEmpty();
        assertThat(RecordingMutator.mutatePaxosCalls.get()).isZero();
    }

    /** v2 exceptional completion: a condition that throws mid-operation still completes exactly once. */
    @Test
    public void throwingConditionCompletesExceptionallyV2()
    {
        setPaxosVariant(Config.PaxosVariant.v2);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk("cas_throwing_v2");

        CASRequest poisoned = new CASRequest()
        {
            @Override
            public SinglePartitionReadCommand readCommand(long nowInSec)
            {
                return SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSec, key);
            }

            @Override
            public boolean appliesTo(FilteredPartition current)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException("poisoned condition");
            }

            @Override
            public PartitionUpdate makeUpdates(FilteredPartition current, ClientState clientState, Ballot ballot)
            {
                throw new AssertionError("unreachable: the condition throws first");
            }
        };

        assertThatThrownBy(() -> cas(KEYSPACE, key, poisoned))
            .hasMessageContaining("poisoned condition");

        assertThat(RecordingMutator.casBegun.get()).isEqualTo(1);
        assertThat(RecordingMutator.casCompleted.get()).as("completion also fires on failure").isEqualTo(1);
        assertThat(RecordingMutator.commits).as("no commit was dispatched").isEmpty();
        assertThat(RecordingMutator.completed).as("no commit dispatched, so none completed").isEmpty();
        assertThat(RecordingMutator.mutatePaxosCalls.get()).isZero();
    }

    /**
     * Leaves an accepted-but-uncommitted proposal in the paxos table (as if another coordinator
     * died between propose and commit), then runs a CAS on the same partition and asserts the
     * foreign round's completion is reported as REPAIR_IN_PROGRESS — not as this operation's own
     * commit.
     */
    private void inProgressRoundIsReportedAsRepair(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        PartitionUpdate foreignUpdate = insertRow(metadata, key, "foreign", foreignBallot.unixMicros());
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, foreignUpdate));
        // Drop the in-memory paxos state so the next prepare reloads the injected proposal from disk.
        PaxosState.unsafeReset();

        // The repair replays the foreign update, so the partition is no longer empty and our own
        // CAS does not apply — exactly the phantom scenario: the only commit this operation
        // dispatches is the foreign round's, and it must be labeled as repair.
        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "unused")))
        {
            assertThat(result).as("CAS must not apply after the foreign round is replayed").isNotNull();
        }

        assertThat(RecordingMutator.casBegun.get()).isEqualTo(1);
        assertThat(RecordingMutator.casCompleted.get()).isEqualTo(1);
        List<CommitRecord> repairs = RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS);
        assertThat(repairs).as("the foreign in-progress round must be reported as REPAIR_IN_PROGRESS").hasSize(1);
        assertThat(repairs.get(0).commit.update.partitionKey()).isEqualTo(key);
        // Documented CL semantics: the v1 repair commit is performed at the operation's commit CL
        // (QUORUM here) -- exactly. Under v2 the completion can take two routes -- the in-engine
        // begin() repair (reports the operation's consensus CL, SERIAL here) or the PaxosRepair
        // machinery (reports its own commit CL, QUORUM for a SERIAL operation) -- so accept either.
        if (variant == Config.PaxosVariant.v1)
            assertThat(repairs.get(0).consistencyLevel).isEqualTo(ConsistencyLevel.QUORUM);
        else
            assertThat(repairs.get(0).consistencyLevel).isIn(ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION))
            .as("the non-applying operation itself must not commit").isEmpty();

        // Terminal-completion coverage for recovery: every dispatched REPAIR_IN_PROGRESS commit is now
        // paired with a terminal onCasCommitCompleted. The v1 engine completes the foreign round with a
        // blocking commitPaxos, so the terminal is APPLIED (the recovered value reached a commit-CL
        // quorum). The v2 engine completes it in-line via begin()'s commitAndPrepare: the commit is fused
        // into the following prepare, whose promise-quorum implies the commit landed, so the terminal is
        // CONFIRMED_BY_PREPARE (or APPLIED if the PaxosRepair machinery drove it instead) -- a success
        // outcome either way. This is the behaviour that previously delivered NO callback on v2.
        List<CommitRecord> repairCompleted = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS);
        assertThat(repairCompleted).as("the recovered foreign round must produce a terminal completion").isNotEmpty();
        CommitRecord repairTerminal = repairCompleted.get(0);
        assertThat(repairTerminal.commit.update.partitionKey()).isEqualTo(key);
        assertThat(repairTerminal.seq)
            .as("the completion fires after the dispatched one").isGreaterThan(repairs.get(0).seq);
        assertThat(repairTerminal.isSuccess())
            .as("a recovered, quorum-confirmed foreign round completes as a success outcome").isTrue();
        if (variant == Config.PaxosVariant.v1)
            assertThat(repairTerminal.outcome)
                .as("v1 awaits a standalone commit, so the terminal is APPLIED").isEqualTo(Mutator.CasCommitOutcome.APPLIED);
        else
            assertThat(repairTerminal.outcome)
                .as("v2 confirms via the fused prepare (or APPLIED via PaxosRepair)")
                .isIn(Mutator.CasCommitOutcome.CONFIRMED_BY_PREPARE, Mutator.CasCommitOutcome.APPLIED);
    }

    @Test
    public void inProgressRoundIsReportedAsRepairV1()
    {
        inProgressRoundIsReportedAsRepair(Config.PaxosVariant.v1, "cas_repair_v1");
    }

    @Test
    public void inProgressRoundIsReportedAsRepairV2()
    {
        inProgressRoundIsReportedAsRepair(Config.PaxosVariant.v2, "cas_repair_v2");
    }

    /**
     * A commit that fails after the proposal was accepted must still be paired with a terminal.
     * {@code doPaxos} announces the client commit (onCasCommit), then commitPaxos throws; because
     * the proposal already succeeded the value is DECIDED, so the terminal is UNCONFIRMED (decided
     * but not confirmed here) and the original exception still surfaces to the caller.
     *
     * <p>This is the v1 path: only v1 drives the operation's commit through {@code commitPaxos}
     * (hence {@code mutatePaxos}); v2 confirms via its fused commit-and-prepare, covered elsewhere.
     * Two failure modes are exercised to justify catching {@code RuntimeException} rather than just
     * {@code WriteTimeoutException}: a {@link WriteFailureException} (replicas answered with errors,
     * not a timeout -- a different branch of the exception hierarchy) and an
     * {@link UncheckedInterruptedException} (the request thread was interrupted while awaiting the
     * commit ack). Neither is a {@code WriteTimeoutException}, so the pre-fix catch would have
     * dropped the terminal.
     */
    private void commitFailureCompletesUnconfirmed(RuntimeException failure, String keyName)
    {
        setPaxosVariant(Config.PaxosVariant.v1);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        RecordingMutator.commitFailure = failure;
        // The empty partition matches the condition, so the proposal is accepted and the commit is
        // attempted -- where our injected handler fails it. The original exception must surface.
        assertThatThrownBy(() -> cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "v-" + keyName)))
            .isSameAs(failure);

        List<CommitRecord> clientCommits = RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCommits).as("the applying CAS announces exactly one client commit").hasSize(1);

        List<CommitRecord> clientCompleted = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCompleted)
            .as("a failed commit still pairs the announcement with exactly one terminal").hasSize(1);
        CommitRecord terminal = clientCompleted.get(0);
        assertThat(terminal.outcome)
            .as("a decided-but-unconfirmed commit completes as UNCONFIRMED")
            .isEqualTo(Mutator.CasCommitOutcome.UNCONFIRMED);
        assertThat(terminal.isSuccess()).as("UNCONFIRMED is not a success outcome").isFalse();
        assertThat(terminal.commit.update.partitionKey()).isEqualTo(key);
        assertThat(terminal.consistencyLevel).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(terminal.thread)
            .as("the terminal fires on the mutateCas caller thread for CLIENT_OPERATION")
            .isSameAs(Thread.currentThread());
        assertThat(terminal.seq)
            .as("the terminal fires after the announced commit").isGreaterThan(clientCommits.get(0).seq);
    }

    @Test
    public void commitWriteFailureCompletesUnconfirmedV1()
    {
        WriteFailureException failure = new WriteFailureException(ConsistencyLevel.QUORUM, 0, 2, WriteType.SIMPLE,
                                                                 Collections.emptyMap());
        commitFailureCompletesUnconfirmed(failure, "cas_commit_fail_v1");
    }

    @Test
    public void commitInterruptedCompletesUnconfirmedV1()
    {
        commitFailureCompletesUnconfirmed(new UncheckedInterruptedException(), "cas_commit_interrupt_v1");
    }

    /**
     * The veto contract of {@link Mutator#onCasCommit}: a {@link OverloadedException} (any
     * RequestExecutionException) thrown for a CLIENT_OPERATION commit propagates to the caller, the
     * commit dispatch is skipped, and the announced commit is paired with an UNCONFIRMED terminal.
     * The value is nonetheless DECIDED: the follow-up CAS on the same partition must find and
     * complete it as a repair, and its condition must see the vetoed row.
     */
    private void vetoPropagatesAndValueCompletesLater(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        OverloadedException veto = new OverloadedException("mutator veto: downstream overloaded");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = Mutator.CasCommitOrigin.CLIENT_OPERATION;

        // The empty partition matches the condition, the proposal is accepted (DECIDED), then the
        // commit announcement vetoes: the exact exception instance must surface to the caller.
        assertThatThrownBy(() -> cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "v-" + keyName)))
            .isSameAs(veto);

        assertThat(RecordingMutator.casBegun.get()).isEqualTo(1);
        assertThat(RecordingMutator.casCompleted.get()).as("completion also fires on a veto").isEqualTo(1);
        List<CommitRecord> clientCommits = RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCommits).as("the veto still records the announced commit").hasSize(1);
        assertThat(RecordingMutator.mutatePaxosCalls.get())
            .as("a vetoed commit must never be dispatched (v1's commit transport is not invoked)")
            .isZero();
        List<CommitRecord> clientCompleted = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCompleted).as("the vetoed commit is paired with exactly one terminal").hasSize(1);
        CommitRecord terminal = clientCompleted.get(0);
        assertThat(terminal.outcome)
            .as("a vetoed commit is decided but not confirmed").isEqualTo(Mutator.CasCommitOutcome.UNCONFIRMED);
        assertThat(terminal.commit.update.partitionKey()).isEqualTo(key);
        assertThat(terminal.thread).isSameAs(Thread.currentThread());
        assertThat(terminal.seq).isGreaterThan(clientCommits.get(0).seq);

        // The vetoed value is decided: a later operation must complete it as a repair, and once the
        // row is visible this second CAS must not apply.
        RecordingMutator.reset();
        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "unused")))
        {
            assertThat(result)
                .as("the vetoed-but-decided row must be visible to the follow-up CAS (condition not met)")
                .isNotNull();
        }
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the vetoed round is completed by the follow-up operation as an in-progress repair")
            .hasSize(1);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION))
            .as("the non-applying follow-up must not commit anything of its own").isEmpty();
    }

    @Test
    public void vetoPropagatesAndValueCompletesLaterV1()
    {
        vetoPropagatesAndValueCompletesLater(Config.PaxosVariant.v1, "cas_veto_v1");
    }

    @Test
    public void vetoPropagatesAndValueCompletesLaterV2()
    {
        vetoPropagatesAndValueCompletesLater(Config.PaxosVariant.v2, "cas_veto_v2");
    }

    /**
     * Only the RequestExecutionException family may veto: any other throwable from
     * {@link Mutator#onCasCommit} — including RequestValidationExceptions like
     * {@link InvalidRequestException} — keeps the historic containment (logged and ignored), so the
     * operation applies normally and completes with an APPLIED terminal.
     */
    private void containedOnCasCommitFailure(Config.PaxosVariant variant, RuntimeException failure, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        RecordingMutator.onCasCommitFailure = failure;
        RecordingMutator.onCasCommitFailureOrigin = Mutator.CasCommitOrigin.CLIENT_OPERATION;

        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "v-" + keyName)))
        {
            assertThat(result).as("a contained onCasCommit failure must not affect the operation").isNull();
        }
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION)).hasSize(1);
        List<CommitRecord> clientCompleted = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCompleted).hasSize(1);
        assertThat(clientCompleted.get(0).outcome)
            .as("the commit proceeds and is acknowledged despite the contained callback failure")
            .isEqualTo(Mutator.CasCommitOutcome.APPLIED);
        assertThat(RecordingMutator.mutatePaxosCalls.get())
            .as("the commit is dispatched normally")
            .isEqualTo(variant == Config.PaxosVariant.v1 ? 1 : 0);
    }

    @Test
    public void runtimeExceptionFromOnCasCommitIsContainedV1()
    {
        containedOnCasCommitFailure(Config.PaxosVariant.v1, new RuntimeException("boom"), "cas_contained_rte_v1");
    }

    @Test
    public void runtimeExceptionFromOnCasCommitIsContainedV2()
    {
        containedOnCasCommitFailure(Config.PaxosVariant.v2, new RuntimeException("boom"), "cas_contained_rte_v2");
    }

    @Test
    public void invalidRequestFromOnCasCommitIsContainedV1()
    {
        containedOnCasCommitFailure(Config.PaxosVariant.v1, new InvalidRequestException("not a veto"),
                                    "cas_contained_ire_v1");
    }

    @Test
    public void invalidRequestFromOnCasCommitIsContainedV2()
    {
        containedOnCasCommitFailure(Config.PaxosVariant.v2, new InvalidRequestException("not a veto"),
                                    "cas_contained_ire_v2");
    }

    /**
     * The veto applies to repair origins too — as a "defer, retry later" signal: throwing a
     * RequestExecutionException while completing a foreign in-progress round skips the repair
     * commit and fails the driving operation, but the DECIDED round is not lost — the next
     * operation on the partition re-attempts the repair (re-firing the callback) and, once the
     * implementation stops throwing, completes it.
     */
    private void repairOriginVetoDefersAndRetries(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        PartitionUpdate foreignUpdate = insertRow(metadata, key, "foreign", foreignBallot.unixMicros());
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, foreignUpdate));
        PaxosState.unsafeReset();

        OverloadedException veto = new OverloadedException("mutator veto during repair");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = null; // every origin

        // The operation must complete the foreign round before proceeding, so the veto of that
        // repair commit aborts the whole (innocent) driving operation with the thrown exception.
        assertThatThrownBy(() -> cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "unused")))
            .isSameAs(veto);
        assertThat(RecordingMutator.casCompleted.get()).as("completion also fires on a veto").isEqualTo(1);
        List<CommitRecord> repairs = RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS);
        assertThat(repairs).as("the vetoed repair dispatch is still announced").isNotEmpty();
        assertThat(RecordingMutator.completed)
            .as("nothing may complete successfully: the vetoed repair commit was never dispatched")
            .noneMatch(CommitRecord::isSuccess);
        if (variant == Config.PaxosVariant.v1)
        {
            assertThat(RecordingMutator.mutatePaxosCalls.get())
                .as("the vetoed repair commit must not reach the v1 commit transport").isZero();
            List<CommitRecord> terminals = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS);
            assertThat(terminals).as("the v1 in-progress site pairs the veto with a terminal").hasSize(1);
            assertThat(terminals.get(0).outcome).isEqualTo(Mutator.CasCommitOutcome.UNCONFIRMED);
        }
        else
        {
            // v2's begin()-path repair pairs the veto with an UNCONFIRMED terminal; a completion
            // driven by the PaxosRepair machinery instead delivers none on failure — either way no
            // success terminal may appear (asserted above), and any delivered one is UNCONFIRMED.
            assertThat(RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
                .allMatch(r -> r.outcome == Mutator.CasCommitOutcome.UNCONFIRMED);
        }

        // "Retried later, not lost": once the implementation stops throwing, the next operation
        // finds the still-uncommitted round, re-fires the repair callback and completes it — the
        // foreign row becomes visible, so this CAS does not apply.
        RecordingMutator.reset();
        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "unused")))
        {
            assertThat(result)
                .as("the retried repair must complete the vetoed round (foreign row visible)")
                .isNotNull();
        }
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the retry re-fires the repair-origin callback").isNotEmpty();
    }

    @Test
    public void repairOriginVetoDefersAndRetriesV1()
    {
        repairOriginVetoDefersAndRetries(Config.PaxosVariant.v1, "cas_repair_veto_v1");
    }

    @Test
    public void repairOriginVetoDefersAndRetriesV2()
    {
        repairOriginVetoDefersAndRetries(Config.PaxosVariant.v2, "cas_repair_veto_v2");
    }

    /**
     * Containment at repair origins is unchanged for anything that is not a
     * RequestExecutionException: completing a foreign in-progress round succeeds despite the
     * throwing callback.
     */
    private void nonVetoAtRepairOriginIsContained(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        PartitionUpdate foreignUpdate = insertRow(metadata, key, "foreign", foreignBallot.unixMicros());
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, foreignUpdate));
        PaxosState.unsafeReset();

        RecordingMutator.onCasCommitFailure = new RuntimeException("contained at repair origins");
        RecordingMutator.onCasCommitFailureOrigin = null; // every origin

        try (RowIterator result = cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "unused")))
        {
            assertThat(result).as("the repaired foreign round makes the CAS non-applying").isNotNull();
        }
        assertThat(RecordingMutator.casCompleted.get()).isEqualTo(1);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the foreign round was still repaired despite the throwing callback").hasSize(1);
    }

    @Test
    public void nonVetoAtRepairOriginIsContainedV1()
    {
        nonVetoAtRepairOriginIsContained(Config.PaxosVariant.v1, "cas_repair_contained_v1");
    }

    @Test
    public void nonVetoAtRepairOriginIsContainedV2()
    {
        nonVetoAtRepairOriginIsContained(Config.PaxosVariant.v2, "cas_repair_contained_v2");
    }

    /**
     * The veto reaches SERIAL readers too: a read that must complete a foreign in-progress round
     * before proceeding fails with the exact vetoed exception, marks the CAS-read unavailables
     * meter, and — once the implementation stops throwing — the retried read completes the round
     * and observes the foreign row.
     */
    private void serialReadVetoDefersAndRetries(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        PartitionUpdate foreignUpdate = insertRow(metadata, key, "foreign", foreignBallot.unixMicros());
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, foreignUpdate));
        PaxosState.unsafeReset();

        OverloadedException veto = new OverloadedException("mutator veto during serial read");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = null; // every origin

        long unavailablesBefore = casReadUnavailables();
        assertThatThrownBy(() -> serialRead(metadata, key)).isSameAs(veto);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the vetoed repair dispatch is still announced to the reader's mutator").isNotEmpty();
        assertThat(casReadUnavailables())
            .as("an Overloaded veto of a serial read marks the CAS-read unavailables meter (both engines)")
            .isEqualTo(unavailablesBefore + 1);

        // Deferred, not lost: the retried read completes the round and sees the foreign row.
        RecordingMutator.reset();
        try (PartitionIterator partitions = serialRead(metadata, key))
        {
            assertThat(partitions.hasNext()).as("the retried read returns the partition").isTrue();
            try (RowIterator rows = partitions.next())
            {
                assertThat(rows.hasNext())
                    .as("the retried read must observe the row of the previously vetoed round").isTrue();
            }
        }
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the retried read re-fires the repair-origin callback").isNotEmpty();
    }

    private static PartitionIterator serialRead(TableMetadata metadata, DecoratedKey key)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);
        return StorageProxy.read(SinglePartitionReadCommand.Group.one(command),
                                 ConsistencyLevel.SERIAL,
                                 ClientState.forInternalCalls(),
                                 Dispatcher.RequestTime.forImmediateExecution());
    }

    private static long casReadUnavailables()
    {
        return ClientRequestsMetricsProvider.instance.metrics(KEYSPACE).casReadMetrics.unavailables.getCount();
    }

    @Test
    public void serialReadVetoDefersAndRetriesV1()
    {
        serialReadVetoDefersAndRetries(Config.PaxosVariant.v1, "cas_read_veto_v1");
    }

    @Test
    public void serialReadVetoDefersAndRetriesV2()
    {
        serialReadVetoDefersAndRetries(Config.PaxosVariant.v2, "cas_read_veto_v2");
    }

    /** A RequestExecutionException subtype outside the concrete write/read exception families. */
    static final class CustomVetoException extends RequestExecutionException
    {
        CustomVetoException(String message)
        {
            super(ExceptionCode.SERVER_ERROR, message);
        }
    }

    /** A timeout-family subtype outside the concrete write/read timeout classes. */
    static final class CustomTimeoutVetoException extends RequestTimeoutException
    {
        CustomTimeoutVetoException(String message)
        {
            super(ExceptionCode.WRITE_TIMEOUT, ConsistencyLevel.QUORUM, 0, 1, message);
        }
    }

    /** A failure-family subtype outside the concrete write/read failure classes. */
    static final class CustomFailureVetoException extends RequestFailureException
    {
        CustomFailureVetoException(String message)
        {
            super(ExceptionCode.WRITE_FAILURE, message, ConsistencyLevel.QUORUM, 0, 1, Collections.emptyMap());
        }
    }

    private static ClientRequestsMetrics requestMetrics()
    {
        return ClientRequestsMetricsProvider.instance.metrics(KEYSPACE);
    }

    /** Arms {@code veto} for CLIENT_OPERATION, runs an applying CAS and asserts the exact instance surfaces. */
    private void clientVeto(RuntimeException veto, String keyName)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);
        RecordingMutator.reset();
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = Mutator.CasCommitOrigin.CLIENT_OPERATION;
        assertThatThrownBy(() -> cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "v-" + keyName))).isSameAs(veto);
    }

    /**
     * Arms {@code veto} for every origin, injects a foreign in-progress round for {@code keyName}
     * and asserts a SERIAL read fails with the exact instance while completing it.
     */
    private void serialReadVeto(RuntimeException veto, String keyName)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);
        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, insertRow(metadata, key, "foreign", foreignBallot.unixMicros())));
        PaxosState.unsafeReset();
        RecordingMutator.reset();
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = null;
        assertThatThrownBy(() -> serialRead(metadata, key)).isSameAs(veto);
    }

    /**
     * Every veto exception family marks its matching client-request meter on the v2 engine
     * ({@code Paxos.markCasVeto}): Unavailable → unavailables, timeout family → timeouts, failure
     * family → failures. (The Overloaded → unavailables branch is covered by the veto tests above.)
     */
    @Test
    public void vetoFamilyMarksMatchingMeterV2()
    {
        setPaxosVariant(Config.PaxosVariant.v2);
        ClientRequestsMetrics metrics = requestMetrics();

        long unavailables = metrics.casWriteMetrics.unavailables.getCount();
        clientVeto(UnavailableException.create(ConsistencyLevel.QUORUM, 1, 0), "cas_veto_meter_ua_v2");
        assertThat(metrics.casWriteMetrics.unavailables.getCount()).isEqualTo(unavailables + 1);

        long timeouts = metrics.casWriteMetrics.timeouts.getCount();
        clientVeto(new CasWriteTimeoutException(WriteType.CAS, ConsistencyLevel.QUORUM, 0, 1, 0), "cas_veto_meter_to_v2");
        assertThat(metrics.casWriteMetrics.timeouts.getCount()).isEqualTo(timeouts + 1);

        long failures = metrics.casWriteMetrics.failures.getCount();
        clientVeto(new WriteFailureException(ConsistencyLevel.QUORUM, 0, 1, WriteType.CAS, Collections.emptyMap()), "cas_veto_meter_fl_v2");
        assertThat(metrics.casWriteMetrics.failures.getCount()).isEqualTo(failures + 1);
    }

    /**
     * The v1 fallback catch in legacyCas routes veto types outside the concrete classes to the
     * closest family meter, and the exact instance still surfaces.
     */
    @Test
    public void customFamilyVetoMarksMatchingMeterV1()
    {
        setPaxosVariant(Config.PaxosVariant.v1);
        ClientRequestsMetrics metrics = requestMetrics();

        long timeouts = metrics.casWriteMetrics.timeouts.getCount();
        clientVeto(new CustomTimeoutVetoException("custom timeout veto"), "cas_veto_meter_to_v1");
        assertThat(metrics.casWriteMetrics.timeouts.getCount()).isEqualTo(timeouts + 1);

        long failures = metrics.casWriteMetrics.failures.getCount();
        clientVeto(new CustomFailureVetoException("custom failure veto"), "cas_veto_meter_fl_v1");
        assertThat(metrics.casWriteMetrics.failures.getCount()).isEqualTo(failures + 1);
    }

    /** Same family routing through the v1 fallback catch in legacyReadWithPaxos (serial-read path). */
    @Test
    public void serialReadCustomFamilyVetoMarksMatchingMeterV1()
    {
        setPaxosVariant(Config.PaxosVariant.v1);
        ClientRequestsMetrics metrics = requestMetrics();

        long timeouts = metrics.casReadMetrics.timeouts.getCount();
        serialReadVeto(new CustomTimeoutVetoException("custom timeout veto"), "cas_read_veto_meter_to_v1");
        assertThat(metrics.casReadMetrics.timeouts.getCount()).isEqualTo(timeouts + 1);

        long failures = metrics.casReadMetrics.failures.getCount();
        serialReadVeto(new CustomFailureVetoException("custom failure veto"), "cas_read_veto_meter_fl_v1");
        assertThat(metrics.casReadMetrics.failures.getCount()).isEqualTo(failures + 1);
    }

    /** The v2 read-side veto marks the CAS-read meters ({@code markCasVeto} with isWrite=false). */
    @Test
    public void serialReadTimeoutFamilyVetoMarksTimeoutsV2()
    {
        setPaxosVariant(Config.PaxosVariant.v2);
        ClientRequestsMetrics metrics = requestMetrics();

        long timeouts = metrics.casReadMetrics.timeouts.getCount();
        serialReadVeto(new CustomTimeoutVetoException("custom timeout veto"), "cas_read_veto_meter_to_v2");
        assertThat(metrics.casReadMetrics.timeouts.getCount()).isEqualTo(timeouts + 1);
    }

    /**
     * A veto typed outside the concrete exception families still propagates on both engines — on
     * v1 through the fallback catch in legacyCas that keeps the query tracker informed — skips the
     * dispatch, and pairs the announcement with an UNCONFIRMED terminal.
     */
    private void customVetoTypePropagates(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        CustomVetoException veto = new CustomVetoException("custom veto type");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = Mutator.CasCommitOrigin.CLIENT_OPERATION;

        assertThatThrownBy(() -> cas(KEYSPACE, key, ifEmptyInsert(metadata, key, "v-" + keyName)))
            .isSameAs(veto);
        assertThat(RecordingMutator.mutatePaxosCalls.get())
            .as("the vetoed commit must never be dispatched").isZero();
        List<CommitRecord> terminals = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(terminals).as("the vetoed commit is paired with exactly one terminal").hasSize(1);
        assertThat(terminals.get(0).outcome).isEqualTo(Mutator.CasCommitOutcome.UNCONFIRMED);
    }

    @Test
    public void customVetoTypePropagatesV1()
    {
        customVetoTypePropagates(Config.PaxosVariant.v1, "cas_custom_veto_v1");
    }

    @Test
    public void customVetoTypePropagatesV2()
    {
        customVetoTypePropagates(Config.PaxosVariant.v2, "cas_custom_veto_v2");
    }

    /**
     * Custom-typed veto on the serial-read path: exercises the fallback catch in
     * legacyReadWithPaxos (v1) and the raw v2 propagation — the exact instance surfaces to the
     * reader even for a type outside the concrete exception families.
     */
    private void serialReadCustomVetoPropagates(Config.PaxosVariant variant, String keyName)
    {
        setPaxosVariant(variant);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk(keyName);

        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        PartitionUpdate foreignUpdate = insertRow(metadata, key, "foreign", foreignBallot.unixMicros());
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, foreignUpdate));
        PaxosState.unsafeReset();

        CustomVetoException veto = new CustomVetoException("custom veto during serial read");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = null; // every origin

        assertThatThrownBy(() -> serialRead(metadata, key)).isSameAs(veto);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the vetoed repair dispatch is still announced").isNotEmpty();
    }

    @Test
    public void serialReadCustomVetoPropagatesV1()
    {
        serialReadCustomVetoPropagates(Config.PaxosVariant.v1, "cas_read_custom_veto_v1");
    }

    @Test
    public void serialReadCustomVetoPropagatesV2()
    {
        serialReadCustomVetoPropagates(Config.PaxosVariant.v2, "cas_read_custom_veto_v2");
    }

    /**
     * Background {@code PaxosRepair} pairs every announce with a terminal even on failure: a veto
     * thrown from {@link Mutator#onCasCommit} fails the repair attempt AND closes the announce
     * with an UNCONFIRMED terminal (previously the failure was swallowed with no terminal, leaking
     * any in-flight state a tracking implementation opened on the announce). Once the
     * implementation stops throwing, a re-run repair completes the round with an APPLIED terminal.
     */
    @Test
    public void paxosRepairVetoDeliversUnconfirmed() throws Exception
    {
        setPaxosVariant(Config.PaxosVariant.v2);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk("cas_paxos_repair_veto");

        Ballot foreignBallot = BallotGenerator.Global.nextBallot(Ballot.Flag.GLOBAL);
        PartitionUpdate foreignUpdate = insertRow(metadata, key, "foreign", foreignBallot.unixMicros());
        SystemKeyspace.savePaxosProposal(Commit.newProposal(foreignBallot, foreignUpdate));
        PaxosState.unsafeReset();

        OverloadedException veto = new OverloadedException("mutator veto during background repair");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = null; // every origin

        // null success criteria: passing the foreign ballot itself would clash with the witnessed
        // ballot (reproposalMayBeRejected) and poison-loop to the retry timeout instead of
        // completing the accepted proposal
        AbstractPaxosRepair.Result result =
            PaxosRepair.create(ConsistencyLevel.SERIAL, key, null, metadata).start().await();
        assertThat(result).as("the vetoed repair attempt fails").isInstanceOf(AbstractPaxosRepair.Failure.class);
        List<CommitRecord> announces = RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS);
        assertThat(announces).as("the vetoed repair commit is still announced").hasSize(1);
        // the completion listener runs just after await() unblocks, on the repair's thread
        List<CommitRecord> terminals = awaitCompleted(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS, 1);
        assertThat(terminals).as("the failed repair pairs the announce with a terminal").hasSize(1);
        assertThat(terminals.get(0).outcome).isEqualTo(Mutator.CasCommitOutcome.UNCONFIRMED);
        assertThat(terminals.get(0).commit.update.partitionKey()).isEqualTo(key);

        // Deferred, not lost: with the veto disarmed a re-run repair completes the round.
        RecordingMutator.reset();
        result = PaxosRepair.create(ConsistencyLevel.SERIAL, key, null, metadata).start().await();
        assertThat(result).as("the retried repair succeeds").isNotInstanceOf(AbstractPaxosRepair.Failure.class);
        assertThat(RecordingMutator.commitsWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS))
            .as("the retried repair re-announces the commit").hasSize(1);
        List<CommitRecord> applied = awaitCompleted(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS, 1);
        assertThat(applied).hasSize(1);
        assertThat(applied.get(0).outcome)
            .as("an acknowledged repair commit completes as APPLIED")
            .isEqualTo(Mutator.CasCommitOutcome.APPLIED);
    }

    /** The repair state machine delivers terminals on its own threads, just after await() unblocks. */
    private static List<CommitRecord> awaitCompleted(Mutator.CasCommitOrigin origin, int expected)
    {
        try
        {
            Awaitility.await()
                      .atMost(10, TimeUnit.SECONDS)
                      .pollInterval(10, TimeUnit.MILLISECONDS)
                      .until(() -> RecordingMutator.completedWithOrigin(origin).size() >= expected);
        }
        catch (ConditionTimeoutException e)
        {
            // fall through: the caller's assertion reports the terminals actually delivered
        }
        return RecordingMutator.completedWithOrigin(origin);
    }

    /**
     * v1, commit CL=ANY: normally ANY delivers no terminal (commitPaxos does not await an ack), but
     * a veto always pairs the announcement with an UNCONFIRMED terminal — ANY included.
     */
    @Test
    public void vetoAtConsistencyAnyDeliversUnconfirmedV1()
    {
        setPaxosVariant(Config.PaxosVariant.v1);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey key = dk("cas_veto_any_v1");

        OverloadedException veto = new OverloadedException("mutator veto at ANY");
        RecordingMutator.onCasCommitFailure = veto;
        RecordingMutator.onCasCommitFailureOrigin = Mutator.CasCommitOrigin.CLIENT_OPERATION;

        assertThatThrownBy(() -> StorageProxy.cas(KEYSPACE,
                                                  TABLE,
                                                  key,
                                                  ifEmptyInsert(metadata, key, "v-any"),
                                                  ConsistencyLevel.SERIAL,
                                                  ConsistencyLevel.ANY,
                                                  ClientState.forInternalCalls(),
                                                  FBUtilities.nowInSeconds(),
                                                  Dispatcher.RequestTime.forImmediateExecution()))
            .isSameAs(veto);

        List<CommitRecord> clientCompleted = RecordingMutator.completedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientCompleted).as("a veto delivers the UNCONFIRMED terminal even at CL=ANY").hasSize(1);
        assertThat(clientCompleted.get(0).outcome).isEqualTo(Mutator.CasCommitOutcome.UNCONFIRMED);
        assertThat(RecordingMutator.mutatePaxosCalls.get()).isZero();
    }
}
