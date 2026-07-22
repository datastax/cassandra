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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

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
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

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

    /** One record per {@link Mutator#onCasCommit} / {@link Mutator#onCasCommitApplied} callback. */
    static final class CommitRecord
    {
        final Commit commit;
        final ConsistencyLevel consistencyLevel;
        final Mutator.CasCommitOrigin origin;
        final Thread thread;
        final int seq;

        CommitRecord(Commit commit, ConsistencyLevel consistencyLevel, Mutator.CasCommitOrigin origin)
        {
            this.commit = commit;
            this.consistencyLevel = consistencyLevel;
            this.origin = origin;
            this.thread = Thread.currentThread();
            this.seq = sequence.incrementAndGet();
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
        static final List<CommitRecord> applied = new CopyOnWriteArrayList<>();

        private final Mutator delegate = new StorageProxy.DefaultMutator();

        static void reset()
        {
            casBegun.set(0);
            casCompleted.set(0);
            mutatePaxosCalls.set(0);
            commits.clear();
            applied.clear();
            sequence.set(0);
        }

        static List<CommitRecord> commitsWithOrigin(Mutator.CasCommitOrigin origin)
        {
            return withOrigin(commits, origin);
        }

        static List<CommitRecord> appliedWithOrigin(Mutator.CasCommitOrigin origin)
        {
            return withOrigin(applied, origin);
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
        }

        @Override
        public void onCasCommitApplied(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin)
        {
            applied.add(new CommitRecord(committed, consistencyLevel, origin));
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
            return delegate.mutatePaxos(proposal, consistencyLevel, allowHints, requestTime);
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
        // The applied callback fires once, after the dispatched one, on the same (request) thread,
        // once the commit is acknowledged at the commit CL -- under both paxos variants.
        List<CommitRecord> clientApplied = RecordingMutator.appliedWithOrigin(Mutator.CasCommitOrigin.CLIENT_OPERATION);
        assertThat(clientApplied).as("exactly one CLIENT_OPERATION applied callback for an applied CAS").hasSize(1);
        CommitRecord applied = clientApplied.get(0);
        assertThat(applied.commit.update.partitionKey()).isEqualTo(key);
        assertThat(applied.commit.update.isEmpty()).isFalse();
        assertThat(applied.consistencyLevel).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(applied.thread)
            .as("onCasCommitApplied fires on the mutateCas caller thread for CLIENT_OPERATION")
            .isSameAs(Thread.currentThread());
        assertThat(applied.seq)
            .as("onCasCommitApplied fires AFTER the dispatched onCasCommit")
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
        assertThat(RecordingMutator.applied)
            .as("a non-applying CAS commits nothing, so no applied callback fires").isEmpty();
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
        assertThat(RecordingMutator.applied).as("no commit dispatched, so none applied").isEmpty();
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
        assertThat(RecordingMutator.applied).as("no commit dispatched, so none applied").isEmpty();
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

        // Applied-callback coverage for recovery: the v1 engine completes the foreign round with a
        // blocking commitPaxos, so REPAIR_IN_PROGRESS also produces an applied callback (with the
        // foreign payload, once the recovered value reached a commit-CL quorum). The v2 engine
        // completes it in-line via begin()'s commitAndPrepare, where the commit piggybacks on the
        // following prepare and has no separable ack -- that path is dispatched-only by design, so
        // no applied callback fires here.
        List<CommitRecord> repairApplied = RecordingMutator.appliedWithOrigin(Mutator.CasCommitOrigin.REPAIR_IN_PROGRESS);
        if (variant == Config.PaxosVariant.v1)
        {
            assertThat(repairApplied).as("v1 repair commit is awaited, so an applied callback fires").isNotEmpty();
            assertThat(repairApplied.get(0).commit.update.partitionKey()).isEqualTo(key);
            assertThat(repairApplied.get(0).seq)
                .as("the applied callback fires after the dispatched one").isGreaterThan(repairs.get(0).seq);
        }
        else
        {
            assertThat(repairApplied)
                .as("v2 begin()-path repair is dispatched-only; no applied callback for this recovery route")
                .isEmpty();
        }
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
}
