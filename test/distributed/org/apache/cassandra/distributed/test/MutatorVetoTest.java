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

package org.apache.cassandra.distributed.test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.Mutator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_USE_SELF_EXECUTION;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_AND_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REFRESH_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PROPOSE_REQ;

/**
 * Multi-node coverage for the {@link Mutator#onCasCommit} veto paths that a single-node unit test
 * cannot reach — they all require a replica that missed a commit:
 * <ul>
 *   <li>{@code Paxos.begin}'s FOUND_INCOMPLETE_COMMITTED (REFRESH_COMMITTED) veto catch: only
 *       taken when no promise carries a read response with the latest commit, i.e. when the only
 *       up-to-date replica is a pending (bootstrapping) one, which is sent its prepare without a
 *       read;</li>
 *   <li>{@code PaxosRepair}'s query-phase REFRESH_COMMITTED announce, its veto/UNCONFIRMED
 *       pairing, and the re-announce-closes-previous-announce branch (a failed commit attempt
 *       retried);</li>
 *   <li>{@code PaxosRepair.CommitAndRestart}'s APPLIED terminal (poison prepare finding an
 *       incomplete commit).</li>
 * </ul>
 * Uses the {@link CASTestBase} choreography (paxos v2, self-execution via messaging so that
 * commit messages — self included — can be dropped with filters).
 */
public class MutatorVetoTest extends CASTestBase
{
    private static final String REQUEST_TIMEOUT = "2000ms";
    private static final String CONTENTION_TIMEOUT = "2000ms";

    private static Cluster cluster;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        PAXOS_USE_SELF_EXECUTION.setBoolean(false);
        // Read once at MutatorProvider class-init inside every instance classloader (system
        // properties are JVM-global), exactly how a real deployment installs a custom Mutator.
        CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS.setString(VetoMutator.class.getName());
        TestBaseImpl.beforeClass();
        cluster = init(Cluster.build(4)
                              .withConfig(config -> config.set("paxos_variant", "v2")
                                                          .set("write_request_timeout", REQUEST_TIMEOUT)
                                                          .set("cas_contention_timeout", CONTENTION_TIMEOUT)
                                                          .set("request_timeout", REQUEST_TIMEOUT))
                              .withoutVNodes()
                              .start(), 3);
        // Warm the paxos paths on every coordinator: the first CAS of a cold cluster can burn its
        // whole operation deadline before reaching the commit phase, leaving the scenarios below
        // with nothing to repair.
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".warmup (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = 1; i <= 3; i++)
            cluster.coordinator(i).execute("INSERT INTO " + KEYSPACE + ".warmup (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, i);
        // Schema changes need every instance reachable: create all scenario tables while the ring
        // is whole.
        for (String table : new String[]{ "veto_repair", "veto_poison", "veto_begin" })
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        // Class invariant: {4} stays out of the ring, so every scenario runs on the stable
        // {1, 2, 3} topology; only beginIncompleteCommittedVetoAbortsOperation promotes {4} to
        // bootstrapping and puts it back afterwards.
        takeNodeFourOutOfRing();
    }

    @AfterClass
    public static void afterClass()
    {
        if (cluster != null)
            cluster.close();
    }

    /**
     * Runs inside each instance; delegates the engine work and records/vetoes commit callbacks.
     * Mirrors the deployment pattern of MutatorCasTest.RecordingMutator.
     */
    public static class VetoMutator implements Mutator
    {
        /** When true, {@link #onCasCommit} vetoes REFRESH_COMMITTED dispatches with an OverloadedException. */
        public static volatile boolean vetoRefresh;
        public static final AtomicInteger refreshAnnounced = new AtomicInteger();
        /** Every announce, as "ORIGIN" (diagnostics). */
        public static final List<String> announces = new CopyOnWriteArrayList<>();
        /** One "ORIGIN:OUTCOME" entry per {@link #onCasCommitCompleted}. */
        public static final List<String> terminals = new CopyOnWriteArrayList<>();

        private final Mutator delegate = new StorageProxy.DefaultMutator();

        public static void reset()
        {
            vetoRefresh = false;
            refreshAnnounced.set(0);
            announces.clear();
            terminals.clear();
        }

        @Override
        public void onCasCommit(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin)
        {
            announces.add(origin.toString());
            if (origin == CasCommitOrigin.REFRESH_COMMITTED)
            {
                refreshAnnounced.incrementAndGet();
                if (vetoRefresh)
                    throw new OverloadedException("dtest mutator veto");
            }
        }

        @Override
        public void onCasCommitCompleted(Commit committed, ConsistencyLevel consistencyLevel, CasCommitOrigin origin, CasCommitOutcome outcome)
        {
            terminals.add(origin + ":" + outcome);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateStandard(Mutation mutation, ConsistencyLevel consistencyLevel, String localDataCenter,
                                                                      StorageProxy.WritePerformer writePerformer, Runnable callback,
                                                                      WriteType writeType, Dispatcher.RequestTime requestTime)
        {
            return delegate.mutateStandard(mutation, consistencyLevel, localDataCenter, writePerformer, callback, writeType, requestTime);
        }

        @Override
        public void mutateAtomically(Collection<Mutation> mutations, ConsistencyLevel consistencyLevel, boolean requireQuorumForRemove,
                                     Dispatcher.RequestTime requestTime, ClientRequestsMetrics metrics, ClientState clientState)
        {
            delegate.mutateAtomically(mutations, consistencyLevel, requireQuorumForRemove, requestTime, metrics, clientState);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, Dispatcher.RequestTime requestTime)
        {
            return delegate.mutateCounter(cm, localDataCenter, requestTime);
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounterOnLeader(CounterMutation mutation, String localDataCenter,
                                                                             StorageProxy.WritePerformer performer, Runnable callback,
                                                                             Dispatcher.RequestTime requestTime)
        {
            return delegate.mutateCounterOnLeader(mutation, localDataCenter, performer, callback, requestTime);
        }

        @Nullable
        @Override
        public AbstractWriteResponseHandler<Commit> mutatePaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, Dispatcher.RequestTime requestTime)
        {
            return delegate.mutatePaxos(proposal, consistencyLevel, allowHints, requestTime);
        }

        @Override
        public RowIterator mutateCas(TableMetadata metadata, DecoratedKey key, CASRequest request, ConsistencyLevel consistencyForPaxos,
                                     ConsistencyLevel consistencyForCommit, ClientState clientState, long nowInSeconds, Dispatcher.RequestTime requestTime)
        {
            return Mutator.super.mutateCas(metadata, key, request, consistencyForPaxos, consistencyForCommit, clientState, nowInSeconds, requestTime);
        }

        @Override
        public void persistBatchlog(Collection<Mutation> mutations, Dispatcher.RequestTime requestTime, ReplicaPlan.ForWrite replicaPlan, TimeUUID batchUUID)
        {
            delegate.persistBatchlog(mutations, requestTime, replicaPlan, batchUUID);
        }

        @Override
        public void clearBatchlog(String keyspace, Dispatcher.RequestTime requestTime, ReplicaPlan.ForWrite replicaPlan, TimeUUID batchUUID)
        {
            delegate.clearBatchlog(keyspace, requestTime, replicaPlan, batchUUID);
        }
    }

    private static void resetMutator(int node)
    {
        cluster.get(node).runOnInstance(VetoMutator::reset);
    }

    private static String announces(int node)
    {
        return cluster.get(node).callOnInstance(() -> String.join(",", VetoMutator.announces));
    }

    private static void armRefreshVeto(int node, boolean armed)
    {
        cluster.get(node).runOnInstance(() -> VetoMutator.vetoRefresh = armed);
    }

    private static int refreshAnnounced(int node)
    {
        // NOTE: must be a lambda, not a bound method reference — the latter would capture this
        // classloader's static AtomicInteger instead of the instance's.
        return cluster.get(node).callOnInstance(() -> VetoMutator.refreshAnnounced.get());
    }

    private static List<String> awaitTerminals(int node, int expected)
    {
        try
        {
            Awaitility.await()
                      .atMost(20, TimeUnit.SECONDS)
                      .pollInterval(50, TimeUnit.MILLISECONDS)
                      .until(() -> terminals(node).size() >= expected);
        }
        catch (ConditionTimeoutException e)
        {
            // fall through: the caller's assertion reports the terminals actually delivered
        }
        return terminals(node);
    }

    private static List<String> terminals(int node)
    {
        String joined = cluster.get(node).callOnInstance(() -> String.join(",", VetoMutator.terminals));
        return joined.isEmpty() ? List.of() : List.of(joined.split(","));
    }

    private static void takeNodeFourOutOfRing()
    {
        for (int i = 1; i <= 4; ++i)
            cluster.get(i).acceptsOnInstance(CASTestBase::removeFromRing).accept(cluster.get(4));
    }


    /** Runs a PaxosRepair for {@code pk} on {@code node}; returns "OK", or the failure cause prefixed with "FAILURE: ". */
    private static String repairOnNode(int node, String tableName, int pk)
    {
        return cluster.get(node).callOnInstance(() -> {
            TableMetadata schema = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName).metadata.get();
            DecoratedKey key = schema.partitioner.decorateKey(Int32Type.instance.decompose(pk));
            try
            {
                AbstractPaxosRepair.Result result = PaxosRepair.create(SERIAL, key, null, schema).start().await();
                if (!(result instanceof AbstractPaxosRepair.Failure))
                    return "OK";
                Throwable cause = ((AbstractPaxosRepair.Failure) result).failure;
                return "FAILURE: " + cause;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private static boolean hasRowInternally(int node, String tableName, int pk)
    {
        return cluster.get(node)
                      .executeInternal("SELECT ck FROM " + KEYSPACE + '.' + tableName + " WHERE pk = ? AND ck = 1", pk)
                      .length > 0;
    }

    /**
     * Leaves {@code pk} decided (accepted by a quorum) but committed only on {@code holder}: the
     * coordinator's commit messages to every replica in {@code dropTo} are dropped, and the CAS
     * times out awaiting its commit quorum. Self-verifying: asserts the commit landed exactly on
     * the holder.
     */
    private static void commitOnlyOn(int coordinator, int holder, String tableName, int pk, int[] dropTo, int[] mustLack)
    {
        IMessageFilters.Filter dropCommits =
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(coordinator).to(dropTo).drop();
        try
        {
            cluster.coordinator(coordinator)
                   .execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", QUORUM, pk);
            Assert.fail("the CAS commit cannot reach a quorum and must time out");
        }
        catch (RuntimeException expected)
        {
            // CasWriteTimeout: decided, but the commit reached only the holder
        }
        finally
        {
            dropCommits.off();
        }
        Assert.assertTrue("scenario setup: the commit must have landed on node " + holder,
                          hasRowInternally(holder, tableName, pk));
        for (int node : mustLack)
            Assert.assertFalse("scenario setup: node " + node + " must have missed the commit",
                               hasRowInternally(node, tableName, pk));
    }

    /**
     * PaxosRepair announce/terminal pairing: a REFRESH_COMMITTED veto fails the repair attempt AND
     * closes the announce with UNCONFIRMED; a commit attempt that fails re-announces on retry
     * (closing the previous announce as UNCONFIRMED) before completing with APPLIED.
     */
    @Test
    public void paxosRepairRefreshVetoAndReannouncePairing() throws Throwable
    {
        // schema changes need the whole ring: create the table before taking {4} out
        String tableName = "veto_repair";
        int pk = pk(cluster, 1, 2);

        commitOnlyOn(1, 3, tableName, pk, to(1, 2), to(1, 2));

        // Vetoed refresh: the repair fails, the announce is paired with UNCONFIRMED
        resetMutator(2);
        armRefreshVeto(2, true);
        String vetoedResult = repairOnNode(2, tableName, pk);
        Assert.assertTrue("the vetoed repair attempt must fail with the veto; got " + vetoedResult
                          + "; announces=" + announces(2) + " terminals=" + terminals(2),
                          vetoedResult.contains("dtest mutator veto"));
        List<String> terminals = awaitTerminals(2, 1);
        Assert.assertEquals(1, refreshAnnounced(2));
        Assert.assertEquals(List.of("REFRESH_COMMITTED:UNCONFIRMED"), terminals);

        // Disarmed, but the first commit attempt is dropped towards the other replicas: the retry
        // re-announces (closing the previous announce as UNCONFIRMED) and then completes APPLIED
        armRefreshVeto(2, false);
        resetMutator(2);
        AtomicInteger dropped = new AtomicInteger();
        IMessageFilters.Filter dropFirstAttempt =
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(2).to(1, 2, 3)
                   .messagesMatching((from, to, message) -> dropped.incrementAndGet() <= 3).drop();
        try
        {
            Assert.assertEquals("the retried repair must complete", "OK", repairOnNode(2, tableName, pk));
        }
        finally
        {
            dropFirstAttempt.off();
        }
        terminals = awaitTerminals(2, 2);
        Assert.assertEquals("the failed attempt's announce is closed by the retry's re-announce",
                            List.of("REFRESH_COMMITTED:UNCONFIRMED", "REFRESH_COMMITTED:APPLIED"), terminals);
        Assert.assertEquals(2, refreshAnnounced(2));

        assertRows(cluster.coordinator(3).execute("SELECT pk, ck, v FROM " + KEYSPACE + '.' + tableName + " WHERE pk = ?",
                                                  org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL, pk),
                   row(pk, 1, 1));
    }

    /**
     * CommitAndRestart: a poison prepare (no reads attached) that finds an incomplete commit
     * refreshes it and delivers the APPLIED terminal. Setup: a commit held by one node only, plus
     * a bare promise newer than the accepted round (a prepare whose operation never proposed), so
     * the repair takes the poison path rather than the query-phase refresh.
     */
    @Test
    public void paxosRepairPoisonRefreshDeliversApplied() throws Throwable
    {
        String tableName = "veto_poison";
        int pk = pk(cluster, 1, 2);

        commitOnlyOn(1, 3, tableName, pk, to(1, 2), to(1, 2));

        // Leave a bare newer promise: prepare succeeds everywhere, but the propose and the
        // commit-and-prepare (which would refresh the lagging commit) and the prepare-refresh
        // never leave the coordinator.
        IMessageFilters.Filter dropProgress =
            cluster.filters().verbs(PAXOS2_PROPOSE_REQ.id, PAXOS_PROPOSE_REQ.id, PAXOS2_COMMIT_AND_PREPARE_REQ.id, PAXOS2_PREPARE_REFRESH_REQ.id)
                   .from(1).to(1, 2, 3).drop();
        try
        {
            cluster.coordinator(1)
                   .execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, ck, v) VALUES (?, 2, 2) IF NOT EXISTS", QUORUM, pk);
            Assert.fail("the promised-but-never-proposed CAS must time out");
        }
        catch (RuntimeException expected)
        {
            // CasWriteTimeout: the prepare succeeded but nothing was proposed, leaving the bare promise
        }
        finally
        {
            dropProgress.off();
        }

        resetMutator(2);
        Assert.assertEquals("the repair must complete", "OK", repairOnNode(2, tableName, pk));
        List<String> terminals = awaitTerminals(2, 1);
        Assert.assertTrue("the poison-path refresh must deliver an APPLIED terminal: " + terminals,
                          terminals.contains("REFRESH_COMMITTED:APPLIED"));

        assertRows(cluster.coordinator(1).execute("SELECT pk, ck, v FROM " + KEYSPACE + '.' + tableName + " WHERE pk = ?",
                                                  org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL, pk),
                   row(pk, 1, 1));
    }

    /**
     * Paxos.begin FOUND_INCOMPLETE_COMMITTED veto: when the only replica having the latest commit
     * is a pending (bootstrapping) one — which is sent its prepare without a read — the driving
     * operation must refresh the commit before proceeding; a REFRESH_COMMITTED veto aborts it with
     * the thrown exception and an UNCONFIRMED terminal, and once disarmed the operation succeeds.
     */
    @Test
    public void beginIncompleteCommittedVetoAbortsOperation() throws Throwable
    {
        String tableName = "veto_begin";

        // {4} is bootstrapping (pending), witnessed by every node
        takeNodeFourOutOfRing();
        for (int i = 1; i <= 4; ++i)
        {
            cluster.get(i).acceptsOnInstance(CASTestBase::addToRingBootstrapping).accept(cluster.get(4));
            cluster.get(i).acceptsOnInstance(CASTestBase::assertVisibleInRing).accept(cluster.get(4));
        }
        try
        {
            int pk = pk(cluster, 3, 4);

            // commit lands only on the pending {4}: dropped towards the natural replicas
            commitOnlyOn(2, 4, tableName, pk, to(1, 2, 3), to(1, 2, 3));

            resetMutator(3);
            armRefreshVeto(3, true);
            // The consensus quorum (3 of the 3-natural + 1-pending electorate) must include the
            // pending {4}: without this, {1, 2, 3} can answer first and the round is completed as
            // an in-progress repair instead. Excluding {1} from the prepare forces {2, 3, 4}.
            IMessageFilters.Filter excludeNodeOne =
                cluster.filters().verbs(org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REQ.id).from(3).to(1).drop();
            try
            {
                cluster.coordinator(3)
                       .execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, ck, v) VALUES (?, 3, 3) IF NOT EXISTS", QUORUM, pk);
                Assert.fail("the refresh veto must abort the driving operation; announces=" + announces(3) + " terminals=" + terminals(3));
            }
            catch (RuntimeException e)
            {
                Assert.assertTrue("expected the veto to surface, got: " + e.getMessage(),
                                  e.getMessage() != null && e.getMessage().contains("dtest mutator veto"));
            }
            Assert.assertTrue(refreshAnnounced(3) >= 1);
            List<String> terminals = awaitTerminals(3, 1);
            Assert.assertTrue("the vetoed refresh must be paired with UNCONFIRMED: " + terminals,
                              terminals.contains("REFRESH_COMMITTED:UNCONFIRMED"));

            // disarmed: the operation refreshes the commit and completes
            armRefreshVeto(3, false);
            try
            {
                cluster.coordinator(3)
                       .execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, ck, v) VALUES (?, 3, 3) IF NOT EXISTS", QUORUM, pk);
            }
            finally
            {
                excludeNodeOne.off();
            }
        }
        finally
        {
            takeNodeFourOutOfRing();
        }
    }
}
