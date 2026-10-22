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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.paxos.Paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end proof that a secondary index can see where a replica-side apply came from.
 *
 * A custom index ({@link WriteOriginLoggingIndex}) logs the {@code WriteOptions} and {@code WriteOrigin}
 * its Indexer is handed for every apply; the test drives writes from datacenter1 and reads each node's
 * log to check what that node actually saw. Four nodes, two datacenters, NTS {dc1:2, dc2:2}, so every
 * node is a replica of every key and the only thing that differs between them is provenance.
 *
 * What is being demonstrated, and why it could not be observed before:
 *
 *  - a write coordinated in datacenter1 reaches datacenter2 as ONE MUTATION_REQ carrying a
 *    ForwardingInfo, which the receiving relay fans out locally. Every dc2 replica must nonetheless see
 *    the ORIGINAL coordinator (RESPOND_TO survives the relay), while only the node the message actually
 *    reached is marked direct;
 *  - the coordinator's own replica applies with no inbound message at all, and must be distinguishable
 *    from its same-datacenter peer, which applies one it received;
 *  - a paxos commit applies with WriteOptions.FOR_PAXOS_COMMIT and a read repair with
 *    FOR_READ_REPAIR, where an ordinary write applies with DEFAULT -- distinctions
 *    IndexTransaction.Type cannot make, since all three are UPDATE;
 *  - a read repair is stamped by a different verb handler than an ordinary write, and is attributed to
 *    the node that ran the READ rather than to whoever originally wrote the row.
 *
 * Paxos is covered under BOTH variants, since their transports differ: under v1 every replica gets
 * PAXOS_COMMIT_REQ (labelled FOR_PAXOS_COMMIT, attributed to the delivering peer via the threading in
 * PaxosState), while under v2 a datacenter-local commit reaches the remote datacenter as
 * PAXOS2_COMMIT_REMOTE_REQ -- an ordinary mutation, attributed by MutationVerbHandler but labelled
 * DEFAULT rather than as a paxos commit.
 *
 * Run with: ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.WriteOriginDistributedTest
 */
public class WriteOriginDistributedTest extends TestBaseImpl
{
    private static final String KS = "write_origin_ks";
    private static final String TBL = "tbl";
    private static final String KS_TBL = KS + '.' + TBL;
    private static final String IDX = "write_origin_idx";

    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";

    /** What Config ships with; restored after any test that flips it, so test order does not matter. */
    private static final String DEFAULT_PAXOS_VARIANT = "v1";

    private static Cluster cluster;

    @BeforeClass
    public static void setUp() throws Throwable
    {
        Consumer<IInstanceConfig> conf = config -> config.with(Feature.GOSSIP, Feature.NETWORK)
                                                         .set("dynamic_snitch", false);

        cluster = new Cluster.Builder().withNodes(4).withDCs(2).withConfig(conf).start();

        // Guard the node -> datacenter assumption every assertion below relies on, so a change in the
        // framework's DC naming surfaces here rather than as a mysterious failure later.
        assertEquals(DC1, cluster.get(1).config().localDatacenter());
        assertEquals(DC1, cluster.get(2).config().localDatacenter());
        assertEquals(DC2, cluster.get(3).config().localDatacenter());
        assertEquals(DC2, cluster.get(4).config().localDatacenter());

        cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = "
                             + "{'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 2}");
        cluster.schemaChange("CREATE TABLE " + KS_TBL + " (k int PRIMARY KEY, v int)");
        cluster.schemaChange("CREATE CUSTOM INDEX " + IDX + " ON " + KS_TBL + " (v) USING '"
                             + WriteOriginLoggingIndex.class.getName() + '\'');
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
    }

    /**
     * An ordinary write coordinated on node1, in datacenter1.
     *
     * Before this change every one of these four applies looked identical to the index: same
     * IndexTransaction.Type, same WriteOptions.DEFAULT, nothing naming the coordinator.
     */
    @Test
    public void ordinaryWriteIsLabelledPerReplica()
    {
        long[] marks = mark();

        cluster.coordinator(1).execute("INSERT INTO " + KS_TBL + " (k, v) VALUES (?, ?)",
                                       ConsistencyLevel.LOCAL_QUORUM, 1, 1);

        // The coordinator's own replica: applied with no inbound message behind it, so there is no
        // coordinator to name. This is WriteOrigin.LOCAL, and it is NOT the same as "no origin".
        assertOneLine(1, marks, "key=1", "options=DEFAULT", "coordinator=null", "crossDc=false", "direct=false");

        // Its datacenter peer received the write from node1: a coordinator, but a local one.
        assertOneLine(2, marks, "key=1", "options=DEFAULT", "coordinatorDc=" + DC1, "crossDc=false", "direct=false");

        // Both datacenter2 replicas name the ORIGINAL datacenter1 coordinator, even though only one of
        // them heard from it directly -- ForwardingInfo puts it in RESPOND_TO, which survives the relay.
        for (int node : new int[]{ 3, 4 })
            assertOneLine(node, marks, "key=1", "options=DEFAULT", "coordinatorDc=" + DC1, "crossDc=true");

        // ... and exactly one of them is the node the cross-DC message actually reached. That uniqueness
        // is a property of MUTATION_REQ's fan-out, not of the signal: see WriteOrigin's javadoc for the
        // verbs where every receiving replica is direct.
        long direct = count(3, marks, "key=1.*direct=true") + count(4, marks, "key=1.*direct=true");
        assertEquals("exactly one datacenter2 replica should have received the cross-DC message directly",
                     1, direct);
    }

    /**
     * A paxos commit under paxos_variant v1: labelled by WriteOptions AND attributed to the peer that
     * delivered it.
     *
     * A commit's Mutation never travels as one -- PAXOS_COMMIT_REQ carries an Agreed, and each replica
     * manufactures the mutation in PaxosState.applyCommit -- so MutationVerbHandler's stamping cannot
     * cover it. The origin is threaded from PaxosCommit.RequestHandler instead, built from the message's
     * sender, which is never serialized: no wire format is involved.
     *
     * The commit is sent point-to-point to every replica, so the datacenter2 replicas are BOTH direct --
     * the documented non-uniqueness of that bit, visible here. And the "coordinator" is the peer the
     * commit came FROM, which for the coordinator's own replica is nobody: it applies via executeOnSelf
     * with no message behind it, and stays LOCAL.
     *
     * Compare {@link #dcLocalPaxosCommitUnderV2StampsTheRemoteLeg}: same statement, same consistency
     * levels, only the variant differs.
     */
    @Test
    public void paxosCommitUnderV1IsAttributedToThePeerThatDeliveredIt()
    {
        setPaxosVariant("v1");
        try
        {
            long[] marks = mark();

            cluster.coordinator(1).execute("INSERT INTO " + KS_TBL + " (k, v) VALUES (?, ?) IF NOT EXISTS",
                                           ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_QUORUM, 2, 2);

            // The proposing node applies its own commit: no message, LOCAL.
            assertOneLine(1, marks, "key=2",
                          "options=FOR_PAXOS_COMMIT", "coordinator=null", "crossDc=false");

            // Its datacenter peer received the commit from node1: a coordinator, but a local one.
            assertOneLine(2, marks, "key=2",
                          "options=FOR_PAXOS_COMMIT", "coordinatorDc=" + DC1, "crossDc=false", "direct=false");

            // The datacenter2 replicas know the commit came from datacenter1 -- previously these read
            // "coordinator=null, crossDc=false", indistinguishable from a locally proposed commit.
            for (int node : new int[]{ 3, 4 })
                assertOneLine(node, marks, "key=2",
                              "options=FOR_PAXOS_COMMIT", "coordinatorDc=" + DC1, "crossDc=true", "direct=true");
        }
        finally
        {
            setPaxosVariant(DEFAULT_PAXOS_VARIANT);
        }
    }

    /**
     * v2's datacenter-local commit, whose remote leg takes a different transport than v1's.
     *
     * With paxos_variant v2 and a datacenter-local serial consistency, PaxosCommit sends the remote
     * datacenter a PAXOS2_COMMIT_REMOTE_REQ instead of a PAXOS_COMMIT_REQ -- and that verb is handled by
     * the ordinary MutationVerbHandler, so it is stamped there like a client write and labelled DEFAULT
     * rather than FOR_PAXOS_COMMIT. The local-datacenter legs still go through PaxosState.applyCommit
     * and keep the paxos label. Either way, every replica now knows where the commit came from.
     */
    @Test
    public void dcLocalPaxosCommitUnderV2StampsTheRemoteLeg()
    {
        setPaxosVariant("v2");
        try
        {
            long[] marks = mark();

            cluster.coordinator(1).execute("INSERT INTO " + KS_TBL + " (k, v) VALUES (?, ?) IF NOT EXISTS",
                                           ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_QUORUM, 4, 4);

            // The proposing node applies its own commit with no message behind it...
            assertOneLine(1, marks, "key=4",
                          "options=FOR_PAXOS_COMMIT", "coordinator=null", "crossDc=false");

            // ... while its datacenter peer is delivered one, and attributes it.
            assertOneLine(2, marks, "key=4",
                          "options=FOR_PAXOS_COMMIT", "coordinatorDc=" + DC1, "crossDc=false");

            // datacenter2 receives it as a plain mutation: attributed, but labelled DEFAULT -- the one
            // paxos leg where the ORIGIN survives and the KIND does not.
            for (int node : new int[]{ 3, 4 })
                assertOneLine(node, marks, "key=4",
                              "options=DEFAULT", "coordinatorDc=" + DC1, "crossDc=true");
        }
        finally
        {
            setPaxosVariant(DEFAULT_PAXOS_VARIANT);
        }
    }

    /**
     * A blocking read repair, which reaches the stale replica through ReadRepairVerbHandler rather than
     * MutationVerbHandler -- a separate stamping site, and one whose origin is the node that ran the
     * READ, not the node that ran the original write.
     *
     * node4 is kept from ever receiving the write (the filter covers the relayed leg as well as the
     * direct one, so ForwardingInfo cannot sneak it in), then a read at ALL from datacenter1 finds the
     * digest mismatch and repairs it. The MUTATION_REQ filter stays up throughout, so read repair is
     * provably the only path the row can take to node4.
     */
    @Test
    public void readRepairIsLabelledAndAttributedToTheReadCoordinator()
    {
        long[] marks = mark();

        cluster.filters().verbs(Verb.MUTATION_REQ.id).to(4).drop();
        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + KS_TBL + " (k, v) VALUES (?, ?)",
                                           ConsistencyLevel.LOCAL_QUORUM, 3, 3);

            assertEquals("node4 must not have seen the write", 0, count(4, marks, "key=3"));

            // Reading at ALL forces node4 into the quorum, mismatching, and repairs it.
            cluster.coordinator(1).execute("SELECT * FROM " + KS_TBL + " WHERE k = ?",
                                           ConsistencyLevel.ALL, 3);

            assertOneLine(4, marks, "key=3",
                          "options=FOR_READ_REPAIR", "coordinatorDc=" + DC1, "crossDc=true");
        }
        finally
        {
            cluster.filters().reset();
        }
    }

    /**
     * The happy path of a multi-datacenter LOGGED batch.
     *
     * A logged batch is two mechanisms glued together, and only one of them is datacenter-aware:
     *
     *  - the BATCHLOG is strictly local to the coordinator's datacenter. ReplicaPlans.forBatchlogWrite
     *    picks min(2, candidates) nodes from the local DC only, excluding the coordinator itself unless
     *    it is a single-node DC -- so here, with datacenter1 = {node1, node2} and node1 coordinating,
     *    the batch is stored on node2 alone. The remote datacenter never learns a batch existed;
     *  - the MUTATIONS then take the ordinary write path (asyncWriteBatchedMutations ->
     *    sendToHintedReplicas), including the per-remote-DC ForwardingInfo relay. So each partition of
     *    the batch looks to every replica exactly like an independent ordinary write.
     *
     * The test pins both halves: a counting filter proves not one BATCH_STORE_REQ crossed into
     * datacenter2 (while at least one reached node2, so the batch demonstrably used the batchlog --
     * single-partition logged batches skip it), and the per-replica origins of both partitions are
     * identical to what an ordinary write produces, each with exactly one direct delivery into
     * datacenter2.
     */
    @Test
    public void loggedBatchHappyPathBehavesLikeOrdinaryWritesPerPartition()
    {
        long[] marks = mark();

        AtomicInteger batchStoreCrossDc = new AtomicInteger();
        AtomicInteger batchStoreToNode2 = new AtomicInteger();
        cluster.filters().verbs(Verb.BATCH_STORE_REQ.id).from(1).to(3, 4)
               .messagesMatching((from, to, message) -> { batchStoreCrossDc.incrementAndGet(); return false; })
               .drop();
        cluster.filters().verbs(Verb.BATCH_STORE_REQ.id).from(1).to(2)
               .messagesMatching((from, to, message) -> { batchStoreToNode2.incrementAndGet(); return false; })
               .drop();
        try
        {
            // Two partitions: a single-partition logged batch bypasses the batchlog entirely.
            cluster.coordinator(1).execute("BEGIN BATCH "
                                           + "INSERT INTO " + KS_TBL + " (k, v) VALUES (10, 10); "
                                           + "INSERT INTO " + KS_TBL + " (k, v) VALUES (11, 11); "
                                           + "APPLY BATCH",
                                           ConsistencyLevel.LOCAL_QUORUM);

            for (int key : new int[]{ 10, 11 })
            {
                String keyFragment = "key=" + key;

                // Indistinguishable from two ordinary writes, per replica:
                assertOneLine(1, marks, keyFragment, "options=DEFAULT", "coordinator=null", "crossDc=false");
                assertOneLine(2, marks, keyFragment, "options=DEFAULT", "coordinatorDc=" + DC1, "crossDc=false");
                for (int node : new int[]{ 3, 4 })
                    assertOneLine(node, marks, keyFragment, "options=DEFAULT", "coordinatorDc=" + DC1, "crossDc=true");

                // ... and each partition's cross-DC message went through the ForwardingInfo relay:
                // exactly one direct delivery into datacenter2 per partition.
                long direct = count(3, marks, keyFragment + ".*direct=true")
                            + count(4, marks, keyFragment + ".*direct=true");
                assertEquals("exactly one datacenter2 replica should be direct for " + keyFragment, 1, direct);
            }

            // The batchlog half: written in the coordinator's DC, never sent across.
            assertTrue("the batch should have been stored on node2 (single local-DC batchlog candidate)",
                       batchStoreToNode2.get() >= 1);
            assertEquals("the batchlog must never leave the coordinator's datacenter", 0, batchStoreCrossDc.get());
        }
        finally
        {
            cluster.filters().reset();
        }
    }

    /**
     * The recovery path of a multi-datacenter LOGGED batch: the coordinator crashes after storing the
     * batch, before the mutations complete.
     *
     * Recovery is NOT per-datacenter, and this is the part worth seeing end to end. The batchlog lives
     * only in the originating coordinator's datacenter (on node2 here, see the happy-path test), so
     * that lone datacenter1 node is what drives recovery for the whole cluster: its BatchlogManager
     * replays batches older than the replay timeout by applying each mutation locally with
     * WriteOptions.FOR_BATCH_REPLAY and sending ONE plain MUTATION_REQ per live remote replica --
     * point-to-point, no ForwardingInfo, no per-DC relay (BatchlogManager.sendSingleReplayMutation).
     *
     * Consequences the test pins, replica by replica:
     *  - datacenter2 first learns of the rows from the REPLAY: origin = node2 (the batchlog node, in
     *    datacenter1), NOT the dead coordinator -- consistent with the origin contract everywhere else:
     *    "who delivered this apply", not "who the client talked to";
     *  - both datacenter2 replicas are direct=true: the point-to-point fan-out again, same as read
     *    repair and hints;
     *  - the replayed applies arrive as ordinary DEFAULT mutations; only the batchlog node's own
     *    re-apply carries FOR_BATCH_REPLAY (with no coordinator -- there is no message behind it).
     *
     * Scenario: datacenter2 is partitioned away, so a QUORUM batch (3 of 4) times out after the batch
     * was durably stored on node2; node1 then crashes, taking with it both the client's only
     * acknowledgement path and the hints it stored for datacenter2. The partition heals, node2's
     * batchlog replay is forced until datacenter2 holds both rows -- provably via replay, since node1
     * (and its hints) stay down until the assertions are done.
     */
    @Test
    public void loggedBatchRecoveryIsDrivenByTheBatchlogNodeAcrossDatacenters() throws Throwable
    {
        long[] marks = mark();

        cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(3, 4).drop();
        try
        {
            try
            {
                cluster.coordinator(1).execute("BEGIN BATCH "
                                               + "INSERT INTO " + KS_TBL + " (k, v) VALUES (20, 20); "
                                               + "INSERT INTO " + KS_TBL + " (k, v) VALUES (21, 21); "
                                               + "APPLY BATCH",
                                               ConsistencyLevel.QUORUM);
                fail("the batch should have timed out with datacenter2 unreachable");
            }
            catch (RuntimeException expected)
            {
                // WriteTimeoutException (writeType=BATCH) surfaced through the dtest coordinator: the
                // batchlog write succeeded, the mutation phase could not reach QUORUM.
            }

            // datacenter2 saw nothing...
            for (int node : new int[]{ 3, 4 })
                for (int key : new int[]{ 20, 21 })
                    assertEquals("node" + node + " must not have applied key=" + key + " while partitioned",
                                 0, count(node, marks, "key=" + key));

            // ... and node2 applied its ordinary write-path legs (proof the batch got as far as the
            // mutation phase before dying).
            for (int key : new int[]{ 20, 21 })
                assertOneLine(2, marks, "key=" + key, "options=DEFAULT", "coordinatorDc=" + DC1, "crossDc=false");

            // The coordinator "crashes". This also strands the hints node1 wrote for datacenter2 when
            // the mutation phase timed out, so nothing but node2's batchlog can deliver these rows.
            cluster.get(1).shutdown().get();
        }
        finally
        {
            cluster.filters().reset();
        }

        try
        {
            // Heal and force replay on node2 until datacenter2 holds both rows. The replay only picks
            // up batches older than the replay window (2x the write timeout by default), hence the loop.
            long deadline = System.currentTimeMillis() + 120_000;
            while (System.currentTimeMillis() < deadline
                   && (cluster.get(3).executeInternal("SELECT k FROM " + KS_TBL + " WHERE k = 20").length == 0
                       || cluster.get(3).executeInternal("SELECT k FROM " + KS_TBL + " WHERE k = 21").length == 0
                       || cluster.get(4).executeInternal("SELECT k FROM " + KS_TBL + " WHERE k = 20").length == 0
                       || cluster.get(4).executeInternal("SELECT k FROM " + KS_TBL + " WHERE k = 21").length == 0))
            {
                cluster.get(2).runOnInstance(() -> {
                    try
                    {
                        BatchlogManager.instance.forceBatchlogReplay();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
                Thread.sleep(2000);
            }

            for (int key : new int[]{ 20, 21 })
            {
                String keyFragment = "key=" + key;

                // The batchlog node re-applied its own copy: the one leg labelled as a replay, and the
                // one with no message behind it.
                assertEquals("node2 should have re-applied " + keyFragment + " exactly once as a replay",
                             1, count(2, marks, keyFragment + ".*options=FOR_BATCH_REPLAY"));
                assertEquals("node2's replay self-apply has no message behind it",
                             1, count(2, marks, keyFragment + ".*options=FOR_BATCH_REPLAY, coordinator=null"));

                // datacenter2's first and only applies: delivered by node2 -- the batchlog node, not
                // the dead coordinator -- as plain mutations, point-to-point, so both are direct.
                for (int node : new int[]{ 3, 4 })
                    assertOneLine(node, marks, keyFragment,
                                  "options=DEFAULT",
                                  "coordinator=/127.0.0.2",
                                  "coordinatorDc=" + DC1,
                                  "crossDc=true",
                                  "direct=true");
            }
        }
        finally
        {
            cluster.get(1).startup();
        }
    }

    /**
     * Flips the paxos variant on every node. Passed as a String rather than as a Config.PaxosVariant so
     * that no enum constant has to cross the boundary between the test's classloader and the isolated
     * per-instance ones; the constant is resolved inside the node.
     */
    private static void setPaxosVariant(String variant)
    {
        cluster.forEach(i -> i.runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.valueOf(variant))));
    }

    private static long[] mark()
    {
        long[] marks = new long[5];
        for (int n = 1; n <= 4; n++)
            marks[n] = cluster.get(n).logs().mark();
        return marks;
    }

    private static long count(int node, long[] marks, String regex)
    {
        return cluster.get(node).logs().grep(marks[node], WriteOriginLoggingIndex.MARKER + ".*" + regex)
                      .getResult().size();
    }

    /**
     * Waits for the node to log an apply for this key, then asserts it logged exactly one and that the
     * line contains every given fragment.
     *
     * The wait is not optional politeness: the legs of a write that do not block the coordinator -- the
     * cross-datacenter delivery of a LOCAL_QUORUM write, the remote commit of a LOCAL_SERIAL LWT -- land
     * asynchronously, so grepping immediately after execute() races the apply. Requiring exactly one
     * line afterwards is what stops a second, differently labelled apply of the same row (a read
     * repair, say) from satisfying the assertion silently.
     */
    private static void assertOneLine(int node, long[] marks, String keyFragment, String... fragments)
    {
        String pattern = WriteOriginLoggingIndex.MARKER + ".*" + keyFragment + ",";
        try
        {
            cluster.get(node).logs().watchFor(marks[node], Duration.ofSeconds(30), Pattern.compile(pattern));
        }
        catch (TimeoutException e)
        {
            throw new AssertionError("node" + node + " never logged an apply for " + keyFragment, e);
        }

        List<String> lines = cluster.get(node).logs().grep(marks[node], pattern).getResult();
        assertEquals("node" + node + " should have logged exactly one apply for " + keyFragment
                     + ", got: " + lines, 1, lines.size());

        String line = lines.get(0);
        for (String fragment : fragments)
            assertTrue("node" + node + " line should contain '" + fragment + "', got: " + line,
                       line.contains(fragment));
    }
}
