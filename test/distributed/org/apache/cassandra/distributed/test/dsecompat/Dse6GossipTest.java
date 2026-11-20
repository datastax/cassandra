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

package org.apache.cassandra.distributed.test.dsecompat;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Gossip between nodes of this software in {@code DSE_6} mode: they publish the {@code NET_VERSION} DSE can
 * parse (12) and a {@code DSE_GOSSIP_STATE} with DSE version 6.8.1, and still negotiate their real messaging version
 * with each other, through a restart and a schema change. Gossip with a node speaking the DSE dialect is covered by
 * the unit test {@code DseGossipTest}, against gossip states captured from DSE. No HCD 1.2 node takes part: the class
 * is a plain in-JVM dtest, which needs no HCD 1.2 dtest jar and runs with the other in-JVM dtests; it uses the
 * {@code DSE_6} configuration and the {@code compatstatus} helper of {@link DseDialectTestBase}. The attempt to have an
 * HCD 1.2 node join such nodes is {@code Dse6DialectSmokeTest#hcd12PeerAtTheDialectJoinsDse6Nodes}.
 */
public class Dse6GossipTest extends TestBaseImpl
{
    private static final String PUBLISHED_NET_VERSION = "12";
    private static final String PUBLISHED_DSE_VERSION = "6.8.1";

    /** The SAI on-disk version pin of the class, see {@link #pinSaiVersion()}. */
    private static WithProperties saiVersionPin;

    /**
     * A node in {@code DSE_6} mode refuses to start unless the SAI on-disk version is pinned: {@code aa}, what DSE 6.8
     * writes, for the whole class, as {@link DseDialectTestBase} pins it. A test that stores a DSE 6.9 release sets
     * {@code ca} for its cluster.
     */
    @BeforeClass
    public static void pinSaiVersion()
    {
        saiVersionPin = new WithProperties().set(SAI_CURRENT_VERSION, "aa");
    }

    @AfterClass
    public static void unpinSaiVersion()
    {
        if (saiVersionPin != null)
            saiVersionPin.close();
        saiVersionPin = null;
    }

    @Test
    public void dse6NodesGossipTheDseVersionAndNegotiateTheirOwn() throws Throwable
    {
        // the DSE_6 mode sets the trie sstable version as a system property the in-JVM instances share: restored at the end
        // a subnet of its own: in-JVM clusters of other suites on 127.0.0.x must not reach these nodes
        try (WithProperties trieVersion = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
             Cluster cluster = formerDseNodes(Cluster.build(3)
                                                     .withSubnet(8)
                                                     .withConfig(config -> formerDseNode(config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL)))))
        {
            // the schema is frozen in DSE_6 mode: the property lifts the freeze for the schema changes of the test
            try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            }
            for (int node = 1; node <= 3; node++)
            {
                assertGossipsPublishedNetVersion(cluster, node);
                awaitPeersAtCurrentVersion(cluster, node);
            }

            // a restarted node gossips 12 again to peers that had negotiated the current version with it, and
            // the peers negotiate it again
            cluster.get(3).shutdown().get();
            cluster.get(3).startup();
            awaitNewGeneration(cluster, 3);
            for (int node = 1; node <= 3; node++)
            {
                assertGossipsPublishedNetVersion(cluster, node);
                awaitPeersAtCurrentVersion(cluster, node);
            }
            ClusterUtils.awaitGossipSchemaMatch(cluster);
            try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t ADD w int");
            }
            ClusterUtils.awaitGossipSchemaMatch(cluster);
            cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".t (k, v, w) VALUES (1, 'one', 1)", ConsistencyLevel.ALL);
            assertThat(cluster.coordinator(1).execute("SELECT v, w FROM " + KEYSPACE + ".t WHERE k = 1", ConsistencyLevel.ALL))
            .isEqualTo(new Object[][]{ { "one", 1 } });

            // peers of this software are not DSE peers, whatever they gossip
            for (int node = 1; node <= 3; node++)
                assertThat(DseDialectTestBase.compatStatus(cluster, node)).contains("DSE peers                    : 0");
        }
    }

    /** The settings of the {@code DSE_6} mode without the mode itself: the storage a DSE 6 node can read. */
    private static void formerDseNode(IInstanceConfig config)
    {
        DseDialectTestBase.configureDse6(config);
        config.set("internode_compatibility_mode", InternodeCompatibilityMode.NONE.name());
    }

    /**
     * As on a real upgrade, the nodes enter the mode on a restart, on data they wrote before: the operational guards of the
     * mode refuse a node that has not completed its bootstrap. The nodes of {@code builder}, configured with
     * {@link #formerDseNode}, first start without the mode, with the trie sstable version DSE reads, and the test keyspace
     * is created; then each node restarts in {@code DSE_6} mode.
     */
    private static Cluster formerDseNodes(Cluster.Builder builder) throws Throwable
    {
        Cluster cluster = builder.createWithoutStarting();
        try
        {
            try (WithProperties bb = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                cluster.startup();
                init(cluster);
            }
            for (IInvokableInstance instance : cluster)
            {
                instance.shutdown().get();
                DseDialectTestBase.configureDse6(instance.config());
                instance.startup();
            }
            return cluster;
        }
        catch (Throwable t)
        {
            cluster.close();
            throw t;
        }
    }

    /**
     * A node restarting with stale peer rows that give its peers of this software a DSE 6 release (as after a peer was
     * upgraded while the node was down) takes them for DSE nodes: its first connections to them are attempted with the
     * pre-4.0 handshake. It and its peers come back to the current version as soon as they know each other's version,
     * every connection of the restarted node ends at the current version, and the cluster keeps working.
     */
    @Test
    public void peersTakenForDseNodesByAStaleReleaseAreBackAtTheCurrentVersion() throws Throwable
    {
        // the DSE_6 mode sets the trie sstable version as a system property the in-JVM instances share: restored at the end
        // a subnet of its own: in-JVM clusters of other suites on 127.0.0.x must not reach these nodes
        // the restart below stores DSE 6.9 releases: the SAI pin is ca, what 6.9 writes, for the whole life of the cluster
        // (accepted at the first starts too, where no DSE release is stored)
        try (WithProperties trieVersion = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
             WithProperties saiPin = new WithProperties().set(SAI_CURRENT_VERSION, "ca");
             Cluster cluster = formerDseNodes(Cluster.build(3)
                                                     .withSubnet(13)
                                                     .withConfig(config -> formerDseNode(config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL)))))
        {
            // the schema is frozen in DSE_6 mode: the property lifts the freeze for the schema changes of the test
            try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            }
            for (int node = 1; node <= 3; node++)
                awaitPeersAtCurrentVersion(cluster, node);

            // the rows are saved at once and not rewritten on shutdown: gossip rewrites a peer row only when a state changes
            String[] peers = { hostAddressAndPort(cluster, 1), hostAddressAndPort(cluster, 2) };
            String staleRelease = DseVersion.DSE_6_9_26.releaseVersion;
            for (String peer : peers)
                cluster.get(3).runOnInstance(() -> Nodes.peers().update(address(peer), p -> p.setReleaseVersion(new CassandraVersion(staleRelease)), true));
            long mark = cluster.get(3).logs().mark();
            cluster.get(3).shutdown().get();
            // the saved peers now name DSE 6.9: the SAI on-disk version is pinned to what 6.9 writes (for the whole cluster)
            cluster.get(3).startup();
            assertThat(cluster.get(3).logs().grep(mark, "SAI on-disk version pinned to ca \\(cassandra.sai.latest.version\\), derived from DSE 6.9").getResult())
            .hasSize(1);
            awaitNewGeneration(cluster, 3);
            List<String> seeded = cluster.get(3).logs().grep(mark, "stored with a DSE 6 release \\(4\\.0\\.0\\.<build>\\); connecting to them with the pre-4.0 handshake at version 11").getResult();
            assertThat(seeded).as("the stale rows took both peers for DSE nodes").hasSize(1);
            for (String peer : peers)
                assertThat(seeded.get(0)).contains('/' + peer);
            for (int node = 1; node <= 3; node++)
                awaitPeersAtCurrentVersion(cluster, node);
            ClusterUtils.awaitGossipSchemaMatch(cluster);
            try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t ADD w int");
            }
            ClusterUtils.awaitGossipSchemaMatch(cluster);
            cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".t (k, v, w) VALUES (1, 'one', 1)", ConsistencyLevel.ALL);
            assertThat(cluster.coordinator(1).execute("SELECT v, w FROM " + KEYSPACE + ".t WHERE k = 1", ConsistencyLevel.ALL))
            .isEqualTo(new Object[][]{ { "one", 1 } });

            // the recovery ran for both peers: their version rose out of the dialect the stale rows had put them at
            List<String> raised = cluster.get(3).logs().grep(mark, "is at messaging version \\d+, not at the pre-4.0 dialect; reconnecting to it with the 4.0-style handshake").getResult();
            for (String peer : peers)
                assertThat(raised).as("version of " + peer + " raised").anyMatch(line -> line.contains('/' + peer + " is at messaging version"));
            // every connection the restarted node made to its peers ended at the current version: gossip (urgent) and
            // the writes above (small) made one to each peer, a large one may or may not exist
            int current = cluster.get(3).callOnInstance(() -> MessagingService.current_version);
            List<String> connected = cluster.get(3).logs().grep(mark, "successfully connected, version = ").getResult();
            for (String peer : peers)
            {
                for (String type : new String[]{ "URGENT_MESSAGES", "SMALL_MESSAGES", "LARGE_MESSAGES" })
                {
                    String last = null;
                    for (String line : connected)
                        if (line.contains("->/" + peer + '-' + type + '-'))
                            last = line;
                    if (last == null && type.equals("LARGE_MESSAGES"))
                        continue;
                    assertThat(last).as(type + " connection to " + peer).isNotNull();
                    assertThat(last).as(type + " connection to " + peer).contains("successfully connected, version = " + current + ',');
                }
            }
            // the peers gossip the release of this software again: the next start connects to them with the 4.0-style handshake
            for (String peer : peers)
            {
                await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
                    String release = cluster.get(3).callOnInstance(() -> String.valueOf(Nodes.peers().get(address(peer)).getReleaseVersion()));
                    assertThat(release).doesNotStartWith("4.0.0.");
                });
            }
        }
    }

    /**
     * Two nodes of this software both restarted with a stale DSE 6 release stored for the other, as in an upgrade of the
     * whole cluster at once: each presumes the other to be a DSE node and sends it no endpoint state at first. Each
     * presumption ends, by the other's message without any state or by the other's NET_VERSION (in whichever order the
     * shadow round and the first gossip messages go; the first way is covered by DsePeerGossipTest). Both are then up, at
     * the current version, and the cluster works.
     */
    @Test
    public void twoNodesPresumingEachOtherToBeDseNodesStillExchangeTheirStates() throws Throwable
    {
        // the DSE_6 mode sets the trie sstable version as a system property the in-JVM instances share: restored at the end
        // a subnet of its own: in-JVM clusters of other suites on 127.0.0.x must not reach these nodes
        try (WithProperties trieVersion = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
             Cluster cluster = formerDseNodes(Cluster.build(2)
                                                     .withSubnet(11)
                                                     .withConfig(config -> formerDseNode(config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL)))))
        {
            // the schema is frozen in DSE_6 mode: the property lifts the freeze for the schema changes of the test
            try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            }
            for (int node = 1; node <= 2; node++)
                awaitPeersAtCurrentVersion(cluster, node);

            String[] addresses = { hostAddressAndPort(cluster, 1), hostAddressAndPort(cluster, 2) };
            String staleRelease = DseVersion.DSE_6_8_65.releaseVersion;
            long[] marks = new long[2];
            for (int node = 1; node <= 2; node++)
            {
                String other = addresses[2 - node];
                cluster.get(node).runOnInstance(() -> Nodes.peers().update(address(other), p -> p.setReleaseVersion(new CassandraVersion(staleRelease)), true));
                marks[node - 1] = cluster.get(node).logs().mark();
            }
            cluster.get(2).shutdown().get();
            cluster.get(1).shutdown().get();
            // the in-JVM instances start one at a time: the first one's cluster-wide access-control check cannot
            // reach the replicas of system_auth on the other, still down, so it relies on the check of its own replica
            try (WithProperties localCheck = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
            {
                cluster.get(1).startup();
                cluster.get(2).startup();
            }

            for (int node = 1; node <= 2; node++)
            {
                List<String> seeded = cluster.get(node).logs().grep(marks[node - 1], "stored with a DSE 6 release").getResult();
                assertThat(seeded).as("node " + node + " seeded the other").hasSize(1);
                assertThat(seeded.get(0)).contains('/' + addresses[2 - node]);
                // the stale 6.8 release also determines the SAI on-disk version to pin: aa, the pin of the harness
                assertThat(cluster.get(node).logs().grep(marks[node - 1], "SAI on-disk version pinned to aa \\(cassandra.sai.latest.version\\), " +
                                                                          "derived from DSE 6.8").getResult())
                .as("node " + node + " derived the pin").hasSize(1);
            }
            for (int node = 1; node <= 2; node++)
            {
                awaitPeersAtCurrentVersion(cluster, node);
                int n = node;
                await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
                    String status = cluster.get(n).nodetoolResult("status").getStdout();
                    assertThat(Pattern.compile("^UN ", Pattern.MULTILINE).matcher(status).results().count()).as("node " + n + ":\n" + status).isEqualTo(2);
                });
                List<String> ended = cluster.get(node).logs().grep(marks[node - 1], "is not a DSE 6 node .*it was presumed one").getResult();
                assertThat(ended).as("node " + node + " ended its presumption of the other").anyMatch(line -> line.contains('/' + addresses[2 - n] + " is not a DSE 6 node"));
            }
            cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (1, 'one')", ConsistencyLevel.ALL);
            assertThat(cluster.coordinator(1).execute("SELECT v FROM " + KEYSPACE + ".t WHERE k = 1", ConsistencyLevel.ALL))
            .isEqualTo(new Object[][]{ { "one" } });
        }
    }

    /**
     * The cluster-wide pass of the access-control check must not run before the replicas of system_auth can be reached,
     * or it refuses the start: it retries while they come up. Node 1 restarts while node 2, which holds part of
     * system_auth, is down; its start waits, and completes once node 2 is back.
     */
    @Test
    public void theAccessControlCheckWaitsForTheReplicasOfSystemAuth() throws Throwable
    {
        ExecutorService starter = Executors.newSingleThreadExecutor();
        // the DSE_6 mode sets the trie sstable version as a system property the in-JVM instances share: restored at the end
        // a subnet of its own: in-JVM clusters of other suites on 127.0.0.x must not reach these nodes
        try (WithProperties trieVersion = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
             Cluster cluster = formerDseNodes(Cluster.build(2)
                                                     .withSubnet(12)
                                                     .withConfig(config -> formerDseNode(config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL)))))
        {
            long mark = cluster.get(1).logs().mark();
            cluster.get(2).shutdown().get();
            cluster.get(1).shutdown().get();
            try (WithProperties wait = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, 240))
            {
                Future<?> first = starter.submit(() -> cluster.get(1).startup());
                // node 1 cannot read system_auth at LOCAL_QUORUM without node 2: it retries
                await().atMost(3, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                       .until(() -> !cluster.get(1).logs().grep(mark, "The startup check of the DSE access controls could not read system_auth.role_permissions " +
                                                                      "at LOCAL_QUORUM \\(attempt 1: .*\\); retrying in 5 s, for up to 240 s").getResult().isEmpty());
                assertThat(first.isDone()).as("node 1 is still starting").isFalse();
                cluster.get(2).startup();
                first.get(4, TimeUnit.MINUTES);
            }
            assertThat(cluster.get(1).logs().grep(mark, "The startup check of the DSE access controls read system_auth.role_permissions at LOCAL_QUORUM on attempt \\d+").getResult())
            .hasSize(1);
            for (int node = 1; node <= 2; node++)
            {
                int n = node;
                await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
                    String status = cluster.get(n).nodetoolResult("status").getStdout();
                    assertThat(Pattern.compile("^UN ", Pattern.MULTILINE).matcher(status).results().count()).as("node " + n + ":\n" + status).isEqualTo(2);
                });
            }
        }
        finally
        {
            starter.shutdownNow();
        }
    }

    private static String hostAddressAndPort(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
    }

    /** {@code address:port} as an endpoint, inside an instance. */
    private static InetAddressAndPort address(String hostAddressAndPort)
    {
        try
        {
            return InetAddressAndPort.getByName(hostAddressAndPort);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    /** Every node of the cluster is seen by {@code node} with {@code NET_VERSION} 12 in gossip. */
    private static void assertGossipsPublishedNetVersion(Cluster cluster, int node)
    {
        String gossip = cluster.get(node).nodetoolResult("gossipinfo").getStdout();
        for (int peer = 1; peer <= cluster.size(); peer++)
        {
            String address = cluster.get(peer).broadcastAddress().getAddress().getHostAddress();
            int start = gossip.indexOf('/' + address);
            assertThat(start).as("state of " + address + " in gossipinfo of node " + node + ":\n" + gossip).isNotNegative();
            int end = gossip.indexOf("\n/", start + 1);
            String state = gossip.substring(start, end < 0 ? gossip.length() : end);
            assertThat(state).as("NET_VERSION gossiped by " + address).containsPattern("NET_VERSION:\\d+:" + PUBLISHED_NET_VERSION + "\\b");
            // DSE's DSE_GOSSIP_STATE, with the DSE version below the one from which DSE groups the range reads of an index query
            assertThat(state).as("DSE_GOSSIP_STATE gossiped by " + address).containsPattern("X_11_PADDING:\\d+:\\{.*\"dse_version\":\"" + PUBLISHED_DSE_VERSION + "\"");
        }
    }

    /** Waits until every node has the state of {@code restarted} of its current generation, that is after its restart. */
    private static void awaitNewGeneration(Cluster cluster, int restarted)
    {
        int generation = cluster.get(restarted).callOnInstance(() -> Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddressAndPort()));
        String address = cluster.get(restarted).broadcastAddress().getAddress().getHostAddress();
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (int node = 1; node <= cluster.size(); node++)
            {
                String gossip = cluster.get(node).nodetoolResult("gossipinfo").getStdout();
                int start = gossip.indexOf('/' + address);
                assertThat(start).as("state of " + address + " in gossipinfo of node " + node).isNotNegative();
                int end = gossip.indexOf("\n/", start + 1);
                String state = gossip.substring(start, end < 0 ? gossip.length() : end);
                assertThat(state).as("generation of " + address + " seen by node " + node).contains("generation:" + generation + '\n');
            }
        });
    }

    /** The messaging version {@code node} records for each peer converges to the current one once they have talked. */
    private static void awaitPeersAtCurrentVersion(Cluster cluster, int node)
    {
        int current = cluster.get(node).callOnInstance(() -> MessagingService.current_version);
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            String status = DseDialectTestBase.compatStatus(cluster, node);
            assertThat(status).contains("(0 speaking the pre-4.0 dialect)");
            for (int peer = 1; peer <= cluster.size(); peer++)
            {
                if (peer == node)
                    continue;
                IInvokableInstance instance = cluster.get(peer);
                String address = instance.callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
                assertThat(status).containsPattern(Pattern.quote(address) + "\\s+" + current + "\\b");
            }
        });
    }
}
