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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The harness itself: an HCD 1.2 cluster at messaging version 11 writing trie {@code bb}, one node upgraded
 * in place to this software in {@code DSE_6} mode. The first test checks the upgraded node starts on the HCD 1.2
 * files, reports the mode and reads the data; the second that it exchanges messages with the peer at version 11; the
 * third that the read guard refuses a read with SAI optimizer options in the mixed cluster. A fourth, ignored, would
 * have an HCD 1.2 node join nodes of this software over gossip; the gossip between nodes of this software in
 * {@code DSE_6} mode is {@code Dse6GossipTest}'s.
 */
public class Dse6DialectSmokeTest extends DseDialectTestBase
{
    /** The {@code NET_VERSION} a node of this software gossips in {@code DSE_6} mode, the version DSE parses. */
    private static final String PUBLISHED_NET_VERSION = "12";

    @Test
    public void upgradedNodeStartsOnHcd12FilesInDse6Mode() throws Throwable
    {
        dse6TestCase(2)
        .nodesToUpgrade(1)
        .setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            for (int i = 0; i < 20; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, i, "v" + i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);
            // both HCD 1.2 nodes gossip the DSE dialect as their messaging version
            String gossip = cluster.get(1).nodetoolResult("gossipinfo").getStdout();
            assertThat(countMatches(gossip, "NET_VERSION:\\d+:" + DSE_DIALECT + "\\b")).isEqualTo(2);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            String status = compatStatus(cluster, node);
            assertThat(status).contains("Internode compatibility mode : DSE_6");
            assertThat(status).contains("Storage compatibility mode   : HCD_1");
            // the HCD 1.2 peer is recorded at version 11, the dialect
            assertThat(status).contains("(1 speaking the pre-4.0 dialect)");
            // the schema and the data written before the upgrade are on disk and readable
            Object[][] rows = cluster.get(node).executeInternal("SELECT k, v FROM " + KEYSPACE + ".t WHERE k = 7");
            assertThat(rows).hasNumberOfRows(1);
            assertThat(rows[0][1]).isEqualTo("v7");
        })
        .run();
    }

    @Test
    public void upgradedNodeTalksToTheHcd12Peer() throws Throwable
    {
        dse6TestCase(2)
        .nodesToUpgrade(1)
        .setup(cluster -> cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)"))
        .runAfterNodeUpgrade((cluster, node) -> {
            assertThat(compatStatus(cluster, node)).contains("(1 speaking the pre-4.0 dialect)");
            // a write coordinated by the upgraded node reaches the HCD 1.2 replica at version 11 ...
            cluster.coordinator(node).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (1, 'one')", ConsistencyLevel.ALL);
            assertThat(cluster.get(2).executeInternal("SELECT v FROM " + KEYSPACE + ".t WHERE k = 1")[0][0]).isEqualTo("one");
            // ... and a read coordinated by the HCD 1.2 node reaches the upgraded replica
            cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (2, 'two')");
            Object[][] rows = cluster.coordinator(2).execute("SELECT v FROM " + KEYSPACE + ".t WHERE k = 2", ConsistencyLevel.ALL);
            assertThat(rows).hasNumberOfRows(1);
            assertThat(rows[0][0]).isEqualTo("two");
        })
        .run();
    }

    /**
     * A read with SAI optimizer options (CNDB-19234) coordinated by the upgraded node, in a cluster with an HCD 1.2
     * peer at version 11. The in-JVM cluster delivers messages without opening connections, so the upgraded node has
     * no outbound connection pool, as with a DSE 6 peer it has never opened a connection to: the options' own check
     * ({@code OptimizerOptions.validate}, which looks at the connection pools) lets the read through, and the
     * {@code DSE_6} read guard refuses it with its own message and counts it. With a pool at version 11, as in a real
     * cluster once the peers talk, that check refuses the read first, in generic words and uncounted, as the unit test
     * {@code Dse6ReadGuardTest} shows. The clause on a read without a filtering restriction is dropped with its empty
     * row filter, so that read is served.
     */
    @Test
    public void optimizerOptionsAreRefusedWithoutAConnectionPool() throws Throwable
    {
        dse6TestCase(2)
        .nodesToUpgrade(1)
        .setup(cluster -> cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)"))
        .runAfterNodeUpgrade((cluster, node) -> {
            cluster.coordinator(node).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (1, 'one')", ConsistencyLevel.ALL);
            assertThat(compatStatus(cluster, node)).contains("(1 speaking the pre-4.0 dialect)");
            IInvokableInstance upgraded = (IInvokableInstance) cluster.get(node);
            assertThat(upgraded.callOnInstance(() -> MessagingService.instance().channelManagers.isEmpty())).isTrue();
            assertThat(upgraded.callOnInstance(() -> MessagingService.instance()
                                                                     .endpointsWithConnectionsOnVersionBelow(KEYSPACE, MessagingService.VERSION_DS_20)
                                                                     .isEmpty())).isTrue();

            String filtered = "SELECT k FROM " + KEYSPACE + ".t WHERE v = 'one' ALLOW FILTERING";
            String options = " WITH optimizer_options = {'query_optimization_level': '0'}";
            Throwable refused = catchThrowable(() -> cluster.coordinator(node).execute(filtered + options, ConsistencyLevel.ALL));
            assertThat(refused).hasMessageContaining("DSE 6 compatibility mode: an SAI optimizer option is not available while the " +
                                                     "upgrade from DSE 6 is in progress");
            String status = compatStatus(cluster, node);
            assertThat(status).contains("Requests refused (upgrade)   : 1\n");
            assertThat(status).containsPattern("  read with an SAI optimizer option +1\n");

            // the same read without the clause is served, and so is the clause on a read with no filtering restriction
            assertThat(cluster.coordinator(node).execute(filtered, ConsistencyLevel.ALL)).isEqualTo(new Object[][]{ { 1 } });
            assertThat(cluster.coordinator(node).execute("SELECT v FROM " + KEYSPACE + ".t WHERE k = 1" + options, ConsistencyLevel.ALL))
            .isEqualTo(new Object[][]{ { "one" } });
            assertThat(compatStatus(cluster, node)).contains("Requests refused (upgrade)   : 1\n");
        })
        .run();
    }

    /**
     * An HCD 1.2 node at version 11 joins the {@code DSE_6} nodes. HCD 1.2 started at version 11 exchanges its own 4.0
     * application-state ordinals without translation; the upgraded nodes never identify it as a DSE node (its first
     * handshake message is a plain one at 11, its answer to theirs is 11 and it gossips NET_VERSION 11), so they exchange
     * this software's ordinals with it too (unit test {@code DsePeerGossipTest}).
     * <p>
     * It still cannot run: the cluster starts as three HCD 1.2 nodes at version 11, which gossip needs to connect over
     * real sockets, and HCD 1.2 forced to version 11 refuses an inbound pre-4.0 connection from a peer requesting 11 (its
     * inbound handshake asserts "cndb-6.8 peer with bad requestMessagingVersion: 11 == 10": with its messaging version
     * overridden it accepts only its minimum, 10), so the first node's shadow round fails with "Unable to gossip with any
     * peers". The other tests of the harness run without the network, with the in-JVM message delivery. Gossip with a
     * real DSE node is covered by the unit tests against the captured DSE states.
     */
    @Test
    @Ignore("HCD 1.2 at version 11 refuses inbound pre-4.0 connections requesting 11, so an HCD 1.2 cluster with gossip never forms (see javadoc)")
    public void hcd12PeerAtTheDialectJoinsDse6Nodes() throws Throwable
    {
        dse6TestCase(3, config -> config.with(GOSSIP).with(NETWORK))
        // a subnet of its own: in-JVM clusters of other suites on 127.0.0.x must not reach these nodes
        .withBuilder(builder -> builder.withSubnet(9))
        .nodesToUpgrade(1, 2)
        .setup(cluster -> cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)"))
        .runAfterClusterUpgrade(cluster -> {
            // the two upgraded nodes see each other at the current version and the HCD 1.2 node at the dialect
            for (int node = 1; node <= 2; node++)
            {
                String status = compatStatus(cluster, node);
                assertThat(status).contains("Peers with a known version   : 2 (1 speaking the pre-4.0 dialect)");
                assertThat(status).contains("DSE peers                    : 0"); // HCD 1.2 gossips 11, not a DSE version
            }
            // the HCD 1.2 node sees the upgraded nodes at 12, the version DSE parses, and still UN
            String gossip = cluster.get(3).nodetoolResult("gossipinfo").getStdout();
            assertThat(gossip).containsPattern("NET_VERSION:\\d+:" + PUBLISHED_NET_VERSION + "\\b");
            assertThat(cluster.get(3).nodetoolResult("status").getStdout()).doesNotContain("DN");
            // the peers table of an upgraded node keeps the 4.0-line release version of the HCD 1.2 node
            Object[][] rows = cluster.get(1).executeInternal("SELECT release_version FROM system.peers_v2 WHERE peer = ? AND peer_port = ?",
                                                             cluster.get(3).broadcastAddress().getAddress(), cluster.get(3).broadcastAddress().getPort());
            assertThat(rows).hasNumberOfRows(1);
            assertThat((String) rows[0][0]).startsWith("4.0.");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (1, 'one')", ConsistencyLevel.ALL);
            assertThat(cluster.get(3).executeInternal("SELECT v FROM " + KEYSPACE + ".t WHERE k = 1")[0][0]).isEqualTo("one");
        })
        .run();
    }

    private static int countMatches(String text, String regex)
    {
        Matcher matcher = Pattern.compile(regex).matcher(text);
        int count = 0;
        while (matcher.find())
            count++;
        return count;
    }
}
