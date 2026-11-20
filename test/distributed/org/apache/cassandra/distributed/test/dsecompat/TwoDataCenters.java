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

import java.net.InetAddress;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.upgrade.UpgradeTestBase.TestCase;
import org.apache.cassandra.distributed.util.SingleHostLoadBalancingPolicy;
import org.apache.cassandra.net.Verb;

/**
 * The two-data-center layout of the DSE 6 compatibility tests of several data centers: six nodes, data center
 * {@code datacenter1} with nodes 1 to 3 and {@code datacenter2} with nodes 4 to 6, one rack each (or, with
 * {@link #testCaseWithTwoRacks}, a rack of two nodes and a rack of one in each). Every node starts as
 * HCD 1.2 at messaging version 11 (standing in for DSE 6, see {@link DseDialectTestBase}); nodes 1, 4 and 5 are upgraded to
 * this software in {@code DSE_6} mode, in that order: one data center partly upgraded (one node of three), the other mostly
 * (two of three), so that every coordinator, forwarder, leader and replica role is held by both versions somewhere. An
 * HCD 1.2 coordinator gives every forwarding target the forwarder's message id, where DSE gives each its own: the
 * forwarding with DSE's ids is built by hand in {@code MixedModeForwardingTest}.
 * <p>
 * Not a test: the tests extend {@link DseDialectTestBase} directly and use these helpers.
 */
final class TwoDataCenters
{
    static final String DC1 = "datacenter1";
    static final String DC2 = "datacenter2";
    /** The racks of {@link #testCaseWithTwoRacks}; {@link #testCase} has one rack, {@value #RACK1}, in each data center. */
    static final String RACK1 = "rack1";
    static final String RACK2 = "rack2";
    static final int NODES = 6;
    /** The nodes upgraded to this software, in the order of the upgrade. */
    static final int[] UPGRADED = { 1, 4, 5 };
    /** The HCD 1.2 nodes of the cluster (two in datacenter1, one in datacenter2): the peers of each upgraded node at the dialect. */
    private static final int HCD_PEERS = 3;

    private TwoDataCenters()
    {
    }

    /**
     * The test case of the layout: {@code extraConfig} is applied to every node after the harness configuration. The
     * layout takes the test case's builder consumer for its topology ({@code withRacks(2, 1, 3)}), so a test cannot add
     * one of its own (a test case takes one consumer of each kind).
     */
    static TestCase testCase(Consumer<IInstanceConfig> extraConfig)
    {
        return DseDialectTestBase.dse6TestCase(NODES, extraConfig)
                                 .withBuilder(builder -> builder.withRacks(2, 1, 3))
                                 .nodesToUpgrade(UPGRADED);
    }

    /**
     * As {@link #testCase}, with two racks in each data center, as observed on a DSE 6.9.26 cluster of two data centers:
     * {@value #RACK1} for the first two nodes of a data center (1 and 2, 4 and 5) and {@value #RACK2} for the third (3, 6).
     * The tokens are those of {@link #testCase}: one per node, evenly spaced in the order of the nodes, so the ring holds
     * the nodes of {@code datacenter1} (1, 2, 3) and then those of {@code datacenter2} (4, 5, 6).
     */
    static TestCase testCaseWithTwoRacks(Consumer<IInstanceConfig> extraConfig)
    {
        return DseDialectTestBase.dse6TestCase(NODES, extraConfig)
                                 .withBuilder(builder -> builder.withNodeIdTopology(NetworkTopology.networkTopology(NODES, node -> NetworkTopology.dcAndRack(dataCenter(node), rack(node)))))
                                 .nodesToUpgrade(UPGRADED);
    }

    /** The rack of a node in the layout of {@link #testCaseWithTwoRacks}. */
    static String rack(int node)
    {
        return (node - 1) % 3 < 2 ? RACK1 : RACK2;
    }

    /** The replication of a keyspace with {@code dc1} replicas in {@code datacenter1} and {@code dc2} in {@code datacenter2}. */
    static String networkTopology(int dc1, int dc2)
    {
        return "{'class': 'NetworkTopologyStrategy', '" + DC1 + "': " + dc1 + ", '" + DC2 + "': " + dc2 + '}';
    }

    static boolean isUpgraded(int node)
    {
        for (int upgraded : UPGRADED)
            if (upgraded == node)
                return true;
        return false;
    }

    static String dataCenter(int node)
    {
        return node <= 3 ? DC1 : DC2;
    }

    /** A node of the other version in the other data center: 4 for 2, 2 for 4, and so on (the lowest such node). */
    static int otherVersionInOtherDataCenter(int node)
    {
        for (int other = 1; other <= NODES; other++)
            if (!dataCenter(other).equals(dataCenter(node)) && isUpgraded(other) != isUpgraded(node))
                return other;
        throw new AssertionError("no node of the other version in the other data center of node " + node);
    }

    /** The other nodes of the node's data center. */
    static Set<Integer> localPeers(int node)
    {
        Set<Integer> peers = new TreeSet<>();
        for (int other = 1; other <= NODES; other++)
            if (other != node && dataCenter(other).equals(dataCenter(node)))
                peers.add(other);
        return peers;
    }

    /** The nodes of the other data center. */
    static Set<Integer> remoteNodes(int node)
    {
        Set<Integer> remote = new TreeSet<>();
        for (int other = 1; other <= NODES; other++)
            if (!dataCenter(other).equals(dataCenter(node)))
                remote.add(other);
        return remote;
    }

    /** Every upgraded node speaks the pre-4.0 dialect with the three HCD 1.2 nodes, and nothing else. */
    static void assertMixed(UpgradeableCluster cluster)
    {
        for (int node : UPGRADED)
            DseDialectTestBase.assertPeersAtDialect(cluster, node, HCD_PEERS);
    }

    /**
     * Records, without dropping anything, the route ({@code from->to}, node numbers) of every message of the verb sent
     * while the filter is on. HCD 1.2 and this software give the verbs the tests record the same id.
     */
    static IMessageFilters.Filter recordRoutes(UpgradeableCluster cluster, Verb verb, Set<String> routes)
    {
        return cluster.filters().outbound()
                      .verbs(verb.id)
                      .messagesMatching((from, to, message) -> {
                          routes.add(from + "->" + to);
                          return false;
                      })
                      .drop();
    }

    static String route(int from, int to)
    {
        return from + "->" + to;
    }

    /** The sending node of a route. */
    static int source(String route)
    {
        return Integer.parseInt(route.substring(0, route.indexOf("->")));
    }

    /** The receiving node of a route. */
    static int target(String route)
    {
        return Integer.parseInt(route.substring(route.indexOf("->") + 2));
    }

    /** The host id of a node, read from its {@code system.local}, whatever its version. */
    static UUID hostId(UpgradeableCluster cluster, int node)
    {
        return (UUID) cluster.get(node).executeInternal("SELECT host_id FROM system.local WHERE key = 'local'")[0][0];
    }

    /**
     * Runs statements through the native protocol on one node only, as a client would: the in-JVM coordinator of HCD 1.2
     * cannot set the serial consistency of a statement. The nodes need {@code Feature.NATIVE_PROTOCOL}.
     */
    static <T> T withSession(UpgradeableCluster cluster, int node, Function<Session, T> action)
    {
        InetAddress address = cluster.get(node).broadcastAddress().getAddress();
        try (com.datastax.driver.core.Cluster client = com.datastax.driver.core.Cluster.builder()
                                                                                         .addContactPoints(address)
                                                                                         .withProtocolVersion(ProtocolVersion.V4)
                                                                                         .withLoadBalancingPolicy(new SingleHostLoadBalancingPolicy(address))
                                                                                         .withoutJMXReporting()
                                                                                         .build();
             Session session = client.connect())
        {
            return action.apply(session);
        }
    }
}
