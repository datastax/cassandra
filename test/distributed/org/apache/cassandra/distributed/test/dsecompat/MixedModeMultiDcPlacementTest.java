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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC1;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC2;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.RACK1;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.RACK2;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.dataCenter;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.isUpgraded;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.rack;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replica placement across two data centers and two racks per data center, computed by HCD 1.2 nodes at messaging
 * version 11 (standing in for DSE 6) and by nodes of this software in {@code DSE_6} mode, in the layout of
 * {@link TwoDataCenters#testCaseWithTwoRacks}: in each data center a rack of two nodes ({@code rack1}: 1 and 2, 4 and
 * 5) and a rack of one ({@code rack2}: 3, 6), as observed on a DSE 6.9.26 cluster of two data centers. With one rack
 * per data center ({@link TwoDataCenters#testCase}) or one rack per node, the rack-aware walk of
 * {@code NetworkTopologyStrategy} takes the next nodes as {@code SimpleStrategy} does, so such a layout cannot tell the
 * strategies apart; here, with a replication factor of 2 in a data center of three nodes in two racks, the walk skips
 * the second node of a rack it has seen. A choice between nodes of a seen rack (more replicas than racks, fewer than
 * nodes) needs four nodes in a data center, more than this layout has.
 * <p>
 * For every token range of every keyspace, the replicas each node computes, of either version, equal, in the same order,
 * those every node computed while the whole cluster ran HCD 1.2; the replicas of sample keys agree on every node, and
 * writes at {@code ALL} through a coordinator of each version land on exactly those replicas.
 */
public class MixedModeMultiDcPlacementTest extends DseDialectTestBase
{
    /** Two replicas per data center: the minimal layout where the rack rule and {@code SimpleStrategy} differ. */
    private static final String NTS_2_2 = "ks_nts22";
    /** Three replicas in {@code datacenter1} (all its nodes, more than its racks) and one in {@code datacenter2}. */
    private static final String NTS_3_1 = "ks_nts31";
    /** {@code SimpleStrategy} with two replicas: the next two nodes of the ring, whatever their data center and rack. */
    private static final String SIMPLE_2 = "ks_simple2";
    private static final Map<String, String> REPLICATION = Map.of(NTS_2_2, networkTopology(2, 2),
                                                                  NTS_3_1, networkTopology(3, 1),
                                                                  SIMPLE_2, "{'class': 'SimpleStrategy', 'replication_factor': 2}");
    private static final int KEYS = 40;

    @Test
    public void bothVersionsPlaceTheReplicasAlike() throws Throwable
    {
        // keyspace -> token range -> replicas in the order listed, as every node computed them while the cluster ran HCD 1.2 only
        Map<String, Map<String, List<Integer>>> beforeTheUpgrade = new ConcurrentHashMap<>();
        TwoDataCenters.testCaseWithTwoRacks(config -> {})
                      .setup(cluster -> {
                          for (Map.Entry<String, String> keyspace : REPLICATION.entrySet())
                          {
                              cluster.schemaChange("CREATE KEYSPACE " + keyspace.getKey() + " WITH replication = " + keyspace.getValue());
                              cluster.schemaChange("CREATE TABLE " + keyspace.getKey() + ".t (k int PRIMARY KEY, v int)");
                          }
                      })
                      .runBeforeClusterUpgrade(cluster -> {
                          for (String keyspace : REPLICATION.keySet())
                          {
                              Map<String, List<Integer>> first = placement(cluster, 1, keyspace);
                              for (int node = 2; node <= cluster.size(); node++)
                                  assertThat(placement(cluster, node, keyspace)).as("%s on HCD 1.2 node %s", keyspace, node).isEqualTo(first);
                              beforeTheUpgrade.put(keyspace, first);
                          }
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          // a sanity check of the layout, not of either version: the in-JVM snitch of both versions
                          // reads the topology the harness gives every instance
                          for (int node = 1; node <= cluster.size(); node++)
                              for (int other = 1; other <= cluster.size(); other++)
                                  assertThat(snitchOf(cluster, node, other)).as("node %s about node %s", node, other).isEqualTo(dataCenter(other) + '/' + rack(other));
                          for (String keyspace : REPLICATION.keySet())
                          {
                              Map<String, List<Integer>> expected = beforeTheUpgrade.get(keyspace);
                              assertThat(expected).as(keyspace).hasSize(cluster.size());
                              for (int node = 1; node <= cluster.size(); node++)
                                  assertThat(placement(cluster, node, keyspace))
                                  .as("%s on %s node %s", keyspace, isUpgraded(node) ? "upgraded" : "HCD 1.2", node)
                                  .isEqualTo(expected);
                          }
                          assertTheRackRule(beforeTheUpgrade.get(NTS_2_2));
                          assertMoreReplicasThanRacks(beforeTheUpgrade.get(NTS_3_1));
                          assertSimpleStrategyIgnoresTheRacks(beforeTheUpgrade.get(SIMPLE_2));
                          for (String keyspace : REPLICATION.keySet())
                              writesLandOnTheReplicasBothVersionsCompute(cluster, keyspace);
                      })
                      .run();
    }

    /**
     * Two replicas per data center: in every range, one node of each rack of each data center. The rack of one node
     * ({@code rack2}) holds a replica of every range, where the next two nodes of the ring would often be the two of
     * {@code rack1}.
     */
    private static void assertTheRackRule(Map<String, List<Integer>> placement)
    {
        placement.forEach((range, replicas) -> {
            assertThat(replicas).as("replicas of %s in %s", range, NTS_2_2).hasSize(4);
            for (String dataCenter : new String[]{ DC1, DC2 })
                for (String rack : new String[]{ RACK1, RACK2 })
                    assertThat(replicas.stream().filter(n -> dataCenter(n).equals(dataCenter) && rack(n).equals(rack)).count())
                    .as("replicas of %s in %s, %s", range, dataCenter, rack)
                    .isEqualTo(1);
        });
    }

    /**
     * Three replicas in a data center of three nodes in two racks, one in the other: every node of {@code datacenter1}
     * holds every range, listed in the order of the ring from the end of the range (the walk takes the first node of a seen
     * rack it meets, as many as the replication factor exceeds the racks), and the one replica of {@code datacenter2} is
     * each of its nodes in turn. With as many replicas as nodes no strategy can pick another set of nodes in
     * {@code datacenter1}: what this keyspace checks is the order both versions list them in and the replication factors
     * that differ by data center, not a choice between nodes of a seen rack, which needs more nodes than replicas in a data
     * center.
     */
    private static void assertMoreReplicasThanRacks(Map<String, List<Integer>> placement)
    {
        Set<Integer> secondDataCenter = new TreeSet<>();
        placement.forEach((range, replicas) -> {
            assertThat(replicas).as("replicas of %s in %s", range, NTS_3_1).hasSize(4);
            List<Integer> firstDataCenter = replicas.stream().filter(n -> dataCenter(n).equals(DC1)).collect(Collectors.toList());
            assertThat(firstDataCenter).as("replicas of %s in %s of %s", range, DC1, NTS_3_1)
                                       .isIn(List.of(1, 2, 3), List.of(2, 3, 1), List.of(3, 1, 2));
            replicas.stream().filter(n -> dataCenter(n).equals(DC2)).forEach(secondDataCenter::add);
        });
        assertThat(secondDataCenter).as("replicas in %s of %s", DC2, NTS_3_1).containsExactly(4, 5, 6);
    }

    /**
     * {@code SimpleStrategy} takes the next two nodes of the ring: the two nodes of a rack in one range, and both data
     * centers in another, which is where it and the rack rule differ.
     */
    private static void assertSimpleStrategyIgnoresTheRacks(Map<String, List<Integer>> placement)
    {
        placement.forEach((range, replicas) -> assertThat(replicas).as("replicas of %s in %s", range, SIMPLE_2).hasSize(2));
        assertThat(placement.values()).as(SIMPLE_2).contains(List.of(1, 2), List.of(4, 5));
        assertThat(placement.values().stream().filter(replicas -> replicas.stream().map(TwoDataCenters::dataCenter).distinct().count() == 2))
        .as("ranges of %s with a replica in each data center", SIMPLE_2)
        .isNotEmpty();
    }

    /**
     * The replicas of sample keys, as every node computes them, then where writes at {@code ALL} land: through the
     * upgraded node 1 for even keys, the HCD 1.2 node 2 for odd ones, the rows are on exactly the replicas computed.
     */
    private static void writesLandOnTheReplicasBothVersionsCompute(UpgradeableCluster cluster, String keyspace)
    {
        for (int k = 0; k < KEYS; k++)
        {
            List<Integer> replicas = replicasOfKey(cluster, 1, keyspace, k);
            for (int node = 2; node <= cluster.size(); node++)
                assertThat(replicasOfKey(cluster, node, keyspace, k)).as("replicas of key %s of %s on node %s", k, keyspace, node).isEqualTo(replicas);
            int coordinator = k % 2 == 0 ? 1 : 2;
            cluster.coordinator(coordinator).execute("INSERT INTO " + keyspace + ".t (k, v) VALUES (?, ?)", ALL, k, coordinator);
            Set<Integer> holders = new TreeSet<>();
            for (int node = 1; node <= cluster.size(); node++)
                if (cluster.get(node).executeInternal("SELECT v FROM " + keyspace + ".t WHERE k = ?", k).length == 1)
                    holders.add(node);
            assertThat(holders).as("nodes holding key %s of %s written through node %s", k, keyspace, coordinator).isEqualTo(new TreeSet<>(replicas));
        }
    }

    /**
     * The replicas of every token range of a keyspace as a node computes them ({@code StorageService.getRangeToEndpointWithPortMap},
     * the source of {@code nodetool describering}), by node number, in the order the node lists them. Called by reflection
     * through the instance's class loader: test classes are not loaded in the instances of this harness, and both versions
     * have the method.
     */
    private static Map<String, List<Integer>> placement(UpgradeableCluster cluster, int node, String keyspace)
    {
        @SuppressWarnings("unchecked")
        Map<List<String>, List<String>> ranges = (Map<List<String>, List<String>>) callStorageService(node, "getRangeToEndpointWithPortMap",
                                                                                                        new Class<?>[]{ String.class }, keyspace);
        Map<String, List<Integer>> placement = new TreeMap<>();
        ranges.forEach((range, endpoints) -> placement.put("(" + range.get(0) + ", " + range.get(1) + ']', nodes(cluster, endpoints)));
        return placement;
    }

    /**
     * The replicas of a key as a node computes them ({@code StorageService.getNaturalEndpointsWithPort}, the source of
     * {@code nodetool getendpoints}), in the order the node lists them.
     */
    private static List<Integer> replicasOfKey(UpgradeableCluster cluster, int node, String keyspace, int k)
    {
        @SuppressWarnings("unchecked")
        List<String> endpoints = (List<String>) callStorageService(node, "getNaturalEndpointsWithPort",
                                                                   new Class<?>[]{ String.class, String.class, String.class }, keyspace, "t", Integer.toString(k));
        return nodes(cluster, endpoints);
    }

    /** The data center and rack of node {@code other}, as {@code <data center>/<rack>}, as the snitch of {@code node} gives them. */
    private static String snitchOf(UpgradeableCluster cluster, int node, int other)
    {
        try
        {
            ClassLoader classLoader = instanceClassLoader(node);
            Object snitch = Class.forName("org.apache.cassandra.config.DatabaseDescriptor", true, classLoader).getMethod("getEndpointSnitch").invoke(null);
            Class<?> addressClass = Class.forName("org.apache.cassandra.locator.InetAddressAndPort", true, classLoader);
            Object address = addressClass.getMethod("getByName", String.class)
                                         .invoke(null, cluster.get(other).broadcastAddress().getAddress().getHostAddress() + ':' +
                                                       cluster.get(other).broadcastAddress().getPort());
            Class<?> snitchInterface = Class.forName("org.apache.cassandra.locator.IEndpointSnitch", true, classLoader);
            return snitchInterface.getMethod("getDatacenter", addressClass).invoke(snitch, address) + "/" +
                   snitchInterface.getMethod("getRack", addressClass).invoke(snitch, address);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("cannot read the data center and rack of node " + other + " on node " + node, e);
        }
    }

    private static Object callStorageService(int node, String method, Class<?>[] types, Object... arguments)
    {
        try
        {
            Class<?> storageService = Class.forName("org.apache.cassandra.service.StorageService", true, instanceClassLoader(node));
            Object instance = storageService.getField("instance").get(null);
            return storageService.getMethod(method, types).invoke(instance, arguments);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("cannot call StorageService." + method + " on node " + node, e);
        }
    }

    /** The node numbers of {@code <address>:<port>} endpoints, in their order. */
    private static List<Integer> nodes(UpgradeableCluster cluster, List<String> endpoints)
    {
        List<Integer> nodes = new ArrayList<>();
        for (String endpoint : endpoints)
            nodes.add(node(cluster, endpoint));
        assertThat(nodes).as("nodes of %s", endpoints).doesNotHaveDuplicates();
        return nodes;
    }

    private static int node(UpgradeableCluster cluster, String endpoint)
    {
        String host = endpoint.substring(0, endpoint.lastIndexOf(':'));
        for (int node = 1; node <= cluster.size(); node++)
            if (cluster.get(node).broadcastAddress().getAddress().getHostAddress().equals(host))
                return node;
        throw new AssertionError("no node at " + endpoint);
    }
}
