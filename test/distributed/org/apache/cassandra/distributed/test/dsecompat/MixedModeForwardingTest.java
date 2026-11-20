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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes forwarded across datacenters between HCD 1.2 nodes at messaging version 11 and nodes of this software in
 * {@code DSE_6} mode. Two datacenters of two nodes, one of each version in each (nodes 1 and 3 upgraded), and a keyspace
 * with two replicas in each datacenter: a write coordinated in one datacenter reaches the other through one of its nodes,
 * which the mutation names with the {@code FWD_TO} param; that node forwards it to its neighbour with the {@code FWD_FRM}
 * param, and the neighbour answers the coordinator directly. With writes at ALL coordinated by every node, the forwarder
 * and its neighbour are of either version in turn: both params are then written and read by both versions. The second
 * test forwards a write with DSE's message ids, in the six-node layout of {@link TwoDataCenters}.
 */
public class MixedModeForwardingTest extends DseDialectTestBase
{
    private static final String TABLE = "ks_dc.kv";
    /** Per coordinator: with a random forwarder, the chance that one of the two is never chosen is 2^-(KEYS-1). */
    private static final int KEYS = 30;
    /** The key, and the ids DSE would give the mutation to the forwarder and to each target, in the test of the ids. */
    private static final int KEY = 77;
    private static final long FORWARDER_ID = 1_000_001;
    private static final long UPGRADED_TARGET_ID = 1_000_002;
    private static final long HCD_TARGET_ID = 1_000_003;

    @Test
    public void forwardingAcrossDatacentersInBothDirections() throws Throwable
    {
        // the routes (sender->receiver) of the mutations of the writes of each coordinator
        Map<Integer, Set<String>> routes = new ConcurrentHashMap<>();
        AtomicInteger currentCoordinator = new AtomicInteger();
        dse6TestCase(4)
        .withBuilder(builder -> builder.withRacks(2, 1, 2))
        .nodesToUpgrade(1, 3)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE ks_dc WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 2}");
            cluster.schemaChange("CREATE TABLE " + TABLE + " (k int PRIMARY KEY, v int)");
        })
        .runAfterClusterUpgrade(cluster -> {
            assertPeersAtDialect(cluster, 1, 2);
            assertPeersAtDialect(cluster, 3, 2);
            // record the route of every mutation as it is sent, dropping none (on receipt the in-JVM message names the
            // coordinator as its sender, a forwarded one included)
            IMessageFilters.Filter recorder = cluster.filters().outbound()
                                                     .verbs(Verb.MUTATION_REQ.id)
                                                     .messagesMatching((from, to, message) -> {
                                                         routes.computeIfAbsent(currentCoordinator.get(), c -> ConcurrentHashMap.newKeySet()).add(from + "->" + to);
                                                         return false;
                                                     })
                                                     .drop();
            try
            {
                for (int coordinator = 1; coordinator <= 4; coordinator++)
                {
                    currentCoordinator.set(coordinator);
                    // at ALL the write returns once every replica acknowledged it, so its mutations have all been sent
                    writeAtAll(cluster, coordinator);
                }
            }
            finally
            {
                recorder.off();
            }

            // datacenter 1 is nodes 1 (upgraded) and 2, datacenter 2 nodes 3 (upgraded) and 4. A coordinator names either node
            // of the other datacenter as the forwarder (FWD_TO), which forwards to its neighbour (FWD_FRM): for a coordinator
            // in datacenter 1 the mutations inside datacenter 2 are all forwarded ones, and the other way round
            assertThat(routes.get(1)).as("upgraded coordinator 1: FWD_TO to both versions, FWD_FRM both ways in datacenter 2")
                                     .contains("1->3", "1->4", "3->4", "4->3");
            assertThat(routes.get(2)).as("HCD 1.2 coordinator 2: FWD_TO to both versions").contains("2->3", "2->4");
            assertThat(routes.get(3)).as("upgraded coordinator 3: FWD_TO to both versions, FWD_FRM both ways in datacenter 1")
                                     .contains("3->1", "3->2", "1->2", "2->1");
            assertThat(routes.get(4)).as("HCD 1.2 coordinator 4: FWD_TO to both versions").contains("4->1", "4->2");
        })
        .run();
    }

    /**
     * A coordinator at the pre-4.0 dialect (DSE 6) lists a message id of its own for each target of {@code FWD_TO} and
     * expects each target to answer under it, as Cassandra 3.0 did; HCD 1.2, of the 4.0 line, gives them all the
     * forwarder's id, so the mutation DSE would send is built here. In the layout of {@link TwoDataCenters}, node 4
     * (this software) is the forwarder for node 2 (HCD 1.2, standing in for the DSE coordinator) and forwards to node 5
     * (this software) and node 6 (HCD 1.2): each answers node 2 under its own id, node 4 under the forwarder's.
     */
    @Test
    public void upgradedForwarderKeepsTheIdOfEachTarget() throws Throwable
    {
        Map<String, Long> responseIds = new ConcurrentHashMap<>();
        TwoDataCenters.testCase(config -> {})
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE KEYSPACE ks_dc WITH replication = " + TwoDataCenters.networkTopology(3, 3));
                          cluster.schemaChange("CREATE TABLE " + TABLE + " (k int PRIMARY KEY, v int)");
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          IMessageFilters.Filter recorder = cluster.filters().outbound()
                                                                   .verbs(Verb.MUTATION_RSP.id)
                                                                   .to(2)
                                                                   .messagesMatching((from, to, message) -> {
                                                                       responseIds.put(from + "->" + to, (long) message.id());
                                                                       return false;
                                                                   })
                                                                   .drop();
                          byte[] coordinator = address(cluster, 2);
                          byte[] upgradedTarget = address(cluster, 5);
                          byte[] hcdTarget = address(cluster, 6);
                          int coordinatorPort = cluster.get(2).broadcastAddress().getPort();
                          int upgradedTargetPort = cluster.get(5).broadcastAddress().getPort();
                          int hcdTargetPort = cluster.get(6).broadcastAddress().getPort();
                          try
                          {
                              ((IInvokableInstance) cluster.get(4)).runOnInstance(() -> {
                                  TableMetadata table = Schema.instance.getTableMetadata("ks_dc", "kv");
                                  Mutation.SimpleBuilder builder = Mutation.simpleBuilder(table.keyspace, table.partitioner.decorateKey(Int32Type.instance.decompose(KEY)))
                                                                           .timestamp(FBUtilities.timestampMicros());
                                  builder.update(table).row().add("v", KEY);
                                  Mutation mutation = builder.build();
                                  try
                                  {
                                      ForwardingInfo forwardTo = new ForwardingInfo(Arrays.asList(InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(upgradedTarget), upgradedTargetPort),
                                                                                                  InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(hcdTarget), hcdTargetPort)),
                                                                                    new long[]{ UPGRADED_TARGET_ID, HCD_TARGET_ID });
                                      MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation)
                                                                                 .from(InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(coordinator), coordinatorPort))
                                                                                 .withId(FORWARDER_ID)
                                                                                 .withParam(ParamType.FORWARD_TO, forwardTo)
                                                                                 .build());
                                  }
                                  catch (UnknownHostException e)
                                  {
                                      throw new AssertionError(e);
                                  }
                              });
                              awaitAsserted(() -> assertThat(responseIds).containsEntry("4->2", FORWARDER_ID)
                                                                         .containsEntry("5->2", UPGRADED_TARGET_ID)
                                                                         .containsEntry("6->2", HCD_TARGET_ID));
                          }
                          finally
                          {
                              recorder.off();
                          }
                          for (int node : new int[]{ 4, 5, 6 })
                              assertRows(cluster.get(node).executeInternal("SELECT v FROM " + TABLE + " WHERE k = ?", KEY), row(KEY));
                      })
                      .run();
    }

    /** The broadcast address of a node, as bytes (IPv4 or IPv6), to hand to code run inside an instance. */
    private static byte[] address(UpgradeableCluster cluster, int node)
    {
        return cluster.get(node).broadcastAddress().getAddress().getAddress();
    }

    private static void writeAtAll(UpgradeableCluster cluster, int coordinator)
    {
        int base = coordinator * 1000;
        for (int k = base; k < base + KEYS; k++)
            cluster.coordinator(coordinator).execute("INSERT INTO " + TABLE + " (k, v) VALUES (?, ?)", ALL, k, coordinator);
        // at ALL every replica acknowledged, forwarded ones included; the rows are on every node
        for (int node = 1; node <= cluster.size(); node++)
            assertRows(cluster.get(node).executeInternal("SELECT count(*) FROM " + TABLE + " WHERE k >= ? AND k < ? ALLOW FILTERING", base, base + KEYS),
                       row((long) KEYS));
    }
}
