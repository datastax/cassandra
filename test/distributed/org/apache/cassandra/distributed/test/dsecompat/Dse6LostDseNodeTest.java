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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.DseUpgradeGuards;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6GuardsTest.DSE_NODE;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6GuardsTest.SUBNET;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6GuardsTest.assertRefused;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6GuardsTest.formerDseNodes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The removal of a DSE 6 node lost for good during the upgrade, by nodes of this software in {@code DSE_6} mode and
 * without the mode: removenode once every other DSE 6 node is lost too (its data streamed between nodes of this
 * software), assassinate while one is not, and the replacement of the lost node once every node runs without the mode.
 * The lost node is a gossip state injected on node 1 as a ring member that is down: the in-JVM nodes of HCD 1.2 run
 * without gossip, so they cannot model the failure detection. A class of its own, so that one test JVM starts fewer
 * clusters.
 */
public class Dse6LostDseNodeTest extends TestBaseImpl
{
    /** A DSE node lost for good: a ring member that exists only in the gossip state injected on node 1, convicted down. */
    private static final String LOST_DSE_NODE = "127.0.10.98:7012";
    private static final String RF2 = "ks_rf2";
    private static final int ROWS = 100;

    /** As in {@link Dse6GuardsTest}: the trie version of the mode is a JVM-wide property, and the SAI pin is taken as set. */
    private WithProperties trieIndexFormatVersion;
    private WithProperties saiVersionPin;

    @Before
    public void setSystemProperties()
    {
        trieIndexFormatVersion = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
        saiVersionPin = new WithProperties().set(SAI_CURRENT_VERSION, "aa");
    }

    @After
    public void restoreSystemProperties()
    {
        saiVersionPin.close();
        trieIndexFormatVersion.close();
    }

    /**
     * A DSE 6 node lost for good (its gossip state injected on node 1 as a ring member, then convicted) is removed by
     * {@code nodetool removenode} in {@code DSE_6} mode once every other DSE 6 node is lost too (here there is none):
     * its ranges are streamed between the nodes of this software.
     */
    @Test
    public void aLostDseNodeIsRemovedOnceNoDseNodeIsAlive() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(2, builder -> {}))
        {
            removeALostDseNode(cluster);
        }
    }

    /** The same without the mode: the lost DSE 6 node keeps the upgrade in progress there, and is removed the same way. */
    @Test
    public void aLostDseNodeIsRemovedWithoutTheMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withSubnet(SUBNET).withDynamicPortAllocation(true)
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            removeALostDseNode(cluster);
        }
    }

    /**
     * Injects a lost DSE 6 node on node 1, takes writes at ONE (those of its ranges reach one node only), removes it
     * with removenode from node 1 and checks that the other node received the rows of those ranges by streaming.
     */
    private static void removeALostDseNode(Cluster cluster) throws Exception
    {
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            cluster.schemaChange("CREATE KEYSPACE " + RF2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + RF2 + ".t (k int PRIMARY KEY, v text)");
        }
        IInvokableInstance node = cluster.get(1);
        String hostId = UUID.randomUUID().toString();
        node.runOnInstance(() -> DseUpgradeGuards.setLostPeerSilenceMillis(1000));
        node.runOnInstance(() -> injectDseRingMember(LOST_DSE_NODE, hostId, 0L));
        awaitStatus(node, "DN", address(LOST_DSE_NODE));
        // the rows of the ranges the lost node replicates reach a single node: the other one has fewer before the removal
        for (int i = 0; i < ROWS; i++)
            cluster.coordinator(1).execute("INSERT INTO " + RF2 + ".t (k, v) VALUES (?, ?)", ConsistencyLevel.ONE, i, "v" + i);
        long before = localRows(cluster, 1) + localRows(cluster, 2);
        assertThat(before).isGreaterThanOrEqualTo(ROWS).isLessThan(2L * ROWS);
        Thread.sleep(1500); // the silence of a lost node

        node.nodetoolResult("removenode", hostId).asserts().success();
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
               .until(() -> !node.nodetoolResult("status").getStdout().contains(address(LOST_DSE_NODE)));
        assertThat(node.logs().grep("Allowing the removal of the DSE 6 node /" + LOST_DSE_NODE + ", silent for 0 minutes, during the DSE 6 upgrade").getResult()).hasSize(1);
        // both nodes now replicate every row, streamed from the node that had them
        assertThat(localRows(cluster, 1)).isEqualTo(ROWS);
        assertThat(localRows(cluster, 2)).isEqualTo(ROWS);
        assertThat(node.nodetoolResult("compatstatus").getStdout()).contains("Requests refused (upgrade)   : 0\n");
    }

    private static long localRows(Cluster cluster, int node)
    {
        return (Long) cluster.get(node).executeInternal("SELECT count(*) FROM " + RF2 + ".t")[0][0];
    }

    /**
     * A DSE 6 node lost for good while another DSE 6 node is alive (both gossip states injected on node 1): removenode,
     * which would stream with the live one, is refused with the way out, and assassinate, which streams nothing, removes
     * the lost node; the live DSE 6 node cannot be assassinated.
     */
    @Test
    public void aLostDseNodeIsAssassinatedWhileAnotherIsAlive() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(2, builder -> {}))
        {
            IInvokableInstance node = cluster.get(1);
            String hostId = UUID.randomUUID().toString();
            node.runOnInstance(() -> DseUpgradeGuards.setLostPeerSilenceMillis(1000));
            node.runOnInstance(() -> injectDseRingMember(LOST_DSE_NODE, hostId, 0L));
            // a DSE 6 node marked alive and never convicted: it never sends a heartbeat, so this node's failure detector,
            // which has no arrival time for it, never convicts it
            node.runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                try
                {
                    Gossiper.instance.initializeNodeUnsafe(InetAddressAndPort.getByName(DSE_NODE), UUID.randomUUID(), 1536, 1);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }));
            awaitStatus(node, "DN", address(LOST_DSE_NODE));
            Thread.sleep(1500); // the silence of a lost node

            assertRefused(node.nodetoolResult("removenode", hostId),
                          "removenode of the DSE 6 node /" + LOST_DSE_NODE + " lost for good is refused while DSE 6 nodes not known to be lost " +
                          "are in the cluster ([/" + DSE_NODE + "])");
            assertRefused(node.nodetoolResult("removenode", hostId), "remove the lost node with nodetool assassinate " + LOST_DSE_NODE);
            assertRefused(node.nodetoolResult("assassinate", DSE_NODE),
                          "assassinate is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6, DSE 6 peers: [");

            node.nodetoolResult("assassinate", LOST_DSE_NODE).asserts().success();
            await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                   .until(() -> !node.nodetoolResult("status").getStdout().contains(address(LOST_DSE_NODE)));
            assertThat(node.logs().grep("Allowing the assassination of the DSE 6 node /" + LOST_DSE_NODE + ", silent for 0 minutes, during the DSE 6 upgrade").getResult()).hasSize(1);
            String out = node.nodetoolResult("compatstatus").getStdout();
            assertThat(out).contains("Requests refused (upgrade)   : 3\n");
            assertThat(out).containsPattern("  removenode +2\n");
            assertThat(out).containsPattern("  assassinate +1\n");
        }
    }

    /**
     * Once every node runs without the mode, a node without the mode replaces a DSE 6 node lost for good (its gossip
     * state injected on node 1 as a ring member that is down, which keeps the upgrade in progress there): its shadow
     * round does not count the replaced node, and no other DSE 6 node is in gossip, so it takes over the tokens of the
     * lost node and streams their data from node 1. The other guarded operations stay refused on node 1.
     */
    @Test
    public void aNodeWithoutTheModeReplacesALostDseNode() throws Throwable
    {
        // the storage port of every node, the one of the lost node too (a replacement names the replaced node by its
        // address, at its own storage port)
        try (Cluster cluster = init(Cluster.build(1).withSubnet(SUBNET)
                                           .withTokenSupplier(evenlyDistributedTokens(4, 1))
                                           .withNodeIdTopology(singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            IInvokableInstance node = cluster.get(1);
            String hostId = UUID.randomUUID().toString();
            node.runOnInstance(() -> injectDseRingMember(LOST_DSE_NODE, hostId, 12345L));
            awaitStatus(node, "DN", address(LOST_DSE_NODE));
            // the upgrade is in progress on node 1, without the mode, because of the lost DSE 6 node: other operations are
            // still refused
            assertRefused(node.nodetoolResult("move", "1"), "move is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: " +
                                                            "NONE, but DSE 6 peers are in the cluster: [/" + LOST_DSE_NODE + "])");

            IInstanceConfig replacement = cluster.newInstanceConfig();
            replacement.set("auto_bootstrap", true);
            IInvokableInstance replacing = cluster.bootstrap(replacement);
            try (WithProperties properties = new WithProperties().set(REPLACE_ADDRESS, LOST_DSE_NODE))
            {
                replacing.startup(cluster);
            }
            String address = replacing.broadcastAddress().getAddress().getHostAddress();
            awaitStatus(node, "UN", address);
            await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                   .until(() -> !node.nodetoolResult("status").getStdout().contains(address(LOST_DSE_NODE)));
            assertThat(replacing.callOnInstance(() -> StorageService.instance.getTokens().toString())).isEqualTo("[12345]");
        }
    }

    /**
     * Adds a DSE 6 node to the gossip state of this instance as a ring member that never answers (its address has no
     * node), marked down as the failure detector marks a node lost for good.
     */
    private static void injectDseRingMember(String address, String hostId, long token)
    {
        try
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
            VersionedValue.VersionedValueFactory values = StorageService.instance.valueFactory;
            Collection<Token> tokens = Collections.singleton(new Murmur3Partitioner.LongToken(token));
            Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
            states.put(ApplicationState.NET_VERSION, values.networkVersion(1536));
            states.put(ApplicationState.HOST_ID, values.hostId(UUID.fromString(hostId)));
            states.put(ApplicationState.RELEASE_VERSION, values.releaseVersion("4.0.0.6865"));
            states.put(ApplicationState.DC, values.datacenter(DatabaseDescriptor.getLocalDataCenter()));
            states.put(ApplicationState.RACK, values.rack(DatabaseDescriptor.getEndpointSnitch().getLocalRack()));
            states.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, values.internalAddressAndPort(endpoint));
            states.put(ApplicationState.RPC_ADDRESS, values.rpcaddress(endpoint.getAddress()));
            states.put(ApplicationState.TOKENS, values.tokens(tokens));
            states.put(ApplicationState.STATUS_WITH_PORT, values.normal(tokens));
            states.put(ApplicationState.STATUS, values.normal(tokens));
            EndpointState state = new EndpointState(new HeartBeatState(1, 1), states);
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.applyStateLocally(Collections.singletonMap(endpoint, state)));
            // its echo is never answered (?N in nodetool status): convicted as a node that stopped answering is
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(endpoint, Gossiper.instance.getEndpointStateForEndpoint(endpoint)));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String address(String addressAndPort)
    {
        return addressAndPort.substring(0, addressAndPort.lastIndexOf(':'));
    }

    /** Waits until {@code nodetool status} of {@code node} shows {@code address} (without port) in {@code status}. */
    private static void awaitStatus(IInvokableInstance node, String status, String address)
    {
        String line = "(?m)^" + status + " +" + Pattern.quote(address) + "\\b";
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(node.nodetoolResult("status").getStdout()).containsPattern(line));
    }
}
