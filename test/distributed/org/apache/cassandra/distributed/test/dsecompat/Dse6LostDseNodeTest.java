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
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS;
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
 * Once removed, the node is no longer on the record of DSE 6 peers. The lost node is a gossip state injected on node 1
 * as a ring member that is down: the in-JVM nodes of HCD 1.2 run without gossip, so they cannot model the failure
 * detection. The silence of a lost node is set to one second once the nodes have started, below the floor their startup
 * check would refuse. A class of its own, so that one test JVM starts fewer clusters; the lost node of this software is
 * {@link Dse6LostNodeOfThisSoftwareTest}'s, which uses the helpers here.
 */
public class Dse6LostDseNodeTest extends TestBaseImpl
{
    /** A DSE node lost for good: a ring member that exists only in the gossip state injected on node 1, convicted down. */
    static final String LOST_DSE_NODE = "127.0.10.98:7012";
    /** The NET_VERSION a DSE 6.8 node gossips. */
    static final int DSE_NET_VERSION = 1536;
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
            removeLostNodes(cluster, new String[]{ LOST_DSE_NODE }, new int[]{ DSE_NET_VERSION }, new String[]{ "the DSE 6 node /" + LOST_DSE_NODE });
        }
    }

    /** The same without the mode: the lost DSE 6 node keeps the upgrade in progress there, and is removed the same way. */
    @Test
    public void aLostDseNodeIsRemovedWithoutTheMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withSubnet(SUBNET).withDynamicPortAllocation(true)
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            removeLostNodes(cluster, new String[]{ LOST_DSE_NODE }, new int[]{ DSE_NET_VERSION }, new String[]{ "the DSE 6 node /" + LOST_DSE_NODE });
        }
    }

    /**
     * Injects the lost nodes {@code lostNodes} on node 1, each gossiping its entry of {@code netVersions} and owning a
     * token of its own, takes writes at ONE (those of their ranges reach one node only), removes them in turn with
     * removenode from node 1 and checks that each WARN names the node as its entry of {@code subjects}, that the other
     * node received the rows of those ranges by streaming, and that in DSE_6 mode the record of DSE 6 peers lists the
     * lost DSE 6 nodes before the removals and none after.
     */
    static void removeLostNodes(Cluster cluster, String[] lostNodes, int[] netVersions, String[] subjects) throws Exception
    {
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            cluster.schemaChange("CREATE KEYSPACE " + RF2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + RF2 + ".t (k int PRIMARY KEY, v text)");
        }
        IInvokableInstance node = cluster.get(1);
        boolean dse6Mode = node.callOnInstance(DatabaseDescriptor::isDse6CompatibilityMode);
        String[] hostIds = new String[lostNodes.length];
        try (WithProperties silence = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS, 1))
        {
            int dseNodes = 0;
            for (int n = 0; n < lostNodes.length; n++)
            {
                String lostNode = lostNodes[n];
                String hostId = UUID.randomUUID().toString();
                long token = n * (Long.MAX_VALUE / 2);
                int netVersion = netVersions[n];
                hostIds[n] = hostId;
                node.runOnInstance(() -> injectRingMember(lostNode, hostId, token, netVersion));
                awaitStatus(node, "DN", address(lostNode));
                if (netVersion == DSE_NET_VERSION)
                    dseNodes++;
            }
            // a DSE 6 node gossiped by a peer is on its record of DSE 6 peers, a node of this software is not
            if (dse6Mode)
                assertThat(node.nodetoolResult("compatstatus").getStdout())
                .contains("DSE 6 peers on record        : " + (dseNodes == 0 ? "0\n" : dseNodes + " ["));
            // the rows of the ranges the lost nodes replicate reach a single node: the other one has fewer before the removal
            for (int i = 0; i < ROWS; i++)
                cluster.coordinator(1).execute("INSERT INTO " + RF2 + ".t (k, v) VALUES (?, ?)", ConsistencyLevel.ONE, i, "v" + i);
            long before = localRows(cluster, 1) + localRows(cluster, 2);
            assertThat(before).isGreaterThanOrEqualTo(ROWS).isLessThan(2L * ROWS);
            Thread.sleep(1500); // the silence of a lost node

            for (int n = 0; n < lostNodes.length; n++)
            {
                String lostNode = lostNodes[n];
                node.nodetoolResult("removenode", hostIds[n]).asserts().success();
                await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                       .until(() -> !node.nodetoolResult("status").getStdout().contains(address(lostNode)));
                assertThat(node.logs().grep(Pattern.quote("Allowing the removal of " + subjects[n] + ", silent for ") +
                                            "[0-9]+ seconds, during the DSE 6 upgrade").getResult()).hasSize(1);
            }
            // both nodes now replicate every row, streamed from the node that had them
            assertThat(localRows(cluster, 1)).isEqualTo(ROWS);
            assertThat(localRows(cluster, 2)).isEqualTo(ROWS);
            String out = node.nodetoolResult("compatstatus").getStdout();
            assertThat(out).contains("Requests refused (upgrade)   : 0\n");
            // the removed nodes no longer keep a start without the mode refused
            if (dse6Mode)
                assertThat(out).contains("DSE 6 peers on record        : 0\n");
        }
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
            assassinateALostNode(cluster.get(1), LOST_DSE_NODE, DSE_NET_VERSION, "the DSE 6 node /" + LOST_DSE_NODE);
        }
    }

    /**
     * Injects on {@code node} a lost node gossiping {@code netVersion}, named {@code subject} in the texts, and a DSE 6
     * node that is alive; removenode of the lost node is refused, assassinate of the live DSE 6 node too, and
     * assassinate of the lost node removes it, with its WARN, and takes it off the record of DSE 6 peers if it was on it.
     */
    static void assassinateALostNode(IInvokableInstance node, String lostNode, int netVersion, String subject) throws Exception
    {
        String hostId = UUID.randomUUID().toString();
        try (WithProperties silence = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS, 1))
        {
            node.runOnInstance(() -> injectRingMember(lostNode, hostId, 0L, netVersion));
            // a DSE 6 node marked alive and never convicted: it never sends a heartbeat, so this node's failure detector,
            // which has no arrival time for it, never convicts it
            node.runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                try
                {
                    Gossiper.instance.initializeNodeUnsafe(InetAddressAndPort.getByName(DSE_NODE), UUID.randomUUID(), DSE_NET_VERSION, 1);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }));
            awaitStatus(node, "DN", address(lostNode));
            // a lost DSE 6 node is on the record of DSE 6 peers, a lost node of this software is not
            String onRecord = "DSE 6 peers on record +: [^\n]*" + Pattern.quote(lostNode);
            if (netVersion == DSE_NET_VERSION)
                assertThat(node.nodetoolResult("compatstatus").getStdout()).containsPattern(onRecord);
            else
                assertThat(node.nodetoolResult("compatstatus").getStdout()).doesNotContainPattern(onRecord);
            Thread.sleep(1500); // the silence of a lost node

            assertRefused(node.nodetoolResult("removenode", hostId),
                          "removenode of " + subject + " lost for good is refused while DSE 6 nodes not known to be lost are in the cluster " +
                          "([/" + DSE_NODE + "])");
            assertRefused(node.nodetoolResult("removenode", hostId), "remove the lost node with nodetool assassinate " + lostNode);
            assertRefused(node.nodetoolResult("assassinate", DSE_NODE),
                          "assassinate is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6, DSE 6 peers: [");

            node.nodetoolResult("assassinate", lostNode).asserts().success();
            await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                   .until(() -> !node.nodetoolResult("status").getStdout().contains(address(lostNode)));
            assertThat(node.logs().grep(Pattern.quote("Allowing the assassination of " + subject + ", silent for ") +
                                        "[0-9]+ seconds, during the DSE 6 upgrade").getResult()).hasSize(1);
            String out = node.nodetoolResult("compatstatus").getStdout();
            assertThat(out).contains("Requests refused (upgrade)   : 3\n");
            assertThat(out).containsPattern("  removenode +2\n");
            assertThat(out).containsPattern("  assassinate +1\n");
            // the assassinated node is not on the record of DSE 6 peers any more
            assertThat(out).doesNotContainPattern(onRecord);
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
            node.runOnInstance(() -> injectRingMember(LOST_DSE_NODE, hostId, 12345L, DSE_NET_VERSION));
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
     * Adds a node gossiping {@code netVersion} (a DSE 6 node's, or this software's) to the gossip state of this instance
     * as a ring member that never answers (its address has no node), marked down as the failure detector marks a node
     * lost for good.
     */
    static void injectRingMember(String address, String hostId, long token, int netVersion)
    {
        try
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
            VersionedValue.VersionedValueFactory values = StorageService.instance.valueFactory;
            Collection<Token> tokens = Collections.singleton(new Murmur3Partitioner.LongToken(token));
            Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
            states.put(ApplicationState.NET_VERSION, values.networkVersion(netVersion));
            states.put(ApplicationState.HOST_ID, values.hostId(UUID.fromString(hostId)));
            states.put(ApplicationState.RELEASE_VERSION, netVersion == DSE_NET_VERSION ? values.releaseVersion("4.0.0.6865") : values.releaseVersion());
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

    static String address(String addressAndPort)
    {
        return addressAndPort.substring(0, addressAndPort.lastIndexOf(':'));
    }

    /** Waits until {@code nodetool status} of {@code node} shows {@code address} (without port) in {@code status}. */
    static void awaitStatus(IInvokableInstance node, String status, String address)
    {
        String line = "(?m)^" + status + " +" + Pattern.quote(address) + "\\b";
        await().atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(node.nodetoolResult("status").getStdout()).containsPattern(line));
    }
}
