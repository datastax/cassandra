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
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The operational guards of the DSE 6 upgrade on a cluster of this software in {@code DSE_6} mode: every refused
 * operation answers with its error, the schema freeze applies to both statement paths and is lifted by the property,
 * no node can bootstrap or replace, and {@code compatstatus} counts the refusals (the removal of a node lost for good is
 * {@link Dse6LostDseNodeTest}'s and {@link Dse6LostNodeOfThisSoftwareTest}'s).
 * <p>
 * A node may only run in {@code DSE_6} mode once it has joined a ring (a former DSE node has), so the nodes
 * are started without the mode first, on the storage settings DSE reads (trie {@code bb}, numeric generations,
 * {@code HCD_1}), and restarted in the mode: the same restart as an upgraded DSE node.
 */
public class Dse6GuardsTest extends TestBaseImpl
{
    private static final String REFUSED = "is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6)";
    private static final String SCHEMA_FROZEN = "schema changes are frozen during the DSE 6 upgrade; run them after the flag is dropped";
    /** A DSE node that exists only in the gossip state injected on node 1. */
    static final String DSE_NODE = "127.0.10.99:7012";
    /** A subnet of its own: in-JVM clusters of other suites on 127.0.0.x must not reach these nodes. */
    static final int SUBNET = 10;

    /**
     * The {@code DSE_6} mode sets {@code cassandra.trie_index_format_version} as a system property, which the in-JVM
     * instances share: every test starts without it and restores it, so that the order of the tests does not matter.
     */
    private WithProperties trieIndexFormatVersion;

    /**
     * A node in {@code DSE_6} mode refuses to start unless the SAI on-disk version is pinned; these nodes store no DSE
     * release, so the pin is taken as set: {@code aa} for every test, restored after it.
     */
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

    private static void dseStorage(IInstanceConfig config)
    {
        config.with(Feature.NETWORK, Feature.GOSSIP)
              .set("storage_compatibility_mode", "HCD_1")
              .set("uuid_sstable_identifiers_enabled", false);
    }

    /** Starts the nodes without the mode on DSE-readable storage settings, creates the test keyspace, then restarts each node in DSE_6 mode. */
    static Cluster formerDseNodes(int nodes, Consumer<Cluster.Builder> more) throws Throwable
    {
        Cluster.Builder builder = Cluster.build(nodes).withSubnet(SUBNET).withDynamicPortAllocation(true).withConfig(Dse6GuardsTest::dseStorage);
        more.accept(builder);
        Cluster cluster = builder.createWithoutStarting();
        try
        {
            try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                cluster.startup();
                init(cluster);
            }
            for (int node = 1; node <= nodes; node++)
                restartInDse6Mode(cluster.get(node));
            return cluster;
        }
        catch (Throwable t)
        {
            cluster.close();
            throw t;
        }
    }

    private static void restartInDse6Mode(IInvokableInstance node) throws Throwable
    {
        node.shutdown().get();
        node.config().set("internode_compatibility_mode", "DSE_6");
        node.startup();
    }

    @Test
    public void operationsAndSchemaChangesAreRefusedInDse6Mode() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(2, builder -> {}))
        {
            // the schema is frozen: the property is needed to get a table at all
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (id int PRIMARY KEY, v text)"));
                cluster.coordinator(1).execute(withKeyspace("CREATE TABLE %s.t2 (id int PRIMARY KEY)"), ConsistencyLevel.ALL);
            }
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (id, v) VALUES (?, ?)"), ConsistencyLevel.ALL, i, "v" + i);
            cluster.get(1).flush(KEYSPACE);

            // the freeze, through the client path and the internal one
            assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace("ALTER TABLE %s.t ADD w int"), ConsistencyLevel.ALL))
            .hasMessageContaining(SCHEMA_FROZEN);
            Throwable internal = catchThrowable(() -> cluster.schemaChange(withKeyspace("DROP TABLE %s.t2")));
            assertThat(internal).isNotNull();
            assertThat(messages(internal)).contains(SCHEMA_FROZEN);
            // DML and reads are not frozen
            Object[][] rows = cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.t WHERE id = 7"), ConsistencyLevel.ALL);
            assertThat(rows).hasNumberOfRows(1);
            assertThat(rows[0][0]).isEqualTo("v7");

            IInvokableInstance node = cluster.get(1);
            String hostId2 = cluster.get(2).callOnInstance(() -> StorageService.instance.getLocalHostId());
            assertRefused(node.nodetoolResult("decommission"), "decommission " + REFUSED);
            assertRefused(node.nodetoolResult("move", "0"), "move " + REFUSED);
            assertRefused(node.nodetoolResult("removenode", hostId2), "removenode " + REFUSED);
            assertRefused(node.nodetoolResult("removenode", UUID.randomUUID().toString()), "removenode " + REFUSED);
            assertRefused(node.nodetoolResult("removenode", "force"), "removenode force " + REFUSED);
            assertRefused(node.nodetoolResult("assassinate", cluster.get(2).broadcastAddress().getAddress().getHostAddress()), "assassinate " + REFUSED);
            assertRefused(node.nodetoolResult("rebuild"), "rebuild " + REFUSED);
            // nodetool bootstrap resume is disabled in this build: the JMX operation behind it is called directly
            Throwable resume = catchThrowable(() -> node.runOnInstance(() -> StorageService.instance.resumeBootstrap()));
            assertThat(resume).isNotNull();
            assertThat(messages(resume)).contains("bootstrap resume " + REFUSED);
            assertRefused(node.nodetoolResult("upgradesstables", KEYSPACE, "t"), "upgradesstables " + REFUSED);
            assertRefused(node.nodetoolResult("recompress_sstables", KEYSPACE, "t"), "recompress_sstables " + REFUSED);
            assertRefused(node.nodetoolResult("import", KEYSPACE, "t", "/nonexistent/dse6-import"), "import " + REFUSED);
            assertRefused(node.nodetoolResult("refresh", KEYSPACE, "t"), "refresh " + REFUSED);
            assertRefused(node.nodetoolResult("repair", KEYSPACE), "repair " + REFUSED);
            assertRefused(node.nodetoolResult("resetlocalschema"), "resetlocalschema " + REFUSED);
            // paxos_variant through its JMX operation (no nodetool command sets it)
            Throwable paxos = catchThrowable(() -> node.runOnInstance(() -> StorageProxy.instance.setPaxosVariant("v2")));
            assertThat(paxos).isNotNull();
            assertThat(messages(paxos)).contains("changing paxos_variant to v2 " + REFUSED);
            // and to the v1 variant without linearizable reads: the mode accepts exactly v1
            String nonLinearizableV1 = "v1_without_linearizable_reads_or_rejected_writes";
            Throwable nonLinearizable = catchThrowable(() -> node.runOnInstance(() -> StorageProxy.instance.setPaxosVariant(nonLinearizableV1)));
            assertThat(nonLinearizable).isNotNull();
            assertThat(messages(nonLinearizable)).contains("changing paxos_variant to " + nonLinearizableV1 + " " + REFUSED);
            assertThat(node.callOnInstance(() -> StorageProxy.instance.getPaxosVariant())).isEqualTo("v1");

            // the cluster still works: every node is up, the data is still readable
            node.nodetoolResult("status").asserts().success();
            rows = cluster.coordinator(1).execute(withKeyspace("SELECT count(*) FROM %s.t"), ConsistencyLevel.ALL);
            assertThat(rows[0][0]).isEqualTo(10L);

            NodeToolResult status = node.nodetoolResult("compatstatus");
            status.asserts().success();
            String out = status.getStdout();
            assertThat(out).contains("Internode compatibility mode : DSE_6");
            // the peer of this software is not a DSE peer
            assertThat(out).contains("(0 speaking the pre-4.0 dialect)");
            assertThat(out).contains("DSE 6 peers on record        : 0\n");
            assertThat(out).contains("Requests refused (upgrade)   : 18\n");
            assertThat(out).containsPattern("  changing paxos_variant to v2 +1\n");
            assertThat(out).containsPattern("  changing paxos_variant to v1_without_linearizable_reads_or_rejected_writes +1\n");
            assertThat(out).containsPattern("  assassinate +1\n");
            assertThat(out).containsPattern("  bootstrap resume +1\n");
            assertThat(out).containsPattern("  decommission +1\n");
            assertThat(out).containsPattern("  import +1\n");
            assertThat(out).containsPattern("  move +1\n");
            assertThat(out).containsPattern("  rebuild +1\n");
            assertThat(out).containsPattern("  recompress_sstables +1\n");
            assertThat(out).containsPattern("  refresh +1\n");
            assertThat(out).containsPattern("  removenode +2\n");
            assertThat(out).containsPattern("  removenode force +1\n");
            assertThat(out).containsPattern("  repair +1\n");
            assertThat(out).containsPattern("  resetlocalschema +1\n");
            assertThat(out).containsPattern("  schema change +2\n");
            assertThat(out).containsPattern("  upgradesstables +1\n");

            // the periodic status line was scheduled at the DSE_6 start
            List<String> scheduled = node.logs().grep("The DSE 6 upgrade status will be logged every 10 minutes").getResult();
            assertThat(scheduled).isNotEmpty();
        }
    }

    @Test
    public void noNodeCanBootstrapOrReplaceInDse6Mode() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(1, builder -> builder.withTokenSupplier(evenlyDistributedTokens(4, 1))
                                                                   .withNodeIdTopology(singleDcNetworkTopology(4, "dc0", "rack0"))))
        {
            // a new node that would stream (auto_bootstrap) and one that would just take tokens (a seed-like node with
            // auto_bootstrap: false): neither has completed a bootstrap, both are refused
            for (boolean autoBootstrap : new boolean[]{ true, false })
            {
                IInstanceConfig config = cluster.newInstanceConfig();
                config.set("auto_bootstrap", autoBootstrap).set("internode_compatibility_mode", "DSE_6");
                IInvokableInstance newNode = cluster.bootstrap(config);
                Throwable failure = catchThrowable(() -> newNode.startup(cluster));
                assertThat(failure).as("auto_bootstrap " + autoBootstrap).isNotNull();
                assertThat(messages(failure)).contains("bootstrapping a new node (bootstrap state NEEDS_BOOTSTRAP) " + REFUSED);
                shutdownIfRunning(newNode);
            }

            // a replacement of node 1 (its address, on a fresh directory) is refused too
            IInstanceConfig replacement = cluster.newInstanceConfig();
            replacement.set("auto_bootstrap", true).set("internode_compatibility_mode", "DSE_6");
            IInvokableInstance replacing = cluster.bootstrap(replacement);
            String replaced = cluster.get(1).broadcastAddress().getAddress().getHostAddress() + ':' + cluster.get(1).broadcastAddress().getPort();
            Throwable failure;
            try (WithProperties properties = new WithProperties().set(REPLACE_ADDRESS, replaced))
            {
                failure = catchThrowable(() -> replacing.startup(cluster));
            }
            assertThat(failure).isNotNull();
            assertThat(messages(failure)).contains("replacing a node " + REFUSED)
                                         .contains("a replacement cannot start in internode_compatibility_mode DSE_6; remove the node lost " +
                                                   "for good instead, a DSE 6 node or a node of this software (nodetool removenode once every " +
                                                   "DSE 6 node still in the cluster is lost, nodetool assassinate otherwise), and add a new node " +
                                                   "once the upgrade is finished");
            shutdownIfRunning(replacing);

            cluster.get(1).nodetoolResult("status").asserts().success();
        }
    }

    @Test
    public void aNodeWithoutTheModeCannotJoinWhileDseNodesAreInGossip() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(1, builder -> builder.withTokenSupplier(evenlyDistributedTokens(4, 1))
                                                                   .withNodeIdTopology(singleDcNetworkTopology(4, "dc0", "rack0"))))
        {
            // node 1 gossips a DSE node, as an upgraded node does while DSE nodes remain (DSE's own NET_VERSION)
            cluster.get(1).runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                try
                {
                    Gossiper.instance.initializeNodeUnsafe(InetAddressAndPort.getByName(DSE_NODE), UUID.randomUUID(), 1536, 1);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }));

            // node 1 (DSE_6 mode) names the DSE node known only from its gossip state, in its refusals and in compatstatus
            assertRefused(cluster.get(1).nodetoolResult("move", "0"), "DSE 6 peers: [/" + DSE_NODE + "]");
            assertThat(cluster.get(1).nodetoolResult("compatstatus").getStdout()).containsPattern("DSE peers +: 1\n  " + DSE_NODE.replace(".", "\\.") + " +release ");

            // a new node without the mode, with and without auto_bootstrap: refused by the shadow round of its start
            for (boolean autoBootstrap : new boolean[]{ true, false })
            {
                IInstanceConfig config = cluster.newInstanceConfig();
                config.set("auto_bootstrap", autoBootstrap);
                IInvokableInstance newNode = cluster.bootstrap(config);
                Throwable failure = catchThrowable(() -> newNode.startup(cluster));
                assertThat(failure).as("auto_bootstrap " + autoBootstrap).isNotNull();
                assertThat(messages(failure)).contains("bootstrapping a new node is refused while the DSE 6 upgrade is in progress " +
                                                       "(DSE 6 nodes in gossip: [/" + DSE_NODE + "]); finish upgrading (or remove) the DSE 6 nodes first");
                shutdownIfRunning(newNode);
            }

            // a replacement without the mode: refused by the shadow round of the replacement
            IInstanceConfig replacement = cluster.newInstanceConfig();
            replacement.set("auto_bootstrap", true);
            IInvokableInstance replacing = cluster.bootstrap(replacement);
            String replaced = cluster.get(1).broadcastAddress().getAddress().getHostAddress() + ':' + cluster.get(1).broadcastAddress().getPort();
            Throwable failure;
            try (WithProperties properties = new WithProperties().set(REPLACE_ADDRESS, replaced))
            {
                failure = catchThrowable(() -> replacing.startup(cluster));
            }
            assertThat(failure).isNotNull();
            assertThat(messages(failure)).contains("replacing a node is refused while the DSE 6 upgrade is in progress (DSE 6 nodes in gossip: [/" +
                                                   DSE_NODE + "])");
            shutdownIfRunning(replacing);
        }
    }

    /** A node whose start was refused is usually not running any more; one whose start got further is stopped. */
    private static void shutdownIfRunning(IInvokableInstance node) throws Exception
    {
        if (!node.isShutdown())
            node.shutdown().get();
    }

    static void assertRefused(NodeToolResult result, String message)
    {
        result.asserts().failure();
        String stderr = String.valueOf(result.getStderr());
        String error = result.getError() == null ? "" : messages(result.getError());
        assertThat(stderr + "\n" + error).as(String.join(" ", result.getCommandAndArgs())).contains(message);
    }

    /** The messages of a throwable and its causes, one per line. */
    private static String messages(Throwable t)
    {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = t; cause != null; cause = cause.getCause())
            messages.append(cause.getMessage()).append('\n');
        return messages.toString();
    }
}
