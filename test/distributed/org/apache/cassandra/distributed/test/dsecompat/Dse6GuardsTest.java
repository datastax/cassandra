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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.Dse6AccessControlCheck;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS;
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
 * no node can bootstrap or replace, the access-control check refuses a start, and {@code compatstatus} counts the
 * refusals (the removal of a node lost for good is {@link Dse6LostDseNodeTest}'s and
 * {@link Dse6LostNodeOfThisSoftwareTest}'s).
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
     * instances share: every test starts without it and restores it, so that the order of the tests does not matter
     * (the {@code NONE} test must not run with the trie version of the mode).
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
            // the cluster-wide pass ran at the start in the mode (mirrored from CassandraDaemon)
            assertThat(out).contains("DSE access controls ignored  : 0 (found by a LOCAL_QUORUM read)\n");
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

            // the periodic status line and the access-control check were scheduled at the DSE_6 start
            List<String> scheduled = node.logs().grep("The DSE 6 upgrade status will be logged, and the DSE access controls checked again, every 10 minutes").getResult();
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

    @Test
    public void dseAccessControlsRefuseTheStart() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(2, builder -> {}))
        {
            // a DSE row-level grant, as DSE leaves it in system_auth (replicated to one node: the default RF of system_auth)
            cluster.coordinator(1).execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                           "VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})", ConsistencyLevel.ALL);
            int holder = 0;
            for (int node = 1; node <= 2; node++)
                if (cluster.get(node).executeInternal("SELECT role FROM system_auth.role_permissions WHERE role = 'analyst'").length > 0)
                    holder = node;
            assertThat(holder).isNotZero();
            final int replica = holder;
            final int other = 3 - replica;

            // the cluster-wide pass finds the grant from either node, the local pass only on the replica
            for (IInvokableInstance each : new IInvokableInstance[]{ cluster.get(1), cluster.get(2) })
            {
                Throwable quorum = catchThrowable(() -> each.runOnInstance(Dse6GuardsTest::quorumPass));
                assertThat(quorum).as("cluster-wide pass on " + each).isNotNull();
                assertThat(messages(quorum)).contains("(found by a LOCAL_QUORUM read): row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]");
            }
            cluster.get(other).runOnInstance(Dse6GuardsTest::localPass);
            Throwable local = catchThrowable(() -> cluster.get(replica).runOnInstance(Dse6GuardsTest::localPass));
            assertThat(local).isNotNull();
            assertThat(messages(local)).contains("(found by this node's replica): row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]");

            // the replica does not start again in DSE_6 mode while the grant exists
            IInvokableInstance node = cluster.get(replica);
            node.shutdown().get();
            Throwable failure = catchThrowable(node::startup);
            assertThat(failure).isNotNull();
            assertThat(messages(failure)).contains("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access if ignored");
            shutdownIfRunning(node);

            // nothing of the refusal stays behind: started with the override, the grant removed while the node is up, the node
            // restarts without any property
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
            {
                node.startup();
            }
            cluster.coordinator(other).execute("DELETE FROM system_auth.role_permissions WHERE role = 'analyst'", ConsistencyLevel.ALL);
            node.shutdown().get();
            node.startup();
            assertThat(node.nodetoolResult("compatstatus").getStdout()).contains("DSE access controls ignored  : 0 (found by a LOCAL_QUORUM read)\n");
        }
    }

    @Test
    public void tablesRestrictedToRowsRefuseTheStart() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(1, builder -> {}))
        {
            // a table as DSE's RESTRICT ROWS ON <table> USING tenant leaves it (0x74656e616e74 is 'tenant' in UTF-8); the schema
            // is frozen in DSE_6 mode, the property lets the test create it
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.restricted (tenant text PRIMARY KEY, v int) " +
                                                  "WITH extensions = {'DSE_RLACA': 0x74656e616e74, 'other': 0x02}"));
            }
            IInvokableInstance node = cluster.get(1);
            String table = KEYSPACE + ".restricted (tenant column tenant)";
            Throwable local = catchThrowable(() -> node.runOnInstance(Dse6GuardsTest::localPass));
            assertThat(local).isNotNull();
            assertThat(messages(local)).contains("(found by this node's replica): row-level restricted tables in the schema [" + table + "]. " +
                                                 "Roll this node back to DSE, remove them there " +
                                                 "(UNRESTRICT ROWS ON " + KEYSPACE + ".restricted) and upgrade it again");
            Throwable quorum = catchThrowable(() -> node.runOnInstance(Dse6GuardsTest::quorumPass));
            assertThat(quorum).isNotNull();
            assertThat(messages(quorum)).contains("(found by a LOCAL_QUORUM read): row-level restricted tables in the schema [" + table + "]");

            // the periodic check logs it and compatstatus lists it; the other extension key is not reported
            node.runOnInstance(() -> Dse6AccessControlCheck.recheck());
            assertThat(node.logs().grep("The periodic check \\(a LOCAL_QUORUM read\\) found DSE access controls this node does not enforce: " +
                                        "row-level restricted tables in the schema \\[" + KEYSPACE + "\\.restricted \\(tenant column tenant\\)\\]")
                           .getResult()).hasSize(1);
            String out = node.nodetoolResult("compatstatus").getStdout();
            assertThat(out).contains("DSE access controls ignored  : 1 (found by a LOCAL_QUORUM read)\n" +
                                     "  row-level restricted table in the schema " + table + '\n');
            // the first pass lists it in one INFO line
            assertThat(node.logs().grep("INFO .*Tables with schema extensions other than DSE_RLACA, kept and not interpreted by this " +
                                        "software \\(not DSE access controls\\): \\[" + KEYSPACE + "\\.restricted: \\['other'\\]\\]").getResult())
            .hasSize(1);
            assertThat(node.logs().grep("schema extensions").getResult()).noneMatch(line -> line.contains("WARN"));

            // the override drops it knowingly
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
            {
                node.runOnInstance(Dse6GuardsTest::localPass);
            }
            assertThat(node.logs().grep("access opens up accordingly \\(this node's replica\\): row-level restricted tables in the schema").getResult())
            .hasSize(1);

            // the node does not start again in DSE_6 mode while the table is restricted
            node.shutdown().get();
            Throwable failure = catchThrowable(node::startup);
            assertThat(failure).isNotNull();
            assertThat(messages(failure)).contains("row-level restricted tables in the schema [" + table + "]");
            shutdownIfRunning(node);

            // nothing of the refusal stays behind: started with the override, the extension removed while the node is up, the
            // node restarts without any property
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
            {
                node.startup();
            }
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange(withKeyspace("ALTER TABLE %s.restricted WITH extensions = {'other': 0x02}"));
            }
            node.shutdown().get();
            node.startup();
            assertThat(node.nodetoolResult("compatstatus").getStdout()).contains("DSE access controls ignored  : 0 (found by a LOCAL_QUORUM read)\n");
        }
    }

    @Test
    public void aKeyspaceOfAnUnsupportedDseFeatureRefusesTheStart() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(1, builder -> {}))
        {
            IInvokableInstance node = cluster.get(1);
            // the start in DSE_6 mode found no trace, and compatstatus says so
            assertThat(node.logs().grep("INFO .*DSE 6 compatibility mode: no trace of DSE Search, Graph, Analytics, DSEFS, CFS or " +
                                        "Advanced Replication found").getResult()).hasSize(1);
            assertThat(node.nodetoolResult("compatstatus").getStdout()).contains("Non-core DSE traces          : none\n");

            // the keyspace of DSE Advanced Replication, empty, as DSE creates it; the schema is frozen in DSE_6 mode, the
            // property lets the test create it
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange("CREATE KEYSPACE dse_advrep WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            }
            String refusal = "internode_compatibility_mode DSE_6 refused: found traces of DSE features that this software does not " +
                             "support; only DSE core (the Cassandra workload) can be upgraded. DSE Advanced Replication: keyspace " +
                             "dse_advrep. Roll this node back to DSE, remove them there (stop Advanced Replication, migrate what its " +
                             "keyspace holds if it is needed and drop it, empty or not) and upgrade it again";
            Throwable check = catchThrowable(() -> node.runOnInstance(Dse6GuardsTest::nonCoreTracesCheck));
            assertThat(check).isNotNull();
            assertThat(messages(check)).contains(refusal);

            // the node does not start again in DSE_6 mode while the keyspace exists; the override of the entries of
            // nodes/peers does not cover it
            node.shutdown().get();
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEER_WORKLOADS, true))
            {
                Throwable failure = catchThrowable(node::startup);
                assertThat(failure).isNotNull();
                assertThat(messages(failure)).contains(refusal);
            }
            shutdownIfRunning(node);
        }
    }

    @Test
    public void aSearchIndexAndASolrDataDirectoryRefuseTheStart() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(1, builder -> {}))
        {
            IInvokableInstance node = cluster.get(1);
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.searched (k int PRIMARY KEY, v text)"));
            }
            // the row of system_schema.indexes DSE writes for CREATE SEARCH INDEX ON the table (its class and target), and
            // the empty solr.data directory of a Search node, in the first data directory
            node.executeInternal("INSERT INTO system_schema.indexes (keyspace_name, table_name, index_name, kind, options) " +
                                 "VALUES (?, 'searched', 'searched_solr_query_index', 'CUSTOM', " +
                                 "{'class_name': 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex', 'target': 'solr_query'})", KEYSPACE);
            node.flush("system_schema");
            String solrData = node.callOnInstance(() -> {
                File directory = new File(DatabaseDescriptor.getAllDataFileLocations()[0], Dse6NonCoreTraces.SOLR_DATA);
                directory.createDirectoriesIfNotExists();
                return directory.toString();
            });

            // one refusal from the startup check, before the index is instantiated
            node.shutdown().get();
            Throwable failure = catchThrowable(node::startup);
            assertThat(failure).isNotNull();
            assertThat(messages(failure)).contains("internode_compatibility_mode DSE_6 refused: found traces of DSE features that this software " +
                                                   "does not support; only DSE core (the Cassandra workload) can be upgraded. DSE Search: " +
                                                   "index " + KEYSPACE + ".searched.searched_solr_query_index (com.datastax.bdp.search.solr." +
                                                   "Cql3SolrSecondaryIndex), directory " + solrData + ". Roll this node back to DSE, remove " +
                                                   "them there (drop the search indexes (DROP SEARCH INDEX ON <keyspace>.<table>); delete the " +
                                                   "solr.data directories once no search index is left) and upgrade it again");
            assertThat(messages(failure)).doesNotContain("the schema contains the DSE Search index");
            shutdownIfRunning(node);
        }
    }

    /** The startup check of the traces of the DSE features this software does not support, run inside an instance. */
    private static void nonCoreTracesCheck()
    {
        try
        {
            Dse6NonCoreTraces.verifyAtStartup();
        }
        catch (StartupException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Test
    public void noneModeIsUnchanged() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withSubnet(SUBNET).withDynamicPortAllocation(true).withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (id int PRIMARY KEY, v text)"));
            cluster.coordinator(1).execute(withKeyspace("ALTER TABLE %s.t ADD w int"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                                           "VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})", ConsistencyLevel.ALL);
            cluster.get(1).nodetoolResult("upgradesstables", KEYSPACE, "t").asserts().success();
            cluster.get(1).nodetoolResult("refresh", KEYSPACE, "t").asserts().success();
            cluster.get(1).runOnInstance(Dse6GuardsTest::localPass);
            cluster.get(1).runOnInstance(Dse6GuardsTest::quorumPass);
            String out = cluster.get(1).nodetoolResult("compatstatus").getStdout();
            assertThat(out).contains("Internode compatibility mode : NONE");
            assertThat(out).contains("Requests refused (upgrade)   : 0\n");
            assertThat(cluster.get(1).logs().grep("DSE 6 upgrade status").getResult()).isEmpty();
            assertThat(cluster.get(1).logs().grep("DSE access controls").getResult()).isEmpty();

            // the traces of the DSE features this software does not support are not looked for: a node with the keyspace of
            // DSE Advanced Replication and a solr.data directory restarts
            cluster.schemaChange("CREATE KEYSPACE dse_advrep WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.get(1).runOnInstance(() -> new File(DatabaseDescriptor.getAllDataFileLocations()[0], Dse6NonCoreTraces.SOLR_DATA).createDirectoriesIfNotExists());
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            assertThat(cluster.get(1).nodetoolResult("compatstatus").getStdout())
            .contains("Non-core DSE traces          : not checked (internode_compatibility_mode NONE)\n");
            assertThat(cluster.get(1).logs().grep("trace of DSE Search").getResult()).isEmpty();
        }
    }

    /** A node whose start was refused is usually not running any more; one whose start got further is stopped. */
    private static void shutdownIfRunning(IInvokableInstance node) throws Exception
    {
        if (!node.isShutdown())
            node.shutdown().get();
    }

    /** The first pass of the access-control check, run inside an instance; a refusal surfaces as a RuntimeException. */
    private static void localPass()
    {
        try
        {
            Dse6AccessControlCheck.verifyAtStartup();
        }
        catch (StartupException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /** The cluster-wide pass of the access-control check, run inside an instance. */
    private static void quorumPass()
    {
        try
        {
            Dse6AccessControlCheck.verifyAfterJoin();
        }
        catch (StartupException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
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
