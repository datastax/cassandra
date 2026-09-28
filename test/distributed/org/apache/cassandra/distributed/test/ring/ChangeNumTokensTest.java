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

package org.apache.cassandra.distributed.test.ring;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.BROADCAST_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Documents what happens when an operator tries to reduce {@code num_tokens} (e.g. 256 -> 16) on a live cluster.
 * <ul>
 *     <li>an already bootstrapped node refuses to restart with a different {@code num_tokens};</li>
 *     <li>a replacement node takes over all the tokens of the replaced node, so a replacement with a different
 *     {@code num_tokens} is refused;</li>
 *     <li>a node bootstrapped with fewer tokens into the same datacenter owns a share of data proportional to its
 *     token count, even with the replication aware token allocator;</li>
 *     <li>the supported path is to build a new datacenter with the new {@code num_tokens}, rebuild it from the old
 *     one, move the replicas and decommission the old datacenter.</li>
 * </ul>
 * See docs/operations/reduce-num-tokens-runbook.md.
 */
public class ChangeNumTokensTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ChangeNumTokensTest.class);

    private static final int OLD_NUM_TOKENS = 256;
    private static final int NEW_NUM_TOKENS = 16;
    private static final int DC1_NODES = 3;
    private static final int ROWS = 1000;

    /**
     * Legacy 256 vnode clusters were built with random token allocation: give the initial nodes random tokens
     * (seeded, so that the test is reproducible). Nodes added later get their tokens from the test.
     */
    private static TokenSupplier oldTokens(int nodes)
    {
        Random random = new Random(42);
        Set<Long> used = new HashSet<>();
        List<List<String>> tokens = new ArrayList<>();
        for (int n = 0; n < nodes; n++)
        {
            List<String> nodeTokens = new ArrayList<>(OLD_NUM_TOKENS);
            while (nodeTokens.size() < OLD_NUM_TOKENS)
            {
                long token = random.nextLong();
                if (token != Long.MIN_VALUE && used.add(token))
                    nodeTokens.add(Long.toString(token));
            }
            tokens.add(nodeTokens);
        }
        return node -> node <= nodes ? tokens.get(node - 1) : Collections.singletonList("0");
    }

    /**
     * Starts {@code DC1_NODES} nodes with 256 tokens in dc1. Nodes added later go to dc1 for the first
     * {@code dc1ExtraNodes}, then to dc2.
     */
    private static Cluster.Builder buildCluster(int dc1ExtraNodes, int dc2Nodes)
    {
        int dc1Nodes = DC1_NODES + dc1ExtraNodes;
        return Cluster.build(DC1_NODES)
                      .withConfig(c -> c.with(GOSSIP, NETWORK))
                      .withTokenCount(OLD_NUM_TOKENS)
                      .withTokenSupplier(oldTokens(DC1_NODES))
                      .withNodeIdTopology(networkTopology(dc1Nodes + dc2Nodes, n -> n <= dc1Nodes ? dcAndRack("dc1", "rack1")
                                                                                                    : dcAndRack("dc2", "rack1")));
    }

    private static void withNewNumTokens(InstanceConfig config)
    {
        config.remove("initial_token");
        config.set("num_tokens", NEW_NUM_TOKENS);
        config.set("allocate_tokens_for_local_replication_factor", 3);
    }

    private static void fastRingProperties(WithProperties properties)
    {
        properties.set(BROADCAST_INTERVAL_MS, Long.toString(TimeUnit.SECONDS.toMillis(30)));
        properties.set(RING_DELAY, Long.toString(TimeUnit.SECONDS.toMillis(10)));
        properties.set(BOOTSTRAP_SCHEMA_DELAY_MS, TimeUnit.SECONDS.toMillis(10));
    }

    /**
     * Restarts the node expecting the num_tokens check to reject the startup; the half started instance is then
     * shut down so that it releases its resources (ports, threads).
     */
    private static void assertRestartRejected(IInvokableInstance node, int savedTokens, int configuredTokens) throws Exception
    {
        node.config().set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
        assertThatThrownBy(node::startup)
        .hasMessageContaining("Cannot change the number of tokens from " + savedTokens + " to " + configuredTokens);
        node.shutdown().get();
        node.config().set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, true);
    }

    private static int localTokenCount(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> StorageService.instance.getTokens().size());
    }

    private static void writeRows(Cluster cluster, int coordinator)
    {
        for (int i = 0; i < ROWS; i++)
            cluster.coordinator(coordinator).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"),
                                                     ConsistencyLevel.LOCAL_QUORUM, i, i);
    }

    private static long localRowCount(IInvokableInstance instance)
    {
        return (long) instance.executeInternal(withKeyspace("SELECT count(*) FROM %s.tbl"))[0][0];
    }

    @Test
    public void testRestartWithDifferentNumTokensFails() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(GOSSIP, NETWORK))
                                      .withTokenCount(OLD_NUM_TOKENS)
                                      .withTokenSupplier(oldTokens(1))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);
            assertThat(localTokenCount(node)).isEqualTo(OLD_NUM_TOKENS);

            node.shutdown().get();
            withNewNumTokens((InstanceConfig) node.config());
            assertRestartRejected(node, OLD_NUM_TOKENS, NEW_NUM_TOKENS);

            // going back to the original value is the only way to start the node again
            node.config().set("num_tokens", OLD_NUM_TOKENS);
            node.startup();
            assertThat(localTokenCount(node)).isEqualTo(OLD_NUM_TOKENS);
        }
    }

    @Test
    public void testReplacementWithDifferentNumTokensIsRefused() throws Throwable
    {
        try (Cluster cluster = buildCluster(1, 0).start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            writeRows(cluster, 1);

            IInvokableInstance toReplace = cluster.get(DC1_NODES);
            ClusterUtils.stopUnchecked(toReplace);

            IInvokableInstance replacement = ClusterUtils.addInstance(cluster, "dc1", "rack1", c -> {
                c.set("auto_bootstrap", true);
                withNewNumTokens((InstanceConfig) c);
            });
            InetSocketAddress replaced = toReplace.config().broadcastAddress();
            String replaceAddress = replaced.getAddress().getHostAddress() + ':' + replaced.getPort();

            // a replacement takes over every token of the replaced node, so a different num_tokens is refused upfront
            replacement.config().set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
            assertThatThrownBy(() -> ClusterUtils.start(replacement, properties -> {
                fastRingProperties(properties);
                properties.set(REPLACE_ADDRESS_FIRST_BOOT, replaceAddress);
            })).hasMessageContaining("owns " + OLD_NUM_TOKENS + " tokens, with a node configured with num_tokens: " + NEW_NUM_TOKENS);
            replacement.shutdown().get();
            replacement.config().set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, true);

            // with the same number of tokens the replacement goes through
            replacement.config().set("num_tokens", OLD_NUM_TOKENS);
            ClusterUtils.start(replacement, properties -> {
                fastRingProperties(properties);
                properties.set(REPLACE_ADDRESS_FIRST_BOOT, replaceAddress);
            });
            assertThat(localTokenCount(replacement)).isEqualTo(OLD_NUM_TOKENS);
            assertThat(localRowCount(replacement)).isEqualTo(ROWS);
        }
    }

    @Test
    public void testBootstrapWithFewerTokensInSameDatacenterIsUnbalanced() throws Throwable
    {
        try (Cluster cluster = buildCluster(1, 0).start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            writeRows(cluster, 1);

            IInvokableInstance newNode = ClusterUtils.addInstance(cluster, "dc1", "rack1", c -> {
                c.set("auto_bootstrap", true);
                withNewNumTokens((InstanceConfig) c);
            });
            ClusterUtils.start(newNode, ChangeNumTokensTest::fastRingProperties);
            assertThat(localTokenCount(newNode)).isEqualTo(NEW_NUM_TOKENS);
            for (int n = 1; n <= DC1_NODES; n++)
                ClusterUtils.awaitRingState(cluster.get(n), newNode, "Normal");
            long newNodeRows = localRowCount(newNode);

            String ks = KEYSPACE;
            // what nodetool status reports as "Owns (effective)"
            Map<String, Float> ownership = cluster.get(1).callOnInstance(() -> StorageService.instance.effectiveOwnershipWithPort(ks));
            logger.info("{} nodes x {} tokens + 1 node x {} tokens, RF=3: effective ownership {}, rows on the new node {}/{}",
                        DC1_NODES, OLD_NUM_TOKENS, NEW_NUM_TOKENS, ownership, newNodeRows, ROWS);

            String newNodeAddress = newNode.config().broadcastAddress().getAddress().getHostAddress();
            float newNodeOwnership = ownership.entrySet().stream()
                                               .filter(e -> e.getKey().contains(newNodeAddress))
                                               .findFirst().orElseThrow(AssertionError::new).getValue();

            // A balanced 4 node ring with RF=3 would give 75% to each node. The allocator targets the same ownership
            // for every token, so with 16 tokens out of 784 the new node owns ~3*16/784 ~= 6% and the old ones ~98%.
            assertThat(newNodeOwnership).isLessThan(0.15f);
            assertThat(newNodeRows).isLessThan(ROWS * 15 / 100);
            ownership.forEach((node, owns) -> {
                if (!node.contains(newNodeAddress))
                    assertThat(owns).isGreaterThan(0.9f);
            });
        }
    }

    @Test
    public void testMigrateToFewerTokensThroughNewDatacenter() throws Throwable
    {
        int dc2Nodes = 3;
        try (Cluster cluster = buildCluster(0, dc2Nodes).start())
        {
            // 0. make sure every keyspace that must survive uses NetworkTopologyStrategy
            for (String systemKs : new String[]{ "system_auth", "system_distributed", "system_traces" })
                cluster.schemaChange("ALTER KEYSPACE " + systemKs + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}");
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            writeRows(cluster, 1);

            // 1. add the new datacenter with the new num_tokens, without streaming
            for (int i = 0; i < dc2Nodes; i++)
            {
                IInvokableInstance node = ClusterUtils.addInstance(cluster, "dc2", "rack1", c -> {
                    c.set("auto_bootstrap", false);
                    withNewNumTokens((InstanceConfig) c);
                });
                ClusterUtils.start(node, ChangeNumTokensTest::fastRingProperties);
                assertThat(localTokenCount(node)).isEqualTo(NEW_NUM_TOKENS);
            }
            int firstDc2 = DC1_NODES + 1;
            int lastDc2 = DC1_NODES + dc2Nodes;

            // 2. replicate every keyspace to the new datacenter
            for (String systemKs : new String[]{ "system_auth", "system_distributed", "system_traces" })
                cluster.schemaChange("ALTER KEYSPACE " + systemKs + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}");
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}"));

            // 3. stream the existing data to the new datacenter
            for (int n = firstDc2; n <= lastDc2; n++)
            {
                cluster.get(n).nodetoolResult("rebuild", "dc1").asserts().success();
                assertThat(localRowCount(cluster.get(n))).isEqualTo(ROWS);
            }

            // 4. clients move to dc2 (LOCAL_* consistency, dc2 local DC); remove dc1 from the replication settings.
            // system_auth must keep every datacenter that still has nodes, so it is changed after decommissioning dc1.
            for (String systemKs : new String[]{ "system_distributed", "system_traces" })
                cluster.schemaChange("ALTER KEYSPACE " + systemKs + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3}", false, cluster.get(firstDc2));
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3}"), false, cluster.get(firstDc2));

            // 5. decommission the old datacenter; --force is required because system_auth has RF=3 in dc1
            for (int n = 1; n <= DC1_NODES; n++)
            {
                cluster.get(n).nodetoolResult("decommission", "--force").asserts().success();
                cluster.get(n).shutdown().get();
            }
            cluster.schemaChange("ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3}", true, cluster.get(firstDc2));

            // 6. data is all there, every node has the new number of tokens and survives a restart
            Object[][] rows = cluster.coordinator(firstDc2).execute(withKeyspace("SELECT count(*) FROM %s.tbl"), ConsistencyLevel.ALL);
            assertThat(rows[0][0]).isEqualTo((long) ROWS);
            IInvokableInstance restarted = cluster.get(lastDc2);
            restarted.shutdown().get();
            restarted.startup();
            for (int n = firstDc2; n <= lastDc2; n++)
            {
                assertThat(localTokenCount(cluster.get(n))).isEqualTo(NEW_NUM_TOKENS);
                assertThat(localRowCount(cluster.get(n))).isEqualTo(ROWS);
            }
            assertThat(cluster.get(firstDc2).callOnInstance(() -> StorageService.instance.getTokenMetadata().sortedTokens().size()))
            .isEqualTo(dc2Nodes * NEW_NUM_TOKENS);

            // 7. the cluster keeps working at LOCAL_QUORUM
            writeRows(cluster, firstDc2);
        }
    }
}
