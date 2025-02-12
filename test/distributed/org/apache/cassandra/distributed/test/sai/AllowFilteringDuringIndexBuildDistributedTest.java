/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.distributed.test.sai;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ReflectionUtils;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Distributed tests for ANN options.
 */
public class AllowFilteringDuringIndexBuildDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    /**
     * Test that ALLOW FILTERING during index builds is accepted in clusters with all nodes in DS 11.
     */
    @Test
    public void testAllowFilteringDuringIndexBuildWithAllDS11() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, null);
        }
    }

    /**
     * Test that ALLOW FILTERING during index builds is rejected in clusters with all nodes below DS 11.
     */
    @Test
    public void testAllowFilteringDuringIndexBuildWithAllDS10() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_10);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster,
                                                            "^Operation failed - received 0 responses" +
                                                            " and 2 failures: INDEX_NOT_AVAILABLE from .+, " +
                                                            "INDEX_NOT_AVAILABLE from .+$");
        }
    }

    /**
     * Test that ALLOW FILTERING during index builds are rejected in clusters with some nodes below DS 11.
     */
    @Test
    public void testAllowFIlteringDuringIndxBuildWithMixedDS10AndDS11() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(AllowFilteringDuringIndexBuildDistributedTest.BB::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                           .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, SecondaryIndexManager.REQUIRES_HIGHER_MESSAGING_VERSION);
        }
    }

    private static void testSelectWithAllowFilteringDuringIndexBuilding(Cluster cluster, String expectedErrorMessage)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, n int, v vector<float, 2>)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(n) USING 'StorageAttachedIndex'"));

        for (int i = 1; i <= cluster.size(); i++)
            markIndexBuilding(cluster.get(i), KEYSPACE, "t", "t_n_idx");

        for (int i = 1; i <= cluster.size(); i++)
            for (int j = 1; j <= cluster.size(); j++)
                waitForIndexingStatus(cluster.get(i), KEYSPACE, "t_n_idx", cluster.get(j), Index.Status.FULL_REBUILD_STARTED);

        String select = withKeyspace("SELECT * FROM %s.t WHERE n = 1 ALLOW FILTERING");

        for (int i = 1; i <= cluster.size(); i++)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            if (expectedErrorMessage == null)
                coordinator.execute(select, ConsistencyLevel.ONE);
            else
                Assertions.assertThatThrownBy(() -> coordinator.execute(select, ConsistencyLevel.ONE))
                          .hasMessageMatching(expectedErrorMessage);
        }
    }

    /**
     * Injection to set the current version of the first cluster node to DS 10.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, int node)
        {
            if (node == 1)
            {
                new ByteBuddy().rebase(MessagingService.class)
                               .method(named("currentVersion"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static int currentVersion()
        {
            return MessagingService.VERSION_DS_10;
        }
        }

    private static void markIndexBuilding(IInvokableInstance node, String keyspace, String table, String indexName)
    {
        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Schema.instance.getKeyspaceInstance(keyspace)
                                                       .getColumnFamilyStore(table)
                                        .indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.markIndexesBuilding(Collections.singleton(index), true, false);
        });
    }

    private static void waitForIndexingStatus(IInvokableInstance node, String keyspace, String index, IInvokableInstance replica, Index.Status status)
    {
        InetAddressAndPort replicaAddressAndPort = getFullAddress(replica);
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> node.callOnInstance(() -> getIndexStatus(keyspace, index, replicaAddressAndPort) == status));
    }

    private static InetAddressAndPort getFullAddress(IInvokableInstance node)
    {
        InetAddress address = node.broadcastAddress().getAddress();
        int port = node.callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().port);
        return InetAddressAndPort.getByAddressOverrideDefaults(address, port);
    }

    private static Index.Status getIndexStatus(String keyspaceName, String indexName, InetAddressAndPort replica)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            return Index.Status.UNKNOWN;

        TableMetadata table = keyspace.findIndexedTable(indexName).orElse(null);
        if (table == null)
            return Index.Status.UNKNOWN;

        SecondaryIndexManager indexManager = Keyspace.openAndGetStore(table).indexManager;

        return indexManager.getIndexStatus(replica, keyspaceName, indexName);
    }
}
