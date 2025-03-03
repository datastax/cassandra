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

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Distributed tests for ANN options.
 */
public class ANNOptionsDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    /**
     * Test that ANN options are accepted in clusters with all nodes in DS 11 (although SAI will reject them for now).
     */
    @Test
    public void testANNOptionsWithAllDS11() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            // null indicates that the query should succeed
            testSelectWithAnnOptions(cluster, null);
        }
    }

    /**
     * Test that ANN options are rejected in clusters with all nodes below DS 11.
     */
    @Test
    public void testANNOptionsWithAllDS10() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_10);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            testSelectWithAnnOptions(cluster, "ANN options are not supported in clusters below DS 11.");
        }
    }

    /**
     * Test that ANN options are rejected in clusters with some nodes below DS 11.
     */
    @Test
    public void testANNOptionsWithMixedDS10AndDS11() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                           .start(), RF))
        {
            testSelectWithAnnOptions(cluster, "ANN options are not supported in clusters below DS 11.");
        }
    }

    private static void testSelectWithAnnOptions(Cluster cluster, String expectedErrorMessage)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, n int, v vector<float, 2>)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(v) USING 'StorageAttachedIndex'"));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        String select = withKeyspace("SELECT * FROM %s.t ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 10}");

        for (int i = 1; i <= cluster.size(); i++)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            if (expectedErrorMessage == null)
                coordinator.execute(select, ConsistencyLevel.ONE);
            else
                Assertions.assertThatThrownBy(() -> coordinator.execute(select, ConsistencyLevel.ONE))
                          .hasMessageContaining(expectedErrorMessage);
        }
    }

    /**
     * Injection to set the current version of the first cluster node to DS 10.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, int node)
        {
            if (node > 1)
            {
                // Relies on CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION set to DS_11 or higher
                // set the current verson to DS 10, which does NOT support ANN options
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
}
