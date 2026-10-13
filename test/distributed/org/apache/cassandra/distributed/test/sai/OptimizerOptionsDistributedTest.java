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

package org.apache.cassandra.distributed.test.sai;

import org.junit.After;
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
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Distributed tests for {@link org.apache.cassandra.db.filter.OptimizerOptions}, verifying how
 * {@code WITH optimizer_options = {...}} behaves in clusters with different messaging versions.
 */
public class OptimizerOptionsDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;
    private static final String UNSUPPORTED_MESSAGE = "SAI optimizer options are not supported in clusters below DS 20.";

    @After
    public void restoreMessagingVersion()
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.clearValue();
    }

    /**
     * Test that optimizer options are accepted in clusters with all nodes in DS 20.
     */
    @Test
    public void testOptimizerOptionsWithAllDS20() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_20);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(),
                                    RF))
        {
            // null indicates that the query should succeed
            testSelectWithOptimizerOptions(cluster, null);
        }
    }

    /**
     * Test that optimizer options are rejected in clusters with all nodes below DS 20.
     */
    @Test
    public void testOptimizerOptionsWithAllDS12() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(),
                                    RF))
        {
            testSelectWithOptimizerOptions(cluster, UNSUPPORTED_MESSAGE);
        }
    }

    /**
     * Test that optimizer options are rejected in clusters with some nodes below DS 20.
     */
    @Test
    public void testOptimizerOptionsWithMixedDS12AndDS20() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_20;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                           .start(),
                                    RF))
        {
            testSelectWithOptimizerOptions(cluster, UNSUPPORTED_MESSAGE);
        }
    }

    private static void testSelectWithOptimizerOptions(Cluster cluster, String expectedErrorMessage) throws Throwable
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(v) USING 'StorageAttachedIndex'"));
        // Wait for index to be queryable on all nodes since test queries from all coordinators
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (0, 1)"), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (1, 1)"), ConsistencyLevel.ALL);
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (2, 2)"), ConsistencyLevel.ALL);

        String select = withKeyspace("SELECT k FROM %s.t WHERE v = 1");
        Object[][] expectedRows = new Object[][]{ row(1), row(0) };

        beforeAndAfterFlush(cluster, KEYSPACE, () -> {
            assertSelect(cluster, expectedErrorMessage,
                         select + " WITH optimizer_options = {'intersection_clause_limit': '3'}", expectedRows);
            assertSelect(cluster, expectedErrorMessage,
                         select + " WITH optimizer_options = {'query_optimization_level': '0', 'use_term_statistics': 'false'}",
                         expectedRows);
        });
    }

    private static void assertSelect(Cluster cluster, String expectedErrorMessage, String select, Object[]... expectedRows)
    {
        for (int i = 1; i <= cluster.size(); i++)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            if (expectedErrorMessage == null)
                assertRows(coordinator.execute(select, ConsistencyLevel.ONE), expectedRows);
            else
                Assertions.assertThatThrownBy(() -> coordinator.execute(select, ConsistencyLevel.ONE))
                          .hasMessageContaining(expectedErrorMessage);
        }
    }

    /**
     * Injection to set the current version of the first cluster node to DS 12.
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
            return MessagingService.VERSION_DS_12;
        }
    }
}
