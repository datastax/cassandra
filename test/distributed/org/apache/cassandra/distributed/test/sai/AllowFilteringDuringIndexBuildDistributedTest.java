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

import org.apache.cassandra.distributed.test.index.IndexTestBase;

import org.apache.cassandra.distributed.test.ByteBuddyUtils;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Distributed tests for ALLOW FILTERING during index build.
 */
public class AllowFilteringDuringIndexBuildDistributedTest extends IndexTestBase
{
    private static final String INDEX_NOT_AVAILABLE_MESSAGE = "^Operation failed - received 0 responses" +
                                                              " and 2 failures: INDEX_NOT_AVAILABLE from .+" +
                                                              " INDEX_NOT_AVAILABLE from .+$";
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    @Test
    public void testAllowFilteringDuringInitialIndexBuildWithAllDS11() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, null, true, false);
        }
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithAllDS11NewCF() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, false, true);
        }
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithAllDS11ExistingCF() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, false, false);
        }
    }

    @Test
    public void testAllowFilteringDuringInitialIndexBuildWithAllDS10() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_10);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, true, false);
        }
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithAllDS10NewCF() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_10);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, false, true);
        }
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithAllDS10ExistingCF() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_10);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, false, false);
        }
    }

    @Test
    public void testAllowFilteringDuringInitialIndexBuildWithMixedDS10AndDS11() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withInstanceInitializer(ByteBuddyUtils.MessagingVersionSetter::setDS10OnNode1)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, true, false);
        }
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithMixedDS10AndDS11NewCF() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withInstanceInitializer(ByteBuddyUtils.MessagingVersionSetter::setDS10OnNode1)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, false, true);
        }
    }

    @Test
    public void testAllowFilteringDuringIndexRebuildWithMixedDS10AndDS11ExistingCF() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withInstanceInitializer(ByteBuddyUtils.MessagingVersionSetter::setDS10OnNode1)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, INDEX_NOT_AVAILABLE_MESSAGE, false, false);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitialBuildWithNewCFShouldFail() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .start(), RF))
        {
            testSelectWithAllowFilteringDuringIndexBuilding(cluster, null, true, true);
        }
    }

    private static void testSelectWithAllowFilteringDuringIndexBuilding(Cluster cluster, 
                                                                      String expectedErrorMessage, 
                                                                      boolean isInitialBuild,
                                                                      boolean isNewCF)
    {
        if (isInitialBuild && isNewCF) {
            throw new IllegalArgumentException("Initial build cannot happen with a new CF");
        }

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, n int, v vector<float, 2>)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(n) USING 'StorageAttachedIndex'"));

        Index.Status expectedStatus = isInitialBuild ? Index.Status.INITIAL_BUILD_STARTED : Index.Status.FULL_REBUILD_STARTED;

        for (int i = 1; i <= cluster.size(); i++)
            markIndexBuilding(cluster.get(i), KEYSPACE, "t", "t_n_idx", isInitialBuild, isNewCF);

        for (int i = 1; i <= cluster.size(); i++)
            for (int j = 1; j <= cluster.size(); j++)
                waitForIndexingStatus(cluster.get(i), KEYSPACE, "t_n_idx", cluster.get(j), expectedStatus);

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
}
