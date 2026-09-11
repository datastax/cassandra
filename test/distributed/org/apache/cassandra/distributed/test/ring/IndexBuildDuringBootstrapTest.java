/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.ring;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.test.ByteBuddyUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.assertj.core.api.Assertions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.cassandra.distributed.action.GossipHelper.*;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.count;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.waitForIndexQueryable;

public class IndexBuildDuringBootstrapTest extends TestBaseImpl
{
    private static final String INDEX_NOT_AVAILABLE_MESSAGE = "^Operation failed - received 0 responses" +
                                                              " and 2 failures: INDEX_NOT_AVAILABLE from .+" +
                                                              " INDEX_NOT_AVAILABLE from .+$";
    private long savedMigrationDelay;

    @Before
    public void beforeTest()
    {
        // MigrationCoordinator schedules schema pull requests immediatelly when the node is just starting up, otherwise
        // the first pull request is sent in 60 seconds. Whether we are starting up or not is detected by examining
        // the node up-time and if it is lower than MIGRATION_DELAY, we consider the server is starting up.
        // When we are running multiple test cases in the class, where each starts a node but in the same JVM, the
        // up-time will be more or less relevant only for the first test. In order to enforce the startup-like behaviour
        // for each test case, the MIGRATION_DELAY time is adjusted accordingly
        savedMigrationDelay = CassandraRelevantProperties.MIGRATION_DELAY.getLong();
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(ManagementFactory.getRuntimeMXBean().getUptime() + savedMigrationDelay);
    }

    @After
    public void afterTest()
    {
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(savedMigrationDelay);
    }

    private static final String TABLE = "tbl";
    private static final String INDEX_NAME = "idx_v1";
    private static final String SCAN_QUERY = "SELECT * FROM " + KEYSPACE + '.' + TABLE;
    private static final String INDEX_QUERY = "SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE v = 50";
    private static final String ALLOW_FILTERING_QUERY = "SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE v = 50 ALLOW FILTERING";

    // Test Case 1: We initiate index create before bootstrap and finish it during bootstrap.
    @Test
    public void indexBuildDuringBootstrapTestV1() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withInstanceInitializer(ByteBuddyUtils.IndexBuildBlocker::blockIndexBuildingOnNode3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            // Start index build before bootstrap
            cluster.schemaChange("CREATE CUSTOM INDEX " + INDEX_NAME + " ON " + KEYSPACE + '.' + TABLE + "(v) USING 'StorageAttachedIndex'");

            // Prepare the new node but don't start bootstrap yet
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);

            // Start bootstrap
            withProperty("cassandra.join_ring", false,
                         () -> newInstance.startup(cluster));
            cluster.forEach(statusToBootstrap(newInstance));

            // Verify queries work during bootstrap and index build; though no data has been streamed yet
            Assert.assertEquals(0, newInstance.executeInternal(SCAN_QUERY).length);
            Assertions.assertThatThrownBy(() -> newInstance.executeInternal(INDEX_QUERY))
                      .hasMessageMatching("The secondary index 'idx_v1' is not yet available");
            Assertions.assertThatThrownBy(() -> newInstance.executeInternal(ALLOW_FILTERING_QUERY))
                      .hasMessageMatching("The secondary index 'idx_v1' is not yet available");

            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.QUORUM))
                      .hasMessageMatching(INDEX_NOT_AVAILABLE_MESSAGE);
            // Not allowed due to index not available on 2 nodes - 1 and 2 on main; expected and ok
            //cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.QUORUM);

            waitForIndexQueryable(cluster, KEYSPACE);

            // Verify queries after index build completion and before bootstrap completion
            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.QUORUM);
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assert.assertEquals(0, newInstance.executeInternal(SCAN_QUERY).length);
            Assert.assertEquals(0, newInstance.executeInternal(INDEX_QUERY).length);
            Assert.assertEquals(0, newInstance.executeInternal(ALLOW_FILTERING_QUERY).length);

            // Complete bootstrap
            cluster.run(asList(pullSchemaFrom(cluster.get(1)),
                               bootstrap()),
                        newInstance.config().num());

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state", 100L, e.getValue().longValue());

            // Verify final state
            assertQueryResults(newInstance, INDEX_QUERY);
            assertQueryResults(newInstance, ALLOW_FILTERING_QUERY);
            // Below fails with "Cannot achieve consistency level ALL"
            //cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            // Below 2 fail with Cannot achieve consistency level ALL
            // cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL);
            //cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL);

            cluster.coordinator(3).execute(SCAN_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(INDEX_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL);
        }
    }

    // Test Case 2: We initiate index create during bootstrap and finish it during bootstrap
    @Test
    public void indexBuildDuringBootstrapTestV2() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withInstanceInitializer(ByteBuddyUtils.IndexBuildBlocker::blockIndexBuildingOnNode3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            // Prepare the new node but don't start bootstrap yet
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);

            // Start bootstrap
            withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster));
            cluster.forEach(statusToBootstrap(newInstance));

            // Start index build during bootstrap
            cluster.schemaChange("CREATE CUSTOM INDEX " + INDEX_NAME + " ON " + KEYSPACE + '.' + TABLE + "(v) USING 'StorageAttachedIndex'");

            // Verify queries work during bootstrap and index build; though no data has been streamed yet
            Assert.assertEquals(0, newInstance.executeInternal(SCAN_QUERY).length);
            Assertions.assertThatThrownBy(() -> newInstance.executeInternal(INDEX_QUERY))
                      .hasMessageMatching("The secondary index 'idx_v1' is not yet available");
            // org.apache.cassandra.index.IndexNotAvailableException: The secondary index 'idx_v1' is not yet available on main
            //newInstance.executeInternal(ALLOW_FILTERING_QUERY);

            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");

            cluster.coordinator(3).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            Assertions.assertThatThrownBy(() -> cluster.coordinator(3).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(3).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");

            waitForIndexQueryable(cluster, KEYSPACE);

            // Verify queries after index build completion and before bootstrap completion
            Assert.assertEquals(0, newInstance.executeInternal(SCAN_QUERY).length);
            Assert.assertEquals(0, newInstance.executeInternal(INDEX_QUERY).length);
            Assert.assertEquals(0, newInstance.executeInternal(ALLOW_FILTERING_QUERY).length);

            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.QUORUM);

            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");

            cluster.coordinator(3).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            Assertions.assertThatThrownBy(() -> cluster.coordinator(3).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(3).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");

            // Complete bootstrap
            cluster.run(asList(pullSchemaFrom(cluster.get(1)), bootstrap()), newInstance.config().num());

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state", 100L, e.getValue().longValue());

            // Verify final state
            assertQueryResults(newInstance, INDEX_QUERY);
            assertQueryResults(newInstance, ALLOW_FILTERING_QUERY);
            // Below fails with "Cannot achieve consistency level ALL"
            //cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            // Below 2 fail with Cannot achieve consistency level ALL
            // cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL);
            // cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL);

            cluster.coordinator(3).execute(SCAN_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(INDEX_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL);
        }
    }

    // Test Case 3: We initiate index create during bootstrap and finish it after bootstrap
    @Test
    public void indexBuildDuringBootstrapTestV3() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withInstanceInitializer(ByteBuddyUtils.IndexBuildBlocker::blockIndexBuildingOnNode3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            populate(cluster, 0, 100);

            // Prepare the new node but don't start bootstrap yet
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);

            // Start bootstrap
            withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster));
            cluster.forEach(statusToBootstrap(newInstance));

            // Start index build during bootstrap
            cluster.schemaChange("CREATE CUSTOM INDEX " + INDEX_NAME + " ON " + KEYSPACE + '.' + TABLE + "(v) USING 'StorageAttachedIndex'");

            // Verify queries work during bootstrap and index build; though no data has been streamed yet
            Assert.assertEquals(0, newInstance.executeInternal(SCAN_QUERY).length);
            Assertions.assertThatThrownBy(() -> newInstance.executeInternal(INDEX_QUERY))
                      .hasMessageMatching("The secondary index 'idx_v1' is not yet available");
            // The secondary index 'idx_v1' is not yet available on main
            // Assert.assertEquals(0, newInstance.executeInternal(ALLOW_FILTERING_QUERY).length);

            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            // operation failed - received 1 responses and 1 failures: INDEX_NOT_AVAILABLE from /127.0.0.1:7012 on main
            // cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.QUORUM);
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");

            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL))
                      .hasMessageMatching("Cannot achieve consistency level ALL");

            // Complete bootstrap
            cluster.run(asList(pullSchemaFrom(cluster.get(1)), bootstrap()), newInstance.config().num());

            waitForIndexQueryable(cluster, KEYSPACE);

            for (Map.Entry<Integer, Long> e : count(cluster).entrySet())
                Assert.assertEquals("Node " + e.getKey() + " has incorrect row state", 100L, e.getValue().longValue());

            // Verify final state
            assertQueryResults(newInstance, INDEX_QUERY);
            assertQueryResults(newInstance, ALLOW_FILTERING_QUERY);
            // Below fails with "Cannot achieve consistency level ALL"
            //cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(SCAN_QUERY, ConsistencyLevel.QUORUM);
            // Below 2 fail with Cannot achieve consistency level ALL
            // cluster.coordinator(1).execute(INDEX_QUERY, ConsistencyLevel.ALL);
            // cluster.coordinator(1).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL);

            cluster.coordinator(3).execute(SCAN_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(INDEX_QUERY, ConsistencyLevel.ALL);
            cluster.coordinator(3).execute(ALLOW_FILTERING_QUERY, ConsistencyLevel.ALL);
        }
    }

    private void assertQueryResults(IInvokableInstance instance, String query)
    {
        Object[][] results = instance.executeInternal(query);
        Assert.assertEquals(1, results.length);
    }
}
