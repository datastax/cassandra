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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_QUARANTINE_DELAY;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertInRing;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getDirectories;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokenMetadataTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.gms.Gossiper.Props.DISABLE_THREAD_VALIDATION;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertFalse;

public class HostReplacementTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HostReplacementTest.class);

    static
    {
        // Gossip has a notiion of quarantine, which is used to remove "fat clients" and "gossip only members"
        // from the ring if not updated recently (recently is defined by this config).
        // The reason for setting to 0 is to make sure even under such an aggressive environment, we do NOT remove
        // nodes from the peers table
        GOSSIPER_QUARANTINE_DELAY.setInt(0);
    }

    /**
     * Attempt to do a host replacement on a down host
     */
    @Test
    public void replaceDownedHost() throws IOException
    {
        // start with 2 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);

            setupCluster(cluster);

            // collect rows to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            stopUnchecked(nodeToRemove);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove, props -> {
                // since we have a downed host there might be a schema version which is old show up but
                // can't be fetched since the host is down...
                props.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
            });

            // wait till the replacing node is in the ring
            awaitRingJoin(seed, replacingNode);
            awaitRingJoin(replacingNode, seed);

            // make sure all nodes are healthy
            awaitRingHealthy(seed);

            assertRingIs(seed, seed, replacingNode);
            logger.info("Current ring is {}", assertRingIs(replacingNode, seed, replacingNode));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
    }

    /**
     * Attempt to do a host replacement on a alive host
     */
    @Test
    public void replaceAliveHost() throws IOException
    {
        // start with 2 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);

            setupCluster(cluster);

            // collect rows to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            // now create a new node to replace the other node
            Assertions.assertThatThrownBy(() -> replaceHostAndStart(cluster, nodeToRemove))
                      .as("Startup of instance should have failed as you can not replace a alive node")
                      .hasMessageContaining("Cannot replace a live node")
                      .isInstanceOf(UnsupportedOperationException.class);

            // make sure all nodes are healthy
            awaitRingHealthy(seed);

            assertRingIs(seed, seed, nodeToRemove);
            logger.info("Current ring is {}", assertRingIs(nodeToRemove, seed, nodeToRemove));

            validateRows(seed.coordinator(), expectedState);
            validateRows(nodeToRemove.coordinator(), expectedState);
        }
    }

    /**
     * If the seed goes down, then another node, once the seed comes back, make sure host replacements still work.
     */
    @Test
    public void seedGoesDownBeforeDownHost() throws IOException
    {
        // start with 3 nodes, stop both nodes, start the seed, host replace the down node)
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 4 ? 2 : node))
                                      .start())
        {
            // call early as this can't be touched on a down node
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance nodeToStayAlive = cluster.get(3);

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            List<String> beforeCrashTokens = getTokenMetadataTokens(seed);

            // shutdown the seed, then the node to remove
            stopUnchecked(seed);
            stopUnchecked(nodeToRemove);

            // restart the seed
            seed.startup();

            // make sure the node to remove is still in the ring
            assertInRing(seed, nodeToRemove);

            // make sure node1 still has node2's tokens
            List<String> currentTokens = getTokenMetadataTokens(seed);
            Assertions.assertThat(currentTokens)
                      .as("Tokens no longer match after restarting")
                      .isEqualTo(beforeCrashTokens);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove);

            List<IInvokableInstance> expectedRing = Arrays.asList(seed, replacingNode, nodeToStayAlive);

            // wait till the replacing node is in the ring
            awaitRingJoin(seed, replacingNode);
            awaitRingJoin(replacingNode, seed);
            awaitRingJoin(nodeToStayAlive, replacingNode);

            // make sure all nodes are healthy
            logger.info("Current ring is {}", awaitRingHealthy(seed));

            expectedRing.forEach(i -> assertRingIs(i, expectedRing));

            validateRows(seed.coordinator(), expectedState);
            validateRows(replacingNode.coordinator(), expectedState);
        }
    }

    /**
     * Make sure that a node stuck in hibernate state due to failed replacement can retry the replacement procedure and succeed.
     */
    @Test
    public void retryingFailedReplaceWithNodeInHibernateState() throws IOException
    {
        try (WithProperties properties = new WithProperties())
        {
            properties.setProperty(DISABLE_THREAD_VALIDATION, "true");

            // given a two node cluster with one seed
            TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
            try (Cluster cluster = Cluster.build(2)
                                          .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                            .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, true))
                                          .withTokenSupplier(node -> even.token(node == 3 ? 2 : node))
                                          .start() )
            {
                IInvokableInstance seed = cluster.get(1);
                IInvokableInstance nodeToReplace = cluster.get(2);

                setupCluster(cluster);
                SimpleQueryResult expectedState = nodeToReplace.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

                // when
                // stop the node to replace
                stopUnchecked(nodeToReplace);
                // wipe the node to replace
                getDirectories(nodeToReplace).forEach(FileUtils::deleteRecursive);

                String toReplaceAddress = nodeToReplace.config().broadcastAddress().getAddress().getHostAddress();
                // set hibernate status for the node to replace on seed
                seed.runOnInstance(putInHibernation(toReplaceAddress));

                // we need to fake a new host id
                ((InstanceConfig) nodeToReplace.config()).setHostId(UUID.randomUUID());
                // enable autoboostrap
                nodeToReplace.config().set("auto_bootstrap", true);

                // first replacement will fail as the node was announced as hibernated and no-one can contact it as startup
                assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
                    ClusterUtils.start(nodeToReplace, props -> {
                        // set the replacement address
                        props.setProperty("cassandra.replace_address", toReplaceAddress);
                    });
                }).withMessageContaining("Unable to contact any seeds");

                // then
                // retrying replacement will succeed as the node announced itself as shutdown before killing itself
                ClusterUtils.start(nodeToReplace, props -> {
                    // set the replacement address
                    props.setProperty("cassandra.replace_address", toReplaceAddress);
                });
                assertFalse("replaces node should be up", nodeToReplace.isShutdown());

                // the data after replacement should be consistent
                awaitRingJoin(seed, nodeToReplace);
                awaitRingJoin(nodeToReplace, seed);

                validateRows(seed.coordinator(), expectedState);
                validateRows(nodeToReplace.coordinator(), expectedState);
            }
        }
    }

    private static IIsolatedExecutor.SerializableRunnable putInHibernation(String address)
    {
        return () -> {
            InetAddressAndPort endpoint;
            try
            {
                endpoint = InetAddressAndPort.getByName(address);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            VersionedValue newStatus = StorageService.instance.valueFactory.hibernate(true);
            epState.addApplicationState(ApplicationState.STATUS, newStatus);
            epState.addApplicationState(ApplicationState.STATUS_WITH_PORT, newStatus);
            Gossiper.instance.handleMajorStateChange(endpoint, epState);
        };
    }

    static void setupCluster(Cluster cluster)
    {
        fixDistributedSchemas(cluster);
        init(cluster);

        populate(cluster);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    static void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    static void validateRows(ICoordinator coordinator, SimpleQueryResult expected)
    {
        expected.reset();
        SimpleQueryResult rows = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
        AssertUtils.assertRows(rows, expected);
    }
}
