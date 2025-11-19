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

package org.apache.cassandra.distributed.test.sai;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.virtual.ColumnIndexesSystemView;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.assertj.core.util.Streams;

import static org.awaitility.Awaitility.await;

public class SAIUtil
{
    private static final long INDEX_BUILD_TIMEOUT_SECONDS = 60;

    /**
     * Waits until all indexes in the given keyspace become queryable.
     * This checks from node1's perspective. If your test queries from multiple nodes,
     * use {@link #waitForIndexQueryableOnAllNodes(Cluster, String)} instead.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     */
    public static void waitForIndexQueryableOnFirstNode(Cluster cluster, String keyspace)
    {
        waitForIndexQueryable(cluster, keyspace, 1);
    }

    /**
     * Waits until given index becomes queryable.
     * This checks from node1's perspective. If your test queries from multiple nodes,
     * use {@link #waitForIndexQueryable(Cluster, String, String, int)} instead.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     * @param index the index name
     */
    public static void waitForIndexQueryableOnFirstNode(Cluster cluster, String keyspace, String index)
    {
        waitForIndexQueryable(cluster, keyspace, index, 1);
    }

    /**
     * Waits until all indexes in the given keyspace become queryable, as verified by the specified coordinator node.
     * This method checks index status from the coordinator's perspective, ensuring that when the coordinator
     * executes queries, it will see the indexes as queryable.
     * <p>
     * Use this method when your test will query from a specific node, to ensure that the coordinator node
     * has received gossip updates about index status.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     * @param coordinatorNode the node number (1-based) that will act as coordinator for queries
     */
    public static void waitForIndexQueryable(Cluster cluster, String keyspace, int coordinatorNode)
    {
        assertGossipEnabled(cluster);
        final List<String> indexes = getIndexes(cluster, keyspace);
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .untilAsserted(() -> assertIndexesQueryable(cluster, keyspace, indexes, coordinatorNode));
    }

    /**
     * Waits until the given index becomes queryable, as verified by the specified coordinator node.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     * @param index the index name
     * @param coordinatorNode the node number (1-based) that will act as coordinator for queries
     */
    public static void waitForIndexQueryable(Cluster cluster, String keyspace, String index, int coordinatorNode)
    {
        assertGossipEnabled(cluster);
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .untilAsserted(() -> assertIndexQueryable(cluster, keyspace, index, coordinatorNode));
    }

    /**
     * Waits until all indexes in the given keyspace become queryable on ALL cluster nodes.
     * This is useful when tests query from multiple coordinators, ensuring that each node
     * has received gossip updates about index status before queries are executed.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     */
    public static void waitForIndexQueryableOnAllNodes(Cluster cluster, String keyspace)
    {
        assertGossipEnabled(cluster);
        final List<String> indexes = getIndexes(cluster, keyspace);

        for (int nodeNum = 1; nodeNum <= cluster.size(); nodeNum++)
        {
            final int node = nodeNum;
            await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertIndexesQueryable(cluster, keyspace, indexes, node));
        }
    }

    private static void assertGossipEnabled(Cluster cluster)
    {
        cluster.stream().forEach(node -> {
            assert node.config().has(Feature.NETWORK) : "Network not enabled on this cluster";
            assert node.config().has(Feature.GOSSIP) : "Gossip not enabled on this cluster";
        });
    }

    /**
     * Checks if index is known to be queryable, by pulling index state from {@link SecondaryIndexManager}
     * on the specified coordinator node. Requires gossip.
     *
     * @param coordinatorNode the node number (1-based) that checks index status
     */
    public static void assertIndexQueryable(Cluster cluster, String keyspace, String index, int coordinatorNode)
    {
        assertIndexesQueryable(cluster, keyspace, Collections.singleton(index), coordinatorNode);
    }

    /**
     * Checks if all indexes are known to be queryable, by pulling index state from {@link SecondaryIndexManager}
     * on the specified coordinator node. Requires gossip.
     *
     * @param coordinatorNode the node number (1-based) that checks index status from its perspective
     */
    private static void assertIndexesQueryable(Cluster cluster, String keyspace, final Iterable<String> indexes, int coordinatorNode)
    {
        IInvokableInstance localNode = cluster.get(coordinatorNode);
        final List<InetAddressAndPort> nodes =
            cluster.stream()
                   .map(node -> nodeAddress(node.broadcastAddress()))
                   .collect(Collectors.toList());

        localNode.runOnInstance(() -> {
            for (String index : indexes)
            {
                for (InetAddressAndPort node : nodes)
                {
                    Index.Status status = IndexStatusManager.instance.getIndexStatus(node, keyspace, index);
                    assert status == Index.Status.BUILD_SUCCEEDED
                        : "Index " + index + " not queryable on node " + node + " (status = " + status + ')';
                }
            }
        });
    }

    private static InetAddressAndPort nodeAddress(InetSocketAddress address)
    {
        return InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
    }

    /**
     * Returns names of the indexes in the keyspace, found on the first node of the cluster.
     */
    public static List<String> getIndexes(Cluster cluster, String keyspace)
    {
        waitForSchemaAgreement(cluster);
        String query = String.format("SELECT index_name FROM system_views.%s WHERE keyspace_name = '%s' ALLOW FILTERING",
                                     ColumnIndexesSystemView.NAME, keyspace);
        SimpleQueryResult result = cluster.get(1).executeInternalWithResult(query);
        return Streams.stream(result)
                      .map(row -> (String) row.get("index_name"))
                      .collect(Collectors.toList());
    }

    public static void waitForSchemaAgreement(Cluster cluster)
    {
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .until(() -> schemaAgrees(cluster));
    }

    /**
     * Returns true if schema agrees on all nodes of the cluster
     */
    public static boolean schemaAgrees(Cluster cluster)
    {
        Set<UUID> versions = cluster.stream()
                                    .map(IInstance::schemaVersion)
                                    .collect(Collectors.toSet());
        return versions.size() == 1;
    }

    /**
     * Checks if an index is known to have failed its build, by pulling index state from {@link SecondaryIndexManager}.
     * Requires gossip.
     */
    public static void assertIndexBuildFailed(IInvokableInstance coordinator, IInvokableInstance replica, String keyspace, String index)
    {
        InetAddressAndPort nodeAddress = nodeAddress(replica.broadcastAddress());

        coordinator.runsOnInstance(() -> {
            Index.Status status = IndexStatusManager.instance.getIndexStatus(nodeAddress, keyspace, index);
            assert status == Index.Status.BUILD_FAILED
                : "Index " + index + " not failed on node " + nodeAddress + " (status = " + status + ')';
        });
    }
}
