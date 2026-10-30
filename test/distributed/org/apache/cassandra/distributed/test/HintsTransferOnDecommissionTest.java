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

package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.DecommissionHook;
import org.apache.cassandra.service.StorageService;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.action.GossipHelper.decommission;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * The hints a decommissioning node holds must be handed over to another node, whatever the state of the nodes
 * they are for (HCD-473).
 *
 * Topology of every test: 4 nodes without vnodes, a keyspace with RF 2, node 4 down and node 1 leaving. Node 1's
 * ranges of the test keyspace stream to nodes 2 and 3 only, so node 4 can only get the rows node 1 coordinated
 * through the hints node 1 hands over. Node 4 is a new replica of system_distributed (RF 3), but that keyspace is
 * empty here, so the stream session to the down node 4 has no task and completes without connecting.
 */
public class HintsTransferOnDecommissionTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS = 128;

    /**
     * Transferring the hints must not depend on the liveness of the node they are for, only on the liveness of
     * the transfer target: otherwise every decommission of a coordinator holding hints for a down (or left) node
     * fails with "Failed to transfer all hints to ...".
     */
    @Test
    public void shouldTransferHintsForDownNode() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            setUpWithNode4Down(cluster);
            // Write data using the leaving node as the coordinator...
            for (int i = 0; i < ROWS; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + TABLE + " (key, value) VALUES (?, ?)"),
                                               ConsistencyLevel.ONE, i, i);

            // ...and verify that it has accumulated hints intended for node 4, which is down.
            await().atMost(60, SECONDS).until(() -> cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount()) > 0);
            long node4Rows = rowsReplicatedBy(cluster, 1, 4, 0, ROWS);

            // Decommission node 1: its hints for node 4 must be handed over to node 2 or 3...
            cluster.run(decommission(), 1);
            await().atMost(60, SECONDS).pollDelay(1, SECONDS).until(() -> endpointsKnownTo(cluster.get(2)) == 3);
            assertNoHintsLeftOn(cluster.get(1));
            for (int i = 0; i < ROWS; i++)
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT key, value FROM %s." + TABLE + " WHERE key = ?"),
                                                          ConsistencyLevel.ONE, i),
                           row(i, i));

            // ...which deliver them once node 4 is back.
            assertRowsReachNode4(cluster, node4Rows);
        }
    }

    /**
     * The leaving node keeps coordinating writes after it transferred its hints, until it shuts down its client
     * servers, decommission hooks included. The hints written meanwhile for a down replica must be handed over too,
     * not left on the node that is going away.
     */
    @Test
    public void shouldTransferHintsWrittenAfterLeavingTheRing() throws Exception
    {
        try (Cluster cluster = startCluster())
        {
            setUpWithNode4Down(cluster);
            IInvokableInstance leaving = cluster.get(1);
            leaving.runOnInstance(() -> StorageService.instance.registerDecommissionHook(new WriteHook()));

            cluster.run(decommission(), 1);
            await().atMost(60, SECONDS).pollDelay(1, SECONDS).until(() -> endpointsKnownTo(cluster.get(2)) == 3);
            assertNoHintsLeftOn(leaving);

            // the ring the hook wrote in: node 1 had left it already
            long node4Rows = rowsReplicatedBy(cluster, 2, 4, 0, ROWS);
            assertRowsReachNode4(cluster, node4Rows);
        }
    }

    public static class WriteHook implements DecommissionHook
    {
        @Override
        public String name()
        {
            return "write-after-leaving";
        }

        @Override
        public void onDecommission()
        {
            for (int i = 0; i < ROWS; i++)
                QueryProcessor.process(String.format("INSERT INTO %s.%s (key, value) VALUES (%d, %d)", KEYSPACE, TABLE, i, i),
                                       org.apache.cassandra.db.ConsistencyLevel.ONE);
        }
    }

    private Cluster startCluster() throws Exception
    {
        return builder().withNodes(4)
                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                        .withoutVNodes()
                        .start();
    }

    private static void setUpWithNode4Down(Cluster cluster) throws Exception
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (key int PRIMARY KEY, value int)"));
        cluster.get(4).shutdown().get();
        assertEquals(4, endpointsKnownTo(cluster.get(2)));
    }

    /**
     * @return how many of the keys in [from, to) the given node replicates, in the ring as seen by the observer
     */
    private static long rowsReplicatedBy(Cluster cluster, int observer, int node, int from, int to)
    {
        InetSocketAddress address = cluster.get(node).broadcastAddress();
        long rows = cluster.get(observer).callOnInstance(() -> {
            InetAddressAndPort endpoint = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            return IntStream.range(from, to)
                            .filter(i -> StorageService.instance.getNaturalReplicasForToken(KEYSPACE, TABLE, String.valueOf(i))
                                                                .endpoints().contains(endpoint))
                            .count();
        });
        assertThat(rows).isGreaterThan(0);
        return rows;
    }

    private static void assertRowsReachNode4(Cluster cluster, long expected)
    {
        cluster.get(4).startup();
        // startup, gossip, a dispatch trigger tick and the throttled delivery; CI is about twice as slow as a laptop
        await().atMost(120, SECONDS).pollDelay(3, SECONDS).until(() -> countOnNode4(cluster) >= expected);
        assertEquals(expected, countOnNode4(cluster));
    }

    private static long countOnNode4(Cluster cluster)
    {
        return (Long) cluster.get(4).executeInternal(withKeyspace("SELECT COUNT(*) FROM %s." + TABLE))[0][0];
    }

    private static int endpointsKnownTo(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> StorageService.instance.getTokenMetadata().getAllEndpoints().size());
    }

    /** The decommissioned node must not keep hints behind: they would be lost when its process stops. */
    private static void assertNoHintsLeftOn(IInvokableInstance decommissioned)
    {
        assertEquals(0, (int) decommissioned.callOnInstance(() -> HintsService.instance.getTotalFilesNum()));
        assertEquals(0L, (long) decommissioned.callOnInstance(() -> StorageMetrics.totalHintsInProgress.getCount()));
    }
}
