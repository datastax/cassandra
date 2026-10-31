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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The speculative write of a blocking read repair sent by a node of this software in {@code DSE_6} mode to an HCD 1.2
 * replica at messaging version 11, below the lowest version this software otherwise knows. Three nodes, RF 3, node 1
 * upgraded first, then node 2, node 3 staying on HCD 1.2. After each upgrade the upgraded node coordinates a QUORUM read
 * of a row only it holds, with the other contacted replica stale and the repair mutation to that replica lost: the
 * coordinator then sends the merged repair to node 3, the replica it did not contact, and the read completes with
 * node 3's acknowledgement.
 */
public class MixedModeSpeculativeReadRepairTest extends DseDialectTestBase
{
    private static final int HCD = 3;
    private static final String KS = "ks_srr";
    private static final String TABLE = KS + ".t";
    private static final String SPECULATED_WRITE = "org.apache.cassandra.metrics.ReadRepair.SpeculatedWrite";

    @Test
    public void speculativeRepairWriteToHcdReplica() throws Throwable
    {
        // no dynamic snitch: a QUORUM read coordinated by node 1 or 2 contacts the coordinator, then the replicas in
        // ring order, so for a partition whose replicas are nodes 1, 2, 3 in that order node 3 is never contacted. The
        // harness turns the configuration key check off; were the key ignored, node 3 could be contacted and the read
        // would not speculate, which the assertions on the metric and on the stale replica catch
        dse6TestCase(3, config -> config.set("dynamic_snitch", false))
        .nodesToUpgrade(1, 2)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
            // no speculative read, so that the read contacts two replicas only; the repair write speculates after 100ms
            cluster.schemaChange("CREATE TABLE " + TABLE + " (k int, c int, v int, PRIMARY KEY (k, c)) " +
                                 "WITH speculative_retry = 'NONE' AND additional_write_policy = '100ms'");
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            assertPeersAtDialect(cluster, upgraded, cluster.size() - upgraded);
            int coordinator = upgraded;
            int stale = coordinator == 1 ? 2 : 1;
            int k = keyReplicatedInNodeOrder(cluster, coordinator, upgraded * 1_000);

            cluster.get(coordinator).executeInternal("INSERT INTO " + TABLE + " (k, c, v) VALUES (?, 1, 1)", k);
            long speculatedBefore = speculatedWrites(cluster, coordinator);
            cluster.filters().outbound().verbs(READ_REPAIR_REQ.id).from(coordinator).to(stale).drop();
            try
            {
                assertRows(cluster.coordinator(coordinator).execute("SELECT c, v FROM " + TABLE + " WHERE k = ?", QUORUM, k), row(1, 1));
            }
            finally
            {
                cluster.filters().reset();
            }
            assertThat(speculatedWrites(cluster, coordinator)).isGreaterThan(speculatedBefore);
            // node 3 received the speculative repair at version 11, the stale replica never received its repair
            assertRows(cluster.get(HCD).executeInternal("SELECT c, v FROM " + TABLE + " WHERE k = ?", k), row(1, 1));
            assertRows(cluster.get(stale).executeInternal("SELECT c, v FROM " + TABLE + " WHERE k = ?", k));
        })
        .run();
    }

    /** The first key from {@code from} whose replicas, as {@code node} sees them, are nodes 1, 2 and 3 in that order. */
    private static int keyReplicatedInNodeOrder(UpgradeableCluster cluster, int node, int from)
    {
        List<String> expected = new ArrayList<>();
        for (int n = 1; n <= cluster.size(); n++)
            expected.add(cluster.get(n).broadcastAddress().getAddress().getHostAddress());
        for (int k = from; k < from + 1_000; k++)
        {
            NodeToolResult result = cluster.get(node).nodetoolResult("getendpoints", KS, "t", String.valueOf(k));
            result.asserts().success();
            List<String> endpoints = new ArrayList<>();
            for (String line : result.getStdout().split("\n"))
                if (!line.trim().isEmpty())
                    endpoints.add(line.trim());
            if (endpoints.equals(expected))
                return k;
        }
        throw new AssertionError("no key in [" + from + ", " + (from + 1_000) + ") is replicated on nodes 1, 2, 3 in that order");
    }

    /** ReadRepairMetrics.speculatedWrite of the node: 0 when the meter is missing, which the callers' increase check catches. */
    private static long speculatedWrites(UpgradeableCluster cluster, int node)
    {
        return cluster.get(node).metrics().getCounter(SPECULATED_WRITE);
    }
}
