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

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.isUpgraded;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.localPeers;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.recordRoutes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.remoteNodes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.route;
import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The additional writes of a blocking read repair across two data centers, between HCD 1.2 nodes at messaging
 * version 11 (standing in for DSE 6, which sends them too) and nodes of this software in {@code DSE_6} mode, in the
 * layout of {@link TwoDataCenters}, three replicas in each data center. A {@code QUORUM} read (four replicas of six)
 * contacts the coordinator's data center and one replica of the other; when the repair of a stale contacted replica is
 * not acknowledged in time, the coordinator sends the merged repair to the replicas it did not contact, all in the
 * other data center, and counts their acknowledgements instead. Through a coordinator of each version in each data
 * center, the uncontacted replicas include a node of the other version every time a DSE 6 stand-in coordinates in
 * {@code datacenter1}, or an upgraded node in {@code datacenter2}.
 */
public class MixedModeMultiDcReadRepairTest extends DseDialectTestBase
{
    private static final String KS = "ks_rr";
    private static final String TABLE = KS + ".t";

    @Test
    public void additionalRepairWritesToUncontactedReplicasOfTheOtherDataCenter() throws Throwable
    {
        // no dynamic snitch: a read contacts the replicas by proximity, the coordinator's data center first
        TwoDataCenters.testCase(config -> config.set("dynamic_snitch", false))
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " + networkTopology(3, 3));
                          // no speculative read, so that the read contacts four replicas only; the repair write speculates after 100ms
                          cluster.schemaChange("CREATE TABLE " + TABLE + " (k int, c int, v int, PRIMARY KEY (k, c)) " +
                                               "WITH speculative_retry = 'NONE' AND additional_write_policy = '100ms'");
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          // a coordinator of each version in each data center: 2 and 6 run HCD 1.2, 1 and 4 this software
                          for (int coordinator : new int[]{ 2, 1, 6, 4 })
                          {
                              Set<Integer> additional = additionalWrites(cluster, coordinator, 100 * coordinator);
                              // two of the three nodes of the other data center: for node 2 an upgraded one is always among
                              // them (4 or 5), for node 4 an HCD 1.2 one (2 or 3)
                              if (coordinator == 2 || coordinator == 4)
                                  assertThat(additional).as("additional writes of node %s", coordinator)
                                                        .anyMatch(node -> isUpgraded(node) != isUpgraded(coordinator));
                          }
                      })
                      .run();
    }

    /**
     * A row only the coordinator holds, read at {@code QUORUM} with the repair to one stale replica of its data center
     * lost: the read completes on the acknowledgements of the additional writes, which reached exactly the replicas the
     * read did not contact, all in the other data center. Returns those replicas.
     */
    private static Set<Integer> additionalWrites(UpgradeableCluster cluster, int coordinator, int k)
    {
        int stale = localPeers(coordinator).stream().min(Integer::compare).orElseThrow(AssertionError::new);
        cluster.get(coordinator).executeInternal("INSERT INTO " + TABLE + " (k, c, v) VALUES (?, 1, 1)", k);

        Set<String> reads = ConcurrentHashMap.newKeySet();
        Set<String> repairs = ConcurrentHashMap.newKeySet();
        IMessageFilters.Filter readRecorder = recordRoutes(cluster, READ_REQ, reads);
        IMessageFilters.Filter repairRecorder = recordRoutes(cluster, READ_REPAIR_REQ, repairs);
        IMessageFilters.Filter lostRepair = cluster.filters().outbound().verbs(READ_REPAIR_REQ.id).from(coordinator).to(stale).drop();
        try
        {
            assertRows(cluster.coordinator(coordinator).execute("SELECT c, v FROM " + TABLE + " WHERE k = ?", QUORUM, k), row(1, 1));
        }
        finally
        {
            lostRepair.off();
            readRecorder.off();
            repairRecorder.off();
        }

        Set<Integer> uncontacted = new TreeSet<>();
        for (int node = 1; node <= cluster.size(); node++)
            if (node != coordinator && !reads.contains(route(coordinator, node)))
                uncontacted.add(node);
        assertThat(uncontacted).as("replicas the read of node %s did not contact", coordinator)
                               .hasSize(2)
                               .isSubsetOf(remoteNodes(coordinator));
        for (int node : uncontacted)
        {
            assertThat(repairs).as("additional writes of node %s", coordinator).contains(route(coordinator, node));
            // the read returned on the first acknowledgement it needed: the other additional write may still be applying
            awaitAsserted(() -> assertRows(cluster.get(node).executeInternal("SELECT c, v FROM " + TABLE + " WHERE k = ?", k), row(1, 1)));
        }
        // the lost repair never reached the stale replica: the additional writes made up for it
        assertRows(cluster.get(stale).executeInternal("SELECT c, v FROM " + TABLE + " WHERE k = ?", k));
        return uncontacted;
    }
}
