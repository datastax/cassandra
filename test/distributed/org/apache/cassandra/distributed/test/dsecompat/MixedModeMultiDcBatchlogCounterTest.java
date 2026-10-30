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
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC2;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.dataCenter;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.isUpgraded;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.localPeers;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.recordRoutes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.remoteNodes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.route;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.target;
import static org.apache.cassandra.net.Verb.BATCH_STORE_REQ;
import static org.apache.cassandra.net.Verb.COUNTER_MUTATION_REQ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Logged batches and counter writes across two data centers, between HCD 1.2 nodes at messaging version 11 (standing in
 * for DSE 6) and nodes of this software in {@code DSE_6} mode, in the layout of {@link TwoDataCenters}: one rack per
 * data center, three replicas in each. A logged batch stores its batchlog on the two other nodes of the coordinator's
 * data center (both products choose them in the local data center, with no regard to their version), at least one of
 * them of the other version for every coordinator in this layout, then writes to both data centers and removes the
 * batchlog. A counter write goes to a leader picked at random among the live replicas of the coordinator's data center,
 * which applies it and replicates the result to both data centers; with no replica in the coordinator's data center,
 * the closest replica of the other one leads, and a data-center-local level is unavailable.
 */
public class MixedModeMultiDcBatchlogCounterTest extends DseDialectTestBase
{
    private static final String KS = "ks_bc";
    /** Replicas in datacenter2 only: the counter leader of a coordinator of datacenter1 is remote. */
    private static final String KS_DC2 = "ks_dc2";
    private static final int KEYS = 10;
    /**
     * Counter writes per coordinator and level: 30 per level, 60 per coordinator, each led by one of three candidates at
     * random, so a candidate of the other version is picked with certainty but for (2/3)^60 at worst.
     */
    private static final int COUNTER_WRITES = 30;
    /**
     * Counter writes per coordinator to the keyspace of datacenter2 only. Each leads on the first replica of datacenter2
     * in ring order (node 4 for about two thirds of the keys, nodes 5 and 6 for a sixth each); the keys and the tokens of
     * the layout are fixed, so the leaders are too, and they include a node of the other version for both coordinators,
     * as asserted.
     */
    private static final int REMOTE_COUNTER_WRITES = 100;

    @Test
    public void batchlogAndCountersAcrossDataCenters() throws Throwable
    {
        TwoDataCenters.testCase(config -> config.set("dynamic_snitch", false))
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " + networkTopology(3, 3));
                          cluster.schemaChange("CREATE TABLE " + KS + ".t (k int PRIMARY KEY, v int)");
                          cluster.schemaChange("CREATE TABLE " + KS + ".cnt (k int PRIMARY KEY, c counter)");
                          cluster.schemaChange("CREATE KEYSPACE " + KS_DC2 + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + DC2 + "': 3}");
                          cluster.schemaChange("CREATE TABLE " + KS_DC2 + ".cnt (k int PRIMARY KEY, c counter)");
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          for (int coordinator = 1; coordinator <= cluster.size(); coordinator++)
                          {
                              loggedBatches(cluster, coordinator, 1_000 * coordinator);
                              counters(cluster, coordinator, 100_000 + 1_000 * coordinator);
                          }
                          // the batchlog entries were removed on every node, whichever version stored them
                          for (int node = 1; node <= cluster.size(); node++)
                          {
                              int n = node;
                              awaitAsserted(() -> assertRows(cluster.get(n).executeInternal("SELECT count(*) FROM system.batches"), row(0L)));
                          }
                          // node 1 (this software) and node 2 (HCD 1.2) each lead on a remote replica of the other version
                          for (int coordinator : new int[]{ 1, 2 })
                              assertThat(remoteCounterLeaders(cluster, coordinator, 200_000 + 1_000 * coordinator))
                              .as("remote leaders of node %s", coordinator)
                              .anyMatch(r -> isUpgraded(target(r)) != isUpgraded(coordinator));
                      })
                      .run();
    }

    /**
     * Logged batches at {@code QUORUM} and {@code LOCAL_QUORUM} through one node: the batchlog goes to the two other nodes
     * of its data center, the rows to every replica of both data centers.
     */
    private static void loggedBatches(UpgradeableCluster cluster, int coordinator, int base)
    {
        ConsistencyLevel[] levels = { QUORUM, LOCAL_QUORUM };
        for (int l = 0; l < levels.length; l++)
        {
            int first = base + l * 100;
            StringBuilder batch = new StringBuilder("BEGIN BATCH\n");
            for (int k = first; k < first + KEYS; k++)
                batch.append(String.format("INSERT INTO %s.t (k, v) VALUES (%d, %d);%n", KS, k, coordinator));
            Set<String> stores = ConcurrentHashMap.newKeySet();
            IMessageFilters.Filter recorder = recordRoutes(cluster, BATCH_STORE_REQ, stores);
            try
            {
                cluster.coordinator(coordinator).execute(batch.append("APPLY BATCH").toString(), levels[l]);
            }
            finally
            {
                recorder.off();
            }
            Set<String> expected = new TreeSet<>();
            for (int peer : localPeers(coordinator))
                expected.add(route(coordinator, peer));
            assertThat(stores).as("batchlog of node %s at %s", coordinator, levels[l])
                              .isEqualTo(expected)
                              .anyMatch(r -> isUpgraded(target(r)) != isUpgraded(coordinator));
            for (int k = first; k < first + KEYS; k++)
                assertOnEveryReplica(cluster, "SELECT v FROM " + KS + ".t WHERE k = " + k, row(coordinator));
        }
    }

    /**
     * Counter increments at {@code LOCAL_QUORUM} and {@code QUORUM} through one node: some lead on a replica of the other
     * version in its data center (the routes of the counter writes the coordinator forwards show which), and every
     * replica of both data centers ends with the same values.
     */
    private static void counters(UpgradeableCluster cluster, int coordinator, int base)
    {
        Set<String> leaders = ConcurrentHashMap.newKeySet();
        IMessageFilters.Filter recorder = recordRoutes(cluster, COUNTER_MUTATION_REQ, leaders);
        try
        {
            for (ConsistencyLevel level : new ConsistencyLevel[]{ LOCAL_QUORUM, QUORUM })
                for (int i = 0; i < COUNTER_WRITES; i++)
                    cluster.coordinator(coordinator).execute("UPDATE " + KS + ".cnt SET c = c + ? WHERE k = ?", level, (long) i + 1, base + i);
        }
        finally
        {
            recorder.off();
        }
        assertThat(leaders).as("leaders of the counter writes of node %s", coordinator)
                           .allMatch(r -> r.startsWith(coordinator + "->") && localPeers(coordinator).contains(target(r)))
                           .anyMatch(r -> isUpgraded(target(r)) != isUpgraded(coordinator));
        for (int i = 0; i < COUNTER_WRITES; i++)
            assertOnEveryReplica(cluster, "SELECT c FROM " + KS + ".cnt WHERE k = " + (base + i), row(2L * (i + 1)));
    }

    /**
     * A counter of a keyspace replicated in {@code datacenter2} only, written through a node of {@code datacenter1}: the
     * leader is a replica of {@code datacenter2}, and {@code LOCAL_QUORUM} is unavailable, through both versions. Returns
     * the routes of the counter writes to their leaders.
     */
    private static Set<String> remoteCounterLeaders(UpgradeableCluster cluster, int coordinator, int base)
    {
        Set<String> leaders = ConcurrentHashMap.newKeySet();
        IMessageFilters.Filter recorder = recordRoutes(cluster, COUNTER_MUTATION_REQ, leaders);
        try
        {
            for (int i = 0; i < REMOTE_COUNTER_WRITES; i++)
                cluster.coordinator(coordinator).execute("UPDATE " + KS_DC2 + ".cnt SET c = c + 1 WHERE k = ?", QUORUM, base + i);
        }
        finally
        {
            recorder.off();
        }
        assertThat(leaders).as("remote leaders of the counter writes of node %s", coordinator)
                           .isNotEmpty()
                           .allMatch(r -> r.startsWith(coordinator + "->") && remoteNodes(coordinator).contains(target(r)));
        for (int i = 0; i < REMOTE_COUNTER_WRITES; i++)
            for (int node = 1; node <= cluster.size(); node++)
                if (dataCenter(node).equals(DC2))
                {
                    int n = node;
                    int k = base + i;
                    awaitAsserted(() -> assertRows(cluster.get(n).executeInternal("SELECT c FROM " + KS_DC2 + ".cnt WHERE k = ?", k), row(1L)));
                }

        assertThatThrownBy(() -> cluster.coordinator(coordinator).execute("UPDATE " + KS_DC2 + ".cnt SET c = c + 1 WHERE k = ?", LOCAL_QUORUM, base))
        .as("LOCAL_QUORUM counter write through node %s with no replica in its data center", coordinator)
        .satisfies(t -> assertThat(t.getClass().getSimpleName()).isEqualTo("UnavailableException"));
        return leaders;
    }
}
