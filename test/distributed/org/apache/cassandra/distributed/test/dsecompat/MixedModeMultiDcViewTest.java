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
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC1;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC2;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.dataCenter;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.isUpgraded;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.otherVersionInOtherDataCenter;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.recordRoutes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.source;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.target;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Materialized views across two data centers, between HCD 1.2 nodes at messaging version 11 (standing in for DSE 6) and
 * nodes of this software in {@code DSE_6} mode, in the layout of {@link TwoDataCenters}, two replicas of three in each
 * data center, so that a base replica and its paired view replica (paired in the base replica's own data center, in
 * DSE 6 too) are often different nodes, of either version: the test checks that some view update goes between versions.
 * Base rows are written at {@code EACH_QUORUM} or {@code LOCAL_QUORUM} through a coordinator of each version in each
 * data center and read at {@code ALL} through a node of the other version in the other data center; that node then
 * updates or deletes them, and the writer reads them at {@code ALL}. Every node is also read locally: two replicas in
 * each data center hold each live view row. Only writers 2 and 4 can show a view update between versions (the two other
 * nodes of writer 1's data center, and of writer 6's, are of one version); which ones do is fixed by the keys and the
 * tokens of the layout.
 * <p>
 * HCD 1.2 has no hidden view columns (see {@link MixedModeMaterializedViewTest}); DSE 6's own view code (the deletions
 * of old view entries it sends to every replica of every data center, as the DSE 6.9.26 source does) is not exercised
 * here: it needs DSE 6 itself.
 */
public class MixedModeMultiDcViewTest extends DseDialectTestBase
{
    private static final String KS = "ks_mv";
    private static final int KEYS = 10;

    @Test
    public void viewUpdatesAcrossDataCenters() throws Throwable
    {
        // the 4.0 name: HCD 1.2 only knows it, this software still accepts it
        TwoDataCenters.testCase(config -> config.set("enable_materialized_views", true))
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " + networkTopology(2, 2));
                          cluster.schemaChange("CREATE TABLE " + KS + ".base (k int, c int, v int, w text, PRIMARY KEY (k, c))");
                          cluster.schemaChange("CREATE MATERIALIZED VIEW " + KS + ".by_v AS SELECT * FROM " + KS + ".base " +
                                               "WHERE v IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (v, k, c)");
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          // a writer of each version in each data center: 2 and 6 run HCD 1.2, 1 and 4 this software
                          Set<String> viewWrites = ConcurrentHashMap.newKeySet();
                          for (int writer : new int[]{ 2, 1, 6, 4 })
                              viewUpdates(cluster, writer, otherVersionInOtherDataCenter(writer), 1_000 * writer, viewWrites);
                          assertThat(viewWrites).as("view updates between base and view replicas")
                                                .anyMatch(r -> isUpgraded(source(r)) != isUpgraded(target(r)));
                      })
                      .run();
    }

    /**
     * The view updates of one writer's base rows; {@code viewWrites} collects the mutations sent between two other nodes of
     * the writer's data center while it writes, which are view updates from a base replica to its paired view replica (a
     * write is forwarded only into the other data center).
     */
    private static void viewUpdates(UpgradeableCluster cluster, int writer, int reader, int base, Set<String> viewWrites)
    {
        String table = KS + ".base";
        String view = KS + ".by_v";
        Set<String> mutations = ConcurrentHashMap.newKeySet();
        IMessageFilters.Filter recorder = recordRoutes(cluster, MUTATION_REQ, mutations);
        try
        {
            for (int i = 0; i < KEYS; i++)
                cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v, w) VALUES (?, 1, ?, ?)", EACH_QUORUM, base + i, base + i, "w" + i);
            for (int i = 0; i < KEYS; i++)
                assertView(cluster, reader, view, base + i, row(base + i, base + i, 1, "w" + i));

            // moving the view row: the old one is deleted on its view replicas, the new one written on others
            for (int i = 0; i < KEYS; i++)
                cluster.coordinator(writer).execute("UPDATE " + table + " SET v = ? WHERE k = ? AND c = 1", LOCAL_QUORUM, base + 500 + i, base + i);
            for (int i = 0; i < KEYS; i++)
                assertView(cluster, reader, view, base + 500 + i, row(base + 500 + i, base + i, 1, "w" + i));
        }
        finally
        {
            recorder.off();
        }
        for (String route : mutations)
            if (source(route) != writer && target(route) != writer
                && dataCenter(source(route)).equals(dataCenter(writer)) && dataCenter(target(route)).equals(dataCenter(writer)))
                viewWrites.add(route);

        for (int i = 0; i < KEYS; i++)
            assertView(cluster, reader, view, base + i);

        // a regular column update and a base row deletion, from the other version in the other data center
        for (int i = 0; i < KEYS; i++)
        {
            if (i % 2 == 0)
                cluster.coordinator(reader).execute("UPDATE " + table + " SET w = 'updated' WHERE k = ? AND c = 1", EACH_QUORUM, base + i);
            else
                cluster.coordinator(reader).execute("DELETE FROM " + table + " WHERE k = ? AND c = 1", EACH_QUORUM, base + i);
        }
        for (int i = 0; i < KEYS; i++)
        {
            if (i % 2 == 0)
                assertView(cluster, writer, view, base + 500 + i, row(base + 500 + i, base + i, 1, "updated"));
            else
                assertView(cluster, writer, view, base + 500 + i);
        }
    }

    /**
     * The rows of a view partition on each of its replicas, then read at ALL by the given coordinator: two nodes of each
     * data center hold a live view row, none a deleted one (view updates are asynchronous, hence the wait). The replicas
     * are read first, so that the read repair of the ALL read cannot make up for a view update that never arrived.
     */
    private static void assertView(UpgradeableCluster cluster, int coordinator, String view, int v, Object[]... expected)
    {
        String select = "SELECT v, k, c, w FROM " + view + " WHERE v = " + v;
        awaitAsserted(() -> {
            int[] holders = new int[2];
            for (int node = 1; node <= cluster.size(); node++)
            {
                Object[][] rows = cluster.get(node).executeInternal(select);
                if (rows.length > 0)
                {
                    holders[dataCenter(node).equals(DC1) ? 0 : 1]++;
                    assertRows(rows, expected);
                }
            }
            int perDataCenter = expected.length == 0 ? 0 : 2;
            assertThat(holders[0]).as("nodes of %s holding the view rows of v = %s", DC1, v).isEqualTo(perDataCenter);
            assertThat(holders[1]).as("nodes of %s holding the view rows of v = %s", DC2, v).isEqualTo(perDataCenter);
        });
        assertRows(cluster.coordinator(coordinator).execute(select, ALL), expected);
    }
}
