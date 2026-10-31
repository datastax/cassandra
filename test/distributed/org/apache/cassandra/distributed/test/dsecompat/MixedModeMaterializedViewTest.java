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

import org.junit.Test;

import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.ReadRepairMetrics;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Materialized views between HCD 1.2 nodes at messaging version 11 and nodes of this software in {@code DSE_6}
 * mode. Three nodes, RF 2 so that a base replica and its paired view replica are often different nodes, node 1
 * upgraded first, then node 2. Base rows are inserted, updated so that their view rows move, and deleted by
 * coordinators of both versions; the view is read at ALL by the other version and on every node locally.
 * <p>
 * HCD 1.2 has no hidden view columns: the harness gives its data directories an empty DSE {@code system_schema.hidden_columns}
 * (see {@link DseDialectTestBase}), so the upgraded nodes exchange the view data at version 11 as it is, as with the views
 * DSE keeps no hidden column for. The translation of DSE's hidden view column is covered by {@code DseViewWireFormatTest},
 * with DSE-shaped messages and the view of the DSE 6.8 fixture, and was observed with DSE 6.9.26 nodes, which counted
 * every row of such a view at ALL in a cluster with upgraded nodes.
 */
public class MixedModeMaterializedViewTest extends DseDialectTestBase
{
    private static final int HCD = 3;
    private static final String KS2 = "ks_rf2";
    private static final int KEYS = 10;

    @Test
    public void viewUpdatesAcrossVersions() throws Throwable
    {
        // the 4.0 name: HCD 1.2 only knows it, this software still accepts it
        dse6TestCase(3, config -> config.set("enable_materialized_views", true))
        .nodesToUpgrade(1, 2)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE " + KS2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + KS2 + ".base (k int, c int, v int, w text, PRIMARY KEY (k, c))");
            cluster.schemaChange("CREATE MATERIALIZED VIEW " + KS2 + ".by_v AS SELECT * FROM " + KS2 + ".base " +
                                 "WHERE v IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (v, k, c)");
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            for (int node = 1; node <= upgraded; node++)
            {
                assertPeersAtDialect(cluster, node, cluster.size() - upgraded);
                // the view data is exchanged as it is: DSE's (empty) table lists no hidden column for the view, and nothing is refused
                assertThat(cluster.get(node).logs().grep("lists no hidden column for materialized view " + KS2 + ".by_v").getResult()).isNotEmpty();
                assertThat(compatStatus(cluster, node)).doesNotContain(DseViewHiddenColumnCatalog.REFUSED_VIEW);
            }
            int[][] directions = { { upgraded, HCD }, { HCD, upgraded } };
            for (int d = 0; d < directions.length; d++)
                viewUpdates(cluster, directions[d][0], directions[d][1], upgraded * 1_000 + d * 100);
        })
        .run();
    }

    private static void viewUpdates(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KS2 + ".base";
        String view = KS2 + ".by_v";
        for (int i = 0; i < KEYS; i++)
            cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v, w) VALUES (?, 1, ?, ?)", QUORUM, base + i, base + i, "w" + i);
        for (int i = 0; i < KEYS; i++)
            assertView(cluster, reader, view, base + i, row(base + i, base + i, 1, "w" + i));

        // moving the view row: the old one is deleted on its view replica, the new one written on another
        for (int i = 0; i < KEYS; i++)
            cluster.coordinator(writer).execute("UPDATE " + table + " SET v = ? WHERE k = ? AND c = 1", QUORUM, base + 50 + i, base + i);
        for (int i = 0; i < KEYS; i++)
        {
            assertView(cluster, reader, view, base + i);
            assertView(cluster, reader, view, base + 50 + i, row(base + 50 + i, base + i, 1, "w" + i));
        }

        // a regular column update and a base row deletion, from the other version
        for (int i = 0; i < KEYS; i++)
        {
            if (i % 2 == 0)
                cluster.coordinator(reader).execute("UPDATE " + table + " SET w = 'updated' WHERE k = ? AND c = 1", QUORUM, base + i);
            else
                cluster.coordinator(reader).execute("DELETE FROM " + table + " WHERE k = ? AND c = 1", QUORUM, base + i);
        }
        for (int i = 0; i < KEYS; i++)
        {
            if (i % 2 == 0)
                assertView(cluster, writer, view, base + 50 + i, row(base + 50 + i, base + i, 1, "updated"));
            else
                assertView(cluster, writer, view, base + 50 + i);
        }
    }

    /**
     * The view partition read at ALL by the given coordinator, and the same rows on each of its replicas (exactly RF
     * nodes hold a live view row; view updates are asynchronous, hence the wait). Once the replicas agree, a read at ALL
     * coordinated by an upgraded node finds the digests of its replicas equal, whichever their versions: no read repair.
     */
    private static void assertView(UpgradeableCluster cluster, int coordinator, String view, int v, Object[]... expected)
    {
        String select = "SELECT v, k, c, w FROM " + view + " WHERE v = " + v;
        awaitAsserted(() -> assertRows(cluster.coordinator(coordinator).execute(select, ALL), expected));
        awaitAsserted(() -> {
            int holders = 0;
            for (int node = 1; node <= cluster.size(); node++)
            {
                Object[][] rows = cluster.get(node).executeInternal(select);
                if (rows.length > 0)
                {
                    holders++;
                    assertRows(rows, expected);
                }
            }
            if (holders != (expected.length == 0 ? 0 : 2))
                throw new AssertionError(holders + " nodes hold the view rows of v = " + v);
        });
        if (coordinator != HCD)
        {
            // the upgradeable wrapper delegates to an invokable instance of the version it runs
            IInvokableInstance upgraded = (IInvokableInstance) cluster.get(coordinator);
            long repairsBefore = upgraded.callOnInstance(() -> ReadRepairMetrics.repairedBlocking.getCount());
            assertRows(cluster.coordinator(coordinator).execute(select, ALL), expected);
            assertThat(upgraded.callOnInstance(() -> ReadRepairMetrics.repairedBlocking.getCount()))
            .as("read repairs (digest mismatches) of a read at ALL of v = %d through node %d", v, coordinator)
            .isEqualTo(repairsBefore);
        }
    }
}
