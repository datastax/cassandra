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

import org.apache.cassandra.distributed.UpgradeableCluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * TRUNCATE between HCD 1.2 nodes at messaging version 11 and nodes of this software in {@code DSE_6} mode. Three
 * nodes, node 1 upgraded first, then node 2; after each upgrade a table with RF 3 and one with RF 1, with rows both
 * in sstables and in memtables, are truncated by a coordinator of each version, and every node is checked empty.
 */
public class MixedModeTruncateTest extends DseDialectTestBase
{
    private static final int HCD = 3;
    private static final String KS1 = "ks_rf1";
    private static final int ROWS = 50;

    @Test
    public void truncateAcrossVersions() throws Throwable
    {
        dse6TestCase(3)
        .nodesToUpgrade(1, 2)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE " + KS1 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int, c int, v text, PRIMARY KEY (k, c))");
            cluster.schemaChange("CREATE TABLE " + KS1 + ".t (k int, c int, v text, PRIMARY KEY (k, c))");
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            for (int node = 1; node <= upgraded; node++)
                assertPeersAtDialect(cluster, node, cluster.size() - upgraded);
            for (int coordinator : new int[]{ upgraded, HCD })
            {
                for (String ks : new String[]{ KEYSPACE, KS1 })
                {
                    fill(cluster, ks, coordinator);
                    cluster.coordinator(coordinator).execute("TRUNCATE " + ks + ".t", ALL);
                    for (int node = 1; node <= cluster.size(); node++)
                        assertRows(cluster.get(node).executeInternal("SELECT count(*) FROM " + ks + ".t"), row(0L));
                    // the table takes writes again, from the other version too
                    int other = coordinator == HCD ? upgraded : HCD;
                    cluster.coordinator(other).execute("INSERT INTO " + ks + ".t (k, c, v) VALUES (1, 1, 'after')", ALL);
                    assertRows(cluster.coordinator(coordinator).execute("SELECT v FROM " + ks + ".t WHERE k = 1", ALL), row("after"));
                    cluster.coordinator(other).execute("TRUNCATE " + ks + ".t", ALL);
                    for (int node = 1; node <= cluster.size(); node++)
                        assertRows(cluster.get(node).executeInternal("SELECT count(*) FROM " + ks + ".t"), row(0L));
                }
            }
        })
        .run();
    }

    /** The first {@code ROWS / 2 + 1} rows flushed to sstables on every node, the others left in the memtables. */
    private static void fill(UpgradeableCluster cluster, String ks, int coordinator)
    {
        for (int i = 0; i < ROWS; i++)
        {
            cluster.coordinator(coordinator).execute("INSERT INTO " + ks + ".t (k, c, v) VALUES (?, ?, ?)", ALL, i % 10, i, "v" + i);
            if (i == ROWS / 2)
                for (int node = 1; node <= cluster.size(); node++)
                    cluster.get(node).flush(ks);
        }
        long total = 0;
        for (int node = 1; node <= cluster.size(); node++)
            total += (Long) cluster.get(node).executeInternal("SELECT count(*) FROM " + ks + ".t")[0][0];
        int replicationFactor = ks.equals(KS1) ? 1 : 3;
        if (total != (long) ROWS * replicationFactor)
            throw new AssertionError("expected " + ROWS * replicationFactor + " rows over the replicas of " + ks + ".t, found " + total);
    }
}
