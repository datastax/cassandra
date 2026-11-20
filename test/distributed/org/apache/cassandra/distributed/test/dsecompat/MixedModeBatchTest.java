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
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.BATCH_STORE_REQ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Logged and unlogged batches between HCD 1.2 nodes at messaging version 11 and nodes of this software in
 * {@code DSE_6} mode. Three nodes, node 1 upgraded first, then node 2; after each upgrade every node coordinates
 * batches that span the ring, on a keyspace with RF 3 and one with RF 2, so that the batchlog, its removal and the
 * batched mutations cross the version boundary in both directions.
 */
public class MixedModeBatchTest extends DseDialectTestBase
{
    private static final String KS2 = "ks_rf2";
    private static final int KEYS_PER_BATCH = 10;

    @Test
    public void batchesAcrossVersions() throws Throwable
    {
        dse6TestCase(3)
        .nodesToUpgrade(1, 2)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE " + KS2 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            for (String ks : new String[]{ KEYSPACE, KS2 })
            {
                cluster.schemaChange("CREATE TABLE " + ks + ".names (k int PRIMARY KEY, v text)");
                cluster.schemaChange("CREATE TABLE " + ks + ".wide (k int, c int, s text static, v text, PRIMARY KEY (k, c))");
            }
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".cnt (k int PRIMARY KEY, c counter)");
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            for (int node = 1; node <= upgraded; node++)
                assertPeersAtDialect(cluster, node, cluster.size() - upgraded);
            for (int coordinator = 1; coordinator <= cluster.size(); coordinator++)
            {
                int base = upgraded * 100_000 + coordinator * 10_000;
                batches(cluster, coordinator, true, base, QUORUM);
                batches(cluster, coordinator, true, base + 1_000, ALL);
                batches(cluster, coordinator, false, base + 2_000, ONE);
                batches(cluster, coordinator, false, base + 3_000, ALL);
                counterBatch(cluster, coordinator, base + 4_000);
                failedBatchlogWrite(cluster, coordinator, base + 5_000);
            }
            // the batchlog entries of the successful logged batches were removed on every node, whichever version stored them
            for (int node = 1; node <= cluster.size(); node++)
            {
                int n = node;
                awaitAsserted(() -> assertRows(cluster.get(n).executeInternal("SELECT count(*) FROM system.batches"), row(0L)));
            }
        })
        .run();
    }

    /** One batch per keyspace spanning the ring and two tables, then every replica checked with local queries. */
    private static void batches(UpgradeableCluster cluster, int coordinator, boolean logged, int base, ConsistencyLevel cl)
    {
        for (String ks : new String[]{ KEYSPACE, KS2 })
        {
            StringBuilder batch = new StringBuilder(logged ? "BEGIN BATCH\n" : "BEGIN UNLOGGED BATCH\n");
            for (int i = 0; i < KEYS_PER_BATCH; i++)
            {
                int k = base + i;
                batch.append(String.format("INSERT INTO %s.names (k, v) VALUES (%d, 'n%d');%n", ks, k, k));
                batch.append(String.format("INSERT INTO %s.wide (k, c, v) VALUES (%d, 1, 'w%d');%n", ks, k, k));
                batch.append(String.format("UPDATE %s.wide SET s = 's%d' WHERE k = %d;%n", ks, k, k));
                batch.append(String.format("DELETE FROM %s.wide WHERE k = %d AND c = 0;%n", ks, k));
            }
            cluster.coordinator(coordinator).execute(batch.append("APPLY BATCH").toString(), cl);

            int replicationFactor = ks.equals(KEYSPACE) ? 3 : 2;
            for (int i = 0; i < KEYS_PER_BATCH; i++)
            {
                int k = base + i;
                // every statement of the batch reached exactly the replicas of its key: RF of them hold the rows
                String names = "SELECT v FROM " + ks + ".names WHERE k = " + k;
                String wide = "SELECT c, s, v FROM " + ks + ".wide WHERE k = " + k;
                awaitAsserted(() -> {
                    int holders = 0;
                    for (int node = 1; node <= cluster.size(); node++)
                    {
                        Object[][] rows = cluster.get(node).executeInternal(names);
                        if (rows.length == 0)
                            continue;
                        holders++;
                        assertRows(rows, row("n" + k));
                        assertRows(cluster.get(node).executeInternal(wide), row(1, "s" + k, "w" + k));
                    }
                    assertThat(holders).as("replicas of key %s in %s", k, ks).isEqualTo(replicationFactor);
                });
            }
        }
    }

    /** A counter batch coordinated by each node, the counters read back from every replica. */
    private static void counterBatch(UpgradeableCluster cluster, int coordinator, int base)
    {
        StringBuilder batch = new StringBuilder("BEGIN COUNTER BATCH\n");
        for (int i = 0; i < KEYS_PER_BATCH; i++)
            batch.append(String.format("UPDATE %s.cnt SET c = c + %d WHERE k = %d;%n", KEYSPACE, i + 1, base + i));
        String statement = batch.append("APPLY BATCH").toString();
        cluster.coordinator(coordinator).execute(statement, QUORUM);
        cluster.coordinator(coordinator).execute(statement, ALL);
        for (int i = 0; i < KEYS_PER_BATCH; i++)
            assertOnEveryReplica(cluster, "SELECT c FROM " + KEYSPACE + ".cnt WHERE k = " + (base + i), row(2L * (i + 1)));
    }

    /**
     * A logged batch whose batchlog writes are all dropped times out before any of its mutations is sent, as in a
     * cluster of one version: the batchlog replicas of the other version are waited for, not skipped.
     */
    private static void failedBatchlogWrite(UpgradeableCluster cluster, int coordinator, int base)
    {
        cluster.filters().outbound().verbs(BATCH_STORE_REQ.id).drop();
        try
        {
            StringBuilder batch = new StringBuilder("BEGIN BATCH\n");
            for (int i = 0; i < KEYS_PER_BATCH; i++)
                batch.append(String.format("INSERT INTO %s.names (k, v) VALUES (%d, 'lost');%n", KEYSPACE, base + i));
            String statement = batch.append("APPLY BATCH").toString();
            assertThatThrownBy(() -> cluster.coordinator(coordinator).execute(statement, QUORUM))
            .satisfies(t -> assertThat(t.getClass().getSimpleName()).isEqualTo("WriteTimeoutException"));
        }
        finally
        {
            cluster.filters().reset();
        }
        for (int node = 1; node <= cluster.size(); node++)
            assertRows(cluster.get(node).executeInternal("SELECT k FROM " + KEYSPACE + ".names WHERE k >= ? AND k < ? ALLOW FILTERING",
                                                        base, base + KEYS_PER_BATCH));
    }
}
