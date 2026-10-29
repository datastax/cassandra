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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads and writes between HCD 1.2 nodes at messaging version 11 and nodes of this software in {@code DSE_6} mode:
 * a three-node cluster with RF 3 (and a keyspace with RF 1, so that requests go to a single owner), node 1
 * upgraded first, then node 2, node 3 staying on HCD 1.2. After each upgrade every scenario runs twice, once written
 * by the upgraded node and read by the HCD 1.2 node, once the other way round, at the consistency level pairs that
 * make the read see the write (ONE/ALL, QUORUM/QUORUM, ALL/ONE); the replicas are then checked one by one with
 * local queries, so each mutation is proven to have crossed the version boundary in both directions.
 */
public class MixedModeReadWriteTest extends DseDialectTestBase
{
    private static final int HCD = 3;
    private static final String KS1 = "ks_rf1";
    private static final Pattern REPAIRED_BLOCKING = Pattern.compile("Mismatch \\(Blocking\\): (\\d+)");
    /** Write CL, read CL: pairs whose replica sets always overlap at RF 3. */
    private static final ConsistencyLevel[][] CL_PAIRS = { { ONE, ALL }, { QUORUM, QUORUM }, { ALL, ONE } };

    @Test
    public void readsAndWritesAcrossVersions() throws Throwable
    {
        dse6TestCase(3)
        .nodesToUpgrade(1, 2)
        .setup(MixedModeReadWriteTest::createSchema)
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            // after the first upgrade node 1 has two HCD 1.2 peers, after the second nodes 1 and 2 have one
            for (int node = 1; node <= upgraded; node++)
                assertPeersAtDialect(cluster, node, cluster.size() - upgraded);
            int[][] directions = { { upgraded, HCD }, { HCD, upgraded } };
            for (int d = 0; d < directions.length; d++)
            {
                int writer = directions[d][0];
                int reader = directions[d][1];
                // keys (and range-read tables) are private to the round and the direction
                int base = upgraded * 10_000 + d * 1_000;
                String suffix = "_" + upgraded + '_' + d;
                clusteringAndStatics(cluster, writer, reader, base);
                rangeReadsAndPaging(cluster, writer, reader, "pages" + suffix);
                complexTypes(cluster, writer, reader, base);
                counters(cluster, writer, reader, base);
                singleOwnerReadsAndWrites(cluster, writer, reader, base);
                tombstones(cluster, writer, reader, base);
                readRepair(cluster, writer, reader, base);
            }
        })
        .run();
    }

    private static void createSchema(UpgradeableCluster cluster)
    {
        cluster.schemaChange("CREATE TYPE " + KEYSPACE + ".addr (street text, zip int)");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".cs (k int, c int, s int static, v text, PRIMARY KEY (k, c))");
        for (int round = 1; round <= 2; round++)
            for (int d = 0; d < 2; d++)
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".pages_" + round + '_' + d + " (k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".types (k int PRIMARY KEY, u frozen<addr>, nu addr, tup tuple<int, text>, " +
                             "l list<int>, s set<text>, m map<text, int>, fl frozen<list<int>>, fs frozen<set<int>>, fm frozen<map<int, text>>)");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".cnt (k int PRIMARY KEY, c counter)");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tomb (k int, c int, v int, w int, PRIMARY KEY (k, c))");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".rr (k int, c int, v int, PRIMARY KEY (k, c))");
        cluster.schemaChange("CREATE KEYSPACE " + KS1 + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        cluster.schemaChange("CREATE TABLE " + KS1 + ".kv (k int PRIMARY KEY, v int)");
        cluster.schemaChange("CREATE TABLE " + KS1 + ".cnt (k int PRIMARY KEY, c counter)");
    }

    /** Point reads, clustering slices, limits and static columns. */
    private static void clusteringAndStatics(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KEYSPACE + ".cs";
        for (int p = 0; p < CL_PAIRS.length; p++)
        {
            ConsistencyLevel writeCl = CL_PAIRS[p][0], readCl = CL_PAIRS[p][1];
            int k = base + p;
            for (int c = 0; c < 10; c++)
                cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?)", writeCl, k, c, "v" + c);
            cluster.coordinator(writer).execute("UPDATE " + table + " SET s = ? WHERE k = ?", writeCl, 42, k);

            assertRows(cluster.coordinator(reader).execute("SELECT c, s, v FROM " + table + " WHERE k = ? AND c = ?", readCl, k, 4),
                       row(4, 42, "v4"));
            assertRows(cluster.coordinator(reader).execute("SELECT c, v FROM " + table + " WHERE k = ? AND c >= ? AND c < ?", readCl, k, 3, 6),
                       row(3, "v3"), row(4, "v4"), row(5, "v5"));
            assertRows(cluster.coordinator(reader).execute("SELECT c FROM " + table + " WHERE k = ? AND c > ? ORDER BY c DESC LIMIT 3", readCl, k, 2),
                       row(9), row(8), row(7));
            assertRows(cluster.coordinator(reader).execute("SELECT c FROM " + table + " WHERE k = ? AND c IN (1, 7, 12)", readCl, k),
                       row(1), row(7));
            // a selection of the static column only, and a DISTINCT query: the static row alone
            assertRows(cluster.coordinator(reader).execute("SELECT s FROM " + table + " WHERE k = ? LIMIT 1", readCl, k), row(42));
            assertRows(cluster.coordinator(reader).execute("SELECT DISTINCT k, s FROM " + table + " WHERE k = ?", readCl, k), row(k, 42));
            assertOnEveryReplica(cluster, "SELECT count(*) FROM " + table + " WHERE k = " + k, row(10L));
        }

        // a partition that only has a static row
        int k = base + 10;
        cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, s) VALUES (?, ?)", ALL, k, 7);
        assertRows(cluster.coordinator(reader).execute("SELECT k, c, s, v FROM " + table + " WHERE k = ?", ALL, k), row(k, null, 7, null));
        assertRows(cluster.coordinator(reader).execute("SELECT s FROM " + table + " WHERE k = ?", ALL, k), row(7));
        assertRows(cluster.coordinator(reader).execute("SELECT c, v FROM " + table + " WHERE k = ? AND c = 1", ALL, k));
    }

    /** Range reads and paging, across partitions and within one, with statics, limits and per-partition limits. */
    private static void rangeReadsAndPaging(UpgradeableCluster cluster, int writer, int reader, String name)
    {
        String table = KEYSPACE + '.' + name;
        int partitions = 30, rows = 5;
        List<Object[]> expected = new ArrayList<>();
        for (int k = 0; k < partitions; k++)
        {
            cluster.coordinator(writer).execute("UPDATE " + table + " SET s = ? WHERE k = ?", QUORUM, k * 10, k);
            for (int c = 0; c < rows; c++)
            {
                cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?)", QUORUM, k, c, k * 100 + c);
                expected.add(row(k, c, k * 10, k * 100 + c));
            }
        }
        // a partition with 50 rows, paged within the partition
        int wide = partitions;
        for (int c = 0; c < 50; c++)
            cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?)", QUORUM, wide, c, c);

        for (ConsistencyLevel cl : Arrays.asList(QUORUM, ALL))
        {
            // a filtered range read above ONE (its row filter needs reconciliation, a flag the dialect does not carry), paged
            List<Object[]> all = drain(cluster.coordinator(reader).executeWithPaging("SELECT k, c, s, v FROM " + table + " WHERE k < " + wide + " ALLOW FILTERING", cl, 7));
            assertThat(sorted(all)).containsExactlyElementsOf(expected);

            // the same through an unpaged range read, a LIMIT, and a per-partition limit
            assertThat(cluster.coordinator(reader).execute("SELECT k FROM " + table, cl)).hasNumberOfRows(partitions * rows + 50);
            assertThat(cluster.coordinator(reader).execute("SELECT k FROM " + table + " LIMIT 13", cl)).hasNumberOfRows(13);
            Object[][] perPartition = cluster.coordinator(reader).execute("SELECT k, c FROM " + table + " PER PARTITION LIMIT 2", cl);
            assertThat(perPartition).hasNumberOfRows((partitions + 1) * 2);
            for (Object[] r : perPartition)
                assertThat((Integer) r[1]).isLessThan(2);

            // DISTINCT over the ring: one row per partition with its static value, paged
            List<Object[]> distinct = drain(cluster.coordinator(reader).executeWithPaging("SELECT DISTINCT k, s FROM " + table, cl, 4));
            assertThat(distinct).hasSize(partitions + 1);
            for (Object[] r : distinct)
                assertThat(r[1]).isEqualTo((Integer) r[0] == wide ? null : (Integer) r[0] * 10);

            // paging inside one partition, forwards and backwards
            List<Object[]> forward = drain(cluster.coordinator(reader).executeWithPaging("SELECT c FROM " + table + " WHERE k = ?", cl, 3, wide));
            List<Object[]> backward = drain(cluster.coordinator(reader).executeWithPaging("SELECT c FROM " + table + " WHERE k = ? ORDER BY c DESC", cl, 3, wide));
            assertThat(forward).hasSize(50);
            assertThat(backward).hasSize(50);
            for (int c = 0; c < 50; c++)
            {
                assertThat(forward.get(c)[0]).isEqualTo(c);
                assertThat(backward.get(c)[0]).isEqualTo(49 - c);
            }
        }
    }

    /** UDTs, tuples, frozen and non-frozen collections, with element updates and deletes. */
    private static void complexTypes(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KEYSPACE + ".types";
        String select = "SELECT toJson(u), toJson(nu), toJson(tup), l, s, m, fl, fs, fm FROM " + table + " WHERE k = ?";
        for (int p = 0; p < CL_PAIRS.length; p++)
        {
            ConsistencyLevel writeCl = CL_PAIRS[p][0], readCl = CL_PAIRS[p][1];
            int k = base + p;
            cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, u, nu, tup, l, s, m, fl, fs, fm) VALUES " +
                                                "(?, {street: 'main', zip: 1}, {street: 'side', zip: 2}, (3, 'three'), [1, 2, 3], {'a', 'b', 'x'}, " +
                                                "{'a': 1, 'b': 2}, [4, 5], {6, 7}, {8: 'eight'})", writeCl, k);
            assertRows(cluster.coordinator(reader).execute(select, readCl, k),
                       row("{\"street\": \"main\", \"zip\": 1}", "{\"street\": \"side\", \"zip\": 2}", "[3, \"three\"]",
                           ImmutableList.of(1, 2, 3), ImmutableSet.of("a", "b", "x"), ImmutableMap.of("a", 1, "b", 2),
                           ImmutableList.of(4, 5), ImmutableSet.of(6, 7), ImmutableMap.of(8, "eight")));

            // element operations on the non-frozen collections and a field update of the non-frozen UDT
            cluster.coordinator(writer).execute("UPDATE " + table + " SET l = [0] + l, l = l + [9], s = s - {'x'}, s = s + {'z'}, " +
                                                "m['c'] = 3, nu.zip = 20 WHERE k = ?", writeCl, k);
            cluster.coordinator(writer).execute("DELETE m['a'], l[1] FROM " + table + " WHERE k = ?", writeCl, k);
            cluster.coordinator(writer).execute("UPDATE " + table + " SET l = l - [3] WHERE k = ?", writeCl, k);
            Object[] expected = row("{\"street\": \"main\", \"zip\": 1}", "{\"street\": \"side\", \"zip\": 20}", "[3, \"three\"]",
                                    ImmutableList.of(0, 2, 9), ImmutableSet.of("a", "b", "z"), ImmutableMap.of("b", 2, "c", 3),
                                    ImmutableList.of(4, 5), ImmutableSet.of(6, 7), ImmutableMap.of(8, "eight"));
            assertRows(cluster.coordinator(reader).execute(select, readCl, k), expected);
            assertOnEveryReplica(cluster, select.replace("?", String.valueOf(k)), expected);

            // overwriting a non-frozen collection (a complex deletion plus new cells) and dropping one entirely
            cluster.coordinator(writer).execute("UPDATE " + table + " SET l = [7, 8], m = null, u = null WHERE k = ?", writeCl, k);
            // toJson of a null value is the JSON literal
            Object[] overwritten = row("null", "{\"street\": \"side\", \"zip\": 20}", "[3, \"three\"]",
                                       ImmutableList.of(7, 8), ImmutableSet.of("a", "b", "z"), null,
                                       ImmutableList.of(4, 5), ImmutableSet.of(6, 7), ImmutableMap.of(8, "eight"));
            assertRows(cluster.coordinator(reader).execute(select, readCl, k), overwritten);
            assertOnEveryReplica(cluster, select.replace("?", String.valueOf(k)), overwritten);
        }
    }

    /** Counter increments coordinated by both versions on the same counters; RF 1 forwards them to the owner. */
    private static void counters(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KEYSPACE + ".cnt";
        for (int p = 0; p < CL_PAIRS.length; p++)
        {
            ConsistencyLevel writeCl = CL_PAIRS[p][0], readCl = CL_PAIRS[p][1];
            int k = base + p;
            cluster.coordinator(writer).execute("UPDATE " + table + " SET c = c + 1 WHERE k = ?", writeCl, k);
            cluster.coordinator(reader).execute("UPDATE " + table + " SET c = c + 10 WHERE k = ?", writeCl, k);
            cluster.coordinator(writer).execute("UPDATE " + table + " SET c = c + 100 WHERE k = ?", writeCl, k);
            cluster.coordinator(reader).execute("UPDATE " + table + " SET c = c - 1000 WHERE k = ?", writeCl, k);
            assertRows(cluster.coordinator(reader).execute("SELECT c FROM " + table + " WHERE k = ?", readCl, k), row(-889L));
            assertRows(cluster.coordinator(writer).execute("SELECT c FROM " + table + " WHERE k = ?", readCl, k), row(-889L));
            assertOnEveryReplica(cluster, "SELECT c FROM " + table + " WHERE k = " + k, row(-889L));
        }

        // RF 1: most counter mutations are forwarded to a leader of the other version
        String rf1 = KS1 + ".cnt";
        for (int i = 0; i < 20; i++)
        {
            cluster.coordinator(writer).execute("UPDATE " + rf1 + " SET c = c + 2 WHERE k = ?", ONE, base + i);
            cluster.coordinator(reader).execute("UPDATE " + rf1 + " SET c = c + 3 WHERE k = ?", ONE, base + i);
        }
        for (int i = 0; i < 20; i++)
        {
            assertRows(cluster.coordinator(reader).execute("SELECT c FROM " + rf1 + " WHERE k = ?", ONE, base + i), row(5L));
            assertRows(cluster.coordinator(writer).execute("SELECT c FROM " + rf1 + " WHERE k = ?", ONE, base + i), row(5L));
        }
    }

    /**
     * RF 1: writes and reads sent to the single owner, which is of the other version for part of the keys (a replica the
     * coordinator is not; the FWD_TO/FWD_FRM forwarding between datacenters is MixedModeForwardingTest's).
     */
    private static void singleOwnerReadsAndWrites(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KS1 + ".kv";
        for (int i = 0; i < 20; i++)
            cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, v) VALUES (?, ?)", ONE, base + i, i);
        for (int i = 0; i < 20; i++)
            assertRows(cluster.coordinator(reader).execute("SELECT v FROM " + table + " WHERE k = ?", ONE, base + i), row(i));
        // every key is on exactly one node
        int found = 0;
        for (int node = 1; node <= cluster.size(); node++)
            found += (int) (long) (Long) cluster.get(node).executeInternal("SELECT count(*) FROM " + table + " WHERE k >= ? AND k < ? ALLOW FILTERING",
                                                                            base, base + 20)[0][0];
        assertThat(found).isEqualTo(20);
    }

    /**
     * Partition, row, range, cell and TTL tombstones. Each kind is written once through a coordinator at ALL, and once
     * on the writer's replica only: the read at ALL coordinated by the other version must return the tombstone's effect,
     * which needs the tombstone to travel in a read response, and read repair must then deliver it to the other replicas.
     */
    private static void tombstones(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KEYSPACE + ".tomb";
        String[] deletes = { "DELETE FROM " + table + " WHERE k = %d",
                             "DELETE FROM " + table + " WHERE k = %d AND c = 1",
                             "DELETE FROM " + table + " WHERE k = %d AND c >= 1 AND c < 3",
                             "DELETE v FROM " + table + " WHERE k = %d AND c = 1" };
        Object[][][] remaining = { {},
                                   { row(0, 0, 0), row(2, 2, 2), row(3, 3, 3) },
                                   { row(0, 0, 0), row(3, 3, 3) },
                                   { row(0, 0, 0), row(1, null, 1), row(2, 2, 2), row(3, 3, 3) } };
        for (int local = 0; local < 2; local++)
        {
            for (int i = 0; i < deletes.length; i++)
            {
                int k = base + local * 10 + i;
                for (int c = 0; c < 4; c++)
                    cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v, w) VALUES (?, ?, ?, ?)", ALL, k, c, c, c);
                String delete = String.format(deletes[i], k);
                if (local == 0)
                    cluster.coordinator(writer).execute(delete, ALL);
                else
                    cluster.get(writer).executeInternal(delete);
                String select = "SELECT c, v, w FROM " + table + " WHERE k = " + k;
                assertRows(cluster.coordinator(reader).execute(select, ALL), remaining[i]);
                assertOnEveryReplica(cluster, select, remaining[i]);
            }
        }

        // TTLs: a row inserted with a TTL and a cell updated with one, read back after they expired
        int k = base + 20;
        cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v, w) VALUES (?, 0, 0, 0) USING TTL 4", ALL, k);
        cluster.coordinator(writer).execute("INSERT INTO " + table + " (k, c, v, w) VALUES (?, 1, 1, 1)", ALL, k);
        cluster.coordinator(writer).execute("UPDATE " + table + " USING TTL 4 SET w = 5 WHERE k = ? AND c = 1", ALL, k);
        assertRows(cluster.coordinator(reader).execute("SELECT c, v, w FROM " + table + " WHERE k = ?", ALL, k), row(0, 0, 0), row(1, 1, 5));
        assertThat((Integer) cluster.coordinator(reader).execute("SELECT ttl(w) FROM " + table + " WHERE k = ? AND c = 1", ALL, k)[0][0]).isBetween(1, 4);
        // the TTL is in seconds: poll until both have expired
        awaitAsserted(() -> assertRows(cluster.coordinator(reader).execute("SELECT c, v, w FROM " + table + " WHERE k = ?", ALL, k), row(1, 1, null)));
        assertOnEveryReplica(cluster, "SELECT c, v, w FROM " + table + " WHERE k = " + k, row(1, 1, null));
    }

    /**
     * Rows present on the writer's replica only: a read at ALL coordinated by the other version sees a digest mismatch,
     * returns the row and repairs the other replicas. A read of rows that agree must not see a mismatch at all: the
     * digests of both versions have to match.
     */
    private static void readRepair(UpgradeableCluster cluster, int writer, int reader, int base)
    {
        String table = KEYSPACE + ".rr";
        int k = base;
        for (int c = 0; c < 3; c++)
            cluster.get(writer).executeInternal("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, ?)", k, c, c);
        long repairsBefore = repairedBlocking(cluster, reader);
        assertRows(cluster.coordinator(reader).execute("SELECT c, v FROM " + table + " WHERE k = ?", ALL, k), row(0, 0), row(1, 1), row(2, 2));
        assertThat(repairedBlocking(cluster, reader)).isGreaterThan(repairsBefore);
        for (int node = 1; node <= cluster.size(); node++)
            assertRows(cluster.get(node).executeInternal("SELECT c, v FROM " + table + " WHERE k = ?", k), row(0, 0), row(1, 1), row(2, 2));

        // the replicas now agree: digests computed by both versions match, whichever coordinates
        for (int coordinator : new int[]{ reader, writer })
        {
            long before = repairedBlocking(cluster, coordinator);
            for (int i = 0; i < 5; i++)
                assertRows(cluster.coordinator(coordinator).execute("SELECT c, v FROM " + table + " WHERE k = ?", ALL, k), row(0, 0), row(1, 1), row(2, 2));
            assertThat(repairedBlocking(cluster, coordinator)).isEqualTo(before);
        }

        // a single cell newer on one replica only, the read resolves it by timestamp and repairs
        cluster.get(writer).executeInternal("UPDATE " + table + " SET v = 100 WHERE k = ? AND c = 1", k);
        assertRows(cluster.coordinator(reader).execute("SELECT c, v FROM " + table + " WHERE k = ? AND c = 1", ALL, k), row(1, 100));
        for (int node = 1; node <= cluster.size(); node++)
            assertRows(cluster.get(node).executeInternal("SELECT v FROM " + table + " WHERE k = ? AND c = 1", k), row(100));
    }

    private static long repairedBlocking(UpgradeableCluster cluster, int node)
    {
        // nodetool netstats prints ReadRepairMetrics.repairedBlocking, marked by the coordinator on every digest mismatch,
        // in the same format in both versions
        NodeToolResult result = cluster.get(node).nodetoolResult("netstats");
        result.asserts().success();
        Matcher matcher = REPAIRED_BLOCKING.matcher(result.getStdout());
        assertThat(matcher.find()).as("netstats output: %s", result.getStdout()).isTrue();
        return Long.parseLong(matcher.group(1));
    }

    private static List<Object[]> drain(Iterator<Object[]> iterator)
    {
        List<Object[]> rows = new ArrayList<>();
        iterator.forEachRemaining(rows::add);
        return rows;
    }

    /** Range reads return partitions in token order: sorts by the first two int columns. */
    private static List<Object[]> sorted(List<Object[]> rows)
    {
        List<Object[]> copy = new ArrayList<>(rows);
        copy.sort((a, b) -> {
            int cmp = Integer.compare((Integer) a[0], (Integer) b[0]);
            return cmp != 0 ? cmp : Integer.compare((Integer) a[1], (Integer) b[1]);
        });
        return copy;
    }
}
