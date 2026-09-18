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

package org.apache.cassandra.index.sai.cql;

import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.index.sai.plan.QueryController;

import static org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.MIN_PQ_ROWS;

public class VectorHybridSearchTest extends VectorTester.VersionedWithChecksums
{
    @Test
    public void testHybridSearchWithPrimaryKeyHoles() throws Throwable
    {
        setMaxBruteForceRows(0);
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, val text, vec vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert rows into two sstables. The tokens for each PK are in each line's comment.
        execute("INSERT INTO %s (pk, val, vec) VALUES (1, 'A', [1, 3])"); // -4069959284402364209
        execute("INSERT INTO %s (pk, val, vec) VALUES (2, 'A', [1, 2])"); // -3248873570005575792
        // Make the last row in the sstable the correct result. That way we verify the ceiling logic
        // works correctly.
        execute("INSERT INTO %s (pk, val, vec) VALUES (3, 'A', [1, 1])"); // 9010454139840013625
        flush();
        execute("INSERT INTO %s (pk, val, vec) VALUES (5, 'A', [1, 5])"); // -7509452495886106294
        execute("INSERT INTO %s (pk, val, vec) VALUES (4, 'A', [1, 4])"); // -2729420104000364805
        execute("INSERT INTO %s (pk, val, vec) VALUES (6, 'A', [1, 6])"); // 2705480034054113608

        // Get all rows using first predicate, then filter to get top 1
        // Use a small limit to ensure we do not use brute force
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1"),
                       row(3));
        });
    }

    // Clustering columns hit a different code path, we need both sets of tests, even though queries
    // that expose the underlying regression are the same.
    @Test
    public void testHybridSearchWithPrimaryKeyHolesAndWithClusteringColumns() throws Throwable
    {
        setMaxBruteForceRows(0);
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, a int, val text, vec vector<float, 2>, PRIMARY KEY(pk, a))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert rows into two sstables. The tokens for each PK are in each line's comment.
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 1, 'A', [1, 3])"); // -4069959284402364209
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (2, 1, 'A', [1, 2])"); // -3248873570005575792
        // Make the last row in the sstable the correct result. That way we verify the ceiling logic
        // works correctly.
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (3, 1, 'A', [1, 1])"); // 9010454139840013625
        flush();
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (5, 1, 'A', [1, 5])"); // -7509452495886106294
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (4, 1, 'A', [1, 4])"); // -2729420104000364805
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (6, 1, 'A', [1, 6])"); // 2705480034054113608

        // Get all rows using first predicate, then filter to get top 1
        // Use a small limit to ensure we do not use brute force
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1"),
                       row(3));
        });
    }

    @Test
    public void testHybridSearchSequentialClusteringColumns() throws Throwable
    {
        setMaxBruteForceRows(0);
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, a int, val text, vec vector<float, 2>, PRIMARY KEY(pk, a))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 1, 'A', [1, 3])");
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 2, 'A', [1, 2])");
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 3, 'A', [1, 1])");

        // Get all rows using first predicate, then filter to get top 1
        // Use a small limit to ensure we do not use brute force
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,3] LIMIT 1"), row(1));
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,2] LIMIT 1"), row(2));
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1"), row(3));
        });
    }

    @Test
    public void testHybridSearchHoleInClusteringColumnOrdering() throws Throwable
    {
        setMaxBruteForceRows(0);
        QueryController.QUERY_OPT_LEVEL = 0;
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, a int, val text, vec vector<float, 2>, PRIMARY KEY(pk, a))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Create two sstables. The first needs a hole forcing us to skip.
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 1, 'A', [1, 3])");
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 3, 'A', [1, 2])");
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 4, 'A', [1, 1])");
        flush();
        execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, 2, 'A', [1, 4])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1"), row(4));
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,2] LIMIT 1"), row(3));
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,3] LIMIT 1"), row(1));
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,4] LIMIT 1"), row(2));
        });
    }

    @Test
    public void testHybridSearchSeqLogicForMappingPKsBackToRowIds() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, a int, val text, vec vector<float, 2>, PRIMARY KEY(pk, a))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert rows into two sstables. The rows are interleaved to ensure binary search is less efficient, which
        // pushes us to use a sequential scan when we map PKs back to row ids in the sstable.
        int rowCount = 100;
        // Insert even rows to first sstable
        for (int i = 0; i < rowCount; i += 2)
            execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, ?, 'A', ?)", i, vector(1, i));

        flush();
        // Insert odd rows to new sstable
        for (int i = 1; i < rowCount; i += 2)
            execute("INSERT INTO %s (pk, a, val, vec) VALUES (1, ?, 'A', ?)", i, vector(1, i));

        // Verify result for rows in different memtables/sstables
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,49] LIMIT 1"),
                       row(49));
            assertRows(execute("SELECT a FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,50] LIMIT 1"),
                       row(50));
        });
    }

    // This test covers a bug in the RowIdMatchingOrdinalsView logic. Essentially, when the final rows in a segment
    // do not have an associated vector, we will think we can do fast mapping from row id to ordinal, but in reality
    // we have to do bounds checking still.
    @Test
    public void testHybridIndexWithPartialRowInsertsAtSegmentBoundaries() throws Throwable
    {
        // This test requires the non-bruteforce route
        setMaxBruteForceRows(0);
        createTable("CREATE TABLE %s (pk int, val text, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, val, vec) VALUES (1, 'A', [1, 1])");
        execute("INSERT INTO %s (pk, val) VALUES (2, 'A')");
        flush();
        execute("INSERT INTO %s (pk, vec) VALUES (2, [1,3])");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1"), row(1));
        });

        // Assert the opposite with these writes where the lower bound is not present. (This case actually pushes us to
        // use disk based ordinal mapping.)
        execute("INSERT INTO %s (pk, val, vec) VALUES (2, 'A', [1, 2])");
        execute("INSERT INTO %s (pk, val) VALUES (1, 'A')");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE val = 'A' ORDER BY vec ANN OF [1,1] LIMIT 1"), row(1));
        });
    }

    @Test
    public void testHybridQueryWithMissingVectorValuesForMaxSegmentRow() throws Throwable
    {
        // Want to test the search then order path
        QueryController.QUERY_OPT_LEVEL = 0;

        // We use a clustered primary key to simplify the mental model for this test.
        // The bug this test exposed happens when the last row(s) in a segment, based on PK order, are present
        // in a peer index for an sstable's search index but not its vector index.
        createTable("CREATE TABLE %s (k int, i int, v vector<float, 2>, c int,  PRIMARY KEY(k, i))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        // We'll manually control compaction.
        disableCompaction();

        // Insert a complete row. We need at least one row with a vector and an entry for column c to ensure that
        // the query doesn't skip the query portion where we map from Primary Key back to sstable row id.
        execute("INSERT INTO %s (k, i, v, c) VALUES (0, ?, ?, ?)", 1, vector(1, 1), 1);

        // Insert the first and last row in the table and leave of the vector
        execute("INSERT INTO %s (k, i, c) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, i, c) VALUES (0, 2, 2)");

        // The bug was specifically for sstables after compaction, but it's trivial to cover the before flush and before
        // compaction cases here, so we do.
        runThenFlushThenCompact(() -> {
            // There is only one row that satisfies the WHERE clause and has a vector for each of these queries.
            assertRows(execute("SELECT i FROM %s WHERE c <= 1 ORDER BY v ANN OF [1,1] LIMIT 1"), row(1));
            assertRows(execute("SELECT i FROM %s WHERE c >= 1 ORDER BY v ANN OF [1,1] LIMIT 1"), row(1));
        });
    }

    @Test
    public void testReranklessHybridSearch()
    {
        // Want to test the search then order path
        QueryController.QUERY_OPT_LEVEL = 0;

        createTable("CREATE TABLE %s (pk int, val int, vec vector<float, 128>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert many rows in parallel
        IntStream.range(0, MIN_PQ_ROWS * 2).parallel().forEach(i -> {
            execute("INSERT INTO %s (pk, val, vec) VALUES (?, ?, ?)", i, i, randomVectorBoxed(128));
        });

        flush();

        // Search the graph with rerankless search. We restrict val to 1/4th of the dataset
        setMaxBruteForceRows(0);
        var result = execute("SELECT pk FROM %s WHERE val < ? ORDER BY vec ANN OF ? LIMIT 10 with ann_options = { 'rerank_k': 0 }", MIN_PQ_ROWS / 2, randomVectorBoxed(128));
        // Just testing that we can run the query, so only assert that we got results
        assertRowCount(result, 10);

        // Now search with brute force (skipping the graph). We restrict to 1/10th of the dataset to trigger brute force.
        setMaxBruteForceRows(MIN_PQ_ROWS * 2);
        result = execute("SELECT pk FROM %s WHERE val < ? ORDER BY vec ANN OF ? LIMIT 10 with ann_options = { 'rerank_k': 0 }", MIN_PQ_ROWS / 5, randomVectorBoxed(128));
        assertRowCount(result, 10);
        // Also ensure that a negative rerank_k value works
        result = execute("SELECT pk FROM %s WHERE val < ? ORDER BY vec ANN OF ? LIMIT 10 with ann_options = { 'rerank_k': -1 }", MIN_PQ_ROWS / 5, randomVectorBoxed(128));
        assertRowCount(result, 10);
    }

    /**
     * Regression test for CNDB-19058 / CNDB-15622.
     *
     * A search-then-sort ANN hybrid query whose WHERE predicate is on a static column and whose ORDER BY is ANN must
     * return the correct rows both in-memtable and after flush.  Before the fix, the static PrimaryKey from the
     * predicate index was not found in the vector graph (which stores regular-row keys), yielding 0 results.
     */
    @Test
    public void testHybridANNQueryWithStaticPredicate() throws Throwable
    {
        // Exact (brute-force) scoring makes results deterministic regardless of graph structure.
        // The brute-force vs graph scoring choice is orthogonal to the static-key expansion being tested.
        setMaxBruteForceRows(Integer.MAX_VALUE);

        createTable("CREATE TABLE %s (k int, c int, s text static, r vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 1, 'target', [1, 1])");
        execute("INSERT INTO %s (k, c, s, r) VALUES (2, 1, 'target', [2, 2])");
        // Enough non-matching partitions to trigger search-then-sort
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, c, s, r) VALUES (?, 1, 'other', ?)", i, vector(i, i));

        String select = "SELECT k, c FROM %s WHERE s = ? ORDER BY r ANN OF [1, 1] LIMIT 2";

        // Must work in memtable state
        assertRows(execute(select, "target"), row(1, 1), row(2, 1));

        // Must still work after flush
        flush();
        assertRows(execute(select, "target"), row(1, 1), row(2, 1));

        // And after compaction
        compact();
        assertRows(execute(select, "target"), row(1, 1), row(2, 1));
    }

    /**
     * Regression test for CNDB-19058 / CNDB-15622: ANN hybrid query with a static column predicate where the
     * matching partition has multiple clustering rows.  All regular rows must be resolved for re-ranking, both
     * in-memtable and after flush.
     */
    @Test
    public void testHybridANNQueryWithStaticPredicateMultipleClusterings() throws Throwable
    {
        // Use exact (brute-force) scoring so results are deterministic regardless of graph structure.
        setMaxBruteForceRows(Integer.MAX_VALUE);

        createTable("CREATE TABLE %s (k int, c int, s text static, r vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // One partition matching the predicate with two clustering rows
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 1, 'target', [1, 3])");
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 2, 'target', [1, 1])");
        // Non-matching partitions to trigger search-then-sort
        for (int i = 2; i < 100; i++)
            execute("INSERT INTO %s (k, c, s, r) VALUES (?, 1, 'other', ?)", i, vector(i, i));

        // c=2 ([1,1]) is the nearest neighbour to [1,1]
        String select = "SELECT k, c FROM %s WHERE s = ? ORDER BY r ANN OF [1, 1] LIMIT 1";

        assertRows(execute(select, "target"), row(1, 2));

        flush();
        assertRows(execute(select, "target"), row(1, 2));

        compact();
        assertRows(execute(select, "target"), row(1, 2));
    }

    /**
     * Covers the null-vector early-return path in VectorMemtableIndex.addKeyToGraph.
     * When some rows have a null vector value they must be silently skipped rather than
     * causing a NullPointerException or incorrect results.
     */
    @Test
    public void testANNWithNullVectorRows()
    {
        setMaxBruteForceRows(Integer.MAX_VALUE);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, s int, r vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // k=1 has a vector; k=2 has s=1 but NO vector (null) — addKeyToGraph must return early for k=2
        execute("INSERT INTO %s (k, s, r) VALUES (1, 1, [1, 1])");
        execute("INSERT INTO %s (k, s) VALUES (2, 1)");
        // Fill enough non-matching rows to trigger search-then-sort
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, s, r) VALUES (?, 99, ?)", i, vector(i, i));

        String select = "SELECT k FROM %s WHERE s = 1 ORDER BY r ANN OF [1, 1] LIMIT 5";

        // Only k=1 has a vector; k=2 (null vector) must be skipped; result must not crash
        assertRows(execute(select), row(1));

        // Flush exercises the SSTable path; null-vector rows are simply absent from the index
        flush();
        assertRows(execute(select), row(1));
    }
}
