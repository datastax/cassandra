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

import org.junit.Test;

import org.apache.cassandra.index.sai.plan.QueryController;

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
}
