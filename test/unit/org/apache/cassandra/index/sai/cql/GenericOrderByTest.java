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

import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Plan;
import org.apache.cassandra.index.sai.plan.QueryController;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GenericOrderByTest extends SAITester
{
    @Test
    public void testOrderingAcrossManySstables()
    {
        // Disable query optimizer to prevent skipping hybrid query logic.
        QueryController.QUERY_OPT_LEVEL = 0;
        // We don't want our sstables getting compacted away
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int, str_val ascii)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        disableCompaction();

        var expectedResults = new TreeMap<String, Integer>();

        // Put the first and last ones in first to put them in sstables to guarantee we hit each for ASC and DESC, respectively.
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -1, -1, "AA");
        expectedResults.put("AA", -1);
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -2, -2, "zz");
        expectedResults.put("zz", -2);

        for (int i = 0; i < 200; i++)
        {
            // Use ascii because its ordering works the way we expect.
            var str = getRandom().nextAsciiString(10, 30);
            execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", i, i, str);
            expectedResults.put(str, i);
            if (getRandom().nextIntBetween(0, 100) < 2)
                flush();
        }

        // Put the first and last ones in a memtable to guarantee we hit each for ASC and DESC, respectively.
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -3, -1, "A");
        expectedResults.put("A", -3);
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -4, -2, "z");
        expectedResults.put("z", -4);

        assertRows(execute("SELECT pk FROM %s ORDER BY str_val ASC LIMIT 1"),
                   expectedResults.values().stream().map(CQLTester::row).limit(1).toArray(Object[][]::new));
        assertRows(execute("SELECT pk FROM %s WHERE val < 15 ORDER BY str_val ASC LIMIT 1"),
                   expectedResults.values().stream().filter(x -> x < 15).map(CQLTester::row).limit(1).toArray(Object[][]::new));

        assertRows(execute("SELECT pk FROM %s ORDER BY str_val DESC LIMIT 1"),
                   expectedResults.descendingMap().values().stream().map(CQLTester::row).limit(1).toArray(Object[][]::new));
        assertRows(execute("SELECT pk FROM %s WHERE val < 15 ORDER BY str_val DESC LIMIT 1"),
                   expectedResults.descendingMap().values().stream().filter(x -> x < 15).map(CQLTester::row).limit(1).toArray(Object[][]::new));
    }

    @Test
    public void testOrderingAcrossMemtableAndSSTable() throws Throwable
    {
        QueryController.QUERY_OPT_LEVEL = 0;
        // We don't want our sstables getting compacted away
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int, str_val ascii)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        disableCompaction();

        // 'A' will be first
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -1, -1, "A");
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -2, -2, "z");

        flush();

        // 'zz' will be last
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -3, -3, "AA");
        execute("INSERT INTO %s (pk, val, str_val) VALUES (?, ?, ?)", -4, -4, "zz");

        beforeAndAfterFlush(() -> {
            // Query for limit 1 to make sure that we don't reorder the results later on in the stack.
            // This test verifies the correctness of the text encoding.
            assertRows(execute("SELECT pk FROM %s ORDER BY str_val ASC LIMIT 1"), row(-1));
            assertRows(execute("SELECT pk FROM %s WHERE val < 0 ORDER BY str_val ASC LIMIT 1"), row(-1));

            assertRows(execute("SELECT pk FROM %s ORDER BY str_val DESC LIMIT 1"), row(-4));
            assertRows(execute("SELECT pk FROM %s WHERE val < 0 ORDER BY str_val DESC LIMIT 1"), row(-4));
        });
    }

    @Test
    public void testPrimaryKeyRestrictionToEnsureBoundsAreCorrectlyHandled() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, x int, val int, str_val ascii)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        // Insert many rows and then ensure we can get each of them when querying with specific bounds.
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, x, val, str_val) VALUES (?, ?, ?, ?)", i, i, i, i);

        // Test caught a bug in the way we created boundaries.
        beforeAndAfterFlush(() -> {
            for (int i = 0; i < 100; i++)
            {
                assertRows(execute("SELECT pk FROM %s WHERE pk = ? ORDER BY str_val ASC LIMIT 1", i), row(i));
                assertRows(execute("SELECT pk FROM %s WHERE pk = ? ORDER BY val ASC LIMIT 1", i), row(i));
                assertRows(execute("SELECT pk FROM %s WHERE pk = ? ORDER BY str_val DESC LIMIT 1", i), row(i));
                assertRows(execute("SELECT pk FROM %s WHERE pk = ? ORDER BY val DESC LIMIT 1", i), row(i));
            }
        });
    }

    @Test
    public void testMultiplePrimaryKeysForSameTerm() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, x int, val int, str_val ascii, PRIMARY KEY (pk, x))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        // We use a primary key with the same partition column value to ensure it goes to the same shard in the
        // memtable, which reproduces a bug we hit.
        execute("INSERT INTO %s (pk, x, val, str_val) VALUES (?, ?, ?, ?)", 1, 1, 1, "A");
        execute("INSERT INTO %s (pk, x, val, str_val) VALUES (?, ?, ?, ?)", 1, 2, 1, "A");
        // Goes to a different shard in the memtable
        execute("INSERT INTO %s (pk, x, val, str_val) VALUES (?, ?, ?, ?)", 2, 3, 2, "B");

        beforeAndAfterFlush(() -> {
            // Literal order by
            assertRows(execute("SELECT x FROM %s ORDER BY str_val ASC LIMIT 2"), row(1), row(2));
            assertRows(execute("SELECT x FROM %s ORDER BY str_val DESC LIMIT 1"), row(3));
            assertRows(execute("SELECT x FROM %s WHERE val = 1 ORDER BY str_val ASC LIMIT 2"), row(1), row(2));
            // Numeric order by
            assertRows(execute("SELECT x FROM %s ORDER BY val ASC LIMIT 2"), row(1), row(2));
            assertRows(execute("SELECT x FROM %s ORDER BY val DESC LIMIT 1"), row(3));
            assertRows(execute("SELECT x FROM %s WHERE str_val = 'A' ORDER BY val ASC LIMIT 2"), row(1), row(2));
        });
    }

    @Test
    public void testSelectionAndOrderByOnTheSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, x int, v int, PRIMARY KEY (pk, x))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 1, 2, 5);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 1, 3, 2);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 1, 4, 4);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 2, 1, 7);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 2, 2, 6);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 2, 3, 8);
        execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", 2, 4, 3);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT v FROM %s WHERE v >= -10 ORDER BY v ASC LIMIT 4"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT v FROM %s WHERE v > 1 ORDER BY v ASC LIMIT 4"), row(2), row(3), row(4), row(5));
            assertRows(execute("SELECT v FROM %s WHERE v <= 3 ORDER BY v ASC LIMIT 4"), row(1), row(2), row(3));
            assertRows(execute("SELECT v FROM %s WHERE v >= 4 AND v <= 6 ORDER BY v ASC LIMIT 4"), row(4), row(5), row(6));
            assertRows(execute("SELECT v FROM %s WHERE v >= 7 ORDER BY v ASC LIMIT 4"), row(7), row(8));
            assertRows(execute("SELECT v FROM %s WHERE v >= 10 ORDER BY v ASC LIMIT 4"));

            assertRows(execute("SELECT v FROM %s WHERE v >= -10 ORDER BY v DESC LIMIT 4"), row(8), row(7), row(6), row(5));
            assertRows(execute("SELECT v FROM %s WHERE v > 1 ORDER BY v DESC LIMIT 4"), row(8), row(7), row(6), row(5));
            assertRows(execute("SELECT v FROM %s WHERE v <= 3 ORDER BY v DESC LIMIT 4"), row(3), row(2), row(1));
            assertRows(execute("SELECT v FROM %s WHERE v >= 4 AND v <= 6 ORDER BY v DESC LIMIT 4"), row(6), row(5), row(4));
            assertRows(execute("SELECT v FROM %s WHERE v >= 7 ORDER BY v DESC LIMIT 4"), row(8), row(7));
            assertRows(execute("SELECT v FROM %s WHERE v >= 10 ORDER BY v DESC LIMIT 4"));
        });
    }

    private void testSelectionAndOrderByOnTheSameColumnWithLargeRowCount(boolean asc) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, x int, v int, PRIMARY KEY (pk, x))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        Object[][] rows = new Object[100][];
        var lowerBound = asc ? 0 : 4900;
        var upperBound = asc ? 100 : 5000;
        for (int i = 0; i < 10000; i++)
        {
            execute("INSERT INTO %s (pk, x, v) VALUES (?, ?, ?)", i, i, i);
            if (i >= lowerBound && i < upperBound)
            {
                var pos = asc ? i - lowerBound : upperBound - i - 1;
                rows[pos] = row(i);
            }
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT v FROM %s WHERE v < 5000 ORDER BY v " + (asc ? "ASC" : "DESC") + " LIMIT 100"), rows);
        });
    }


    /*
     * The following two tests show that we can correctly select and order by a column for which the table contains
     * sufficient rows to stress ranges within the backing data structure (e.g., BKDReader spanning multiple leaves).
     */
    @Test
    public void testSelectionAndOrderByOnTheSameColumnWithLargeRowCountAsc() throws Throwable
    {
        testSelectionAndOrderByOnTheSameColumnWithLargeRowCount(true);
    }

    @Test
    public void testSelectionAndOrderByOnTheSameColumnWithLargeRowCountDesc() throws Throwable
    {
        testSelectionAndOrderByOnTheSameColumnWithLargeRowCount(false);
    }

    @Test
    public void cannotHaveAggregationOnOrderByQuery()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v) VALUES (1, 4)");
        execute("INSERT INTO %s (k, v) VALUES (2, 3)");
        execute("INSERT INTO %s (k, v) VALUES (3, 2)");
        execute("INSERT INTO %s (k, v) VALUES (4, 1)");

        assertThatThrownBy(() -> execute("SELECT sum(v) FROM %s ORDER BY v LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT sum(v) FROM %s WHERE k = 1 ORDER BY v LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT * FROM %s GROUP BY k ORDER BY v LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT count(*) FROM %s ORDER BY v LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);
    }

    @Test
    public void testWidePartitionWithPkPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // write two partitions, one with increasing values and the other with decreasing values
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 3)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 2, 2)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 3, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 2, 2)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 3, 3)");

        String query = "SELECT c FROM %s WHERE k = ? ORDER BY v LIMIT ?";
        beforeAndAfterFlush(() ->
        {
            // query first partition with different limits
            assertRows(execute(query, 0, 100), row(3), row(2), row(1));
            assertRows(execute(query, 0, 3), row(3), row(2), row(1));
            assertRows(execute(query, 0, 2), row(3), row(2));
            assertRows(execute(query, 0, 1), row(3));

            // query second partition with different limits
            assertRows(execute(query, 1, 100), row(1), row(2), row(3));
            assertRows(execute(query, 1, 3), row(1), row(2), row(3));
            assertRows(execute(query, 1, 2), row(1), row(2));
            assertRows(execute(query, 1, 1), row(1));
        });
    }

    @Test
    public void testPlaningOnHybridQueries()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        String literalIndex = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        String numericIndex = createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 0, 'a', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 1, 'b', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 2, 'c', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 3, 'd', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 4, 'e', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 5, 'f', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 6, 'g', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 7, 'h', 1)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 8, 'i', 0)");
        execute("INSERT INTO %s (k, c, s, n) VALUES (0, 9, 'j', 0)");

        // hybrid query that prefers the filtering index due to selectivity, ascending order
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(8), row(9));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 WITH included_indexes = {" + numericIndex + '}',
                              Plan.NumericIndexScan.class,
                              row(8), row(9));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 WITH included_indexes = {" + literalIndex + '}',
                              Plan.LiteralIndexScan.class,
                              row(8), row(9));
        assertInvalidMessage(IndexContext.MULTIPLE_HINTS_WITH_ORDER_BY_ERROR_MESSAGE,
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 WITH included_indexes = {" + numericIndex + ',' + literalIndex + '}');
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 WITH excluded_indexes = {" + numericIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 WITH excluded_indexes = {" + literalIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 WITH excluded_indexes = {" + numericIndex + ',' + literalIndex + '}');

        // hybrid query that prefers the ordering index due to selectivity, ascending order
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5",
                              Plan.LiteralIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5 WITH included_indexes = {" + numericIndex + '}',
                              Plan.NumericIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5 WITH included_indexes = {" + literalIndex + '}',
                              Plan.LiteralIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));
        assertInvalidMessage(IndexContext.MULTIPLE_HINTS_WITH_ORDER_BY_ERROR_MESSAGE,
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5 WITH included_indexes = {" + numericIndex + ',' + literalIndex + '}');
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5 WITH excluded_indexes = {" + numericIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5 WITH excluded_indexes = {" + literalIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s ASC LIMIT 5 WITH excluded_indexes = {" + numericIndex + ',' + literalIndex + '}');

        // hybrid query that prefers the filtering index due to selectivity, descending order
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(9), row(8));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5 WITH included_indexes = {" + numericIndex + '}',
                              Plan.NumericIndexScan.class,
                              row(9), row(8));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5 WITH included_indexes = {" + literalIndex + '}',
                              Plan.LiteralIndexScan.class,
                              row(9), row(8));
        assertInvalidMessage(IndexContext.MULTIPLE_HINTS_WITH_ORDER_BY_ERROR_MESSAGE,
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5 WITH included_indexes = {" + numericIndex + ',' + literalIndex + '}');
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5 WITH excluded_indexes = {" + numericIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5 WITH excluded_indexes = {" + literalIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 0 ORDER BY s DESC LIMIT 5 WITH excluded_indexes = {" + numericIndex + ',' + literalIndex + '}');

        // hybrid query that prefers the ordering index due to selectivity, descending order
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5",
                              Plan.LiteralIndexScan.class,
                              row(7), row(6), row(5), row(4), row(3));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5 WITH included_indexes = {" + numericIndex + '}',
                              Plan.NumericIndexScan.class,
                              row(7), row(6), row(5), row(4), row(3));
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5 WITH included_indexes = {" + literalIndex + '}',
                              Plan.LiteralIndexScan.class,
                              row(7), row(6), row(5), row(4), row(3));
        assertInvalidMessage(IndexContext.MULTIPLE_HINTS_WITH_ORDER_BY_ERROR_MESSAGE,
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5 WITH included_indexes = {" + numericIndex + ',' + literalIndex + '}');
        assertInvalidMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5 WITH excluded_indexes = {" + numericIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5 WITH excluded_indexes = {" + literalIndex + '}');
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 's'),
                             "SELECT c FROM %s WHERE n = 1 ORDER BY s DESC LIMIT 5 WITH excluded_indexes = {" + numericIndex + ',' + literalIndex + '}');
    }

    /**
     * Regression test for CNDB-19058 / CNDB-15622.
     *
     * A search-then-sort generic ORDER BY hybrid query whose WHERE predicate is on a static column and whose ORDER BY
     * is on a regular column must return the correct rows after flush.  Before the fix, {@code Slice.make(clustering)}
     * was called with {@code STATIC_CLUSTERING} or {@code EMPTY}, which is invalid for regular column fetches, causing
     * an NPE, AssertionError, or 0 results after flush.
     */
    @Test
    public void testHybridQueryWithStaticPredicateAndGenericOrder()
    {
        // Disable query optimiser to ensure the search-then-sort execution path is exercised
        QueryController.QUERY_OPT_LEVEL = 0;

        createTable("CREATE TABLE %s (k int, c int, s text static, r int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 1, 'target', 1)");
        execute("INSERT INTO %s (k, c, s, r) VALUES (2, 1, 'target', 2) USING TIMESTAMP 1");
        // Enough non-matching partitions to trigger search-then-sort
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, c, s, r) VALUES (?, 1, 'other', ?)", i, i);

        String select = "SELECT k, c FROM %s WHERE s = ? ORDER BY r LIMIT 2";

        // Must work in memtable state
        assertRows(execute(select, "target"), row(1, 1), row(2, 1));

        // Must still work after flush – this is the case that broke before the fix
        flush();
        assertRows(execute(select, "target"), row(1, 1), row(2, 1));

        // And after compaction
        compact();
        assertRows(execute(select, "target"), row(1, 1), row(2, 1));
    }

    /**
     * Regression test for CNDB-15622: static column predicate with generic ORDER BY on a wide partition with
     * multiple clustering rows.  The query must not produce 0 results / crash after flush.
     * At least one clustering row from the matching partition must be returned.
     */
    @Test
    public void testHybridQueryWithStaticPredicateMultipleClusteringsAndGenericOrder()
    {
        QueryController.QUERY_OPT_LEVEL = 0;

        createTable("CREATE TABLE %s (k int, c int, s text static, r int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // One matching partition with two clustering rows
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 1, 'target', 10)");
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 2, 'target', 5)");
        // Non-matching partitions to trigger search-then-sort
        for (int i = 2; i < 100; i++)
            execute("INSERT INTO %s (k, c, s, r) VALUES (?, 1, 'other', ?)", i, i * 100);

        // At least one row from k=1 must be returned before and after flush
        String select = "SELECT k FROM %s WHERE s = ? ORDER BY r LIMIT 5";
        assertRowCount(execute(select, "target"), 1);

        flush();
        assertRowCount(execute(select, "target"), 1);
    }

    /**
     * Covers the path in IndexSearcher where a row exists in the partition but has no cell value
     * for the ORDER BY column (cell == null).  That row must be skipped gracefully, not crash.
     */
    @Test
    public void testOrderByColumnWithNullCellSkipped()
    {
        QueryController.QUERY_OPT_LEVEL = 0;

        createTable("CREATE TABLE %s (k int, c int, s text static, r int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // k=1 has a matching static value; c=1 has r set, c=2 does not (r is null → cell absent)
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 1, 'target', 10)");
        execute("INSERT INTO %s (k, c, s) VALUES (1, 2, 'target')");  // no r value
        for (int i = 2; i < 100; i++)
            execute("INSERT INTO %s (k, c, s, r) VALUES (?, 1, 'other', ?)", i, i);

        String select = "SELECT k, c FROM %s WHERE s = ? ORDER BY r LIMIT 2";

        // The row with null r must be skipped; only (1,1) should come back
        assertRows(execute(select, "target"), row(1, 1));

        flush();
        assertRows(execute(select, "target"), row(1, 1));
    }
}
