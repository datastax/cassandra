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

package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.marshal.Redaction;
import org.assertj.core.api.Assertions;

/// Tests for the simplification of row filters built from restrictions that applies the absorption law
/// and removes the expressions that contradict the ones in the parent conjunction (see [RowFilterSimplifier]).
///
/// Every query is checked against a model of the rows that includes null values, to verify that the simplification
/// doesn't change the results. This is particularly relevant because `a != 0` is not satisfied by null values,
/// so it is not the negation of `a = 0`.
@RunWith(Parameterized.class)
public class RowFilterAbsorptionTest extends CQLTester
{
    private static final Integer[] VALUES = { null, 0, 1, 2 };

    /// Whether to index the queried columns.
    @Parameterized.Parameter
    public boolean index;

    /// Whether to flush the table data before querying.
    @Parameterized.Parameter(1)
    public boolean flush;

    @Parameterized.Parameters(name = "index={0} flush={1}")
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList(new Object[]{ false, false },
                             new Object[]{ false, true },
                             new Object[]{ true, false },
                             new Object[]{ true, true });
    }

    /// The model of a row in the test table.
    private static class Row
    {
        final int k;
        final Integer a;
        final Integer b;
        final Integer c;

        Row(int k, Integer a, Integer b, Integer c)
        {
            this.k = k;
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }

    private final List<Row> rows = new ArrayList<>();

    private void createAndPopulateTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int)");

        if (index)
        {
            for (String column : new String[]{ "a", "b", "c" })
                createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        }

        // all the combinations of null and non-null values in the columns, so there are rows with null values
        int k = 0;
        for (Integer a : VALUES)
        {
            for (Integer b : VALUES)
            {
                for (Integer c : VALUES)
                    rows.add(new Row(k++, a, b, c));
            }
        }

        for (Row row : rows)
        {
            // don't write the null values, to avoid creating tombstones
            List<String> columns = new ArrayList<>(List.of("k"));
            List<Object> values = new ArrayList<>(List.of(row.k));
            if (row.a != null) { columns.add("a"); values.add(row.a); }
            if (row.b != null) { columns.add("b"); values.add(row.b); }
            if (row.c != null) { columns.add("c"); values.add(row.c); }

            execute("INSERT INTO %s (" + String.join(", ", columns) + ") VALUES (" + "?, ".repeat(columns.size() - 1) + "?)",
                    values.toArray());
        }

        if (flush)
            flush();
    }

    // The null-aware evaluation of the relations, as the row filter does it: nothing is satisfied by a null value.
    private static boolean eq(Integer value, int x) { return value != null && value == x; }
    private static boolean neq(Integer value, int x) { return value != null && value != x; }
    private static boolean lt(Integer value, int x) { return value != null && value < x; }
    private static boolean gt(Integer value, int x) { return value != null && value > x; }

    @Test
    public void testAbsorption()
    {
        createAndPopulateTable();

        // x AND (x OR y) -> x
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a = 0 OR b = 1)",
                    r -> eq(r.a, 0),
                    "a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (b = 1 OR a = 0)",
                    r -> eq(r.a, 0),
                    "a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a = 0 OR b = 1 OR c = 1)",
                    r -> eq(r.a, 0),
                    "a = 0");
        assertQuery("SELECT k FROM %s WHERE a > 0 AND (a > 0 OR b = 1)",
                    r -> gt(r.a, 0),
                    "a > 0");
        assertQuery("SELECT k FROM %s WHERE a != 0 AND (a != 0 OR b = 1)",
                    r -> neq(r.a, 0),
                    "a != 0");

        // x OR (x AND y) -> x
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a = 0 AND b = 1)",
                    r -> eq(r.a, 0),
                    "a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (b = 1 AND a = 0)",
                    r -> eq(r.a, 0),
                    "a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a = 0 AND b = 1 AND c = 1)",
                    r -> eq(r.a, 0),
                    "a = 0");
        assertQuery("SELECT k FROM %s WHERE a < 1 OR (a < 1 AND b = 1)",
                    r -> lt(r.a, 1),
                    "a < 1");
        assertQuery("SELECT k FROM %s WHERE a != 0 OR (a != 0 AND b = 1)",
                    r -> neq(r.a, 0),
                    "a != 0");

        // the absorbed child is removed, but the other children and expressions are kept
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a = 0 OR b = 1) AND (b = 1 OR c = 1)",
                    r -> eq(r.a, 0) && (eq(r.b, 1) || eq(r.c, 1)),
                    "a = 0 AND (b = 1 OR c = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a = 0 AND b = 1) OR (b = 1 AND c = 1)",
                    r -> eq(r.a, 0) || (eq(r.b, 1) && eq(r.c, 1)),
                    "a = 0 OR (b = 1 AND c = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND b = 1 AND (a = 0 OR c = 1)",
                    r -> eq(r.a, 0) && eq(r.b, 1),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");

        // absorption is not limited to simple comparisons
        assertQuery("SELECT k FROM %s WHERE a IN (0, 1) AND (a IN (0, 1) OR b = 1)",
                    r -> eq(r.a, 0) || eq(r.a, 1));
        assertQuery("SELECT k FROM %s WHERE a IN (0, 1) OR (a IN (0, 1) AND b = 1)",
                    r -> eq(r.a, 0) || eq(r.a, 1));
        assertQuery("SELECT k FROM %s WHERE a NOT IN (0, 1) OR (a NOT IN (0, 1) AND b = 1)",
                    r -> r.a != null && !eq(r.a, 0) && !eq(r.a, 1));

        // different expressions are not absorbed
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a = 1 OR b = 1)",
                    r -> eq(r.a, 0) && (eq(r.a, 1) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a = 1 AND b = 1)",
                    r -> eq(r.a, 0) || (eq(r.a, 1) && eq(r.b, 1)),
                    "a = 0 OR (a = 1 AND b = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (b = 0 AND c = 1)",
                    r -> eq(r.a, 0) || (eq(r.b, 0) && eq(r.c, 1)),
                    "a = 0 OR (b = 0 AND c = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (b = 0 OR c = 1)",
                    r -> eq(r.a, 0) && (eq(r.b, 0) || eq(r.c, 1)),
                    "a = 0 AND (b = 0 OR c = 1)");
    }

    @Test
    public void testContradiction()
    {
        createAndPopulateTable();

        // the issue's example: the contradicting expression is removed from the disjunction
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR b = 1)",
                    r -> eq(r.a, 0) && (neq(r.a, 0) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (b = 1 OR a != 0)",
                    r -> eq(r.a, 0) && (eq(r.b, 1) || neq(r.a, 0)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");
        assertQuery("SELECT k FROM %s WHERE (a != 0 OR b = 1) AND a = 0",
                    r -> eq(r.a, 0) && (neq(r.a, 0) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");

        // equalities with different values
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a = 1 OR b = 1)",
                    r -> eq(r.a, 0) && (eq(r.a, 1) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");

        // equality with slices that don't contain the value
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a > 0 OR b = 1)",
                    r -> eq(r.a, 0) && (gt(r.a, 0) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a < 0 OR b = 1)",
                    r -> eq(r.a, 0) && (lt(r.a, 0) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");
        assertQuery("SELECT k FROM %s WHERE a = 1 AND (a < 1 OR b = 1)",
                    r -> eq(r.a, 1) && (lt(r.a, 1) || eq(r.b, 1)),
                    "a = 1 AND b = 1", "b = 1 AND a = 1");

        // slices that define an empty range
        assertQuery("SELECT k FROM %s WHERE a < 1 AND (a > 2 OR b = 1)",
                    r -> lt(r.a, 1) && (gt(r.a, 2) || eq(r.b, 1)),
                    "a < 1 AND b = 1", "b = 1 AND a < 1");
        assertQuery("SELECT k FROM %s WHERE a > 2 AND (a < 1 OR b = 1)",
                    r -> gt(r.a, 2) && (lt(r.a, 1) || eq(r.b, 1)),
                    "a > 2 AND b = 1", "b = 1 AND a > 2");
        assertQuery("SELECT k FROM %s WHERE a < 1 AND (a >= 1 OR b = 1)",
                    r -> lt(r.a, 1) && (r.a != null && r.a >= 1 || eq(r.b, 1)),
                    "a < 1 AND b = 1", "b = 1 AND a < 1");

        // only some of the expressions of the disjunction contradict the parent
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR b = 1 OR c = 1)",
                    r -> eq(r.a, 0) && (neq(r.a, 0) || eq(r.b, 1) || eq(r.c, 1)),
                    "a = 0 AND (b = 1 OR c = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR a = 1 OR b = 1)",
                    r -> eq(r.a, 0) && (neq(r.a, 0) || eq(r.a, 1) || eq(r.b, 1)),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");

        // several contradicting parent expressions and several affected disjunctions
        assertQuery("SELECT k FROM %s WHERE a = 0 AND b = 1 AND (a != 0 OR c = 1) AND (b != 1 OR c = 2)",
                    r -> eq(r.a, 0) && eq(r.b, 1) && (neq(r.a, 0) || eq(r.c, 1)) && (neq(r.b, 1) || eq(r.c, 2)));

        // if all the expressions of the disjunction contradict the parent the whole query is empty
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR a = 1)",
                    r -> false);
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a > 0 OR a < 0)",
                    r -> false);

        // they are not contradictions if the values can be equal
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 1 OR b = 1)",
                    r -> eq(r.a, 0) && (neq(r.a, 1) || eq(r.b, 1)),
                    "a = 0 AND (a != 1 OR b = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a >= 0 OR b = 1)",
                    r -> eq(r.a, 0) && (r.a != null && r.a >= 0 || eq(r.b, 1)),
                    "a = 0 AND (a >= 0 OR b = 1)");
        assertQuery("SELECT k FROM %s WHERE a < 2 AND (a > 1 OR b = 1)",
                    r -> lt(r.a, 2) && (gt(r.a, 1) || eq(r.b, 1)),
                    "a < 2 AND (a > 1 OR b = 1)");
        assertQuery("SELECT k FROM %s WHERE a <= 1 AND (a >= 1 OR b = 1)",
                    r -> r.a != null && r.a <= 1 && (r.a >= 1 || eq(r.b, 1)),
                    "a <= 1 AND (a >= 1 OR b = 1)");

        // a null-safe check: the rows where the compared column is null are not returned
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR b = 1)",
                    r -> r.a != null && r.a == 0 && eq(r.b, 1));
    }

    /// The expression that is left in the disjunction is moved to the parent unless this would produce a combination
    /// of restrictions in the same column that CQL doesn't allow.
    @Test
    public void testLeftoverExpression()
    {
        createAndPopulateTable();

        // moved to the parent
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR b = 1)",
                    r -> eq(r.a, 0) && eq(r.b, 1),
                    "a = 0 AND b = 1", "b = 1 AND a = 0");

        // not moved to the parent if it's in a column with other restrictions, because CQL doesn't allow
        // multiple lower bounds (or multiple upper bounds) on the same column
        assertQuery("SELECT k FROM %s WHERE a > 0 AND (a < 0 OR a > 1)",
                    r -> gt(r.a, 0) && (lt(r.a, 0) || gt(r.a, 1)),
                    "a > 0 AND (a > 1)");
        assertQuery("SELECT k FROM %s WHERE a < 2 AND (a > 3 OR a < 1)",
                    r -> lt(r.a, 2) && (gt(r.a, 3) || lt(r.a, 1)),
                    "a < 2 AND (a < 1)");
        assertQuery("SELECT k FROM %s WHERE a != 0 AND (a = 0 OR a != 1)",
                    r -> neq(r.a, 0) && (eq(r.a, 0) || neq(r.a, 1)),
                    "a != 0 AND (a != 1)");

        // not moved to the parent if the disjunction has more than one expression left
        assertQuery("SELECT k FROM %s WHERE a = 0 AND (a != 0 OR b = 1 OR c = 1)",
                    r -> eq(r.a, 0) && (eq(r.b, 1) || eq(r.c, 1)),
                    "a = 0 AND (b = 1 OR c = 1)");
    }

    /// The simplification is not valid for disjunctions, because a null value doesn't satisfy an expression
    /// nor its opposite one. `a = 0 OR (a != 0 AND b = 1)` can't be replaced by `a = 0 OR b = 1`, because the latter
    /// accepts the rows with a null `a` and `b = 1`.
    @Test
    public void testNoSimplificationInDisjunctions()
    {
        createAndPopulateTable();

        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a != 0 AND b = 1)",
                    r -> eq(r.a, 0) || (neq(r.a, 0) && eq(r.b, 1)),
                    "a = 0 OR (a != 0 AND b = 1)");
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a = 1 AND b = 1)",
                    r -> eq(r.a, 0) || (eq(r.a, 1) && eq(r.b, 1)),
                    "a = 0 OR (a = 1 AND b = 1)");
        assertQuery("SELECT k FROM %s WHERE a < 1 OR (a >= 1 AND b = 1)",
                    r -> lt(r.a, 1) || (r.a != null && r.a >= 1 && eq(r.b, 1)),
                    "a < 1 OR (a >= 1 AND b = 1)");

        // the rows with null in the column are not returned
        assertQuery("SELECT k FROM %s WHERE a = 0 OR (a != 0 AND b = 1)",
                    r -> r.a != null && (r.a == 0 || eq(r.b, 1)));
    }

    /// Only the simple comparisons are checked for contradictions. `IN` is not, whereas `NOT IN` is split into
    /// several `!=` restrictions when the query is parsed.
    @Test
    public void testNotSimplifiedExpressions()
    {
        createAndPopulateTable();

        assertQuery("SELECT k FROM %s WHERE a IN (0, 1) AND (a = 2 OR b = 1)",
                    r -> (eq(r.a, 0) || eq(r.a, 1)) && (eq(r.a, 2) || eq(r.b, 1)),
                    "a IN [0, 1] AND (a = 2 OR b = 1)");
        assertQuery("SELECT k FROM %s WHERE a NOT IN (0, 1) AND (a = 2 OR b = 1)",
                    r -> neq(r.a, 0) && neq(r.a, 1) && (eq(r.a, 2) || eq(r.b, 1)),
                    "a != 0 AND a != 1 AND (a = 2 OR b = 1)");

        // NOT IN is split into several != restrictions, so each of them can contradict an equality
        assertQuery("SELECT k FROM %s WHERE a NOT IN (0, 1) AND (a = 0 OR b = 1)",
                    r -> neq(r.a, 0) && neq(r.a, 1) && (eq(r.a, 0) || eq(r.b, 1)),
                    "a != 0 AND a != 1 AND b = 1");
    }

    /// The model of a row in the test table with a clustering column. Clustering columns are never null.
    private static class ClusteringRow
    {
        final int k;
        final int ck;
        final Integer b;

        ClusteringRow(int k, int ck, Integer b)
        {
            this.k = k;
            this.ck = ck;
            this.b = b;
        }
    }

    private final List<ClusteringRow> clusteringRows = new ArrayList<>();

    private void createAndPopulateClusteringTable()
    {
        createTable("CREATE TABLE %s (k int, ck int, s int static, b int, PRIMARY KEY (k, ck))");

        if (index)
            createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        for (int k = 0; k < 2; k++)
        {
            for (int ck = 0; ck < 4; ck++)
            {
                for (Integer b : VALUES)
                {
                    // one row per ck and value of b, so the partitions are spread over several partition keys
                    int partition = k * 100 + (b == null ? 99 : b);
                    clusteringRows.add(new ClusteringRow(partition, ck, b));
                }
            }
        }

        for (ClusteringRow row : clusteringRows)
        {
            if (row.b == null)
                execute("INSERT INTO %s (k, ck, s) VALUES (?, ?, ?)", row.k, row.ck, 7);
            else
                execute("INSERT INTO %s (k, ck, s, b) VALUES (?, ?, ?, ?)", row.k, row.ck, 7, row.b);
        }

        // a partition that only has a static row, which must never be returned by the queries on clustering columns
        execute("INSERT INTO %s (k, s) VALUES (?, ?)", 1000, 7);

        if (flush)
            flush();
    }

    /// The tautology `ck = x OR ck != x` is only applied to clustering columns, which are never null.
    @Test
    public void testTautologyInClusteringColumns()
    {
        createAndPopulateClusteringTable();

        // x OR (!x AND y) -> x OR y
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck = 1 OR (ck != 1 AND b = 1)",
                              r -> r.ck == 1 || eq(r.b, 1),
                              "ck = 1 OR b = 1");
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck != 1 OR (ck = 1 AND b = 1)",
                              r -> r.ck != 1 || eq(r.b, 1),
                              "ck != 1 OR b = 1");

        // overlapping and touching slices
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck < 2 OR (ck >= 2 AND b = 1)",
                              r -> r.ck < 2 || eq(r.b, 1),
                              "ck < 2 OR b = 1");
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck < 2 OR (ck > 1 AND b = 1)",
                              r -> r.ck < 2 || eq(r.b, 1),
                              "ck < 2 OR b = 1");
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck != 1 OR (ck != 2 AND b = 1)",
                              r -> r.ck != 1 || eq(r.b, 1),
                              "ck != 1 OR b = 1");

        // slices that leave a gap or don't cover the excluded bound are not tautologies
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck < 2 OR (ck > 2 AND b = 1)",
                              r -> r.ck < 2 || (r.ck > 2 && eq(r.b, 1)),
                              "ck < 2 OR (ck > 2 AND b = 1)");
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck = 1 OR (ck = 2 AND b = 1)",
                              r -> r.ck == 1 || (r.ck == 2 && eq(r.b, 1)),
                              "ck = 1 OR (ck = 2 AND b = 1)");

        // the expression is only removed from the conjunction, the rest of it is kept
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck = 1 OR (ck != 1 AND b = 1 AND s = 7)",
                              r -> r.ck == 1 || eq(r.b, 1));

        // a top-level restriction on a clustering column is not part of the row filter, so it can't simplify it,
        // but the results must be the same
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck = 1 AND (ck != 1 OR b = 1)",
                              r -> r.ck == 1 && eq(r.b, 1));

        // the partitions that only have a static row are never returned
        assertClusteringQuery("SELECT k, ck FROM %s WHERE ck = 1 OR (ck != 1 AND s = 7)",
                              r -> true);
    }

    /// Verifies that the specified query on the table with a clustering column returns the rows of the model that
    /// satisfy the specified predicate, and optionally its row filter.
    private void assertClusteringQuery(String query, Predicate<ClusteringRow> expected, String... optimizedFilters)
    {
        Object[][] expectedRows = clusteringRows.stream()
                                                .filter(expected)
                                                .map(r -> new Object[]{ r.k, r.ck })
                                                .toArray(Object[][]::new);
        assertQuery(query, true, expectedRows, optimizedFilters);
    }

    /// Verifies that the specified query returns the rows of the model that satisfy the specified predicate,
    /// and that its row filter has one of the specified CQL representations, ignoring the order of the terms of each
    /// conjunction and disjunction.
    ///
    /// @param query a CQL query selecting the primary key
    /// @param expected the model of the rows that the query should return
    /// @param optimizedFilters the acceptable CQL representations of the optimized row filter.
    /// If it's empty the row filter is not checked.
    private void assertQuery(String query, Predicate<Row> expected, String... optimizedFilters)
    {
        Object[][] expectedRows = rows.stream()
                                      .filter(expected)
                                      .map(r -> new Object[]{ r.k })
                                      .toArray(Object[][]::new);
        assertQuery(query, false, expectedRows, optimizedFilters);
    }

    private void assertQuery(String query, boolean allowFiltering, Object[][] expectedRows, String... optimizedFilters)
    {
        List<String> variants = new ArrayList<>();
        if (index)
        {
            variants.add(allowFiltering ? query + " ALLOW FILTERING" : query);
            variants.add(query + " ALLOW FILTERING WITH excluded_indexes = {*}");
        }
        else
        {
            variants.add(query + " ALLOW FILTERING");
        }

        for (String variant : variants)
        {
            assertRowsIgnoringOrder(execute(variant), expectedRows);

            if (optimizedFilters.length > 0)
            {
                ReadQuery readQuery = parseReadQuery(variant);
                Assertions.assertThat(readQuery.isEmpty()).isFalse();
                String actual = normalize(readQuery.rowFilter().toCQLString(Redaction.NONE));
                Assertions.assertThat(actual).isIn((Object[]) Arrays.stream(optimizedFilters)
                                                                    .map(RowFilterAbsorptionTest::normalize)
                                                                    .toArray(String[]::new));
            }
        }
    }

    /// Sorts the terms of the conjunctions and disjunctions of the specified CQL filter, recursively, so
    /// that equivalent filters with a different order of terms have the same representation.
    private static String normalize(String filter)
    {
        for (String separator : new String[]{ " AND ", " OR " })
        {
            List<String> terms = splitAtDepthZero(filter, separator);
            if (terms.size() > 1)
                return terms.stream().map(RowFilterAbsorptionTest::normalize).sorted().collect(Collectors.joining(separator));
        }

        // a parenthesized group
        if (filter.startsWith("(") && filter.endsWith(")"))
            return "(" + normalize(filter.substring(1, filter.length() - 1)) + ")";

        return filter;
    }

    private static List<String> splitAtDepthZero(String s, String separator)
    {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);
            if (c == '(')
                depth++;
            else if (c == ')')
                depth--;
            else if (depth == 0 && s.startsWith(separator, i))
            {
                parts.add(s.substring(start, i));
                start = i + separator.length();
                i = start - 1;
            }
        }
        parts.add(s.substring(start));
        return parts;
    }
}
