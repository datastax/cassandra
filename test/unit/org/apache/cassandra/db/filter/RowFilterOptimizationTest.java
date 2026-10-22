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

package org.apache.cassandra.db.filter;

import java.util.Arrays;
import javax.annotation.Nullable;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

///
/// Tests for the optimization of row filters built from restrictions that:
/// * Replaces conjunctions with always-false branches by an optimized always-false filter element.
/// * Replaces disjunctions with always-true branches by an optimized always-true filter element.
/// * Removes always-true branches from conjunctions.
/// * Removes always-false branches from disjunctions.
/// * Removes unnecessary conjunction/disjunction nesting.
///
@RunWith(Parameterized.class)
public class RowFilterOptimizationTest extends CQLTester
{
    @Parameterized.Parameter
    public boolean indexed;

    @Parameterized.Parameters(name = "indexed={0}")
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList(new Object[] {false}, new Object[] {true});
    }

    @Test
    public void testOptimize()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int)");
        if (indexed)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        }

        for (int i = -10; i < 10; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i, i);
        }

        // without any row filter (nothing to optimize)
        assertIsNotEmptyQuery("SELECT * FROM %s");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k = 0");

        // with a single-restriction row filter (nothing to optimize)
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0");

        // with multiple expressions in the same column that don't neeed optimization
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 AND a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1 AND a < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1 AND a <= 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 1 AND a < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 1 AND a <= 2");

        // with multiple expressions in the same column that do neeed optimization
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 0 AND a < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 0 AND a <= 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a < 1 AND a > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2 AND a < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a < 1 AND a >= 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2 AND a <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 1 AND a > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 2 AND a < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 1 AND a >= 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 2 AND a <= 1");

        // unsatisfiable conjunctions should be removed from conjunctions, even though the entire query is not empty
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b < 1 AND b > 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b < 1 AND b >= 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b <= 1 AND b >= 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b <= 1 AND b >= 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b < 1 AND b > 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b < 1 AND b >= 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b <= 1 AND b >= 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b <= 1 AND b >= 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b < 1 AND b > 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b < 1 AND b >= 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b <= 1 AND b >= 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b <= 1 AND b >= 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b < 1 AND b > 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b < 1 AND b >= 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b <= 1 AND b >= 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b <= 1 AND b >= 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b < 1 AND b > 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b < 1 AND b >= 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b <= 1 AND b >= 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b <= 1 AND b >= 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a <= 0", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a <= 0", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a <= 0", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a <= 0", "a <= 0");

        // Fragmented intervals
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a > 0 AND a < 2) OR (a > 4 AND a < 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a > 0 OR a < 2) AND (a > 4 OR a < 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a < 0 AND a > 2) OR (a > 4 AND a < 6)", "a > 4 AND a < 6");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a > 0 AND a < 2) OR (a < 4 AND a > 6)", "a > 0 AND a < 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a < 0 AND a > 2) OR (a < 4 AND a > 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a < 0 OR a > 2) AND (a < 4 OR a > 6)");

        // some more convoluted cases
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a < 1 AND b > 1) AND (a > 2 AND b < 2)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a > 2 AND b < 2) AND (a < 1 AND b > 1)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (b > 2 AND b < 1) OR (a > 2 AND a < 1)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a > 2 AND a < 1) OR (b > 2 AND b < 1)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR (a = 0 AND b = 0)", "a = 0 AND b = 0", "b = 0 AND a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a = 0 AND b = 0) OR (b < 1 AND b > 2)", "a = 0 AND b = 0", "b = 0 AND a = 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 0 AND a != 1");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 AND (b = 0 OR (c < 1 AND c > 2))", "a = 0 AND (b = 0)");

        // Similar cases that don't need simplification because they are forbidden by pre-binding validation
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT * FROM %s WHERE a = 0 AND a != 0");
        assertInvalidMessage("Column \"a\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT * FROM %s WHERE a NOT IN (0, 1) AND a IN (1, 2)");
        assertInvalidMessage("Column \"a\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT * FROM %s WHERE a > 0 AND a IN (1, 2)");
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE a > 0 AND a < 2 AND a > 4 AND a < 6");
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE (a > 0 AND a < 2) AND (a > 4 AND a < 6)");

        /*
         * TODO: further row filter simplifications could be added at some point:
         *  assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 AND (a != 0 OR b = 1)", "a = 0 AND b = 1");
         *  assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (a != 0 AND b = 1)", "a = 0 OR b = 1");
         */
    }

    @Test
    public void testOptimizePartitionKey()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, a int, PRIMARY KEY ((k1, k2), c))");
        if (indexed)
            createIndex("CREATE CUSTOM INDEX ON %s(k1) USING 'StorageAttachedIndex'");

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k1 > 1 AND k1 < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k1 >= 1 AND k1 <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE k1 < 1 AND k1 > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE k1 >= 0 AND k1 < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE k1 > 0 AND k1 <= 0");
    }

    @Test
    public void testOptimizeClusteringKey()
    {
        testOptimizeClusteringKey(Ordering.Direction.ASC);
        testOptimizeClusteringKey(Ordering.Direction.DESC);
    }

    private void testOptimizeClusteringKey(Ordering.Direction direction)
    {
        createTable("CREATE TABLE %s (k int, c int, a int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c " + direction + ')');
        if (indexed)
            createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE c > 1 AND c < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE c >= 1 AND c <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE c < 1 AND c > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE c >= 0 AND c < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE c > 0 AND c <= 0");
    }

    @Test
    public void testOptimizeBigint()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a bigint, b bigint)");
        if (indexed)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        }

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1 AND a < 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2 AND a < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 0 AND a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b < 1 AND b > 2)", "a = 0");
    }

    @Test
    public void testOptimizeFloat()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a float, b float)");
        if (indexed)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        }

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1.0 AND a < 2.0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2.0 AND a < 1.0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 0.0 AND a < 0.0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0.0 OR (b < 1.0 AND b > 2.0)", "a = 0.0");
    }

    @Test
    public void testOptimizeText()
    {
        Assume.assumeFalse("SAI doesn't support ranges on text columns", indexed);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a text, b text)");

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 'bar' AND a < 'foo'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 'foo' AND a < 'bar'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 'foo' AND a < 'foo'");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 'hi' OR (b < 'bar' AND b > 'foo')", "a = 'hi'");
    }

    @Test
    public void testOptimizeDate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a date, b date)");
        if (indexed)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        }

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > '2000-01-01' AND a < '2099-12-31'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > '2099-12-31' AND a < '2000-01-01'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= '2000-01-01' AND a < '2000-01-01'");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = '2025-01-01' OR (b < '2000-01-01' AND b > '2099-12-31')", "a = '2025-...");
    }

    /// Verifies that the specified query produces an empty row filter.
    ///
    /// @param query A CQL query.
    private void assertIsEmptyQuery(String query)
    {
        query = maybeAddAllowFiltering(query);

        assertEmpty(execute(query));

        ReadQuery readQuery = parseReadQuery(query);
        Assertions.assertThat(readQuery.isEmpty()).isTrue();

        RowFilter rowFilter = readQuery.rowFilter();
        Assertions.assertThat(rowFilter)
                  .isEqualTo(RowFilter.ALL)
                  .matches(RowFilter::isEmpty);

        // the unsatisfiable row filter shouldn't be sent to replicas; the serializer should reject it.
        Assertions.assertThatThrownBy(() -> RowFilter.serializer.serialize(rowFilter, new DataOutputBuffer(), MessagingService.current_version))
                  .isInstanceOf(AssertionError.class);
    }

    /// Verifies that the specified query produces a not empty row filter.
    ///
    /// @param query A CQL query.
    private void assertIsNotEmptyQuery(String query)
    {
        assertIsNotEmptyQuery(query, (String[]) null);
    }

    /// Verifies that the specified query produces a not empty row filter,
    /// with one of the specified CQL representations.
    ///
    /// @param query A CQL query.
    /// @param optimizedFilters The acceptable CQL representations of the row optimized filter.
    /// There can be more than one because the order of the restrictions is not deterministic.
    private void assertIsNotEmptyQuery(String query, @Nullable String... optimizedFilters)
    {
        query = maybeAddAllowFiltering(query);

        ReadQuery readQuery = parseReadQuery(query);
        Assertions.assertThat(readQuery.isEmpty()).isFalse();

        RowFilter rowFilter = readQuery.rowFilter();
        Assertions.assertThat(rowFilter)
                  .matches(f -> f != RowFilter.ALL);

        if (optimizedFilters != null)
            Assertions.assertThat(rowFilter.toCQLString(Redaction.NONE)).isIn((Object[]) optimizedFilters);
    }

    private String maybeAddAllowFiltering(String query)
    {
        return indexed ? query : query + " ALLOW FILTERING";
    }
}
