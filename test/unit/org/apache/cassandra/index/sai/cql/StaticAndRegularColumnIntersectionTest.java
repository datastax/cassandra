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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests SAI queries with inequality conditions on both static and regular indexed columns.
/// This test creates a table with one indexed static column and one indexed regular column,
/// generates random data once for all tests, and verifies that queries with various WHERE
/// clause combinations return correct results.
///
/// This test is parameterized to run with both AA (earliest), default and the latest SAI version.
@RunWith(Parameterized.class)
public class StaticAndRegularColumnIntersectionTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Stream.of(Version.AA, Version.Selector.DEFAULT_VERSION, Version.LATEST)
                     .map(v -> new Object[]{ v })
                     .collect(Collectors.toList());
    }

    private static final int QUERIES_PER_TEST = 25;
    private static final int NUM_ROWS = 1000;

    // Cache all rows for efficient filtering in getExpectedRows
    private Set<RowData> allRowsCache;

    @Before
    public void setup()
    {
        SAIUtil.setCurrentVersion(version);
        disableQueryOptimization();

        createTable("CREATE TABLE %s (pk int, ck int, s int static, v int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // Pre-generate all data deterministically in a single thread
        Random random = new Random(0);
        List<RowData> dataToInsert = new ArrayList<>(NUM_ROWS);
        for (int i = 0; i < NUM_ROWS; i++)
        {
            int pk = random.nextInt(100);
            int ck = random.nextInt(10);
            int s = random.nextInt(10);
            int v = random.nextInt(10);
            dataToInsert.add(new RowData(pk, ck, s, v));
        }

        // Insert pre-generated data in parallel for better performance.
        // Note that the latest written value of s into the same partition wins because s is static,
        // so later inserts may modify the rows inserted earlier to the same partition.
        IntStream.range(0, NUM_ROWS).parallel().forEach(i -> {
            RowData row = dataToInsert.get(i);
            execute("INSERT INTO %s (pk, ck, s, v) VALUES (?, ?, ?, ?)", row.pk, row.ck, row.s, row.v);
        });

        // Cache all rows once for efficient filtering in getExpectedRows.
        // This avoids having to query C* for all rows for each test, which would be inefficient.
        // We're getting the data back from C* instead of using `dataToInsert` here because
        // a subsequent write to the same partition could change the static column value (s)
        // of some rows inserted earlier.
        allRowsCache = new HashSet<>();
        UntypedResultSet allRows = execute("SELECT pk, ck, s, v FROM %s");
        for (UntypedResultSet.Row row : allRows)
        {
            int pk = row.getInt("pk");
            int ck = row.getInt("ck");
            int s = row.getInt("s");
            int v = row.getInt("v");
            allRowsCache.add(new RowData(pk, ck, s, v));
        }
    }

    @Test
    public void testIntersectionQueries() throws Throwable
    {
        beforeAndAfterFlush(() -> {

            repeat(1, random -> {
                int vExclude = random.nextInt(15);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v != vExclude && row.s > sThreshold,
                                      "v != ? AND s > ?",
                                      vExclude, sThreshold);
            });

            repeat(2, random -> {
                int vExclude = random.nextInt(15);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v != vExclude && row.s < sThreshold,
                                      "v != ? AND s < ?",
                                      vExclude, sThreshold);
            });

            repeat(3, random -> {
                int vExclude = random.nextInt(15);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v != vExclude && row.s <= sThreshold,
                                      "v != ? AND s <= ?",
                                      vExclude, sThreshold);
            });

            repeat(4, random -> {
                int vExclude = random.nextInt(15);
                int sThreshold = random.nextInt(11);
                testIntersectionQuery(row -> row.v != vExclude && row.s >= sThreshold,
                                      "v != ? AND s >= ?",
                                      vExclude, sThreshold);
            });

            repeat(5, random -> {
                int vValue = random.nextInt(10);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v == vValue && row.s < sThreshold,
                                      "v = ? AND s < ?",
                                      vValue, sThreshold);
            });

            repeat(6, random -> {
                int vThreshold = random.nextInt(10);
                int sValue = random.nextInt(10);
                testIntersectionQuery(row -> row.v > vThreshold && row.s == sValue,
                                      "v > ? AND s = ?",
                                      vThreshold, sValue);
            });

            repeat(7, random -> {
                int vThreshold = random.nextInt(10);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v <= vThreshold && row.s >= sThreshold,
                                      "v <= ? AND s >= ?",
                                      vThreshold, sThreshold);
            });

            repeat(8, random -> {
                int vMin = random.nextInt(5);
                int vMax = vMin + 2 + random.nextInt(5);
                int sMin = random.nextInt(5);
                int sMax = sMin + 2 + random.nextInt(5);
                testIntersectionQuery(row -> row.v > vMin && row.v < vMax && row.s > sMin && row.s < sMax,
                                      "v > ? AND v < ? AND s > ? AND s < ?",
                                      vMin, vMax, sMin, sMax);
            });

            repeat(9, random -> {
                int vThreshold = random.nextInt(10);
                int sExclude = random.nextInt(10);
                testIntersectionQuery(row -> row.v > vThreshold && row.s != sExclude,
                                      "v > ? AND s != ?",
                                      vThreshold, sExclude);
            });

            repeat(10, random -> {
                int vExclude = random.nextInt(10);
                int sExclude = random.nextInt(10);
                testIntersectionQuery(row -> row.v != vExclude && row.s != sExclude,
                                      "v != ? AND s != ?",
                                      vExclude, sExclude);
            });

            repeat(11, random -> {
                int vValue = random.nextInt(10);
                int sValue = random.nextInt(10);
                testIntersectionQuery(row -> row.v == vValue && row.s == sValue,
                                      "v = ? AND s = ?",
                                      vValue, sValue);
            });

            repeat(12, random -> {
                int vThreshold = random.nextInt(10);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v < vThreshold && row.s < sThreshold,
                                      "v < ? AND s < ?",
                                      vThreshold, sThreshold);
            });

            repeat(13, random -> {
                int vThreshold = random.nextInt(10);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v > vThreshold && row.s > sThreshold,
                                      "v > ? AND s > ?",
                                      vThreshold, sThreshold);
            });

            repeat(14, random -> {
                int vThreshold = random.nextInt(10);
                int sExclude = random.nextInt(10);
                testIntersectionQuery(row -> row.v < vThreshold && row.s != sExclude,
                                      "v < ? AND s != ?",
                                      vThreshold, sExclude);
            });

            repeat(15, random -> {
                int vValue = random.nextInt(10);
                int sExclude = random.nextInt(10);
                testIntersectionQuery(row -> row.v == vValue && row.s != sExclude,
                                      "v = ? AND s != ?",
                                      vValue, sExclude);
            });

            repeat(16, random -> {
                int vExclude = random.nextInt(10);
                int sValue = random.nextInt(10);
                testIntersectionQuery(row -> row.v != vExclude && row.s == sValue,
                                      "v != ? AND s = ?",
                                      vExclude, sValue);
            });

            repeat(17, random -> {
                int vThreshold = random.nextInt(10);
                int sThreshold = random.nextInt(10);
                testIntersectionQuery(row -> row.v >= vThreshold && row.s <= sThreshold,
                                      "v >= ? AND s <= ?",
                                      vThreshold, sThreshold);
            });

            repeat(18, random -> {
                int vMin = random.nextInt(5);
                int vMax = vMin + 2 + random.nextInt(5);
                int sExclude = random.nextInt(10);
                testIntersectionQuery(row -> row.v > vMin && row.v < vMax && row.s != sExclude,
                                      "v > ? AND v < ? AND s != ?",
                                      vMin, vMax, sExclude);
            });

            repeat(19, random -> {
                int vMin = random.nextInt(5);
                int vMax = vMin + 2 + random.nextInt(5);
                int sValue = random.nextInt(10);
                testIntersectionQuery(row -> row.v >= vMin && row.v <= vMax && row.s == sValue,
                                      "v >= ? AND v <= ? AND s = ?",
                                      vMin, vMax, sValue);
            });
        });
    }

    private void repeat(long seed, Consumer<Random> tester)
    {
        Random random = new Random(seed);
        for (int i = 0; i < QUERIES_PER_TEST; i++)
        {
            tester.accept(random);
        }
    }

    /// Execute and verify a single intersection query.
    ///
    /// @param filter filter to apply to all rows to obtain the reference results used for validating the query response
    /// @param whereClause the CQL condition for the WHERE clause for the indexed query; must match the filter
    /// @param params the parameters to bind to the WHERE clause
    private void testIntersectionQuery(Predicate<RowData> filter, String whereClause, Object... params)
    {
        Set<RowData> expectedRows = getExpectedRows(filter);

        // Execute the query using indexes
        UntypedResultSet result = execute("SELECT pk, ck, s, v FROM %s WHERE " + whereClause, params);

        // Collect actual results
        Set<RowData> actualRows = new HashSet<>();
        for (UntypedResultSet.Row row : result)
        {
            int pk = row.getInt("pk");
            int ck = row.getInt("ck");
            int v = row.getInt("v");
            int s = row.getInt("s");

            RowData rowData = new RowData(pk, ck, s, v);
            actualRows.add(rowData);
            // Assert the row is in expected results
            assertThat(expectedRows)
                .as("Unexpected row (filter condition: %s with params %s), but got v=%d, s=%d for pk=%d, ck=%d",
                    whereClause, Arrays.toString(params), v, s, pk, ck)
                .contains(rowData);
        }

        // Assert that we got all expected rows (no missing rows)
        Set<RowData> missingRows = new HashSet<>(expectedRows);
        missingRows.removeAll(actualRows);
        assertThat(actualRows.size())
            .as("Expected %d rows but got %d rows for filter condition %s with params %s. Missing rows: %s",
                expectedRows.size(), actualRows.size(), whereClause, Arrays.toString(params), missingRows)
            .isEqualTo(expectedRows.size());
    }

    /// Helper method to get expected rows by filtering cached data client-side.
    /// This avoids using server-side indexes to ensure we have a correct baseline for comparison.
    /// The cache is populated once in setup() for efficiency.
    private Set<RowData> getExpectedRows(Predicate<RowData> filter)
    {
        Set<RowData> expected = new HashSet<>();
        for (RowData rowData : allRowsCache)
        {
            if (filter.test(rowData))
            {
                expected.add(rowData);
            }
        }
        return expected;
    }

    // Helper class to store row data for verification
    private static class RowData
    {
        final int pk;
        final int ck;
        final int s;
        final int v;

        RowData(int pk, int ck, int s, int v)
        {
            this.pk = pk;
            this.ck = ck;
            this.s = s;
            this.v = v;
        }
        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowData rowData = (RowData) o;
            return pk == rowData.pk && ck == rowData.ck && s == rowData.s && v == rowData.v;
        }
        @Override
        public int hashCode()
        {
            return 31 * pk + ck;
        }
        @Override
        public String toString()
        {
            return String.format("Row(pk=%d, ck=%d, s=%d, v=%d)", pk, ck, s, v);
        }
    }
}
