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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the queries with the OR operator: the shapes that DSE Search queries need, and the columns they can restrict.
 */
@RunWith(Parameterized.class)
public class DisjunctionQueryTest extends SAITester
{
    private static final String SAI = "CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'";

    private static final String RELAXING_CRITERIA = "(tax = 't1' AND last = 'l1' AND dob = '1980-01-01') OR " +
                                                    "(tax = 't1' AND last = 'l1') OR " +
                                                    "(tax = 't1')";

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object> data()
    {
        return Stream.of(Version.AA, SAIUtil.currentVersion(), Version.LATEST).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public Version version;

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    private void createPeopleTable()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, tax text, last text, dob date)");
        createIndex(String.format(SAI, "tax"));
        createIndex(String.format(SAI, "last"));
        createIndex(String.format(SAI, "dob"));
    }

    @Test
    public void testBranchesRelaxingTheSameCriteria() throws Throwable
    {
        createPeopleTable();
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (1, 't1', 'l1', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (2, 't1', 'l1', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (3, 't1', 'l2', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (4, 't2', 'l1', '1980-01-01')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + RELAXING_CRITERIA),
                                                          row(1), row(2), row(3)));
    }

    @Test
    public void testBranchesWithDifferentValues() throws Throwable
    {
        createPeopleTable();
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (1, 't1', 'l1', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (2, 't1', 'l1', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (3, 't2', 'l2', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (4, 't2', 'l3', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (5, 't3', 'l4', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (6, 't4', 'l4', '1980-01-01')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " +
                                                                  "(tax = 't1' AND last = 'l1' AND dob = '1980-01-01') OR " +
                                                                  "(tax = 't2' AND last = 'l2') OR " +
                                                                  "(tax = 't3')"),
                                                          row(1), row(3), row(5)));
    }

    /**
     * A branch is redundant only if all of the conditions of another branch are among its own.
     */
    @Test
    public void testBranchesThatLookRedundant() throws Throwable
    {
        createPeopleTable();
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (1, 't1', 'l1', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (2, 't1', 'l2', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (3, 't2', 'l1', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (4, 't2', 'l2', '1980-01-01')");

        beforeAndAfterFlush(() -> {
            // no branch includes the conditions of the other
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR (last = 'l1' AND dob = '1990-01-01')"),
                                    row(1), row(3));
            // same branches
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tax = 't1' OR tax = 't1'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR (last = 'l1' AND tax = 't1')"), row(1));
            // the disjunction is in a branch of a conjunction
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE dob = '1980-01-01' AND (tax = 't2' OR (tax = 't2' AND last = 'l2'))"), row(4));
            // the branch to keep has a nested disjunction, so it's narrower than its expressions alone
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND (last = 'l1' OR dob = '1990-01-01')) OR (tax = 't1' AND last = 'l2')"),
                                    row(1), row(2));
            // a different operator on the same value
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE dob != '1980-01-01' OR (dob = '1980-01-01' AND last = 'l1')"), row(1), row(2), row(3));
        });
    }

    @Test
    public void testAnalyzedColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text, code int)");
        createIndex("CREATE CUSTOM INDEX ON %s(name) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex(String.format(SAI, "code"));

        execute("INSERT INTO %s (id, name, code) VALUES (1, 'john smith', 1)");
        execute("INSERT INTO %s (id, name, code) VALUES (2, 'john', 2)");
        execute("INSERT INTO %s (id, name, code) VALUES (3, 'smith', 3)");
        execute("INSERT INTO %s (id, name, code) VALUES (4, 'bob', 4)");

        beforeAndAfterFlush(() -> {
            // the tokens of a term must all match, also under a disjunction
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE name : 'john smith' OR code = 4"), row(1), row(4));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (name : 'john smith' AND code = 1) OR (name : 'john smith')"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (name : 'john' AND code = 2) OR (name : 'smith')"), row(1), row(2), row(3));
        });
    }

    @Test
    public void testRowsSplitAcrossSSTables()
    {
        createPeopleTable();
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id, tax) VALUES (1, 't1')");
        execute("INSERT INTO %s (id, last) VALUES (2, 'l1')");
        flush();
        execute("INSERT INTO %s (id, last) VALUES (1, 'l1')");
        execute("INSERT INTO %s (id, tax) VALUES (2, 't2')");
        flush();

        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR (tax = 't2' AND last = 'l1')"), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR tax = 't1'"), row(1));
    }

    @Test
    public void testRowsThatStopMatching()
    {
        createPeopleTable();
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id, tax, last) VALUES (1, 't1', 'l1')");
        execute("INSERT INTO %s (id, tax, last) VALUES (2, 't1', 'l1')");
        execute("INSERT INTO %s (id, tax, last) VALUES (3, 't1', 'l1')");
        flush();
        execute("UPDATE %s SET tax = 't2', last = 'l2' WHERE id = 1");
        execute("DELETE FROM %s WHERE id = 2");
        execute("UPDATE %s SET last = 'l2' WHERE id = 3");

        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tax = 't1' OR last = 'l1'"), row(3));
        flush();
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tax = 't1' OR last = 'l1'"), row(3));
    }

    @Test
    public void testPagingAndLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "a"));
        createIndex(String.format(SAI, "b"));

        // all the rows match the first branch, half of them match the second too
        int rows = 0;
        for (int pk = 0; pk < 20; pk++)
            for (int ck = 0; ck < 5; ck++, rows++)
                execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, 1, ?)", pk, ck, rows % 2);
        int expected = rows;

        beforeAndAfterFlush(() -> {
            for (int pageSize : new int[]{ 1, 3, 10, 1000 })
            {
                ResultSet result = executeNetWithPaging("SELECT pk, ck FROM %s WHERE a = 1 OR b = 1", pageSize);
                List<String> keys = new ArrayList<>();
                for (Row row : result)
                    keys.add(row.getInt("pk") + ":" + row.getInt("ck"));
                assertThat(keys).hasSize(expected);
                assertThat(new HashSet<>(keys)).hasSize(expected);
            }

            assertThat(execute("SELECT pk, ck FROM %s WHERE a = 1 OR b = 1 LIMIT 7").size()).isEqualTo(7);
            assertThat(execute("SELECT pk, ck FROM %s WHERE a = 1 OR b = 1 PER PARTITION LIMIT 1").size()).isEqualTo(20);
        });
    }

    @Test
    public void testIndexRequiredWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, a int, b int)");
        createIndex(String.format(SAI, "a"));

        execute("INSERT INTO %s (id, a, b) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (id, a, b) VALUES (2, 2, 2)");
        execute("INSERT INTO %s (id, a, b) VALUES (3, 3, 3)");

        beforeAndAfterFlush(() -> {
            assertThatThrownBy(() -> execute("SELECT id FROM %s WHERE a = 1 OR b = 2"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE a = 1 OR b = 2 ALLOW FILTERING"), row(1), row(2));
        });
    }

    @Test
    public void testStaticColumnsWithoutIndexes() throws Throwable
    {
        testStaticColumns(false);
    }

    @Test
    public void testStaticColumnsWithIndexes() throws Throwable
    {
        testStaticColumns(true);
    }

    private void testStaticColumns(boolean indexes) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v int, PRIMARY KEY(pk, ck))");
        if (indexes)
        {
            createIndex(String.format(SAI, "s"));
            createIndex(String.format(SAI, "v"));
        }

        execute("INSERT INTO %s (pk, s) VALUES (1, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 20)");
        execute("INSERT INTO %s (pk, s) VALUES (2, 2)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 10)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 2, 20)");
        execute("INSERT INTO %s (pk, s) VALUES (3, 3)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (4, 1, 30)");

        String filtering = indexes ? "" : " ALLOW FILTERING";
        beforeAndAfterFlush(() -> {
            // a row can match without a match of its static row, and all the rows of a matching static row match
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s = 1 OR v = 20" + filtering), row(1, 1), row(1, 2), row(2, 2));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s = 9 OR v = 30" + filtering), row(4, 1));
            // both conditions of a branch must match
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE (s = 1 AND v = 10) OR v = 30" + filtering), row(1, 1), row(4, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE (s = 1 AND v = 10) OR (s = 2 AND v = 20)" + filtering), row(1, 1), row(2, 2));
            // a partition that only has a static row
            assertRowsIgnoringOrder(execute("SELECT pk, s FROM %s WHERE s = 3 OR v = 30" + filtering), row(3, 3), row(4, null));
            assertRowsIgnoringOrder(execute("SELECT pk, s FROM %s WHERE s = 3 OR s = 9" + filtering), row(3, 3));
            assertRows(execute("SELECT pk, s FROM %s WHERE s = 9 OR v = 99" + filtering));
            // the partition key is checked with the rest of the tree
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (s = 2 OR v = 20)" + filtering), row(1, 2));
        });
    }

    /**
     * A disjunction with only static columns in nested branches doesn't filter rows, so it keeps the partitions without rows.
     */
    @Test
    public void testNestedStaticBranchesWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s1 int static, s2 int static, PRIMARY KEY(pk, ck))");
        execute("INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (pk, s1, s2) VALUES (2, 2, 2)");

        beforeAndAfterFlush(() -> assertRows(execute("SELECT pk FROM %s WHERE (s1 = 1 AND s2 = 1) OR s1 = 9 ALLOW FILTERING"), row(1)));
    }

    /**
     * A partition with only a static row matches an index on a partition key column after a flush, a compaction and an
     * index build.
     */
    @Test
    public void testIndexOnPartitionKeyWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, a int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "pk"));
        createIndex(String.format(SAI, "a"));
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (pk, s) VALUES (1, 1)");
        execute("INSERT INTO %s (pk, ck, s, a) VALUES (2, 1, 2, 2)");
        execute("INSERT INTO %s (pk, ck, s, a) VALUES (3, 1, 3, 3)");
        // the rows are gone and the static row stays
        execute("INSERT INTO %s (pk, s) VALUES (4, 4)");
        execute("INSERT INTO %s (pk, ck, a) VALUES (4, 1, 4)");
        execute("DELETE FROM %s WHERE pk = 4 AND ck = 1");
        // a partition without a static row
        execute("INSERT INTO %s (pk, ck, a) VALUES (5, 1, 5)");

        CheckedFunction assertions = () -> {
            assertRowsIgnoringOrder(execute("SELECT pk, s FROM %s WHERE pk = 1 OR a = 3"), row(1, 1), row(3, 3));
            assertRowsIgnoringOrder(execute("SELECT pk, s FROM %s WHERE pk = 4 OR a = 2"), row(4, 4), row(2, 2));
            assertRowsIgnoringOrder(execute("SELECT pk, s FROM %s WHERE pk = 1 OR pk = 2"), row(1, 1), row(2, 2));
            assertRows(execute("SELECT pk, a FROM %s WHERE pk = 5 OR a = -1"), row(5, 5));
        };

        assertions.apply();
        flush();
        assertions.apply();
        compact();
        assertions.apply();

        // the index is created after the data
        dropIndex("DROP INDEX %s." + currentTable() + "_pk_idx");
        createIndex(String.format(SAI, "pk"));
        assertions.apply();
    }

    /**
     * The same for an index on a component of the partition key.
     */
    @Test
    public void testIndexOnPartitionKeyComponentWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, ck int, s int static, a int, PRIMARY KEY((k1, k2), ck))");
        createIndex(String.format(SAI, "k1"));
        createIndex(String.format(SAI, "a"));
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (k1, k2, s) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (k1, k2, ck, s, a) VALUES (2, 2, 1, 2, 2)");

        CheckedFunction assertions = () -> {
            assertRows(execute("SELECT k1, s FROM %s WHERE k1 = 1"), row(1, 1));
            assertRowsIgnoringOrder(execute("SELECT k1, s FROM %s WHERE k1 = 1 OR a = 2"), row(1, 1), row(2, 2));
        };

        assertions.apply();
        flush();
        assertions.apply();
        compact();
        assertions.apply();
    }

    /**
     * An index build on sstables that already have the components of another index must assign the same row ids. Those
     * sstables don't have a row for an empty static row, in a table with static columns.
     */
    @Test
    public void testIndexCreatedOnSSTablesWithEmptyStaticRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "a"));
        disableCompaction(KEYSPACE);

        // partitions without a static row, partitions with only a static row, and partitions with both
        for (int pk = 0; pk < 30; pk++)
        {
            if (pk % 3 == 0)
                execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, 1, ?, ?)", pk, pk, pk);
            else if (pk % 3 == 1)
                execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);
            else
                execute("INSERT INTO %s (pk, ck, s, a, b) VALUES (?, 1, ?, ?, ?)", pk, pk, pk, pk);
        }
        flush();

        createIndex(String.format(SAI, "b"));
        createIndex(String.format(SAI, "pk"));

        for (int pk = 0; pk < 30; pk++)
        {
            Object[] expected = pk % 3 == 1 ? row(pk, pk, null) : row(pk, pk % 3 == 0 ? null : pk, pk);
            assertRows(execute("SELECT pk, s, b FROM %s WHERE pk = ? OR a = -1", pk), expected);
            if (pk % 3 != 1)
                assertRows(execute("SELECT pk FROM %s WHERE b = ?", pk), row(pk));
        }
    }

    /**
     * A partition with no matching rows doesn't match if all the branches of the disjunction need a matching row.
     */
    @Test
    public void testDisjunctionOfRegularColumnsWithPartitionWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, a int, b int, PRIMARY KEY(pk, ck))");

        for (int pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, 1)", pk);
        for (int pk = 5; pk < 10; pk++)
            execute("INSERT INTO %s (pk, ck, a) VALUES (?, 1, 1)", pk);
        // a live static row and a range tombstone
        execute("INSERT INTO %s (pk, s) VALUES (10, 1)");
        execute("DELETE FROM %s WHERE pk = 10 AND ck > 0");

        beforeAndAfterFlush(() -> {
            assertThat(execute("SELECT pk, s FROM %s WHERE a = 1 OR b = 1 ALLOW FILTERING").size()).isEqualTo(5);
            assertThat(execute("SELECT pk, s FROM %s WHERE a = 1 OR b = 1 LIMIT 3 ALLOW FILTERING").size()).isEqualTo(3);
            assertThat(execute("SELECT pk, s FROM %s WHERE (a = 1 AND s = 1) OR (b = 1 AND s = 1) ALLOW FILTERING").size()).isEqualTo(0);
            // a branch without a row level restriction selects the partitions without rows
            assertThat(execute("SELECT pk, s FROM %s WHERE (a = 1 AND b = 1) OR s = 1 ALLOW FILTERING").size()).isEqualTo(6);
        });
    }

    @Test
    public void testLikeOnOnlyPartitionKeyColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, a int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "pk"));
        createIndex(String.format(SAI, "a"));

        execute("INSERT INTO %s (pk, ck, a) VALUES ('abc', 1, 1)");
        execute("INSERT INTO %s (pk, ck, a) VALUES ('abd', 1, 2)");
        execute("INSERT INTO %s (pk, ck, a) VALUES ('xyz', 1, 3)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE pk LIKE 'ab%%'"), row("abc"), row("abd"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE pk LIKE 'abc%%' OR a = 3"), row("abc"), row("xyz"));
        });
    }

    /**
     * The same term of an index on a key component can be in a partition with a static row and in one without it.
     */
    @Test
    public void testIndexOnPartitionKeyComponentIntersectionWithStaticRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, ck int, s int static, a int, PRIMARY KEY((k1, k2), ck))");
        createIndex(String.format(SAI, "k1"));
        createIndex(String.format(SAI, "a"));
        disableCompaction(KEYSPACE);

        for (int k2 = 0; k2 < 50; k2++)
        {
            if (k2 % 2 == 0)
                execute("INSERT INTO %s (k1, k2, s) VALUES (1, ?, 1)", k2);
            for (int ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (k1, k2, ck, a) VALUES (1, ?, ?, ?)", k2, ck, ck);
        }

        CheckedFunction assertions = () -> {
            assertThat(execute("SELECT k2, ck FROM %s WHERE k1 = 1 AND a = 5").size()).isEqualTo(50);
            assertThat(execute("SELECT k2, ck FROM %s WHERE (k1 = 1 AND a = 5) OR a = 7").size()).isEqualTo(100);
        };

        assertions.apply();
        flush();
        assertions.apply();
        compact();
        assertions.apply();
    }

    /**
     * A partition with only a static row is a row in the results, so it counts for the limit and the paging.
     */
    @Test
    public void testLimitWithPartitionsWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "s"));
        createIndex(String.format(SAI, "v"));

        for (int pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, 3)", pk);
        for (int pk = 5; pk < 15; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, 1, 30)", pk);

        beforeAndAfterFlush(() -> {
            for (int limit : new int[]{ 1, 4, 7, 12, 100 })
            {
                int expected = Math.min(limit, 15);
                assertThat(execute("SELECT pk, s FROM %s WHERE s = 3 OR v = 30 LIMIT " + limit).size()).isEqualTo(expected);
                for (int pageSize : new int[]{ 1, 3, 6, 1000 })
                {
                    ResultSet result = executeNetWithPaging("SELECT pk, s FROM %s WHERE s = 3 OR v = 30 LIMIT " + limit, pageSize);
                    assertThat(result.all()).hasSize(expected);
                }
            }

            for (int pageSize : new int[]{ 1, 3, 6, 1000 })
            {
                ResultSet result = executeNetWithPaging("SELECT pk, s FROM %s WHERE pk = 0 OR v = 30 ALLOW FILTERING", pageSize);
                assertThat(result.all()).hasSize(11);
            }
        });
    }

    /**
     * An index on the partition key doesn't change the queries that don't need it.
     */
    @Test
    public void testIndexOnPartitionKeyDoesNotAffectFullyRestrictedPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 int, c2 int, v int, PRIMARY KEY(pk, c1, c2))");
        createIndex(String.format(SAI, "pk"));

        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 1, 1, 3)");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 2, 2, 3)");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (2, 1, 1, 3)");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (3, 1, 1, 3)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk, c1 FROM %s WHERE pk IN (1, 2) AND v = 3 ALLOW FILTERING"), row(1, 1), row(1, 2), row(2, 1));
            assertRows(execute("SELECT c1 FROM %s WHERE pk = 1 AND v = 3 ORDER BY c1 DESC ALLOW FILTERING"), row(2), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk, c1 FROM %s WHERE pk IN (1, 2) AND c2 = 1 ALLOW FILTERING"), row(1, 1), row(2, 1));
            assertThatThrownBy(() -> execute("SELECT c1 FROM %s WHERE pk = 1 AND v = 3"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
        });
    }

    @Test
    public void testPartitionKeyOperatorsWithoutIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "a"));
        insertKeyedRows();

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk IN (1, 3) OR a = 3 ALLOW FILTERING"), row(1, 1), row(1, 2), row(2, 1), row(3, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk > 2 OR a = 1 ALLOW FILTERING"), row(1, 1), row(3, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk >= 2 AND a = 3 OR pk = 1 AND a = 1 ALLOW FILTERING"), row(2, 1), row(1, 1));
        });
    }

    @Test
    public void testAggregationsAndDistinct() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "pk"));
        createIndex(String.format(SAI, "a"));
        insertKeyedRows();

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT count(*) FROM %s WHERE pk = 1 OR a = 3"), row(3L));
            assertRows(execute("SELECT max(a) FROM %s WHERE pk = 1 OR a = 3"), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk, count(*) FROM %s WHERE pk = 1 OR a = 3 GROUP BY pk"), row(1, 2L), row(2, 1L));
            assertRowsIgnoringOrder(execute("SELECT DISTINCT pk FROM %s WHERE pk = 1 OR a = 3"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 3 PER PARTITION LIMIT 1"), row(1, 1), row(2, 1));
        });
    }

    /**
     * A key that is too long for an index isn't indexed, as for any other column. It's still read directly by its key.
     */
    @Test
    public void testIndexOnPartitionKeyWithOversizedKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, a int)");
        createIndex(String.format(SAI, "pk"));
        createIndex(String.format(SAI, "a"));

        String longKey = "k".repeat(10000);
        execute("INSERT INTO %s (pk, a) VALUES (?, 1)", longKey);
        execute("INSERT INTO %s (pk, a) VALUES ('short', 2)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT a FROM %s WHERE pk = ?", longKey), row(1));
            // the index can't find the long key, only its other branch matches
            assertRows(execute("SELECT a FROM %s WHERE pk = ? OR a = 2", longKey), row(2));
            assertRows(execute("SELECT a FROM %s WHERE pk = 'short' OR a = 9"), row(2));
        });
    }

    private void insertKeyedRows()
    {
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (1, 1, 1, 5)");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (1, 2, 2, 6)");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (2, 1, 3, 7)");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (2, 2, 4, 8)");
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (3, 1, 5, 9)");
    }

    @Test
    public void testPartitionKeyWithoutIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "a"));
        insertKeyedRows();

        beforeAndAfterFlush(() -> {
            assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 3"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

            assertPartitionKeyUnderDisjunction(" ALLOW FILTERING");
        });
    }

    @Test
    public void testOnlyPartitionKeyColumnWithIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "pk"));
        createIndex(String.format(SAI, "a"));
        createIndex(String.format(SAI, "b"));
        insertKeyedRows();

        beforeAndAfterFlush(() -> {
            assertPartitionKeyUnderDisjunction("");

            // the index also serves relations that aren't nested
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1"), row(1, 1), row(1, 2));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1 AND a = 2"), row(1, 2));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk > 2"), row(3, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk != 1"), row(2, 1), row(2, 2), row(3, 1));
        });
    }

    @Test
    public void testOnlyPartitionKeyColumnWithIndexAndNoClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int)");
        createIndex(String.format(SAI, "pk"));
        createIndex(String.format(SAI, "a"));

        execute("INSERT INTO %s (pk, a) VALUES (1, 1)");
        execute("INSERT INTO %s (pk, a) VALUES (2, 2)");
        execute("INSERT INTO %s (pk, a) VALUES (3, 3) USING TTL 1000");
        execute("INSERT INTO %s (pk, a) VALUES (4, 4)");
        execute("DELETE FROM %s WHERE pk = 4");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE pk = 1 OR a = 2"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE pk IN (1, 3) OR a = 2"), row(1), row(2), row(3));
            assertRows(execute("SELECT pk FROM %s WHERE pk = 4 OR a = 4"));
            assertRows(execute("SELECT pk FROM %s WHERE pk = 9 OR a = 9"));
        });
    }

    @Test
    public void testPartitionKeyComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, ck int, a int, PRIMARY KEY((k1, k2), ck))");
        createIndex(String.format(SAI, "k1"));
        createIndex(String.format(SAI, "a"));

        execute("INSERT INTO %s (k1, k2, ck, a) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (k1, k2, ck, a) VALUES (1, 2, 1, 2)");
        execute("INSERT INTO %s (k1, k2, ck, a) VALUES (2, 1, 1, 3)");
        execute("INSERT INTO %s (k1, k2, ck, a) VALUES (2, 2, 1, 4)");

        beforeAndAfterFlush(() -> {
            // an indexed component doesn't need filtering
            assertRowsIgnoringOrder(execute("SELECT k1, k2 FROM %s WHERE k1 = 1 OR a = 3"), row(1, 1), row(1, 2), row(2, 1));

            // a component that isn't indexed does
            assertThatThrownBy(() -> execute("SELECT k1, k2 FROM %s WHERE k2 = 1 OR a = 4"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
            assertRowsIgnoringOrder(execute("SELECT k1, k2 FROM %s WHERE k2 = 1 OR a = 4 ALLOW FILTERING"), row(1, 1), row(2, 1), row(2, 2));

            // all the components in a branch
            assertRowsIgnoringOrder(execute("SELECT k1, k2 FROM %s WHERE (k1 = 1 AND k2 = 2) OR a = 3 ALLOW FILTERING"), row(1, 2), row(2, 1));
        });
    }

    /**
     * Only the queries can have a partition key under a disjunction, and it must be a value of the column.
     */
    @Test
    public void testPartitionKeyRestrictionsThatRemainInvalid()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "a"));

        String tokenError = String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk");
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE token(pk) > 0 OR a = 1 ALLOW FILTERING"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(tokenError);
        assertThatThrownBy(() -> execute("DELETE FROM %s WHERE pk = 1 OR pk = 2"))
        .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("UPDATE %s SET a = 1 WHERE (pk = 1 OR pk = 2) AND ck = 1"))
        .isInstanceOf(InvalidRequestException.class);
    }

    private void assertPartitionKeyUnderDisjunction(String filtering) throws Throwable
    {
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 3" + filtering), row(1, 1), row(1, 2), row(2, 1));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1 OR pk = 3" + filtering), row(1, 1), row(1, 2), row(3, 1));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE a = 2 OR (pk = 2 AND a = 4)" + filtering), row(1, 2), row(2, 2));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 9 OR a = 3" + filtering), row(2, 1));
        assertRows(execute("SELECT pk, ck FROM %s WHERE pk = 9 OR a = 99" + filtering));
        // a top level relation selects the partition, and the nested ones filter it
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (pk = 2 OR a = 2)" + filtering), row(1, 2));
    }

    /**
     * Legacy indexes serve the top level conjunction, and they must ignore what is under a disjunction when they select
     * the keys they read.
     */
    @Test
    public void testLegacyIndexWithKeysUnderDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, v int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE INDEX ON %s(v)");

        execute("INSERT INTO %s (pk, ck, a, v) VALUES (1, 1, 1, 3)");
        execute("INSERT INTO %s (pk, ck, a, v) VALUES (1, 2, 2, 3)");
        execute("INSERT INTO %s (pk, ck, a, v) VALUES (2, 1, 3, 3)");
        execute("INSERT INTO %s (pk, ck, a, v) VALUES (2, 2, 4, 4)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = 3 AND (ck = 1 OR a = 2) ALLOW FILTERING"), row(1, 1), row(1, 2), row(2, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = 3 AND (pk = 1 OR a = 3) ALLOW FILTERING"), row(1, 1), row(1, 2), row(2, 1));
        });
    }

    @Test
    public void testBM25OrderIsNotAffectedByRedundantBranches() throws Throwable
    {
        // BM25 needs a version that AA doesn't support, and the query is the same for all the others
        Assume.assumeTrue(version.onOrAfter(Version.BM25_EARLIEST));

        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text, code int)");
        createIndex("CREATE CUSTOM INDEX ON %s(body) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex(String.format(SAI, "code"));

        execute("INSERT INTO %s (id, body, code) VALUES (1, 'apple', 1)");
        execute("INSERT INTO %s (id, body, code) VALUES (2, 'apple apple apple', 1)");
        execute("INSERT INTO %s (id, body, code) VALUES (3, 'pear', 2)");

        beforeAndAfterFlush(() -> {
            Object[][] plain = getRows(execute("SELECT id FROM %s WHERE code = 1 ORDER BY body BM25 OF 'apple' LIMIT 10"));
            Object[][] redundant = getRows(execute("SELECT id FROM %s WHERE code = 1 OR (code = 1 AND body : 'apple') ORDER BY body BM25 OF 'apple' LIMIT 10"));
            assertThat(redundant).isDeepEqualTo(plain);
            assertThat(plain.length).isEqualTo(2);
        });
    }
}
