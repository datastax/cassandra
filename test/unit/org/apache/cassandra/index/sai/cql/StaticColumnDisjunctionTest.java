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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the queries with the OR operator across static and regular columns, with SAI indexes and with ALLOW FILTERING.
 */
@RunWith(Parameterized.class)
public class StaticColumnDisjunctionTest extends SAITester
{
    private static final String SAI = "CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'";

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
                ResultSet result = executeNetWithPaging("SELECT pk, s FROM %s WHERE s = 3 OR v = 30 ALLOW FILTERING", pageSize);
                assertThat(result.all()).hasSize(15);
            }
        });
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

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = 3 AND (ck = 1 OR a = 2) ALLOW FILTERING"),
                                                          row(1, 1), row(1, 2), row(2, 1)));
    }
}
