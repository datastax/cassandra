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

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests SAI indexes on partition key columns, and the indexes built next to them, in tables with static rows.
 */
@RunWith(Parameterized.class)
public class PartitionKeyIndexWithStaticRowsTest extends SAITester
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

    /**
     * A partition with only a static row matches an index on a partition key component after a flush, a compaction and
     * an index build.
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
        // the rows are gone and the static row stays
        execute("INSERT INTO %s (k1, k2, s) VALUES (3, 3, 3)");
        execute("INSERT INTO %s (k1, k2, ck, a) VALUES (3, 3, 1, 3)");
        execute("DELETE FROM %s WHERE k1 = 3 AND k2 = 3 AND ck = 1");
        // a partition without a static row
        execute("INSERT INTO %s (k1, k2, ck, a) VALUES (4, 4, 1, 4)");

        CheckedFunction assertions = () -> {
            assertRows(execute("SELECT k1, s FROM %s WHERE k1 = 1"), row(1, 1));
            assertRows(execute("SELECT k1, s, a FROM %s WHERE k1 = 2"), row(2, 2, 2));
            assertRows(execute("SELECT k1, s FROM %s WHERE k1 = 3"), row(3, 3));
            assertRows(execute("SELECT k1, a FROM %s WHERE k1 = 4"), row(4, 4));
        };

        assertions.apply();
        flush();
        assertions.apply();
        compact();
        assertions.apply();

        // the index is created after the data
        dropIndex("DROP INDEX %s." + currentTable() + "_k1_idx");
        createIndex(String.format(SAI, "k1"));
        assertions.apply();
    }

    /**
     * An index build on sstables that already have the components of another index must assign the same row ids. Those
     * sstables don't have a row for an empty static row, in a table with static columns.
     */
    @Test
    public void testIndexCreatedOnSSTablesWithEmptyStaticRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, ck int, s int static, a int, b int, PRIMARY KEY((k1, k2), ck))");
        createIndex(String.format(SAI, "a"));
        disableCompaction(KEYSPACE);

        // partitions without a static row, partitions with only a static row, and partitions with both
        for (int k = 0; k < 30; k++)
        {
            if (k % 3 == 0)
                execute("INSERT INTO %s (k1, k2, ck, a, b) VALUES (?, 0, 1, ?, ?)", k, k, k);
            else if (k % 3 == 1)
                execute("INSERT INTO %s (k1, k2, s) VALUES (?, 0, ?)", k, k);
            else
                execute("INSERT INTO %s (k1, k2, ck, s, a, b) VALUES (?, 0, 1, ?, ?, ?)", k, k, k, k);
        }
        flush();

        createIndex(String.format(SAI, "b"));
        createIndex(String.format(SAI, "k1"));

        for (int k = 0; k < 30; k++)
        {
            Object[] expected = k % 3 == 1 ? row(k, k, null) : row(k, k % 3 == 0 ? null : k, k);
            assertRows(execute("SELECT k1, s, b FROM %s WHERE k1 = ?", k), expected);
            if (k % 3 != 1)
                assertRows(execute("SELECT k1 FROM %s WHERE b = ?", k), row(k));
        }
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

        CheckedFunction assertions = () -> assertThat(execute("SELECT k2, ck FROM %s WHERE k1 = 1 AND a = 5").size()).isEqualTo(50);

        assertions.apply();
        flush();
        assertions.apply();
        compact();
        assertions.apply();
    }
}
