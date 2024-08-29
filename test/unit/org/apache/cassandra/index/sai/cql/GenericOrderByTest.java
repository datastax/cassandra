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
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;

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
        waitForIndexQueryable();
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
        waitForIndexQueryable();
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
        waitForIndexQueryable();

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
        waitForIndexQueryable();

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
        waitForIndexQueryable();

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
        });

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT v FROM %s WHERE v >= -10 ORDER BY v DESC LIMIT 4"), row(8), row(7), row(6), row(5));
            assertRows(execute("SELECT v FROM %s WHERE v > 1 ORDER BY v DESC LIMIT 4"), row(8), row(7), row(6), row(5));
            assertRows(execute("SELECT v FROM %s WHERE v <= 3 ORDER BY v DESC LIMIT 4"), row(3), row(2), row(1));
            assertRows(execute("SELECT v FROM %s WHERE v >= 4 AND v <= 6 ORDER BY v DESC LIMIT 4"), row(6), row(5), row(4));
            assertRows(execute("SELECT v FROM %s WHERE v >= 7 ORDER BY v DESC LIMIT 4"), row(8), row(7));
            assertRows(execute("SELECT v FROM %s WHERE v >= 10 ORDER BY v DESC LIMIT 4"));
        });
    }

}
