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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;

public class GenericOrderByUpdateDeleteTest extends SAITester
{

    @Before
    public void setup() throws Throwable
    {
        // Enable the optimizer by default. If there are any tests that need to disable it, they can do so explicitly.
        QueryController.QUERY_OPT_LEVEL = 1;
    }

    @Test
    public void testPreparedQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert some data
        for (int i = -100; i < 100; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, i);

        forcePreparedValues();
        var query1 = "SELECT pk FROM %s WHERE val > ? ORDER BY val ASC LIMIT 4";
        var query2 = "SELECT pk FROM %s WHERE val > ? ORDER BY val DESC LIMIT 4";
        var query3 = "SELECT pk FROM %s ORDER BY val ASC LIMIT 4";
        var query4 = "SELECT pk FROM %s ORDER BY val DESC LIMIT 4";
        prepare(query1);
        prepare(query2);
        prepare(query3);
        prepare(query4);

        assertRows(execute(query1, 0), row(1), row(2), row(3), row(4));
        assertRows(execute(query2, 0), row(99), row(98), row(97), row(96));
        assertRows(execute(query3), row(-100), row(-99), row(-98), row(-97));
        assertRows(execute(query4), row(99), row(98), row(97), row(96));
    }

    @Test
    public void testOrderingWhereRowHasComplexColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val int, m map<text, text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, val, m) VALUES (?, ?, ?)", 1, 1, map("a", "b"));
        execute("INSERT INTO %s (pk, val, m) VALUES (?, ?, ?)", 2, 2, map("a", "b"));
        flush();

        // Add more data to pk 1 in another sstable. This triggers merging cells in the RowWithSourceTable.
        execute("INSERT INTO %s (pk, m) VALUES (?, ?)", 1, map("b", "c"));

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY val", 2, row(1), row(2));
        });
    }

    @Test
    public void endToEndTextOrderingTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val) VALUES (0, 'a')");
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 'z')");

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 10, row(0), row(1));
        });

        execute("INSERT INTO %s (pk, str_val) VALUES (2, 'b')");
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 'A')");

        // Now we get memtable + sstable and then two sstables in the query, which confirms merging index results.
        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 4,
                                  row(3), row(0), row(2), row(1));
        });
    }

    @Test
    public void endToEndIntOrderingTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val) VALUES (0, -10)");
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 0)");

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 10, row(0), row(1));
        });

        execute("INSERT INTO %s (pk, str_val) VALUES (2, -5)");
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 100)");

        // Now we get memtable + sstable and then two sstables in the query, which confirms merging index results.
        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 4,
                                  row(0), row(2), row(1), row(3));
        });
    }

    @Test
    public void testTextOverwrittenRowsInDifferentMemtableOrSSTable() throws Throwable
    {
        testTextOverwritten(true);
    }

    @Test
    public void testTextOverwrittenRowsInSameMemtableOrSSTable() throws Throwable
    {
        testTextOverwritten(false);
    }

    private void testTextOverwritten(boolean shouldFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        disableCompaction();

        execute("INSERT INTO %s (pk, str_val) VALUES (0, 'a')");
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 'b')");
        if (shouldFlush)
            flush();
        execute("INSERT INTO %s (pk, str_val) VALUES (0, 'c')");

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 10, row(1), row(0));
        });
    }

    @Test
    public void testIntOverwrittenRowsInDifferentMemtableOrSSTable() throws Throwable
    {
        testIntOverwritten(true);
    }

    @Test
    public void testIntOverwrittenRowsInSameMemtableOrSSTable() throws Throwable
    {
        testIntOverwritten(false);
    }

    private void testIntOverwritten(boolean shouldFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val int)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val) VALUES (0, 1)");
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 2)");
        if (shouldFlush)
            flush();
        execute("INSERT INTO %s (pk, str_val) VALUES (0, 3)");

        beforeAndAfterFlush(() -> {
            assertRowsInBothOrder("SELECT pk FROM %s ORDER BY str_val", 10, row(1), row(0));
        });
    }

    private void assertRowsInBothOrder(String query, int limit, Object[]... rowsInAscendingOrder)
    {
        assertRows(execute(query + " ASC LIMIT " + limit), rowsInAscendingOrder);
        assertRows(execute(query + " DESC LIMIT " + limit), reverse(rowsInAscendingOrder));
    }

    private static Object[][] reverse(Object[][] rows)
    {
        Object[][] reversed = new Object[rows.length][];
        for (int i = 0; i < rows.length; i++)
        {
            reversed[i] = rows[rows.length - i - 1];
        }
        return reversed;
    }

}