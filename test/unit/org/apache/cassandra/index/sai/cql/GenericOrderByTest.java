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
}
