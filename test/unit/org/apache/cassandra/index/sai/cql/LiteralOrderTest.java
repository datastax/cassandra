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

import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.SAITester;

public class LiteralOrderTest extends SAITester
{
    @Test
    public void endToEndTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, str_val text, num int)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // Each iteration overwrites the previous data, so this test deals with a lot of overwrites.
        for (int i = 0; i < 5; i++)
            insertRowsAndMakeAssertions();
    }

    private void insertRowsAndMakeAssertions() throws Throwable
    {
        var sortedMap = new TreeMap<String, Integer>();
        for (int i = 0; i < 1000; i++)
        {
            var value = getRandom().nextAsciiString(10, 100);
            sortedMap.put(value, i);
            execute("INSERT INTO %s (pk, str_val, num) VALUES (?, ?, ?)", i, value, i);
        }

        beforeAndAfterFlush(() -> {
            for (int i = 0; i < 1000; i++)
            {
                var randomLimit = getRandom().nextIntBetween(5, 100);
                var expectedRows = sortedMap.values().stream().limit(randomLimit).map(CQLTester::row).toArray(Object[][]::new);
                assertRows(execute("SELECT pk FROM %s ORDER BY str_val LIMIT ?", randomLimit), expectedRows);
                // Utilize the fact that the num is also the PK
                var randomUpperBound = getRandom().nextIntBetween(5, 100);
                var expectedHybridRows = sortedMap.values().stream()
                                                  .filter(n -> n < randomUpperBound)
                                                  .map(CQLTester::row)
                                                  .limit(randomLimit)
                                                  .toArray(Object[][]::new);
                assertRows(execute("SELECT pk FROM %s WHERE num < ? ORDER BY str_val LIMIT ?", randomUpperBound, randomLimit), expectedHybridRows);
            }
        });
    }
}