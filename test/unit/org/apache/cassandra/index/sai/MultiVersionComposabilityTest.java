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

package org.apache.cassandra.index.sai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;

public class MultiVersionComposabilityTest extends SAITester
{
    @Test
    public void testMultiVersionMapRangeQuery() throws Throwable
    {
        // The map type is selected because the encoding changes over time.
        createTable("CREATE TABLE %s (pk int, data map<text,int>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(data)) USING 'StorageAttachedIndex'");

        // Don't want compaction changing versions on us
        disableCompaction();
        waitForTableIndexesQueryable();

        // Copy and randomize versions to ensure we can cover different orders of versions.
        var versions = new ArrayList<>(Version.ALL);
        Collections.shuffle(versions, getRandom().getRandom());

        // Maps from pk to value for each key in the map.
        var a = new HashMap<Integer, Integer>();
        var b = new HashMap<Integer, Integer>();
        var c = new HashMap<Integer, Integer>();

        boolean compactFirst = true;
        for (Version version : versions)
        {
            // Flush before writing so we have a memtable to test as well.
            if (!compactFirst)
                flush();
            SAIUtil.setLatestVersion(version);
            // Insert 100 random rows with mostly the same keys.
            for (int i = 0; i < 100; i++)
            {
                var data = new HashMap<String, Integer>();
                // Using random int opens us up to the case where we might have some overwrites, which is
                // a good thing to cover in this test
                var pk = getRandom().nextIntBetween(0, 1000);
                addRandomValue(pk, a, "a", data);
                addRandomValue(pk, b, "b", data);
                addRandomValue(pk, c, "c", data);
                execute("INSERT INTO %s (pk, data) VALUES (?, ?)", pk, data);
            }

            // Compact the first sstable so we can trigger it to have many segments, which
            // some additional logic.
            if (compactFirst)
            {
                // Creat 3 segments per flush
                SegmentBuilder.updateLastValidSegmentRowId(10);
                flush();
                compact();
                compactFirst = false;
            }
        }

        var invertedA = invertMap(a);
        var invertedB = invertMap(b);
        var invertedC = invertMap(c);

        beforeAndAfterFlush(() -> {
            // Confirm that we get the correct results for equality searches on the extrema of each map key.
            // Note: the min/max queries caught a bug in the way we compare terms when the current version
            // is different from the index's version.
            assertMinMaxValues("a", invertedA);
            assertMinMaxValues("b", invertedB);
            assertMinMaxValues("c", invertedC);
            // Run some range queries.
            assertRangeSlice("a", invertedA);
            assertRangeSlice("b", invertedB);
            assertRangeSlice("c", invertedC);
        });
    }

    private void assertMinMaxValues(String key, NavigableMap<Integer, SortedSet<Integer>> map)
    {
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE data[?] = ?", key, map.firstKey()), row(map.firstEntry().getValue().toArray()));
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE data[?] = ?", key, map.lastKey()), row(map.lastEntry().getValue().toArray()));
    }

    private void assertRangeSlice(String key, NavigableMap<Integer, SortedSet<Integer>> map)
    {
        for (int i = 0; i < 10; i++)
        {
            var from = getRandom().nextIntBetween(map.firstKey(), map.lastKey());
            var to = getRandom().nextIntBetween(map.tailMap(from).firstKey(), map.lastKey());
            var rows = map.subMap(from, true, to, true).values().stream()
                          .flatMap(SortedSet::stream).map(CQLTester::row).toArray(Object[][]::new);
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE data[?] >= ? AND data[?] <= ?", key, from, key, to), rows);
        }
    }

    private void addRandomValue(int pk, Map<Integer, Integer> rows, String key, Map<String, Integer> data)
    {
        var value = getRandom().nextInt();
        data.put(key, value);
        rows.put(pk, value);
    }

    private NavigableMap<Integer, SortedSet<Integer>> invertMap(Map<Integer, Integer> rows)
    {
        var inverted = new TreeMap<Integer, SortedSet<Integer>>();
        for (Map.Entry<Integer, Integer> entry : rows.entrySet())
        {
            inverted.computeIfAbsent(entry.getValue(), k -> new TreeSet<>()).add(entry.getKey());
        }
        return inverted;
    }
}
