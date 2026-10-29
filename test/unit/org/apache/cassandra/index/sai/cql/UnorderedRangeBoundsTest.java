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

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

/**
 * Tests that ranges whose lower bound is after their upper bound (e.g. {@code x > 5 AND x < 3}), or whose bounds
 * are equal but not both inclusive, match nothing, rather than failing the memtable index trie slices, which
 * require ordered bounds.
 */
public class UnorderedRangeBoundsTest extends SAITester
{
    @Test
    public void testInt() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, x int)");
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, x) VALUES (?, ?)", i, i);

        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 3"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x >= 5 AND x <= 3"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 5"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x >= 5 AND x < 5"), 0);
            assertRows(execute("SELECT pk FROM %s WHERE x >= 5 AND x <= 5"), row(5));
        });
    }

    @Test
    public void testUuid() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, u uuid)");
        createIndex("CREATE CUSTOM INDEX ON %s(u) USING 'StorageAttachedIndex'");
        UUID low = UUID.fromString("00000000-0000-4000-8000-000000000001");
        UUID high = UUID.fromString("00000000-0000-4000-8000-000000000002");
        execute("INSERT INTO %s (pk, u) VALUES (?, ?)", 1, low);
        execute("INSERT INTO %s (pk, u) VALUES (?, ?)", 2, high);

        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT pk FROM %s WHERE u > ? AND u < ?", high, low), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE u > ? AND u <= ?", high, high), 0);
            assertRows(execute("SELECT pk FROM %s WHERE u >= ? AND u <= ?", high, high), row(2));
        });
    }

    @Test
    public void testText() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "b");

        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT pk FROM %s WHERE v > 'b' AND v < 'a'"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE v >= 'b' AND v < 'b'"), 0);
            assertRows(execute("SELECT pk FROM %s WHERE v >= 'b' AND v <= 'b'"), row(2));
        });
    }
}
