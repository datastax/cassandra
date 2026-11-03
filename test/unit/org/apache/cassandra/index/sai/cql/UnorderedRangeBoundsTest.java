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
 * are equal but not both inclusive, match nothing. The coordinator answers them without querying the replicas,
 * whose index scans require ordered bounds (and assert them).
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
            assertRowCount(execute("SELECT pk FROM %s WHERE x > ? AND x < ?", 5, 3), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE pk = 1 AND x > 5 AND x < 3"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 3 OR x = 7"), 1);
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 3 OR x > 8 AND x < 2"), 0);
        });
    }

    @Test
    public void testNotIndexed() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, x int, y int)");
        createIndex("CREATE CUSTOM INDEX ON %s(y) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, x, y) VALUES (?, ?, 0)", i, i);

        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 3 ALLOW FILTERING"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE y = 0 AND x > 5 AND x < 3 ALLOW FILTERING"), 0);
            assertRows(execute("SELECT pk FROM %s WHERE y = 0 AND x >= 5 AND x <= 5 ALLOW FILTERING"), row(5));
        });
    }

    @Test
    public void testCounter() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, c counter)");
        execute("UPDATE %s SET c = c + 5 WHERE pk = 1");

        assertRowCount(execute("SELECT pk FROM %s WHERE c > 5 AND c < 3 ALLOW FILTERING"), 0);
        assertRows(execute("SELECT pk FROM %s WHERE c >= 5 AND c <= 5 ALLOW FILTERING"), row(1));
    }

    @Test
    public void testReversedClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, x int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, ck, x) VALUES (0, ?, 0)", i);

        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT ck FROM %s WHERE ck > 5 AND ck < 3"), 0);
            assertRows(execute("SELECT ck FROM %s WHERE ck > 3 AND ck < 6"), row(5), row(4));
        });
    }

    /**
     * The bounds are compared once normalized like the terms of the index: {@code v > 'b' AND v < 'C'} is empty in
     * the raw order, but not once normalized, and {@code v >= 'B' AND v < 'b'} the other way round.
     */
    @Test
    public void testCaseInsensitive() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "Bb");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "b");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE v > 'b' AND v < 'C'"), row(1));
            assertRowCount(execute("SELECT pk FROM %s WHERE v >= 'B' AND v < 'b'"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE v > 'C' AND v < 'b'"), 0);
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
