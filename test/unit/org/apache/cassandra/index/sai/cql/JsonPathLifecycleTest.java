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

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.index.sai.SAITester;

/**
 * Lifecycle of JSON path index entries: overwrites, shape changes, deletes, TTL, non-JSON values, compaction and
 * building an index over existing data. Every check runs against the memtable, after a flush and after a compaction.
 */
public class JsonPathLifecycleTest extends SAITester
{
    private static final String PREFIX = "CREATE CUSTOM INDEX ON %s(doc) USING 'StorageAttachedIndex' WITH OPTIONS = ";
    private static final String TAGS_INDEX = PREFIX + "{'json_path': '$.tags', 'json_type': 'text'}";
    private static final String N_INDEX = PREFIX + "{'json_path': '$.n', 'json_type': 'int'}";

    /**
     * Creates a test table with text and int JSON path indexes.
     */
    private void createJsonTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(TAGS_INDEX);
        createIndex(N_INDEX);
    }

    /**
     * Tests overwriting JSON documents containing shared array terms across memtable and SSTables.
     */
    @Test
    public void overwriteWithSharedTerm() throws Throwable
    {
        createJsonTable();

        // pk 1: both versions in the same memtable; pk 2: the old version in an sstable, the new one in the memtable
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":[\"a\",\"b\"]}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":[\"a\",\"b\"]}')");
        flush();
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":[\"a\",\"b\"]}')");
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":[\"a\",\"c\"]}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":[\"a\",\"c\"]}')");

        runThenFlushThenCompact(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'b'"));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'c'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] != 'b'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] != 'c'"));
        });
    }

    /**
     * Tests changing JSON document structure at the indexed path (scalar -> array -> object).
     */
    @Test
    public void shapeChanges() throws Throwable
    {
        createJsonTable();

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"n\":5}')");
        runThenFlushThenCompact(() -> assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 5"), row(1)));

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"n\":[5,6]}')");
        runThenFlushThenCompact(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 5"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 6"), row(1));
        });

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"n\":{\"x\":5}}')");
        runThenFlushThenCompact(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 5"));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 6"));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] > 0"));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] != 5"), row(1));
        });
    }

    /**
     * Tests JSON shape changes occurring purely within the active memtable before flushing.
     */
    @Test
    public void shapeChangesWithinMemtable() throws Throwable
    {
        createJsonTable();

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"n\":5}')");
        assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 5"), row(1));
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"n\":[5,6]}')");
        assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 5"), row(1));
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"n\":{\"x\":5}}')");

        runThenFlushThenCompact(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 5"));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] >= 5"));
        });
    }

    /**
     * Tests that duplicate elements in JSON arrays are properly indexed and matched.
     */
    @Test
    public void duplicateElements() throws Throwable
    {
        createJsonTable();

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":[\"a\",\"a\"],\"n\":[7,7,7]}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":[\"b\",\"a\",\"b\"]}')");

        runThenFlushThenCompact(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(1), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'b'"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 7"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] > 6"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a' AND doc['$.n'] = 7"), row(1));
        });
    }

    /**
     * Tests deleting rows or setting the JSON column to null across memtable and SSTables.
     */
    @Test
    public void deleteAndNullUpdate() throws Throwable
    {
        createJsonTable();

        // pk 1, 2: data in an sstable, deletion in the memtable; pk 3, 4: both in the memtable
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":\"a\",\"n\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":\"a\",\"n\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (5, '{\"tags\":\"a\",\"n\":1}')");
        flush();
        execute("INSERT INTO %s (pk, doc) VALUES (3, '{\"tags\":\"a\",\"n\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (4, '{\"tags\":\"a\",\"n\":1}')");

        execute("DELETE FROM %s WHERE pk = 1");
        execute("UPDATE %s SET doc = null WHERE pk = 2");
        execute("DELETE FROM %s WHERE pk = 3");
        execute("UPDATE %s SET doc = null WHERE pk = 4");

        runThenFlushThenCompact(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(5));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 1"), row(5));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] < 10"), row(5));
            // the rows written with INSERT still exist after doc is nulled, so NEQ (missing path) matches them
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] != 'a'"), row(2), row(4));
        });
    }

    /**
     * Tests that rows with TTL expired values are purged from JSON path index search results.
     */
    @Test
    public void ttlExpiry() throws Throwable
    {
        createJsonTable();

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":\"a\",\"n\":1}') USING TTL 1");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":\"a\",\"n\":1}')");
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(1), row(2));

        // wait (bounded) until the TTL'd row has expired
        Util.spinAssert(() -> assertRows(execute("SELECT pk FROM %s WHERE pk = 1")), 30);

        runThenFlushThenCompact(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 1"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] >= 1"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] != 'b'"), row(2));
        });
    }

    /**
     * Tests queries when invalid/non-JSON text values are stored in the indexed column.
     */
    @Test
    public void nonJsonValuesMixedWithJson() throws Throwable
    {
        createJsonTable();

        execute("INSERT INTO %s (pk, doc) VALUES (1, 'not json')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":[\"a\",\"b\"],\"n\":3}')");
        execute("INSERT INTO %s (pk, doc) VALUES (3, '{broken')");
        execute("INSERT INTO %s (pk, doc) VALUES (4, '{\"tags\":\"a\",\"n\":\"three\"}')");
        execute("INSERT INTO %s (pk, doc) VALUES (5, '[1,2,3]')");
        execute("INSERT INTO %s (pk, doc) VALUES (6, '')");

        runThenFlushThenCompact(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(2), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'b'"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 3"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] > 0"), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.n'] != 3"), row(1), row(3), row(4), row(5), row(6));
        });
    }

    /**
     * Tests building JSON path indexes asynchronously over existing pre-populated SSTables and memtables.
     */
    @Test
    public void indexBuiltOverExistingData() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        disableCompaction();

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":[\"a\",\"b\"],\"n\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"tags\":\"b\",\"n\":[2,3]}')");
        flush();
        execute("INSERT INTO %s (pk, doc) VALUES (3, 'not json')");
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"tags\":[\"a\",\"c\"],\"n\":1}')");
        flush();
        execute("INSERT INTO %s (pk, doc) VALUES (4, '{\"tags\":\"a\",\"n\":4}')"); // still in the memtable

        createIndexAsync(TAGS_INDEX);
        createIndexAsync(N_INDEX);
        waitForTableIndexesQueryable();

        runThenFlushThenCompact(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'a'"), row(1), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'b'"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'c'"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.n'] = 3"), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.n'] > 1"), row(2), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] != 'b'"), row(1), row(3), row(4));
        });
    }
}
