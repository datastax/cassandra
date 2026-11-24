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

import org.apache.cassandra.index.sai.SAITester;

/**
 * CQL queries on JSON path indexes, using the map-element syntax {@code doc['<path>'] op value}.
 */
public class JsonPathQueryTest extends SAITester
{
    private static final String PREFIX = "CREATE CUSTOM INDEX ON %s(doc) USING 'StorageAttachedIndex' WITH OPTIONS = ";

    /**
     * Creates a test table with multiple JSON path indexes and inserts sample rows.
     */
    private void createJsonTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.profile.age', 'json_type': 'int'}");
        createIndex(PREFIX + "{'json_path': '$.tags', 'json_type': 'text'}");
        createIndex(PREFIX + "{'json_path': '$.s', 'json_type': 'int'}");
        createIndex(PREFIX + "{'json_path': '$.city', 'json_type': 'text'}");
        createIndex(PREFIX + "{'json_path': '$.d', 'json_type': 'double'}");

        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"profile\":{\"age\":25},\"tags\":[\"admin\",\"dev\"],\"s\":[10,50],\"d\":31}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"profile\":{\"age\":35},\"tags\":\"admin\",\"s\":30,\"d\":30.5}')");
        execute("INSERT INTO %s (pk, doc) VALUES (3, '{\"city\":\"berlin\",\"d\":30}')");
    }

    /**
     * Tests basic queries on JSON path indexes including EQ, NEQ, slice, and OR filters.
     */
    @Test
    public void queries() throws Throwable
    {
        createJsonTable();

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.profile.age'] > 30"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$[''profile''].age'] = 25"), row(1)); // normalized path
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'admin'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.tags'] != 'dev'"), row(2), row(3));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.s'] > 30 AND doc['$.s'] < 40"), row(1)); // independent
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.s'] > 40 AND doc['$.s'] < 30"), row(1)); // independent array elements match different bounds
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.d'] > 30"), row(1), row(2)); // mixed numbers on a double path
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.d'] > 30.5"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.city'] = 'berlin' OR doc['$.profile.age'] = 35"), row(3), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.profile.age'] = ? AND doc['$.tags'] = ?", 25, "admin"), row(1));
        });
    }

    /**
     * Tests multi-path query intersections and parameter binding on JSON path expressions.
     */
    @Test
    public void moreQueries() throws Throwable
    {
        createJsonTable();

        beforeAndAfterFlush(() -> {
            // restrictions on different paths of the same column are intersected
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'admin' AND doc['$.s'] = 30"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.tags'] = 'admin' AND doc['$.tags'] = 'dev'"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.s'] >= 50 AND doc['$.profile.age'] < 30"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.s'] != 30"), row(1), row(3));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.s'] > ?", 40), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.s'] = 30 AND doc['$.d'] = 30.5"), row(2));
        });
    }

    /**
     * Tests that NOT EQUALS on a JSON path index correctly returns rows missing the path across SSTables.
     */
    @Test
    public void notEqualsMatchesRowsWithoutPathAcrossSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.a', 'json_type': 'int'}");

        // an sstable without any value at the path
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"b\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (4, 'not json')");
        flush();
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"a\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (3, '{\"a\":[2,3]}')");
        execute("INSERT INTO %s (pk) VALUES (5)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.a'] != 1"), row(1), row(3), row(4), row(5));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE doc['$.a'] != 3"), row(1), row(2), row(4), row(5));
            assertRows(execute("SELECT pk FROM %s WHERE doc['$.a'] != 1 AND doc['$.a'] > 2"), row(3));
        });
    }

    /**
     * Tests that invalid CQL queries on JSON paths (missing indexes, wrong types, invalid syntax) are rejected.
     */
    @Test
    public void invalidQueries()
    {
        createJsonTable();

        assertInvalidMessage("No JSON path index", "SELECT pk FROM %s WHERE doc['$.nope'] = 1");
        assertInvalidMessage("string literal", "SELECT pk FROM %s WHERE doc[?] = 1", "$.s");
        assertInvalid("SELECT pk FROM %s WHERE doc['$.tags'] > 'a'"); // range on text path
        assertInvalid("SELECT pk FROM %s WHERE doc['$.s'] = 'x'");    // wrong value type
        assertInvalidMessage("cannot be restricted by both a JSON path relation and",
                             "SELECT pk FROM %s WHERE doc = 'x' AND doc['$.s'] = 1");
        assertInvalidMessage("cannot be restricted by both a JSON path relation and",
                             "SELECT pk FROM %s WHERE doc['$.s'] = 1 AND doc = 'x'");
        assertInvalid("SELECT pk FROM %s WHERE doc['$.s'] IN (1, 2)");
        assertInvalidMessage("Invalid null value", "SELECT pk FROM %s WHERE doc['$.s'] = ?", (Object) null);
        assertInvalid("SELECT pk FROM %s WHERE doc['not a path'] = 1");
    }

    /**
     * Tests that non-text columns cannot be queried with map-element index syntax unless they are maps.
     */
    @Test
    public void nonTextColumnCannotBeUsedAsMap()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        assertInvalidMessage("cannot be used as a map", "SELECT pk FROM %s WHERE v['$.a'] = 1 ALLOW FILTERING");
    }

    /**
     * Tests that standard map entries indexing behavior is unchanged by JSON path index support.
     */
    @Test
    public void mapEntriesIndexUnchanged() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, m map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(m)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, m) VALUES (1, {'k': 1, 'j': 5})");
        execute("INSERT INTO %s (pk, m) VALUES (2, {'k': 2})");
        execute("INSERT INTO %s (pk, m) VALUES (3, {'k': 3, 'j': 0})");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE m['k'] > 1"), row(2), row(3));
            assertRows(execute("SELECT pk FROM %s WHERE m['k'] > 1 AND m['k'] < 3"), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE m['k'] = 1"), row(1));
        });
    }
}
