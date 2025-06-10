/*
 * Copyright DataStax, Inc.
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

import org.junit.Test;

/**
 * Tests vector indexes in different column positions (primary key, clustering, static, regular)
 */
public class VectorColumnPositionsTest extends VectorTester
{
    @Test
    public void testPartitionKey()
    {
        createTable("CREATE TABLE %s (k vector<float, 2> PRIMARY KEY)");
        assertInvalidMessage("Cannot create secondary index on the only partition key column k",
                             "CREATE CUSTOM INDEX ON %s(k) USING 'StorageAttachedIndex'");
    }

    @Test
    public void testPartitionKeyComponent() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, v int, PRIMARY KEY((k1, k2)))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f), 0));
        execute(insert, row(2, vector(0.1f, 0.2f), 0));
        execute(insert, row(3, vector(0.1f, 0.3f), 1));
        execute(insert, row(4, vector(0.1f, 0.4f), 1));

        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT k1 FROM %s WHERE v>=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k1 FROM %s WHERE v=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT k1 FROM %s WHERE v>0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });
    }

    @Test
    public void testClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c vector<float, 2>, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f), 0));
        execute(insert, row(2, vector(0.1f, 0.2f), 0));
        execute(insert, row(3, vector(0.1f, 0.3f), 1));
        execute(insert, row(4, vector(0.1f, 0.4f), 1));

        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY c ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });
    }

    @Test
    public void testClusteringKeyComponent() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 vector<float, 2>, v int, PRIMARY KEY(k, c1, c2))");
        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(1, 1, vector(0.1f, 0.1f), 0));
        execute(insert, row(1, 2, vector(0.1f, 0.2f), 0));
        execute(insert, row(2, 3, vector(0.1f, 0.3f), 1));
        execute(insert, row(2, 4, vector(0.1f, 0.4f), 1));

        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT c1 FROM %s ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT c1 FROM %s ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT c1 FROM %s WHERE v>=0 ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT c1 FROM %s WHERE v=0 ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT c1 FROM %s WHERE v>0 ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, c, s, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(1, 10, vector(0.1f, 0.1f), 0));
        execute(insert, row(2, 20, vector(0.1f, 0.2f), 0));
        execute(insert, row(3, 30, vector(0.1f, 0.3f), 1));
        execute(insert, row(4, 40, vector(0.1f, 0.4f), 1));

        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });

        // update the best vector to make it the worst
        execute("INSERT INTO %s (k, c, s) VALUES (?, ?, ?)", row(1, 10, vector(0.1f, 0.5f)));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
        });

        // update the worst vector to make it the best
        execute(insert, row(1, 10, vector(0.1f, 0.1f), 0));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
        });
    }
}
