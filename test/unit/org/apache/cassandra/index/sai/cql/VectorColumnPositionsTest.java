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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAIUtil;

/**
 * Tests vector indexes in different column positions (partition key, clustering key, static and regular columns).
 */
public class VectorColumnPositionsTest extends VectorTester.Versioned
{
    @Before
    public void setupVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testPartitionKey()
    {
        createTable("CREATE TABLE %s (k vector<float, 2> PRIMARY KEY)");
        assertInvalidMessage("Cannot create secondary index on the only partition key column k",
                             "CREATE CUSTOM INDEX ON %s(k) USING 'StorageAttachedIndex'");
    }

    @Test
    public void testPartitionKeyComponentWithColumns() throws Throwable
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

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.4f), 0));
        execute(insert, row(2, vector(0.1f, 0.3f), 0));
        execute(insert, row(3, vector(0.1f, 0.2f), 1));
        execute(insert, row(4, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT k1 FROM %s WHERE v>=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s WHERE v=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s WHERE v>0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
        });
    }

    @Test
    public void testPartitionKeyComponentWithoutColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, v int, PRIMARY KEY((k1, k2)))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k1, k2) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f)));
        execute(insert, row(2, vector(0.1f, 0.2f)));
        execute(insert, row(3, vector(0.1f, 0.3f)));
        execute(insert, row(4, vector(0.1f, 0.4f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.4f)));
        execute(insert, row(2, vector(0.1f, 0.3f)));
        execute(insert, row(3, vector(0.1f, 0.2f)));
        execute(insert, row(4, vector(0.1f, 0.1f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));
        });
    }

    @Test
    public void testPartitionKeyComponentWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, v int, PRIMARY KEY((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // insert static rows and non-static rows all at once
        String insert = "INSERT INTO %s (k1, k2, c, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f), 10, 0));
        execute(insert, row(2, vector(0.1f, 0.2f), 20, 0));
        execute(insert, row(3, vector(0.1f, 0.3f), 30, 1));
        execute(insert, row(4, vector(0.1f, 0.4f), 40, 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT k1 FROM %s WHERE v>=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k1 FROM %s WHERE v=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT k1 FROM %s WHERE v>0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.4f), 10, 0));
        execute(insert, row(2, vector(0.1f, 0.3f), 20, 0));
        execute(insert, row(3, vector(0.1f, 0.2f), 30, 1));
        execute(insert, row(4, vector(0.1f, 0.1f), 40, 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT k1 FROM %s WHERE v>=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s WHERE v=0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT k1 FROM %s WHERE v>0 ORDER BY k2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
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

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.4f), 0));
        execute(insert, row(2, vector(0.1f, 0.3f), 0));
        execute(insert, row(3, vector(0.1f, 0.2f), 1));
        execute(insert, row(4, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY c ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY c ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
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

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, 1, vector(0.1f, 0.4f), 0));
        execute(insert, row(1, 2, vector(0.1f, 0.3f), 0));
        execute(insert, row(2, 3, vector(0.1f, 0.2f), 1));
        execute(insert, row(2, 4, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT c1 FROM %s ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT c1 FROM %s ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT c1 FROM %s WHERE v>=0 ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT c1 FROM %s WHERE v=0 ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT c1 FROM %s WHERE v>0 ORDER BY c2 ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
        });
    }

    @Test
    public void testRegularColumnWithoutClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, r vector<float, 2>, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, r, v) VALUES (?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f), 0));
        execute(insert, row(2, vector(0.1f, 0.2f), 0));
        execute(insert, row(3, vector(0.1f, 0.3f), 1));
        execute(insert, row(4, vector(0.1f, 0.4f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.4f), 0));
        execute(insert, row(2, vector(0.1f, 0.3f), 0));
        execute(insert, row(3, vector(0.1f, 0.2f), 1));
        execute(insert, row(4, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
        });
    }

    @Test
    public void testRegularColumnWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, r vector<float, 2>, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, c, r, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(0, 1, vector(0.1f, 0.1f), 0));
        execute(insert, row(1, 2, vector(0.1f, 0.2f), 0));
        execute(insert, row(0, 3, vector(0.1f, 0.3f), 1));
        execute(insert, row(1, 4, vector(0.1f, 0.4f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));

            // query with hybrid search
            assertRows(execute("SELECT c FROM %s WHERE v>=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT c FROM %s WHERE v=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT c FROM %s WHERE v>0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });

        // update the best vector to make it the worst
        execute(insert, row(0, 1, vector(0.1f, 0.5f), 0));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 2"), row(2), row(3));

            // query with hybrid search
            assertRows(execute("SELECT c FROM %s WHERE v>=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT c FROM %s WHERE v=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT c FROM %s WHERE v>0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(0, 1, vector(0.1f, 0.4f), 0));
        execute(insert, row(1, 2, vector(0.1f, 0.3f), 0));
        execute(insert, row(0, 3, vector(0.1f, 0.2f), 1));
        execute(insert, row(1, 4, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT c FROM %s WHERE v>=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT c FROM %s WHERE v=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT c FROM %s WHERE v>0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
        });

        // update the best vector to make it the worst
        execute(insert, row(1, 4, vector(0.1f, 0.5f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT c FROM %s ORDER BY r ANN OF [0.1, 0.1] LIMIT 2"), row(3), row(2));

            // query with hybrid search
            assertRows(execute("SELECT c FROM %s WHERE v>=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT c FROM %s WHERE v=0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT c FROM %s WHERE v>0 ORDER BY r ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });
    }

    @Test
    public void testStaticColumnWithoutRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");

        // insert static rows alone, without non-static rows
        String insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f)));
        execute(insert, row(2, vector(0.1f, 0.2f)));
        execute(insert, row(3, vector(0.1f, 0.3f)));
        execute(insert, row(4, vector(0.1f, 0.4f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(1), row(2));
        });

        // update the best vector to make it the worst
        insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.5f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(2), row(3));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.4f)));
        execute(insert, row(2, vector(0.1f, 0.3f)));
        execute(insert, row(3, vector(0.1f, 0.2f)));
        execute(insert, row(4, vector(0.1f, 0.1f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));
        });

        // update the best vector to make it the worst
        insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(4, vector(0.1f, 0.5f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(3), row(2));
        });
    }

    @Test
    public void testStaticColumnWithRowsTogether() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // insert static rows and non-static rows all at once
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
        execute(insert, row(1, 10, vector(0.1f, 0.5f), 0));
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

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, 10, vector(0.1f, 0.4f), 0));
        execute(insert, row(2, 20, vector(0.1f, 0.3f), 0));
        execute(insert, row(3, 30, vector(0.1f, 0.2f), 1));
        execute(insert, row(4, 40, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(4), row(3));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3));
        });

        // update the best vector to make it the worst
        execute(insert, row(4, 40, vector(0.1f, 0.5f), 1));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
        });

        // update the worst vector to make it the best
        execute(insert, row(4, 40, vector(0.1f, 0.1f), 1));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(4), row(3), row(2), row(1));
        });
    }

    @Test
    public void testStaticColumnWithRowsSeparate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // insert static rows alone, without non-static rows
        String insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f)));
        execute(insert, row(2, vector(0.1f, 0.2f)));
        execute(insert, row(3, vector(0.1f, 0.3f)));
        execute(insert, row(4, vector(0.1f, 0.4f)));
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"));
        });

        // insert non-static rows, overlapping and non-overlapping with static rows
        insert = "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)";
        execute(insert, row(1, 10, 0));
        execute(insert, row(1, 11, 0));
        execute(insert, row(2, 20, 0));
        execute(insert, row(2, 21, 0));
        execute(insert, row(3, 30, 1));
        execute(insert, row(4, 40, 1));
        execute(insert, row(5, 50, 1)); // this one does not have a static row
        beforeAndAfterFlush(() -> {
            // query with ANN only
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));

            // query with hybrid search
            assertRows(execute("SELECT k FROM %s WHERE v>=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2));
            assertRows(execute("SELECT k FROM %s WHERE v>0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(4));
        });

        // update the best vector to make it the worst
        insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.5f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
        });

        // update the worst vector to make it the best
        insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.1f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1), row(2), row(3), row(4));
        });
    }

    @Test
    public void testStaticColumnWithDifferentSourcesWithRegulars() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // insert static rows alone, with non-static rows
        String insert = "INSERT INTO %s (k, s, c, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.4f), 1, 0));
        execute(insert, row(2, vector(0.1f, 0.3f), 1, 1));
        execute(insert, row(3, vector(0.1f, 0.2f), 1, 0));
        execute(insert, row(4, vector(0.1f, 0.1f), 1, 1));
        flush();

        // update the best vector to make it the worst, with non-static rows
        execute(insert, row(4, vector(0.1f, 0.5f), 1, 1));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(3), row(2));

            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(3));

            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(2));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.1f), 1, 0));
        execute(insert, row(2, vector(0.1f, 0.2f), 1, 1));
        execute(insert, row(3, vector(0.1f, 0.3f), 1, 0));
        execute(insert, row(4, vector(0.1f, 0.4f), 1, 1));
        flush();

        // update the best vector to make it the worst, with non-static rows
        execute(insert, row(1, vector(0.1f, 0.5f), 1, 0));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(2), row(3));

            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(3));

            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(2));
        });
    }

    @Test
    public void testStaticColumnWithDifferentSourcesWithoutRegulars() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");

        // insert static rows alone, without non-static rows
        String insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.4f)));
        execute(insert, row(2, vector(0.1f, 0.3f)));
        execute(insert, row(3, vector(0.1f, 0.2f)));
        execute(insert, row(4, vector(0.1f, 0.1f)));
        flush();

        // update the best vector to make it the worst, without non-static rows
        execute(insert, row(4, vector(0.1f, 0.5f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(3), row(2));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.1f)));
        execute(insert, row(2, vector(0.1f, 0.2f)));
        execute(insert, row(3, vector(0.1f, 0.3f)));
        execute(insert, row(4, vector(0.1f, 0.4f)));
        flush();

        // update the best vector to make it the worst, without non-static rows
        execute(insert, row(1, vector(0.1f, 0.5f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(2), row(3));
        });
    }

    @Test
    public void testStaticColumnWithDifferentSourcesWithAndWithoutRegulars() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s vector<float, 2> static, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // insert static rows alone, with non-static rows
        String insert = "INSERT INTO %s (k, s, c, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.4f), 1, 0));
        execute(insert, row(2, vector(0.1f, 0.3f), 1, 1));
        execute(insert, row(3, vector(0.1f, 0.2f), 1, 0));
        execute(insert, row(4, vector(0.1f, 0.1f), 1, 1));
        flush();

        // update the best vector to make it the worst, without non-static rows
        insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        execute(insert, row(4, vector(0.1f, 0.5f)));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(2), row(1), row(4));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(3), row(2));

            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(3), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(3));

            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(4));
            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(2));
        });

        // test again with a different order
        execute("TRUNCATE TABLE %s");
        execute(insert, row(1, vector(0.1f, 0.1f)));
        execute(insert, row(2, vector(0.1f, 0.2f)));
        execute(insert, row(3, vector(0.1f, 0.3f)));
        execute(insert, row(4, vector(0.1f, 0.4f)));
        flush();

        // update the best vector to make it the worst, without non-static rows
        insert = "INSERT INTO %s (k, s, c, v) VALUES (?, ?, ?, ?)";
        execute(insert, row(1, vector(0.1f, 0.5f), 1, 0));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(2), row(3), row(4), row(1));
            assertRows(execute("SELECT k FROM %s ORDER BY s ANN OF [0.1, 0.1] LIMIT 2"), row(2), row(3));

            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"), row(1));
            assertRows(execute("SELECT k FROM %s WHERE v=0 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"), row(1));

            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 10"));
            assertRows(execute("SELECT k FROM %s WHERE v=1 ORDER BY s ANN OF [0.1, 0.1] LIMIT 1"));
        });
    }
}
