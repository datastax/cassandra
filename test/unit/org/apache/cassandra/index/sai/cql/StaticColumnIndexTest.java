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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

public class StaticColumnIndexTest extends SAITester
{
    @Test
    public void staticIndexReturnsAllRowsInPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val1 int static, val2 int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 1, 1, 2, 1);
        execute("INSERT INTO %s(pk, ck,       val2) VALUES(?, ?,    ?)", 1, 2,    2);
        execute("INSERT INTO %s(pk, ck,       val2) VALUES(?, ?,    ?)", 1, 3,    3);

        beforeAndAfterFlush(() -> assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 2"),
                                             row(1, 1, 2, 1), row(1, 2, 2, 2), row(1, 3, 2, 3)));
    }

    @Test
    public void staticIndexAndNonStaticIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val1 int static, val2 int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 1, 1, 20, 1000);
        execute("INSERT INTO %s(pk, ck,       val2) VALUES(?, ?,    ?)", 1, 2,     2000);
        execute("INSERT INTO %s(pk, ck, val1, val2) VALUES(?, ?, ?, ?)", 2, 1, 40, 2000);

        beforeAndAfterFlush(() -> assertRows(execute("SELECT pk, ck, val1, val2 FROM %s WHERE val1 = 20 AND val2 = 2000"),
                                             row(1, 2, 20, 2000)));
    }

    @Test
    public void staticAndNonStaticRangeIntersectionOnePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, s1 int static, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s1) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 1, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 2, 1);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 3, 2);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 4, 3);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 5, 4);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 6, 5);

        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 0, 100);

        beforeAndAfterFlush(() -> assertRowCount(execute("SELECT * FROM %s WHERE pk = ? AND v1 > ? AND s1 = ?", 0, 2, 100), 3));
    }

    @Test
    public void staticAndNonStaticRangeIntersectionManyPartitions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, s1 int static, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s1) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 1, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 2, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 3, 2);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 4, 2);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 0, 5, 2);

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1, 1, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1, 2, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1, 3, 2);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1, 4, 2);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1, 5, 2);

        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 2, 2, 0);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 2, 2, 1);
        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 2, 2, 1);

        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 0, 100);
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 1, 200);
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 2, 300);


        beforeAndAfterFlush(() -> {
            UntypedResultSet result = execute("SELECT * FROM %s WHERE v1 = ? AND s1 > ?", 2, 99);
            assertRowCount(result, 6);
        });
    }
}
