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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertTrue;

public class SinglePartitionReadCommandCQLTest extends ReadCommandCQLTester<SinglePartitionReadCommand>
{
    @Test
    public void partitionLevelDeletionTest()
    {
        createTable("CREATE TABLE %s (bucket_id TEXT,name TEXT,data TEXT,PRIMARY KEY (bucket_id, name))");
        execute("insert into %s (bucket_id, name, data) values ('8772618c9009cf8f5a5e0c18', 'test', 'hello')");
        flush();
        execute("insert into %s (bucket_id, name, data) values ('8772618c9009cf8f5a5e0c19', 'test2', 'hello');");
        execute("delete from %s where bucket_id = '8772618c9009cf8f5a5e0c18'");
        flush();
        UntypedResultSet res = execute("select * from %s where bucket_id = '8772618c9009cf8f5a5e0c18' and name = 'test'");
        assertTrue(res.isEmpty());
    }

    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        assertToCQLString("SELECT * FROM %s WHERE k = 0", "SELECT * FROM %s WHERE k = 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0", "SELECT * FROM %s WHERE k = 0 AND c = 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND (c) = (0)", "SELECT * FROM %s WHERE k = 0 AND c = 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0", "SELECT * FROM %s WHERE k = 0 AND c > 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c > ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0", "SELECT * FROM %s WHERE k = 0 AND c < 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c < ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0", "SELECT * FROM %s WHERE k = 0 AND c >= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c >= ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0", "SELECT * FROM %s WHERE k = 0 AND c <= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c <= ? ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND v = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c = ? AND v = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c > ? AND v = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c < ? AND v = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c >= ? AND v = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = ? AND c <= ? AND v = ? ALLOW FILTERING");

        // test clustering-based ORDER BY
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY c1",
                          "SELECT * FROM %s WHERE k = 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC",
                          "SELECT * FROM %s WHERE k = 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY c1 ASC, c2 ASC",
                          "SELECT * FROM %s WHERE k = 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC",
                          "SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 DESC ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? ORDER BY c1 DESC, c2 DESC ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 DESC",
                          "SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC, c2 DESC ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? ORDER BY c1 DESC, c2 DESC ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1",
                          "SELECT * FROM %s WHERE k = 0 AND c1 > 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? AND c1 > ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1 ASC",
                          "SELECT * FROM %s WHERE k = 0 AND c1 > 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? AND c1 > ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1 ASC, c2 ASC",
                          "SELECT * FROM %s WHERE k = 0 AND c1 > 0 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? AND c1 > ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1 DESC",
                          "SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1 DESC, c2 DESC ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? AND c1 > ? ORDER BY c1 DESC, c2 DESC ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1 DESC, c2 DESC",
                          "SELECT * FROM %s WHERE k = 0 AND c1 > 0 ORDER BY c1 DESC, c2 DESC ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k = ? AND c1 > ? ORDER BY c1 DESC, c2 DESC ALLOW FILTERING");

        // test with multi-column partition key (without index to test SinglePartitionReadCommand directly)
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))");
        assertToCQLString("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2",
                          "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k1 = ? AND k2 = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c = 3",
                          "SELECT * FROM %s WHERE k1 = 1 AND k2 = 2 AND c = 3 ALLOW FILTERING",
                          "SELECT * FROM %s WHERE k1 = ? AND k2 = ? AND c = ? ALLOW FILTERING");

        // test literals
        createTable("CREATE TABLE %s (k text, c text, m map<text, text>, PRIMARY KEY (k, c))");
        assertToCQLString("SELECT m['key'] FROM %s WHERE k = 'k' AND m['key'] = 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = 'k' AND m['key'] = 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = ? AND m[?] = ? ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE k = 'k' AND c = 'c' AND m['key'] = 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = 'k' AND c = 'c' AND m['key'] = 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = ? AND c = ? AND m[?] = ? ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE k = 'k' AND c > 'c' AND m['key'] > 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = 'k' AND c > 'c' AND m['key'] > 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = ? AND c > ? AND m[?] > ? ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE k = 'k' AND c < 'c' AND m['key'] < 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = 'k' AND c < 'c' AND m['key'] < 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = ? AND c < ? AND m[?] < ? ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE k = 'k' AND c >= 'c' AND m['key'] >= 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = 'k' AND c >= 'c' AND m['key'] >= 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = ? AND c >= ? AND m[?] >= ? ALLOW FILTERING");
        assertToCQLString("SELECT m['key'] FROM %s WHERE k = 'k' AND c <= 'c' AND m['key'] <= 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = 'k' AND c <= 'c' AND m['key'] <= 'value' ALLOW FILTERING",
                          "SELECT m FROM %s WHERE k = ? AND c <= ? AND m[?] <= ? ALLOW FILTERING");

        // test with quoted identifiers
        createKeyspace("CREATE KEYSPACE \"K\" WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable("CREATE TABLE \"K\".\"T\" (\"K\" int, \"C\" int, \"S\" int static, \"V\" int, PRIMARY KEY(\"K\", \"C\"))");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = ? ALLOW FILTERING");
        assertToCQLString("SELECT \"K\" FROM \"K\".\"T\" WHERE \"K\" = 0",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = ? ALLOW FILTERING");
        assertToCQLString("SELECT \"S\" FROM \"K\".\"T\" WHERE \"K\" = 0",
                          "SELECT \"S\" FROM \"K\".\"T\" WHERE \"K\" = 0 ALLOW FILTERING",
                          "SELECT \"S\" FROM \"K\".\"T\" WHERE \"K\" = ? ALLOW FILTERING");
        assertToCQLString("SELECT \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0",
                          "SELECT \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0 ALLOW FILTERING",
                          "SELECT \"V\" FROM \"K\".\"T\" WHERE \"K\" = ? ALLOW FILTERING");
        assertToCQLString("SELECT \"K\", \"C\", \"S\", \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0",
                          "SELECT \"S\", \"V\" FROM \"K\".\"T\" WHERE \"K\" = 0 ALLOW FILTERING",
                          "SELECT \"S\", \"V\" FROM \"K\".\"T\" WHERE \"K\" = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" = 1",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" = 1 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = ? AND \"C\" = ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" > 1 AND \"C\" <= 2",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" > 1 AND \"C\" <= 2 ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = ? AND \"C\" > ? AND \"C\" <= ? ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 ORDER BY \"C\" DESC",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 ORDER BY \"C\" DESC ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = ? ORDER BY \"C\" DESC ALLOW FILTERING");
        assertToCQLString("SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" = 1 ORDER BY \"C\" DESC",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = 0 AND \"C\" = 1 ORDER BY \"C\" DESC ALLOW FILTERING",
                          "SELECT * FROM \"K\".\"T\" WHERE \"K\" = ? AND \"C\" = ? ORDER BY \"C\" DESC ALLOW FILTERING");
    }

    @Override
    protected SinglePartitionReadCommand parseCommand(String query)
    {
        return parseReadCommandGroup(query).get(0);
    }
}
