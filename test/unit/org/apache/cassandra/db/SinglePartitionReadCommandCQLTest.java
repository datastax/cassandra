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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertTrue;

public class SinglePartitionReadCommandCQLTest extends CQLTester
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

        testToCQLString("SELECT * FROM %s WHERE k = 0", "SELECT * FROM %s WHERE k = 0");

        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0", "SELECT * FROM %s WHERE k = 0 AND (c) = (0)");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND (c) = (0)", "SELECT * FROM %s WHERE k = 0 AND (c) = (0)");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0", "SELECT * FROM %s WHERE k = 0 AND c > 0");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0", "SELECT * FROM %s WHERE k = 0 AND c < 0");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0", "SELECT * FROM %s WHERE k = 0 AND c >= 0");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0", "SELECT * FROM %s WHERE k = 0 AND c <= 0");

        testToCQLString("SELECT * FROM %s WHERE k = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 AND (c) = (0)");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 AND c > 0");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 AND c < 0");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 AND c >= 0");
        testToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 AND c <= 0");
    }

    private void testToCQLString(String query, String expected)
    {
        List<SinglePartitionReadCommand> commands = parseReadCommandGroup(query);
        Assertions.assertThat(commands.get(0).toCQLString())
                  .isEqualTo(formatQuery(expected));
    }
}
