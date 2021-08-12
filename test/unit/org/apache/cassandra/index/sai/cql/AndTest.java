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

import static org.junit.Assert.assertEquals;

public class AndTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int, val2 int)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val, val2) VALUES ('0', 0, 0)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('1', 1, 1)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('2', 2, 2)");

        flush();

        execute("INSERT INTO %s (id, val, val2) VALUES ('9', 9, 9)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('10', 10, 10)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('11', 11, 11)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('12', 12, 12)");

        //flush();

        //assertEquals(1, execute("SELECT id FROM %s WHERE val = 0 AND val2 = 0").size());
        //assertEquals(0, execute("SELECT id FROM %s WHERE val = 0 AND val2 = 1").size());

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val = 10 AND val2 = 10");
        System.out.println("val=" + result.one().getInt("val")
                           + " val2=" + result.one().getInt("val2"));

        assertEquals(1, result.size());
    }
}
