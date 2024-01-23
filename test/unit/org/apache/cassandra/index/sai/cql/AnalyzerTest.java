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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.plan.QueryController;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AnalyzerTest extends VectorTester
{
    @Test
    public void createAnalyzerWrongTypeTest()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, val2 int, PRIMARY KEY((pk1, pk2)))");
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(pk2) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'b', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'b', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'b', 3)");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'a', -1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'a', -2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'a', -3)");

        flush();

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'};");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'd', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'd', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'd', 3)");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'c', -1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'c', -2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'c', -3)");
    }
}
