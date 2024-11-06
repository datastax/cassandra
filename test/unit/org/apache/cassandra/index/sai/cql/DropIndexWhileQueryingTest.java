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

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;

import org.junit.Test;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertTrue;

public class DropIndexWhileQueryingTest extends SAITester
{
    @Test
    public void testReproductionForCNDB10732() throws Throwable
    {
        createTable("CREATE TABLE %s (electronicVehicle TEXT PRIMARY KEY, modelYear INT, make TEXT, vehType TEXT) WITH compaction = " +
                "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }");

        createIndex("CREATE CUSTOM INDEX ON %s(make) USING 'StorageAttachedIndex'");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(modelYear) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vehType) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        Injection injection = Injections.newCustom("drop_index")
                                        .add(newInvokePoint()
                                             .onClass(QueryController.class)
                                             .onMethod("buildPlan")
                                             .atEntry())
                                        .add(ActionBuilder
                                             .newActionBuilder()
                                             .actions()
                                             .doAction("org.apache.cassandra.index.sai.cql.DropIndexWhileQueryingTest" +
                                                       ".dropIndexForBytemanInjections" +
                                                       "(\"DROP INDEX cql_test_keyspace." + indexName + "\")"))
                                        .build();

        Injections.inject(injection);
        injection.enable();
        assertTrue("Injection should be enabled", injection.isEnabled());
        execute("INSERT INTO %s (electronicVehicle, modelYear, make, vehType) VALUES (?, ?, ?, ?)", "car", 1998, "Ford", "SUV");
        assertInvalidMessage("An index may have been dropped.Cannot execute this query as it might involve " +
                             "data filtering and thus may have unpredictable performance. If you want to execute this query" +
                             " despite the performance unpredictability, use ALLOW FILTERING",
                             "SELECT * FROM %s WHERE modelYear IN (1998, 2000) OR (make IN ('FORD', 'OPEL' ) OR vehType IN ('car', 'truck'));");
    }

    @Test
    public void testReproductionForCNDB10535() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        Injection injection = Injections.newCustom("drop_index2")
                                        .add(newInvokePoint()
                                             .onClass(QueryController.class)
                                             .onMethod("buildPlan")
                                             .atExit())
                                        .add(ActionBuilder
                                             .newActionBuilder()
                                             .actions()
                                             .doAction("org.apache.cassandra.index.sai.cql.DropIndexWhileQueryingTest" +
                                                       ".dropIndexForBytemanInjections" +
                                                       "(\"DROP INDEX cql_test_keyspace." + indexName + "\")"))
                                        .build();

        Injections.inject(injection);
        injection.enable();
        assertTrue("Injection should be enabled", injection.isEnabled());

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        assertInvalidMessage("An index may have been dropped. Ordering on non-clustering column requires the " +
                             "column to be indexed", "SELECT pk FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
    }

    // the method is used by the byteman rule to drop the index
    public static void dropIndexForBytemanInjections(String query)
    {
        String fullQuery = String.format(query, KEYSPACE);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }
}
