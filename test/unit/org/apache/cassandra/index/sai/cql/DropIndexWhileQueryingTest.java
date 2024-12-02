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

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.TopKProcessor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import org.junit.Test;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertTrue;

public class DropIndexWhileQueryingTest extends SAITester
{
    // See CNDB-10732
    @Test
    public void testDropIndexWhileQuerying() throws Throwable

    {
        createTable("CREATE TABLE %s (electronicVehicle TEXT PRIMARY KEY, modelYear INT, make TEXT, vehType TEXT) WITH compaction = " +
                "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }");

        createIndex("CREATE CUSTOM INDEX ON %s(make) USING 'StorageAttachedIndex'");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(modelYear) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vehType) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        injectIndexDrop("drop_index", indexName, true);

        execute("INSERT INTO %s (electronicVehicle, modelYear, make, vehType) VALUES (?, ?, ?, ?)", "car", 1998, "Ford", "SUV");
        String query = "SELECT * FROM %s WHERE modelYear IN (1998, 2000) OR (make IN ('FORD', 'OPEL' ) OR vehType IN ('car', 'truck'))";
        assertInvalidMessage(QueryController.INDEX_MAY_HAVE_BEEN_DROPPED, query);
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);
    }

    // See CNDB-10535
    @Test
    public void testDropVectorIndexWhileQuerying() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        injectIndexDrop("drop_index2", indexName, false);

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");

        String query = "SELECT pk FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2";
        assertInvalidMessage(TopKProcessor.INDEX_MAY_HAVE_BEEN_DROPPED, query);
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"), query);
    }

    private static void injectIndexDrop(String injectionName, String indexName, boolean atEntry) throws Throwable
    {
        InvokePointBuilder invokePoint = newInvokePoint().onClass(QueryController.class).onMethod("buildPlan");
        Injection injection = Injections.newCustom(injectionName)
                                        .add(atEntry ? invokePoint.atEntry() : invokePoint.atExit())
                                        .add(ActionBuilder
                                             .newActionBuilder()
                                             .actions()
                                             .doAction("org.apache.cassandra.index.sai.cql.DropIndexWhileQueryingTest" +
                                                       ".dropIndexForBytemanInjections(\"" + indexName + "\")"))
                                        .build();
        Injections.inject(injection);
        injection.enable();
        assertTrue("Injection should be enabled", injection.isEnabled());
    }

    // the method is used by the byteman rule to drop the index
    @SuppressWarnings("unused")
    public static void dropIndexForBytemanInjections(String indexName)
    {
        String fullQuery = String.format("DROP INDEX %s.%s", KEYSPACE, indexName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }
}
