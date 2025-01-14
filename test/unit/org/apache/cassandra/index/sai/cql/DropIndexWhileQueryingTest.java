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
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x int, y text, z text)");

        createIndex("CREATE CUSTOM INDEX ON %s(y) USING 'StorageAttachedIndex'");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(z) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        injectIndexDrop("drop_index", indexName, "buildPlan", true);

        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "car", 0, "y0", "z0");
        String query = "SELECT * FROM %s WHERE x IN (0, 1) OR (y IN ('Y0', 'Y1' ) OR z IN ('z1', 'z2'))";
        assertInvalidMessage(QueryController.INDEX_MAY_HAVE_BEEN_DROPPED, query);
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, query);
    }

    @Test
    public void testFallbackToAnotherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x int, y text, z text)");

        createIndex("CREATE CUSTOM INDEX ON %s(y) USING 'StorageAttachedIndex'");
        String indexName1 = createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        String indexName2 = createIndex("CREATE CUSTOM INDEX ON %s(z) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        injectIndexDrop("drop_index_1", indexName1, "buildIterator", true);
        injectIndexDrop("drop_index_2", indexName2, "buildIterator", true);

        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "k1", 0, "y0", "z0"); // match
        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "k2", 0, "y1", "z2"); // no match
        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "k3", 5, "y2", "z0"); // no match
        String query = "SELECT * FROM %s WHERE x = 0 AND y = 'y0' AND z = 'z0'";
        assertRowCount(execute(query), 1);
    }

    // See CNDB-10535
    @Test
    public void testDropVectorIndexWhileQuerying() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        injectIndexDrop("drop_index2", indexName, "buildPlan", false);

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");

        String query = "SELECT pk FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2";
        assertInvalidMessage(TopKProcessor.INDEX_MAY_HAVE_BEEN_DROPPED, query);
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"), query);
    }

    private static void injectIndexDrop(String injectionName, String indexName, String methodName, boolean atEntry) throws Throwable
    {
        InvokePointBuilder invokePoint = newInvokePoint().onClass(QueryController.class).onMethod(methodName);
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
        String fullQuery = String.format("DROP INDEX IF EXISTS %s.%s", KEYSPACE, indexName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }
}
