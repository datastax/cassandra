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

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.TopKProcessor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DropIndexWhileQueryingTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "version={0}")
    public static Collection<Object> data()
    {
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                          .map(v -> new Object[]{ v})
                          .collect(Collectors.toList());
    }

    @Before
    public void setCurrentSAIVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }
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
        assertThatThrownBy(() -> executeInternal(query)).hasMessage(QueryController.INDEX_MAY_HAVE_BEEN_DROPPED);
        assertThatThrownBy(() -> executeInternal(query)).hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    @Test
    public void testFallbackToAnotherIndexMemtable() throws Throwable
    {
        testFallbackToAnotherIndex(false);
    }

    @Test
    public void testFallbackToAnotherIndexSSTable() throws Throwable
    {
        testFallbackToAnotherIndex(true);
    }

    public void testFallbackToAnotherIndex(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x int, y text, z text)");

        createIndex("CREATE CUSTOM INDEX ON %s(y) USING 'StorageAttachedIndex'");
        String indexName1 = createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        String indexName2 = createIndex("CREATE CUSTOM INDEX ON %s(z) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // TODO this isn't the right place to drop the iterator when we have the histograms. getQueryView
        // might be better but it might get called at the wrong time and might not lead to the right
        // coverage for the fallback logic this test is meant to cover.
        injectIndexDropInGetQueryView("drop_index_1", indexName1);
        injectIndexDropInGetQueryView("drop_index_2", indexName2);

        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "k1", 0, "y0", "z0"); // match
        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "k2", 0, "y1", "z2"); // no match
        execute("INSERT INTO %s (k, x, y, z) VALUES (?, ?, ?, ?)", "k3", 5, "y2", "z0"); // no match
        String query = "SELECT * FROM %s WHERE x = 0 AND y = 'y0' AND z = 'z0'";
        if (flush)
            flush();
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
        assertThatThrownBy(() -> executeInternal(query)).hasMessage(TopKProcessor.INDEX_MAY_HAVE_BEEN_DROPPED);
        assertThatThrownBy(() -> executeInternal(query)).hasMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"));
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

    private static void injectIndexDropInGetQueryView(String injectionName, String indexName) throws Throwable
    {
        // Inject a byteman rule that will drop the index when the getQueryView method is called only for that particular index.
        InvokePointBuilder invokePoint = newInvokePoint().onClass(QueryController.class).onMethod("getQueryView");
        Injection injection = Injections.newCustom(injectionName)
                                        .add(invokePoint.atEntry())
                                        .add(ActionBuilder
                                             .newActionBuilder()
                                             .actions()
                                             .doAction("org.apache.cassandra.index.sai.cql.DropIndexWhileQueryingTest" +
                                                       ".dropIndexForBytemanInjections(\"" + indexName + "\", $1);"))
                                        .build();
        Injections.inject(injection);
        injection.enable();
        assertTrue("Injection should be enabled", injection.isEnabled());
    }


    // the method is used by the byteman rule to drop the index conditionally when running with given context
    @SuppressWarnings("unused")
    public static void dropIndexForBytemanInjections(String indexName, IndexContext context)
    {
        if (context.getIndexName().equals(indexName))
            dropIndexForBytemanInjections(indexName);
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
