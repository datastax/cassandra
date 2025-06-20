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

package org.apache.cassandra.index.sai.plan;

import java.util.concurrent.ForkJoinPool;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test to cover edge cases related to memtable flush during query execution.
 */
public class FlushIndexWhileQueryingTest extends SAITester
{
    @Test
    public void testFlushAfterQueryViewBuildEqualityQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x int)");

        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "a", 0);
        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "b", 0);
        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "c", 1);

        testFlushAfterQueryViewBuiltBeforeQueryExecution("SELECT k FROM %s WHERE x = 0", 2);
    }

    @Test
    public void testFlushAfterQueryViewBuildNotContainsQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x set<int>)");

        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, x) VALUES ('a', {1, 2, 3})");
        execute("INSERT INTO %s (k, x) VALUES ('b', {1, 2, 3})");
        execute("INSERT INTO %s (k, x) VALUES ('c', {1, 2, 4})");

        testFlushAfterQueryViewBuiltBeforeQueryExecution("SELECT k FROM %s WHERE x NOT CONTAINS 3", 1);
    }

    private void testFlushAfterQueryViewBuiltBeforeQueryExecution(String query, int expectedRowCount) throws Throwable
    {
        // We use a barrier to trigger flush at precisely the right time
        var viewBuildPoint = InvokePointBuilder.newInvokePoint().onClass(QueryView.Builder.class).onMethod("build").atExit();
        var viewBuildBarrier = Injections.newBarrier("pause_query_view_build", 2, false).add(viewBuildPoint).build();
        var getViewPoint = InvokePointBuilder.newInvokePoint().onClass(QueryController.class).onMethod("getQueryView").atExit();
        var getViewBarrier = Injections.newBarrier("pause_get_query_view", 2, false).add(getViewPoint).build();
        Injections.inject(viewBuildBarrier, getViewBarrier);

        // Flush in a separate thread to allow the query to run concurrently
        ForkJoinPool.commonPool().submit(() -> {
            try
            {
                viewBuildBarrier.arrive();
                flush();
                getViewBarrier.arrive();
            }
            catch (InterruptedException t)
            {
                throw new RuntimeException(t);
            }
        });

        assertRowCount(execute(query), expectedRowCount);
        assertEquals("Confirm that we hit the barrier", 0, viewBuildBarrier.getCount());
        assertEquals("Confirm that we hit the barrier", 0, getViewBarrier.getCount());
    }
}