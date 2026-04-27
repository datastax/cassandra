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

package org.apache.cassandra.index.sai.plan;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.view.IndexViewManager;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * When a query is in progress and an index build fails, we need to mark the index as non-queryable instead of
 * returning partial or incorrect results.
 */
public class FailedSSTableIndexLoadWhileQueryingTest extends SAITester
{

    @Test
    public void testSSTableIndexInitFailsAfterQueryViewBuildEqualityQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x int)");

        var indexName = createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "a", 0);
        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "b", 0);
        execute("INSERT INTO %s (k, x) VALUES (?, ?)", "c", 1);

        testSSTableIndexInitFailsAfterQueryViewBuiltBeforeQueryExecution(indexName, "SELECT k FROM %s WHERE x = 0");
    }

    @Test
    public void testSSTableIndexInitFailsAfterQueryViewBuildNotContainsQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, x set<int>)");

        var indexName = createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, x) VALUES ('a', {1, 2, 3})");
        execute("INSERT INTO %s (k, x) VALUES ('b', {1, 2, 3})");
        execute("INSERT INTO %s (k, x) VALUES ('c', {1, 2, 4})");

        testSSTableIndexInitFailsAfterQueryViewBuiltBeforeQueryExecution(indexName, "SELECT k FROM %s WHERE x NOT CONTAINS 3");
    }

    private void testSSTableIndexInitFailsAfterQueryViewBuiltBeforeQueryExecution(String indexName, String query) throws Throwable
    {
        // This bug is only reachable when you fail to replace an sstable via compaction.
        flush();

        Injection failSSTableIndexLoadOnInit = Injections.newCustom("FailSSTableIndexLoadOnInit-" + indexName)
                                                         .add(InvokePointBuilder.newInvokePoint()
                                                                                .onClass("org.apache.cassandra.index.sai.SSTableIndex")
                                                                                .onMethod("<init>")
                                                                                .atEntry()
                                                         )
                                                         .add(ActionBuilder.newActionBuilder().actions()
                                                                           .doThrow(java.lang.RuntimeException.class, Expression.quote("Byteman-injected fault in MemtableIndexWriter.complete"))
                                                         )
                                                         .build();
        Injections.inject(failSSTableIndexLoadOnInit);

        // We use two barriers to ensure that we first flush and fail to load the index (thereby putting in place
        // an invalid view) and then to make sure the query gets that view.
        var badViewPoint = InvokePointBuilder.newInvokePoint().onClass(IndexViewManager.class).onMethod("update").atExit();
        var badViewBarrier = Injections.newBarrier("pause_after_setting_bad_view", 2, false).add(badViewPoint).build();
        Injections.inject(badViewBarrier);

        // Flush in a separate thread since the badViewPointBarrier will block it, thereby preventing it from
        // marking the index as non-queryable.
        ForkJoinPool.commonPool().submit(() -> compact());

        // Wait for compaction to reach the badViewBarrier (the point where we know the view is bad but the
        // index is still considered queryable).
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> badViewBarrier.getCount() == 1);

        // The query hits the getViewBarrier.
        assertThrows(ReadFailureException.class, () -> execute(query));
        assertEquals("Confirm that flush hit the barrier, but did not pass it", 1, badViewBarrier.getCount());

        // Confirm index is considered queryable. The primary point of the remaining assertions is to show to readers
        // that we have a period of time after creating a broken view and before marking the index as non-queryable.
        assertTrue(isIndexQueryable(KEYSPACE, indexName));
        // Arrive and unblock the thread that will mark the index as non-queryable.
        badViewBarrier.arrive();
        // Expect index to go non-queryable soon.
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> !isIndexQueryable(KEYSPACE, indexName));
    }
}
