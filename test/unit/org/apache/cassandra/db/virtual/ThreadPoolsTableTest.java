/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.db.virtual;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThreadPoolsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String POOL = "VtLiveness";
    private static final String SELECT = "SELECT * FROM vts.thread_pools WHERE name = '" + POOL + "'";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new ThreadPoolsTable(KS_NAME))));
    }

    private static final class Blocker implements Runnable
    {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public void run()
        {
            started.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    @Test
    public void testLivenessColumns() throws Throwable
    {
        ExecutorPlus es = executorFactory().withJmxInternal().sequential(POOL);
        Blocker a = new Blocker();
        Blocker b = new Blocker();
        try
        {
            UntypedResultSet.Row row = execute(SELECT).one();
            assertEquals(0L, row.getLong("oldest_task_queue_micros"));
            assertEquals(0L, row.getLong("longest_running_task_micros"));
            assertFalse(row.has("longest_running_task_class"));

            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);
            Thread.sleep(50);

            Util.spinAssertEquals(true, () -> execute(SELECT).one().getLong("oldest_task_queue_micros") >= 40_000L, 5);
            Util.spinAssertEquals(true, () -> execute(SELECT).one().getLong("longest_running_task_micros") >= 40_000L, 5);
            assertEquals(Blocker.class.getName(), execute(SELECT).one().getString("longest_running_task_class"));
        }
        finally
        {
            a.release.countDown();
            b.release.countDown();
            es.shutdownNow();
        }
    }
}
