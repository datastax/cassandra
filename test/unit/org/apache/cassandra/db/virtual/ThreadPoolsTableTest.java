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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThreadPoolsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

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
        JMXEnabledThreadPoolExecutor es = new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new NamedThreadFactory("VtLiveness"), "internal");
        Blocker a = new Blocker();
        Blocker b = new Blocker();
        try
        {
            UntypedResultSet idle = execute("SELECT * FROM vts.thread_pools WHERE name = 'VtLiveness'");
            UntypedResultSet.Row row = idle.one();
            assertEquals(0L, row.getLong("oldest_queued_task_age_ms"));
            assertEquals(0L, row.getLong("longest_running_task_age_ms"));
            assertFalse(row.has("longest_running_task"));

            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);
            Thread.sleep(50);

            String select = "SELECT * FROM vts.thread_pools WHERE name = 'VtLiveness'";
            Util.spinAssertEquals(true, () -> execute(select).one().getLong("oldest_queued_task_age_ms") >= 40L, 5);
            Util.spinAssertEquals(true, () -> execute(select).one().getLong("longest_running_task_age_ms") >= 40L, 5);
            row = execute(select).one();
            assertEquals(Blocker.class.getName(), row.getString("longest_running_task"));
        }
        finally
        {
            a.release.countDown();
            b.release.countDown();
            es.shutdownNow();
        }
    }
}
