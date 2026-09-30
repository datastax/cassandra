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

package org.apache.cassandra.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MonotonicClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The approximate clock that stamps tasks is refreshed by a task on {@link ScheduledExecutors#scheduledFastTasks}.
 * When that single thread is stuck the clock stops advancing; the liveness ages must keep growing rather than read 0.
 */
public class ExecutorLivenessClockStallTest
{
    private static final long STALL_NANOS = MILLISECONDS.toNanos(200);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
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
    public void testAgesGrowWhileApproxTimeIsStalled() throws Exception
    {
        assertTrue(approxTime instanceof MonotonicClock.SampledClock);   // the clock refreshed by scheduledFastTasks
        assertEquals(1, ScheduledExecutors.scheduledFastTasks.getCorePoolSize());

        DebuggableThreadPoolExecutor dtpe = new DebuggableThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                                                                             new LinkedBlockingQueue<>(),
                                                                             new NamedThreadFactory("liveness-stall"));
        SharedExecutorPool pool = new SharedExecutorPool("LivenessStallPool");
        SEPExecutor sep = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStallStage");
        Blocker clockBlocker = new Blocker();
        Blocker dtpeRunning = new Blocker();
        Blocker dtpeQueued = new Blocker();
        Blocker sepRunning = new Blocker();
        Blocker sepQueued = new Blocker();
        try
        {
            ScheduledExecutors.scheduledFastTasks.execute(clockBlocker);
            assertTrue(clockBlocker.started.await(10, TimeUnit.SECONDS));
            long frozenAt = approxTime.now();

            // stamped after the stall began, so every stamp below is the frozen instant
            dtpe.execute(dtpeRunning);
            assertTrue(dtpeRunning.started.await(10, TimeUnit.SECONDS));
            dtpe.execute(dtpeQueued);
            sep.execute(sepRunning);
            assertTrue(sepRunning.started.await(10, TimeUnit.SECONDS));
            sep.execute(sepQueued);

            Util.spinAssertEquals(true, () -> ScheduledExecutors.scheduledFastTasks.longestRunningTaskAgeNanos() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> dtpe.longestRunningTaskAgeNanos() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> dtpe.oldestQueuedTaskAgeNanos() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> sep.longestRunningTaskAgeNanos() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> sep.oldestQueuedTaskAgeNanos() >= STALL_NANOS, 5);

            assertEquals(frozenAt, approxTime.now());                      // the refresher really was stalled throughout
        }
        finally
        {
            clockBlocker.release.countDown();
            dtpeRunning.release.countDown();
            dtpeQueued.release.countDown();
            sepRunning.release.countDown();
            sepQueued.release.countDown();
            dtpe.shutdownNow();
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }
}
