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
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MonotonicClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The approximate clock that stamps tasks is refreshed by a task on {@link ScheduledExecutors#scheduledFastTasks}.
 * When that single thread is stuck the clock stops advancing; the liveness times must keep growing rather than read 0.
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

    // a submitted request, as Dispatcher.RequestProcessor
    private static final class Request implements DebuggableTask.RunnableDebuggableTask
    {
        final long createdAtNanos = MonotonicClock.Global.preciseTime.now();

        public void run() {}
        public long creationTimeNanos() { return createdAtNanos; }
        public long startTimeNanos() { return 0; }
        public String description() { return "request"; }
    }

    @Test
    public void testTimesGrowWhileApproxTimeIsStalled() throws Exception
    {
        assertTrue(approxTime instanceof MonotonicClock.SampledClock);   // the clock refreshed by scheduledFastTasks
        assertEquals(1, ScheduledExecutors.scheduledFastTasks.getCorePoolSize());

        ExecutorPlus tpe = executorFactory().sequential("liveness-stall");
        SharedExecutorPool pool = new SharedExecutorPool("LivenessStallPool");
        SEPExecutor sep = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStallStage");
        Blocker clockBlocker = new Blocker();
        Blocker tpeRunning = new Blocker();
        Blocker tpeQueued = new Blocker();
        Blocker sepRunning = new Blocker();
        try
        {
            ScheduledExecutors.scheduledFastTasks.execute(clockBlocker);
            assertTrue(clockBlocker.started.await(10, TimeUnit.SECONDS));
            long frozenAt = approxTime.now();

            // stamped after the stall began, so every stamp below is the frozen instant
            tpe.execute(tpeRunning);
            assertTrue(tpeRunning.started.await(10, TimeUnit.SECONDS));
            tpe.execute(tpeQueued);
            sep.execute(sepRunning);
            assertTrue(sepRunning.started.await(10, TimeUnit.SECONDS));
            sep.submit(new Request());                                     // a debuggable head

            Util.spinAssertEquals(true, () -> ScheduledExecutors.scheduledFastTasks.longestRunningTaskTime() >= STALL_NANOS, 5);
            // the clock refresh itself is overdue behind the blocker
            Util.spinAssertEquals(true, () -> ScheduledExecutors.scheduledFastTasks.oldestTaskQueueTime() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> tpe.longestRunningTaskTime() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> tpe.oldestTaskQueueTime() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> sep.longestRunningTaskTime() >= STALL_NANOS, 5);
            Util.spinAssertEquals(true, () -> sep.oldestTaskQueueTime() >= STALL_NANOS, 5);

            assertEquals(frozenAt, approxTime.now());                      // the refresher really was stalled throughout
        }
        finally
        {
            clockBlocker.release.countDown();
            tpeRunning.release.countDown();
            tpeQueued.release.countDown();
            sepRunning.release.countDown();
            tpe.shutdownNow();
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }
}
