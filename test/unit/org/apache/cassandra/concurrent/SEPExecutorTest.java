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

package org.apache.cassandra.concurrent;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.concurrent.DebuggableThreadPoolExecutorTest.checkLocalStateIsPropagated;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.assertj.core.api.Assertions.assertThat;

public class SEPExecutorTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void shutdownTest() throws Throwable
    {
        for (int i = 0; i < 1000; i++)
        {
            shutdownOnce(i);
        }
    }

    private static void shutdownOnce(int run) throws Throwable
    {
        SharedExecutorPool sharedPool = new SharedExecutorPool("SharedPool");
        String MAGIC = "UNREPEATABLE_MAGIC_STRING";
        OutputStream nullOutputStream = new OutputStream() {
            public void write(int b) { }
        };
        try (PrintStream nullPrintSteam = new PrintStream(nullOutputStream))
        {
            for (int idx = 0; idx < 20; idx++)
            {
                ExecutorService es = sharedPool.newExecutor(FBUtilities.getAvailableProcessors(), "STAGE", run + MAGIC + idx);
                // Write to black hole
                es.execute(() -> nullPrintSteam.println("TEST" + es));
            }
        }

        // shutdown does not guarantee that threads are actually dead once it exits, only that they will stop promptly afterwards
        sharedPool.shutdownAndWait(1L, TimeUnit.MINUTES);
        for (Thread thread : Thread.getAllStackTraces().keySet())
        {
            if (thread.getName().contains(MAGIC))
            {
                thread.join(1000);
                if (thread.isAlive())
                    Assert.fail(thread + " is still running " + Arrays.toString(thread.getStackTrace()));
            }
        }
    }

    private static class BusyExecutor
    {
        // Number of busy worker threads to run and gum things up. Chosen to be
        // between the low and high max pool size so the test exercises resizing
        // under a number of different conditions.
        static final int numBusyWorkers = 2;
        final AtomicInteger notifiedMaxPoolSize = new AtomicInteger();

        SharedExecutorPool sharedPool;
        LocalAwareExecutorPlus executor;

        Thread makeBusy;
        AtomicBoolean stayBusy;

        public BusyExecutor(String poolName, String executorName)
        {
            sharedPool = new SharedExecutorPool(poolName);
            executor = sharedPool.newExecutor(0, notifiedMaxPoolSize::set, "internal", executorName);
        }

        public void start()
        {
            // Keep feeding the executor work while resizing
            // so it stays under load.
            stayBusy = new AtomicBoolean(true);
            Semaphore busyWorkerPermits = new Semaphore(numBusyWorkers);
            makeBusy = new Thread(() -> {
                while (stayBusy.get())
                {
                    try
                    {
                        if (busyWorkerPermits.tryAcquire(1, MILLISECONDS)) {
                            executor.execute(new BusyWork(busyWorkerPermits));
                        }
                    }
                    catch (InterruptedException e)
                    {
                        // ignore, will either stop looping if done or retry the lock
                    }
                }
            });

            makeBusy.start();
        }

        public void shutdown() throws TimeoutException, InterruptedException
        {
            stayBusy.set(false);
            makeBusy.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertFalse("makeBusy thread should have checked stayBusy and exited",
                               makeBusy.isAlive());
            sharedPool.shutdownAndWait(1L, MINUTES);
        }

        public LocalAwareExecutorPlus getExecutor()
        {
            return executor;
        }

        public int getNotifiedMaxPoolSize()
        {
            return notifiedMaxPoolSize.get();
        }
    }

    @Test
    public void changingMaxWorkersMeetsConcurrencyGoalsTest() throws InterruptedException, TimeoutException
    {
        BusyExecutor busyExecutor = new BusyExecutor("ChangingMaxWorkersMeetsConcurrencyGoalsTest", "resizetest");
        LocalAwareExecutorPlus executor = busyExecutor.getExecutor();

        busyExecutor.start();
        try
        {
            for (int repeat = 0; repeat < 1000; repeat++)
            {
                assertMaxTaskConcurrency(executor, 1);
                Assert.assertEquals(1, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 2);
                Assert.assertEquals(2, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 1);
                Assert.assertEquals(1, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 3);
                Assert.assertEquals(3, busyExecutor.getNotifiedMaxPoolSize());

                executor.setMaximumPoolSize(0);
                Assert.assertEquals(0, busyExecutor.getNotifiedMaxPoolSize());

                assertMaxTaskConcurrency(executor, 4);
                Assert.assertEquals(4, busyExecutor.getNotifiedMaxPoolSize());
            }
        }
        finally
        {
            busyExecutor.shutdown();
        }
    }

    @Test
    public void stoppedWorkersProcessTasksWhenConcurrencyIncreases() throws InterruptedException
    {
        BusyExecutor busyExecutor = new BusyExecutor("StoppedWorkersProcessTasksWhenConcurrencyIncreases", "stoptest");
        LocalAwareExecutorPlus executor = busyExecutor.getExecutor();
        busyExecutor.start();
        try
        {
            for (int repeat = 0; repeat < 25; repeat++)
            {
                assertMaxTaskConcurrency(executor, 3);
                Assert.assertEquals(3, busyExecutor.getNotifiedMaxPoolSize());

                executor.setMaximumPoolSize(0);
                Assert.assertEquals(0, busyExecutor.getNotifiedMaxPoolSize());
                Thread.sleep(250);

                assertMaxTaskConcurrency(executor, 4);
                Assert.assertEquals(4, busyExecutor.getNotifiedMaxPoolSize());
            }
        }
        finally
        {
            executor.shutdown();
       }
    }

    static class LatchWaiter implements Runnable
    {
        CountDownLatch latch;
        long timeout;
        TimeUnit unit;

        public LatchWaiter(CountDownLatch latch, long timeout, TimeUnit unit)
        {
            this.latch = latch;
            this.timeout = timeout;
            this.unit = unit;
        }

        public void run()
        {
            latch.countDown();
            try
            {
                latch.await(timeout, unit); // block until all the latch waiters have run, now at desired concurrency
            }
            catch (InterruptedException e)
            {
                Assert.fail("interrupted: " + e);
            }
        }
    }

    static class BusyWork implements Runnable
    {
        private final Semaphore busyWorkers;

        public BusyWork(Semaphore busyWorkers)
        {
            this.busyWorkers = busyWorkers;
        }

        public void run()
        {
            busyWorkers.release();
        }
    }

    void assertMaxTaskConcurrency(LocalAwareExecutorPlus executor, int concurrency) throws InterruptedException
    {
        executor.setMaximumPoolSize(concurrency);

        CountDownLatch concurrencyGoal = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++)
        {
            executor.execute(new LatchWaiter(concurrencyGoal, 5L, TimeUnit.SECONDS));
        }
        // Will return true if all of the LatchWaiters count down before the timeout
        Assert.assertTrue("Test tasks did not hit max concurrency goal", concurrencyGoal.await(3L, TimeUnit.SECONDS));
    }

    @Test
    public void testLocalStatePropagation() throws InterruptedException, TimeoutException
    {
        SharedExecutorPool sharedPool = new SharedExecutorPool("TestPool");
        try
        {
            LocalAwareExecutorPlus executor = sharedPool.newExecutor(1, "TEST", "TEST");
            assertThat(executor).isInstanceOf(LocalAwareExecutorPlus.class);
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            sharedPool.shutdownAndWait(1, TimeUnit.SECONDS);
        }
    }

    private static final class Blocker extends CountDownLatch implements Runnable
    {
        final CountDownLatch started = new CountDownLatch(1);

        Blocker()
        {
            super(1);
        }

        public void run()
        {
            started.countDown();
            try { await(); } catch (InterruptedException e) { throw new AssertionError(e); }
        }
    }

    // a submitted request, as Dispatcher.RequestProcessor: its queue time is measured from its creation
    private static final class Request implements DebuggableTask.RunnableDebuggableTask
    {
        final long createdAtNanos;

        Request(long createdAtNanos)
        {
            this.createdAtNanos = createdAtNanos;
        }

        public void run() {}
        public long creationTimeNanos() { return createdAtNanos; }
        public long startTimeNanos() { return 0; }
        public String description() { return "request"; }
    }

    @Test
    public void testQueueTimeAndLongestRunning() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage");
        try
        {
            Assert.assertEquals(0L, es.oldestTaskQueueTime());
            Assert.assertEquals(0L, es.longestRunningTaskTime());
            Assert.assertNull(es.getLongestRunningTaskClass());

            Blocker a = new Blocker();
            Blocker b = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);                       // 1 thread: b waits at the head of the queue
            Thread.sleep(50);

            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);
            Util.spinAssertEquals(true, () -> es.longestRunningTaskTime() >= MILLISECONDS.toNanos(40), 5);
            Assert.assertEquals(Blocker.class.getName(), es.getLongestRunningTaskClass());
            Assert.assertEquals(0L, es.oldestDebuggableTaskQueueTime());   // not a submitted debuggable task

            a.countDown();
            Assert.assertTrue(b.started.await(10, TimeUnit.SECONDS));
            Util.spinAssertEquals(0L, es::oldestTaskQueueTime, 5);
            b.countDown();
            Util.spinAssertEquals(0L, es::longestRunningTaskTime, 5);
            Util.spinAssertEquals(null, es::getLongestRunningTaskClass, 5);
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testSubmittedTaskQueueTime() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool-submit");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage-submit");
        try
        {
            Blocker a = new Blocker();
            es.submit(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(Blocker.class.getName(), es.getLongestRunningTaskClass());
            Runnable queued = () -> {};
            es.submit(queued);                   // a FutureTask that is not debuggable
            Thread.sleep(50);
            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);
            Assert.assertSame(queued.getClass(), WrappedTask.classOf(es.tasks.peek()));
            Assert.assertEquals(0L, es.oldestDebuggableTaskQueueTime());
            a.countDown();
            Util.spinAssertEquals(0L, es::oldestTaskQueueTime, 5);
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testDebuggableTaskQueueTimeFromEnqueue() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool-debuggable");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage-debuggable");
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));

            // created long before it was queued: backpressure counts from creation on the approximate clock, the
            // queue time from when this executor queued it
            Request request = new Request(preciseTime.now() - TimeUnit.SECONDS.toNanos(10));
            es.submit(request);
            long before = approxTime.now();
            long debuggableTime = es.oldestDebuggableTaskQueueTime();
            long after = approxTime.now();
            Assert.assertTrue(debuggableTime + " < " + (before - request.createdAtNanos), debuggableTime >= before - request.createdAtNanos);
            Assert.assertTrue(debuggableTime + " > " + (after - request.createdAtNanos), debuggableTime <= after - request.createdAtNanos);
            long queueTime = es.oldestTaskQueueTime();
            Assert.assertTrue(queueTime + " >= 5s", queueTime >= 0 && queueTime < TimeUnit.SECONDS.toNanos(5));
            Thread.sleep(50);
            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);
            Assert.assertEquals(Request.class, WrappedTask.classOf(es.tasks.peek()));
            a.countDown();
            Util.spinAssertEquals(0L, es::oldestDebuggableTaskQueueTime, 5);
            Util.spinAssertEquals(0L, es::oldestTaskQueueTime, 5);
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testDebuggableTaskCreatedAheadOfApproxTimeIsNeverNegative() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool-ahead");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage-ahead");
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));

            // a creation stamp ahead of the approximate clock, as a precise-clock stamp can be
            Request request = new Request(approxTime.now() + TimeUnit.SECONDS.toNanos(1));
            es.submit(request);
            // backpressure still reads the creation-based value, unchanged by this executor's queue stamp
            long before = approxTime.now();
            long debuggableTime = es.oldestDebuggableTaskQueueTime();
            long after = approxTime.now();
            Assert.assertTrue(debuggableTime >= before - request.createdAtNanos && debuggableTime <= after - request.createdAtNanos);
            for (int i = 0; i < 100; i++)
                Assert.assertTrue(es.oldestTaskQueueTime() >= 0);
            a.countDown();
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testLongestRunningTaskClassForLambda() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool2");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage2");
        try
        {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            es.execute(() -> {
                started.countDown();
                try { release.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
            });
            Assert.assertTrue(started.await(10, TimeUnit.SECONDS));
            String c = es.getLongestRunningTaskClass();
            Assert.assertNotNull(c);
            Assert.assertTrue(c, c.contains(SEPExecutorTest.class.getName()));
            release.countDown();
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testAgeNeverNegative() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool3");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(2, "internal", "LivenessStage3");
        try
        {
            for (int i = 0; i < 20_000; i++)
            {
                es.execute(() -> {});
                Assert.assertTrue(es.oldestTaskQueueTime() >= 0);
                Assert.assertTrue(es.longestRunningTaskTime() >= 0);
            }
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testRunningTimeNeverExceedsElapsedUnderChurn() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool-churn");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(4, "internal", "LivenessStage-churn");
        SEPExecutor other = (SEPExecutor) pool.newExecutor(4, "internal", "LivenessStage-churn-other");
        try
        {
            long begin = approxTime.now();
            for (int round = 0; round < 200; round++)
            {
                // workers hop between the two executors, so a scan races both task and executor changes
                for (int i = 0; i < 1000; i++)
                {
                    es.execute(() -> {});
                    other.execute(() -> {});
                }
                for (int i = 0; i < 1000; i++)
                {
                    long age = es.longestRunningTaskTime();
                    long elapsed = preciseTime.now() - begin;
                    Assert.assertTrue(age + " > " + elapsed, elapsed + TimeUnit.SECONDS.toNanos(1) >= age);
                }
            }
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testLivenessAfterShutdown() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool4");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage4");
        es.execute(() -> {});
        pool.shutdownAndWait(1, TimeUnit.MINUTES);
        Assert.assertEquals(0L, es.oldestTaskQueueTime());
        Assert.assertEquals(0L, es.longestRunningTaskTime());
        Assert.assertNull(es.getLongestRunningTaskClass());
    }

    @Test
    public void testExitedWorkerNotCounted() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool5");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage5");
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            SEPWorker worker = pool.allWorkers.stream().filter(w -> w.runningFor.get() == es && w.currentTask.get() != null).findFirst().orElse(null);
            Assert.assertNotNull(worker);
            Assert.assertEquals(Blocker.class.getName(), es.getLongestRunningTaskClass());

            // a worker exits after finishing a task for an executor that was shut down individually
            es.shutdown();
            a.countDown();
            worker.thread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertEquals(Thread.State.TERMINATED, worker.thread.getState());
            Assert.assertFalse(pool.allWorkers.contains(worker));
            Assert.assertEquals(0L, es.longestRunningTaskTime());
            Assert.assertNull(es.getLongestRunningTaskClass());
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testIdleWorkerDoesNotReferenceExecutor() throws Exception
    {
        SharedExecutorPool pool = new SharedExecutorPool("LivenessPool6");
        SEPExecutor es = (SEPExecutor) pool.newExecutor(1, "internal", "LivenessStage6");
        try
        {
            es.submit(() -> {}).get(10, TimeUnit.SECONDS);
            Assert.assertFalse(pool.allWorkers.isEmpty());
            // once idle, no worker keeps the executor it last served reachable
            Util.spinAssertEquals(false, () -> pool.allWorkers.stream().anyMatch(w -> w.runningFor.get() == es), 5);
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void testLongestRunningTaskClassThroughStage() throws Exception
    {
        Blocker blocker = new Blocker();
        try
        {
            Stage.READ.execute(blocker);
            Assert.assertTrue(blocker.started.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(Blocker.class.getName(), Stage.READ.executor().getLongestRunningTaskClass());
        }
        finally
        {
            blocker.countDown();
        }
    }
}
