package org.apache.cassandra.concurrent;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.TraceStateImpl;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FailingRunnable;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class DebuggableThreadPoolExecutorTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSerialization()
    {
        ExecutorPlus executor = executorFactory().configureSequential("TEST").withQueueLimit(1).build();
        WrappedRunnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(50);
            }
        };
        long start = nanoTime();
        for (int i = 0; i < 10; i++)
        {
            executor.execute(runnable);
        }
        assert executor.getPendingTaskCount() > 0 : executor.getPendingTaskCount();
        while (executor.getCompletedTaskCount() < 10)
            continue;
        long delta = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);
        assert delta >= 9 * 50 : delta;
    }

    @Test
    public void testLocalStatePropagation()
    {
        ExecutorPlus executor = executorFactory().localAware().sequential("TEST");
        assertThat(executor).isInstanceOf(LocalAwareExecutorPlus.class);
        try
        {
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testNoLocalStatePropagation() throws InterruptedException
    {
        ExecutorPlus executor = executorFactory().sequential("TEST");
        assertThat(executor).isNotInstanceOf(LocalAwareExecutorPlus.class);
        try
        {
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            executor.shutdown();
        }
    }

    public static void checkLocalStateIsPropagated(ExecutorPlus executor)
    {
        checkClientWarningsArePropagated(executor, () -> executor.execute(() -> ClientWarn.instance.warn("msg")));
        checkClientWarningsArePropagated(executor, () -> executor.submit(() -> ClientWarn.instance.warn("msg")));
        checkClientWarningsArePropagated(executor, () -> executor.submit(() -> ClientWarn.instance.warn("msg"), null));
        checkClientWarningsArePropagated(executor, () -> executor.submit((Callable<Void>) () -> {
            ClientWarn.instance.warn("msg");
            return null;
        }));

        checkTracingIsPropagated(executor, () -> executor.execute(() -> Tracing.trace("msg")));
        checkTracingIsPropagated(executor, () -> executor.submit(() -> Tracing.trace("msg")));
        checkTracingIsPropagated(executor, () -> executor.submit(() -> Tracing.trace("msg"), null));
        checkTracingIsPropagated(executor, () -> executor.submit((Callable<Void>) () -> {
            Tracing.trace("msg");
            return null;
        }));
    }

    public static void checkClientWarningsArePropagated(ExecutorPlus executor, Runnable schedulingTask) {
        ClientWarn.instance.captureWarnings();
        assertThat(ClientWarn.instance.getWarnings()).isNullOrEmpty();

        ClientWarn.instance.warn("msg0");
        long initCompletedTasks = executor.getCompletedTaskCount();
        schedulingTask.run();
        while (executor.getCompletedTaskCount() == initCompletedTasks) Uninterruptibles.sleepUninterruptibly(10, MILLISECONDS);
        ClientWarn.instance.warn("msg1");

        if (executor instanceof LocalAwareExecutorPlus)
            assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder("msg0", "msg", "msg1");
        else
            assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder("msg0", "msg1");
    }

    public static void checkTracingIsPropagated(ExecutorPlus executor, Runnable schedulingTask) {
        ClientState clientState = ClientState.forInternalCalls();
        ClientWarn.instance.captureWarnings();
        assertThat(ClientWarn.instance.getWarnings()).isNullOrEmpty();

        ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();
        Tracing.instance.set(new TraceState(clientState, FBUtilities.getLocalAddressAndPort(), nextTimeUUID(), Tracing.TraceType.NONE)
        {
            @Override
            protected void traceImpl(String message)
            {
                q.add(message);
            }
        });
        Tracing.trace("msg0");
        long initCompletedTasks = executor.getCompletedTaskCount();
        schedulingTask.run();
        while (executor.getCompletedTaskCount() == initCompletedTasks) Uninterruptibles.sleepUninterruptibly(10, MILLISECONDS);
        Tracing.trace("msg1");

        if (executor instanceof LocalAwareExecutorPlus)
            assertThat(q.toArray()).containsExactlyInAnyOrder("msg0", "msg", "msg1");
        else
            assertThat(q.toArray()).containsExactlyInAnyOrder("msg0", "msg1");
    }

    @Test
    public void testExecuteFutureTaskWhileTracing()
    {
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus executor = executorFactory()
                                .localAware()
                                .configureSequential("TEST")
                                .withUncaughtExceptionHandler(ueh)
                                .withQueueLimit(1).build();
        Runnable test = () -> executor.execute(failingTask());
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(ueh, test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(ueh, () -> withTracing(test));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testSubmitFutureTaskWhileTracing()
    {
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus executor = executorFactory().localAware()
                                                 .configureSequential("TEST")
                                                 .withUncaughtExceptionHandler(ueh)
                                                 .withQueueLimit(1).build();
        FailingRunnable test = () -> executor.submit(failingTask()).get();
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(ueh, test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(ueh, () -> withTracing(test));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testSubmitWithResultFutureTaskWhileTracing()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus executor = executorFactory().localAware()
                                                 .configureSequential("TEST")
                                                 .withUncaughtExceptionHandler(ueh)
                                                 .withQueueLimit(1).build();
        FailingRunnable test = () -> executor.submit(failingTask(), 42).get();
        try
        {
            Throwable cause = catchUncaughtExceptions(ueh, test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
            cause = catchUncaughtExceptions(ueh, () -> withTracing(test));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static void withTracing(Runnable fn)
    {
        TraceState state = Tracing.instance.get();
        try {
            Tracing.instance.set(new TraceStateImpl(ClientState.forInternalCalls(), InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.0.1")), nextTimeUUID(), Tracing.TraceType.NONE));
            fn.run();
        }
        finally
        {
            Tracing.instance.set(state);
        }
    }

    private static class SettableUncaughtExceptionHandler implements UncaughtExceptionHandler
    {
        volatile Supplier<UncaughtExceptionHandler> cur;

        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            cur.get().uncaughtException(t, e);
        }

        void set(Supplier<UncaughtExceptionHandler> set)
        {
            cur = set;
        }

        void clear()
        {
            cur = Thread::getDefaultUncaughtExceptionHandler;
        }
    }

    private static Throwable catchUncaughtExceptions(SettableUncaughtExceptionHandler ueh, Runnable fn)
    {
        try
        {
            AtomicReference<Throwable> ref = new AtomicReference<>(null);
            CountDownLatch latch = new CountDownLatch(1);
            ueh.set(() -> (thread, cause) -> {
                ref.set(cause);
                latch.countDown();
            });
            fn.run();
            try
            {
                latch.await(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            return ref.get();
        }
        finally
        {
            ueh.clear();
        }
    }

    private static String failingFunction()
    {
        throw new DebuggingThrowsException();
    }

    private static RunnableFuture<String> failingTask()
    {
        return new FutureTask<>(DebuggableThreadPoolExecutorTest::failingFunction);
    }

    private static final class DebuggingThrowsException extends RuntimeException {

    }

    static final class Blocker implements Runnable
    {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public void run()
        {
            started.countDown();
            // an interrupt only comes from shutdownNow() once the test is over
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    @Test
    public void testQueueTimeAndLongestRunning() throws Exception
    {
        ExecutorPlus es = executorFactory().sequential("liveness-tpe");
        try
        {
            Assert.assertEquals(0L, es.oldestTaskQueueTime());
            Assert.assertEquals(0L, es.longestRunningTaskTime());
            Assert.assertNull(es.getLongestRunningTaskClass());

            Blocker a = new Blocker();
            Blocker b = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);
            Thread.sleep(50);

            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);
            Util.spinAssertEquals(true, () -> es.longestRunningTaskTime() >= MILLISECONDS.toNanos(40), 5);
            Assert.assertEquals(Blocker.class.getName(), es.getLongestRunningTaskClass());

            a.release.countDown();
            Assert.assertTrue(b.started.await(10, TimeUnit.SECONDS));
            Util.spinAssertEquals(0L, es::oldestTaskQueueTime, 5);
            b.release.countDown();
            Util.spinAssertEquals(0L, es::longestRunningTaskTime, 5);
            Util.spinAssertEquals(null, es::getLongestRunningTaskClass, 5);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testDebuggableTaskQueueTimeIsZero() throws Exception
    {
        // only SEP executors report it, so a custom native-transport executor never applies backpressure
        ExecutorPlus es = executorFactory().sequential("liveness-debuggable");
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(() -> {});
            Thread.sleep(50);
            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);
            Assert.assertEquals(0L, es.oldestDebuggableTaskQueueTime());
            a.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testSubmitReportsUserClass() throws Exception
    {
        ThreadPoolExecutorPlus es = (ThreadPoolExecutorPlus) executorFactory().sequential("liveness-submit");
        try
        {
            Blocker a = new Blocker();
            es.submit(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(Blocker.class.getName(), es.getLongestRunningTaskClass());

            Callable<Integer> callable = () -> 42;
            es.submit(callable);
            Runnable runnable = () -> {};
            es.submit(runnable, 42);
            Thread.sleep(50);
            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);

            Runnable[] queued = es.getQueue().toArray(new Runnable[0]);
            Assert.assertEquals(2, queued.length);
            Assert.assertSame(callable.getClass(), WrappedTask.classOf(queued[0]));
            Assert.assertSame(runnable.getClass(), WrappedTask.classOf(queued[1]));
            a.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testAtLeastOnceTriggerReportsUserClass() throws Exception
    {
        SequentialExecutorPlus es = executorFactory().sequential("liveness-at-least-once");
        try
        {
            Blocker a = new Blocker();
            Assert.assertTrue(es.atLeastOnceTrigger(a).trigger());
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(Blocker.class.getName(), es.getLongestRunningTaskClass());
            a.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testLocalAwareExecuteKeepsLocalsAndReportsUserClass() throws Exception
    {
        ThreadPoolExecutorPlus es = (ThreadPoolExecutorPlus) executorFactory().localAware().sequential("liveness-locals");
        ClientWarn.instance.captureWarnings();
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));

            CountDownLatch done = new CountDownLatch(1);
            Runnable task = () -> { ClientWarn.instance.warn("msg"); done.countDown(); };
            es.execute(task);
            Runnable head = es.getQueue().peek();
            Assert.assertTrue(String.valueOf(head), head instanceof TimedTask);
            Assert.assertSame(task.getClass(), WrappedTask.classOf(head));

            a.release.countDown();
            Assert.assertTrue(done.await(10, TimeUnit.SECONDS));
            assertThat(ClientWarn.instance.getWarnings()).containsExactly("msg");
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            es.shutdownNow();
        }
    }

    @Test
    public void testBlockedSubmissionKeepsOriginalStamp() throws Exception
    {
        ThreadPoolExecutorPlus es = (ThreadPoolExecutorPlus) executorFactory().configureSequential("liveness-blocked")
                                                                             .withQueueLimit(1)
                                                                             .build();
        try
        {
            Blocker a = new Blocker();
            Blocker b = new Blocker();
            Blocker c = new Blocker();
            es.execute(a);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);                                    // fills the 1-slot queue
            Thread submitter = new Thread(() -> es.execute(c)); // blocks in the rejection handler
            submitter.start();
            Util.spinAssertEquals(Thread.State.TIMED_WAITING, submitter::getState, 5); // stamped, now blocked in offer()
            Thread.sleep(100);
            long releasedAt = approxTime.now();
            a.release.countDown();                            // b starts, c lands in the queue
            Assert.assertTrue(b.started.await(10, TimeUnit.SECONDS));
            submitter.join(10_000);
            Runnable head = es.getQueue().peek();
            Assert.assertNotNull(head);
            Assert.assertTrue(releasedAt - ((TimedTask) head).enqueuedAtNanos() >= MILLISECONDS.toNanos(80));
            b.release.countDown();
            Assert.assertTrue(c.started.await(10, TimeUnit.SECONDS));
            c.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testExecuteStillReportsExceptions()
    {
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ExecutorPlus es = executorFactory().configureSequential("liveness-exc")
                                           .withUncaughtExceptionHandler(ueh)
                                           .build();
        try
        {
            Throwable cause = catchUncaughtExceptions(ueh, () -> es.execute(() -> { throw new DebuggingThrowsException(); }));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
            Util.spinAssertEquals(0L, es::longestRunningTaskTime, 5);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testDeadWorkerSlotIsPruned()
    {
        ThreadPoolExecutorPlus es = (ThreadPoolExecutorPlus) executorFactory().configurePooled("liveness-prune", 1)
                                                                             .withKeepAlive(50, MILLISECONDS)
                                                                             .build();
        try
        {
            es.execute(() -> {});
            Util.spinAssertEquals(1, es::workerSlotCount, 5);
            Util.spinAssertEquals(0, es::getPoolSize, 5);          // the core thread timed out
            Assert.assertEquals(0L, es.longestRunningTaskTime());
            // the pool drops a worker just before its thread exits, so pruning (done by every read) is eventual
            Util.spinAssertEquals(0, () -> { es.longestRunningTaskTime(); return es.workerSlotCount(); }, 5);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testDeadSlotsPrunedOnRegistration() throws Exception
    {
        ThreadPoolExecutorPlus es = (ThreadPoolExecutorPlus) executorFactory().configurePooled("liveness-register-prune", 1)
                                                                             .withKeepAlive(10, MILLISECONDS)
                                                                             .build();
        try
        {
            // the gauges are never read here, as for an executor that has no metrics
            for (int i = 0; i < 5; i++)
            {
                AtomicReference<Thread> worker = new AtomicReference<>();
                es.execute(() -> worker.set(Thread.currentThread()));
                Util.spinAssertEquals(true, () -> worker.get() != null, 5);
                worker.get().join(10_000);                    // the core thread timed out and exited
                Assert.assertFalse(worker.get().isAlive());
            }
            Assert.assertEquals(1, es.workerSlotCount());     // each new worker pruned its predecessor's slot
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testDeadThreadWithStampNotReported() throws Exception
    {
        AtomicReference<Thread> worker = new AtomicReference<>();
        SettableUncaughtExceptionHandler ueh = new SettableUncaughtExceptionHandler();
        ThreadPoolExecutorPlus es = ThreadPoolExecutorBuilder.<ThreadPoolExecutorPlus>pooled(builder -> new ThreadPoolExecutorPlus(builder)
        {
            @Override
            protected void beforeExecute(Thread t, Runnable r)
            {
                super.beforeExecute(t, r);
                worker.set(t);
                throw new DebuggingThrowsException();          // the worker dies stamped, afterExecute never runs
            }
        }, null, null, ueh, "liveness-dead-stamped", 1).build();
        try
        {
            Throwable cause = catchUncaughtExceptions(ueh, () -> es.execute(() -> {}));
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
            worker.get().join(10_000);
            Assert.assertFalse(worker.get().isAlive());
            Assert.assertEquals(0L, es.longestRunningTaskTime());
            Assert.assertNull(es.getLongestRunningTaskClass());
            Assert.assertEquals(0, es.workerSlotCount());
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testAgeNeverNegative()
    {
        ExecutorPlus es = executorFactory().pooled("liveness-neg", 2);
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
            es.shutdownNow();
        }
    }

    @Test
    public void testRunningTimeNeverExceedsElapsedUnderChurn()
    {
        ExecutorPlus es = executorFactory().pooled("liveness-churn", 4);
        try
        {
            long begin = approxTime.now();
            for (int round = 0; round < 200; round++)
            {
                for (int i = 0; i < 1000; i++)
                    es.execute(() -> {});
                for (int i = 0; i < 1000; i++)
                {
                    // no task can have been running longer than this test; a stamp re-read after the worker cleared
                    // it would report the whole clock value instead
                    long age = es.longestRunningTaskTime();
                    long elapsed = preciseTime.now() - begin;
                    Assert.assertTrue(age + " > " + elapsed, elapsed + TimeUnit.SECONDS.toNanos(1) >= age);
                }
            }
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testLivenessAfterShutdown() throws Exception
    {
        ExecutorPlus es = executorFactory().sequential("liveness-shutdown");
        es.execute(() -> {});
        es.shutdown();
        Assert.assertTrue(es.awaitTermination(10, TimeUnit.SECONDS));
        Assert.assertEquals(0L, es.oldestTaskQueueTime());
        Assert.assertEquals(0L, es.longestRunningTaskTime());
        Assert.assertNull(es.getLongestRunningTaskClass());
    }

    @Test
    public void testScheduledExecutorReportsOverdueTimeAndRunning() throws Exception
    {
        ScheduledExecutorPlus es = executorFactory().scheduled("liveness-sched");
        try
        {
            Blocker a = new Blocker();
            es.schedule(a, 0, MILLISECONDS);
            Assert.assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.schedule(() -> {}, 0, MILLISECONDS);           // due now, queued behind a
            Thread.sleep(50);
            Util.spinAssertEquals(true, () -> es.oldestTaskQueueTime() >= MILLISECONDS.toNanos(40), 5);
            Util.spinAssertEquals(true, () -> es.longestRunningTaskTime() >= MILLISECONDS.toNanos(40), 5);
            Assert.assertNotNull(es.getLongestRunningTaskClass()); // the JDK's ScheduledFutureTask; the user class is not visible
            a.release.countDown();
            Util.spinAssertEquals(0L, es::longestRunningTaskTime, 5);
            Util.spinAssertEquals(0L, es::oldestTaskQueueTime, 5);

            es.schedule(() -> {}, 1, TimeUnit.HOURS);         // not due: not waiting
            Assert.assertEquals(0L, es.oldestTaskQueueTime());
        }
        finally
        {
            es.shutdownNow();
        }
    }
}
