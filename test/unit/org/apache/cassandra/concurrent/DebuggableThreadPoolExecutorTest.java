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


import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Before;
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
import org.apache.cassandra.utils.WrappedRunnable;
import org.assertj.core.api.Assertions;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.junit.Assert.*;

public class DebuggableThreadPoolExecutorTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // several tests leave tracing or client warning state on the test thread; start each test without it so the
    // submission paths are the ones the test names
    @Before
    public void clearExecutorLocals()
    {
        ExecutorLocals.set(null);
    }

    @Test
    public void testSerialization()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        WrappedRunnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(50);
            }
        };
        long start = System.nanoTime();
        for (int i = 0; i < 10; i++)
        {
            executor.execute(runnable);
        }
        assert q.size() > 0 : q.size();
        while (executor.getCompletedTaskCount() < 10)
            continue;
        long delta = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assert delta >= 9 * 50 : delta;
    }

    @Test
    public void testLocalStatePropagation()
    {
        DebuggableThreadPoolExecutor executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("TEST", 1);
        try
        {
            checkLocalStateIsPropagated(executor);
        }
        finally
        {
            executor.shutdown();
        }
    }

    public static void checkLocalStateIsPropagated(LocalAwareExecutorService executor)
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

    public static void checkClientWarningsArePropagated(LocalAwareExecutorService executor, Runnable schedulingTask) {
        ClientWarn.instance.captureWarnings();
        Assertions.assertThat(ClientWarn.instance.getWarnings()).isNullOrEmpty();

        ClientWarn.instance.warn("msg0");
        long initCompletedTasks = executor.getCompletedTaskCount();
        schedulingTask.run();
        while (executor.getCompletedTaskCount() == initCompletedTasks) Uninterruptibles.sleepUninterruptibly(10, MILLISECONDS);
        ClientWarn.instance.warn("msg1");

        Assertions.assertThat(ClientWarn.instance.getWarnings()).containsExactlyInAnyOrder("msg0", "msg", "msg1");
    }

    public static void checkTracingIsPropagated(LocalAwareExecutorService executor, Runnable schedulingTask) {
        ClientState clientState = ClientState.forInternalCalls();
        ClientWarn.instance.captureWarnings();
        Assertions.assertThat(ClientWarn.instance.getWarnings()).isNullOrEmpty();

        ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();
        Tracing.instance.set(new TraceState(clientState, FBUtilities.getLocalAddressAndPort(), UUID.randomUUID(), Tracing.TraceType.NONE)
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

        Assertions.assertThat(q.toArray()).containsExactlyInAnyOrder("msg0", "msg", "msg1");
    }

    @Test
    public void testExecuteFutureTaskWhileTracing()
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        Runnable test = () -> executor.execute(failingTask());
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(() -> withTracing(test));
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
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>(1);
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        FailingRunnable test = () -> executor.submit(failingTask()).get();
        try
        {
            // make sure the non-tracing case works
            Throwable cause = catchUncaughtExceptions(test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());

            // tracing should have the same semantics
            cause = catchUncaughtExceptions(() -> withTracing(test));
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
        DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(1,
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 q,
                                                                                 new NamedThreadFactory("TEST"));
        FailingRunnable test = () -> executor.submit(failingTask(), 42).get();
        try
        {
            Throwable cause = catchUncaughtExceptions(test);
            Assert.assertEquals(DebuggingThrowsException.class, cause.getClass());
            cause = catchUncaughtExceptions(() -> withTracing(test));
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
            Tracing.instance.set(new TraceStateImpl(ClientState.forInternalCalls(), InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.0.1")), UUID.randomUUID(), Tracing.TraceType.NONE));
            fn.run();
        }
        finally
        {
            Tracing.instance.set(state);
        }
    }

    private static Throwable catchUncaughtExceptions(Runnable fn)
    {
        Thread.UncaughtExceptionHandler defaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        try
        {
            AtomicReference<Throwable> ref = new AtomicReference<>(null);
            CountDownLatch latch = new CountDownLatch(1);
            Thread.setDefaultUncaughtExceptionHandler((thread, cause) -> {
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
            Thread.setDefaultUncaughtExceptionHandler(defaultHandler);
        }
    }

    private static String failingFunction()
    {
        throw new DebuggingThrowsException();
    }

    private static RunnableFuture<String> failingTask()
    {
        return ListenableFutureTask.create(DebuggableThreadPoolExecutorTest::failingFunction);
    }

    private static final class DebuggingThrowsException extends RuntimeException {

    }

    // REVIEWER : I know this is the same as WrappedRunnable, but that doesn't support lambda...
    private interface FailingRunnable extends Runnable
    {
        void doRun() throws Throwable;

        default void run()
        {
            try
            {
                doRun();
            }
            catch (Throwable t)
            {
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }
    }

    private static final class Blocker implements Runnable
    {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public void run()
        {
            started.countDown();
            // an interrupt only comes from shutdownNow() once the test is over; throwing here would reach whatever
            // uncaught exception handler a later test (or its spinAssertEquals) has installed
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private static DebuggableThreadPoolExecutor oneThread(String name, int queueCapacity)
    {
        return new DebuggableThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<>(queueCapacity),
                                                new NamedThreadFactory(name));
    }

    @Test
    public void testQueueAgeAndLongestRunningDtpe() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-dtpe", 10);
        try
        {
            assertEquals(0L, es.oldestQueuedTaskAgeNanos());
            assertEquals(0L, es.longestRunningTaskAgeNanos());
            assertNull(es.longestRunningTaskClass());

            Blocker a = new Blocker();
            Blocker b = new Blocker();
            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);
            Thread.sleep(50);

            Util.spinAssertEquals(true, () -> es.oldestQueuedTaskAgeNanos() >= MILLISECONDS.toNanos(40), 5);
            Util.spinAssertEquals(true, () -> es.longestRunningTaskAgeNanos() >= MILLISECONDS.toNanos(40), 5);
            assertSame(Blocker.class, es.longestRunningTaskClass());

            a.release.countDown();
            assertTrue(b.started.await(10, TimeUnit.SECONDS));
            Util.spinAssertEquals(0L, es::oldestQueuedTaskAgeNanos, 5);
            b.release.countDown();
            Util.spinAssertEquals(0L, es::longestRunningTaskAgeNanos, 5);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testSubmittedCallableIsAged() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-submit", 10);
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.submit(() -> 42);           // JDK FutureTask from newTaskFor, wrapped by execute()
            Thread.sleep(50);
            Util.spinAssertEquals(true, () -> es.oldestQueuedTaskAgeNanos() >= MILLISECONDS.toNanos(40), 5);
            a.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testLocalSessionWrapperNotDoubleWrapped() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-locals", 10);
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            ExecutorLocals locals = ExecutorLocals.create(null, new ClientWarn.State(), null, null);
            Blocker b = new Blocker();
            es.execute(b, locals);
            Thread.sleep(50);
            Runnable head = es.getQueue().peek();
            assertTrue(head.getClass().getName(), head instanceof TimedTask);
            assertFalse(head instanceof TimedRunnable);           // LocalSessionWrapper carries its own stamp
            assertSame(Blocker.class, ((TimedTask) head).taskClass());
            Util.spinAssertEquals(true, () -> es.oldestQueuedTaskAgeNanos() >= MILLISECONDS.toNanos(40), 5);
            a.release.countDown();
            assertTrue(b.started.await(10, TimeUnit.SECONDS));
            b.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testTimedTaskWithLocalsKeepsLocalsAndClass() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-timed-locals", 10);
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            ClientWarn.State state = new ClientWarn.State();
            AtomicReference<ClientWarn.State> seen = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            Runnable task = () -> { seen.set(ClientWarn.instance.get()); done.countDown(); };
            // an already-timed task (as Stage submits) still needs the locals carried to the worker
            es.execute(new TimedRunnable(task), ExecutorLocals.create(null, state, null, null));
            Runnable head = es.getQueue().peek();
            assertNotNull(head);
            assertSame(task.getClass(), ((TimedTask) head).taskClass());
            a.release.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            assertSame(state, seen.get());
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testLocalStatePropagationThroughSingleThreadedStage() throws Exception
    {
        // single-threaded stages are backed by DebuggableThreadPoolExecutor and submit Stage's own timed wrapper
        ClientWarn.instance.captureWarnings();
        try
        {
            CountDownLatch executed = new CountDownLatch(1);
            Stage.MISC.execute(() -> { ClientWarn.instance.warn("msg"); executed.countDown(); });
            assertTrue(executed.await(10, TimeUnit.SECONDS));

            CountDownLatch executedWithLocals = new CountDownLatch(1);
            Stage.MISC.execute(() -> { ClientWarn.instance.warn("msg-locals"); executedWithLocals.countDown(); }, ExecutorLocals.create());
            assertTrue(executedWithLocals.await(10, TimeUnit.SECONDS));

            Assertions.assertThat(ClientWarn.instance.getWarnings()).containsExactly("msg", "msg-locals");
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    @Test
    public void testBlockedSubmissionKeepsOriginalStamp() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-blocked", 1);
        try
        {
            Blocker a = new Blocker();
            Blocker b = new Blocker();
            Blocker c = new Blocker();
            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.execute(b);                                    // fills the 1-slot queue
            Thread submitter = new Thread(() -> es.execute(c)); // blocks in the rejection handler
            submitter.start();
            Util.spinAssertEquals(Thread.State.TIMED_WAITING, submitter::getState, 5); // stamped, now blocked in offer()
            Thread.sleep(100);
            long releasedAt = approxTime.now();
            a.release.countDown();                            // b starts, c lands in the queue
            assertTrue(b.started.await(10, TimeUnit.SECONDS));
            submitter.join(10_000);
            Runnable head = es.getQueue().peek();
            assertNotNull(head);
            assertTrue(releasedAt - ((TimedTask) head).enqueuedAtNanos() >= MILLISECONDS.toNanos(80));
            b.release.countDown();
            assertTrue(c.started.await(10, TimeUnit.SECONDS));
            c.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testTimedRunnableUnwrapsFutureExceptions() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-exc", 10);
        try
        {
            ListenableFutureTask<Object> ft = ListenableFutureTask.create(() -> { throw new IllegalStateException("boom"); });
            // not spinAssertEquals: awaitility installs its own uncaught exception handler while it waits
            Throwable seen = catchUncaughtExceptions(() -> es.execute(ft)); // a JDK FutureTask, wrapped in TimedRunnable
            assertTrue(String.valueOf(seen), seen instanceof IllegalStateException);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testInlineExecutionNotCounted() throws Exception
    {
        DebuggableThreadPoolExecutor es = new DebuggableThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new NamedThreadFactory("liveness-inline"))
        {
            @Override
            public boolean canRunImmediately() { return true; }
        };
        try
        {
            AtomicReference<Long> duringRun = new AtomicReference<>();
            es.maybeExecuteImmediately(() -> duringRun.set(es.longestRunningTaskAgeNanos()));
            assertEquals(Long.valueOf(0L), duringRun.get());   // ran inline: not counted
            assertEquals(0L, es.longestRunningTaskAgeNanos());
            assertNull(es.longestRunningTaskClass());
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testDeadWorkerSlotIsPruned() throws Exception
    {
        DebuggableThreadPoolExecutor es = new DebuggableThreadPoolExecutor(1, Integer.MAX_VALUE, 50, MILLISECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new NamedThreadFactory("liveness-prune"));
        try
        {
            es.execute(() -> {});
            Util.spinAssertEquals(1, es::workerSlotCount, 5);
            Util.spinAssertEquals(0, es::getPoolSize, 5);          // core thread timed out (allowCoreThreadTimeOut)
            assertEquals(0L, es.longestRunningTaskAgeNanos());
            // the pool drops a worker just before its thread exits, so pruning (done by every read) is eventual
            Util.spinAssertEquals(0, () -> { es.longestRunningTaskAgeNanos(); return es.workerSlotCount(); }, 5);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testAgeNeverNegativeDtpe() throws Exception
    {
        DebuggableThreadPoolExecutor es = DebuggableThreadPoolExecutor.createWithFixedPoolSize("liveness-neg", 2);
        try
        {
            for (int i = 0; i < 20_000; i++)
            {
                es.execute(() -> {});
                assertTrue(es.oldestQueuedTaskAgeNanos() >= 0);
                assertTrue(es.longestRunningTaskAgeNanos() >= 0);
            }
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testLivenessAfterShutdownDtpe() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-shutdown", 10);
        es.execute(() -> {});
        es.shutdown();
        assertTrue(es.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(0L, es.oldestQueuedTaskAgeNanos());
        assertEquals(0L, es.longestRunningTaskAgeNanos());
        assertNull(es.longestRunningTaskClass());
    }

    @Test
    public void testScheduledExecutorReportsRunningOnly() throws Exception
    {
        DebuggableScheduledThreadPoolExecutor es = new DebuggableScheduledThreadPoolExecutor("liveness-sched");
        try
        {
            Blocker a = new Blocker();
            es.schedule(a, 0, MILLISECONDS);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            es.schedule(() -> {}, 0, MILLISECONDS);            // queued behind a
            Thread.sleep(50);
            assertEquals(0L, es.oldestQueuedTaskAgeNanos());   // scheduled queue: untracked by design
            Util.spinAssertEquals(true, () -> es.longestRunningTaskAgeNanos() >= MILLISECONDS.toNanos(40), 5);
            assertNotNull(es.longestRunningTaskClass());   // JDK ScheduledFutureTask; exact attribution is not attempted here
            a.release.countDown();
            Util.spinAssertEquals(0L, es::longestRunningTaskAgeNanos, 5);
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testRunningAgeNeverExceedsElapsedUnderChurn() throws Exception
    {
        DebuggableThreadPoolExecutor es = DebuggableThreadPoolExecutor.createWithFixedPoolSize("liveness-churn", 4);
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
                    long age = es.longestRunningTaskAgeNanos();
                    long elapsed = approxTime.now() - begin;
                    assertTrue(age + " > " + elapsed, elapsed + TimeUnit.SECONDS.toNanos(1) >= age);
                }
            }
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testDeadSlotsPrunedOnRegistration() throws Exception
    {
        DebuggableThreadPoolExecutor es = new DebuggableThreadPoolExecutor(1, Integer.MAX_VALUE, 10, MILLISECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new NamedThreadFactory("liveness-register-prune"));
        try
        {
            // the gauges are never read here, as for an executor that has no metrics
            for (int i = 0; i < 5; i++)
            {
                AtomicReference<Thread> worker = new AtomicReference<>();
                es.execute(() -> worker.set(Thread.currentThread()));
                Util.spinAssertEquals(true, () -> worker.get() != null, 5);
                worker.get().join(10_000);                    // core thread timed out and exited
                assertFalse(worker.get().isAlive());
            }
            assertEquals(1, es.workerSlotCount());            // each new worker pruned its predecessor's slot
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
        DebuggableThreadPoolExecutor es = new DebuggableThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new NamedThreadFactory("liveness-dead-stamped"))
        {
            @Override
            protected void beforeExecute(Thread t, Runnable r)
            {
                super.beforeExecute(t, r);
                worker.set(t);
                throw new IllegalStateException("worker dies after being stamped, afterExecute never runs");
            }
        };
        try
        {
            Throwable seen = catchUncaughtExceptions(() -> es.execute(() -> {}));
            assertTrue(String.valueOf(seen), seen instanceof IllegalStateException);
            worker.get().join(10_000);
            assertFalse(worker.get().isAlive());
            assertEquals(0L, es.longestRunningTaskAgeNanos());
            assertNull(es.longestRunningTaskClass());
            assertEquals(0, es.workerSlotCount());
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testSubmitReportsUserClass() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-submit-class", 10);
        try
        {
            Blocker a = new Blocker();
            es.submit(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            assertSame(Blocker.class, es.longestRunningTaskClass());

            Callable<Integer> callable = () -> 42;
            es.submit(callable);
            ListenableFutureTask<Object> future = ListenableFutureTask.create(() -> null);
            es.submit(future);
            Runnable[] queued = es.getQueue().toArray(new Runnable[0]);
            assertEquals(2, queued.length);
            for (Runnable r : queued)
                assertFalse(r.getClass().getName(), r instanceof TimedRunnable);     // the future carries its own stamp
            assertSame(callable.getClass(), ((TimedTask) queued[0]).taskClass());
            assertSame(ListenableFutureTask.class, ((TimedTask) queued[1]).taskClass());
            a.release.countDown();
        }
        finally
        {
            es.shutdownNow();
        }
    }

    @Test
    public void testTimedFutureWithLocalsKeepsExceptionAndClass() throws Exception
    {
        DebuggableThreadPoolExecutor es = oneThread("liveness-timed-future", 10);
        try
        {
            Blocker a = new Blocker();
            es.execute(a);
            assertTrue(a.started.await(10, TimeUnit.SECONDS));
            ListenableFutureTask<Object> ft = ListenableFutureTask.create(() -> { throw new IllegalStateException("boom"); });
            ExecutorLocals locals = ExecutorLocals.create(null, new ClientWarn.State(), null, null);
            // as CompactionExecutor.submitIfRunning submits, from a thread that has locals
            es.execute(new TimedRunnable(ft, Blocker.class), locals);
            assertSame(Blocker.class, ((TimedTask) es.getQueue().peek()).taskClass());
            Throwable seen = catchUncaughtExceptions(a.release::countDown);
            assertTrue(String.valueOf(seen), seen instanceof IllegalStateException);
        }
        finally
        {
            es.shutdownNow();
        }
    }
}
