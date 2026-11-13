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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * The slot index is a static thread local (a worker thread belongs to exactly one executor), so a thread that has
 * stamped one WorkerSlots keeps stamping it: every test drives fresh worker threads, never the JUnit thread, and never
 * shares a thread between WorkerSlots instances.
 */
public class WorkerSlotsTest
{
    private static class TaskA {}
    private static class TaskB {}
    private static class TaskC {}

    private final List<Worker> workers = new ArrayList<>();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void stopWorkers() throws InterruptedException
    {
        for (Worker w : workers)
            w.exit();
    }

    /** A thread that runs commands handed to it from the test thread, one at a time. */
    private static class Worker
    {
        private static final Runnable STOP = () -> {};

        final BlockingQueue<Runnable> commands = new LinkedBlockingQueue<>();
        final Thread thread = new Thread(this::loop, "WorkerSlotsTest-worker");

        private void loop()
        {
            try
            {
                for (Runnable command = commands.take(); command != STOP; command = commands.take())
                    command.run();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }

        <T> T call(Callable<T> command) throws Exception
        {
            FutureTask<T> task = new FutureTask<>(command);
            commands.add(task);
            return task.get(10, TimeUnit.SECONDS);
        }

        void run(Runnable command) throws Exception
        {
            call(() -> { command.run(); return null; });
        }

        void exit() throws InterruptedException
        {
            commands.add(STOP);
            thread.join(10_000);
            Assert.assertFalse(thread.isAlive());
        }
    }

    private Worker newWorker()
    {
        Worker w = new Worker();
        w.thread.setDaemon(true);
        w.thread.start();
        workers.add(w);
        return w;
    }

    /** Marks a task running on the worker and waits until approxTime has moved past its stamp, so later stamps are strictly greater. */
    private static void startTask(WorkerSlots slots, Worker w, Class<?> taskClass) throws Exception
    {
        long notBeforeStamp = w.call(() -> { slots.markRunning(taskClass); return approxTime.now(); });
        Util.spinAssertEquals(true, () -> approxTime.now() > notBeforeStamp, 5);
    }

    @Test
    public void testNoneRunning() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Assert.assertNull(slots.oldestRunning());
        Assert.assertEquals(0, slots.size());

        Worker w1 = newWorker();
        Worker w2 = newWorker();
        w1.run(() -> { slots.markRunning(TaskA.class); WorkerSlots.markIdle(); });
        w2.run(() -> { slots.markRunning(TaskB.class); WorkerSlots.markIdle(); });
        Assert.assertNull(slots.oldestRunning());
        Assert.assertEquals(2, slots.size());
    }

    @Test
    public void testRunningThenIdle() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Worker w = newWorker();

        w.run(() -> slots.markRunning(TaskA.class));
        WorkerSlots.Running running = slots.oldestRunning();
        Assert.assertNotNull(running);
        Assert.assertTrue(running.capturedStartNanos > 0);
        Assert.assertEquals(TaskA.class.getName(), running.taskClassName);

        w.run(WorkerSlots::markIdle);
        Assert.assertNull(slots.oldestRunning());
        Assert.assertEquals(1, slots.size());
    }

    @Test
    public void testOldestOfSeveral() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Worker w1 = newWorker();
        Worker w2 = newWorker();
        Worker w3 = newWorker();

        // w1 registers first but is idle, so the oldest task is not simply the first slot
        w1.run(() -> { slots.markRunning(TaskC.class); WorkerSlots.markIdle(); });
        startTask(slots, w2, TaskA.class);
        startTask(slots, w3, TaskB.class);
        startTask(slots, w1, TaskC.class);
        Assert.assertEquals(3, slots.size());

        WorkerSlots.Running oldest = slots.oldestRunning();
        Assert.assertNotNull(oldest);
        Assert.assertEquals(TaskA.class.getName(), oldest.taskClassName);
        long oldestStamp = oldest.capturedStartNanos;

        w2.run(WorkerSlots::markIdle);
        oldest = slots.oldestRunning();
        Assert.assertNotNull(oldest);
        Assert.assertEquals(TaskB.class.getName(), oldest.taskClassName);
        Assert.assertTrue(oldest.capturedStartNanos > oldestStamp);

        w3.run(WorkerSlots::markIdle);
        oldest = slots.oldestRunning();
        Assert.assertNotNull(oldest);
        Assert.assertEquals(TaskC.class.getName(), oldest.taskClassName);

        w1.run(WorkerSlots::markIdle);
        Assert.assertNull(slots.oldestRunning());
    }

    @Test
    public void testThreadNameOfOldest() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Worker w1 = newWorker();
        Worker w2 = newWorker();
        w1.thread.setName("WorkerSlotsTest-worker-1");
        w2.thread.setName("WorkerSlotsTest-worker-2");

        startTask(slots, w2, TaskA.class);
        startTask(slots, w1, TaskB.class);
        WorkerSlots.Running oldest = slots.oldestRunning();
        Assert.assertNotNull(oldest);
        Assert.assertEquals(TaskA.class.getName(), oldest.taskClassName);
        Assert.assertEquals("WorkerSlotsTest-worker-2", oldest.threadName);

        w2.run(WorkerSlots::markIdle);
        oldest = slots.oldestRunning();
        Assert.assertNotNull(oldest);
        Assert.assertEquals(TaskB.class.getName(), oldest.taskClassName);
        Assert.assertEquals("WorkerSlotsTest-worker-1", oldest.threadName);

        w1.run(WorkerSlots::markIdle);
        Assert.assertNull(slots.oldestRunning());
    }

    @Test
    public void testThreadDiedStamped() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Worker dead = newWorker();
        Worker live = newWorker();

        // the live thread registers first, so registration does not prune the dead slot
        live.run(() -> { slots.markRunning(TaskB.class); WorkerSlots.markIdle(); });
        // the dead thread holds the older stamp, as when beforeExecute throws after markRunning
        startTask(slots, dead, TaskA.class);
        dead.exit();
        startTask(slots, live, TaskB.class);
        Assert.assertEquals(2, slots.size());

        WorkerSlots.Running oldest = slots.oldestRunning();
        Assert.assertNotNull(oldest);
        Assert.assertEquals(TaskB.class.getName(), oldest.taskClassName);
        Assert.assertEquals(1, slots.size());

        live.run(WorkerSlots::markIdle);
        Assert.assertNull(slots.oldestRunning());
    }

    @Test
    public void testRegisterPrunesExitedThreads() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Worker w1 = newWorker();
        Worker w2 = newWorker();
        w1.run(() -> { slots.markRunning(TaskA.class); WorkerSlots.markIdle(); });
        w2.run(() -> slots.markRunning(TaskB.class));
        w1.exit();
        w2.exit();
        Assert.assertEquals(2, slots.size());

        Worker w3 = newWorker();
        w3.run(() -> { slots.markRunning(TaskC.class); WorkerSlots.markIdle(); });
        Assert.assertEquals(1, slots.size());
    }

    @Test
    public void testSlotReusedAcrossTasks() throws Exception
    {
        WorkerSlots slots = new WorkerSlots();
        Worker w = newWorker();
        for (Class<?> taskClass : new Class<?>[]{ TaskA.class, TaskB.class, TaskC.class })
        {
            w.run(() -> slots.markRunning(taskClass));
            WorkerSlots.Running running = slots.oldestRunning();
            Assert.assertNotNull(running);
            Assert.assertEquals(taskClass.getName(), running.taskClassName);
            w.run(WorkerSlots::markIdle);
            Assert.assertNull(slots.oldestRunning());
            Assert.assertEquals(1, slots.size());
        }
    }
}
