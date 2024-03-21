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

package org.apache.cassandra.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.Flags;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * An executor which relies on the {@link ParkedThreadsMonitor} to unpark it's threads which are waiting on lock-less
 * queues. How can threads wait on lock-less queues? They just park themselves when they see the queue as empty. Since
 * this observation is non-atomic to other threads putting work on the queue, and since putting work on the queue of a
 * parked thread doesn't notify the parked worker, we have the {@link ParkedThreadsMonitor} check when/if new work is
 * available for parked threads and unpark them.
 * <p>
 * This has the benefit of removing the cost of {@link LockSupport#unpark(Thread)} from the threads which are trying to
 * offload work onto the executor.
 */
public abstract class ParkedExecutor implements ShutdownableExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ParkedExecutor.class);
    private static final Runnable POISON_PILL = () -> {};
    volatile boolean isShutdown = false;

    protected final ParkedThreadsMonitor monitor = ParkedThreadsMonitor.instance.get().orElseThrow(() -> new IllegalStateException("Cannot use because ParkedThreadsMonitor is disabled"));

    /**
     * Create a {@code ParkedExecutor} if {@link ParkedThreadsMonitor} is enabled, otherwise a standard executor.
     *
     * @param name A base name for the threads in the thread pool (not null)
     * @param threadCount number of threads for the backing thread pool (>= 1)
     */
    public static ShutdownableExecutor createParkedExecutor(String name, int threadCount)
    {
        if (threadCount < 1)
            throw new IllegalArgumentException("Less than 1 thread in thread pool is not allowed, but " + threadCount +" requested for pool " + name);
        if (threadCount == 1)
        {
            return ParkedThreadsMonitor.IS_ENABLED ?
                   new ParkedSingleThreadedExecutor(name) :
                   new FallbackExecutor(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(name).build()));
        }
        return ParkedThreadsMonitor.IS_ENABLED ?
               new ParkedMultiThreadedExecutor(name, threadCount) :
               new FallbackExecutor(Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(name).build()));
    }

    private static void safeRun(Runnable r)
    {
        try
        {
            r.run();
        }
        catch (Throwable throwable)
        {
            JVMStabilityInspector.inspectThrowable(throwable);
            logger.error("Error while runningThreadsBitmap tasks: ", throwable);
        }
    }

    static class ParkedMultiThreadedExecutor extends ParkedExecutor implements ParkedThreadsMonitor.MonitorableThread
    {
        private final int allThreadsRunning;
        private final Thread[] workers;
        // bounded pre-allocated queue to avoid per task allocation overheads
        private final MpmcArrayQueue<Runnable> tasks = new MpmcArrayQueue<>(1024);
        // unbounded overflow for when the above is not enough
        private final Queue<Runnable> overflowTasks = new ConcurrentLinkedQueue<>();
        // this acts as an isRunning indicator bit map for the threads
        private final AtomicInteger runningThreadsBitmap = new AtomicInteger();
        private final int threadCount;

        private ParkedMultiThreadedExecutor(String name, int threadCount)
        {
            if (threadCount > 32)
                throw new IllegalStateException("ParkedExecutors pool size is limited to 32 threads currently");
            this.threadCount = threadCount;
            allThreadsRunning = -1 >>> (32 - threadCount);
            workers = new Thread[threadCount];
            NamedThreadFactory namedThreadFactory = new NamedThreadFactory(name);
            for (int i = 0; i < threadCount; i++)
            {
                int tIndex = i;
                workers[i] = namedThreadFactory.newThread(() -> processTasks(tIndex));
                workers[i].start();
            }

            monitor.addThreadToMonitor(this);
        }


        @Override
        public void shutdown() throws InterruptedException
        {
            isShutdown = true;
            for (int i = 0; i < threadCount; i++)
            {
                // poison pill always on overflow
                overflowTasks.offer(POISON_PILL);
            }

            for (int i = 0; i < threadCount; i++)
            {
                workers[i].join();
            }
            monitor.removeThreadToMonitor(this);
        }

        @Override
        public void execute(Runnable task)
        {
            if (isShutdown)
                throw new RejectedExecutionException("Executor has already been shutdown");
            // We have a bounded tasks queue, if it overflows we send via the unbounded MPMC queue. This can introduce
            // task reordering, but this is valid behavior for an Executor.
            if (!tasks.offer(task))
                overflowTasks.offer(task);
        }

        private void processTasks(int threadIndex)
        {
            // the bit to set/unset in the runningThreadsBitmap for this thread index
            int tMask = 1 << threadIndex;
            setThreadRunning(tMask);

            Runnable r;
            while (true)
            {
                boolean noTask = true;
                // always check both queues to ensure one cannot starve the other
                if ((r = overflowTasks.poll()) != null)
                {
                    if (r == POISON_PILL)
                        break;
                    noTask = false;
                    safeRun(r);
                }
                if ((r = tasks.relaxedPoll()) != null)
                {
                    // no poison on tasks queue
                    noTask = false;
                    safeRun(r);
                }

                if (noTask)
                {
                    setThreadNotRunning(tMask);
                    LockSupport.park();
                    setThreadRunning(tMask);

                    if (Thread.interrupted())
                        break;
                }
            }

            // after poison pill observed make best effort to drain queue
            while ((r = tasks.poll()) != null)
            {
                safeRun(r);
            }
        }

        private void setThreadRunning(int tMask)
        {
            int current;
            do
            {
                current = runningThreadsBitmap.get();
            }
            while (!runningThreadsBitmap.compareAndSet(current, Flags.add(current, tMask)));
        }

        private void setThreadNotRunning(int tMask)
        {
            int current;
            do
            {
                current = runningThreadsBitmap.get();
            }
            while (!runningThreadsBitmap.compareAndSet(current, Flags.remove(current, tMask)));
        }

        @Override
        public void unpark(long epoch)
        {
            int currentlyRunningMask = runningThreadsBitmap.get();
            for (int i = 0; i < threadCount; i++)
            {
                if (!Flags.contains(currentlyRunningMask, 1 << i))
                {
                    Thread worker = workers[i];
                    LockSupport.unpark(worker);
                    return;
                }
            }
        }

        @Override
        public boolean shouldUnpark(long epoch)
        {
            return (runningThreadsBitmap.get() != allThreadsRunning) &&
                    !(tasks.isEmpty() && overflowTasks.isEmpty());
        }
    }

    /**
     * A specialized implementation for a single threaded executor allows us to use the unbounded MPSC queue for
     * improved performance and simpler workflow.
     */
    static class ParkedSingleThreadedExecutor extends ParkedExecutor implements ParkedThreadsMonitor.MonitorableThread
    {
        private final Thread worker;
        private final MpscUnboundedArrayQueue<Runnable> tasks = new MpscUnboundedArrayQueue<>(1024);
        private final AtomicInteger running = new AtomicInteger();

        private ParkedSingleThreadedExecutor(String name)
        {
            NamedThreadFactory namedThreadFactory = new NamedThreadFactory(name);
            worker = namedThreadFactory.newThread(this::processTasks);
            worker.start();
            monitor.addThreadToMonitor(this);
        }


        @Override
        public void shutdown() throws InterruptedException
        {
            isShutdown = true;
            tasks.offer(POISON_PILL);
            worker.join();
            monitor.removeThreadToMonitor(this);
        }

        @Override
        public void execute(Runnable task)
        {
            if (isShutdown)
                throw new RejectedExecutionException("Executor has already been shutdown");
            // This is an unbounded queue, so no need to check for failed offers
            tasks.offer(task);
        }

        private void processTasks()
        {
            running.lazySet(1);

            Runnable r;
            while ((r = tasks.relaxedPoll()) != POISON_PILL)
            {
                if (r == null)
                {
                    running.lazySet(0);
                    LockSupport.park();
                    running.lazySet(1);

                    if (Thread.interrupted())
                        break;
                    continue;
                }
                ParkedExecutor.safeRun(r);
            }
        }

        @Override
        public void unpark(long epoch)
        {
            int currentlyRunningMask = running.get();
            if (currentlyRunningMask == 0)
            {
                LockSupport.unpark(this.worker);
            }
        }

        @Override
        public boolean shouldUnpark(long epoch)
        {
            return running.get() != 1 && !tasks.isEmpty();
        }
    }

    static class FallbackExecutor implements ShutdownableExecutor
    {
        private final ExecutorService executor;

        private FallbackExecutor(ExecutorService executor)
        {
            this.executor = executor;
        }

        @Override
        public void shutdown() throws InterruptedException
        {
            executor.shutdownNow();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }

        @Override
        public void execute(Runnable command)
        {
            executor.execute(command);
        }
    }
}
