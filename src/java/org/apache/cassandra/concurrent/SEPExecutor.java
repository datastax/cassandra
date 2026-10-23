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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.concurrent.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.metrics.ThreadPoolMetrics;

import static org.apache.cassandra.concurrent.SEPExecutor.TakeTaskPermitResult.*;
import static org.apache.cassandra.concurrent.SEPWorker.Work;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class SEPExecutor implements LocalAwareExecutorPlus, SEPExecutorMBean, RunningTaskSource
{
    private static final Logger logger = LoggerFactory.getLogger(SEPExecutor.class);
    private static final TaskFactory taskFactory = TaskFactory.localAware();

    private final SharedExecutorPool pool;

    private final AtomicInteger maximumPoolSize;
    private final MaximumPoolSizeListener maximumPoolSizeListener;
    public final String name;
    private final String mbeanName;
    @VisibleForTesting
    public final ThreadPoolMetrics metrics;

    // stores both a set of work permits and task permits:
    //  bottom 32 bits are number of queued tasks, in the range [0..maxTasksQueued]   (initially 0)
    //  top 32 bits are number of work permits available in the range [-resizeDelta..maximumPoolSize]   (initially maximumPoolSize)
    private final AtomicLong permits = new AtomicLong();

    private final AtomicLong completedTasks = new AtomicLong();

    volatile boolean shuttingDown = false;
    final Condition shutdown = newOneTimeCondition();

    // TODO: see if other queue implementations might improve throughput
    protected final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();

    SEPExecutor(SharedExecutorPool pool, int maximumPoolSize, MaximumPoolSizeListener maximumPoolSizeListener, String jmxPath, String name)
    {
        this.pool = pool;
        this.name = NamedThreadFactory.globalPrefix() + name;
        this.mbeanName = "org.apache.cassandra." + jmxPath + ":type=" + name;
        this.maximumPoolSize = new AtomicInteger(maximumPoolSize);
        this.maximumPoolSizeListener = maximumPoolSizeListener;
        this.permits.set(combine(0, maximumPoolSize));
        this.metrics = new ThreadPoolMetrics(this, jmxPath, name).register();
        MBeanWrapper.instance.registerMBean(this, mbeanName);
    }

    protected void onCompletion()
    {
        completedTasks.incrementAndGet();
    }

    /**
     * The time since this executor queued the task at the head, for every task; native-transport backpressure reads
     * {@link #oldestDebuggableTaskQueueTime()} instead.
     */
    @Override
    public long oldestTaskQueueTime()
    {
        return TimedTask.queuedNanos(tasks.peek());
    }

    @Override
    public long oldestDebuggableTaskQueueTime()
    {
        DebuggableTask debuggableTask = submittedDebuggableTask(tasks.peek());
        if (debuggableTask == null)
            return 0L;

        return debuggableTask.elapsedSinceCreation();
    }

    private static DebuggableTask submittedDebuggableTask(Runnable task)
    {
        if (!(task instanceof FutureTask))
            return null;

        return ((FutureTask<?>) task).debuggableTask();
    }

    @Override
    public long longestRunningTaskTime()
    {
        RunningTask oldest = oldestRunningTask();
        return oldest == null ? 0L : TimedTask.ageNanos(oldest.capturedStartNanos);
    }

    @Override
    public String getLongestRunningTaskClass()
    {
        RunningTask oldest = oldestRunningTask();
        return oldest == null ? null : oldest.taskClass.getName();
    }

    @Override
    public RunningTaskSnapshot longestRunningTask()
    {
        RunningTask oldest = oldestRunningTask();
        return oldest == null ? null : new RunningTaskSnapshot(TimedTask.ageNanos(oldest.capturedStartNanos),
                                                               oldest.taskClass.getName(),
                                                               oldest.worker.threadName(this));
    }

    // the stamp, class and worker of this executor's oldest running task, all captured in the scan that selected it, so
    // none is re-read from a worker that may since have moved on. The worker's thread name is derived only for a
    // snapshot, so that the gauges do not build it on every read
    private static final class RunningTask
    {
        final long capturedStartNanos;
        final Class<?> taskClass;
        final SEPWorker worker;

        RunningTask(long capturedStartNanos, Class<?> taskClass, SEPWorker worker)
        {
            this.capturedStartNanos = capturedStartNanos;
            this.taskClass = taskClass;
            this.worker = worker;
        }
    }

    // the oldest task running for this executor, or null when none is; see SEPWorker.runningFor for the read protocol.
    // The stamp read is at least as new as the task and executor read, so this never over-reports. A read racing a task
    // boundary or a reassignment can attribute the new task's near-zero running time to the previous task's class
    // (possibly a task of the executor the worker just left); we accept that, as the reported time is then near zero.
    // The thread name is the one the worker gives its thread while it serves this executor, not read from the thread,
    // which may since have been renamed for another executor.
    private RunningTask oldestRunningTask()
    {
        Runnable oldest = null;
        SEPWorker oldestWorker = null;
        long oldestStart = Long.MAX_VALUE;
        for (SEPWorker worker : pool.allWorkers)
        {
            Runnable task = worker.currentTask.get();
            if (task == null || worker.runningFor.get() != this)
                continue;
            long start = worker.taskStartedAtNanos;
            if (start < oldestStart)
            {
                oldestStart = start;
                oldest = task;
                oldestWorker = worker;
            }
        }
        return oldest == null ? null : new RunningTask(oldestStart, WrappedTask.classOf(oldest), oldestWorker);
    }

    @Override
    public int getMaxTasksQueued()
    {
        return Integer.MAX_VALUE;
    }

    // schedules another worker for this pool if there is work outstanding and there are no spinning threads that
    // will self-assign to it in the immediate future
    boolean maybeSchedule()
    {
        if (pool.spinningCount.get() > 0 || !takeWorkPermit(true))
            return false;

        pool.schedule(new Work(this));
        return true;
    }

    protected <T extends Runnable> T addTask(T task)
    {
        if (task instanceof TimedTask)
            ((TimedTask) task).markEnqueued(approxTime.now());

        // we add to the queue first, so that when a worker takes a task permit it can be certain there is a task available
        // this permits us to schedule threads non-spuriously; it also means work is serviced fairly
        tasks.add(task);
        int taskPermits;
        while (true)
        {
            long current = permits.get();
            taskPermits = taskPermits(current);
            // because there is no difference in practical terms between the work permit being added or not (the work is already in existence)
            // we always add our permit, but block after the fact if we breached the queue limit
            if (permits.compareAndSet(current, updateTaskPermits(current, taskPermits + 1)))
                break;
        }

        if (taskPermits == 0)
        {
            // we only need to schedule a thread if there are no tasks already waiting to be processed, as
            // the original enqueue will have started a thread to service its work which will have itself
            // spawned helper workers that would have either exhausted the available tasks or are still being spawned.
            // to avoid incurring any unnecessary signalling penalties we also do not take any work to hand to the new
            // worker, we simply start a worker in a spinning state
            pool.maybeStartSpinningWorker();
        }
        return task;
    }

    public enum TakeTaskPermitResult
    {
        NONE_AVAILABLE,        // No task permits available
        TOOK_PERMIT,           // Took a permit and reduced task permits
        RETURNED_WORK_PERMIT   // Detected pool shrinking and returned work permit ahead of SEPWorker exit.
    }

    // takes permission to perform a task, if any are available; once taken it is guaranteed
    // that a proceeding call to tasks.poll() will return some work
    TakeTaskPermitResult takeTaskPermit(boolean checkForWorkPermitOvercommit)
    {
        TakeTaskPermitResult result;
        while (true)
        {
            long current = permits.get();
            long updated;
            int workPermits = workPermits(current);
            int taskPermits = taskPermits(current);
            if (workPermits < 0 && checkForWorkPermitOvercommit)
            {
                // Work permits are negative when the pool is reducing in size.  Atomically
                // adjust the number of work permits so there is no race of multiple SEPWorkers
                // exiting.  On conflicting update, recheck.
                result = RETURNED_WORK_PERMIT;
                updated = updateWorkPermits(current, workPermits + 1);
            }
            else
            {
                if (taskPermits == 0)
                    return NONE_AVAILABLE;
                result = TOOK_PERMIT;
                updated = updateTaskPermits(current, taskPermits - 1);
            }
            if (permits.compareAndSet(current, updated))
            {
                return result;
            }
        }
    }

    // takes a worker permit and (optionally) a task permit simultaneously; if one of the two is unavailable, returns false
    boolean takeWorkPermit(boolean takeTaskPermit)
    {
        int taskDelta = takeTaskPermit ? 1 : 0;
        while (true)
        {
            long current = permits.get();
            int workPermits = workPermits(current);
            int taskPermits = taskPermits(current);
            if (workPermits <= 0 || taskPermits == 0)
                return false;
            if (permits.compareAndSet(current, combine(taskPermits - taskDelta, workPermits - 1)))
            {
                return true;
            }
        }
    }

    // gives up a work permit
    void returnWorkPermit()
    {
        while (true)
        {
            long current = permits.get();
            int workPermits = workPermits(current);
            if (permits.compareAndSet(current, updateWorkPermits(current, workPermits + 1)))
                return;
        }
    }

    @Override
    public void maybeExecuteImmediately(Runnable task)
    {
        task = taskFactory.toExecute(task);
        if (!takeWorkPermit(false))
        {
            addTask(task);
        }
        else
        {
            try
            {
                task.run();
            }
            finally
            {
                returnWorkPermit();
                // we have to maintain our invariant of always scheduling after any work is performed
                // in this case in particular we are not processing the rest of the queue anyway, and so
                // the work permit may go wasted if we don't immediately attempt to spawn another worker
                maybeSchedule();
            }
        }
    }

    @Override
    public void execute(Runnable run)
    {
        addTask(taskFactory.toExecute(run));
    }

    @Override
    public void execute(WithResources withResources, Runnable run)
    {
        addTask(taskFactory.toExecute(withResources, run));
    }

    @Override
    public Future<?> submit(Runnable run)
    {
        return addTask(taskFactory.toSubmit(run));
    }

    @Override
    public <T> Future<T> submit(Runnable run, T result)
    {
        return addTask(taskFactory.toSubmit(run, result));
    }

    @Override
    public <T> Future<T> submit(Callable<T> call)
    {
        return addTask(taskFactory.toSubmit(call));
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable run, T result)
    {
        return addTask(taskFactory.toSubmit(withResources, run, result));
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable run)
    {
        return addTask(taskFactory.toSubmit(withResources, run));
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> call)
    {
        return addTask(taskFactory.toSubmit(withResources, call));
    }

    @Override
    public boolean inExecutor()
    {
        throw new UnsupportedOperationException();
    }

    public synchronized void shutdown()
    {
        if (shuttingDown)
            return;
        shuttingDown = true;
        pool.executors.remove(this);
        if (getActiveTaskCount() == 0)
            shutdown.signalAll();

        // release metrics
        metrics.release();
        MBeanWrapper.instance.unregisterMBean(mbeanName);
    }

    public synchronized List<Runnable> shutdownNow()
    {
        shutdown();
        List<Runnable> aborted = new ArrayList<>();
        while (takeTaskPermit(false) == TOOK_PERMIT)
            aborted.add(tasks.poll());
        return aborted;
    }

    public boolean isShutdown()
    {
        return shuttingDown;
    }

    public boolean isTerminated()
    {
        return shuttingDown && shutdown.isSignalled();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        shutdown.await(timeout, unit);
        return isTerminated();
    }

    @Override
    public int getPendingTaskCount()
    {
        return taskPermits(permits.get());
    }

    @Override
    public long getCompletedTaskCount()
    {
        return completedTasks.get();
    }

    public int getActiveTaskCount()
    {
        return maximumPoolSize.get() - workPermits(permits.get());
    }

    public int getCorePoolSize()
    {
        return 0;
    }

    public void setCorePoolSize(int newCorePoolSize)
    {
        throw new IllegalArgumentException("Cannot resize core pool size of SEPExecutor");
    }

    @Override
    public int getMaximumPoolSize()
    {
        return maximumPoolSize.get();
    }

    @Override
    public synchronized void setMaximumPoolSize(int newMaximumPoolSize)
    {
        final int oldMaximumPoolSize = maximumPoolSize.get();

        if (newMaximumPoolSize < 0)
        {
            throw new IllegalArgumentException("Maximum number of workers must not be negative");
        }

        int deltaWorkPermits = newMaximumPoolSize - oldMaximumPoolSize;
        if (!maximumPoolSize.compareAndSet(oldMaximumPoolSize, newMaximumPoolSize))
        {
            throw new IllegalStateException("Maximum pool size has been changed while resizing");
        }

        if (deltaWorkPermits == 0)
            return;

        permits.updateAndGet(cur -> updateWorkPermits(cur, workPermits(cur) + deltaWorkPermits));
        logger.info("Resized {} maximum pool size from {} to {}", name, oldMaximumPoolSize, newMaximumPoolSize);

        // If we we have more work permits than before we should spin up a worker now rather than waiting
        // until either a new task is enqueued (if all workers are descheduled) or a spinning worker calls
        // maybeSchedule().
        pool.maybeStartSpinningWorker();

        maximumPoolSizeListener.onUpdateMaximumPoolSize(newMaximumPoolSize);
    }

    private static int taskPermits(long both)
    {
        return (int) both;
    }

    private static int workPermits(long both) // may be negative if resizing
    {
        return (int) (both >> 32); // sign extending right shift
    }

    private static long updateTaskPermits(long prev, int taskPermits)
    {
        return (prev & (-1L << 32)) | taskPermits;
    }

    private static long updateWorkPermits(long prev, int workPermits)
    {
        return (((long) workPermits) << 32) | (prev & (-1L >>> 32));
    }

    private static long combine(int taskPermits, int workPermits)
    {
        return (((long) workPermits) << 32) | taskPermits;
    }
}
