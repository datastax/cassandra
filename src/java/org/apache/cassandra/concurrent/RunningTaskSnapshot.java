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

/**
 * The oldest task executing in a pool when it was read: how long it had been running, its class and the name of the
 * thread running it, all captured in the scan that selected it rather than by separate reads, so a task that keeps
 * running is reported with its own class and thread. A read racing that worker's task boundary can still pair one
 * task's time with the class of the task before or after it, as the executors' scans describe. It holds no reference
 * to the thread or the class.
 */
public final class RunningTaskSnapshot
{
    private final long runningNanos;
    private final String taskClassName;
    private final String threadName;

    public RunningTaskSnapshot(long runningNanos, String taskClassName, String threadName)
    {
        this.runningNanos = runningNanos;
        this.taskClassName = taskClassName;
        this.threadName = threadName;
    }

    /**
     * The oldest task executing in {@code pool}, null when idle. It is read in one scan if the pool is a
     * {@link RunningTaskSource}; otherwise it is built from {@link ResizableThreadPool#longestRunningTaskTime()} and
     * {@link ResizableThreadPool#getLongestRunningTaskClass()}, two reads that can pair one task's time with another
     * task's class, or with none, and its thread name is null.
     */
    public static RunningTaskSnapshot longestRunningTask(ResizableThreadPool pool)
    {
        if (pool instanceof RunningTaskSource)
            return ((RunningTaskSource) pool).longestRunningTask();
        long runningNanos = pool.longestRunningTaskTime();
        return runningNanos == 0 ? null : new RunningTaskSnapshot(runningNanos, pool.getLongestRunningTaskClass(), null);
    }

    /** Nanoseconds the task had been running when the pool was read. */
    public long getRunningNanos()
    {
        return runningNanos;
    }

    /** Fully qualified class name of the task, as {@link ResizableThreadPool#getLongestRunningTaskClass()}. */
    public String getTaskClassName()
    {
        return taskClassName;
    }

    /** Name of the thread running the task when the pool was read, null if the pool could not tell. */
    public String getThreadName()
    {
        return threadName;
    }

    @Override
    public String toString()
    {
        return taskClassName + " on " + threadName + " running for " + runningNanos + "ns";
    }
}
