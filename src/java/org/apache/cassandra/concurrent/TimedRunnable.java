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

import java.util.Objects;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Wraps a raw {@link Runnable} executed on a {@link DebuggableThreadPoolExecutor} with its
 * submission time. 32 bytes with compressed oops; created only when the executed task does not
 * already implement {@link TimedTask} (futures from {@code submit} carry their own stamp).
 * Callers that wrap a task in a future of their own can wrap that future and name the user task's
 * class, so the executor reports the class of the work rather than of the future.
 */
public final class TimedRunnable implements Runnable, TimedTask
{
    final Runnable task;
    private final Class<?> taskClass;
    private final long enqueuedAtNanos;

    TimedRunnable(Runnable task)
    {
        this(task, task.getClass());
    }

    public TimedRunnable(Runnable task, Class<?> taskClass)
    {
        this.task = Objects.requireNonNull(task);
        this.taskClass = Objects.requireNonNull(taskClass);
        this.enqueuedAtNanos = approxTime.now();
    }

    @Override
    public void run()
    {
        task.run();
    }

    @Override
    public long enqueuedAtNanos()
    {
        return enqueuedAtNanos;
    }

    @Override
    public Class<?> taskClass()
    {
        return taskClass;
    }

    /** The user task behind {@code r}, or {@code r} itself when it is not wrapped. */
    static Runnable unwrap(Runnable r)
    {
        return r instanceof TimedRunnable ? ((TimedRunnable) r).task : r;
    }
}
