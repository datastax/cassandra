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

import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * A task object built by {@link TaskFactory} that the executor stamps when it queues it, so the executor can report
 * how long its head-of-queue task has waited. The stamp is written by the submitting thread before the task is added
 * to the executor's queue, which publishes it to any thread that later finds the task there.
 * <p>
 * Stamps use {@link org.apache.cassandra.utils.MonotonicClock.Global#approxTime}, which keeps the per-task cost to a
 * volatile load; ages are read against {@link org.apache.cassandra.utils.MonotonicClock.Global#preciseTime}, which
 * the default approximate clock samples, so both share a timebase. A stamp lags the precise clock by up to one refresh
 * interval ({@code approxTime.error()}), so an age over-reports by about that and never under-reports.
 * <p>
 * The approximate clock is refreshed by a task on {@link ScheduledExecutors#scheduledFastTasks}. If that thread stalls,
 * new stamps freeze at the stall instant and every age read against the frozen clock would be ~0; read against the
 * precise clock they are upper bounds instead, so fresh tasks look old on every pool while the stuck task on
 * scheduledFastTasks, stamped before the freeze, reports its true age. A custom approximate clock configured with
 * {@code cassandra.monotonic_clock.approx} must share the precise clock's timebase for ages to be meaningful.
 */
interface TimedTask extends WrappedTask
{
    /** The approximate-clock time at which the executor queued this task, or 0 if it was never queued. */
    long enqueuedAtNanos();

    /** Called by the executor on the submitting thread, before the task is added to its queue. */
    void markEnqueued(long approxNanos);

    /** The age of an approximate-clock stamp, read against the precise clock. */
    static long ageNanos(long stampNanos)
    {
        return ageNanos(stampNanos, preciseTime.now());
    }

    static long ageNanos(long stampNanos, long nowNanos)
    {
        return Math.max(0L, nowNanos - stampNanos);
    }

    /** The age of {@code task}'s queue stamp, or 0 if it is not a stamped task. */
    static long queuedNanos(Object task)
    {
        if (!(task instanceof TimedTask))
            return 0L;
        long stamp = ((TimedTask) task).enqueuedAtNanos();
        return stamp == 0L ? 0L : ageNanos(stamp);
    }
}
