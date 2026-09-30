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

import static org.apache.cassandra.utils.MonotonicClock.preciseTime;

/**
 * A queued task that remembers when it was submitted and what it wraps, so an executor can report
 * how long its head-of-queue task has waited and what its workers are running.
 * <p>
 * Stamps use {@link org.apache.cassandra.utils.MonotonicClock#approxTime}, which keeps the per-task cost to a volatile
 * load; ages are read against {@link org.apache.cassandra.utils.MonotonicClock#preciseTime}, which the default
 * approximate clock samples, so both share a timebase. A stamp lags the precise clock by up to one refresh interval
 * ({@code approxTime.error()}), so an age over-reports by about that and never under-reports.
 * <p>
 * The approximate clock is refreshed by a task on {@link ScheduledExecutors#scheduledFastTasks}. If that thread stalls,
 * new stamps freeze at the stall instant and every age read against the frozen clock would be ~0; read against the
 * precise clock they are upper bounds instead, so fresh tasks look old on every pool while the stuck task on
 * scheduledFastTasks, stamped before the freeze, reports its true age. A custom approximate clock configured with
 * {@code cassandra.monotonic_clock.approx} must share the precise clock's timebase for ages to be meaningful.
 */
interface TimedTask
{
    long enqueuedAtNanos();

    /**
     * The class of the user-supplied task, for diagnostics. Tasks submitted through {@code Stage.execute} report the
     * user class; tasks submitted through {@code Stage.submit} go via {@code CompletableFuture.runAsync/supplyAsync}
     * and therefore report the JDK {@code AsyncRun}/{@code AsyncSupply} class.
     */
    Class<?> taskClass();

    /** The age of an approximate-clock stamp, read against the precise clock. */
    static long ageNanos(long stampNanos)
    {
        return ageNanos(stampNanos, preciseTime.now());
    }

    static long ageNanos(long stampNanos, long nowNanos)
    {
        return Math.max(0L, nowNanos - stampNanos);
    }
}
