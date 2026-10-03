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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Check;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Pool;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The watchdog keeps state per pool name, so a node that keeps creating pools under new names, as repair does with a
 * {@code Repair#N} pool per session, must not grow it without bound. Each check here adds a new stalled pool and drops
 * the oldest, over many report intervals, with the approximate clock stalling every few checks, and the watchdog's
 * state must stay within the bounds its design gives it.
 */
public class ExecutorLivenessWatchdogPoolChurnTest
{
    private static final long CHECK_INTERVAL = SECONDS.toNanos(5);
    private static final long THRESHOLD = SECONDS.toNanos(300);
    private static final long REPORT_INTERVAL = SECONDS.toNanos(600);
    // an arbitrary precise-clock reading, far from 0 and from overflow
    private static final long START = SECONDS.toNanos(1_000_000);

    // over 80 report intervals
    private static final int CHECKS = 10_000;
    // how many checks a pool lives for: under a report interval, so that each pool is reported once, when it appears,
    // and is still remembered as reported for a while after it is gone
    private static final int POOL_LIFETIME = 100;
    // a pool reported within the last report interval is remembered, and one is reported at each check
    private static final int MAX_REPORTED = (int) (REPORT_INTERVAL / CHECK_INTERVAL);
    // the approximate clock lags the precise one by over the stall tolerance at every so many checks
    private static final int CLOCK_STALL_EVERY = 3;
    private static final long CLOCK_LAG = SECONDS.toNanos(2);

    /** A pool stalled for good: its longest-running task is over the threshold. */
    private static final class StalledPool implements ResizableThreadPool, RunningTaskSource
    {
        private final RunningTaskSnapshot running;

        StalledPool(String name)
        {
            running = new RunningTaskSnapshot(THRESHOLD + 1, "org.apache.cassandra.repair.RepairJob", name + ":1");
        }

        public int getCorePoolSize() { return 1; }
        public void setCorePoolSize(int newCorePoolSize) {}
        public int getMaximumPoolSize() { return 1; }
        public void setMaximumPoolSize(int newMaximumPoolSize) {}
        public int getActiveTaskCount() { return 1; }
        public long getCompletedTaskCount() { return 0; }
        public int getPendingTaskCount() { return 0; }
        public long oldestTaskQueueTime() { return 0; }
        public RunningTaskSnapshot longestRunningTask() { return running; }
    }

    @Test
    public void testStateStaysBoundedUnderPoolChurn()
    {
        Deque<Pool> pools = new ArrayDeque<>();
        ExecutorLivenessWatchdog watchdog = new ExecutorLivenessWatchdog(() -> pools, CHECK_INTERVAL, THRESHOLD, THRESHOLD,
                                                                         REPORT_INTERVAL,
                                                                         ExecutorLivenessWatchdog.excludedPools(""));
        int maxReported = 0;
        int maxFreezes = 0;
        int dumps = 0;
        for (int i = 0; i < CHECKS; i++)
        {
            pools.addLast(Pool.of("Repair#" + i, new StalledPool("Repair#" + i)));
            if (pools.size() > POOL_LIFETIME)
                pools.removeFirst();
            Set<String> live = new HashSet<>();
            for (Pool pool : pools)
                live.add(pool.name);

            long now = START + i * CHECK_INTERVAL;
            // a new frozen reading at each clock stall, so each is a stall of its own
            long approxNow = i % CLOCK_STALL_EVERY == 0 ? now - CLOCK_LAG : now;
            Check check = watchdog.check(() -> now, approxNow, true);
            if (check.dumpDue)
            {
                watchdog.dumped(check);
                dumps++;
            }
            // the new pool is reported at once, and only it
            List<String> reported = new ArrayList<>();
            check.findings.forEach(finding -> reported.add(finding.poolName));
            assertEquals(List.of("Repair#" + i), reported);

            // the watchdog's state kept per pool name or per clock stall; it touches it only on the checking thread,
            // here the test's
            int reportedPools = watchdog.reportedPoolCount();
            assertTrue(i + ": " + reportedPools, reportedPools <= MAX_REPORTED);
            maxReported = Math.max(maxReported, reportedPools);

            Set<String> dumped = watchdog.dumpedPools();
            assertTrue(i + ": " + dumped, live.containsAll(dumped));

            int freezes = watchdog.freezeCount();
            assertTrue(i + ": " + freezes, freezes <= ExecutorLivenessWatchdog.MAX_FREEZES);
            maxFreezes = Math.max(maxFreezes, freezes);
        }

        // the bounds are reached, so the churn did exercise them
        assertEquals(MAX_REPORTED, maxReported);
        assertEquals(ExecutorLivenessWatchdog.MAX_FREEZES, maxFreezes);
        // one dump per report interval, as a new pool stalls at every check
        long reportIntervals = (CHECKS - 1) * CHECK_INTERVAL / REPORT_INTERVAL;
        assertEquals(reportIntervals + 1, dumps);
    }
}
