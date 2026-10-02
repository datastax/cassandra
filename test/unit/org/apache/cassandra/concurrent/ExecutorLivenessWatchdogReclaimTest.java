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

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Check;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Finding;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A memtable reclaim stuck behind a read, as in HCD-584: the read holds the memtable's read ordering, so the reclaim
 * task blocks in OpOrder.Barrier.await(). The watchdog must name the reclaim pool, its task and its thread, and the
 * thread dump must show that thread waiting on the barrier.
 */
public class ExecutorLivenessWatchdogReclaimTest extends CQLTester
{
    private static final long THRESHOLD = MILLISECONDS.toNanos(500);
    private static final long REPORT_INTERVAL = SECONDS.toNanos(60);

    private static Finding findingFor(Check check, String poolName)
    {
        for (Finding finding : check.findings)
            if (finding.poolName.equals(poolName))
                return finding;
        return null;
    }

    @Test
    public void testStalledReclaimIsReported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ThreadPoolExecutorPlus reclaim = (ThreadPoolExecutorPlus) cfs.reclaimExecutor();
        String poolName = reclaim.getThreadFactory().id;
        assertTrue(poolName, poolName.startsWith("MemtableReclaimMemory"));

        // the default configuration, apart from thresholds short enough for a test
        ExecutorLivenessWatchdog watchdog = new ExecutorLivenessWatchdog(ExecutorLivenessWatchdog::registeredPools,
                                                                         SECONDS.toNanos(5), THRESHOLD, THRESHOLD, REPORT_INTERVAL,
                                                                         ExecutorLivenessWatchdog.excludedPools(EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS.getString()));
        long reportedAt;
        Memtable memtable = cfs.getCurrentMemtable();
        try (OpOrder.Group read = memtable.readOrdering().start())
        {
            execute("INSERT INTO %s (k, v) VALUES (1, 1)");
            flush();
            Util.spinAssertEquals(true, () -> {
                RunningTaskSnapshot running = reclaim.longestRunningTask();
                return running != null && running.getRunningNanos() > THRESHOLD;
            }, 10);

            // the approximate clock in step with the precise one, so that a busy machine delaying its refresh cannot
            // make this a clock stall
            reportedAt = preciseTime.now();
            long checkedAt = reportedAt;
            Check check = watchdog.check(() -> checkedAt, reportedAt, true);
            Finding finding = findingFor(check, poolName);
            assertNotNull(check.findings.toString(), finding);
            assertNotNull(finding.longestRunning);
            String threadName = finding.longestRunning.getThreadName();
            assertTrue(threadName, threadName.startsWith(poolName));
            assertTrue(check.dumpDue);
            assertTrue(check.stalledThreadNames().contains(threadName));

            assertTrue(check.stalled().toString(), check.stalled().contains(poolName));
            String dump = ThreadDump.dumpAllThreads(check.stalled(), check.stalledThreadNames());
            int start = dump.indexOf("\n\"" + threadName + "\" ");
            assertTrue(dump, start >= 0);
            int end = dump.indexOf("\n\"", start + 1);
            String section = end < 0 ? dump.substring(start) : dump.substring(start, end);
            assertTrue(section, section.contains("\tat " + OpOrder.Barrier.class.getName() + ".await("));
        }

        Util.spinAssertEquals(null, reclaim::longestRunningTask, 10);
        // past the rate limit, so a stall would be reported again
        long later = reportedAt + REPORT_INTERVAL;
        Check check = watchdog.check(() -> later, later, true);
        assertNull(check.findings.toString(), findingFor(check, poolName));
    }
}
