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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Check;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Finding;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The watchdog as the daemon runs it: started by {@link ExecutorLivenessWatchdog#start()} from its properties, checking
 * the node's registered pools on its own scheduled thread, and logging on its real loggers. A memtable reclaim stuck
 * behind a read, as in HCD-584, must be warned about once, naming the reclaim pool, its task and its thread, and
 * dumped once, the dump showing that thread waiting on the read barrier; once the reclaim is done, nothing more is
 * logged about it.
 */
public class ExecutorLivenessWatchdogEndToEndTest extends CQLTester
{
    // the smallest valid configuration
    private static final long CHECK_INTERVAL_MS = 1_000;
    private static final long THRESHOLD_MS = ExecutorLivenessWatchdog.MIN_THRESHOLD_MILLIS;
    private static final long REPORT_INTERVAL_MS = ExecutorLivenessWatchdog.MIN_REPORT_INTERVAL_MILLIS;
    // how long to wait for the stall to be reported, past the threshold
    private static final long REPORT_TIMEOUT_MS = THRESHOLD_MS + 20_000;

    private static final String WATCHDOG_LOGGER = ExecutorLivenessWatchdog.class.getName();
    private static final String DUMP_LOGGER = ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME;

    // collects what the loggers it is attached to log, from any thread
    private static final class CollectingAppender extends AppenderBase<ILoggingEvent>
    {
        final Queue<ILoggingEvent> events = new ConcurrentLinkedQueue<>();

        @Override
        protected void append(ILoggingEvent event)
        {
            events.add(event);
        }

        // the warnings logged on the given logger
        List<String> warnings(String loggerName)
        {
            return events.stream()
                         .filter(event -> event.getLoggerName().equals(loggerName) && event.getLevel() == Level.WARN)
                         .map(ILoggingEvent::getFormattedMessage)
                         .collect(Collectors.toList());
        }

        // the watchdog's warnings that mention the pool
        List<String> poolWarnings(String poolName)
        {
            return warnings(WATCHDOG_LOGGER).stream()
                                            .filter(message -> message.contains(poolName))
                                            .collect(Collectors.toList());
        }
    }

    private static Finding findingFor(Check check, String poolName)
    {
        for (Finding finding : check.findings)
            if (finding.poolName.equals(poolName))
                return finding;
        return null;
    }

    @Test
    public void testStalledReclaimIsReportedOnce() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ThreadPoolExecutorPlus reclaim = (ThreadPoolExecutorPlus) cfs.reclaimExecutor();
        String poolName = reclaim.getThreadFactory().id;
        assertTrue(poolName, poolName.startsWith("MemtableReclaimMemory"));

        // start() of a watchdog already running would keep its configuration, and stopping it would affect whoever
        // started it, so nothing else in this JVM may run it
        assertFalse(ExecutorLivenessWatchdog.isRunning());

        Logger watchdogLogger = (Logger) LoggerFactory.getLogger(WATCHDOG_LOGGER);
        Logger dumpLogger = (Logger) LoggerFactory.getLogger(DUMP_LOGGER);
        CollectingAppender appender = new CollectingAppender();
        appender.start();
        watchdogLogger.addAppender(appender);
        dumpLogger.addAppender(appender);
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, CHECK_INTERVAL_MS)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS, THRESHOLD_MS)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS, THRESHOLD_MS)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS, REPORT_INTERVAL_MS))
        {
            ExecutorLivenessWatchdog.start();
            assertTrue(ExecutorLivenessWatchdog.isRunning());
            ExecutorLivenessWatchdog watchdog = ExecutorLivenessWatchdog.watchdog();
            ScheduledExecutorPlus executor = ExecutorLivenessWatchdog.executor();

            RunningTaskSnapshot stalled;
            Memtable memtable = cfs.getCurrentMemtable();
            try (OpOrder.Group read = memtable.readOrdering().start())
            {
                execute("INSERT INTO %s (k, v) VALUES (1, 1)");
                flush();
                Util.spinAssertEquals(true, () -> reclaim.longestRunningTask() != null, 10);
                stalled = reclaim.longestRunningTask();
                assertNotNull(stalled);

                // the first check past the threshold warns, then dumps
                Util.spinAssert("no thread dump logged within " + REPORT_TIMEOUT_MS + "ms",
                                () -> assertFalse(appender.warnings(DUMP_LOGGER).isEmpty()),
                                REPORT_TIMEOUT_MS, MILLISECONDS);
                // the next checks find the stall still going on, and log nothing more about it
                Uninterruptibles.sleepUninterruptibly(3 * CHECK_INTERVAL_MS, MILLISECONDS);
            }

            // the reclaim finishes once the read is done; the next checks find nothing new to log
            Util.spinAssertEquals(null, reclaim::longestRunningTask, 10);
            Uninterruptibles.sleepUninterruptibly(2 * CHECK_INTERVAL_MS, MILLISECONDS);
            ExecutorLivenessWatchdog.stop();
            assertTrue(executor.awaitTermination(10, SECONDS));

            String taskClass = stalled.getTaskClassName();
            String threadName = stalled.getThreadName();
            assertTrue(taskClass, taskClass.startsWith(ColumnFamilyStore.class.getName() + "$Flush"));
            assertTrue(threadName, threadName.startsWith(poolName));

            List<String> warnings = appender.poolWarnings(poolName);
            assertEquals(warnings.toString(), 1, warnings.size());
            String warning = warnings.get(0);
            assertTrue(warning, warning.startsWith("Executor liveness: " + poolName + " looks stalled: "));
            assertTrue(warning, warning.contains(" " + taskClass + " on thread " + threadName + ", running for "));

            List<String> dumps = appender.warnings(DUMP_LOGGER);
            assertEquals(1, dumps.size());
            String dump = dumps.get(0);
            String header = dump.substring(0, dump.indexOf('\n'));
            assertTrue(header, header.startsWith("Executor liveness thread dump ("));
            int stalledAt = header.indexOf("; stalled: ");
            assertTrue(header, stalledAt >= 0 && header.substring(stalledAt).contains(poolName));
            int start = dump.indexOf("\n\"" + threadName + "\" ");
            assertTrue(dump, start >= 0);
            int end = dump.indexOf("\n\"", start + 1);
            String section = end < 0 ? dump.substring(start) : dump.substring(start, end);
            assertTrue(section, section.contains("\tat " + OpOrder.Barrier.class.getName() + ".await("));

            // the rate limit, not the recovery, holds back another warning within the report interval; past it, the
            // watchdog, its state as the scheduled checks left it, still finds nothing to report
            long later = preciseTime.now() + MILLISECONDS.toNanos(REPORT_INTERVAL_MS);
            Check check = watchdog.check(() -> later, later, true);
            assertNull(check.findings.toString(), findingFor(check, poolName));
            assertFalse(check.stalled().toString(), check.stalled().contains(poolName));
        }
        finally
        {
            ExecutorLivenessWatchdog.stop();
            watchdogLogger.detachAppender(appender);
            dumpLogger.detachAppender(appender);
            appender.stop();
        }
    }
}
