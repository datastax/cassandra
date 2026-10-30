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

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Check;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Finding;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Pool;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.metrics.ThreadPoolMetrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ExecutorLivenessWatchdogTest
{
    private static final long CHECK_INTERVAL = SECONDS.toNanos(5);
    private static final long RUNNING_THRESHOLD = SECONDS.toNanos(300);
    private static final long QUEUED_THRESHOLD = SECONDS.toNanos(200);
    private static final long REPORT_INTERVAL = SECONDS.toNanos(600);
    // an arbitrary precise-clock reading, far from 0 and from overflow
    private static final long NOW = SECONDS.toNanos(1_000_000);

    private final List<Pool> pools = new ArrayList<>();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void stopWatchdog()
    {
        ExecutorLivenessWatchdog.stop();
    }

    /** A pool whose liveness readings are set by the test. */
    private static final class FakePool implements ResizableThreadPool, RunningTaskSource
    {
        long oldestQueuedNanos;
        RunningTaskSnapshot longestRunning;
        int active;
        int pending;
        boolean throwOnRead;
        Runnable onRead = () -> {};

        public int getCorePoolSize() { return 1; }
        public void setCorePoolSize(int newCorePoolSize) {}
        public int getMaximumPoolSize() { return 1; }
        public void setMaximumPoolSize(int newMaximumPoolSize) {}
        public int getActiveTaskCount() { return active; }
        public long getCompletedTaskCount() { return 0; }
        public int getPendingTaskCount() { return pending; }

        @Override
        public long oldestTaskQueueTime()
        {
            if (throwOnRead)
                throw new IllegalStateException("broken pool");
            return oldestQueuedNanos;
        }

        @Override
        public RunningTaskSnapshot longestRunningTask()
        {
            if (throwOnRead)
                throw new IllegalStateException("broken pool");
            onRead.run();
            return longestRunning;
        }

        FakePool running(long nanos, String taskClassName, String threadName)
        {
            longestRunning = new RunningTaskSnapshot(nanos, taskClassName, threadName);
            active = 1;
            return this;
        }

        FakePool queued(long nanos, int pending)
        {
            oldestQueuedNanos = nanos;
            this.pending = pending;
            return this;
        }

        FakePool idle()
        {
            longestRunning = null;
            oldestQueuedNanos = 0;
            active = 0;
            pending = 0;
            return this;
        }
    }

    private FakePool pool(String name)
    {
        FakePool pool = new FakePool();
        pools.add(Pool.of(name, pool));
        return pool;
    }

    private ExecutorLivenessWatchdog watchdog(Predicate<String> excluded)
    {
        return new ExecutorLivenessWatchdog(() -> pools, CHECK_INTERVAL, RUNNING_THRESHOLD, QUEUED_THRESHOLD,
                                            REPORT_INTERVAL, excluded);
    }

    private ExecutorLivenessWatchdog watchdog()
    {
        return watchdog(ExecutorLivenessWatchdog.excludedPools(""));
    }

    // the approximate clock in step with the precise one
    private static Check check(ExecutorLivenessWatchdog watchdog, long nowNanos)
    {
        return check(watchdog, nowNanos, nowNanos);
    }

    // the thread dump logger on
    private static Check check(ExecutorLivenessWatchdog watchdog, long nowNanos, long approxNowNanos)
    {
        return check(watchdog, () -> nowNanos, approxNowNanos);
    }

    // the thread dump logger on, and a dump due taken
    private static Check check(ExecutorLivenessWatchdog watchdog, LongSupplier preciseClock, long approxNowNanos)
    {
        Check check = watchdog.check(preciseClock, approxNowNanos, true);
        if (check.dumpDue)
            watchdog.dumped(check);
        return check;
    }

    private static Finding only(Check check)
    {
        assertEquals(check.findings.toString(), 1, check.findings.size());
        return check.findings.get(0);
    }

    private static List<String> poolNames(Check check)
    {
        return check.findings.stream().map(finding -> finding.poolName).collect(Collectors.toList());
    }

    // an executor with HCD-595's two running-task accessors but not RunningTaskSource, as one built outside
    // ExecutorFactory may be
    private static ExecutorPlus nonSnapshotExecutor(long runningNanos, String taskClassName)
    {
        Class<?>[] interfaces = { ExecutorPlus.class };
        return (ExecutorPlus) Proxy.newProxyInstance(ExecutorPlus.class.getClassLoader(), interfaces, (proxy, method, args) -> {
            switch (method.getName())
            {
                case "longestRunningTaskTime": return runningNanos;
                case "getLongestRunningTaskClass": return taskClassName;
                case "oldestTaskQueueTime": return 0L;
                case "getActiveTaskCount": return runningNanos == 0 ? 0 : 1;
                case "getPendingTaskCount": return 0;
                case "isShutdown": return false;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
    }

    // captures what a logger logs, its descendants included, until closed
    private static final class CapturedLog implements AutoCloseable
    {
        private final Logger logger;
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        CapturedLog(String loggerName)
        {
            logger = (Logger) LoggerFactory.getLogger(loggerName);
            appender.start();
            logger.addAppender(appender);
        }

        // the messages logged on exactly this logger
        List<String> messages(String loggerName)
        {
            return appender.list.stream()
                                .filter(event -> event.getLoggerName().equals(loggerName))
                                .map(ILoggingEvent::getFormattedMessage)
                                .collect(Collectors.toList());
        }

        @Override
        public void close()
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    public void testBelowThresholdNoFinding()
    {
        pool("ReadStage").running(RUNNING_THRESHOLD, "org.example.Task", "ReadStage-1").queued(QUEUED_THRESHOLD, 3);
        pool("MutationStage").idle();

        Check check = check(watchdog(), NOW);
        assertTrue(check.findings.isEmpty());
        assertFalse(check.dumpDue);
    }

    @Test
    public void testRunningOverThreshold()
    {
        pool("ReadStage").idle();
        pool("MemtableReclaimMemory").running(RUNNING_THRESHOLD + 1, "org.example.Reclaim", "MemtableReclaimMemory:1").queued(SECONDS.toNanos(2), 4);

        Check check = check(watchdog(), NOW);
        Finding finding = only(check);
        assertEquals("MemtableReclaimMemory", finding.poolName);
        assertFalse(finding.isClockStall());
        assertEquals("org.example.Reclaim", finding.longestRunning.getTaskClassName());
        assertEquals("MemtableReclaimMemory:1", finding.longestRunning.getThreadName());
        assertEquals(SECONDS.toNanos(2), finding.oldestQueuedNanos);
        assertEquals(1, finding.activeTasks);
        assertEquals(4, finding.pendingTasks);
        assertTrue(check.dumpDue);
        assertEquals(List.of("MemtableReclaimMemory:1"), check.stalledThreadNames());

        String message = finding.message;
        assertTrue(message, message.contains("MemtableReclaimMemory"));
        assertTrue(message, message.contains("org.example.Reclaim"));
        assertTrue(message, message.contains("MemtableReclaimMemory:1"));
        assertTrue(message, message.contains("running threshold of 300.0s"));
        assertFalse(message, message.contains("queued threshold"));
        assertTrue(message, message.contains("active 1, pending 4"));
    }

    @Test
    public void testQueuedOverThreshold()
    {
        pool("MemtablePostFlush").running(SECONDS.toNanos(1), "org.example.PostFlush", "MemtablePostFlush:1").queued(QUEUED_THRESHOLD + 1, 7);

        Check check = check(watchdog(), NOW);
        Finding finding = only(check);
        assertEquals("MemtablePostFlush", finding.poolName);
        assertEquals("org.example.PostFlush", finding.longestRunning.getTaskClassName());
        assertTrue(finding.message, finding.message.contains("queued threshold of 200.0s"));
        assertFalse(finding.message, finding.message.contains("running threshold"));
        assertTrue(check.dumpDue);
    }

    @Test
    public void testQueuedOverThresholdWithNothingRunning()
    {
        pool("GossipStage").queued(QUEUED_THRESHOLD + 1, 1);

        Check check = check(watchdog(), NOW);
        Finding finding = only(check);
        assertNull(finding.longestRunning);
        assertTrue(check.stalledThreadNames().isEmpty());
        assertTrue(check.dumpDue);
    }

    @Test
    public void testAgesPrintedOnlyForTasksPresent()
    {
        pool("MemtableReclaimMemory").running(RUNNING_THRESHOLD + 1, "org.example.Reclaim", "MemtableReclaimMemory:1");
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1").queued(SECONDS.toNanos(2), 4);
        pool("GossipStage").queued(QUEUED_THRESHOLD + 1, 1);

        Check check = check(watchdog(), NOW);
        assertEquals(List.of("MemtableReclaimMemory", "MemtableFlushWriter", "GossipStage"), poolNames(check));
        // nothing queued: no queued age
        String reclaimMessage = check.findings.get(0).message;
        assertTrue(reclaimMessage, reclaimMessage.contains("on thread MemtableReclaimMemory:1, running for 300.0s; active 1, pending 0"));
        assertFalse(reclaimMessage, reclaimMessage.contains("oldest queued task waiting"));
        // a task queued: its age
        String flushWriterMessage = check.findings.get(1).message;
        assertTrue(flushWriterMessage, flushWriterMessage.contains("on thread MemtableFlushWriter:1, running for 300.0s; " +
                                                                   "oldest queued task waiting for 2.0s; active 1, pending 4"));
        // nothing running: no running age
        String gossipMessage = check.findings.get(2).message;
        assertTrue(gossipMessage, gossipMessage.contains("Longest running task: none; oldest queued task waiting for 200.0s; " +
                                                         "active 0, pending 1"));
        assertFalse(gossipMessage, gossipMessage.contains("running for"));
    }

    @Test
    public void testExcludedPools()
    {
        pool("CompactionExecutor").running(RUNNING_THRESHOLD + 1, "org.example.Compaction", "CompactionExecutor:1");
        pool("Repair#1").running(RUNNING_THRESHOLD + 1, "org.example.Repair", "Repair#1:1");
        pool("Repair#2").queued(QUEUED_THRESHOLD + 1, 1);
        pool("Repair").running(RUNNING_THRESHOLD + 1, "org.example.Other", "Repair:1");

        Check check = check(watchdog(ExecutorLivenessWatchdog.excludedPools(" CompactionExecutor , Repair#* ,")), NOW);
        assertEquals("Repair", only(check).poolName);
    }

    @Test
    public void testExcludedPoolsParsing()
    {
        Predicate<String> excluded = ExecutorLivenessWatchdog.excludedPools("A, B*,,C ");
        assertTrue(excluded.test("A"));
        assertFalse(excluded.test("AA"));
        assertTrue(excluded.test("B"));
        assertTrue(excluded.test("B1"));
        assertTrue(excluded.test("C"));
        assertFalse(excluded.test(""));
        assertFalse(excluded.test("D"));
        assertFalse(ExecutorLivenessWatchdog.excludedPools("").test(""));
        assertFalse(ExecutorLivenessWatchdog.excludedPools(" , ").test("A"));
    }

    @Test
    public void testDefaultExclusions()
    {
        Predicate<String> excluded = ExecutorLivenessWatchdog.excludedPools(EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS.getDefaultValue());
        for (String pool : new String[]{ "CompactionExecutor", "ValidationExecutor", "ViewBuildExecutor", "CacheCleanupExecutor",
                                         "SecondaryIndexExecutor", "SecondaryIndexManagement", "HintsDispatcher" })
            assertTrue(pool, excluded.test(pool));
        // the pools a stuck flush or reclaim shows up in are always watched
        for (String pool : new String[]{ "MemtableFlushWriter", "MemtablePostFlush", "MemtableReclaimMemory", "MemtableReclaimMemory1",
                                         "PerDiskMemtableFlushWriter_0", "LocalSystemKeyspacesDiskMemtableFlushWriter",
                                         "ReadStage", "MutationStage", "Native-Transport-Requests", "GossipStage",
                                         "ScheduledFastTasks", "ScheduledTasks", "NonPeriodicTasks", "OptionalTasks" })
            assertFalse(pool, excluded.test(pool));
    }

    @Test
    public void testRateLimiting()
    {
        FakePool reclaim = pool("MemtableReclaimMemory").running(RUNNING_THRESHOLD + 1, "org.example.Reclaim", "MemtableReclaimMemory:1");
        ExecutorLivenessWatchdog watchdog = watchdog();

        Check first = check(watchdog, NOW);
        assertEquals("MemtableReclaimMemory", only(first).poolName);
        assertTrue(first.dumpDue);

        // still stalled, but reported within the interval: neither a finding nor a dump
        Check within = check(watchdog, NOW + REPORT_INTERVAL - 1);
        assertTrue(within.findings.isEmpty());
        assertFalse(within.dumpDue);

        // reported again, but the same stall is not dumped again
        Check after = check(watchdog, NOW + REPORT_INTERVAL);
        assertEquals("MemtableReclaimMemory", only(after).poolName);
        assertFalse(after.dumpDue);

        // idle, then stalled again: the warning limit still counts from the last report, but the stall is new
        reclaim.idle();
        assertTrue(check(watchdog, NOW + REPORT_INTERVAL + 1).findings.isEmpty());
        reclaim.running(RUNNING_THRESHOLD + 1, "org.example.Reclaim", "MemtableReclaimMemory:1");
        Check again = check(watchdog, NOW + 2 * REPORT_INTERVAL - 1);
        assertTrue(again.findings.isEmpty());
        assertTrue(again.dumpDue);
        assertEquals(List.of("MemtableReclaimMemory:1"), again.stalledThreadNames());
        Check reported = check(watchdog, NOW + 2 * REPORT_INTERVAL);
        assertEquals(1, reported.findings.size());
        assertFalse(reported.dumpDue);
    }

    @Test
    public void testDumpOnlyWhenAPoolNewlyStalls()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        FakePool postFlush = pool("MemtablePostFlush").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();

        assertTrue(check(watchdog, NOW).dumpDue);
        // the same stall, re-reported every interval, never dumps again
        for (int i = 1; i <= 3; i++)
        {
            Check check = check(watchdog, NOW + i * REPORT_INTERVAL);
            assertEquals("MemtableFlushWriter", only(check).poolName);
            assertFalse(check.dumpDue);
        }

        // a pool newly stalling does, the flush writer still stalled
        postFlush.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:1");
        Check check = check(watchdog, NOW + 3 * REPORT_INTERVAL + 1);
        assertEquals("MemtablePostFlush", only(check).poolName);
        assertTrue(check.dumpDue);
        // both stalled pools' threads first, and both pools named, the unreported one included
        assertEquals(List.of("MemtableFlushWriter:1", "MemtablePostFlush:1"), check.stalledThreadNames());
        assertEquals(List.of("MemtableFlushWriter", "MemtablePostFlush"), check.stalled());
    }

    @Test
    public void testDumpWhenAClockStallIsNewlyDetected()
    {
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).running(SECONDS.toNanos(250), "org.example.Stuck", "ScheduledFastTasks:1");
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(check(watchdog, NOW).dumpDue);

        // the flush writer was stamped long before the stall, so is still reported; the stall is new, so dumped
        long now = NOW + REPORT_INTERVAL;
        long lag = QUEUED_THRESHOLD + 1;
        Check stalled = check(watchdog, now, now - lag);
        assertEquals(List.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL, "MemtableFlushWriter"), poolNames(stalled));
        assertTrue(stalled.findings.get(0).isClockStall());
        assertTrue(stalled.dumpDue);

        // the same stall, an interval on: re-reported, not dumped
        Check ongoing = check(watchdog, now + REPORT_INTERVAL, now - lag);
        assertEquals(List.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL, "MemtableFlushWriter"), poolNames(ongoing));
        assertFalse(ongoing.dumpDue);
    }

    @Test
    public void testFindingSaysWhereTheDumpIs()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        ExecutorLivenessWatchdog watchdog = watchdog();

        String dumped = only(check(watchdog, NOW)).message;
        assertTrue(dumped, dumped.endsWith("; a thread dump follows on logger " + ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME));
        String notDumped = only(check(watchdog, NOW + REPORT_INTERVAL)).message;
        assertTrue(notDumped, notDumped.endsWith("; see the latest thread dump on logger " + ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME));
        assertEquals("org.apache.cassandra.concurrent.ThreadDump", ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME);
    }

    @Test
    public void testFindingSaysWhenItsDumpIsOwed()
    {
        String follows = "; a thread dump follows on logger " + ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME;
        String owed = "; a thread dump will follow on logger " + ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME +
                      " once the report interval allows, if the stall remains";
        String latest = "; see the latest thread dump on logger " + ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME;
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        FakePool postFlush = pool("MemtablePostFlush").idle();
        FakePool reclaim = pool("MemtableReclaimMemory").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();
        String first = only(check(watchdog, NOW)).message;
        assertTrue(first, first.endsWith(follows));

        // stalls within the interval of that dump, which does not show it: its dump is owed
        postFlush.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:1");
        Check stalled = check(watchdog, NOW + REPORT_INTERVAL / 2);
        assertFalse(stalled.dumpDue);
        String stalledMessage = only(stalled).message;
        assertTrue(stalledMessage, stalledMessage.endsWith(owed));

        // taken with the flush writer's next warning
        Check dumped = check(watchdog, NOW + REPORT_INTERVAL);
        assertTrue(dumped.dumpDue);
        String dumpedMessage = only(dumped).message;
        assertTrue(dumpedMessage, dumpedMessage.startsWith("Executor liveness: MemtableFlushWriter "));
        assertTrue(dumpedMessage, dumpedMessage.endsWith(follows));

        // the reclaim pool stalls after that dump, so is owed one; the post-flush pool, reported again in the same
        // check, is in that dump
        reclaim.running(RUNNING_THRESHOLD + 1, "org.example.Reclaim", "MemtableReclaimMemory:1");
        Check mixed = check(watchdog, NOW + REPORT_INTERVAL / 2 + REPORT_INTERVAL);
        assertFalse(mixed.dumpDue);
        assertEquals(List.of("MemtablePostFlush", "MemtableReclaimMemory"), poolNames(mixed));
        String postFlushMessage = mixed.findings.get(0).message;
        assertTrue(postFlushMessage, postFlushMessage.endsWith(latest));
        String reclaimMessage = mixed.findings.get(1).message;
        assertTrue(reclaimMessage, reclaimMessage.endsWith(owed));
    }

    @Test
    public void testPerPoolLimitsAreIndependent()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        FakePool b = pool("MemtablePostFlush").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();

        Check first = check(watchdog, NOW);
        assertEquals("MemtableFlushWriter", only(first).poolName);
        assertTrue(first.dumpDue);

        // another pool stalls within the interval: it is reported, but the dump is global and already taken
        b.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:1");
        Check second = check(watchdog, NOW + REPORT_INTERVAL / 2);
        assertEquals("MemtablePostFlush", only(second).poolName);
        assertFalse(second.dumpDue);

        // a's interval is over, b's is not; b's stall is in no dump yet, so one is taken now
        Check third = check(watchdog, NOW + REPORT_INTERVAL);
        assertEquals("MemtableFlushWriter", only(third).poolName);
        assertTrue(third.dumpDue);

        Check fourth = check(watchdog, NOW + REPORT_INTERVAL / 2 + REPORT_INTERVAL);
        assertEquals("MemtablePostFlush", only(fourth).poolName);
        assertFalse(fourth.dumpDue);
    }

    @Test
    public void testOwedDumpTakenOnceTheLimitAllows()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        FakePool postFlush = pool("MemtablePostFlush").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(check(watchdog, NOW).dumpDue);

        // the post-flush pool stalls within the interval of that dump, and stays stalled: no dump until it is over
        postFlush.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:1");
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL / 2).dumpDue);
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL - 1).dumpDue);

        // then dumped, with the threads stalled now first
        postFlush.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:2");
        Check owed = check(watchdog, NOW + REPORT_INTERVAL);
        assertTrue(owed.dumpDue);
        assertEquals(List.of("MemtableFlushWriter:1", "MemtablePostFlush:2"), owed.stalledThreadNames());

        // and once only
        for (long now = NOW + REPORT_INTERVAL + CHECK_INTERVAL; now <= NOW + 3 * REPORT_INTERVAL; now += CHECK_INTERVAL)
            assertFalse(check(watchdog, now).dumpDue);
    }

    @Test
    public void testOwedDumpTakenForAClockStall()
    {
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).running(SECONDS.toNanos(250), "org.example.Stuck", "ScheduledFastTasks:1");
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(check(watchdog, NOW).dumpDue);

        // the clock refresher stalls within the interval of that dump: reported, then dumped once the interval is over
        long lag = QUEUED_THRESHOLD + 1;
        long now = NOW + REPORT_INTERVAL / 2;
        Check stalled = check(watchdog, now, now - lag);
        assertTrue(stalled.findings.get(0).isClockStall());
        assertFalse(stalled.dumpDue);
        String message = stalled.findings.get(0).message;
        assertTrue(message, message.endsWith(" once the report interval allows, if the stall remains"));
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL - 1, now - lag).dumpDue);
        Check owed = check(watchdog, NOW + REPORT_INTERVAL, now - lag);
        assertTrue(owed.dumpDue);
        assertEquals(List.of("ScheduledFastTasks:1", "MemtableFlushWriter:1"), owed.stalledThreadNames());
        assertEquals(List.of("approximate clock refresher", "MemtableFlushWriter"), owed.stalled());
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL + CHECK_INTERVAL, now - lag).dumpDue);
    }

    @Test
    public void testOwedDumpDroppedWhenEverythingRecovers()
    {
        FakePool flushWriter = pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        FakePool postFlush = pool("MemtablePostFlush").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(check(watchdog, NOW).dumpDue);
        postFlush.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:1");
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL / 2).dumpDue);

        // both recover before the interval is over: nothing is left to dump
        flushWriter.idle();
        postFlush.idle();
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL / 2 + CHECK_INTERVAL).dumpDue);
        for (long now = NOW + REPORT_INTERVAL; now <= NOW + 2 * REPORT_INTERVAL; now += CHECK_INTERVAL)
            assertFalse(check(watchdog, now).dumpDue);
    }

    @Test
    public void testOwedDumpDroppedWhenItsPoolRecovers()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        FakePool postFlush = pool("MemtablePostFlush").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(check(watchdog, NOW).dumpDue);
        postFlush.running(RUNNING_THRESHOLD + 1, "org.example.PostFlush", "MemtablePostFlush:1");
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL / 2).dumpDue);

        // the post-flush pool recovers before the interval is over; the flush writer, still stalled, is in the last
        // dump, so nothing is left to dump
        postFlush.idle();
        assertFalse(check(watchdog, NOW + REPORT_INTERVAL / 2 + CHECK_INTERVAL).dumpDue);
        for (long now = NOW + REPORT_INTERVAL; now <= NOW + 2 * REPORT_INTERVAL; now += CHECK_INTERVAL)
        {
            Check check = check(watchdog, now);
            assertFalse(check.dumpDue);
            for (Finding finding : check.findings)
                assertTrue(finding.message, finding.message.endsWith("; see the latest thread dump on logger " +
                                                                     ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME));
        }
    }

    @Test
    public void testClockRefresherStall()
    {
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).running(SECONDS.toNanos(250), "org.example.Stuck", "ScheduledFastTasks:1");
        // the lag is over the smaller of the two thresholds
        long lag = QUEUED_THRESHOLD + 1;
        // queued during the stall, so their ages read as the lag, up to the tolerance, whatever they truly are: these
        // must not be reported
        long tolerance = ExecutorLivenessWatchdog.FROZEN_STAMP_TOLERANCE_NANOS;
        pool("ReadStage").queued(lag + tolerance, 10);
        pool("MutationStage").queued(lag, 10);
        ExecutorLivenessWatchdog watchdog = watchdog();

        Check check = check(watchdog, NOW, NOW - lag);
        Finding finding = only(check);
        assertTrue(finding.isClockStall());
        assertEquals(lag, finding.clockLagNanos);
        assertEquals(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL, finding.poolName);
        assertEquals("org.example.Stuck", finding.longestRunning.getTaskClassName());
        assertEquals("ScheduledFastTasks:1", finding.longestRunning.getThreadName());
        assertTrue(finding.message, finding.message.contains("approximate clock refresher stalled for 200.0s"));
        assertTrue(check.dumpDue);
        assertEquals(List.of("ScheduledFastTasks:1"), check.stalledThreadNames());

        // rate limited like a pool finding
        Check within = check(watchdog, NOW + 1, NOW + 1 - lag);
        assertTrue(within.findings.isEmpty());
        assertFalse(within.dumpDue);
    }

    @Test
    public void testTaskStuckBeforeClockStallIsReportedDuringIt()
    {
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).running(SECONDS.toNanos(250), "org.example.Stuck", "ScheduledFastTasks:1");
        // over both thresholds
        long lag = RUNNING_THRESHOLD + SECONDS.toNanos(10);
        long tolerance = ExecutorLivenessWatchdog.FROZEN_STAMP_TOLERANCE_NANOS;
        // stamped before the stall, so reading their true ages, older than the lag by more than the tolerance
        pool("MemtableReclaimMemory").running(lag + tolerance + 1, "org.example.Reclaim", "MemtableReclaimMemory:1");
        pool("MemtablePostFlush").queued(lag + SECONDS.toNanos(60), 3);
        // stamped during the stall: not reported
        pool("ReadStage").running(lag - tolerance, "org.example.Read", "ReadStage-1").queued(lag + tolerance, 10);

        Check check = check(watchdog(), NOW, NOW - lag);
        assertEquals(List.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL, "MemtableReclaimMemory", "MemtablePostFlush"),
                     poolNames(check));
        assertTrue(check.findings.get(0).isClockStall());
        assertFalse(check.findings.get(1).isClockStall());
        assertEquals("org.example.Reclaim", check.findings.get(1).longestRunning.getTaskClassName());
        assertTrue(check.dumpDue);
    }

    @Test
    public void testTaskStampedDuringClockStallIsNotReportedAfterRecovery()
    {
        long lag = QUEUED_THRESHOLD + 1;
        long frozen = NOW - lag;
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(only(check(watchdog, NOW, frozen)).isClockStall());

        // the clock recovers; a task queued and one started during the stall, at the frozen reading, are still there,
        // their ages over both thresholds
        long tolerance = ExecutorLivenessWatchdog.FROZEN_STAMP_TOLERANCE_NANOS;
        long later = NOW + SECONDS.toNanos(200);
        pool("ReadStage").running(later - frozen - tolerance, "org.example.Read", "ReadStage-1")
                         .queued(later - frozen + tolerance, 4);
        assertTrue(later - frozen - tolerance > RUNNING_THRESHOLD);
        Check check = check(watchdog, later);
        assertTrue(check.findings.toString(), check.findings.isEmpty());
        assertFalse(check.dumpDue);
    }

    @Test
    public void testNewStallAfterClockRecoveryIsReported()
    {
        long lag = QUEUED_THRESHOLD + 1;
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(only(check(watchdog, NOW, NOW - lag)).isClockStall());

        // the clock recovers at NOW; a task started 10s later stalls in its turn
        long later = NOW + SECONDS.toNanos(10) + RUNNING_THRESHOLD + 1;
        pool("ReadStage").running(RUNNING_THRESHOLD + 1, "org.example.Read", "ReadStage-1");
        assertEquals("ReadStage", only(check(watchdog, later)).poolName);
    }

    @Test
    public void testLaterClockStallKeepsTheEarlierFrozenReading()
    {
        long lag = QUEUED_THRESHOLD + 1;
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(only(check(watchdog, NOW, NOW - lag)).isClockStall());
        // no check finds the clock refreshed in between: the first stall ended by the second's frozen reading, at the
        // latest
        long next = NOW + 2 * REPORT_INTERVAL;
        assertTrue(only(check(watchdog, next, next - lag)).isClockStall());

        long later = next + SECONDS.toNanos(10);
        // started at the first stall's frozen reading: still taken for a stamp taken during that stall, so at least
        // later - (next - lag) old, under the threshold
        FakePool read = pool("ReadStage").running(later - (NOW - lag), "org.example.Read", "ReadStage-1");
        // queued at the second's: not reported
        FakePool mutation = pool("MutationStage").queued(later - (next - lag), 1);
        assertTrue(later - (next - lag) > QUEUED_THRESHOLD);
        Check check = check(watchdog, later);
        assertTrue(check.findings.toString(), check.findings.isEmpty());

        long over = next - lag + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        read.running(over - (NOW - lag), "org.example.Read", "ReadStage-1");
        mutation.queued(over - (next - lag), 1);
        Finding finding = only(check(watchdog, over));
        assertEquals("ReadStage", finding.poolName);
        assertTrue(finding.message, finding.message.contains(", running for at least 301.0s;"));
    }

    @Test
    public void testShortClockStallAfterLongOneKeepsTheLongOnesFrozenReading()
    {
        FakePool reclaim = pool("MemtableReclaimMemory").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();

        // a 6-minute stall, during which a reclaim task starts and stays blocked; the clock recovers by the next check
        long frozen = NOW - SECONDS.toNanos(360);
        reclaim.running(NOW - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(only(check(watchdog, NOW, frozen)).isClockStall());
        long recovered = NOW + CHECK_INTERVAL;
        reclaim.running(recovered - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, recovered).findings.isEmpty());

        // a minute on, the clock lags by 2 seconds, and recovers by the next check
        long hiccup = recovered + SECONDS.toNanos(60);
        reclaim.running(hiccup - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, hiccup, hiccup - SECONDS.toNanos(2)).findings.isEmpty());
        reclaim.running(hiccup + CHECK_INTERVAL - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, hiccup + CHECK_INTERVAL).findings.isEmpty());

        // the reclaim task is still taken for one started during the long stall: not reported for its inflated age
        long later = recovered + SECONDS.toNanos(200);
        reclaim.running(later - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(later - frozen > RUNNING_THRESHOLD);
        assertTrue(check(watchdog, later).findings.isEmpty());

        long over = recovered + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        reclaim.running(over - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        Finding finding = only(check(watchdog, over));
        assertTrue(finding.message, finding.message.contains(", running for at least 301.0s;"));
    }

    @Test
    public void testRememberedClockStallsAreCapped()
    {
        // a 6-minute stall, during which a reclaim task starts and stays blocked, then short stalls, each recovered
        // from by the next check: the long stall is remembered until MAX_FREEZES later ones are
        for (int shortStalls : new int[]{ ExecutorLivenessWatchdog.MAX_FREEZES - 1, ExecutorLivenessWatchdog.MAX_FREEZES })
        {
            pools.clear();
            FakePool reclaim = pool("MemtableReclaimMemory").idle();
            ExecutorLivenessWatchdog watchdog = watchdog();
            long frozen = NOW - SECONDS.toNanos(360);
            reclaim.running(NOW - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
            assertTrue(only(check(watchdog, NOW, frozen)).isClockStall());
            long recovered = NOW + CHECK_INTERVAL;
            reclaim.running(recovered - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
            assertTrue(check(watchdog, recovered).findings.isEmpty());

            List<Finding> findings = new ArrayList<>();
            for (int i = 1; i <= shortStalls; i++)
            {
                long stall = recovered + i * SECONDS.toNanos(10);
                reclaim.running(stall - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
                findings.addAll(check(watchdog, stall, stall - SECONDS.toNanos(2)).findings);
                reclaim.running(stall + CHECK_INTERVAL - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
                findings.addAll(check(watchdog, stall + CHECK_INTERVAL).findings);
            }
            long later = recovered + SECONDS.toNanos(200);
            reclaim.running(later - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
            findings.addAll(check(watchdog, later).findings);

            if (shortStalls < ExecutorLivenessWatchdog.MAX_FREEZES)
            {
                assertTrue(findings.toString(), findings.isEmpty());
            }
            else
            {
                // forgotten, so its age is taken as read
                Finding finding = findings.get(0);
                assertEquals(findings.toString(), 1, findings.size());
                assertEquals("MemtableReclaimMemory", finding.poolName);
                assertFalse(finding.message, finding.message.contains("at least"));
            }
        }
    }

    @Test
    public void testTaskStampedDuringClockStallIsReportedOnceSurelyOverThreshold()
    {
        FakePool reclaim = pool("MemtableReclaimMemory").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();

        // the clock refresher stalls for 6 minutes, over both thresholds; a reclaim task starts during the stall, at
        // the frozen reading, and stays blocked
        long frozen = NOW - SECONDS.toNanos(360);
        reclaim.running(NOW - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(only(check(watchdog, NOW, frozen)).isClockStall());

        // the clock recovers by the next check
        long recovered = NOW + CHECK_INTERVAL;
        reclaim.running(recovered - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, recovered).findings.isEmpty());

        // its age reads over the threshold, but it may have started just before the recovery
        long later = recovered + SECONDS.toNanos(200);
        reclaim.running(later - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(later - frozen > RUNNING_THRESHOLD);
        assertTrue(check(watchdog, later).findings.isEmpty());
        long atThreshold = recovered + RUNNING_THRESHOLD;
        reclaim.running(atThreshold - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, atThreshold).findings.isEmpty());

        // more than a threshold after the recovery, it has surely run that long; reported with that bound, not with
        // the age it reads
        long over = recovered + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        reclaim.running(over - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        Finding finding = only(check(watchdog, over));
        assertEquals("MemtableReclaimMemory", finding.poolName);
        assertTrue(finding.message, finding.message.contains("Longest running task: org.example.Reclaim on thread " +
                                                             "MemtableReclaimMemory:1, running for at least 301.0s;"));
        assertFalse(finding.message, finding.message.contains("666.0s"));
    }

    @Test
    public void testTaskStampedDuringShortClockStallIsNotReportedForItsInflatedAge()
    {
        FakePool reclaim = pool("MemtableReclaimMemory").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();

        // the clock refresher stalls for 2 minutes, under both thresholds, so that is not reported; a reclaim task
        // starts during the stall, at the frozen reading, and stays blocked
        long frozen = NOW - SECONDS.toNanos(120);
        reclaim.running(NOW - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, NOW, frozen).findings.isEmpty());
        long recovered = NOW + CHECK_INTERVAL;
        reclaim.running(recovered - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(check(watchdog, recovered).findings.isEmpty());

        // 4 minutes after the recovery its age reads 6 minutes, over the threshold, but it may have run for only 4
        long later = recovered + SECONDS.toNanos(240);
        reclaim.running(later - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(later - frozen > RUNNING_THRESHOLD);
        Check check = check(watchdog, later);
        assertTrue(check.findings.toString(), check.findings.isEmpty());
        assertFalse(check.dumpDue);

        long over = recovered + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        reclaim.running(over - frozen, "org.example.Reclaim", "MemtableReclaimMemory:1");
        Finding finding = only(check(watchdog, over));
        assertEquals("MemtableReclaimMemory", finding.poolName);
        assertTrue(finding.message, finding.message.contains(", running for at least 301.0s;"));
    }

    @Test
    public void testAgeOfTaskStampedDuringClockStallIsPrintedAsItsLeast()
    {
        long lag = SECONDS.toNanos(120);
        long frozen = NOW - lag;
        long tolerance = ExecutorLivenessWatchdog.FROZEN_STAMP_TOLERANCE_NANOS;
        // queued before the stall and over the threshold, while its running task started during the stall
        FakePool postFlush = pool("MemtablePostFlush").running(lag, "org.example.PostFlush", "MemtablePostFlush:1")
                                                      .queued(QUEUED_THRESHOLD + SECONDS.toNanos(60), 2);
        // running since before the stall and over the threshold, while its oldest queued task was queued during it
        FakePool flushWriter = pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + SECONDS.toNanos(60), "org.example.Flush", "MemtableFlushWriter:1")
                                                          .queued(lag + tolerance / 2, 3);
        ExecutorLivenessWatchdog watchdog = watchdog();

        // the clock still frozen, nothing is known of those ages: not printed
        Check stalled = check(watchdog, NOW, frozen);
        assertEquals(List.of("MemtablePostFlush", "MemtableFlushWriter"), poolNames(stalled));
        String postFlushMessage = stalled.findings.get(0).message;
        assertTrue(postFlushMessage, postFlushMessage.contains("Longest running task: org.example.PostFlush on thread " +
                                                               "MemtablePostFlush:1; oldest queued task waiting for 260.0s;"));
        String flushWriterMessage = stalled.findings.get(1).message;
        assertTrue(flushWriterMessage, flushWriterMessage.contains("running for 360.0s; active 1, pending 3"));
        assertFalse(flushWriterMessage, flushWriterMessage.contains("oldest queued task waiting"));

        // after the recovery, the least they can be
        long recovered = NOW + CHECK_INTERVAL;
        assertTrue(check(watchdog, recovered).findings.isEmpty());
        long later = NOW + REPORT_INTERVAL;
        long elapsed = later - NOW;
        postFlush.running(lag + elapsed, "org.example.PostFlush", "MemtablePostFlush:1")
                 .queued(QUEUED_THRESHOLD + SECONDS.toNanos(60) + elapsed, 2);
        flushWriter.running(RUNNING_THRESHOLD + SECONDS.toNanos(60) + elapsed, "org.example.Flush", "MemtableFlushWriter:1")
                   .queued(lag + tolerance / 2 + elapsed, 3);
        Check reported = check(watchdog, later);
        assertEquals(List.of("MemtablePostFlush", "MemtableFlushWriter"), poolNames(reported));
        postFlushMessage = reported.findings.get(0).message;
        assertTrue(postFlushMessage, postFlushMessage.contains("Longest running task: org.example.PostFlush on thread " +
                                                               "MemtablePostFlush:1, running for at least 595.0s; " +
                                                               "oldest queued task waiting for 860.0s;"));
        flushWriterMessage = reported.findings.get(1).message;
        assertTrue(flushWriterMessage, flushWriterMessage.contains("running for 960.0s; oldest queued task waiting for " +
                                                                   "at least 595.0s; active 1, pending 3"));
    }

    // a pool read during which the checking thread pauses for 2 seconds, and that reads the age of a reclaim task
    // stamped at the given approximate clock reading against the later time
    private static void pausesOnRead(FakePool pool, long[] clock, long stamp)
    {
        pool.onRead = () -> {
            clock[0] += SECONDS.toNanos(2);
            pool.running(clock[0] - stamp, "org.example.Reclaim", "MemtableReclaimMemory:1");
        };
    }

    @Test
    public void testFrozenStampRecognisedWhenTheCheckPausesWhileTheClockIsFrozen()
    {
        long[] clock = { NOW };
        // a reclaim task started during a 6-minute stall
        long frozen = NOW - SECONDS.toNanos(360);
        pausesOnRead(pool("MemtableReclaimMemory"), clock, frozen);

        // not known to be over the threshold
        Check stalled = check(watchdog(), () -> clock[0], frozen);
        assertEquals(List.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL), poolNames(stalled));
    }

    @Test
    public void testFrozenStampRecognisedWhenTheCheckPausesAfterTheClockRecovers()
    {
        long[] clock = { NOW };
        // a reclaim task started during a 6-minute stall, the clock recovering by the next check
        long frozen = NOW - SECONDS.toNanos(360);
        FakePool reclaim = pool("MemtableReclaimMemory").idle();
        ExecutorLivenessWatchdog watchdog = watchdog();
        assertTrue(only(check(watchdog, NOW, frozen)).isClockStall());
        long recovered = NOW + CHECK_INTERVAL;
        assertTrue(check(watchdog, recovered).findings.isEmpty());

        // not known to be over the threshold 200 seconds on
        pausesOnRead(reclaim, clock, frozen);
        clock[0] = recovered + SECONDS.toNanos(200);
        Check later = check(watchdog, () -> clock[0], clock[0]);
        assertTrue(later.findings.toString(), later.findings.isEmpty());

        // more than a threshold on, counted from the pool's reading before the pause
        clock[0] = recovered + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        Finding finding = only(check(watchdog, () -> clock[0], clock[0]));
        assertEquals("MemtableReclaimMemory", finding.poolName);
        assertTrue(finding.message, finding.message.contains(", running for at least 301.0s;"));
    }

    @Test
    public void testAgeMatchingAnOngoingAndAnEndedClockStallIsUnknown()
    {
        ExecutorLivenessWatchdog watchdog = watchdog();
        // the clock freezes, then, with no check finding it refreshed in between, freezes again 1.5 seconds on: the
        // first stall had ended by the second's reading
        long frozen = NOW - SECONDS.toNanos(100);
        long refrozen = frozen + MILLISECONDS.toNanos(1500);
        assertTrue(check(watchdog, NOW, frozen).findings.isEmpty());
        assertTrue(check(watchdog, NOW + CHECK_INTERVAL, refrozen).findings.isEmpty());

        // stamped between the two readings, within the tolerance of both; a threshold after the first stall ended,
        // but during the second, still going on
        long now = refrozen + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        pool("MemtableReclaimMemory").running(now - (frozen + MILLISECONDS.toNanos(750)), "org.example.Reclaim", "MemtableReclaimMemory:1");
        Check check = check(watchdog, now, refrozen);
        assertEquals(List.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL), poolNames(check));
    }

    @Test
    public void testAgeMatchingSeveralEndedClockStallsIsTheLeastOfTheirs()
    {
        ExecutorLivenessWatchdog watchdog = watchdog();
        // as above, and the clock then recovers 10 seconds after the second stall began
        long frozen = NOW - SECONDS.toNanos(100);
        long refrozen = frozen + MILLISECONDS.toNanos(1500);
        assertTrue(check(watchdog, NOW, frozen).findings.isEmpty());
        assertTrue(check(watchdog, NOW + CHECK_INTERVAL, refrozen).findings.isEmpty());
        long recovered = NOW + 2 * CHECK_INTERVAL;
        assertTrue(check(watchdog, recovered).findings.isEmpty());

        // stamped within the tolerance of both stalls: at least as old as the time since the later one ended
        long stamp = frozen + MILLISECONDS.toNanos(750);
        FakePool reclaim = pool("MemtableReclaimMemory");
        long later = recovered + SECONDS.toNanos(200);
        reclaim.running(later - stamp, "org.example.Reclaim", "MemtableReclaimMemory:1");
        assertTrue(later - refrozen > RUNNING_THRESHOLD);
        assertTrue(check(watchdog, later).findings.isEmpty());
        long over = recovered + RUNNING_THRESHOLD + SECONDS.toNanos(1);
        reclaim.running(over - stamp, "org.example.Reclaim", "MemtableReclaimMemory:1");
        Finding finding = only(check(watchdog, over));
        assertTrue(finding.message, finding.message.contains(", running for at least 301.0s;"));
    }

    @Test
    public void testClockRefresherRunningAgeIsAnUpperBound()
    {
        // the refresher's own task, stuck since it started, stamped at the frozen reading
        long lag = QUEUED_THRESHOLD + SECONDS.toNanos(1);
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).running(lag, "org.example.Stuck", "ScheduledFastTasks:1");
        Finding finding = only(check(watchdog(), NOW, NOW - lag));
        assertTrue(finding.isClockStall());
        assertTrue(finding.message, finding.message.contains("ScheduledFastTasks longest running task: org.example.Stuck on " +
                                                             "thread ScheduledFastTasks:1, running for up to 201.0s;"));
    }

    @Test
    public void testClockRefresherShutDown()
    {
        ScheduledExecutorPlus refresher = executorFactory().scheduled("ExecutorLivenessWatchdogTest-refresher");
        refresher.shutdownNow();
        pools.add(Pool.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL, refresher));

        Finding finding = only(check(watchdog(), NOW, NOW - RUNNING_THRESHOLD - 1));
        assertTrue(finding.isClockStall());
        assertTrue(finding.message, finding.message.contains("approximate clock not refreshed for 300.0s, as ScheduledFastTasks, " +
                                                             "which refreshes it, is shut down"));
        assertFalse(finding.message, finding.message.contains("refresher stalled"));
    }

    @Test
    public void testClockLagAtThresholdChecksPools()
    {
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).idle();
        pool("ReadStage").running(RUNNING_THRESHOLD + 1, "org.example.Read", "ReadStage-1");

        Check check = check(watchdog(), NOW, NOW - QUEUED_THRESHOLD);
        Finding finding = only(check);
        assertFalse(finding.isClockStall());
        assertEquals("ReadStage", finding.poolName);
    }

    @Test
    public void testClockRefresherStallWithoutRefresherPool()
    {
        pool("ReadStage").running(RUNNING_THRESHOLD + 1, "org.example.Read", "ReadStage-1");

        Finding finding = only(check(watchdog(), NOW, NOW - RUNNING_THRESHOLD - 1));
        assertTrue(finding.isClockStall());
        assertNull(finding.longestRunning);
    }

    @Test
    public void testThrowingPoolDoesNotStopOthers()
    {
        pool("Broken").throwOnRead = true;
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");

        Check check = check(watchdog(), NOW);
        assertEquals("MemtableFlushWriter", only(check).poolName);
        assertTrue(check.dumpDue);
    }

    @Test
    public void testOwnExecutorNeverReported()
    {
        pool(ExecutorLivenessWatchdog.NAME).running(RUNNING_THRESHOLD + 1, "org.example.Check", ExecutorLivenessWatchdog.NAME + ":1").queued(QUEUED_THRESHOLD + 1, 1);

        Check check = check(watchdog(), NOW);
        assertTrue(check.findings.isEmpty());
        assertFalse(check.dumpDue);
    }

    @Test
    public void testRegisteredPoolsIncludeScheduledExecutors()
    {
        List<String> names = new ArrayList<>();
        for (Pool pool : ExecutorLivenessWatchdog.registeredPools())
            names.add(pool.name);
        assertTrue(names.toString(), names.containsAll(List.of("ScheduledFastTasks", "ScheduledTasks", "NonPeriodicTasks", "OptionalTasks")));
    }

    @Test
    public void testPoolWithoutRunningTaskSource()
    {
        ExecutorPlus executor = nonSnapshotExecutor(RUNNING_THRESHOLD + 1, "org.example.Task");
        assertFalse(executor instanceof RunningTaskSource);
        // read directly, as the global scheduled executors, and through its metrics, as every registered pool
        pools.add(Pool.of("Direct", executor));
        pools.add(Pool.of(new ThreadPoolMetrics(executor, "internal", "ThroughMetrics")));

        Check check = check(watchdog(), NOW);
        assertEquals(List.of("Direct", "ThroughMetrics"), poolNames(check));
        for (Finding finding : check.findings)
        {
            assertEquals(RUNNING_THRESHOLD + 1, finding.longestRunning.getRunningNanos());
            assertEquals("org.example.Task", finding.longestRunning.getTaskClassName());
            assertNull(finding.longestRunning.getThreadName());
            assertTrue(finding.message, finding.message.contains("Longest running task: org.example.Task, running for 300.0s;"));
        }
        assertTrue(check.stalledThreadNames().isEmpty());
        assertTrue(check.dumpDue);

        // wrapped, as the compaction executors
        assertEquals("org.example.Task", new WrappedExecutorPlus(executor).longestRunningTask().getTaskClassName());

        // idle
        ExecutorPlus idle = nonSnapshotExecutor(0, null);
        assertNull(RunningTaskSnapshot.longestRunningTask(idle));
        assertNull(Pool.of("Idle", idle).longestRunningTask.get());
        assertNull(new ThreadPoolMetrics(idle, "internal", "IdleThroughMetrics").longestRunningTask.get());
        assertNull(new WrappedExecutorPlus(idle).longestRunningTask());
    }

    @Test
    public void testEveryBrokenPoolIsLogged()
    {
        // unique to this run, as NoSpamLogger would not log a name it logged within the report interval
        String prefix = "ExecutorLivenessWatchdogTest-broken-" + System.nanoTime() + '-';
        pool(prefix + 1).throwOnRead = true;
        pool(prefix + 2).throwOnRead = true;

        try (CapturedLog log = new CapturedLog(ExecutorLivenessWatchdog.class.getName()))
        {
            assertTrue(check(watchdog(), NOW).findings.isEmpty());
            List<String> messages = log.messages(ExecutorLivenessWatchdog.class.getName());
            assertEquals(messages.toString(),
                         List.of("Executor liveness: could not read pool " + prefix + 1,
                                 "Executor liveness: could not read pool " + prefix + 2),
                         messages);
        }
    }

    @Test
    public void testRunCheckLogsTheDumpOnItsOwnLogger()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        ExecutorLivenessWatchdog watchdog = watchdog();
        String name = ExecutorLivenessWatchdog.class.getName();

        // the dump logger is not a descendant of the watchdog's, so each sees only its own
        try (CapturedLog log = new CapturedLog(name);
             CapturedLog dumpLog = new CapturedLog(ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME))
        {
            watchdog.runCheck();
            List<String> warnings = log.messages(name);
            assertEquals(warnings.toString(), 1, warnings.size());
            assertTrue(warnings.get(0), warnings.get(0).startsWith("Executor liveness: MemtableFlushWriter looks stalled"));
            assertTrue(log.messages(ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME).isEmpty());
            List<String> dumps = dumpLog.messages(ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME);
            assertEquals(1, dumps.size());
            assertTrue(dumps.get(0).startsWith("Executor liveness thread dump ("));
            assertTrue(dumps.get(0), dumps.get(0).contains(" threads); stalled: MemtableFlushWriter\n"));
            assertTrue(dumps.get(0).contains("\n\"" + Thread.currentThread().getName() + "\" "));
        }
    }

    @Test
    public void testDumpAndWarningLoggersAreTurnedOffApart()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        String name = ExecutorLivenessWatchdog.class.getName();
        String dumpName = ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME;
        Logger watchdogLogger = (Logger) LoggerFactory.getLogger(name);
        Logger dumpLogger = (Logger) LoggerFactory.getLogger(dumpName);
        Level watchdogLevel = watchdogLogger.getLevel();
        Level dumpLevel = dumpLogger.getLevel();

        // the watchdog's logger off: the dump is still logged
        watchdogLogger.setLevel(Level.OFF);
        try (CapturedLog dumpLog = new CapturedLog(dumpName))
        {
            watchdog().runCheck();
            assertEquals(1, dumpLog.messages(dumpName).size());
        }
        finally
        {
            watchdogLogger.setLevel(watchdogLevel);
        }

        // the dump logger off: the warning is still logged, and points at no dump, as none is taken
        ExecutorLivenessWatchdog watchdog = watchdog();
        dumpLogger.setLevel(Level.OFF);
        try (CapturedLog log = new CapturedLog(name);
             CapturedLog dumpLog = new CapturedLog(dumpName))
        {
            watchdog.runCheck();
            List<String> warnings = log.messages(name);
            assertEquals(1, warnings.size());
            assertFalse(warnings.get(0), warnings.get(0).contains("thread dump"));
            assertTrue(dumpLog.messages(dumpName).isEmpty());
        }
        finally
        {
            dumpLogger.setLevel(dumpLevel);
        }

        // the dump logger on again: the stall, not dumped yet, is dumped at the next check
        try (CapturedLog log = new CapturedLog(name);
             CapturedLog dumpLog = new CapturedLog(dumpName))
        {
            watchdog.runCheck();
            assertTrue(log.messages(name).isEmpty());
            assertEquals(1, dumpLog.messages(dumpName).size());
        }
    }

    @Test
    public void testNoDumpWhenTheDumpLoggerIsOff()
    {
        pool(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL).idle();
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        ExecutorLivenessWatchdog watchdog = watchdog();
        long lag = QUEUED_THRESHOLD + 1;

        // neither due nor mentioned
        Check off = watchdog.check(() -> NOW, NOW - lag, false);
        assertFalse(off.dumpDue);
        assertEquals(List.of(ExecutorLivenessWatchdog.CLOCK_REFRESHER_POOL, "MemtableFlushWriter"), poolNames(off));
        for (Finding finding : off.findings)
            assertFalse(finding.message, finding.message.contains("thread dump"));
        assertFalse(watchdog.check(() -> NOW + CHECK_INTERVAL, NOW - lag, false).dumpDue);

        // and not recorded as taken: once the logger is on, the stalls are dumped at the next check
        Check on = check(watchdog, NOW + 2 * CHECK_INTERVAL, NOW - lag);
        assertTrue(on.findings.isEmpty());
        assertTrue(on.dumpDue);
        assertFalse(check(watchdog, NOW + 3 * CHECK_INTERVAL, NOW - lag).dumpDue);
        String latest = "; see the latest thread dump on logger " + ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME;
        Check reported = check(watchdog, NOW + REPORT_INTERVAL, NOW - lag);
        assertEquals(2, reported.findings.size());
        for (Finding finding : reported.findings)
            assertTrue(finding.message, finding.message.endsWith(latest));
    }

    @Test
    public void testDumpThatFailsIsTakenAtTheNextCheck()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        AtomicBoolean fail = new AtomicBoolean(true);
        ExecutorLivenessWatchdog watchdog = new ExecutorLivenessWatchdog(() -> pools, CHECK_INTERVAL, RUNNING_THRESHOLD, QUEUED_THRESHOLD,
                                                                         REPORT_INTERVAL, ExecutorLivenessWatchdog.excludedPools(""), check -> {
            if (fail.get())
                throw new IllegalStateException("no dump");
            return "the dump";
        });
        String name = ExecutorLivenessWatchdog.class.getName();
        String dumpName = ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME;

        try (CapturedLog log = new CapturedLog(name);
             CapturedLog dumpLog = new CapturedLog(dumpName))
        {
            watchdog.runCheck();
            List<String> messages = log.messages(name);
            assertEquals(messages.toString(), 2, messages.size());
            assertTrue(messages.get(0), messages.get(0).startsWith("Executor liveness: MemtableFlushWriter looks stalled"));
            assertEquals("Executor liveness check failed", messages.get(1));
            assertTrue(dumpLog.messages(dumpName).isEmpty());

            // not recorded as taken, so taken at the next check, within the report interval, and then not again
            fail.set(false);
            watchdog.runCheck();
            assertEquals(List.of("the dump"), dumpLog.messages(dumpName));
            watchdog.runCheck();
            assertEquals(List.of("the dump"), dumpLog.messages(dumpName));
        }
    }

    @Test
    public void testRunCheckSurvivesAThrowingPoolSource()
    {
        pool("MemtableFlushWriter").running(RUNNING_THRESHOLD + 1, "org.example.Flush", "MemtableFlushWriter:1");
        AtomicBoolean fail = new AtomicBoolean(true);
        ExecutorLivenessWatchdog watchdog = new ExecutorLivenessWatchdog(() -> {
            if (fail.get())
                throw new IllegalStateException("no pools");
            return pools;
        }, CHECK_INTERVAL, RUNNING_THRESHOLD, QUEUED_THRESHOLD, REPORT_INTERVAL, ExecutorLivenessWatchdog.excludedPools(""));
        String name = ExecutorLivenessWatchdog.class.getName();

        try (CapturedLog log = new CapturedLog(name);
             CapturedLog dumpLog = new CapturedLog(ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME))
        {
            watchdog.runCheck();
            assertEquals(List.of("Executor liveness check failed"), log.messages(name));

            // the next check runs as usual
            fail.set(false);
            watchdog.runCheck();
            List<String> messages = log.messages(name);
            assertEquals(messages.toString(), 2, messages.size());
            assertTrue(messages.get(1), messages.get(1).startsWith("Executor liveness: MemtableFlushWriter looks stalled"));
            assertEquals(1, dumpLog.messages(ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME).size());
        }
    }

    @Test
    public void testScheduledPoolReportsItsRunningTask() throws Exception
    {
        // as the global scheduled executors, which are read directly rather than through ThreadPoolMetrics
        String name = "ExecutorLivenessWatchdogTest-scheduled";
        ScheduledExecutorPlus executor = executorFactory().scheduled(name);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try
        {
            Pool pool = Pool.of(name, executor);
            assertNull(pool.longestRunningTask.get());
            executor.submit(() -> { started.countDown(); return release.await(10, SECONDS); });
            assertTrue(started.await(10, SECONDS));
            RunningTaskSnapshot running = pool.longestRunningTask.get();
            assertNotNull(running);
            assertTrue(running.getThreadName(), running.getThreadName().startsWith(name));
        }
        finally
        {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testStartAndStopAreIdempotent()
    {
        assertFalse(ExecutorLivenessWatchdog.isRunning());
        ExecutorLivenessWatchdog.stop();
        assertFalse(ExecutorLivenessWatchdog.isRunning());

        ExecutorLivenessWatchdog.start();
        assertTrue(ExecutorLivenessWatchdog.isRunning());
        ScheduledExecutorPlus executor = ExecutorLivenessWatchdog.executor();
        ExecutorLivenessWatchdog.start();
        assertSame(executor, ExecutorLivenessWatchdog.executor());

        ExecutorLivenessWatchdog.stop();
        assertFalse(ExecutorLivenessWatchdog.isRunning());
        assertTrue(executor.isShutdown());
        ExecutorLivenessWatchdog.stop();
        assertFalse(ExecutorLivenessWatchdog.isRunning());
    }

    @Test
    public void testStartDisabled()
    {
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_ENABLED, false))
        {
            ExecutorLivenessWatchdog.start();
            assertFalse(ExecutorLivenessWatchdog.isRunning());
        }
    }

    @Test
    public void testStartReadsTheProperties()
    {
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, 1234)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS, 12345)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS, 13456)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS, 65432)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS, "Foo,Bar*"))
        {
            ExecutorLivenessWatchdog.start();
            ExecutorLivenessWatchdog watchdog = ExecutorLivenessWatchdog.watchdog();
            assertNotNull(watchdog);
            assertEquals(MILLISECONDS.toNanos(1234), watchdog.checkIntervalNanos);
            assertEquals(MILLISECONDS.toNanos(12345), watchdog.runningThresholdNanos);
            assertEquals(MILLISECONDS.toNanos(13456), watchdog.queuedThresholdNanos);
            assertEquals(MILLISECONDS.toNanos(65432), watchdog.reportIntervalNanos);
            assertTrue(watchdog.excluded.test("Foo"));
            assertTrue(watchdog.excluded.test("Bar1"));
            assertFalse(watchdog.excluded.test("CompactionExecutor"));
        }
    }

    @Test
    public void testStartWithDefaults()
    {
        ExecutorLivenessWatchdog.start();
        ExecutorLivenessWatchdog watchdog = ExecutorLivenessWatchdog.watchdog();
        assertEquals(SECONDS.toNanos(5), watchdog.checkIntervalNanos);
        assertEquals(SECONDS.toNanos(300), watchdog.runningThresholdNanos);
        assertEquals(SECONDS.toNanos(300), watchdog.queuedThresholdNanos);
        assertEquals(SECONDS.toNanos(600), watchdog.reportIntervalNanos);
        assertTrue(watchdog.excluded.test("CompactionExecutor"));
    }

    @Test
    public void testInvalidConfigurationIsNotStarted()
    {
        String checkInterval = EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS.getKey();
        String running = EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS.getKey();
        String queued = EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS.getKey();
        String reportInterval = EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS.getKey();
        assertStartRefused(checkInterval + "=0 is not positive", 0, 10_000, 10_000, 60_000);
        assertStartRefused(checkInterval + "=-1 is not positive", -1, 10_000, 10_000, 60_000);
        assertStartRefused(running + "=0 is under 10000", 1, 0, 10_000, 60_000);
        assertStartRefused(running + "=9999 is under 10000", 1, 9_999, 10_000, 60_000);
        assertStartRefused(queued + "=-5 is under 10000", 1, 10_000, -5, 60_000);
        assertStartRefused(queued + "=9999 is under 10000", 1, 10_000, 9_999, 60_000);
        assertStartRefused(reportInterval + "=59999 is under 60000", 1, 10_000, 10_000, 59_999);
        assertStartRefused(reportInterval + "=60000 is under 70000", 70_000, 141_000, 141_000, 60_000);
        assertStartRefused(checkInterval + "=0 is not positive; " + running + "=0 is under 10000", 0, 0, 10_000, 60_000);
        // over 10 seconds, but not over twice the check interval and the clock tolerance
        assertStartRefused(running + "=140999 is under 141000", 70_000, 140_999, 141_000, 70_000);
        assertStartRefused(queued + "=140999 is under 141000", 70_000, 141_000, 140_999, 70_000);
        assertStartRefused(running + "=10999 is under 11000", 5_000, 10_999, 11_000, 60_000);

        // the bounds themselves are valid
        assertNull(ExecutorLivenessWatchdog.invalidConfiguration(1, 10_000, 10_000, 60_000));
        assertNull(ExecutorLivenessWatchdog.invalidConfiguration(5_000, 11_000, 11_000, 60_000));
        assertNull(ExecutorLivenessWatchdog.invalidConfiguration(70_000, 141_000, 141_000, 70_000));
    }

    private static void assertStartRefused(String expected, long checkInterval, long running, long queued, long reportInterval)
    {
        String invalid = ExecutorLivenessWatchdog.invalidConfiguration(checkInterval, running, queued, reportInterval);
        assertNotNull(invalid);
        assertTrue(invalid, invalid.startsWith(expected));
        try (WithProperties properties = new WithProperties().set(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, checkInterval)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS, running)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS, queued)
                                                             .set(EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS, reportInterval);
             CapturedLog log = new CapturedLog(ExecutorLivenessWatchdog.class.getName()))
        {
            ExecutorLivenessWatchdog.start();
            assertFalse(ExecutorLivenessWatchdog.isRunning());
            assertEquals(List.of("Executor liveness watchdog not started, as its configuration is invalid: " + invalid),
                         log.messages(ExecutorLivenessWatchdog.class.getName()));
        }
    }

    @Test
    public void testRunsOnItsOwnThread() throws Exception
    {
        ExecutorLivenessWatchdog.start();
        String threadName = ExecutorLivenessWatchdog.executor().submit(() -> Thread.currentThread().getName()).get(10, SECONDS);
        assertTrue(threadName, threadName.startsWith(ExecutorLivenessWatchdog.NAME));
        assertTrue(ExecutorLivenessWatchdog.executor().submit(() -> Thread.currentThread().isDaemon()).get(10, SECONDS));
    }
}
