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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Check;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Finding;
import org.apache.cassandra.concurrent.ExecutorLivenessWatchdog.Pool;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * What {@link ExecutorLivenessWatchdog#runCheck()} adds to {@link ExecutorLivenessWatchdog#check}: the order in which
 * it reads the two clocks, and a thread dump logger turned off between the check and the dump.
 */
public class ExecutorLivenessWatchdogRunCheckTest
{
    private static final long CHECK_INTERVAL = SECONDS.toNanos(5);
    private static final long RUNNING_THRESHOLD = SECONDS.toNanos(300);
    private static final long QUEUED_THRESHOLD = SECONDS.toNanos(200);
    private static final long REPORT_INTERVAL = SECONDS.toNanos(600);
    // an arbitrary precise-clock reading, far from 0 and from overflow
    private static final long NOW = SECONDS.toNanos(1_000_000);

    private static final String APPROX = "approx";
    private static final String PRECISE = "precise";

    private final List<Pool> pools = new ArrayList<>();

    /**
     * The two clocks, set by the test, recording each read; a refresh of the approximate clock can be made to land
     * right after the next read of either.
     */
    private static final class Clocks
    {
        final List<String> reads = new ArrayList<>();
        long preciseNanos;
        long approxNanos;
        private Runnable afterNextRead;

        long approx()
        {
            reads.add(APPROX);
            long now = approxNanos;
            afterRead();
            return now;
        }

        long precise()
        {
            reads.add(PRECISE);
            long now = preciseNanos;
            afterRead();
            return now;
        }

        // the approximate clock in step with the precise one
        void inStep(long nowNanos)
        {
            preciseNanos = nowNanos;
            approxNanos = nowNanos;
        }

        // the approximate clock refreshed, at the precise clock's time refreshNanos, right after the next read
        void refreshAfterNextRead(long refreshNanos)
        {
            afterNextRead = () -> inStep(refreshNanos);
        }

        private void afterRead()
        {
            Runnable refresh = afterNextRead;
            afterNextRead = null;
            if (refresh != null)
                refresh.run();
        }
    }

    /** A pool whose liveness readings are set by the test. */
    private static final class FakePool implements ResizableThreadPool, RunningTaskSource
    {
        LongSupplier oldestQueuedNanos = () -> 0;
        RunningTaskSnapshot longestRunning;

        public int getCorePoolSize() { return 1; }
        public void setCorePoolSize(int newCorePoolSize) {}
        public int getMaximumPoolSize() { return 1; }
        public void setMaximumPoolSize(int newMaximumPoolSize) {}
        public int getActiveTaskCount() { return longestRunning == null ? 0 : 1; }
        public long getCompletedTaskCount() { return 0; }
        public int getPendingTaskCount() { return 0; }

        @Override
        public long oldestTaskQueueTime()
        {
            return oldestQueuedNanos.getAsLong();
        }

        @Override
        public RunningTaskSnapshot longestRunningTask()
        {
            return longestRunning;
        }
    }

    private FakePool pool(String name)
    {
        FakePool pool = new FakePool();
        pools.add(Pool.of(name, pool));
        return pool;
    }

    private ExecutorLivenessWatchdog watchdog(Clocks clocks, Function<Check, String> threadDump)
    {
        return new ExecutorLivenessWatchdog(() -> pools, CHECK_INTERVAL, RUNNING_THRESHOLD, QUEUED_THRESHOLD,
                                            REPORT_INTERVAL, ExecutorLivenessWatchdog.excludedPools(""), threadDump,
                                            clocks::approx, clocks::precise);
    }

    // the thread dump logger on, and a dump due taken, as runCheck() does
    private static Check check(ExecutorLivenessWatchdog watchdog, Clocks clocks)
    {
        Check check = watchdog.check(clocks::precise, clocks.approx(), true);
        if (check.dumpDue)
            watchdog.dumped(check);
        return check;
    }

    @Test
    public void testApproximateClockIsReadFirst()
    {
        pool("ReadStage");
        Clocks clocks = new Clocks();
        clocks.inStep(NOW);

        watchdog(clocks, check -> "the dump").runCheck();

        // once, before the precise clock is read at all; the precise clock then at the start of the check, and just
        // before and just after the pool's readings
        assertEquals(List.of(APPROX, PRECISE, PRECISE, PRECISE), clocks.reads);
    }

    @Test
    public void testRefreshBetweenTheClockReadsDoesNotEndTheFreezeEarly()
    {
        // a task queued while the approximate clock was frozen at F, so stamped F: its age reads as now - F
        long frozen = NOW - SECONDS.toNanos(30);
        Clocks clocks = new Clocks();
        pool("ReadStage").oldestQueuedNanos = () -> clocks.preciseNanos - frozen;
        ExecutorLivenessWatchdog watchdog = watchdog(clocks, check -> "the dump");

        // the stall is seen
        clocks.preciseNanos = NOW;
        clocks.approxNanos = frozen;
        assertTrue(check(watchdog, clocks).findings.isEmpty());

        // the clock refreshed at T, between the check's first clock read and its second, as when the checking thread
        // is paused there: read first, the approximate clock still reads F, so the stall is not taken to have ended
        // yet; read second, it would be taken to have ended by the precise clock's earlier reading, before T
        long refreshed = NOW + SECONDS.toNanos(15);
        clocks.preciseNanos = NOW + CHECK_INTERVAL;
        clocks.refreshAfterNextRead(refreshed);
        watchdog.runCheck();
        assertEquals(refreshed, clocks.approxNanos);

        // the next check finds the clocks in step, so the stall ended by its time, R
        long recovered = refreshed + CHECK_INTERVAL;
        clocks.inStep(recovered);
        assertTrue(check(watchdog, clocks).findings.isEmpty());

        // the task was stamped no later than T, so its true age is at least now - T, but may be no more: under the
        // threshold, it is not reported
        clocks.inStep(refreshed + QUEUED_THRESHOLD - SECONDS.toNanos(5));
        Check underThreshold = check(watchdog, clocks);
        assertTrue(underThreshold.findings.toString(), underThreshold.findings.isEmpty());

        // once now - R is over it, it is, as at least now - R, never more than now - T
        clocks.inStep(recovered + QUEUED_THRESHOLD + SECONDS.toNanos(1));
        Check overThreshold = check(watchdog, clocks);
        assertEquals(overThreshold.findings.toString(), 1, overThreshold.findings.size());
        Finding finding = overThreshold.findings.get(0);
        assertEquals("ReadStage", finding.poolName);
        assertTrue(finding.message, finding.message.contains("201.0s"));
    }

    @Test
    public void testDumpLoggerTurnedOffBeforeTheDumpIsLogged()
    {
        pool("MemtableFlushWriter").longestRunning = new RunningTaskSnapshot(RUNNING_THRESHOLD + 1, "org.example.Flush",
                                                                             "MemtableFlushWriter:1");
        Clocks clocks = new Clocks();
        clocks.inStep(NOW);
        Logger dumpLogger = (Logger) LoggerFactory.getLogger(ExecutorLivenessWatchdog.THREAD_DUMP_LOGGER_NAME);
        Level dumpLevel = dumpLogger.getLevel();
        AtomicInteger dumpsTaken = new AtomicInteger();
        // the logger turned off while the first dump is taken, after the check found it on and the dump due
        ExecutorLivenessWatchdog watchdog = watchdog(clocks, check -> {
            if (dumpsTaken.incrementAndGet() == 1)
                dumpLogger.setLevel(Level.OFF);
            return "the dump";
        });
        ListAppender<ILoggingEvent> dumpLog = new ListAppender<>();
        dumpLog.start();
        dumpLogger.addAppender(dumpLog);
        try
        {
            watchdog.runCheck();
            assertEquals(1, dumpsTaken.get());
            assertTrue(dumpLog.list.isEmpty());

            // not recorded as taken, so still due
            dumpLogger.setLevel(dumpLevel);
            clocks.inStep(NOW + CHECK_INTERVAL);
            Check check = watchdog.check(clocks::precise, clocks.approx(), true);
            assertTrue(check.dumpDue);

            // and taken at the next check, within the report interval, and then not again
            clocks.inStep(NOW + 2 * CHECK_INTERVAL);
            watchdog.runCheck();
            assertEquals(2, dumpsTaken.get());
            assertEquals(1, dumpLog.list.size());
            assertEquals("the dump", dumpLog.list.get(0).getFormattedMessage());
            clocks.inStep(NOW + 3 * CHECK_INTERVAL);
            watchdog.runCheck();
            assertEquals(2, dumpsTaken.get());
            assertFalse(watchdog.check(clocks::precise, clocks.approx(), true).dumpDue);
        }
        finally
        {
            dumpLogger.setLevel(dumpLevel);
            dumpLogger.detachAppender(dumpLog);
            dumpLog.stop();
        }
    }
}
