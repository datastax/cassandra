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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * Warns when an executor looks stalled: its longest-running task, or its oldest queued task, is older than a threshold.
 * The warning names the pool, the task's class, its thread and its age. A stall that no thread dump shows yet is
 * followed by one dump of every thread, the stalled ones first, so the stuck task and whatever it waits for show
 * together. The dump goes to its own logger, {@value #THREAD_DUMP_LOGGER_NAME}, which inherits the root logger's
 * appenders unless logback.xml routes or disables it; while that logger is off, no dump is taken.
 * <p>
 * It only reads the executors' liveness accessors ({@link RunningTaskSource#longestRunningTask()},
 * {@link ResizableThreadPool#oldestTaskQueueTime()}), on a scheduled thread of its own; it adds nothing to any task's
 * path and never cancels or interrupts a task. Warnings are limited to one per pool per report interval. A dump is
 * wanted while some stalled pool, or a stalled clock refresher, is in no dump taken since its stall began, and is
 * taken at most once per report interval, so a stall that begins within the interval of the last dump is dumped once
 * the interval is over, if it still lasts.
 * <p>
 * Ages are stamped on the approximate clock, which a task on {@link ScheduledExecutors#scheduledFastTasks} refreshes,
 * and read against the precise clock (see {@link TimedTask}). While that refresher is stalled, the approximate clock
 * reads a frozen value F, every task stamped during the stall is stamped F, and its age reads as {@code now - F}
 * whatever its true age; a task stamped before the stall still reads its true age. When the precise clock leads the
 * approximate one by more than {@link #FROZEN_STAMP_TOLERANCE_NANOS}, a check remembers F, and reports the stalled
 * refresher if the lead is over the smaller threshold; the first check after that to find the clocks within the
 * tolerance, or the approximate clock frozen at a later reading, remembers a time R by when the stall had ended. From
 * then on, an age within the tolerance of {@code now - F} is taken for a stamp taken during the stall: its true age
 * is unknown, but at least {@code now - R}, or 0 while the stall lasts, and it is reported once that least age is over
 * the threshold, and printed as such. Other ages are reported and printed as read. The last {@link #MAX_FREEZES}
 * stalls are remembered so, each with its own F and R, as a later, shorter stall does not end the uncertainty about
 * the tasks stamped during an earlier one.
 * <p>
 * {@link org.apache.cassandra.service.CassandraDaemon} starts it; an embedder that does not go through the daemon's
 * setup can call {@link #start()} itself.
 */
public final class ExecutorLivenessWatchdog
{
    private static final Logger logger = LoggerFactory.getLogger(ExecutorLivenessWatchdog.class);

    /**
     * The logger of the thread dumps, kept apart from the warnings as a dump can run to megabytes. It is not a
     * descendant of the watchdog's own logger, so that either can be turned off without the other.
     */
    static final String THREAD_DUMP_LOGGER_NAME = "org.apache.cassandra.concurrent.ThreadDump";
    private static final Logger threadDumpLogger = LoggerFactory.getLogger(THREAD_DUMP_LOGGER_NAME);

    /** The name of the watchdog's own executor, which it never reports. */
    static final String NAME = "ExecutorLivenessWatchdog";

    /** The pool whose task refreshes the approximate clock. */
    static final String CLOCK_REFRESHER_POOL = "ScheduledFastTasks";

    /** How a thread dump's header names a stalled approximate clock refresher. */
    private static final String CLOCK_REFRESHER_STALL = "approximate clock refresher";

    /**
     * The shortest report interval accepted, so that a multi-megabyte thread dump can be logged at most once a minute.
     */
    static final long MIN_REPORT_INTERVAL_MILLIS = 60_000;

    /**
     * The shortest running or queued threshold accepted. A threshold must also be at least twice the check interval
     * plus {@link #FROZEN_STAMP_TOLERANCE_NANOS}: a refresher stall shorter than about a check interval and the
     * tolerance may be seen by no check, so the stamps taken during it are not recognised, and their ages read up to
     * that much too high; such a threshold keeps that to about half the threshold at most.
     */
    static final long MIN_THRESHOLD_MILLIS = 10_000;

    /**
     * How close a task age must be to {@code now - F}, F being the approximate clock's frozen reading during one of the
     * remembered refresher stalls, to be taken for a stamp taken during that stall. Such a stamp is F exactly, and the
     * pool reads its age against the precise clock at some {@code now} between the check's readings just before and
     * just after the pool's, which bound it; but a stamp taken just before the stall is up to one approximate clock
     * refresh interval ({@code cassandra.approximate_time_precision_ms}, 2 ms by default) before F. One second covers
     * that by orders of magnitude, while a task stamped more than a second before the stall still reads as stamped
     * before it, and stall thresholds are at least {@link #MIN_THRESHOLD_MILLIS}. It is also how far the approximate
     * clock may lag the precise one before a check takes it for frozen.
     */
    static final long FROZEN_STAMP_TOLERANCE_NANOS = SECONDS.toNanos(1);

    /**
     * How many stalls of the approximate clock refresher are remembered, the oldest forgotten first. Forgetting one
     * changes no report once {@code now - R} is over both thresholds, as an age taken for a stamp taken during it is
     * then over both whether taken so or as read; it only prints that age as read, inflated by up to the stall's
     * length. Forgetting it before then lets a task stamped during it be reported for its inflated age, as if the
     * stall had never been seen. That takes this many later stalls, each of over a second, within a threshold of the
     * forgotten one's end, on a node whose clock refresher, a 2 ms task, then barely runs.
     */
    static final int MAX_FREEZES = 8;

    // what frozenStampLeastAgeNanos returns for an age not taken for a stamp taken during a stall, and for one taken so
    // during a stall not yet found to have ended; a least age it returns otherwise is never negative
    private static final long NOT_FROZEN = -1;
    private static final long UNKNOWN_AGE = -2;

    private static ScheduledExecutorPlus executor;   // guarded by the class
    private static ExecutorLivenessWatchdog watchdog;   // guarded by the class; the one running on executor

    private final Supplier<? extends Iterable<Pool>> pools;
    final long checkIntervalNanos;
    final long runningThresholdNanos;
    final long queuedThresholdNanos;
    final long reportIntervalNanos;
    final Predicate<String> excluded;
    private final Function<Check, String> threadDump;
    private final LongSupplier approxNow;
    private final LongSupplier preciseNow;

    // rate limiting, on the precise clock and touched only by the checking thread: when each pool, and the clock
    // refresher, was last reported, and when the last dump was taken; null for never
    private final Map<String, Long> lastPoolReportNanos = new HashMap<>();
    private Long lastClockReportNanos;
    private Long lastDumpNanos;

    // touched only by the checking thread: the stalled pools, and whether the stalled clock refresher, that a dump
    // taken since their stall began shows, to dump only a stall no dump shows yet, and so that a warning says whether
    // its dump was taken; and the last MAX_FREEZES stalls of the approximate clock, oldest first
    private Set<String> dumpedPools = new HashSet<>();
    private boolean clockDumped;
    private final ArrayDeque<Freeze> freezes = new ArrayDeque<>(MAX_FREEZES);

    @VisibleForTesting
    ExecutorLivenessWatchdog(Supplier<? extends Iterable<Pool>> pools,
                             long checkIntervalNanos,
                             long runningThresholdNanos,
                             long queuedThresholdNanos,
                             long reportIntervalNanos,
                             Predicate<String> excluded)
    {
        this(pools, checkIntervalNanos, runningThresholdNanos, queuedThresholdNanos, reportIntervalNanos, excluded,
             check -> ThreadDump.dumpAllThreads(check.stalled(), check.stalledThreadNames()));
    }

    @VisibleForTesting
    ExecutorLivenessWatchdog(Supplier<? extends Iterable<Pool>> pools,
                             long checkIntervalNanos,
                             long runningThresholdNanos,
                             long queuedThresholdNanos,
                             long reportIntervalNanos,
                             Predicate<String> excluded,
                             Function<Check, String> threadDump)
    {
        this(pools, checkIntervalNanos, runningThresholdNanos, queuedThresholdNanos, reportIntervalNanos, excluded,
             threadDump, approxTime::now, preciseTime::now);
    }

    @VisibleForTesting
    ExecutorLivenessWatchdog(Supplier<? extends Iterable<Pool>> pools,
                             long checkIntervalNanos,
                             long runningThresholdNanos,
                             long queuedThresholdNanos,
                             long reportIntervalNanos,
                             Predicate<String> excluded,
                             Function<Check, String> threadDump,
                             LongSupplier approxNow,
                             LongSupplier preciseNow)
    {
        this.pools = pools;
        this.checkIntervalNanos = checkIntervalNanos;
        this.runningThresholdNanos = runningThresholdNanos;
        this.queuedThresholdNanos = queuedThresholdNanos;
        this.reportIntervalNanos = reportIntervalNanos;
        this.excluded = excluded;
        this.threadDump = threadDump;
        this.approxNow = approxNow;
        this.preciseNow = preciseNow;
    }

    /**
     * Starts the watchdog on its own executor, unless it is disabled or already running. The configuration is read
     * here, once; if it is invalid, the watchdog logs why and stays stopped.
     */
    public static synchronized void start()
    {
        if (executor != null || !EXECUTOR_LIVENESS_WATCHDOG_ENABLED.getBoolean())
            return;

        long checkIntervalMillis = EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS.getLong();
        long runningThresholdMillis = EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS.getLong();
        long queuedThresholdMillis = EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS.getLong();
        long reportIntervalMillis = EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS.getLong();
        String excludedPools = EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS.getString();
        String invalid = invalidConfiguration(checkIntervalMillis, runningThresholdMillis, queuedThresholdMillis,
                                              reportIntervalMillis);
        if (invalid != null)
        {
            logger.warn("Executor liveness watchdog not started, as its configuration is invalid: {}", invalid);
            return;
        }
        ExecutorLivenessWatchdog newWatchdog =
            new ExecutorLivenessWatchdog(ExecutorLivenessWatchdog::registeredPools,
                                         MILLISECONDS.toNanos(checkIntervalMillis),
                                         MILLISECONDS.toNanos(runningThresholdMillis),
                                         MILLISECONDS.toNanos(queuedThresholdMillis),
                                         MILLISECONDS.toNanos(reportIntervalMillis),
                                         excludedPools(excludedPools));

        // a thread of its own, as a stall of any watched pool, the clock refresher's included, must not stop the checks
        ScheduledExecutorPlus newExecutor = executorFactory().scheduled(false, NAME);
        long checkIntervalNanos = newWatchdog.checkIntervalNanos;
        newExecutor.scheduleWithFixedDelay(newWatchdog::runCheck, checkIntervalNanos, checkIntervalNanos, NANOSECONDS);
        executor = newExecutor;
        watchdog = newWatchdog;
        logger.info("Executor liveness watchdog started: checking every {}ms; running threshold {}ms, " +
                    "queued threshold {}ms, report interval {}ms; excluded pools: {}",
                    checkIntervalMillis, runningThresholdMillis, queuedThresholdMillis, reportIntervalMillis,
                    excludedPools);
    }

    /** Stops the watchdog if it is running. */
    public static synchronized void stop()
    {
        if (executor == null)
            return;
        executor.shutdownNow();
        executor = null;
        watchdog = null;
    }

    @VisibleForTesting
    static synchronized boolean isRunning()
    {
        return executor != null;
    }

    @VisibleForTesting
    static synchronized ScheduledExecutorPlus executor()
    {
        return executor;
    }

    @VisibleForTesting
    static synchronized ExecutorLivenessWatchdog watchdog()
    {
        return watchdog;
    }

    /**
     * Why the configuration is invalid, naming each invalid property and its value, or null if it is valid.
     */
    @VisibleForTesting
    static String invalidConfiguration(long checkIntervalMillis,
                                       long runningThresholdMillis,
                                       long queuedThresholdMillis,
                                       long reportIntervalMillis)
    {
        List<String> invalid = new ArrayList<>();
        if (checkIntervalMillis <= 0)
            invalid.add(notPositive(EXECUTOR_LIVENESS_WATCHDOG_CHECK_INTERVAL_MS, checkIntervalMillis));
        long toleranceMillis = NANOSECONDS.toMillis(FROZEN_STAMP_TOLERANCE_NANOS);
        long minThresholdMillis = Math.max(MIN_THRESHOLD_MILLIS, 2 * checkIntervalMillis + toleranceMillis);
        if (runningThresholdMillis < minThresholdMillis)
            invalid.add(underMinThreshold(EXECUTOR_LIVENESS_WATCHDOG_RUNNING_THRESHOLD_MS, runningThresholdMillis,
                                          minThresholdMillis, toleranceMillis));
        if (queuedThresholdMillis < minThresholdMillis)
            invalid.add(underMinThreshold(EXECUTOR_LIVENESS_WATCHDOG_QUEUED_THRESHOLD_MS, queuedThresholdMillis,
                                          minThresholdMillis, toleranceMillis));
        long minReportIntervalMillis = Math.max(checkIntervalMillis, MIN_REPORT_INTERVAL_MILLIS);
        if (reportIntervalMillis < minReportIntervalMillis)
            invalid.add(EXECUTOR_LIVENESS_WATCHDOG_REPORT_INTERVAL_MS.getKey() + '=' + reportIntervalMillis +
                        " is under " + minReportIntervalMillis + ", the larger of the check interval and " +
                        MIN_REPORT_INTERVAL_MILLIS);
        return invalid.isEmpty() ? null : String.join("; ", invalid);
    }

    private static String notPositive(CassandraRelevantProperties property, long value)
    {
        return property.getKey() + '=' + value + " is not positive";
    }

    private static String underMinThreshold(CassandraRelevantProperties property, long value, long minMillis,
                                            long toleranceMillis)
    {
        return property.getKey() + '=' + value + " is under " + minMillis + ", the larger of " + MIN_THRESHOLD_MILLIS +
               " and twice the check interval plus " + toleranceMillis;
    }

    /**
     * Every pool registered with {@link ThreadPoolMetrics}, and the global {@link ScheduledExecutors}, which are not.
     */
    @VisibleForTesting
    static List<Pool> registeredPools()
    {
        List<Pool> pools = new ArrayList<>();
        for (ThreadPoolMetrics metrics : CassandraMetricsRegistry.Metrics.allThreadPoolMetrics())
            pools.add(Pool.of(metrics));
        pools.add(Pool.of(CLOCK_REFRESHER_POOL, ScheduledExecutors.scheduledFastTasks));
        pools.add(Pool.of("ScheduledTasks", ScheduledExecutors.scheduledTasks));
        pools.add(Pool.of("NonPeriodicTasks", ScheduledExecutors.nonPeriodicTasks));
        pools.add(Pool.of("OptionalTasks", ScheduledExecutors.optionalTasks));
        return pools;
    }

    /**
     * Comma-separated pool names, trimmed; a trailing {@code *} matches every name with that prefix.
     */
    @VisibleForTesting
    static Predicate<String> excludedPools(String names)
    {
        Set<String> exact = new HashSet<>();
        List<String> prefixes = new ArrayList<>();
        for (String name : names.split(","))
        {
            name = name.trim();
            if (name.isEmpty())
                continue;
            if (name.endsWith("*"))
                prefixes.add(name.substring(0, name.length() - 1));
            else
                exact.add(name);
        }
        return pool -> {
            if (exact.contains(pool))
                return true;
            for (String prefix : prefixes)
            {
                if (pool.startsWith(prefix))
                    return true;
            }
            return false;
        };
    }

    @VisibleForTesting
    void runCheck()
    {
        try
        {
            // the approximate clock first, as check() requires
            long approxNowNanos = approxNow.getAsLong();
            Check check = check(preciseNow, approxNowNanos, threadDumpLogger.isWarnEnabled());
            for (Finding finding : check.findings)
                logger.warn(finding.message);
            if (check.dumpDue)
            {
                String dump = threadDump.apply(check);
                // recorded as taken only once logged, so that a dump that failed, or that the logger, turned off since
                // the check, did not log, is taken at the next check
                if (threadDumpLogger.isWarnEnabled())
                {
                    threadDumpLogger.warn(dump);
                    dumped(check);
                }
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Executor liveness check failed", t);
        }
    }

    /**
     * One check: the stalls to report now, and whether a thread dump is due. It logs nothing, but records what it
     * returns as reported, for the rate limits; a dump it makes due is recorded as taken by {@link #dumped(Check)}.
     *
     * @param preciseClock   the precise clock, read at the start of the check, and just before and just after the
     *                       readings of each pool, against which the pool reads its ages
     * @param approxNowNanos the approximate clock's time, read before the precise clock is first, so that the lag is
     *                       never negative, and a check that finds the clock refreshed again reads its own time, which
     *                       it remembers as by when the stall had ended, after that refresh
     * @param dumpEnabled    whether a thread dump would be logged; if not, none is due, and no warning mentions one
     */
    @VisibleForTesting
    Check check(LongSupplier preciseClock, long approxNowNanos, boolean dumpEnabled)
    {
        long nowNanos = preciseClock.getAsLong();
        lastPoolReportNanos.values().removeIf(last -> isDue(last, nowNanos));

        long clockLagNanos = nowNanos - approxNowNanos;
        recordFreeze(nowNanos, approxNowNanos);

        Iterable<Pool> pools = this.pools.get();
        List<Finding> findings = new ArrayList<>();
        List<Finding> stalled = new ArrayList<>();   // reported or not, for the dump's header and first threads
        boolean clockStall = clockLagNanos > Math.min(runningThresholdNanos, queuedThresholdNanos);
        if (clockStall)
        {
            Finding finding = clockRefresherFinding(pools, preciseClock, clockLagNanos);
            stalled.add(finding);
            if (isDue(lastClockReportNanos, nowNanos))
            {
                lastClockReportNanos = nowNanos;
                findings.add(finding);
            }
        }

        Set<String> stalledPoolNames = new HashSet<>();
        for (Pool pool : pools)
        {
            // the watchdog's own executor is not registered, so not among the pools; skipped should that change
            if (pool.name.equals(NAME) || excluded.test(pool.name))
                continue;
            Finding finding = poolFinding(pool, preciseClock);
            if (finding == null)
                continue;
            stalledPoolNames.add(pool.name);
            stalled.add(finding);
            if (!lastPoolReportNanos.containsKey(pool.name))
            {
                lastPoolReportNanos.put(pool.name, nowNanos);
                findings.add(finding);
            }
        }

        // a pool that recovers leaves the dumped ones, so that it is dumped again if it stalls again
        dumpedPools.retainAll(stalledPoolNames);
        clockDumped &= clockStall;
        boolean dumpWanted = (clockStall && !clockDumped) || !dumpedPools.containsAll(stalledPoolNames);
        boolean dumpDue = dumpEnabled && dumpWanted && isDue(lastDumpNanos, nowNanos);
        List<Finding> reported = findings;
        if (dumpEnabled)
        {
            reported = new ArrayList<>(findings.size());
            for (Finding finding : findings)
            {
                boolean dumped = finding.clockStall ? clockDumped : dumpedPools.contains(finding.poolName);
                reported.add(finding.withDumpNote(dumpDue, dumped));
            }
        }
        return new Check(nowNanos, reported, dumpDue, stalledPoolNames, clockStall, stalledNames(stalled),
                         stalledThreadNames(stalled));
    }

    /** Records the dump the check made due as taken, showing the stalls that check found. */
    @VisibleForTesting
    void dumped(Check check)
    {
        lastDumpNanos = check.nowNanos;
        dumpedPools = check.stalledPoolNames;
        clockDumped = check.clockStall;
    }

    @VisibleForTesting
    int reportedPoolCount() { return lastPoolReportNanos.size(); }

    @VisibleForTesting
    Set<String> dumpedPools() { return Collections.unmodifiableSet(dumpedPools); }

    @VisibleForTesting
    int freezeCount() { return freezes.size(); }

    private boolean isDue(Long lastNanos, long nowNanos)
    {
        return lastNanos == null || nowNanos - lastNanos >= reportIntervalNanos;
    }

    // remembers a new stall of the approximate clock refresher, and when the last one was found to have ended
    private void recordFreeze(long nowNanos, long approxNowNanos)
    {
        Freeze last = freezes.peekLast();
        boolean lastOngoing = last != null && last.recoveredNanos == null;
        if (nowNanos - approxNowNanos <= FROZEN_STAMP_TOLERANCE_NANOS)
        {
            if (lastOngoing)
                last.recoveredNanos = nowNanos;
            return;
        }
        if (lastOngoing && last.frozenApproxNanos == approxNowNanos)
            return;   // the same stall, still going on
        // a reading is the precise clock's time when it was taken, so the clock had moved on from the last stall's by
        // this reading's time
        if (lastOngoing)
            last.recoveredNanos = approxNowNanos;
        if (freezes.size() == MAX_FREEZES)
            freezes.removeFirst();
        freezes.addLast(new Freeze(approxNowNanos));
    }

    // for an age read against the precise clock at some time between beforeNanos and afterNanos, within the tolerance
    // of that time - F of a remembered stall, so maybe of a stamp taken during it: the least its true age can be, the
    // time since the stall was found to have ended, the least of these if several; UNKNOWN_AGE if one has not; and
    // NOT_FROZEN for any other age
    private long frozenStampLeastAgeNanos(long ageNanos, long beforeNanos, long afterNanos)
    {
        long least = NOT_FROZEN;
        for (Freeze freeze : freezes)
        {
            if (ageNanos < beforeNanos - freeze.frozenApproxNanos - FROZEN_STAMP_TOLERANCE_NANOS
                || ageNanos > afterNanos - freeze.frozenApproxNanos + FROZEN_STAMP_TOLERANCE_NANOS)
                continue;
            if (freeze.recoveredNanos == null)
                return UNKNOWN_AGE;
            long sinceRecovered = beforeNanos - freeze.recoveredNanos;
            least = least == NOT_FROZEN ? sinceRecovered : Math.min(least, sinceRecovered);
        }
        return least;
    }

    // the least a task's true age can be: its age as read, or, for a stamp taken while the approximate clock was
    // frozen, the time since the clock was found refreshed again, or 0 while it is not
    private long leastAgeNanos(long ageNanos, long beforeNanos, long afterNanos)
    {
        long least = frozenStampLeastAgeNanos(ageNanos, beforeNanos, afterNanos);
        return least == NOT_FROZEN ? ageNanos : least == UNKNOWN_AGE ? 0 : least;
    }

    // a task's age to print: as read, or, for a stamp taken while the approximate clock was frozen, the least it can
    // be, or null if nothing is known of it yet
    private String age(long ageNanos, long beforeNanos, long afterNanos)
    {
        long least = frozenStampLeastAgeNanos(ageNanos, beforeNanos, afterNanos);
        return least == NOT_FROZEN ? seconds(ageNanos) : least == UNKNOWN_AGE ? null : "at least " + seconds(least);
    }

    // the pool's finding if it is over a threshold, else null; a pool that cannot be read is skipped
    private Finding poolFinding(Pool pool, LongSupplier preciseClock)
    {
        try
        {
            long beforeNanos = preciseClock.getAsLong();
            RunningTaskSnapshot running = pool.longestRunningTask.get();
            long queuedNanos = pool.oldestTaskQueueTime.getAsLong();
            long afterNanos = preciseClock.getAsLong();
            boolean runningOver = running != null
                                  && leastAgeNanos(running.getRunningNanos(), beforeNanos, afterNanos)
                                     > runningThresholdNanos;
            boolean queuedOver = leastAgeNanos(queuedNanos, beforeNanos, afterNanos) > queuedThresholdNanos;
            if (!runningOver && !queuedOver)
                return null;

            int active = pool.activeTasks.getAsInt();
            int pending = pool.pendingTasks.getAsInt();
            StringBuilder message = new StringBuilder("Executor liveness: ").append(pool.name)
                                    .append(" looks stalled: ");
            if (runningOver)
                message.append("its longest-running task is over the running threshold of ")
                       .append(seconds(runningThresholdNanos));
            if (runningOver && queuedOver)
                message.append(", and ");
            if (queuedOver)
                message.append("its oldest queued task is over the queued threshold of ")
                       .append(seconds(queuedThresholdNanos));
            message.append(". Longest running task: ");
            appendRunning(message, running,
                          running == null ? null : age(running.getRunningNanos(), beforeNanos, afterNanos));
            String queuedAge = queuedNanos == 0 ? null : age(queuedNanos, beforeNanos, afterNanos);   // 0: none queued
            if (queuedAge != null)
                message.append("; oldest queued task waiting for ").append(queuedAge);
            message.append("; active ").append(active).append(", pending ").append(pending);
            return new Finding(pool.name, running, queuedNanos, active, pending, false, 0L, message.toString());
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logCouldNotRead(pool, t);
            return null;
        }
    }

    private Finding clockRefresherFinding(Iterable<Pool> pools, LongSupplier preciseClock, long clockLagNanos)
    {
        RunningTaskSnapshot running = null;
        String runningAge = null;
        long queuedNanos = 0;
        int active = 0;
        int pending = 0;
        boolean shutdown = false;
        for (Pool pool : pools)
        {
            if (!pool.name.equals(CLOCK_REFRESHER_POOL))
                continue;
            try
            {
                long beforeNanos = preciseClock.getAsLong();
                running = pool.longestRunningTask.get();
                long afterNanos = preciseClock.getAsLong();
                if (running != null)
                    runningAge = refresherAge(running.getRunningNanos(), beforeNanos, afterNanos);
                queuedNanos = pool.oldestTaskQueueTime.getAsLong();
                active = pool.activeTasks.getAsInt();
                pending = pool.pendingTasks.getAsInt();
                shutdown = pool.isShutdown.getAsBoolean();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logCouldNotRead(pool, t);
            }
            break;
        }

        StringBuilder message = new StringBuilder("Executor liveness: ");
        if (shutdown)
            message.append("approximate clock not refreshed for ").append(seconds(clockLagNanos))
                   .append(", as ").append(CLOCK_REFRESHER_POOL).append(", which refreshes it, is shut down");
        else
            message.append("approximate clock refresher stalled for ").append(seconds(clockLagNanos));
        message.append(", over the threshold of ")
               .append(seconds(Math.min(runningThresholdNanos, queuedThresholdNanos)))
               .append("; a task queued or started since then reads as old as that whatever its true age, so is ")
               .append("reported only once it has been queued or running for its threshold after the clock is ")
               .append("refreshed again. ").append(CLOCK_REFRESHER_POOL).append(" longest running task: ");
        appendRunning(message, running, runningAge);
        message.append("; active ").append(active).append(", pending ").append(pending);
        return new Finding(CLOCK_REFRESHER_POOL, running, queuedNanos, active, pending, true, clockLagNanos,
                           message.toString());
    }

    // the age to print of the clock refresher pool's running task, likely the stuck refresher itself, stamped at the
    // frozen reading: as age() prints it, but while the stall lasts, as at most its age as read, as a stamp never
    // leads the time it was taken
    private String refresherAge(long ageNanos, long beforeNanos, long afterNanos)
    {
        long least = frozenStampLeastAgeNanos(ageNanos, beforeNanos, afterNanos);
        return least == UNKNOWN_AGE ? "up to " + seconds(ageNanos) : age(ageNanos, beforeNanos, afterNanos);
    }

    // keyed by the pool, so that one broken pool does not hide another
    private void logCouldNotRead(Pool pool, Throwable t)
    {
        String message = "Executor liveness: could not read pool {}";
        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, message + ' ' + pool.name, reportIntervalNanos, NANOSECONDS,
                         message, pool.name, t);
    }

    // age is the running task's age to print, or null to print none
    private static void appendRunning(StringBuilder message, RunningTaskSnapshot running, String age)
    {
        if (running == null)
        {
            message.append("none");
            return;
        }
        message.append(running.getTaskClassName());
        if (running.getThreadName() != null)   // not known for a pool that is not a RunningTaskSource
            message.append(" on thread ").append(running.getThreadName());
        if (age != null)
            message.append(", running for ").append(age);
    }

    // what is stalled, for the dump's header
    private static List<String> stalledNames(List<Finding> stalled)
    {
        List<String> names = new ArrayList<>(stalled.size());
        for (Finding finding : stalled)
            names.add(finding.clockStall ? CLOCK_REFRESHER_STALL : finding.poolName);
        return names;
    }

    // the threads running the stalled pools' tasks, to list first in the dump
    private static List<String> stalledThreadNames(List<Finding> stalled)
    {
        List<String> names = new ArrayList<>(stalled.size());
        for (Finding finding : stalled)
        {
            if (finding.longestRunning != null && finding.longestRunning.getThreadName() != null)
                names.add(finding.longestRunning.getThreadName());
        }
        return names;
    }

    // seconds to one decimal place, independent of the locale
    private static String seconds(long nanos)
    {
        long tenths = NANOSECONDS.toMillis(nanos) / 100;
        return tenths / 10 + "." + tenths % 10 + 's';
    }

    /**
     * A stall of the approximate clock refresher: the approximate clock's frozen reading, and a precise clock time by
     * when the stall had ended, null until known.
     */
    private static final class Freeze
    {
        final long frozenApproxNanos;
        Long recoveredNanos;

        Freeze(long frozenApproxNanos)
        {
            this.frozenApproxNanos = frozenApproxNanos;
        }
    }

    /** A pool to watch: its name and its liveness readings. */
    static final class Pool
    {
        final String name;
        final LongSupplier oldestTaskQueueTime;
        final Supplier<RunningTaskSnapshot> longestRunningTask;
        final IntSupplier activeTasks;
        final IntSupplier pendingTasks;
        final BooleanSupplier isShutdown;

        private Pool(String name,
                     LongSupplier oldestTaskQueueTime,
                     Supplier<RunningTaskSnapshot> longestRunningTask,
                     IntSupplier activeTasks,
                     IntSupplier pendingTasks,
                     BooleanSupplier isShutdown)
        {
            this.name = name;
            this.oldestTaskQueueTime = oldestTaskQueueTime;
            this.longestRunningTask = longestRunningTask;
            this.activeTasks = activeTasks;
            this.pendingTasks = pendingTasks;
            this.isShutdown = isShutdown;
        }

        static Pool of(ThreadPoolMetrics metrics)
        {
            return new Pool(metrics.poolName, metrics.oldestTaskQueueTime::getValue, metrics.longestRunningTask,
                            metrics.activeTasks::getValue, metrics.pendingTasks::getValue, () -> false);
        }

        static Pool of(String name, ResizableThreadPool executor)
        {
            BooleanSupplier isShutdown = executor instanceof ExecutorService ? ((ExecutorService) executor)::isShutdown
                                                                             : () -> false;
            return new Pool(name, executor::oldestTaskQueueTime, () -> RunningTaskSnapshot.longestRunningTask(executor),
                            executor::getActiveTaskCount, executor::getPendingTaskCount, isShutdown);
        }
    }

    /** A pool over a threshold, or a stalled approximate clock refresher. */
    static final class Finding
    {
        final String poolName;
        final RunningTaskSnapshot longestRunning;   // null when nothing is running
        final long oldestQueuedNanos;
        final int activeTasks;
        final int pendingTasks;
        final boolean clockStall;
        final long clockLagNanos;   // 0 unless a clock stall
        final String message;

        private Finding(String poolName,
                        RunningTaskSnapshot longestRunning,
                        long oldestQueuedNanos,
                        int activeTasks,
                        int pendingTasks,
                        boolean clockStall,
                        long clockLagNanos,
                        String message)
        {
            this.poolName = poolName;
            this.longestRunning = longestRunning;
            this.oldestQueuedNanos = oldestQueuedNanos;
            this.activeTasks = activeTasks;
            this.pendingTasks = pendingTasks;
            this.clockStall = clockStall;
            this.clockLagNanos = clockLagNanos;
            this.message = message;
        }

        boolean isClockStall()
        {
            return clockStall;
        }

        // this finding, its message saying where the thread dump that shows its stall is: taken now, taken earlier in
        // the stall, or not yet, and so to be taken once the report interval allows
        private Finding withDumpNote(boolean dumpDue, boolean dumped)
        {
            String note = dumpDue ? "; a thread dump follows on logger " + THREAD_DUMP_LOGGER_NAME
                          : dumped ? "; see the latest thread dump on logger " + THREAD_DUMP_LOGGER_NAME
                          : "; a thread dump will follow on logger " + THREAD_DUMP_LOGGER_NAME +
                            " once the report interval allows, if the stall remains";
            return new Finding(poolName, longestRunning, oldestQueuedNanos, activeTasks, pendingTasks, clockStall,
                               clockLagNanos, message + note);
        }

        @Override
        public String toString()
        {
            return message;
        }
    }

    /** The result of one check. */
    static final class Check
    {
        private final long nowNanos;
        final List<Finding> findings;
        final boolean dumpDue;
        private final Set<String> stalledPoolNames;
        private final boolean clockStall;
        private final List<String> stalled;
        private final List<String> stalledThreadNames;

        private Check(long nowNanos,
                      List<Finding> findings,
                      boolean dumpDue,
                      Set<String> stalledPoolNames,
                      boolean clockStall,
                      List<String> stalled,
                      List<String> stalledThreadNames)
        {
            this.nowNanos = nowNanos;
            this.findings = findings;
            this.dumpDue = dumpDue;
            this.stalledPoolNames = stalledPoolNames;
            this.clockStall = clockStall;
            this.stalled = stalled;
            this.stalledThreadNames = stalledThreadNames;
        }

        /** The threads running the stalled pools' tasks, reported in this check or not, to list first in the dump. */
        List<String> stalledThreadNames()
        {
            return stalledThreadNames;
        }

        /**
         * What is stalled, reported in this check or not, for the dump's header: the clock refresher, then the pools.
         */
        List<String> stalled()
        {
            return stalled;
        }
    }
}
