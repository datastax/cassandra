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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * Cost of one {@link ExecutorLivenessWatchdog} check, without the logging, over the pools a node registers: 10 shared
 * (SEP) and 21 thread pool executors registered with ThreadPoolMetrics, 7 of them excluded by default, plus the 4
 * global scheduled executors, read through {@link ExecutorLivenessWatchdog#registeredPools()} as on a node.
 * <ul>
 * <li>{@code idle}: no task runs.</li>
 * <li>{@code busy}: 4 SEP and 4 thread pool executors each run a task, and one of those has a task queued behind it,
 * all under the default thresholds: no finding, the steady state of a node.</li>
 * <li>{@code stalled}: as busy, but every such task is over the thresholds, and was reported and dumped by an earlier
 * check, so this one reports nothing: the state for the report interval after a stall is reported.</li>
 * <li>{@code reporting}: as stalled, but with no report interval, so every check reports every stalled pool and asks
 * for a dump: the cost of the check that reports a stall.</li>
 * </ul>
 *
 * Run in-process (forked JMH cannot reach the host on this machine):
 *   jmh-profile.sh {@code <repo>} ExecutorLivenessWatchdogBench -- -prof gc
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(0)
@Threads(1)
@State(Scope.Benchmark)
public class ExecutorLivenessWatchdogBench
{
    private static final String[] SEP_POOLS = { "ReadStage", "MutationStage", "Native-Transport-Requests",
                                                "RequestResponseStage", "CounterMutationStage", "ViewMutationStage",
                                                "Native-Transport-Auth-Requests", "PaxosRepairStage",
                                                "ReadRepairStage", "BackgroundIOStage" };
    private static final String[] THREAD_POOLS = { "MemtableFlushWriter", "MemtablePostFlush", "GossipStage",
                                                   "MiscStage", "MemtableReclaimMemory", "PerDiskMemtableFlushWriter_0",
                                                   "AntiEntropyStage", "MigrationStage", "InternalResponseStage",
                                                   "TracingStage", "PendingRangeCalculator", "Sampler", "Repair-Task",
                                                   "CacheReloadExecutor", "CompactionExecutor", "ValidationExecutor",
                                                   "ViewBuildExecutor", "CacheCleanupExecutor", "SecondaryIndexExecutor",
                                                   "SecondaryIndexManagement", "HintsDispatcher" };
    private static final int RUNNING_SEP_POOLS = 4;
    private static final int RUNNING_THREAD_POOLS = 4;

    @Param({ "idle", "busy", "stalled", "reporting" })
    public String scenario;

    private SharedExecutorPool sharedPool;
    private final List<ExecutorPlus> executors = new ArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private ExecutorLivenessWatchdog watchdog;

    @Setup
    public void setup() throws InterruptedException
    {
        DatabaseDescriptor.daemonInitialization();
        sharedPool = new SharedExecutorPool("BenchSharedPool");
        for (String name : SEP_POOLS)
            executors.add(sharedPool.newExecutor(4, "internal", name));
        for (String name : THREAD_POOLS)
            executors.add(executorFactory().withJmxInternal().pooled(name, "GossipStage".equals(name) ? 1 : 4));

        if (!"idle".equals(scenario))
        {
            for (int i = 0; i < RUNNING_SEP_POOLS; i++)
                executors.get(i).execute(this::block);
            for (int i = 0; i < RUNNING_THREAD_POOLS; i++)
                executors.get(SEP_POOLS.length + i).execute(this::block);
            // a task queued behind GossipStage's single thread
            executors.get(SEP_POOLS.length + 2).execute(() -> {});
            for (ExecutorPlus executor : executors)
            {
                while (executor.getActiveTaskCount() > 0 && executor.longestRunningTaskTime() == 0)
                    Thread.sleep(1);
            }
            Thread.sleep(50);
        }

        boolean stalled = "stalled".equals(scenario) || "reporting".equals(scenario);
        long thresholdNanos = stalled ? MILLISECONDS.toNanos(10) : SECONDS.toNanos(300);
        long reportIntervalNanos = "reporting".equals(scenario) ? 0 : "stalled".equals(scenario) ? HOURS.toNanos(1)
                                                                                                    : SECONDS.toNanos(600);
        watchdog = new ExecutorLivenessWatchdog(ExecutorLivenessWatchdog::registeredPools, SECONDS.toNanos(5),
                                                thresholdNanos, thresholdNanos, reportIntervalNanos,
                                                ExecutorLivenessWatchdog.excludedPools(EXECUTOR_LIVENESS_WATCHDOG_EXCLUDED_POOLS.getDefaultValue()));
        ExecutorLivenessWatchdog.Check first = check();
        if ("stalled".equals(scenario))
            watchdog.dumped(first);
        System.out.printf("%n%s: %d pools, %d stalled, %d reported%n", scenario,
                          ExecutorLivenessWatchdog.registeredPools().size(), first.stalled().size(), check().findings.size());
    }

    private void block()
    {
        try
        {
            release.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @TearDown
    public void tearDown() throws Exception
    {
        release.countDown();
        for (ExecutorPlus executor : executors)
            executor.shutdownNow();
        sharedPool.shutdownAndWait(1, TimeUnit.MINUTES);
    }

    // as ExecutorLivenessWatchdog.runCheck(), without logging the findings or taking the dump
    @Benchmark
    public ExecutorLivenessWatchdog.Check check()
    {
        long approxNowNanos = approxTime.now();
        return watchdog.check(preciseTime::now, approxNowNanos, true);
    }
}
