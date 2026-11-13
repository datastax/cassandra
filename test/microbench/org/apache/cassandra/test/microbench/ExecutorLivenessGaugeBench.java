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
package org.apache.cassandra.test.microbench;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.SharedExecutorPool;
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

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Cost of one read of each liveness gauge, as metrics reporters, tpstats, the thread_pools table and StatusLogger
 * read them, on an idle pool and on a pool running one task. It uses only the gauges' executor API, so that it runs
 * unchanged on any tree that has them.
 *
 * Run in-process (forked JMH cannot reach the host on this machine):
 *   jmh-profile.sh {@code <repo>} ExecutorLivenessGaugeBench -- -prof gc
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(0)
@Threads(1)
@State(Scope.Benchmark)
public class ExecutorLivenessGaugeBench
{
    private static final AtomicInteger trials = new AtomicInteger();

    @Param({ "sep", "tpe" })
    public String family;

    @Param({ "idle", "running" })
    public String state;

    private ExecutorPlus executor;
    private SharedExecutorPool pool;
    private final CountDownLatch release = new CountDownLatch(1);

    @Setup
    public void setup() throws InterruptedException
    {
        DatabaseDescriptor.daemonInitialization();
        String name = "BenchGauge" + trials.incrementAndGet();
        if ("sep".equals(family))
        {
            pool = new SharedExecutorPool(name + "Pool");
            executor = pool.newExecutor(4, "internal", name);
        }
        else
        {
            executor = executorFactory().withJmxInternal().pooled(name, 4);
        }
        if ("running".equals(state))
        {
            executor.execute(() -> {
                try
                {
                    release.await();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            });
            while (executor.longestRunningTaskTime() == 0)
                Thread.sleep(1);
        }
    }

    @TearDown
    public void tearDown() throws Exception
    {
        release.countDown();
        if (pool != null)
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        else
            executor.shutdownNow();
    }

    @Benchmark
    public long longestRunningTaskTime()
    {
        return executor.longestRunningTaskTime();
    }

    @Benchmark
    public String getLongestRunningTaskClass()
    {
        return executor.getLongestRunningTaskClass();
    }

    @Benchmark
    public long oldestTaskQueueTime()
    {
        return executor.oldestTaskQueueTime();
    }
}
