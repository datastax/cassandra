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
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.openjdk.jmh.infra.Blackhole;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Submission throughput of the two executor families under a concurrent gauge reader, to bound the
 * per-task cost of the liveness signals (oldest task queue time, longest running task).
 *
 * With 4 submitters each waiting on its own task and 4 workers, the queue is mostly empty, so
 * {@code oldestTaskQueueTime()} usually peeks an empty queue. The per-submission stamp and the
 * per-task worker stamps are exercised on every task regardless, and the reader exercises the scan path.
 *
 * Run in-process (forked JMH cannot reach the host on this machine):
 *   jmh-profile.sh {@code <repo>} ExecutorLivenessBench -- -wi 3 -i 5 -r 2s -t 4 -prof gc
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(0)
@Threads(4)
@State(Scope.Benchmark)
public class ExecutorLivenessBench
{
    @Param({ "sep", "tpe" })
    public String family;

    @Param({ "true", "false" })
    public boolean reader;

    private ExecutorPlus executor;
    private SharedExecutorPool pool;
    private Thread readerThread;
    private final AtomicBoolean stop = new AtomicBoolean();
    private volatile long sink;

    @Setup
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        if ("sep".equals(family))
        {
            pool = new SharedExecutorPool("BenchPool");
            executor = pool.newExecutor(4, "internal", "BenchStage");
        }
        else
        {
            executor = executorFactory().withJmxInternal().pooled("BenchTpe", 4);
        }
        if (reader)
        {
            readerThread = new Thread(() -> {
                while (!stop.get())
                {
                    sink += readGauges(executor);
                    try { Thread.sleep(1); } catch (InterruptedException e) { return; }
                }
            }, "gauge-reader");
            readerThread.setDaemon(true);
            readerThread.start();
        }
    }

    // Reads the pre-existing gauges and the liveness gauges, as a metrics poller would.
    static long readGauges(ExecutorPlus e)
    {
        String c = e.getLongestRunningTaskClass();
        return e.getPendingTaskCount() + e.getActiveTaskCount()
             + e.oldestTaskQueueTime() + e.longestRunningTaskTime()
             + (c == null ? 0 : 1);
    }

    @TearDown
    public void tearDown() throws Exception
    {
        stop.set(true);
        if (readerThread != null)
            readerThread.join(1000);
        if (pool != null)
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        else
            executor.shutdownNow();
    }

    @Benchmark
    public void submitAndWait() throws InterruptedException
    {
        CountDownLatch done = new CountDownLatch(1);
        executor.execute(() -> {
            Blackhole.consumeCPU(1);
            done.countDown();
        });
        done.await();
    }
}
