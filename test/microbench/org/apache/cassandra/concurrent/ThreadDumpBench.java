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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Cost of the {@link ExecutorLivenessWatchdog}'s thread dump with {@code threads} extra threads parked about 30 frames
 * deep, every fourth holding a monitor, and optionally {@code liveHeapMB} of small live objects on the heap:
 * <ul>
 * <li>{@code dump}: what the watchdog does, {@link ThreadDump#dumpAllThreads}: the JVM's dump, then the formatting.</li>
 * <li>{@code dumpAllThreads}: the JVM's dump alone, as the watchdog takes it, without ownable synchronizers.</li>
 * <li>{@code dumpAllThreadsWithSynchronizers}: the JVM's dump with ownable synchronizers, which the watchdog skips, as
 * finding them walks the heap at a safepoint.</li>
 * </ul>
 *
 * Run in-process (forked JMH cannot reach the host on this machine):
 *   jmh-profile.sh {@code <repo>} ThreadDumpBench -- -prof gc
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(0)
@Threads(1)
@State(Scope.Benchmark)
public class ThreadDumpBench
{
    private static final int DEPTH = 30;

    @Param({ "200", "1000", "3000" })
    public int threads;

    @Param({ "0" })
    public int liveHeapMB;

    private final List<Thread> parked = new ArrayList<>();
    private volatile boolean stop;
    private Object[] liveHeap;

    @Setup
    public void setup() throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++)
        {
            boolean holdsMonitor = i % 4 == 0;
            Thread thread = new Thread(() -> recurse(DEPTH, holdsMonitor, started), "BenchParked-" + i);
            thread.setDaemon(true);
            thread.start();
            parked.add(thread);
        }
        started.await();
        Thread.sleep(100);

        // small objects, as the heap walk that finds ownable synchronizers visits every object
        liveHeap = new Object[liveHeapMB * (1 << 20) / 32];
        for (int i = 0; i < liveHeap.length; i++)
            liveHeap[i] = new long[2];

        String dump = dump();
        System.out.printf("%n%d threads in the JVM; dump %d bytes, %d lines%n",
                          ManagementFactory.getThreadMXBean().getThreadCount(), dump.getBytes(UTF_8).length,
                          dump.split("\n").length);
    }

    private void recurse(int depth, boolean holdsMonitor, CountDownLatch started)
    {
        if (depth == DEPTH / 2 && holdsMonitor)
        {
            Object monitor = new Object();
            synchronized (monitor)
            {
                recurse(depth - 1, false, started);
            }
            return;
        }
        if (depth > 0)
        {
            recurse(depth - 1, holdsMonitor, started);
            return;
        }
        started.countDown();
        while (!stop)
            LockSupport.park(this);
    }

    @TearDown
    public void tearDown() throws InterruptedException
    {
        stop = true;
        for (Thread thread : parked)
            LockSupport.unpark(thread);
        for (Thread thread : parked)
            thread.join();
        liveHeap = null;
    }

    @Benchmark
    public String dump()
    {
        return ThreadDump.dumpAllThreads(Collections.singletonList("ReadStage"), Collections.singletonList("BenchParked-0"));
    }

    @Benchmark
    public ThreadInfo[] dumpAllThreads()
    {
        return ManagementFactory.getThreadMXBean().dumpAllThreads(true, false);
    }

    @Benchmark
    public ThreadInfo[] dumpAllThreadsWithSynchronizers()
    {
        return ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
    }
}
