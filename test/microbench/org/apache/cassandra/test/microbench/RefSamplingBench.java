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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;

/**
 * Cost of the {@link Ref} debug record: the reference life cycle under each debug configuration, the stack
 * capture alternatives for one record, and the per-reference sampling decision.
 * <p>
 * {@code Ref} reads {@code cassandra.debugrefcount}, {@code cassandra.debugrefcount.primary_sample_interval} and
 * {@code cassandra.debugrefcount.copy_sample_interval} once, at class initialisation, so {@link #refRelease} and
 * {@link #refCopyRelease} measure whatever configuration the JVM was started with. Run them once per configuration,
 * in its own JVM, with the properties on the JVM command line (or in {@code -jvmArgsAppend} when forking).
 * <p>
 * Everything runs at the bottom of a {@link #depth}-frame recursion, because the cost of a stack capture grows
 * with the depth of the stack, and a server thread creates references well below the top of its stack.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class RefSamplingBench
{
    private static final int REFS_PER_INVOCATION = 100;

    private static final Object REFERENT = new Object();

    private static final RefCounted.Tidy TIDY = new RefCounted.Tidy()
    {
        public void tidy()
        {
        }

        public String name()
        {
            return "RefSamplingBench";
        }
    };

    private static final ThreadLocal<int[]> COUNTDOWN = ThreadLocal.withInitial(() -> new int[1]);

    @Param({ "40" })
    public int depth;

    /** The interval used by the sampling-decision benchmarks; it does not configure {@link Ref}. */
    @Param({ "1024" })
    public int interval;

    private Ref<Object> primary;

    @Setup
    public void createPrimary()
    {
        primary = new Ref<>(REFERENT, TIDY);
    }

    @TearDown
    public void releasePrimary()
    {
        primary.release();
    }

    /**
     * {@code new Ref(referent, tidy)} followed by {@code release()}, under the configuration the JVM was started
     * with: the references {@code cassandra.debugrefcount.primary_sample_interval} samples.
     */
    @Benchmark
    @OperationsPerInvocation(REFS_PER_INVOCATION)
    public int refRelease()
    {
        return refReleaseAt(depth);
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private int refReleaseAt(int remaining)
    {
        if (remaining > 0)
            return refReleaseAt(remaining - 1) + 1;

        for (int i = 0; i < REFS_PER_INVOCATION; i++)
            new Ref<>(REFERENT, TIDY).release();
        return 0;
    }

    /**
     * {@code ref.ref()} followed by {@code release()}: the copies
     * {@code cassandra.debugrefcount.copy_sample_interval} samples, as {@code SharedCloseable.sharedCopy()} makes
     * them.
     */
    @Benchmark
    @OperationsPerInvocation(REFS_PER_INVOCATION)
    public int refCopyRelease()
    {
        return refCopyReleaseAt(depth);
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private int refCopyReleaseAt(int remaining)
    {
        if (remaining > 0)
            return refCopyReleaseAt(remaining - 1) + 1;

        for (int i = 0; i < REFS_PER_INVOCATION; i++)
            primary.ref().release();
        return 0;
    }

    /**
     * {@code get()} on a live reference, which now checks the reference's own record where it used to check a
     * constant; run with {@code cassandra.debugrefcount.primary_sample_interval=1} to make the reference sampled.
     */
    @Benchmark
    @OperationsPerInvocation(REFS_PER_INVOCATION)
    public void refGet(Blackhole bh)
    {
        Ref<Object> primary = this.primary;
        for (int i = 0; i < REFS_PER_INVOCATION; i++)
            bh.consume(primary.get());
    }

    /** The recursion alone, to subtract from the capture benchmarks. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public Object captureNothing()
    {
        return captureAt(depth, 0);
    }

    /** What a debug record captured up to now: {@code Thread.getStackTrace()}. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public Object captureThreadGetStackTrace()
    {
        return captureAt(depth, 1);
    }

    /** The VM's compact backtrace only; the elements are materialised only if a report is logged. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public Object captureThrowable()
    {
        return captureAt(depth, 2);
    }

    /** The thread name a debug record captures next to its trace. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public Object captureThreadToString()
    {
        return captureAt(depth, 3);
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private static Object captureAt(int remaining, int what)
    {
        if (remaining > 0)
            return captureAt(remaining - 1, what);

        switch (what)
        {
            case 1:
                return Thread.currentThread().getStackTrace();
            case 2:
                return new Throwable();
            case 3:
                return Thread.currentThread().toString();
            default:
                return REFERENT;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void sampleNothing(Blackhole bh)
    {
        bh.consume(interval > 0);
    }

    /** {@code ThreadLocalRandom.nextInt(bound)}; with a power-of-two bound the JDK masks instead of dividing. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void sampleThreadLocalRandom(Blackhole bh)
    {
        int interval = this.interval;
        bh.consume(interval > 0 && ThreadLocalRandom.current().nextInt(interval) == 0);
    }

    /** An explicit mask, valid for power-of-two intervals only. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void sampleThreadLocalRandomMask(Blackhole bh)
    {
        int interval = this.interval;
        bh.consume(interval > 0 && (ThreadLocalRandom.current().nextInt() & (interval - 1)) == 0);
    }

    /** A per-thread countdown: samples every interval-th reference a thread creates. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void sampleCountdown(Blackhole bh)
    {
        int interval = this.interval;
        if (interval <= 0)
        {
            bh.consume(false);
            return;
        }
        int[] countdown = COUNTDOWN.get();
        if (--countdown[0] > 0)
        {
            bh.consume(false);
            return;
        }
        countdown[0] = interval;
        bh.consume(true);
    }
}
