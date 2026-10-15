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

package org.apache.cassandra.test.microbench.index.sai.disk.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.index.sai.disk.vector.VectorDeduplicator;
import org.apache.cassandra.index.sai.disk.vector.VectorHandle;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

/**
 * Measures the throughput of {@link VectorDeduplicator#deduplicate} at production scale
 * (10 million vectors per iteration).
 *
 * <p>Each measurement iteration uses a fresh, empty {@link VectorDeduplicator} so that
 * ChronicleMap is never pre-warmed across iterations. Vectors are generated on-the-fly
 * inside the benchmark loop using {@link SplittableRandom} re-seeded per unique vector,
 * so memory usage is O(numUnique) longs regardless of {@link #dimension}.
 *
 * <p>Run via the Cassandra microbench target, e.g.:
 * <pre>
 *   ant microbench -Dbenchmark.name=VectorDeduplicatorBench
 * </pre>
 */
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class VectorDeduplicatorBench
{
    private static final int NUM_VECTORS = 10_000_000;

    /** Vector dimensionality. Covers small (SIFT-like) and large (embedding-model) sizes. */
    @Param({ "128", "1536" })
    public int dimension;

    /**
     * Fraction of the 10M calls that should hit an already-seen vector.
     * 0.0 = all unique, 0.1 = 10% duplicates, 1.0 = all identical.
     */
    @Param({ "0.0", "0.1", "1.0" })
    public double duplicateRatio;

    private VectorDeduplicator deduplicator;

    // Scratch buffer reused on every loop iteration to avoid per-call allocation.
    private byte[] vectorBytes;
    // Number of distinct vector identities.
    private int numUnique;
    // One seed per unique vector; re-seeding SplittableRandom with the same seed
    // always produces the same byte sequence, so duplicate slots collide in the map.
    private long[] seeds;
    // Reusable RNG — re-seeded per vector inside the benchmark loop.
    private SplittableRandom rng;

    // -------------------------------------------------------------------------
    // Trial-level setup: compute sizing and allocate the scratch buffer + seeds.
    // -------------------------------------------------------------------------

    @Setup(Level.Trial)
    public void trialSetup()
    {
        numUnique = Math.max(1, (int) (NUM_VECTORS * (1.0 - duplicateRatio)));
        vectorBytes = new byte[dimension * Float.BYTES];
        rng = new SplittableRandom();

        // One 64-bit seed per unique vector — negligible memory even for 10M unique.
        seeds = new long[numUnique];
        for (int i = 0; i < numUnique; i++)
            seeds[i] = ThreadLocalRandom.current().nextLong();
    }

    // -------------------------------------------------------------------------
    // Iteration-level lifecycle: fresh deduplicator for every timed iteration.
    // -------------------------------------------------------------------------

    @Setup(Level.Iteration)
    public void iterationSetup() throws IOException
    {
        deduplicator = new VectorDeduplicator(NUM_VECTORS);
    }

    @TearDown(Level.Iteration)
    public void iterationTearDown() throws IOException
    {
        deduplicator.close();
        deduplicator = null;
    }

    // -------------------------------------------------------------------------
    // Benchmark: 10M deduplicate() calls per iteration.
    // -------------------------------------------------------------------------

    /**
     * Deduplicates 10 million vectors generated on-the-fly.
     *
     * <p>Each unique vector is reproduced deterministically by re-seeding
     * {@link SplittableRandom} with its assigned seed, so the same seed always
     * produces identical bytes — giving ChronicleMap a consistent hit/miss
     * pattern that matches {@link #duplicateRatio}.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(VectorDeduplicatorBench.NUM_VECTORS)
    public void deduplicateVectors(Blackhole bh)
    {
        for (int i = 0; i < NUM_VECTORS; i++)
        {
            // Re-seed so duplicate slots always hash to the same bytes.
            rng = new SplittableRandom(seeds[i % numUnique]);
            rng.nextBytes(vectorBytes);

            ByteBuffer buf = ByteBuffer.wrap(vectorBytes);
            VectorHandle result = deduplicator.deduplicate(buf, new VectorHandle(0, i));
            bh.consume(result);
        }
    }
}
