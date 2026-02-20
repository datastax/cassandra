/*
 * Copyright DataStax, Inc.
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.vector.OnDiskVectorValues;
import org.apache.cassandra.index.sai.disk.vector.OnDiskVectorValuesWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

/**
 * Benchmark to verify read and write performance of OnDiskVectorValues and OnDiskVectorValuesWriter.
 * Tests writing 128k vectors and measuring the time to iterate through them.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class OnDiskVectorValuesBench extends SAITester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final int NUM_VECTORS = 128_000; // 128k vectors is the number of vectors used to create a PQ

    // Cover small and large vectors
    @Param({ "384", "1536" })
    public int dimension;

    private File vectorFile;
    private OnDiskVectorValues reader;
    private VectorFloat<?>[] testVectors;

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
        CQLTester.setUpClass();

        // Create temporary file for vectors
        vectorFile = FileUtils.createTempFile("vectors", ".bin");

        // Generate random test vectors
        testVectors = new VectorFloat<?>[NUM_VECTORS];
        for (int i = 0; i < NUM_VECTORS; i++)
        {
            float[] vector = new float[dimension];
            for (int j = 0; j < dimension; j++)
            {
                vector[j] = ThreadLocalRandom.current().nextFloat();
            }
            testVectors[i] = vts.createFloatVector(vector);
        }

        // Write vectors to disk
        writeVectors();

        // Create reader for read benchmarks
        reader = new OnDiskVectorValues(vectorFile, dimension);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    private void writeVectors() throws IOException
    {
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(vectorFile, dimension))
        {
            for (int ordinal = 0; ordinal < NUM_VECTORS; ordinal++)
            {
                writer.write(ordinal, testVectors[ordinal]);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        if (reader != null)
        {
            reader.close();
        }
        if (vectorFile != null && vectorFile.exists())
        {
            vectorFile.delete();
        }
    }

    /**
     * Benchmark writing 128k vectors to disk.
     */
    @Benchmark
    public void writeVectorsToDisk(Blackhole bh) throws IOException
    {
        File tempFile = FileUtils.createTempFile("vectors-write", ".bin");
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int ordinal = 0; ordinal < NUM_VECTORS; ordinal++)
            {
                writer.write(ordinal, testVectors[ordinal]);
            }
            bh.consume(writer.getLastOrdinal());
        }
        finally
        {
            tempFile.delete();
        }
    }

    /**
     * Benchmark sequential reading of all vectors using getVector().
     */
    @Benchmark
    public void readAllVectorsSequential(Blackhole bh)
    {
        for (int ordinal = 0; ordinal < NUM_VECTORS; ordinal++)
        {
            VectorFloat<?> vector = reader.getVector(ordinal);
            bh.consume(vector);
        }
    }
}
