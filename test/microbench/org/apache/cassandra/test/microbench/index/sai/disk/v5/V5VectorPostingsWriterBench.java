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

package org.apache.cassandra.test.microbench.index.sai.disk.v5;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class V5VectorPostingsWriterBench
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private AtomicInteger maxRowId = new AtomicInteger(0);

    @Param({ "768", "1536"})
    public int dimension;

    @Param({"100000", "1000000", "10000000"})
    public int numVectors;

    private File postingsFile;
    private ChronicleMap<VectorFloat<?>, VectorPostings.CompactionVectorPostings> postingsMap;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        postingsFile = File.createTempFile("postings", ".map");
        // Build a ChronicleMap
        postingsMap = ChronicleMapBuilder.of((Class<VectorFloat<?>>) (Class) VectorFloat.class, (Class<VectorPostings.CompactionVectorPostings>) (Class) VectorPostings.CompactionVectorPostings.class)
                                         .averageKeySize(dimension * Float.BYTES)
                                         .keySizeMarshaller(SizeMarshaller.constant((long) dimension * Float.BYTES))
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES)
                                         .keyMarshaller(new CompactionGraph.VectorFloatMarshaller(dimension))
                                         .valueMarshaller(new VectorPostings.Marshaller())
                                         .entries(numVectors)
                                         .createPersistedTo(postingsFile);

        var rowId = new AtomicInteger(0);
        IntStream.range(0, numVectors)
                 .parallel()
                 .forEach(ordinal -> {
                     float[] vector = new float[dimension];
                     for (int j = 0; j < dimension; j++)
                         vector[j] = ThreadLocalRandom.current().nextFloat();
                     var floatVector = vts.createFloatVector(vector);
                     var row = rowId.getAndIncrement();
                     var postings = new VectorPostings.CompactionVectorPostings(ordinal, row);
                     // Add a dupe every so often, shifting it to ensure unique row ids and to ensure that ordinals
                     // still map to their respective row id and allow for the ONE_TO_MANY logic to work correctly.
                     if (row % 100 == 0)
                     {
                         maxRowId.accumulateAndGet(row + numVectors, Integer::max);
                         postings.add(row + numVectors);
                     }
                     postingsMap.put(floatVector, postings);
                 });
    }

    @Benchmark
    public void describeForCompactionOneToMany()
    {
        V5VectorPostingsWriter.describeForCompaction(V5VectorPostingsWriter.Structure.ONE_TO_MANY, numVectors, maxRowId.get(), numVectors - 1, postingsMap, null);
    }

    @TearDown
    public void tearDown() throws Throwable
    {
        postingsMap.close();
        boolean success = postingsFile.delete();
        if (!success)
            throw new AssertionError("failed to delete " + postingsFile);
    }
}
