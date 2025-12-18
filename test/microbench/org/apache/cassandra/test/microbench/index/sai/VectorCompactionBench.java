/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench.index.sai;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.index.sai.SAITester;
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

/**
 * Benchmark for measuring vector index compaction performance using the SIFT dataset.
 * This benchmark loads the siftsmall dataset and measures the time it takes to compact
 * multiple SSTables containing vector data.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class VectorCompactionBench extends SAITester
{
    private static final String DATASET = "siftsmall";

    /**
     * The percent of vectors to duplicate expressed as a decimal from 0 to 1. This covers several code paths related
     * to different kinds of postings lists generated for no dupes, some dupes, and many dupes.
     */
    @Param({ "0", "0.009", "0.999", "10" })
    public double dupeVectorFactor;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();

        // Load the SIFT dataset
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));

        // Create table and index
        createTable("CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // Disable background compactions to avoid interference
        disableCompaction();

        // Select dupes by percent
        int numDupes = (int) (baseVectors.size() * dupeVectorFactor);
        for (int i = 0; i < numDupes; i++)
            baseVectors.add(baseVectors.get(i));

        IntStream.range(0, baseVectors.size()).parallel().forEach(i -> {
            float[] arrayVector = baseVectors.get(i);
            try {
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(arrayVector));
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        });

        // Flush since we only want to measure compaction performance
        flush();
    }


    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    /**
     * Benchmark the compaction of vector indexes.
     * This measures the time it takes to compact a single sstable, which currently builds the whole
     * index from scratch each time.
     */
    @Benchmark
    public void compactVectorIndex() throws Throwable
    {
        compact();
    }

    /**
     * Read float vectors from a .fvecs file (SIFT dataset format).
     */
    private static ArrayList<float[]> readFvecs(String filePath) throws IOException
    {
        var vectors = new ArrayList<float[]>();
        try (var dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath))))
        {
            while (dis.available() > 0)
            {
                var dimension = Integer.reverseBytes(dis.readInt());
                assert dimension > 0 : dimension;
                var buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

                var vector = new float[dimension];
                for (var i = 0; i < dimension; i++)
                {
                    vector[i] = byteBuffer.getFloat();
                }
                vectors.add(vector);
            }
        }
        return vectors;
    }
}
