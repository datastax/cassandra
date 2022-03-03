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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class HllSketchMergeBenchmark
{
    @Param({ "1000", "10000", "100000", "1000000"})
    private int numberOfTerms;

    private int logK = 10;
    private int[] terms1;
    private int[] terms2;
    private HllSketch sketch1;
    private HllSketch sketch2;
    private Union union;
    private long seed;
    private Random random;

    @Setup(Level.Trial)
    public void generateTerms()
    {
        seed = Long.getLong("cassandra.test.random.seed", System.nanoTime());
        random = new Random(seed);

        terms1 = new int[numberOfTerms];
        terms2 = new int[numberOfTerms];

        for (int index = 0; index < numberOfTerms; index++)
        {
            terms1[index] = RandomInts.randomIntBetween(random, 0, numberOfTerms / 10);
            terms2[index] = RandomInts.randomIntBetween(random, 0, numberOfTerms / 10);
        }
    }

    @Setup(Level.Invocation)
    public void createSketch()
    {
        sketch1 = new HllSketch(logK);
        sketch2 = new HllSketch(logK);
        for (int index = 0; index < numberOfTerms; index++)
        {
            sketch1.update(terms1[index]);
            sketch2.update(terms2[index]);
        }
        union = new Union(logK);
    }

    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    public void mergeSketches(Blackhole bh)
    {
        union.update(sketch1);
        union.update(sketch2);
        bh.consume(union.getResult());
    }
}
