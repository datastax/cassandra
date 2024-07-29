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

package org.apache.cassandra.test.microbench.index.sai.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
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
@State(Scope.Thread)
public class MergePostingListBench
{
    @Param({"3", "17"})
    int stepMax = 7;

    @Param({"50", "5000"})
    int sources = 50;

    @Param({"1000000"})
    int count = 1_000_000;

    @Param({"UNIFORM", "NORMAL", "SEQUENTIAL", "ROUND_ROBIN"})
    Distribution distribution = Distribution.NORMAL;

    public enum Distribution
    {
        UNIFORM, NORMAL, SEQUENTIAL, ROUND_ROBIN
    }

    List<int[]> splitPostingLists = new ArrayList<>();
    PostingList merge;

    @Setup(Level.Trial)
    public void generatePostings()
    {
        final AtomicInteger rowId = new AtomicInteger();
        final Random rand = new Random(1);
        final int[] postings = IntStream.generate(() -> rowId.addAndGet(rand.nextInt(stepMax)))
                                        .limit(count)
                                        .toArray();

        // split postings into multiple lists
        Function<Integer, Integer> grouping;
        switch (distribution)
        {
            case UNIFORM:
                grouping = x -> rand.nextInt(sources);
                break;
            case NORMAL:
                grouping = x -> (int) Math.min(sources - 1, Math.abs(rand.nextGaussian()) * sources / 5);
                break;
            case SEQUENTIAL:
            {
                AtomicInteger index = new AtomicInteger();
                int sizePerList = Math.max(count / sources, 1);
                grouping = x -> index.getAndIncrement() / sizePerList;
                break;
            }
            case ROUND_ROBIN:
            {
                AtomicInteger index = new AtomicInteger();
                grouping = x -> index.getAndIncrement() % sources;
                break;
            }
            default:
                throw new AssertionError();
        }
        final Map<Integer, List<Integer>> splitPostings = Arrays.stream(postings)
                                                                .boxed()
                                                                .collect(Collectors.groupingBy(grouping));

        for (List<Integer> split : splitPostings.values())
        {
            // Remove any duplicates in each individual set
            int[] data = split.stream().distinct().mapToInt(Integer::intValue).toArray();
            splitPostingLists.add(data);
        }
    }

    @Setup(Level.Invocation)
    public void mergePostings()
    {
        var lists = new ArrayList<PostingList>();
        for (int[] postings : splitPostingLists)
        {
            lists.add(new ArrayPostingList(postings));
        }
        merge = MergePostingList.merge(lists);
    }

    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void nextPostingIteration(Blackhole bh) throws IOException
    {
        long id;
        while ((id = merge.nextPosting()) != PostingList.END_OF_STREAM)
        {
            bh.consume(id);
        }
    }

    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void advanceIteration(Blackhole bh) throws IOException
    {
        int id = 0;
        while ((id = merge.advance(id + stepMax)) != PostingList.END_OF_STREAM)
        {
            bh.consume(id);
        }
    }
}
