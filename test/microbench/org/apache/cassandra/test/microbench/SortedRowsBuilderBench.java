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

package org.apache.cassandra.test.microbench;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.selection.SortedRowsBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.Index;
import org.openjdk.jmh.annotations.*;

/**
 * Benchmarks each implementation of {@link SortedRowsBuilder}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2) // seconds
@Measurement(iterations = 5, time = 2) // seconds
@Fork(value = 4)
@Threads(4)
@State(Scope.Benchmark)
public class SortedRowsBuilderBench extends CQLTester
{
    private static final int NUM_COLUMNS = 10;
    private static final int SORTED_COLUMN_CARDINALITY = 1000;
    private static final Index.Scorer SCORER = row -> Int32Type.instance.compose(row.get(0));
    private static final Comparator<List<ByteBuffer>> COMPARATOR = (o1, o2) -> Int32Type.instance.compare(o1.get(0), o2.get(0));
    private static final Random RANDOM = new Random();

    @Param({ "1", "2", "3", "4", "5", "6", "8", "16", "32" })
    public int nodes;

    @Param({ "10", "100", "1000", "10000" })
    public int limit;

    @Param({ "0", "0.1", "0.5", "1" })
    public float offsetRatio;

    private int offset;

    private List<List<ByteBuffer>> rows;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        int rowsPerNode = limit + offset;
        int rowsPerCoordinator = rowsPerNode * nodes;
        offset = (int) (rowsPerNode * offsetRatio);
        rows = new ArrayList<>(rowsPerCoordinator);
        for (int r = 0; r < rowsPerCoordinator; r++)
        {
            rows.add(randomRow());
        }
    }

    @Benchmark
    public Object insertion()
    {
        return test(SortedRowsBuilder.create(limit, offset));
    }

    @Benchmark
    public Object comparatorWithList()
    {
        return test(SortedRowsBuilder.WithListSort.create(limit, offset, COMPARATOR));
    }

    @Benchmark
    public Object comparatorWithHeap()
    {
        return test(SortedRowsBuilder.WithHeapSort.create(limit, offset, COMPARATOR));
    }

    @Benchmark
    public Object comparatorWithHybrid()
    {
        return test(SortedRowsBuilder.WithHybridSort.create(limit, offset, COMPARATOR));
    }

    @Benchmark
    public Object scorerWithList()
    {
        return test(SortedRowsBuilder.WithListSort.create(limit, offset, SCORER));
    }

    @Benchmark
    public Object scorerWithHeap()
    {
        return test(SortedRowsBuilder.WithHeapSort.create(limit, offset, SCORER));
    }

    @Benchmark
    public Object scorerWithHybrid()
    {
        return test(SortedRowsBuilder.WithHybridSort.create(limit, offset, SCORER));
    }

    private List<List<ByteBuffer>> test(SortedRowsBuilder builder)
    {
        rows.forEach(builder::add);
        return builder.build();
    }

    private static List<ByteBuffer> randomRow()
    {
        List<ByteBuffer> row = new ArrayList<>(NUM_COLUMNS);
        for (int c = 0; c < NUM_COLUMNS; c++)
        {
            row.add(Int32Type.instance.decompose(RANDOM.nextInt(SORTED_COLUMN_CARDINALITY)));
        }
        return row;
    }
}
