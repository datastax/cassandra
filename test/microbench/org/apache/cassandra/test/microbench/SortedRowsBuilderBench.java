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
    private static final Random RANDOM = new Random();

    @Param({ "1", "2", "3", "4", "6", "8", "16", "32" })
    public int numReplicas;

    @Param({ "100", "10000" })
    public int limit;

    @Param({ "0" })
    public int offset;

    private List<List<ByteBuffer>> rows;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        int numRows = limit * numReplicas;
        rows = new ArrayList<>(numRows);
        for (int r = 0; r < numRows; r++)
        {
            List<ByteBuffer> row = new ArrayList<>(NUM_COLUMNS);
            rows.add(row);

            for (int c = 0; c < NUM_COLUMNS; c++)
            {
                row.add(Int32Type.instance.decompose(RANDOM.nextInt(SORTED_COLUMN_CARDINALITY)));
            }
        }
    }

    @Benchmark
    public Object insertion()
    {
        return test(SortedRowsBuilder.create(limit, offset));
    }

    @Benchmark
    public Object list()
    {
        return test(SortedRowsBuilder.WithList.create(limit, offset, SCORER));
    }

    @Benchmark
    public Object queue()
    {
        return test(SortedRowsBuilder.WithPriorityQueue.create(limit, offset, SCORER));
    }

    @Benchmark
    public Object hybrid()
    {
        return test(SortedRowsBuilder.WithListAndPriorityQueue.create(limit, offset, SCORER));
    }

    private List<List<ByteBuffer>> test(SortedRowsBuilder builder)
    {
        rows.forEach(builder::add);
        return builder.build();
    }
}
