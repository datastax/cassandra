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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.index.sai.disk.v1.PostingsReader;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class PostingsReaderBenchmark extends AbstractOnDiskBenchmark
{
    private static final int NUM_INVOCATIONS = 10_000;

    @Param({ "1", "10", "100", "1000"})
    public int skippingDistance;

    protected PostingsReader reader;
    protected PrimaryKey[] primaryKeys;

    @Override
    public int numRows()
    {
        return 10_000_000;
    }

    @Override
    public int numPostings()
    {
        return 10_000_000;
    }

    @Override
    public void beforeInvocation() throws Throwable
    {
        reader = openPostingsReader();

        primaryKeys = new PrimaryKey[NUM_INVOCATIONS];
        for (int i = 0; i < primaryKeys.length; i++)
        {
            int rowId = toPosting(i * skippingDistance);
            primaryKeys[i] = primaryKeyMap.primaryKeyFromRowId(rowId);
        }
    }

    @Override
    public void afterInvocation() throws Throwable
    {
        reader.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode(Mode.AverageTime)
    public void advance(Blackhole bh) throws Throwable
    {
        for (int i = 0; i < primaryKeys.length;)
        {
            PrimaryKey key = primaryKeys[i];
            bh.consume(reader.advance(key));
            i++;
        }
    }
}
