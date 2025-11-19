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

package org.apache.cassandra.test.microbench.index.sai;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class NEQQueryBench extends SAITester
{

    /**
     * The more rows, the deeper the NEQ query has to search, based on the implementation at the time of this commit.
     */
    @Param({ "1000", "10000" })
    public int numRowsWithinPartition;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();

        // create the schema
        createTable("CREATE TABLE %s (k int, c int, l list<text>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");

        // Insert the data so that there are many keys in the index, most of them will not match the query
        // predicate (l NOT CONTAINS 'a'), and the ones that do contain it are at the end of the posting list
        // because of the clustering order.
        for (int k = 0; k < 100; k++)
        {
            for (int c = 0; c < numRowsWithinPartition; c++)
            {
                execute("INSERT INTO %s (k, c, l) VALUES (?, ?, ?)", k, c, list("a"));
            }
            // Now add one at the end of the partition's clustering order that will satisfy the query
            execute("INSERT INTO %s (k, c, l) VALUES (?, ?, ?)", k, 1000, list("zzz"));
        }
        flush();
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    /**
     * Test the cost of creating the execution info object.
     */
    @Benchmark
    public Object queryNEQ()
    {
        return execute("SELECT * FROM %s WHERE l NOT CONTAINS 'a'");
    }
}
