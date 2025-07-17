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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.commitlog.CommitLog;
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
 * Benchmarks single-partition SAI queries.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2) // seconds
@Measurement(iterations = 4, time = 2) // seconds
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class SinglePartitionQueryBench extends CQLTester
{
    static final Random RANDOM = new Random();

    @Param({ "10" })
    public int partitions;

    @Param({ "10000" })
    public int rowsPerPartition;

    @Param({ "0.001", "0.002", "0.005", "0.007", "0.09", "0.01", "0.02", "0.03", "0.05", "0.07", "0.1", "0.2", "0.3", "0.4", "0.5", "0.7", "0.9", "1" })
    public float selectivity;

    @Param({ "aa-1.0", "ec-0.01", "ec-0.2", "ec-1.0", "none" })
    public String version;

    private String query;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        if (version.contains("-"))
        {
            String[] components = version.split("-");
            version = components[0];
            CassandraRelevantProperties.SAI_CURRENT_VERSION.setString(version);
            double selectivityThreshold = Double.parseDouble(components[1]);
            CassandraRelevantProperties.SAI_SELECTIVITY_THRESHOLD.setDouble(selectivityThreshold);
        }

        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();
        DatabaseDescriptor.setAutoSnapshot(false);

        String table = createTable("CREATE TABLE %s (k1 text, k2 text, c1 text, c2 text, v int, PRIMARY KEY ((k1, k2), c1, c2))");
        if (!version.equals("none"))
            createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex'");
        query = String.format("SELECT * FROM %s.%s WHERE k1 = '0_k1' AND k2 = '0_k2' AND v = 0 ALLOW FILTERING", KEYSPACE, table);

        for (int i = 0; i < partitions; i++)
        {
            String k1 = i + "_k1";
            String k2 = i + "_k2";
            for (int j = 0; j < rowsPerPartition; j++)
            {
                String c1 = j + "_c1";
                String c2 = j + "_c2";
                int value = RANDOM.nextInt((int) (1.0f / selectivity));
                execute("INSERT INTO %s (k1, k2, c1, c2, v) VALUES (?, ?, ?, ?, ?)", k1, k2, c1, c2, value);
            }
        }
        flush();
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    @Benchmark
    public Object select()
    {
        return execute(query).size();
    }
}
