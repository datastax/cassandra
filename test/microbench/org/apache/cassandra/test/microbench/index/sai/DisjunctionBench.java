/*
 * Copyright IBM Corp.
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
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
 * Benchmarks SAI queries with the OR operator at different selectivities, against the queries they can be compared with.
 * The setup prints how long it takes to load the data, to compare the write cost of an index on the partition key.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class DisjunctionBench extends CQLTester
{
    private static final Random RANDOM = new Random(42);

    private static final String TAX = "tax = 't0'";
    private static final String LAST = "last = 'l0'";
    private static final String DOB = "dob = 'd0'";

    /** A query where each branch relaxes the criteria of the previous one, so it's the same as {@link #TAX}. */
    private static final String RELAXING = String.format("(%s AND %s AND %s) OR (%s AND %s) OR (%s)", TAX, LAST, DOB, TAX, LAST, TAX);

    /** The same shape with different values, so no branch is redundant. */
    private static final String DIFFERENT = String.format("(%s AND %s AND %s) OR (tax = 't1' AND last = 'l1') OR (tax = 't2')", TAX, LAST, DOB);

    /** Query option that disables the plan optimizations, including the removal of redundant branches. */
    private static final String NO_OPTIMIZATIONS = " WITH optimizer_options = {'query_optimization_level': 0}";

    /** The number of partitions to be inserted. */
    @Param({ "1000" })
    public int partitions;

    /** The number of rows per partition to be inserted. */
    @Param({ "100" })
    public int rowsPerPartition;

    /** The fraction of rows that match an equality on an indexed column. */
    @Param({ "0.0001", "0.001", "0.01", "0.1" })
    public double selectivity;

    /** The LIMIT of the queries. */
    @Param({ "5000" })
    public int limit;

    /** Whether the partition key column is indexed. */
    @Param({ "false", "true" })
    public boolean indexPartitionKey;

    private String table;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();
        DatabaseDescriptor.setAutoSnapshot(false);

        table = createTable("CREATE TABLE %s (k int, c int, tax text, last text, dob text, unindexed text, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(tax) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(last) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(dob) USING 'StorageAttachedIndex'");
        if (indexPartitionKey)
            createIndex("CREATE CUSTOM INDEX ON %s(k) USING 'StorageAttachedIndex'");

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        int cardinality = Math.max(1, (int) Math.round(1.0 / selectivity));
        long start = System.nanoTime();
        for (int k = 0; k < partitions; k++)
        {
            for (int c = 0; c < rowsPerPartition; c++)
                execute("INSERT INTO %s (k, c, tax, last, dob, unindexed) VALUES (?, ?, ?, ?, ?, ?)",
                        k, c, value("t", cardinality), value("l", cardinality), value("d", cardinality), value("u", cardinality));
        }
        flush();
        System.out.printf("Loaded %d rows, indexPartitionKey=%b, selectivity=%s: %d ms%n",
                          partitions * rowsPerPartition, indexPartitionKey, selectivity, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private static String value(String prefix, int cardinality)
    {
        return prefix + RANDOM.nextInt(cardinality);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    private int query(String where, String suffix)
    {
        return execute(String.format("SELECT k, c FROM %s.%s WHERE %s LIMIT %d%s", KEYSPACE, table, where, limit, suffix)).size();
    }

    /** What a query with only redundant branches should cost. */
    @Benchmark
    public int singlePredicate()
    {
        return query(TAX, "");
    }

    @Benchmark
    public int relaxingBranches()
    {
        return query(RELAXING, "");
    }

    @Benchmark
    public int relaxingBranchesWithoutOptimizations()
    {
        return query(RELAXING, NO_OPTIMIZATIONS);
    }

    @Benchmark
    public int differentBranches()
    {
        return query(DIFFERENT, "");
    }

    @Benchmark
    public int twoColumns()
    {
        return query(TAX + " OR " + LAST, "");
    }

    /** What the query costs when a column isn't indexed: a scan of the table. */
    @Benchmark
    public int unindexedColumn()
    {
        return query(TAX + " OR unindexed = 'u0'", " ALLOW FILTERING");
    }

    /** Needs ALLOW FILTERING, and it's a scan of the table, if the key isn't indexed. */
    @Benchmark
    public int partitionKey()
    {
        return query("k = " + RANDOM.nextInt(partitions) + " OR " + TAX, indexPartitionKey ? "" : " ALLOW FILTERING");
    }
}
