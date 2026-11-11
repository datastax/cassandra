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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
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

/**
 * Compares the range queries on a {@code text} column, served by the trie terms dictionary of the literal indexes,
 * with the same range queries on a {@code bigint} column, served by the kd-tree of the numeric indexes.
 * <p>
 * Every row has a distinct number, stored in the {@code bigint} column and, zero-padded so that the byte order of the
 * strings is the order of the numbers, in the {@code text} column: a range selects the same rows on both columns.
 * With a small {@code LIMIT} the queries mostly measure the search of the index, with a large one they also measure
 * the read of the matching rows, which is the same for both columns.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class LiteralRangeQueryBench extends SAITester
{
    private static final int ROWS = 100_000;

    /** The number of rows, that is of indexed terms, in the queried range. */
    @Param({ "100", "10000" })
    public int matchingRows;

    @Param({ "10", "100000" })
    public int limit;

    /** Whether the bounds of the range are inclusive. */
    @Param({ "true", "false" })
    public boolean inclusive;

    /** Whether the rows are in an sstable or in the memtable. */
    @Param({ "true", "false" })
    public boolean flushed;

    private String textQuery;
    private String numericQuery;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, t text, n bigint)");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s (pk, t, n) VALUES (?, ?, ?)", i, text(i), (long) i);
        if (flushed)
            flush();

        String lower = inclusive ? ">=" : ">";
        String upper = inclusive ? "<=" : "<";
        textQuery = "SELECT pk FROM %s WHERE t " + lower + " ? AND t " + upper + " ? LIMIT " + limit;
        numericQuery = "SELECT pk FROM %s WHERE n " + lower + " ? AND n " + upper + " ? LIMIT " + limit;
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    @Benchmark
    public Object textRange()
    {
        int first = firstMatchingRow();
        return execute(textQuery, text(lowerBound(first)), text(upperBound(first)));
    }

    @Benchmark
    public Object numericRange()
    {
        int first = firstMatchingRow();
        return execute(numericQuery, (long) lowerBound(first), (long) upperBound(first));
    }

    private int firstMatchingRow()
    {
        return 1 + ThreadLocalRandom.current().nextInt(ROWS - matchingRows - 1);
    }

    private int lowerBound(int firstMatchingRow)
    {
        return inclusive ? firstMatchingRow : firstMatchingRow - 1;
    }

    private int upperBound(int firstMatchingRow)
    {
        int lastMatchingRow = firstMatchingRow + matchingRows - 1;
        return inclusive ? lastMatchingRow : lastMatchingRow + 1;
    }

    private static String text(int number)
    {
        return String.format("%010d", number);
    }
}
