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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
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
 * The table has {@link #ROWS} rows and {@code ROWS / rowsPerTerm} distinct numbers, each one in {@code rowsPerTerm}
 * rows: in the {@code bigint} column and, zero-padded so that the byte order of the strings is the order of the
 * numbers, in the {@code text} column. A range selects the same {@code matchingTerms * rowsPerTerm} rows on both
 * columns. With a small {@code LIMIT} the queries mostly measure the search of the index, with a page-sized one they
 * also measure the read of the matching rows, which is the same for both columns.
 * <p>
 * The rows are either all in the memtable or all in a single sstable: the memtable is sized so that it is not
 * flushed while the rows are written, and the benchmark fails if it finds the rows where they are not expected.
 * <p>
 * Run with {@code ant microbench -Dbenchmark.name=LiteralRangeQueryBench}, and with {@code -Djmh.args="-prof gc"} to
 * also report the memory allocated by a query ({@code gc.alloc.rate.norm}).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xms4G", "-Xmx4G" })
@Threads(1)
@State(Scope.Benchmark)
public class LiteralRangeQueryBench extends SAITester
{
    private static final int ROWS = 1_000_000;

    /** Above what the rows and their index entries take, so that the memtable is not flushed for its size. */
    private static final int MEMTABLE_SPACE_MIB = 3072;

    /**
     * The number of rows with the same indexed term, that is the size of the posting list of a term: 1 for an index
     * of unique values.
     */
    @Param({ "1", "100" })
    public int rowsPerTerm;

    /** The number of indexed terms in the queried range, which then has {@code matchingTerms * rowsPerTerm} rows. */
    @Param({ "100", "5000" })
    public int matchingTerms;

    /** The limit of the query: a few rows, or a page. */
    @Param({ "10", "5000" })
    public int limit;

    /** Whether the bounds of the range are inclusive. */
    @Param({ "true", "false" })
    public boolean inclusive;

    /** Whether the rows are in an sstable or in the memtable. */
    @Param({ "true", "false" })
    public boolean flushed;

    private ColumnFamilyStore cfs;
    /** The memtable switches before the rows are written: the creation of an index flushes the, empty, memtable. */
    private long switchesBeforeWrites;
    private int terms;
    private String textQuery;
    private String numericQuery;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        terms = ROWS / rowsPerTerm;
        if (matchingTerms > terms - 2)
            throw new IllegalArgumentException(String.format("A range of %d terms doesn't fit in the %d terms of the index",
                                                             matchingTerms, terms));

        DatabaseDescriptor.daemonInitialization();
        // Set before any memtable, and so their memory pool, is created.
        Config config = DatabaseDescriptor.getRawConfig();
        config.memtable_heap_space = new DataStorageSpec.IntMebibytesBound(MEMTABLE_SPACE_MIB);
        config.memtable_offheap_space = new DataStorageSpec.IntMebibytesBound(MEMTABLE_SPACE_MIB);
        config.memtable_cleanup_threshold = 0.9f;
        CQLTester.setUpClass();
        beforeTest();

        // no commit log, whose size could also flush the memtable
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, t text, n bigint)");
        String qualified = keyspace + '.' + table;
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable(keyspace, table);
        switchesBeforeWrites = cfs.metric.memtableSwitchCount.getCount();

        String insert = "INSERT INTO " + qualified + " (pk, t, n) VALUES (?, ?, ?)";
        for (int i = 0; i < ROWS; i++)
        {
            int term = i % terms;
            execute(insert, i, text(term), (long) term);
        }

        checkNotFlushed("after the writes");
        if (flushed)
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            checkFlushed();
        }

        String lower = inclusive ? ">=" : ">";
        String upper = inclusive ? "<=" : "<";
        textQuery = "SELECT pk FROM " + qualified + " WHERE t " + lower + " ? AND t " + upper + " ? LIMIT " + limit;
        numericQuery = "SELECT pk FROM " + qualified + " WHERE n " + lower + " ? AND n " + upper + " ? LIMIT " + limit;

        // both queries select the expected rows, at both ends of the index
        for (int first : new int[]{ 1, terms - matchingTerms - 1 })
        {
            checkRows(execute(textQuery, text(lowerBound(first)), text(upperBound(first))));
            checkRows(execute(numericQuery, (long) lowerBound(first), (long) upperBound(first)));
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        try
        {
            // the measured queries read what the setup left
            if (flushed)
                checkFlushed();
            else
                checkNotFlushed("at the end of the benchmark");
        }
        finally
        {
            CommitLog.instance.shutdownBlocking();
            CQLTester.cleanup();
        }
    }

    @Benchmark
    public Object textRange()
    {
        int first = firstMatchingTerm();
        return execute(textQuery, text(lowerBound(first)), text(upperBound(first)));
    }

    @Benchmark
    public Object numericRange()
    {
        int first = firstMatchingTerm();
        return execute(numericQuery, (long) lowerBound(first), (long) upperBound(first));
    }

    private void checkNotFlushed(String when)
    {
        long switches = cfs.metric.memtableSwitchCount.getCount() - switchesBeforeWrites;
        int sstables = cfs.getLiveSSTables().size();
        if (switches != 0 || sstables != 0)
            throw new IllegalStateException(String.format("The memtable was flushed %s: %d memtable switches, %d sstables",
                                                          when, switches, sstables));
    }

    private void checkFlushed()
    {
        long switches = cfs.metric.memtableSwitchCount.getCount() - switchesBeforeWrites;
        int sstables = cfs.getLiveSSTables().size();
        boolean emptyMemtable = cfs.getTracker().getView().getCurrentMemtable().isClean();
        if (switches != 1 || sstables != 1 || !emptyMemtable)
            throw new IllegalStateException(String.format("The rows are not in a single sstable: %d memtable switches, %d sstables, memtable %s",
                                                          switches, sstables, emptyMemtable ? "empty" : "not empty"));
    }

    private void checkRows(UntypedResultSet rows)
    {
        int expected = Math.min(limit, matchingTerms * rowsPerTerm);
        if (rows.size() != expected)
            throw new IllegalStateException(String.format("The query returned %d rows instead of %d", rows.size(), expected));
    }

    private int firstMatchingTerm()
    {
        return 1 + ThreadLocalRandom.current().nextInt(terms - matchingTerms - 1);
    }

    private int lowerBound(int firstMatchingTerm)
    {
        return inclusive ? firstMatchingTerm : firstMatchingTerm - 1;
    }

    private int upperBound(int firstMatchingTerm)
    {
        int lastMatchingTerm = firstMatchingTerm + matchingTerms - 1;
        return inclusive ? lastMatchingTerm : lastMatchingTerm + 1;
    }

    private static String text(int number)
    {
        return String.format("%010d", number);
    }
}
