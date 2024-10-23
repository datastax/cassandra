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

package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

/// This test is a counterpart to InMemoryTrieThreadedTest that makes sure TrieMemtable is wiring the trie consistency
/// machinery correctly. Note that this test always applies mutations the same way (with partition-level forced copying)
/// and is effectively doing the same test but checking different correctness properties.
///
/// A problem with this will only appear as intermittent failures, never treat this test as flaky.
@RunWith(Parameterized.class)
public class MemtableThreadedTest extends CQLTester
{
    @Parameterized.Parameter()
    public String memtableClass;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("SkipListMemtable",
                                "TrieMemtable",
                                "TrieMemtableStage1",
                                "PersistentMemoryMemtable");
    }

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        CQLTester.disablePreparedReuseForTest();
        System.err.println("setupClass done.");
    }

    static String keyspace;
    String table;
    ColumnFamilyStore cfs;

    private static final int COUNT = 45678;
    private static final int PROGRESS_UPDATE = COUNT / 15;
    private static final int READERS = 17;
    private static final int WALKERS = 3;

    @Test
    public void testConsistentAndAtomicUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // Check that multi-row mutations are safe for concurrent readers,
        // and that content is atomically applied, i.e. that readers see either nothing from the update or all of it,
        // and consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(3, true, true);
        // Note: using 3 per mutation, so that the first and second update fit in a sparse in-memory trie block.
    }

    @Test
    public void testConsistentUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // Check that multi-row mutations are safe for concurrent readers,
        // and that content is consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(3, false, true);
        // Note: using 3 per mutation, so that the first and second update fit in a sparse in-memory trie block.
    }

    @Test
    public void testAtomicUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // Check that multi-row mutations are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it.
        testAtomicUpdates(3, true, false);
    }

    @Test
    public void testSafeUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // Check that multi row mutations are safe for concurrent readers.
        testAtomicUpdates(3, false, false);
    }

    @Test
    public void testConsistentAndAtomicSinglePathUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // Check that single row mutations are safe for concurrent readers,
        // and that content is atomically applied, i.e. that readers see either nothing from the update or all of it,
        // and consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(1, true, true);
    }

    @Test
    public void testConsistentSinglePathUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // Check that single row mutations are safe for concurrent readers,
        // and that content is consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(1, false, true);
    }

    @Test
    public void testAtomicSinglePathUpdates() throws Exception
    {
        // Note: Intermittent failures of this test other than timeout should be treated as a bug.

        // When doing single path updates atomicity comes for free. This only checks that the branching checker is
        // not doing anything funny.
        testAtomicUpdates(1, true, false);
    }

    @Test
    public void testSafeSinglePathUpdates() throws Exception
    {
        // Check that single path updates without additional copying are safe for concurrent readers.
        testAtomicUpdates(1, true, false);
    }

    public void testAtomicUpdates(int PER_MUTATION,
                                  boolean checkAtomicity,
                                  boolean checkSequence)
    throws Exception
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, ck bigint, value bigint, seq bigint, PRIMARY KEY(pk, ck))" +
                                      " with compression = {'enabled': false}" +
                                      " and memtable = { 'class': '" + memtableClass + "'}" +
                                      " and compaction = { 'class': 'UnifiedCompactionStrategy', 'min_sstable_size_in_mb': '1' }"); // to trigger splitting of sstables, STAR-1826
        execute("use " + keyspace + ';');

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(UNIT_TESTS);

        int ckCount = COUNT;
        int pkCount = Math.min(100, COUNT / 10);  // to guarantee repetition

        String ws;
        if (PER_MUTATION == 1)
            ws = "INSERT INTO " + table + "(pk,ck,value,seq) VALUES (?,?,?,?)";
        else
        {
            ws = "BEGIN UNLOGGED BATCH\n";
            for (int i = 0; i < PER_MUTATION; ++i)
                ws += "INSERT INTO " + table + "(pk,ck,value,seq) VALUES (?,?,?,?)\n";
            ws += "APPLY BATCH";
        }
        String writeStatement = ws;

        /*
         * Adds COUNT partitions each with perPartition separate clusterings, where the sum of the values
         * of all clusterings is 0.
         * If the sum for any walk covering whole partitions is non-zero, we have had non-atomic updates.
         */

        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<Thread>();
        AtomicBoolean writeCompleted = new AtomicBoolean(false);
        AtomicInteger writeProgress = new AtomicInteger(0);

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        while (!writeCompleted.get())
                        {
                            int min = writeProgress.get();
                            var results = execute("SELECT * FROM " + table);
                            checkEntries("", min, checkAtomicity, checkSequence, PER_MUTATION, results);
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });

        for (int i = 0; i < READERS; ++i)
        {
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        // await at least one ready partition
                        while (writeProgress.get() == 0) {}

                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted.get())
                        {
                            long pk = r.nextInt(pkCount);
                            int min = writeProgress.get() / (pkCount * PER_MUTATION) * PER_MUTATION;
                            var results = execute("SELECT * FROM " + table + " WHERE pk = ?", pk);
                            checkEntries(" in partition " + pk, min, checkAtomicity, checkSequence, PER_MUTATION, results);
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });
        }

        threads.add(new Thread()
        {
            public void run()
            {
                try
                {
                    int lastUpdate = 0;
                    Object[] values = new Object[PER_MUTATION * 4];

                    for (int i = 0; i < COUNT; i += PER_MUTATION)
                    {
                        long pk = (i / PER_MUTATION) % pkCount;
                        int vidx = 0;
                        for (int j = 0; j < PER_MUTATION; ++j)
                        {

                            long ck = i + j;
                            long value = j == 0 ? -PER_MUTATION + 1 : 1;
                            long seq = (i / PER_MUTATION / pkCount) * PER_MUTATION + j;
                            values[vidx++] = pk;
                            values[vidx++] = ck;
                            values[vidx++] = value;
                            values[vidx++] = seq;
                        }
                        execute(writeStatement, values);

                        if (i >= pkCount * PER_MUTATION && i - lastUpdate >= PROGRESS_UPDATE)
                        {
                            writeProgress.set(i);
                            lastUpdate = i;
                        }
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
                finally
                {
                    writeCompleted.set(true);
                }
            }
        });

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }

    public void checkEntries(String location,
                             int min,
                             boolean checkAtomicity,
                             boolean checkConsecutiveIds,
                             int PER_MUTATION,
                             UntypedResultSet entries)
    {
        long sum = 0;
        int count = 0;
        long idSum = 0;
        long idMax = 0;
        int currentPk = -1;
        for (var en : entries)
        {
            long pk = en.getLong("pk");
            if (pk != currentPk)
            {
                currentPk = (int) pk;
                idMax = idSum = sum = 0;
            }
            ++count;
            sum += en.getLong("value");
            long seq = en.getLong("seq");
            idSum += seq;
            if (seq > idMax)
                idMax = seq;
        }

        Assert.assertTrue("Values" + location + " should be at least " + min + ", got " + count, min <= count);

        if (checkAtomicity)
        {
            // If mutations apply atomically, the row count is always a multiple of the mutation size...
            Assert.assertTrue("Values" + location + " should be a multiple of " + PER_MUTATION + ", got " + count, count % PER_MUTATION == 0);
            // ... and the sum of the values is 0 (as the sum for each individual mutation is 0).
            Assert.assertEquals("Value sum" + location, 0, sum);
        }

        if (checkConsecutiveIds)
        {
            // If mutations apply consistently for the partition, for any row we see we have to have seen all rows that
            // were applied before that. In other words, the id sum should be the sum of the integers from 1 to the
            // highest id seen in the partition.
            Assert.assertEquals("Id sum" + location, idMax * (idMax + 1) / 2, idSum);
        }
    }
}
