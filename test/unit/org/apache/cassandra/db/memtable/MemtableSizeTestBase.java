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

import java.lang.reflect.Field;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.SlabAllocator;
import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.Guess;

import static org.assertj.core.api.Assertions.assertThat;

// Note: This test can be run in idea with the allocation type configured in the test yaml and memtable using the
// value memtableClass is initialized with.
@RunWith(Parameterized.class)
public abstract class MemtableSizeTestBase extends CQLTester
{
    // Note: To see a printout of the usage for each object, add .printVisitedTree() here (most useful with smaller number of
    // partitions).
    static MemoryMeter meter =  MemoryMeter.builder()
                                           .withGuessing(Guess.INSTRUMENTATION_AND_SPECIFICATION,
                                                         Guess.UNSAFE)
//                                           .printVisitedTreeUpTo(1000)
                                           .build();

    static final Logger logger = LoggerFactory.getLogger(MemtableSizeTestBase.class);

    static String keyspace;
    String table;
    ColumnFamilyStore cfs;

    static final int partitions = 50_000;
    static final int rowsPerPartition = 4;

    static final int deletedPartitions = 10_000;
    static final int deletedRows = 5_000;

    @Parameterized.Parameter(0)
    public String memtableClass = "skiplist";

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("skiplist",
                                "skiplist_sharded",
                                "trie");
    }

    // Must be within 3% of the real usage. We are actually more precise than this, but the threshold is set higher to
    // avoid flakes. For on-heap allocators we allow for extra overheads below.
    final int MAX_DIFFERENCE_PERCENT = 3;
    // Extra leniency for unslabbed buffers. We are not as precise there, and it's not a mode in real use.
    final int UNSLABBED_EXTRA_PERCENT = 2;

    public static void setup(Config.MemtableAllocationType allocationType)
    {
        ServerTestUtils.daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.memtable_allocation_type = allocationType;
            conf.memtable_cleanup_threshold = 0.8f; // give us more space to fit test data without flushing
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }

        CQLTester.setUpClass();
        CQLTester.prepareServer();
        logger.info("setupClass done, allocation type {}", allocationType);
    }

    void checkMemtablePool()
    {
        // overridden by instances
    }

    private void buildAndFillTable(String memtableClass) throws Throwable
    {
        // Make sure memtables use the correct allocation type, i.e. that setup has worked.
        // If this fails, make sure the test is not reusing an already-initialized JVM.
        checkMemtablePool();

        CQLTester.disablePreparedReuseForTest();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");

        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                      " with compression = {'enabled': false}" +
                                      " and memtable = { 'class': '" + memtableClass + "'}");
        execute("use " + keyspace + ';');

        forcePreparedValues();

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        Util.flush(cfs);
    }

    @Test
    public void testSize() throws Throwable
    {

        try
        {
            buildAndFillTable(memtableClass);

            String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";

            Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
            long deepSizeBefore = meter.measureDeep(memtable);
            System.out.println("Memtable deep size before " +
                               FBUtilities.prettyPrintMemory(deepSizeBefore));

            long i;
            long limit = partitions;
            logger.info("Writing {} partitions of {} rows", partitions, rowsPerPartition);
            for (i = 0; i < limit; ++i)
            {
                for (long j = 0; j < rowsPerPartition; ++j)
                    execute(writeStatement, i, j, i + j);
            }

            logger.info("Deleting {} partitions", deletedPartitions);
            limit += deletedPartitions;
            for (; i < limit; ++i)
            {
                // no partition exists, but we will create a tombstone
                execute("DELETE FROM " + table + " WHERE userid = ?", i);
            }

            logger.info("Deleting {} rows", deletedRows);
            limit += deletedRows;
            for (; i < limit; ++i)
            {
                // no row exists, but we will create a tombstone (and partition)
                execute("DELETE FROM " + table + " WHERE userid = ? AND picid = ?", i, 0L);
            }

            Assert.assertSame("Memtable flushed during test. Test was not carried out correctly.",
                              memtable,
                              cfs.getTracker().getView().getCurrentMemtable());

            Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
            long calculatedHeap = usage.ownsOnHeap;
            logger.info(String.format("Memtable in %s mode: %d ops, %s serialized bytes, %s",
                                      DatabaseDescriptor.getMemtableAllocationType(),
                                      memtable.operationCount(),
                                      FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                                      usage));

            long deepSizeAfter = meter.measureDeep(memtable);
            logger.info("Memtable deep size {}", FBUtilities.prettyPrintMemory(deepSizeAfter));

            long actualHeap = deepSizeAfter - deepSizeBefore;
            long maxDifference = MAX_DIFFERENCE_PERCENT * actualHeap / 100;
            long trieOverhead = memtable instanceof TrieMemtable ? ((TrieMemtable) memtable).unusedReservedMemory() : 0;
            calculatedHeap += trieOverhead;    // adjust trie memory with unused buffer space if on-heap
            switch (DatabaseDescriptor.getMemtableAllocationType())
            {
                case heap_buffers:
                    // MemoryUsage only counts the memory actually used by cells,
                    // so add in the slab overhead to match what MemoryMeter sees
                    int slabCount = memtable instanceof TrieMemtable ? ((TrieMemtable) memtable).getShardCount() : 1;
                    maxDifference += (long) SlabAllocator.REGION_SIZE * slabCount;
                    break;
                case unslabbed_heap_buffers:
                    // add a hardcoded slack factor
                    maxDifference += actualHeap * UNSLABBED_EXTRA_PERCENT / 100;
                    break;
            }
            String message = String.format("Actual heap usage is %s, got %s, %s difference.\n",
                                           FBUtilities.prettyPrintMemory(actualHeap),
                                           FBUtilities.prettyPrintMemory(calculatedHeap),
                                           FBUtilities.prettyPrintMemory(actualHeap - calculatedHeap));
            logger.info(message);
            Assert.assertTrue(message, Math.abs(calculatedHeap - actualHeap) <= maxDifference);
        }
        finally
        {
            execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspace));
        }
    }

    @Test
    public void testRowCountInTrieMemtable() throws Throwable
    {
        buildAndFillTable("TrieMemtable");

        String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        System.out.println("Writing " + partitions + " partitions of " + rowsPerPartition + " rows");
        for (long i = 0; i < partitions; ++i)
        {
            for (long j = 0; j < rowsPerPartition; ++j)
                execute(writeStatement, i, j, i + j);
        }

        assertThat(memtable).isExactlyInstanceOf(TrieMemtable.class);
        ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(cfs.metadata(), true);
        long rowCount = ((TrieMemtable)cfs.getTracker().getView().getCurrentMemtable()).rowCount(builder.build(), DataRange.allData(cfs.getPartitioner()));
        Assert.assertEquals(rowCount, partitions*rowsPerPartition);
    }

    @Test
    public void testRowSize() throws Throwable
    {
        buildAndFillTable(memtableClass);

        String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        System.out.println("Writing " + partitions + " partitions of " + rowsPerPartition + " rows");
        for (long i = 0; i < partitions; ++i)
        {
            for (long j = 0; j < rowsPerPartition; ++j)
                execute(writeStatement, i, j, i + j);
        }

        long rowSize = memtable.getEstimatedAverageRowSize();
        double expectedRowSize = (double) memtable.getLiveDataSize() / (partitions * rowsPerPartition);
        Assert.assertEquals(expectedRowSize, rowSize, (partitions * rowsPerPartition) * 0.05);  // 5% accuracy
    }
}
