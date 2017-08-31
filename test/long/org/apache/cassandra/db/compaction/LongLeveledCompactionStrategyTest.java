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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Lists;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LongLeveledCompactionStrategyTest
{
    public static final String KEYSPACE1 = "LongLeveledCompactionStrategyTest";
    public static final String CF_STANDARDLVL = "StandardLeveled";
    public static final String CF_STANDARDLVL2 = "StandardLeveled2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLVL)
                                                .compaction(CompactionParams.lcs(leveledOptions)),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLVL2)
                                                .compaction(CompactionParams.lcs(leveledOptions)));
    }

    @Test
    public void testParallelLeveledCompaction() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        store.disableAutoCompaction();

        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) store.getCompactionStrategyManager().getStrategies().get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) store.getCompactionStrategyManager().getStrategies().get(1);

        // Enough data to have a level 1 and 2
        insertData(store, 32, 10);

        // Execute LCS in parallel
        ExecutorService executor = new ThreadPoolExecutor(4, 4,
                                                          Long.MAX_VALUE, TimeUnit.SECONDS,
                                                          new LinkedBlockingDeque<Runnable>());
        compact(unrepaired, executor);

        // Assert all SSTables are lined up correctly.
        checkLevels(unrepaired.manifest);

        // Make sure there are no repaired SSTables
        checkEmpty(repaired);

        // Mark all SSTables as repaired
        store.mutateRepairedAt(store.getLiveSSTables(), System.currentTimeMillis());

        // Make sure there are no unrepaired sstables
        checkEmpty(unrepaired);

        // Check repaired levels
        checkLevels(repaired.manifest);

        //Check that all repaired SSTables are either in L1 or L2
        assertTrue(repaired.manifest.getLevel(0).size() == 0);
        assertTrue(repaired.manifest.getLevel(1).size() > 0);
        assertTrue(repaired.manifest.getLevel(2).size() > 0);

        // Insert more unrepaired data - enough to have L1 but not L2
        insertData(store, 22, 4);

        compact(unrepaired, executor);

        // Assert all SSTables are lined up correctly.
        checkLevels(unrepaired.manifest);

        //Check that all unrepaired SSTables are in L1
        assertEquals(0, unrepaired.manifest.getLevel(0).size());
        assertTrue(unrepaired.manifest.getLevel(1).size() > 0);
        assertEquals(0, unrepaired.manifest.getLevel(2).size());

        List<SSTableReader> previousRepairedL1 = new ArrayList<>(repaired.manifest.getLevel(1));
        List<SSTableReader> previousRepairedL2 = new ArrayList<>(repaired.manifest.getLevel(2));
        List<SSTableReader> previousUnrepairedL1 = new ArrayList<>(unrepaired.manifest.getLevel(1));

        //Call forceMarkAllSSTablesAsUnrepaired
        StorageService.instance.forceMarkAllSSTablesAsUnrepaired(ksname, cfname);

        // Make sure there are no repaired SSTables
        checkEmpty(repaired);

        //Check that all unrepaired SSTables are in L0, L1 or L2
        assertTrue(unrepaired.manifest.getLevel(0).size() > 0);
        assertTrue(unrepaired.manifest.getLevel(1).size() > 0);
        assertTrue(unrepaired.manifest.getLevel(2).size() > 0);

        //After marking sstables as unrepaired, unrepaired L1 should remain unchanged
        assertTrue(previousUnrepairedL1.stream().allMatch(s -> s.getSSTableLevel() == 1));

        //repaired L1 should be dropped to L0 due to overlap with unrepaired L1
        previousRepairedL1.stream().forEach(s -> System.out.println(s + " " + s.getSSTableLevel()));
        assertTrue(previousRepairedL1.stream().allMatch(s -> s.getSSTableLevel() == 0));

        //repaired L2 should remain unchanged since there is no overlap with repaired L2
        assertTrue(previousRepairedL2.stream().allMatch(s -> s.getSSTableLevel() == 2));

        //now compact unrepaired
        compact(unrepaired, executor);

        // Assert all SSTables are lined up correctly.
        checkLevels(unrepaired.manifest);

        //Check that all repaired SSTables are either in L1 or L2
        assertTrue(unrepaired.manifest.getLevel(0).size() == 0);
        assertTrue(unrepaired.manifest.getLevel(1).size() > 0);
        assertTrue(unrepaired.manifest.getLevel(2).size() > 0);

        // Make sure again there are no repaired SSTables
        checkEmpty(repaired);
    }

    private void insertData(ColumnFamilyStore store, int rows, int columns)
    {
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            UpdateBuilder builder = UpdateBuilder.create(store.metadata, key);
            for (int c = 0; c < columns; c++)
                builder.newRow("column" + c).add("val", value);

            Mutation rm = new Mutation(builder.build());
            rm.apply();
            store.forceBlockingFlush();
        }
    }

    private void compact(LeveledCompactionStrategy lcs, ExecutorService executor)
    {
        List<Runnable> tasks = new ArrayList<Runnable>();
        while (true)
        {
            while (true)
            {
                final AbstractCompactionTask nextTask = lcs.getNextBackgroundTask(Integer.MIN_VALUE);
                if (nextTask == null)
                    break;
                tasks.add(new Runnable()
                {
                    public void run()
                    {
                        nextTask.execute(null);
                    }
                });
            }
            if (tasks.isEmpty())
                break;

            List<Future<?>> futures = new ArrayList<Future<?>>(tasks.size());
            for (Runnable r : tasks)
                futures.add(executor.submit(r));
            FBUtilities.waitOnFutures(futures);

            tasks.clear();
        }
    }

    private void checkEmpty(LeveledCompactionStrategy repaired)
    {
        for (Integer size : repaired.getAllLevelSize())
        {
            assertEquals((Integer)0, size);
        }
    }

    private void checkLevels(LeveledManifest manifest)
    {
        int levels = manifest.getLevelCount();
        for (int level = 0; level < levels; level++)
        {
            List<SSTableReader> sstables = manifest.getLevel(level);
            // score check
            assert (double) SSTableReader.getTotalBytes(sstables) / LeveledManifest.maxBytesForLevel(level, 1 * 1024 * 1024) < 1.00;
            // overlap check for levels greater than 0
            for (SSTableReader sstable : sstables)
            {
                // level check
                assert level == sstable.getSSTableLevel();

                if (level > 0)
                {// overlap check for levels greater than 0
                    Set<SSTableReader> overlaps = LeveledManifest.overlapping(sstable, sstables);
                    assert overlaps.size() == 1 && overlaps.contains(sstable);
                }
            }
        }
    }

    @Test
    public void testLeveledScanner() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARDLVL2);
        store.disableAutoCompaction();

        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy)store.getCompactionStrategyManager().getStrategies().get(1);

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Adds 10 partitions
        for (int r = 0; r < 10; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            UpdateBuilder builder = UpdateBuilder.create(store.metadata, key);
            for (int c = 0; c < 10; c++)
                builder.newRow("column" + c).add("val", value);

            Mutation rm = new Mutation(builder.build());
            rm.apply();
        }

        //Flush sstable
        store.forceBlockingFlush();

        store.runWithCompactionsDisabled(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                Iterable<SSTableReader> allSSTables = store.getSSTables(SSTableSet.LIVE);
                for (SSTableReader sstable : allSSTables)
                {
                    if (sstable.getSSTableLevel() == 0)
                    {
                        System.out.println("Mutating L0-SSTABLE level to L1 to simulate a bug: " + sstable.getFilename());
                        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
                        sstable.reloadSSTableMetadata();
                    }
                }

                try (AbstractCompactionStrategy.ScannerList scannerList = lcs.getScanners(Lists.newArrayList(allSSTables)))
                {
                    //Verify that leveled scanners will always iterate in ascending order (CASSANDRA-9935)
                    for (ISSTableScanner scanner : scannerList.scanners)
                    {
                        DecoratedKey lastKey = null;
                        while (scanner.hasNext())
                        {
                            UnfilteredRowIterator row = scanner.next();
                            if (lastKey != null)
                            {
                                assertTrue("row " + row.partitionKey() + " received out of order wrt " + lastKey, row.partitionKey().compareTo(lastKey) >= 0);
                            }
                            lastKey = row.partitionKey();
                        }
                    }
                }
                return null;
            }
        }, true, true);


    }
}
