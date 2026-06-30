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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.unified.AdaptiveController;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.StaticController;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CQL tests on a table configured with Unified Compaction.
 *
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 *
 * It has properties of  both tiered and leveled compactions and it adapts to the workload
 * by switching between strategies or increasing / decreasing the fanout factor.
 *
 * The essential formulae are the calculations of buckets:
 *
 * S = ⌊log_oF(size / m)⌋ = ⌊(ln size - ln m) / (ln F + ln o)⌋
 *
 * where log_oF is the log with oF as the base
 * o is the survival factor, currently fixed to 1
 * F is the fanout factor calculated below
 * m is the minimal size, fixed in the strategy options
 * size is the sorted run size (sum of all the sizes of the sstables in the sorted run)
 *
 * Also, T is the number of sorted runs that trigger compaction.
 *
 * Give a parameter W, which is fixed in these tests, then T and F are calculated as follows:
 *
 * - W < 0 then T = 2 and F = 2 - W (leveled merge policy)
 * - W > 0 then T = F and F = 2 + W (tiered merge policy)
 * - W = 0 then T = F = 2 (middle ground)
 */
public class CQLUnifiedCompactionTest extends CQLTester
{
    @BeforeClass
    public static void beforeClass()
    {
        System.setProperty("unified_compaction.l0_shards_enabled", "true");
        CQLTester.setUpClass();
        StorageService.instance.initServer();
    }

    @After
    public void tearDown()
    {
        // This prevents unwanted flushing in future tests
        // Dirty CL segments cause memtables to be flushed after a schema change and we don't want this
        // to happen asynchronously in CQLTester.afterTest() because it would interfere with the tests
        // that rely on an exact number of sstables

        for (String table: currentTables())
        {
            logger.debug("Dropping {} synchronously to prevent unwanted flushing due to CL dirty", table);
            schemaChange(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, table));
        }

        CommitLog.instance.forceRecycleAllSegments();
    }

    @Test
    public void testCreateTable()
    {
        createTable("create table %s (id int primary key, val text) with compaction = {'class':'UnifiedCompactionStrategy'}");
        assertTrue(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);
    }

    @Test
    public void testStaticOptions()
    {
        testStaticOptions(512, 2, 50, -2);
        testStaticOptions(1024, 4, 150, 0);
        testStaticOptions(2048, 10, 250, 2);
    }

    private void testStaticOptions(int dataSetSizeGB, int numShards, int minSSTableSize, int ... Ws)
    {
        testStaticOptions(false, dataSetSizeGB, numShards, minSSTableSize, Ws);
        testStaticOptions(true, dataSetSizeGB, numShards, minSSTableSize, Ws);
    }

    private void testStaticOptions(boolean useSiUnits, long dataSetSizeGB, int numShards, long sstableSizeMB, int ... Ws)
    {
        String scalingParametersStr = String.join(",", Arrays.stream(Ws)
                                                          .mapToObj(i -> Integer.toString(i))
                                                          .collect(Collectors.toList()));

        long minSizeMB = sstableSizeMB * 10 / 15;
        createTable("create table %s (id int primary key, val text) with compaction = " +
                    "{'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', " +
                    (useSiUnits
                     ? String.format("'dataset_size' : '%s', ", FBUtilities.prettyPrintMemory(dataSetSizeGB << 30))
                     : String.format("'dataset_size_in_gb' : '%d', ", dataSetSizeGB)) +
                    String.format("'base_shard_count' : '%d', ", numShards) +
                    (useSiUnits
                     ? String.format("'min_sstable_size' : '%s', ", FBUtilities.prettyPrintMemory(minSizeMB << 20))
                     : String.format("'min_sstable_size_in_mb' : '%d', ", minSizeMB)) +
                    String.format("'target_sstable_size' : '%s', ", FBUtilities.prettyPrintMemory(sstableSizeMB << 20)) +
                    String.format("'scaling_parameters' : '%s'}", scalingParametersStr));

        CompactionStrategy strategy = getCurrentCompactionStrategy();
        assertTrue(strategy instanceof UnifiedCompactionStrategy);

        UnifiedCompactionStrategy unifiedCompactionStrategy = (UnifiedCompactionStrategy) strategy;
        Controller controller = unifiedCompactionStrategy.getController();
        assertEquals(dataSetSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards(numShards * sstableSizeMB << 20));
        assertEquals(minSizeMB << 20, controller.getMinSstableSizeBytes());
        assertEquals(sstableSizeMB << 20, controller.getTargetSSTableSize());

        assertTrue(unifiedCompactionStrategy.getController() instanceof StaticController);
        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], unifiedCompactionStrategy.getW(i));
    }

    @Test
    public void testAdaptiveOptions()
    {
        testAdaptiveOptions(512, 2, 50, -2);
        testAdaptiveOptions(1024, 4, 150, 0);
        testAdaptiveOptions(2048, 10, 250, 2);
    }

    private void testAdaptiveOptions(int dataSetSizeGB, int numShards, int sstableSizeMB, int w)
    {
        testAdaptiveOptions(false, dataSetSizeGB, numShards, sstableSizeMB, w);
        testAdaptiveOptions(true, dataSetSizeGB, numShards, sstableSizeMB, w);
    }

    private void testAdaptiveOptions(boolean useSiUnits, long dataSetSizeGB, int numShards, long sstableSizeMB, int w)
    {
        long minSizeMB = sstableSizeMB * 10 / 15;
        createTable("create table %s (id int primary key, val text) with compaction = " +
                    "{'class':'UnifiedCompactionStrategy', 'adaptive' : 'true', " +
                    (useSiUnits
                        ? String.format("'dataset_size' : '%s', ", FBUtilities.prettyPrintMemory(dataSetSizeGB << 30))
                        : String.format("'dataset_size_in_gb' : '%d', ", dataSetSizeGB)) +
                    String.format("'base_shard_count' : '%d', ", numShards) +
                    (useSiUnits
                        ? String.format("'min_sstable_size' : '%s', ", FBUtilities.prettyPrintMemory(minSizeMB << 20))
                        : String.format("'min_sstable_size_in_mb' : '%d', ", minSizeMB)) +
                    String.format("'target_sstable_size' : '%s', ", FBUtilities.prettyPrintMemory(sstableSizeMB << 20)) +
                    String.format("'scaling_parameters' : '%s', ", w) +
                    String.format("'adaptive_min_scaling_parameter' : '%s', ", -6) +
                    String.format("'adaptive_max_scaling_parameter' : '%s', ", 16) +
                    String.format("'adaptive_interval_sec': '%d', ", 300) +
                    String.format("'adaptive_threshold': '%f', ", 0.25) +
                    String.format("'max_adaptive_compactions': '%d', ", 5) +
                    String.format("'adaptive_min_cost': '%d'}", 1));

        CompactionStrategy strategy = getCurrentCompactionStrategy();
        assertTrue(strategy instanceof UnifiedCompactionStrategy);

        UnifiedCompactionStrategy unifiedCompactionStrategy = (UnifiedCompactionStrategy) strategy;

        assertTrue(unifiedCompactionStrategy.getController() instanceof AdaptiveController);
        for (int i = 0; i < 10; i++)
            assertEquals(w, unifiedCompactionStrategy.getW(i));

        AdaptiveController controller = (AdaptiveController) unifiedCompactionStrategy.getController();
        assertEquals(dataSetSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards(numShards * sstableSizeMB << 20));
        assertEquals(minSizeMB << 20, controller.getMinSstableSizeBytes());
        assertEquals(sstableSizeMB << 20, controller.getTargetSSTableSize());
        assertEquals(-6, controller.getMinScalingParameter());
        assertEquals(16, controller.getMaxScalingParameter());
        assertEquals(300, controller.getInterval());
        assertEquals(0.25, controller.getThreshold(), 0.000001);
        assertEquals(1, controller.getMinCost());
        assertEquals(5, controller.getMaxRecentAdaptiveCompactions());
    }

    @Test
    public void testAlterTable()
    {
        createTable("create table %s (id int primary key, val text) with compaction = {'class' : 'SizeTieredCompactionStrategy'}");
        assertFalse(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);

        alterTable("alter table %s with compaction = {'class' : 'UnifiedCompactionStrategy', 'adaptive' : 'true'}");
        assertTrue(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);
        assertTrue(((UnifiedCompactionStrategy) getCurrentCompactionStrategy()).getController() instanceof AdaptiveController);

        alterTable("alter table %s with compaction = {'class' : 'UnifiedCompactionStrategy', 'adaptive' : 'false'}");
        assertTrue(getCurrentCompactionStrategy() instanceof UnifiedCompactionStrategy);
        assertTrue(((UnifiedCompactionStrategy) getCurrentCompactionStrategy()).getController() instanceof StaticController);
    }

    @Test
    public void testSingleCompaction() throws Throwable
    {
        testSingleCompaction(4, 6); // W = 4 => T = 6 sstables required to trigger a compaction, see doc for formula
        testSingleCompaction(2, 4); // W = 2 => T = 4
        testSingleCompaction(0, 2); // W = 0 => T = 2
        testSingleCompaction(-2, 2); // W = -2 => T = 2
        testSingleCompaction(-4, 2); // W = -4 => T = 2
    }

    private void testSingleCompaction(int W, int T) throws Throwable
    {
        // Start with sstables whose size is minimal_size_in_mb, 1mb, ensure that there are no overlaps between sstables
        int numInserts = 1024;
        int valSize = 1024;

        createTable("create table %s (id int primary key, val blob) with compaction = {'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', " +
                    String.format("'scaling_parameters' : '%d', 'min_sstable_size_in_mb' : '1', 'base_shard_count': '1', 'log_all' : 'true'}", W));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        assertEquals(0, cfs.getLiveSSTables().size());

        int key = 0;
        ByteBuffer val = ByteBuffer.wrap(new byte[valSize]);
        for (int i = 0; i < T; i++)
            key = insertAndFlush(numInserts, key, val);

        int expectedInserts = numInserts * T;

        assertEquals(T, cfs.getLiveSSTables().size());
        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);

        cfs.enableAutoCompaction(true);

        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);
    }

    @Test
    public void testMultipleCompactionsSingleW_Static() throws Throwable
    {
        // tiered tests with W = 2 and T = F = 4
        testMultipleCompactions(4, 1, 1, new int[] {2});  //  4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 1, new int[] {2});  //  8 sstables should be compacted into 1
        testMultipleCompactions(16, 1, 1, new int[] {2}); // 16 sstables should be compacted into 1

        // middle-point tests between tiered and leveled with W = 0, T = F = 2
        testMultipleCompactions(2, 1, 1, new int[] {0});   // 2 sstables should be compacted into 1
        testMultipleCompactions(4, 1, 1, new int[] {0});   // 4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 1, new int[] {0});   // 2 sstables should be compacted into 1
        testMultipleCompactions(16, 1, 1, new int[] {0});  // 16 sstables should be compacted into 1

        // leveled tests with W = -2 and T = 2, F = 4
        testMultipleCompactions(2, 1, 1, new int[] {-2});  //  2 sstables should be compacted into 1
        testMultipleCompactions(4, 1, 1, new int[] {-2});  //  4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 1, new int[] {-2});  //  8 sstables should be compacted into 1
        testMultipleCompactions(9, 1, 1, new int[] {-2});  //  9 sstables should be compacted into 2
        testMultipleCompactions(16, 1, 1, new int[] {-2}); // 12 sstables should be compacted into 1
    }

    @Test
    public void testMultipleCompactionsDifferentWs_Static() throws Throwable
    {
        // tiered tests with W = [4, -6] and T = [6, 2], F = [6, 8]
        testMultipleCompactions(12, 1, 1, new int[] {4, -6});  //  sstables: 12 -> (6,6) => 2 => 1

        // tiered tests with W = [30, 2, -6] and T = [32, 4, 2], F = [32, 4, 8]
        testMultipleCompactions(128, 1, 1, new int[] {30, 2, -6});  //  sstables: 128 -> (32,32, 32, 32) => 4 => (4) => 1
    }

    @Test
    public void testMultipleCompactionsSingleW_TwoShards() throws Throwable
    {
        testMultipleCompactions(4, 1, 2, new int[]{2});  //  4 sstables should be compacted into 1
        testMultipleCompactions(8, 1, 2, new int[]{2});  //  8 sstables should be compacted into 1
    }

    private void testMultipleCompactions(int numInitialSSTables, int numFinalSSTables, int numShards, int[] Ws) throws Throwable
    {
        int numInserts = 1024 * numShards;
        int valSize = 2048;

        String scalingParamsStr = Arrays.stream(Ws)
                                        .mapToObj(Integer::toString)
                                        .collect(Collectors.joining(","));

        createTable("create table %s (id int primary key, val blob) with compression = { 'enabled' : false } AND " +
                    "compaction = {'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', 'max_sstables_to_compact': 256, " +
                    String.format("'scaling_parameters' : '%s', 'min_sstable_size' : '0B', 'base_shard_count': '%d', 'log_all' : 'true'}",
                                  scalingParamsStr, numShards));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int key = 0;
        byte[] bytes = new byte[valSize];
        (new Random(87652)).nextBytes(bytes);
        ByteBuffer val = ByteBuffer.wrap(bytes);

        for (int i = 0; i < numInitialSSTables; i++)
            key = insertAndFlush(numInserts, key, val);

        int expectedInserts = numInserts * numInitialSSTables;

        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);
        assertEquals(numInitialSSTables * numShards, cfs.getLiveSSTables().size());

        // trigger a compaction, wait for the future because otherwise the check below
        // may be called before the strategy has executed getNextBackgroundTask()
        cfs.enableAutoCompaction(true);

        int numChecks = 0;
        int numTimesWithNoCompactions = 0;
        while(numTimesWithNoCompactions < 10  && numChecks < 1500) // 15 seconds
        {
            // check multiple times because we don't look ahead to future buckets at the moment so there is a brief
            // window without pending compactions and without compactions in progress, this may make the test flaky on slow J2
            if (cfs.getCompactionStrategy().getTotalCompactions() == 0)
                numTimesWithNoCompactions++;

            FBUtilities.sleepQuietly(10);
            numChecks++;
        }

        assertEquals(expectedInserts, getRows(execute("SELECT * FROM %s")).length);
        assertEquals(numFinalSSTables * numShards, cfs.getLiveSSTables().size());
    }

    private int insertAndFlush(int numInserts, int key, ByteBuffer val) throws Throwable
    {
        for (int i = 0; i < numInserts; i++)
            execute("INSERT INTO %s (id, val) VALUES(?,?)", key++, val);

        flush();
        return key;
    }

    private CompactionStrategy getCurrentCompactionStrategy()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        return cfs.getCompactionStrategyContainer()
                  .getStrategies()
                  .get(0);
    }

    @Test
    public void testTimeDrivenLevels() throws Throwable
    {
        createTable("create table %s (id int primary key, val text) with compaction = " +
                    "{'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', 'num_shards': '1', 'min_sstable_size': '0B', " +
                    "'scaling_parameters': 'T2 until 10m; T100'}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // 1. Insert 4 old SSTables (20 minutes ago)
        long nowUs = System.currentTimeMillis() * 1000L;
        long oldTimestampUs = nowUs - TimeUnit.MINUTES.toMicros(20);

        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (id, val) VALUES (?, ?) USING TIMESTAMP ?", 0, "old_" + i, oldTimestampUs);
            flush();
        }

        assertEquals(4, cfs.getLiveSSTables().size());

        // Enable auto-compaction and wait briefly to ensure no compaction is triggered on the old SSTables
        cfs.enableAutoCompaction(true);
        FBUtilities.sleepQuietly(500);
        assertEquals(4, cfs.getLiveSSTables().size());

        // 2. Insert 4 fresh SSTables (current time)
        cfs.disableAutoCompaction();
        long newTimestampUs = System.currentTimeMillis() * 1000L;
        for (int i = 4; i < 8; i++)
        {
            execute("INSERT INTO %s (id, val) VALUES (?, ?) USING TIMESTAMP ?", 4, "new_" + i, newTimestampUs);
            flush();
        }

        assertEquals(8, cfs.getLiveSSTables().size());

        // Enable auto-compaction: the 4 fresh SSTables fall into the 'until_10m' arena using T2 (threshold=2),
        // so they must trigger compaction, while the 4 old SSTables in T100 arena should not compact.
        cfs.enableAutoCompaction(true);

        int numChecks = 0;
        while (cfs.getLiveSSTables().size() > 5 && numChecks < 200) // up to 2 seconds
        {
            FBUtilities.sleepQuietly(10);
            numChecks++;
        }

        // Final state: 4 old SSTables + 1 compacted fresh SSTable = 5 SSTables
        assertEquals(5, cfs.getLiveSSTables().size());
    }

    /**
     * Tests that compaction is triggered after a scheduled time has elapsed.
     * <p>
     * Configures a table with {@code T100 until 3s; T2}.
     * Initially inserts 4 fresh SSTables. Since they are fresh (< 3-second age),
     * they match {@code T100} and do not trigger compaction.
     * After sleeping for 4 seconds, the SSTables age past the 3-second threshold,
     * matching {@code T2}, and triggering compaction.
     * </p>
     *
     * @throws Throwable if execution fails
     */
    @Test
    public void testScheduledTimeCompaction() throws Throwable
    {
        createTable("create table %s (id int primary key, val text) with compaction = " +
                    "{'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', 'num_shards': '1', 'min_sstable_size': '0B', " +
                    "'scaling_parameters': 'T100 until 3s; T2'}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // 1. Insert 4 fresh SSTables
        long newTimestampUs = System.currentTimeMillis() * 1000L;
        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (id, val) VALUES (?, ?) USING TIMESTAMP ?", 0, "val_" + i, newTimestampUs);
            flush();
        }

        assertEquals(4, cfs.getLiveSSTables().size());

        // Enable auto-compaction and wait briefly to ensure no compaction is triggered on the fresh SSTables
        cfs.enableAutoCompaction(true);
        FBUtilities.sleepQuietly(500);
        assertEquals(4, cfs.getLiveSSTables().size());

        // Disable auto-compaction to control the timing
        cfs.disableAutoCompaction();

        // 2. Sleep for 4 seconds to let the SSTables age past 3 seconds
        FBUtilities.sleepQuietly(4000);

        // Enable auto-compaction again: the 4 SSTables should now fall into the T2 arena and trigger compaction
        cfs.enableAutoCompaction(true);

        int numChecks = 0;
        while (cfs.getLiveSSTables().size() > 1 && numChecks < 200) // up to 2 seconds
        {
            FBUtilities.sleepQuietly(10);
            numChecks++;
        }

        // Final state: 1 compacted SSTable
        assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void testHolidayCaseDynamicBaseSizeAdjustment() throws Throwable
    {
        createTable("create table %s (id int primary key, val blob) with compression = { 'enabled' : false } AND " +
                    "compaction = {'class':'UnifiedCompactionStrategy', 'adaptive' : 'false', " +
                    "'target_sstable_size': '1MiB', 'scaling_parameters': 'T2 until 2s; T2'}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int key = 0;
        byte[] bytes = new byte[2048];
        new Random(87652).nextBytes(bytes);
        ByteBuffer val = ByteBuffer.wrap(bytes);

        // First SSTable: ~2.5MiB (1300 rows).
        long oldTimestampUs = (System.currentTimeMillis() * 1000L) - TimeUnit.MINUTES.toMicros(20);
        for (int i = 0; i < 1300; i++)
            execute("INSERT INTO %s (id, val) VALUES (?, ?) USING TIMESTAMP ?", key++, val, oldTimestampUs);
        flush();

        // Second SSTable: ~400KiB (200 rows).
        for (int i = 0; i < 200; i++)
            execute("INSERT INTO %s (id, val) VALUES (?, ?) USING TIMESTAMP ?", key++, val, oldTimestampUs);
        flush();

        assertEquals(2, cfs.getLiveSSTables().size());

        // Enable auto-compaction temporarily to verify that they do NOT compact yet,
        // since the 2.5MiB SSTable is on Level 1 (range 2MiB-4MiB) and the 400KiB SSTable is on Level 0 of the base arena.
        cfs.enableAutoCompaction(true);
        FBUtilities.sleepQuietly(500);
        assertEquals(2, cfs.getLiveSSTables().size());

        // Disable auto-compaction to write the fresh younger SSTable.
        cfs.disableAutoCompaction();

        // 2. Create a fresh younger SSTable that is large: ~5.1MiB (2600 rows).
        // Since it has the current timestamp, it falls into the younger 'until 2s' bucket.
        long newTimestampUs = System.currentTimeMillis() * 1000L;
        for (int i = 0; i < 2600; i++)
            execute("INSERT INTO %s (id, val) VALUES (?, ?) USING TIMESTAMP ?", key++, val, newTimestampUs);
        flush();

        assertEquals(3, cfs.getLiveSSTables().size());

        //  The fresh 400KiB SSTable is in the younger bucket, making youngerMaxDensity = 400KiB.
        //  The base sstable size of the older bucket is adjusted to 400KiB.
        //  This shifts the 150KiB SSTable from Level 1 to Level 0.
        //  Now Level 0 of the older bucket contains 2 SSTables (150KiB and 40KiB).
        //  Since 2 >= threshold (2), they will be compacted together!
        cfs.enableAutoCompaction(true);

        int numChecks = 0;
        while (cfs.getLiveSSTables().size() > 2 && numChecks < 200) // up to 2 seconds
        {
            FBUtilities.sleepQuietly(10);
            numChecks++;
        }

        // Final state: 1 compacted older SSTable + 1 fresh younger SSTable = 2 SSTables
        assertEquals(2, cfs.getLiveSSTables().size());
    }
}
