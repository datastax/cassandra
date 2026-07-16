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

package org.apache.cassandra.db.compaction.unified;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.db.compaction.ArenaSelector;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class StaticControllerTest extends ControllerTest
{
    static final int[] Ws = new int[] { 30, 2, 0, -6};

    @Test
    public void testFromOptions()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(false, options);

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));
    }

    private static void addOptions(boolean useIntegers, Map<String, String> options)
    {
        String wStr = Arrays.stream(Ws)
                            .mapToObj(useIntegers ? Integer::toString : UnifiedCompactionStrategy::printScalingParameter)
                            .collect(Collectors.joining(","));
        options.put(StaticController.SCALING_PARAMETERS_OPTION, wStr);
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");
    }

    @Test
    public void testFromOptionsIntegers()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(true, options);

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);


        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));
    }

    @Test
    public void testFromOptionsIntegersDeprecatedName()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(true, options);
        options.put(StaticController.STATIC_SCALING_FACTORS_OPTION,
                    options.remove(StaticController.SCALING_PARAMETERS_OPTION));

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);


        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));
    }

    @Test
    public void testFromOptionsVectorTable()
    {
        useVector = true;
        Map<String, String> options = new HashMap<>();
        Controller controller = Controller.fromOptions(cfs, new HashMap<>());
        assertNotNull(controller);
        assertNotNull(controller.toString());

        assertEquals(Controller.DEFAULT_VECTOR_BASE_SHARD_COUNT, controller.baseShardCount);
        assertEquals(Controller.DEFAULT_VECTOR_SSTABLE_GROWTH, controller.sstableGrowthModifier, 0.01);
        assertEquals(Controller.DEFAULT_VECTOR_MIN_SSTABLE_SIZE, controller.minSSTableSize);
        assertEquals(Controller.DEFAULT_VECTOR_RESERVED_THREADS, controller.getReservedThreads());
        assertEquals(Controller.DEFAULT_VECTOR_TARGET_SSTABLE_SIZE, controller.getTargetSSTableSize());
        int[] vectorScalingParameter = Controller.parseScalingParameters(StaticController.DEFAULT_VECTOR_STATIC_SCALING_PARAMETERS);
        for (int i = 0; i < vectorScalingParameter.length; i++)
            assertEquals(vectorScalingParameter[i], controller.getScalingParameter(i));

        addOptions(false, options);

        // Test overrides still work.
        controller = testFromOptionsVector(false, options);
        assertTrue(controller instanceof StaticController);

        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));
    }

    @Test
    public void testValidateOptions()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(false, options);

        super.testValidateOptions(options, false);
    }

    @Test
    public void testValidateOptionsIntegers()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(true, options);

        super.testValidateOptions(options, false);
    }

    @Test
    public void testValidateOptionsIntegersDeprecatedName()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(true, options);
        options.put(StaticController.STATIC_SCALING_FACTORS_OPTION,
                    options.remove(StaticController.SCALING_PARAMETERS_OPTION));

        super.testValidateOptions(options, false);
    }

    @Test
    public void testValidateCompactionStrategyOptions()
    {
        super.testValidateCompactionStrategyOptions(true);
    }

    @Test
    public void testSurvivalFactorForSharedStorage()
    {
        System.setProperty("unified_compaction.shared_storage", "true");
        try
        {
            final int rf = 3;
            when(replicationStrategy.getReplicationFactor()).thenReturn(ReplicationFactor.fullOnly(rf));

            Controller controller = Controller.fromOptions(cfs,  new HashMap<>());
            assertNotNull(controller);
            assertNotNull(controller.toString());

            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR / rf, controller.getSurvivalFactor(0), epsilon);
            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(1), epsilon);
            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(2), epsilon);

            assertThatThrownBy(() -> controller.getSurvivalFactor(-1)).isInstanceOf(IllegalArgumentException.class);

        }
        finally
        {
            System.clearProperty("unified_compaction.shared_storage");
        }
    }

    @Test
    public void testStartShutdown()
    {
        StaticController controller = new StaticController(env,
                                                           Ws,
                                                           Controller.DEFAULT_SURVIVAL_FACTORS,
                                                           dataSizeGB << 30,
                                                           0,
                                                           0,
                                                           0,
                                                           Controller.DEFAULT_MAX_SPACE_OVERHEAD,
                                                           0,
                                                           Controller.DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS,
                                                           Controller.DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION,
                                                           numShards,
                                                           false,
                                                           sstableSizeMB << 20,
                                                           Controller.DEFAULT_SSTABLE_GROWTH,
                                                           Controller.DEFAULT_RESERVED_THREADS,
                                                           Controller.DEFAULT_RESERVED_THREADS_TYPE,
                                                           Controller.DEFAULT_OVERLAP_INCLUSION_METHOD,
                                                           true,
                                                           false,
                                                           Controller.DEFAULT_MAX_SSTABLES_PER_SHARD_FACTOR,
                                                           metadata);
        super.testStartShutdown(controller);
    }

    @Test
    public void testShutdownNotStarted()
    {
        StaticController controller = new StaticController(env,
                                                           Ws,
                                                           Controller.DEFAULT_SURVIVAL_FACTORS,
                                                           dataSizeGB << 30,
                                                           0,
                                                           0,
                                                           0,
                                                           Controller.DEFAULT_MAX_SPACE_OVERHEAD,
                                                           0,
                                                           Controller.DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS,
                                                           Controller.DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION,
                                                           numShards,
                                                           false,
                                                           sstableSizeMB << 20,
                                                           Controller.DEFAULT_SSTABLE_GROWTH,
                                                           Controller.DEFAULT_RESERVED_THREADS,
                                                           Controller.DEFAULT_RESERVED_THREADS_TYPE,
                                                           Controller.DEFAULT_OVERLAP_INCLUSION_METHOD,
                                                           true,
                                                           false,
                                                           Controller.DEFAULT_MAX_SSTABLES_PER_SHARD_FACTOR,
                                                           metadata);
        super.testShutdownNotStarted(controller);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartAlreadyStarted()
    {
        StaticController controller = new StaticController(env,
                                                           Ws,
                                                           Controller.DEFAULT_SURVIVAL_FACTORS,
                                                           dataSizeGB << 30,
                                                           0,
                                                           0,
                                                           0,
                                                           Controller.DEFAULT_MAX_SPACE_OVERHEAD,
                                                           0,
                                                           Controller.DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS,
                                                           Controller.DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION,
                                                           numShards,
                                                           false,
                                                           sstableSizeMB << 20,
                                                           Controller.DEFAULT_SSTABLE_GROWTH,
                                                           Controller.DEFAULT_RESERVED_THREADS,
                                                           Controller.DEFAULT_RESERVED_THREADS_TYPE,
                                                           Controller.DEFAULT_OVERLAP_INCLUSION_METHOD,
                                                           true,
                                                           false,
                                                           Controller.DEFAULT_MAX_SSTABLES_PER_SHARD_FACTOR,
                                                           metadata);
        super.testStartAlreadyStarted(controller);
    }

    @Test
    public void testV1MaxSpaceOverhead()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, Integer.toString(numShards));
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "20MiB");

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        assertEquals(maxSpaceOverhead, controller.getMaxSpaceOverhead(), 0.0d);

        options.put(Controller.MAX_SPACE_OVERHEAD_OPTION, "0.5");
        controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        assertEquals(0.5d, controller.getMaxSpaceOverhead(), 0.0d);

        options.put(Controller.MAX_SPACE_OVERHEAD_OPTION, "0.1");
        controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        assertEquals(1.0d / ControllerTest.numShards, controller.getMaxSpaceOverhead(), 0.0d);

        for (Double d : ImmutableList.of(0.0, 10.0, -10.0))
        {
            String s = d.toString();
            try
            {
                options.put(Controller.MAX_SPACE_OVERHEAD_OPTION, s);
                testFromOptions(false, options);
                fail(String.format("%s validation must have failed for the value %s", Controller.MAX_SPACE_OVERHEAD_OPTION, s));
            }
            catch (ConfigurationException ce)
            {
                // expected
                assertEquals(ce.getMessage(), String.format("Invalid configuration, %s must be between %f and %f: %s",
                                                            Controller.MAX_SPACE_OVERHEAD_OPTION,
                                                            Controller.MAX_SPACE_OVERHEAD_LOWER_BOUND,
                                                            Controller.MAX_SPACE_OVERHEAD_UPPER_BOUND,
                                                            s));
            }
        }
    }

    @Test
    public void testMaxSSTablesToCompact()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");
        Controller controller = testFromOptions(false, options);
        assertTrue(controller.maxSSTablesToCompact <= controller.dataSetSize * controller.maxSpaceOverhead / controller.minSSTableSize);

        options.put(Controller.MAX_SPACE_OVERHEAD_OPTION, "0.1");
        controller = testFromOptions(false, options);
        assertTrue(controller.maxSSTablesToCompact <= controller.dataSetSize * controller.maxSpaceOverhead / controller.minSSTableSize);

        options.put(Controller.MAX_SSTABLES_TO_COMPACT_OPTION, "100");
        controller = testFromOptions(false, options);
        assertEquals(100, controller.maxSSTablesToCompact);

        options.put(Controller.MAX_SSTABLES_TO_COMPACT_OPTION, "0");
        controller = testFromOptions(false, options);
        assertTrue(controller.maxSSTablesToCompact <= controller.dataSetSize * controller.maxSpaceOverhead / controller.minSSTableSize);
    }

    @Test
    public void testExpiredSSTableCheckFrequency()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);
        assertEquals(TimeUnit.MILLISECONDS.convert(Controller.DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS, TimeUnit.SECONDS),
                     controller.getExpiredSSTableCheckFrequency());

        options.put(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, "5");
        controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);
        assertEquals(5000L, controller.getExpiredSSTableCheckFrequency());

        try
        {
            options.put(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, "0");
            testFromOptions(false, options);
            fail("Exception should be thrown");
        }
        catch (ConfigurationException e)
        {
            // valid path
        }
    }

    @Test
    public void testAllowOverlaps()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);
        assertEquals(Controller.DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION, controller.getIgnoreOverlapsInExpirationCheck());

        options.put(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, "true");
        controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);
        assertEquals(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION, controller.getIgnoreOverlapsInExpirationCheck());
    }

    @Test
    public void testBaseShardCountDefault()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(Controller.DEFAULT_BASE_SHARD_COUNT, controller.baseShardCount);

        String prevKS = keyspaceName;
        try
        {
            keyspaceName = SchemaConstants.SYSTEM_KEYSPACE_NAME;
            controller = controller.fromOptions(cfs, options);
            assertEquals(4, controller.baseShardCount);
        }
        finally
        {
            keyspaceName = prevKS;
        }

        numDirectories = 3;
        controller = controller.fromOptions(cfs, options);
        assertEquals(4, controller.baseShardCount);

        numDirectories = 1;
        controller = controller.fromOptions(cfs, options);
        assertEquals(Controller.DEFAULT_BASE_SHARD_COUNT, controller.baseShardCount);
    }

    @Test
    public void testTimeBucketsParsing()
    {
        // test duration parsing
        assertEquals(TimeUnit.DAYS.toMicros(1), TimeBucket.parseDurationUs("1d"));
        assertEquals(TimeUnit.DAYS.toMicros(14), TimeBucket.parseDurationUs("2w"));
        assertEquals(TimeUnit.HOURS.toMicros(12), TimeBucket.parseDurationUs("12h"));
        assertEquals(TimeUnit.MINUTES.toMicros(45), TimeBucket.parseDurationUs("45m"));
        assertEquals(TimeUnit.SECONDS.toMicros(30), TimeBucket.parseDurationUs("30s"));
        assertEquals(TimeUnit.DAYS.toMicros(1) + TimeUnit.HOURS.toMicros(12), TimeBucket.parseDurationUs("1d12h"));

        // test duration formatting
        assertEquals("1d", TimeBucket.formatDurationUs(TimeUnit.DAYS.toMicros(1)));
        assertEquals("2w", TimeBucket.formatDurationUs(TimeUnit.DAYS.toMicros(14)));
        assertEquals("12h", TimeBucket.formatDurationUs(TimeUnit.HOURS.toMicros(12)));
        assertEquals("45m", TimeBucket.formatDurationUs(TimeUnit.MINUTES.toMicros(45)));
        assertEquals("30s", TimeBucket.formatDurationUs(TimeUnit.SECONDS.toMicros(30)));

        // invalid durations
        try
        {
            TimeBucket.parseDurationUs("abc");
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            // expected
        }

        try
        {
            TimeBucket.parseDurationUs("1y");
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            // expected
        }

        try
        {
            TimeBucket.parseDurationUs("-1d");
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testFromOptionsWithTimeBuckets()
    {
        Map<String, String> options = new HashMap<>();
        options.put(StaticController.SCALING_PARAMETERS_OPTION, "T6, T2 until 1d; L5 until 2d; L1000000 every 2w");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        List<TimeBucket> buckets = controller.getTimeBuckets();
        assertEquals(3, buckets.size());

        // 'until' buckets are in insertion order (strictly increasing durations: 1d then 2d)
        TimeBucket until1d = buckets.get(0);
        assertEquals(TimeBucket.Mode.UNTIL, until1d.mode);
        assertEquals(TimeUnit.DAYS.toMicros(1), until1d.durationUs);
        assertEquals(2, until1d.scalingParameters.length); // T6, T2

        TimeBucket until2d = buckets.get(1);
        assertEquals(TimeBucket.Mode.UNTIL, until2d.mode);
        assertEquals(TimeUnit.DAYS.toMicros(2), until2d.durationUs);
        assertEquals(-3, until2d.scalingParameters[0]); // L5 (w=-3 corresponds to L5)

        // 'every' bucket is placed at the end
        TimeBucket everyBucket = buckets.get(2);
        assertEquals(TimeBucket.Mode.EVERY, everyBucket.mode);
        assertEquals(TimeUnit.DAYS.toMicros(14), everyBucket.durationUs);
        assertEquals(-999998, everyBucket.scalingParameters[0]); // L1000000
    }

    @Test
    public void testArenaSelectorWithTimeBuckets() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(StaticController.SCALING_PARAMETERS_OPTION, "L1000000 until 10m; T2 every 1h");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        // We mock a custom MonotonicClock and MonotonicClockTranslation to have a deterministic time domain.
        long nowUs = TimeUnit.HOURS.toMicros(100) + TimeUnit.MINUTES.toMicros(2); // 100 hours and 2 minutes since epoch
        MonotonicClock mockClock = Mockito.mock(MonotonicClock.class);
        MonotonicClockTranslation mockTranslation = Mockito.mock(MonotonicClockTranslation.class);

        when(mockClock.translate()).thenReturn(mockTranslation);
        when(mockClock.now()).thenReturn(1000L); // dummy monotonic nanoseconds
        when(mockTranslation.toMillisSinceEpoch(1000L)).thenReturn(nowUs / 1000L); // return nowUs in epoch milliseconds

        // Replace the clock on the controller using reflection
        Field clockField = Controller.class.getDeclaredField("clock");
        clockField.setAccessible(true);
        clockField.set(controller, mockClock);

        org.apache.cassandra.db.DiskBoundaries diskBoundaries = Mockito.mock(org.apache.cassandra.db.DiskBoundaries.class);
        when(diskBoundaries.getNumBoundaries()).thenReturn(1);

        ArenaSelector selector = new ArenaSelector(controller, diskBoundaries).withTimeBuckets(nowUs);

        // Define SSTables with different timestamps:
        // Recent 1: 2 minutes old (same UNTIL 10m bucket: 100h 2m - 2m = 100h)
        CompactionSSTable sstableRecent1 = Mockito.mock(CompactionSSTable.class);
        when(sstableRecent1.getMinTimestamp()).thenReturn(nowUs - TimeUnit.MINUTES.toMicros(2));

        // Recent 2: 3 minutes old (same UNTIL 10m bucket: 100h 2m - 3m = 99h 59m)
        CompactionSSTable sstableRecent2 = Mockito.mock(CompactionSSTable.class);
        when(sstableRecent2.getMinTimestamp()).thenReturn(nowUs - TimeUnit.MINUTES.toMicros(3));

        // Recent 3: 1.5 minutes old (same UNTIL 10m bucket)
        CompactionSSTable sstableRecent3 = Mockito.mock(CompactionSSTable.class);
        when(sstableRecent3.getMinTimestamp()).thenReturn(nowUs - TimeUnit.SECONDS.toMicros(90));

        // Old: 15 minutes old (older than 10 minutes, matches 'every 1h' bucket)
        CompactionSSTable sstableOld = Mockito.mock(CompactionSSTable.class);
        when(sstableOld.getMinTimestamp()).thenReturn(nowUs - TimeUnit.MINUTES.toMicros(15));

        org.apache.cassandra.io.sstable.Descriptor desc = Mockito.mock(org.apache.cassandra.io.sstable.Descriptor.class);
        when(desc.filenameFor(any())).thenReturn("dummy-Data.db");

        for (CompactionSSTable sst : new CompactionSSTable[] { sstableRecent1, sstableRecent2, sstableRecent3, sstableOld })
        {
            when(sst.getDescriptor()).thenReturn(desc);
            when(sst.getKeyspaceName()).thenReturn("ks");
            when(sst.getColumnFamilyName()).thenReturn("tbl");
            when(sst.isRepaired()).thenReturn(false);
            when(sst.isPendingRepair()).thenReturn(false);
        }

        // Verify grouping names
        assertEquals("unrepaired-until_10m", selector.name(sstableRecent1));
        assertEquals("unrepaired-until_10m", selector.name(sstableRecent2));
        assertEquals("unrepaired-until_10m", selector.name(sstableRecent3));
        // Window index for sstableOld (99h 47m) is 99. 99h from epoch is 1970-01-05 03:00 UTC
        assertEquals("unrepaired-every_1970-01-05-03-00", selector.name(sstableOld));

        // Verify comparison/equality logic:
        // 1. Same UNTIL window compares as equal:
        assertEquals(0, selector.compare(sstableRecent1, sstableRecent3));
        assertEquals(0, selector.compare(sstableRecent1, sstableRecent2));

        // 2. Recent and old compare as different:
        assertNotEquals(0, selector.compare(sstableRecent1, sstableOld));
    }
}
