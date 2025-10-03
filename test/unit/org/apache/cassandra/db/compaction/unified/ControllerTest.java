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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.compaction.CompactionStrategyOptions;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.Overlaps;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.creation.MockSettingsImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@Ignore
public abstract class ControllerTest
{
    static final double epsilon = 0.00000001;
    static final long dataSizeGB = 512;
    static final int numShards = 4; // pick it so that dataSizeGB is exactly divisible or tests will break
    static final long sstableSizeMB = 2;
    static final double maxSpaceOverhead = 0.3d;
    static final boolean allowOverlaps = false;
    static final long checkFrequency= 600L;
    static final float tombstoneThresholdOption = 1;
    static final long tombstoneCompactionIntervalOption = 1;
    static final boolean uncheckedTombstoneCompactionOption = true;
    static final boolean logAllOption = true;
    static final String logTypeOption = "all";
    static final int logPeriodMinutesOption = 1;
    static final boolean compactionEnabled = true;
    static final double readMultiplier = 0.5;
    static final double writeMultiplier = 1.0;
    static final String tableName = "tbl";

    @Mock
    ColumnFamilyStore cfs;

    TableMetadata metadata;

    @Mock
    UnifiedCompactionStrategy strategy;

    @Mock
    ScheduledExecutorService executorService;

    @Mock
    ScheduledFuture fut;

    @Mock
    Environment env;

    @Mock
    AbstractReplicationStrategy replicationStrategy;

    @Mock
    DiskBoundaries boundaries;

    protected String keyspaceName = "TestKeyspace";
    protected int numDirectories = 1;
    protected boolean useVector = false;

    @BeforeClass
    public static void setUpClass()
    {
        // The def below should match Controller.PREFIX + Controller.OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES
        // We can't reference these, because it would initialize Controller (and get the value before we modify it).
        UCS_OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES.setBoolean(true);
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        metadata = Mockito.mock(TableMetadata.class, new MockSettingsImpl<>()
                                                     .useConstructor(standardCFMD(keyspaceName, tableName))
                                                     .defaultAnswer(Answers.RETURNS_SMART_NULLS));

        when(strategy.getMetadata()).thenReturn(metadata);
        when(strategy.getEstimatedRemainingTasks()).thenReturn(0);

        when(replicationStrategy.getReplicationFactor()).thenReturn(ReplicationFactor.fullOnly(3));
        when(cfs.makeUCSEnvironment()).thenAnswer(invocation -> new RealEnvironment(cfs));
        when(cfs.getKeyspaceReplicationStrategy()).thenReturn(replicationStrategy);
        when(cfs.getKeyspaceName()).thenAnswer(invocation -> keyspaceName);
        when(cfs.getDiskBoundaries()).thenReturn(boundaries);
        when(cfs.buildShardManager()).thenCallRealMethod();
        when(cfs.makeUCSEnvironment()).thenCallRealMethod();
        when(cfs.getTableName()).thenReturn(tableName);
        when(boundaries.getNumBoundaries()).thenAnswer(invocation -> numDirectories);

        when(executorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(fut);

        when(env.flushSize()).thenReturn((double) (sstableSizeMB << 20));
        when(cfs.metadata()).thenReturn(metadata);
        when(metadata.hasVectorType()).thenAnswer(invocation -> useVector);
    }

    Controller testFromOptions(boolean adaptive, Map<String, String> options)
    {
        addOptions(adaptive, options);
        Controller.validateOptions(options);

        Controller controller = Controller.fromOptions(cfs, options);
        assertNotNull(controller);
        assertNotNull(controller.toString());

        assertEquals(dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertFalse(controller.isRunning());
        for (int i = 0; i < 5; i++) // simulate 5 levels
            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(i), epsilon);
        assertNull(controller.getCalculator());
        if (!options.containsKey(Controller.NUM_SHARDS_OPTION))
        {
            assertEquals(1, controller.getNumShards(0));
            assertEquals(4, controller.getNumShards(16 * 100 << 20));
            assertEquals(Overlaps.InclusionMethod.SINGLE, controller.overlapInclusionMethod());
        }
        else
        {
            int numShards = Integer.parseInt(options.get(Controller.NUM_SHARDS_OPTION));
            long minSSTableSize = FBUtilities.parseHumanReadableBytes(options.get(Controller.MIN_SSTABLE_SIZE_OPTION));
            assertEquals(1, controller.getNumShards(0));
            assertEquals(numShards, controller.getNumShards(numShards * minSSTableSize));
            assertEquals(numShards, controller.getNumShards(16 * 100 << 20));
        }

        return controller;
    }

    Controller testFromOptionsVector(boolean adaptive, Map<String, String> options)
    {
        useVector = true;
        addOptions(adaptive, options);
        Controller.validateOptions(options);

        Controller controller = Controller.fromOptions(cfs, options);
        assertNotNull(controller);
        assertNotNull(controller.toString());

        assertEquals(dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertFalse(controller.isRunning());
        for (int i = 0; i < 5; i++) // simulate 5 levels
            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(i), epsilon);
        assertNull(controller.getCalculator());

        return controller;
    }

    void testValidateOptions(Map<String, String> options, boolean adaptive)
    {
        addOptions(adaptive, options);
        options = Controller.validateOptions(options);
        assertTrue(options.toString(), options.isEmpty());
    }

    private static void putWithAlt(Map<String, String> options, String opt, String alt, int altShift, long altVal)
    {
        if (options.containsKey(opt) || options.containsKey(alt))
            return;
        if (ThreadLocalRandom.current().nextBoolean())
            options.put(opt, FBUtilities.prettyPrintMemory(altVal << altShift));
        else
            options.put(alt, Long.toString(altVal));
    }

    private static void addOptions(boolean adaptive, Map<String, String> options)
    {
        options.putIfAbsent(Controller.ADAPTIVE_OPTION, Boolean.toString(adaptive));
        putWithAlt(options, Controller.DATASET_SIZE_OPTION, Controller.DATASET_SIZE_OPTION_GB, 30, dataSizeGB);

        if (ThreadLocalRandom.current().nextBoolean())
            options.putIfAbsent(Controller.MAX_SPACE_OVERHEAD_OPTION, Double.toString(maxSpaceOverhead));
        else
            options.putIfAbsent(Controller.MAX_SPACE_OVERHEAD_OPTION, String.format("%.1f%%", maxSpaceOverhead * 100));

        options.putIfAbsent(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, Boolean.toString(allowOverlaps));
        options.putIfAbsent(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, Long.toString(checkFrequency));

        if (!options.containsKey(Controller.NUM_SHARDS_OPTION))
        {
            options.putIfAbsent(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(4));
            options.putIfAbsent(Controller.TARGET_SSTABLE_SIZE_OPTION, FBUtilities.prettyPrintMemory(1 << 30));
        }
        options.putIfAbsent(Controller.OVERLAP_INCLUSION_METHOD_OPTION, Overlaps.InclusionMethod.SINGLE.toString().toLowerCase());
    }

    void testStartShutdown(Controller controller)
    {
        assertNotNull(controller);

        assertEquals((long) dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards(1));
        assertEquals((long) sstableSizeMB << 20, controller.getTargetSSTableSize());
        assertFalse(controller.isRunning());
        assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(0), epsilon);
        assertNull(controller.getCalculator());

        controller.startup(strategy, executorService);
        assertTrue(controller.isRunning());
        assertNotNull(controller.getCalculator());

        controller.shutdown();
        assertFalse(controller.isRunning());
        assertNull(controller.getCalculator());

        controller.shutdown(); // no op
    }

    void testShutdownNotStarted(Controller controller)
    {
        assertNotNull(controller);

        controller.shutdown(); // no op.
    }

    void testStartAlreadyStarted(Controller controller)
    {
        assertNotNull(controller);

        controller.startup(strategy, executorService);
        assertTrue(controller.isRunning());
        assertNotNull(controller.getCalculator());

        controller.startup(strategy, executorService);
    }

    @Test
    public void testScalingParameterConversion()
    {
        testScalingParameterConversion("T4", 2);
        testScalingParameterConversion("L4", -2);
        testScalingParameterConversion("N", 0);
        testScalingParameterConversion("L2, T2, N", 0, 0, 0);
        testScalingParameterConversion("T10, T8, T4, N, L4, L6", 8, 6, 2, 0, -2, -4);
        testScalingParameterConversion("T10000, T1000, T100, T10, T2, L10, L100, L1000, L10000", 9998, 998, 98, 8, 0, -8, -98, -998, -9998);

        testScalingParameterParsing("-50 ,  T5  ,  3 ,  N  ,  L7 ,  +5 , -12  ,T9,L4,6,-7,+0,-0", -50, 3, 3, 0, -5, 5, -12, 7, -2, 6, -7, 0, 0);

        testScalingParameterError("Q6");
        testScalingParameterError("L4,,T5");
        testScalingParameterError("L1");
        testScalingParameterError("T1");
        testScalingParameterError("L0");
        testScalingParameterError("T0");
        testScalingParameterError("T-5");
        testScalingParameterError("T+5");
        testScalingParameterError("L-5");
        testScalingParameterError("L+5");
        testScalingParameterError("N3");
        testScalingParameterError("7T");
        testScalingParameterError("T,5");
        testScalingParameterError("L,5");
    }

    void testScalingParameterConversion(String definition, int... parameters)
    {
        testScalingParameterParsing(definition, parameters);

        String normalized = definition.replaceAll("T2|L2", "N");
        assertEquals(normalized, Controller.printScalingParameters(parameters));

        testScalingParameterParsing(Arrays.toString(parameters).replaceAll("[\\[\\]]", ""), parameters);
    }

    void testScalingParameterParsing(String definition, int... parameters)
    {
        assertArrayEquals(parameters, Controller.parseScalingParameters(definition));
    }

    void testScalingParameterError(String definition)
    {
        try
        {
            Controller.parseScalingParameters(definition);
            Assert.fail("Expected error on " + definition);
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testGetNumShards_growth_0()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(0.0, controller.sstableGrowthModifier, 0.0);

        // Easy ones
        // x00 MiB = x * 100
        assertEquals(6, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(24, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(6 * 1024, controller.getNumShards(Math.scalb(600, 30)));
        // Check rounding
        assertEquals(6, controller.getNumShards(Math.scalb(800, 20)));
        assertEquals(12, controller.getNumShards(Math.scalb(900, 20)));
        assertEquals(6 * 1024, controller.getNumShards(Math.scalb(800, 30)));
        assertEquals(12 * 1024, controller.getNumShards(Math.scalb(900, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(1, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testGetNumShards_growth_1()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "1.0");
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(1.0, controller.sstableGrowthModifier, 0.0);

        // Easy ones
        // x00 MiB = x * 100
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(600, 30)));
        // Check rounding
        assertEquals(3, controller.getNumShards(Math.scalb(800, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(800, 30)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(1, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testGetNumShards_legacy()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, Integer.toString(3));
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // Easy ones
        // x00 MiB = x * 100
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(600, 30)));
        // Check rounding
        assertEquals(3, controller.getNumShards(Math.scalb(800, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(800, 30)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(500, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(300, 20)));
        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(290, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(190, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(1, controller.getNumShards(Double.NaN));

        assertEquals(Integer.MAX_VALUE, controller.getReservedThreads());
    }

    @Test
    public void testGetNumShards_legacy_disabled()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, Integer.toString(-1));
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // The number of shards grows with local density, the controller works as if number of shards was not defined
        assertEquals(2, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(4, controller.getNumShards(Math.scalb(200, 25)));
    }

    @Test
    public void testGetNumShards_legacy_validation()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, Integer.toString(-1));
        Map<String, String> validatedOptions = Controller.validateOptions(options);
        assertTrue("-1 should be a valid option: " + validatedOptions, validatedOptions.isEmpty());

        options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, Integer.toString(-1));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "128MB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "0B");
        validatedOptions = Controller.validateOptions(options);
        assertTrue("-1 num of shards should be acceptable with V2 params: " + validatedOptions, validatedOptions.isEmpty());

        Map<String, String> invalidOptions = new HashMap<>();
        invalidOptions.put(Controller.NUM_SHARDS_OPTION, Integer.toString(32));
        invalidOptions.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "128MB");
        assertThrows("Positive num of shards should not be acceptable with V2 params",
                     ConfigurationException.class, () -> Controller.validateOptions(invalidOptions));
    }

    @Test
    public void testGetNumShards_growth_1_2()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.5");
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(0.5, controller.sstableGrowthModifier, 0.0);

        // Easy ones
        // x00 MiB = x * 3 * 100
        assertEquals(3 * 2, controller.getNumShards(Math.scalb(4 * 3 * 100, 20)));
        assertEquals(3 * 4, controller.getNumShards(Math.scalb(16 * 3 * 100, 20)));
        assertEquals(3 * 32, controller.getNumShards(Math.scalb(3 * 100, 20 + 10)));
        // Check rounding. Note: Size must grow by 2x to get sqrt(2) times more shards.
        assertEquals(6, controller.getNumShards(Math.scalb(2350, 20)));
        assertEquals(12, controller.getNumShards(Math.scalb(2450, 20)));
        assertEquals(3 * 32, controller.getNumShards(Math.scalb(550, 30)));
        assertEquals(6 * 32, controller.getNumShards(Math.scalb(650, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 80)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(1, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testGetNumShards_growth_1_3()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.333");
        Controller controller = Controller.fromOptions(cfs, options);

        // Easy ones
        // x00 MiB = x * 3 * 100
        assertEquals(3 * 4, controller.getNumShards(Math.scalb(8 * 3 * 100, 20)));
        assertEquals(3 * 16, controller.getNumShards(Math.scalb(64 * 3 * 100, 20)));
        assertEquals(3 * 64, controller.getNumShards(Math.scalb(3 * 100, 20 + 9)));
        // Check rounding. Note: size must grow by 2 ^ 3/4 to get sqrt(2) times more shards
        assertEquals(12, controller.getNumShards(Math.scalb(4000, 20)));
        assertEquals(24, controller.getNumShards(Math.scalb(4100, 20)));
        assertEquals(3 * 64, controller.getNumShards(Math.scalb(500, 29)));
        assertEquals(6 * 64, controller.getNumShards(Math.scalb(550, 29)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 50)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
        assertEquals(1, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testMinSizeAuto()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "200MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "auto");
//        options.put(Controller.SSTABLE_GROWTH_OPTION, "0");
        mockFlushSize(45); // rounds up to 50MiB
        Controller controller = Controller.fromOptions(cfs, options);

        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(149, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(99, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(49, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(10, 20)));

        // sanity check
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(6, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
    }

    private void mockFlushSize(double d)
    {
        TableMetrics metrics = Mockito.mock(TableMetrics.class);
        MovingAverage flushSize = Mockito.mock(MovingAverage.class);
        when(cfs.metrics()).thenReturn(metrics);
        when(metrics.flushSizeOnDisk()).thenReturn(flushSize);
        when(flushSize.get()).thenReturn(Math.scalb(d, 20)); // rounds up to 50MiB
    }

    @Test
    public void testMinSizeAutoAtMostTargetMin()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "200MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "Auto");
        mockFlushSize(300); // above target min, set to 141MiB
        Controller controller = Controller.fromOptions(cfs, options);

        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(400, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(300, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(100, 20)));
        // sanity check
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(6, controller.getNumShards(Math.scalb(2400, 20)));
    }

    @Test
    public void testParallelizeOutputShards()
    {
        testBooleanOption(Controller.PARALLELIZE_OUTPUT_SHARDS_OPTION, Controller.DEFAULT_PARALLELIZE_OUTPUT_SHARDS, Controller::parallelizeOutputShards);
    }

    @Test
    public void testMaxSstablesPerShardFactor()
    {
        HashMap<String, String> options = new HashMap<>();
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(Controller.DEFAULT_MAX_SSTABLES_PER_SHARD_FACTOR, controller.getMaxSstablesPerShardFactor(), epsilon);

        options.put(Controller.MAX_SSTABLES_PER_SHARD_FACTOR_OPTION, "123.456");
        Controller.validateOptions(options);
        controller = Controller.fromOptions(cfs, options);
        assertEquals(123.456, controller.getMaxSstablesPerShardFactor(), epsilon);

        options.put(Controller.MAX_SSTABLES_PER_SHARD_FACTOR_OPTION, "1e1000");
        Controller.validateOptions(options);
        controller = Controller.fromOptions(cfs, options);
        assertEquals(Double.POSITIVE_INFINITY, controller.getMaxSstablesPerShardFactor(), epsilon);

        options.put(Controller.MAX_SSTABLES_PER_SHARD_FACTOR_OPTION, "0.9");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));

        options.put(Controller.MAX_SSTABLES_PER_SHARD_FACTOR_OPTION, "invalid");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
    }

    public void testBooleanOption(String name, boolean defaultValue, Predicate<Controller> getter, String... extraSettings)
    {
        Controller controller = Controller.fromOptions(cfs, newOptions(extraSettings));
        assertEquals(defaultValue, getter.test(controller));
        for (boolean b : new boolean[] { true, false })
        {
            Map<String, String> options = newOptions(extraSettings);
            options.put(name, Boolean.toString(b));
            controller = Controller.fromOptions(cfs, options);
            assertEquals(b, getter.test(controller));
        }
    }

    private HashMap<String, String> newOptions(String... settings)
    {
        HashMap<String, String> options = new HashMap<>();
        for (int i = 0; i < settings.length; i += 2)
            options.put(settings[i], settings[i + 1]);
        return options;
    }

    void testValidateCompactionStrategyOptions(boolean testLogType)
    {
        Map<String, String> options = new HashMap<>();
        options.put(CompactionStrategyOptions.TOMBSTONE_THRESHOLD_OPTION, Float.toString(tombstoneThresholdOption));
        options.put(CompactionStrategyOptions.TOMBSTONE_COMPACTION_INTERVAL_OPTION, Long.toString(tombstoneCompactionIntervalOption));
        options.put(CompactionStrategyOptions.UNCHECKED_TOMBSTONE_COMPACTION_OPTION, Boolean.toString(uncheckedTombstoneCompactionOption));

        if (testLogType)
            options.put(CompactionStrategyOptions.LOG_TYPE_OPTION, logTypeOption);
        else
            options.put(CompactionStrategyOptions.LOG_ALL_OPTION, Boolean.toString(logAllOption));

        options.put(CompactionStrategyOptions.LOG_PERIOD_MINUTES_OPTION, Integer.toString(logPeriodMinutesOption));
        options.put(CompactionStrategyOptions.COMPACTION_ENABLED, Boolean.toString(compactionEnabled));
        options.put(CompactionStrategyOptions.READ_MULTIPLIER_OPTION, Double.toString(readMultiplier));
        options.put(CompactionStrategyOptions.WRITE_MULTIPLIER_OPTION, Double.toString(writeMultiplier));

        CompactionStrategyOptions compactionStrategyOptions = new CompactionStrategyOptions(UnifiedCompactionStrategy.class, options, true);
        assertNotNull(compactionStrategyOptions);
        assertNotNull(compactionStrategyOptions.toString());
        assertEquals(tombstoneThresholdOption, compactionStrategyOptions.getTombstoneThreshold(), epsilon);
        assertEquals(tombstoneCompactionIntervalOption, compactionStrategyOptions.getTombstoneCompactionInterval());
        assertEquals(uncheckedTombstoneCompactionOption, compactionStrategyOptions.isUncheckedTombstoneCompaction());

        if (testLogType)
        {
            assertEquals((logTypeOption.equals("all") || logTypeOption.equals("events_only")), compactionStrategyOptions.isLogEnabled());
            assertEquals(logTypeOption.equals("all"), compactionStrategyOptions.isLogAll());
        }
        else
        {
            assertEquals(logAllOption, compactionStrategyOptions.isLogEnabled());
            assertEquals(logAllOption, compactionStrategyOptions.isLogAll());
        }
        assertEquals(logPeriodMinutesOption, compactionStrategyOptions.getLogPeriodMinutes());
        assertEquals(readMultiplier, compactionStrategyOptions.getReadMultiplier(), epsilon);
        assertEquals(writeMultiplier, compactionStrategyOptions.getWriteMultiplier(), epsilon);

        Map<String, String> uncheckedOptions = CompactionStrategyOptions.validateOptions(options);
        assertNotNull(uncheckedOptions);
    }

    @Test
    public void testFactorizedShardGrowth()
    {
        // Test factorization-based growth for num_shards=1000 (5^3 * 2^3)
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, "1000");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "1GiB");
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // Verify shard count is set
        assertEquals(1.0, controller.sstableGrowthModifier, 0.0);
        assertTrue(controller.useFactorizationShardCountGrowth());

        // Test the smooth progression sequence: 1, 5, 25, 125, 250, 500, 1000
        assertEquals(1, controller.getNumShards(0));    // 0 GiB → 1 shard
        assertEquals(1, controller.getNumShards(Math.scalb(1.5, 30)));    // 1.5 GiB → 1 shard
        assertEquals(5, controller.getNumShards(Math.scalb(6.0, 30)));    // 6 GiB → 5 shards
        assertEquals(25, controller.getNumShards(Math.scalb(30.0, 30)));  // 30 GiB → 25 shards
        assertEquals(125, controller.getNumShards(Math.scalb(130.0, 30))); // 130 GiB → 125 shards
        assertEquals(250, controller.getNumShards(Math.scalb(300.0, 30))); // 300 GiB → 250 shards
        assertEquals(500, controller.getNumShards(Math.scalb(600.0, 30))); // 600 GiB → 500 shards
        assertEquals(1000, controller.getNumShards(Math.scalb(1000.0, 30))); // 1000 GiB → 1000 shards

        // Test boundary cases
        assertEquals(1, controller.getNumShards(Math.scalb(4.9, 30)));    // Just below 5
        assertEquals(5, controller.getNumShards(Math.scalb(5.0, 30)));    // Exactly 5
        assertEquals(5, controller.getNumShards(Math.scalb(24.9, 30)));   // Just below 25
        assertEquals(25, controller.getNumShards(Math.scalb(25.0, 30)));  // Exactly 25

        // Test very large values
        assertEquals(1000, controller.getNumShards(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testFactorizedShardGrowthPowerOfTwo()
    {
        // Test with a power of 2 (should behave similarly to old logic)
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, "1024"); // 2^10
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "1GiB");
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // Should give smooth power-of-2 progression: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
        assertEquals(1, controller.getNumShards(Math.scalb(1.5, 30)));
        assertEquals(2, controller.getNumShards(Math.scalb(2.5, 30)));
        assertEquals(4, controller.getNumShards(Math.scalb(5.0, 30)));
        assertEquals(8, controller.getNumShards(Math.scalb(10.0, 30)));
        assertEquals(16, controller.getNumShards(Math.scalb(20.0, 30)));
        assertEquals(32, controller.getNumShards(Math.scalb(40.0, 30)));
        assertEquals(64, controller.getNumShards(Math.scalb(80.0, 30)));
        assertEquals(128, controller.getNumShards(Math.scalb(150.0, 30)));
        assertEquals(256, controller.getNumShards(Math.scalb(300.0, 30)));
        assertEquals(512, controller.getNumShards(Math.scalb(600.0, 30)));
        assertEquals(1024, controller.getNumShards(Math.scalb(1024.0, 30)));
    }

    @Test
    public void testNoFactorizedShardGrowthWithPowerOfTwo()
    {
        // Test no factorization-based growth for num_shards=128
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, "128");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "1GiB");
        Controller controller = Controller.fromOptions(cfs, options);

        assertFalse(controller.useFactorizationShardCountGrowth());
    }

    @Test
    public void testFactorizedShardGrowthPrimeTarget()
    {
        // Test with a prime number (should stay at 1 until reaching the target)
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, "17"); // Prime number
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "1GiB");
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // Since 17 is prime, sequence should be just: 1, 17
        assertEquals(1, controller.getNumShards(Double.NaN));
        assertEquals(1, controller.getNumShards(Math.scalb(1.5, 30)));
        assertEquals(1, controller.getNumShards(Math.scalb(5.0, 30)));
        assertEquals(1, controller.getNumShards(Math.scalb(10.0, 30)));
        assertEquals(1, controller.getNumShards(Math.scalb(16.9, 30)));
        assertEquals(17, controller.getNumShards(Math.scalb(17.0, 30)));
        assertEquals(17, controller.getNumShards(Math.scalb(100.0, 30)));
    }

    @Test
    public void testFactorizedShardSequence()
    {
        // Test small numbers
        assertArrayEquals(new int[]{ 1 }, Controller.factorizedSmoothShardSequence(1));
        assertArrayEquals(new int[]{ 1, 2 }, Controller.factorizedSmoothShardSequence(2));
        assertArrayEquals(new int[]{ 1, 3 }, Controller.factorizedSmoothShardSequence(3));
        assertArrayEquals(new int[]{ 1, 2, 4 }, Controller.factorizedSmoothShardSequence(4));

        // Test perfect squares
        assertArrayEquals(new int[]{ 1, 3, 9 }, Controller.factorizedSmoothShardSequence(9));
        assertArrayEquals(new int[]{ 1, 2, 4, 8, 16 }, Controller.factorizedSmoothShardSequence(16));

        // Test primes
        assertArrayEquals(new int[]{ 1, 7 }, Controller.factorizedSmoothShardSequence(7));
        assertArrayEquals(new int[]{ 1, 11 }, Controller.factorizedSmoothShardSequence(11));

        // Test composite numbers
        assertArrayEquals(new int[]{ 1, 3, 6, 12 }, Controller.factorizedSmoothShardSequence(12));

        // Test 1000 = 5^3 * 2^3
        int[] expected1000 = new int[]{ 1, 5, 25, 125, 250, 500, 1000 };
        assertArrayEquals(expected1000, Controller.factorizedSmoothShardSequence(1000));

        // Test 3200 = 5^2 * 2^7
        int[] expected3200 = new int[]{ 1, 5, 25, 50, 100, 200, 400, 800, 1600, 3200 };
        assertArrayEquals(expected3200, Controller.factorizedSmoothShardSequence(3200));

        // Test Max Int
        assertArrayEquals(new int[]{ 1, Integer.MAX_VALUE }, Controller.factorizedSmoothShardSequence(Integer.MAX_VALUE));
    }

    @Test
    public void testFactorizedShardSequenceInputValidation()
    {
        assertThatThrownBy(() -> Controller.factorizedSmoothShardSequence(0)).hasMessageContaining("must be positive");
        assertThatThrownBy(() -> Controller.factorizedSmoothShardSequence(-5)).hasMessageContaining("must be positive");
    }

    @Test
    public void testPrimeFactors()
    {
        // Test small numbers
        assertEquals(Arrays.asList(2), Controller.primeFactors(2));
        assertEquals(Arrays.asList(3), Controller.primeFactors(3));
        assertEquals(Arrays.asList(2, 2), Controller.primeFactors(4));

        // Test larger numbers
        assertEquals(Arrays.asList(2, 2, 2, 5, 5, 5), Controller.primeFactors(1000));
        assertEquals(Arrays.asList(2, 2, 2, 2, 2, 2, 2, 5, 5), Controller.primeFactors(3200));

        // Test primes
        assertEquals(Arrays.asList(7), Controller.primeFactors(7));
        assertEquals(Arrays.asList(97), Controller.primeFactors(97));
        assertEquals(Arrays.asList(Integer.MAX_VALUE), Controller.primeFactors(Integer.MAX_VALUE));
    }

    @Test
    public void testPrimeFactorsInputValidation()
    {
        assertThatThrownBy(() -> Controller.primeFactors(0)).hasMessageContaining("greater than 1");
        assertThatThrownBy(() -> Controller.primeFactors(1)).hasMessageContaining("greater than 1");
        assertThatThrownBy(() -> Controller.primeFactors(-10)).hasMessageContaining("greater than 1");
    }

    @Test
    public void testGetLargestFactorizedShardCount()
    {
        // Test with num_shards=1000 to get sequence: [1, 5, 25, 125, 250, 500, 1000]
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, "1000");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "1GiB");
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // Test exact matches
        assertEquals(1, controller.getLargestFactorizedShardCount(1.0));
        assertEquals(5, controller.getLargestFactorizedShardCount(5.0));
        assertEquals(25, controller.getLargestFactorizedShardCount(25.0));
        assertEquals(125, controller.getLargestFactorizedShardCount(125.0));
        assertEquals(250, controller.getLargestFactorizedShardCount(250.0));
        assertEquals(500, controller.getLargestFactorizedShardCount(500.0));
        assertEquals(1000, controller.getLargestFactorizedShardCount(1000.0));

        // Test values between sequence elements (should return largest ≤ input)
        assertEquals(1, controller.getLargestFactorizedShardCount(1.5));
        assertEquals(1, controller.getLargestFactorizedShardCount(4.9));
        assertEquals(5, controller.getLargestFactorizedShardCount(5.1));
        assertEquals(5, controller.getLargestFactorizedShardCount(24.9));
        assertEquals(25, controller.getLargestFactorizedShardCount(25.1));
        assertEquals(25, controller.getLargestFactorizedShardCount(124.9));
        assertEquals(125, controller.getLargestFactorizedShardCount(125.1));
        assertEquals(125, controller.getLargestFactorizedShardCount(249.9));
        assertEquals(250, controller.getLargestFactorizedShardCount(250.1));
        assertEquals(250, controller.getLargestFactorizedShardCount(499.9));
        assertEquals(500, controller.getLargestFactorizedShardCount(500.1));
        assertEquals(500, controller.getLargestFactorizedShardCount(999.9));

        // Test edge cases
        assertEquals(1, controller.getLargestFactorizedShardCount(0.0));
        assertEquals(1, controller.getLargestFactorizedShardCount(0.5));
        assertEquals(1000, controller.getLargestFactorizedShardCount(1000.1));
        assertEquals(1000, controller.getLargestFactorizedShardCount(2000.0));
        assertEquals(1000, controller.getLargestFactorizedShardCount(Double.POSITIVE_INFINITY));

        // Test NaN
        assertEquals(1, controller.getLargestFactorizedShardCount(Double.NaN));

        // Test negative values (should return first element)
        assertEquals(1, controller.getLargestFactorizedShardCount(-1.0));
        assertEquals(1, controller.getLargestFactorizedShardCount(-100.0));
    }
}
