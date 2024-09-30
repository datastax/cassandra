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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class StaticControllerTest extends ControllerTest
{
    static final int[] Ws = new int[] { 30, 2, 0, -6};
    static final int[] VectorWs = new int[] {-8, -8, -8, -8};
    static final int vectorBaseShardCount = 1;
    static final double vectorSstableGrowthModifier = 1;
    static final int vectorReservedThreads = Integer.MAX_VALUE;
    static final int vectorScalingParameter = -8;
    static final String vectorMinSSTableSize = "1024MiB";
    static final String  vectorTargetSSTableSize = "5GiB";

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

    private static void addVectorOptions(Map<String, String> options)
    {
        options.put(Controller.VECTOR_BASE_SHARD_COUNT_OPTION, String.valueOf(vectorBaseShardCount));
        options.put(Controller.VECTOR_SSTABLE_GROWTH_OPTION, String.valueOf(vectorSstableGrowthModifier));
        options.put(Controller.VECTOR_RESERVED_THREADS_OPTION, String.valueOf(vectorReservedThreads));
        options.put(Controller.VECTOR_SCALING_PARAMETERS_OPTION, String.valueOf(vectorScalingParameter));
        options.put(Controller.VECTOR_MIN_SSTABLE_SIZE_OPTION, vectorMinSSTableSize);
        options.put(Controller.VECTOR_TARGET_SSTABLE_SIZE_OPTION, vectorTargetSSTableSize);
        options.put(Controller.OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES_OPTION, String.valueOf(true));
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
        Map<String, String> options = new HashMap<>();
        addOptions(false, options);
        addVectorOptions(options);

        Controller controller = testFromOptions(false, options);
        assertTrue(controller instanceof StaticController);

        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));

        controller = testFromOptionsVector(false, options);

        assertEquals(vectorBaseShardCount, controller.baseShardCount);
        assertEquals(vectorSstableGrowthModifier, controller.sstableGrowthModifier, 0.01);
        assertEquals(FBUtilities.parseHumanReadableBytes(vectorMinSSTableSize), controller.minSSTableSize);
        assertEquals(vectorReservedThreads, controller.getReservedThreads());
        assertEquals(FBUtilities.parseHumanReadableBytes(vectorTargetSSTableSize), controller.getTargetSSTableSize());
        for (int i = 0; i < Ws.length; i++)
            assertEquals(vectorScalingParameter, controller.getScalingParameter(i));
    }

    @Test
    public void testValidateOptions()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(false, options);

        super.testValidateOptions(options, false);
    }

    @Test
    public void testValidateVectorOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.VECTOR_BASE_SHARD_COUNT_OPTION, "-1");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
        options.clear();
        options.put(Controller.VECTOR_SSTABLE_GROWTH_OPTION, "-1");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
        options.clear();
        options.put(Controller.VECTOR_RESERVED_THREADS_OPTION, "-1");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
        options.clear();
        options.put(Controller.VECTOR_MIN_SSTABLE_SIZE_OPTION, "10GiB");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
        options.clear();
        options.put(Controller.VECTOR_TARGET_SSTABLE_SIZE_OPTION, "-1MiB");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
        options.clear();
        options.put(Controller.OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES_OPTION, "not true");
        assertThrows(ConfigurationException.class, () -> Controller.validateOptions(options));
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
                                                           false,
                                                           keyspaceName,
                                                           tableName);
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
                                                           false,
                                                           keyspaceName,
                                                           tableName);
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
                                                           false,
                                                           keyspaceName,
                                                           tableName);
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
}
