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

package org.apache.cassandra.sensors;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.MonotonicClock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SensorsRegistryTest
{
    public static final String KEYSPACE = "SensorsRegistryTest";
    public static final String CF1 = "Standard1";
    public static final String CF2 = "Standard2";

    private Context context1;
    private Type type1;
    private Context context2;
    private Type type2;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF1,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF2,
                                                              1, AsciiType.instance, AsciiType.instance, null));
    }

    @Before
    public void beforeTest()
    {
        context1 = new Context(KEYSPACE, CF1, Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata().id.toString());
        type1 = Type.READ_BYTES;

        context2 = new Context(KEYSPACE, CF2, Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2).metadata().id.toString());
        type2 = Type.SEARCH_BYTES;
    }

    @After
    public void afterTest()
    {
        SensorsRegistry.instance.clear();
    }

    @Test
    public void testCreateAndGetSensors()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2).metadata());

        Sensor context1Type1Sensor = SensorsRegistry.instance.getOrCreateSensor(context1, type1).get();
        Sensor context1Type2Sensor = SensorsRegistry.instance.getOrCreateSensor(context1, type2).get();
        Sensor context2Type1Sensor = SensorsRegistry.instance.getOrCreateSensor(context2, type1).get();
        Sensor context2Type2Sensor = SensorsRegistry.instance.getOrCreateSensor(context2, type2).get();

        assertThat(SensorsRegistry.instance.getSensor(context1, type1)).hasValue(context1Type1Sensor);
        assertThat(SensorsRegistry.instance.getSensor(context1, type2)).hasValue(context1Type2Sensor);
        assertThat(SensorsRegistry.instance.getSensor(context2, type1)).hasValue(context2Type1Sensor);
        assertThat(SensorsRegistry.instance.getSensor(context2, type2)).hasValue(context2Type2Sensor);

        // get sensor should not have side effects
        Type unRegisteredType = Type.WRITE_BYTES;
        assertThat(SensorsRegistry.instance.getSensor(context1, unRegisteredType)).isEmpty();

        assertThat(SensorsRegistry.instance.getSensorsByKeyspace(KEYSPACE)).containsAll(
        ImmutableSet.of(context1Type1Sensor, context1Type2Sensor, context2Type1Sensor, context2Type2Sensor));

        assertThat(SensorsRegistry.instance.getSensorsByTableId(context1.getTableId())).containsAll(
        ImmutableSet.of(context1Type1Sensor, context1Type2Sensor));

        assertThat(SensorsRegistry.instance.getSensorsByTableId(context2.getTableId())).containsAll(
        ImmutableSet.of(context2Type1Sensor, context2Type2Sensor));

        assertThat(SensorsRegistry.instance.getSensorsByType(type1)).containsAll(
        ImmutableSet.of(context1Type1Sensor, context2Type1Sensor));

        assertThat(SensorsRegistry.instance.getSensorsByType(type2)).containsAll(
        ImmutableSet.of(context1Type2Sensor, context2Type2Sensor));
    }

    @Test
    public void testCannotGetSensorForMissingKeyspace()
    {
        assertThat(SensorsRegistry.instance.getOrCreateSensor(context1, type1)).isEmpty();
    }

    @Test
    public void testCannotGetSensorForMissingTable()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());

        assertThat(SensorsRegistry.instance.getOrCreateSensor(context1, type1)).isEmpty();
    }

    @Test
    public void testUpdateSensor()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        SensorsRegistry.instance.updateSensor(context1, type1, 1.0);
        assertThat(SensorsRegistry.instance.getOrCreateSensor(context1, type1)).hasValueSatisfying((s) -> assertThat(s.getValue()).isEqualTo(1.0));
    }

    @Test
    public void testUpdateSensorAsync() throws ExecutionException, InterruptedException, TimeoutException
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        SensorsRegistry.instance.updateSensorAsync(context1, type1, 1.0, 1, TimeUnit.MILLISECONDS).get(1, TimeUnit.SECONDS);
        assertThat(SensorsRegistry.instance.getOrCreateSensor(context1, type1)).hasValueSatisfying((s) -> assertThat(s.getValue()).isEqualTo(1.0));
    }

    @Test
    public void testSensorRegistryListener()
    {
        SensorsRegistryListener listener = Mockito.mock(SensorsRegistryListener.class);
        SensorsRegistry.instance.registerListener(listener);

        // The sensor will not be created as the keyspace has not been created yet
        Optional<Sensor> emptySensor = SensorsRegistry.instance.getOrCreateSensor(context1, type1);
        assertThat(emptySensor).isEmpty();
        verify(listener, never()).onSensorCreated(any());
        verify(listener, never()).onSensorRemoved(any());

        // Initialize the schema
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2).metadata());

        // Create sensors and verify the listener is notified
        Sensor context1Type1Sensor = SensorsRegistry.instance.getOrCreateSensor(context1, type1).get();
        verify(listener, times(1)).onSensorCreated(context1Type1Sensor);

        Sensor context1Type2Sensor = SensorsRegistry.instance.getOrCreateSensor(context1, type2).get();
        verify(listener, times(1)).onSensorCreated(context1Type2Sensor);

        Sensor context2Type1Sensor = SensorsRegistry.instance.getOrCreateSensor(context2, type1).get();
        verify(listener, times(1)).onSensorCreated(context2Type1Sensor);

        Sensor context2Type2Sensor = SensorsRegistry.instance.getOrCreateSensor(context2, type2).get();
        verify(listener, times(1)).onSensorCreated(context2Type2Sensor);

        verify(listener, never()).onSensorRemoved(any());

        // Drop the table and verify the listener is notified about removal of related sensors
        SensorsRegistry.instance.onDropTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2).metadata(), false);
        verify(listener, times(1)).onSensorRemoved(context2Type1Sensor);
        verify(listener, times(1)).onSensorRemoved(context2Type2Sensor);
        verify(listener, never()).onSensorRemoved(context1Type1Sensor);
        verify(listener, never()).onSensorRemoved(context1Type2Sensor);

        // Drop the keyspace and verify the listener is notified about removal of the remaining sensors
        SensorsRegistry.instance.onDropKeyspace(Keyspace.open(KEYSPACE).getMetadata(), false);
        verify(listener, times(1)).onSensorRemoved(context1Type1Sensor);
        verify(listener, times(1)).onSensorRemoved(context1Type2Sensor);
        verify(listener, times(1)).onSensorRemoved(context2Type1Sensor);
        verify(listener, times(1)).onSensorRemoved(context2Type2Sensor);

        // Unregister the listener and verify it is not notified anymore about creation and removal of sensors
        clearInvocations(listener);
        SensorsRegistry.instance.unregisterListener(listener);

        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        assertThat(SensorsRegistry.instance.getOrCreateSensor(context1, type1)).isPresent();
        SensorsRegistry.instance.onDropKeyspace(Keyspace.open(KEYSPACE).getMetadata(), false);

        verify(listener, never()).onSensorCreated(any());
        verify(listener, never()).onSensorRemoved(any());
    }

    @Test
    public void testAggregateSensors()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF2).metadata());

        SensorsRegistry.instance.updateSensor(context1, type1, 1.0);
        SensorsRegistry.instance.updateSensor(context1, type1, 2.0);
        SensorsRegistry.instance.updateSensor(context2, type1, 100.0);
        SensorsRegistry.instance.updateSensor(context2, type2, 4.0); // should not be aggregated
        SensorsRegistry.instance.updateSensor(context2, type2, 5.0); // should not be aggregated

        SensorsRegisterAggregator aggregator = new SensorsRegisterAggregator()
        {
            @Override
            public Function<Sensor, Double> aggregatorFn()
            {
                return s -> s.getValue() < 100 ? s.getValue() * s.getValue() : s.getValue();
            }

            @Override
            public Predicate<Sensor> aggregatorFilter()
            {
                return (s) -> s.getContext() == context1 || s.getContext() == context2;
            }
        };

        SensorsRegistry.instance.registerSensorAggregator(aggregator, type1);
        double aggregatedValue = SensorsRegistry.instance.aggregateSensorsByType(type1);
        double expectedContext1Type1Value = 1.0 + 2.0; // (context1, type1) summed up because of the monotonic nature of the registry
        double expectedValue = expectedContext1Type1Value * expectedContext1Type1Value + 100.0;  // (context1, type1) is squared and (context2, type1) is not because of the aggregator definition
        assertThat(aggregatedValue).isEqualTo(expectedValue);
    }

    @Test
    public void testSensorsRate()
    {
        int rateWindowInSeconds = 10;
        System.setProperty(SensorsRegistry.SENSORS_RATE_WINDOW_IN_SECONDS_SYSTEM_PROPERTY, "" + rateWindowInSeconds);
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        MonotonicClock clock = Mockito.mock(MonotonicClock.class);
        SensorsRegistry.instance.setClock(clock);
        AtomicInteger clockCalls = new AtomicInteger();
        Mockito.doAnswer(ignored -> {
            clockCalls.incrementAndGet();
            /**
             * TEST CASE 1: aggregate the first 4 sensors updates into the same window:
             * w0 = [0, 10), now0 = 1. The 5th clock call is when rate is calculated
             */
            if (clockCalls.get() <= 5) //
                return TimeUnit.SECONDS.toNanos(1);
            /**
             * TEST CASE 2A: advance the clock to the next rate window w1 = [10, 20), now1 = 11
             */
            else if (clockCalls.get() == 6)
                return TimeUnit.SECONDS.toNanos(rateWindowInSeconds + 1);
            /**
             * TEST CASE 2B: advance the clock to the next rate window: w1 = [10, 20), now2 = 12. The 8th clock cal is when rate is calculated
             */
            else if (clockCalls.get() == 7 || (clockCalls.get() == 8))
            /**
             * TEST CASE 3: advance the clock to the next rate window without updating the sensor: w1 = [20, 30), now3 = 22
             */
                return TimeUnit.SECONDS.toNanos(rateWindowInSeconds + 2);
            else if (clockCalls.get() == 9)
                return TimeUnit.SECONDS.toNanos(TimeUnit.SECONDS.toNanos(rateWindowInSeconds * 2 + 2));
            throw new IllegalStateException("Unexpected clock call");
        }).when(clock).now();

        // TEST CASE 1
        // all the following updates should happen withing the same rate window
        double v1 = 1.0, v2 = 2.0, v3 = 3.0, v4 = 4.0;
        SensorsRegistry.instance.updateSensor(context1, type1, v1);
        SensorsRegistry.instance.updateSensor(context1, type1, v2);
        SensorsRegistry.instance.updateSensor(context1, type1, v3);
        SensorsRegistry.instance.updateSensor(context1, type1, v4);

        Sensor sensor = SensorsRegistry.instance.getSensor(context1, type1).get();
        double rate = SensorsRegistry.instance.getSensorRate(sensor);
        assertThat(rate).isEqualTo(v1 + v2 + v3 + v4);

        // TEST CASE 2A
        double v5 = 5.1;
        SensorsRegistry.instance.updateSensor(context1, type1, v5);

        // TEST CASE 2B
        double v6 = 6.2;
        SensorsRegistry.instance.updateSensor(context1, type1, v6);

        rate = SensorsRegistry.instance.getSensorRate(sensor);
        double epsilon = 0.00000001;
        assertThat(rate).isEqualTo(v5 + v6, withPrecision(epsilon));
        // make sure the value is indeed ever increasing
        assertThat(sensor.getValue()).isEqualTo(v1 + v2 + v3 + v4 + v5 + v6, withPrecision(epsilon));

        // TEST CASE 3
        rate = SensorsRegistry.instance.getSensorRate(sensor);
        assertThat(rate).isEqualTo(0D, withPrecision(epsilon));
    }
}