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

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
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
}