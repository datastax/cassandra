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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        assertNull(SensorsRegistry.instance.getOrCreateSensor(context1, type1).orElse(null));

    }

    @Test
    public void testCannotGetSensorForMissingTable()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());

        assertNull(SensorsRegistry.instance.getOrCreateSensor(context1, type1).orElse(null));
    }

    @Test
    public void testUpdateSensor()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        SensorsRegistry.instance.updateSensor(context1, type1, 1.0);
        assertEquals(1.0, SensorsRegistry.instance.getOrCreateSensor(context1, type1).get().getValue(), 0);
    }

    @Test
    public void testUpdateSensorAsync() throws ExecutionException, InterruptedException, TimeoutException
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        SensorsRegistry.instance.updateSensorAsync(context1, type1, 1.0, 1, TimeUnit.MILLISECONDS).get(1, TimeUnit.SECONDS);
        assertEquals(1.0, SensorsRegistry.instance.getOrCreateSensor(context1, type1).get().getValue(), 0);

    }
}