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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TableSensorTest
{
    public static final String KEYSPACE = "SensorTableTest";
    public static final String CF1 = "Standard1";

    private Context context1;
    private Type type1;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF1,
                                                              1, AsciiType.instance, AsciiType.instance, null));
    }

    @Before
    public void beforeTest()
    {
        context1 = new Context(KEYSPACE, CF1, Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata().id.toString());
        type1 = Type.READ_BYTES;
    }

    @Test
    public void testSyncSensor()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1).metadata());

        TableSensor sensor = new TableSensor(context1, type1);
        double sensorValueSum = 0d;
        sensor.incrementSensor(10d);
        sensor.incrementSensor(3.3d);

        assertThat(sensor.getValue()).isEqualTo(13.3d);
        sensorValueSum += 13.3d;

        sensor.syncSensor();
        assertThat(sensor.getValue()).isEqualTo(0d);
        assertThat(SensorsRegistry.instance.getSensor(context1, type1)).isPresent();

        Sensor registrySensor = SensorsRegistry.instance.getSensor(context1, type1).get();
        assertThat(registrySensor.getValue()).isEqualTo(sensorValueSum);

        sensor.incrementSensor(20d);
        sensor.incrementSensor(2.3d);
        assertThat(sensor.getValue()).isEqualTo(22.3);
        sensorValueSum += 22.3d;

        sensor.syncSensor();
        assertThat(registrySensor.getValue()).isEqualTo(sensorValueSum);
    }
}
