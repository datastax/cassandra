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

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

public class ActiveRequestSensorsFactoryTest
{
    private ActiveRequestSensorsFactory factory;
    private SensorEncoder encoder;

    @Before
    public void before()
    {
        factory = new ActiveRequestSensorsFactory();
        encoder = factory.createSensorEncoder();
    }

    @Test
    public void testCreateActiveRequestSensors()
    {
        RequestSensors sensors = factory.create("ks1");
        assertThat(sensors).isNotNull();
        assertThat(sensors).isInstanceOf(ActiveRequestSensors.class);
        RequestSensors anotherSensors = factory.create("ks1");
        assertThat(sensors).isNotSameAs(anotherSensors);
    }

    @Test
    public void testEncodeTableInReadByteRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("READ_BYTES_REQUEST.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.READ_BYTES);
        String actualParam = encoder.encodeRequestSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInReadByteTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("READ_BYTES_GLOBAL.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.READ_BYTES);
        String actualParam = encoder.encodeGlobalSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInWriteByteRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_REQUEST.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.WRITE_BYTES);
        String actualParam = encoder.encodeRequestSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInWriteByteTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_GLOBAL.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.WRITE_BYTES);
        String actualParam = encoder.encodeGlobalSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("INDEX_WRITE_BYTES_REQUEST.%s", table);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        String actualParam = encoder.encodeRequestSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("INDEX_WRITE_BYTES_GLOBAL.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        String actualParam = encoder.encodeGlobalSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInInternodeBytesRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("INTERNODE_BYTES_REQUEST.%s", table);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        String actualParam = encoder.encodeRequestSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInInternodeBytesTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("INTERNODE_BYTES_GLOBAL.%s", table);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        String actualParam = encoder.encodeGlobalSensor(sensor);
        assertEquals(expectedParam, actualParam);
    }

    static class mockingSensor extends Sensor
    {
        public mockingSensor(Context context, Type type)
        {
            super(context, type);
        }
    }
}
