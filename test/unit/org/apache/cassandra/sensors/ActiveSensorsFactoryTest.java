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
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ActiveSensorsFactoryTest
{
    private final static String KEYSPACE = "ks1";
    private final static String TABLE = "table1";
    private ActiveSensorsFactory factory;
    private SensorEncoder encoder;

    @Before
    public void before()
    {
        factory = new ActiveSensorsFactory();
        encoder = factory.createSensorEncoder();
    }

    @Test
    public void testCreateActiveRequestSensors()
    {
        RequestSensors sensors = factory.createRequestSensors(KEYSPACE);
        assertThat(sensors).isNotNull();
        assertThat(sensors).isInstanceOf(ActiveRequestSensors.class);
        RequestSensors anotherSensors = factory.createRequestSensors(KEYSPACE);
        assertThat(sensors).isNotSameAs(anotherSensors);
    }

    @Test
    public void testEncodeTableInReadByteRequestParam()
    {
        String expectedParam = String.format("READ_BYTES_REQUEST.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.READ_BYTES);
        Optional<String> actualParam = encoder.encodeRequestSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInReadByteGlobalParam()
    {
        String expectedParam = String.format("READ_BYTES_GLOBAL.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.READ_BYTES);
        Optional<String> actualParam = encoder.encodeGlobalSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInWriteByteRequestParam()
    {
        String expectedParam = String.format("WRITE_BYTES_REQUEST.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.WRITE_BYTES);
        Optional<String> actualParam = encoder.encodeRequestSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInWriteByteGlobalParam()
    {
        String expectedParam = String.format("WRITE_BYTES_GLOBAL.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.WRITE_BYTES);
        Optional<String> actualParam = encoder.encodeGlobalSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesRequestParam()
    {
        String expectedParam = String.format("INDEX_WRITE_BYTES_REQUEST.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        Optional<String> actualParam = encoder.encodeRequestSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesGlobalParam()
    {
        String expectedParam = String.format("INDEX_WRITE_BYTES_GLOBAL.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        Optional<String> actualParam = encoder.encodeGlobalSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInInternodeBytesRequestParam()
    {
        String expectedParam = String.format("INTERNODE_BYTES_REQUEST.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        Optional<String> actualParam = encoder.encodeRequestSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    @Test
    public void testEncodeTableInInternodeBytesGlobalParam()
    {
        String expectedParam = String.format("INTERNODE_BYTES_GLOBAL.%s.%s", KEYSPACE, TABLE);
        Context context = new Context(KEYSPACE, TABLE, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        Optional<String> actualParam = encoder.encodeGlobalSensorName(sensor);
        assertThat(actualParam).hasValue(expectedParam);
    }

    static class mockingSensor extends Sensor
    {
        public mockingSensor(Context context, Type type)
        {
            super(context, type);
        }
    }
}
