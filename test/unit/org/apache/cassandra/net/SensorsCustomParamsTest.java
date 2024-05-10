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

package org.apache.cassandra.net;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SensorsCustomParamsTest
{
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        // enables constructuing Messages with custom parameters
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);
    }

    @Test
    public void testSensorValueAsBytes()
    {
        double d = Double.MAX_VALUE;
        byte[] bytes = SensorsCustomParams.sensorValueAsBytes(d);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        assertEquals(Double.MAX_VALUE, bb.getDouble(), 0.0);
    }

    @Test
    public void testSensorValueFromBytes()
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(Double.MAX_VALUE);
        double d = SensorsCustomParams.sensorValueFromBytes(buffer.array());
        assertEquals(Double.MAX_VALUE, d, 0.0);
    }

    @Test
    public void testEncodeTableInWriteByteRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_REQUEST.%s", "t1");
        String actualParam = SensorsCustomParams.encodeTableInWriteBytesRequestParam(table);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInWriteByteTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_TABLE.%s", "t1");
        String actualParam = SensorsCustomParams.encodeTableInWriteBytesTableParam(table);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("INDEX_WRITE_BYTES_REQUEST.%s", table);
        String actualParam = SensorsCustomParams.encodeTableInIndexWriteBytesRequestParam(table);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("INDEX_WRITE_BYTES_TABLE.%s", "t1");
        String actualParam = SensorsCustomParams.encodeTableInIndexWriteBytesTableParam(table);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testIndexReadBytesRequestParam()
    {
        String expectedParam = "INDEX_READ_BYTES_REQUEST";
        String actualParam = SensorsCustomParams.INDEX_READ_BYTES_REQUEST;
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testIndexReadBytesTableParam()
    {
        String expectedParam = "INDEX_READ_BYTES_TABLE";
        String actualParam = SensorsCustomParams.INDEX_READ_BYTES_TABLE;
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testAddWriteSensorToResponse()
    {
        RequestSensors sensors = new RequestSensors();
        UUID tableId = UUID.randomUUID();
        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks1", null);
        TableMetadata tm = TableMetadata.builder("ks1", "t1", TableId.fromString(tableId.toString()))
                                        .addPartitionKeyColumn("pk", AsciiType.instance)
                                        .build();
        SensorsRegistry.instance.onCreateKeyspace(ksm);
        SensorsRegistry.instance.onCreateTable(tm);

        Context context = new Context("ks1", "t1", tableId.toString());
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 17.0);
        sensors.syncAllSensors();

        Message.Builder<NoPayload> builder =
        Message.builder(Verb._TEST_1, noPayload)
               .withId(1);

        SensorsCustomParams.addWriteSensorToResponse(builder, sensors, context);

        Message<NoPayload> msg = builder.build();
        assertNotNull(msg.header.customParams());
        assertEquals(2, msg.header.customParams().size());
        String requestParam = SensorsCustomParams.encodeTableInWriteBytesRequestParam("t1");
        String tableParam = SensorsCustomParams.encodeTableInWriteBytesTableParam("t1");
        assertTrue(msg.header.customParams().containsKey(requestParam));
        assertTrue(msg.header.customParams().containsKey(tableParam));
        double epsilon = 0.000001;
        assertEquals(17.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(requestParam)), epsilon);
        assertEquals(17.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(tableParam)), epsilon);
    }

    @Test
    public void testAddReadSensorToResponse()
    {
        RequestSensors sensors = new RequestSensors();
        UUID tableId = UUID.randomUUID();
        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks1", null);
        TableMetadata tm = TableMetadata.builder("ks1", "t1", TableId.fromString(tableId.toString()))
                                        .addPartitionKeyColumn("pk", AsciiType.instance)
                                        .build();
        SensorsRegistry.instance.onCreateKeyspace(ksm);
        SensorsRegistry.instance.onCreateTable(tm);

        Context context = new Context("ks1", "t1", tableId.toString());
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.incrementSensor(context, Type.READ_BYTES, 13.0);
        sensors.syncAllSensors();

        Message.Builder<NoPayload> builder =
        Message.builder(Verb._TEST_1, noPayload)
               .withId(1);

        SensorsCustomParams.addReadSensorToResponse(builder, sensors, context);

        Message<NoPayload> msg = builder.build();
        assertNotNull(msg.header.customParams());
        assertEquals(2, msg.header.customParams().size());
        assertTrue(msg.header.customParams().containsKey(SensorsCustomParams.READ_BYTES_REQUEST));
        assertTrue(msg.header.customParams().containsKey(SensorsCustomParams.READ_BYTES_REQUEST));
        double epsilon = 0.000001;
        assertEquals(13.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(SensorsCustomParams.READ_BYTES_REQUEST)), epsilon);
        assertEquals(13.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(SensorsCustomParams.READ_BYTES_REQUEST)), epsilon);
    }
}
