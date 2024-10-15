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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.ActiveRequestSensorsFactory;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestSensorsFactory;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SensorsCustomParamsTest
{
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.REQUEST_SENSORS_FACTORY.setString(ActiveRequestSensorsFactory.class.getName());

        // enables constructing Messages with custom parameters
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
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.WRITE_BYTES);
        String actualParam = SensorsCustomParams.requestParamForSensor(sensor, true);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInWriteByteTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_TABLE.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.WRITE_BYTES);
        String actualParam = SensorsCustomParams.tableParamForSensor(sensor, true);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("INDEX_WRITE_BYTES_REQUEST.%s", table);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        String actualParam = SensorsCustomParams.requestParamForSensor(sensor, true);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInIndexWriteBytesTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("INDEX_WRITE_BYTES_TABLE.%s", "t1");
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        String actualParam = SensorsCustomParams.tableParamForSensor(sensor, true);
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
    public void testEncodeTableInInternodeBytesRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("INTERNODE_MSG_BYTES_REQUEST.%s", table);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        String actualParam = SensorsCustomParams.requestParamForSensor(sensor, true);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInInternodeBytesTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("INTERNODE_MSG_BYTES_TABLE.%s", table);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Sensor sensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        String actualParam = SensorsCustomParams.tableParamForSensor(sensor, true);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testAddWriteSensorToResponse()
    {
        testAddSensorsToResponse(Type.WRITE_BYTES,
                                 true);
    }

    @Test
    public void testAddReadSensorToResponse()
    {
        testAddSensorsToResponse(Type.READ_BYTES,
                                 false);
    }

    @Test
    public void testSensorValueAsByteBuffer()
    {
        double d = Double.MAX_VALUE;
        ByteBuffer bb = SensorsCustomParams.sensorValueAsByteBuffer(d);
        // bb should already be flipped
        assertEquals(bb.position(), 0);
        assertEquals(d, ByteBufferUtil.toDouble(bb), 0.0);
    }

    @Test
    public void testAddSensorsToMessageResponse()
    {
        String table = "t1";
        RequestSensors sensors = RequestSensorsFactory.instance.create("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;
        double expectedValue = 17.0;

        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, expectedValue);

        SensorsCustomParams.addSensorToMessageResponse(message, ProtocolVersion.V4, sensors, context, type);

        assertNotNull(message.getCustomPayload());

        String expectedHeader = SensorsCustomParams.requestParamForSensor(sensors.getSensor(context, type).get(), true);
        assertTrue(message.getCustomPayload().containsKey(expectedHeader));
        assertEquals(17.0, message.getCustomPayload().get(expectedHeader).getDouble(), 0.0);
    }

    @Test
    public void testAddSensorsToMessageResponseSkipped()
    {
        String table = "t1";
        RequestSensors sensors = RequestSensorsFactory.instance.create("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;
        double expectedValue = 17.0;

        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, expectedValue);

        SensorsCustomParams.addSensorToMessageResponse(null, ProtocolVersion.V4, sensors, context, type);
        SensorsCustomParams.addSensorToMessageResponse(message, ProtocolVersion.V4, null, context, type);
        SensorsCustomParams.addSensorToMessageResponse(message, ProtocolVersion.V3, null, context, type);

        assertNull(message.getCustomPayload());
    }

    private void testAddSensorsToResponse(Type sensorType, boolean applySuffix)
    {
        RequestSensors sensors = RequestSensorsFactory.instance.create("ks1");
        UUID tableId = UUID.randomUUID();
        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks1", null);
        TableMetadata tm = TableMetadata.builder("ks1", "t1", TableId.fromString(tableId.toString()))
                                        .addPartitionKeyColumn("pk", AsciiType.instance)
                                        .build();
        SensorsRegistry.instance.onCreateKeyspace(ksm);
        SensorsRegistry.instance.onCreateTable(tm);

        Context context = new Context("ks1", "t1", tableId.toString());
        sensors.registerSensor(context, sensorType);
        sensors.incrementSensor(context, sensorType, 17.0);
        sensors.syncAllSensors();

        Message.Builder<NoPayload> builder =
        Message.builder(Verb._TEST_1, noPayload)
               .withId(1);

        SensorsCustomParams.addSensorsToResponse(sensors, builder, applySuffix);

        Message<NoPayload> msg = builder.build();
        assertNotNull(msg.header.customParams());
        assertEquals(2, msg.header.customParams().size());
        String requestParam = SensorsCustomParams.requestParamForSensor(sensors.getSensor(context, sensorType).get(), applySuffix);
        String tableParam = SensorsCustomParams.tableParamForSensor(sensors.getSensor(context, sensorType).get(), applySuffix);
        assertTrue(msg.header.customParams().containsKey(requestParam));
        assertTrue(msg.header.customParams().containsKey(tableParam));
        double epsilon = 0.000001;
        assertEquals(17.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(requestParam)), epsilon);
        assertEquals(17.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(tableParam)), epsilon);
    }

    static class mockingSensor extends Sensor
    {
        public mockingSensor(Context context, Type type)
        {
            super(context, type);
        }
    }
}
