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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SensorsCustomParamsWithActiveSensorsFactoryTest
{
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

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
    public void testAddWriteSensorToInternodeResponse()
    {
        testAddSensorsToInternodeResponse(Type.WRITE_BYTES);
    }

    @Test
    public void testAddReadSensorToInternodeResponse()
    {
        testAddSensorsToInternodeResponse(Type.READ_BYTES);
    }

    @Test
    public void testAddRMUSensorToInternodeResponse()
    {
        // RMU is now a coordinator-side computation; replicas just propagate READ_BYTES.
        // Verify that an RMU sensor registered and incremented on the coordinator is encoded
        // correctly in an internode response message (used when the coordinator forwards the
        // aggregated result to a peer or for testing the internode encoding path).
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        UUID tableId = UUID.randomUUID();
        Context context = new Context("ks1", "t1", tableId.toString());

        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.RMU);

        sensors.incrementSensor(context, Type.READ_BYTES, 500_000.0);
        sensors.incrementSensor(context, Type.READ_EXECUTION_TIME, 100_000_000.0); // 100 ms
        // With default baseline=-1, RMU = read_bytes * 4000
        SensorsCustomParams.computeRMU(sensors);

        Message.Builder<NoPayload> builder = Message.builder(Verb._TEST_1, noPayload).withId(1);
        SensorsCustomParams.addSensorsToInternodeResponse(sensors, builder);

        Message<NoPayload> msg = builder.build();
        assertNotNull(msg.header.customParams());

        Sensor rmuSensor = sensors.getSensor(context, Type.RMU).get();
        String rmuRequestParam = SensorsCustomParams.paramForRequestSensor(rmuSensor).get();
        assertTrue(msg.header.customParams().containsKey(rmuRequestParam));

        // baseline=-1 → RMU = read_bytes * MU_SCALE = 500_000 * 4000 = 2_000_000_000
        double expectedRMU = 500_000.0 * 4000.0;
        assertEquals(expectedRMU, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(rmuRequestParam)), 0.0);
    }

    @Test
    public void testComputeTMU_viaCustomParams_readPath()
    {
        // Pure read: TMU = RMU (WMU absent); baseline=-1 → RMU = read_bytes * MU_SCALE
        String ks = "ks_tmu1";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 400_000.0);
        sensors.incrementSensor(context, Type.READ_EXECUTION_TIME, 100_000_000.0); // 100 ms

        SensorsCustomParams.computeRMU(sensors);
        SensorsCustomParams.computeTMU(sensors);

        double expectedRMU = 400_000.0 * 4000.0;
        assertEquals(expectedRMU, sensors.getSensor(context, Type.TMU).get().getValue(), 0.0);
    }

    @Test
    public void testComputeTMU_viaCustomParams_writePath()
    {
        // Pure write: TMU = WMU (RMU absent); baseline=-1 → WMU = write_bytes * MU_SCALE
        String ks = "ks_tmu2";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context, Type.WMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 250_000.0);
        sensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, 200_000_000.0); // 200 ms

        SensorsCustomParams.computeWMU(sensors);
        SensorsCustomParams.computeTMU(sensors);

        double expectedWMU = 250_000.0 * 4000.0;
        assertEquals(expectedWMU, sensors.getSensor(context, Type.TMU).get().getValue(), 0.0);
    }

    @Test
    public void testComputeTMU_viaCustomParams_casPath()
    {
        // CAS: TMU = WMU + RMU; baseline=-1 → each = bytes * MU_SCALE
        String ks = "ks_tmu3";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.WMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 300_000.0);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 150_000.0);

        SensorsCustomParams.computeRMU(sensors);
        SensorsCustomParams.computeWMU(sensors);
        SensorsCustomParams.computeTMU(sensors);

        double expectedTMU = (300_000.0 + 150_000.0) * 4000.0;
        assertEquals(expectedTMU, sensors.getSensor(context, Type.TMU).get().getValue(), 0.0);
    }

    @Test
    public void testAddWMUSensorToInternodeResponse()
    {
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        UUID tableId = UUID.randomUUID();
        Context context = new Context("ks1", "t1", tableId.toString());

        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context, Type.WMU);

        sensors.incrementSensor(context, Type.WRITE_BYTES, 300_000.0);
        sensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, 200_000_000.0); // 200 ms
        // With default baseline=-1, WMU = write_bytes * 4000
        SensorsCustomParams.computeWMU(sensors);

        Message.Builder<NoPayload> builder = Message.builder(Verb._TEST_2, noPayload).withId(2);
        SensorsCustomParams.addSensorsToInternodeResponse(sensors, builder);

        Message<NoPayload> msg = builder.build();
        assertNotNull(msg.header.customParams());

        Sensor wmuSensor = sensors.getSensor(context, Type.WMU).get();
        String wmuRequestParam = SensorsCustomParams.paramForRequestSensor(wmuSensor).get();
        assertTrue(msg.header.customParams().containsKey(wmuRequestParam));

        double expectedWMU = 300_000.0 * 4000.0;
        assertEquals(expectedWMU, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(wmuRequestParam)), 0.0);
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
    public void testAddSensorsToCQLResponse()
    {
        String table = "t1";
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;
        double expectedValue = 17.0;

        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, expectedValue);

        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V4, sensors, context, type);

        assertNotNull(message.getCustomPayload());

        Sensor sensor = sensors.getSensor(context, type).get();
        String expectedHeader = SensorsCustomParams.paramForRequestSensor(sensor).get();
        assertTrue(message.getCustomPayload().containsKey(expectedHeader));
        assertEquals(expectedValue, message.getCustomPayload().get(expectedHeader).getDouble(), 0.0);
    }

    @Test
    public void testAddSensorsToCQLResponseWithExistingCustomPayload()
    {
        String table = "t1";
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        ResultMessage message = new ResultMessage.Void();
        String existingKey = "existingKey";
        String existingValue = "existingValue";
        Map<String, ByteBuffer> customPayload = new HashMap<>();
        customPayload.put(existingKey, ByteBuffer.wrap(existingValue.getBytes(StandardCharsets.UTF_8)));
        message.setCustomPayload(customPayload);
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Type type = Type.READ_BYTES;
        double expectedValue = 13.0;

        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, expectedValue);

        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V4, sensors, context, type);

        assertNotNull(message.getCustomPayload());
        assertEquals( 2, message.getCustomPayload().size());

        Sensor sensor = sensors.getSensor(context, type).get();
        String expectedHeader = SensorsCustomParams.paramForRequestSensor(sensor).get();
        assertTrue(message.getCustomPayload().containsKey(expectedHeader));
        assertEquals(expectedValue, message.getCustomPayload().get(expectedHeader).getDouble(), 0.0);

        assertTrue(message.getCustomPayload().containsKey(existingKey));
        assertEquals(existingValue, StandardCharsets.UTF_8.decode(message.getCustomPayload().get(existingKey)).toString());
    }

    @Test
    public void testAddSensorsToCQLResponseSkippedWhenResponseIsNull()
    {
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        Context context = new Context("ks1", "t1", UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;
        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, 17.0);

        // Must not throw even when the response is null
        SensorsCustomParams.addSensorToCQLResponse(null, ProtocolVersion.V4, sensors, context, type);
    }

    @Test
    public void testAddSensorsToCQLResponseSkippedWhenSensorsIsNull()
    {
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", "t1", UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;

        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V4, null, context, type);

        assertNull(message.getCustomPayload());
    }

    @Test
    public void testAddSensorsToCQLResponseSkippedWhenProtocolVersionBelowV4()
    {
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", "t1", UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;
        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, 17.0);

        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V3, sensors, context, type);

        assertNull(message.getCustomPayload());
    }

    @Test
    public void testAddSensorsToCQLResponseSkippedWhenDisabledViaProperty()
    {
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", "t1", UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;
        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, 17.0);

        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(false);
        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V4, sensors, context, type);

        assertNull(message.getCustomPayload());
    }

    @Test
    public void testAddSensorsToCQLResponseSkippedWhenZero()
    {
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", "t1", UUID.randomUUID().toString());
        Type type = Type.WRITE_BYTES;

        // Sensor registered but never incremented — value is 0.
        sensors.registerSensor(context, type);

        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V4, sensors, context, type);

        assertNull(message.getCustomPayload());
    }

    private void testAddSensorsToInternodeResponse(Type sensorType)
    {
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
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

        SensorsCustomParams.addSensorsToInternodeResponse(sensors, builder);

        Message<NoPayload> msg = builder.build();
        assertNotNull(msg.header.customParams());
        assertEquals(2, msg.header.customParams().size());
        Sensor sensor = sensors.getSensor(context, sensorType).get();
        String requestParam = SensorsCustomParams.paramForRequestSensor(sensor).get();
        String globalParam = SensorsCustomParams.paramForGlobalSensor(sensor).get();
        assertTrue(msg.header.customParams().containsKey(requestParam));
        assertTrue(msg.header.customParams().containsKey(globalParam));
        double epsilon = 0.000001;
        assertEquals(17.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(requestParam)), epsilon);
        assertEquals(17.0, SensorsCustomParams.sensorValueFromBytes(msg.header.customParams().get(globalParam)), epsilon);
    }
}
