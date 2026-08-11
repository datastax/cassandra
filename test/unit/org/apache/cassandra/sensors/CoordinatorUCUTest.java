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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CoordinatorUCUTest
{
    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);
    }

    @Test
    public void testCoordinatorUCUAggregationAndCQLPayload()
    {
        String ks = "ks1";
        String table = "t1";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.WRITE_BYTES);
        coordinatorSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        coordinatorSensors.registerSensor(context, Type.UCU);

        // Simulate replica 1 response sending UCU = 150.0
        RequestSensors replica1Sensors = SensorsFactory.instance.createRequestSensors(ks);
        replica1Sensors.registerSensor(context, Type.READ_BYTES);
        replica1Sensors.registerSensor(context, Type.UCU);
        replica1Sensors.incrementSensor(context, Type.READ_BYTES, 150.0);

        Message.Builder<NoPayload> builder1 = Message.builder(Verb._TEST_1, noPayload).withId(1);
        SensorsCustomParams.addSensorsToInternodeResponse(replica1Sensors, builder1);
        Message<NoPayload> msg1 = builder1.build();

        // Simulate replica 2 response sending UCU = 200.0
        RequestSensors replica2Sensors = SensorsFactory.instance.createRequestSensors(ks);
        replica2Sensors.registerSensor(context, Type.WRITE_BYTES);
        replica2Sensors.registerSensor(context, Type.UCU);
        replica2Sensors.incrementSensor(context, Type.WRITE_BYTES, 200.0);

        Message.Builder<NoPayload> builder2 = Message.builder(Verb._TEST_2, noPayload).withId(2);
        SensorsCustomParams.addSensorsToInternodeResponse(replica2Sensors, builder2);
        Message<NoPayload> msg2 = builder2.build();

        // Aggregate replica 1 and replica 2 UCU values onto coordinator sensors
        Sensor ucuSensor = coordinatorSensors.getSensor(context, Type.UCU).get();
        String ucuHeader = SensorsCustomParams.paramForRequestSensor(ucuSensor).get();

        double replica1UCU = SensorsCustomParams.sensorValueFromInternodeResponse(msg1, ucuHeader);
        double replica2UCU = SensorsCustomParams.sensorValueFromInternodeResponse(msg2, ucuHeader);

        coordinatorSensors.incrementSensor(context, Type.UCU, replica1UCU);
        coordinatorSensors.incrementSensor(context, Type.UCU, replica2UCU);

        assertThat(coordinatorSensors.getSensor(context, Type.UCU).get().getValue()).isEqualTo(350.0);

        // Populate CQL protocol response custom payload
        ResultMessage result = new ResultMessage.Void();
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, coordinatorSensors, context, Type.UCU);

        assertNotNull(result.getCustomPayload());
        assertTrue(result.getCustomPayload().containsKey(ucuHeader));
        assertThat(result.getCustomPayload().get(ucuHeader).getDouble()).isEqualTo(350.0);
    }
}
