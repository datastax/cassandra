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
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.junit.Assert.assertNull;

public class SensorsCustomParamsWithDefaultSensorsFactoryTest
{
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        // falls back to default SensorsFactory
        CassandraRelevantProperties.SENSORS_FACTORY.reset();
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        // enables constructing Messages with custom parameters
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testAddSensorsToInternodeResponse()
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
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 17.0);
        sensors.syncAllSensors();

        Message.Builder<NoPayload> builder =
        Message.builder(Verb._TEST_1, noPayload)
               .withId(1);

        SensorsCustomParams.addSensorsToInternodeResponse(sensors, builder);

        Message<NoPayload> msg = builder.build();
        assertNull(msg.header.customParams());
    }

    @Test
    public void testAddSensorToCQLResponse()
    {
        String table = "t1";
        RequestSensors sensors = SensorsFactory.instance.createRequestSensors("ks1");
        ResultMessage message = new ResultMessage.Void();
        Context context = new Context("ks1", table, UUID.randomUUID().toString());
        Type type = Type.READ_BYTES;
        sensors.registerSensor(context, type);
        sensors.incrementSensor(context, type, 13.0);

        SensorsCustomParams.addSensorToCQLResponse(message, ProtocolVersion.V4, sensors, context, type);

        assertNull(message.getCustomPayload());
    }
}
