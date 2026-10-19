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

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.ActiveRequestSensors;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.sensors.Type;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ResponseVerbHandler sensor tracking, specifically:
 * - Paxos commit responses now track READ_BYTES in addition to WRITE_BYTES
 * - Paxos V2 callbacks (PaxosPrepare, PaxosPropose, PaxosCommit) track both READ and WRITE bytes
 */
public class ResponseVerbHandlerSensorsTest
{
    private static final String KEYSPACE = "ResponseVerbHandlerSensorsTest";
    private static final String TABLE = "Standard";

    private static Keyspace ks;
    private static ColumnFamilyStore cfs;
    private static TableMetadata metadata;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        ks = Keyspace.open(KEYSPACE);
        cfs = ks.getColumnFamilyStore(TABLE);
        metadata = cfs.metadata();
    }

    private RequestSensors requestSensors;
    private Context context;

    @Before
    public void before()
    {
        requestSensors = new ActiveRequestSensors();
        context = Context.from(metadata);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
    }

    @Test
    public void testPaxosCommitResponseIncludesReadBytes() throws Exception
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key1").build();

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Message<Mutation> requestMessage = Message.builder(Verb.PAXOS_COMMIT_REQ, mutation).build();
        RequestCallbacks.WriteCallbackInfo callbackInfo = createWriteCallbackInfo(requestMessage, peer);

        // Create a response message with sensor data (simulating 100 bytes read, 200 bytes written)
        Message<?> responseMessage = createResponseMessageWithSensors(100.0, 200.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        // Verify both READ_BYTES and WRITE_BYTES were incremented
        // This validates the change in ResponseVerbHandler where commit responses now track READ_BYTES
        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("READ_BYTES should be tracked for Paxos commit responses (CNDB-15911)")
            .isEqualTo(100.0);

        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("WRITE_BYTES should be tracked for Paxos commit responses")
            .isEqualTo(200.0);
    }

    @Test
    public void testRegularMutationResponseIncludesReadBytes() throws Exception
    {
        // For regular mutations, the WriteCallbackInfo path should also track READ_BYTES
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key2").build();

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Message<Mutation> requestMessage = Message.builder(Verb.MUTATION_REQ, mutation).build();
        RequestCallbacks.WriteCallbackInfo callbackInfo = createWriteCallbackInfo(requestMessage, peer);
        Message<?> responseMessage = createResponseMessageWithSensors(50.0, 150.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        // Both sensors should be incremented
        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue()).isEqualTo(50.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue()).isEqualTo(150.0);
    }

    @Test
    public void testSensorValuesAccumulateFromMessage() throws Exception
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key3").build();

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Message<Mutation> requestMessage = Message.builder(Verb.MUTATION_REQ, mutation).build();
        RequestCallbacks.WriteCallbackInfo callbackInfo = createWriteCallbackInfo(requestMessage, peer);
        Message<?> responseMessage = createResponseMessageWithSensors(75.0, 175.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue()).isEqualTo(75.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue()).isEqualTo(175.0);
    }

    @Test
    public void testPaxosV2PrepareCallbackSensors() throws Exception
    {
        org.apache.cassandra.service.paxos.PaxosPrepare mockCallback =
            Mockito.mock(org.apache.cassandra.service.paxos.PaxosPrepare.class);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(metadata);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);

        RequestCallbacks.CallbackInfo callbackInfo = createCallbackInfo(mockCallback);
        Message<?> responseMessage = createResponseMessageWithSensors(80.0, 120.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("PaxosPrepare V2 should track READ_BYTES")
            .isEqualTo(80.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("PaxosPrepare V2 should track WRITE_BYTES")
            .isEqualTo(120.0);
    }

    @Test
    public void testPaxosV2ProposeCallbackSensors() throws Exception
    {
        org.apache.cassandra.service.paxos.PaxosPropose mockCallback =
            Mockito.mock(org.apache.cassandra.service.paxos.PaxosPropose.class);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(metadata);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);

        RequestCallbacks.CallbackInfo callbackInfo = createCallbackInfo(mockCallback);
        Message<?> responseMessage = createResponseMessageWithSensors(90.0, 130.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("PaxosPropose V2 should track READ_BYTES")
            .isEqualTo(90.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("PaxosPropose V2 should track WRITE_BYTES")
            .isEqualTo(130.0);
    }

    @Test
    public void testPaxosV2CommitCallbackSensors() throws Exception
    {
        org.apache.cassandra.service.paxos.PaxosCommit mockCallback =
            Mockito.mock(org.apache.cassandra.service.paxos.PaxosCommit.class);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(metadata);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);

        RequestCallbacks.CallbackInfo callbackInfo = createCallbackInfo(mockCallback);
        Message<?> responseMessage = createResponseMessageWithSensors(95.0, 140.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("PaxosCommit V2 should track READ_BYTES (CNDB-15911)")
            .isEqualTo(95.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("PaxosCommit V2 should track WRITE_BYTES")
            .isEqualTo(140.0);
    }

    /**
     * Create a WriteCallbackInfo with the given message and peer
     */
    private RequestCallbacks.WriteCallbackInfo createWriteCallbackInfo(Message message, InetAddressAndPort peer)
    {
        // Create a minimal callback that returns our request sensors
        RequestCallback<?> callback = new RequestCallback<Object>()
        {
            @Override
            public void onResponse(Message msg) {}

            @Override
            public void onFailure(InetAddressAndPort from,
                                 org.apache.cassandra.exceptions.RequestFailureReason failureReason) {}

            @Override
            public RequestSensors getRequestSensors() { return requestSensors; }

            @Override
            public boolean invokeOnFailure() { return true; }
        };

        return new RequestCallbacks.WriteCallbackInfo(message, peer, callback);
    }

    /**
     * Create a CallbackInfo with the given callback (for testing Paxos V2 callbacks)
     */
    private RequestCallbacks.CallbackInfo createCallbackInfo(RequestCallback<?> callback) throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "test").build();
        Message<Mutation> message = Message.builder(Verb.PAXOS2_PREPARE_REQ, mutation).build();

        return new RequestCallbacks.CallbackInfo(message, peer, callback);
    }

    /**
     * Create a response message with sensor data in custom params
     */
    private Message<?> createResponseMessageWithSensors(double readBytes, double writeBytes) throws Exception
    {
        InetAddressAndPort from = InetAddressAndPort.getByName("127.0.0.2");
        Message.Builder<NoPayload> builder = Message.builder(Verb.MUTATION_RSP, NoPayload.noPayload)
                                                    .from(from);

        MockSensor readSensor = new MockSensor(context, Type.READ_BYTES);
        readSensor.increment(readBytes);
        MockSensor writeSensor = new MockSensor(context, Type.WRITE_BYTES);
        writeSensor.increment(writeBytes);

        builder.withCustomParam(
            SensorsCustomParams.paramForRequestSensor(readSensor).get(),
            SensorsCustomParams.sensorValueAsBytes(readSensor.getValue())
        );
        builder.withCustomParam(
            SensorsCustomParams.paramForRequestSensor(writeSensor).get(),
            SensorsCustomParams.sensorValueAsBytes(writeSensor.getValue())
        );

        return builder.build();
    }

    /**
     * Mock sensor for testing
     */
    static class MockSensor extends Sensor
    {
        public MockSensor(Context context, Type type)
        {
            super(context, type);
        }
    }

    /**
     * Use reflection to call the private trackReplicaSensors method
     */
    private void trackReplicaSensors(RequestCallbacks.CallbackInfo callbackInfo, Message<?> message) throws Exception
    {
        Method method = ResponseVerbHandler.class.getDeclaredMethod("trackReplicaSensors",
                                                                     RequestCallbacks.CallbackInfo.class,
                                                                     Message.class);
        method.setAccessible(true);
        method.invoke(ResponseVerbHandler.instance, callbackInfo, message);
    }
}
