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
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.reads.ReadCallback;
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
 * Tests for {@link ResponseVerbHandler} sensor tracking:
 * <ul>
 *   <li>Read responses ({@link ReadCallback}) track {@link Type#READ_BYTES} only.</li>
 *   <li>Paxos V1 commit responses ({@code PAXOS_COMMIT_REQ} via {@link RequestCallbacks.WriteCallbackInfo})
 *       track only {@link Type#WRITE_BYTES} — the commit phase does not read system.paxos.</li>
 *   <li>Regular mutation responses (same {@link RequestCallbacks.WriteCallbackInfo} branch) also track
 *       only {@link Type#WRITE_BYTES}.</li>
 *   <li>Paxos V2 callbacks ({@link org.apache.cassandra.service.paxos.PaxosPrepare},
 *       {@link org.apache.cassandra.service.paxos.PaxosPropose},
 *       {@link org.apache.cassandra.service.paxos.PaxosCommit}) track both
 *       {@link Type#READ_BYTES} and {@link Type#WRITE_BYTES}.</li>
 * </ul>
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
        requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
    }

    /**
     * Read responses go through the {@link ReadCallback} branch in
     * {@link ResponseVerbHandler#trackReplicaSensors} and track only {@link Type#READ_BYTES};
     * {@link Type#WRITE_BYTES} is not touched by the read path.
     */
    @Test
    public void testReadCallbackResponseTracksReadBytesOnly() throws Exception
    {
        ReadCallback<?, ?> mockReadCallback = Mockito.mock(ReadCallback.class);
        Mockito.when(mockReadCallback.command()).thenReturn(
            SinglePartitionReadCommand.fullPartitionRead(metadata, 0, ByteBufferUtil.bytes("key0")));
        Mockito.when(mockReadCallback.getRequestSensors()).thenReturn(requestSensors);

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Mutation requestMutation = new RowUpdateBuilder(metadata, 0, "key0").build();
        Message<Mutation> requestMessage = Message.builder(Verb.READ_REQ, requestMutation).build();
        RequestCallbacks.CallbackInfo callbackInfo = new RequestCallbacks.CallbackInfo(requestMessage, peer, mockReadCallback);

        Message<?> responseMessage = createResponseMessageWithSensors(300.0, 400.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("READ_BYTES should be tracked for read responses")
            .isEqualTo(300.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("WRITE_BYTES should NOT be tracked for read responses")
            .isEqualTo(0.0);
    }

    /**
     * Paxos V1 commit uses a plain {@link Commit} payload on {@code PAXOS_COMMIT_REQ} and goes
     * through the {@link RequestCallbacks.WriteCallbackInfo} branch in
     * {@link ResponseVerbHandler#trackReplicaSensors}.  That branch tracks only {@link Type#WRITE_BYTES};
     * {@link Type#READ_BYTES} is intentionally not tracked there because the commit phase does not
     * read from system.paxos — only Prepare and Propose do.
     */
    @Test
    public void testPaxosV1CommitResponseTracksWriteBytesOnly() throws Exception
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key1").build();
        PartitionUpdate update = mutation.getPartitionUpdate(metadata);
        Commit commit = new Commit(Ballot.none(), update);

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Message<Commit> requestMessage = Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build();
        RequestCallbacks.WriteCallbackInfo callbackInfo = createWriteCallbackInfo(requestMessage, peer);

        Message<?> responseMessage = createResponseMessageWithSensors(100.0, 200.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        // WRITE_BYTES is tracked for the commit response
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("WRITE_BYTES should be tracked for Paxos V1 commit responses")
            .isEqualTo(200.0);

        // READ_BYTES is not tracked for commit: the commit phase writes to system.paxos but does not read it
        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("READ_BYTES should NOT be tracked for Paxos V1 commit responses (no system.paxos read during commit)")
            .isEqualTo(0.0);
    }

    /**
     * Regular mutation responses go through the {@link RequestCallbacks.WriteCallbackInfo} branch and
     * track only {@link Type#WRITE_BYTES}, not {@link Type#READ_BYTES}.
     */
    @Test
    public void testRegularMutationResponseTracksWriteBytesOnly() throws Exception
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key2").build();

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Message<Mutation> requestMessage = Message.builder(Verb.MUTATION_REQ, mutation).build();
        RequestCallbacks.WriteCallbackInfo callbackInfo = createWriteCallbackInfo(requestMessage, peer);
        Message<?> responseMessage = createResponseMessageWithSensors(50.0, 150.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("WRITE_BYTES should be tracked for regular mutation responses")
            .isEqualTo(150.0);
        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("READ_BYTES should NOT be tracked for regular mutation responses")
            .isEqualTo(0.0);
    }

    /**
     * {@link Type#WRITE_BYTES} accumulates additively across multiple replica responses for the same
     * request: two responses each carrying 175 bytes should sum to 350 bytes.
     */
    @Test
    public void testSensorValuesAccumulateFromMessage() throws Exception
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "key3").build();

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Message<Mutation> requestMessage = Message.builder(Verb.MUTATION_REQ, mutation).build();
        RequestCallbacks.WriteCallbackInfo callbackInfo = createWriteCallbackInfo(requestMessage, peer);
        Message<?> responseMessage = createResponseMessageWithSensors(75.0, 175.0);

        trackReplicaSensors(callbackInfo, responseMessage);
        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("WRITE_BYTES should accumulate additively across two replica responses")
            .isEqualTo(350.0);
        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("READ_BYTES should remain zero for mutation responses")
            .isEqualTo(0.0);
    }

    /**
     * Paxos V2 Prepare responses go through the {@link org.apache.cassandra.service.paxos.PaxosPrepare}
     * branch in {@link ResponseVerbHandler#trackReplicaSensors} and track both
     * {@link Type#READ_BYTES} (system.paxos read via {@code loadPaxosState}) and
     * {@link Type#WRITE_BYTES} (system.paxos write via {@code savePaxosReadPromise/savePaxosWritePromise}).
     */
    @Test
    public void testPaxosV2PrepareCallbackSensors() throws Exception
    {
        org.apache.cassandra.service.paxos.PaxosPrepare mockCallback =
            Mockito.mock(org.apache.cassandra.service.paxos.PaxosPrepare.class);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(metadata);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);

        RequestCallbacks.CallbackInfo callbackInfo = createCallbackInfo(mockCallback);
        Message<?> responseMessage = createResponseMessageWithSensors(80.0, 120.0, 60.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("PaxosPrepare V2 should track READ_BYTES")
            .isEqualTo(80.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("PaxosPrepare V2 should track WRITE_BYTES")
            .isEqualTo(120.0);
        assertThat(requestSensors.getSensor(context, Type.INTERNODE_BYTES).get().getValue())
            .as("PaxosPrepare V2 should track INTERNODE_BYTES")
            .isEqualTo(60.0);
    }

    /**
     * Paxos V2 Propose responses go through the {@link org.apache.cassandra.service.paxos.PaxosPropose}
     * branch in {@link ResponseVerbHandler#trackReplicaSensors} and track both
     * {@link Type#READ_BYTES} (system.paxos read via {@code loadPaxosState}) and
     * {@link Type#WRITE_BYTES} (system.paxos write via {@code savePaxosProposal}).
     */
    @Test
    public void testPaxosV2ProposeCallbackSensors() throws Exception
    {
        org.apache.cassandra.service.paxos.PaxosPropose mockCallback =
            Mockito.mock(org.apache.cassandra.service.paxos.PaxosPropose.class);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(metadata);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);

        RequestCallbacks.CallbackInfo callbackInfo = createCallbackInfo(mockCallback);
        Message<?> responseMessage = createResponseMessageWithSensors(90.0, 130.0, 70.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("PaxosPropose V2 should track READ_BYTES")
            .isEqualTo(90.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("PaxosPropose V2 should track WRITE_BYTES")
            .isEqualTo(130.0);
        assertThat(requestSensors.getSensor(context, Type.INTERNODE_BYTES).get().getValue())
            .as("PaxosPropose V2 should track INTERNODE_BYTES")
            .isEqualTo(70.0);
    }

    /**
     * Paxos V2 Commit responses go through the {@link org.apache.cassandra.service.paxos.PaxosCommit}
     * branch in {@link ResponseVerbHandler#trackReplicaSensors} and track both
     * {@link Type#READ_BYTES} and {@link Type#WRITE_BYTES} (system.paxos write via {@code savePaxosCommit},
     * plus the user-table mutation apply when the condition was met).
     */
    @Test
    public void testPaxosV2CommitCallbackSensors() throws Exception
    {
        org.apache.cassandra.service.paxos.PaxosCommit mockCallback =
            Mockito.mock(org.apache.cassandra.service.paxos.PaxosCommit.class);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(metadata);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);

        RequestCallbacks.CallbackInfo callbackInfo = createCallbackInfo(mockCallback);
        Message<?> responseMessage = createResponseMessageWithSensors(95.0, 140.0, 80.0);

        trackReplicaSensors(callbackInfo, responseMessage);

        assertThat(requestSensors.getSensor(context, Type.READ_BYTES).get().getValue())
            .as("PaxosCommit V2 should track READ_BYTES")
            .isEqualTo(95.0);
        assertThat(requestSensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
            .as("PaxosCommit V2 should track WRITE_BYTES")
            .isEqualTo(140.0);
        assertThat(requestSensors.getSensor(context, Type.INTERNODE_BYTES).get().getValue())
            .as("PaxosCommit V2 should track INTERNODE_BYTES")
            .isEqualTo(80.0);
    }

    /**
     * Builds a {@link RequestCallbacks.WriteCallbackInfo} backed by a minimal {@link RequestCallback}
     * that exposes {@link #requestSensors}. Used for the {@link RequestCallbacks.WriteCallbackInfo}
     * branch tests (mutations and Paxos V1 commit).
     */
    private RequestCallbacks.WriteCallbackInfo createWriteCallbackInfo(Message message, InetAddressAndPort peer)
    {
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
     * Builds a plain {@link RequestCallbacks.CallbackInfo} wrapping the given callback.
     * Used for the Paxos V2 branch tests where the callback itself carries the sensor reference.
     */
    private RequestCallbacks.CallbackInfo createCallbackInfo(RequestCallback<?> callback) throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        Mutation mutation = new RowUpdateBuilder(metadata, 0, "test").build();
        Message<Mutation> message = Message.builder(Verb.PAXOS2_PREPARE_REQ, mutation).build();

        return new RequestCallbacks.CallbackInfo(message, peer, callback);
    }

    /**
     * Builds a response {@link Message} with {@link Type#READ_BYTES} and {@link Type#WRITE_BYTES}
     * encoded as custom parameters, simulating a replica response that carries sensor data.
     */
    private Message<?> createResponseMessageWithSensors(double readBytes, double writeBytes) throws Exception
    {
        return createResponseMessageWithSensors(readBytes, writeBytes, 0.0);
    }

    /**
     * Builds a response {@link Message} with {@link Type#READ_BYTES}, {@link Type#WRITE_BYTES},
     * and {@link Type#INTERNODE_BYTES} encoded as custom parameters, simulating a replica response
     * that carries sensor data.
     */
    private Message<?> createResponseMessageWithSensors(double readBytes, double writeBytes, double internodeBytes) throws Exception
    {
        InetAddressAndPort from = InetAddressAndPort.getByName("127.0.0.2");
        Message.Builder<NoPayload> builder = Message.builder(Verb.MUTATION_RSP, NoPayload.noPayload)
                                                    .from(from);

        MockSensor readSensor = new MockSensor(context, Type.READ_BYTES);
        readSensor.increment(readBytes);
        MockSensor writeSensor = new MockSensor(context, Type.WRITE_BYTES);
        writeSensor.increment(writeBytes);
        MockSensor internodeSensor = new MockSensor(context, Type.INTERNODE_BYTES);
        internodeSensor.increment(internodeBytes);

        builder.withCustomParam(
            SensorsCustomParams.paramForRequestSensor(readSensor).get(),
            SensorsCustomParams.sensorValueAsBytes(readSensor.getValue())
        );
        builder.withCustomParam(
            SensorsCustomParams.paramForRequestSensor(writeSensor).get(),
            SensorsCustomParams.sensorValueAsBytes(writeSensor.getValue())
        );
        builder.withCustomParam(
            SensorsCustomParams.paramForRequestSensor(internodeSensor).get(),
            SensorsCustomParams.sensorValueAsBytes(internodeSensor.getValue())
        );

        return builder.build();
    }

    /**
     * Minimal {@link Sensor} subclass used to build response messages with known sensor values.
     */
    static class MockSensor extends Sensor
    {
        public MockSensor(Context context, Type type)
        {
            super(context, type);
        }
    }

    /**
     * Invokes the private {@link ResponseVerbHandler#trackReplicaSensors} method via reflection,
     * accumulating the sensor values from {@code message} into the callback's {@link RequestSensors}.
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
