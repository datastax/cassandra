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

import com.google.common.base.Predicates;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mockito.Mockito;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests to verify that sensors reported from replicas in {@link Message.Header#customParams()} are tracked correctly
 * in the {@link RequestSensors} of the request.
 */
public class ReplicaSensorsTrackedTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static EndpointsForToken targets;
    static EndpointsForToken pending;
    static Token dummy;

    @BeforeClass
    public static void beforeClass() throws Exception
    {

        CassandraRelevantProperties.REQUEST_SENSORS_FACTORY.setString(ActiveRequestSensorsFactory.class.getName());
        CassandraRelevantProperties.PROPAGATE_REQUEST_SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.simple(3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        dummy = Murmur3Partitioner.instance.getMinimumToken();
        targets = EndpointsForToken.of(dummy,
                                       full(InetAddressAndPort.getByName("127.0.0.255")),
                                       full(InetAddressAndPort.getByName("127.0.0.254")),
                                       full(InetAddressAndPort.getByName("127.0.0.253"))
        );
        pending = EndpointsForToken.empty(DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0)));
        cfs.sampleReadLatencyNanos = 0;
    }

    @Test
    public void testReadSensors()
    {
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(cfs, key).build();
        Message<ReadCommand> readRequest = Message.builder(Verb.READ_REQ, command).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(command);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        Sensor acutalReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init callback
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, targets);
        final long startNanos = System.nanoTime();
        final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(command, plan, startNanos, QueryInfoTracker.ReadTracker.NOOP);
        final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = new ReadCallback<>(resolver, command, plan, startNanos);

        // mimic a sensor to be used in replica repsponse
        Sensor mockingReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingReadSensor.increment(11.0);

        assertReplicaSensorsTracked(readRequest, callback, acutalReadSensor, mockingReadSensor);
    }

    @Test
    public void testWriteSensors()
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> writeRequest = Message.builder(Verb.MUTATION_REQ, mutation).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        Sensor acutalReadSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init callback
        AbstractWriteResponseHandler<?> callback = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        // mimic a sensor to be used in replica repsponse
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);

        assertReplicaSensorsTracked(writeRequest, callback, acutalReadSensor, mockingWriteSensor);
    }

    private void assertReplicaSensorsTracked(Message<?> request, RequestCallback<?> callback, Sensor trackingSensor, Sensor replicaSensor)
    {
        assertThat(trackingSensor.getValue()).isZero();
        assertThat(replicaSensor.getValue()).isGreaterThan(0);

        // sensors should be incremented with each response
        for (int responses = 1; responses <= targets.size(); responses++)
        {
            simulateResponseFromReplica(targets.get(responses - 1), request, callback, replicaSensor);
            assertThat(trackingSensor.getValue()).isEqualTo(replicaSensor.getValue() * responses);
        }
    }

    private void simulateResponseFromReplica(Replica replica, Message<?> request, RequestCallback<?> callback, Sensor sensor)
    {
        // AbstractWriteResponseHandler has a special handling for the callback
        if (callback instanceof AbstractWriteResponseHandler)
            MessagingService.instance().callbacks.addWithExpiration((AbstractWriteResponseHandler<?>) callback, request, replica, ConsistencyLevel.ALL, true);
        else
            MessagingService.instance().callbacks.addWithExpiration(callback, request, replica.endpoint());
        Message<?> response = createResponseMessageWithSensor(replica.endpoint(), request.id(), sensor);
        ResponseVerbHandler.instance.doVerb(response);
    }

    private ReplicaPlan.SharedForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas));
    }

    private Message<?> createResponseMessageWithSensor(InetAddressAndPort from, long id, Sensor sensor)
    {
        switch (sensor.getType())
        {
            case READ_BYTES:
                return createReadResponseMessage(from, id, sensor);
            case WRITE_BYTES:
                return createMutationResponseMessage(from, id, sensor);
            default:
                throw new IllegalArgumentException("Unsupported sensor type: " + sensor.getType());
        }
    }

    private Message<ReadResponse> createReadResponseMessage(InetAddressAndPort from, long id, Sensor readSensor)
    {
        ReadResponse response = new ReadResponse()
        {
            @Override
            public UnfilteredPartitionIterator makeIterator(ReadCommand command)
            {
                UnfilteredPartitionIterator iterator = Mockito.mock(UnfilteredPartitionIterator.class);
                Mockito.when(iterator.metadata()).thenReturn(command.metadata());
                return iterator;
            }

            @Override
            public ByteBuffer digest(ReadCommand command)
            {
                return null;
            }

            @Override
            public ByteBuffer repairedDataDigest()
            {
                return null;
            }

            @Override
            public boolean isRepairedDigestConclusive()
            {
                return false;
            }

            @Override
            public boolean mayIncludeRepairedDigest()
            {
                return false;
            }

            @Override
            public boolean isDigestResponse()
            {
                return false;
            }
        };

        return Message.builder(Verb.READ_RSP, response)
                      .from(from)
                      .withId(id)
                      .withCustomParam(SensorsCustomParams.requestParamForSensor(readSensor), SensorsCustomParams.sensorValueAsBytes(readSensor.getValue()))
                      .build();
    }

    private Message<NoPayload> createMutationResponseMessage(InetAddressAndPort from, long id, Sensor writeSensor)
    {
        return Message.builder(Verb.MUTATION_RSP, NoPayload.noPayload)
                      .from(from)
                      .withId(id)
                      .withCustomParam(SensorsCustomParams.requestParamForSensor(writeSensor), SensorsCustomParams.sensorValueAsBytes(writeSensor.getValue()))
                      .build();
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal)
    {
        return createWriteResponseHandler(cl, ideal, System.nanoTime());
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, long queryStartTime)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(ReplicaPlans.forWrite(ks, cl, targets, pending, Predicates.alwaysTrue(), ReplicaPlans.writeAll),
                                                                   null, WriteType.SIMPLE, queryStartTime, ideal);
    }

    static class mockingSensor extends Sensor
    {
        public mockingSensor(Context context, Type type)
        {
            super(context, type);
        }
    }
}
