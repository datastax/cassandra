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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicates;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
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
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareCallback;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.paxos.ProposeCallback;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.mockito.Mockito;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests to verify that sensors reported from replicas in {@link Message.Header#customParams()} are tracked correctly
 * in the {@link RequestSensors} of the request.
 */
@RunWith(BMUnitRunner.class)
public class ReplicaSensorsTrackingTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static EndpointsForToken targets;
    static EndpointsForToken pending;
    static Token dummy;
    /**
     * Used by byteman to signal that onResponse is about to be called for one of the replica responses. This enables
     * unit tests to start asserting that replica sensors are already tracked at this point
     */
    static CountDownLatch[] onResponseAboutToStartSignal;
    /**
     * Signalled by units tests once after sensor tracking assertions are done to make sure onResponse is not returned
     * before assertions are completed
     */
    static CountDownLatch[] onResponseStartSignal;
    static AtomicInteger responses = new AtomicInteger(0);

    @BeforeClass
    public static void beforeClass() throws Exception
    {

        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

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

    @Before
    public void before()
    {
        onResponseAboutToStartSignal = new CountDownLatch[targets.size()];
        onResponseStartSignal = new CountDownLatch[targets.size()];
        for (int i = 0; i < targets.size(); i++)
        {
            onResponseAboutToStartSignal[i] = new CountDownLatch(1);
            onResponseStartSignal[i] = new CountDownLatch(1);
        }
        responses.set(0);
    }

    @After
    public void after()
    {
        // just in case the test failed and the latches were not counted down
        for (int i = 0; i < targets.size(); i++)
        {
            onResponseAboutToStartSignal[i].countDown();
            onResponseStartSignal[i].countDown();
        }
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.reads.ReadCallback",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForReadCallback() throws InterruptedException
    {
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(cfs, key).build();
        Message<ReadCommand> readRequest = Message.builder(Verb.READ_REQ, command).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(command);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init callback
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, targets);
        final long startNanos = System.nanoTime();
        final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(command, plan, startNanos, QueryInfoTracker.ReadTracker.NOOP);
        final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = new ReadCallback<>(resolver, command, plan, startNanos);

        // mimic a sensor to be used in replica response
        Sensor mockingReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingReadSensor.increment(11.0);

        assertReplicaSensorsTracked(readRequest, callback, Pair.create(actualReadSensor, mockingReadSensor));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.WriteResponseHandler",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_HintsEnabled() throws InterruptedException
    {
        boolean allowHints = true;
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> writeRequest = Message.builder(Verb.MUTATION_REQ, mutation).build();
        assertSensorsTrackedForWriteRequest(writeRequest, allowHints);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.WriteResponseHandler",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_HintsDisabled() throws InterruptedException
    {
        boolean allowHints = false;
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> writeRequest = Message.builder(Verb.MUTATION_REQ, mutation).build();
        assertSensorsTrackedForWriteRequest(writeRequest, allowHints);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.WriteResponseHandler",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_CounterMutation() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ALL);
        Message<CounterMutation> writeRequest = Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation).build();
        boolean allowHints = false;
        assertSensorsTrackedForWriteRequest(writeRequest, allowHints);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.WriteResponseHandler",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_HintsEnabled_PaxosCommit() throws InterruptedException
    {
        Commit commit = Commit.emptyCommit(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0")), cfs.metadata());
        Message<Commit> writeRequest = Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build();
        boolean allowHints = true;
        assertSensorsTrackedForWriteRequest(writeRequest, allowHints);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.WriteResponseHandler",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_HintsDisabled_PaxosCommit() throws InterruptedException
    {
        Commit commit = Commit.emptyCommit(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0")), cfs.metadata());
        Message<Commit> writeRequest = Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build();
        boolean allowHints = false;
        assertSensorsTrackedForWriteRequest(writeRequest, allowHints);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.paxos.PrepareCallback",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosPrepareCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> prepare = Message.builder(Verb.PAXOS_PREPARE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init prepare callback
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0"));
        AbstractPaxosCallback<?> callback = new PrepareCallback(key, cfs.metadata(), targets.size(), ConsistencyLevel.ALL, 0);

        Sensor mockingPrepareWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingPrepareWriteSensor.increment(13.0);
        Sensor mockingPrepareReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingPrepareReadSensor.increment(14.0);
        Pair<Sensor, Sensor> prepareWriterSensors = Pair.create(actualWriteSensor, mockingPrepareWriteSensor);
        Pair<Sensor, Sensor> prepareReadSensors = Pair.create(actualReadSensor, mockingPrepareReadSensor);

        assertReplicaSensorsTracked(prepare, callback, prepareWriterSensors, prepareReadSensors);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.service.paxos.ProposeCallback",
    targetMethod = "onResponse",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosProposeCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> propose = Message.builder(Verb.PAXOS_PROPOSE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init propose callback
        AbstractPaxosCallback<?> callback = new ProposeCallback(cfs.metadata(), targets.size(), targets.size(), false, ConsistencyLevel.ALL, 0);

        Sensor mockingProposeWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingProposeWriteSensor.increment(15.0);
        Sensor mockingProposeReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingProposeReadSensor.increment(16.0);
        Pair<Sensor, Sensor> proposeWriterSensors = Pair.create(actualWriteSensor, mockingProposeWriteSensor);
        Pair<Sensor, Sensor> proposeReadSensors = Pair.create(actualReadSensor, mockingProposeReadSensor);

        assertReplicaSensorsTracked(propose, callback, proposeWriterSensors, proposeReadSensors);
    }

    /**
     * Used by Byteman to count down the onResponseAboutToStartSignal latch and await the onResponseStartSignal latch
     * for the current replica response.
     */
    public static void countDownAndAwaitOnResponseLatches() throws InterruptedException
    {
        int replica = responses.getAndIncrement();
        onResponseAboutToStartSignal[replica].countDown();
        // don't wait indefinitely if the test is stuck.
        assertThat(onResponseStartSignal[replica].await(1, TimeUnit.SECONDS)).isTrue();
    }

    private void assertSensorsTrackedForWriteRequest(Message writeRequest, boolean allowHints) throws InterruptedException
    {
        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init callback
        AbstractWriteResponseHandler<?> callback = createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL);

        // mimic a sensor to be used in replica response
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);

        assertReplicaSensorsTracked(writeRequest, callback, allowHints, Pair.create(actualWriteSensor, mockingWriteSensor));
    }

    @SafeVarargs
    private void assertReplicaSensorsTracked(Message<?> request, RequestCallback<?> callback, Pair<Sensor, Sensor>... trackingToReplicaSensors) throws InterruptedException
    {
        assertReplicaSensorsTracked(request, callback, false, trackingToReplicaSensors);
    }

    @SafeVarargs
    private void assertReplicaSensorsTracked(Message<?> request, RequestCallback<?> callback, boolean allowHints, Pair<Sensor, Sensor>... trackingToReplicaSensors) throws InterruptedException
    {
        for (Pair<Sensor, Sensor> pair : trackingToReplicaSensors)
        {
            Sensor trackingSensor = pair.left;
            Sensor replicaSensor = pair.right;
            assertThat(trackingSensor.getValue()).isZero();
            assertThat(replicaSensor.getValue()).isGreaterThan(0);
        }

        // sensors should be incremented with each response
        for (int responses = 1; responses <= targets.size(); responses++)
        {
            simulateResponseFromReplica(targets.get(responses - 1), request, callback, allowHints, Arrays.stream(trackingToReplicaSensors).map(Pair::right).toArray(Sensor[]::new));
            // don't wait indefinitely if the test is stuck. Delay the assertion of the await results to give a better change of a meaningful error by virtue of the core test assertion
            boolean awaitResult = onResponseAboutToStartSignal[responses - 1].await(1, TimeUnit.SECONDS);
            for (Pair<Sensor, Sensor> pair : trackingToReplicaSensors)
            {
                Sensor trackingSensor = pair.left;
                Sensor replicaSensor = pair.right;
                assertThat(trackingSensor.getValue()).isEqualTo(replicaSensor.getValue() * responses);
                assertThat(awaitResult).isTrue();
            }
            onResponseStartSignal[responses - 1].countDown();
        }

        // reset tracking sensors for next assertions, if any
        for (Pair<Sensor, Sensor> pair : trackingToReplicaSensors)
        {
            Sensor trackingSensor = pair.left;
            trackingSensor.reset();
        }
    }

    private void simulateResponseFromReplica(Replica replica, Message<?> request, RequestCallback<?> callback, boolean allowHints, Sensor... sensor)
    {
        new Thread(() -> {
            // AbstractWriteResponseHandler has a special handling for the callback
            if (callback instanceof AbstractWriteResponseHandler)
                MessagingService.instance().callbacks.addWithExpiration((AbstractWriteResponseHandler<?>) callback, request, replica, ConsistencyLevel.ALL, allowHints);
            else
                MessagingService.instance().callbacks.addWithExpiration(callback, request, replica.endpoint());
            Message<?> response = createResponseMessageWithSensor(request.verb(), replica.endpoint(), request.id(), sensor);
            ResponseVerbHandler.instance.doVerb(response);
        }).start();
    }

    private ReplicaPlan.SharedForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas));
    }

    private Message<?> createResponseMessageWithSensor(Verb requestVerb, InetAddressAndPort from, long id, Sensor... sensors)
    {
        if (requestVerb == Verb.READ_REQ)
            return createReadResponseMessage(from, id, sensors[0]);
        else if (requestVerb == Verb.MUTATION_REQ)
            return createResponseMessage(Verb.MUTATION_RSP, NoPayload.noPayload, from, id, sensors);
        else if (requestVerb == Verb.COUNTER_MUTATION_REQ)
            return createResponseMessage(Verb.COUNTER_MUTATION_RSP, NoPayload.noPayload, from, id, sensors);
        else if (requestVerb == Verb.PAXOS_PREPARE_REQ)
        {
            DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
            Commit commit = Commit.newPrepare(key, cfs.metadata(), UUIDGen.getTimeUUID());
            return createResponseMessage(Verb.PAXOS_PREPARE_RSP, new PrepareResponse(false, commit, commit), from, id, sensors);
        }
        else if (requestVerb == Verb.PAXOS_PROPOSE_REQ)
            return createResponseMessage(Verb.PAXOS_PROPOSE_RSP, true, from, id, sensors);
        else if (requestVerb == Verb.PAXOS_COMMIT_REQ)
            return createResponseMessage(Verb.PAXOS_COMMIT_RSP, NoPayload.noPayload, from, id, sensors);
        else
            throw new IllegalArgumentException("Unsupported verb: " + requestVerb);
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
                      .withCustomParam(SensorsCustomParams.paramForRequestSensor(readSensor).get(), SensorsCustomParams.sensorValueAsBytes(readSensor.getValue()))
                      .build();
    }

    private <T> Message<T> createResponseMessage(Verb responseVerb, T payload, InetAddressAndPort from, long id, Sensor... sensors)
    {
        Message.Builder<T> builder = Message.builder(responseVerb, payload)
                                            .from(from)
                                            .withId(id);

        for (Sensor sensor : sensors)
            builder.withCustomParam(SensorsCustomParams.paramForRequestSensor(sensor).get(), SensorsCustomParams.sensorValueAsBytes(sensor.getValue()));

        return builder.build();
    }

    private static AbstractWriteResponseHandler<?> createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal)
    {
        return createWriteResponseHandler(cl, ideal, System.nanoTime());
    }

    private static AbstractWriteResponseHandler<?> createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, long queryStartTime)
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
