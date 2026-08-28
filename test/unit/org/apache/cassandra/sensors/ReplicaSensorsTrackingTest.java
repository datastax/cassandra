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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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
import org.apache.cassandra.db.CounterMutationCallback;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RepairedDataInfo;
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
import org.apache.cassandra.service.BatchlogResponseHandler;
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
    static ColumnFamilyStore counterCfs;
    static EndpointsForToken targets;
    static EndpointsForToken pending;
    static Token dummy;

    /**
     * Used by byteman to signal that sensor tracking is done for one of the replica responses. This enables
     * unit tests to start asserting that replica sensors are actually tracked at this point
     */
    static CountDownLatch[] onResponseAboutToStartSignal;
    /**
     * Signalled by units tests once after sensor tracking assertions are done to make sure response is not returned
     * before assertions are completed
     */
    static CountDownLatch[] onResponseStartSignal;
    /**
     * The two latches above use ExecutionTimeSensorAccumulator#onResponse(), which is invoked first thing on the callback
     * own onResponse() and is the last sensor to be populated.
     */

    static AtomicInteger responses = new AtomicInteger(0);

    @BeforeClass
    public static void beforeClass() throws Exception
    {

        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.simple(3),
                                    SchemaLoader.standardCFMD("Foo", "Bar"),
                                    SchemaLoader.counterCFMD("Foo", "Counter"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        counterCfs = ks.getColumnFamilyStore("Counter");
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
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
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
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init callback
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ALL, targets);
        final long startNanos = System.nanoTime();
        final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(command, plan, startNanos, QueryInfoTracker.ReadTracker.NOOP);
        final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = new ReadCallback<>(resolver, command, plan, startNanos);

        // READ_BYTES is accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingReadSensor.increment(11.0);
        // READ_EXECUTION_TIME is accumulated from replica responses by ResponseVerbHandler via
        // ExecutionTimeSensorAccumulator: the running max is written once when blockFor responses arrive.
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(readRequest, callback,
                             List.of(Pair.create(actualReadSensor, mockingReadSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
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
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
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
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_CounterMutation() throws InterruptedException
    {
        // Build a counter mutation request against a real counter table so isCounter() == true.
        Mutation mutation = new RowUpdateBuilder(counterCfs.metadata(), 0, "0").build();
        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ALL);
        Message<CounterMutation> writeRequest = Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation).build();

        // Set up the leader's RequestSensors.
        RequestSensors leaderSensors = new ActiveRequestSensors();
        Context context = Context.from(counterCfs.metadata());
        leaderSensors.registerSensor(context, Type.WRITE_BYTES);
        leaderSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = leaderSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualExecutionTimeSensor = leaderSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        ExecutorLocals.set(ExecutorLocals.create(leaderSensors));

        // Wire a real CounterMutationCallback as the response handler callback.
        CounterMutationCallback counterCallback = new CounterMutationCallback(writeRequest, writeRequest.from(), leaderSensors);
        AbstractWriteResponseHandler<?> responseHandler = createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL,
                                                                                     System.nanoTime(), counterCallback);
        // WRITE_BYTES is accumulated from sub-replica responses by ResponseVerbHandler on the leader (additive).
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        // WRITE_EXECUTION_TIME sub-replica max is accumulated by ResponseVerbHandler via ExecutionTimeSensorAccumulator.
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        // Simulate the leader apply time added by counterWriteTask before the sub-replica fan-out.
        double leaderApplyTime = 500_000L;
        leaderSensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, leaderApplyTime);

        assertReplicaSensors(writeRequest, responseHandler, false,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_LoggedBatch() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> writeRequest = Message.builder(Verb.MUTATION_REQ, mutation).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualIndexWriteSensor = requestSensors.getSensor(context, Type.INDEX_WRITE_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        ExecutorLocals.set(ExecutorLocals.create(requestSensors));

        // BatchlogResponseHandler wraps the real WriteResponseHandler; sensors accumulate on the
        // BatchlogResponseHandler instance (its inherited execTimeAccumulator) because that is the
        // object passed to sendToHintedReplicas and registered with MessagingService.
        @SuppressWarnings("unchecked")
        AbstractWriteResponseHandler<IMutation> writeHandler = (AbstractWriteResponseHandler<IMutation>) createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL);
        BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(1, () -> {});
        BatchlogResponseHandler<IMutation> batchHandler = new BatchlogResponseHandler<>(writeHandler, targets.size(), cleanup, System.nanoTime());

        // WRITE_BYTES and INDEX_WRITE_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        Sensor mockingIndexWriteSensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        mockingIndexWriteSensor.increment(7.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator on the BatchlogResponseHandler:
        // the running max is written once blockFor responses arrive.
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(writeRequest, batchHandler, false,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor),
                                     Pair.create(actualIndexWriteSensor, mockingIndexWriteSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
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
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
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
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosPrepareCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> prepare = Message.builder(Verb.PAXOS_PREPARE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created.
        // INDEX_WRITE_BYTES is intentionally not registered: prepare only writes to system.paxos, which has no indexes.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init prepare callback
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0"));
        AbstractPaxosCallback<?> callback = new PrepareCallback(key, cfs.metadata(), targets.size(), ConsistencyLevel.ALL, 0);

        // WRITE_BYTES and READ_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingPrepareWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingPrepareWriteSensor.increment(13.0);
        Sensor mockingPrepareReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingPrepareReadSensor.increment(14.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator: the running max is written
        // once all targets have responded (paxos awaits all replicas before proceeding to the next phase).
        Sensor mockingPrepareExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingPrepareExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(prepare, callback,
                             List.of(Pair.create(actualWriteSensor, mockingPrepareWriteSensor),
                                     Pair.create(actualReadSensor, mockingPrepareReadSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingPrepareExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosProposeCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> propose = Message.builder(Verb.PAXOS_PROPOSE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created.
        // INDEX_WRITE_BYTES is intentionally not registered: propose only writes to system.paxos, which has no indexes.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init propose callback
        AbstractPaxosCallback<?> callback = new ProposeCallback(cfs.metadata(), targets.size(), targets.size(), false, ConsistencyLevel.ALL, 0);

        // WRITE_BYTES and READ_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingProposeWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingProposeWriteSensor.increment(15.0);
        Sensor mockingProposeReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingProposeReadSensor.increment(16.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator: the running max is written
        // once all targets have responded (paxos awaits all replicas before proceeding to the next phase).
        Sensor mockingProposeExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingProposeExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(propose, callback,
                             List.of(Pair.create(actualWriteSensor, mockingProposeWriteSensor),
                                     Pair.create(actualReadSensor, mockingProposeReadSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingProposeExecutionTimeSensor)));
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
        assertThat(onResponseStartSignal[replica].await(5, TimeUnit.SECONDS)).isTrue();
    }

    private void assertSensorsTrackedForWriteRequest(Message writeRequest, boolean allowHints) throws InterruptedException
    {
        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualIndexWriteSensor = requestSensors.getSensor(context, Type.INDEX_WRITE_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        ExecutorLocals locals = ExecutorLocals.create(requestSensors);
        ExecutorLocals.set(locals);

        // init callback
        AbstractWriteResponseHandler<?> callback = createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL);

        // WRITE_BYTES and INDEX_WRITE_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        Sensor mockingIndexWriteSensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        mockingIndexWriteSensor.increment(7.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator: the running max is written
        // once blockFor responses arrive (replicas within a write phase execute in parallel).
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(writeRequest, callback, allowHints,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor), Pair.create(actualIndexWriteSensor, mockingIndexWriteSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));
    }

    private void assertReplicaSensors(Message<?> request, RequestCallback<?> callback,
                                      List<Pair<Sensor, Sensor>> additiveSensors,
                                      List<Pair<Sensor, Sensor>> maxSensors) throws InterruptedException
    {
        assertReplicaSensors(request, callback, false, additiveSensors, maxSensors);
    }

    private void assertReplicaSensors(Message<?> request, RequestCallback<?> callback, boolean allowHints,
                                      List<Pair<Sensor, Sensor>> additiveSensors,
                                      List<Pair<Sensor, Sensor>> maxSensors) throws InterruptedException
    {
        for (Pair<Sensor, Sensor> pair : additiveSensors)
        {
            assertThat(pair.left.getValue()).isZero();
            assertThat(pair.right.getValue()).isGreaterThan(0);
        }
        // Snapshot any pre-seeded value (e.g. leader apply time) before replica responses arrive.
        // The accumulator adds max(replica_times) on top, so the expected final value is
        // initialValue + pair.right.getValue().
        Map<Sensor, Double> maxSensorInitialValues = new HashMap<>();
        for (Pair<Sensor, Sensor> pair : maxSensors)
        {
            maxSensorInitialValues.put(pair.left, pair.left.getValue());
            assertThat(pair.right.getValue()).isGreaterThan(0);
        }

        // Build the combined sensor array sent in every replica response message.
        Sensor[] allReplicaSensors = Stream.concat(additiveSensors.stream(), maxSensors.stream())
                                           .map(Pair::right)
                                           .toArray(Sensor[]::new);

        for (int responseIdx = 1; responseIdx <= targets.size(); responseIdx++)
        {
            simulateResponseFromReplica(targets.get(responseIdx - 1), request, callback, allowHints, allReplicaSensors);

            // don't wait indefinitely if the test is stuck. Delay the assertion of the await results to give a better
            // chance of a meaningful error by virtue of the core test assertion
            boolean awaitResult = onResponseAboutToStartSignal[responseIdx - 1].await(5, TimeUnit.SECONDS);

            // additive sensors must grow linearly with each response
            for (Pair<Sensor, Sensor> pair : additiveSensors)
                assertThat(pair.left.getValue()).isEqualTo(pair.right.getValue() * responseIdx);

            assertThat(awaitResult).isTrue();
            onResponseStartSignal[responseIdx - 1].countDown();
        }

        // max sensors are written once when the accumulator threshold is reached;
        // the final value is initialValue + max(replica_times)
        for (Pair<Sensor, Sensor> pair : maxSensors)
            assertThat(pair.left.getValue()).isEqualTo(maxSensorInitialValues.get(pair.left) + pair.right.getValue());

        // reset sensors for subsequent assertions within the same test, if any
        for (Pair<Sensor, Sensor> pair : additiveSensors)
            pair.left.reset();
        for (Pair<Sensor, Sensor> pair : maxSensors)
            pair.left.reset();
    }

    private void simulateResponseFromReplica(Replica replica, Message<?> request, RequestCallback<?> callback, boolean allowHints, Sensor... sensor)
    {
        new Thread(() -> {
            // AbstractWriteResponseHandler has a special handling for the callback
            if (callback instanceof AbstractWriteResponseHandler)
                MessagingService.instance().callbacks.addWithExpiration((AbstractWriteResponseHandler<?>) callback, request, replica, ConsistencyLevel.ALL, allowHints);
            else
                MessagingService.instance().callbacks.addWithExpiration(callback, request, replica.endpoint());
            Message<?> response = createResponseMessageWithSensor(request, replica.endpoint(), sensor);
            ResponseVerbHandler.instance.doVerb(response);
        }).start();
    }

    private ReplicaPlan.SharedForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas));
    }

    private Message<?> createResponseMessageWithSensor(Message<?> request, InetAddressAndPort endpoint, Sensor... sensors)
    {
        if (request.verb() == Verb.READ_REQ)
            return createReadResponseMessage(request, endpoint, sensors);
        else if (request.verb() == Verb.MUTATION_REQ)
            return createResponseMessage(Verb.MUTATION_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.COUNTER_MUTATION_REQ)
            return createResponseMessage(Verb.COUNTER_MUTATION_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.PAXOS_PREPARE_REQ)
        {
            DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
            Commit commit = Commit.newPrepare(key, cfs.metadata(), UUIDGen.getTimeUUID());
            return createResponseMessage(Verb.PAXOS_PREPARE_RSP, new PrepareResponse(false, commit, commit), endpoint, request.id(), sensors);
        }
        else if (request.verb() == Verb.PAXOS_PROPOSE_REQ)
            return createResponseMessage(Verb.PAXOS_PROPOSE_RSP, true, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.PAXOS_COMMIT_REQ)
            return createResponseMessage(Verb.PAXOS_COMMIT_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else
            throw new IllegalArgumentException("Unsupported verb: " + request.verb());
    }

    private Message<ReadResponse> createReadResponseMessage(Message<?> request, InetAddressAndPort endpoint, Sensor... sensors)
    {
        UnfilteredPartitionIterator data = Mockito.mock(UnfilteredPartitionIterator.class);
        Mockito.when(data.metadata()).thenReturn(((ReadCommand) request.payload).metadata());
        ReadResponse response = ReadResponse.createDataResponse(data, (ReadCommand) request.payload, RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        Message.Builder<ReadResponse> builder = Message.builder(Verb.READ_RSP, response)
                                                       .from(endpoint)
                                                       .withId(request.id());

        for (Sensor sensor : sensors)
            builder.withCustomParam(SensorsCustomParams.paramForRequestSensor(sensor).get(), SensorsCustomParams.sensorValueAsBytes(sensor.getValue()));

        return builder.build();
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
        return createWriteResponseHandler(cl, ideal, queryStartTime, null);
    }

    private static AbstractWriteResponseHandler<?> createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, long queryStartTime, Runnable callback)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(ReplicaPlans.forWrite(ks, cl, targets, pending, Predicates.alwaysTrue(), ReplicaPlans.writeAll),
                                                                   callback, WriteType.SIMPLE, queryStartTime, ideal);
    }

    static class mockingSensor extends Sensor
    {
        public mockingSensor(Context context, Type type)
        {
            super(context, type);
        }
    }
}
