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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.SystemKeyspace.PAXOS;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class SensorsWriteTest
{
    private static final String KEYSPACE1 = "SensorsWriteTest";
    private static final String CF_STANDARD = "Standard";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String CF_STANDARD_CLUSTERING = "StandardClustering";
    private static final String CF_COUTNER = "Counter";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_CLUSTERING,
                                                              1, AsciiType.instance, AsciiType.instance, AsciiType.instance),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUTNER));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).metadata());

        // enable sensor registy for system keyspace
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open("system").getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open("system").getColumnFamilyStore(PAXOS).metadata());

        capturedOutboundMessages = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) ->
                                                     {
                                                         capturedOutboundMessages.add(message);
                                                         return false;
                                                     });
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).truncateBlocking();
        Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PAXOS).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BloomFilter.recreateOnFPChanceChange = false;
    }

    @Test
    public void testSingleRowMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        double writeSensorSum = 0;
        for (int j = 0; j < 10; j++)
        {
            Mutation m = new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                         .add("val", String.valueOf(j))
                         .build();
            handleMutation(m);
            Sensor localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
            assertThat(localSensor.getValue()).isGreaterThan(0);
            Sensor registrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
            assertThat(registrySensor).isEqualTo(localSensor);
            writeSensorSum += localSensor.getValue();

            // check global registry is synchronized
            assertThat(registrySensor.getValue()).isEqualTo(writeSensorSum);
            assertResponseSensors(localSensor.getValue(), writeSensorSum, CF_STANDARD);
        }
    }

    @Test
    public void testSingleRowMutationWithClusteringKey()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        double writeSensorSum = 0;
        for (int j = 0; j < 10; j++)
        {
            Mutation m = new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                         .clustering(String.valueOf(j))
                         .add("val", String.valueOf(j))
                         .build();
            handleMutation(m);
            Sensor localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
            assertThat(localSensor.getValue()).isGreaterThan(0);
            Sensor registrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
            assertThat(registrySensor).isEqualTo(localSensor);
            writeSensorSum += localSensor.getValue();

            // check global registry is synchronized
            assertThat(registrySensor.getValue()).isEqualTo(writeSensorSum);
            assertResponseSensors(localSensor.getValue(), writeSensorSum, CF_STANDARD_CLUSTERING);
        }
    }

    @Test
    public void testMultipleRowsMutationWithClusteringKey()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        List<Mutation> mutations = new ArrayList<>();
        String partitionKey = "0";

        // record the written bytes for a single row update
        String oneCharString = "0"; // a single char string to establish a baseline for the sensor
        Mutation mutation = new RowUpdateBuilder(store.metadata(), 0, partitionKey)
                            .clustering(oneCharString)
                            .add("val", oneCharString)
                            .build();

        handleMutation(mutation);
        Sensor localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(localSensor.getValue()).isGreaterThan(0);
        double singleRowWriteBytes = localSensor.getValue();

        // build a list of mutations equivalent in written bytes to the single row update but targeting different rows
        // so we can actually tell if the sensor accommodated for all of them
        int rowsNum = 10;
        for (int j = 0; j < rowsNum; j++)
        {
            oneCharString = String.valueOf(j);
            // verify that columns are updated with single char values to match the established singleRowWriteBytes baseline
            // it is important that each value is different, to enforce proportionality between written bytes and the number of rows
            // if the values were the same, the mutations will optimize/collapse to a single write
            assertThat(oneCharString).hasSize(1);
            mutations.add(new RowUpdateBuilder(store.metadata(), j, partitionKey)
                          .clustering(String.valueOf(j))
                          .add("val", String.valueOf(j))
                          .build());
        }

        mutation = Mutation.merge(mutations);
        handleMutation(mutation);

        localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(localSensor.getValue()).isEqualTo(10 * singleRowWriteBytes);

        Sensor registrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(registrySensor).isEqualTo(localSensor);
        assertThat(registrySensor.getValue()).isEqualTo(localSensor.getValue() + singleRowWriteBytes);
        assertResponseSensors(localSensor.getValue(), registrySensor.getValue(), CF_STANDARD_CLUSTERING);
    }

    @Test
    public void testMultipleTableMutations()
    {
        ColumnFamilyStore store1 = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context1 = new Context(KEYSPACE1, CF_STANDARD, store1.metadata.id.toString());

        ColumnFamilyStore store2 = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD2);
        Context context2 = new Context(KEYSPACE1, CF_STANDARD2, store2.metadata.id.toString());

        List<Mutation> mutations = new ArrayList<>();
        String partitionKey = "0";

        // first table mutation
        mutations.add(new RowUpdateBuilder(store1.metadata(), 0, partitionKey)
                          .add("val", "value")
                          .build());

        // second table mutation
        mutations.add(new RowUpdateBuilder(store2.metadata(), 0, partitionKey)
                          .add("val", "another value")
                          .build());

        Mutation mutation = Mutation.merge(mutations);
        handleMutation(mutation);

        Sensor localSensor1 = SensorsTestUtil.getThreadLocalRequestSensor(context1, Type.WRITE_BYTES);
        assertThat(localSensor1.getValue()).isGreaterThan(0);

        Sensor localSensor2 = SensorsTestUtil.getThreadLocalRequestSensor(context2, Type.WRITE_BYTES);
        assertThat(localSensor2.getValue()).isGreaterThan(0);

        Sensor registrySensor1 = SensorsTestUtil.getRegistrySensor(context1, Type.WRITE_BYTES);
        assertThat(registrySensor1).isEqualTo(localSensor1);
        assertThat(registrySensor1.getValue()).isEqualTo(localSensor1.getValue());

        Sensor registrySensor2 = SensorsTestUtil.getRegistrySensor(context2, Type.WRITE_BYTES);
        assertThat(registrySensor2).isEqualTo(localSensor2);
        assertThat(registrySensor2.getValue()).isEqualTo(localSensor2.getValue());

        assertResponseSensors(localSensor1.getValue(), registrySensor1.getValue(), CF_STANDARD);
        assertResponseSensors(localSensor2.getValue(), registrySensor2.getValue(), CF_STANDARD2);
    }

    @Test
    public void testSingleCounterMutation() throws WriteTimeoutException
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_COUTNER);
        Context context = new Context(KEYSPACE1, CF_COUTNER, store.metadata.id.toString());
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER);
        cfs.truncateBlocking();

        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                            .clustering("cc")
                            .add("val", 1L).build();

        // Use consistency level ANY to disable the live replicas assertion as we don't have any replica in the unit test
        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ANY);
        handleCounterMutation(counterMutation);

        Sensor localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(localSensor.getValue()).isGreaterThan(0);
        Sensor registrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(registrySensor).isEqualTo(localSensor);
        assertResponseSensors(localSensor.getValue(), registrySensor.getValue(), CF_COUTNER);
    }

    @Test
    public void testLWTPrepare() {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        PartitionUpdate update = new RowUpdateBuilder(store.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        Commit proposal = Commit.newPrepare(update.partitionKey(), store.metadata(), UUIDGen.getTimeUUID());
        handlePaxosPrepare(proposal);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor.getValue()).isGreaterThan(0);
        Sensor registryWriteSensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(registryWriteSensor).isEqualTo(writeSensor);
        assertResponseSensors(writeSensor.getValue(), registryWriteSensor.getValue(), CF_STANDARD);
        Sensor readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isZero();

        // handle the commit again, this time paxos has state because of the first proposal and read bytes will be populated
        handlePaxosPrepare(proposal);
        readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isGreaterThan(0);
        Sensor registryReadSensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(registryReadSensor).isEqualTo(readSensor);
        assertReadResponseSensors(readSensor.getValue(), registryReadSensor.getValue());
    }

    @Test
    public void testLWTPropose() {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        PartitionUpdate update = new RowUpdateBuilder(store.metadata(), 0, "0")
                                .add("val", "0")
                                .buildUpdate();
        Commit proposal = Commit.newProposal(UUIDGen.getTimeUUID(), update);
        handlePaxosPropose(proposal);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor.getValue()).isGreaterThan(0);
        Sensor registryWriteSensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(registryWriteSensor).isEqualTo(writeSensor);
        assertResponseSensors(writeSensor.getValue(), registryWriteSensor.getValue(), CF_STANDARD);
        Sensor readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isZero();

        // handle the commit again, this time paxos has state because of the first proposal and read bytes will be populated
        handlePaxosPropose(proposal);
        readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isGreaterThan(0);
        Sensor registryReadSensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(registryReadSensor).isEqualTo(readSensor);
        assertReadResponseSensors(readSensor.getValue(), registryReadSensor.getValue());
    }

    @Test
    public void testLWTCommit() {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        PartitionUpdate update = new RowUpdateBuilder(store.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        Commit proposal = Commit.newPrepare(update.partitionKey(), store.metadata(), UUIDGen.getTimeUUID());
        handlePaxosCommit(proposal);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor.getValue()).isGreaterThan(0);
        Sensor registryWriteSensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(registryWriteSensor).isEqualTo(writeSensor);
        assertResponseSensors(writeSensor.getValue(), registryWriteSensor.getValue(), CF_STANDARD);

        // No read is done in the commit phase
        assertThat(RequestTracker.instance.get().getSensor(context, Type.READ_BYTES)).isEmpty();
        assertThat(SensorsRegistry.instance.getSensor(context, Type.READ_BYTES)).isEmpty();
    }

    private static void handlePaxosPrepare(Commit prepare)
    {
        PrepareVerbHandler.instance.doVerb(Message.builder(Verb.PAXOS_PREPARE_REQ, prepare).build());
    }

    private static void handlePaxosPropose(Commit proposal)
    {
        ProposeVerbHandler.instance.doVerb(Message.builder(Verb.PAXOS_PROPOSE_REQ, proposal).build());
    }

    private static void handlePaxosCommit(Commit commit)
    {
        CommitVerbHandler.instance.doVerb(Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build());
    }

    private static void handleMutation(Mutation mutation)
    {
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
    }

    private static void handleCounterMutation(CounterMutation mutation)
    {
        CounterMutationVerbHandler.instance.doVerb(Message.builder(Verb.COUNTER_MUTATION_REQ, mutation).build());
    }

    private void assertReadResponseSensors(double requestValue, double registryValue)
    {
        // verify against the last message to enable testing of multiple mutations in a for loop
        Message message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        assertResponseSensors(message, requestValue, registryValue, () -> SensorsCustomParams.READ_BYTES_REQUEST, () -> SensorsCustomParams.READ_BYTES_TABLE);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = SensorsTestUtil.serialize(message);
        Message deserializedMessage = SensorsTestUtil.deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestValue, registryValue, () -> SensorsCustomParams.READ_BYTES_REQUEST, () -> SensorsCustomParams.READ_BYTES_TABLE);
    }

    private void assertResponseSensors(double requestValue, double registryValue, String table)
    {
        Supplier<String> requestParamSupplier = () -> SensorsCustomParams.encodeTableInWriteBytesRequestParam(table);
        Supplier<String> tableParamSupplier = () -> SensorsCustomParams.encodeTableInWriteBytesTableParam(table);
        // verify against the last message to enable testing of multiple mutations in a for loop
        Message message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        assertResponseSensors(message, requestValue, registryValue, requestParamSupplier, tableParamSupplier);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = SensorsTestUtil.serialize(message);
        Message deserializedMessage = SensorsTestUtil.deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestValue, registryValue, requestParamSupplier, tableParamSupplier);
    }

    private void assertResponseSensors(Message message, double requestValue, double registryValue, Supplier<String> requestParamSupplier, Supplier<String> tableParamSupplier)
    {
        assertThat(message.header.customParams()).isNotNull();
        String expectedRequestParam = requestParamSupplier.get();
        String expectedTableParam = tableParamSupplier.get();

        assertThat(message.header.customParams()).containsKey(expectedRequestParam);
        assertThat(message.header.customParams()).containsKey(expectedTableParam);
        double requestWriteBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedRequestParam));
        double tableWriteBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedTableParam));
        assertThat(requestWriteBytes).isEqualTo(requestValue);
        assertThat(tableWriteBytes).isEqualTo(registryValue);
    }
}
