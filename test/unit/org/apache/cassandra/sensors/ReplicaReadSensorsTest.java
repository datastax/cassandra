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

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that replica-side read sensors ({@link Type#READ_BYTES}, {@link Type#READ_EXECUTION_TIME})
 * are correctly recorded and propagated to the global {@link SensorsRegistry} by
 * {@link ReadCommandVerbHandler}.
 * <p>
 * Each test exercises a specific read path (memtable, SSTable, secondary index, SAI) and asserts
 * that both the per-request sensor and the corresponding global registry sensor carry the expected
 * values after the verb handler completes.
 *
 * @see CoordinatorReadSensorsTest for the coordinator-side read counterpart
 * @see ReplicaWriteSensorsTest for the replica-side write counterpart
 */
public class ReplicaReadSensorsTest
{
    public static final String KEYSPACE1 = "ReplicaReadSensorsTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD_CLUSTERING = "StandardClustering";
    public static final String CF_STANDARD_SAI = "StandardSAI";
    public static final String CF_STANDARD_SECONDARY_INDEX = "StandardSecondaryIndex";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        // build SAI indexes
        Indexes.Builder saiIndexes = Indexes.builder();
        saiIndexes.add(IndexMetadata.fromSchemaMetadata(CF_STANDARD_SAI + "_val", IndexMetadata.Kind.CUSTOM, new HashMap<>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "val");
        }}));

        // build secondary indexes
        Indexes.Builder secondaryIndexes = Indexes.builder();
        IndexTarget indexTarget = new IndexTarget(new ColumnIdentifier("val", true), IndexTarget.Type.VALUES);
        secondaryIndexes.add(IndexMetadata.fromIndexTargets(Collections.singletonList(indexTarget),
                                                            CF_STANDARD_SECONDARY_INDEX + "_val",
                                                            IndexMetadata.Kind.COMPOSITES,
                                                            Collections.emptyMap()));

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_CLUSTERING,
                                                              1, AsciiType.instance, AsciiType.instance, AsciiType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SAI,
                                                              1, AsciiType.instance, LongType.instance, null)
                                                .partitioner(Murmur3Partitioner.instance) // supported by SAI
                                                .indexes(saiIndexes.build()),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX,
                                                              1, AsciiType.instance, LongType.instance, null)
                                                .indexes(secondaryIndexes.build()));

        CompactionManager.instance.disableAutoCompaction();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).metadata());
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SECONDARY_INDEX).metadata());

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
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SECONDARY_INDEX).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BloomFilter.recreateOnFPChanceChange = false;
    }

    @Test
    public void testMemtableRead()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        DecoratedKey key = store.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(store, key).build();
        handleReadCommand(command);

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
    }

    @Test
    public void testSinglePartitionReadCommand_ByPartitionKey()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(store, key).build();
        handleReadCommand(command);

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
    }

    @Test
    public void testSinglePartitionReadCommand_ByClustering()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, "0")
            .clustering(String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command = Util.cmd(store, key).includeRow("0").build();
        handleReadCommand(command);

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
    }

    @Test
    public void testSinglePartitionReadCommand_AllowFiltering()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, "0")
            .clustering(String.valueOf(j))
            .add("val", String.valueOf(j))
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command1 = Util.cmd(store, key).includeRow("0").build();
        handleReadCommand(command1);

        Sensor bytes1Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytes1Sensor.getValue()).isGreaterThan(0);
        // Extract the value as later we will reset the thread local and the sensor value will be lost
        long request1Bytes = (long) bytes1Sensor.getValue();
        Sensor bytes1RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytes1RegistrySensor).isEqualTo(bytes1Sensor);
        Sensor execution1Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution1Sensor.getValue()).isGreaterThan(0);
        Sensor execution1RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution1RegistrySensor).isEqualTo(execution1Sensor);
        assertThat(execution1RegistrySensor.getValue()).isEqualTo(execution1Sensor.getValue());
        assertResponseSensors(Pair.create(bytes1Sensor, bytes1RegistrySensor),
                              Pair.create(execution1Sensor, execution1RegistrySensor));

        SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES).reset();
        capturedOutboundMessages.clear();

        ReadCommand command2 = Util.cmd(store, key).filterOn("val", Operator.EQ, "9").build();
        handleReadCommand(command2);

        Sensor bytes2Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytes2Sensor.getValue()).isEqualTo(request1Bytes * 10);
        Sensor bytes2RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytes2RegistrySensor.getValue()).isEqualTo(request1Bytes + bytes2Sensor.getValue());
        Sensor execution2Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution2Sensor.getValue()).isGreaterThan(0);
        Sensor execution2RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution2RegistrySensor).isEqualTo(execution2Sensor);
        // execution time accumulates across both handleReadCommand calls
        assertThat(execution2RegistrySensor.getValue()).isGreaterThan(execution2Sensor.getValue());
        assertResponseSensors(Pair.create(bytes2Sensor, bytes2RegistrySensor),
                              Pair.create(execution2Sensor, execution2RegistrySensor));
    }

    @Test
    public void testPartitionRangeReadCommand_ByPartitionKey()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command1 = Util.cmd(store, key).build();
        handleReadCommand(command1);

        Sensor bytes1Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytes1Sensor.getValue()).isGreaterThan(0);
        // Extract the value as later we will reset the thread local and the sensor value will be lost
        long request1Bytes = (long) bytes1Sensor.getValue();
        Sensor bytes1RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytes1RegistrySensor).isEqualTo(bytes1Sensor);
        Sensor execution1Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution1Sensor.getValue()).isGreaterThan(0);
        Sensor execution1RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution1RegistrySensor).isEqualTo(execution1Sensor);
        assertThat(execution1RegistrySensor.getValue()).isEqualTo(execution1Sensor.getValue());
        assertResponseSensors(Pair.create(bytes1Sensor, bytes1RegistrySensor),
                              Pair.create(execution1Sensor, execution1RegistrySensor));

        SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES).reset();
        capturedOutboundMessages.clear();

        ReadCommand command2 = Util.cmd(store).fromKeyIncl("0").toKeyIncl("9").build();
        handleReadCommand(command2);

        Sensor bytes2Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytes2Sensor.getValue()).isEqualTo(request1Bytes * 10);
        Sensor bytes2RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytes2RegistrySensor.getValue()).isEqualTo(request1Bytes + bytes2Sensor.getValue());
        Sensor execution2Sensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution2Sensor.getValue()).isGreaterThan(0);
        Sensor execution2RegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(execution2RegistrySensor).isEqualTo(execution2Sensor);
        // execution time accumulates across both handleReadCommand calls
        assertThat(execution2RegistrySensor.getValue()).isGreaterThan(execution2Sensor.getValue());
        assertResponseSensors(Pair.create(bytes2Sensor, bytes2RegistrySensor),
                              Pair.create(execution2Sensor, execution2RegistrySensor));
    }

    @Test
    public void testSAIIndexScan()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SAI);
        Context context = new Context(KEYSPACE1, CF_STANDARD_SAI, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", (long) j)
            .build()
            .applyUnsafe();
        }

        ReadCommand readCommand = Util.cmd(store)
                                      .columns("val")
                                      .filterOn("val", Operator.GT, 0L)
                                      .build();

        handleReadCommand(readCommand);

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
    }

    @Test
    public void testSAISingleRowSearchVSIndexScan()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SAI);
        Context context = new Context(KEYSPACE1, CF_STANDARD_SAI, store.metadata.id.toString());

        int numRows = 10;
        for (int j = 0; j < numRows; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", (long) j)
            .build()
            .applyUnsafe();
        }

        // Match a single row
        ReadCommand readCommand = Util.cmd(store)
                                      .columns("val")
                                      .filterOn("val", Operator.EQ, 0L)
                                      .build();
        handleReadCommand(readCommand);

        // Store the request sensor value for comparison with full index scan
        Sensor indexReadSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        double singleRowSearchBytes = indexReadSensor.getValue();
        indexReadSensor.reset();

        // Scan the whole index
        readCommand = Util.cmd(store)
                          .columns("val")
                          .filterOn("val", Operator.GTE, 0L)
                          .build();
        handleReadCommand(readCommand);

        double fullIndexScanBytes = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES).getValue();
        assertThat(fullIndexScanBytes).isEqualTo(numRows * singleRowSearchBytes);
    }

    @Test
    public void testSecondayIndexSingleRow()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX);
        Context context = new Context(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", (long) j)
            .build()
            .applyUnsafe();
        }

        ReadCommand readCommand = Util.cmd(store)
                                      .fromKeyIncl("0").toKeyIncl("10")
                                      .columns("val")
                                      .filterOn("val", Operator.EQ, 1L) // only EQ is supported by CassandraIndex
                                      .build();

        handleReadCommand(readCommand);

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
    }

    private static void handleReadCommand(ReadCommand command)
    {
        ReadCommandVerbHandler.instance.doVerb(Message.builder(Verb.READ_REQ, command).build());
    }

    @SafeVarargs
    private void assertResponseSensors(Pair<Sensor, Sensor>... requestToRegistrySensors)
    {
        assertThat(capturedOutboundMessages).hasSize(1);
        Message message = capturedOutboundMessages.get(0);
        assertResponseSensors(message, requestToRegistrySensors);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = SensorsTestUtil.serialize(message);
        Message deserializedMessage = SensorsTestUtil.deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestToRegistrySensors);
    }

    @SafeVarargs
    private void assertResponseSensors(Message message, Pair<Sensor, Sensor>... requestToRegistrySensors)
    {
        assertThat(message.header.customParams()).isNotNull();
        for (Pair<Sensor, Sensor> pair : requestToRegistrySensors)
        {
            Sensor requestSensor = pair.left;
            Sensor registrySensor = pair.right;

            Optional<String> expectedRequestParam = SensorsCustomParams.paramForRequestSensor(requestSensor);
            Optional<String> expectedGlobalParam = SensorsCustomParams.paramForGlobalSensor(registrySensor);
            assertThat(expectedRequestParam).isPresent();
            assertThat(expectedGlobalParam).isPresent();

            assertThat(message.header.customParams()).containsKey(expectedRequestParam.get());
            assertThat(message.header.customParams()).containsKey(expectedGlobalParam.get());

            double requestValue = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedRequestParam.get()));
            double globalValue = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedGlobalParam.get()));
            assertThat(requestValue).isEqualTo(requestSensor.getValue());
            assertThat(globalValue).isEqualTo(registrySensor.getValue());
        }
    }
}
