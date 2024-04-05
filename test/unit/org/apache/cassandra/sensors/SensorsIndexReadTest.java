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
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.BloomFilter;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorsIndexReadTest
{
    public static final String KEYSPACE1 = "SensorsIndexReadTest";
    public static final String CF_STANDARD_SAI = "StandardSAI";
    public static final String CF_STANDARD_SECONDARY_INDEX = "StandardSecondaryIndex";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        // build SAI indexes
        Indexes.Builder saiIndexes = Indexes.builder();
        saiIndexes.add(IndexMetadata.fromSchemaMetadata(CF_STANDARD_SAI + "_val", IndexMetadata.Kind.CUSTOM, new HashMap<>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "val");
        }}));

        // build secondary indexes
        Indexes.Builder secondaryIndexes = Indexes.builder();
        secondaryIndexes.add(IndexMetadata.fromIndexTargets(Collections.singletonList(
                                                            new IndexTarget(
                                                            new ColumnIdentifier("val", true), IndexTarget.Type.VALUES)), CF_STANDARD_SECONDARY_INDEX + "_val",
                                                            IndexMetadata.Kind.COMPOSITES,
                                                            Collections.emptyMap()));

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SAI,
                                                              1, AsciiType.instance, LongType.instance, null)
                                                .partitioner(Murmur3Partitioner.instance) // supported by SAI
                                                .indexes(saiIndexes.build()),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX,
                                                              1, AsciiType.instance, LongType.instance, null)
                                                .indexes(secondaryIndexes.build()));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
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
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SECONDARY_INDEX).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BloomFilter.recreateOnFPChanceChange = false;
    }

    @Test
    public void testSAIIndexScan()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SAI);
        Context context = new Context(KEYSPACE1, CF_STANDARD_SAI, store.metadata.id.toString());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", (long)j)
            .build()
            .applyUnsafe();
        }

        ReadCommand readCommand = Util.cmd(store)
                             .columns("val")
                             .filterOn("val", Operator.GT, 0L)
                             .build();

        handleReadCommand(readCommand);

        assertRequestAndRegistrySensorsEquality(context);

        Sensor requestSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_READ_BYTES);
        assertResponseSensors(requestSensor.getValue(), requestSensor.getValue());
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
            .add("val", (long)j)
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
        Sensor indexReadSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_READ_BYTES);
        double singleRowSearchBytes = indexReadSensor.getValue();
        indexReadSensor.reset();

        // Scan the whole index
        readCommand = Util.cmd(store)
                          .columns("val")
                          .filterOn("val", Operator.GTE, 0L)
                          .build();
        handleReadCommand(readCommand);

        double fullIndexScanBytes = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_READ_BYTES).getValue();
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
            .add("val", (long)j)
            .build()
            .applyUnsafe();
        }

        ReadCommand readCommand = Util.cmd(store)
                                      .fromKeyIncl("0").toKeyIncl("10")
                                      .columns("val")
                                      .filterOn("val", Operator.EQ, 1L) // only EQ is supported by CassnadraIndex
                                      .build();

        handleReadCommand(readCommand);

        assertRequestAndRegistrySensorsEquality(context);

        Sensor requestSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_READ_BYTES);
        assertResponseSensors(requestSensor.getValue(), requestSensor.getValue());
    }

    private static void handleReadCommand(ReadCommand command)
    {
        ReadCommandVerbHandler.instance.doVerb(Message.builder(Verb.READ_REQ, command).build());
    }

    private void assertRequestAndRegistrySensorsEquality(Context context)
    {
        Sensor localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_READ_BYTES);
        assertThat(localSensor.getValue()).isGreaterThan(0);

        Sensor registrySensor = SensorsTestUtil.getRegistrySensor(context, Type.INDEX_READ_BYTES);
        assertThat(registrySensor).isEqualTo(localSensor);
    }

    private void assertResponseSensors(double requestValue, double registryValue)
    {
        assertThat(capturedOutboundMessages).hasSize(1);
        Message message = capturedOutboundMessages.get(0);
        assertResponseSensors(message, requestValue, registryValue);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = SensorsTestUtil.serialize(message);
        Message deserializedMessage = SensorsTestUtil.deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestValue, registryValue);
    }

    private void assertResponseSensors(Message message, double requestValue, double registryValue)
    {
        assertThat(message.header.customParams()).isNotNull();
        assertThat(message.header.customParams()).containsKey(SensorsCustomParams.INDEX_READ_BYTES_REQUEST);
        assertThat(message.header.customParams()).containsKey(SensorsCustomParams.INDEX_READ_BYTES_TABLE);

        double requestReadBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(SensorsCustomParams.INDEX_READ_BYTES_REQUEST));
        double tableReadBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(SensorsCustomParams.INDEX_READ_BYTES_TABLE));
        assertThat(requestReadBytes).isEqualTo(requestValue);
        assertThat(tableReadBytes).isEqualTo(registryValue);
    }
}
