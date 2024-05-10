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
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
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

import static org.assertj.core.api.Assertions.assertThat;

public class SensorsIndexWriteTest
{
    public static final String KEYSPACE1 = "SensorsIndexWriteTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD_SAI = "StandardSAI";
    public static final String CF_STANDARD_SECONDARY_INDEX = "StandardSecondaryIndex";

    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();

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

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SAI,
                                                              1, AsciiType.instance, AsciiType.instance, null)
                                                .partitioner(Murmur3Partitioner.instance) // supported by SAI
                                                .indexes(saiIndexes.build()),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX,
                                                              1, AsciiType.instance, AsciiType.instance, null)
                                                .indexes(secondaryIndexes.build()));

        CompactionManager.instance.disableAutoCompaction();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
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
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SECONDARY_INDEX).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        CassandraRelevantProperties.BF_RECREATE_ON_FP_CHANCE_CHANGE.setBoolean(false);
    }

    @Test
    public void testSingleRowMutationWithSAI()
    {
        ColumnFamilyStore standardStore = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context standardContext = new Context(KEYSPACE1, CF_STANDARD, standardStore.metadata.id.toString());

        ColumnFamilyStore saiStore = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SAI);
        Context saiContext = new Context(KEYSPACE1, CF_STANDARD_SAI, saiStore.metadata.id.toString());

        String partitionKey = "0";
        Mutation standardMutation = new RowUpdateBuilder(standardStore.metadata(), 0, partitionKey)
                                    .add("val", "hi there")
                                    .build();
        handleMutation(standardMutation);

        Sensor standardSensor = SensorsTestUtil.getThreadLocalRequestSensor(standardContext, Type.WRITE_BYTES);
        assertThat(standardSensor.getValue()).isGreaterThan(0);
        Sensor standardRegistrySensor = SensorsTestUtil.getRegistrySensor(standardContext, Type.WRITE_BYTES);
        assertThat(standardRegistrySensor).isEqualTo(standardSensor);

        // check global registry is synchronized for Standard table
        assertThat(standardRegistrySensor.getValue()).isEqualTo(standardSensor.getValue());
        String writeRequestParam = String.format(SensorsCustomParams.WRITE_BYTES_REQUEST_TEMPLATE, CF_STANDARD);
        String writeTableParam = String.format(SensorsCustomParams.WRITE_BYTES_TABLE_TEMPLATE, CF_STANDARD);
        assertResponseSensors(standardSensor.getValue(), standardRegistrySensor.getValue(), writeRequestParam, writeTableParam);

        Mutation saiMutation = new RowUpdateBuilder(saiStore.metadata(), 0, partitionKey)
                               .add("val", "hi there")
                               .build();
        handleMutation(saiMutation);

        Sensor saiSensor = SensorsTestUtil.getThreadLocalRequestSensor(saiContext, Type.INDEX_WRITE_BYTES);
        // Writing the same amount of data to an SAI indexed column should generate at least the same number of bytes (the SAI write >= the vanilla write bytes)
        assertThat(saiSensor.getValue()).isGreaterThanOrEqualTo(standardSensor.getValue());
        Sensor saiRegistrySensor = SensorsTestUtil.getRegistrySensor(saiContext, Type.INDEX_WRITE_BYTES);
        assertThat(saiRegistrySensor).isEqualTo(saiSensor);

        // check global registry is synchronized for SAI table
        assertThat(saiRegistrySensor.getValue()).isEqualTo(saiSensor.getValue());
        String requestParam = String.format(SensorsCustomParams.INDEX_WRITE_BYTES_REQUEST_TEMPLATE, CF_STANDARD_SAI);
        String tableParam = String.format(SensorsCustomParams.INDEX_WRITE_BYTES_TABLE_TEMPLATE, CF_STANDARD_SAI);
        assertResponseSensors(saiSensor.getValue(), saiRegistrySensor.getValue(), requestParam, tableParam);
    }

    @Test
    public void testSingleRowMutationWithSecondaryIndex()
    {
        ColumnFamilyStore standardStore = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context standardContext = new Context(KEYSPACE1, CF_STANDARD, standardStore.metadata.id.toString());

        ColumnFamilyStore secondaryIndexStore = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX);
        Context secondaryIndexContext = new Context(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX, secondaryIndexStore.metadata.id.toString());

        String partitionKey = "0";
        Mutation standardMutation = new RowUpdateBuilder(standardStore.metadata(), 0, partitionKey)
                                    .add("val", "hi there")
                                    .build();
        handleMutation(standardMutation);

        Sensor standardSensor = SensorsTestUtil.getThreadLocalRequestSensor(standardContext, Type.WRITE_BYTES);
        assertThat(standardSensor.getValue()).isGreaterThan(0);
        Sensor standardRegistrySensor = SensorsTestUtil.getRegistrySensor(standardContext, Type.WRITE_BYTES);
        assertThat(standardRegistrySensor).isEqualTo(standardSensor);

        // check global registry is synchronized for Standard table
        assertThat(standardRegistrySensor.getValue()).isEqualTo(standardSensor.getValue());
        String writeRequestParam = String.format(SensorsCustomParams.WRITE_BYTES_REQUEST_TEMPLATE, CF_STANDARD);
        String writeTableParam = String.format(SensorsCustomParams.WRITE_BYTES_TABLE_TEMPLATE, CF_STANDARD);
        assertResponseSensors(standardSensor.getValue(), standardRegistrySensor.getValue(), writeRequestParam, writeTableParam);

        Mutation secondaryIndexMutation = new RowUpdateBuilder(secondaryIndexStore.metadata(), 0, partitionKey)
                                          .add("val", "hi there")
                                          .build();
        handleMutation(secondaryIndexMutation);

        Sensor secondaryIndexSensor = SensorsTestUtil.getThreadLocalRequestSensor(secondaryIndexContext, Type.INDEX_WRITE_BYTES);
        // Writing the same amount of data to an indexed column should generate at least the same number of bytes (the SecondaryIndex write bytes >= the vanilla write bytes)
        assertThat(secondaryIndexSensor.getValue()).isGreaterThanOrEqualTo(standardSensor.getValue());
        Sensor secondaryIndexRegistrySensor = SensorsTestUtil.getRegistrySensor(secondaryIndexContext, Type.INDEX_WRITE_BYTES);
        assertThat(secondaryIndexRegistrySensor).isEqualTo(secondaryIndexSensor);

        // check global registry is synchronized for Secondary Index table
        assertThat(secondaryIndexRegistrySensor.getValue()).isEqualTo(secondaryIndexSensor.getValue());
        String indexRequestParam = String.format(SensorsCustomParams.INDEX_WRITE_BYTES_REQUEST_TEMPLATE, CF_STANDARD_SECONDARY_INDEX);
        String indexTableParam = String.format(SensorsCustomParams.INDEX_WRITE_BYTES_TABLE_TEMPLATE, CF_STANDARD_SECONDARY_INDEX);
        assertResponseSensors(secondaryIndexSensor.getValue(), secondaryIndexRegistrySensor.getValue(), indexRequestParam, indexTableParam);
    }

    private static void handleMutation(Mutation mutation)
    {
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
    }

    private void assertResponseSensors(double requestValue, double registryValue, String requestParam, String tableParam)
    {
        // verify against the last message to enable testing of multiple mutations in a for loop
        Message message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        assertResponseSensors(message, requestValue, registryValue, requestParam, tableParam);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = SensorsTestUtil.serialize(message);
        Message deserializedMessage = SensorsTestUtil.deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestValue, registryValue, requestParam, tableParam);
    }

    private void assertResponseSensors(Message message, double requestValue, double registryValue, String expectedRequestParam, String expectedTableParam)
    {
        assertThat(message.header.customParams()).isNotNull();
        assertThat(message.header.customParams()).containsKey(expectedRequestParam);
        assertThat(message.header.customParams()).containsKey(expectedTableParam);

        double requestBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedRequestParam));
        double tableBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedTableParam));
        assertThat(requestBytes).isEqualTo(requestValue);
        assertThat(tableBytes).isEqualTo(registryValue);
    }
}
