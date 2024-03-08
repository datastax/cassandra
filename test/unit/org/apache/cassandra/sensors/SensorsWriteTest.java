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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.config.CassandraRelevantProperties.BF_RECREATE_ON_FP_CHANCE_CHANGE;
import static org.assertj.core.api.Assertions.assertThat;

public class SensorsWriteTest
{
    public static final String KEYSPACE1 = "SensorsWriteTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_STANDARD_CLUSTERING = "StandardClustering";


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
                                                              1, AsciiType.instance, AsciiType.instance, AsciiType.instance));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).metadata());

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

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BF_RECREATE_ON_FP_CHANCE_CHANGE.setBoolean(false);
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

    private static void handleMutation(Mutation mutation)
    {
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
    }

    private void assertResponseSensors(double requestValue, double registryValue, String table)
    {
        // verify against the last message to enable testing of multiple mutations in a for loop
        Message message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        assertResponseSensors(message, requestValue, registryValue, table);

        // make sure messages with sensor values can be deserialized on the receiving node
        DataOutputBuffer out = SensorsTestUtil.serialize(message);
        Message deserializedMessage = SensorsTestUtil.deserialize(out, message.from());
        assertResponseSensors(deserializedMessage, requestValue, registryValue, table);
    }

    private void assertResponseSensors(Message message, double requestValue, double registryValue, String table)
    {
        assertThat(message.header.customParams()).isNotNull();
        String expectedRequestParam = String.format(SensorsCustomParams.WRITE_BYTES_REQUEST_TEMPLATE, table);
        String expectedTableParam = String.format(SensorsCustomParams.WRITE_BYTES_TABLE_TEMPLATE, table);

        assertThat(message.header.customParams()).containsKey(expectedRequestParam);
        assertThat(message.header.customParams()).containsKey(expectedTableParam);

        double requestWriteBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedRequestParam));
        double tableWriteBytes = SensorsTestUtil.bytesToDouble(message.header.customParams().get(expectedTableParam));
        assertThat(requestWriteBytes).isEqualTo(requestValue);
        assertThat(tableWriteBytes).isEqualTo(registryValue);
    }
}
