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

import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorsInternodeMessageTest
{
    private static final String KEYSPACE1 = "SensorsInternodeMessageTest";
    private static final String CF_STANDARD = "Standard";
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
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUTNER)
        );

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).metadata());

        capturedOutboundMessages = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) ->
                                                     {
                                                         capturedOutboundMessages.add(message);
                                                         return true;
                                                     });
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
    }

    @Test
    public void testInternodeMessageSensorsForRead()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context ksContext = new Context(KEYSPACE1);

        new RowUpdateBuilder(store.metadata(), 0, "0")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build()
        .applyUnsafe();


        DecoratedKey key = store.getPartitioner().decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command = Util.cmd(store, key).build();
        Runnable handler = () -> ReadCommandVerbHandler.instance.doVerb(Message.builder(Verb.READ_REQ, command).build());
        testInternodeMessageSensors(handler, ksContext);
    }

    @Test
    public void testInternodeMessageSensorsForMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context ksContext = new Context(KEYSPACE1);

        Mutation mutation = new RowUpdateBuilder(store.metadata(), 0, "0")
                            .add("val", "0")
                            .build();

        Runnable handler = () -> MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
        testInternodeMessageSensors(handler, ksContext);
    }

    @Test
    public void testInternodeMessageSensorsForCounterMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context ksContext = new Context(KEYSPACE1);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER);
        cfs.truncateBlocking();

        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                            .clustering("cc")
                            .add("val", 1L).build();

        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ANY);

        Runnable handler = () -> CounterMutationVerbHandler.instance.doVerb(Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation).build());
        testInternodeMessageSensors(handler, ksContext);
    }

    private void testInternodeMessageSensors(Runnable handler, Context ksContext)
    {
        // Boostraps the internode sensors in the registry. Notice that the first outbound message will not have any internode sensors
        // because the resitry will be initilized after the first outbound message is intercepted.
        handler.run();
        Sensor internodeBytesSensor = SensorsRegistry.instance.getSensor(ksContext, Type.INTERNODE_MSG_BYTES).get();
        internodeBytesSensor.reset();
        Sensor internodeCountSensor = SensorsRegistry.instance.getSensor(ksContext, Type.INTERNODE_MSG_COUNT).get();
        internodeCountSensor.reset();

        // Capture the first values of internode message sensors
        handler.run();
        double internodeBytes = internodeBytesSensor.getValue();
        assertThat(internodeBytes).isGreaterThan(0);
        double internodeCount = internodeCountSensor.getValue();
        assertThat(internodeCount).isEqualTo(1.0);

        // handle the same command/mutation two more times
        handler.run();
        handler.run();

        double newInternodeBytes = internodeBytesSensor.getValue();
        assertThat(newInternodeBytes).isEqualTo(internodeBytes * 3.0);
        double newInternodeCount = internodeCountSensor.getValue();
        assertThat(newInternodeCount).isEqualTo(internodeCount * 3.0);

        // check the latest outbound message accomodated for the previous two internode messages
        Message<?> message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        assertThat(message.header.customParams()).isNotNull();
        assertThat(message.header.customParams()).containsKey(SensorsCustomParams.INTERNODE_MSG_BYTES);
        assertThat(message.header.customParams()).containsKey(SensorsCustomParams.INTERNODE_MSG_COUNT);

        double internodeBytesInHeader = SensorsTestUtil.bytesToDouble(message.header.customParams().get(SensorsCustomParams.INTERNODE_MSG_BYTES));
        double internodeCountInHeader = SensorsTestUtil.bytesToDouble(message.header.customParams().get(SensorsCustomParams.INTERNODE_MSG_COUNT));
        assertThat(internodeBytesInHeader).isEqualTo(internodeBytes * 2.0);
        assertThat(internodeCountInHeader).isEqualTo(internodeCount * 2.0);
    }
}
