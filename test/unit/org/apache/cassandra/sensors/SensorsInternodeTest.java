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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;

import com.google.common.collect.ImmutableSet;
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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorsInternodeTest
{
    private static final String KEYSPACE1 = "SensorsInternodeTest";
    private static final String CF_STANDARD = "Standard";
    private static final String CF_STANDARD2 = "Standard2";

    private static final String CF_COUTNER = "Counter";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;
    private BiPredicate<Message<?>, InetAddressAndPort> outboundSinkHandler;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUTNER)
        );

        CompactionManager.instance.disableAutoCompaction();
        MessagingService.instance().listen();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).metadata());

        capturedOutboundMessages = new CopyOnWriteArrayList<>();
        outboundSinkHandler = (message, to) -> capturedOutboundMessages.add(message);
        MessagingService.instance().outboundSink.add(outboundSinkHandler);
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
        MessagingService.instance().outboundSink.remove(outboundSinkHandler);
    }

    @Test
    public void testInternodeSensorsForRead()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        new RowUpdateBuilder(store.metadata(), 0, "0")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build()
        .applyUnsafe();

        DecoratedKey key = store.getPartitioner().decorateKey(ByteBufferUtil.bytes("0"));
        ReadCommand command = Util.cmd(store, key).build();
        Message request = Message.builder(Verb.READ_REQ, command).build();
        Runnable handler = () -> ReadCommandVerbHandler.instance.doVerb(request);
        testInternodeSensors(request, handler, ImmutableSet.of(context));
    }

    @Test
    public void testInternodeSensorsForMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        Mutation mutation = new RowUpdateBuilder(store.metadata(), 0, "0")
                            .add("val", "0")
                            .build();

        Message request = Message.builder(Verb.MUTATION_REQ, mutation).build();
        Runnable handler = () -> MutationVerbHandler.instance.doVerb(request);
        testInternodeSensors(request, handler, ImmutableSet.of(context));
    }

    @Test
    public void testInternodeSensorsForBatchMutation()
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
        Message request = Message.builder(Verb.MUTATION_REQ, mutation).build();
        Runnable handler = () -> MutationVerbHandler.instance.doVerb(request);
        testInternodeSensors(request, handler, ImmutableSet.of(context1, context2));
    }

    @Test
    public void testInternodeSensorsForCounterMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_COUTNER);
        Context context = new Context(KEYSPACE1, CF_COUTNER, store.metadata.id.toString());
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER);
        cfs.truncateBlocking();

        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                            .clustering("cc")
                            .add("val", 1L).build();

        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ANY);

        Message request = Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation).build();
        Runnable handler = () -> CounterMutationVerbHandler.instance.doVerb(request);
        testInternodeSensors(request, handler, ImmutableSet.of(context));
    }

    private void testInternodeSensors(Message request, Runnable handler, Collection<Context> contexts)
    {
        // Run the handler:
        handler.run();

        // Get the request size, response size and total size per table:
        int tableCount = contexts.size();
        int requestSizePerTable = request.payloadSize(MessagingService.current_version) / tableCount;
        Message response = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        int responseSizePerTable = response.payloadSize(MessagingService.current_version) / tableCount;
        int total = requestSizePerTable + responseSizePerTable;

        // For each context/table, get the internode bytes and verify their value is between the request and total size:
        // it can't be equal to the total size because we don't record the custom headers in the internode sensor.
        for (Context context : contexts)
        {
            Sensor internodeBytesSensor = SensorsRegistry.instance.getSensor(context, Type.INTERNODE_BYTES).get();
            double internodeBytes = internodeBytesSensor.getValue();
            assertThat(internodeBytes).isBetween(requestSizePerTable * 1.0, total * 1.0);
        }
    }
}
