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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
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
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that sensors are not tracked when disabled. Please note that sensor tracking is and opt-in feature so any existing
 * test that exercise the {@link org.apache.cassandra.net.IVerbHandler#doVerb(Message)} would surface functionality regression (e.g. NPEs).
 * Here we make sure that sensors are indeed not tracked when disabled and for that it suffices to cover a happy case verb handler invocation scenario.
 */
public class DisabledSensorsTest
{
    public static final String KEYSPACE1 = "SensorsReadTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD_SAI = "StandardSAI";
    private static final String CF_COUTNER = "Counter";

    private ColumnFamilyStore store;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        CassandraRelevantProperties.REQUEST_SENSORS_FACTORY.setString(SensorsTestUtil.NoOpRequestSensorsFactory.class.getName());

        // build SAI indexes
        Indexes.Builder saiIndexes = Indexes.builder();
        saiIndexes.add(IndexMetadata.fromSchemaMetadata(CF_STANDARD_SAI + "_val", IndexMetadata.Kind.CUSTOM, new HashMap<>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "val");
        }}));

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SAI,
                                                              1, AsciiType.instance, AsciiType.instance, null)
                                                .partitioner(Murmur3Partitioner.instance)
                                                .indexes(saiIndexes.build()),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUTNER));

        CompactionManager.instance.disableAutoCompaction();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).truncateBlocking();
    }

    @Test
    public void testSensorsForReadVerbHandler()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);

        new RowUpdateBuilder(store.metadata(), 0, "0")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build()
        .applyUnsafe();

        DecoratedKey key = store.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(store, key).build();
        handleReadCommand(command);

        assertSensorsAreNotTracked();
    }

    @Test
    public void testSensorsForMutationVerbHandler()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);

        Mutation m = new RowUpdateBuilder(store.metadata(), 0, "0")
                     .add("val", "0")
                     .build();
        handleMutation(m);

        assertSensorsAreNotTracked();
    }

    @Test
    public void testSensorsForMutationVerbHandlerWithSAI()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SAI);
        Mutation saiMutation = new RowUpdateBuilder(store.metadata(), 0, "0")
                               .add("val", "hi there")
                               .build();
        handleMutation(saiMutation);
        assertSensorsAreNotTracked();
    }

    @Test
    public void testSensorsForCounterMutationVerbHandler() throws WriteTimeoutException
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_COUTNER);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER);
        cfs.truncateBlocking();

        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 5, "key1")
                            .clustering("cc")
                            .add("val", 1L).build();

        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ANY);
        handleCounterMutation(counterMutation);

        assertSensorsAreNotTracked();
    }

    @Test
    public void testLWTSensors()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        PartitionUpdate update = new RowUpdateBuilder(store.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        Commit commit = Commit.newPrepare(update.partitionKey(), store.metadata(), UUIDGen.getTimeUUID());
        handlePaxosPrepare(commit);
        handlePaxosPropose(commit);
        handlePaxosCommit(commit);

        assertSensorsAreNotTracked();
    }

    private static void handleReadCommand(ReadCommand command)
    {
        ReadCommandVerbHandler.instance.doVerb(Message.builder(Verb.READ_REQ, command).build());
    }

    private static void handleMutation(Mutation mutation)
    {
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
    }

    private static void handleCounterMutation(CounterMutation mutation)
    {
        CounterMutationVerbHandler.instance.doVerb(Message.builder(Verb.COUNTER_MUTATION_REQ, mutation).build());
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

    private static void assertSensorsAreNotTracked()
    {
        assertThat(RequestTracker.instance.get()).isInstanceOf(NoOpRequestSensors.class);
        for (Type type : Type.values())
        {
            assertThat(SensorsRegistry.instance.getSensorsByType(type)).isEmpty();
        }
    }
}
