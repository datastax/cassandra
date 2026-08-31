/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sensors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
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
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.SystemKeyspace.PAXOS;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that replica-side write sensors ({@link Type#WRITE_BYTES}, {@link Type#WRITE_EXECUTION_TIME},
 * {@link Type#INDEX_WRITE_BYTES}, {@link Type#INTERNODE_BYTES}) are correctly recorded and propagated
 * to the global {@link SensorsRegistry} by the replica verb handlers:
 * {@link MutationVerbHandler}, {@link CounterMutationVerbHandler}, {@link PrepareVerbHandler},
 * {@link ProposeVerbHandler}, and {@link CommitVerbHandler}.
 * <p>
 * Each test exercises a specific write path (standard mutation, counter mutation, Paxos LWT rounds,
 * and mutations on indexed tables) and asserts that both the per-request sensor and the corresponding
 * global registry sensor carry the expected values after the verb handler completes.
 *
 * @see CoordinatorWriteSensorsTest for the coordinator-side write counterpart
 * @see ReplicaReadSensorsTest for the replica-side read counterpart
 */
public class ReplicaWriteSensorsTest
{
    private static final String KEYSPACE1 = "ReplicaWriteSensorsTest";
    private static final String CF_STANDARD = "Standard";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String CF_STANDARD_CLUSTERING = "StandardClustering";
    private static final String CF_COUTNER = "Counter";
    private static final String CF_STANDARD_SAI = "StandardSAI";
    private static final String CF_STANDARD_SECONDARY_INDEX = "StandardSecondaryIndex";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        SchemaLoader.prepareServer();

        // build SAI indexes
        Indexes.Builder saiIndexes = Indexes.builder();
        saiIndexes.add(IndexMetadata.fromSchemaMetadata(CF_STANDARD_SAI + "_val", IndexMetadata.Kind.CUSTOM, new HashMap<>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "val");
        }}));

        // build secondary (2i) indexes
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
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_CLUSTERING,
                                                              1, AsciiType.instance, AsciiType.instance, AsciiType.instance),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUTNER),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SAI,
                                                              1, AsciiType.instance, AsciiType.instance, null)
                                                .partitioner(Murmur3Partitioner.instance)
                                                .indexes(saiIndexes.build()),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX,
                                                              1, AsciiType.instance, AsciiType.instance, null)
                                                .partitioner(Murmur3Partitioner.instance)
                                                .indexes(secondaryIndexes.build()));


        // Align the global partitioner with the indexed tables (SAI requires Murmur3). This must be
        // called after createKeyspace so that the plain tables (counter, standard) inherit the default
        // partitioner during schema creation and are not affected by this switch.
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SECONDARY_INDEX).metadata());

        // enable sensor registry for system keyspace
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
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SAI).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_SECONDARY_INDEX).truncateBlocking();
        Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PAXOS).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BloomFilter.recreateOnFPChanceChange = false;
    }

    // -------------------------------------------------------------------------
    // Standard mutation paths
    // -------------------------------------------------------------------------

    @Test
    public void testSingleRowMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        double bytesSensorSum = 0;
        double executionSensorSum = 0;
        for (int j = 0; j < 10; j++)
        {
            Mutation m = new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                         .add("val", String.valueOf(j))
                         .build();
            handleMutation(m);
            Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
            assertThat(bytesSensor.getValue()).isGreaterThan(0);
            Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
            assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
            bytesSensorSum += bytesSensor.getValue();
            Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
            assertThat(executionSensor.getValue()).isGreaterThan(0);
            Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
            assertThat(executionRegistrySensor).isEqualTo(executionSensor);
            executionSensorSum += executionSensor.getValue();

            // check global registry is synchronized
            assertThat(bytesRegistrySensor.getValue()).isEqualTo(bytesSensorSum);
            assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensorSum);
            assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                                  Pair.create(executionSensor, executionRegistrySensor));
        }
    }

    @Test
    public void testSingleRowMutationWithClusteringKey()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);
        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());

        double bytesSensorSum = 0;
        double executionSensorSum = 0;
        for (int j = 0; j < 10; j++)
        {
            Mutation m = new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                         .clustering(String.valueOf(j))
                         .add("val", String.valueOf(j))
                         .build();
            handleMutation(m);
            Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
            assertThat(bytesSensor.getValue()).isGreaterThan(0);
            Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
            assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
            bytesSensorSum += bytesSensor.getValue();
            Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
            assertThat(executionSensor.getValue()).isGreaterThan(0);
            Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
            assertThat(executionRegistrySensor).isEqualTo(executionSensor);
            executionSensorSum += executionSensor.getValue();

            // check global registry is synchronized
            assertThat(bytesRegistrySensor.getValue()).isEqualTo(bytesSensorSum);
            assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensorSum);
            assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                                  Pair.create(executionSensor, executionRegistrySensor));
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

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(bytesSensor.getValue()).isEqualTo(10 * singleRowWriteBytes);

        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        assertThat(bytesRegistrySensor.getValue()).isEqualTo(bytesSensor.getValue() + singleRowWriteBytes);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        // execution time accumulates across both handleMutation calls
        assertThat(executionRegistrySensor.getValue()).isGreaterThan(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
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

        Sensor bytesSensor1 = SensorsTestUtil.getThreadLocalRequestSensor(context1, Type.WRITE_BYTES);
        assertThat(bytesSensor1.getValue()).isGreaterThan(0);

        Sensor bytesSensor2 = SensorsTestUtil.getThreadLocalRequestSensor(context2, Type.WRITE_BYTES);
        assertThat(bytesSensor2.getValue()).isGreaterThan(0);

        Sensor bytesRegistrySensor1 = SensorsTestUtil.getRegistrySensor(context1, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor1).isEqualTo(bytesSensor1);
        assertThat(bytesRegistrySensor1.getValue()).isEqualTo(bytesSensor1.getValue());

        Sensor bytesRegistrySensor2 = SensorsTestUtil.getRegistrySensor(context2, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor2).isEqualTo(bytesSensor2);
        assertThat(bytesRegistrySensor2.getValue()).isEqualTo(bytesSensor2.getValue());

        Sensor executionSensor1 = SensorsTestUtil.getThreadLocalRequestSensor(context1, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor1.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor1 = SensorsTestUtil.getRegistrySensor(context1, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor1).isEqualTo(executionSensor1);
        assertThat(executionRegistrySensor1.getValue()).isEqualTo(executionSensor1.getValue());
        assertResponseSensors(Pair.create(bytesSensor1, bytesRegistrySensor1),
                              Pair.create(executionSensor1, executionRegistrySensor1));

        Sensor executionSensor2 = SensorsTestUtil.getThreadLocalRequestSensor(context2, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor2.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor2 = SensorsTestUtil.getRegistrySensor(context2, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor2).isEqualTo(executionSensor2);
        assertThat(executionRegistrySensor2.getValue()).isEqualTo(executionSensor2.getValue());
        assertResponseSensors(Pair.create(bytesSensor2, bytesRegistrySensor2),
                              Pair.create(executionSensor2, executionRegistrySensor2));
    }

    // -------------------------------------------------------------------------
    // Counter mutation path
    // -------------------------------------------------------------------------

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

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
    }

    // -------------------------------------------------------------------------
    // Paxos LWT paths
    // -------------------------------------------------------------------------

    @Test
    public void testLWTPrepare() {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        PartitionUpdate update = new RowUpdateBuilder(store.metadata(), 0, "0")
                                 .add("val", "0")
                                 .buildUpdate();
        Commit proposal = Commit.newPrepare(update.partitionKey(), store.metadata(), UUIDGen.getTimeUUID());
        handlePaxosPrepare(proposal);

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
        Sensor readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isZero();

        // handle the commit again, this time paxos has state because of the first proposal and read bytes will be populated
        handlePaxosPrepare(proposal);
        readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isGreaterThan(0);
        Sensor registryReadSensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(registryReadSensor).isEqualTo(readSensor);
        assertResponseSensors(Pair.create(readSensor, registryReadSensor));
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

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));
        Sensor readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isZero();

        // handle the commit again, this time paxos has state because of the first proposal and read bytes will be populated
        handlePaxosPropose(proposal);
        readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).isGreaterThan(0);
        Sensor registryReadSensor = SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES);
        assertThat(registryReadSensor).isEqualTo(readSensor);
        assertResponseSensors(Pair.create(readSensor, registryReadSensor));
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

        Sensor bytesSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(bytesSensor.getValue()).isGreaterThan(0);
        Sensor bytesRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(bytesRegistrySensor).isEqualTo(bytesSensor);
        Sensor executionSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionSensor.getValue()).isGreaterThan(0);
        Sensor executionRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(executionRegistrySensor).isEqualTo(executionSensor);
        assertThat(executionRegistrySensor.getValue()).isEqualTo(executionSensor.getValue());
        assertResponseSensors(Pair.create(bytesSensor, bytesRegistrySensor),
                              Pair.create(executionSensor, executionRegistrySensor));

        // No read is done in the commit phase
        assertThat(RequestTracker.instance.get().getSensor(context, Type.READ_BYTES)).isEmpty();
        assertThat(SensorsRegistry.instance.getSensor(context, Type.READ_BYTES)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Index write paths (SAI and secondary index)
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@link Type#INDEX_WRITE_BYTES} is tracked when a mutation is applied to a table
     * with an SAI index via {@link MutationVerbHandler}. Writing the same amount of data to an SAI-indexed
     * column produces at least as many index bytes as base-table bytes.
     */
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
        assertThat(standardRegistrySensor.getValue()).isEqualTo(standardSensor.getValue());
        assertResponseSensors(Pair.create(standardSensor, standardRegistrySensor));

        Mutation saiMutation = new RowUpdateBuilder(saiStore.metadata(), 0, partitionKey)
                               .add("val", "hi there")
                               .build();
        handleMutation(saiMutation);

        Sensor saiSensor = SensorsTestUtil.getThreadLocalRequestSensor(saiContext, Type.INDEX_WRITE_BYTES);
        // Writing the same amount of data to an SAI indexed column should generate at least the same number of bytes
        assertThat(saiSensor.getValue()).isGreaterThanOrEqualTo(standardSensor.getValue());
        Sensor saiRegistrySensor = SensorsTestUtil.getRegistrySensor(saiContext, Type.INDEX_WRITE_BYTES);
        assertThat(saiRegistrySensor).isEqualTo(saiSensor);
        assertThat(saiRegistrySensor.getValue()).isEqualTo(saiSensor.getValue());
        assertResponseSensors(Pair.create(saiSensor, saiRegistrySensor));
    }

    /**
     * Verifies that {@link Type#INDEX_WRITE_BYTES} is tracked when a mutation is applied to a table
     * with a secondary (2i) index via {@link MutationVerbHandler}. The 2i index write bytes are expected
     * to be at least half the base-table write bytes. The base-table {@link Type#WRITE_BYTES} for the
     * indexed table must equal those of an equivalent plain-table write.
     */
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
        assertThat(standardRegistrySensor.getValue()).isEqualTo(standardSensor.getValue());
        assertResponseSensors(Pair.create(standardSensor, standardRegistrySensor));

        Mutation secondaryIndexMutation = new RowUpdateBuilder(secondaryIndexStore.metadata(), 0, partitionKey)
                                          .add("val", "hi there")
                                          .build();
        handleMutation(secondaryIndexMutation);

        Sensor secondaryIndexSensor = SensorsTestUtil.getThreadLocalRequestSensor(secondaryIndexContext, Type.INDEX_WRITE_BYTES);
        // We are not guaranteed that the amount of data we write to the secondary index is more than what we write to the main file,
        // and we are not tracking it very precisely. It should, though, at least include the cell data and deletions which is about
        // half the standard write size.
        assertThat(secondaryIndexSensor.getValue()).isGreaterThanOrEqualTo(standardSensor.getValue() / 2);
        Sensor secondaryIndexRegistrySensor = SensorsTestUtil.getRegistrySensor(secondaryIndexContext, Type.INDEX_WRITE_BYTES);
        assertThat(secondaryIndexRegistrySensor).isEqualTo(secondaryIndexSensor);
        // Check that we also get the correct vanilla write bytes for this operation.
        assertThat(SensorsTestUtil.getThreadLocalRequestSensor(secondaryIndexContext, Type.WRITE_BYTES).getValue()).isEqualTo(standardSensor.getValue());
        assertThat(secondaryIndexRegistrySensor.getValue()).isEqualTo(secondaryIndexSensor.getValue());
        assertResponseSensors(Pair.create(secondaryIndexSensor, secondaryIndexRegistrySensor));
    }

    /**
     * Verifies that {@link Type#INDEX_WRITE_BYTES} is tracked when a Paxos commit applies a mutation
     * to a table with an SAI index via {@link CommitVerbHandler}. Only the Commit phase is tested here
     * because it is the only Paxos phase that writes to the base table and triggers SAI index updaters.
     */
    @Test
    public void testPaxosCommitWithSAI()
    {
        ColumnFamilyStore saiStore = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SAI);
        Context context = new Context(KEYSPACE1, CF_STANDARD_SAI, saiStore.metadata.id.toString());

        PartitionUpdate update = new RowUpdateBuilder(saiStore.metadata(), 0, "0")
                                 .add("val", "hi there")
                                 .buildUpdate();
        Commit commit = Commit.newProposal(UUIDGen.getTimeUUID(), update);
        CommitVerbHandler.instance.doVerb(Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build());

        Sensor indexWriteSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_WRITE_BYTES);
        assertThat(indexWriteSensor).isNotNull();
        assertThat(indexWriteSensor.getValue()).isGreaterThan(0);
        Sensor indexWriteRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.INDEX_WRITE_BYTES);
        assertThat(indexWriteRegistrySensor).isEqualTo(indexWriteSensor);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor).isNotNull();
        assertThat(writeSensor.getValue()).isGreaterThan(0);
        Sensor writeRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(writeRegistrySensor).isEqualTo(writeSensor);

        assertResponseSensors(Pair.create(indexWriteSensor, indexWriteRegistrySensor),
                              Pair.create(writeSensor, writeRegistrySensor));
    }

    /**
     * Verifies that {@link Type#INDEX_WRITE_BYTES} is tracked when a Paxos commit applies a mutation
     * to a table with a secondary (2i) index via {@link CommitVerbHandler}. Only the Commit phase is
     * tested here because it is the only Paxos phase that writes to the base table and triggers
     * secondary index updaters.
     */
    @Test
    public void testPaxosCommitWithSecondaryIndex()
    {
        ColumnFamilyStore secondaryIndexStore = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX);
        Context context = new Context(KEYSPACE1, CF_STANDARD_SECONDARY_INDEX, secondaryIndexStore.metadata.id.toString());

        PartitionUpdate update = new RowUpdateBuilder(secondaryIndexStore.metadata(), 0, "0")
                                 .add("val", "hi there")
                                 .buildUpdate();
        Commit commit = Commit.newProposal(UUIDGen.getTimeUUID(), update);
        CommitVerbHandler.instance.doVerb(Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build());

        Sensor indexWriteSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INDEX_WRITE_BYTES);
        assertThat(indexWriteSensor).isNotNull();
        assertThat(indexWriteSensor.getValue()).isGreaterThan(0);
        Sensor indexWriteRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.INDEX_WRITE_BYTES);
        assertThat(indexWriteRegistrySensor).isEqualTo(indexWriteSensor);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor).isNotNull();
        assertThat(writeSensor.getValue()).isGreaterThan(0);
        Sensor writeRegistrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
        assertThat(writeRegistrySensor).isEqualTo(writeSensor);

        assertResponseSensors(Pair.create(indexWriteSensor, indexWriteRegistrySensor),
                              Pair.create(writeSensor, writeRegistrySensor));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    @SafeVarargs
    private void assertResponseSensors(Pair<Sensor, Sensor>... requestToRegistrySensors)
    {
        // verify against the last message to enable testing of multiple mutations in a for loop
        Message message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
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
