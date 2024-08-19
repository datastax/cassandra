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

package org.apache.cassandra.db;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.ActiveRequestSensors;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.net.ParamType.TRACE_SESSION;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class CounterMutationCallbackTest
{
    private static final String KEYSPACE1 = "CounterMutationCallbackTest";
    private static final String CF_COUTNER = "Counter";
    private static final double COUNTER_MUTATION_WRITE_BYTES = 56.0;
    private static final double COUNTER_MUTATION_INTERNODE_BYTES = 72.0;

    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(3),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUTNER));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).metadata());

        capturedOutboundMessages = new CopyOnWriteArrayList<>();
        MessagingService.instance().outboundSink.add((message, to) -> capturedOutboundMessages.add(message));
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUTNER).truncateBlocking();

        SensorsRegistry.instance.clear();

        CassandraRelevantProperties.BF_RECREATE_ON_FP_CHANCE_CHANGE.setBoolean(false);
    }

    @Parameterized.Parameter()
    public Pair<Integer, Integer> replicaCountAndExpectedSensorValueMultiplier;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        // pairs of (replica count, expected sensor value multiplier)
        return ImmutableList.of(
        Pair.create(0, 1), // CL.ANY
        Pair.create(1, 1), // CL.ONE
        Pair.create(2, 2),  // CL.TWO
        Pair.create(3, 3)  // CL.THREE
        );
    }

    @Test
    public void testCounterMutationCallback()
    {
        // dummy mutation
        TableMetadata metadata = MockSchema.newTableMetadata(KEYSPACE1, CF_COUTNER);
        Mutation mutation = new Mutation(PartitionUpdate.simpleBuilder(metadata, "").build());
        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ANY); // CL here just for serialization, otherwise ignored
        Message<CounterMutation> msg = Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation)
               .withId(1)
               .from(FBUtilities.getLocalAddressAndPort())
               .withCreatedAt(approxTime.now())
               .withExpiresAt(approxTime.now() + TimeUnit.SECONDS.toNanos(1))
               .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
               .withParam(TRACE_SESSION, UUID.randomUUID())
               .build();

        RequestSensors requestSensors = new ActiveRequestSensors();

        Context context = Context.from(Keyspace.open(KEYSPACE1).getMetadata().tables.get(CF_COUTNER).get());
        requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, COUNTER_MUTATION_WRITE_BYTES); // mimic a counter mutation of size COUNTER_MUTATION_WRITE_BYTES on the leader node
        requestSensors.incrementSensor(context, Type.INTERNODE_BYTES, COUNTER_MUTATION_INTERNODE_BYTES); // mimic an inter-node payload of size COUNTER_MUTATION_INTERNODE_BYTES on the leader node
        requestSensors.syncAllSensors();
        CounterMutationCallback callback = new CounterMutationCallback(msg, FBUtilities.getLocalAddressAndPort(), requestSensors);
        Integer replicaCount = replicaCountAndExpectedSensorValueMultiplier.left;
        callback.setReplicaCount(replicaCount);

        callback.run();

        // Sensor values on the leader should not accommodate for replica sensors
        Sensor localSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        assertThat(localSensor.getValue()).isEqualTo(COUNTER_MUTATION_WRITE_BYTES);
        Sensor registerSensor = SensorsRegistry.instance.getSensor(context, Type.WRITE_BYTES).get();
        assertThat(registerSensor.getValue()).isEqualTo(COUNTER_MUTATION_WRITE_BYTES);
        localSensor = requestSensors.getSensor(context, Type.INTERNODE_BYTES).get();
        assertThat(localSensor.getValue()).isEqualTo(COUNTER_MUTATION_INTERNODE_BYTES);
        registerSensor = SensorsRegistry.instance.getSensor(context, Type.INTERNODE_BYTES).get();
        assertThat(registerSensor.getValue()).isEqualTo(COUNTER_MUTATION_INTERNODE_BYTES);

        // verify custom headers have the sensors values adjusted for the replica count
        assertThat(capturedOutboundMessages).size().isEqualTo(1);
        Map<String, byte[]> customParam = capturedOutboundMessages.get(0).header.customParams();
        assertThat(customParam).isNotNull();
        int expectedSensorValueMultiplier = replicaCountAndExpectedSensorValueMultiplier.right;
        assertThat(customParam).hasEntrySatisfying("WRITE_BYTES_REQUEST.Counter",
                                                   v -> {
                                                       double actual = SensorsCustomParams.sensorValueFromBytes(v);
                                                       assertThat(actual).isEqualTo(COUNTER_MUTATION_WRITE_BYTES * expectedSensorValueMultiplier);
                                                   });
        assertThat(customParam).hasEntrySatisfying("WRITE_BYTES_TABLE.Counter",
                                                   v -> {
                                                       double actual = SensorsCustomParams.sensorValueFromBytes(v);
                                                       assertThat(actual).isEqualTo(COUNTER_MUTATION_WRITE_BYTES * expectedSensorValueMultiplier);
                                                   });
        assertThat(customParam).hasEntrySatisfying("INTERNODE_MSG_BYTES_REQUEST.Counter",
                                                   v -> {
                                                       double actual = SensorsCustomParams.sensorValueFromBytes(v);
                                                       assertThat(actual).isEqualTo(COUNTER_MUTATION_INTERNODE_BYTES * expectedSensorValueMultiplier);
                                                   });
        assertThat(customParam).hasEntrySatisfying("INTERNODE_MSG_BYTES_TABLE.Counter",
                                                   v -> {
                                                       double actual = SensorsCustomParams.sensorValueFromBytes(v);
                                                       assertThat(actual).isEqualTo(COUNTER_MUTATION_INTERNODE_BYTES * expectedSensorValueMultiplier);
                                                   });
    }
}
