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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.UUIDGen;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code WRITE_BYTES} is tracked when a {@code system_traces.events} mutation is
 * applied on the replica side, exercising the same code path that runs after a coordinator sends
 * a {@code MUTATION_REQ} for a trace write.
 *
 * <p>The complementary coordinator-level property — that trace writes do not inflate the user
 * table's {@code WRITE_BYTES} reported to the client — is covered by
 * {@code SensorsTest#testTraceWriteDoesNotInflateSensors()} in the distributed test suite.
 */
public class SensorsTraceWriteTest
{
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        SchemaLoader.prepareServer();
        // system_traces is a distributed system keyspace not loaded by SchemaLoader.prepareServer(),
        // so register it explicitly so Keyspace.open(TRACE_KEYSPACE_NAME) works in the test.
        Schema.instance.transform(SchemaTransformations.addKeyspace(TraceKeyspace.metadata(), true));
    }

    @After
    public void afterTest()
    {
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
    }

    /**
     * Verifies that {@code WRITE_BYTES} is incremented when a {@code system_traces.events} mutation
     * is applied via {@link MutationVerbHandler} — the replica-side path that executes after a
     * coordinator sends a {@code MUTATION_REQ} message.
     */
    @Test
    public void testWriteBytesIncrementedForTraceMutation()
    {
        TableMetadata events = Keyspace.open(SchemaConstants.TRACE_KEYSPACE_NAME)
                                       .getColumnFamilyStore("events")
                                       .metadata();
        Context context = new Context(SchemaConstants.TRACE_KEYSPACE_NAME, "events", events.id.toString());

        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(SchemaConstants.TRACE_KEYSPACE_NAME).getMetadata());
        SensorsRegistry.instance.onCreateTable(events);

        Mutation mutation = new RowUpdateBuilder(events, 0, UUIDGen.getTimeUUID())
                            .clustering(UUIDGen.getTimeUUID())
                            .add("activity", "test trace event")
                            .build();

        // Drive the write on the current thread via MutationVerbHandler — same replica-side path
        // that executes after a coordinator sends a MUTATION_REQ.
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors)
                .as("RequestSensors must be set on the thread-local after MutationVerbHandler.doVerb()")
                .isNotNull();
        assertThat(sensors.getSensor(context, Type.WRITE_BYTES))
                .as("WRITE_BYTES must be registered for system_traces.events")
                .isPresent();
        assertThat(sensors.getSensor(context, Type.WRITE_BYTES).get().getValue())
                .as("WRITE_BYTES must be > 0 after the trace mutation is applied")
                .isGreaterThan(0);
    }
}
