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

import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.PaxosV2TestHelper;

import static org.apache.cassandra.db.SystemKeyspace.PAXOS;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the CAS v2 replica-side verb handlers ({@link org.apache.cassandra.service.paxos.PaxosPrepare.RequestHandler},
 * {@link org.apache.cassandra.service.paxos.PaxosPropose.RequestHandler},
 * {@link org.apache.cassandra.service.paxos.PaxosCommit.RequestHandler}) correctly populate
 * and propagate sensors to the global {@link SensorsRegistry}.
 *
 * <p>Message construction and dispatch use {@link PaxosV2TestHelper}, which lives in the
 * {@code org.apache.cassandra.service.paxos} package to access package-private constructors.
 *
 * @see ReplicaWriteSensorsTest for the CAS v1 counterpart
 */
public class PaxosV2SensorsTest
{
    private static final String KEYSPACE1 = "PaxosV2SensorsTest";
    private static final String CF_STANDARD = "Standard";

    private ColumnFamilyStore store;
    private CopyOnWriteArrayList<Message> capturedOutboundMessages;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null));
    }

    @Before
    public void beforeTest()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);

        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(store.metadata());

        // enable sensor registry for system.paxos so Paxos state reads/writes are tracked
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
        store.truncateBlocking();
        Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PAXOS).truncateBlocking();
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
    }

    // -------------------------------------------------------------------------
    // v2 Prepare (PaxosPrepare.RequestHandler)
    // -------------------------------------------------------------------------

    /**
     * v2 Prepare: WRITE_BYTES and WRITE_EXECUTION_TIME must be non-zero (Paxos promise written to system.paxos).
     * On the first prepare the in-memory cache is empty so READ_BYTES is zero.
     * After evicting the cache a second prepare reads from system.paxos and produces READ_BYTES > 0.
     * INTERNODE_BYTES must be non-zero.
     */
    @Test
    public void testV2Prepare()
    {
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        resetState();
        PaxosV2TestHelper.dispatchV2Prepare(store);

        // first prepare: no prior paxos state → READ_BYTES = 0, WRITE_BYTES > 0
        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor.getValue()).as("v2 Prepare: WRITE_BYTES must be > 0").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES))
                .as("v2 Prepare: registry WRITE_BYTES must equal request sensor").isEqualTo(writeSensor);

        Sensor readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).as("v2 Prepare: READ_BYTES must be 0 on first prepare (cache empty)").isZero();

        assertWriteExecutionTimeNonZero(context);
        assertInternodeBytesNonZero(context);
        assertResponseContainsSensorParams(Type.WRITE_BYTES, Type.WRITE_EXECUTION_TIME);

        // evict cache and re-prepare: now reads from system.paxos → READ_BYTES > 0
        PaxosState.unsafeReset();
        resetState();
        PaxosV2TestHelper.dispatchV2Prepare(store);

        readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).as("v2 Prepare: READ_BYTES must be > 0 after cache eviction").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES))
                .as("v2 Prepare: registry READ_BYTES must equal request sensor").isEqualTo(readSensor);
        assertResponseContainsSensorParams(Type.READ_BYTES, Type.WRITE_EXECUTION_TIME);
    }

    // -------------------------------------------------------------------------
    // v2 Propose (PaxosPropose.RequestHandler)
    // -------------------------------------------------------------------------

    /**
     * v2 Propose: WRITE_BYTES and WRITE_EXECUTION_TIME must be non-zero (Paxos proposal written to system.paxos).
     * On the first propose READ_BYTES is zero; after cache eviction READ_BYTES > 0.
     * INTERNODE_BYTES must be non-zero.
     */
    @Test
    public void testV2Propose()
    {
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        resetState();
        PaxosV2TestHelper.dispatchV2Propose(store);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor.getValue()).as("v2 Propose: WRITE_BYTES must be > 0").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES))
                .as("v2 Propose: registry WRITE_BYTES must equal request sensor").isEqualTo(writeSensor);

        Sensor readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).as("v2 Propose: READ_BYTES must be 0 on first propose (cache empty)").isZero();

        assertWriteExecutionTimeNonZero(context);
        assertInternodeBytesNonZero(context);
        assertResponseContainsSensorParams(Type.WRITE_BYTES, Type.WRITE_EXECUTION_TIME);

        // evict cache and re-propose: READ_BYTES now > 0
        PaxosState.unsafeReset();
        resetState();
        PaxosV2TestHelper.dispatchV2Propose(store);

        readSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.READ_BYTES);
        assertThat(readSensor.getValue()).as("v2 Propose: READ_BYTES must be > 0 after cache eviction").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.READ_BYTES))
                .as("v2 Propose: registry READ_BYTES must equal request sensor").isEqualTo(readSensor);
        assertResponseContainsSensorParams(Type.READ_BYTES, Type.WRITE_EXECUTION_TIME);
    }

    // -------------------------------------------------------------------------
    // v2 Commit (PaxosCommit.RequestHandler)
    // -------------------------------------------------------------------------

    /**
     * v2 Commit: WRITE_BYTES and WRITE_EXECUTION_TIME must be non-zero (base-table row committed).
     * INDEX_WRITE_BYTES is registered (value is 0 for a plain table without indexes).
     * INTERNODE_BYTES must be non-zero.
     * READ_BYTES is not registered in the Commit phase.
     */
    @Test
    public void testV2Commit()
    {
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        resetState();
        PaxosV2TestHelper.dispatchV2Commit(store);

        Sensor writeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
        assertThat(writeSensor.getValue()).as("v2 Commit: WRITE_BYTES must be > 0").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES))
                .as("v2 Commit: registry WRITE_BYTES must equal request sensor").isEqualTo(writeSensor);

        // INDEX_WRITE_BYTES is registered; value is 0 for a plain table without indexes
        assertThat(RequestTracker.instance.get().getSensor(context, Type.INDEX_WRITE_BYTES))
                .as("v2 Commit: INDEX_WRITE_BYTES sensor must be registered").isPresent();

        assertThat(RequestTracker.instance.get().getSensor(context, Type.READ_BYTES))
                .as("v2 Commit: READ_BYTES must not be registered").isEmpty();

        assertWriteExecutionTimeNonZero(context);
        assertInternodeBytesNonZero(context);
        assertResponseContainsSensorParams(Type.WRITE_BYTES, Type.WRITE_EXECUTION_TIME);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Resets per-invocation sensor/registry/message state without touching Paxos cache. */
    private void resetState()
    {
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(store.metadata());
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open("system").getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open("system").getColumnFamilyStore(PAXOS).metadata());
        capturedOutboundMessages.clear();
    }

    private void assertWriteExecutionTimeNonZero(Context context)
    {
        Sensor execTimeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_EXECUTION_TIME);
        assertThat(execTimeSensor.getValue()).as("WRITE_EXECUTION_TIME must be > 0").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.WRITE_EXECUTION_TIME))
                .as("registry WRITE_EXECUTION_TIME must equal request sensor").isEqualTo(execTimeSensor);
    }

    private void assertInternodeBytesNonZero(Context context)
    {
        Sensor internodeSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.INTERNODE_BYTES);
        assertThat(internodeSensor.getValue()).as("INTERNODE_BYTES must be > 0").isGreaterThan(0);
        assertThat(SensorsTestUtil.getRegistrySensor(context, Type.INTERNODE_BYTES))
                .as("registry INTERNODE_BYTES must equal request sensor").isEqualTo(internodeSensor);
    }

    /**
     * Asserts that the last captured outbound response message carries sensor custom params for the
     * given sensor types, and that those params survive a serialize/deserialize round-trip.
     */
    private void assertResponseContainsSensorParams(Type... types)
    {
        assertThat(capturedOutboundMessages).as("at least one outbound response message expected").isNotEmpty();
        Message message = capturedOutboundMessages.get(capturedOutboundMessages.size() - 1);
        assertThat(message.header.customParams()).isNotNull();

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();

        for (Type type : types)
        {
            sensors.getSensors(s -> s.getType() == type).forEach(sensor ->
            {
                SensorsCustomParams.paramForRequestSensor(sensor).ifPresent(param ->
                        assertThat(message.header.customParams())
                                .as("response must carry sensor param " + param)
                                .containsKey(param));
                SensorsCustomParams.paramForGlobalSensor(sensor).ifPresent(param ->
                        assertThat(message.header.customParams())
                                .as("response must carry global sensor param " + param)
                                .containsKey(param));
            });
        }

        // verify round-trip serialization preserves sensor params
        org.apache.cassandra.io.util.DataOutputBuffer buf = SensorsTestUtil.serialize(message);
        Message deserialized = SensorsTestUtil.deserialize(buf, message.from());
        for (Type type : types)
        {
            sensors.getSensors(s -> s.getType() == type).forEach(sensor ->
                    SensorsCustomParams.paramForRequestSensor(sensor).ifPresent(param ->
                            assertThat(deserialized.header.customParams())
                                    .as("deserialized response must carry sensor param " + param)
                                    .containsKey(param)));
        }
    }
}
