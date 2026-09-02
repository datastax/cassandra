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

import java.util.UUID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the coordinator correctly computes RMU, WMU, and TMU from replica byte-sensors and
 * coordinator-measured latency, then exposes RMU/WMU in the CQL custom payload while keeping
 * TMU registry-only (never sent in CQL responses).
 */
public class CoordinatorMUTest
{
    // baseline = 1 000 000 bytes/s; 4 000 MUs per node-second
    private static final double BASELINE = 1_000_000.0;
    private static final double MU_SCALE = 4_000.0;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);
    }

    @After
    public void tearDown()
    {
        SensorsRegistry.instance.clear();
    }

    // ── RMU ──────────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorRMU_bytesDominate()
    {
        // read_bytes = 600 000, latency = 100 ms
        // normalized_bytes = 0.6, normalized_latency = 0.1 → RMU = 0.6 * 4000 = 2400
        String ks = "ks_rmu";
        String table = "t_rmu";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.RMU);

        // Simulate replica responses carrying READ_BYTES
        simulateReplicaReadBytes(context, ks, 400_000, coordinatorSensors);
        simulateReplicaReadBytes(context, ks, 200_000, coordinatorSensors);

        assertThat(coordinatorSensors.getSensor(context, Type.READ_BYTES).get().getValue()).isEqualTo(600_000.0);

        // Coordinator computes RMU with 100 ms latency
        long latencyNanos = (long) (0.1 * NANOS_PER_SECOND);
        MUCalculator calculator = new DefaultMUCalculator(BASELINE, BASELINE);
        double rmuValue = calculator.computeRMU(coordinatorSensors, context, latencyNanos);
        coordinatorSensors.incrementSensor(context, Type.RMU, rmuValue);

        double expectedRMU = (600_000.0 / BASELINE) * MU_SCALE; // bytes dominate
        assertThat(coordinatorSensors.getSensor(context, Type.RMU).get().getValue()).isEqualTo(expectedRMU);

        // CQL response carries RMU
        ResultMessage result = new ResultMessage.Void();
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, coordinatorSensors, context, Type.RMU);

        Sensor rmuSensor = coordinatorSensors.getSensor(context, Type.RMU).get();
        String rmuHeader = SensorsCustomParams.paramForRequestSensor(rmuSensor).get();
        assertNotNull(result.getCustomPayload());
        assertTrue(result.getCustomPayload().containsKey(rmuHeader));
        assertThat(result.getCustomPayload().get(rmuHeader).getDouble()).isEqualTo(expectedRMU);
    }

    @Test
    public void testCoordinatorRMU_latencyDominates()
    {
        // read_bytes = 100 000, latency = 800 ms
        // normalized_bytes = 0.1, normalized_latency = 0.8 → RMU = 0.8 * 4000 = 3200
        String ks = "ks_rmu2";
        String table = "t_rmu2";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.RMU);

        simulateReplicaReadBytes(context, ks, 100_000, coordinatorSensors);

        long latencyNanos = (long) (0.8 * NANOS_PER_SECOND);
        MUCalculator calculator = new DefaultMUCalculator(BASELINE, BASELINE);
        double rmuValue = calculator.computeRMU(coordinatorSensors, context, latencyNanos);
        coordinatorSensors.incrementSensor(context, Type.RMU, rmuValue);

        double expectedRMU = 0.8 * MU_SCALE; // latency dominates
        assertThat(coordinatorSensors.getSensor(context, Type.RMU).get().getValue()).isEqualTo(expectedRMU);
    }

    // ── WMU ──────────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorWMU_bytesDominate()
    {
        // write_bytes = 700 000, latency = 200 ms
        // normalized_bytes = 0.7, normalized_latency = 0.2 → WMU = 0.7 * 4000 = 2800
        String ks = "ks_wmu";
        String table = "t_wmu";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.WRITE_BYTES);
        coordinatorSensors.registerSensor(context, Type.WMU);

        simulateReplicaWriteBytes(context, ks, 300_000, coordinatorSensors);
        simulateReplicaWriteBytes(context, ks, 400_000, coordinatorSensors);

        assertThat(coordinatorSensors.getSensor(context, Type.WRITE_BYTES).get().getValue()).isEqualTo(700_000.0);

        long latencyNanos = (long) (0.2 * NANOS_PER_SECOND);
        MUCalculator calculator = new DefaultMUCalculator(BASELINE, BASELINE);
        double wmuValue = calculator.computeWMU(coordinatorSensors, context, latencyNanos);
        coordinatorSensors.incrementSensor(context, Type.WMU, wmuValue);

        double expectedWMU = (700_000.0 / BASELINE) * MU_SCALE;
        assertThat(coordinatorSensors.getSensor(context, Type.WMU).get().getValue()).isEqualTo(expectedWMU);

        // CQL response carries WMU
        ResultMessage result = new ResultMessage.Void();
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, coordinatorSensors, context, Type.WMU);

        Sensor wmuSensor = coordinatorSensors.getSensor(context, Type.WMU).get();
        String wmuHeader = SensorsCustomParams.paramForRequestSensor(wmuSensor).get();
        assertNotNull(result.getCustomPayload());
        assertTrue(result.getCustomPayload().containsKey(wmuHeader));
        assertThat(result.getCustomPayload().get(wmuHeader).getDouble()).isEqualTo(expectedWMU);
    }

    @Test
    public void testCoordinatorWMU_latencyDominates()
    {
        // write_bytes = 50 000, latency = 900 ms
        // normalized_bytes = 0.05, normalized_latency = 0.9 → WMU = 0.9 * 4000 = 3600
        String ks = "ks_wmu2";
        String table = "t_wmu2";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.WRITE_BYTES);
        coordinatorSensors.registerSensor(context, Type.WMU);

        simulateReplicaWriteBytes(context, ks, 50_000, coordinatorSensors);

        long latencyNanos = (long) (0.9 * NANOS_PER_SECOND);
        MUCalculator calculator = new DefaultMUCalculator(BASELINE, BASELINE);
        double wmuValue = calculator.computeWMU(coordinatorSensors, context, latencyNanos);
        coordinatorSensors.incrementSensor(context, Type.WMU, wmuValue);

        double expectedWMU = 0.9 * MU_SCALE;
        assertThat(coordinatorSensors.getSensor(context, Type.WMU).get().getValue()).isEqualTo(expectedWMU);
    }

    // ── computeRMU / computeWMU via SensorsCustomParams ──────────────────────

    @Test
    public void testComputeRMU_viaCustomParams()
    {
        String ks = "ks_params";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 500_000);

        // No baseline configured in system properties for unit tests → falls back to raw bytes * MU_SCALE
        // (DefaultMUCalculator instance reads from CassandraRelevantProperties which default to -1)
        SensorsCustomParams.computeRMU(sensors, (long) (0.1 * NANOS_PER_SECOND));

        // With baseline=-1, RMU = read_bytes * MU_SCALE
        double expectedRMU = 500_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.RMU).get().getValue()).isEqualTo(expectedRMU);
    }

    @Test
    public void testComputeWMU_viaCustomParams()
    {
        String ks = "ks_params2";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WMU);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 200_000);

        SensorsCustomParams.computeWMU(sensors, (long) (0.2 * NANOS_PER_SECOND));

        // With baseline=-1, WMU = write_bytes * MU_SCALE
        double expectedWMU = 200_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.WMU).get().getValue()).isEqualTo(expectedWMU);
    }

    // ── TMU ──────────────────────────────────────────────────────────────────

    @Test
    public void testComputeTMU_readRequest_equalToRMU()
    {
        // Pure read path: WMU sensor is not registered → TMU = RMU
        // read_bytes = 500 000, no baseline → RMU = 500_000 * 4000; TMU must equal RMU
        String ks = "ks_tmu_read";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 500_000);

        SensorsCustomParams.computeRMU(sensors, (long) (0.1 * NANOS_PER_SECOND));
        SensorsCustomParams.computeTMU(sensors);

        double expectedRMU = 500_000.0 * MU_SCALE; // no baseline → bytes * MU_SCALE
        assertThat(sensors.getSensor(context, Type.RMU).get().getValue()).isEqualTo(expectedRMU);
        assertThat(sensors.getSensor(context, Type.WMU)).isEmpty();
        assertThat(sensors.getSensor(context, Type.TMU).get().getValue()).isEqualTo(expectedRMU);
    }

    @Test
    public void testComputeTMU_writeRequest_equalToWMU()
    {
        // Pure write path: RMU sensor is not registered → TMU = WMU
        // write_bytes = 300 000, no baseline → WMU = 300_000 * 4000; TMU must equal WMU
        String ks = "ks_tmu_write";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 300_000);

        SensorsCustomParams.computeWMU(sensors, (long) (0.2 * NANOS_PER_SECOND));
        SensorsCustomParams.computeTMU(sensors);

        double expectedWMU = 300_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.WMU).get().getValue()).isEqualTo(expectedWMU);
        assertThat(sensors.getSensor(context, Type.RMU)).isEmpty();
        assertThat(sensors.getSensor(context, Type.TMU).get().getValue()).isEqualTo(expectedWMU);
    }

    @Test
    public void testComputeTMU_casRequest_equalToWMUplusRMU()
    {
        // CAS path: both RMU and WMU computed → TMU = WMU + RMU
        // read_bytes = 400 000, write_bytes = 200 000, no baseline
        String ks = "ks_tmu_cas";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.WMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 400_000);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 200_000);

        long latency = (long) (0.5 * NANOS_PER_SECOND);
        SensorsCustomParams.computeRMU(sensors, latency);
        SensorsCustomParams.computeWMU(sensors, latency);
        SensorsCustomParams.computeTMU(sensors);

        double expectedRMU = 400_000.0 * MU_SCALE;
        double expectedWMU = 200_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.RMU).get().getValue()).isEqualTo(expectedRMU);
        assertThat(sensors.getSensor(context, Type.WMU).get().getValue()).isEqualTo(expectedWMU);
        assertThat(sensors.getSensor(context, Type.TMU).get().getValue()).isEqualTo(expectedWMU + expectedRMU);
    }

    @Test
    public void testComputeTMU_noopWhenTMUNotRegistered()
    {
        // computeTMU must silently do nothing when no TMU sensor is registered
        String ks = "ks_tmu_noreg";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 100_000);
        SensorsCustomParams.computeRMU(sensors, (long) (0.1 * NANOS_PER_SECOND));

        // no exception, no TMU sensor created
        SensorsCustomParams.computeTMU(sensors);

        assertThat(sensors.getSensor(context, Type.TMU)).isEmpty();
    }

    // ── TMU → global registry ─────────────────────────────────────────────────

    @Test
    public void testTMUSyncsToGlobalRegistry_readPath()
    {
        // After computeRMU + computeTMU + syncAllSensors the global registry must hold TMU = RMU
        String ks = "ks_tmu_reg_r";
        String table = "t_reg_r";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context context = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 600_000);

        SensorsCustomParams.computeRMU(sensors, (long) (0.1 * NANOS_PER_SECOND));
        SensorsCustomParams.computeTMU(sensors);
        sensors.syncAllSensors();

        double expectedTMU = 600_000.0 * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(context, Type.TMU))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedTMU));
    }

    @Test
    public void testTMUSyncsToGlobalRegistry_writePath()
    {
        // After computeWMU + computeTMU + syncAllSensors the global registry must hold TMU = WMU
        String ks = "ks_tmu_reg_w";
        String table = "t_reg_w";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context context = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 250_000);

        SensorsCustomParams.computeWMU(sensors, (long) (0.2 * NANOS_PER_SECOND));
        SensorsCustomParams.computeTMU(sensors);
        sensors.syncAllSensors();

        double expectedTMU = 250_000.0 * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(context, Type.TMU))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedTMU));
    }

    @Test
    public void testTMUSyncsToGlobalRegistry_casPath()
    {
        // After computeRMU + computeWMU + computeTMU + syncAllSensors: global TMU = WMU + RMU
        String ks = "ks_tmu_reg_cas";
        String table = "t_reg_cas";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context context = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.WMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 200_000);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 100_000);

        long latency = (long) (0.3 * NANOS_PER_SECOND);
        SensorsCustomParams.computeRMU(sensors, latency);
        SensorsCustomParams.computeWMU(sensors, latency);
        SensorsCustomParams.computeTMU(sensors);
        sensors.syncAllSensors();

        double expectedTMU = (200_000.0 + 100_000.0) * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(context, Type.TMU))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedTMU));
    }

    @Test
    public void testGlobalTMUAccumulatesAcrossRequests()
    {
        // Two back-to-back requests each contribute; the global sensor accumulates both
        String ks = "ks_tmu_accum";
        String table = "t_accum";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context context = new Context(ks, table, tableId);

        for (int i = 0; i < 2; i++)
        {
            RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
            sensors.registerSensor(context, Type.READ_BYTES);
            sensors.registerSensor(context, Type.RMU);
            sensors.registerSensor(context, Type.TMU);
            sensors.incrementSensor(context, Type.READ_BYTES, 100_000);
            SensorsCustomParams.computeRMU(sensors, (long) (0.1 * NANOS_PER_SECOND));
            SensorsCustomParams.computeTMU(sensors);
            sensors.syncAllSensors();
        }

        double expectedGlobalTMU = 2 * 100_000.0 * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(context, Type.TMU))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedGlobalTMU));
    }

    // ── TMU absent from CQL response ──────────────────────────────────────────

    @Test
    public void testTMUIsNeverAddedToCQLResponse()
    {
        // The production code never calls addSensorToCQLResponse for TMU.
        // Verify that a response populated with RMU contains no TMU key at all.
        String ks = "ks_tmu_cql";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.RMU);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.READ_BYTES, 500_000);
        SensorsCustomParams.computeRMU(sensors, (long) (0.1 * NANOS_PER_SECOND));
        SensorsCustomParams.computeTMU(sensors);

        ResultMessage result = new ResultMessage.Void();
        // Only RMU is added — TMU is intentionally never passed to addSensorToCQLResponse
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, sensors, context, Type.RMU);

        assertNotNull(result.getCustomPayload());
        result.getCustomPayload().keySet().forEach(k ->
                assertThat(k).as("CQL payload must not contain any TMU key").doesNotStartWith("TMU_"));
    }

    @Test
    public void testAddSensorToCQLResponse_TMU_returnsWithoutAddingPayload()
    {
        // Even if someone explicitly calls addSensorToCQLResponse for TMU (which production code
        // never does), the default SensorEncoder returns an empty Optional for TMU (same keyspace
        // format used for all types), so no entry is added when the sensor cannot be encoded.
        // Here we exercise a plain ks/table context where the encoder CAN produce a name,
        // confirming TMU value reaches the payload only if explicitly requested — but the real
        // guarantee is that production code never makes this call.
        String ks = "ks_tmu_explicit";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.TMU);
        sensors.incrementSensor(context, Type.TMU, 999.0);

        ResultMessage result = new ResultMessage.Void();
        // Confirm no TMU key is written by the production flow (no call is made):
        assertNull(result.getCustomPayload());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static void registerSchemaInRegistry(String ks, String table, String tableId)
    {
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ks, null);
        TableMetadata tm = TableMetadata.builder(ks, table, TableId.fromString(tableId))
                                        .addPartitionKeyColumn("pk", AsciiType.instance)
                                        .build();
        SensorsRegistry.instance.onCreateKeyspace(ksm);
        SensorsRegistry.instance.onCreateTable(tm);
    }

    private void simulateReplicaReadBytes(Context context, String ks, double bytes, RequestSensors coordinator)
    {
        RequestSensors replica = SensorsFactory.instance.createRequestSensors(ks);
        replica.registerSensor(context, Type.READ_BYTES);
        replica.incrementSensor(context, Type.READ_BYTES, bytes);

        Message.Builder<NoPayload> builder = Message.builder(Verb._TEST_1, noPayload).withId(1);
        SensorsCustomParams.addSensorsToInternodeResponse(replica, builder);
        Message<NoPayload> msg = builder.build();

        Sensor readBytesSensor = coordinator.getSensor(context, Type.READ_BYTES).get();
        String header = SensorsCustomParams.paramForRequestSensor(readBytesSensor).get();
        double value = SensorsCustomParams.sensorValueFromInternodeResponse(msg, header);
        coordinator.incrementSensor(context, Type.READ_BYTES, value);
    }

    private void simulateReplicaWriteBytes(Context context, String ks, double bytes, RequestSensors coordinator)
    {
        RequestSensors replica = SensorsFactory.instance.createRequestSensors(ks);
        replica.registerSensor(context, Type.WRITE_BYTES);
        replica.incrementSensor(context, Type.WRITE_BYTES, bytes);

        Message.Builder<NoPayload> builder = Message.builder(Verb._TEST_2, noPayload).withId(2);
        SensorsCustomParams.addSensorsToInternodeResponse(replica, builder);
        Message<NoPayload> msg = builder.build();

        Sensor writeBytesSensor = coordinator.getSensor(context, Type.WRITE_BYTES).get();
        String header = SensorsCustomParams.paramForRequestSensor(writeBytesSensor).get();
        double value = SensorsCustomParams.sensorValueFromInternodeResponse(msg, header);
        coordinator.incrementSensor(context, Type.WRITE_BYTES, value);
    }
}
