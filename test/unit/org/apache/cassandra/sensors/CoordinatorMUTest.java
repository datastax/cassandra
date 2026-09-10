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
import static org.junit.Assert.assertTrue;

/**
 * Tests that the coordinator correctly computes READ_COST, WRITE_COST, and TOTAL_COST from replica byte-sensors and
 * coordinator-measured latency, then exposes all three in the CQL custom payload.
 * READ_COST and WRITE_COST are keyed per-table; TOTAL_COST is keyed on {@link Context#request()} and
 * aggregates cost across all tables touched by the request.
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
        CassandraRelevantProperties.SENSORS_FACTORY.setString(TestSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);
    }

    @After
    public void tearDown()
    {
        SensorsRegistry.instance.clear();
    }

    // ── READ_COST ──────────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorReadCost_bytesDominate()
    {
        // read_bytes = 600 000, read_execution_time = 100 ms
        // normalized_bytes = 0.6, normalized_execution_time = 0.1 → READ_COST = 0.6 * 4000 = 2400
        String ks = "ks_rmu";
        String table = "t_rmu";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        coordinatorSensors.registerSensor(context, Type.READ_COST);

        // Simulate replica responses carrying READ_BYTES
        simulateReplicaReadBytes(context, ks, 400_000, coordinatorSensors);
        simulateReplicaReadBytes(context, ks, 200_000, coordinatorSensors);

        assertThat(coordinatorSensors.getSensor(context, Type.READ_BYTES).get().getValue()).isEqualTo(600_000.0);

        // Coordinator records 100 ms execution time and computes READ_COST
        coordinatorSensors.incrementSensor(context, Type.READ_EXECUTION_TIME, 0.1 * NANOS_PER_SECOND);
        CostCalculator calculator = new TestCostCalculator(BASELINE, BASELINE, 1);
        double rmuValue = calculator.computeReadCost(coordinatorSensors, context);
        coordinatorSensors.incrementSensor(context, Type.READ_COST, rmuValue);

        double expectedReadCost = (600_000.0 / BASELINE) * MU_SCALE; // bytes dominate
        assertThat(coordinatorSensors.getSensor(context, Type.READ_COST).get().getValue()).isEqualTo(expectedReadCost);

        // CQL response carries READ_COST
        ResultMessage result = new ResultMessage.Void();
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, coordinatorSensors, context, Type.READ_COST);

        Sensor rmuSensor = coordinatorSensors.getSensor(context, Type.READ_COST).get();
        String rmuHeader = SensorsCustomParams.paramForRequestSensor(rmuSensor).get();
        assertNotNull(result.getCustomPayload());
        assertTrue(result.getCustomPayload().containsKey(rmuHeader));
        assertThat(result.getCustomPayload().get(rmuHeader).getDouble()).isEqualTo(expectedReadCost);
    }

    @Test
    public void testCoordinatorReadCost_executionTimeDominates()
    {
        // read_bytes = 100 000, read_execution_time = 800 ms
        // normalized_bytes = 0.1, normalized_execution_time = 0.8 → READ_COST = 0.8 * 4000 = 3200
        String ks = "ks_rmu2";
        String table = "t_rmu2";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        coordinatorSensors.registerSensor(context, Type.READ_COST);

        simulateReplicaReadBytes(context, ks, 100_000, coordinatorSensors);

        coordinatorSensors.incrementSensor(context, Type.READ_EXECUTION_TIME, 0.8 * NANOS_PER_SECOND);
        CostCalculator calculator = new TestCostCalculator(BASELINE, BASELINE, 1);
        double rmuValue = calculator.computeReadCost(coordinatorSensors, context);
        coordinatorSensors.incrementSensor(context, Type.READ_COST, rmuValue);

        double expectedReadCost = 0.8 * MU_SCALE; // execution time dominates
        assertThat(coordinatorSensors.getSensor(context, Type.READ_COST).get().getValue()).isEqualTo(expectedReadCost);
    }

    // ── WRITE_COST ──────────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorWriteCost_bytesDominate()
    {
        // write_bytes = 700 000, write_execution_time = 200 ms
        // normalized_bytes = 0.7, normalized_execution_time = 0.2 → WRITE_COST = 0.7 * 4000 = 2800
        String ks = "ks_wmu";
        String table = "t_wmu";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.WRITE_BYTES);
        coordinatorSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        coordinatorSensors.registerSensor(context, Type.WRITE_COST);

        simulateReplicaWriteBytes(context, ks, 300_000, coordinatorSensors);
        simulateReplicaWriteBytes(context, ks, 400_000, coordinatorSensors);

        assertThat(coordinatorSensors.getSensor(context, Type.WRITE_BYTES).get().getValue()).isEqualTo(700_000.0);

        coordinatorSensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, 0.2 * NANOS_PER_SECOND);
        CostCalculator calculator = new TestCostCalculator(BASELINE, BASELINE, 1);
        double wmuValue = calculator.computeWriteCost(coordinatorSensors, context);
        coordinatorSensors.incrementSensor(context, Type.WRITE_COST, wmuValue);

        double expectedWriteCost = (700_000.0 / BASELINE) * MU_SCALE;
        assertThat(coordinatorSensors.getSensor(context, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCost);

        // CQL response carries WRITE_COST
        ResultMessage result = new ResultMessage.Void();
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, coordinatorSensors, context, Type.WRITE_COST);

        Sensor wmuSensor = coordinatorSensors.getSensor(context, Type.WRITE_COST).get();
        String wmuHeader = SensorsCustomParams.paramForRequestSensor(wmuSensor).get();
        assertNotNull(result.getCustomPayload());
        assertTrue(result.getCustomPayload().containsKey(wmuHeader));
        assertThat(result.getCustomPayload().get(wmuHeader).getDouble()).isEqualTo(expectedWriteCost);
    }

    @Test
    public void testCoordinatorWriteCost_executionTimeDominates()
    {
        // write_bytes = 50 000, write_execution_time = 900 ms
        // normalized_bytes = 0.05, normalized_execution_time = 0.9 → WRITE_COST = 0.9 * 4000 = 3600
        String ks = "ks_wmu2";
        String table = "t_wmu2";
        Context context = new Context(ks, table, UUID.randomUUID().toString());

        RequestSensors coordinatorSensors = SensorsFactory.instance.createRequestSensors(ks);
        coordinatorSensors.registerSensor(context, Type.WRITE_BYTES);
        coordinatorSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        coordinatorSensors.registerSensor(context, Type.WRITE_COST);

        simulateReplicaWriteBytes(context, ks, 50_000, coordinatorSensors);

        coordinatorSensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, 0.9 * NANOS_PER_SECOND);
        CostCalculator calculator = new TestCostCalculator(BASELINE, BASELINE, 1);
        double wmuValue = calculator.computeWriteCost(coordinatorSensors, context);
        coordinatorSensors.incrementSensor(context, Type.WRITE_COST, wmuValue);

        double expectedWriteCost = 0.9 * MU_SCALE;
        assertThat(coordinatorSensors.getSensor(context, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCost);
    }

    // ── computeReadCost / computeWriteCost via SensorsCustomParams ──────────────────────

    @Test
    public void testComputeReadCost_viaCustomParams()
    {
        String ks = "ks_params";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.READ_COST);
        sensors.incrementSensor(context, Type.READ_BYTES, 500_000);
        sensors.incrementSensor(context, Type.READ_EXECUTION_TIME, 0.1 * NANOS_PER_SECOND);

        // No baseline configured in system properties for unit tests → falls back to raw bytes * MU_SCALE
        // (TestCostCalculator reads from CassandraRelevantProperties which default to -1)
        CostCalculator.computeCost(sensors);

        // With baseline=-1, READ_COST = read_bytes * MU_SCALE
        double expectedReadCost = 500_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.READ_COST).get().getValue()).isEqualTo(expectedReadCost);
    }

    @Test
    public void testComputeWriteCost_viaCustomParams()
    {
        String ks = "ks_params2";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context, Type.WRITE_COST);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 200_000);
        sensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, 0.2 * NANOS_PER_SECOND);

        CostCalculator.computeCost(sensors);

        // With baseline=-1, WRITE_COST = write_bytes * MU_SCALE
        double expectedWriteCost = 200_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCost);
    }

    // ── TOTAL_COST ──────────────────────────────────────────────────────────────────

    @Test
    public void testCompute_readRequest_equalToReadCost()
    {
        // Pure read path: WRITE_COST sensor is not registered → TOTAL_COST = READ_COST
        // read_bytes = 500 000, no baseline → READ_COST = 500_000 * 4000; TOTAL_COST must equal READ_COST
        String ks = "ks_tmu_read";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.READ_COST);
        sensors.registerSensor(Context.request(), Type.TOTAL_COST);
        sensors.incrementSensor(context, Type.READ_BYTES, 500_000);

        CostCalculator.computeCost(sensors);

        double expectedReadCost = 500_000.0 * MU_SCALE; // no baseline → bytes * MU_SCALE
        assertThat(sensors.getSensor(context, Type.READ_COST).get().getValue()).isEqualTo(expectedReadCost);
        assertThat(sensors.getSensor(context, Type.WRITE_COST)).isEmpty();
        assertThat(sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue()).isEqualTo(expectedReadCost);
    }

    @Test
    public void testComputeTotalCost_writeRequest_equalToWriteCost()
    {
        // Pure write path: READ_COST sensor is not registered → TOTAL_COST = WRITE_COST
        // write_bytes = 300 000, no baseline → WRITE_COST = 300_000 * 4000; TOTAL_COST must equal WRITE_COST
        String ks = "ks_tmu_write";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context, Type.WRITE_COST);
        sensors.registerSensor(Context.request(), Type.TOTAL_COST);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 300_000);

        CostCalculator.computeCost(sensors);

        double expectedWriteCost = 300_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCost);
        assertThat(sensors.getSensor(context, Type.READ_COST)).isEmpty();
        assertThat(sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue()).isEqualTo(expectedWriteCost);
    }

    @Test
    public void testComputeTotalCost_casRequest_equalToWmuPlusRmu()
    {
        // CAS path: both READ_COST and WRITE_COST computed → TOTAL_COST = WRITE_COST + READ_COST
        // read_bytes = 400 000, write_bytes = 200 000, no baseline
        String ks = "ks_tmu_cas";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.WRITE_BYTES);
        sensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        sensors.registerSensor(context, Type.READ_COST);
        sensors.registerSensor(context, Type.WRITE_COST);
        sensors.registerSensor(Context.request(), Type.TOTAL_COST);
        sensors.incrementSensor(context, Type.READ_BYTES, 400_000);
        sensors.incrementSensor(context, Type.WRITE_BYTES, 200_000);

        CostCalculator.computeCost(sensors);

        double expectedReadCost = 400_000.0 * MU_SCALE;
        double expectedWriteCost = 200_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(context, Type.READ_COST).get().getValue()).isEqualTo(expectedReadCost);
        assertThat(sensors.getSensor(context, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCost);
        assertThat(sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue()).isEqualTo(expectedWriteCost + expectedReadCost);
    }

    @Test
    public void testComputeTotalCost_multipleTableContexts_sumsAcrossAllContexts()
    {
        // Multi-table batch write: two different table contexts in one RequestSensors.
        // TOTAL_COST on Context.request() must equal the sum of both tables' WRITE_COST values.
        String ks = "ks_tmu_multi";
        Context ctx1 = new Context(ks, "t1", UUID.randomUUID().toString());
        Context ctx2 = new Context(ks, "t2", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        // table 1: 300 000 write bytes
        sensors.registerSensor(ctx1, Type.WRITE_BYTES);
        sensors.registerSensor(ctx1, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(ctx1, Type.WRITE_COST);
        sensors.incrementSensor(ctx1, Type.WRITE_BYTES, 300_000);
        // table 2: 100 000 write bytes
        sensors.registerSensor(ctx2, Type.WRITE_BYTES);
        sensors.registerSensor(ctx2, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(ctx2, Type.WRITE_COST);
        sensors.incrementSensor(ctx2, Type.WRITE_BYTES, 100_000);
        // single request-level TOTAL_COST sensor
        sensors.registerSensor(Context.request(), Type.TOTAL_COST);

        CostCalculator.computeCost(sensors);

        double expectedWriteCostTable1 = 300_000.0 * MU_SCALE;
        double expectedWriteCostTable2 = 100_000.0 * MU_SCALE;
        assertThat(sensors.getSensor(ctx1, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCostTable1);
        assertThat(sensors.getSensor(ctx2, Type.WRITE_COST).get().getValue()).isEqualTo(expectedWriteCostTable2);
        // TOTAL_COST is a single sensor on Context.request(), aggregating both tables
        assertThat(sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue())
                .isEqualTo(expectedWriteCostTable1 + expectedWriteCostTable2);
    }

    @Test
    public void testComputeCost_onlyPopulatesRegisteredCostSensors()
    {
        // computeCost uses registered sensors as gates: it only increments cost sensor types
        // that were explicitly registered, leaving unregistered ones absent.

        // ── read-only: READ_COST registered, TOTAL_COST not registered → request-level TOTAL_COST absent ──
        String ks1 = "ks_gate_r";
        Context ctx1 = new Context(ks1, "t", UUID.randomUUID().toString());
        RequestSensors readSensors = SensorsFactory.instance.createRequestSensors(ks1);
        readSensors.registerSensor(ctx1, Type.READ_BYTES);
        readSensors.registerSensor(ctx1, Type.READ_EXECUTION_TIME);
        readSensors.registerSensor(ctx1, Type.READ_COST);
        readSensors.incrementSensor(ctx1, Type.READ_BYTES, 100_000);
        CostCalculator.computeCost(readSensors);

        assertThat(readSensors.getSensor(ctx1, Type.READ_COST)).isPresent();
        assertThat(readSensors.getSensor(ctx1, Type.WRITE_COST)).isEmpty();
        assertThat(readSensors.getSensor(Context.request(), Type.TOTAL_COST)).isEmpty();

        // ── write-only: WRITE_COST registered, TOTAL_COST not registered → request-level TOTAL_COST absent ──
        String ks2 = "ks_gate_w";
        Context ctx2 = new Context(ks2, "t", UUID.randomUUID().toString());
        RequestSensors writeSensors = SensorsFactory.instance.createRequestSensors(ks2);
        writeSensors.registerSensor(ctx2, Type.WRITE_BYTES);
        writeSensors.registerSensor(ctx2, Type.WRITE_EXECUTION_TIME);
        writeSensors.registerSensor(ctx2, Type.WRITE_COST);
        writeSensors.incrementSensor(ctx2, Type.WRITE_BYTES, 200_000);
        CostCalculator.computeCost(writeSensors);

        assertThat(writeSensors.getSensor(ctx2, Type.READ_COST)).isEmpty();
        assertThat(writeSensors.getSensor(ctx2, Type.WRITE_COST)).isPresent();
        assertThat(writeSensors.getSensor(Context.request(), Type.TOTAL_COST)).isEmpty();

        // ── all three registered: all three populated ────────────────────────────────────
        String ks3 = "ks_gate_all";
        Context ctx3 = new Context(ks3, "t", UUID.randomUUID().toString());
        RequestSensors allSensors = SensorsFactory.instance.createRequestSensors(ks3);
        allSensors.registerSensor(ctx3, Type.READ_BYTES);
        allSensors.registerSensor(ctx3, Type.READ_EXECUTION_TIME);
        allSensors.registerSensor(ctx3, Type.WRITE_BYTES);
        allSensors.registerSensor(ctx3, Type.WRITE_EXECUTION_TIME);
        allSensors.registerSensor(ctx3, Type.READ_COST);
        allSensors.registerSensor(ctx3, Type.WRITE_COST);
        allSensors.registerSensor(Context.request(), Type.TOTAL_COST);
        allSensors.incrementSensor(ctx3, Type.READ_BYTES, 100_000);
        allSensors.incrementSensor(ctx3, Type.WRITE_BYTES, 50_000);
        CostCalculator.computeCost(allSensors);

        assertThat(allSensors.getSensor(ctx3, Type.READ_COST).get().getValue()).isGreaterThan(0);
        assertThat(allSensors.getSensor(ctx3, Type.WRITE_COST).get().getValue()).isGreaterThan(0);
        assertThat(allSensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue()).isGreaterThan(0);
    }

    // ── computeCost syncs to global registry ─────────────────────────────────────────────────

    @Test
    public void testComputeCost_syncsReadCostToGlobalRegistry()
    {
        // computeCost must sync READ_COST into the global registry by itself.
        String ks = "ks_sync_r";
        String table = "t_sync_r";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context ctx = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(ctx, Type.READ_BYTES);
        sensors.registerSensor(ctx, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(ctx, Type.READ_COST);
        sensors.incrementSensor(ctx, Type.READ_BYTES, 600_000);
        CostCalculator.computeCost(sensors); // no syncAllSensors() call after this

        double expectedRead = 600_000.0 * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(ctx, Type.READ_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedRead));
    }

    @Test
    public void testComputeCost_syncsWriteCostToGlobalRegistry()
    {
        // computeCost must sync WRITE_COST into the global registry by itself.
        String ks = "ks_sync_w";
        String table = "t_sync_w";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context ctx = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(ctx, Type.WRITE_BYTES);
        sensors.registerSensor(ctx, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(ctx, Type.WRITE_COST);
        sensors.incrementSensor(ctx, Type.WRITE_BYTES, 250_000);
        CostCalculator.computeCost(sensors); // no syncAllSensors() call after this

        double expectedWrite = 250_000.0 * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(ctx, Type.WRITE_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedWrite));
    }

    @Test
    public void testComputeCost_syncsTotalCostToGlobalRegistry()
    {
        // computeCost must sync TOTAL_COST into the global registry by itself.
        // TOTAL_COST is keyed on the singleton Context.request() — independent of which table
        // was queried — so the registry assertion also uses Context.request().
        String ks = "ks_sync_cas";
        String table = "t_sync_cas";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context ctx = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(ctx, Type.READ_BYTES);
        sensors.registerSensor(ctx, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(ctx, Type.WRITE_BYTES);
        sensors.registerSensor(ctx, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(ctx, Type.INDEX_WRITE_BYTES);
        sensors.registerSensor(ctx, Type.READ_COST);
        sensors.registerSensor(ctx, Type.WRITE_COST);
        sensors.registerSensor(Context.request(), Type.TOTAL_COST);
        sensors.incrementSensor(ctx, Type.READ_BYTES, 200_000);
        sensors.incrementSensor(ctx, Type.WRITE_BYTES, 100_000);
        CostCalculator.computeCost(sensors); // no syncAllSensors() call after this

        double expectedTotal = (200_000.0 + 100_000.0) * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedTotal));
    }

    @Test
    public void testComputeCost_doesNotSyncWhenNoCostSensorsRegistered()
    {
        // When no cost sensors are registered, computeCost must not sync anything into
        // the global registry — the registry must remain empty for the context.
        String ks = "ks_sync_none";
        String table = "t_sync_none";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context context = new Context(ks, table, tableId);

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.incrementSensor(context, Type.READ_BYTES, 100_000);
        CostCalculator.computeCost(sensors);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.READ_COST)).isEmpty();
        assertThat(SensorsRegistry.instance.getSensor(context, Type.WRITE_COST)).isEmpty();
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST)).isEmpty();
    }

    @Test
    public void testComputeCost_accumulatesAcrossRequests()
    {
        // Two back-to-back requests each contribute; the global sensor accumulates both
        // via the sync performed inside computeCost.
        String ks = "ks_sync_accum";
        String table = "t_sync_accum";
        String tableId = UUID.randomUUID().toString();
        registerSchemaInRegistry(ks, table, tableId);
        Context context = new Context(ks, table, tableId);

        for (int i = 0; i < 2; i++)
        {
            RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
            sensors.registerSensor(context, Type.READ_BYTES);
            sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
            sensors.registerSensor(context, Type.READ_COST);
            sensors.registerSensor(Context.request(), Type.TOTAL_COST);
            sensors.incrementSensor(context, Type.READ_BYTES, 100_000);
            CostCalculator.computeCost(sensors); // no syncAllSensors() call after this
        }

        double expectedGlobalTotalCost = 2 * 100_000.0 * MU_SCALE;
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(expectedGlobalTotalCost));
    }

    // ── TOTAL_COST in CQL response ──────────────────────────────────────────────────

    @Test
    public void testTotalCostIsAddedToCQLResponse()
    {
        // TOTAL_COST is sent in the CQL response as a single request-level entry keyed on Context.request().
        // The wire key is TOTAL_COST_REQUEST (no table suffix).
        String ks = "ks_tmu_cql";
        Context context = new Context(ks, "t", UUID.randomUUID().toString());

        RequestSensors sensors = SensorsFactory.instance.createRequestSensors(ks);
        sensors.registerSensor(context, Type.READ_BYTES);
        sensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        sensors.registerSensor(context, Type.READ_COST);
        sensors.registerSensor(Context.request(), Type.TOTAL_COST);
        sensors.incrementSensor(context, Type.READ_BYTES, 500_000);
        sensors.incrementSensor(context, Type.READ_EXECUTION_TIME, 0.1 * NANOS_PER_SECOND);
        CostCalculator.computeCost(sensors);

        ResultMessage result = new ResultMessage.Void();
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, sensors, context, Type.READ_COST);
        SensorsCustomParams.addSensorToCQLResponse(result, ProtocolVersion.V4, sensors, Context.request(), Type.TOTAL_COST);

        assertNotNull(result.getCustomPayload());

        Sensor totalCostSensor = sensors.getSensor(Context.request(), Type.TOTAL_COST).get();
        String totalCostKey = SensorsCustomParams.paramForRequestSensor(totalCostSensor).get();
        assertThat(totalCostKey).isEqualTo("TOTAL_COST_REQUEST");
        assertThat(result.getCustomPayload()).containsKey(totalCostKey);
        double expectedTotalCost = 500_000.0 * MU_SCALE;
        assertThat(result.getCustomPayload().get(totalCostKey).getDouble()).isEqualTo(expectedTotalCost);
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
