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

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link Type#READ_COST}, {@link Type#WRITE_COST}, and {@link Type#TOTAL_COST} sensors
 * are populated correctly after real CQL operations when {@link TestCostCalculator} is injected via
 * the {@link CassandraRelevantProperties#COST_CALCULATOR} system property.
 *
 * <p>{@link TestCostCalculator} uses a simple additive formula:
 * <pre>
 *   readCost  = read_bytes
 *   writeCost = write_bytes + index_write_bytes
 *   totalCost = sum(READ_COST) + sum(WRITE_COST)
 * </pre>
 * This lets tests assert exact cost values directly from the byte sensors.
 *
 * <p>Six code paths are covered:
 * <ul>
 *   <li><b>Range SELECT</b> — exercises the read path; asserts READ_COST = READ_BYTES and
 *       TOTAL_COST = READ_COST.</li>
 *   <li><b>Single-partition SELECT</b> — same assertions on the single-partition read path.</li>
 *   <li><b>INSERT</b> — exercises the standard write path; asserts WRITE_COST = WRITE_BYTES and
 *       TOTAL_COST = WRITE_COST.</li>
 *   <li><b>Unlogged batch</b> — same path as single INSERT via {@code StorageProxy.mutate()};
 *       asserts WRITE_COST = WRITE_BYTES and TOTAL_COST = WRITE_COST.</li>
 *   <li><b>Logged batch</b> — goes through {@code StorageProxy.mutateAtomically()};
 *       asserts WRITE_COST = WRITE_BYTES and TOTAL_COST = WRITE_COST.</li>
 *   <li><b>Counter update</b> — counter path via {@code StorageProxy.mutateCounter()};
 *       asserts WRITE_COST = WRITE_BYTES and TOTAL_COST = WRITE_COST.</li>
 *   <li><b>CAS (INSERT IF NOT EXISTS)</b> — exercises the Paxos path; asserts READ_COST = READ_BYTES,
 *       WRITE_COST = WRITE_BYTES, and TOTAL_COST = READ_COST + WRITE_COST.</li>
 * </ul>
 */
@RunWith(BMUnitRunner.class)
public class CoordinatorCostSensorsTest
{
    private static final String KEYSPACE = "coordinatorcosttest";
    private static final String TABLE = "tbl";
    private static final String TABLE_COUNTER = "tbl_counter";

    private ColumnFamilyStore store;
    private ColumnFamilyStore counterStore;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.COST_CALCULATOR.setString(TestCostCalculator.class.getName());

        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.counterCFMD(KEYSPACE, TABLE_COUNTER));
    }

    @Before
    public void before()
    {
        store = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        counterStore = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_COUNTER);
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(store.metadata());
        SensorsRegistry.instance.onCreateTable(counterStore.metadata());

        // Pre-load a row so reads return non-zero bytes
        new RowUpdateBuilder(store.metadata(), 0, "row0")
                .add("val", "value0")
                .build()
                .applyUnsafe();
    }

    @After
    public void after()
    {
        store.truncateBlocking();
        counterStore.truncateBlocking();
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
    }

    // ── Range SELECT ──────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsReadCostRangeRead()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        SelectStatement select = (SelectStatement) prepared.statement;
        select.execute(QueryState.forInternalCalls(), queryOptions(PageSize.NONE), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 (rows were read)").isGreaterThan(0.0);

        double readCost = sensors.getSensor(context, Type.READ_COST).get().getValue();
        assertThat(readCost).as("READ_COST must equal READ_BYTES").isEqualTo(readBytes);

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal READ_COST for a pure read").isEqualTo(readCost);

        // Cost sensors must also be reflected in the global registry
        assertThat(SensorsRegistry.instance.getSensor(context, Type.READ_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(readCost));
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(totalCost));
    }

    // ── Single-partition SELECT ────────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsReadCostSinglePartitionRead()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("SELECT * FROM %s.%s WHERE key = 'row0'", KEYSPACE, TABLE));
        SelectStatement select = (SelectStatement) prepared.statement;
        select.execute(QueryState.forInternalCalls(), queryOptions(PageSize.NONE), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 (row was read)").isGreaterThan(0.0);

        double readCost = sensors.getSensor(context, Type.READ_COST).get().getValue();
        assertThat(readCost).as("READ_COST must equal READ_BYTES").isEqualTo(readBytes);

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal READ_COST for a pure read").isEqualTo(readCost);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.READ_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(readCost));
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(totalCost));
    }

    // ── INSERT ────────────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsWriteCostSingleInsert()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("INSERT INTO %s.%s (key, val) VALUES ('k', 'v')", KEYSPACE, TABLE));
        UpdateStatement statement = (UpdateStatement) prepared.statement;
        statement.execute(QueryState.forInternalCalls(), queryOptions(null), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 (data was written)").isGreaterThan(0.0);

        double writeCost = sensors.getSensor(context, Type.WRITE_COST).get().getValue();
        assertThat(writeCost).as("WRITE_COST must equal WRITE_BYTES + INDEX_WRITE_BYTES")
                             .isEqualTo(writeBytes); // no secondary index, so index bytes = 0

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal WRITE_COST for a pure write").isEqualTo(writeCost);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.WRITE_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(writeCost));
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(totalCost));
    }

    // ── Unlogged batch INSERT ──────────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsWriteCostUnloggedBatch()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("BEGIN UNLOGGED BATCH" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k1', 'v1');" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k2', 'v2');" +
                              "APPLY BATCH", KEYSPACE, TABLE, KEYSPACE, TABLE));
        BatchStatement statement = (BatchStatement) prepared.statement;
        statement.execute(QueryState.forInternalCalls(), queryOptions(null), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 (data was written)").isGreaterThan(0.0);

        double writeCost = sensors.getSensor(context, Type.WRITE_COST).get().getValue();
        assertThat(writeCost).as("WRITE_COST must equal WRITE_BYTES for unlogged batch").isEqualTo(writeBytes);

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal WRITE_COST for a pure write").isEqualTo(writeCost);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.WRITE_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(writeCost));
    }

    // ── Logged batch INSERT ────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsWriteCostLoggedBatch()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("BEGIN BATCH" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k1', 'v1');" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k2', 'v2');" +
                              "APPLY BATCH", KEYSPACE, TABLE, KEYSPACE, TABLE));
        BatchStatement statement = (BatchStatement) prepared.statement;
        statement.execute(QueryState.forInternalCalls(), queryOptions(null), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 (data was written)").isGreaterThan(0.0);

        double writeCost = sensors.getSensor(context, Type.WRITE_COST).get().getValue();
        assertThat(writeCost).as("WRITE_COST must equal WRITE_BYTES for logged batch").isEqualTo(writeBytes);

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal WRITE_COST for a pure write").isEqualTo(writeCost);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.WRITE_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(writeCost));
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(totalCost));
    }

    // ── Counter update ────────────────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsWriteCostCounterUpdate()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("UPDATE %s.%s SET val = val + 1 WHERE key = 'k' AND name = 'n'",
                              KEYSPACE, TABLE_COUNTER));
        UpdateStatement statement = (UpdateStatement) prepared.statement;
        statement.execute(QueryState.forInternalCalls(), queryOptions(null), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(counterStore.metadata());

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for counter update (data was written)").isGreaterThan(0.0);

        double writeCost = sensors.getSensor(context, Type.WRITE_COST).get().getValue();
        assertThat(writeCost).as("WRITE_COST must equal WRITE_BYTES for counter update").isEqualTo(writeBytes);

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal WRITE_COST for a counter update").isEqualTo(writeCost);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.WRITE_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(writeCost));
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(totalCost));
    }

    // ── CAS (INSERT IF NOT EXISTS) ─────────────────────────────────────────────────

    @Test
    public void testCoordinatorAddsReadAndWriteCostCas()
    {
        // CAS registers both READ_COST and WRITE_COST sensors on the same context (see StorageProxy.cas),
        // so TOTAL_COST = READ_COST + WRITE_COST.
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("INSERT INTO %s.%s (key, val) VALUES ('cas_k', 'cas_v') IF NOT EXISTS", KEYSPACE, TABLE));
        UpdateStatement statement = (UpdateStatement) prepared.statement;
        statement.execute(QueryState.forInternalCalls(), casQueryOptions(), System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 for CAS (Paxos read phase)").isGreaterThan(0.0);

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for CAS (data was committed)").isGreaterThan(0.0);

        double readCost = sensors.getSensor(context, Type.READ_COST).get().getValue();
        assertThat(readCost).as("READ_COST must equal READ_BYTES").isEqualTo(readBytes);

        double writeCost = sensors.getSensor(context, Type.WRITE_COST).get().getValue();
        assertThat(writeCost).as("WRITE_COST must equal WRITE_BYTES").isEqualTo(writeBytes);

        double totalCost = sensors.getSensor(Context.request(), Type.TOTAL_COST).get().getValue();
        assertThat(totalCost).as("TOTAL_COST must equal READ_COST + WRITE_COST").isEqualTo(readCost + writeCost);

        assertThat(SensorsRegistry.instance.getSensor(context, Type.READ_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(readCost));
        assertThat(SensorsRegistry.instance.getSensor(context, Type.WRITE_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(writeCost));
        assertThat(SensorsRegistry.instance.getSensor(Context.request(), Type.TOTAL_COST))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getValue()).isEqualTo(totalCost));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────

    private static QueryOptions queryOptions(PageSize pageSize)
    {
        return QueryOptions.create(
                ConsistencyLevel.ONE,
                Collections.emptyList(),
                false,
                pageSize,
                null,
                null,
                ProtocolVersion.CURRENT,
                KEYSPACE);
    }

    private static QueryOptions casQueryOptions()
    {
        return QueryOptions.create(
                ConsistencyLevel.ONE,
                Collections.emptyList(),
                false,
                null,
                null,
                ConsistencyLevel.SERIAL,
                ProtocolVersion.CURRENT,
                KEYSPACE);
    }
}
