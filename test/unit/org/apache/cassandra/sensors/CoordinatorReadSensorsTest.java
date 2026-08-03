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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the coordinator correctly populates {@link Type#READ_EXECUTION_TIME} and
 * {@link Type#READ_BYTES} sensors and propagates them to the global {@link SensorsRegistry}.
 * <p>
 * All tests run with RF=1 and the coordinator as the only replica, so all reads execute on the
 * local replica path. {@link org.apache.cassandra.service.StorageProxy.LocalReadRunnable} submits
 * the read to {@link org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator} via
 * {@code accumulateExecutionTimeSensor}, which fires into the request sensors when the threshold
 * of {@code blockFor()} responses is reached.
 * <p>
 * {@link Type#READ_EXECUTION_TIME} is measured tightly around the actual read work:
 * {@link org.apache.cassandra.service.StorageProxy.LocalReadRunnable} wraps
 * {@code ReadCommand.executeLocally()} and feeds the elapsed time into the handler's accumulator.
 * {@link Type#READ_BYTES} is accumulated by
 * {@link org.apache.cassandra.sensors.read.TrackingRowIterator} during row iteration.
 * Each test injects a Byteman sleep inside {@code ReadCommand.executeLocally} to assert
 * the execution-time sensor gains at least that much time, and also asserts byte sensors are non-zero.
 * <p>
 * Four paths are covered:
 * <ul>
 *   <li><b>Range read, non-paging</b> — {@code SELECT *} with no page size routes through
 *       {@link org.apache.cassandra.db.PartitionRangeReadCommand} →
 *       {@code StorageProxy.getRangeSlice} → {@code LocalReadRunnable}, taking the
 *       {@code canSkipPaging == true} fast path. Verifies {@link Type#READ_EXECUTION_TIME} and
 *       {@link Type#READ_BYTES}.</li>
 *   <li><b>Range read, paging</b> — {@code SELECT *} with {@code PageSize.inRows(1)} forces the
 *       {@code execute(Pager,...)} paging path, still routing through
 *       {@code StorageProxy.getRangeSlice} → {@code LocalReadRunnable}.
 *       Verifies {@link Type#READ_EXECUTION_TIME} and {@link Type#READ_BYTES}.</li>
 *   <li><b>Single-partition read, non-paging</b> — {@code SELECT * WHERE key = ?} routes through
 *       {@link org.apache.cassandra.db.SinglePartitionReadCommand} →
 *       {@code StorageProxy.read} → {@code LocalReadRunnable}, taking the
 *       {@code canSkipPaging == true} fast path. Verifies {@link Type#READ_EXECUTION_TIME} and
 *       {@link Type#READ_BYTES}.</li>
 *   <li><b>Single-partition read, paging</b> — {@code SELECT * WHERE key = ?} with
 *       {@code PageSize.inRows(1)} forces the {@code execute(Pager,...)} paging path, routing through
 *       {@code StorageProxy.read} → {@code LocalReadRunnable}.
 *       Verifies {@link Type#READ_EXECUTION_TIME} and {@link Type#READ_BYTES}.</li>
 * </ul>
 *
 * @see CoordinatorWriteSensorsTest for the write-side counterpart
 * @see ReplicaSensorsTrackingTest for the replica-side counterpart
 */
@RunWith(BMUnitRunner.class)
public class CoordinatorReadSensorsTest
{
    private static final String KEYSPACE = "coordinatorsensorstrackingtest";
    private static final String TABLE = "tbl";

    /** Number of rows pre-loaded. A page size of 1 forces the paging path when all rows are requested. */
    private static final int NUM_ROWS = 5;

    private ColumnFamilyStore store;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE,
                                                              1, AsciiType.instance, AsciiType.instance, null));
    }

    @Before
    public void before()
    {
        store = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(store.metadata());

        for (int i = 0; i < NUM_ROWS; i++)
            new RowUpdateBuilder(store.metadata(), i, String.valueOf(i))
            .add("val", String.valueOf(i))
            .build()
            .applyUnsafe();
    }

    @After
    public void after()
    {
        store.truncateBlocking();
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
    }

    // -------------------------------------------------------------------------
    // Non-paging path  (canSkipPaging == true, no PageSize supplied)
    // -------------------------------------------------------------------------

    /**
     * Range read, non-paging path: Byteman sleeps 50 ms inside {@code ReadCommand.executeLocally},
     * which is inside the timing window of {@code LocalReadRunnable.runMayThrow}. The sensor must
     * gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in ReadCommand.executeLocally to force read execution time >= 50ms (range, non-paging)",
            targetClass = "org.apache.cassandra.db.ReadCommand",
            targetMethod = "executeLocally",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCoordinatorAddsExecutionTimeNonPaging()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        SelectStatement select = (SelectStatement) prepared.statement;

        QueryOptions options = queryOptions(PageSize.NONE);
        select.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double execTime = sensors.getSensor(context, Type.READ_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("READ_EXECUTION_TIME must be >= 50ms for range non-paging path")
                            .isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry READ_EXECUTION_TIME must equal request sensor")
                                               .isEqualTo(execTime);

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 for range non-paging path (rows were read)")
                             .isGreaterThan(0.0);
        Sensor registryReadBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get();
        assertThat(registryReadBytes.getValue()).as("registry READ_BYTES must equal request sensor")
                                                .isEqualTo(readBytes);
    }

    // -------------------------------------------------------------------------
    // Paging path  (canSkipPaging == false, PageSize.inRows(1) with NUM_ROWS rows)
    // -------------------------------------------------------------------------

    /**
     * Range read, paging path: Byteman sleeps 50 ms inside {@code ReadCommand.executeLocally},
     * which is inside the timing window of {@code LocalReadRunnable.runMayThrow}. Supplying
     * {@code PageSize.inRows(1)} with {@value #NUM_ROWS} rows forces the paging code path through
     * {@code execute(Pager,...)}. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in ReadCommand.executeLocally to force read execution time >= 50ms (range, paging)",
            targetClass = "org.apache.cassandra.db.ReadCommand",
            targetMethod = "executeLocally",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCoordinatorAddsExecutionTimeWithPaging()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        SelectStatement select = (SelectStatement) prepared.statement;

        QueryOptions options = queryOptions(PageSize.inRows(1));
        select.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double execTime = sensors.getSensor(context, Type.READ_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("READ_EXECUTION_TIME must be >= 50ms for range paging path")
                            .isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry READ_EXECUTION_TIME must equal request sensor")
                                               .isEqualTo(execTime);

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 for range paging path (rows were read)")
                             .isGreaterThan(0.0);
        Sensor registryReadBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get();
        assertThat(registryReadBytes.getValue()).as("registry READ_BYTES must equal request sensor")
                                                .isEqualTo(readBytes);
    }

    // -------------------------------------------------------------------------
    // Single-partition read, non-paging path  (SinglePartitionReadCommand → StorageProxy.read)
    // -------------------------------------------------------------------------

    /**
     * Single-partition read, non-paging path: Byteman sleeps 50 ms inside
     * {@code ReadCommand.executeLocally}, which is inside the timing window of
     * {@code LocalReadRunnable.runMayThrow}. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in ReadCommand.executeLocally to force read execution time >= 50ms (single-partition, non-paging)",
            targetClass = "org.apache.cassandra.db.ReadCommand",
            targetMethod = "executeLocally",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCoordinatorAddsExecutionTimeSinglePartitionNonPaging()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("SELECT * FROM %s.%s WHERE key = ?", KEYSPACE, TABLE));
        SelectStatement select = (SelectStatement) prepared.statement;

        QueryOptions options = queryOptionsWithValues(PageSize.NONE, "0");
        select.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double execTime = sensors.getSensor(context, Type.READ_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("READ_EXECUTION_TIME must be >= 50ms for single-partition non-paging path")
                            .isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry READ_EXECUTION_TIME must equal request sensor")
                                               .isEqualTo(execTime);

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 for single-partition non-paging path (rows were read)")
                             .isGreaterThan(0.0);
        Sensor registryReadBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get();
        assertThat(registryReadBytes.getValue()).as("registry READ_BYTES must equal request sensor")
                                                .isEqualTo(readBytes);
    }

    // -------------------------------------------------------------------------
    // Single-partition read, paging path  (SinglePartitionReadCommand → StorageProxy.read)
    // -------------------------------------------------------------------------

    /**
     * Single-partition read, paging path: Byteman sleeps 50 ms inside
     * {@code ReadCommand.executeLocally}, which is inside the timing window of
     * {@code LocalReadRunnable.runMayThrow}. Supplying {@code PageSize.inRows(1)} forces the
     * {@code execute(Pager,...)} paging path. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in ReadCommand.executeLocally to force read execution time >= 50ms (single-partition, paging)",
            targetClass = "org.apache.cassandra.db.ReadCommand",
            targetMethod = "executeLocally",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCoordinatorAddsExecutionTimeSinglePartitionWithPaging()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("SELECT * FROM %s.%s WHERE key = ?", KEYSPACE, TABLE));
        SelectStatement select = (SelectStatement) prepared.statement;

        QueryOptions options = queryOptionsWithValues(PageSize.inRows(1), "0");
        select.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(store.metadata());

        double execTime = sensors.getSensor(context, Type.READ_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("READ_EXECUTION_TIME must be >= 50ms for single-partition paging path")
                            .isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry READ_EXECUTION_TIME must equal request sensor")
                                               .isEqualTo(execTime);

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 for single-partition paging path (rows were read)")
                             .isGreaterThan(0.0);
        Sensor registryReadBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get();
        assertThat(registryReadBytes.getValue()).as("registry READ_BYTES must equal request sensor")
                                                .isEqualTo(readBytes);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    private static QueryOptions queryOptionsWithValues(PageSize pageSize, String... values)
    {
        List<ByteBuffer> boundValues = new ArrayList<>();
        for (String v : values)
            boundValues.add(AsciiType.instance.fromString(v));
        return QueryOptions.create(
                ConsistencyLevel.ONE,
                boundValues,
                false,
                pageSize,
                null,
                null,
                ProtocolVersion.CURRENT,
                KEYSPACE);
    }
}
