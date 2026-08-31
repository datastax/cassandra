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

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the coordinator correctly populates {@link Type#WRITE_EXECUTION_TIME} and
 * {@link Type#WRITE_BYTES} sensors and propagates them to the global {@link SensorsRegistry}.
 * <p>
 * All tests run with RF=1 and the coordinator as the only replica, so all writes execute on the
 * local replica path. {@link org.apache.cassandra.net.ResponseVerbHandler} and
 * {@link org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator} are not involved; execution
 * times are recorded directly via {@code incrementSensor} inside the local
 * execution methods.
 * <p>
 * {@link Type#WRITE_EXECUTION_TIME} is measured tightly around the actual write work:
 * {@link org.apache.cassandra.service.StorageProxy#performMutationLocally} wraps {@code mutation.apply()}
 * for standard writes; {@code commitPaxosLocal} wraps {@code PaxosState.commit()} for CAS; and
 * {@code counterWriteTask} wraps {@code applyCounterMutation()} for counter writes, accumulating the
 * leader apply time directly into the sensor.
 * {@link Type#WRITE_BYTES} is accumulated by {@link org.apache.cassandra.db.ColumnFamilyStore} during
 * the local memtable apply. Each test injects a Byteman sleep inside the actual apply method to assert
 * the execution-time sensor gains at least that much time, and also asserts byte sensors are non-zero.
 * <p>
 * Four paths are covered (CAS is disabled — see inline comment):
 * <ul>
 *   <li><b>Single-statement INSERT</b> — {@code performMutationLocally} wraps {@code Keyspace.apply}
 *       and accumulates the elapsed time. Verifies {@link Type#WRITE_EXECUTION_TIME} and
 *       {@link Type#WRITE_BYTES}.</li>
 *   <li><b>Unlogged batch</b> — same path as single INSERT via {@code StorageProxy.mutate()}.
 *       Verifies {@link Type#WRITE_EXECUTION_TIME} and {@link Type#WRITE_BYTES}.</li>
 *   <li><b>Logged batch</b> — same path via {@code StorageProxy.mutateAtomically()}.
 *       Verifies {@link Type#WRITE_EXECUTION_TIME} and {@link Type#WRITE_BYTES}.</li>
 *   <li><b>Counter update</b> — {@code counterWriteTask} wraps {@code CounterMutation.applyCounterMutation}
 *       and writes the leader apply time directly into the sensor via {@code incrementSensor}.
 *       Since coordinator == leader == only replica, there are no sub-replica ACKs.
 *       Verifies {@link Type#WRITE_EXECUTION_TIME} and {@link Type#WRITE_BYTES}.</li>
 * </ul>
 *
 * @see CoordinatorReadSensorsTest for the read-side counterpart
 */
@RunWith(BMUnitRunner.class)
public class CoordinatorWriteSensorsTest
{
    private static final String KEYSPACE = "coordinatorwriteexectimesensortest";
    private static final String TABLE = "tbl";
    private static final String TABLE_COUNTER = "tbl_counter";

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
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.counterCFMD(KEYSPACE, TABLE_COUNTER));
    }

    @Before
    public void before()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_COUNTER).metadata());
    }

    @After
    public void after()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_COUNTER).truncateBlocking();
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
    }

    // -------------------------------------------------------------------------
    // Single-statement INSERT  (UpdateStatement path)
    // -------------------------------------------------------------------------

    /**
     * Single-statement INSERT: Byteman sleeps 50 ms inside {@code Keyspace.apply}, which is inside
     * the timing window of {@code performMutationLocally}. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in Keyspace.apply to force write execution time >= 50ms (single INSERT)",
            targetClass = "org.apache.cassandra.db.Keyspace",
            targetMethod = "apply",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testSingleInsert()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("INSERT INTO %s.%s (key, val) VALUES ('k', 'v')", KEYSPACE, TABLE));
        UpdateStatement statement = (UpdateStatement) prepared.statement;

        QueryOptions options = queryOptions();
        statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata());

        double execTime = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("WRITE_EXECUTION_TIME must be >= 50ms for single INSERT").isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry WRITE_EXECUTION_TIME must equal request sensor").isEqualTo(execTime);

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for single INSERT (data was written)").isGreaterThan(0.0);
        Sensor registryWriteBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_BYTES).get();
        assertThat(registryWriteBytes.getValue()).as("registry WRITE_BYTES must equal request sensor").isEqualTo(writeBytes);
    }

    // -------------------------------------------------------------------------
    // Unlogged batch  (BatchStatement path → StorageProxy.mutate)
    // -------------------------------------------------------------------------

    /**
     * Unlogged batch: Byteman sleeps 50 ms inside {@code Keyspace.apply}, which is inside
     * the timing window of {@code performMutationLocally}. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in Keyspace.apply to force write execution time >= 50ms (unlogged batch)",
            targetClass = "org.apache.cassandra.db.Keyspace",
            targetMethod = "apply",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testUnloggedBatch()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("BEGIN UNLOGGED BATCH" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k1', 'v1');" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k2', 'v2');" +
                              "APPLY BATCH", KEYSPACE, TABLE, KEYSPACE, TABLE));
        BatchStatement statement = (BatchStatement) prepared.statement;

        QueryOptions options = queryOptions();
        statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata());

        double execTime = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("WRITE_EXECUTION_TIME must be >= 50ms for unlogged batch").isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry WRITE_EXECUTION_TIME must equal request sensor").isEqualTo(execTime);

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for unlogged batch (data was written)").isGreaterThan(0.0);
        Sensor registryWriteBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_BYTES).get();
        assertThat(registryWriteBytes.getValue()).as("registry WRITE_BYTES must equal request sensor").isEqualTo(writeBytes);
    }

    // -------------------------------------------------------------------------
    // Logged batch  (BatchStatement path → StorageProxy.mutateAtomically)
    // -------------------------------------------------------------------------

    /**
     * Logged batch: Byteman sleeps 50 ms inside {@code Keyspace.apply}, which is inside
     * the timing window of {@code performMutationLocally}. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in Keyspace.apply to force write execution time >= 50ms (logged batch)",
            targetClass = "org.apache.cassandra.db.Keyspace",
            targetMethod = "apply",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testLoggedBatch()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("BEGIN BATCH" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k1', 'v1');" +
                              "  INSERT INTO %s.%s (key, val) VALUES ('k2', 'v2');" +
                              "APPLY BATCH", KEYSPACE, TABLE, KEYSPACE, TABLE));
        BatchStatement statement = (BatchStatement) prepared.statement;

        QueryOptions options = queryOptions();
        statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata());

        double execTime = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("WRITE_EXECUTION_TIME must be >= 50ms for logged batch").isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry WRITE_EXECUTION_TIME must equal request sensor").isEqualTo(execTime);

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for logged batch (data was written)").isGreaterThan(0.0);
        Sensor registryWriteBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_BYTES).get();
        assertThat(registryWriteBytes.getValue()).as("registry WRITE_BYTES must equal request sensor").isEqualTo(writeBytes);
    }

    // -------------------------------------------------------------------------
    // CAS (INSERT IF NOT EXISTS)  (UpdateStatement path → StorageProxy.cas)
    // -------------------------------------------------------------------------

    /**
     * CAS write: Byteman sleeps 50 ms inside {@code PaxosState.commit}, which is the local commit
     * step executed for the paxos commit round. The commit execution time is accumulated via
     * {@link org.apache.cassandra.net.ResponseVerbHandler} into the coordinator's
     * {@link Type#WRITE_EXECUTION_TIME} sensor. The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in PaxosState.commit to force write execution time >= 50ms (CAS)",
            targetClass = "org.apache.cassandra.service.paxos.PaxosState",
            targetMethod = "commit",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCas()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("INSERT INTO %s.%s (key, val) VALUES ('k', 'v') IF NOT EXISTS", KEYSPACE, TABLE));
        UpdateStatement statement = (UpdateStatement) prepared.statement;

        QueryOptions options = casQueryOptions();
        statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata());

        double execTime = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("WRITE_EXECUTION_TIME must be >= 50ms for CAS").isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry WRITE_EXECUTION_TIME must equal request sensor").isEqualTo(execTime);

        // CAS writes include a Paxos read, so WRITE_BYTES tracks the committed data and READ_BYTES tracks the read phase
        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for CAS (data was written)").isGreaterThan(0.0);
        Sensor registryWriteBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_BYTES).get();
        assertThat(registryWriteBytes.getValue()).as("registry WRITE_BYTES must equal request sensor").isEqualTo(writeBytes);

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).get().getValue();
        assertThat(readBytes).as("READ_BYTES must be > 0 for CAS (Paxos read phase)").isGreaterThan(0.0);
        Sensor registryReadBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get();
        assertThat(registryReadBytes.getValue()).as("registry READ_BYTES must equal request sensor").isEqualTo(readBytes);
    }

    // -------------------------------------------------------------------------
    // Counter update  (UpdateStatement path → StorageProxy.mutateCounter)
    // -------------------------------------------------------------------------

    /**
     * Counter update: Byteman sleeps 50 ms inside {@code CounterMutation.applyCounterMutation}, which
     * is wrapped by the timing window in {@code counterWriteTask}. Since coordinator == leader == only
     * replica (RF=1), the elapsed time is written directly into the sensor via {@code incrementSensor}
     * before {@code responseHandler.onResponse(null)} — there are no sub-replica ACKs to accumulate.
     * The sensor must gain at least 50 ms.
     */
    @Test
    @BMRule(name = "sleep 50ms in CounterMutation.applyCounterMutation to force write execution time >= 50ms (counter)",
            targetClass = "org.apache.cassandra.db.CounterMutation",
            targetMethod = "applyCounterMutation",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCounterUpdate()
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(
                String.format("UPDATE %s.%s SET val = val + 1 WHERE key = 'k' AND name = 'n'", KEYSPACE, TABLE_COUNTER));
        UpdateStatement statement = (UpdateStatement) prepared.statement;

        QueryOptions options = queryOptions();
        statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());

        RequestSensors sensors = RequestTracker.instance.get();
        assertThat(sensors).isNotNull();
        Context context = Context.from(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_COUNTER).metadata());

        double execTime = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get().getValue();
        assertThat(execTime).as("WRITE_EXECUTION_TIME must be >= 50ms for counter update").isGreaterThanOrEqualTo(50_000_000.0);
        Sensor registryExecTime = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_EXECUTION_TIME).get();
        assertThat(registryExecTime.getValue()).as("registry WRITE_EXECUTION_TIME must equal request sensor").isEqualTo(execTime);

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).get().getValue();
        assertThat(writeBytes).as("WRITE_BYTES must be > 0 for counter update (data was written)").isGreaterThan(0.0);
        Sensor registryWriteBytes = SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_BYTES).get();
        assertThat(registryWriteBytes.getValue()).as("registry WRITE_BYTES must equal request sensor").isEqualTo(writeBytes);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static QueryOptions queryOptions()
    {
        return QueryOptions.create(
                ConsistencyLevel.ONE,
                Collections.emptyList(),
                false,
                null,
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
