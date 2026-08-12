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
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.ConsistencyLevel;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the coordinator folds its own execution time into {@link Type#READ_LATENCY_TIER} via {@code max},
 * so the CQL client receives the worst-case tier across the entire request path (replica + coordinator).
 * <p>
 * Each test is run twice: once via the non-paging path ({@code canSkipPaging == true}, goes through
 * {@code executeInternal} directly to {@code processResults}) and once via the paging path
 * ({@code canSkipPaging == false}, goes through {@code execute(Pager,...)}). Both paths must record
 * the coordinator's execution time and fold it into {@link Type#READ_LATENCY_TIER} via {@code max}.
 *
 * @see ReplicaSensorsTrackingTest for the replica-side counterpart
 */
@RunWith(BMUnitRunner.class)
public class CoordinatorSensorsTrackingTest
{
    private static final String KEYSPACE = "coordinatorsensorstrackingtest";
    private static final String TABLE = "tbl";

    /** Number of rows pre-loaded. A page size of 1 forces the paging path when all rows are requested. */
    private static final int NUM_ROWS = 5;

    private ColumnFamilyStore store;
    private RequestSensors requestSensors;
    private Context context;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());

        SchemaLoader.prepareServer();
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

        requestSensors = new ActiveRequestSensors();
        context = Context.from(store.metadata());
        requestSensors.registerSensor(context, Type.READ_LATENCY_TIER);

        ExecutorLocals.set(ExecutorLocals.create(requestSensors));
    }

    @After
    public void after()
    {
        store.truncateBlocking();
        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();
        ExecutorLocals.set(null);
    }

    // -------------------------------------------------------------------------
    // Non-paging path  (canSkipPaging == true, no PageSize supplied)
    // -------------------------------------------------------------------------

    /**
     * Non-paging path: Byteman injects a 50 ms sleep inside {@code processResults}, which is called
     * within the coordinator timing window in {@code executeInternal}'s fast path, making the elapsed
     * time exceed the {@link ReadLatencyTier.Bounds#MILLIS_10} boundary → at least {@link ReadLatencyTier#TIER_3}.
     * With the replica seeded at {@link ReadLatencyTier#TIER_1}, the coordinator tier must win and raise the sensor.
     */
    @Test
    @BMRule(name = "sleep 50ms in processResults to force coordinator tier >= TIER_3 (non-paging)",
            targetClass = "org.apache.cassandra.cql3.statements.SelectStatement",
            targetMethod = "processResults",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCoordinatorTierRaisesLatencyTierWhenHigherThanReplica()
    {
        // Seed with TIER_1 to simulate a fast replica response
        requestSensors.incrementSensor(context, Type.READ_LATENCY_TIER, ReadLatencyTier.TIER_1.value());

        QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE));

        double finalTier = requestSensors.getSensor(context, Type.READ_LATENCY_TIER).get().getValue();
        assertThat(finalTier).isGreaterThanOrEqualTo(ReadLatencyTier.TIER_3.value());
    }

    /**
     * Non-paging path: the coordinator runs in sub-millisecond time in a unit test environment,
     * always mapping to {@link ReadLatencyTier#TIER_1}. Seeding the sensor at {@link ReadLatencyTier#TIER_5}
     * and verifying it is unchanged after the query proves the {@code max} logic does not lower a tier
     * already set by a slower replica.
     */
    @Test
    public void testCoordinatorTierDoesNotLowerLatencyTierWhenReplicaWasSlower()
    {
        // Seed with TIER_5 to simulate a very slow replica response
        requestSensors.incrementSensor(context, Type.READ_LATENCY_TIER, ReadLatencyTier.TIER_5.value());

        QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE));

        double finalTier = requestSensors.getSensor(context, Type.READ_LATENCY_TIER).get().getValue();
        assertThat(finalTier).isEqualTo(ReadLatencyTier.TIER_5.value());
    }

    // -------------------------------------------------------------------------
    // Paging path  (canSkipPaging == false, PageSize.inRows(1) with NUM_ROWS rows)
    // -------------------------------------------------------------------------

    /**
     * Paging path: Byteman injects a 50 ms sleep inside {@code processResults}, which is called within
     * the coordinator timing window in {@code execute(Pager,...)}. Supplying {@code PageSize.inRows(1)}
     * with {@value #NUM_ROWS} rows in the table forces {@code canSkipPaging} to return {@code false}
     * and routes execution through the paging code path.
     * With the replica seeded at {@link ReadLatencyTier#TIER_1}, the coordinator tier must win.
     */
    @Test
    @BMRule(name = "sleep 50ms in processResults to force coordinator tier >= TIER_3 (paging)",
            targetClass = "org.apache.cassandra.cql3.statements.SelectStatement",
            targetMethod = "processResults",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(50L)")
    public void testCoordinatorTierRaisesLatencyTierWhenHigherThanReplicaWithPaging()
    {
        // Seed with TIER_1 to simulate a fast replica response
        requestSensors.incrementSensor(context, Type.READ_LATENCY_TIER, ReadLatencyTier.TIER_1.value());

        executeInternalWithPaging(PageSize.inRows(1));

        double finalTier = requestSensors.getSensor(context, Type.READ_LATENCY_TIER).get().getValue();
        assertThat(finalTier).isGreaterThanOrEqualTo(ReadLatencyTier.TIER_3.value());
    }

    /**
     * Paging path: the coordinator runs in sub-millisecond time in a unit test environment, always
     * mapping to {@link ReadLatencyTier#TIER_1}. Seeding the sensor at {@link ReadLatencyTier#TIER_5}
     * and verifying it is unchanged after the paged query proves the {@code max} logic does not lower
     * a tier already set by a slower replica on the paging code path.
     */
    @Test
    public void testCoordinatorTierDoesNotLowerLatencyTierWhenReplicaWasSlowerWithPaging()
    {
        // Seed with TIER_5 to simulate a very slow replica response
        requestSensors.incrementSensor(context, Type.READ_LATENCY_TIER, ReadLatencyTier.TIER_5.value());

        executeInternalWithPaging(PageSize.inRows(1));

        double finalTier = requestSensors.getSensor(context, Type.READ_LATENCY_TIER).get().getValue();
        assertThat(finalTier).isEqualTo(ReadLatencyTier.TIER_5.value());
    }

    /**
     * Invokes {@link SelectStatement#executeInternal} with the given {@code pageSize} so that
     * {@code canSkipPaging} returns {@code false} and the {@code execute(Pager,...)} branch is taken.
     */
    private void executeInternalWithPaging(PageSize pageSize)
    {
        String cql = String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE);
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(cql);
        SelectStatement select = (SelectStatement) prepared.statement;

        QueryOptions options = QueryOptions.create(
                ConsistencyLevel.ONE,
                Collections.emptyList(),
                false,
                pageSize,
                null,
                null,
                ProtocolVersion.CURRENT,
                KEYSPACE);

        int nowInSec = FBUtilities.nowInSeconds();
        select.executeInternal(QueryState.forInternalCalls(), options, nowInSec, System.nanoTime());
    }
}
