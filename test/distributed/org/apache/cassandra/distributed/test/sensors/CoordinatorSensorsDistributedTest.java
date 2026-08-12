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

package org.apache.cassandra.distributed.test.sensors;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed tests for coordinator-side sensor tracking in {@link SelectStatement#execute}.
 * <p>
 * {@link org.apache.cassandra.sensors.CoordinatorSensorsTrackingTest} covers the {@code executeInternal} path.
 * This class covers the distributed {@code execute} path, which routes through a real coordinator and replica
 * stack, exercising both:
 * <ul>
 *   <li><b>Non-paging path</b> — {@code canSkipPaging == true}: {@link PageSize#NONE} supplied, single-partition
 *   lookup. The coordinator calls the private {@code execute(ReadQuery, ...)} overload.</li>
 *   <li><b>Paging path</b> — {@code canSkipPaging == false}: {@link PageSize#inRows(1)} with multiple rows
 *   forces the {@code execute(Pager, ...)} overload.</li>
 * </ul>
 * Both paths must emit {@code READ_BYTES} and {@code READ_LATENCY_TIER} sensor values in the CQL response
 * custom payload.
 */
public class CoordinatorSensorsDistributedTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String EXPECTED_READ_BYTES_HEADER = "READ_BYTES_REQUEST." + KEYSPACE + "." + TABLE;
    private static final String EXPECTED_READ_LATENCY_TIER_HEADER = "READ_LATENCY_TIER_REQUEST." + KEYSPACE + "." + TABLE;

    /** Number of rows pre-loaded. A page size of 1 forces the paging path when all rows are requested. */
    private static final int NUM_ROWS = 5;

    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);
    }

    /**
     * Non-paging path: single-partition lookup with {@link PageSize#NONE}.
     * {@code canSkipPaging} returns {@code true} and the coordinator routes through the private
     * {@code execute(ReadQuery, ...)} overload. The response custom payload must contain both
     * {@code READ_BYTES} and {@code READ_LATENCY_TIER} headers with positive values.
     */
    @Test
    public void testSensorsEmittedOnNonPagingPath() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text)"));
            cluster.coordinator(1).execute(
                    withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (1, 'x')"),
                    org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);

            // Single-partition SELECT, PageSize.NONE → canSkipPaging == true → execute(ReadQuery, ...) branch
            String query = withKeyspace("SELECT * FROM %s." + TABLE + " WHERE pk = 1");
            AtomicReference<Map<String, ByteBuffer>> payloadRef = new AtomicReference<>();
            cluster.get(1).acceptsOnInstance(
                    (IIsolatedExecutor.SerializableConsumer<AtomicReference<Map<String, ByteBuffer>>>)
                    ref -> ref.set(executeWithPageSize(query, PageSize.NONE)))
                   .accept(payloadRef);

            Map<String, ByteBuffer> payload = payloadRef.get();
            assertThat(payload).as("custom payload must not be null").isNotNull();
            assertThat(payload).containsKey(EXPECTED_READ_BYTES_HEADER);
            assertThat(ByteBufferUtil.toDouble(payload.get(EXPECTED_READ_BYTES_HEADER)))
                    .as("READ_BYTES must be > 0 on non-paging path").isGreaterThan(0.0);
            assertThat(payload).containsKey(EXPECTED_READ_LATENCY_TIER_HEADER);
            assertThat(ByteBufferUtil.toDouble(payload.get(EXPECTED_READ_LATENCY_TIER_HEADER)))
                    .as("READ_LATENCY_TIER must be >= TIER_1 on non-paging path").isGreaterThanOrEqualTo(1.0);
        }
    }

    /**
     * Paging path: range scan with {@link PageSize#inRows(1)} and {@value #NUM_ROWS} rows.
     * {@code canSkipPaging} returns {@code false} and the coordinator routes through the private
     * {@code execute(Pager, ...)} overload. The response custom payload must contain both
     * {@code READ_BYTES} and {@code READ_LATENCY_TIER} headers with positive values.
     */
    @Test
    public void testSensorsEmittedOnPagingPath() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text)"));
            for (int i = 0; i < NUM_ROWS; i++)
                cluster.coordinator(1).execute(
                        withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (" + i + ", 'x')"),
                        org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);

            // Range SELECT, PageSize.inRows(1) → canSkipPaging == false → execute(Pager, ...) branch
            String query = withKeyspace("SELECT * FROM %s." + TABLE);
            AtomicReference<Map<String, ByteBuffer>> payloadRef = new AtomicReference<>();
            cluster.get(1).acceptsOnInstance(
                    (IIsolatedExecutor.SerializableConsumer<AtomicReference<Map<String, ByteBuffer>>>)
                    ref -> ref.set(executeWithPageSize(query, PageSize.inRows(1))))
                   .accept(payloadRef);

            Map<String, ByteBuffer> payload = payloadRef.get();
            assertThat(payload).as("custom payload must not be null").isNotNull();
            assertThat(payload).containsKey(EXPECTED_READ_BYTES_HEADER);
            assertThat(ByteBufferUtil.toDouble(payload.get(EXPECTED_READ_BYTES_HEADER)))
                    .as("READ_BYTES must be > 0 on paging path").isGreaterThan(0.0);
            assertThat(payload).containsKey(EXPECTED_READ_LATENCY_TIER_HEADER);
            assertThat(ByteBufferUtil.toDouble(payload.get(EXPECTED_READ_LATENCY_TIER_HEADER)))
                    .as("READ_LATENCY_TIER must be >= TIER_1 on paging path").isGreaterThanOrEqualTo(1.0);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers — must be static so they are serialisable for runOnInstance
    // -------------------------------------------------------------------------

    /**
     * Executes a SELECT via {@link SelectStatement#execute} with the given {@link PageSize}.
     * Pass {@link PageSize#NONE} to force {@code canSkipPaging == true} (non-paging branch), or
     * {@link PageSize#inRows(int)} to force {@code canSkipPaging == false} (paging branch).
     * Returns the CQL response custom payload.
     */
    private static Map<String, ByteBuffer> executeWithPageSize(String query, PageSize pageSize)
    {
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        QueryOptions options = QueryOptions.create(
                ConsistencyLevel.ONE,
                null,
                false,
                pageSize,
                null,
                null,
                ProtocolVersion.CURRENT,
                prepared.keyspace);
        return prepared.statement.execute(
                QueryProcessor.internalQueryState(), options, System.nanoTime()).getCustomPayload();
    }
}
