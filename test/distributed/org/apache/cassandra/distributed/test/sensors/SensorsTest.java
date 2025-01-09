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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

/**
 * Test to verify that the sensors are propagated via the native protocol in the custom payload respecting
 * the configuration set in {@link CassandraRelevantProperties#SENSORS_VIA_NATIVE_PROTOCOL}
 */
@RunWith(Parameterized.class)
public class SensorsTest extends TestBaseImpl
{
    private static final String EXPECTED_WRITE_BYTES_HEADER = "WRITE_BYTES_REQUEST." + KEYSPACE + ".tbl";
    private static final String EXPECTED_READ_BYTES_HEADER = "READ_BYTES_REQUEST." + KEYSPACE + ".tbl";
    /**
     * Using a combination of 2 nodes with ALL consistency level to ensure internode communication code paths are exercised in the test
     */
    private static final int NODES_COUNT = 2;
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ALL;

    /**
     * Schema to be used for the test
     */
    @Parameterized.Parameter(0)
    public String schema;
    /**
     * Queries to be executed to prepare the table, for example insert some data before read to populate read sensors.
     * Will be run before the {@link #testQuery}
     */
    @Parameterized.Parameter(1)
    public String[] prepQueries;

    /**
     * Query to be executed to test the sensors, will be run after the {@link #prepQueries}
     */
    @Parameterized.Parameter(2)
    public String testQuery;

    /**
     * Expected headers in the custom payload for the test queries
     */
    @Parameterized.Parameter(3)
    public String[] expectedHeaders;

    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
    }

    @Parameterized.Parameters(name = "schema={0}, prepQueries={1}, testQuery={2}, expectedHeaders={3}")
    public static Collection<Object[]> data()
    {
        String tableSchema = withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v1 text)");
        String counterTableSchema = withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, total counter)");

        String write = withKeyspace("INSERT INTO %s.tbl(pk, v1) VALUES (1, 'read me')");
        String counter = withKeyspace("UPDATE %s.tbl SET total = total + 1 WHERE pk = 1");
        String read = withKeyspace("SELECT * FROM %s.tbl WHERE pk=1");
        String cas = withKeyspace("UPDATE %s.tbl SET v1 = 'cas update' WHERE pk = 1 IF v1 = 'read me'");
        String loggedBatch = String.format("BEGIN BATCH\n" +
                                           "INSERT INTO %s.tbl(pk, v1) VALUES (2, 'read me 2');\n" +
                                           "INSERT INTO %s.tbl(pk, v1) VALUES (3, 'read me 3');\n" +
                                           "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String unloggedBatch = String.format("BEGIN UNLOGGED BATCH\n" +
                                             "INSERT INTO %s.tbl(pk, v1) VALUES (4, 'read me 2');\n" +
                                             "INSERT INTO %s.tbl(pk, v1) VALUES (4, 'read me 3');\n" +
                                             "APPLY BATCH;", KEYSPACE, KEYSPACE);
        String range = withKeyspace("SELECT * FROM %s.tbl");

        List<Object[]> result = new ArrayList<>();
        String[] noPrep = new String[0];
        result.add(new Object[]{ tableSchema, noPrep, write, new String[]{ EXPECTED_WRITE_BYTES_HEADER } });
        result.add(new Object[]{ counterTableSchema, noPrep, counter, new String[]{ EXPECTED_WRITE_BYTES_HEADER } });
        result.add(new Object[]{ tableSchema, new String[]{ write }, read, new String[]{ EXPECTED_READ_BYTES_HEADER } });
        // CAS requests incorporate read (and write) bytes from the paxos (and user) tables
        result.add(new Object[]{ tableSchema, noPrep, cas, new String[]{ EXPECTED_WRITE_BYTES_HEADER, EXPECTED_READ_BYTES_HEADER } });
        result.add(new Object[]{ tableSchema, noPrep, loggedBatch, new String[]{ EXPECTED_WRITE_BYTES_HEADER } });
        result.add(new Object[]{ tableSchema, noPrep, unloggedBatch, new String[]{ EXPECTED_WRITE_BYTES_HEADER } });
        result.add(new Object[]{ tableSchema, new String[]{ write }, range, new String[]{ EXPECTED_READ_BYTES_HEADER } });
        return result;
    }

    @Test
    public void testSensorsInCQLResponseEnabled() throws Throwable
    {
        Map<String, ByteBuffer> customPayload = executeTest(true);
        for (String header : expectedHeaders)
        {
            double requestBytes = getBytesForHeader(customPayload, header);
            Assertions.assertThat(requestBytes).isGreaterThan(0D);
        }
    }

    @Test
    public void testSensorsInCQLResponseDisabled() throws Throwable
    {
        Map<String, ByteBuffer> customPayload = executeTest(false);
        // customPayload will be null if it has no headers. However, non-sensor headers could've been added. So here we check for nullability or non-existence of sensor headers
        if (customPayload != null)
        {
            for (String header : expectedHeaders)
            {
                Assertions.assertThat(customPayload).doesNotContainKey(header);
            }
        } // else do nothing as null customPayload means no sensors were added
    }

    /**
     * Execute the test with the given {@code propagateViaNativeProtocol} flag and return the custom payload
     */
    private Map<String, ByteBuffer> executeTest(boolean propagateViaNativeProtocol) throws Throwable
    {
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(propagateViaNativeProtocol);
        AtomicReference<Map<String, ByteBuffer>> customPayload = new AtomicReference<>();
        try (Cluster cluster = init(Cluster.build(NODES_COUNT).start()))
        {
            cluster.schemaChange(schema);
            for (String prepQuery : this.prepQueries)
                cluster.coordinator(1).execute(prepQuery, ConsistencyLevel.ALL);
            // work around serializability of @Parameterized.Parameter by providing a locally scoped variable
            String query = this.testQuery;
            // Any methods used inside the runOnInstance() block should be static, otherwise java.io.NotSerializableException will be thrown
            cluster.get(1).acceptsOnInstance(
                   (IIsolatedExecutor.SerializableConsumer<AtomicReference<Map<String, ByteBuffer>>>)
                   (reference) -> reference.set(executeWithResult(query).getCustomPayload()))
                   .accept(customPayload);
        }

        return customPayload.get();
    }

    private double getBytesForHeader(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).containsKey(expectedHeader);
        return ByteBufferUtil.toDouble(customPayload.get(expectedHeader));
    }

    /**
     * TODO: update SimpleQueryResult in the dtest-api project to expose custom payload and use Coordinator##executeWithResult instead
     */
    private static ResultMessage<?> executeWithResult(String query)
    {
        long nanoTime = System.nanoTime();
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(CONSISTENCY_LEVEL.name());
        org.apache.cassandra.db.ConsistencyLevel cl = org.apache.cassandra.db.ConsistencyLevel.fromCode(consistencyLevel.ordinal());
        QueryOptions initialOptions = QueryOptions.create(cl,
                                                          null,
                                                          false,
                                                          PageSize.inRows(512),
                                                          null,
                                                          null,
                                                          ProtocolVersion.CURRENT,
                                                          prepared.keyspace);
        return prepared.statement.execute(QueryProcessor.internalQueryState(), initialOptions, nanoTime);
    }
}