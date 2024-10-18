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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.sensors.ActiveRequestSensorsFactory;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.SensorsRegistryListener;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

public class SensorsTest extends TestBaseImpl
{
    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.REQUEST_SENSORS_FACTORY.setString(ActiveRequestSensorsFactory.class.getName());
    }
    @Test
    public void testSensorsInResultMessage() throws Throwable
    {
        CassandraRelevantProperties.PROPAGATE_REQUEST_SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);
        try (Cluster cluster = builder().withNodes(1).start())
        {
            // resister a noop sensor listener before init(cluster) which creates the test keyspace to ensure that the registry singleton instance is subscribed to schema notifications
            cluster.get(1).runsOnInstance(initSensorsRegistry()).run();
            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v1 text)"));
            String write = withKeyspace("INSERT INTO %s.tbl(pk, v1) VALUES (1, 'read me')");
            String query = withKeyspace("SELECT * FROM %s.tbl WHERE pk=1");

            // Any methods used inside the runOnInstance() block should be static, otherwise java.io.NotSerializableException will be thrown
            cluster.get(1).runOnInstance(() -> {
                ResultMessage writeResult = executeWithResult(write);
                Map<String, ByteBuffer> customPayload = writeResult.getCustomPayload();

                String expectedWriteHeader = "WRITE_BYTES_REQUEST.tbl";
                double writeBytesRequest = getWriteBytesRequest(customPayload, expectedWriteHeader);
                Assertions.assertThat(writeBytesRequest).isGreaterThan(0D);

                ResultMessage.Rows readResult = executeWithPagingWithResultMessage(query);
                customPayload = readResult.getCustomPayload();
                String expectedReadHeader = "READ_BYTES_REQUEST.tbl";
                assertReadBytesHeadersExist(customPayload, expectedReadHeader);

                double readBytesRequest = getReadBytesRequest(customPayload, expectedReadHeader);
                Assertions.assertThat(readBytesRequest).isGreaterThan(0D);
            });
        }
    }

    private static void assertReadBytesHeadersExist(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).containsKey(expectedHeader);
    }

    private static double getReadBytesRequest(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).containsKey(expectedHeader);
        return ByteBufferUtil.toDouble(customPayload.get(expectedHeader));
    }

    private static double getWriteBytesRequest(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).containsKey(expectedHeader);
        return ByteBufferUtil.toDouble(customPayload.get(expectedHeader));
    }

    /**
     * Registers a noop listener to ensure that the registry singleton instance is subscribed to schema notifications
     */
    private static IIsolatedExecutor.SerializableRunnable initSensorsRegistry()
    {
        return () ->
               SensorsRegistry.instance.registerListener(new SensorsRegistryListener()
               {
                   @Override
                   public void onSensorCreated(Sensor sensor)
                   {
                   }

                   @Override
                   public void onSensorRemoved(Sensor sensor)
                   {
                   }
               });
    }

    /**
     * Adapted from org.apache.cassandra.distributed.impl.Coordinator#executeWithPagingWithResult(java.lang.String, org.apache.cassandra.distributed.api.ConsistencyLevel, int, java.lang.Object...)
     * TODO: update the dtest-api project to expose this method and hide the implementation in Coordinator
     */
    private static ResultMessage.Rows executeWithPagingWithResultMessage(String query)
    {
        QueryState state = new QueryState(ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042)));
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(ConsistencyLevel.ALL.name());
        CQLStatement prepared = QueryProcessor.getStatement(query, state.getClientState());
        final List<ByteBuffer> boundBBValues = new ArrayList<>();

        prepared.validate(state);
        assert prepared instanceof SelectStatement : "Only SELECT statements can be executed with paging";

        long nanoTime = System.nanoTime();
        SelectStatement selectStatement = (SelectStatement) prepared;
        org.apache.cassandra.db.ConsistencyLevel cl = org.apache.cassandra.db.ConsistencyLevel.fromCode(consistencyLevel.ordinal());
        QueryOptions initialOptions = QueryOptions.create(cl,
                                                          boundBBValues,
                                                          false,
                                                          PageSize.inRows(512),
                                                          null,
                                                          null,
                                                          ProtocolVersion.CURRENT,
                                                          selectStatement.keyspace());

        return selectStatement.execute(state, initialOptions, nanoTime);
    }

    /**
     * TODO: update SimpleQueryResult in the dtest-api project to expose custom payload and use Coordinator##executeWithResult instead
     */
    private static ResultMessage executeWithResult(String query, Object... args)
    {
        long nanoTime = System.nanoTime();
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(ConsistencyLevel.ALL.name());
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