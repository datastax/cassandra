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

package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.net.SensorsCustomParams;
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

public class TrackRequestSensorsTest extends TestBaseImpl
{

    @Test
    public void testTrackingReadSensor() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).start())
        {
            // resister a noop sensor listener to ensure that the registry singleton instance is subscribed to schema notifications
            cluster.coordinator(1).instance().sync(initSensorsRegistry());

            //TODO: remove this sleep, for now, without it, sometimes the onCreateKeyspace is not called before the actual query runs (init(cluster) is responsible foe creating the ks)
            Thread.sleep(10_000);

            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v1 text)"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl(pk, v1) VALUES (1, 'read me')"));
            cluster.get(1).flush(KEYSPACE);

            String query = withKeyspace("SELECT * FROM %s.tbl WHERE pk=1");

            // Any methods used inside the runOnInstance() block should be static, otherwise java.io.NotSerializableException will be thrown
            cluster.get(1).runOnInstance(() -> {
                ResultMessage.Rows initialRows = executeWithPagingWithResultMessage(query);
                Map<String, ByteBuffer> customPayload = initialRows.getCustomPayload();
                assertReadBytesHeadersExist(customPayload);

                double readBytesRequest = getReadBytesRequest(customPayload);
                double readBytesRate = getReadBytesRate(customPayload);
                Assertions.assertThat(readBytesRequest).isGreaterThan(0D);
                Assertions.assertThat(readBytesRate).isGreaterThan(0D);
                // TODO: either isolate the RegistrySensors instance or remove the next assertion
                //  For now, by the nature of distributed tests, writer and coordinator share the same jvm and with SensorsRegistry being a Singleton, READ_BYTES_REQUEST is double counted
                Assertions.assertThat(readBytesRate).isEqualTo(readBytesRequest * 2);

                // Read again. Assert the rate is every increasing (withing the same aggregation window)
                initialRows = executeWithPagingWithResultMessage(query);
                customPayload = initialRows.getCustomPayload();
                assertReadBytesHeadersExist(customPayload);
                double readBytesRate2 = getReadBytesRate(customPayload);
                Assertions.assertThat(readBytesRate2).isGreaterThan(readBytesRate);
            });
        }
    }

    private static void assertReadBytesHeadersExist(Map<String, ByteBuffer> customPayload)
    {
        Assertions.assertThat(customPayload).containsKey(SensorsCustomParams.READ_BYTES_REQUEST);
        Assertions.assertThat(customPayload).containsKey(SensorsCustomParams.READ_BYTES_RATE);
    }

    private static double getReadBytesRequest(Map<String, ByteBuffer> customPayload)
    {
        return ByteBufferUtil.toDouble(customPayload.get(SensorsCustomParams.READ_BYTES_REQUEST));
    }

    private static double getReadBytesRate(Map<String, ByteBuffer> customPayload)
    {
        return ByteBufferUtil.toDouble(customPayload.get(SensorsCustomParams.READ_BYTES_RATE));
    }

    /**
     * Registers a noop listener to ensure that the registry singleton instance is subscribed to schema notifications
     */
    public Runnable initSensorsRegistry()
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
}
