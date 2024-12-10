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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.ActiveSensorsFactory;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.SensorsRegistryListener;
import org.apache.cassandra.sensors.Type;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class CoordinatorSensorsTest extends TestBaseImpl
{
    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
    }

    @Test
    public void testCoordinatorSensors() throws Throwable
    {
        int nodesCount = 2;
        int replicationFactor = 1;
        try (Cluster cluster = Cluster.build(nodesCount).start())
        {
            // a workaround to ensure sensors registry is initialized before creating the keyspace and table
            cluster.get(2).runsOnInstance(initSensorsRegistry()).run();
            init(cluster, replicationFactor);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v1 text)"));

            int numRows = 10;
            double prevRequestMemorySensorValue = 0;
            double prevGlobalMemorySensorValue = 0;
            int pk = 0;
            String v1 = "read me";
            for (int i = 0; i < numRows; i++)
            {
                // generate PK owned by node 1
                pk = generatePKForNode(cluster.get(1), cluster.get(2), pk);
                cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl(pk, v1) VALUES (?, ?)"), ConsistencyLevel.ONE, pk, v1);
                // query from node 2 to force a read from node 1
                SimpleQueryResult result = cluster.coordinator(2).executeWithResult(withKeyspace("SELECT * FROM %s.tbl WHERE pk=?"), ConsistencyLevel.ONE, pk);
                Object[][] newRows = result.toObjectArrays();
                assertRows(newRows, row(pk, v1));
                // double the number of bytes to assert memory is indeed a function of row size
                v1 += v1;
                double requestMemorySensorValue = cluster.get(2).callOnInstance(() -> {
                    TableMetadata table = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata();
                    Context context = Context.from(table);
                    return RequestTracker.instance.get().getSensor(context, Type.MEMORY_BYTES).get().getValue();
                });
                double globalMemorySensorValue = cluster.get(2).callOnInstance(() -> {
                    {
                        TableMetadata table = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata();
                        Context context = Context.from(table);
                        return SensorsRegistry.instance.getSensor(context, Type.MEMORY_BYTES).get().getValue();
                    }
                });
                Assertions.assertThat(requestMemorySensorValue).isGreaterThan(prevRequestMemorySensorValue);
                Assertions.assertThat(globalMemorySensorValue).isGreaterThan(prevGlobalMemorySensorValue);
                prevRequestMemorySensorValue = requestMemorySensorValue;
                prevGlobalMemorySensorValue = globalMemorySensorValue;
            }
        }
    }

    /**
     * Generates a PK that falls within the token range of node1 and different from the previous PK
     */
    private static int generatePKForNode(IInstance node1, IInstance node2, int previousPK)
    {
        Token token1 = Murmur3Partitioner.instance.getTokenFactory().fromString(node1.config().getString("initial_token"));
        Token token2 = Murmur3Partitioner.instance.getTokenFactory().fromString(node2.config().getString("initial_token"));

        int pk = previousPK + 1;
        Token pkToken;
        while (token1.compareTo(pkToken = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk))) < 0 ||
               token2.compareTo(pkToken) <= 0)
        {
            pk++;
        }
        return pk;
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
}
