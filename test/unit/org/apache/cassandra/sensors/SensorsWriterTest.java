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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.BloomFilter;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorsWriterTest
{
    public static final String KEYSPACE1 = "SensorsWrireTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD_CLUSTERING = "StandardClustering";

    private ColumnFamilyStore store;
    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_CLUSTERING,
                                                              1, AsciiType.instance, AsciiType.instance, AsciiType.instance));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).metadata());
    }

    @After
    public void afterTest()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD_CLUSTERING).truncateBlocking();

        RequestTracker.instance.set(null);
        SensorsRegistry.instance.clear();

        BloomFilter.recreateOnFPChanceChange = false;
    }

    @Test
    public void testSingleTableMutation()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        double writeSensorSum = 0;
        for (int j = 0; j < 10; j++)
        {
            Mutation m = new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", String.valueOf(j))
            .build();
            handleWriteCommand(m);
            Sensor localSensor = getThreadLocalRequestSensor();
            assertThat(localSensor.getValue()).isGreaterThan(0);
            Sensor registrySensor = getRegistrySensor(context);
            assertThat(registrySensor).isEqualTo(localSensor);
            writeSensorSum += localSensor.getValue();
        }

        // check global registry is synchronized
        Sensor registrySensor = getRegistrySensor(context);
        assertThat(registrySensor.getValue()).isEqualTo(writeSensorSum);
    }

    private ColumnFamilyStore discardSSTables(String ks, String cf)
    {
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
        cfs.discardSSTables(System.currentTimeMillis());
        return cfs;
    }

    private void assertRequestAndRegistrySensorsEquality(Context context)
    {
        Sensor localSensor = getThreadLocalRequestSensor();
        assertThat(localSensor.getValue()).isGreaterThan(0);

        Sensor registrySensor = getRegistrySensor(context);
        assertThat(registrySensor).isEqualTo(localSensor);
    }

    /**
     * Returns the writer sensor with the given context from the global registry
     * @param context the sensor context
     * @return the requested write sensor from the global registry
     */
    private static Sensor getRegistrySensor(Context context)
    {
        return SensorsRegistry.instance.getOrCreateSensor(context, Type.WRITE_BYTES).get();
    }

    /**
     * Returns the writer sensor registered in the thread local {@link RequestSensors}
     * @return the thread local writer sensor
     */
    private static Sensor getThreadLocalRequestSensor()
    {
        return RequestTracker.instance.get().getSensor(Type.WRITE_BYTES).get();
    }

    private static void handleWriteCommand(Mutation mutation)
    {
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
    }
}
