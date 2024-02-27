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

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.index.sai.IndexingSchemaLoader;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class SensorsSAIWriteTest extends SAITester
{
    private static final String TABLE = "table_name";
    private static final String INDEX = "table_name_index";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s." + TABLE + " (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS " + INDEX + " ON %s." + TABLE + "(%s) USING 'StorageAttachedIndex'";

    public static final String KEYSPACE1 = "SensorsWriteTest";
    public static final String CF_STANDARD = "Standard";
    public static final String CF_STANDARD_CLUSTERING = "StandardClustering";

    public static final String KEYSPACE2 = "SensorsWriteTest2";
    public static final String CF_STANDARD2 = "Standard2";
    private ColumnFamilyStore store;

    @Before
    public void resetSensorsRegistry()
    {
        SensorsRegistry.instance.clear();
    }

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(CF_STANDARD + "_val", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "val");
        }}));
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD,
                                                              1, AsciiType.instance, AsciiType.instance, null)
                                                .indexes(indexes.build()));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void beforeTest()
    {
        SensorsRegistry.instance.onCreateKeyspace(Keyspace.open(KEYSPACE1).getMetadata());
        SensorsRegistry.instance.onCreateTable(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).metadata());


    }


    @Test
    public void testSingleRowUpdateUpdateMutation()
    {
        store = SensorsTestUtil.discardSSTables(KEYSPACE1, CF_STANDARD);
        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());

        double writeSensorSum = 0;
        for (int j = 0; j < 10; j++)
        {
            Mutation m = new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                         .add("val", String.valueOf(j))
                         .build();
            handleMutation(m);
            Sensor localSensor = SensorsTestUtil.getThreadLocalRequestSensor(context, Type.WRITE_BYTES);
            assertThat(localSensor.getValue()).isGreaterThan(0);
            Sensor registrySensor = SensorsTestUtil.getRegistrySensor(context, Type.WRITE_BYTES);
            assertThat(registrySensor).isEqualTo(localSensor);
            writeSensorSum += localSensor.getValue();

            // check global registry is synchronized
            assertThat(registrySensor.getValue()).isEqualTo(writeSensorSum);
            //assertResponseSensors(localSensor.getValue(), writeSensorSum, CF_STANDARD);
        }
    }


    @Test
    public void testInsert_SAI_FlushFromMemtable() throws Throwable
    {
        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace1, "v1"));
        assertTrue(waitForIndex(keyspace1, TABLE, INDEX));


        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace2, "v1"));
        assertTrue(waitForIndex(keyspace2, TABLE, INDEX));

        Context context1 = new Context(keyspace1, TABLE, Keyspace.open(keyspace1).getColumnFamilyStore(TABLE).metadata().id.toString());
        Context context2 = new Context(keyspace2, TABLE, Keyspace.open(keyspace2).getColumnFamilyStore(TABLE).metadata().id.toString());

        execute("INSERT INTO " + keyspace1 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");

        Optional<Sensor> sensor1 = SensorsRegistry.instance.getSensor(context1, Type.WRITE_BYTES);
        Optional<Sensor> sensor2 = SensorsRegistry.instance.getSensor(context2, Type.WRITE_BYTES);

        assertThat(sensor1).isPresent();
        assertThat(sensor2).isEmpty();

        double sensorValueSum = sensor1.get().getValue();
        assertThat(sensor1.get().getValue()).isGreaterThan(0D);
        execute("INSERT INTO " + keyspace1 + "." + TABLE + " (id1, v1, v2) VALUES ('1', 1, '1')");

        assertThat(sensor1.get().getValue()).isGreaterThan(sensorValueSum); // sensor is ever-increasing

        execute("INSERT INTO " + keyspace2 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");

        sensor2 = SensorsRegistry.instance.getSensor(context2, Type.WRITE_BYTES);
        assertThat(sensor2).isPresent();
        assertThat(sensor2.get().getValue()).isEqualTo(sensorValueSum); // same data inserted

        // flushing or compaction should not change the sensor value because we only track memtable index bytes
        flush(keyspace1, TABLE);
        //assert
        compact(keyspace1, TABLE);
        //assert
    }

    protected static void handleMutation(Mutation mutation)
    {
        MutationVerbHandler.instance.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).build());
    }
}