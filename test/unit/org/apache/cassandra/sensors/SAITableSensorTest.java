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

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class SAITableSensorTest extends SAITester
{
    private static final String TABLE = "table_name";
    private static final String INDEX = "table_name_index";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s." + TABLE + " (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS " + INDEX + " ON %s." + TABLE + "(%s) USING 'StorageAttachedIndex'";

    @Before
    public void resetSensorsRegistry()
    {
        SensorsRegistry.instance.clear();
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
        flush(keyspace1, TABLE); // flushes index from memtables directly

        Optional<Sensor> sensor1 = SensorsRegistry.instance.getSensor(context1, Type.SAI_WRITE_BYTES);
        Optional<Sensor> sensor2 = SensorsRegistry.instance.getSensor(context2, Type.SAI_WRITE_BYTES);

        assertThat(sensor1).isPresent();
        assertThat(sensor2).isEmpty();

        double sensorValueSum = sensor1.get().getValue();
        assertThat(sensor1.get().getValue()).isGreaterThan(0D);
        execute("INSERT INTO " + keyspace1 + "." + TABLE + " (id1, v1, v2) VALUES ('1', 1, '1')");
        flush(keyspace1, TABLE);

        assertThat(sensor1.get().getValue()).isGreaterThan(sensorValueSum); // sensor is ever-increasing

        execute("INSERT INTO " + keyspace2 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        flush(keyspace2, TABLE);

        sensor2 = SensorsRegistry.instance.getSensor(context2, Type.SAI_WRITE_BYTES);
        assertThat(sensor2).isPresent();
        assertThat(sensor2.get().getValue()).isEqualTo(sensorValueSum); // same data inserted
    }

    @Test
    public void testInsert_SAI_FlushFromSSTable() throws Throwable
    {
        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1));
        // defer index creation until after data is inserted to force index to be built from SSTables
        execute("INSERT INTO " + keyspace1 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2));

        Context context1 = new Context(keyspace1, TABLE, Keyspace.open(keyspace1).getColumnFamilyStore(TABLE).metadata().id.toString());
        Context context2 = new Context(keyspace2, TABLE, Keyspace.open(keyspace2).getColumnFamilyStore(TABLE).metadata().id.toString());

        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace1, "v1"));
        assertTrue(waitForIndex(keyspace1, TABLE, INDEX));

        Optional<Sensor> sensor1 = SensorsRegistry.instance.getSensor(context1, Type.SAI_WRITE_BYTES);
        Optional<Sensor> sensor2 = SensorsRegistry.instance.getSensor(context2, Type.SAI_WRITE_BYTES);

        assertThat(sensor1).isPresent();
        assertThat(sensor2).isEmpty();

        double sensorValueSum = sensor1.get().getValue();
        assertThat(sensor1.get().getValue()).isGreaterThan(0D);
        execute("INSERT INTO " + keyspace1 + "." + TABLE + " (id1, v1, v2) VALUES ('1', 1, '1')");
        compact(keyspace1, TABLE); // use compact instead of flush to also for index to be built from SSTables

        assertThat(sensor1.get().getValue()).isGreaterThan(sensorValueSum); // sensor is ever-increasing

        execute("INSERT INTO " + keyspace2 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace2, "v1"));
        assertTrue(waitForIndex(keyspace2, TABLE, INDEX));

        sensor2 = SensorsRegistry.instance.getSensor(context2, Type.SAI_WRITE_BYTES);
        assertThat(sensor2).isPresent();
        assertThat(sensor2.get().getValue()).isEqualTo(sensorValueSum); // same data inserted
    }
}