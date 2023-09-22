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
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SensorsReadTest
{
    public static final String KEYSPACE1 = "SensorsReadTest";
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
    public void testMemtableRead()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);

        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        RequestSensors sensors = new RequestSensors(context);
        sensors.registerSensor(Type.READ_BYTES);
        RequestTracker.instance.set(sensors);

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        DecoratedKey key = store.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        Util.getAll(Util.cmd(store, key).build());
        long bytes = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertTrue(bytes > 0);
        assertEquals(bytes, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);
    }

    @Test
    public void testSinglePartitionReadCommand_ByPartitionKey()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);

        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        RequestSensors sensors = new RequestSensors(context);
        sensors.registerSensor(Type.READ_BYTES);
        RequestTracker.instance.set(sensors);

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("4"));
        Util.getAll(Util.cmd(store, key).build());
        long bytes = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertTrue(bytes > 0);
        assertEquals(bytes, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);
    }

    @Test
    public void testSinglePartitionReadCommand_ByClustering()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);

        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());
        RequestSensors sensors = new RequestSensors(context);
        sensors.registerSensor(Type.READ_BYTES);
        RequestTracker.instance.set(sensors);

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, "0")
            .clustering(String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        Util.getAll(Util.cmd(store, key).includeRow("0").build());
        long bytes = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertTrue(bytes > 0);
        assertEquals(bytes, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);
    }

    @Test
    public void testSinglePartitionReadCommand_AllowFiltering()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD_CLUSTERING);

        Context context = new Context(KEYSPACE1, CF_STANDARD_CLUSTERING, store.metadata.id.toString());
        RequestSensors sensors = new RequestSensors(context);
        sensors.registerSensor(Type.READ_BYTES);
        RequestTracker.instance.set(sensors);

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, "0")
            .clustering(String.valueOf(j))
            .add("val", String.valueOf(j))
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        Util.getAll(Util.cmd(store, key).includeRow("0").build());
        long bytes = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertTrue(bytes > 0);
        assertEquals(bytes, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);

        RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().reset();

        Util.getAll(Util.cmd(store, key).filterOn("val", Operator.EQ, "9").build());
        long bytes2 = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertEquals(bytes * 10, bytes2);
        assertEquals(bytes + bytes * 10, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);
    }

    @Test
    public void testPartitionRangeReadCommand_ByPartitionKey()
    {
        store = discardSSTables(KEYSPACE1, CF_STANDARD);

        Context context = new Context(KEYSPACE1, CF_STANDARD, store.metadata.id.toString());
        RequestSensors sensors = new RequestSensors(context);
        sensors.registerSensor(Type.READ_BYTES);
        RequestTracker.instance.set(sensors);

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("0"));
        Util.getAll(Util.cmd(store, key).build());
        long bytes = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertTrue(bytes > 0);
        assertEquals(bytes, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);

        RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().reset();

        Util.getAll(Util.cmd(store).fromKeyIncl("0").toKeyIncl("9").build());
        long bytes2 = (long) RequestTracker.instance.get().getSensor(Type.READ_BYTES).get().getValue();
        assertEquals(bytes * 10, bytes2);
        assertEquals(bytes + bytes * 10, SensorsRegistry.instance.getOrCreateSensor(context, Type.READ_BYTES).get().getValue(), 0);
    }

    private ColumnFamilyStore discardSSTables(String ks, String cf)
    {
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
        cfs.discardSSTables(System.currentTimeMillis());
        return cfs;
    }
}
