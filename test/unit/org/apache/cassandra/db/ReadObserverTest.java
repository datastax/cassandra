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

package org.apache.cassandra.db;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReadObserverTest
{
    public static class TestReadObserverFactory implements ReadObserverFactory
    {
        static List<Pair<TableMetadata, ReadObserver>> issuedObservers = new CopyOnWriteArrayList<>();

        @Override
        public ReadObserver create(TableMetadata table)
        {
            if (table == null || !table.keyspace.equals(KEYSPACE))
                return ReadObserver.NO_OP;

            ReadObserver observer = spy(ReadObserver.class);
            issuedObservers.add(Pair.create(table, observer));
            return observer;
        }
    }

    private static final String CF = "Standard";
    private static final String KEYSPACE = "ReadObserverTest";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CassandraRelevantProperties.CUSTOM_READ_OBSERVER_FACTORY.setString(TestReadObserverFactory.class.getName());

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        Indexes.Builder indexes = Indexes.builder();

        IndexTarget target = new IndexTarget(new ColumnIdentifier("a", true), IndexTarget.Type.SIMPLE);
        indexes.add(IndexMetadata.fromIndexTargets(Collections.singletonList(target), "sai", IndexMetadata.Kind.CUSTOM, Map.of("class_name", "StorageAttachedIndex")));

        TableMetadata.Builder metadata =
        TableMetadata.builder(KEYSPACE, CF)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addStaticColumn("s", AsciiType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .indexes(indexes.build())
                     .caching(CachingParams.CACHE_NOTHING);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata);

        LocalSessionAccessor.startup();
    }

    @Before
    public void beforeEach()
    {
        TestReadObserverFactory.issuedObservers.clear();
    }

    // TODO
    // 4. multi-sstables
    @Test
    public void testObserverCallbacksSinglePartitionRead()
    {
        testObserverCallbacks(cfs -> Util.cmd(cfs, Util.dk("key")).build());
    }

    @Test
    public void testObserverCallbacksRangeRead()
    {
        testObserverCallbacks(cfs -> Util.cmd(cfs).build());
    }

    @Test
    public void testObserverCallbacksSAI()
    {
        testObserverCallbacks(cfs -> Util.cmd(cfs).filterOn("a", Operator.EQ, ByteBufferUtil.bytes("regular")).build());
    }

    private void testObserverCallbacks(Function<ColumnFamilyStore, ReadCommand> commandBuilder)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
        .clustering("cc")
        .add("a", ByteBufferUtil.bytes("regular"))
        .build()
        .apply();

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // duplicated row with newer timestamp
        new RowUpdateBuilder(cfs.metadata(), 1, ByteBufferUtil.bytes("key"))
        .clustering("cc")
        .add("a", ByteBufferUtil.bytes("regular"))
        .build()
        .apply();

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
        .add("s", ByteBufferUtil.bytes("static"))
        .build()
        .apply();

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // duplicated row with newer timestamp in memtable
        new RowUpdateBuilder(cfs.metadata(), 2, ByteBufferUtil.bytes("key"))
        .clustering("cc")
        .add("a", ByteBufferUtil.bytes("regular"))
        .build()
        .apply();

        int sstables = cfs.getLiveSSTables().size();

        ReadCommand readCommand = commandBuilder.apply(cfs);
        assertFalse(Util.getAll(readCommand).isEmpty());

        List<Pair<TableMetadata, ReadObserver>> observers = TestReadObserverFactory.issuedObservers.stream()
                                                                                                   .filter(p -> p.left.name.equals(CF))
                                                                                                   .collect(Collectors.toList());

        // expect one observer per query
        assertEquals(1, observers.size());
        ReadObserver observer = observers.get(0).right;

        int unmergedPartitionRead = -1;
        int unmergedPartitionsRead = -1;
        int mergedPartitionsRead = -1;
        if (readCommand.indexQueryPlan != null) // SAI
        {
            unmergedPartitionRead = sstables + 1; // one unmerged partition per sstable and memtable
            unmergedPartitionsRead = 0;
            mergedPartitionsRead = 1;

        }
        else if (readCommand instanceof PartitionRangeReadCommand)
        {
            unmergedPartitionRead = 0;
            unmergedPartitionsRead = sstables + 1; // one unmerged partitions per sstable and memtable
            mergedPartitionsRead = 1;
        }
        else
        {
            Preconditions.checkArgument(readCommand instanceof SinglePartitionReadCommand);
            unmergedPartitionRead = sstables + 1; // one unmerged partition per sstable and memtable
            unmergedPartitionsRead = 0;
            mergedPartitionsRead = 1;
        }

        verify(observer, times(unmergedPartitionRead)).observeUnmergedPartition(any()); // intercept unmerged partition
        verify(observer, times(unmergedPartitionsRead)).observeUnmergedPartitions(any()); // intercept unmerged partitions
        verify(observer, times(mergedPartitionsRead)).observeMergedPartitions(any()); // intercept merged partitions
        verify(observer, times(1)).onComplete(); // onComplete() once per query
    }
}