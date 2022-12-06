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

package org.apache.cassandra.index.sai.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MemtableIndexTest extends SAITester
{
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    private ColumnFamilyStore cfs;
    private IndexContext indexContext;
    private MemtableIndex memtableIndex;
    private AbstractAllocatorMemtable memtable;
    private IPartitioner partitioner;
    private Map<Token, Integer> tokenMap = new TreeMap<>();

    @BeforeClass
    public static void setShardCount()
    {
        System.setProperty("cassandra.trie.memtable.shard.count", "8");
    }

    @Before
    public void setup() throws Throwable
    {
        assertEquals(8, TrieMemtable.SHARD_COUNT);

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());

        TableMetadata tableMetadata = TableMetadata.builder("ks", "tb")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("val", Int32Type.instance)
                                                   .build();
        cfs = MockSchema.newCFS(tableMetadata);
        partitioner = cfs.getPartitioner();
        memtable = (AbstractAllocatorMemtable) cfs.getCurrentMemtable();
        indexContext = SAITester.createIndexContext("index", Int32Type.instance, cfs);
        indexSearchCounter.reset();

        Injections.inject(indexSearchCounter);
    }

    @After
    public void resetBufferType() throws Exception
    {
        setTrieMemtableBufferType(BufferType.OFF_HEAP);
    }

    @Test
    public void onHeapAllocation() throws Exception
    {
        setTrieMemtableBufferType(BufferType.ON_HEAP);
        memtableIndex = new MemtableIndex(indexContext);
        assertEquals(TrieMemtable.SHARD_COUNT, memtableIndex.shardCount());

        assertTrue(memtable.getAllocator().onHeap().owns() == 0);
        assertTrue(memtable.getAllocator().offHeap().owns() == 0);

        for (int row = 0; row < 100; row++)
        {
            addRow(row, row);
        }

        assertTrue(memtable.getAllocator().onHeap().owns() > 0);
        assertTrue(memtable.getAllocator().offHeap().owns() == 0);
    }

    @Test
    public void offHeapAllocation() throws Exception
    {
        setTrieMemtableBufferType(BufferType.OFF_HEAP);
        memtableIndex = new MemtableIndex(indexContext);
        assertEquals(TrieMemtable.SHARD_COUNT, memtableIndex.shardCount());

        assertTrue(memtable.getAllocator().onHeap().owns() == 0);
        assertTrue(memtable.getAllocator().offHeap().owns() == 0);

        for (int row = 0; row < 100; row++)
        {
            addRow(row, row);
        }

        assertTrue(memtable.getAllocator().onHeap().owns() > 0);
        assertTrue(memtable.getAllocator().offHeap().owns() > 0);
    }

    @Test
    public void unrestrictedQueryTest() throws Exception
    {
        createIndex();

        AbstractBounds<PartitionPosition> keyRange = new Range<>(partitioner.getMinimumToken().minKeyBound(),
                                                                 partitioner.getMinimumToken().minKeyBound());
        List<Integer> expectedKeys = List.of(4, 5, 6, 7);

        runMemtableIndexSearch(keyRange, expectedKeys, TrieMemtable.SHARD_COUNT);
    }

    @Test
    public void singlePartitionBoundsQueryTest() throws Exception
    {
        createIndex();

        AbstractBounds<PartitionPosition> keyRange = new Bounds<>(makeKey(cfs.metadata(), 5), makeKey(cfs.metadata(), 5));
        List<Integer> expectedKeys = List.of(5);

        runMemtableIndexSearch(keyRange, expectedKeys, 1);
    }

    @Test
    public void tokenRangeBoundsQueryTest() throws Exception
    {
        createIndex();

        List<Token> tokens = new ArrayList<>(tokenMap.keySet());
        Token left = tokens.get(3);
        Token right = tokens.get(7);
        AbstractBounds<PartitionPosition> keyRange = new Bounds<>(left.minKeyBound(), right.maxKeyBound());
        List<Integer> expectedKeys = tokenMap.entrySet()
                                             .stream()
                                             .filter(e -> e.getKey().compareTo(left) >= 0 && e.getKey().compareTo(right) <= 0)
                                             .map(e -> e.getValue())
                                             .filter(v -> v >= 4 && v <= 7)
                                             .collect(Collectors.toList());

        runMemtableIndexSearch(keyRange, expectedKeys, 4);
    }

    private void createIndex()
    {
        memtableIndex = new MemtableIndex(indexContext);
        assertEquals(TrieMemtable.SHARD_COUNT, memtableIndex.shardCount());

        for (int row = 0; row < 10; row++)
        {
            addRow(row, row);
        }
    }

    private void runMemtableIndexSearch(AbstractBounds<PartitionPosition> keyRange,
                                        List<Integer> expectedKeys,
                                        int numberOfShardsHit) throws Exception
    {
        Expression expression = new Expression(indexContext).add(Operator.GTE, Int32Type.instance.decompose(4))
                                                            .add(Operator.LTE, Int32Type.instance.decompose(7));

        try (RangeIterator iterator = memtableIndex.search(expression, keyRange))
        {

            int rowCount = 0;
            Set<Integer> foundKeys = new HashSet<>();
            while (iterator.hasNext())
            {
                int key = Int32Type.instance.compose(iterator.next().partitionKey().getKey());
                System.out.println(key);
                assertTrue(expectedKeys.contains(key));
                assertFalse(foundKeys.contains(key));
                foundKeys.add(key);
                rowCount++;
            }
            assertEquals(expectedKeys.size(), rowCount);
        }
        assertEquals(numberOfShardsHit, indexSearchCounter.get());
    }

    private void addRow(int pk, int value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.index(key,
                            Clustering.EMPTY,
                            Int32Type.instance.decompose(value),
                            cfs.getCurrentMemtable(),
                            new OpOrder().start());
        tokenMap.put(key.getToken(), pk);
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }

    private void setTrieMemtableBufferType(final BufferType newBufferType) throws Exception
    {
        Field bufferType = TrieMemtable.class.getDeclaredField("BUFFER_TYPE");
        bufferType.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(bufferType, bufferType.getModifiers() & ~Modifier.FINAL);
        bufferType.set(null, newBufferType);
    }
}
