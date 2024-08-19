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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
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
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrieMemtableIndexTest extends SAITester
{
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    // A non-frozen list of integers
    private final ListType<Integer> integerListType = ListType.getInstance(Int32Type.instance, true);

    private ColumnFamilyStore cfs;
    private IndexContext indexContext;
    private IndexContext integerListIndexContext;
    private TrieMemtableIndex memtableIndex;
    private AbstractAllocatorMemtable memtable;
    private IPartitioner partitioner;
    private Map<DecoratedKey, Integer> keyMap;
    private Map<Integer, Integer> rowMap;

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
                                                   .addRegularColumn("vals", integerListType)
                                                   .build();
        cfs = MockSchema.newCFS(tableMetadata);
        partitioner = cfs.getPartitioner();
        memtable = (AbstractAllocatorMemtable) cfs.getCurrentMemtable();
        indexContext = SAITester.createIndexContext("index", Int32Type.instance, cfs);
        integerListIndexContext = SAITester.createIndexContext("collection_index", integerListType, cfs);
        indexSearchCounter.reset();
        keyMap = new TreeMap<>();
        rowMap = new HashMap<>();

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
        memtableIndex = new TrieMemtableIndex(indexContext);
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
        memtableIndex = new TrieMemtableIndex(indexContext);
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
    public void randomQueryTest() throws Exception
    {
        memtableIndex = new TrieMemtableIndex(indexContext);
        assertEquals(TrieMemtable.SHARD_COUNT, memtableIndex.shardCount());

        for (int row = 0; row < getRandom().nextIntBetween(1000, 5000); row++)
        {
            int pk = getRandom().nextIntBetween(0, 10000);
            while (rowMap.containsKey(pk))
                pk = getRandom().nextIntBetween(0, 10000);
            int value = getRandom().nextIntBetween(0, 100);
            rowMap.put(pk, value);
            addRow(pk, value);
        }

        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            Expression expression = generateRandomExpression();

            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);

            Set<Integer> expectedKeys = keyMap.keySet()
                                              .stream()
                                              .filter(keyRange::contains)
                                              .map(keyMap::get)
                                              .filter(pk -> expression.isSatisfiedBy(Int32Type.instance.decompose(rowMap.get(pk))))
                                              .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();

            try (RangeIterator iterator = memtableIndex.search(new QueryContext(), expression, keyRange, 0))
            {
                while (iterator.hasNext())
                {
                    int key = Int32Type.instance.compose(iterator.next().partitionKey().getKey());
                    assertFalse(foundKeys.contains(key));
                    foundKeys.add(key);
                }
            }

            assertEquals(expectedKeys, foundKeys);
        }
    }

    @Test
    public void indexIteratorTest()
    {
        memtableIndex = new TrieMemtableIndex(indexContext);

        Map<Integer, Set<DecoratedKey>> terms = buildTermMap();

        terms.entrySet()
             .stream()
             .forEach(entry -> entry.getValue()
                                    .forEach(pk -> addRow(Int32Type.instance.compose(pk.getKey()), entry.getKey())));

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            // These keys have midrange tokens that select 3 of the 8 range indexes
            DecoratedKey temp1 = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            DecoratedKey temp2 = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            DecoratedKey minimum = temp1.compareTo(temp2) <= 0 ? temp1 : temp2;
            DecoratedKey maximum = temp1.compareTo(temp2) <= 0 ? temp2 : temp1;

            Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator = memtableIndex.iterator(minimum, maximum);

            while (iterator.hasNext())
            {
                Pair<ByteComparable, Iterator<PrimaryKey>> termPair = iterator.next();
                int term = termFromComparable(termPair.left);
                // The iterator will return keys outside the range of min/max so we need to filter here to
                // get the correct keys
                List<DecoratedKey> expectedPks = terms.get(term)
                                                      .stream()
                                                      .filter(pk -> pk.compareTo(minimum) >= 0 && pk.compareTo(maximum) <= 0)
                                                      .sorted()
                                                      .collect(Collectors.toList());
                List<DecoratedKey> termPks = new ArrayList<>();
                while (termPair.right.hasNext())
                {
                    DecoratedKey pk = termPair.right.next().partitionKey();
                    if (pk.compareTo(minimum) >= 0 && pk.compareTo(maximum) <= 0)
                        termPks.add(pk);
                }
                assertEquals(expectedPks, termPks);
            }
        }
    }

    @Test
    public void updateCollectionTest()
    {
        // Use one shard to test shared keys in the trie
        memtableIndex = new TrieMemtableIndex(integerListIndexContext, 1);
        assertEquals(0, memtable.getAllocator().onHeap().owns());
        assertEquals(0, memtableIndex.estimatedOnHeapMemoryUsed() + memtableIndex.estimatedOffHeapMemoryUsed());

        addRowWithCollection(1, 1, 2, 3); // row 1, values 1, 2, 3
        addRowWithCollection(2, 4, 5, 6); // row 2, values 4, 5, 6
        addRowWithCollection(3, 2, 6);    // row 3, values 2, 6

        // 8 total pk entries at 36 bytes, 6 unique trie keys at 4 bytes, 6 PrimaryKeys objects with
        var expectedOnHeap = 8 * 36 + 6 * 4 + 6 * PrimaryKeys.unsharedHeapSize();
        assertEquals(expectedOnHeap, memtableIndex.estimatedOnHeapMemoryUsed());
        assertEquals(expectedOnHeap, memtable.getAllocator().onHeap().owns());
        // Hard coded cost from trie
        assertEquals(96, memtableIndex.estimatedOffHeapMemoryUsed());

        // Query values
        assertEqualsQuery(2, 1, 3);
        assertEqualsQuery(4, 2);
        assertEqualsQuery(3, 1);
        assertEqualsQuery(6, 2, 3);

        assertEquals(expectedOnHeap, memtable.getAllocator().onHeap().owns());

        // Update row 1 to remove 2 and 3, keep 1, add 7 and 8 (note we have to manually match the 1,2,3 from above)
        updateRowWithCollection(1, List.of(1, 2, 3).iterator(), List.of(1, 7, 8).iterator());

        // We net 1 new PrimaryKeys object and 2 new trie memtable entries. The current implementation
        // does not remove trie keys, so those are still present.
        expectedOnHeap += PrimaryKeys.unsharedHeapSize() + 4 * 2;
        assertEquals(expectedOnHeap, memtable.getAllocator().onHeap().owns());

        updateRowWithCollection(1, List.of(1, 7, 8).iterator(), List.of(1, 4, 8).iterator());

        // We remove a PrimaryKeys object without adding any new keys to the trie.
        expectedOnHeap -= PrimaryKeys.unsharedHeapSize();
        assertEquals(expectedOnHeap, memtable.getAllocator().onHeap().owns());

        // Run additional queries to ensure values
        assertEqualsQuery(1, 1);
        assertEqualsQuery(4, 1, 2);
        assertEqualsQuery(2, 3);
        assertEqualsQuery(3);

        // Show that iteration works as expected and does not include any of the deleted terms.
        var iter = memtableIndex.iterator(makeKey(cfs.metadata(), 1), makeKey(cfs.metadata(), 3));
        assertNextEntryInIterator(iter, 1, 1);
        assertNextEntryInIterator(iter, 2, 3);
        assertNextEntryInIterator(iter, 4, 1, 2);
        assertNextEntryInIterator(iter, 5, 2);
        assertNextEntryInIterator(iter, 6, 2, 3);
        assertNextEntryInIterator(iter, 8, 1);
        assertFalse(iter.hasNext());
    }

    private void assertEqualsQuery(int value, int... partitionKeys)
    {
        // Build eq expression to search for the value
        Expression expression = new Expression(integerListIndexContext);
        expression.add(Operator.EQ, Int32Type.instance.decompose(value));
        AbstractBounds<PartitionPosition> keyRange = new Range<>(partitioner.getMinimumToken().minKeyBound(),
                                                                 partitioner.getMinimumToken().minKeyBound());
        var result = memtableIndex.search(new QueryContext(), expression, keyRange, 0);
        // Confirm the partition keys are as expected in the provided order and that we have no more results
        for (int partitionKey : partitionKeys)
            assertEquals(makeKey(cfs.metadata(), partitionKey), result.next().partitionKey());
        assertFalse(result.hasNext());
    }

    private void assertNextEntryInIterator(Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iter, int term, int... primaryKeys)
    {
        assertTrue(iter.hasNext());
        Pair<ByteComparable, Iterator<PrimaryKey>> entry = iter.next();
        assertEquals(term, termFromComparable(entry.left));
        for (int value : primaryKeys)
        {
            assertTrue(entry.right.hasNext());
            assertEquals(makeKey(cfs.metadata(), value), entry.right.next().partitionKey());
        }
    }

    private Expression generateRandomExpression()
    {
        Expression expression = new Expression(indexContext);

        int equality = getRandom().nextIntBetween(0, 100);
        int lower = getRandom().nextIntBetween(0, 75);
        int upper = getRandom().nextIntBetween(25, 100);
        while (upper <= lower)
            upper = getRandom().nextIntBetween(0, 100);

        if (getRandom().nextBoolean())
            expression.add(Operator.EQ, Int32Type.instance.decompose(equality));
        else
        {
            boolean useLower = getRandom().nextBoolean();
            boolean useUpper = getRandom().nextBoolean();
            if (!useLower && !useUpper)
                useLower = useUpper = true;
            if (useLower)
                expression.add(getRandom().nextBoolean() ? Operator.GT : Operator.GTE, Int32Type.instance.decompose(lower));
            if (useUpper)
                expression.add(getRandom().nextBoolean() ? Operator.LT : Operator.LTE, Int32Type.instance.decompose(upper));
        }
        return expression;
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                 : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().maxKeyBound();

        AbstractBounds<PartitionPosition> keyRange;

        if (leftBound.isMinimum() && rightBound.isMinimum())
            keyRange = new Range<>(leftBound, rightBound);
        else
        {
            if (AbstractBounds.strictlyWrapsAround(leftBound, rightBound))
            {
                PartitionPosition temp = leftBound;
                leftBound = rightBound;
                rightBound = temp;
            }
            if (getRandom().nextBoolean())
                keyRange = new Bounds<>(leftBound, rightBound);
            else if (getRandom().nextBoolean())
                keyRange = new ExcludingBounds<>(leftBound, rightBound);
            else
                keyRange = new IncludingExcludingBounds<>(leftBound, rightBound);
        }
        return keyRange;
    }

    private int termFromComparable(ByteComparable comparable)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(comparable.asComparableBytes(ByteComparable.Version.OSS41));
        return Int32Type.instance.compose(Int32Type.instance.fromComparableBytes(peekable, ByteComparable.Version.OSS41));
    }

    private Map<Integer, Set<DecoratedKey>> buildTermMap()
    {
        Map<Integer, Set<DecoratedKey>> terms = new HashMap<>();

        for (int count = 0; count < 10000; count++)
        {
            int term = getRandom().nextIntBetween(0, 100);
            Set<DecoratedKey> pks;
            if (terms.containsKey(term))
                pks = terms.get(term);
            else
            {
                pks = new HashSet<>();
                terms.put(term, pks);
            }
            DecoratedKey key = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            while (pks.contains(key))
                key = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            pks.add(key);
        }
        return terms;
    }

    private void addRow(int pk, int value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.index(key,
                            Clustering.EMPTY,
                            Int32Type.instance.decompose(value),
                            cfs.getCurrentMemtable(),
                            new OpOrder().start());
        keyMap.put(key, pk);
    }

    private void addRowWithCollection(int pk, Integer... value)
    {
        for (Integer v : value)
            addRow(pk, v);
    }

    private void updateRowWithCollection(int pk, Iterator<Integer> oldValues, Iterator<Integer> newValues)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.update(key,
                             Clustering.EMPTY,
                             Iterators.transform(oldValues, Int32Type.instance::decompose),
                             Iterators.transform(newValues, Int32Type.instance::decompose),
                             cfs.getCurrentMemtable(),
                             new OpOrder().start());
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
