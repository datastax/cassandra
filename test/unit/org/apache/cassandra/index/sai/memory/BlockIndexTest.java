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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.index.sai.memory.BlockIndex.nudge;
import static org.apache.cassandra.index.sai.memory.BlockIndex.nudgeReverse;

public class BlockIndexTest extends SaiRandomizedTest
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PART_KEY_COL = "key";
    private static final String REG_COL = "col";

    private static final AbstractBounds<PartitionPosition> ALL_DATA_RANGE = DataRange.allData(Murmur3Partitioner.instance).keyRange();

    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    @Test
    public void testRadixSort() throws Exception
    {
        for (int x = 0; x < 500; x++)
        {
            doTestRadixSort();
        }
    }

    private void doTestRadixSort() throws Exception
    {
        int size = nextInt(1, 5000);

        OffheapBytes[] values = new OffheapBytes[size];
        OffheapBytes[] keys = new OffheapBytes[size];

        OffheapBytes[] valuesKeys = new OffheapBytes[size];

        int maxLen = -1;

        for (int x = 0; x < size; x++)
        {
            int valuelen = nextInt(1, 50);
            byte[] value = new byte[valuelen];
            nextBytes(value);
            values[x] = new OffheapBytes(value);

            int keylen = nextInt(1, 50);
            byte[] key = new byte[keylen];
            nextBytes(key);
            keys[x] = new OffheapBytes(key);

            maxLen = Math.max(valuelen + keylen, maxLen);

            valuesKeys[x] = new OffheapBytes(Bytes.concat(value, key));
        }

        MultiBlockIndex.ValuesKeysMSBRadixSorter sorter = new MultiBlockIndex.ValuesKeysMSBRadixSorter(Integer.MAX_VALUE, values, keys);

        StringMSBRadixSorter luceneSorter = new StringMSBRadixSorter()
        {
            @Override
            protected void swap(int i, int j)
            {
                OffheapBytes tmp = valuesKeys[i];
                valuesKeys[i] = valuesKeys[j];
                valuesKeys[j] = tmp;
            }

            @Override
            protected BytesRef get(int i)
            {
                return valuesKeys[i].toBytesRef();
            }
        };

        // check byteAt is the same
        for (int x = 0; x < size; x++)
        {
            for (int i = 0; i < Integer.MAX_VALUE; i++)
            {
                int b = sorter.byteAt(x, i);
                int b2 = luceneSorter.byteAt(x, i);

                assertEquals(b, b2);
                if (b == -1)
                    break;
            }
        }

        sorter.sort(0, size);
        luceneSorter.sort(0, size);

        Arrays.sort(valuesKeys);

        for (int x = 0; x < size; x++)
        {
            byte[] allBytes = Bytes.concat(BytesRef.deepCopyOf(values[x].toBytesRef()).bytes,
                                           BytesRef.deepCopyOf(keys[x].toBytesRef()).bytes);

            OffheapBytes offheap = new OffheapBytes(allBytes);

            assertEquals(valuesKeys[x], offheap);
        }
    }

    @Test
    public void testNudge()
    {
        for (int x = 0; x < 100; x++)
        {
            doTestNudge();
        }
    }

    // add a bunch of random length random bytes
    // sort 'em
    // run the same filter with and without nudge
    public void doTestNudge()
    {
        final List<BytesRef> list = new ArrayList<>();

        final int num = 1000;

        for (int x = 0; x < num; x++)
        {
            final int bytesLen = nextInt(1, 10);
            byte[] bytes = new byte[bytesLen];
            nextBytes(bytes);
            list.add(new BytesRef(bytes));
        }

        for (int x = 0; x < 100; x++)
        {
            int startIdx = nextInt(0, num);
            BytesRef lowerBound = list.get(startIdx);
            int endIdx = startIdx + nextInt(0, num - startIdx);
            BytesRef upperBound = list.get(endIdx);

            boolean lowerInclusive = getRandom().nextBoolean();
            boolean upperInclusive = getRandom().nextBoolean();

            Collections.sort(list);

            Iterator<BytesRef> iterator1 = Iterators.filter(list.iterator(), bytes -> {
                boolean lowerMatch = false;
                boolean upperMatch = false;

                if (lowerInclusive && bytes.compareTo(lowerBound) >= 0)
                    lowerMatch = true;
                else if (!lowerInclusive && bytes.compareTo(lowerBound) > 0)
                    lowerMatch = true;

                if (upperBound == null)
                    upperMatch = true;
                else if (upperInclusive && bytes.compareTo(upperBound) <= 0)
                    upperMatch = true;
                else if (!upperInclusive && bytes.compareTo(upperBound) < 0)
                    upperMatch = true;

                if (lowerMatch && upperMatch)
                    return true;

                return false;
            });

            final BytesRef lowerBound2;
            if (!lowerInclusive)
            {
                byte[] bytes = nudge(lowerBound.bytes);
                lowerBound2 = new BytesRef(bytes);
            }
            else
                lowerBound2 = lowerBound;

            final BytesRef upperBound2;
            if (!upperInclusive)
            {
                byte[] bytes = nudgeReverse(upperBound.bytes, 20);
                upperBound2 = new BytesRef(bytes);
            }
            else
                upperBound2 = upperBound;

            Iterator<BytesRef> iterator2 = Iterators.filter(list.iterator(), bytes -> {
                boolean lowerMatch = false;
                boolean upperMatch = false;

                if (bytes.compareTo(lowerBound2) >= 0)
                    lowerMatch = true;

                if (upperBound2 == null)
                    upperMatch = true;
                else if (bytes.compareTo(upperBound2) <= 0)
                    upperMatch = true;

                if (lowerMatch && upperMatch)
                    return true;

                return false;
            });

            BytesRef[] array1 = Iterators.toArray(iterator1, BytesRef.class);
            BytesRef[] array2 = Iterators.toArray(iterator2, BytesRef.class);

            assertArrayEquals(array1, array2);
        }
    }

    @Test
    public void testRandomNudge()
    {
        for (int x = 0; x < 100_000; x++)
        {
            int len = nextInt(1, 10);
            byte[] bytes = new byte[len];
            nextBytes(bytes);
            byte[] bytes2 = nudge(bytes);

            assertTrue("bytes=" + toUnsigned(bytes) + " bytes2=" + toUnsigned(bytes2), new BytesRef(bytes).compareTo(new BytesRef(bytes2)) < 0);
        }
    }

    public static String toUnsigned(byte[] bytes)
    {
        int[] ints = new int[bytes.length];
        for (int x = 0; x < bytes.length; x++)
        {
            ints[x] = UnsignedBytes.toInt(bytes[x]);
        }
        return Arrays.toString(ints);
    }

    @Test
    public void testRandomNudgeReverse()
    {
        int maxLen = 0;
        for (int x = 0; x < 100_000; x++)
        {
            int len = nextInt(1, 10);
            maxLen = Math.max(len, maxLen);
            byte[] bytes = new byte[len];
            nextBytes(bytes);
            byte[] revBytes = nudgeReverse(bytes, maxLen);

            assertTrue("bytes=" + toUnsigned(bytes) + " revbytes=" + toUnsigned(revBytes), new BytesRef(bytes).compareTo(new BytesRef(revBytes)) > 0);
        }
    }

    @Test
    public void testNudgeReverse()
    {
        byte[] bytes1 = new byte[] {115, 90, -86, 71, -1};
        byte[] bytes2 = nudgeReverse(bytes1, 20);

        assertTrue(new BytesRef(bytes1).compareTo(new BytesRef(bytes2)) > 0);

        bytes1 = new byte[] {115, 90, -86, 71, (int)0};
        bytes2 = nudgeReverse(bytes1, 20);

        assertTrue(new BytesRef(bytes1).compareTo(new BytesRef(bytes2)) > 0);
    }

    @Test
    // compare random queries on MultiBlockIndex and TrieMemoryIndex
    public void testRandom() throws Exception
    {
        IndexContext indexContext = createContext();
        MultiBlockIndex blockIndex = new MultiBlockIndex(indexContext);

        TrieMemoryIndex trieIndex = new TrieMemoryIndex(indexContext);

        for (int x = 0; x < 6000; x++)
        {
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(x));
            ByteBuffer value = Int32Type.instance.decompose(x);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
            trieIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        for (int x = 0; x < 6000; x++)
        {
            Expression expression = new Expression(indexContext);

            int start = nextInt(0, 6000 - 10);
            int end = nextInt(start + 1, 6000);

            expression.add(Operator.GTE, Int32Type.instance.decompose(start));
            expression.add(Operator.LTE, Int32Type.instance.decompose(end));

            RangeIterator blockRangeIterator = blockIndex.search(expression, ALL_DATA_RANGE);
            RangeIterator trieRangeIterator = trieIndex.search(expression, ALL_DATA_RANGE);

            if (blockRangeIterator == null)
            {
                assertNull(trieRangeIterator);
            }

            while (blockRangeIterator.hasNext())
            {
                assertTrue(trieRangeIterator.hasNext());

                PrimaryKey key1 = blockRangeIterator.next();
                PrimaryKey key2 = trieRangeIterator.next();

                assertEquals(key1, key2);
            }
        }
    }

    @Test
    public void testMultiBlockRangeQuery() throws Exception
    {
        IndexContext indexContext = createContext();

        MultiBlockIndex blockIndex = new MultiBlockIndex(indexContext);
        for (int x = 0; x < 6000; x++)
        {
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(x));
            ByteBuffer value = Int32Type.instance.decompose(x);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        // ensure 2 blocks are present
        assertNotNull(blockIndex.current);
        assertTrue(blockIndex.existingBlocks.size() >= 1);

        Expression expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(100));
        expression.add(Operator.LTE, Int32Type.instance.decompose(200));
        int hits = countHits(expression, blockIndex);
        assertEquals(101, hits, 1);

        expression = new Expression(indexContext);
        expression.add(Operator.GT, Int32Type.instance.decompose(100));
        expression.add(Operator.LTE, Int32Type.instance.decompose(200));
        hits = countHits(expression, blockIndex, 1);
        assertEquals(100, hits);

        expression = new Expression(indexContext);
        expression.add(Operator.GT, Int32Type.instance.decompose(100));
        expression.add(Operator.LT, Int32Type.instance.decompose(200));
        hits = countHits(expression, blockIndex, 1);
        assertEquals(99, hits);

        expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(3900));
        expression.add(Operator.LTE, Int32Type.instance.decompose(4100));
        hits = countHits(expression, blockIndex, 2);
        assertEquals(201, hits);
    }

    @Test
    public void testMultiBlockOne() throws Exception
    {
        IndexContext indexContext = createContext();

        MultiBlockIndex blockIndex = new MultiBlockIndex(indexContext);
        for (int x = 0; x < 100; x++)
        {
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(x));
            ByteBuffer value = Int32Type.instance.decompose(x);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        Expression expression = new Expression(indexContext);
        expression.add(Operator.EQ, Int32Type.instance.decompose(10));
        int hits = countHits(expression, blockIndex);
        assertEquals(1, hits);

        // no matches
        expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(100));
        expression.add(Operator.LTE, Int32Type.instance.decompose(200));
        hits = countHits(expression, blockIndex);
        assertEquals(0, hits);
    }

    @Test
    public void testCompareTrieIteratorRandom() throws Exception
    {
        IndexContext indexContext = createContext();

        MultiBlockIndex blockIndex = new MultiBlockIndex(indexContext);

        TrieMemoryIndex trieIndex = new TrieMemoryIndex(indexContext);

        // more than the block size to yield 2 internal blocks
        for (int x = 0; x < 6000; x++)
        {
            int valuei = nextInt(0, 6000);
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(valuei));

            ByteBuffer value = Int32Type.instance.decompose(valuei);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});

            trieIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        Iterator<Pair<ByteComparable, Iterable<ByteComparable>>> blockIterator = blockIndex.iterator();
        Iterator<Pair<ByteComparable, Iterable<ByteComparable>>> trieIterator = trieIndex.iterator();

        while (trieIterator.hasNext())
        {
            Pair<ByteComparable, Iterable<ByteComparable>> triePair = trieIterator.next();
            int trieValue = toInt(triePair.left);

            assertTrue(blockIterator.hasNext());

            Pair<ByteComparable, Iterable<ByteComparable>> blockPair = blockIterator.next();

            int blockValue = toInt(blockPair.left);
            assertEquals(trieValue, blockValue);

            Iterator<ByteComparable> trieKeys = triePair.right.iterator();
            Iterator<ByteComparable> blockKeys = blockPair.right.iterator();

            while (trieKeys.hasNext())
            {
                assertTrue(blockKeys.hasNext());

                ByteComparable trieKey = trieKeys.next();
                ByteComparable blockKey = blockKeys.next();

                assertTrue(ByteComparable.compare(trieKey, blockKey, ByteComparable.Version.OSS41) == 0);
            }
            assertFalse(blockKeys.hasNext());
        }
    }

    @Test
    public void testIteratorSorted() throws Exception
    {
        IndexContext indexContext = createContext();

        MultiBlockIndex blockIndex = new MultiBlockIndex(indexContext);
        // more than the block size to yield 2 internal blocks
        for (int x = 0; x < 6000; x++)
        {
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(x));
            ByteBuffer value = Int32Type.instance.decompose(x);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        Iterator<Pair<ByteComparable, Iterable<ByteComparable>>> iterator = blockIndex.iterator();
        int idx = 0;
        while (iterator.hasNext())
        {
            Pair<ByteComparable, Iterable<ByteComparable>> pair = iterator.next();
            int value = toInt(pair.left);
            assertEquals(idx, value);
            idx++;
        }
    }

    @Test
    public void testSingleBlockHits() throws Exception
    {
        IndexContext indexContext = createContext();

        BlockIndex blockIndex = new BlockIndex(indexContext);
        for (int x = 0; x < 100; x++)
        {
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(x));
            ByteBuffer value = Int32Type.instance.decompose(x);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        Expression expression = new Expression(indexContext);
        // test exact match query
        expression.add(Operator.EQ, Int32Type.instance.decompose(10));
        RangeIterator rangeIterator = blockIndex.search(expression, null);
        int hits = 0;
        while (rangeIterator.hasNext())
        {
            rangeIterator.next();
            hits++;
        }
        assertEquals(1, hits);

        // test range query
        expression.add(Operator.GTE, Int32Type.instance.decompose(10));
        expression.add(Operator.LTE, Int32Type.instance.decompose(20));
        rangeIterator = blockIndex.search(expression, null);
        hits = 0;
        while (rangeIterator.hasNext())
        {
            rangeIterator.next();
            hits++;
        }
        assertEquals(11, hits);
    }

    @Test
    public void testContains() throws Exception
    {
        IndexContext indexContext = createContext();

        BlockIndex blockIndex = new BlockIndex(indexContext);
        for (int x = 50; x < 100; x++)
        {
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(x));
            ByteBuffer value = Int32Type.instance.decompose(x);
            blockIndex.add(key, Clustering.EMPTY, value, bytes -> {}, bytes -> {});
        }

        Expression expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(10));
        expression.add(Operator.LTE, Int32Type.instance.decompose(20));
        assertFalse(blockIndex.contains(expression));

        expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(60));
        expression.add(Operator.LTE, Int32Type.instance.decompose(80));
        assertTrue(blockIndex.contains(expression));

        expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(90));
        expression.add(Operator.LTE, Int32Type.instance.decompose(150));
        assertTrue(blockIndex.contains(expression));

        expression = new Expression(indexContext);
        expression.add(Operator.GTE, Int32Type.instance.decompose(150));
        expression.add(Operator.LTE, Int32Type.instance.decompose(200));
        assertFalse(blockIndex.contains(expression));
    }

    public IndexContext createContext()
    {
        AbstractType columnType = Int32Type.instance;
        TableMetadata metadata = TableMetadata.builder(KEYSPACE, TABLE)
                                              .addPartitionKeyColumn(PART_KEY_COL, Int32Type.instance)
                                              .addRegularColumn(REG_COL, columnType)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .caching(CachingParams.CACHE_NOTHING)
                                              .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", REG_COL);
        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("col_index", IndexMetadata.Kind.CUSTOM, options);
        return new IndexContext(metadata, indexMetadata, MockSchema.newCFS(metadata));
    }

    public static int countHits(Expression expression, MultiBlockIndex blockIndex)
    {
        int hits = 0;
        RangeIterator rangeIterator = blockIndex.search(expression, null);

        if (rangeIterator == null)
        {
            return 0;
        }

        while (rangeIterator.hasNext())
        {
            rangeIterator.next();
            hits++;
        }
        return hits;
    }

    public static int countHits(Expression expression, MultiBlockIndex blockIndex, int expectedIteratorCount)
    {
        int hits = 0;
        RangeIterator rangeIterator = blockIndex.search(expression, null);

        if (rangeIterator == null)
        {
            return 0;
        }

        if (expectedIteratorCount >= 0)
        {
            if (expectedIteratorCount == 1)
            {
                assertTrue(rangeIterator instanceof BlockIndex.ValuesRangeIterator);
            }
            else
            {
                assertTrue(rangeIterator instanceof RangeUnionIterator);
                RangeUnionIterator unionIterator = (RangeUnionIterator)rangeIterator;
                assertEquals(expectedIteratorCount, unionIterator.numIterators());
            }
        }

        while (rangeIterator.hasNext())
        {
            rangeIterator.next();
            hits++;
        }
        return hits;
    }

    public static int toInt(ByteComparable comp)
    {
        ByteBuffer bytes = Int32Type.instance.fromComparableBytes(comp.asPeekableBytes(ByteComparable.Version.OSS41), ByteComparable.Version.OSS41);
        return Int32Type.instance.compose(bytes);
    }
}
