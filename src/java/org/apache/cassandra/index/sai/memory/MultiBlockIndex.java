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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.mutable.MutableInt;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.Sorter;

/**
 * Manages blocks of block indexes that are of size BLOCK_SIZE.  A block is filled to the block size,
 * then a new block is used.
 * <p>
 * At query time, blocks that fall within the key and value range are iterated on in merged primary key
 * order until a match is found.
 *
 * @see BlockIndex
 * <p>
 * Implements:
 * @see MemoryIndex
 */
@ThreadSafe
public class MultiBlockIndex extends MemoryIndex
{
    public static final int BLOCK_SIZE = 4000;
    final Object writeLock = new Object();
    protected final ConcurrentLinkedQueue<BlockIndex> existingBlocks = new ConcurrentLinkedQueue<>();
    protected final AtomicReference<BlockIndex> current = new AtomicReference<>();

    public MultiBlockIndex(IndexContext indexContext)
    {
        super(indexContext);
    }

    /**
     * Add a key and value to the current block index.
     *
     * @param key        Partition key
     * @param clustering Clustering key
     * @param value      Index value
     * @return Bytes memory used
     */
    @Override
    public void add(DecoratedKey key, Clustering clustering, ByteBuffer value, LongConsumer onHeapAllocationsTracker, LongConsumer offHeapAllocationsTracker)
    {
        synchronized (writeLock)
        {
            BlockIndex currentIndex = current.get();

            if (currentIndex == null)
                current.lazySet(currentIndex = new BlockIndex(indexContext, this));

            currentIndex.add(key, clustering, value, onHeapAllocationsTracker, offHeapAllocationsTracker);

            // the count may go over the block size from tokenized values
            if (currentIndex.count() >= BLOCK_SIZE)
            {
                existingBlocks.add(currentIndex);
                current.lazySet(null);
            }
        }
    }

    /**
     * Search the block indexes.
     *
     * @param expression Query
     * @return Matches range iterator
     */
    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        final BlockIndex currentIndex = current.get();

        final List<RangeIterator> iterators = new ArrayList<>();

        if (currentIndex != null && currentIndex.contains(expression))
        {
            RangeIterator iterator = currentIndex.search(expression, keyRange);
            if (iterator != null)
                iterators.add(iterator);
        }

        for (BlockIndex blockIndex : existingBlocks)
        {
            // avoid duplicate block indexes
            if (blockIndex != currentIndex && blockIndex.contains(expression))
            {
                RangeIterator iterator = blockIndex.search(expression, keyRange);
                if (iterator != null)
                    iterators.add(iterator);
            }
        }

        if (iterators.isEmpty())
            return RangeIterator.empty();

        return RangeUnionIterator.build(iterators);
    }

    /**
     * @return Sorted iterator over index.
     */
    @Override
    public Iterator<Pair<ByteComparable, Iterable<ByteComparable>>> iterator()
    {
        final boolean isLiteral = TypeUtil.isLiteral(indexContext.getValidator());

        if (current.get() != null)
        {
            assert !existingBlocks.contains(current.get());
            existingBlocks.add(current.get());
        }

        int totalSize = 0;
        for (BlockIndex blockIndex : existingBlocks)
            totalSize += blockIndex.count();

        final OffheapBytes[] values = new OffheapBytes[totalSize];
        final OffheapBytes[] keys = new OffheapBytes[totalSize];
        final MutableInt index = new MutableInt(0);

        int maxLength = -1;
        for (BlockIndex blockIndex : existingBlocks)
        {
            blockIndex.gatherAllValuesAndKeysBytes(values, keys, index);
            maxLength = Math.max(maxLength, blockIndex.maxTermLength());
        }

        if (maxLength == 0)
            return new Iterator<Pair<ByteComparable, Iterable<ByteComparable>>>()
            {
                @Override
                public boolean hasNext()
                {
                    return false;
                }

                @Override
                public Pair<ByteComparable, Iterable<ByteComparable>> next()
                {
                    throw new NoSuchElementException();
                }
            };

        assert index.intValue() == totalSize;

        // radix sort the bytesref arrays
        new ValuesKeysMSBRadixSorter(Integer.MAX_VALUE, values, keys).sort(0, totalSize);

        return new Iterator<Pair<ByteComparable, Iterable<ByteComparable>>>()
        {
            private int index = 0;
            private OffheapBytes lastValue = null;

            @Override
            public boolean hasNext()
            {
                iterateToNext();
                return index < values.length;
            }

            // required for non-exhaustive primary key iteration
            private void iterateToNext()
            {
                while (index < values.length)
                {
                    final OffheapBytes value = values[index];

                    if (lastValue != null && lastValue.compareTo(value) == 0)//lastValue.equals(value))
                        index++;
                    else
                        break;
                }
            }

            private ByteComparable decode(ByteComparable term)
            {
                return isLiteral ? version -> ByteSourceInverse.unescape(ByteSource.peekable(term.asComparableBytes(version)))
                                 : term;
            }

            @Override
            public Pair<ByteComparable, Iterable<ByteComparable>> next()
            {
                iterateToNext();

                final OffheapBytes startVal = values[index];
                lastValue = startVal;

                ByteComparable startValBC = startVal.toByteComparable();

                startValBC = decode(startValBC);

                return Pair.create(startValBC, () -> {
                    return new Iterator<ByteComparable>()
                    {
                        private OffheapBytes lastKey = null;

                        @Override
                        public boolean hasNext()
                        {
                            if (index >= values.length)
                                return false;
                            final OffheapBytes value = values[index];
                            final OffheapBytes key = keys[index];
                            final boolean sameValue = value.equals(startVal);
                            if (!sameValue)
                                return false;

                            // de-duplicate keys
                            if (lastKey != null && lastKey.equals(key))
                            {
                                // TODO: probably need to unroll this somehow?
                                next();
                                return hasNext();
                            }
                            return true;
                        }

                        @Override
                        public ByteComparable next()
                        {
                            final OffheapBytes key = keys[index];

                            index++;

                            lastKey = key;

                            return key.toByteComparable();
                        }
                    };
                });
            }
        };
    }

    static class ValuesKeysMSBRadixSorter extends MSBRadixSorter
    {
        final OffheapBytes[] values;
        final OffheapBytes[] keys;

        public ValuesKeysMSBRadixSorter(int maxLength,
                                        OffheapBytes[] values,
                                        OffheapBytes[] keys)
        {
            super(maxLength);
            this.values = values;
            this.keys = keys;
        }

        @Override
        protected void swap(int i, int j)
        {
            OffheapBytes tmpValue = values[i];
            values[i] = values[j];
            values[j] = tmpValue;

            OffheapBytes tmpKey = keys[i];
            keys[i] = keys[j];
            keys[j] = tmpKey;
        }

        @Override
        // return value + key bytes
        public int byteAt(int i, int k)
        {
            final OffheapBytes value = values[i];
            final OffheapBytes key = keys[i];

            final int totalLen = value.buffer.remaining() + key.buffer.remaining();

            if (totalLen <= k)
                return -1;

            if (k >= value.buffer.remaining())
                return key.buffer.get(key.buffer.position() + k - value.buffer.remaining()) & 0xff;

            return value.buffer.get(value.buffer.position() + k) & 0xff;
        }

        @Override
        protected Sorter getFallbackSorter(int k)
        {
            return new IntroSorter()
            {
                @Override
                protected void swap(int i, int j)
                {
                    ValuesKeysMSBRadixSorter.this.swap(i, j);
                }

                @Override
                protected int compare(int i, int j)
                {
                    for (int o = k; o < maxLength; ++o)
                    {
                        final int b1 = byteAt(i, o);
                        final int b2 = byteAt(j, o);
                        if (b1 != b2)
                        {
                            return b1 - b2;
                        }
                        else if (b1 == -1)
                        {
                            break;
                        }
                    }
                    return 0;
                }

                @Override
                protected void setPivot(int i)
                {
                    pivot.setLength(0);
                    for (int o = k; o < maxLength; ++o) {
                        final int b = byteAt(i, o);
                        if (b == -1) {
                            break;
                        }
                        pivot.append((byte) b);
                    }
                }

                @Override
                protected int comparePivot(int j)
                {
                    for (int o = 0; o < pivot.length(); ++o)
                    {
                        final int b1 = pivot.byteAt(o) & 0xff;
                        final int b2 = byteAt(j, k + o);
                        if (b1 != b2)
                        {
                            return b1 - b2;
                        }
                    }
                    if (k + pivot.length() == maxLength)
                    {
                        return 0;
                    }
                    return -1 - byteAt(j, k + pivot.length());
                }

                private final BytesRefBuilder pivot = new BytesRefBuilder();
            };
        }
    }
}
