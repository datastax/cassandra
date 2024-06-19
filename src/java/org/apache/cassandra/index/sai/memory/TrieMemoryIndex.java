/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithByteComparable;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.BinaryHeap;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class TrieMemoryIndex extends MemoryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemoryIndex.class);
    private static final int MINIMUM_QUEUE_SIZE = 128;
    private static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    private final InMemoryTrie<PrimaryKeys> data;
    private final PrimaryKeysReducer primaryKeysReducer;

    private final Memtable memtable;
    private AbstractBounds<PartitionPosition> keyBounds;

    private ByteBuffer minTerm;
    private ByteBuffer maxTerm;

    private static final FastThreadLocal<Integer> lastQueueSize = new FastThreadLocal<Integer>()
    {
        protected Integer initialValue()
        {
            return MINIMUM_QUEUE_SIZE;
        }
    };

    @VisibleForTesting
    public TrieMemoryIndex(IndexContext indexContext)
    {
        this(indexContext, null, AbstractBounds.unbounded(indexContext.getPartitioner()));
    }

    public TrieMemoryIndex(IndexContext indexContext, Memtable memtable, AbstractBounds<PartitionPosition> keyBounds)
    {
        super(indexContext);
        this.keyBounds = keyBounds;
        this.data = InMemoryTrie.longLived(TypeUtil.BYTE_COMPARABLE_VERSION, TrieMemtable.BUFFER_TYPE, indexContext.owner().readOrdering());
        this.primaryKeysReducer = new PrimaryKeysReducer();
        this.memtable = memtable;
    }

    public synchronized void add(DecoratedKey key,
                                 Clustering clustering,
                                 ByteBuffer value,
                                 LongConsumer onHeapAllocationsTracker,
                                 LongConsumer offHeapAllocationsTracker)
    {
        AbstractAnalyzer analyzer = indexContext.getAnalyzerFactory().create();
        try
        {
            value = TypeUtil.encode(value, indexContext.getValidator());
            analyzer.reset(value);
            final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);
            final long initialSizeOnHeap = data.usedSizeOnHeap();
            final long initialSizeOffHeap = data.usedSizeOffHeap();
            final long reducerHeapSize = primaryKeysReducer.heapAllocations();

            while (analyzer.hasNext())
            {
                final ByteBuffer term = analyzer.next();
                if (!indexContext.validateMaxTermSize(key, term))
                    continue;

                // Note that this term is already encoded once by the TypeUtil.encode call above.
                setMinMaxTerm(term.duplicate());

                final ByteComparable encodedTerm = encode(term.duplicate());

                try
                {
                    data.putSingleton(encodedTerm, primaryKey, primaryKeysReducer, term.limit() <= MAX_RECURSIVE_KEY_LENGTH);
                }
                catch (TrieSpaceExhaustedException e)
                {
                    Throwables.throwAsUncheckedException(e);
                }
            }

            onHeapAllocationsTracker.accept((data.usedSizeOnHeap() - initialSizeOnHeap) +
                                            (primaryKeysReducer.heapAllocations() - reducerHeapSize));
            offHeapAllocationsTracker.accept(data.usedSizeOffHeap() - initialSizeOffHeap);
        }
        finally
        {
            analyzer.end();
        }
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        Iterator<Map.Entry<ByteComparable, PrimaryKeys>> iterator = data.entrySet().iterator();
        return new Iterator<Pair<ByteComparable, PrimaryKeys>>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Pair<ByteComparable, PrimaryKeys> next()
            {
                Map.Entry<ByteComparable, PrimaryKeys> entry = iterator.next();
                return Pair.create(entry.getKey(), entry.getValue());
            }
        };
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, @Nullable Expression slice)
    {
        if (data.isEmpty())
            return CloseableIterator.emptyIterator();

        Trie<PrimaryKeys> subtrie = getSubtrie(slice);
        var iter = subtrie.entrySet(orderer.isAscending() ? Direction.FORWARD : Direction.REVERSE).iterator();
        return new AllTermsIterator(iter);
    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        if (logger.isTraceEnabled())
            logger.trace("Searching memtable index on expression '{}'...", expression);

        switch (expression.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return exactMatch(expression, keyRange);
            case RANGE:
                return rangeMatch(expression, keyRange);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }

    public RangeIterator exactMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        final ByteComparable prefix = expression.lower == null ? ByteComparable.EMPTY : encode(expression.lower.value.encoded);
        final PrimaryKeys primaryKeys = data.get(prefix);
        if (primaryKeys == null)
        {
            return RangeIterator.empty();
        }
        return new FilteringKeyRangeIterator(new SortedSetRangeIterator(primaryKeys.keys()), keyRange);
    }

    private RangeIterator rangeMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        Trie<PrimaryKeys> subtrie = getSubtrie(expression);

        var capacity = Math.max(MINIMUM_QUEUE_SIZE, lastQueueSize.get());
        var mergingIteratorBuilder = MergingRangeIterator.builder(keyBounds, indexContext.keyFactory(), capacity);
        lastQueueSize.set(mergingIteratorBuilder.size());

        if (!Version.latest().onOrAfter(Version.DB) && TypeUtil.isComposite(expression.validator))
            subtrie.entrySet().forEach(entry -> {
                // Before version DB, we encoded composite types using a non order-preserving function. In order to
                // perform a range query on a map, we use the bounds to get all entries for a given map key and then
                // only keep the map entries that satisfy the expression.
                byte[] key = ByteSourceInverse.readBytes(entry.getKey().asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
                if (expression.isSatisfiedBy(ByteBuffer.wrap(key)))
                    mergingIteratorBuilder.add(entry.getValue());
            });
        else
            subtrie.values().forEach(mergingIteratorBuilder::add);

        return mergingIteratorBuilder.isEmpty()
               ? RangeIterator.empty()
               : new FilteringKeyRangeIterator(mergingIteratorBuilder.build(), keyRange);
    }

    private Trie<PrimaryKeys> getSubtrie(@Nullable Expression expression)
    {
        if (expression == null)
            return data;

        ByteComparable lowerBound, upperBound;
        boolean lowerInclusive, upperInclusive;
        if (expression.lower != null)
        {
            lowerBound = expression.getEncodedLowerBoundByteComparable(Version.latest());
            lowerInclusive = expression.lower.inclusive;
        }
        else
        {
            lowerBound = ByteComparable.EMPTY;
            lowerInclusive = false;
        }

        if (expression.upper != null)
        {
            upperBound = expression.getEncodedUpperBoundByteComparable(Version.latest());
            upperInclusive = expression.upper.inclusive;
        }
        else
        {
            upperBound = null;
            upperInclusive = false;
        }

        return data.subtrie(lowerBound, lowerInclusive, upperBound, upperInclusive);
    }

    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    private void setMinMaxTerm(ByteBuffer term)
    {
        assert term != null;

        minTerm = TypeUtil.min(term, minTerm, indexContext.getValidator(), Version.latest());
        maxTerm = TypeUtil.max(term, maxTerm, indexContext.getValidator(), Version.latest());
    }

    private ByteComparable encode(ByteBuffer input)
    {
        return Version.latest().onDiskFormat().encodeForTrie(input, indexContext.getValidator());
    }


    class PrimaryKeysReducer implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations = new LongAdder();

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
            {
                existing = new PrimaryKeys();
                heapAllocations.add(existing.unsharedHeapSize());
            }
            heapAllocations.add(existing.add(neww));
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }

    /**
     * A sorting iterator over items that can either be singleton PrimaryKey or a SortedSetRangeIterator.
     */
    static class SortingSingletonOrSetIterator extends BinaryHeap
    {
        public SortingSingletonOrSetIterator(Collection<Object> data)
        {
            super(data.toArray());
            heapify();
        }

        @Override
        protected boolean greaterThan(Object a, Object b)
        {
            if (a == null || b == null)
                return b != null;

            return peek(a).compareTo(peek(b)) > 0;
        }

        public PrimaryKey nextOrNull()
        {
            Object key = top();
            if (key == null)
                return null;
            PrimaryKey result = peek(key);
            assert result != null;
            replaceTop(advanceItem(key));
            return result;
        }

        public void skipTo(PrimaryKey target)
        {
            advanceTo(target);
        }

        /**
         * Advance the given keys object to the next key.
         * If the keys object contains a single key, null is returned.
         * If the keys object contains more than one key, the first key is dropped and the iterator to the
         * remaining keys is returned.
         */
        @Override
        protected @Nullable Object advanceItem(Object keys)
        {
            if (keys instanceof PrimaryKey)
                return null;

            SortedSetRangeIterator iterator = (SortedSetRangeIterator) keys;
            assert iterator.hasNext();
            iterator.next();
            return iterator.hasNext() ? iterator : null;
        }

        /**
         * Advance the given keys object to the first element that is greater than or equal to the target key.
         * This is only called when the given item is known to be before the target key.
         * If the keys object contains a single key, null is returned.
         * If the keys object contains more than one key, it is skipped to the given target and the iterator to the
         * remaining keys is returned.
         */
        @Override
        protected @Nullable Object advanceItemTo(Object keys, Object target)
        {
            if (keys instanceof PrimaryKey)
                return null;

            SortedSetRangeIterator iterator = (SortedSetRangeIterator) keys;
            iterator.skipTo((PrimaryKey) target);
            return iterator.hasNext() ? iterator : null;
        }

        /**
         * Resolve a keys object to either its singleton value or the current element in the iterator.
         */
        static PrimaryKey peek(Object keys)
        {
            if (keys instanceof PrimaryKey)
                return (PrimaryKey) keys;
            if (keys instanceof SortedSetRangeIterator)
                return ((SortedSetRangeIterator) keys).peek();

            throw new AssertionError("Unreachable");
        }
    }

    static class MergingRangeIterator extends RangeIterator
    {
        // A sorting iterator of items that can be either singletons or SortedSetRangeIterator
        SortingSingletonOrSetIterator keySets;  // class invariant: each object placed in this queue contains at least one key

        MergingRangeIterator(Collection<Object> keySets,
                             PrimaryKey minKey,
                             PrimaryKey maxKey,
                             long count)
        {
            super(minKey, maxKey, count);

            this.keySets = new SortingSingletonOrSetIterator(keySets);
        }

        static Builder builder(AbstractBounds<PartitionPosition> keyRange, PrimaryKey.Factory factory, int capacity)
        {
            return new Builder(keyRange, factory, capacity);
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            keySets.skipTo(nextKey);
        }

        @Override
        protected PrimaryKey computeNext()
        {
            PrimaryKey result = keySets.nextOrNull();
            if (result == null)
                return endOfData();
            else
                return result;
        }

        @Override
        public void close() throws IOException
        {
        }

        static class Builder
        {
            final List<Object> keySets;

            private final PrimaryKey min;
            private final PrimaryKey max;
            private long count;


            Builder(AbstractBounds<PartitionPosition> keyRange, PrimaryKey.Factory factory, int capacity)
            {
                this.min = factory.createTokenOnly(keyRange.left.getToken());
                this.max = factory.createTokenOnly(keyRange.right.getToken());
                this.keySets = new ArrayList<>(capacity);
            }

            public void add(PrimaryKeys primaryKeys)
            {
                if (primaryKeys.isEmpty())
                    return;

                int size = primaryKeys.size();
                SortedSet<PrimaryKey> keys = primaryKeys.keys();
                if (size == 1)
                    keySets.add(keys.first());
                else
                    keySets.add(new SortedSetRangeIterator(keys, min, max, size));

                count += size;
            }

            public int size()
            {
                return keySets.size();
            }

            public boolean isEmpty()
            {
                return keySets.isEmpty();
            }

            public MergingRangeIterator build()
            {
                return new MergingRangeIterator(keySets, min, max, count);
            }
        }
    }

    static class SortedSetRangeIterator extends RangeIterator
    {
        private SortedSet<PrimaryKey> primaryKeySet;
        private Iterator<PrimaryKey> iterator;
        private PrimaryKey lastComputedKey;

        public SortedSetRangeIterator(SortedSet<PrimaryKey> source)
        {
            super(source.first(), source.last(), source.size());
            this.primaryKeySet = source;
        }

        private SortedSetRangeIterator(SortedSet<PrimaryKey> source, PrimaryKey min, PrimaryKey max, long count)
        {
            super(min, max, count);
            this.primaryKeySet = source;
        }


        @Override
        protected PrimaryKey computeNext()
        {
            // Skip can be called multiple times in a row, so defer iterator creation until needed
            if (iterator == null)
                iterator = primaryKeySet.iterator();
            lastComputedKey = iterator.hasNext() ? iterator.next() : endOfData();
            return lastComputedKey;
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            // Avoid going backwards
            if (lastComputedKey != null && nextKey.compareTo(lastComputedKey) <= 0)
                return;

            primaryKeySet = primaryKeySet.tailSet(nextKey);
            iterator = null;
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    private class AllTermsIterator extends AbstractIterator<PrimaryKeyWithSortKey>
    {
        private final Iterator<Map.Entry<ByteComparable, PrimaryKeys>> iterator;
        private Iterator<PrimaryKey> primaryKeysIterator = CloseableIterator.emptyIterator();
        private ByteComparable byteComparableTerm = null;

        public AllTermsIterator(Iterator<Map.Entry<ByteComparable, PrimaryKeys>> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        protected PrimaryKeyWithSortKey computeNext()
        {
            assert memtable != null;
            if (primaryKeysIterator.hasNext())
                return new PrimaryKeyWithByteComparable(indexContext, memtable, primaryKeysIterator.next(), byteComparableTerm);

            if (iterator.hasNext())
            {
                var entry = iterator.next();
                primaryKeysIterator = entry.getValue().keys().iterator();
                byteComparableTerm = entry.getKey();
                return new PrimaryKeyWithByteComparable(indexContext, memtable, primaryKeysIterator.next(), byteComparableTerm);
            }
            return endOfData();
        }
    }
}
