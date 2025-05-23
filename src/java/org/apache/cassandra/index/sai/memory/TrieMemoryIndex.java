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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.disk.v6.TermsDistribution;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithByteComparable;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.AbstractGuavaIterator;
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
    private final LongAdder primaryKeysHeapAllocations;
    private final PrimaryKeysAccumulator primaryKeysAccumulator;
    private final PrimaryKeysRemover primaryKeysRemover;
    private final boolean analyzerTransformsValue;
    private final Map<PrimaryKey, Integer> docLengths = new HashMap<>();
    private final AtomicInteger indexedRows = new AtomicInteger(0);

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
        this.primaryKeysHeapAllocations = new LongAdder();
        this.primaryKeysAccumulator = new PrimaryKeysAccumulator(primaryKeysHeapAllocations);
        this.primaryKeysRemover = new PrimaryKeysRemover(primaryKeysHeapAllocations);
        this.analyzerTransformsValue = indexContext.getAnalyzerFactory().create().transformValue();
        this.data = InMemoryTrie.longLived(TypeUtil.byteComparableVersionForTermsData(), TrieMemtable.BUFFER_TYPE, indexContext.columnFamilyStore().readOrdering());
        this.memtable = memtable;
    }

    public synchronized Map<PrimaryKey, Integer> getDocLengths()
    {
        return docLengths;
    }

    @Override
    public int indexedRows()
    {
        return indexedRows.get();
    }

    public synchronized void add(DecoratedKey key,
                                 Clustering clustering,
                                 ByteBuffer value,
                                 LongConsumer onHeapAllocationsTracker,
                                 LongConsumer offHeapAllocationsTracker)
    {
        final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);
        applyTransformer(primaryKey, value, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysAccumulator);
    }

    public synchronized void update(DecoratedKey key,
                                    Clustering clustering,
                                    ByteBuffer oldValue,
                                    ByteBuffer newValue,
                                    LongConsumer onHeapAllocationsTracker,
                                    LongConsumer offHeapAllocationsTracker)
    {
        final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);
        try
        {
            if (analyzerTransformsValue)
            {
                // Because an update can add and remove the same term, we collect the set of the seen PrimaryKeys
                // objects touched by the new values and pass it to the remover to prevent removing the PrimaryKey from
                // the PrimaryKeys object if it was updated during the add part of this update.
                var seenPrimaryKeys = new HashSet<PrimaryKeys>();
                primaryKeysAccumulator.setSeenPrimaryKeys(seenPrimaryKeys);
                primaryKeysRemover.setSeenPrimaryKeys(seenPrimaryKeys);
            }

            // Add before removing to prevent a period where the value is not available in the index
            if (newValue != null && newValue.hasRemaining())
                applyTransformer(primaryKey, newValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysAccumulator);
            if (oldValue != null && oldValue.hasRemaining())
                applyTransformer(primaryKey, oldValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysRemover);
        }
        finally
        {
            // Return the accumulator and remover to their default state.
            primaryKeysAccumulator.setSeenPrimaryKeys(null);
            primaryKeysRemover.setSeenPrimaryKeys(null);
        }
    }

    public synchronized void update(DecoratedKey key,
                                    Clustering clustering,
                                    Iterator<ByteBuffer> oldValues,
                                    Iterator<ByteBuffer> newValues,
                                    LongConsumer onHeapAllocationsTracker,
                                    LongConsumer offHeapAllocationsTracker)
    {
        final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);
        try
        {
            // Because an update can add and remove the same term, we collect the set of the seen PrimaryKeys
            // objects touched by the new values and pass it to the remover to prevent removing the PrimaryKey from
            // the PrimaryKeys object if it was updated during the add part of this update.
            var seenPrimaryKeys = new HashSet<PrimaryKeys>();
            primaryKeysAccumulator.setSeenPrimaryKeys(seenPrimaryKeys);
            primaryKeysRemover.setSeenPrimaryKeys(seenPrimaryKeys);

            // Add before removing to prevent a period where the values are not available in the index
            while (newValues != null && newValues.hasNext())
            {
                ByteBuffer newValue = newValues.next();
                if (newValue != null && newValue.hasRemaining())
                    applyTransformer(primaryKey, newValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysAccumulator);
            }

            while (oldValues != null && oldValues.hasNext())
            {
                ByteBuffer oldValue = oldValues.next();
                if (oldValue != null && oldValue.hasRemaining())
                    applyTransformer(primaryKey, oldValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysRemover);
            }
        }
        finally
        {
            // Return the accumulator and remover to their default state.
            primaryKeysAccumulator.setSeenPrimaryKeys(null);
            primaryKeysRemover.setSeenPrimaryKeys(null);
        }
    }

    private void applyTransformer(PrimaryKey primaryKey,
                                  ByteBuffer value,
                                  LongConsumer onHeapAllocationsTracker,
                                  LongConsumer offHeapAllocationsTracker,
                                  InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey> transformer)
    {
        AbstractAnalyzer analyzer = indexContext.getAnalyzerFactory().create();
        try
        {
            value = TypeUtil.asIndexBytes(value, indexContext.getValidator());
            analyzer.reset(value);
            final long initialSizeOnHeap = data.usedSizeOnHeap();
            final long initialSizeOffHeap = data.usedSizeOffHeap();
            final long initialPrimaryKeysHeapAllocations = primaryKeysHeapAllocations.longValue();

            int tokenCount = 0;
            while (analyzer.hasNext())
            {
                final ByteBuffer term = analyzer.next();
                if (!indexContext.validateMaxTermSize(primaryKey.partitionKey(), term))
                    continue;

                tokenCount++;

                // Note that this term is already encoded once by the TypeUtil.encode call above.
                setMinMaxTerm(term.duplicate());

                final ByteComparable encodedTerm = asByteComparable(term.duplicate());

                try
                {
                    data.putSingleton(encodedTerm, primaryKey, transformer, term.limit() <= MAX_RECURSIVE_KEY_LENGTH);
                }
                catch (TrieSpaceExhaustedException e)
                {
                    Throwables.throwAsUncheckedException(e);
                }
            }

            Object prev = docLengths.put(primaryKey, tokenCount);
            if (prev != null)
            {
                // heap used for doc lengths
                long heapUsed = RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
                                + primaryKey.ramBytesUsed() // TODO do we count these bytes?
                                + Integer.BYTES;
                onHeapAllocationsTracker.accept(heapUsed);
            }
            else
            {
                indexedRows.incrementAndGet();
            }

            // memory used by the trie
            onHeapAllocationsTracker.accept((data.usedSizeOnHeap() - initialSizeOnHeap) +
                                            (primaryKeysHeapAllocations.longValue() - initialPrimaryKeysHeapAllocations));
            offHeapAllocationsTracker.accept(data.usedSizeOffHeap() - initialSizeOffHeap);
        }
        finally
        {
            analyzer.end();
        }
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
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

    @Override
    public Iterator<Pair<ByteComparable.Preencoded, List<PkWithFrequency>>> iterator()
    {
        Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> iterator = data.entrySet().iterator();
        return new AbstractGuavaIterator<>()
        {
            @Override
            public Pair<ByteComparable.Preencoded, List<PkWithFrequency>> computeNext()
            {
                while (iterator.hasNext())
                {
                    Map.Entry<ByteComparable.Preencoded, PrimaryKeys> entry = iterator.next();
                    PrimaryKeys primaryKeys = entry.getValue();
                    if (primaryKeys.isEmpty())
                        continue;

                    var pairs = new ArrayList<PkWithFrequency>(primaryKeys.size());
                    Iterators.addAll(pairs, primaryKeys.iterator());
                    return Pair.create(entry.getKey(), pairs);
                }
                return endOfData();
            }
        };
    }

    @VisibleForTesting
    long estimatedTrieValuesMemoryUsed()
    {
        return primaryKeysHeapAllocations.longValue();
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

    private ByteComparable asByteComparable(ByteBuffer input)
    {
        return Version.current().onDiskFormat().encodeForTrie(input, indexContext.getValidator());
    }

    public KeyRangeIterator exactMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        final ByteComparable prefix = expression.lower == null ? ByteComparable.EMPTY : asByteComparable(expression.lower.value.encoded);
        final PrimaryKeys primaryKeys = data.get(prefix);
        if (primaryKeys == null || primaryKeys.keys().isEmpty())
        {
            return KeyRangeIterator.empty();
        }
        return new FilteringKeyRangeIterator(new SortedSetKeyRangeIterator(primaryKeys.keys()), keyRange);
    }

    /**
     * Accumulator that adds a primary key to the primary keys set.
     */
    static class PrimaryKeysAccumulator implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations;
        private HashSet<PrimaryKeys> seenPrimaryKeys;

        PrimaryKeysAccumulator(LongAdder heapAllocations)
        {
            this.heapAllocations = heapAllocations;
        }

        /**
         * Set the PrimaryKeys set to check for each PrimaryKeys object updated by this transformer.
         * Warning: This method is not thread-safe and should only be called from within the synchronized block
         * of the TrieMemoryIndex class.
         * @param seenPrimaryKeys the set of PrimaryKeys objects updated so far
         */
        private void setSeenPrimaryKeys(HashSet<PrimaryKeys> seenPrimaryKeys)
        {
            this.seenPrimaryKeys = seenPrimaryKeys;
        }

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
            {
                existing = new PrimaryKeys();
                heapAllocations.add(PrimaryKeys.unsharedHeapSize());
            }

            // If we are tracking PrimaryKeys via the seenPrimaryKeys set, then we need to reset the
            // counter on the first time seeing each PrimaryKeys object since an update means that the
            // frequency should be reset.
            boolean shouldResetFrequency = false;
            if (seenPrimaryKeys != null)
                shouldResetFrequency = seenPrimaryKeys.add(existing);

            long bytesAdded = shouldResetFrequency ? existing.addAndResetFrequency(neww)
                                                   : existing.addAndIncrementFrequency(neww);
            heapAllocations.add(bytesAdded);
            return existing;
        }
    }

    /**
     * Transformer that removes a primary key from the primary keys set, if present.
     */
    static class PrimaryKeysRemover implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations;
        private Set<PrimaryKeys> seenPrimaryKeys;

        PrimaryKeysRemover(LongAdder heapAllocations)
        {
            this.heapAllocations = heapAllocations;
        }

        /**
         * Set the set of seenPrimaryKeys.
         * Warning: This method is not thread-safe and should only be called from within the synchronized block
         * of the TrieMemoryIndex class.
         * @param seenPrimaryKeys
         */
        private void setSeenPrimaryKeys(Set<PrimaryKeys> seenPrimaryKeys)
        {
            this.seenPrimaryKeys = seenPrimaryKeys;
        }

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
                return null;

            // This PrimaryKeys object was already seen during the add part of this update,
            // so we skip removing the PrimaryKey from the PrimaryKeys class.
            if (seenPrimaryKeys != null && seenPrimaryKeys.contains(existing))
                return existing;

            heapAllocations.add(existing.remove(neww));
            return existing;
        }
    }

    /**
     * A sorting iterator over items that can either be singleton PrimaryKey or a SortedSetKeyRangeIterator.
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

            SortedSetKeyRangeIterator iterator = (SortedSetKeyRangeIterator) keys;
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

            SortedSetKeyRangeIterator iterator = (SortedSetKeyRangeIterator) keys;
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
            if (keys instanceof SortedSetKeyRangeIterator)
                return ((SortedSetKeyRangeIterator) keys).peek();

            throw new AssertionError("Unreachable");
        }
    }

    static class MergingKeyRangeIterator extends KeyRangeIterator
    {
        // A sorting iterator of items that can be either singletons or SortedSetKeyRangeIterator
        SortingSingletonOrSetIterator keySets;  // class invariant: each object placed in this queue contains at least one key

        MergingKeyRangeIterator(Collection<Object> keySets,
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
                    keySets.add(new SortedSetKeyRangeIterator(keys, min, max, size));

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

            public MergingKeyRangeIterator build()
            {
                return new MergingKeyRangeIterator(keySets, min, max, count);
            }
        }
    }

    static class SortedSetKeyRangeIterator extends KeyRangeIterator
    {
        private SortedSet<PrimaryKey> primaryKeySet;
        private Iterator<PrimaryKey> iterator;
        private PrimaryKey lastComputedKey;

        public SortedSetKeyRangeIterator(SortedSet<PrimaryKey> source)
        {
            super(source.first(), source.last(), source.size());
            this.primaryKeySet = source;
        }

        private SortedSetKeyRangeIterator(SortedSet<PrimaryKey> source, PrimaryKey min, PrimaryKey max, long count)
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

    private KeyRangeIterator rangeMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        Trie<PrimaryKeys> subtrie = getSubtrie(expression);

        var capacity = Math.max(MINIMUM_QUEUE_SIZE, lastQueueSize.get());
        var mergingIteratorBuilder = MergingKeyRangeIterator.builder(keyBounds, indexContext.keyFactory(), capacity);
        lastQueueSize.set(mergingIteratorBuilder.size());

        if (!Version.current().onOrAfter(Version.DB) && TypeUtil.isComposite(expression.validator))
            subtrie.entrySet().forEach(entry -> {
                // Before version DB, we encoded composite types using a non order-preserving function. In order to
                // perform a range query on a map, we use the bounds to get all entries for a given map key and then
                // only keep the map entries that satisfy the expression.
                assert entry.getKey().encodingVersion() == TypeUtil.BYTE_COMPARABLE_VERSION || Version.current() == Version.AA;
                byte[] key = ByteSourceInverse.readBytes(entry.getKey().getPreencodedBytes());
                if (expression.isSatisfiedBy(ByteBuffer.wrap(key)))
                    mergingIteratorBuilder.add(entry.getValue());
            });
        else
            subtrie.values().forEach(mergingIteratorBuilder::add);

        return mergingIteratorBuilder.isEmpty()
               ? KeyRangeIterator.empty()
               : new FilteringKeyRangeIterator(mergingIteratorBuilder.build(), keyRange);
    }

    @Override
    public long estimateMatchingRowsCount(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        switch (expression.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return estimateNumRowsMatchingExact(expression);
            case NOT_EQ:
            case NOT_CONTAINS_KEY:
            case NOT_CONTAINS_VALUE:
                if (TypeUtil.supportsRounding(expression.validator))
                    return Memtable.estimateRowCount(memtable);
                else
                    // need to clamp at 0, because row count is imprecise
                    return Math.max(0, Memtable.estimateRowCount(memtable) - estimateNumRowsMatchingExact(expression));
            case RANGE:
                return estimateNumRowsMatchingRange(expression);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }


    private int estimateNumRowsMatchingExact(Expression expression)
    {
        final ByteComparable prefix = expression.lower == null ? ByteComparable.EMPTY : asByteComparable(expression.lower.value.encoded);
        final PrimaryKeys primaryKeys = data.get(prefix);
        return primaryKeys == null ? 0 : primaryKeys.size();
    }

    private long estimateNumRowsMatchingRange(Expression expression)
    {
        final Trie<PrimaryKeys> subtrie = getSubtrie(expression);

        // We could compute the number of matching rows by iterating the subtrie
        // and summing the sizes of PrimaryKeys collections. But this could be very costly
        // if the subtrie is large. Instead, we iterate a limited number of entries, and then we
        // check how far we got by inspecting the term and comparing it to the start term and the end term.
        // For now, we assume that term values are distributed uniformly.

        var iterator = subtrie.entryIterator();
        if (!iterator.hasNext())
            return 0;

        AbstractType<?> termType = indexContext.getValidator();
        ByteBuffer endTerm = expression.upper != null && TypeUtil.compare(expression.upper.value.encoded, maxTerm, termType, Version.current()) < 0
                             ? expression.upper.value.encoded
                             : maxTerm;

        long pointCount = 0;
        long keyCount = 0;

        ByteComparable startTerm = null;
        ByteComparable currentTerm = null;

        while (iterator.hasNext() && pointCount < 64)
        {
            var entry = iterator.next();
            pointCount += 1;
            keyCount += entry.getValue().size();
            currentTerm = entry.getKey();
            if (startTerm == null)
                startTerm = currentTerm;
        }
        assert currentTerm != null;

        // We iterated all points matched by the query, so keyCount contains the exact value of keys.
        // This is a happy path, because the returned value will be accurate.
        if (!iterator.hasNext())
            return keyCount;

        // There are some points remaining; let's estimate their count by extrapolation.
        // Express the distance we iterated as a double value and the whole subtrie range also as a double.
        // Then the ratio of those two values would give us a hint on how many total points there
        // are in the subtrie. This should be fairly accurate assuming values are distributed uniformly.
        BigDecimal startValue = toBigDecimal(startTerm);
        BigDecimal endValue = toBigDecimal(endTerm);
        BigDecimal currentValue = toBigDecimal(currentTerm);
        double totalDistance = endValue.subtract(startValue).doubleValue() + Double.MIN_NORMAL;
        double iteratedDistance = currentValue.subtract(startValue).doubleValue() + Double.MIN_NORMAL;
        assert totalDistance > 0.0;
        assert iteratedDistance > 0.0;

        double extrapolatedPointCount = Math.min((pointCount - 1) * (totalDistance / iteratedDistance), this.data.valuesCount());
        double keysPerPoint = (double) keyCount / pointCount;
        return (long) (extrapolatedPointCount * keysPerPoint);
    }

    /**
     * Converts the term to a BigDecimal in a way that it keeps the sort order
     * (so terms comparing larger yield larger numbers).
     * Works on raw representation (as passed to the index).
     *
     * @see #toBigDecimal(ByteComparable)
     */
    private BigDecimal toBigDecimal(ByteBuffer endTerm)
    {
        ByteComparable bc = Version.current().onDiskFormat().encodeForTrie(endTerm, indexContext.getValidator());
        return toBigDecimal(bc);
    }

    /**
     * Converts the term to a BigDecimal in a way that it keeps the sort order
     * (so terms comparing larger yield larger numbers).
     * @see TermsDistribution#toBigDecimal(ByteComparable, AbstractType, Version, ByteComparable.Version)
     */
    private BigDecimal toBigDecimal(ByteComparable term)
    {
        AbstractType<?> type = indexContext.getValidator();
        return TermsDistribution.toBigDecimal(term, type, Version.current(), TypeUtil.BYTE_COMPARABLE_VERSION);
    }

    private Trie<PrimaryKeys> getSubtrie(@Nullable Expression expression)
    {
        if (expression == null)
            return data;

        ByteComparable lowerBound, upperBound;
        boolean lowerInclusive, upperInclusive;
        if (expression.lower != null)
        {
            lowerBound = expression.getEncodedLowerBoundByteComparable(Version.current());
            lowerInclusive = expression.lower.inclusive;
        }
        else
        {
            lowerBound = ByteComparable.EMPTY;
            lowerInclusive = false;
        }

        if (expression.upper != null)
        {
            upperBound = expression.getEncodedUpperBoundByteComparable(Version.current());
            upperInclusive = expression.upper.inclusive;
        }
        else
        {
            upperBound = null;
            upperInclusive = false;
        }

        return data.subtrie(lowerBound, lowerInclusive, upperBound, upperInclusive);
    }

    private class PrimaryKeysReducer implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
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
            heapAllocations.add(existing.addAndIncrementFrequency(neww));
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    private void setMinMaxTerm(ByteBuffer term)
    {
        assert term != null;

        // Note that an update to a term could make these inaccurate, but they err in the correct direction.
        // An alternative solution could use the trie to find the min/max term, but the trie has ByteComparable
        // objects, not the ByteBuffer, and we would need to implement a custom decoder to undo the encodeForTrie
        // mapping.
        minTerm = TypeUtil.min(term, minTerm, indexContext.getValidator(), Version.current());
        maxTerm = TypeUtil.max(term, maxTerm, indexContext.getValidator(), Version.current());
    }

    /**
     * Iterator that provides ordered access to all indexed terms and their associated primary keys
     * in the TrieMemoryIndex. For each term in the index, yields PrimaryKeyWithSortKey objects that
     * combine a primary key with its associated term.
     * <p>
     * A more verbose name could be KeysMatchingTermsByTermIterator.
     */
    private class AllTermsIterator extends AbstractIterator<PrimaryKeyWithSortKey>
    {
        private final Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> iterator;
        private Iterator<PrimaryKey> primaryKeysIterator = CloseableIterator.emptyIterator();
        private ByteComparable.Preencoded byteComparableTerm = null;

        public AllTermsIterator(Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        protected PrimaryKeyWithSortKey computeNext()
        {
            assert memtable != null;
            if (primaryKeysIterator.hasNext())
                return new PrimaryKeyWithByteComparable(indexContext, memtable, primaryKeysIterator.next(), byteComparableTerm);

            while (iterator.hasNext())
            {
                var entry = iterator.next();
                primaryKeysIterator = entry.getValue().keys().iterator();
                if (!primaryKeysIterator.hasNext())
                    continue;
                byteComparableTerm = entry.getKey();
                return new PrimaryKeyWithByteComparable(indexContext, memtable, primaryKeysIterator.next(), byteComparableTerm);
            }
            return endOfData();
        }
    }
}
