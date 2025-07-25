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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Runnables;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.AbstractMemtableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeConcatIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeLazyIterator;
import org.apache.cassandra.index.sai.memory.MemoryIndex.PkWithFrequency;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.BM25Utils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithByteComparable;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.SortingIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class TrieMemtableIndex extends AbstractMemtableIndex
{
    private final ShardBoundaries boundaries;
    private final MemoryIndex[] rangeIndexes;
    private final IndexContext indexContext;
    private final AbstractType<?> validator;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedOnHeapMemoryUsed = new LongAdder();
    private final LongAdder estimatedOffHeapMemoryUsed = new LongAdder();

    private final Context sensorContext;
    private final RequestTracker requestTracker;

    public TrieMemtableIndex(IndexContext indexContext, Memtable memtable)
    {
        this(indexContext, memtable, TrieMemtable.SHARD_COUNT);
    }

    @VisibleForTesting
    public TrieMemtableIndex(IndexContext indexContext, Memtable memtable, int shardCount)
    {
        super(indexContext, memtable);
        this.boundaries = indexContext.columnFamilyStore().localRangeSplits(shardCount);
        this.rangeIndexes = new MemoryIndex[boundaries.shardCount()];
        this.indexContext = indexContext;
        this.validator = indexContext.getValidator();
        for (int shard = 0; shard < boundaries.shardCount(); shard++)
        {
            this.rangeIndexes[shard] = new TrieMemoryIndex(indexContext, memtable, boundaries.getBounds(shard));
        }
        this.sensorContext = Context.from(indexContext);
        this.requestTracker = RequestTracker.instance;
    }

    @Override
    public Memtable getMemtable()
    {
        return memtable;
    }

    @Override
    public int getRowCount()
    {
        int size = 0;
        for (MemoryIndex memoryIndex : rangeIndexes)
            size += memoryIndex.indexedRows();
        return size;
    }

    /**
     * Approximate total count of terms in the memory index.
     * The count is approximate because some deletions are not accounted for in the current implementation.
     *
     * @return total count of terms for indexes rows.
     */
    @Override
    public long getApproximateTermCount()
    {
        long count = 0;
        for (MemoryIndex memoryIndex : rangeIndexes)
        {
            assert memoryIndex instanceof TrieMemoryIndex;
            count += ((TrieMemoryIndex) memoryIndex).approximateTotalTermCount();
        }
        return count;
    }

    @VisibleForTesting
    public int shardCount()
    {
        return rangeIndexes.length;
    }

    @Override
    public long writeCount()
    {
        return writeCount.sum();
    }

    @Override
    public long estimatedOnHeapMemoryUsed()
    {
        return estimatedOnHeapMemoryUsed.sum();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return estimatedOffHeapMemoryUsed.sum();
    }

    @Override
    public boolean isEmpty()
    {
        return getMinTerm() == null;
    }

    // Returns the minimum indexed term in the combined memory indexes.
    // This can be null if the indexed memtable was empty. Users of the
    // {@code MemtableIndex} requiring a non-null minimum term should
    // use the {@link MemtableIndex#isEmpty} method.
    // Note: Individual index shards can return null here if the index
    // didn't receive any terms within the token range of the shard
    @Override
    @Nullable
    public ByteBuffer getMinTerm()
    {
        return Arrays.stream(rangeIndexes)
                     .map(MemoryIndex::getMinTerm)
                     .filter(Objects::nonNull)
                     .reduce((a, b) -> TypeUtil.min(a, b, validator, Version.current()))
                     .orElse(null);
    }

    // Returns the maximum indexed term in the combined memory indexes.
    // This can be null if the indexed memtable was empty. Users of the
    // {@code MemtableIndex} requiring a non-null maximum term should
    // use the {@link MemtableIndex#isEmpty} method.
    // Note: Individual index shards can return null here if the index
    // didn't receive any terms within the token range of the shard
    @Override
    @Nullable
    public ByteBuffer getMaxTerm()
    {
        return Arrays.stream(rangeIndexes)
                     .map(MemoryIndex::getMaxTerm)
                     .filter(Objects::nonNull)
                     .reduce((a, b) -> TypeUtil.max(a, b, validator, Version.current()))
                     .orElse(null);
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        if (value == null || (value.remaining() == 0 && TypeUtil.skipsEmptyValue(validator)))
            return;

        RequestSensors sensors = requestTracker.get();
        if (sensors != null)
            sensors.registerSensor(sensorContext, Type.INDEX_WRITE_BYTES);
        rangeIndexes[boundaries.getShardForKey(key)].add(key,
                                                         clustering,
                                                         value,
                                                         allocatedBytes -> {
                                                             memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
                                                             estimatedOnHeapMemoryUsed.add(allocatedBytes);
                                                             if (sensors != null)
                                                                 sensors.incrementSensor(sensorContext, Type.INDEX_WRITE_BYTES, allocatedBytes);
                                                         },
                                                         allocatedBytes -> {
                                                             memtable.markExtraOffHeapUsed(allocatedBytes, opGroup);
                                                             estimatedOffHeapMemoryUsed.add(allocatedBytes);
                                                             if (sensors != null)
                                                                 sensors.incrementSensor(sensorContext, Type.INDEX_WRITE_BYTES, allocatedBytes);
                                                         });
        writeCount.increment();
        onIndexUpdated();
    }

    @Override
    public void update(DecoratedKey key, Clustering clustering, ByteBuffer oldValue, ByteBuffer newValue, Memtable memtable, OpOrder.Group opGroup)
    {
        int oldRemaining = oldValue == null ? 0 : oldValue.remaining();
        int newRemaining = newValue == null ? 0 : newValue.remaining();
        if (oldRemaining == 0 && newRemaining == 0)
            return;

        if (oldRemaining == newRemaining && validator.compare(oldValue, newValue) == 0)
            return;

        // The terms inserted into the index could still be the same in the case of certain analyzer configs.
        // We don't know yet though, and instead of eagerly determining it, we leave it to the index to handle it.
        rangeIndexes[boundaries.getShardForKey(key)].update(key,
                                                            clustering,
                                                            oldValue,
                                                            newValue,
                                                            allocatedBytes -> {
                                                                memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
                                                                estimatedOnHeapMemoryUsed.add(allocatedBytes);
                                                                },
                                                            allocatedBytes -> {
                                                                memtable.markExtraOffHeapUsed(allocatedBytes, opGroup);
                                                                estimatedOffHeapMemoryUsed.add(allocatedBytes);
                                                            });
        writeCount.increment();
        onIndexUpdated();
    }

    @Override
    public void update(DecoratedKey key, Clustering clustering, Iterator<ByteBuffer> oldValues, Iterator<ByteBuffer> newValues, Memtable memtable, OpOrder.Group opGroup)
    {
        // We defer on comparing old and new values here. Instead, we rely on the index to do the comparison and then
        // have custom logic in the aggregator to ensure that we properly add/keep new values and remove old values
        // that are not present in the new values.
        rangeIndexes[boundaries.getShardForKey(key)].update(key,
                                                            clustering,
                                                            oldValues,
                                                            newValues,
                                                            allocatedBytes -> {
                                                                memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
                                                                estimatedOnHeapMemoryUsed.add(allocatedBytes);
                                                            },
                                                            allocatedBytes -> {
                                                                memtable.markExtraOffHeapUsed(allocatedBytes, opGroup);
                                                                estimatedOffHeapMemoryUsed.add(allocatedBytes);
                                                            });
        writeCount.increment();
    }

    public KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        int startShard = boundaries.getShardForToken(keyRange.left.getToken());
        int endShard = getEndShardForBounds(keyRange);

        KeyRangeConcatIterator.Builder builder = KeyRangeConcatIterator.builder(endShard - startShard + 1);

        // We want to run the search on the first shard only to get the estimate on the number of matching keys.
        // But we don't want to run the search on the other shards until the user polls more items from the
        // result iterator. Therefore, the first shard search is special - we run the search eagerly,
        // but the rest of the iterators are create lazily in the loop below.
        assert rangeIndexes[startShard] != null;
        KeyRangeIterator firstIterator = rangeIndexes[startShard].search(expression, keyRange);
        // Assume all shards are the same size, but we must not pass 0 because of some checks in KeyRangeIterator
        // that assume 0 means empty iterator and could fail.
        var keyCount = Math.max(1, firstIterator.getMaxKeys());
        builder.add(firstIterator);

        // Prepare the search on the remaining shards, but wrap them in KeyRangeLazyIterator, so they don't run
        // until the user exhaust the results given from the first shard.
        for (int shard  = startShard + 1; shard <= endShard; ++shard)
        {
            assert rangeIndexes[shard] != null;
            var index = rangeIndexes[shard];
            var shardRange = boundaries.getBounds(shard);
            var minKey = index.indexContext.keyFactory().createTokenOnly(shardRange.left.getToken());
            var maxKey = index.indexContext.keyFactory().createTokenOnly(shardRange.right.getToken());
            builder.add(new KeyRangeLazyIterator(() -> index.search(expression, keyRange), minKey, maxKey, keyCount));
        }

        return builder.build();
    }

    public KeyRangeIterator eagerSearch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        int startShard = boundaries.getShardForToken(keyRange.left.getToken());
        int endShard = getEndShardForBounds(keyRange);

        KeyRangeConcatIterator.Builder builder = KeyRangeConcatIterator.builder(endShard - startShard + 1);
        for (int shard = startShard; shard <= endShard; ++shard)
        {
            assert rangeIndexes[shard] != null;
            builder.add(rangeIndexes[shard].search(expression, keyRange));
        }
        return builder.build();
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(QueryContext queryContext,
                                                                  Orderer orderer,
                                                                  Expression slice,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  int limit)
    {
        int startShard = boundaries.getShardForToken(keyRange.left.getToken());
        int endShard = getEndShardForBounds(keyRange);

        if (orderer.isBM25())
        {
            // Intersect iterators to find documents containing all terms
            List<ByteBuffer> queryTerms = orderer.getQueryTerms();
            List<KeyRangeIterator> termIterators = new ArrayList<>(queryTerms.size());
            for (ByteBuffer term : queryTerms)
            {
                Expression expr = new Expression(indexContext).add(Operator.ANALYZER_MATCHES, term);
                KeyRangeIterator iterator = eagerSearch(expr, keyRange);
                termIterators.add(iterator);
            }
            KeyRangeIterator intersectedIterator = KeyRangeIntersectionIterator.builder(termIterators).build();

            return List.of(orderByBM25(Streams.stream(intersectedIterator), orderer));
        }
        else
        {
            var iterators = new ArrayList<CloseableIterator<PrimaryKeyWithSortKey>>(endShard - startShard + 1);
            for (int shard = startShard; shard <= endShard; ++shard)
            {
                assert rangeIndexes[shard] != null;
                iterators.add(rangeIndexes[shard].orderBy(orderer, slice));
            }
            return iterators;
        }
    }

    /**
     * Estimates the number of matching rows by extrapolating from the first shard only.
     * This method provides a fast approximation by calculating the matching row count for the first
     * shard in the key range and then multiplying by the total number of shards involved.
     *
     * <p>This approach assumes that data is uniformly distributed across shards, which may not
     * always be accurate but provides a quick estimate with minimal computational overhead.
     * The estimate is particularly useful for query planning and optimization decisions where
     * speed is more important than precision.</p>
     *
     * @param expression the search expression/predicate to match against indexed terms
     * @param keyRange   the partition key range to search within, used to determine which shards to consider
     * @return an estimated number of matching rows extrapolated from the first shard;
     * @see #estimateMatchingRowsCountUsingAllShards(Expression, AbstractBounds) for a more accurate but slower alternative
     */
    @Override
    public long estimateMatchingRowsCountUsingFirstShard(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        int startShard = boundaries.getShardForToken(keyRange.left.getToken());
        int endShard = getEndShardForBounds(keyRange);
        return rangeIndexes[startShard].estimateMatchingRowsCount(expression, keyRange) * (endShard - startShard + 1);
    }

    /**
     * Estimates the number of matching rows by querying all relevant shards individually.
     * This method provides a more accurate estimate compared to the first-shard extrapolation
     * approach by actually calculating the matching row count for each shard that intersects
     * with the given key range and summing the results.
     *
     * <p>This approach accounts for non-uniform data distribution across shards and provides
     * a more precise estimate at the cost of increased computational overhead. Each shard's
     * memory index is consulted to determine how many rows would match the given expression
     * within the specified key range.</p>
     *
     * @param expression the search expression/predicate to match against indexed terms
     * @param keyRange the partition key range to search within, used to determine which shards to query
     * @return the sum of estimated matching rows from all relevant shards;
     *
     * @see #estimateMatchingRowsCountUsingFirstShard(Expression, AbstractBounds) for a faster but less accurate alternative
     */
    @Override
    public long estimateMatchingRowsCountUsingAllShards(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        int startShard = boundaries.getShardForToken(keyRange.left.getToken());
        int endShard = getEndShardForBounds(keyRange);
        long count = 0;
        for (int shard = startShard; shard <= endShard; ++shard)
        {
            assert rangeIndexes[shard] != null;
            count += rangeIndexes[shard].estimateMatchingRowsCount(expression, keyRange);
        }
        return count;
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(QueryContext queryContext, List<PrimaryKey> keys, Orderer orderer, int limit)
    {
        if (keys.isEmpty())
            return CloseableIterator.emptyIterator();

        if (orderer.isBM25())
            return orderByBM25(keys.stream(), orderer);
        else
            return SortingIterator.createCloseable(
                orderer.getComparator(),
                keys,
                key ->
                {
                    var partition = memtable.getPartition(key.partitionKey());
                    if (partition == null)
                        return null;
                    var row = partition.getRow(key.clustering());
                    if (row == null)
                        return null;
                    var cell = row.getCell(indexContext.getDefinition());
                    if (cell == null)
                        return null;

                    // We do two kinds of encoding... it'd be great to make this more straight forward, but this is what
                    // we have for now. I leave it to the reader to inspect the two methods to see the nuanced differences.
                    var encoding = encode(TypeUtil.encode(cell.buffer(), validator));
                    return new PrimaryKeyWithByteComparable(indexContext, memtable, key, encoding);
                },
                Runnables.doNothing()
            );
    }

    private CloseableIterator<PrimaryKeyWithSortKey> orderByBM25(Stream<PrimaryKey> stream, Orderer orderer)
    {
        assert orderer.isBM25();
        List<ByteBuffer> queryTerms = orderer.getQueryTerms();
        AbstractAnalyzer analyzer = indexContext.getAnalyzerFactory().create();
        Iterator<BM25Utils.DocTF> it = stream
                                       .map(pk -> BM25Utils.EagerDocTF.createFromDocument(pk, getCellForKey(pk), analyzer, queryTerms))
                                       .filter(Objects::nonNull)
                                       .iterator();
        return BM25Utils.computeScores(CloseableIterator.wrap(it),
                                       queryTerms,
                                       orderer.bm25stats,
                                       indexContext,
                                       memtable,
                                       false);
    }

    @Nullable
    private org.apache.cassandra.db.rows.Cell<?> getCellForKey(PrimaryKey key)
    {
        var partition = memtable.getPartition(key.partitionKey());
        if (partition == null)
            return null;
        var row = partition.getRow(key.clustering());
        if (row == null)
            return null;
        return row.getCell(indexContext.getDefinition());
    }

    private ByteComparable encode(ByteBuffer input)
    {
        return Version.current().onDiskFormat().encodeForTrie(input, indexContext.getValidator());
    }

    private int getEndShardForBounds(AbstractBounds<PartitionPosition> bounds)
    {
        PartitionPosition position = bounds.right;
        return position.isMinimum() ? boundaries.shardCount() - 1
                                    : boundaries.getShardForToken(position.getToken());
    }

    /**
     * NOTE: returned data may contain partition key not within the provided min and max which are only used to find
     * corresponding subranges. We don't do filtering here to avoid unnecessary token comparison. In case of JBOD,
     * min/max should align exactly at token boundaries. In case of tiered-storage, keys within min/max may not
     * belong to the given sstable.
     *
     * @param min minimum partition key used to find min subrange
     * @param max maximum partition key used to find max subrange
     *
     * @return iterator of indexed term to primary keys mapping in sorted by indexed term and primary key.
     */
    @Override
    public Iterator<Pair<ByteComparable.Preencoded, List<PkWithFrequency>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        int minSubrange = min == null ? 0 : boundaries.getShardForKey(min);
        int maxSubrange = max == null ? rangeIndexes.length - 1 : boundaries.getShardForKey(max);

        List<Iterator<Pair<ByteComparable.Preencoded, List<PkWithFrequency>>>> rangeIterators = new ArrayList<>(maxSubrange - minSubrange + 1);
        for (int i = minSubrange; i <= maxSubrange; i++)
            rangeIterators.add(rangeIndexes[i].iterator());

        return MergeIterator.get(rangeIterators,
                                 (o1, o2) -> ByteComparable.compare(o1.left, o2.left),
                                 new PrimaryKeysMergeReducer(rangeIterators.size()));
    }

    /**
     * Used to merge sorted primary keys from multiple TrieMemoryIndex shards for a given indexed term.
     * For each term that appears in multiple shards, the reducer:
     * 1. Receives exactly one call to reduce() per shard containing that term
     * 2. Merges all the primary keys for that term via getReduced()
     * 3. Resets state via onKeyChange() before processing the next term
     * <p>
     * While this follows the Reducer pattern, its "reduction" operation is a simple merge since each term
     * appears at most once per shard, and each key will only be found in a given shard, so there are no values to aggregate;
     * we simply combine and sort the primary keys from each shard that contains the term.
     */
    private static class PrimaryKeysMergeReducer extends Reducer<Pair<ByteComparable.Preencoded, List<PkWithFrequency>>, Pair<ByteComparable.Preencoded, List<PkWithFrequency>>>
    {
        private final Pair<ByteComparable.Preencoded, List<PkWithFrequency>>[] rangeIndexEntriesToMerge;
        private final Comparator<PrimaryKey> comparator;

        private ByteComparable.Preencoded term;

        @SuppressWarnings("unchecked")
            // The size represents the number of range indexes that have been selected for the merger
        PrimaryKeysMergeReducer(int size)
        {
            this.rangeIndexEntriesToMerge = new Pair[size];
            this.comparator = PrimaryKey::compareTo;
        }

        @Override
        // Receive the term entry for a range index. This should only be called once for each
        // range index before reduction.
        public void reduce(int index, Pair<ByteComparable.Preencoded, List<PkWithFrequency>> termPair)
        {
            Preconditions.checkArgument(rangeIndexEntriesToMerge[index] == null, "Terms should be unique in the memory index");

            rangeIndexEntriesToMerge[index] = termPair;
            if (termPair != null && term == null)
                term = termPair.left;
        }

        @Override
        // Return a merger of the term keys for the term.
        public Pair<ByteComparable.Preencoded, List<PkWithFrequency>> getReduced()
        {
            Preconditions.checkArgument(term != null, "The term must exist in the memory index");

            var merged = new ArrayList<PkWithFrequency>();
            for (var p : rangeIndexEntriesToMerge)
                if (p != null && p.right != null)
                    merged.addAll(p.right);

            merged.sort((o1, o2) -> comparator.compare(o1.pk, o2.pk));
            return Pair.create(term, merged);
        }

        @Override
        public void onKeyChange()
        {
            Arrays.fill(rangeIndexEntriesToMerge, null);
            term = null;
        }
    }

    @VisibleForTesting
    public MemoryIndex[] getRangeIndexes()
    {
        return rangeIndexes;
    }
}
