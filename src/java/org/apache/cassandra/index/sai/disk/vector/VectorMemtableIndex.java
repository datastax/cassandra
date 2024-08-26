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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.PriorityQueueIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class VectorMemtableIndex implements MemtableIndex
{
    private static final Logger logger = LoggerFactory.getLogger(VectorMemtableIndex.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    public static int GLOBAL_BRUTE_FORCE_ROWS = Integer.MAX_VALUE; // not final so test can inject its own setting

    private final IndexContext indexContext;
    private final CassandraOnHeapGraph<PrimaryKey> graph;
    private final LongAdder writeCount = new LongAdder();

    private PrimaryKey minimumKey;
    private PrimaryKey maximumKey;

    private final NavigableSet<PrimaryKey> primaryKeys = new ConcurrentSkipListSet<>();
    private final Memtable mt;

    public VectorMemtableIndex(IndexContext indexContext, Memtable mt)
    {
        this.indexContext = indexContext;
        this.graph = new CassandraOnHeapGraph<>(indexContext, true);
        this.mt = mt;
    }

    @Override
    public Memtable getMemtable()
    {
        return mt;
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        if (value == null || value.remaining() == 0)
            return;

        var primaryKey = indexContext.keyFactory().create(key, clustering);
        long allocatedBytes = index(primaryKey, value);
        memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
    }

    private long index(PrimaryKey primaryKey, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;

        updateKeyBounds(primaryKey);

        writeCount.increment();
        primaryKeys.add(primaryKey);
        return graph.add(value, primaryKey);
    }

    @Override
    public void update(DecoratedKey key, Clustering clustering, ByteBuffer oldValue, ByteBuffer newValue, Memtable memtable, OpOrder.Group opGroup)
    {
        int oldRemaining = oldValue == null ? 0 : oldValue.remaining();
        int newRemaining = newValue == null ? 0 : newValue.remaining();
        if (oldRemaining == 0 && newRemaining == 0)
            return;

        boolean different;
        if (oldRemaining != newRemaining)
        {
            assert oldRemaining == 0 || newRemaining == 0; // one of them is null
            different = true;
        }
        else
        {
            different = indexContext.getValidator().compare(oldValue, newValue) != 0;
        }

        if (different)
        {
            var primaryKey = indexContext.keyFactory().create(key, clustering);
            // update bounds because only rows with vectors are included in the key bounds,
            // so if the vector was null before, we won't have included it
            updateKeyBounds(primaryKey);

            // make the changes in this order so we don't have a window where the row is not in the index at all
            if (newRemaining > 0)
                graph.add(newValue, primaryKey);
            if (oldRemaining > 0)
                graph.remove(oldValue, primaryKey);

            // remove primary key if it's no longer indexed
            if (newRemaining <= 0 && oldRemaining > 0)
                primaryKeys.remove(primaryKey);
        }
    }

    private void updateKeyBounds(PrimaryKey primaryKey) {
        if (minimumKey == null)
            minimumKey = primaryKey;
        else if (primaryKey.compareTo(minimumKey) < 0)
            minimumKey = primaryKey;
        if (maximumKey == null)
            maximumKey = primaryKey;
        else if (primaryKey.compareTo(maximumKey) > 0)
            maximumKey = primaryKey;
    }

    @Override
    public RangeIterator search(QueryContext context, Expression expr, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        if (expr.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Only BOUNDED_ANN is supported, received: " + expr));
        var qv = vts.createFloatVector(expr.lower.value.vector);
        float threshold = expr.getEuclideanSearchThreshold();

        PriorityQueue<PrimaryKey> keyQueue;
        try (var pkIterator = searchInternal(context, qv, keyRange, graph.size(), threshold))
        {
            // Leverage PQ's O(N) complexity for building a PQ from a list.
            var list = Lists.newArrayList(Iterators.transform(pkIterator, PrimaryKeyWithSortKey::primaryKey));
            keyQueue = new PriorityQueue<>(list);
        }

        if (keyQueue.isEmpty())
            return RangeIterator.empty();
        return new ReorderingRangeIterator(keyQueue);
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(QueryContext context,
                                                                  Orderer orderer,
                                                                  Expression slice,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  int limit)
    {
        assert slice == null : "ANN does not support index slicing";
        assert orderer.isANN() : "Only ANN is supported for vector search, received " + orderer.operator;

        var qv = vts.createFloatVector(orderer.vector);

        return List.of(searchInternal(context, qv, keyRange, limit, 0));
    }

    private CloseableIterator<PrimaryKeyWithSortKey> searchInternal(QueryContext context,
                                                                  VectorFloat<?> queryVector,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  int limit,
                                                                  float threshold)
    {
        Bits bits;
        if (RangeUtil.coversFullRing(keyRange))
        {
            bits = Bits.ALL;
        }
        else
        {
            // if left bound is MIN_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean leftInclusive = keyRange.left.kind() != PartitionPosition.Kind.MAX_BOUND;
            // if right bound is MAX_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean rightInclusive = keyRange.right.kind() != PartitionPosition.Kind.MIN_BOUND;
            // if right token is MAX (Long.MIN_VALUE), there is no upper bound
            boolean isMaxToken = keyRange.right.getToken().isMinimum(); // max token

            PrimaryKey left = indexContext.keyFactory().createTokenOnly(keyRange.left.getToken()); // lower bound
            PrimaryKey right = isMaxToken ? null : indexContext.keyFactory().createTokenOnly(keyRange.right.getToken()); // upper bound

            NavigableSet<PrimaryKey> resultKeys = isMaxToken ? primaryKeys.tailSet(left, leftInclusive)
                                                             : primaryKeys.subSet(left, leftInclusive, right, rightInclusive);

            if (resultKeys.isEmpty())
                return CloseableIterator.emptyIterator();

            int bruteForceRows = maxBruteForceRows(limit, resultKeys.size(), graph.size());
            logger.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                         resultKeys.size(), bruteForceRows, graph.size(), limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                          resultKeys.size(), bruteForceRows, graph.size(), limit);
            if (resultKeys.size() <= bruteForceRows)
                // When we have a threshold, we only need to filter the results, not order them, because it means we're
                // evaluating a boolean predicate in the SAI pipeline that wants to collate by PK
                if (threshold > 0)
                    return filterByBruteForce(queryVector, threshold, resultKeys);
                else
                    return orderByBruteForce(queryVector, resultKeys);
            else
                bits = new KeyRangeFilteringBits(keyRange);
        }

        var nodeScoreIterator = graph.search(context, queryVector, limit, threshold, bits);
        return new NodeScoreToScoredPrimaryKeyIterator(nodeScoreIterator);
    }


    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit)
    {
        if (minimumKey == null)
            // This case implies maximumKey is empty too.
            return CloseableIterator.emptyIterator();

        assert orderer.isANN() : "Only ANN is supported for vector search, received " + orderer;
        // Compute the keys that exist in the current memtable and their corresponding graph ordinals
        var keysInGraph = new HashSet<PrimaryKey>();
        var relevantOrdinals = new IntHashSet();
        keys.stream()
            .dropWhile(k -> k.compareTo(minimumKey) < 0)
            .takeWhile(k -> k.compareTo(maximumKey) <= 0)
            .forEach(k ->
        {
            var v = graph.vectorForKey(k);
            if (v == null)
                return;
            var i = graph.getOrdinal(v);
            if (i < 0)
                // might happen if the vector and/or its postings have been removed in the meantime between getting the
                // vector and getting the ordinal (graph#vectorForKey and graph#getOrdinal are not synchronized)
                return;
            keysInGraph.add(k);
            relevantOrdinals.add(i);
        });

        int maxBruteForceRows = maxBruteForceRows(limit, relevantOrdinals.size(), graph.size());
        Tracing.logAndTrace(logger, "{} rows relevant to current memtable out of {} materialized by SAI; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                            relevantOrdinals.size(), keys.size(), maxBruteForceRows, graph.size(), limit);

        // convert the expression value to query vector
        var qv = vts.createFloatVector(orderer.vector);
        // brute force path
        if (keysInGraph.size() <= maxBruteForceRows)
        {
            if (keysInGraph.isEmpty())
                return CloseableIterator.emptyIterator();
            return orderByBruteForce(qv, keysInGraph);
        }
        // indexed path
        var nodeScoreIterator = graph.search(context, qv, limit, 0, relevantOrdinals::contains);
        return new NodeScoreToScoredPrimaryKeyIterator(nodeScoreIterator);
    }

    /**
     * Filter the keys in the provided set by comparing their vectors to the query vector and returning only those
     * that have a similarity score >= the provided threshold.
     * NOTE: because the threshold is not used for ordering, the result is returned in PK order, not score order.
     * @param queryVector the query vector
     * @param threshold the minimum similarity score to accept
     * @param keys the keys to filter
     * @return an iterator over the keys that pass the filter in PK order
     */
    private CloseableIterator<PrimaryKeyWithSortKey> filterByBruteForce(VectorFloat<?> queryVector, float threshold, NavigableSet<PrimaryKey> keys)
    {
        // Keys are already ordered in ascending PK order, so just use an ArrayList to collect the results.
        var results = new ArrayList<PrimaryKeyWithSortKey>(keys.size());
        scoreKeysAndAddToCollector(queryVector, keys, threshold, results);
        return CloseableIterator.wrap(results.iterator());
    }

    private CloseableIterator<PrimaryKeyWithSortKey> orderByBruteForce(VectorFloat<?> queryVector, Collection<PrimaryKey> keys)
    {
        // Use a priority queue because we often don't need to consume the entire iterator
        var scoredPrimaryKeys = new PriorityQueue<PrimaryKeyWithSortKey>(keys.size());
        scoreKeysAndAddToCollector(queryVector, keys, 0, scoredPrimaryKeys);
        return new PriorityQueueIterator<>(scoredPrimaryKeys);
    }

    private void scoreKeysAndAddToCollector(VectorFloat<?> queryVector,
                                            Collection<PrimaryKey> keys,
                                            float threshold,
                                            Collection<PrimaryKeyWithSortKey> collector)
    {
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        for (var key : keys)
        {
            var vector = graph.vectorForKey(key);
            if (vector == null)
                continue;
            var score = similarityFunction.compare(queryVector, vector);
            if (score >= threshold)
                collector.add(new PrimaryKeyWithScore(indexContext, mt, key, score));
        }
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        return min(max(limit, expectedNodesVisited), GLOBAL_BRUTE_FORCE_ROWS);
    }

    public int estimateAnnNodesVisited(int limit, int nPermittedOrdinals)
    {
        return expectedNodesVisited(limit, nPermittedOrdinals, graph.size());
    }

    /**
     * All parameters must be greater than zero.  nPermittedOrdinals may be larger than graphSize.
     * <p>
     * Returns the expected number of nodes visited by an ANN search.
     * !!!
     * !!! "Visted" means we compute the coarse similarity with the query vector.  This is
     * !!! roughly `degree` times larger than the number of nodes whose edge lists we load!
     * !!!
     */
    public static int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        var K = limit;
        var B = min(nPermittedOrdinals, graphSize);
        var N = graphSize;
        // These constants come from running many searches on a variety of datasets and graph sizes.
        // * It is very consistent that the visited count is slightly less than linear wrt K, for both
        //   unconstrained (B = N) and constrained (B < N) searches.
        // * The behavior wrt B is hard to characterize.  Graphing the result F vs N/B shows ranges of
        //   growth very close to linear, interspersed with sharp jumps up to a higher visit count.  Overall,
        //   approximating it as linear is in the right ballpark.
        // * For unconstrained searches, the visited count is closest to log(N) but for constrained searches
        //   it is closer to log(N)**2 (or a higher exponent), perhaps as a result of N/B being too small.
        //
        // If we need to make this even more accurate, the relationship to B and to log(N) may be the best
        // places to start.
        var raw = (int) (100 + 0.025 * pow(log(N), 2) * pow(K, 0.95) * ((double) N / B));
        return ensureSaneEstimate(raw, limit, graphSize);
    }

    public static int ensureSaneEstimate(int rawEstimate, int rerankK, int graphSize)
    {
        // we will always visit at least min(rerankK, graphSize) nodes, and we can't visit more nodes than exist in the graph
        return min(max(rawEstimate, min(rerankK, graphSize)), graphSize);
    }

    @Override
    public Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        // This method is only used when merging an in-memory index with a RowMapping. This is done a different
        // way with the graph using the writeData method below.
        throw new UnsupportedOperationException();
    }

    /** returns true if the index is non-empty and should be flushed */
    public boolean preFlush(ToIntFunction<PrimaryKey> ordinalMapper)
    {
        return graph.preFlush(ordinalMapper);
    }

    public int size()
    {
        return graph.size();
    }

    public SegmentMetadata.ComponentMetadataMap writeData(IndexComponents.ForWrite perIndexComponents) throws IOException
    {
        return graph.flush(perIndexComponents);
    }

    @Override
    public long writeCount()
    {
        return writeCount.longValue();
    }

    @Override
    public long estimatedOnHeapMemoryUsed()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return 0;
    }

    @Override
    public boolean isEmpty()
    {
        return graph.isEmpty();
    }

    @Nullable
    @Override
    public ByteBuffer getMinTerm()
    {
        return null;
    }

    @Nullable
    @Override
    public ByteBuffer getMaxTerm()
    {
        return null;
    }

    /*
     * A {@link Bits} implementation that filters out all ordinals that do not correspond to a {@link PrimaryKey}
     * in the provided {@link AbstractBounds<PartitionPosition>}.
     */
    private class KeyRangeFilteringBits implements Bits
    {
        private final AbstractBounds<PartitionPosition> keyRange;

        public KeyRangeFilteringBits(AbstractBounds<PartitionPosition> keyRange)
        {
            this.keyRange = keyRange;
        }

        @Override
        public boolean get(int ordinal)
        {
            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }
    }

    private class ReorderingRangeIterator extends RangeIterator
    {
        private final PriorityQueue<PrimaryKey> keyQueue;

        ReorderingRangeIterator(PriorityQueue<PrimaryKey> keyQueue)
        {
            super(minimumKey, maximumKey, keyQueue.size());
            this.keyQueue = keyQueue;
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            while (!keyQueue.isEmpty() && keyQueue.peek().compareTo(nextKey) < 0)
                keyQueue.poll();
        }

        @Override
        public void close() {}

        @Override
        protected PrimaryKey computeNext()
        {
            if (keyQueue.isEmpty())
                return endOfData();
            return keyQueue.poll();
        }
    }

    /**
     * An iterator over {@link PrimaryKeyWithSortKey} sorted by score descending. The iterator converts ordinals (node ids)
     * to {@link PrimaryKey}s and pairs them with the score given by the index.
     */
    private class NodeScoreToScoredPrimaryKeyIterator extends AbstractIterator<PrimaryKeyWithSortKey>
    {
        private final CloseableIterator<SearchResult.NodeScore> nodeScores;
        private Iterator<PrimaryKeyWithScore> primaryKeysForNode = Collections.emptyIterator();

        NodeScoreToScoredPrimaryKeyIterator(CloseableIterator<SearchResult.NodeScore> nodeScores)
        {
            this.nodeScores = nodeScores;
        }

        @Override
        protected PrimaryKeyWithSortKey computeNext()
        {
            if (primaryKeysForNode.hasNext())
                return primaryKeysForNode.next();

            while (nodeScores.hasNext())
            {
                SearchResult.NodeScore nodeScore = nodeScores.next();
                primaryKeysForNode = graph.keysFromOrdinal(nodeScore.node)
                                          .stream()
                                          .map(pk -> new PrimaryKeyWithScore(indexContext, mt, pk, nodeScore.score))
                                          .iterator();
                if (primaryKeysForNode.hasNext())
                    return primaryKeysForNode.next();
            }

            return endOfData();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(nodeScores);
        }
    }

    /** ensures that the graph is connected -- normally not necessary but it can help tests reason about the state */
    public void cleanup()
    {
        graph.cleanup();
    }
}
