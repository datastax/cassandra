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
package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.PrimaryKeyWithSource;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;
import org.apache.cassandra.index.sai.disk.v2.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.vector.BruteForceRowIdIterator;
import org.apache.cassandra.index.sai.disk.vector.CloseableReranker;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PriorityQueueIterator;
import org.apache.cassandra.index.sai.utils.ScoredPrimaryKey;
import org.apache.cassandra.index.sai.utils.ScoredRowIdPrimaryKeyMapIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.metrics.LinearFit;
import org.apache.cassandra.metrics.PairedSlidingWindowReservoir;
import org.apache.cassandra.metrics.QuickSlidingWindowReservoir;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;

import static java.lang.Math.ceil;
import static java.lang.Math.min;

/**
 * Executes ann search against the graph for an individual index segment.
 */
public class V2VectorIndexSearcher extends IndexSearcher implements SegmentOrdering
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final JVectorLuceneOnDiskGraph graph;
    private final PrimaryKey.Factory keyFactory;
    private int globalBruteForceRows; // not final so test can inject its own setting
    private final PairedSlidingWindowReservoir expectedActualNodesVisited = new PairedSlidingWindowReservoir(20);
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;

    public V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                 PerIndexFiles perIndexFiles,
                                 SegmentMetadata segmentMetadata,
                                 IndexDescriptor indexDescriptor,
                                 IndexContext indexContext) throws IOException
    {
        this(primaryKeyMapFactory,
             perIndexFiles,
             segmentMetadata,
             indexDescriptor,
             indexContext,
             new CassandraOnDiskHnsw(segmentMetadata.componentMetadatas, perIndexFiles, indexContext));
    }

    protected V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexDescriptor indexDescriptor,
                                    IndexContext indexContext,
                                    JVectorLuceneOnDiskGraph graph)
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);
        this.graph = graph;
        this.keyFactory = PrimaryKey.factory(indexContext.comparator(), indexContext.indexFeatureSet());
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));

        globalBruteForceRows = Integer.MAX_VALUE;
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    public CompressedVectors getCompressedVectors()
    {
        return graph.getCompressedVectors();
    }

    @Override
    public RangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        PostingList results = searchPosting(context, exp, keyRange, limit);
        return toPrimaryKeyIterator(results, context);
    }

    private PostingList searchPosting(QueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during BOUNDED_ANN index query: " + exp));

        float[] queryVector = exp.lower.value.vector;

        var result = searchInternal(keyRange, context, queryVector, graph.size(), graph.size(), exp.getEuclideanSearchThreshold());
        return new VectorPostingList(result);
    }

    @Override
    public CloseableIterator<ScoredPrimaryKey> orderBy(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        int topK = indexContext.getIndexWriterConfig().getSourceModel().topKFor(limit, graph.getCompressedVectors());
        float[] queryVector = exp.lower.value.vector;

        var result = searchInternal(keyRange, context, queryVector, limit, topK, 0);
        return toScoreOrderedIterator(result, context);
    }

    /**
     * Return bit set to configure a graph search; otherwise return posting list or ScoredRowIdIterator to bypass
     * graph search and use brute force to order matching rows.
     * @param keyRange the key range to search
     * @param context the query context
     * @param queryVector the query vector
     * @param limit the limit for the query
     * @param topK the amplified limit for the query to get more accurate results
     * @param threshold the threshold for the query. When the threshold is greater than 0 and brute force logic is used,
     *                  the results will be filtered by the threshold.
     */
    private CloseableIterator<ScoredRowId> searchInternal(AbstractBounds<PartitionPosition> keyRange,
                                                          QueryContext context,
                                                          float[] queryVector,
                                                          int limit,
                                                          int topK,
                                                          float threshold) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return graph.search(queryVector, topK, threshold, Bits.ALL, context, context::addAnnNodesVisited);

            PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(firstPrimaryKey);
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return CloseableIterator.emptyIterator();
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return CloseableIterator.emptyIterator();

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return graph.search(queryVector, topK, threshold, Bits.ALL, context, context::addAnnNodesVisited);

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // Upper-bound cost based on maximum possible rows included
            int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            var initialCostEstimate = estimateCost(topK, nRows);
            Tracing.logAndTrace(logger, "Search range covers {} rows; expected nodes visited is {} for sstable index with {} nodes, LIMIT {}",
                                nRows, initialCostEstimate.expectedNodesVisited, graph.size(), topK);
            // if we have a small number of results then let TopK processor do exact NN computation
            if (initialCostEstimate.shouldUseBruteForce())
            {
                var segmentRowIds = new IntArrayList(nRows, 0);
                for (long i = minSSTableRowId; i <= maxSSTableRowId; i++)
                    segmentRowIds.add(metadata.toSegmentRowId(i));

                // When we have a threshold, we only need to filter the results, not order them, because it means we're
                // evaluating a boolean predicate in the SAI pipeline that wants to collate by PK
                if (threshold > 0)
                    return filterByBruteForce(queryVector, segmentRowIds, threshold);
                else
                    return orderByBruteForce(queryVector, segmentRowIds, limit, topK);
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            final BitSet bits;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                int startSegmentRowId = metadata.toSegmentRowId(minSSTableRowId);
                int endSegmentRowId = metadata.toSegmentRowId(maxSSTableRowId);

                bits = ordinalsView.buildOrdinalBitSet(startSegmentRowId, endSegmentRowId, this::bitSetForSearch);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            int cardinality = bits.cardinality();
            // We can make a more accurate cost estimate now
            var betterCostEstimate = estimateCost(topK, cardinality);

            if (cardinality == 0)
                return CloseableIterator.emptyIterator();

            return graph.search(queryVector, topK, threshold, bits, context, visited -> {
                betterCostEstimate.updateStatistics(visited);
                context.addAnnNodesVisited(visited);
            });
        }
    }

    protected CloseableIterator<ScoredPrimaryKey> toScoreOrderedIterator(CloseableIterator<ScoredRowId> scoredRowIdIterator, QueryContext queryContext) throws IOException
    {
        if (scoredRowIdIterator == null || !scoredRowIdIterator.hasNext())
            return CloseableIterator.emptyIterator();

        IndexSearcherContext searcherContext = new IndexSearcherContext(metadata.minKey,
                                                                        metadata.maxKey,
                                                                        metadata.minSSTableRowId,
                                                                        metadata.maxSSTableRowId,
                                                                        metadata.segmentRowIdOffset,
                                                                        queryContext,
                                                                        null);
        return new ScoredRowIdPrimaryKeyMapIterator(scoredRowIdIterator,
                                                    primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(),
                                                    searcherContext);
    }

    private CloseableIterator<ScoredRowId> orderByBruteForce(float[] queryVector, IntArrayList segmentRowIds, int limit, int topK) throws IOException
    {
        // If we use compressed vectors, we still have to order the topK results using full resolution similarity
        // scores, so only use the compressed vectors when there are enough vectors to make it worthwhile.
        // VSTODO is there a multiplier for topK that makes sense? Does it depend on vector length? Further
        // testing needed. Initial testing suggests the difference in these two paths is less than a millisecond.
        if (graph.getCompressedVectors() != null && segmentRowIds.size() > topK)
            return orderByBruteForce(graph.getCompressedVectors(), queryVector, segmentRowIds, limit, topK);
        return orderByBruteForce(queryVector, segmentRowIds);
    }

    /**
     * Materialize the compressed vectors for the given segment row ids, put them into a priority queue ordered by
     * approximate similarity score, and then pass to the {@link BruteForceRowIdIterator} to lazily resolve the
     * full resolution ordering as needed.
     */
    private CloseableIterator<ScoredRowId> orderByBruteForce(CompressedVectors cv,
                                                             float[] queryVector,
                                                             IntArrayList segmentRowIds,
                                                             int limit,
                                                             int topK) throws IOException
    {
        var approximateScores = new PriorityQueue<BruteForceRowIdIterator.RowWithApproximateScore>(segmentRowIds.size(),
                                                                                                   (a, b) -> Float.compare(b.getApproximateScore(), a.getApproximateScore()));
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        var scoreFunction = cv.approximateScoreFunctionFor(queryVector, similarityFunction);

        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (int i = 0; i < segmentRowIds.size(); i++)
            {
                int segmentRowId = segmentRowIds.getInt(i);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal < 0)
                    continue;

                var score = scoreFunction.similarityTo(ordinal);
                approximateScores.add(new BruteForceRowIdIterator.RowWithApproximateScore(segmentRowId, ordinal, score));
            }
        }
        var reranker = new CloseableReranker(similarityFunction, queryVector, graph.getVectorSupplier());
        return new BruteForceRowIdIterator(approximateScores, reranker, limit, topK);
    }

    /**
     * Produces a correct ranking of the rows in the given segment. Because this graph does not have compressed
     * vectors, read all vectors and put them into a priority queue to rank them lazily. It is assumed that the whole
     * PQ will often not be needed.
     */
    private CloseableIterator<ScoredRowId> orderByBruteForce(float[] queryVector, IntArrayList segmentRowIds) throws IOException
    {
        PriorityQueue<ScoredRowId> scoredRowIds = new PriorityQueue<>(segmentRowIds.size(), (a, b) -> Float.compare(b.getScore(), a.getScore()));
        addScoredRowIdsToCollector(queryVector, segmentRowIds, 0, scoredRowIds);
        return new PriorityQueueIterator<>(scoredRowIds);
    }

    /**
     * Materialize the full resolution vector for each row id, compute the similarity score, filter
     * out rows that do not meet the threshold, and then return them in an iterator.
     * NOTE: because the threshold is not used for ordering, the result is returned in PK order, not score order.
     */
    private CloseableIterator<ScoredRowId> filterByBruteForce(float[] queryVector,
                                                              IntArrayList segmentRowIds,
                                                              float threshold) throws IOException
    {
        var results = new ArrayList<ScoredRowId>(segmentRowIds.size());
        addScoredRowIdsToCollector(queryVector, segmentRowIds, threshold, results);
        return CloseableIterator.wrap(results.iterator());
    }

    private void addScoredRowIdsToCollector(float[] queryVector,
                                            IntArrayList segmentRowIds,
                                            float threshold,
                                            Collection<ScoredRowId> collector) throws IOException
    {
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        try (var ordinalsView = graph.getOrdinalsView();
             var vectorsView = graph.getVectorSupplier())
        {
            for (int i = 0; i < segmentRowIds.size(); i++)
            {
                int segmentRowId = segmentRowIds.getInt(i);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal < 0)
                    continue;

                float[] vector = vectorsView.getVectorForOrdinal(ordinal);
                assert vector != null : "Vector for ordinal " + ordinal + " is null";
                var score = similarityFunction.compare(queryVector, vector);
                if (score >= threshold)
                    collector.add(new ScoredRowId(segmentRowId, score));
            }
        }
    }

    private long getMaxSSTableRowId(PrimaryKeyMap primaryKeyMap, PartitionPosition right)
    {
        // if the right token is the minimum token, there is no upper bound on the keyRange and
        // we can save a lookup by using the maxSSTableRowId
        if (right.isMinimum())
            return metadata.maxSSTableRowId;

        PrimaryKey lastPrimaryKey = keyFactory.createTokenOnly(right.getToken());
        long max = primaryKeyMap.floor(lastPrimaryKey);
        if (max < 0)
            return metadata.maxSSTableRowId;
        return max;
    }

    private class CostEstimate
    {
        public final int nFilteredRows;
        public final int rawExpectedNodesVisited;
        public final int expectedNodesVisited;

        public CostEstimate(int nFilteredRows, int rawExpectedNodesVisited, int expectedNodesVisited)
        {
            assert rawExpectedNodesVisited >= 0 : rawExpectedNodesVisited;
            assert expectedNodesVisited >= 0 : expectedNodesVisited;

            this.nFilteredRows = nFilteredRows;
            this.rawExpectedNodesVisited = rawExpectedNodesVisited;
            this.expectedNodesVisited = expectedNodesVisited;
        }

        public boolean shouldUseBruteForce()
        {
            // ANN index will do a bunch of extra work besides the full comparisons (performing PQ similarity for each edge);
            // brute force from sstable will also do a bunch of extra work (going through trie index to look up row).
            // VSTODO I'm not sure which one is more expensive (and it depends on things like sstable chunk cache hit ratio)
            // so I'm leaving it as a 1:1 ratio for now.
            return nFilteredRows <= min(globalBruteForceRows, expectedNodesVisited);
        }

        public void updateStatistics(int actualNodesVisited)
        {
            assert actualNodesVisited >= 0 : actualNodesVisited;
            expectedActualNodesVisited.update(rawExpectedNodesVisited, actualNodesVisited);

            if (actualNodesVisited >= 1000 && (actualNodesVisited > 2 * expectedNodesVisited || actualNodesVisited < 0.5 * expectedNodesVisited))
                Tracing.logAndTrace(logger, "Predicted visiting {} nodes, but actually visited {}",
                                    expectedNodesVisited, actualNodesVisited);
        }
    }

    private CostEstimate estimateCost(int limit, int nFilteredRows)
    {
        int rawExpectedNodes = getRawExpectedNodes(limit, nFilteredRows);
        // update the raw expected value with a linear interpolation based on observed data
        var observedValues = expectedActualNodesVisited.getSnapshot().values;
        int expectedNodes;
        if (observedValues.length >= 10)
        {
            var interceptSlope = LinearFit.interceptSlopeFor(observedValues);
            expectedNodes = (int) (interceptSlope.left + interceptSlope.right * rawExpectedNodes);
        }
        else
        {
            expectedNodes = rawExpectedNodes;
        }

        int sanitizedEstimate = VectorMemtableIndex.ensureSaneEstimate(expectedNodes, limit, graph.size());
        return new CostEstimate(nFilteredRows, rawExpectedNodes, sanitizedEstimate);
    }

    private SparseFixedBitSet bitSetForSearch()
    {
        var bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    private int findBoundaryIndex(List<PrimaryKey> keys, boolean findMin)
    {
        // The minKey and maxKey are sometimes just partition keys (not primary keys), so binarySearch
        // may not return the index of the least/greatest match.
        var key = findMin ? metadata.minKey : metadata.maxKey;
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            return -index - 1;
        if (findMin)
        {
            while (index > 0 && keys.get(index - 1).equals(key))
                index--;
        }
        else
        {
            while (index < keys.size() - 1 && keys.get(index + 1).equals(key))
                index++;
            // We must include the PrimaryKey at the boundary
            index++;
        }
        return index;
    }

    @Override
    public CloseableIterator<ScoredPrimaryKey> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit) throws IOException
    {
        // create a sublist of the keys within this segment's bounds
        int minIndex = findBoundaryIndex(keys, true);
        int maxIndex = findBoundaryIndex(keys, false);
        List<PrimaryKey> keysInRange = keys.subList(minIndex, maxIndex);
        if (keysInRange.isEmpty())
            return CloseableIterator.emptyIterator();

        int topK = indexContext.getIndexWriterConfig().getSourceModel().topKFor(limit, graph.getCompressedVectors());
        // Convert PKs to segment row ids and then to ordinals, skipping any that don't exist in this segment
        var bitsAndRows = flatmapPrimaryKeysToBitsAndRows(keysInRange);
        var bits = bitsAndRows.left;
        var rowIds = bitsAndRows.right;
        var numRows = rowIds.size();
        final CostEstimate cost = estimateCost(topK, numRows);
        Tracing.logAndTrace(logger, "{} rows relevant to current sstable out of {} in range; expected nodes visited is {} for index with {} nodes, LIMIT {}",
                            numRows, keysInRange.size(), cost.expectedNodesVisited, graph.size(), limit);
        if (numRows == 0)
            return CloseableIterator.emptyIterator();

        if (cost.shouldUseBruteForce())
        {
            // brute force using the in-memory compressed vectors to cut down the number of results returned
            var queryVector = exp.lower.value.vector;
            return toScoreOrderedIterator(this.orderByBruteForce(queryVector, rowIds, limit, topK), context);
        }
        // else ask the index to perform a search limited to the bits we created
        float[] queryVector = exp.lower.value.vector;
        var results = graph.search(queryVector, topK, 0, bits, context, cost::updateStatistics);
        return toScoreOrderedIterator(results, context);
    }

    private Pair<SparseFixedBitSet, IntArrayList> flatmapPrimaryKeysToBitsAndRows(List<PrimaryKey> keysInRange) throws IOException
    {
        // if we are brute forcing the similarity search, we want to build a list of segment row ids,
        // but if not, we want to build a bitset of ordinals corresponding to the rows.
        // We won't know which path to take until we have an accurate key count.
        SparseFixedBitSet bits = bitSetForSearch();
        IntArrayList rowIds = new IntArrayList();
        try (var primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
             var ordinalsView = graph.getOrdinalsView())
        {
            // track whether we are saving comparisons by using binary search to skip ahead
            // (if most of the keys belong to this sstable, bsearch will actually be slower)
            var comparisonsSavedByBsearch = new QuickSlidingWindowReservoir(10);
            boolean preferSeqScanToBsearch = false;

            for (int i = 0; i < keysInRange.size();)
            {
                // turn the pk back into a row id, with a fast path for the case where the pk is from this sstable
                var primaryKey = keysInRange.get(i);
                long sstableRowId;
                if (primaryKey instanceof PrimaryKeyWithSource
                    && ((PrimaryKeyWithSource) primaryKey).getSourceSstableId().equals(primaryKeyMap.getSSTableId()))
                    sstableRowId = ((PrimaryKeyWithSource) primaryKey).getSourceRowId();
                else
                    sstableRowId = primaryKeyMap.exactRowIdOrInvertedCeiling(primaryKey);

                if (sstableRowId < 0)
                {
                    // The given PK doesn't exist in this sstable, so sstableRowId represents the negation
                    // of the next-highest.  Turn that back into a PK so we can skip ahead in keysInRange.
                    long ceilingRowId = - sstableRowId - 1;
                    if (ceilingRowId > metadata.maxSSTableRowId)
                    {
                        // The next greatest primary key is greater than all the primary keys in this segment
                        break;
                    }
                    var ceilingPrimaryKey = primaryKeyMap.primaryKeyFromRowId(ceilingRowId);

                    boolean ceilingPrimaryKeyMatchesKeyInRange = false;
                    // adaptively choose either seq scan or bsearch to skip ahead in keysInRange until
                    // we find one at least as large as the ceiling key
                    if (preferSeqScanToBsearch)
                    {
                        int keysToSkip = 1; // We already know that the PK at index i is not equal to the ceiling PK.
                        int cmp = 1; // Need to initialize. The value is irrelevant.
                        for ( ; i + keysToSkip < keysInRange.size(); keysToSkip++)
                        {
                            var nextPrimaryKey = keysInRange.get(i + keysToSkip);
                            cmp = nextPrimaryKey.compareTo(ceilingPrimaryKey);
                            if (cmp >= 0)
                                break;
                        }
                        comparisonsSavedByBsearch.update(keysToSkip - (int) ceil(logBase2(keysInRange.size() - i)));
                        i += keysToSkip;
                        ceilingPrimaryKeyMatchesKeyInRange = cmp == 0;
                    }
                    else
                    {
                        // Use a sublist to only search the remaining primary keys in range.
                        var keysRemaining = keysInRange.subList(i, keysInRange.size());
                        int nextIndexForCeiling = Collections.binarySearch(keysRemaining, ceilingPrimaryKey);
                        if (nextIndexForCeiling < 0)
                            // We got: -(insertion point) - 1. Invert it so we get the insertion point.
                            nextIndexForCeiling = -nextIndexForCeiling - 1;
                        else
                            ceilingPrimaryKeyMatchesKeyInRange = true;

                        comparisonsSavedByBsearch.update(nextIndexForCeiling - (int) ceil(logBase2(keysRemaining.size())));
                        i += nextIndexForCeiling;
                    }

                    // update our estimate
                    preferSeqScanToBsearch = comparisonsSavedByBsearch.size() >= 10
                                             && comparisonsSavedByBsearch.getMean() < 0;
                    if (ceilingPrimaryKeyMatchesKeyInRange)
                        sstableRowId = ceilingRowId;
                    else
                        continue; // without incrementing i further. ceilingPrimaryKey is less than the PK at index i.
                }
                // Increment here to simplify the sstableRowId < 0 logic.
                i++;

                // these should still be true based on our computation of keysInRange
                assert sstableRowId >= metadata.minSSTableRowId : String.format("sstableRowId %d < minSSTableRowId %d", sstableRowId, metadata.minSSTableRowId);
                assert sstableRowId <= metadata.maxSSTableRowId : String.format("sstableRowId %d > maxSSTableRowId %d", sstableRowId, metadata.maxSSTableRowId);

                // convert the global row id to segment row id and from segment row id to graph ordinal
                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                {
                    rowIds.add(segmentRowId);
                    bits.set(ordinal);
                }
            }
        }
        return Pair.create(bits, rowIds);
    }

    public static double logBase2(double number) {
        return Math.log(number) / Math.log(2);
    }

    private int getRawExpectedNodes(int topK, int nPermittedOrdinals)
    {
        return VectorMemtableIndex.expectedNodesVisited(topK, nPermittedOrdinals, graph.size());
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
    }
}
