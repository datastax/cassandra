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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PriorityQueueScoredRowIdIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.PrimaryKeyWithSource;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OverqueryUtils;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.OrderIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.ScoredRowId;
import org.apache.cassandra.index.sai.utils.ScoredRowIdIterator;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.metrics.LinearFit;
import org.apache.cassandra.metrics.PairedSlidingWindowReservoir;
import org.apache.cassandra.metrics.QuickSlidingWindowReservoir;
import org.apache.cassandra.tracing.Tracing;

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
        this(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext, new CassandraOnDiskHnsw(segmentMetadata.componentMetadatas, perIndexFiles, indexContext));
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

        if (exp.getOp() != Expression.Op.ANN && exp.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        if (exp.getEuclideanSearchThreshold() > 0)
            limit = 100000;
        int topK = OverqueryUtils.topKFor(limit, graph.getCompressedVectors());
        float[] queryVector = exp.lower.value.vector;

        BitsOrRowIds bitsOrRowIds = bitsOrPostingListForKeyRange(keyRange, queryVector, topK, false);
        if (bitsOrRowIds.skipANN())
            return bitsOrRowIds.postingList();

        var vectorPostings = graph.search(queryVector, topK, exp.getEuclideanSearchThreshold(), limit, bitsOrRowIds.getBits(), context);
        bitsOrRowIds.updateStatistics(vectorPostings.getVisitedCount());
        return vectorPostings;
    }

    @Override
    public OrderIterator searchTopK(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, int limit) throws IOException
    {
        return toScoreOrderedIterator(getTopK(context, exp, keyRange, limit), context);
    }


    private ScoredRowIdIterator getTopK(QueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.ANN && exp.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        int topK = OverqueryUtils.topKFor(limit, graph.getCompressedVectors());
        float[] queryVector = exp.lower.value.vector;

        BitsOrRowIds bitsOrRowIds = bitsOrPostingListForKeyRange(keyRange, queryVector, topK, true);
        if (bitsOrRowIds.skipANN())
            return bitsOrRowIds.scoredRowIdIterator();

        var vectorPostings = graph.searchWithScores(queryVector, topK, limit, bitsOrRowIds.bits, context);
         bitsOrRowIds.updateStatistics(vectorPostings.getVisitedCount());
        return vectorPostings;
    }

    /**
     * Return bit set to configure a graph search; otherwise return posting list or ScoredRowIdIterator to bypass
     * graph search and use brute force to order matching rows.
     */
    private BitsOrRowIds bitsOrPostingListForKeyRange(AbstractBounds<PartitionPosition> keyRange,
                                                      float[] queryVector,
                                                      int topK,
                                                      boolean orderByScore) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return BitsOrRowIds.ALL_BITS;

            PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(firstPrimaryKey);
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return BitsOrRowIds.EMPTY;
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return BitsOrRowIds.EMPTY;

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return BitsOrRowIds.ALL_BITS;

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // Upper-bound cost based on maximum possible rows included
            int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            var cost = estimateCost(topK, nRows);
            Tracing.logAndTrace(logger, "Search range covers {} rows; expected nodes visited is {} for sstable index with {} nodes, LIMIT {}",
                                nRows, cost.expectedNodesVisited, graph.size(), topK);
            // if we have a small number of results then let TopK processor do exact NN computation
            if (cost.shouldUseBruteForce())
            {
                var segmentRowIds = new IntArrayList(nRows, 0);
                for (long i = minSSTableRowId; i <= maxSSTableRowId; i++)
                    segmentRowIds.add(metadata.toSegmentRowId(i));

                if (orderByScore)
                    return new BitsOrRowIds(bruteForceOrdering(queryVector, segmentRowIds));
                var postings = findTopApproximatePostings(queryVector, segmentRowIds, topK);
                return new BitsOrRowIds(new ArrayPostingList(postings));
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            SparseFixedBitSet bits = bitSetForSearch();
            boolean hasMatches = false;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                {
                    int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                    int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                    if (ordinal >= 0)
                    {
                        bits.set(ordinal);
                        hasMatches = true;
                    }
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            // We can make a more accurate cost estimate now
            cost = estimateCost(topK, bits.cardinality());

            if (!hasMatches)
                return BitsOrRowIds.EMPTY;

            return new BitsOrRowIds(bits, cost);
        }
    }

    private int[] findTopApproximatePostings(float[] queryVector, IntArrayList segmentRowIds, int topK) throws IOException
    {
        var cv = graph.getCompressedVectors();
        if (cv == null || segmentRowIds.size() <= topK)
            return segmentRowIds.toIntArray();

        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        var scoreFunction = cv.approximateScoreFunctionFor(queryVector, similarityFunction);

        ArrayList<SearchResult.NodeScore> pairs = new ArrayList<>(segmentRowIds.size());
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (int i = 0; i < segmentRowIds.size(); i++)
            {
                int segmentRowId = segmentRowIds.getInt(i);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal < 0)
                    continue;

                var score = scoreFunction.similarityTo(ordinal);
                pairs.add(new SearchResult.NodeScore(segmentRowId, score));
            }
        }
        // sort descending
        pairs.sort((a, b) -> Float.compare(b.score, a.score));
        int end = Math.min(pairs.size(), topK) - 1;
        int[] postings = new int[end + 1];
        // top K ascending
        for (int i = end; i >= 0; i--)
            postings[end - i] = pairs.get(i).node;
        // Rows are sorted now so that we get the PrimaryKeys in order for correct deduplication in the
        // RangeUnionIterator, where we merge the results from all sstables.
        Arrays.sort(postings);
        return postings;
    }

    // Because we need a correct ordering of vectors, we read and rank them here. By ranking them now, we are able to
    // take the LIMIT of Primary Keys from the final search's iterator, which means fewer rows to read materialize
    // and score in the later steps.
    private ScoredRowIdIterator bruteForceOrdering(float[] queryVector, IntArrayList segmentRowIds) throws IOException
    {
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        PriorityQueue<ScoredRowId> pairs = new PriorityQueue<>(segmentRowIds.size(),
                                                               (a, b) -> Float.compare(b.getScore(), a.getScore()));
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (int i = 0; i < segmentRowIds.size(); i++)
            {
                int segmentRowId = segmentRowIds.getInt(i);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal < 0)
                    continue;

                float[] vector = graph.getVectorForOrdinal(ordinal);
                if (vector == null)
                    continue;
                var score = similarityFunction.compare(queryVector, vector);
                pairs.add(new ScoredRowId(segmentRowId, score));
            }
        }
        return new PriorityQueueScoredRowIdIterator(pairs);
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
    public OrderIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit) throws IOException
    {
        // create a sublist of the keys within this segment's bounds
        int minIndex = findBoundaryIndex(keys, true);
        int maxIndex = findBoundaryIndex(keys, false);
        List<PrimaryKey> keysInRange = keys.subList(minIndex, maxIndex);
        if (keysInRange.isEmpty())
            return OrderIterator.empty();

        int topK = OverqueryUtils.topKFor(limit, graph.getCompressedVectors());
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

        var numRows = rowIds.size();
        var cost = estimateCost(topK, numRows);
        Tracing.logAndTrace(logger, "{} rows relevant to current sstable out of {} in range; expected nodes visited is {} for index with {} nodes, LIMIT {}",
                            numRows, keysInRange.size(), cost.expectedNodesVisited, graph.size(), limit);
        if (numRows == 0)
            return OrderIterator.empty();

        if (cost.shouldUseBruteForce())
        {
            // brute force using the in-memory compressed vectors to cut down the number of results returned
            var queryVector = exp.lower.value.vector;
            return toScoreOrderedIterator(bruteForceOrdering(queryVector, rowIds), context);
        }
        // else ask the index to perform a search limited to the bits we created
        float[] queryVector = exp.lower.value.vector;
        var results = graph.searchWithScores(queryVector, topK, limit, bits, context);
        cost.updateStatistics(results.getVisitedCount());

        return toScoreOrderedIterator(results, context);
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

    private static class BitsOrRowIds
    {
        public static final BitsOrRowIds ALL_BITS = new BitsOrRowIds(Bits.ALL, null);
        public static final BitsOrRowIds EMPTY = new BitsOrRowIds(PostingList.EMPTY,
                                                                  ScoredRowIdIterator.empty(),
                                                                  null,
                                                                  null);
        private final Bits bits;
        private final CostEstimate cost;
        private final PostingList postingList;
        private final ScoredRowIdIterator scores;

        public BitsOrRowIds(Bits bits, CostEstimate cost)
        {
            this (null, null, bits, cost);
        }

        public BitsOrRowIds(ScoredRowIdIterator scores)
        {
            this (null, scores, null, null);
        }

        public BitsOrRowIds(PostingList postingList)
        {
            this (postingList, null, null, null);
        }

        private BitsOrRowIds(PostingList postingList, ScoredRowIdIterator scores, Bits bits, CostEstimate cost)
        {
            this.bits = bits;
            this.cost = cost;
            this.postingList = postingList;
            this.scores = scores;
        }

        @Nullable
        public Bits getBits()
        {
            Preconditions.checkState(!skipANN());
            return bits;
        }

        public PostingList postingList()
        {
            Preconditions.checkState(skipANN());
            return postingList;
        }

        public ScoredRowIdIterator scoredRowIdIterator()
        {
            Preconditions.checkState(skipANN());
            return scores;
        }

        public boolean skipANN()
        {
            return postingList != null || scores != null;
        }

        public void updateStatistics(int visitedCount)
        {
            if (cost != null)
                cost.updateStatistics(visitedCount);
        }
    }
}
