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

import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.IntFunction;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over {@link ScoredRowId} that lazily orders a {@link PriorityQueue} of approximately scored rows.
 * The class works by:
 * 1. Taking a priority queue of {@link ApproximateScore} ordered by approximate similarity score.
 * 2. Polling the topK results from the approximate score queue
 * 3. Materialize and score the full resolution vector for each row from step 2.
 * 4. Add to the full resolution row {@link ScoredRowId} to the priority queue.
 * 5. Return rows from the {@link ScoredRowId} priority queue until the limit is reached.
 * 6. When the limit is reached, if the {@link ApproximateScore} queue is not empty, get the next (topK - limit) rows
 *    from the approximate score queue and repeat 3-6 until the {@link ApproximateScore} queue is empty.
 * 7. Return the remaining rows from the {@link ScoredRowId} queue.
 */
public class CompressedVectorRowIdIterator implements CloseableIterator<ScoredRowId>
{
    public static class ApproximateScore implements Comparable<ApproximateScore>
    {
        private final int rowId;
        private final int ordinal;
        private final float appoximateScore;

        public ApproximateScore(int rowId, int ordinal, float appoximateScore)
        {
            this.rowId = rowId;
            this.ordinal = ordinal;
            this.appoximateScore = appoximateScore;
        }

        @Override
        public int compareTo(ApproximateScore o)
        {
            // We always want scores in descending order
            return Float.compare(o.appoximateScore, appoximateScore);
        }
    }

    private final float[] queryVector;
    // We use two PriorityQueues because we do not need an eager ordering of these results. Depending on how many
    // sstables the query hits and the relative scores of vectors from those sstables, we may not need to return
    // more than the first handful of scores.
    // Priority queue with compressed vector scores
    private final PriorityQueue<ApproximateScore> cvScoreQueue;
    // Priority queue with full resolution scores
    private final PriorityQueue<ScoredRowId> scoredRowIdQueue;
    private final IntFunction<float[]> vectorForOrdinal;
    private final VectorSimilarityFunction similarityFunction;
    private final int topK;
    private final int limit;
    private final float threshold;

    /**
     * @param queryVector The query vector
     * @param cvScoreQueue A priority queue of rows and their ordinal ordered by their approximate similarity scores
     * @param vectorForOrdinal A function that returns the full resolution vector for a given ordinal
     * @param similarityFunction The similarity function to compare full resolution vectors
     * @param limit The query limit
     * @param topK The number of vectors to resolve and score before returning results
     * @param threshold The minimum similarity score to return
     */
    public CompressedVectorRowIdIterator(float[] queryVector,
                                         PriorityQueue<ApproximateScore> cvScoreQueue,
                                         IntFunction<float[]> vectorForOrdinal,
                                         VectorSimilarityFunction similarityFunction,
                                         int limit,
                                         int topK,
                                         float threshold)
    {
        this.queryVector = queryVector;
        this.cvScoreQueue = cvScoreQueue;
        this.scoredRowIdQueue = new PriorityQueue<>(topK, (a, b) -> Float.compare(b.score, a.score));
        this.vectorForOrdinal = vectorForOrdinal;
        this.similarityFunction = similarityFunction;
        assert topK >= limit : "topK must be greater than or equal to limit. Found: " + topK + " < " + limit;
        this.limit = limit;
        this.topK = topK;
        this.threshold = threshold;
    }


    @Override
    public boolean hasNext()
    {
        // The priority queue is only valid for the first limit results. After that, we need to
        // get the next `limit` results from the compressed vector graph, compute the full resolution scores,
        // and add to the priority queue.
        if (!scoredRowIdQueue.isEmpty() && !needsRefill())
            return true;

        while (!cvScoreQueue.isEmpty() && scoredRowIdQueue.size() <= topK)
        {
            ApproximateScore rowOrdinalScore = cvScoreQueue.poll();
            float[] vector = vectorForOrdinal.apply(rowOrdinalScore.ordinal);
            assert vector != null : "Vector for ordinal " + rowOrdinalScore.ordinal + " is null";
            float score = similarityFunction.compare(queryVector, vector);
            if (score >= threshold)
                scoredRowIdQueue.add(new ScoredRowId(rowOrdinalScore.rowId, score));
        }

        return !scoredRowIdQueue.isEmpty();
    }

    @Override
    public ScoredRowId next()
    {
        // Don't call hastNext() because it might unnecessarily trigger unnecessary reads from disk
        if (scoredRowIdQueue.isEmpty() || needsRefill())
            throw new NoSuchElementException();

        return scoredRowIdQueue.poll();
    }

    private boolean needsRefill()
    {
        return scoredRowIdQueue.size() < topK - limit && !cvScoreQueue.isEmpty();
    }

    @Override
    public void close() {}
}
