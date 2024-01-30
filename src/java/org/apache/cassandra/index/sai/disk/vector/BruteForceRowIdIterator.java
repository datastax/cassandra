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

import java.util.PriorityQueue;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;


/**
 * An iterator over {@link ScoredRowId} that lazily consumes from a {@link PriorityQueue} of {@link RowWithApproximateScore}.
 * <p>
 * The idea is that we maintain the same level of accuracy as we would get from a graph search, by re-ranking the top `k``
 * best approximate scores at a time with the full resolution vectors to return the top `limit`.
 * <p>
 * For example, suppose that limit=3 and k=5 and we have ten elements.  After our first re-ranking batch, we have
 *   ABDEF?????
 * We will return A, B, and D; if more elements are requested, we will re-rank another 5 (so three more, including
 * the two remaining from the first batch).  Here we uncover C, G, and H, and order them appropriately:
 *      CEFGH??
 * This illustrates that, also like a graph search, we only guarantee ordering of results within a re-ranking batch,
 * not globally.
 * <p>
 * As an implementation detail, we use a PriorityQueue to maintain state rather than a List and sorting.
 */
public class BruteForceRowIdIterator extends AbstractIterator<ScoredRowId>
{
    public static class RowWithApproximateScore
    {
        private final int rowId;
        private final int ordinal;
        private final float appoximateScore;

        public RowWithApproximateScore(int rowId, int ordinal, float appoximateScore)
        {
            this.rowId = rowId;
            this.ordinal = ordinal;
            this.appoximateScore = appoximateScore;
        }

        public float getApproximateScore()
        {
            return appoximateScore;
        }
    }

    // We use two PriorityQueues because we do not need an eager ordering of these results. Depending on how many
    // sstables the query hits and the relative scores of vectors from those sstables, we may not need to return
    // more than the first handful of scores.
    // Priority queue with compressed vector scores
    private final PriorityQueue<RowWithApproximateScore> approximateScoreQueue;
    // Priority queue with full resolution scores
    private final PriorityQueue<ScoredRowId> exactScoreQueue;
    private final CloseableReranker reranker;
    private final int topK;
    private final int limit;
    private int rerankedCount;

    /**
     * @param approximateScoreQueue A priority queue of rows and their ordinal ordered by their approximate similarity scores
     * @param reranker A function that takes a graph ordinal and returns the exact similarity score
     * @param limit The query limit
     * @param topK The number of vectors to resolve and score before returning results
     */
    public BruteForceRowIdIterator(PriorityQueue<RowWithApproximateScore> approximateScoreQueue,
                                   CloseableReranker reranker,
                                   int limit,
                                   int topK)
    {
        this.approximateScoreQueue = approximateScoreQueue;
        this.exactScoreQueue = new PriorityQueue<>(topK, (a, b) -> Float.compare(b.score, a.score));
        this.reranker = reranker;
        assert topK >= limit : "topK must be greater than or equal to limit. Found: " + topK + " < " + limit;
        this.limit = limit;
        this.topK = topK;
        this.rerankedCount = topK; // placeholder to kick off computeNext
    }

    @Override
    protected ScoredRowId computeNext() {
        int consumed = rerankedCount - exactScoreQueue.size();
        if (consumed >= limit) {
            // Refill the exactScoreQueue until it reaches topK exact scores, or the approximate score queue is empty
            while (!approximateScoreQueue.isEmpty() && exactScoreQueue.size() < topK) {
                RowWithApproximateScore rowOrdinalScore = approximateScoreQueue.poll();
                float score = reranker.similarityTo(rowOrdinalScore.ordinal);
                exactScoreQueue.add(new ScoredRowId(rowOrdinalScore.rowId, score));
            }
            rerankedCount = exactScoreQueue.size();
        }
        return exactScoreQueue.isEmpty() ? endOfData() : exactScoreQueue.poll();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(reranker);
    }
}
