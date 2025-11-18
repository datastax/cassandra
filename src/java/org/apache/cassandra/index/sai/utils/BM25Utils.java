/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import io.github.jbellis.jvector.graph.NodeQueue;
import io.github.jbellis.jvector.util.BoundedLongHeap;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

public class BM25Utils
{
    private static final float K1 = 1.2f;  // BM25 term frequency saturation parameter
    private static final float B = 0.75f;  // BM25 length normalization parameter

    /**
     * Term frequencies within a single document.  All instances of a term are counted. Allows us to optimize for
     * the sstable use case, which is able to skip some reads from disk as well as some memory allocations.
     */
    public interface DocTF
    {
        int getTermFrequency(ByteBuffer term);
        int termCount();
        PrimaryKeyWithSortKey primaryKey(IndexContext context, Memtable source, float score);
        PrimaryKeyWithSortKey primaryKey(IndexContext context, SSTableId<?> source, float score);
    }

    /**
     * Term frequencies within a single document.  All instances of a term are counted. It is eager in that the
     * PrimaryKey is already created.
     */
    public static class EagerDocTF implements DocTF
    {
        private final PrimaryKey pk;
        private final Map<ByteBuffer, Integer> frequencies;
        private final int termCount;

        public EagerDocTF(PrimaryKey pk, int termCount, Map<ByteBuffer, Integer> frequencies)
        {
            this.pk = pk;
            this.frequencies = frequencies;
            this.termCount = termCount;
        }

        public int getTermFrequency(ByteBuffer term)
        {
            return frequencies.getOrDefault(term, 0);
        }

        public int termCount()
        {
            return termCount;
        }

        public PrimaryKeyWithSortKey primaryKey(IndexContext context, Memtable source, float score)
        {
            return new PrimaryKeyWithScore(context, source, pk, score);
        }

        public PrimaryKeyWithSortKey primaryKey(IndexContext context, SSTableId<?> source, float score)
        {
            // BM25 scores are exact.
            return new PrimaryKeyWithScore(context, source, pk, score, false);
        }

        @Nullable
        public static DocTF createFromDocument(PrimaryKey pk,
                                               Cell<?> cell,
                                               AbstractAnalyzer docAnalyzer,
                                               Collection<ByteBuffer> queryTerms)
        {
            if (cell == null)
                return null;

            int count = 0;
            Map<ByteBuffer, Integer> frequencies = new HashMap<>();
            docAnalyzer.reset(cell.buffer());
            try
            {
                while (docAnalyzer.hasNext())
                {
                    ByteBuffer term = docAnalyzer.next();
                    count++;
                    if (queryTerms.contains(term))
                        frequencies.merge(term, 1, Integer::sum);
                }
            }
            finally
            {
                docAnalyzer.end();
            }

            // Every query term must be present in the document
            if (queryTerms.size() > frequencies.size())
                return null;

            return new EagerDocTF(pk, count, frequencies);
        }
    }

    public static CloseableIterator<PrimaryKeyWithSortKey> computeScores(CloseableIterator<DocTF> docIterator,
                                                                         List<ByteBuffer> queryTerms,
                                                                         DocBm25Stats docStats,
                                                                         IndexContext indexContext,
                                                                         Object source,
                                                                         boolean isOldFormat)
    {
        assert source instanceof Memtable || source instanceof SSTableId : "Invalid source " + source.getClass();

        // data structures for document stats and frequencies
        ArrayList<DocTF> documents = new ArrayList<>();
        double totalTermCount = 0;

        // Compute TF within each document
        while (docIterator.hasNext())
        {
            var tf = docIterator.next();
            documents.add(tf);
            if (isOldFormat)
                totalTermCount += tf.termCount();
        }

        // An index format before {@link Version#ED} doesn't store the total term count
        // on the disk to read it back. Thus, for the old format version it is calculated in the old way.
        double avgDocLength = (isOldFormat && !documents.isEmpty())
                              ? totalTermCount / documents.size()
                              : docStats.getAvgDocLength();

        if (documents.isEmpty())
            return CloseableIterator.emptyIterator();

        // Calculate BM25 scores.
        // Uses a NodeQueue that avoids allocating an object for each document.
        var nodeQueue = new NodeQueue(new BoundedLongHeap(documents.size()), NodeQueue.Order.MAX_HEAP);
        // Create an anonymous NodeScoreIterator that holds the logic for computing BM25
        var iter = new NodeQueue.NodeScoreIterator() {
            int current = 0;

            @Override
            public boolean hasNext() {
                return current < documents.size();
            }

            @Override
            public int pop() {
                return current++;
            }

            @Override
            public float topScore() {
                // Compute BM25 for the current document
                return scoreDoc(documents.get(current),
                                docStats.getFrequencies(), docStats.getDocCount(), avgDocLength,
                                queryTerms);
            }
        };
        // pushMany is an O(n) operation where n is the final size of the queue. Iterative calls to push is O(n log n).
        nodeQueue.pushMany(iter, documents.size());

        return new NodeQueueDocTFIterator(nodeQueue, documents, indexContext, source, docIterator);
    }

    private static float scoreDoc(DocTF doc, Map<ByteBuffer, Long> frequencies, long docCount, double avgDocLength, List<ByteBuffer> queryTerms)
    {
        double score = 0.0;
        for (var queryTerm : queryTerms)
        {
            int tf = doc.getTermFrequency(queryTerm);
            Long df = frequencies.get(queryTerm);
            // we shouldn't have more hits for a term than we counted total documents
            assert df <= docCount : String.format("df=%d, totalDocs=%d", df, docCount);

            double normalizedTf = tf / (tf + K1 * (1 - B + B * doc.termCount() / avgDocLength));
            double idf = Math.log(1 + (docCount - df + 0.5) / (df + 0.5));
            double deltaScore = normalizedTf * idf;
            assert deltaScore >= 0 : String.format("BM25 score for tf=%d, df=%d, tc=%d, totalDocs=%d is %f",
                                                   tf, df, doc.termCount(), docCount, deltaScore);
            score += deltaScore;
        }
        return (float) score;
    }

    private static class NodeQueueDocTFIterator extends AbstractIterator<PrimaryKeyWithSortKey>
    {
        private final NodeQueue nodeQueue;
        private final List<DocTF> documents;
        private final IndexContext indexContext;
        private final Object source;
        private final CloseableIterator<DocTF> docIterator;

        NodeQueueDocTFIterator(NodeQueue nodeQueue, List<DocTF> documents, IndexContext indexContext, Object source, CloseableIterator<DocTF> docIterator)
        {
            this.nodeQueue = nodeQueue;
            this.documents = documents;
            this.indexContext = indexContext;
            this.source = source;
            this.docIterator = docIterator;
        }

        @Override
        protected PrimaryKeyWithSortKey computeNext()
        {
            if (nodeQueue.size() == 0)
                return endOfData();

            var score = nodeQueue.topScore();
            var node = nodeQueue.pop();
            var doc = documents.get(node);
            if (source instanceof Memtable)
                return doc.primaryKey(indexContext, (Memtable) source, score);
            else
                return doc.primaryKey(indexContext, (SSTableId<?>) source, score);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(docIterator);
        }
    }
}
