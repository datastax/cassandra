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

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

public class BM25Utils
{
    private static final float K1 = 1.2f;  // BM25 term frequency saturation parameter
    private static final float B = 0.75f;  // BM25 length normalization parameter

    /**
     * Term frequencies across all documents.  Each document is only counted once.
     */
    public static class DocStats
    {
        // Map of term -> count of docs containing that term
        private final Map<ByteBuffer, Long> frequencies;
        // total number of docs in the index
        private final long docCount;

        public DocStats(Map<ByteBuffer, Long> frequencies, long docCount)
        {
            this.frequencies = frequencies;
            this.docCount = docCount;
        }
    }

    /**
     * Term frequencies within a single document.  All instances of a term are counted.
     */
    public static class DocTF
    {
        private final PrimaryKey pk;
        private final Map<ByteBuffer, Integer> frequencies;
        private final int termCount;

        public DocTF(PrimaryKey pk, int termCount, Map<ByteBuffer, Integer> frequencies)
        {
            this.pk = pk;
            this.frequencies = frequencies;
            this.termCount = termCount;
        }

        public int getTermFrequency(ByteBuffer term)
        {
            return frequencies.getOrDefault(term, 0);
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

            return new DocTF(pk, count, frequencies);
        }
    }

    public static CloseableIterator<PrimaryKeyWithSortKey> computeScores(CloseableIterator<DocTF> docIterator,
                                                                         List<ByteBuffer> queryTerms,
                                                                         DocStats docStats,
                                                                         IndexContext indexContext,
                                                                         Object source)
    {
        // data structures for document stats and frequencies
        ArrayList<DocTF> documents = new ArrayList<>();
        double totalTermCount = 0;

        // Compute TF within each document
        while (docIterator.hasNext())
        {
            var tf = docIterator.next();
            documents.add(tf);
            totalTermCount += tf.termCount;
        }
        if (documents.isEmpty())
            return CloseableIterator.emptyIterator();

        // Calculate average document length
        double avgDocLength = totalTermCount / documents.size();

        // Calculate BM25 scores
        var scoredDocs = new ArrayList<PrimaryKeyWithScore>(documents.size());
        for (var doc : documents)
        {
            double score = 0.0;
            for (var queryTerm : queryTerms)
            {
                int tf = doc.getTermFrequency(queryTerm);
                Long df = docStats.frequencies.get(queryTerm);
                // we shouldn't have more hits for a term than we counted total documents
                assert df <= docStats.docCount : String.format("df=%d, totalDocs=%d", df, docStats.docCount);

                double normalizedTf = tf / (tf + K1 * (1 - B + B * doc.termCount / avgDocLength));
                double idf = Math.log(1 + (docStats.docCount - df + 0.5) / (df + 0.5));
                double deltaScore = normalizedTf * idf;
                assert deltaScore >= 0 : String.format("BM25 score for tf=%d, df=%d, tc=%d, totalDocs=%d is %f",
                                                       tf, df, doc.termCount, docStats.docCount, deltaScore);
                score += deltaScore;
            }
            if (source instanceof Memtable)
                scoredDocs.add(new PrimaryKeyWithScore(indexContext, (Memtable) source, doc.pk, (float) score));
            else if (source instanceof SSTableId)
                scoredDocs.add(new PrimaryKeyWithScore(indexContext, (SSTableId) source, doc.pk, (float) score));
            else
                throw new IllegalArgumentException("Invalid source " + source.getClass());
        }

        // sort by score (PKWS implements Comparator correctly for us)
        Collections.sort(scoredDocs);

        return new CloseableIterator<>()
        {
            private final Iterator<PrimaryKeyWithScore> iterator = scoredDocs.iterator();

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public PrimaryKeyWithSortKey next()
            {
                return iterator.next();
            }

            @Override
            public void close()
            {
                FileUtils.closeQuietly(docIterator);
            }
        };
    }
}
