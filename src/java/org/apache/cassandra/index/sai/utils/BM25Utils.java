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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.io.sstable.SSTableId;
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
        private final float termCount;

        private DocTF(PrimaryKey pk, float termCount, Map<ByteBuffer, Integer> frequencies)
        {
            this.pk = pk;
            this.frequencies = frequencies;
            this.termCount = termCount;
        }

        public int getTermFrequency(ByteBuffer term)
        {
            return frequencies.getOrDefault(term, 0);
        }

        public static DocTF createFromDocument(PrimaryKey pk,
                                             Cell<?> cell,
                                             AbstractAnalyzer docAnalyzer,
                                             Collection<ByteBuffer> queryTerms)
        {
            float count = 0;
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

            return new DocTF(pk, count, frequencies);
        }
    }

    @FunctionalInterface
    public interface CellReader
    {
        Cell<?> readCell(PrimaryKey pk);
    }

    public static CloseableIterator<PrimaryKeyWithSortKey> computeScores(Iterator<PrimaryKey> keyIterator,
                                                                         List<ByteBuffer> queryTerms,
                                                                         DocStats docStats,
                                                                         IndexContext indexContext,
                                                                         Object source,
                                                                         CellReader cellReader)
    {
        var docAnalyzer = indexContext.getAnalyzerFactory().create();

        // data structures for document stats and frequencies
        ArrayList<DocTF> documents = new ArrayList<>();
        double totalTermCount = 0;

        // Compute TF within each document
        while (keyIterator.hasNext())
        {
            var pk = keyIterator.next();
            var cell = cellReader.readCell(pk);
            if (cell == null)
                continue;
            var tf = DocTF.createFromDocument(pk, cell, docAnalyzer, queryTerms);

            // sstable index will only send documents that contain all query terms to this method,
            // but memtable is not indexed and will send all documents, so we have to skip documents
            // that don't contain all query terms here to preserve consistency with sstable behavior
            if (tf.frequencies.size() != queryTerms.size())
                continue;

            documents.add(tf);

            totalTermCount += tf.termCount;
        }

        // Calculate average document length
        double avgDocLength = documents.size() > 0 ? totalTermCount / documents.size() : 0.0;

        // Calculate BM25 scores
        var scoredDocs = new ArrayList<PrimaryKeyWithScore>(documents.size());
        for (var doc : documents)
        {
            double score = 0.0;
            for (var queryTerm : queryTerms)
            {
                int tf = doc.getTermFrequency(queryTerm);
                Long df = docStats.frequencies.get(queryTerm);
                double normalizedTf = tf / (tf + K1 * (1 - B + B * doc.termCount / avgDocLength));
                double idf = Math.log(1 + (docStats.docCount - df + 0.5) / (df + 0.5));
                double deltaScore = normalizedTf * idf;
                assert deltaScore >= 0 : String.format("BM25 score for tf=%d, df=%d, totalDocs=%d is %f", tf, df, docStats.docCount, deltaScore);
                score += deltaScore;
            }
            if (source instanceof Memtable)
                scoredDocs.add(new PrimaryKeyWithScore(indexContext, (Memtable) source, doc.pk, (float) score));
            else if (source instanceof SSTableId)
                scoredDocs.add(new PrimaryKeyWithScore(indexContext, (SSTableId) source, doc.pk, (float) score));
            else
                throw new IllegalArgumentException("Invalid source " + source.getClass());
        }

        // sort by score
        scoredDocs.sort(Comparator.comparingDouble((PrimaryKeyWithScore pkws) -> pkws.indexScore).reversed());

        return (CloseableIterator<PrimaryKeyWithSortKey>) (CloseableIterator) CloseableIterator.wrap(scoredDocs.iterator());
    }
}
