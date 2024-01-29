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

package org.apache.cassandra.index.sai.disk.v2.hnsw;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.NodeScoreToScoredRowIdIterator;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.index.sai.disk.vector.VectorSupplier;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class CassandraOnDiskHnsw extends JVectorLuceneOnDiskGraph
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnDiskHnsw.class);

    private final Supplier<VectorsWithCache> vectorsSupplier;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorCache vectorCache;

    private static final int OFFSET_CACHE_MIN_BYTES = 100_000;
    private FileHandle vectorsFile;

    public CassandraOnDiskHnsw(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        vectorsFile = indexFiles.vectors();
        long vectorsSegmentOffset = getComponentMetadata(IndexComponent.VECTOR).offset;
        vectorsSupplier = () -> new VectorsWithCache(new OnDiskVectors(vectorsFile, vectorsSegmentOffset));

        SegmentMetadata.ComponentMetadata postingListsMetadata = getComponentMetadata(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        SegmentMetadata.ComponentMetadata termsMetadata = getComponentMetadata(IndexComponent.TERMS_DATA);
        hnsw = new OnDiskHnswGraph(indexFiles.termsData(), termsMetadata.offset, termsMetadata.length, OFFSET_CACHE_MIN_BYTES);
        var mockContext = new QueryContext();
        try (var vectors = new OnDiskVectors(vectorsFile, vectorsSegmentOffset))
        {
            vectorCache = VectorCache.load(hnsw.getView(mockContext), vectors, CassandraRelevantProperties.SAI_HNSW_VECTOR_CACHE_BYTES.getInt());
        }
    }

    @Override
    public long ramBytesUsed()
    {
        return hnsw.getCacheSizeInBytes() + vectorCache.ramBytesUsed();
    }

    @Override
    public int size()
    {
        return hnsw.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    @Override
    public CloseableIterator<ScoredRowId> search(float[] queryVector, int topK, float threshold, Bits acceptBits, QueryContext context, IntConsumer nodesVisited)
    {
        if (threshold > 0)
            throw new InvalidRequestException("Geo queries are not supported for legacy SAI indexes -- drop the index and recreate it to enable these");

        CassandraOnHeapGraph.validateIndexable(queryVector, similarityFunction);

        NeighborQueue queue;
        try (var vectors = vectorsSupplier.get(); var view = hnsw.getView(context))
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             topK,
                                             vectors,
                                             VectorEncoding.FLOAT32,
                                             LuceneCompat.vsf(similarityFunction),
                                             view,
                                             LuceneCompat.bits(ordinalsMap.ignoringDeleted(acceptBits)),
                                             Integer.MAX_VALUE);
            // Since we do not resume search for HNSW, we call this eagerly.
            nodesVisited.accept(queue.visitedCount());
            Tracing.trace("HNSW search visited {} nodes to return {} results", queue.visitedCount(), queue.size());
            var scores = new ReorderingNodeScoresIterator(queue);
            return new NodeScoreToScoredRowIdIterator(scores, ordinalsMap.getRowIdsView());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * An iterator that reorders the results from HNSW to descending score order.
     */
    static class ReorderingNodeScoresIterator implements CloseableIterator<SearchResult.NodeScore>
    {
        private final SearchResult.NodeScore[] scores;
        private int index;

        public ReorderingNodeScoresIterator(NeighborQueue queue)
        {
            scores = new SearchResult.NodeScore[queue.size()];
            // We start at the last element since the queue is sorted in ascending order
            index = queue.size() - 1;
            // Because the queue is sorted in ascending order, we need to eagerly consume it
            for (int i = 0; i <= index; i++)
            {
                // Get the score first since pop removes the top element
                float score = queue.topScore();
                scores[i] = new SearchResult.NodeScore(queue.pop(), score);
            }
        }

        @Override
        public boolean hasNext()
        {
            if (index < 0)
            {
                logger.warn("HNSW queue is empty, returning false, but the search might not have been exhaustive");
                return false;
            }
            return true;
        }

        @Override
        public SearchResult.NodeScore next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return scores[index--];
        }

        @Override
        public void close() {}
    }

    @Override
    public void close()
    {
        vectorsFile.close();
        ordinalsMap.close();
        hnsw.close();
    }

    @Override
    public OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }

    @Override
    public VectorSupplier getVectorSupplier()
    {
        return new HNSWVectorSupplier(vectorsSupplier.get());
    }

    @Override
    public CompressedVectors getCompressedVectors()
    {
        return null;
    }

    @NotThreadSafe
    class VectorsWithCache implements RandomAccessVectorValues<float[]>, AutoCloseable
    {
        private final OnDiskVectors vectors;

        public VectorsWithCache(OnDiskVectors vectors)
        {
            this.vectors = vectors;
        }

        @Override
        public int size()
        {
            return vectors.size();
        }

        @Override
        public int dimension()
        {
            return vectors.dimension();
        }

        @Override
        public float[] vectorValue(int i) throws IOException
        {
            var cached = vectorCache.get(i);
            if (cached != null)
                return cached;

            return vectors.vectorValue(i);
        }

        @Override
        public RandomAccessVectorValues<float[]> copy()
        {
            throw new UnsupportedOperationException();
        }

        public void close()
        {
            vectors.close();
        }
    }

    private static class HNSWVectorSupplier implements VectorSupplier
    {
        private final VectorsWithCache view;

        private HNSWVectorSupplier(VectorsWithCache view)
        {
            this.view = view;
        }

        @Override
        public float[] getVectorForOrdinal(int ordinal)
        {
            try
            {
                return view.vectorValue(ordinal);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(view);
        }
    }
}
