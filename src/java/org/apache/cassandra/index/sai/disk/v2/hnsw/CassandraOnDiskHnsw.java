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
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.IntStream;

import javax.annotation.concurrent.NotThreadSafe;

import com.github.jbellis.jvector.util.Bits;
import com.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class CassandraOnDiskHnsw implements JVectorLuceneOnDiskGraph, AutoCloseable
{
    private final Function<QueryContext, VectorsWithCache> vectorsSupplier;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorCache vectorCache;

    private static final int OFFSET_CACHE_MIN_BYTES = 100_000;

    public CassandraOnDiskHnsw(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        long vectorsSegmentOffset = componentMetadatas.get(IndexComponent.VECTOR).offset;
        vectorsSupplier = (qc) -> new VectorsWithCache(new OnDiskVectors(indexFiles.vectors(), vectorsSegmentOffset), qc);

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        hnsw = new OnDiskHnswGraph(indexFiles.termsData(), termsMetadata.offset, termsMetadata.length, OFFSET_CACHE_MIN_BYTES);
        var mockContext = new QueryContext();
        try (var vectors = new OnDiskVectors(indexFiles.vectors(), vectorsSegmentOffset))
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
    // VSTODO make this return something with a size
    @Override
    public ReorderingPostingList search(float[] queryVector, int topK, Bits acceptBits, QueryContext context)
    {
        CassandraOnHeapGraph.validateIndexable(queryVector, similarityFunction);

        NeighborQueue queue;
        try (var vectors = vectorsSupplier.apply(context); var view = hnsw.getView(context))
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             topK,
                                             vectors,
                                             VectorEncoding.FLOAT32,
                                             LuceneCompat.vsf(similarityFunction),
                                             view,
                                             LuceneCompat.bits(ordinalsMap.ignoringDeleted(acceptBits)),
                                             Integer.MAX_VALUE);
            return annRowIdsToPostings(queue);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final NeighborQueue queue;
        private final OnDiskOrdinalsMap.RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(NeighborQueue queue)
        {
            this.queue = queue;
        }

        @Override
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && queue.size() > 0) {
                try
                {
                    var ordinal = queue.pop();
                    segmentRowIdIterator = Arrays.stream(rowIdsView.getSegmentRowIdsMatching(ordinal)).iterator();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return segmentRowIdIterator.hasNext();
        }

        @Override
        public int nextInt() {
            if (!hasNext())
                throw new NoSuchElementException();
            return segmentRowIdIterator.nextInt();
        }

        @Override
        public void close()
        {
            rowIdsView.close();
        }
    }

    private ReorderingPostingList annRowIdsToPostings(NeighborQueue queue) throws IOException
    {
        int originalSize = queue.size();
        try (var iterator = new RowIdIterator(queue))
        {
            return new ReorderingPostingList(iterator, originalSize);
        }
    }

    @Override
    public void close()
    {
        ordinalsMap.close();
        hnsw.close();
    }

    @Override
    public OnDiskOrdinalsMap.OrdinalsView getOrdinalsView() throws IOException
    {
        return ordinalsMap.getOrdinalsView();
    }

    @NotThreadSafe
    class VectorsWithCache implements RandomAccessVectorValues<float[]>, AutoCloseable
    {
        private final OnDiskVectors vectors;
        private final QueryContext queryContext;

        public VectorsWithCache(OnDiskVectors vectors, QueryContext queryContext)
        {
            this.vectors = vectors;
            this.queryContext = queryContext;
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
            queryContext.hnswVectorsAccessed++;
            var cached = vectorCache.get(i);
            if (cached != null)
            {
                queryContext.hnswVectorCacheHits++;
                return cached;
            }

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
}
