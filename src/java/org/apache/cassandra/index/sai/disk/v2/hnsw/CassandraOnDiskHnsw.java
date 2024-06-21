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
import java.util.function.IntConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.NodeScoreToRowIdWithScoreIterator;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorValidation;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;

public class CassandraOnDiskHnsw extends JVectorLuceneOnDiskGraph
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnDiskHnsw.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;

    private static final int OFFSET_CACHE_MIN_BYTES = 100_000;
    private final OnDiskVectors vectors;
    private FileHandle vectorsFile;

    public CassandraOnDiskHnsw(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);
        logger.warn("Opening HNSW index -- we thought they were all upgraded to DiskANN");

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        vectorsFile = indexFiles.vectors();
        long vectorsSegmentOffset = this.componentMetadatas.get(IndexComponent.VECTOR).offset;

        SegmentMetadata.ComponentMetadata postingListsMetadata = this.componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        SegmentMetadata.ComponentMetadata termsMetadata = this.componentMetadatas.get(IndexComponent.TERMS_DATA);
        hnsw = new OnDiskHnswGraph(indexFiles.termsData(), termsMetadata.offset, termsMetadata.length, OFFSET_CACHE_MIN_BYTES);
        vectors = new OnDiskVectors(vectorsFile, vectorsSegmentOffset);
    }

    @Override
    public long ramBytesUsed()
    {
        return hnsw.getCacheSizeInBytes();
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
    public CloseableIterator<RowIdWithScore> search(VectorFloat<?> queryVector, int limit, int rerankK, float threshold, Bits acceptBits, QueryContext context, IntConsumer nodesVisited)
    {
        if (threshold > 0)
            throw new InvalidRequestException("Geo queries are not supported for legacy SAI indexes -- drop the index and recreate it to enable these");

        VectorValidation.validateIndexable(queryVector, similarityFunction);

        NeighborQueue queue;
        try (var view = hnsw.getView(context))
        {
            queue = HnswGraphSearcher.search(((ArrayVectorFloat) queryVector).get(),
                                             rerankK,
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
            return new NodeScoreToRowIdWithScoreIterator(scores, ordinalsMap.getRowIdsView());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * An iterator that reorders the results from HNSW to descending score order.
     */
    static class ReorderingNodeScoresIterator extends AbstractIterator<SearchResult.NodeScore>
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
        protected SearchResult.NodeScore computeNext()
        {
            if (index < 0)
            {
                logger.warn("HNSW queue is empty, returning false, but the search might not have been exhaustive");
                return endOfData();
            }
            return scores[index--];
        }
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
        return new HNSWVectorSupplier(vectors);
    }

    @Override
    public VectorCompression getCompression()
    {
        return new VectorCompression(VectorCompression.CompressionType.NONE,
                                     vectors.dimension() * Float.BYTES,
                                     vectors.dimension() * Float.BYTES);
    }

    @Override
    public CompressedVectors getCompressedVectors()
    {
        return null;
    }

    private static class HNSWVectorSupplier implements VectorSupplier
    {
        private final OnDiskVectors view;

        private HNSWVectorSupplier(OnDiskVectors view)
        {
            this.view = view;
        }

        @Override
        public ScoreFunction.ExactScoreFunction getScoreFunction(VectorFloat<?> queryVector, VectorSimilarityFunction similarityFunction)
        {
            return i -> {
                try
                {
                    var v2 = vts.createFloatVector(view.vectorValue(i));
                    return similarityFunction.compare(queryVector, v2);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            };
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(view);
        }
    }
}
