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

package org.apache.cassandra.index.sai.disk.v3;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.AutoResumingNodeScoreIterator;
import org.apache.cassandra.index.sai.disk.vector.NodeScoreToScoredRowIdIterator;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorSupplier;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

public class CassandraDiskAnn extends JVectorLuceneOnDiskGraph
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraDiskAnn.class.getName());

    public static final int PQ_MAGIC = 0xB011A61C; // PQ_MAGIC, with a lot of liberties taken

    private final FileHandle graphHandle;
    private final OnDiskOrdinalsMap ordinalsMap;
    private volatile GraphIndex<float[]> graph;
    private final VectorSimilarityFunction similarityFunction;
    @Nullable
    private final CompressedVectors compressedVectors;
    private final boolean pqUnitVectors;

    public CassandraDiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = getComponentMetadata(IndexComponent.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        graph = new OnDiskGraphIndex<>(graphHandle::createReader, termsMetadata.offset);

        long pqSegmentOffset = getComponentMetadata(IndexComponent.PQ).offset;
        try (var pqFile = indexFiles.pq();
             var reader = pqFile.createReader())
        {
            reader.seek(pqSegmentOffset);
            int version = 0;
            if (reader.readInt() == PQ_MAGIC) {
                version = reader.readInt();
                assert version >= 1 : version;
                pqUnitVectors = reader.readBoolean();
            } else {
                pqUnitVectors = true;
                reader.seek(pqSegmentOffset);
            }
            VectorCompression.CompressionType compressionType = VectorCompression.CompressionType.values()[reader.readByte()];
            if (compressionType == VectorCompression.CompressionType.PRODUCT_QUANTIZATION)
                compressedVectors = PQVectors.load(reader, reader.getFilePointer());
            else if (compressionType == VectorCompression.CompressionType.BINARY_QUANTIZATION)
                compressedVectors = BQVectors.load(reader, reader.getFilePointer());
            else
                compressedVectors = null;
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = getComponentMetadata(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
    }

    private static int logBaseX(double val, double base) {
        if (base <= 1.0d || val <= 1.0d)
            return 0;
        return (int)Math.floor(Math.log(val) / Math.log(base));
    }

    @Override
    public long ramBytesUsed()
    {
        return graph instanceof CachingGraphIndex ? ((CachingGraphIndex) graph).ramBytesUsed() : Long.BYTES * 4;
    }

    @Override
    public int size()
    {
        return graph.size();
    }

    /**
     * @param queryVector the query vector
     * @param topK the number of results to look for in the index (>= limit)
     * @param threshold the minimum similarity score to accept
     * @param acceptBits a Bits indicating which row IDs are acceptable, or null if no constraints
     * @param context unused (vestige from HNSW, retained in signature to allow calling both easily)
     * @param nodesVisitedConsumer a consumer that will be called with the number of nodes visited during the search
     * @return Row IDs associated with the topK vectors near the query. If a threshold is specified, only vectors with
     * a similarity score >= threshold will be returned.
     */
    @Override
    public CloseableIterator<ScoredRowId> search(float[] queryVector,
                                                 int topK,
                                                 float threshold,
                                                 Bits acceptBits,
                                                 QueryContext context,
                                                 IntConsumer nodesVisitedConsumer)
    {
        CassandraOnHeapGraph.validateIndexable(queryVector, similarityFunction);

        // We cannot use try-with-resources here because AutoResumingNodeScoreIterator might need to resume the search,
        // which relies on the graph view.
        var view = getView();
        try
        {
            NodeSimilarity.ScoreFunction scoreFunction;
            NodeSimilarity.Reranker reranker;
            if (compressedVectors == null)
            {
                scoreFunction = (NodeSimilarity.ExactScoreFunction)
                                i -> similarityFunction.compare(queryVector, view.getVector(i));
                reranker = null;
            }
            else
            {
                // unit vectors defined with dot product should switch to cosine similarity for compressed
                // comparisons, since the compression does not maintain unit length
                var sf = pqUnitVectors && similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                         ? VectorSimilarityFunction.COSINE
                         : similarityFunction;
                scoreFunction = compressedVectors.approximateScoreFunctionFor(queryVector, sf);
                reranker = i -> similarityFunction.compare(queryVector, view.getVector(i));
            }
            var searcher = new GraphSearcher.Builder<>(view).build();
            var result = searcher.search(scoreFunction, reranker, topK, threshold, ordinalsMap.ignoringDeleted(acceptBits));
            Tracing.trace("DiskANN search visited {} nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
            // Threshold based searches are comprehensive and do not need to resume the search.
            if (threshold > 0)
            {
                FileUtils.closeQuietly(view);
                nodesVisitedConsumer.accept(result.getVisitedCount());
                var nodeScores = CloseableIterator.wrap(Arrays.stream(result.getNodes()).iterator());
                return new NodeScoreToScoredRowIdIterator(nodeScores, ordinalsMap.getRowIdsView());
            }
            else
            {
                var nodeScores = new AutoResumingNodeScoreIterator(searcher, result, nodesVisitedConsumer, topK, false, view);
                return new NodeScoreToScoredRowIdIterator(nodeScores, ordinalsMap.getRowIdsView());
            }
        }
        catch (RuntimeException e)
        {
            FileUtils.closeQuietly(view);
            throw e;
        }
    }

    @Override
    public CompressedVectors getCompressedVectors()
    {
        return compressedVectors;
    }

    @Override
    public void close() throws IOException
    {
        ordinalsMap.close();
        graph.close();
        graphHandle.close();
    }

    @Override
    public OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }

    @Override
    public VectorSupplier getVectorSupplier()
    {
        return new ANNVectorSupplier(getView());
    }

    private GraphIndex.View<float[]> getView()
    {
        // asynchronously cache the most-accessed parts of the graph
        if (!(graph instanceof CachingGraphIndex))
        {
            Stage.IO.executor().execute(() -> {
                synchronized (this)
                {
                    if (graph instanceof CachingGraphIndex)
                        return;
                    // target 1% of the vectors with a max distance of 3
                    int distance = Math.min(logBaseX(0.01d * graph.size(), graph.maxDegree()), 3);
                    logger.debug("Caching {}@{} to distance {}", this, graphHandle.path(), distance);
                    graph = new CachingGraphIndex((OnDiskGraphIndex<float[]>) graph, distance);
                }
            });
        }
        return graph.getView();
    }

    private static class ANNVectorSupplier implements VectorSupplier
    {
        private final GraphIndex.View<float[]> view;

        private ANNVectorSupplier(GraphIndex.View<float[]> view)
        {
            this.view = view;
        }

        @Override
        public float[] getVectorForOrdinal(int ordinal)
        {
            return view.getVector(ordinal);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(view);
        }
    }
}
