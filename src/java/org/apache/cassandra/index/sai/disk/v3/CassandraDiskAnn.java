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

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.disk.CachingGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.ExplicitThreadLocal;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.AutoResumingNodeScoreIterator;
import org.apache.cassandra.index.sai.disk.vector.NodeScoreToScoredRowIdIterator;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorValidation;
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
    private volatile GraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;
    @Nullable
    private final CompressedVectors compressedVectors;
    final boolean pqUnitVectors;

    private final ExplicitThreadLocal<GraphSearcher> searchers;

    public CassandraDiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = this.componentMetadatas.get(IndexComponent.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        graph = OnDiskGraphIndex.load(graphHandle::createReader, termsMetadata.offset);

        long pqSegmentOffset = this.componentMetadatas.get(IndexComponent.PQ).offset;
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

        SegmentMetadata.ComponentMetadata postingListsMetadata = this.componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        searchers = ExplicitThreadLocal.withInitial(() -> new GraphSearcher(graph));
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
    public CloseableIterator<ScoredRowId> search(VectorFloat<?> queryVector,
                                                 int topK,
                                                 float threshold,
                                                 Bits acceptBits,
                                                 QueryContext context,
                                                 IntConsumer nodesVisitedConsumer)
    {
        VectorValidation.validateIndexable(queryVector, similarityFunction);

        var searcher = searchers.get();
        var view = (GraphIndex.ScoringView) searcher.getView();
        SearchScoreProvider ssp;
        if (compressedVectors == null)
        {
            ssp = new SearchScoreProvider(view.rerankerFor(queryVector, similarityFunction), null);
        }
        else
        {
            // unit vectors defined with dot product should switch to cosine similarity for compressed
            // comparisons, since the compression does not maintain unit length
            var sf = pqUnitVectors && similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                     ? VectorSimilarityFunction.COSINE
                     : similarityFunction;
            var asf = compressedVectors.scoreFunctionFor(queryVector, sf);
            var rr = view.rerankerFor(queryVector, sf);
            ssp = new SearchScoreProvider(asf, rr);
        }
        var result = searcher.search(ssp, topK, threshold, ordinalsMap.ignoringDeleted(acceptBits));
        Tracing.trace("DiskANN search visited {} nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        // Threshold based searches are comprehensive and do not need to resume the search.
        if (threshold > 0)
        {
            nodesVisitedConsumer.accept(result.getVisitedCount());
            var nodeScores = CloseableIterator.wrap(Arrays.stream(result.getNodes()).iterator());
            return new NodeScoreToScoredRowIdIterator(nodeScores, ordinalsMap.getRowIdsView());
        }
        else
        {
            var nodeScores = new AutoResumingNodeScoreIterator(searcher, result, nodesVisitedConsumer, topK, false);
            return new NodeScoreToScoredRowIdIterator(nodeScores, ordinalsMap.getRowIdsView());
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
        searchers.close();
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

    private GraphIndex.ScoringView getView()
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
                    graph = new CachingGraphIndex((OnDiskGraphIndex) graph, distance);
                }
            });
        }
        return (GraphIndex.ScoringView) graph.getView();
    }

    private static class ANNVectorSupplier implements VectorSupplier
    {
        private final GraphIndex.ScoringView view;

        private ANNVectorSupplier(GraphIndex.ScoringView view)
        {
            this.view = view;
        }

        @Override
        public ScoreFunction.ExactScoreFunction getScoreFunction(VectorFloat<?> queryVector, VectorSimilarityFunction similarityFunction)
        {
            return view.rerankerFor(queryVector, similarityFunction);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(view);
        }
    }
}