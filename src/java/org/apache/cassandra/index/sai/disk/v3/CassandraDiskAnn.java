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
import java.util.Set;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.disk.CachingGraphIndex;
import io.github.jbellis.jvector.graph.disk.FeatureId;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.ExplicitThreadLocal;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.AutoResumingNodeScoreIterator;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.PQVersion;
import org.apache.cassandra.index.sai.disk.vector.GraphSearcherAccessManager;
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
import org.apache.cassandra.utils.CloseableIterator;

import static java.lang.Math.min;

public class CassandraDiskAnn extends JVectorLuceneOnDiskGraph
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraDiskAnn.class.getName());

    public static final int PQ_MAGIC = 0xB011A61C; // PQ_MAGIC, with a lot of liberties taken

    private final FileHandle graphHandle;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final Set<FeatureId> features;
    private final GraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;
    @Nullable
    private final CompressedVectors compressedVectors;
    @Nullable
    private final ProductQuantization pq;
    private final VectorCompression compression;
    final boolean pqUnitVectors;

    private final ExplicitThreadLocal<GraphSearcherAccessManager> searchers;

    public CassandraDiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = this.componentMetadatas.get(IndexComponentType.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        var rawGraph = OnDiskGraphIndex.load(graphHandle::createReader, termsMetadata.offset);
        features = rawGraph.getFeatureSet();
        graph = V3OnDiskFormat.ENABLE_EDGES_CACHE ? cachingGraphFor(rawGraph) : rawGraph;

        long pqSegmentOffset = this.componentMetadatas.get(IndexComponentType.PQ).offset;
        try (var pqFile = indexFiles.pq();
             var reader = pqFile.createReader())
        {
            reader.seek(pqSegmentOffset);
            var version = PQVersion.V0;
            if (reader.readInt() == PQ_MAGIC)
            {
                version = PQVersion.values()[reader.readInt()];
                assert PQVersion.V1.compareTo(version) >= 0 : String.format("Old PQ version %s written with PQ_MAGIC!?", version);
                pqUnitVectors = reader.readBoolean();
            }
            else
            {
                pqUnitVectors = true;
                reader.seek(pqSegmentOffset);
            }

            VectorCompression.CompressionType compressionType = VectorCompression.CompressionType.values()[reader.readByte()];
            if (features.contains(FeatureId.FUSED_ADC))
            {
                assert compressionType == VectorCompression.CompressionType.PRODUCT_QUANTIZATION;
                compressedVectors = null;
                // don't load full PQVectors, all we need is the metadata from the PQ at the start
                pq = ProductQuantization.load(reader);
                compression = new VectorCompression(VectorCompression.CompressionType.PRODUCT_QUANTIZATION,
                                                    rawGraph.getDimension() * Float.BYTES,
                                                    pq.compressedVectorSize());
            }
            else
            {
                if (compressionType == VectorCompression.CompressionType.PRODUCT_QUANTIZATION)
                {
                    compressedVectors = PQVectors.load(reader, reader.getFilePointer());
                    pq = ((PQVectors) compressedVectors).getProductQuantization();
                    compression = new VectorCompression(compressionType,
                                                        compressedVectors.getOriginalSize(),
                                                        compressedVectors.getCompressedSize());
                }
                else if (compressionType == VectorCompression.CompressionType.BINARY_QUANTIZATION)
                {
                    compressedVectors = BQVectors.load(reader, reader.getFilePointer());
                    pq = null;
                    compression = new VectorCompression(compressionType,
                                                        compressedVectors.getOriginalSize(),
                                                        compressedVectors.getCompressedSize());
                }
                else
                {
                    compressedVectors = null;
                    pq = null;
                    compression = VectorCompression.NO_COMPRESSION;
                }
            }
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = this.componentMetadatas.get(IndexComponentType.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        searchers = ExplicitThreadLocal.withInitial(() -> new GraphSearcherAccessManager(new GraphSearcher(graph)));
    }

    public ProductQuantization getPQ()
    {
        assert compression.type == VectorCompression.CompressionType.PRODUCT_QUANTIZATION;
        assert pq != null;
        return pq;
    }

    private GraphIndex cachingGraphFor(OnDiskGraphIndex rawGraph)
    {
        // cache edges around the entry point
        // we can easily hold 1% of the edges in memory for typical index sizes, but
        // there is a lot of redundancy in the nodes we observe in practice around the entry point
        // (only 10%-20% are unique), so use 5% as our target.
        //
        // 32**3 = 32k, which would be 4MB if all the nodes are unique, so 3 levels deep is a safe upper bound
        int distance = min(logBaseX(0.05d * rawGraph.size(), rawGraph.maxDegree()), 3);
        var result = new CachingGraphIndex(rawGraph, distance);
        logger.debug("Cached {}@{} to distance {} in {}B",
                     this, graphHandle.path(), distance, result.ramBytesUsed());
        return result;
    }

    private static int logBaseX(double val, double base) {
        if (base <= 1.0d || val <= 1.0d)
            return 0;
        return (int)Math.floor(Math.log(val) / Math.log(base));
    }

    @Override
    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public int size()
    {
        return graph.size();
    }

    /**
     * @param queryVector the query vector
     * @param limit the number of results to look for in the index (>= limit)
     * @param rerankK the number of results to look for in the index (>= limit)
     * @param threshold the minimum similarity score to accept
     * @param acceptBits a Bits indicating which row IDs are acceptable, or null if no constraints
     * @param context unused (vestige from HNSW, retained in signature to allow calling both easily)
     * @param nodesVisitedConsumer a consumer that will be called with the number of nodes visited during the search
     * @return Iterator of Row IDs associated with the vectors near the query. If a threshold is specified, only vectors
     * with a similarity score >= threshold will be returned.
     */
    @Override
    public CloseableIterator<RowIdWithScore> search(VectorFloat<?> queryVector,
                                                    int limit,
                                                    int rerankK,
                                                    float threshold,
                                                    Bits acceptBits,
                                                    QueryContext context,
                                                    IntConsumer nodesVisitedConsumer)
    {
        VectorValidation.validateIndexable(queryVector, similarityFunction);

        var graphAccessManager = searchers.get();
        var searcher = graphAccessManager.get();
        var view = (GraphIndex.ScoringView) searcher.getView();
        SearchScoreProvider ssp;
        if (features.contains(FeatureId.FUSED_ADC))
        {
            var asf = view.approximateScoreFunctionFor(queryVector, similarityFunction);
            var rr = view.rerankerFor(queryVector, similarityFunction);
            ssp = new SearchScoreProvider(asf, rr);
        }
        else if (compressedVectors == null)
        {
            ssp = new SearchScoreProvider(view.rerankerFor(queryVector, similarityFunction));
        }
        else
        {
            // unit vectors defined with dot product should switch to cosine similarity for compressed
            // comparisons, since the compression does not maintain unit length
            var sf = pqUnitVectors && similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                     ? VectorSimilarityFunction.COSINE
                     : similarityFunction;
            var asf = compressedVectors.precomputedScoreFunctionFor(queryVector, sf);
            var rr = view.rerankerFor(queryVector, sf);
            ssp = new SearchScoreProvider(asf, rr);
        }
        var result = searcher.search(ssp, limit, rerankK, threshold, context.getAnnRerankFloor(), ordinalsMap.ignoringDeleted(acceptBits));
        if (V3OnDiskFormat.ENABLE_RERANK_FLOOR)
            context.updateAnnRerankFloor(result.getWorstApproximateScoreInTopK());
        Tracing.trace("DiskANN search for {}/{} visited {} nodes, reranked {} to return {} results",
                      limit, rerankK, result.getVisitedCount(), result.getRerankedCount(), result.getNodes().length);
        if (threshold > 0)
        {
            // Threshold based searches are comprehensive and do not need to resume the search.
            graphAccessManager.release();
            nodesVisitedConsumer.accept(result.getVisitedCount());
            var nodeScores = CloseableIterator.wrap(Arrays.stream(result.getNodes()).iterator());
            return new NodeScoreToRowIdWithScoreIterator(nodeScores, ordinalsMap.getRowIdsView());
        }
        else
        {
            var nodeScores = new AutoResumingNodeScoreIterator(searcher, graphAccessManager, result, nodesVisitedConsumer, limit, rerankK, false);
            return new NodeScoreToRowIdWithScoreIterator(nodeScores, ordinalsMap.getRowIdsView());
        }
    }

    @Override
    public VectorCompression getCompression()
    {
        return compression;
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
        FileUtils.closeQuietly(searchers);
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
        return new ANNVectorSupplier((GraphIndex.ScoringView) graph.getView());
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
