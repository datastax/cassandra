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

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.BQVectors;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.ExplicitThreadLocal;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.PQVersion;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;


public class CassandraDiskAnn
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraDiskAnn.class.getName());

    public static final int PQ_MAGIC = 0xB011A61C; // PQ_MAGIC, with a lot of liberties taken
    protected final PerIndexFiles indexFiles;
    private final ColumnQueryMetrics.VectorIndexMetrics columnQueryMetrics;
    protected final SegmentMetadata.ComponentMetadataMap componentMetadatas;

    private final SSTableId<?> source;
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

    public CassandraDiskAnn(SSTableContext sstableContext, SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context, OrdinalsMapFactory omFactory) throws IOException
    {
        this.source = sstableContext.sstable().getId();
        this.componentMetadatas = componentMetadatas;
        this.indexFiles = indexFiles;
        this.columnQueryMetrics = (ColumnQueryMetrics.VectorIndexMetrics) context.getColumnQueryMetrics();

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = this.componentMetadatas.get(IndexComponentType.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        var rawGraph = OnDiskGraphIndex.load(graphHandle::createReader, termsMetadata.offset);
        features = rawGraph.getFeatureSet();
        graph = rawGraph;

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
                    pq = ((PQVectors) compressedVectors).getCompressor();
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
        ordinalsMap = omFactory.create(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
        if (ordinalsMap.getStructure() == Structure.ZERO_OR_ONE_TO_MANY)
            logger.warn("Index {} has structure ZERO_OR_ONE_TO_MANY, which requires on reading the on disk row id" +
                        " to ordinal mapping for each search. This will be slower.", source);

        searchers = ExplicitThreadLocal.withInitial(() -> new GraphSearcherAccessManager(new GraphSearcher(graph)));

        // Record metrics for this graph
        columnQueryMetrics.onGraphLoaded(compressedVectors == null ? 0 : compressedVectors.ramBytesUsed(),
                                         ordinalsMap.cachedBytesUsed(),
                                         graph.size(0));
    }

    public Structure getPostingsStructure()
    {
        return ordinalsMap.getStructure();
    }

    @FunctionalInterface
    public interface OrdinalsMapFactory {
        OnDiskOrdinalsMap create(FileHandle handle, long offset, long length);
    }

    public ProductQuantization getPQ()
    {
        assert compression.type == VectorCompression.CompressionType.PRODUCT_QUANTIZATION;
        assert pq != null;
        return pq;
    }

    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }

    public int size()
    {
        // The base layer of the graph has all nodes.
        return graph.size(0);
    }

    /**
     * @param queryVector the query vector
     * @param limit the number of results to look for in the index (>= limit)
     * @param rerankK the number of quantized results to look for in the index (>= limit or <= 0). If rerankK is
     *                non-positive, then we will use limit as the value and will skip reranking. Rerankless search
     *                only applies when the graph has compressed vectors.
     * @param threshold the minimum similarity score to accept
     * @param usePruning whether to use pruning to speed up the search
     * @param acceptBits a Bits indicating which row IDs are acceptable, or null if no constraints
     * @param context unused (vestige from HNSW, retained in signature to allow calling both easily)
     * @param nodesVisitedConsumer a consumer that will be called with the number of nodes visited during the search
     * @return Iterator of Row IDs associated with the vectors near the query. If a threshold is specified, only vectors
     * with a similarity score >= threshold will be returned.
     */
    public CloseableIterator<RowIdWithScore> search(VectorFloat<?> queryVector,
                                                    int limit,
                                                    int rerankK,
                                                    float threshold,
                                                    boolean usePruning,
                                                    Bits acceptBits,
                                                    QueryContext context,
                                                    IntConsumer nodesVisitedConsumer)
    {
        VectorValidation.validateIndexable(queryVector, similarityFunction);
        boolean isRerankless = rerankK <= 0;
        if (isRerankless)
            rerankK = limit;

        var graphAccessManager = searchers.get();
        var searcher = graphAccessManager.get();
        // This searcher is reused across searches. We set here every time to ensure it is configured correctly
        // for this search. Note that resume search in AutoResumingNodeScoreIterator will continue to use this setting.
        searcher.usePruning(usePruning);
        try
        {
            var view = (GraphIndex.ScoringView) searcher.getView();
            SearchScoreProvider ssp;
            // FusedADC can no longer be written due to jvector upgrade. However, it's possible these index files
            // still exist, so we have to support them.
            if (features.contains(FeatureId.FUSED_ADC))
            {
                var asf = view.approximateScoreFunctionFor(queryVector, similarityFunction);
                var rr = isRerankless ? null : view.rerankerFor(queryVector, similarityFunction);
                ssp = new SearchScoreProvider(asf, rr);
            }
            else if (compressedVectors == null)
            {
                // no compression, so we ignore isRerankless (except for setting rerankK to limit)
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
                var rr = isRerankless ? null : view.rerankerFor(queryVector, similarityFunction);
                ssp = new SearchScoreProvider(asf, rr);
            }
            long start = nanoTime();
            var result = searcher.search(ssp, limit, rerankK, threshold, context.getAnnRerankFloor(), ordinalsMap.ignoringDeleted(acceptBits));
            long elapsed = nanoTime() - start;
            if (V3OnDiskFormat.ENABLE_RERANK_FLOOR)
                context.updateAnnRerankFloor(result.getWorstApproximateScoreInTopK());
            Tracing.trace("DiskANN search for {}/{} rerankless={}, usePruning={} visited {} nodes, reranked {} to return {} results from {}",
                          limit, rerankK, isRerankless, usePruning, result.getVisitedCount(), result.getRerankedCount(), result.getNodes().length, source);
            columnQueryMetrics.onSearchResult(result, elapsed, false);
            context.addAnnGraphSearchLatency(elapsed);
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
                var nodeScores = new AutoResumingNodeScoreIterator(searcher, graphAccessManager, result, context, columnQueryMetrics, nodesVisitedConsumer, limit, rerankK, false, source.toString());
                return new NodeScoreToRowIdWithScoreIterator(nodeScores, ordinalsMap.getRowIdsView());
            }
        }
        catch (Throwable t)
        {
            // If we don't release it, we'll never be able to aquire it, so catch and rethrow Throwable.
            graphAccessManager.forceRelease();
            throw t;
        }
    }

    public VectorCompression getCompression()
    {
        return compression;
    }

    public CompressedVectors getCompressedVectors()
    {
        return compressedVectors;
    }

    public void close() throws IOException
    {
        FileUtils.close(ordinalsMap, searchers, graph, graphHandle);
        columnQueryMetrics.onGraphClosed(compressedVectors == null ? 0 : compressedVectors.ramBytesUsed(),
                                         ordinalsMap.cachedBytesUsed(),
                                         graph.size(0));
    }

    public OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }

    public GraphIndex.ScoringView getView()
    {
        return (GraphIndex.ScoringView) graph.getView();
    }

    public boolean containsUnitVectors()
    {
        return pqUnitVectors;
    }

    public int maxDegree()
    {
        return graph.maxDegree();
    }
}
