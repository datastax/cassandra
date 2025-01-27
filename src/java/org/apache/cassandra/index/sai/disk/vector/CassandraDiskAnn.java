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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NodesIterator;
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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.PQVersion;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

import static java.lang.Math.min;

public class CassandraDiskAnn
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraDiskAnn.class.getName());

    public static final int PQ_MAGIC = 0xB011A61C; // PQ_MAGIC, with a lot of liberties taken
    protected final PerIndexFiles indexFiles;
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

    private final static DistributionSummary annRerankFloor = Metrics.summary("sai_ann_rerank_floor");
    private final static DistributionSummary annRerankK = Metrics.summary("sai_ann_rerank_k");
    // Note this tag is coordinated with the resume search logic
    private final static Timer annInitialSearch = Timer.builder("sai_ann_search").tag("phase", "initial").publishPercentileHistogram().register(Metrics.globalRegistry);

    public CassandraDiskAnn(SSTableContext sstableContext, SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context, OrdinalsMapFactory omFactory) throws IOException
    {
        this.source = sstableContext.sstable().getId();
        this.componentMetadatas = componentMetadatas;
        this.indexFiles = indexFiles;

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = this.componentMetadatas.get(IndexComponentType.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        var rawGraph = OnDiskGraphIndex.load(() -> new TimedRandomAccessReader(graphHandle.createReader()), termsMetadata.offset);
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

        final GraphIndex wrappedGraphIndex = new TimedGraphIndex(graph);
        searchers = ExplicitThreadLocal.withInitial(() -> new GraphSearcherAccessManager(new GraphSearcher(wrappedGraphIndex)));
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

    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }

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
        try
        {
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
                var rr = view.rerankerFor(queryVector, similarityFunction);
                ssp = new SearchScoreProvider(asf, rr);
            }

            var start = System.nanoTime();
            var result = searcher.search(ssp, limit, rerankK, threshold, context.getAnnRerankFloor(), ordinalsMap.ignoringDeleted(acceptBits));
            var duration = System.nanoTime() - start;
            annInitialSearch.record(duration, TimeUnit.NANOSECONDS);

            if (V3OnDiskFormat.ENABLE_RERANK_FLOOR)
                context.updateAnnRerankFloor(result.getWorstApproximateScoreInTopK());

            // Record temporary metrics.
            annRerankFloor.record(context.getAnnRerankFloor());
            annRerankK.record(rerankK);
            context.addInitialAnnSearchDuration(duration);

            Tracing.trace("DiskANN search for {}/{} visited {} nodes, reranked {} to return {} results from {}",
                          limit, rerankK, result.getVisitedCount(), result.getRerankedCount(), result.getNodes().length, source);
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
                var nodeScores = new AutoResumingNodeScoreIterator(searcher, graphAccessManager, result, context, nodesVisitedConsumer, limit, rerankK, false, source.toString());
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

    private static class TimedRandomAccessReader implements io.github.jbellis.jvector.disk.RandomAccessReader
    {
        private final RandomAccessReader randomAccessReader;
        private static final Timer readFoatsTimer = Timer.builder("sai_ann_disk_read")
                                                         .tag("method", "readFloats")
                                                         .publishPercentileHistogram()
                                                         .register(Metrics.globalRegistry);

        TimedRandomAccessReader(RandomAccessReader randomAccessReader)
        {
            this.randomAccessReader = randomAccessReader;
        }

        @Override
        public void seek(long l) throws IOException
        {
            randomAccessReader.seek(l);
        }

        @Override
        public long getPosition() throws IOException
        {
            return randomAccessReader.getPosition();
        }

        @Override
        public int readInt() throws IOException
        {
            return randomAccessReader.readInt();
        }

        @Override
        public float readFloat() throws IOException
        {
            return randomAccessReader.readFloat();
        }

        @Override
        public void readFully(byte[] bytes) throws IOException
        {
            randomAccessReader.readFully(bytes);
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException
        {
            randomAccessReader.readFully(byteBuffer);
        }

        @Override
        public void readFully(float[] floats) throws IOException
        {
            randomAccessReader.readFully(floats);
        }

        @Override
        public void readFully(long[] longs) throws IOException
        {
            randomAccessReader.readFully(longs);
        }

        @Override
        public void read(int[] ints, int i, int i1) throws IOException
        {
            // Skipping insturmentation for this method as it is only used by getNeighborsIterator which is already instrumented
            randomAccessReader.read(ints, i, i1);
        }

        @Override
        public void read(float[] floats, int i, int i1) throws IOException
        {
            long start = System.nanoTime();
            randomAccessReader.read(floats, i, i1);
            readFoatsTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }

        @Override
        public void close() throws IOException
        {
            randomAccessReader.close();
        }
    }

    private static class TimedGraphIndex implements GraphIndex
    {
        private final GraphIndex delegate;

        TimedGraphIndex(GraphIndex delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public int size()
        {
            return delegate.size();
        }

        @Override
        public NodesIterator getNodes()
        {
            return delegate.getNodes();
        }

        @Override
        public GraphIndex.View getView()
        {
            return new TimedView((ScoringView) delegate.getView());
        }

        @Override
        public int maxDegree()
        {
            return delegate.maxDegree();
        }

        @Override
        public int getIdUpperBound()
        {
            return delegate.getIdUpperBound();
        }

        @Override
        public boolean containsNode(int nodeId)
        {
            return delegate.containsNode(nodeId);
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
        }

        @Override
        public long ramBytesUsed()
        {
            return delegate.ramBytesUsed();
        }
    }

    private static class TimedView implements GraphIndex.ScoringView
    {
        private final GraphIndex.ScoringView view;

        private static final Timer getNeighborsTimer = Timer.builder("sai_ann_get_neighbors")
                                                            .publishPercentileHistogram()
                                                            .register(Metrics.globalRegistry);

        TimedView(GraphIndex.ScoringView view)
        {
            this.view = view;
        }

        @Override
        public NodesIterator getNeighborsIterator(int var1)
        {
            long start = System.nanoTime();
            NodesIterator result = view.getNeighborsIterator(var1);
            getNeighborsTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            return result;
        }

        @Override
        public int entryNode()
        {
            return view.entryNode();
        }

        @Override
        public int size()
        {
            return view.size();
        }

        public Bits liveNodes()
        {
            return view.liveNodes();
        }

        public int getIdUpperBound() {
            return view.getIdUpperBound();
        }

        public void close() throws IOException
        {
            view.close();
        }

        @Override
        public ScoreFunction.ExactScoreFunction rerankerFor(VectorFloat<?> vectorFloat, VectorSimilarityFunction vectorSimilarityFunction)
        {
            return view.rerankerFor(vectorFloat, vectorSimilarityFunction);
        }

        @Override
        public ScoreFunction.ApproximateScoreFunction approximateScoreFunctionFor(VectorFloat<?> vectorFloat, VectorSimilarityFunction vectorSimilarityFunction)
        {
            return view.approximateScoreFunctionFor(vectorFloat, vectorSimilarityFunction);
        }
    }
}
