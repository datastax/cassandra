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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.apache.lucene.store.OutputStreamDataOutput;

import org.agrona.collections.IntArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.disk.CompactionDestination;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.work.ProgressLimiter;
import io.github.jbellis.jvector.util.work.ProgressTracker;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.metrics.VectorCompactionMetrics;
import org.apache.cassandra.io.util.FileUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Merges multiple existing on-disk HNSW graph segments into a single compacted segment using
 * jvector's {@link OnDiskGraphIndexCompactor}. This is the graph-index side of Cassandra's LSM
 * compaction: instead of rebuilding the graph from individual vectors, we merge existing on-disk
 * graph indexes directly, which produces higher-quality results without a full rebuild.
 *
 * <p>Each source is represented by a {@link SourceSegment} containing the on-disk graph and its
 * associated {@link CassandraDiskAnn} (for PQ, unit-vectors flag, and the ordinals map).
 *
 * <p>Node identity — dead-node detection AND postings — is the source node's
 * {@linkplain VectorHandle (source index, local ordinal)} pair, never the
 * full-precision vector value: the
 * segment builder joins each output row to the source node whose cell won its merge (via the
 * compaction row-source tags + the source's primary-key and ordinals maps) and hands this class
 * the resulting surviving-ordinal bitsets and an ordinal-keyed postings map. No source vector is
 * read here outside jvector's own compactor. See doc/vector_merge_ordinal_identity.md.
 *
 * <p>Supported source feature sets:
 * <ul>
 *   <li>{@code INLINE_VECTORS} only: the PQ component is written with {@link CompressionType#NONE}.
 *   <li>{@code INLINE_VECTORS} + {@code FUSED_PQ}: after compaction, the retrained PQ codebook is
 *       read back from the compacted graph and written to the Cassandra PQ component so query
 *       encoding uses the correct codebook.
 * </ul>
 *
 * <p>Any other feature set (NVQ, separated vectors, non-fused PQ) causes {@link #merge} to throw
 * {@link IllegalStateException} so the caller can fall back to the legacy graph-rebuild path.
 */
public class CompactionGraphMerger
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraphMerger.class);

    /** Killswitch: set to false to fall back to the legacy graph-rebuild path without restarting. */
    public static volatile boolean ENABLED = CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getBoolean();

    /**
     * One input segment for the merge. The {@link CassandraDiskAnn} provides the on-disk graph
     * (via {@link CassandraDiskAnn#getOnDiskGraph()}), PQ, and other metadata.
     *
     * @param sstableIndex The SAI index owning this segment's on-disk graph, referenced for the merge's full duration
     *                     so a concurrent DROP / index teardown cannot unmap the source files out from under
     *                     the compactor's reads. Null only on the test path, where the caller manages source lifetime
     *                     directly.
     */
    public record SourceSegment(CassandraDiskAnn diskAnn, long segmentRowIdOffset, SSTableIndex sstableIndex)
    {
        public OnDiskGraphIndex graph()
        {
                return diskAnn.getOnDiskGraph();
        }
    }

    private final List<SourceSegment> sources;
    private final VectorMergePostings postings;
    private final VectorSimilarityFunction similarityFunction;
    private final IndexComponents.ForWrite perIndexComponents;
    private final IndexContext context;
    private final ProgressLimiter progressLimiter;
    private final boolean hasFusedPQ;

    public CompactionGraphMerger(List<SourceSegment> sources,
                                 VectorMergePostings postings,
                                 VectorSimilarityFunction similarityFunction,
                                 IndexComponents.ForWrite perIndexComponents,
                                 ProgressLimiter progressLimiter)
    {
        if (sources.size() < 2)
            throw new IllegalArgumentException("At least 2 source segments required for graph merging; got " + sources.size());
        if (sources.size() != postings.sourceCount())
            throw new IllegalArgumentException("sources.size() " + sources.size() + " != postings.sourceCount() " + postings.sourceCount());
        this.sources = sources;
        this.postings = postings;
        this.similarityFunction = similarityFunction;
        this.perIndexComponents = perIndexComponents;
        this.context = perIndexComponents.context();
        this.progressLimiter = progressLimiter;
        this.hasFusedPQ = validateFeatureSets(sources.stream().map(SourceSegment::graph).collect(toList()));
    }

    /**
     * Merges the source graphs and writes all SAI index components for the output segment.
     *
     * <p>Dead-node detection and postings identity are ORDINAL-KEYED (see
     * doc/vector_merge_ordinal_identity.md): the caller ingested output rows against the
     * {@link VectorHandle} of the source node whose cell won each row's merge. The surviving
     * source nodes and their postings are both held by {@code postings}, which keeps them consistent
     * by construction. No source vector is read here outside jvector's own compactor.
     */
    public SegmentMetadata.ComponentMetadataMap merge() throws IOException
    {
        List<MappedOrdinalMapper> remappers = List.of();
        FileChannel termsChannel = null;
        FileChannel pqChannel = null;
        try
        {
            // --- Step 1: ordinal remapping from the surviving bitsets — pure CPU, no vector I/O.
            // The postings own the ordering (ascending node id across all sources, which mirrors the
            // natural row order of the output SSTable). No source vector is read, so the ProgressLimiter
            // is live from the compactor's first phase.
            // All resources are declared up front and released in a single finally; closeQuietly is null-safe.
            remappers = createRemappers();
            int totalGlobalOrdinals = postings.size();

            // --- Step 2: jvector writes the compacted graph body directly into the SAI TERMS_DATA
            // component, after a reserved SAI header; the footer CRC is then computed over the in-place
            // header+body. No temp file, one write of the (potentially large) graph.
            Path termsFile = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA).file().toJavaIOFile().toPath();
            Path pqFile = perIndexComponents.addOrGet(IndexComponentType.PQ).file().toJavaIOFile().toPath();

            // Open both channels up front so they live for the full compaction + epilogue window.
            // Headers are written directly through the channel — no separate SequentialWriter open.
            termsChannel = FileChannel.open(termsFile,
                                            StandardOpenOption.READ,
                                            StandardOpenOption.WRITE,
                                            StandardOpenOption.CREATE);
            pqChannel = FileChannel.open(pqFile,
                                         StandardOpenOption.READ,
                                         StandardOpenOption.WRITE,
                                         StandardOpenOption.CREATE);

            // Write the SAI header for TERMS_DATA directly through the channel. The channel is left
            // positioned immediately after the header; that position becomes the compaction startOffset.
            Version version = context.version();
            OutputStream termsStream = Channels.newOutputStream(termsChannel);
            SAICodecUtils.writeHeader(new OutputStreamDataOutput(termsStream), version);
            termsStream.flush();

            // For the non-fused PQ sidecar, write SAI header + Cassandra PQ header directly through
            // pqChannel. The channel is left positioned immediately after both headers.
            long pqOffset = -1;
            long pqLength = -1;
            // Determine sidecar once here; compactTermsAndPq reads the same flag to decide whether to
            // pass a compressedDestination to jvector.
            List<CompressedVectors> sourceCompressed = hasFusedPQ ? null : sourceCompressedVectors();
            boolean pqSidecar = sourceCompressed != null;
            if (pqSidecar)
            {
                DataOutputStream pqStream = new DataOutputStream(Channels.newOutputStream(pqChannel));
                SAICodecUtils.writeHeader(new OutputStreamDataOutput(pqStream), version);
                pqStream.flush();
                pqOffset = pqChannel.position();
                CassandraOnHeapGraph.writePqHeader(pqStream,
                                                   sources.getFirst().diskAnn().isPqUnitVectors(),
                                                   CompressionType.PRODUCT_QUANTIZATION,
                                                   version);
                pqStream.flush();
            }
            long pqSidecarStartOffset = pqChannel.position();

            // The graph body is written straight into TERMS_DATA after the SAI header above.
            CompactionResult compactionResult = compactTermsAndPq(sourceCompressed, remappers, termsChannel, pqSidecar ? pqChannel : null);
            long compactMs = compactionResult.compactMs();
            long termsLength = compactionResult.termsLength();
            long termsOffset = compactionResult.termsOffset();
            List<? extends OrdinalMapper> effective = compactionResult.ordinalMapping();
            logger.info("CompactionGraphMerger: TERMS_DATA graph body written in place ({} source segments, {} surviving ordinals, {} bytes body at offset {}) in {} ms",
                        sources.size(), totalGlobalOrdinals, termsLength, termsOffset, compactMs);

            // When FUSED_PQ is in use, the compactor retrains the PQ codebook and embeds it in the
            // output graph. Read it back (from the in-place body offset) so the Cassandra PQ component
            // holds the same codebook used for in-graph compressed neighbor scoring.
            ProductQuantization retrainedPQ = loadRetrainedPQ(termsFile, termsOffset);

            // The footer CRC re-read spans the whole terms file (header+body+8). The limiter reports
            // it in BYTES to the merge operation, so the post-compact window advances instead of
            // sitting at the saturated ordinal ratio.
            long crcSpan = termsOffset + termsLength + 8;
            try (ProgressTracker.PhaseScope crc = progressLimiter.startPhase(VectorCompactionMetrics.Phase.FOOTER_CRC))
            {
                crc.onProgress(0, crcSpan);
                SAICodecUtils.writeFooterForExternalBody(termsFile, termsOffset + termsLength,
                                                         bytes -> crc.onProgress(bytes, crcSpan));
                crc.onProgress(crcSpan, crcSpan);
            }
            if (pqSidecar)
            {
                long pqBodyLength = pqChannel.size() - pqSidecarStartOffset;
                pqLength = pqSidecarStartOffset + pqBodyLength - pqOffset;
                SAICodecUtils.writeFooterForExternalBody(pqFile, pqSidecarStartOffset + pqBodyLength);
            }
            context.getIndexMetrics().ifPresent(m -> {
                m.vectorMergeCount.inc();
                m.vectorMergeMillis.update(compactMs);
                m.vectorMergeBytesWritten.update(termsLength);
                m.vectorMergeSurvivingOrdinals.update(totalGlobalOrdinals);
            });
            logger.debug("CompactionGraphMerger measurement: vectorMergeCount+=1, vectorMergeMillis={}, vectorMergeBytesWritten={}, vectorMergeSurvivingOrdinals={}",
                         compactMs, termsLength, totalGlobalOrdinals);

            // --- Step 3: Write postings and PQ ---
            // The non-fused PQ sidecar was already written in place by jvector.
            // For FUSED_PQ or INLINE_VECTORS-only, write the PQ component now.
            PostingsResult postingsResult;
            try (var _ = progressLimiter.startPhase(VectorCompactionMetrics.Phase.POSTINGS_PQ_WRITE))
            {
                if (!pqSidecar)
                {
                    PqResult pqResult = writePqComponent(retrainedPQ);
                    pqOffset = pqResult.pqOffset();
                    pqLength = pqResult.pqLength();
                }

                postingsResult = writePostings(effective, totalGlobalOrdinals);
            }

            return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength,
                                                          postingsResult.postingsOffset(), postingsResult.postingsLength(),
                                                          pqOffset, pqLength);
        }
        finally
        {
            FileUtils.closeQuietly(pqChannel);
            FileUtils.closeQuietly(termsChannel);
            FileUtils.closeQuietly(remappers);
        }
    }

    /**
     * Builds ordinal remappers from the surviving bitsets, tracking progress under the
     * {@link VectorCompactionMetrics.Phase#ORDINAL_REMAP} phase.
     */
    private List<MappedOrdinalMapper> createRemappers()
    {
        try (var _ = progressLimiter.startPhase(VectorCompactionMetrics.Phase.ORDINAL_REMAP))
        {
            List<MappedOrdinalMapper> remappers = postings.ordinalMapping();
            if (postings.isEmpty())
            {
                FileUtils.closeQuietly(remappers);
                throw new IllegalStateException("CompactionGraphMerger: no surviving nodes found across all source segments; all rows may have been deleted");
            }
            return remappers;
        }
    }

    private record CompactionResult(long termsOffset, long termsLength, List<? extends OrdinalMapper> ordinalMapping, long compactMs) {}

    /**
     * Builds and runs the jvector compactor, writing the graph body into a {@link CompactionDestination}
     * wrapping {@code termsChannel} (and optionally one wrapping {@code pqChannel} for the non-fused
     * PQ sidecar). Reads {@code termsOffset} directly from the channel position, which is already
     * positioned immediately after the SAI header written by the caller.
     *
     * @param pqChannel null when the PQ sidecar is not in use
     */
    private CompactionResult compactTermsAndPq(List<CompressedVectors> sourceCompressed,
                                               List<MappedOrdinalMapper> remappers,
                                               FileChannel termsChannel,
                                               FileChannel pqChannel) throws IOException
    {
        CompactionDestination termsDestination = CompactionDestination.channel(termsChannel);
        CompactionDestination pqDestination = pqChannel != null ? CompactionDestination.channel(pqChannel) : null;
        OnDiskGraphIndexCompactor compactor = new OnDiskGraphIndexCompactor(
                sources.stream().map(SourceSegment::graph).collect(toList()),
                sourceCompressed,
                postings.liveNodesBySource(),
                remappers,
                similarityFunction,
                // All jvector fan-out work runs on the shared Cassandra-managed build pool,
                // keeping vector work off ForkJoinPool.commonPool() and inside the managed budget.
                JVectorVersionUtil.compactionBuildPool(),
                JVectorVersionUtil.compactionBuildThreads());
        // Install the host control surface: forwards jvector's per-phase progress to the merge
        // operation and admits its write bandwidth against the shared compaction throughput budget.
        compactor.setProgressLimiter(progressLimiter);
        // Upstream jvector exposes no codebook-policy, code-cache, decoder-sharing or canonical-code
        // controls: the merge always retrains the codebook and re-encodes. Nothing to configure here.

        long termsOffset = termsChannel.position();
        long compactStart = nanoTime();
        try (var _ = progressLimiter.startPhase(VectorCompactionMetrics.Phase.JVECTOR_COMPACT))
        {
            if (pqDestination != null)
                compactor.compact(termsDestination, pqDestination);
            else
                compactor.compact(termsDestination);
        }
        long compactMs = TimeUnit.NANOSECONDS.toMillis(nanoTime() - compactStart);
        long termsLength = termsChannel.size() - termsOffset;

        // --- Step 2b: use the ordinal mapping jvector ACTUALLY used.
        //
        // The remappers passed to the constructor are a PROPOSAL. A compactor that assigns ordinals
        // itself (e.g. numbering nodes in vector similarity order) ignores their new-ordinal values,
        // and the mapping on disk is then whatever effectiveRemappers() reports. The postings below
        // are resolved through these effective mappers, so rows bind to the right vectors either way.
        // Binding to the wrong vector is silent -- the "could not be joined to a source graph
        // ordinal" guard in SegmentBuilder only catches rows with NO source ordinal.
        List<? extends OrdinalMapper> effective = compactor.effectiveRemappers();
        if (effective.size() != sources.size())
            throw new IllegalStateException("CompactionGraphMerger: effectiveRemappers() returned " +
                                            effective.size() + " mappers for " + sources.size() + " sources");

        return new CompactionResult(termsOffset, termsLength, effective, compactMs);
    }

    /**
     * When {@code hasFusedPQ} is true, reads the retrained {@link ProductQuantization} codebook back
     * from the compacted graph written at {@code termsOffset} in {@code termsFile}. Returns {@code null}
     * when {@code hasFusedPQ} is false.
     */
    private ProductQuantization loadRetrainedPQ(Path termsFile, long termsOffset) throws IOException
    {
        if (!hasFusedPQ)
            return null;
        try (var _ = progressLimiter.startPhase(VectorCompactionMetrics.Phase.LOAD_RETRAINED_PQ);
             ReaderSupplier rs = ReaderSupplierFactory.open(termsFile))
        {
            OnDiskGraphIndex compactedGraph = OnDiskGraphIndex.load(rs, termsOffset);
            return ((FusedPQ) compactedGraph.getFeatures().get(FeatureId.FUSED_PQ)).getPQ();
        }
    }

    private record PqResult(long pqOffset, long pqLength) {}

    /**
     * Writes the Cassandra PQ component for the non-sidecar paths (FUSED_PQ or INLINE_VECTORS-only).
     * For FUSED_PQ the retrained codebook is serialised; for INLINE_VECTORS-only a NONE header is written.
     */
    private PqResult writePqComponent(ProductQuantization retrainedPQ) throws IOException
    {
        Version version = context.version();
        IndexComponent.ForWrite pqComponent = perIndexComponents.addOrGet(IndexComponentType.PQ);
        try (IndexOutputWriter pqOutput = pqComponent.openOutput(true))
        {
            SAICodecUtils.writeHeader(pqOutput);
            long pqOffset = pqOutput.getFilePointer();
            if (hasFusedPQ)
            {
                CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(),
                                                   sources.getFirst().diskAnn().isPqUnitVectors(),
                                                   CompressionType.PRODUCT_QUANTIZATION,
                                                   version);
                retrainedPQ.write(pqOutput.asSequentialWriter(), version.onDiskFormat().jvectorFileFormatVersion());
            }
            else
            {
                CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(),
                                                   false,
                                                   CompressionType.NONE,
                                                   version);
            }
            long pqLength = pqOutput.getFilePointer() - pqOffset;
            SAICodecUtils.writeFooter(pqOutput);
            return new PqResult(pqOffset, pqLength);
        }
    }

    private record PostingsResult(long postingsOffset, long postingsLength) {}

    /**
     * Opens and writes the {@link IndexComponentType#POSTING_LISTS} component, including SAI header,
     * postings body, and footer.
     */
    private PostingsResult writePostings(List<? extends OrdinalMapper> effective,
                                         int totalGlobalOrdinals) throws IOException
    {
        Version version = context.version();
        try (IndexOutputWriter postingsOutput =
                     perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true))
        {
            SAICodecUtils.writeHeader(postingsOutput);
            long postingsOffset = postingsOutput.getFilePointer();
            // Write postings. Each merged ordinal resolves its postings through the key of the
            // source node the compactor placed there — an 8-byte map lookup, no vector reads.
            // Row ids are not assumed sorted: parallel ingest can add a duplicate vector's row
            // ids out of order.
            if (V5OnDiskFormat.writeV5VectorPostings(version))
            {
                IntFunction<IntArrayList> rowIdsByMergedOrdinal = postings.rowIdsByMergedOrdinal(effective);
                Structure structure = postings.discoverStructure(effective);
                V5VectorPostingsWriter.create(totalGlobalOrdinals - 1, rowIdsByMergedOrdinal)
                                      .structure(structure)
                                      .maxRowId(postings.maxRowId())
                                      .writeTo(postingsOutput.asSequentialWriter());
            }
            // else: V2 format doesn't support ZERO_OR_ONE_TO_MANY — fall back is handled at
            // the builder-selection level (merge path is only used for V5+)
            long postingsLength = postingsOutput.getFilePointer() - postingsOffset;
            SAICodecUtils.writeFooter(postingsOutput);
            return new PostingsResult(postingsOffset, postingsLength);
        }
    }

    /**
     * The sources' non-fused {@link CompressedVectors} (PQVectors), in source order, or null when the
     * merged PQ component should carry no vectors. Null if no source has any; also null (with a log
     * line) if only some do, since jvector cannot retrain a codebook from a mix.
     */
    private List<CompressedVectors> sourceCompressedVectors()
    {
        List<CompressedVectors> result = new ArrayList<>(sources.size());
        for (SourceSegment source : sources)
        {
            CompressedVectors cv = source.diskAnn().getCompressedVectors();
            if (cv != null)
                result.add(cv);
        }
        if (result.isEmpty())
            return null;
        if (result.size() != sources.size())
        {
            logger.info("Vector graph merge: only {} of {} sources carry PQ vectors; writing the merged segment without PQ",
                        result.size(), sources.size());
            return null;
        }
        return result;
    }

    /**
     * Validates that all source graphs have compatible feature sets and returns whether
     * {@code FUSED_PQ} is present. Extracted for testability — callers can pass
     * {@code OnDiskGraphIndex} instances directly without Cassandra infrastructure.
     *
     * @throws IllegalStateException if any graph lacks {@code INLINE_VECTORS}, or if
     *                               {@code FUSED_PQ} presence differs across sources
     */
    static boolean validateFeatureSets(List<OnDiskGraphIndex> graphs)
    {
        boolean hasFusedPQ = graphs.getFirst().getFeatureSet().contains(FeatureId.FUSED_PQ);
        for (OnDiskGraphIndex graph : graphs)
        {
            Set<FeatureId> features = graph.getFeatureSet();
            if (!features.contains(FeatureId.INLINE_VECTORS))
                throw new IllegalStateException("CompactionGraphMerger requires INLINE_VECTORS; got " + features);
            if (features.contains(FeatureId.FUSED_PQ) != hasFusedPQ)
                throw new IllegalStateException("CompactionGraphMerger requires consistent FUSED_PQ presence across all sources");
        }
        return hasFusedPQ;
    }
}
