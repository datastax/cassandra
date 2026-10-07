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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.disk.CompactionDestination;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.util.work.ProgressLimiter;
import io.github.jbellis.jvector.util.work.ProgressTracker;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.RemappedPostings;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.metrics.VectorCompactionMetrics;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static java.util.stream.Collectors.toList;

/**
 * Merges multiple existing on-disk HNSW graph segments into a single compacted segment using
 * jvector's {@link OnDiskGraphIndexCompactor}. This is the graph-index side of Cassandra's LSM
 * compaction: instead of rebuilding the graph from individual vectors, we merge existing on-disk
 * graph indexes directly, which produces higher-quality results without a full rebuild.
 *
 * <p>Each source is represented by a {@link SourceSegment} containing the on-disk graph and its
 * associated {@link CassandraDiskAnn} (for PQ, unit-vectors flag, and the ordinals map).
 *
 * <p>Node identity — dead-node detection AND postings — is the MANAGED GLOBAL ORDINAL
 * ({@code ordinalBase[source] + localOrdinal}), never the full-precision vector value: the
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
     * The merger's own post-compact work stage, reported in BYTES (vs jvector's ordinal-unit
     * stages): the footer CRC re-read over the whole terms body. Routed by {@link CompactionProgressLimiter} to
     * {@link VectorMergeOperation#reportBytes} so the epilogue shows byte progress instead of
     * sitting at ratio 1.0 with no unit flow.
     */
    public enum EpilogueStage implements io.github.jbellis.jvector.util.work.WorkStage
    {
        EPILOGUE
    }

    /**
     * One input segment for the merge. The {@link CassandraDiskAnn} provides the on-disk graph
     * (via {@link CassandraDiskAnn#getOnDiskGraph()}), PQ, and other metadata.
     */
    public static final class SourceSegment
    {
        private final CassandraDiskAnn diskAnn;
        private final long segmentRowIdOffset;
        // The SAI index owning this segment's on-disk graph, referenced for the merge's full duration so a
        // concurrent DROP / index teardown cannot unmap the source files out from under the compactor's
        // reads. Null only on the test path, where the caller manages source lifetime directly.
        private final SSTableIndex sstableIndex;

        public SourceSegment(CassandraDiskAnn diskAnn, long segmentRowIdOffset)
        {
            this(diskAnn, segmentRowIdOffset, null);
        }

        public SourceSegment(CassandraDiskAnn diskAnn, long segmentRowIdOffset, SSTableIndex sstableIndex)
        {
            this.diskAnn = diskAnn;
            this.segmentRowIdOffset = segmentRowIdOffset;
            this.sstableIndex = sstableIndex;
        }

        public CassandraDiskAnn diskAnn()
        {
            return diskAnn;
        }

        public long segmentRowIdOffset()
        {
            return segmentRowIdOffset;
        }

        public OnDiskGraphIndex graph()
        {
            return diskAnn.getOnDiskGraph();
        }

        public SSTableIndex sstableIndex()
        {
            return sstableIndex;
        }
    }

    private final List<SourceSegment> sources;
    private final VectorSimilarityFunction similarityFunction;
    private final IndexComponents.ForWrite perIndexComponents;

    public CompactionGraphMerger(List<SourceSegment> sources,
                                 VectorSimilarityFunction similarityFunction,
                                 IndexComponents.ForWrite perIndexComponents)
    {
        if (sources.size() < 2)
            throw new IllegalArgumentException("At least 2 source segments required for graph merging; got " + sources.size());
        this.sources = sources;
        this.similarityFunction = similarityFunction;
        this.perIndexComponents = perIndexComponents;
    }

    /**
     * Merges the source graphs and writes all SAI index components for the output segment.
     *
     * <p>Dead-node detection and postings identity are ORDINAL-KEYED (see
     * doc/vector_merge_ordinal_identity.md): the caller ingested output rows against the
     * global ordinal handle {@code ordinalBase[s] + localOrdinal} of the source node whose
     * cell won each row's merge, building {@code surviving} incrementally. No source vector
     * is read here outside jvector's own compactor.
     *
     * @param postingsMap     global ordinal handle → output postings, built during the
     *                        row-by-row compaction pass
     * @param maxSegmentRowId highest output segment rowid ingested
     * @param progressLimiter host control surface installed on the jvector compactor; pass
     *                        {@link ProgressLimiter#UNLIMITED} for none
     * @param ordinalBase     per-source global ordinal bases (aligned with the constructor's
     *                        source list)
     * @param surviving       per-source surviving-ordinal bitsets (same alignment)
     */
    public SegmentMetadata.ComponentMetadataMap merge(
            ChronicleMap<Long, CompactionVectorPostings> postingsMap,
            int maxSegmentRowId,
            ProgressLimiter progressLimiter,
            long[] ordinalBase,
            java.util.BitSet[] surviving) throws IOException
    {
        // --- Validate source feature sets ---
        boolean hasFusedPQ = validateFeatureSets(sources.stream().map(SourceSegment::graph).collect(toList()));

        // --- Step 1: ordinal remapping from the surviving bitsets — pure CPU, no vector I/O.
        // Output ordinals are packed source-major in ascending node order, matching the
        // ingest-time handle space. The pre-scan this replaces read every source node's
        // full-precision vector; it no longer exists, so the ProgressLimiter is live from the
        // compactor's first phase.
        var liveNodes = new ArrayList<FixedBitSet>(sources.size());
        var remappers = new ArrayList<OrdinalMapper>(sources.size());

        // Parallel arrays: output global ordinal g → source index and local node ID.
        var globalSrcIdxList = new ArrayList<Integer>();
        var globalNodeIdList = new ArrayList<Integer>();
        int totalGlobalOrdinals;
        int[] globalToSrcIdx;
        int[] globalToNodeId;

        try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                VectorCompactionMetrics.Phase.ORDINAL_REMAP, perIndexComponents.context()))
        {
            for (int s = 0; s < sources.size(); s++)
            {
                int idBound = sources.get(s).graph().getIdUpperBound();
                var bs = new FixedBitSet(idBound);
                var oldToNew = new HashMap<Integer, Integer>();
                for (int nid = surviving[s].nextSetBit(0); nid >= 0; nid = surviving[s].nextSetBit(nid + 1))
                {
                    bs.set(nid);
                    oldToNew.put(nid, globalSrcIdxList.size());
                    globalSrcIdxList.add(s);
                    globalNodeIdList.add(nid);
                }
                liveNodes.add(bs);
                remappers.add(new OrdinalMapper.MapMapper(oldToNew));
            }

            totalGlobalOrdinals = globalSrcIdxList.size();
            if (totalGlobalOrdinals == 0)
                throw new IllegalStateException("CompactionGraphMerger: no surviving nodes found across all source segments; all rows may have been deleted");

            globalToSrcIdx = globalSrcIdxList.stream().mapToInt(Integer::intValue).toArray();
            globalToNodeId = globalNodeIdList.stream().mapToInt(Integer::intValue).toArray();
        }

        // --- Step 2: jvector writes the compacted graph body directly into the SAI TERMS_DATA
        // component, after a reserved SAI header; the footer CRC is then computed over the in-place
        // header+body. No temp file, one write of the (potentially large) graph.
        var termsComponent = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA);
        Path termsFile = termsComponent.file().toJavaIOFile().toPath();

        var compactor = new OnDiskGraphIndexCompactor(
                sources.stream().map(SourceSegment::graph).collect(toList()),
                liveNodes,
                remappers,
                similarityFunction,
                // All jvector fan-out work runs on the shared Cassandra-managed build pool,
                // keeping vector work off ForkJoinPool.commonPool() and inside the managed budget.
                JVectorVersionUtil.compactionBuildPool());
        // Install the host control surface: forwards jvector's per-phase progress to the merge
        // operation and admits its write bandwidth against the shared compaction throughput budget.
        compactor.setProgressLimiter(progressLimiter);
        // EXPERIMENTAL retain-largest merge, REQUIRED-EXPLICIT like every other
        // cassandra.sai.vector.* switch. Asking for it does not force it: jvector
        // measures the retained source's share of surviving nodes per merge and
        // falls back to the symmetric path when no source dominates, because below
        // that point the shortcut costs recall instead of time.
        //
        // Resolved BY CAPABILITY, not at compile time: the control exists only when
        // the deployed jvector is the `jvector-experiments` branch. The
        // `integration-infrastructure` jar (authentic upstream compaction + our
        // embedding/measurement surface) has no such method, and both jars install
        // under the same artifact coordinates — opting in or out of the experiment
        // is a jar swap, and this ONE fork build must run against either. Absent
        // control + flag=true is announced per merge rather than failed: the flag
        // states intent, the jar decides capability.
        boolean retainLargest = VectorFeatureFlags.compactionRetainLargest();
        try
        {
            compactor.getClass().getMethod("setRetainLargest", boolean.class)
                     .invoke(compactor, retainLargest);
        }
        catch (NoSuchMethodException e)
        {
            logger.info("deployed jvector has no retain-largest control (infrastructure/upstream jar); " +
                        "compaction_retain_largest={} is inert for this merge", retainLargest);
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException("failed to apply the retain-largest control", e);
        }

        // Upstream jvector exposes no codebook-policy, code-cache, decoder-sharing or canonical-code
        // controls: the merge always retrains the codebook and re-encodes. Nothing to configure here.

        // Reserve the SAI header, then have jvector write its body directly after it (compact
        // preserves [0, startOffset)); finally wrap with a footer whose CRC covers header+body.
        try (var termsOutput = termsComponent.openOutput(true))
        {
            SAICodecUtils.writeHeader(termsOutput);
        }
        long termsOffset = SAICodecUtils.headerSize();

        // The graph body is written straight into TERMS_DATA after the SAI header above: the
        // destination reserves the region [termsOffset, EOF) and jvector writes into it, so there
        // is no temp file and no second copy of a graph that can run to many GB. commit() reports
        // the body length jvector actually wrote and durably flushed; a merge that fails never
        // commits, and the half-written component is discarded by SAI's own component lifecycle
        // rather than unlinked here (the file is Cassandra's, not jvector's, to delete).
        var committedBodyLength = new java.util.concurrent.atomic.AtomicLong(-1);
        CompactionDestination destination = () -> new CompactionDestination.OutputReservation()
        {
            @Override
            public Path file()
            {
                return termsFile;
            }

            @Override
            public long startOffset()
            {
                return termsOffset;
            }

            @Override
            public void commit(long bodyLength)
            {
                committedBodyLength.set(bodyLength);
            }

            @Override
            public void close()
            {
            }
        };

        long compactStart = System.nanoTime();
        try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                VectorCompactionMetrics.Phase.JVECTOR_COMPACT, perIndexComponents.context()))
        {
            compactor.compact(destination);
        }
        long compactMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - compactStart);
        long termsLength = committedBodyLength.get();
        if (termsLength < 0)
            throw new IllegalStateException("CompactionGraphMerger: jvector returned without committing the " +
                                            "reserved TERMS_DATA region; refusing to publish an unattested body");

        // --- Step 2b: adopt the ordinal mapping jvector ACTUALLY used.
        //
        // The remappers passed to the constructor are a PROPOSAL. When the compactor assigns
        // ordinals itself -- setSimilarityOrdinals(true), i.e. numbering nodes in vector
        // similarity order -- it "ignores the ordinal values of the caller-supplied remappers
        // (their source/oldOrdinal structure is still used to enumerate nodes)". The mapping on
        // disk is then whatever effectiveRemappers() reports.
        //
        // globalToSrcIdx/globalToNodeId are indexed BY MERGED ORDINAL and are what the postings
        // writer below resolves through, so they must be rebuilt from the effective mapping or
        // every row binds to the wrong vector. That failure is silent: the "could not be joined
        // to a source graph ordinal" guard in SegmentBuilder catches rows with NO source ordinal,
        // not rows mapped to the WRONG one, so the only symptom would be bad recall.
        //
        // When the compactor keeps the caller's ordinals, effectiveRemappers() returns exactly
        // those, and this rebuild reproduces the arrays byte-for-byte -- so it is unconditional
        // rather than gated on a flag that would then have to be kept in sync.
        final int[] effGlobalToSrcIdx;
        final int[] effGlobalToNodeId;
        {
            List<OrdinalMapper> effective = compactor.effectiveRemappers();
            if (effective == null)
                effective = remappers;
            if (effective.size() != sources.size())
                throw new IllegalStateException("CompactionGraphMerger: effectiveRemappers() returned " +
                                                effective.size() + " mappers for " + sources.size() + " sources");

            int[] newSrcIdx = new int[totalGlobalOrdinals];
            int[] newNodeId = new int[totalGlobalOrdinals];
            // Sentinel so an unfilled slot is detectable; -1 is not a valid source index.
            Arrays.fill(newSrcIdx, -1);

            for (int s = 0; s < sources.size(); s++)
            {
                OrdinalMapper mapper = effective.get(s);
                for (int nid = surviving[s].nextSetBit(0); nid >= 0; nid = surviving[s].nextSetBit(nid + 1))
                {
                    int merged = mapper.oldToNew(nid);
                    if (merged < 0 || merged >= totalGlobalOrdinals)
                        throw new IllegalStateException(String.format(
                                "CompactionGraphMerger: effective mapping put source %d node %d at merged ordinal %d, " +
                                "outside [0,%d)", s, nid, merged, totalGlobalOrdinals));
                    if (newSrcIdx[merged] != -1)
                        throw new IllegalStateException(String.format(
                                "CompactionGraphMerger: merged ordinal %d claimed twice (source %d node %d and source %d node %d)",
                                merged, newSrcIdx[merged], newNodeId[merged], s, nid));
                    newSrcIdx[merged] = s;
                    newNodeId[merged] = nid;
                }
            }
            // A gap means some merged ordinal has no row behind it, which would publish an index
            // with unattributed vectors. Fail the compaction rather than the query.
            for (int g = 0; g < totalGlobalOrdinals; g++)
            {
                if (newSrcIdx[g] == -1)
                    throw new IllegalStateException("CompactionGraphMerger: merged ordinal " + g +
                                                    " of " + totalGlobalOrdinals + " was never assigned by the " +
                                                    "effective mapping; refusing to publish");
            }

            boolean reordered = !Arrays.equals(newSrcIdx, globalToSrcIdx) || !Arrays.equals(newNodeId, globalToNodeId);
            effGlobalToSrcIdx = newSrcIdx;
            effGlobalToNodeId = newNodeId;
            logger.info("Vector graph merge: adopted effective ordinal mapping for {} ordinals ({})",
                        totalGlobalOrdinals, reordered ? "REORDERED by the compactor" : "unchanged from caller proposal");
        }

        // When FUSED_PQ is in use, the compactor retrains the PQ codebook and embeds it in the
        // output graph. Read it back (from the in-place body offset) so the Cassandra PQ component
        // holds the same codebook used for in-graph compressed neighbor scoring.
        ProductQuantization retrainedPQ = null;
        if (hasFusedPQ)
        {
            try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                         VectorCompactionMetrics.Phase.LOAD_RETRAINED_PQ, perIndexComponents.context());
                 var rs = ReaderSupplierFactory.open(termsFile))
            {
                var compactedGraph = OnDiskGraphIndex.load(rs, termsOffset);
                retrainedPQ = ((FusedPQ) compactedGraph.getFeatures().get(FeatureId.FUSED_PQ)).getPQ();
            }
        }

        // Epilogue byte progress: the CRC re-read spans the whole terms file (header+body+8) and
        // the codes tail is a known size; report both cumulatively in BYTES so the post-merge
        // window advances instead of sitting at the saturated ordinal ratio.
        long crcSpan = termsOffset + termsLength + 8;
        long epilogueTotal = crcSpan;
        // One phase for the whole epilogue, opened here and closed when merge() returns. jvector's
        // PhaseScope contract is one scope per phase with non-decreasing counters inside it, so the
        // footer CRC and the postings/PQ write report into the same scope rather than opening the
        // stage twice and restarting its count.
        try (ProgressTracker.PhaseScope epilogue = progressLimiter.startPhase(EpilogueStage.EPILOGUE))
        {
            try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                    VectorCompactionMetrics.Phase.FOOTER_CRC, perIndexComponents.context()))
            {
                epilogue.onProgress(0, epilogueTotal);
                SAICodecUtils.writeFooterForExternalBody(termsFile, termsOffset + termsLength,
                                                         bytes -> epilogue.onProgress(bytes, epilogueTotal));
                epilogue.onProgress(crcSpan, epilogueTotal);
            }
            logger.info("CompactionGraphMerger: TERMS_DATA written in place ({} source segments, {} surviving ordinals, {} bytes body at offset {}) in {}ms",
                        sources.size(), totalGlobalOrdinals, termsLength, termsOffset, compactMs);

            perIndexComponents.context().getIndexMetrics().ifPresent(m -> {
                m.vectorMergeCount.inc();
                m.vectorMergeMillis.update(compactMs);
                m.vectorMergeBytesWritten.update(termsLength);
                m.vectorMergeSurvivingOrdinals.update(totalGlobalOrdinals);
            });
            logger.debug("CompactionGraphMerger measurement: vectorMergeCount+=1, vectorMergeMillis={}, vectorMergeBytesWritten={}, vectorMergeSurvivingOrdinals={}",
                         compactMs, termsLength, totalGlobalOrdinals);

            // --- Step 3: Write postings and PQ ---
            try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                         VectorCompactionMetrics.Phase.POSTINGS_PQ_WRITE, perIndexComponents.context());
                 var postingsOutput = perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true);
                 var pqOutput = perIndexComponents.addOrGet(IndexComponentType.PQ).openOutput(true))
            {
                SAICodecUtils.writeHeader(postingsOutput);
                SAICodecUtils.writeHeader(pqOutput);

                var firstDiskAnn = sources.get(0).diskAnn();
                long pqOffset = pqOutput.getFilePointer();
                var version = perIndexComponents.context().version();

                if (hasFusedPQ)
                {
                    CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(),
                                                       firstDiskAnn.isPqUnitVectors(),
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

                // Write postings using ZERO_OR_ONE_TO_MANY. Each output global ordinal resolves its
                // postings by GLOBAL ORDINAL HANDLE — an 8-byte map lookup, no vector reads. Every
                // ordinal here has postings (only attributed ordinals entered the surviving bitsets).
                long postingsOffset = postingsOutput.getFilePointer();
                var ordinalMapper = new OrdinalMapper.IdentityMapper(totalGlobalOrdinals - 1);
                var rp = new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                              totalGlobalOrdinals - 1,
                                              maxSegmentRowId,
                                              null, null,
                                              ordinalMapper);

                java.util.function.IntFunction<CompactionVectorPostings> postingsByNewOrdinal =
                        g -> postingsMap.get(ordinalBase[effGlobalToSrcIdx[g]] + (long) effGlobalToNodeId[g]);
                if (V5OnDiskFormat.writeV5VectorPostings(version))
                {
                    new V5VectorPostingsWriter<Integer>(rp)
                            .writePostings(postingsOutput.asSequentialWriter(), postingsByNewOrdinal);
                }
                // else: V2 format doesn't support ZERO_OR_ONE_TO_MANY — fall back is handled at
                // the builder-selection level (merge path is only used for V5+)
                long postingsLength = postingsOutput.getFilePointer() - postingsOffset;

                SAICodecUtils.writeFooter(pqOutput);
                SAICodecUtils.writeFooter(postingsOutput);
                epilogue.onProgress(epilogueTotal, epilogueTotal);

                return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength,
                                                              postingsOffset, postingsLength,
                                                              pqOffset, pqLength);
            }
        }
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
        boolean hasFusedPQ = graphs.get(0).getFeatureSet().contains(FeatureId.FUSED_PQ);
        for (var graph : graphs)
        {
            var features = graph.getFeatureSet();
            if (!features.contains(FeatureId.INLINE_VECTORS))
                throw new IllegalStateException("CompactionGraphMerger requires INLINE_VECTORS; got " + features);
            if (features.contains(FeatureId.FUSED_PQ) != hasFusedPQ)
                throw new IllegalStateException("CompactionGraphMerger requires consistent FUSED_PQ presence across all sources");
        }
        return hasFusedPQ;
    }

    /**
     * Builds a {@link FixedBitSet} marking all jvector-live nodes in {@code graph} (i.e. all
     * non-tombstoned slots), without consulting the postings map.
     *
     * <p>When the on-disk graph has no tombstoned slots ({@code idUpperBound == size(0)}), all
     * node IDs fall in [0, liveCount) and a simple bulk-set suffices.  When tombstoned slots
     * exist ({@code idUpperBound > size(0)}), some stored node IDs equal or exceed
     * {@code size(0)}, so the bitset must be sized to {@code idUpperBound} and populated by
     * iterating the actual live-node IDs returned by {@link OnDiskGraphIndex#getNodes(int)}.
     *
     * <p>Used by tests to verify the tombstone-gap fix in isolation. Production code inlines
     * equivalent logic inside {@link #merge} with the additional postings-map check.
     */
    static FixedBitSet buildLiveBitset(OnDiskGraphIndex graph)
    {
        int liveCount = graph.size(0);
        int idBound = graph.getIdUpperBound();
        var bs = new FixedBitSet(idBound);
        if (idBound == liveCount)
        {
            bs.set(0, liveCount);
        }
        else
        {
            var nodeIt = graph.getNodes(0);
            while (nodeIt.hasNext()) bs.set(nodeIt.nextInt());
        }
        return bs;
    }

}
