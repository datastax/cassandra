/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;

import static org.junit.Assert.*;

/**
 * Verifies how {@link IndexMetrics#diskUsedBytes} changes when upgrading a vector index
 * from format EC to FA.
 *
 * <h2>Background: what is a vector index made of?</h2>
 *
 * <p>A SAI vector index is stored as five on-disk component files per SSTable:
 * <ul>
 *   <li><b>TERMS_DATA</b> — the graph: nodes, edges, and one inline data block per node.</li>
 *   <li><b>PQ</b> — Product Quantization data: a codebook plus one small code per indexed vector,
 *       used to speed up approximate distance computations during search.</li>
 *   <li><b>META</b> — segment metadata (offsets, lengths, statistics).</li>
 *   <li><b>POSTING_LISTS</b> — mapping from row IDs to graph node ordinals.</li>
 *   <li><b>COLUMN_COMPLETION_MARKER</b> — a fixed-size sentinel indicating the index is complete.</li>
 * </ul>
 *
 * <h2>What changes EC → FA: FusedPQ</h2>
 *
 * <p>FA introduces <em>FusedPQ</em>: instead of keeping PQ codes only in the separate PQ file,
 * it also embeds them <em>inline in the graph</em> — one PQ code per edge slot of every node.
 * This makes approximate distance lookups faster (no separate file seek) at the cost of more
 * space in TERMS_DATA.
 *
 * <p>The per-node inline data in TERMS_DATA grows as follows:
 * <pre>
 *   EC:  InlineVectors only          →  D × 4 bytes/node  (raw floats)
 *   FA:  InlineVectors + FusedPQ     →  D × 4 + m × M bytes/node
 *
 *   where:
 *     D         = vector dimension
 *     m         = pq.compressedVectorSize()  — bytes per PQ-encoded vector
 *                 (determined by dimension; for D=256 with default settings: m=100)
 *     M         = maxDegree = 2 × {@link IndexWriterConfig#DEFAULT_MAXIMUM_NODE_CONNECTIONS}
 *                 (default: 2 × 16 = 32)
 *
 *   Per-node TERMS_DATA delta:  m × M  (e.g. 100 × 32 = 3,200 bytes for D=256)
 * </pre>
 *
 * <p><b>Note on naming:</b> in the code below {@code M} refers to
 * {@code pq.compressedVectorSize()} (bytes per PQ code) and {@code maxDegree} refers to
 * the graph edge limit.
 *
 * <h2>Full EC → FA disk delta</h2>
 *
 * <p>Three contributors to the TERMS_DATA growth, plus one tiny META change:
 * <pre>
 *   Δ_TERMS_DATA = N × M × maxDegree              per-node FusedPQ bytes (dominant term)
 *                + N_level1 × (4 + M)              writeSparseLevels: one record per level-1 node
 *                                                  (N_level1 = graph.size(1); with hierarchy
 *                                                  enabled this is O(log N); without hierarchy
 *                                                  it is 1 — just the single entry node)
 *                + 2 × (4×(6+M) + 4×256×D)        FusedPQ codebook added to the file header,
 *                                                  written twice (start + footer)
 *
 *   Δ_META       = 8 bytes                         totalTermCount field added in version ED,
 *                                                  present in FA, absent in EC (one segment)
 *
 *   Δ_PQ, Δ_POSTING_LISTS, Δ_MARKER = 0           unchanged EC → FA
 * </pre>
 *
 * <h2>Test design</h2>
 *
 * <p>Each test run creates a fresh table, writes {@code NUM_SSTABLES × ROWS_PER_SSTABLE} vectors
 * across {@code NUM_SSTABLES} flushes, then runs a major compaction producing a single segment.
 * The EC run uses the default flat graph ({@code enable_hierarchy=false}).
 * The FA run enables hierarchy ({@code enable_hierarchy=true}), exercising the multi-level
 * writeSparseLevels path and making the comparison representative of a real upgrade scenario.
 */
public class VectorFormatDiskUsageTest extends VectorTester
{
    private static final int DIMENSIONS = 256;

    private static final int ROWS_PER_SSTABLE = CassandraOnHeapGraph.MIN_PQ_ROWS;

    /** Number of SSTable flushes before the major compaction. */
    private static final int NUM_SSTABLES = 3;

    @BeforeClass
    public static void setUpClass()
    {
        VectorTester.setUpClass();
    }

    @After
    public void resetVersion()
    {
        SAIUtil.resetCurrentVersion();
    }

    @Test
    public void testDiskUsageECvsFA()
    {
        DiskMeasurement ec = measureDiskUsage(Version.EC, "EC");
        DiskMeasurement fa = measureDiskUsage(Version.FA, "FA");

        assertTrue("EC index must have non-zero disk usage", ec.totalBytes > 0);
        assertTrue("FA index must have non-zero disk usage", fa.totalBytes > 0);

        // Both runs index the same rows with the same config, so N, M, and maxDegree must match.
        int N = ec.graphNodeCount;
        assertEquals("FA and EC must index the same number of graph nodes", ec.graphNodeCount, fa.graphNodeCount);
        assertEquals("M must be equal across versions", ec.pqSubspaceCount, fa.pqSubspaceCount);
        assertEquals("maxDegree must be equal across versions", ec.graphMaxDegree, fa.graphMaxDegree);

        int M         = ec.pqSubspaceCount;   // bytes per PQ-encoded vector (m in some docs)
        int maxDegree = ec.graphMaxDegree;     // max graph edges per node   (M in some docs)

        long actualTermsDataDelta = fa.termsDataBytes - ec.termsDataBytes;
        long actualPqDelta        = fa.pqBytes - ec.pqBytes;
        long actualMetaDelta      = fa.metaBytes - ec.metaBytes;
        long actualNetDelta       = fa.totalBytes - ec.totalBytes;

        logger.info("=== EC vs FA disk-usage comparison (D={}, N={}, M={}, maxDegree={}) ===",
                    DIMENSIONS, N, M, maxDegree);
        logger.info("  EC  IndexMetrics.diskUsedBytes : {}", ec.totalBytes);
        logger.info("  FA  IndexMetrics.diskUsedBytes : {}", fa.totalBytes);
        logger.info("  FA/EC ratio                    : {}", String.format("%.3f", (double) fa.totalBytes / ec.totalBytes));
        logger.info("  TERMS_DATA delta               : {}", actualTermsDataDelta);
        logger.info("  PQ delta                       : {}", actualPqDelta);
        logger.info("  META delta                     : {}", actualMetaDelta);
        logger.info("  Net total delta                : {}", actualNetDelta);

        // Sanity: totalBytes must equal the sum of all five components (nothing double-counted or missed).
        assertEquals("EC: totalBytes must equal sum of all per-index components",
                     ec.totalBytes, ec.termsDataBytes + ec.pqBytes + ec.metaBytes + ec.postingListsBytes + ec.completionMarkerBytes);
        assertEquals("FA: totalBytes must equal sum of all per-index components",
                     fa.totalBytes, fa.termsDataBytes + fa.pqBytes + fa.metaBytes + fa.postingListsBytes + fa.completionMarkerBytes);

        // --- Assertion 1: TERMS_DATA grows by N×M×maxDegree (FusedPQ inline bytes) ---
        //
        // Both EC and FA store N×D×4 InlineVectors bytes per node, so those cancel in the delta.
        // The delta is purely the FusedPQ addition: M×maxDegree bytes per node.
        //
        // Beyond the per-node term, two small fixed costs are added by FA:
        //   - writeSparseLevels (version==6): N_level1 × (writeInt(ordinal) + writeSourceFeature(M bytes))
        //     = N_level1 × (4+M) bytes. With hierarchy N_level1 = graph.size(1) (O(log N) nodes).
        //   - File header (Header.size(), written twice — start + footer):
        //       FA adds one extra featureId slot (4 bytes) + FusedPQ.headerSize() = pq.compressorSize()
        //       = 4*(5+M) + 4*256*D bytes per write → delta per write = 4*(6+M) + 4*256*D
        //
        // Lower bound (per-node term only):  N × M × maxDegree
        // Exact formula (all contributors):  N×M×maxDegree + N_level1×(4+M) + 2×(4×(6+M) + 4×256×D)
        // N_level1 and the internal jvector overhead are absorbed by the 1% tolerance.
        long minTermsDataDelta = (long) N * (long) M * maxDegree;
        long headerDeltaPerWrite = 4L * (6 + M) + 4L * 256 * DIMENSIONS;
        // N_level1 is not directly accessible without a getter on CassandraDiskAnn; use 1 as a
        // conservative lower-bound placeholder. The 1% tolerance absorbs the actual level-1 cost.
        long computedTermsDataDelta = (long) N * (long) M * maxDegree
                                   + (4L + M)
                                   + 2L * headerDeltaPerWrite;
        assertTrue(String.format("TERMS_DATA delta %d must be >= N*M*maxDegree = %d",
                                 actualTermsDataDelta, minTermsDataDelta),
                   actualTermsDataDelta >= minTermsDataDelta);
        assertApproxEquals(String.format("TERMS_DATA delta must be within 1%% of exact formula %d", computedTermsDataDelta),
                           computedTermsDataDelta, actualTermsDataDelta, computedTermsDataDelta / 100);

        // --- Assertion 2: PQ component is the same size for EC and FA after compaction ---
        //
        // CompactionGraph.flush() always writes PQVectors (codebook + count + N×M codes) regardless
        // of whether FusedPQ is active, so the PQ file has identical content in both versions.
        assertEquals("PQ component size must be the same for EC and FA after compaction",
                ec.pqBytes, fa.pqBytes);

        // --- Assertion 3: META grows by exactly 8 bytes ---
        //
        // Version ED added a totalTermCount long (8 bytes) to each segment's metadata block.
        // EC predates ED so omits it; FA includes it. One segment after major compaction → +8 bytes.
        assertEquals("FA META must be exactly 8 bytes larger than EC (totalTermCount long added in ED)",
                     ec.metaBytes + Long.BYTES, fa.metaBytes);

        // --- Assertion 4: POSTING_LISTS and COLUMN_COMPLETION_MARKER are unchanged ---
        assertEquals("POSTING_LISTS must be the same size for EC and FA",
                     ec.postingListsBytes, fa.postingListsBytes);
        assertEquals("COLUMN_COMPLETION_MARKER must be the same size for EC and FA",
                     ec.completionMarkerBytes, fa.completionMarkerBytes);

        // --- Assertion 5: conservation check ---
        //
        // Since PQ, POSTING_LISTS, and COMPLETION_MARKER are identical (assertions 2–4),
        // the net total delta must equal the TERMS_DATA delta plus the 8-byte META delta.
        assertEquals("fa.totalBytes must equal ec.totalBytes + TERMS_DATA delta + 8 (META totalTermCount)",
                     ec.totalBytes + actualTermsDataDelta + Long.BYTES, fa.totalBytes);

        logger.info("IndexMetrics.diskUsedBytes EC={} FA={} delta={} ratio={}",
                    ec.totalBytes, fa.totalBytes,
                    fa.totalBytes - ec.totalBytes,
                    String.format("%.4f", (double) fa.totalBytes / ec.totalBytes));
    }

    private static void assertApproxEquals(String label, long expected, long actual, long tolerance)
    {
        assertTrue(String.format("%s: expected ~%d, actual %d, tolerance ±%d", label, expected, actual, tolerance),
                   Math.abs(actual - expected) <= tolerance);
    }

    /** Captures per-run disk measurements and graph metadata needed for formula assertions. */
    private static class DiskMeasurement
    {
        final long totalBytes;
        final long termsDataBytes;
        final long pqBytes;
        final long metaBytes;
        final long postingListsBytes;
        final long completionMarkerBytes;
        final int  graphNodeCount;   // N: indexed vectors in the compacted segment
        final int  pqSubspaceCount;  // M: pq.compressedVectorSize() — bytes per PQ-encoded vector
        final int  graphMaxDegree;   // maxDegree: 2 × maximumNodeConnections
        final int  graphMaxLevel;    // 0 for flat graph (EC), ≥1 when hierarchy is enabled (FA)

        DiskMeasurement(long totalBytes,
                        long termsDataBytes,
                        long pqBytes,
                        long metaBytes,
                        long postingListsBytes,
                        long completionMarkerBytes,
                        int graphNodeCount,
                        int pqSubspaceCount,
                        int graphMaxDegree,
                        int graphMaxLevel)
        {
            this.totalBytes            = totalBytes;
            this.termsDataBytes        = termsDataBytes;
            this.pqBytes               = pqBytes;
            this.metaBytes             = metaBytes;
            this.postingListsBytes     = postingListsBytes;
            this.completionMarkerBytes = completionMarkerBytes;
            this.graphNodeCount        = graphNodeCount;
            this.pqSubspaceCount       = pqSubspaceCount;
            this.graphMaxDegree        = graphMaxDegree;
            this.graphMaxLevel         = graphMaxLevel;
        }
    }

    /**
     * Creates a fresh table at the given SAI version, writes
     * {@code NUM_SSTABLES × ROWS_PER_SSTABLE} D=256 random vectors across
     * {@code NUM_SSTABLES} flushes, runs a major compaction, then returns a
     * {@link DiskMeasurement} with total and per-component sizes plus graph metadata.
     * EC is built with a flat graph; FA is built with hierarchy enabled.
     */
    private DiskMeasurement measureDiskUsage(Version version, String label)
    {
        SAIUtil.setCurrentVersion(version);

        // FusedPQ is version-gated: automatically enabled for FA (JVector format version 6+).
        assertEquals('[' + label + "] FusedPQ version gate",
                     version.equals(Version.FA),
                     JVectorVersionUtil.shouldWriteFused(version));

        boolean hierarchyEnabled = version.equals(Version.FA);
        createTable("CREATE TABLE %s (pk int, v vector<float, " + DIMENSIONS + ">, PRIMARY KEY(pk))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' " +
                                       "WITH OPTIONS = {'" + IndexWriterConfig.ENABLE_HIERARCHY + "': '" + hierarchyEnabled + "'}");
        disableCompaction();

        int pk = 0;
        for (int s = 0; s < NUM_SSTABLES; s++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk++, randomVectorBoxed(DIMENSIONS));
            flush();
        }

        compact();

        var sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        assertNotNull("Index not found: " + indexName, sai);
        var indexContext = sai.getIndexContext();

        long totalDiskBytes        = indexContext.diskUsage();
        long graphComponentBytes   = componentSize(indexContext, IndexComponentType.TERMS_DATA);
        long pqComponentBytes      = componentSize(indexContext, IndexComponentType.PQ);
        long metaComponentBytes    = componentSize(indexContext, IndexComponentType.META);
        long postingListsBytes     = componentSize(indexContext, IndexComponentType.POSTING_LISTS);
        long completionMarkerBytes = componentSize(indexContext, IndexComponentType.COLUMN_COMPLETION_MARKER);

        // Verify there is exactly one segment after major compaction.
        SSTableIndex sstableIndex = indexContext.getView().getIndexes().iterator().next();
        assertEquals(1, sstableIndex.getSegments().size());
        var segment  = sstableIndex.getSegments().get(0);
        var searcher = (V2VectorIndexSearcher) segment.getIndexSearcher();

        // N: number of graph nodes loaded, reported by CassandraDiskAnn.onGraphLoaded().
        var vectorMetrics = (ColumnQueryMetrics.VectorIndexMetrics) indexContext.getColumnQueryMetrics();
        int N = (int) vectorMetrics.onDiskGraphVectorsCount.sum();

        // M: bytes per PQ-encoded vector, determined by dimension and source model.
        // For D=256 with the default OTHER model this is 100.
        int M = indexContext.getIndexWriterConfig()
                            .getSourceModel()
                            .compressionProvider.apply(DIMENSIONS)
                            .getCompressedSize();

        // maxDegree: maximum edges per graph node, passed as-is to FusedPQ.
        int maxDegree = IndexWriterConfig.DEFAULT_MAXIMUM_NODE_CONNECTIONS * 2;

        // graphMaxLevel: 0 for EC (flat graph), ≥1 for FA (hierarchy enabled).
        // ImmutableGraphIndex.getMaxLevel() is on the graph object, not the view, and
        // CassandraDiskAnn.graph is private, so we derive it from the known input instead.
        int graphMaxLevel = hierarchyEnabled ? 1 : 0;

        // FA: compressedVectors is null because PQ codes are fused into TERMS_DATA.
        // EC: compressedVectors holds the standalone PQVectors loaded from the PQ file.
        boolean hasFusedPQ = (searcher.graph.getCompressedVectors() == null);
        assertEquals('[' + label + "] FusedPQ must be active iff version supports it",
                     JVectorVersionUtil.versionSupportsFused(version), hasFusedPQ);

        logger.info("[{}] IndexMetrics.diskUsedBytes  : {}", label, totalDiskBytes);
        logger.info("[{}] graph maxLevel              : {} ({})",
                    label, graphMaxLevel,
                    graphMaxLevel > 0 ? "hierarchical" : "flat");
        logger.info("[{}] TERMS_DATA component bytes  : {} ({} inline bytes/node: {})",
                    label, graphComponentBytes,
                    hasFusedPQ ? M * maxDegree : DIMENSIONS * Float.BYTES,
                    hasFusedPQ ? "FusedPQ M=" + M + " × maxDegree=" + maxDegree
                               : "InlineVectors D=" + DIMENSIONS + " × 4");
        logger.info("[{}] PQ component bytes          : {} (codebook + N×M codes, N={} M={} maxDegree={})",
                    label, pqComponentBytes, N, M, maxDegree);
        logger.info("[{}] META component bytes        : {} ({})",
                    label, metaComponentBytes,
                    version.onOrAfter(Version.ED) ? "includes 8-byte totalTermCount"
                                                  : "no totalTermCount (pre-ED)");
        logger.info("[{}] POSTING_LISTS bytes         : {}", label, postingListsBytes);
        logger.info("[{}] COMPLETION_MARKER bytes     : {}", label, completionMarkerBytes);

        return new DiskMeasurement(totalDiskBytes,
                                   graphComponentBytes, pqComponentBytes,
                                   metaComponentBytes, postingListsBytes, completionMarkerBytes,
                                   N, M, maxDegree, graphMaxLevel);
    }

    /**
     * Returns the total on-disk size across all SSTableIndexes in the current view for one
     * component type. Uses {@code has()} before {@code get()} because {@code get()} throws
     * when the component is absent.
     */
    private long componentSize(org.apache.cassandra.index.sai.IndexContext indexContext,
                               IndexComponentType type)
    {
        return indexContext.getView().getIndexes()
                           .stream()
                           .mapToLong(idx -> {
                               IndexComponents.ForRead perIndex = idx.usedPerIndexComponents();
                               return perIndex.has(type) ? perIndex.get(type).file().length() : 0L;
                           })
                           .sum();
    }
}
