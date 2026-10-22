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

package org.apache.cassandra.index.sai.functional;

import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;

import static org.junit.Assert.*;

/// Verifies on-disk vector index size behavior across SAI format versions,
/// both preCompaction and postCompaction.
public class VectorFormatDiskUsageTest extends VectorTester
{
    private static final int DIMENSION = 128;

    /// Number of flushes before compaction. Each flush produces one SSTable. With only
    /// [CassandraOnHeapGraph#MIN_PQ_ROWS] rows per flush the memory limit is not reached,
    /// so each SSTable contains exactly one segment — asserted in [#measurePostFlushAndPostCompaction].
    private static final int NUM_FLUSHES = 2;

    /// TERMS_DATA delta from EC (jvector format 4) to FB (jvector format 6) per segment:
    /// one extra header copy + FOOTER_SIZE trailer. Independent of dimensions, number of vectors, etc.
    ///
    /// EC header  = (CommonHeader=288) + (feature bitmask int=4)                    = 292 bytes
    /// FB header  = (CommonHeader=288) + (features.size() int=4) + (ordinal int=4)  = 296 bytes
    /// FB writes: start-header + footer-header + FOOTER_SIZE(=Long+Int=12)          = 604 bytes
    /// Δ = 604 − 292 = 312 bytes per segment
    private static final long EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT = 312L;

    /// `PQVectors.write` writes two extra ints — `vectorCount` and `subspaceCount` — before the
    /// per-vector data that `ProductQuantization.write` (the FusedPQ codebook-only path) does not.
    /// This means the fixed overhead `F` in EC PQ files is 8 bytes larger than in FB FusedPQ PQ files:
    ///   F_EC = F_FB + PQVECTORS_EXTRA_HEADER_BYTES
    private static final long PQVECTORS_EXTRA_HEADER_BYTES = 2L * Integer.BYTES;

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

    /// Deep structural verification of the EC → FB format upgrade (jvector format 4 → 6,
    /// no FusedPQ, no hierarchy). Asserts exact per-component byte deltas and that total
    /// disk usage grows by less than 5%.
    ///
    /// A SAI vector index is stored as five on-disk component files per SSTable:
    ///
    /// | Component | Content |
    /// |-----------|---------|
    /// | `TERMS_DATA` | The graph: nodes, edges, and per-node inline vector data |
    /// | `PQ` | Product Quantization codebook + per-vector PQ codes |
    /// | `META` | Segment metadata: offsets, lengths, statistics |
    /// | `POSTING_LISTS` | Mapping from row IDs to graph node ordinals |
    /// | `COLUMN_COMPLETION_MARKER` | Fixed-size sentinel indicating the index is complete |
    ///
    /// FB (jvector format 6) writes the graph header twice (start + footer) plus a 12-byte trailer,
    /// vs EC (jvector format 4) which writes it once.
    ///
    /// Header sizes are derived from {@code CommonHeader.size()} and {@code Header.size()} in jvector:
    ///
    /// ```
    /// CommonHeader.size() (both formats):
    ///   int size = 4;                         // size + dimension + entryNode + maxDegree  (always)
    ///   if (version >= 3) size += 2;           // magic + version
    ///   if (version >= 4) size += 2 + 2 * 32; // idUpperBound + numLayers + 32 LayerInfo pairs
    ///   → (4 + 2 + 2 + 64) × 4 = 288 bytes
    ///
    /// Header.size() adds the feature section on top of CommonHeader:
    ///   EC (format 4, version < 6): +Integer.BYTES for a single FeatureId bitmask int  → 288 + 4 = 292 bytes
    ///   FB (format 6):              +Integer.BYTES (features count) + Integer.BYTES (feature ordinal) → 288 + 4 + 4 = 296 bytes
    ///
    /// The only feature present by default on FB is INLINE_VECTORS. With exactly [CassandraOnHeapGraph#MIN_PQ_ROWS]
    /// rows, PQ training is met but FusedPQ is disabled, so no FusedPQ feature is written.
    /// InlineVectors.headerSize() == 0 because dimension is already stored in CommonHeader.
    ///
    /// FOOTER_SIZE = FOOTER_MAGIC_SIZE(Integer.BYTES=4) + FOOTER_OFFSET_SIZE(Long.BYTES=8) = 12 bytes.
    /// These constants and writeFooter() are in {@code AbstractGraphIndexWriter} in jvector.
    /// writeFooter() writes: full header copy + 8-byte offset (pointing back to the header) + 4-byte magic.
    /// Cassandra always passes useFooter=false to OnDiskGraphIndex.load(), so the footer is written
    /// as part of the format but never read — see [CassandraDiskAnn] constructor.
    ///
    /// FB writes: start-header(296) + footer-header(296) + FOOTER_SIZE(Long+Int=12) = 604 bytes
    /// EC writes: start-header(292)                                                  = 292 bytes
    /// Δ = 604 − 292 = 312 bytes per segment
    /// ```
    ///
    /// `META` grows by exactly 8 bytes per segment (a `totalTermCount` long added in ED).
    /// All other components (`PQ`, `POSTING_LISTS`, `COLUMN_COMPLETION_MARKER`) are unchanged.
    @Test
    public void testDiskUsageECvsFB()
    {
        DiskMeasurement[] ec = measurePostFlushAndPostCompaction(Version.EC, "EC", null);
        DiskMeasurement[] fb = measurePostFlushAndPostCompaction(Version.FB, "FB", null);

        verifyECvsFB(ec[0], fb[0], "preCompaction");
        verifyECvsFB(ec[1], fb[1], "postCompaction");
    }

    private void verifyECvsFB(DiskMeasurement ec, DiskMeasurement fb, String phase)
    {
        assertTrue("EC index must have non-zero disk usage", ec.totalBytes > 0);
        assertTrue("FB index must have non-zero disk usage", fb.totalBytes > 0);

        long termsDataDelta = fb.termsDataBytes - ec.termsDataBytes;
        double diskGrowthPercent = 100.0 * (fb.totalBytes - ec.totalBytes) / ec.totalBytes;

        logger.debug("  EC {}  diskUsage() : {} ({} segments)", phase, ec.totalBytes, ec.segmentCount);
        logger.debug("  FB {}  diskUsage() : {} ({} segments)", phase, fb.totalBytes, fb.segmentCount);
        logger.debug("  Total disk usage growth {}  : +{} bytes ({} %)",
                     phase, fb.totalBytes - ec.totalBytes, String.format("%.4f", diskGrowthPercent));
        logger.debug("  TERMS_DATA delta {}  : {} (expected {} × {} = {})",
                     phase, termsDataDelta, NUM_FLUSHES, EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT,
                     NUM_FLUSHES * EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT);

        verifyComponentAccounting(ec, fb, phase);
        verifyTermsDataDelta(termsDataDelta, ec.segmentCount, phase);
        verifyUnchangedComponents(ec, fb, phase);
        verifyConservation(ec, fb, termsDataDelta, phase);
        verifyTotalDiskGrowthUnder5Percent(diskGrowthPercent, phase);
    }

    /// Regression guard: for every vector-capable version from [Version#JVECTOR_EARLIEST] (CA)
    /// up to but not including [Version#LATEST], asserts that [Version#LATEST] uses less than
    /// 5% more disk than that older version. Catches accidental large regressions introduced
    /// by a new format version.
    @Test
    public void testDiskGrowthAcrossVersions()
    {
        DiskMeasurement[] latest = measurePostFlushAndPostCompaction(Version.LATEST, Version.LATEST.toString(), null);

        for (Version version : Version.ALL)
        {
            if (version == Version.LATEST || !version.onOrAfter(Version.JVECTOR_EARLIEST))
                continue;

            DiskMeasurement[] older = measurePostFlushAndPostCompaction(version, version.toString(), null);

            for (int i = 0; i < 2; i++)
            {
                String phase = i == 0 ? "preCompaction" : "postCompaction";
                double diskGrowthPercent = 100.0 * (latest[i].totalBytes - older[i].totalBytes) / older[i].totalBytes;

                logger.debug("  {} → {} {}  : {} → {} bytes ({} %)",
                             version, Version.LATEST, phase, older[i].totalBytes, latest[i].totalBytes,
                             String.format("%.4f", diskGrowthPercent));

                verifyTotalDiskGrowthUnder5Percent(diskGrowthPercent, version + " → " + Version.LATEST + ' ' + phase);
            }
        }
    }

    /// Sanity: totalBytes must equal the sum of all five components (nothing missed or double-counted).
    private static void verifyComponentAccounting(DiskMeasurement ec, DiskMeasurement fb, String phase)
    {
        assertEquals("EC " + phase + ": totalBytes must equal sum of all per-index components",
                     ec.totalBytes, ec.termsDataBytes + ec.pqBytes + ec.metaBytes + ec.postingListsBytes + ec.completionMarkerBytes);
        assertEquals("FB " + phase + ": totalBytes must equal sum of all per-index components",
                     fb.totalBytes, fb.termsDataBytes + fb.pqBytes + fb.metaBytes + fb.postingListsBytes + fb.completionMarkerBytes);
    }

    /// The TERMS_DATA delta must equal exactly [#EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT] × segmentCount.
    /// Each segment contributes one graph to the TERMS_DATA file, and each graph carries one header delta.
    private static void verifyTermsDataDelta(long actualTermsDataDelta, int segmentCount, String phase)
    {
        long expected = EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT * segmentCount;
        assertEquals("TERMS_DATA delta " + phase + " must equal " + EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT
                     + " bytes × " + segmentCount + " segment(s) = " + expected
                     + " (jvector format 4→6 header layout change only)",
                     expected, actualTermsDataDelta);
    }

    private static void verifyUnchangedComponents(DiskMeasurement ec, DiskMeasurement fb, String phase)
    {
        assertEquals("PQ component size must be the same for EC and FB (" + phase + ')',
                     ec.pqBytes, fb.pqBytes);
        assertEquals("POSTING_LISTS must be the same size for EC and FB (" + phase + ')',
                     ec.postingListsBytes, fb.postingListsBytes);
        assertEquals("COLUMN_COMPLETION_MARKER must be the same size for EC and FB (" + phase + ')',
                     ec.completionMarkerBytes, fb.completionMarkerBytes);
    }

    /// META grows by exactly 8 bytes per segment EC → FB (a `totalTermCount` long added in ED).
    /// Because all other components except TERMS_DATA are identical, the net total delta equals
    /// the TERMS_DATA delta plus 8 × segmentCount bytes.
    private static void verifyConservation(DiskMeasurement ec, DiskMeasurement fb,
                                           long actualTermsDataDelta, String phase)
    {
        assertEquals("FB META must be exactly " + Long.BYTES + " bytes × " + ec.segmentCount
                     + " segment(s) larger than EC META (" + phase + ')',
                     ec.metaBytes + (long) Long.BYTES * ec.segmentCount, fb.metaBytes);
        assertEquals("FB.totalBytes must equal ec.totalBytes + TERMS_DATA delta + 8 × segmentCount(" + phase + ')',
                     ec.totalBytes + actualTermsDataDelta + (long) Long.BYTES * ec.segmentCount,
                     fb.totalBytes);
    }

    /// Total disk usage must grow by less than 5% between consecutive format versions.
    /// The fixed per-segment overhead (header delta + META) is negligible relative to
    /// the graph node data for any realistic dataset.
    private static void verifyTotalDiskGrowthUnder5Percent(double diskGrowthPercent, String phase)
    {
        assertTrue(String.format("Total disk usage growth %s must be < 5%% but was %.4f%%",
                                 phase, diskGrowthPercent),
                   diskGrowthPercent < 5.0);
    }

    private static class DiskMeasurement
    {
        final long totalBytes;
        final long termsDataBytes;
        final long pqBytes;
        final long metaBytes;
        final long postingListsBytes;
        final long completionMarkerBytes;
        final int segmentCount; // total segments across all SSTables

        private DiskMeasurement(Builder b)
        {
            this.totalBytes = b.totalBytes;
            this.termsDataBytes = b.termsDataBytes;
            this.pqBytes = b.pqBytes;
            this.metaBytes = b.metaBytes;
            this.postingListsBytes = b.postingListsBytes;
            this.completionMarkerBytes = b.completionMarkerBytes;
            this.segmentCount = b.segmentCount;
        }

        static class Builder
        {
            long totalBytes;
            long termsDataBytes;
            long pqBytes;
            long metaBytes;
            long postingListsBytes;
            long completionMarkerBytes;
            int segmentCount;

            Builder totalBytes(long v)
            {
                this.totalBytes = v;
                return this;
            }

            Builder termsDataBytes(long v)
            {
                this.termsDataBytes = v;
                return this;
            }

            Builder pqBytes(long v)
            {
                this.pqBytes = v;
                return this;
            }

            Builder metaBytes(long v)
            {
                this.metaBytes = v;
                return this;
            }

            Builder postingListsBytes(long v)
            {
                this.postingListsBytes = v;
                return this;
            }

            Builder completionMarkerBytes(long v)
            {
                this.completionMarkerBytes = v;
                return this;
            }

            Builder segmentCount(int v)
            {
                this.segmentCount = v;
                return this;
            }

            DiskMeasurement build()
            {
                return new DiskMeasurement(this);
            }
        }
    }

    private DiskMeasurement[] measurePostFlushAndPostCompaction(Version version, String label, String indexOptions)
    {
        SAIUtil.setCurrentVersion(version);
        createTable("CREATE TABLE %s (pk int, v vector<float, " + DIMENSION + ">, PRIMARY KEY(pk))");
        String createIndex = "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'";
        if (indexOptions != null)
            createIndex += " WITH OPTIONS = " + indexOptions;
        String indexName = createIndex(createIndex);
        disableCompaction();

        for (int flush = 0; flush < NUM_FLUSHES; flush++)
        {
            for (int i = 0; i < CassandraOnHeapGraph.MIN_PQ_ROWS; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)",
                        flush * CassandraOnHeapGraph.MIN_PQ_ROWS + i, randomVectorBoxed(DIMENSION));
            flush();
        }

        DiskMeasurement afterFlush = snapshot(indexName, label + "-flush", false);
        compact();
        DiskMeasurement afterCompact = snapshot(indexName, label + "-compact", true);
        return new DiskMeasurement[]{ afterFlush, afterCompact };
    }

    private DiskMeasurement snapshot(String indexName, String label, boolean compact)
    {
        StorageAttachedIndex sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        assertNotNull("Index not found: " + indexName, sai);
        IndexContext indexContext = sai.getIndexContext();

        long totalDiskBytes = indexContext.diskUsage();
        long graphComponentBytes = componentSize(indexContext, IndexComponentType.TERMS_DATA);
        long pqComponentBytes = componentSize(indexContext, IndexComponentType.PQ);
        long metaComponentBytes = componentSize(indexContext, IndexComponentType.META);
        long postingListsBytes = componentSize(indexContext, IndexComponentType.POSTING_LISTS);
        long completionMarkerBytes = componentSize(indexContext, IndexComponentType.COLUMN_COMPLETION_MARKER);

        List<SSTableIndex> sstableIndexes = List.copyOf(indexContext.getView().getIndexes());
        int totalSegments = sstableIndexes.stream().mapToInt(s -> s.getSegments().size()).sum();
        int expectedSegments = compact ? 1 : NUM_FLUSHES;
        assertEquals("Expected " + expectedSegments + " segment(s) " + label, expectedSegments, totalSegments);

        logger.debug("[{}] diskUsage()                : {} ({} segment(s))", label, totalDiskBytes, totalSegments);
        logger.debug("[{}] TERMS_DATA component bytes : {}", label, graphComponentBytes);
        logger.debug("[{}] PQ component bytes         : {}", label, pqComponentBytes);
        logger.debug("[{}] META component bytes       : {}", label, metaComponentBytes);
        logger.debug("[{}] POSTING_LISTS bytes        : {}", label, postingListsBytes);
        logger.debug("[{}] COMPLETION_MARKER bytes    : {}", label, completionMarkerBytes);

        return new DiskMeasurement.Builder()
               .totalBytes(totalDiskBytes)
               .termsDataBytes(graphComponentBytes)
               .pqBytes(pqComponentBytes)
               .metaBytes(metaComponentBytes)
               .postingListsBytes(postingListsBytes)
               .completionMarkerBytes(completionMarkerBytes)
               .segmentCount(totalSegments)
               .build();
    }

    /// Regression test for the FusedPQ compaction bug - CNDB-18842
    ///
    /// CompactionGraph unconditionally wrote full PQVectors (codebook + N×m
    /// per-vector codes) to the PQ file even when FusedPQ was active, duplicating on disk
    /// the same codes already embedded in TERMS_DATA. The flush path ([CassandraOnHeapGraph])
    /// was already correct: with FusedPQ it writes only the codebook to the PQ file.
    @Test
    public void testFusedPQPqFileContainsCodebookOnlyAfterFlushAndCompaction()
    {
        // FusedPQ should be always run with hierarchy and parallel graph writing enabled
        String hierarchyOptions = "{'" + IndexWriterConfig.ENABLE_HIERARCHY + "': 'true'}";
        SAIUtil.setParallelEncodingWriting(true);

        try
        {
            // The PQ file size follows:
            //
            //   EC (PQVectors.write):           pqBytes(N) = F_EC + N×m
            //   FB FusedPQ (PQ.write codebook): pqBytes    = F_FB          (independent of N)
            //
            //   F  = fixed overhead: SAI header + SAI PQ header (magic/version/unitVectors/type) + PQ codebook body
            //   m  = bytes per PQ-encoded vector (compressedVectorSize), from VectorSourceModel
            //   N  = number of indexed vectors in the segment
            //
            // F_EC ≠ F_FB: PQVectors.write prepends the codebook write with two extra ints before the
            // per-vector data — vectorCount (4 bytes) + subspaceCount (4 bytes) — that
            // ProductQuantization.write (codebook only) does not write.
            // Therefore, F_EC = F_FB + 8.
            //
            // m is statically known: the test uses no explicit source-model option, so
            // VectorSourceModel.OTHER applies. For DIMENSION=128, defaultPQBytesFor(128)
            // falls in the 64 < D <= 200 branch → m = (int)(128 * 0.5) = 64 bytes.
            //
            // Each flushed SSTable has N = MIN_PQ_ROWS vectors.
            // The compacted SSTable has N = NUM_FLUSHES × MIN_PQ_ROWS vectors.
            //
            // EC:
            //   ecFlush.pqBytes / NUM_FLUSHES  = F_EC + MIN_PQ_ROWS × m
            //   ecCompact.pqBytes              = F_EC + NUM_FLUSHES × MIN_PQ_ROWS × m
            //
            // FB FusedPQ:
            //   fbFlush.pqBytes / NUM_FLUSHES  = F_FB = F_EC - 8
            //   fbCompact.pqBytes              = F_FB = F_EC - 8  (independent of N)
            int m = VectorSourceModel.OTHER.compressionProvider.apply(DIMENSION).getCompressedSize();

            DiskMeasurement[] ec = measurePostFlushAndPostCompaction(Version.EC, "EC", hierarchyOptions);
            DiskMeasurement ecFlush = ec[0], ecCompact = ec[1];

            long ecFlushPqBytesPerSegment = ecFlush.pqBytes / NUM_FLUSHES;
            // The extra vectors compaction adds over a single flush: (NUM_FLUSHES-1) × MIN_PQ_ROWS
            long extraVectors = (long) (NUM_FLUSHES - 1) * CassandraOnHeapGraph.MIN_PQ_ROWS;
            // Derive the measured m from EC observations and cross-check against the static value.
            long measuredM = (ecCompact.pqBytes - ecFlushPqBytesPerSegment) / extraVectors;
            assertEquals("Measured per-vector PQ code bytes from EC must match VectorSourceModel.OTHER formula",
                         m, measuredM);
            long expectedEcCompactPqBytes = ecFlushPqBytesPerSegment + extraVectors * m;

            // FB + FusedPQ enabled via -D flag + hierarchy + parallel graph writing.
            SAIUtil.setEnableFused(true);
            try
            {
                DiskMeasurement[] fb = measurePostFlushAndPostCompaction(Version.FB, "FB", hierarchyOptions);
                DiskMeasurement fbFlush = fb[0], fbCompact = fb[1];

                long fbFlushPqBytesPerSegment = fbFlush.pqBytes / NUM_FLUSHES;

                logger.info("EC flush    PQ bytes/segment : {}", ecFlushPqBytesPerSegment);
                logger.info("EC compact  PQ bytes (actual)  : {}", ecCompact.pqBytes);
                logger.info("EC compact  PQ bytes (expected, m={} per vector): {}", m, expectedEcCompactPqBytes);
                logger.info("FB flush    PQ bytes/segment : {}", fbFlushPqBytesPerSegment);
                logger.info("FB compact  PQ bytes         : {}", fbCompact.pqBytes);

                assertEquals("EC compacted PQ must equal F + NUM_FLUSHES×MIN_PQ_ROWS×m " +
                             "(full PQVectors, size scales linearly with N)",
                             expectedEcCompactPqBytes, ecCompact.pqBytes);

                verifyFusedPQPqFileIsCodebookOnly(ecFlushPqBytesPerSegment, ecCompact.pqBytes,
                                                  fbFlushPqBytesPerSegment, fbCompact.pqBytes, m);

                verifyFusedPQSearchWorks();
            }
            finally
            {
                SAIUtil.setEnableFused(false);
            }
        }
        finally
        {
            SAIUtil.setParallelEncodingWriting(false);
        }
    }

    private static void verifyFusedPQPqFileIsCodebookOnly(long ecFlushPqBytesPerSegment, long ecCompactPqBytes,
                                                          long fbFlushPqBytesPerSegment, long fbCompactPqBytes,
                                                          int m)
    {
        assertFusedPqSavings("flush PQ/segment", ecFlushPqBytesPerSegment,
                             (long) CassandraOnHeapGraph.MIN_PQ_ROWS * m, fbFlushPqBytesPerSegment);

        assertEquals("FB-FusedPQ compacted PQ must equal FB-FusedPQ flushed PQ/segment " +
                     "(both codebook-only, F; bug wrote F + N×m on compaction).",
                     fbFlushPqBytesPerSegment, fbCompactPqBytes);

        assertFusedPqSavings("compact PQ", ecCompactPqBytes,
                             (long) NUM_FLUSHES * CassandraOnHeapGraph.MIN_PQ_ROWS * m, fbCompactPqBytes);
    }

    private static void assertFusedPqSavings(String label, long ecBytes, long perVectorBytes, long actualFb)
    {
        long expected = ecBytes - perVectorBytes - PQVECTORS_EXTRA_HEADER_BYTES;
        assertEquals("FB-FusedPQ " + label + " must be exactly N×m + PQVECTORS_EXTRA_HEADER_BYTES smaller than EC " +
                     "(codebook only vs full PQVectors)",
                     expected, actualFb);
    }

    private void verifyFusedPQSearchWorks()
    {
        int limit = 10;
        var results = execute("SELECT pk FROM %s ORDER BY v ANN OF ? LIMIT ?", randomVectorBoxed(DIMENSION), limit);
        assertEquals("ANN search must return " + limit + " results on FB-FusedPQ compacted index",
                     limit, results.size());
    }

    private long componentSize(IndexContext indexContext,
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
