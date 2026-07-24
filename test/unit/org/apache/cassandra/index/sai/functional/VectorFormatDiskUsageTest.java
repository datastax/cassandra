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

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;

import static org.junit.Assert.*;

/// Verifies on-disk vector index size behavior across SAI format versions,
/// both preCompaction and postCompaction.
public class VectorFormatDiskUsageTest extends VectorTester
{
    private static final int DIMENSION = 128;

    /// Number of flushes before compaction. Each flush produces one SSTable. With only
    /// [CassandraOnHeapGraph#MIN_PQ_ROWS] rows per flush the memory limit is not reached,
    /// so each SSTable contains exactly one segment — asserted in [#measureDiskUsage].
    private static final int NUM_FLUSHES = 2;

    /// TERMS_DATA delta from EC (jvector format 4) to FB (jvector format 6) per segment:
    /// one extra header copy + FOOTER_SIZE trailer. Independent of dimensions, number of vectors, etc.
    ///
    /// EC header  = (CommonHeader=288) + (feature bitmask int=4)                    = 292 bytes
    /// FB header  = (CommonHeader=288) + (features.size() int=4) + (ordinal int=4)  = 296 bytes
    /// FB writes: start-header + footer-header + FOOTER_SIZE(=Long+Int=12)          = 604 bytes
    /// Δ = 604 − 292 = 312 bytes per segment
    private static final long EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT = 312L;

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
        DiskMeasurement ecPreCompaction  = measureDiskUsage(Version.EC, "EC-preCompaction",  false);
        DiskMeasurement ecPostCompaction = measureDiskUsage(Version.EC, "EC-postCompaction", true);
        DiskMeasurement fbPreCompaction  = measureDiskUsage(Version.FB, "FB-preCompaction",  false);
        DiskMeasurement fbPostCompaction = measureDiskUsage(Version.FB, "FB-postCompaction", true);

        assertTrue("EC preCompaction index must have non-zero disk usage",  ecPreCompaction.totalBytes  > 0);
        assertTrue("FB preCompaction index must have non-zero disk usage",  fbPreCompaction.totalBytes  > 0);
        assertTrue("EC postCompaction index must have non-zero disk usage", ecPostCompaction.totalBytes > 0);
        assertTrue("FB postCompaction index must have non-zero disk usage", fbPostCompaction.totalBytes > 0);

        long preCompactionTermsDataDelta  = fbPreCompaction.termsDataBytes  - ecPreCompaction.termsDataBytes;
        long postCompactionTermsDataDelta = fbPostCompaction.termsDataBytes - ecPostCompaction.termsDataBytes;

        double diskGrowthPercentPreCompaction  = 100.0 * (fbPreCompaction.totalBytes  - ecPreCompaction.totalBytes)  / ecPreCompaction.totalBytes;
        double diskGrowthPercentPostCompaction = 100.0 * (fbPostCompaction.totalBytes - ecPostCompaction.totalBytes) / ecPostCompaction.totalBytes;

        logger.debug("  EC preCompaction  diskUsage() : {} ({} segments)", ecPreCompaction.totalBytes,  ecPreCompaction.segmentCount);
        logger.debug("  FB preCompaction  diskUsage() : {} ({} segments)", fbPreCompaction.totalBytes,  fbPreCompaction.segmentCount);
        logger.debug("  EC postCompaction diskUsage() : {} ({} segments)", ecPostCompaction.totalBytes, ecPostCompaction.segmentCount);
        logger.debug("  FB postCompaction diskUsage() : {} ({} segments)", fbPostCompaction.totalBytes, fbPostCompaction.segmentCount);
        logger.debug("  Total disk usage growth preCompaction  : +{} bytes ({} %)",
                    fbPreCompaction.totalBytes  - ecPreCompaction.totalBytes,  String.format("%.4f", diskGrowthPercentPreCompaction));
        logger.debug("  Total disk usage growth postCompaction : +{} bytes ({} %)",
                    fbPostCompaction.totalBytes - ecPostCompaction.totalBytes, String.format("%.4f", diskGrowthPercentPostCompaction));
        logger.debug("  TERMS_DATA delta preCompaction  : {} (expected {} × {} = {})",
                    preCompactionTermsDataDelta,  NUM_FLUSHES, EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT,
                    NUM_FLUSHES * EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT);
        logger.debug("  TERMS_DATA delta postCompaction : {} (expected {})",
                    postCompactionTermsDataDelta, EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT);

        verifyComponentAccounting(ecPreCompaction,  fbPreCompaction,  "preCompaction");
        verifyComponentAccounting(ecPostCompaction, fbPostCompaction, "postCompaction");
        verifyTermsDataDelta(preCompactionTermsDataDelta,  ecPreCompaction.segmentCount,  "preCompaction");
        verifyTermsDataDelta(postCompactionTermsDataDelta, ecPostCompaction.segmentCount, "postCompaction");
        verifyUnchangedComponents(ecPreCompaction,  fbPreCompaction,  "preCompaction");
        verifyUnchangedComponents(ecPostCompaction, fbPostCompaction, "postCompaction");
        verifyConservation(ecPreCompaction,  fbPreCompaction,  preCompactionTermsDataDelta,  "preCompaction");
        verifyConservation(ecPostCompaction, fbPostCompaction, postCompactionTermsDataDelta, "postCompaction");
        verifyTotalDiskGrowthUnder5Percent(diskGrowthPercentPreCompaction,  "preCompaction");
        verifyTotalDiskGrowthUnder5Percent(diskGrowthPercentPostCompaction, "postCompaction");
    }

    /// Regression guard: for every vector-capable version from [Version#JVECTOR_EARLIEST] (CA)
    /// up to but not including [Version#LATEST], asserts that [Version#LATEST] uses less than
    /// 5% more disk than that older version. Catches accidental large regressions introduced
    /// by a new format version.
    @Test
    public void testDiskGrowthAcrossVersions()
    {
        DiskMeasurement latestPreCompaction  = measureDiskUsage(Version.LATEST, Version.LATEST + "-preCompaction",  false);
        DiskMeasurement latestPostCompaction = measureDiskUsage(Version.LATEST, Version.LATEST + "-postCompaction", true);

        Version.ALL.stream()
                   .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST) && !v.equals(Version.LATEST))
                   .forEach(older ->
        {
            DiskMeasurement olderPreCompaction  = measureDiskUsage(older, older + "-preCompaction",  false);
            DiskMeasurement olderPostCompaction = measureDiskUsage(older, older + "-postCompaction", true);

            double diskGrowthPercentPreCompaction  = 100.0 * (latestPreCompaction.totalBytes  - olderPreCompaction.totalBytes)  / olderPreCompaction.totalBytes;
            double diskGrowthPercentPostCompaction = 100.0 * (latestPostCompaction.totalBytes - olderPostCompaction.totalBytes) / olderPostCompaction.totalBytes;

            logger.debug("  {} → {} preCompaction  : {} → {} bytes ({} %)",
                         older, Version.LATEST, olderPreCompaction.totalBytes,  latestPreCompaction.totalBytes,
                         String.format("%.4f", diskGrowthPercentPreCompaction));
            logger.debug("  {} → {} postCompaction : {} → {} bytes ({} %)",
                         older, Version.LATEST, olderPostCompaction.totalBytes, latestPostCompaction.totalBytes,
                         String.format("%.4f", diskGrowthPercentPostCompaction));

            verifyTotalDiskGrowthUnder5Percent(diskGrowthPercentPreCompaction,  older + " → " + Version.LATEST + " preCompaction");
            verifyTotalDiskGrowthUnder5Percent(diskGrowthPercentPostCompaction, older + " → " + Version.LATEST + " postCompaction");
        });
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
        assertEquals("FB.totalBytes must equal ec.totalBytes + TERMS_DATA delta + 8 × segmentCount"
                     + " (" + phase + ')',
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

    /// Snapshot of per-component file sizes and segment count for one index build.
    private static class DiskMeasurement
    {
        final long totalBytes;
        final long termsDataBytes;
        final long pqBytes;
        final long metaBytes;
        final long postingListsBytes;
        final long completionMarkerBytes;
        final int  segmentCount; // total segments across all SSTables

        private DiskMeasurement(Builder b)
        {
            this.totalBytes            = b.totalBytes;
            this.termsDataBytes        = b.termsDataBytes;
            this.pqBytes               = b.pqBytes;
            this.metaBytes             = b.metaBytes;
            this.postingListsBytes     = b.postingListsBytes;
            this.completionMarkerBytes = b.completionMarkerBytes;
            this.segmentCount          = b.segmentCount;
        }

        static class Builder
        {
            long totalBytes;
            long termsDataBytes;
            long pqBytes;
            long metaBytes;
            long postingListsBytes;
            long completionMarkerBytes;
            int  segmentCount;

            Builder totalBytes(long v)            { this.totalBytes            = v; return this; }
            Builder termsDataBytes(long v)        { this.termsDataBytes        = v; return this; }
            Builder pqBytes(long v)               { this.pqBytes               = v; return this; }
            Builder metaBytes(long v)             { this.metaBytes             = v; return this; }
            Builder postingListsBytes(long v)     { this.postingListsBytes     = v; return this; }
            Builder completionMarkerBytes(long v) { this.completionMarkerBytes = v; return this; }
            Builder segmentCount(int v)           { this.segmentCount          = v; return this; }

            DiskMeasurement build()               { return new DiskMeasurement(this); }
        }
    }

    /// Builds a fresh table at `version`, writes [#NUM_FLUSHES] × [CassandraOnHeapGraph#MIN_PQ_ROWS]
    /// vectors in separate flushes, then either returns the pre-compaction measurement
    /// (`compact == false`) or runs major compaction first (`compact == true`).
    ///
    /// Each flush produces one SSTable with one segment (one graph in TERMS_DATA), so
    /// pre-compaction there are [#NUM_FLUSHES] segments across [#NUM_FLUSHES] SSTables;
    /// post-compaction there is exactly 1 segment in 1 SSTable.
    private DiskMeasurement measureDiskUsage(Version version, String label, boolean compact)
    {
        SAIUtil.setCurrentVersion(version);
        createTable("CREATE TABLE %s (pk int, v vector<float, " + DIMENSION + ">, PRIMARY KEY(pk))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int flush = 0; flush < NUM_FLUSHES; flush++)
        {
            for (int i = 0; i < CassandraOnHeapGraph.MIN_PQ_ROWS; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)",
                        flush * CassandraOnHeapGraph.MIN_PQ_ROWS + i, randomVectorBoxed(DIMENSION));
            flush();
        }

        if (compact)
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

        List<SSTableIndex> sstableIndexes = List.copyOf(indexContext.getView().getIndexes());
        int totalSegments = sstableIndexes.stream().mapToInt(s -> s.getSegments().size()).sum();
        int expectedSegments = compact ? 1 : NUM_FLUSHES;
        assertEquals("Expected " + expectedSegments + " segment(s) " + label,
                     expectedSegments, totalSegments);

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
