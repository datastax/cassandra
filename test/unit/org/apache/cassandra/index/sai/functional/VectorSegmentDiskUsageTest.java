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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.index.sai.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v5.V5VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/**
 * Reproduces the disk-usage difference between the first on-heap vector segment and a subsequent
 * off-heap segment. The first segment has no reusable PQ and therefore uses CassandraOnHeapGraph.
 * It trains a PQ, which SSTableIndexWriter then reuses to build the second segment with CompactionGraph.
 */
public class VectorSegmentDiskUsageTest extends VectorTester
{
    private static final int DIMENSION = 768;
    private static final int ROWS_PER_SEGMENT = 3_000;
    private static final int FIRST_SEGMENT_DISTINCT_VECTORS = 1_024;

    @After
    public void resetTestConfiguration()
    {
        SAIUtil.resetCurrentVersion();
        SAIUtil.setEnableNVQ(false);
//        SAIUtil.setEnableFused(false);
        SegmentBuilder.updateLastValidSegmentRowId(-1);
        V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 1.0;
    }

    @Test
    public void testOnHeapThenOffHeapSegmentDiskUsage()
    {
        // EC uses V5 postings while keeping full-resolution vectors inline. Disabling NVQ and FusedPQ
        // isolates the sparse-ordinal effect seen in production.
        SAIUtil.setCurrentVersion(Version.EC);
        SAIUtil.setEnableNVQ(false);
//        SAIUtil.setEnableFused(false);
        V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.01;

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, " + DIMENSION + ">)");
        disableCompaction();

        Vector<Float> repeatedVector = randomVectorBoxed(DIMENSION);

        // SSTables are ordered by decorated key rather than insertion order. Assign vectors in that
        // order so each forced segment receives the intended duplicate/distinct-vector population.
        List<Integer> primaryKeys = new ArrayList<>(2 * ROWS_PER_SEGMENT);
        for (int pk = 0; pk < 2 * ROWS_PER_SEGMENT; pk++)
            primaryKeys.add(pk);
        var partitioner = getCurrentColumnFamilyStore().getPartitioner();
        primaryKeys.sort(Comparator.comparing((Integer pk) ->
                                              partitioner.decorateKey(Int32Type.instance.decompose(pk))));

        // Build one 3000-vector population and write it identically to both segments: 1977 copies
        // of repeatedVector followed by 1023 other vectors. This gives exactly 1024 distinct vectors,
        // the minimum needed to train the PQ that enables the off-heap path for segment 2. Ending
        // with distinct vectors also keeps maxOrdinal at 2999 after duplicate rows create holes.
        int repeatedRows = ROWS_PER_SEGMENT - FIRST_SEGMENT_DISTINCT_VECTORS + 1;
        List<Vector<Float>> segmentVectors = new ArrayList<>(ROWS_PER_SEGMENT);
        for (int ordinal = 0; ordinal < ROWS_PER_SEGMENT; ordinal++)
        {
            Vector<Float> value = ordinal < repeatedRows ? repeatedVector : randomVectorBoxed(DIMENSION);
            segmentVectors.add(value);
        }

        for (int ordinal = 0; ordinal < 2 * ROWS_PER_SEGMENT; ordinal++)
        {
            Vector<Float> value = segmentVectors.get(ordinal % ROWS_PER_SEGMENT);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", primaryKeys.get(ordinal), value);
        }

        // Flush without an index, then build the index over the SSTable. Normal memtable flush uses a
        // single CassandraOnHeapGraph directly; SSTableIndexWriter segmentation is exercised by this
        // initial index build (and by compaction in production).
        flush();
        // An incoming row whose segment-local row ID is 3000 flushes the first 3000 rows.
        SegmentBuilder.updateLastValidSegmentRowId(ROWS_PER_SEGMENT - 1L);
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        StorageAttachedIndex sai = (StorageAttachedIndex) getCurrentColumnFamilyStore()
                                                           .indexManager
                                                           .getIndexByName(indexName);
        assertThat(sai).as("Index not found: " + indexName).isNotNull();

        IndexContext indexContext = sai.getIndexContext();
        Collection<SSTableIndex> indexes = indexContext.getView().getIndexes();
        assertThat(indexes).as("Expected one SSTable index").hasSize(1);

        List<Segment> segments = indexes.iterator().next().getSegments();
        assertThat(segments).as("Expected exactly two segments").hasSize(2);

        SegmentMeasurement onHeap = measure(segments.get(0));
        SegmentMeasurement offHeap = measure(segments.get(1));

        assertThat(onHeap.rows).as("on-heap rows").isEqualTo((long) ROWS_PER_SEGMENT);
        assertThat(offHeap.rows).as("off-heap rows").isEqualTo((long) ROWS_PER_SEGMENT);
        assertThat(onHeap.rowIdOffset).as("on-heap rowIdOffset").isEqualTo(0L);
        assertThat(offHeap.rowIdOffset).as("off-heap rowIdOffset").isEqualTo((long) ROWS_PER_SEGMENT);
        assertThat(onHeap.graphNodes).as("on-heap graphNodes").isEqualTo(FIRST_SEGMENT_DISTINCT_VECTORS);
        assertThat(offHeap.graphNodes).as("off-heap graphNodes").isEqualTo(FIRST_SEGMENT_DISTINCT_VECTORS);
        assertThat(onHeap.postingsStructure).as("on-heap postingsStructure")
                                            .isEqualTo(V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY);
        assertThat(offHeap.postingsStructure).as("off-heap postingsStructure")
                                             .isEqualTo(V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY);

        logMeasurement("segment 1 / on-heap / dense ordinals", onHeap);
        logMeasurement("segment 2 / off-heap / sparse ordinals", offHeap);

        double termsRatio = (double) offHeap.termsDataBytes / onHeap.termsDataBytes;
        double totalRatio = (double) offHeap.totalBytes / onHeap.totalBytes;
        logger.info("Off-heap/on-heap size ratio: TERMS_DATA={}x, all segment components={}x",
                    String.format("%.3f", termsRatio), String.format("%.3f", totalRatio));

        // After the bug fix both segments hold the same number of distinct vectors and use dense
        // ordinal mapping, so their sizes should be within 20% of each other.
        double tolerance = 0.20;
        assertThat(termsRatio).as("TERMS_DATA size ratio (off-heap / on-heap)")
                              .isCloseTo(1.0, offset(tolerance));
        assertThat(totalRatio).as("total segment size ratio (off-heap / on-heap)")
                              .isCloseTo(1.0, offset(tolerance));
    }

    private static SegmentMeasurement measure(Segment segment)
    {
        V2VectorIndexSearcher searcher = (V2VectorIndexSearcher) segment.getIndexSearcher();
        V5VectorIndexSearcher v5Searcher = (V5VectorIndexSearcher) searcher;

        return new SegmentMeasurement(segment.metadata.numRows,
                                      segment.metadata.segmentRowIdOffset,
                                      searcher.graph.size(),
                                      v5Searcher.getPostingsStructure(),
                                      componentLength(segment, IndexComponentType.TERMS_DATA),
                                      componentLength(segment, IndexComponentType.PQ),
                                      componentLength(segment, IndexComponentType.POSTING_LISTS),
                                      (long) segment.metadata.componentMetadatas.indexSize());
    }

    private static long componentLength(Segment segment, IndexComponentType component)
    {
        return segment.metadata.componentMetadatas.get(component).length;
    }

    private static void logMeasurement(String label, SegmentMeasurement measurement)
    {
        logger.info("{}: rows={}, graphNodes={}, structure={}, TERMS_DATA={} bytes, PQ={} bytes, " +
                    "POSTING_LISTS={} bytes, total={} bytes, totalBytesPerRow={}",
                    label,
                    measurement.rows,
                    measurement.graphNodes,
                    measurement.postingsStructure,
                    measurement.termsDataBytes,
                    measurement.pqBytes,
                    measurement.postingListsBytes,
                    measurement.totalBytes,
                    String.format("%.1f", (double) measurement.totalBytes / measurement.rows));
    }

    private static class SegmentMeasurement
    {
        final long rows;
        final long rowIdOffset;
        final int graphNodes;
        final V5VectorPostingsWriter.Structure postingsStructure;
        final long termsDataBytes;
        final long pqBytes;
        final long postingListsBytes;
        final long totalBytes;

        private SegmentMeasurement(long rows,
                                   long rowIdOffset,
                                   int graphNodes,
                                   V5VectorPostingsWriter.Structure postingsStructure,
                                   long termsDataBytes,
                                   long pqBytes,
                                   long postingListsBytes,
                                   long totalBytes)
        {
            this.rows = rows;
            this.rowIdOffset = rowIdOffset;
            this.graphNodes = graphNodes;
            this.postingsStructure = postingsStructure;
            this.termsDataBytes = termsDataBytes;
            this.pqBytes = pqBytes;
            this.postingListsBytes = postingListsBytes;
            this.totalBytes = totalBytes;
        }
    }
}
