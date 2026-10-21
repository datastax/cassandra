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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.IntUnaryOperator;
import java.util.stream.StreamSupport;

import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseBits;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.vector.RamAwareVectorValues;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class V5OnDiskOrdinalsMapTest extends VectorTester
{
    @Before
    public void setup() throws Throwable
    {
        super.setup();
        // this can be removed once LATEST is >= DC
        SAIUtil.setCurrentVersion(Version.DC);
    }

    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final int DIMENSION = 2; // not relevant to postings mapping, nothing gained by using something larger

    @Test
    public void testRandomPostings() throws Exception
    {
        for (int i = 0; i < 10; i++) {
            // ONE_TO_MANY is no longer written; only test the two valid write-path structures
            for (Structure structure: new Structure[]{ Structure.ONE_TO_ONE, Structure.ZERO_OR_ONE_TO_MANY }) {
                testRandomPostingsOnce(structure);
            }
        }
    }

    @Test
    public void testOneToOne() throws Exception
    {
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> C
        // 1 -> A
        // 2 -> B
        postingsMap.put(vv.getVector(0), createPostings(0, "C"));
        postingsMap.put(vv.getVector(1), createPostings(1, "A"));
        postingsMap.put(vv.getVector(2), createPostings(2, "B"));
        computeRowIds(postingsMap);
        validate(Structure.ONE_TO_ONE, postingsMap, vv);
    }

    @Test
    public void testOneToMany() throws Exception
    {
        // ONE_TO_MANY is no longer written; duplicate vectors now produce ZERO_OR_ONE_TO_MANY
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> B
        // 1 -> C, A  (duplicate: produces ZERO_OR_ONE_TO_MANY)
        // 2 -> D
        postingsMap.put(vv.getVector(0), createPostings(0, "B"));
        postingsMap.put(vv.getVector(1), createPostings(1, "C", "A"));
        postingsMap.put(vv.getVector(2), createPostings(2, "D"));
        computeRowIds(postingsMap);
        validate(Structure.ZERO_OR_ONE_TO_MANY, postingsMap, vv);
    }

    @Test
    public void testOneToManyLarger() throws Exception
    {
        // ONE_TO_MANY is no longer written; duplicate vectors now produce ZERO_OR_ONE_TO_MANY
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> B
        // 1 -> C, A, E  (duplicates: produces ZERO_OR_ONE_TO_MANY)
        // 2 -> D, F
        // 3 -> G
        postingsMap.put(vv.getVector(0), createPostings(0, "B"));
        postingsMap.put(vv.getVector(1), createPostings(1, "C", "A", "E"));
        postingsMap.put(vv.getVector(2), createPostings(2, "D", "F"));
        postingsMap.put(vv.getVector(3), createPostings(3, "G"));
        computeRowIds(postingsMap);
        validate(Structure.ZERO_OR_ONE_TO_MANY, postingsMap, vv);
    }

    @Test
    public void testGeneric() throws Exception
    {
        var vv = generateVectors(4);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        // 0 -> B
        // 1 -> C, A
        // 2 -> E    (no D, this makes it not 1:many)
        postingsMap.put(vv.getVector(0), createPostings(0, "B"));
        postingsMap.put(vv.getVector(1), createPostings(1, "C", "A"));
        postingsMap.put(vv.getVector(2), createPostings(2, "E"));
        computeRowIds(postingsMap);
        validate(Structure.ZERO_OR_ONE_TO_MANY, postingsMap, vv);
    }

    @Test
    public void testDeltaViewsMatchExpectedMapping() throws Exception
    {
        double originalValue = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.5;
            for (int iteration = 0; iteration < 20; iteration++)
            {
                // ordinals are assigned densely in order of first row; some rows have no vector, some repeat one
                int rows = 1 + randomInt(2000);
                var rowIdsByOrdinal = new java.util.ArrayList<IntArrayList>();
                int[] expectedOrdinal = new int[rows];
                for (int row = 0; row < rows; row++)
                {
                    int kind = randomInt(9);
                    if (kind == 0)
                    {
                        expectedOrdinal[row] = -1;
                    }
                    else if (kind == 1 && !rowIdsByOrdinal.isEmpty())
                    {
                        int ordinal = randomInt(rowIdsByOrdinal.size() - 1);
                        rowIdsByOrdinal.get(ordinal).add(row);
                        expectedOrdinal[row] = ordinal;
                    }
                    else
                    {
                        var list = new IntArrayList();
                        list.add(row);
                        expectedOrdinal[row] = rowIdsByOrdinal.size();
                        rowIdsByOrdinal.add(list);
                    }
                }
                if (rowIdsByOrdinal.isEmpty())
                    continue;

                File tempFile = createTempFile("testfile");
                var sw = new SequentialWriter(tempFile, SequentialWriterOption.newBuilder().finishOnClose(true).build());
                long length = V5VectorPostingsWriter.create(rowIdsByOrdinal.size() - 1, rowIdsByOrdinal::get)
                                                    .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                    .maxRowId(rows - 1)
                                                    .writeTo(sw);
                sw.close();

                try (FileHandle fh = new FileHandle.Builder(tempFile).withCompressionMetadata(null).complete())
                {
                    var odom = new V5OnDiskOrdinalsMap(fh, 0, length);
                    assertEquals(Structure.ZERO_OR_ONE_TO_MANY, odom.structure);
                    assertTrue("expected the in-memory delta views to be used", odom.cachedBytesUsed() > 0);

                    try (var ordinalsView = odom.getOrdinalsView();
                         var rowIdsView = odom.getRowIdsView())
                    {
                        for (int row = 0; row < rows; row++)
                            assertEquals(expectedOrdinal[row], ordinalsView.getOrdinalForRowId(row));
                        assertEquals(-1, ordinalsView.getOrdinalForRowId(rows));

                        for (int ordinal = 0; ordinal < rowIdsByOrdinal.size(); ordinal++)
                        {
                            var it = rowIdsView.getSegmentRowIdsMatching(ordinal);
                            var actual = new IntArrayList();
                            while (it.hasNext())
                                actual.add(it.nextInt());
                            assertEquals(rowIdsByOrdinal.get(ordinal), actual);
                        }

                        for (int i = 0; i < 10; i++)
                        {
                            int start = randomInt(rows);
                            int end = start + randomInt(rows);
                            var seen = new java.util.ArrayList<int[]>();
                            ordinalsView.forEachOrdinalInRange(start, end, (r, o) -> seen.add(new int[]{ r, o }));
                            var expected = new java.util.ArrayList<int[]>();
                            for (int row = start; row <= Math.min(end, rows - 1); row++)
                                if (expectedOrdinal[row] != -1)
                                    expected.add(new int[]{ row, expectedOrdinal[row] });
                            assertEquals(expected.size(), seen.size());
                            for (int j = 0; j < expected.size(); j++)
                                assertArrayEquals(expected.get(j), seen.get(j));
                        }
                    }
                    odom.close();
                }
            }
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalValue;
        }
    }

    @Test
    public void testEmpty() throws Exception
    {
        var vv = generateVectors(0);
        Map<VectorFloat<?>, VectorPostings<String>> postingsMap = emptyPostingsMap();
        validate(Structure.ONE_TO_ONE, postingsMap, vv);
    }

    @Test
    public void testTooManyOrdinalMappingHoles_WithZeroHolesAllowed()
    {
        double originalValue = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.0;

            // With 0% holes allowed, any holes should be too many
            assertEquals(false, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(10, 10)); // 0 holes
            assertEquals(true, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(10, 11));  // 1 hole
            assertEquals(true, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(10, 12));  // 2 holes
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalValue;
        }
    }

    @Test
    public void testTooManyOrdinalMappingHoles_WithSmallNumberOfRows()
    {
        double originalValue = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.1; // 10% holes allowed

            // With 2 rows and 1 vector, there is 1 hole (50% holes) - should be too many
            assertEquals(true, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(1, 2));

            // With 10 rows and 9 vectors, there is 1 hole (10% holes) - exactly at threshold
            assertEquals(false, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(9, 10));

            // With 10 rows and 8 vectors, there are 2 holes (20% holes) - over threshold
            assertEquals(true, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(8, 10));
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalValue;
        }
    }

    @Test
    public void testTooManyOrdinalMappingHoles_StandardCases()
    {
        double originalValue = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.1; // 10% holes allowed

            // 100 vectors, 110 rows = 10 holes = 10% holes (exactly at threshold)
            assertEquals(false, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(100, 110));

            // 100 vectors, 111 rows = 11 holes = ~9.9% holes (rounds down due to truncation)
            assertEquals(false, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(100, 111));

            // 100 vectors, 109 rows = 9 holes = 9% holes (under threshold)
            assertEquals(false, V5VectorPostingsWriter.tooManyOrdinalMappingHoles(100, 109));
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalValue;
        }
    }

    /**
     * Fast path correctness — write a ZERO_OR_ONE_TO_MANY segment with a known deterministic mix of
     * extra-rows (multi-row vectors) and missing-vector rows, open it, assert the fast (in-memory delta)
     * path was chosen via cachedBytesUsed() > 0, then exhaustively verify every getOrdinalForRowId and
     * getSegmentRowIdsMatching result.
     *
     * Layout (rowId → ordinal):
     *   row 0 → ordinal 0   (implicit, normal)
     *   row 1 → ordinal 1   (implicit, normal)
     *   row 2 → ordinal 2   (implicit, normal)
     *   row 3 → ordinal 0   (extra-row: shares ordinal 0, first discontinuity)
     *   row 4 → ordinal 3   (implicit after delta=1)
     *   row 5 → ordinal 4   (implicit after delta=1)
     *   row 6 → -1          (missing-vector, second discontinuity)
     *   row 7 → ordinal 5   (implicit after delta=2)
     *
     * maxRowId=7, maxOrdinal=5, discontinuities=2 → ratio 2/8 = 0.25 ≤ default GLOBAL_HOLES_ALLOWED
     */
    @Test
    public void testFastPathCorrectness_KnownMixedDiscontinuities() throws Exception
    {
        // rowIdsByOrdinal: ordinal → list of rowIds (ascending)
        // ordinal 0: [0, 3]  (extra-row)
        // ordinal 1: [1]
        // ordinal 2: [2]
        // ordinal 3: [4]
        // ordinal 4: [5]
        // ordinal 5: [7]     (row 6 is a missing-vector gap)
        var rowIdsByOrdinal = new java.util.ArrayList<IntArrayList>();
        for (int o = 0; o < 6; o++)
            rowIdsByOrdinal.add(new IntArrayList());
        rowIdsByOrdinal.get(0).add(0); rowIdsByOrdinal.get(0).add(3);
        rowIdsByOrdinal.get(1).add(1);
        rowIdsByOrdinal.get(2).add(2);
        rowIdsByOrdinal.get(3).add(4);
        rowIdsByOrdinal.get(4).add(5);
        rowIdsByOrdinal.get(5).add(7);

        File tempFile = createTempFile("testFastPath");
        var sw = new SequentialWriter(tempFile, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        long length = V5VectorPostingsWriter.create(5, rowIdsByOrdinal::get)
                                            .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                            .maxRowId(7)
                                            .writeTo(sw);
        sw.close();

        try (FileHandle fh = new FileHandle.Builder(tempFile).withCompressionMetadata(null).complete())
        {
            var odom = new V5OnDiskOrdinalsMap(fh, 0, length);
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, odom.structure);
            assertTrue("expected fast (delta) path to be chosen: cachedBytesUsed() should be > 0",
                       odom.cachedBytesUsed() > 0);

            try (var ordinalsView = odom.getOrdinalsView();
                 var rowIdsView = odom.getRowIdsView())
            {
                // ---- forward: rowId → ordinal ----
                assertEquals(0,  ordinalsView.getOrdinalForRowId(0));
                assertEquals(1,  ordinalsView.getOrdinalForRowId(1));
                assertEquals(2,  ordinalsView.getOrdinalForRowId(2));
                assertEquals(0,  ordinalsView.getOrdinalForRowId(3));  // extra-row
                assertEquals(3,  ordinalsView.getOrdinalForRowId(4));
                assertEquals(4,  ordinalsView.getOrdinalForRowId(5));
                assertEquals(-1, ordinalsView.getOrdinalForRowId(6)); // missing-vector
                assertEquals(5,  ordinalsView.getOrdinalForRowId(7));
                assertEquals(-1, ordinalsView.getOrdinalForRowId(8)); // out-of-bounds

                // ---- reverse: ordinal → rowIds ----
                // ordinal 0: rows 0 and 3 (sorted ascending)
                var it0 = rowIdsView.getSegmentRowIdsMatching(0);
                var list0 = new IntArrayList();
                while (it0.hasNext()) list0.add(it0.nextInt());
                assertArrayEquals(new int[]{ 0, 3 }, list0.toIntArray());

                // ordinals 1-4: single row each
                for (int ordinal = 1; ordinal <= 4; ordinal++)
                {
                    int expectedRow = ordinal; // rows 1-5 map 1:1 except row 3 which is an extra-row
                    if (ordinal == 3) expectedRow = 4;
                    if (ordinal == 4) expectedRow = 5;
                    var it = rowIdsView.getSegmentRowIdsMatching(ordinal);
                    assertTrue("ordinal " + ordinal + " should have a row", it.hasNext());
                    assertEquals(expectedRow, it.nextInt());
                    assertTrue("ordinal " + ordinal + " should have exactly one row", !it.hasNext());
                }

                // ordinal 5: row 7
                var it5 = rowIdsView.getSegmentRowIdsMatching(5);
                assertTrue(it5.hasNext());
                assertEquals(7, it5.nextInt());
                assertTrue(!it5.hasNext());
            }

            odom.close();
        }
    }

    /**
     * Fallback to generic (disk-based) path — write a ZERO_OR_ONE_TO_MANY segment whose hole density
     * exceeds GLOBAL_HOLES_ALLOWED, assert that cachedBytesUsed() == 0 (generic path chosen).
     *
     * With GLOBAL_HOLES_ALLOWED forced to 0.0, ANY discontinuity causes fallback.
     */
    @Test
    public void testFallbackToGenericPath_WhenHoleDensityExceedsThreshold() throws Exception
    {
        // ordinal 0 → row 0 (single row, no extras)
        // ordinal 1 → row 2 (row 1 is a gap → 1 discontinuity)
        // Even a single gap will exceed ratio 0.0.
        var rowIdsByOrdinal = new java.util.ArrayList<IntArrayList>();
        var row0 = new IntArrayList(); row0.add(0); rowIdsByOrdinal.add(row0);
        var row2 = new IntArrayList(); row2.add(2); rowIdsByOrdinal.add(row2);

        double originalValue = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.0;

            File tempFile = createTempFile("testFallback");
            var sw = new SequentialWriter(tempFile, SequentialWriterOption.newBuilder().finishOnClose(true).build());
            long length = V5VectorPostingsWriter.create(1, rowIdsByOrdinal::get)
                                                .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                .maxRowId(2)
                                                .writeTo(sw);
            sw.close();

            try (FileHandle fh = new FileHandle.Builder(tempFile).withCompressionMetadata(null).complete())
            {
                var odom = new V5OnDiskOrdinalsMap(fh, 0, length);
                assertEquals(Structure.ZERO_OR_ONE_TO_MANY, odom.structure);
                assertEquals("generic (disk) path should be chosen when hole density exceeds threshold",
                             0, odom.cachedBytesUsed());

                // Sanity: lookups still work (via the generic path)
                try (var ordinalsView = odom.getOrdinalsView();
                     var rowIdsView = odom.getRowIdsView())
                {
                    assertEquals(0,  ordinalsView.getOrdinalForRowId(0));
                    assertEquals(-1, ordinalsView.getOrdinalForRowId(1)); // gap
                    assertEquals(1,  ordinalsView.getOrdinalForRowId(2));

                    var it0 = rowIdsView.getSegmentRowIdsMatching(0);
                    assertTrue(it0.hasNext()); assertEquals(0, it0.nextInt()); assertTrue(!it0.hasNext());
                    var it1 = rowIdsView.getSegmentRowIdsMatching(1);
                    assertTrue(it1.hasNext()); assertEquals(2, it1.nextInt()); assertTrue(!it1.hasNext());
                }
                odom.close();
            }
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalValue;
        }
    }

    /**
     * Backwards compatibility — write a ONE_TO_MANY segment directly via V5VectorPostingsWriter
     * (the format that newer write paths no longer produce) and verify that V5OnDiskOrdinalsMap
     * reads it back correctly.
     *
     * Layout: ordinal 0 → rows [0, 2] (extra-row), ordinal 1 → row [1], ordinal 2 → row [3]
     */
    @Test
    public void testBackwardsCompat_OneToManyReadPath() throws Exception
    {
        // ONE_TO_MANY invariant: min(rowIds for ordinal i) == i (the primary rowId equals the ordinal).
        // Layout: ordinal 0 → rows [0] (primary only), ordinal 1 → rows [1, 3] (extra row 3), ordinal 2 → row [2].
        // All primaries equal their ordinal; maxRowId must be 3 to cover the extra row.
        var rowIdsByOrdinal = new java.util.ArrayList<IntArrayList>();
        var o0 = new IntArrayList(); o0.add(0);            rowIdsByOrdinal.add(o0); // ordinal 0: row 0
        var o1 = new IntArrayList(); o1.add(1); o1.add(3); rowIdsByOrdinal.add(o1); // ordinal 1: rows 1,3
        var o2 = new IntArrayList(); o2.add(2);            rowIdsByOrdinal.add(o2); // ordinal 2: row 2

        File tempFile = createTempFile("testOneToManyBackwards");
        var sw = new SequentialWriter(tempFile, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        // maxRowId must be 3 (the extra row) — must not be less than maxVectorOrdinal=2.
        long length = V5VectorPostingsWriter.create(2, rowIdsByOrdinal::get)
                                            .structure(Structure.ONE_TO_MANY)
                                            .maxRowId(3)
                                            .writeTo(sw);
        sw.close();

        try (FileHandle fh = new FileHandle.Builder(tempFile).withCompressionMetadata(null).complete())
        {
            var odom = new V5OnDiskOrdinalsMap(fh, 0, length);
            assertEquals("should be read back as ONE_TO_MANY", Structure.ONE_TO_MANY, odom.structure);

            try (var ordinalsView = odom.getOrdinalsView();
                 var rowIdsView = odom.getRowIdsView())
            {
                // forward: rowId → ordinal
                assertEquals(0, ordinalsView.getOrdinalForRowId(0));
                assertEquals(1, ordinalsView.getOrdinalForRowId(1));
                assertEquals(2, ordinalsView.getOrdinalForRowId(2));
                assertEquals(1, ordinalsView.getOrdinalForRowId(3)); // extra-row for ordinal 1

                // reverse: ordinal → rowIds
                var it0 = rowIdsView.getSegmentRowIdsMatching(0);
                assertTrue(it0.hasNext()); assertEquals(0, it0.nextInt()); assertTrue(!it0.hasNext());

                // ordinal 1 → rows [1, 3]
                var it1 = rowIdsView.getSegmentRowIdsMatching(1);
                var list1 = new IntArrayList();
                while (it1.hasNext()) list1.add(it1.nextInt());
                assertArrayEquals(new int[]{ 1, 3 }, list1.toIntArray());

                var it2 = rowIdsView.getSegmentRowIdsMatching(2);
                assertTrue(it2.hasNext()); assertEquals(2, it2.nextInt()); assertTrue(!it2.hasNext());
            }
            odom.close();
        }
    }

    /**
     * forEachOrdinalInRange on the delta view must return exactly the same (rowId, ordinal) pairs
     * as calling getOrdinalForRowId in a loop over the same range, skipping missing-vector rows.
     *
     * Uses the same deterministic 8-row layout as testFastPathCorrectness_KnownMixedDiscontinuities,
     * and also exercises several sub-ranges including range boundaries at discontinuity positions.
     */
    @Test
    public void testForEachOrdinalInRange_MatchesLoopOnDeltaView() throws Exception
    {
        // Same layout as testFastPathCorrectness:
        // row 0→0, 1→1, 2→2, 3→0(extra), 4→3, 5→4, 6→-1(gap), 7→5
        var rowIdsByOrdinal = new java.util.ArrayList<IntArrayList>();
        for (int o = 0; o < 6; o++) rowIdsByOrdinal.add(new IntArrayList());
        rowIdsByOrdinal.get(0).add(0); rowIdsByOrdinal.get(0).add(3);
        rowIdsByOrdinal.get(1).add(1);
        rowIdsByOrdinal.get(2).add(2);
        rowIdsByOrdinal.get(3).add(4);
        rowIdsByOrdinal.get(4).add(5);
        rowIdsByOrdinal.get(5).add(7);

        File tempFile = createTempFile("testForEach");
        var sw = new SequentialWriter(tempFile, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        long length = V5VectorPostingsWriter.create(5, rowIdsByOrdinal::get)
                                            .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                            .maxRowId(7)
                                            .writeTo(sw);
        sw.close();

        try (FileHandle fh = new FileHandle.Builder(tempFile).withCompressionMetadata(null).complete())
        {
            var odom = new V5OnDiskOrdinalsMap(fh, 0, length);
            assertTrue("delta view must be active for this test", odom.cachedBytesUsed() > 0);

            // Test several ranges including ones that straddle discontinuities
            int[][] ranges = {
                { 0, 7 },  // full range
                { 0, 0 },  // single row
                { 3, 3 },  // extra-row only
                { 6, 6 },  // missing-vector row only
                { 5, 7 },  // crosses the gap at row 6
                { 2, 5 },  // crosses the extra-row at row 3
                { 0, 3 },  // up to and including the extra-row
                { 4, 8 },  // past the end (row 8 doesn't exist)
            };

            try (var ordinalsView = odom.getOrdinalsView())
            {
                for (int[] range : ranges)
                {
                    int start = range[0];
                    int end = range[1];

                    // expected: loop over [start, min(end, 7)] skipping -1
                    var expected = new java.util.ArrayList<int[]>();
                    for (int row = start; row <= Math.min(end, 7); row++)
                    {
                        int ordinal = ordinalsView.getOrdinalForRowId(row);
                        if (ordinal != -1)
                            expected.add(new int[]{ row, ordinal });
                    }

                    // actual: forEachOrdinalInRange
                    var actual = new java.util.ArrayList<int[]>();
                    ordinalsView.forEachOrdinalInRange(start, end, (r, o) -> actual.add(new int[]{ r, o }));

                    assertEquals("range [" + start + "," + end + "] size mismatch", expected.size(), actual.size());
                    for (int j = 0; j < expected.size(); j++)
                        assertArrayEquals("range [" + start + "," + end + "] entry " + j,
                                          expected.get(j), actual.get(j));
                }
            }

            odom.close();
        }
    }

    /**
     * Writes a ZERO_OR_ONE_TO_MANY segment for the given rowId -> ordinal table (-1 = no vector), opens it and
     * verifies every lookup against the table: getOrdinalForRowId, getSegmentRowIdsMatching (rowIds ascending),
     * and forEachOrdinalInRange over the full range and random sub-ranges.
     *
     * @param expectFast TRUE: the in-memory delta views must be used; FALSE: the generic disk path must be used;
     *                   null: no expectation, only correctness is checked
     */
    private void checkMapping(String name, int[] ordinalOfRow, Boolean expectFast) throws Exception
    {
        int maxRowId = ordinalOfRow.length - 1;
        int maxOrdinal = Arrays.stream(ordinalOfRow).max().getAsInt();
        var rowIdsByOrdinal = new java.util.ArrayList<IntArrayList>();
        for (int o = 0; o <= maxOrdinal; o++)
            rowIdsByOrdinal.add(new IntArrayList());
        for (int r = 0; r <= maxRowId; r++)
            if (ordinalOfRow[r] >= 0)
                rowIdsByOrdinal.get(ordinalOfRow[r]).add(r);

        double originalValue = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED;
        try
        {
            // never fall back because of hole density; we are testing the shape of the mapping
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 1.0;

            File tempFile = createTempFile(name);
            var sw = new SequentialWriter(tempFile, SequentialWriterOption.newBuilder().finishOnClose(true).build());
            long length = V5VectorPostingsWriter.create(maxOrdinal, rowIdsByOrdinal::get)
                                                .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                .maxRowId(maxRowId)
                                                .writeTo(sw);
            sw.close();

            try (FileHandle fh = new FileHandle.Builder(tempFile).withCompressionMetadata(null).complete())
            {
                var odom = new V5OnDiskOrdinalsMap(fh, 0, length);
                if (expectFast != null)
                    assertEquals(name + ": unexpected path chosen: " + Arrays.toString(ordinalOfRow),
                                 expectFast, odom.cachedBytesUsed() > 0);

                try (var ordinalsView = odom.getOrdinalsView();
                     var rowIdsView = odom.getRowIdsView())
                {
                    for (int r = 0; r <= maxRowId; r++)
                        assertEquals(name + ": rowId " + r + " in " + Arrays.toString(ordinalOfRow),
                                     ordinalOfRow[r], ordinalsView.getOrdinalForRowId(r));
                    assertEquals(-1, ordinalsView.getOrdinalForRowId(maxRowId + 1));

                    for (int o = 0; o <= maxOrdinal; o++)
                    {
                        var it = rowIdsView.getSegmentRowIdsMatching(o);
                        var actual = new IntArrayList();
                        while (it.hasNext())
                            actual.add(it.nextInt());
                        assertArrayEquals(name + ": ordinal " + o + " in " + Arrays.toString(ordinalOfRow),
                                          rowIdsByOrdinal.get(o).toIntArray(), actual.toIntArray());
                    }

                    var random = new java.util.Random(getRandom().nextIntBetween(0, Integer.MAX_VALUE));
                    for (int i = 0; i < 20; i++)
                    {
                        int start = i == 0 ? 0 : random.nextInt(maxRowId + 1);
                        int end = i == 0 ? maxRowId + 1 : start + random.nextInt(maxRowId + 2 - start);
                        var expected = new java.util.ArrayList<int[]>();
                        for (int r = start; r <= Math.min(end, maxRowId); r++)
                            if (ordinalOfRow[r] >= 0)
                                expected.add(new int[]{ r, ordinalOfRow[r] });
                        var actual = new java.util.ArrayList<int[]>();
                        ordinalsView.forEachOrdinalInRange(start, end, (r, o) -> actual.add(new int[]{ r, o }));
                        assertEquals(name + ": range [" + start + "," + end + "]", expected.size(), actual.size());
                        for (int j = 0; j < expected.size(); j++)
                            assertArrayEquals(name + ": range [" + start + "," + end + "] entry " + j,
                                              expected.get(j), actual.get(j));
                    }
                }
                odom.close();
            }
        }
        finally
        {
            V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = originalValue;
        }
    }

    /** Several consecutive discontinuities: the same would-be ordinal repeats in the reverse search. */
    @Test
    public void testFastPath_RunsOfConsecutiveDiscontinuities() throws Exception
    {
        // the example from the V5OnDiskOrdinalsMap.tryBuildDeltaIndex javadoc
        checkMapping("javadocExample", new int[]{ 0, 1, 2, 1, -1, 3, 4, 5, 0, 6 }, true);

        checkMapping("runs", new int[]{ 0, 1, -1, -1, -1, 2, 0, 0, 3, -1, 1, 4 }, true);
        // holes right at the start and the end
        checkMapping("edges", new int[]{ -1, -1, 0, 1, 0, -1 }, true);
        // only holes after the first row
        checkMapping("holesOnly", new int[]{ 0, -1, -1, -1 }, true);
    }

    /** Extra rows of different ordinals interleaved, several per ordinal: exercises holesByOrdinal grouping. */
    @Test
    public void testFastPath_MultipleExtraRowsInterleavedAcrossOrdinals() throws Exception
    {
        checkMapping("interleaved", new int[]{ 0, 1, 2, 1, 0, 1, 2, 3, 0, 3, -1, 4, 2 }, true);
        // everything after the first row is an extra row of ordinal 0
        checkMapping("allExtra", new int[]{ 0, 0, 0, 0, 0 }, true);
    }

    /** Ordinals not in first-appearance order cannot be encoded by the delta model: must fall back, still correct. */
    @Test
    public void testJumpAheadOrdinalsFallBackToGenericPath() throws Exception
    {
        checkMapping("swap", new int[]{ 1, 0 }, false);
        checkMapping("jumpMiddle", new int[]{ 0, 2, 1, 3 }, false);
        checkMapping("jumpWithHoles", new int[]{ 0, -1, 2, 1, 1, -1 }, false);
    }

    /** Random mappings where new ordinals first appear in increasing rowId order: the fast path must be used. */
    @Test
    public void testFastPath_RandomFirstAppearanceOrderMappings() throws Exception
    {
        var random = new java.util.Random(getRandom().nextIntBetween(0, Integer.MAX_VALUE));
        for (int iter = 0; iter < 200; iter++)
        {
            int rows = 1 + random.nextInt(200);
            double missingProb = random.nextDouble() * 0.4;
            double extraProb = random.nextDouble() * 0.4;
            int[] ordinalOfRow = new int[rows];
            int nextOrdinal = 0;
            boolean hasHole = false;
            for (int r = 0; r < rows; r++)
            {
                double x = random.nextDouble();
                if (r > 0 && nextOrdinal > 0 && x < extraProb)
                {
                    ordinalOfRow[r] = random.nextInt(nextOrdinal);
                    hasHole = true;
                }
                else if (r > 0 && x < extraProb + missingProb)
                {
                    ordinalOfRow[r] = -1;
                    hasHole = true;
                }
                else
                {
                    ordinalOfRow[r] = nextOrdinal++;
                }
            }
            if (nextOrdinal == 0)
                continue; // no vectors at all
            // with no holes the in-memory structure is empty (cachedBytesUsed() == 0), so don't assert the path
            checkMapping("randomFirstAppearance-" + iter, ordinalOfRow, hasHole ? Boolean.TRUE : null);
        }
    }

    /** Fully arbitrary mappings (any ordinal order, any sharing): lookups must be correct whichever path is used. */
    @Test
    public void testRandomArbitraryMappings() throws Exception
    {
        var random = new java.util.Random(getRandom().nextIntBetween(0, Integer.MAX_VALUE));
        for (int iter = 0; iter < 200; iter++)
        {
            int rows = 1 + random.nextInt(100);
            int ordinals = 1 + random.nextInt(rows);
            // every ordinal needs at least one row: give the first `ordinals` rows of a random permutation
            // to ordinals 0..ordinals-1, and assign the rest arbitrarily
            var permutation = new java.util.ArrayList<Integer>();
            for (int r = 0; r < rows; r++)
                permutation.add(r);
            java.util.Collections.shuffle(permutation, random);
            int[] ordinalOfRow = new int[rows];
            for (int i = 0; i < rows; i++)
                ordinalOfRow[permutation.get(i)] = i < ordinals ? i : random.nextInt(ordinals + 1) - 1;
            checkMapping("randomArbitrary-" + iter, ordinalOfRow, null);
        }
    }

    private static void computeRowIds(Map<VectorFloat<?>, VectorPostings<String>> postingsMap)
    {
        for (var p: postingsMap.entrySet())
        {
            p.getValue().computeRowIds(s -> {
                assert s.length() == 1;
                return s.charAt(0) - 'A';
            });
        }
    }

    private VectorPostings<String> createPostings(int ordinal, String... postings)
    {
        var vp = new VectorPostings<>(List.of(postings));
        vp.setOrdinal(ordinal);
        return vp;
    }

    private void testRandomPostingsOnce(Structure structure) throws Exception
    {
        var vectorValues = generateVectors(100 + randomInt(1000));
        var postingsMap = generatePostingsMap(vectorValues, structure);
        // call computeRowIds because that's how VectorPostings is designed.
        // since we're natively using ints as our rowids (not PrimaryKey objects) this can be the identity
        for (var p: postingsMap.entrySet())
            p.getValue().computeRowIds(x -> x);

        validate(structure, postingsMap, vectorValues);
    }

    private static int randomInt(int maxValueInclusive)
    {
        return getRandom().nextIntBetween(0, maxValueInclusive);
    }

    private <T> void validate(Structure structure, Map<VectorFloat<?>, VectorPostings<T>> postingsMap, RamAwareVectorValues vectorValues) throws IOException
    {
        // build the remapping and write the postings
        var remapped = RemappedPostings.remapForMemtable(postingsMap, Version.current(KEYSPACE));
        assert remapped.structure == structure : remapped.structure + " != " + structure;
        File tempFile = createTempFile("testfile");
        PostingsMetadata postingsMd = writePostings(tempFile, vectorValues, postingsMap, remapped);

        // test V5OnDiskOrdinalsMap
        FileHandle.Builder builder = new FileHandle.Builder(tempFile).withCompressionMetadata(null);
        try (FileHandle fileHandle = builder.complete())
        {
            var odom = new V5OnDiskOrdinalsMap(fileHandle, postingsMd.postingsOffset, postingsMd.postingsLength);
            assertEquals(structure, odom.structure);

            // check row -> ordinal and ordinal -> rows
            try (var rowIdsView = odom.getRowIdsView();
                 var ordinalsView = odom.getOrdinalsView())
            {
                for (var vp : postingsMap.values())
                {
                    var rowIds = vp.getRowIds().toIntArray();
                    Arrays.sort(rowIds);

                    var newOrdinal = remapped.ordinalMapper.oldToNew(vp.getOrdinal());
                    var it = rowIdsView.getSegmentRowIdsMatching(newOrdinal);
                    int[] a = StreamSupport.intStream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false).toArray();
                    Arrays.sort(a);

                    assertArrayEquals(rowIds, a);

                    for (int i = 0; i < rowIds.length; i++)
                    {
                        int ordinal = ordinalsView.getOrdinalForRowId(rowIds[i]);
                        assertEquals(newOrdinal, ordinal);
                    }
                }
            }

            // test buildOrdinalBits
            try (var ordinalsView = odom.getOrdinalsView())
            {
                for (int i = 0; i < 10; i++)
                {
                    // neither min nor max rowId has to actually exist in the postings
                    int minRowId = randomInt(vectorValues.size());
                    int maxRowId = minRowId + randomInt(vectorValues.size());
                    var bits = ordinalsView.buildOrdinalBits(minRowId, maxRowId, SparseBits::new);

                    var expectedBits = new SparseBits();
                    for (var vp : postingsMap.values())
                    {
                        for (int rowId : vp.getRowIds())
                        {
                            if (rowId >= minRowId && rowId <= maxRowId)
                                expectedBits.set(remapped.ordinalMapper.oldToNew(vp.getOrdinal()));
                        }
                    }

                    assertEqualsBits(expectedBits, bits, remapped.maxRowId);
                }
            }

            odom.close();
        }
    }

    private void assertEqualsBits(Bits b1, Bits b2, int maxBit)
    {
        for (int i = 0; i <= maxBit; i++)
            assertEquals(b1.get(i), b2.get(i));
    }

    private static <T> PostingsMetadata writePostings(File tempFile,
                                                  RandomAccessVectorValues vectorValues,
                                                  Map<VectorFloat<?>, VectorPostings<T>> postingsMap, RemappedPostings remapped) throws IOException
    {
        SequentialWriter writer = new SequentialWriter(tempFile,
                                                       SequentialWriterOption.newBuilder().finishOnClose(true).build());

        long postingsOffset = writer.position();
        long postingsPosition = remapped.writer(vectorValues, postingsMap).writeTo(writer);
        long postingsLength = postingsPosition - postingsOffset;

        writer.close();
        return new PostingsMetadata(postingsOffset, postingsLength);
    }

    private static class PostingsMetadata
    {
        public final long postingsOffset;
        public final long postingsLength;

        public PostingsMetadata(long postingsOffset, long postingsLength)
        {
            this.postingsOffset = postingsOffset;
            this.postingsLength = postingsLength;
        }
    }

    private static Map<VectorFloat<?>, VectorPostings<Integer>> generatePostingsMap(RandomAccessVectorValues vectorValues, Structure structure)
    {
        Map<VectorFloat<?>, VectorPostings<Integer>> postingsMap = emptyPostingsMap();

        // generate a list of rowIds that we'll initially assign 1:1 to ordinals,
        // leaving holes in the rowid sequence if we want ZERO_OR_ONE_TO_MANY
        var rowIds = new IntArrayList(vectorValues.size(), IntArrayList.DEFAULT_NULL_VALUE);
        IntUnaryOperator populator = structure == Structure.ZERO_OR_ONE_TO_MANY
                                   ? i -> 2 * i
                                   : i -> i;
        for (int i = 0; i < vectorValues.size(); i++)
            rowIds.add(populator.applyAsInt(i));

        // assign each ordinal a random rowid, without replacement
        for (int ordinal = 0; ordinal < vectorValues.size(); ordinal++)
        {
            var vector = vectorValues.getVector(ordinal);
            var rowIdIdx = randomIndex(rowIds);
            var vp = new VectorPostings<>(rowIds.getInt(rowIdIdx));
            rowIds.remove(rowIdIdx);
            vp.setOrdinal(ordinal);
            postingsMap.put(vector, vp);
        }

        if (structure == Structure.ONE_TO_ONE)
            return postingsMap;

        // make some of them 1:many
        int extraRows = 1 + randomInt(vectorValues.size());
        for (int i = 0; i < extraRows; i++)
        {
            var vector = randomVector(vectorValues);
            var vp = postingsMap.get(vector);
            vp.add(populator.applyAsInt(i + vectorValues.size()));
        }

        return postingsMap;
    }

    private static <T> ConcurrentSkipListMap<VectorFloat<?>, VectorPostings<T>> emptyPostingsMap()
    {
        return new ConcurrentSkipListMap<>((a, b) -> {
            return Arrays.compare(((ArrayVectorFloat) a).get(), ((ArrayVectorFloat) b).get());
        });
    }

    private static VectorFloat<?> randomVector(RandomAccessVectorValues ravv)
    {
        return ravv.getVector(randomInt(ravv.size() - 1));
    }

    private static int randomIndex(List<?> L)
    {
        return randomInt(L.size() - 1); // randomInt(max) is inclusive
    }

    /**
     * Generate `count` non-random vectors
     */
    private ConcurrentVectorValues generateVectors(int count)
    {
        var vectorValues = new ConcurrentVectorValues(DIMENSION);
        for (int i = 0; i < count; i++)
        {
            float[] rawVector = new float[DIMENSION];
            Arrays.fill(rawVector, (float) i);
            vectorValues.add(i, vts.createFloatVector(rawVector));
        }
        return vectorValues;
    }

    /**
     * Create a temporary file with the given prefix.
     */
    private static File createTempFile(String prefix)
    {
        File file = FileUtils.createTempFile(prefix, "tmp");
        file.deleteOnExit();
        return file;
    }

}
