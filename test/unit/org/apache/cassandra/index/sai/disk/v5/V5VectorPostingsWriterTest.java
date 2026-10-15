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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntFunction;
import java.util.stream.StreamSupport;

import org.junit.Before;
import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.commons.lang3.ArrayUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link V5VectorPostingsWriter#create} / {@link V5VectorPostingsWriter#writeTo}.
 * Covers: round-trips for all three posting structures and the empty case, structure validation,
 * header checks, and sort/skipSort behavior.
 */
public class V5VectorPostingsWriterTest extends VectorTester
{
    @Override
    @Before
    public void setup() throws Throwable
    {
        super.setup();
        SAIUtil.setCurrentVersion(Version.DC);
    }

    // =========================================================================
    // Round-trip tests
    // =========================================================================

    @Test
    public void testRoundTripEmpty() throws Exception
    {
        try (V5OnDiskOrdinalsMap map = roundtrip(V5VectorPostingsWriter.create(-1, asFunction(Map.of()))))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure); // default structure
        }
    }

    @Test
    public void testRoundTripOneToOne() throws Exception
    {
        int maxOrdinal = 4;
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(maxOrdinal, V5VectorPostingsWriterTest::array)
                                                         .structure(Structure.ONE_TO_ONE);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ONE_TO_ONE, map.structure);
            try (OrdinalsView ov = map.getOrdinalsView())
            {
                for (int i = 0; i <= maxOrdinal; i++)
                {
                    assertArrayEquals("ordinal " + i, new int[]{ i }, rowIds(map, i));
                    assertEquals("rowId " + i, i, ov.getOrdinalForRowId(i));
                }
            }
        }
    }

    @Test
    public void testRoundTripOneToMany() throws Exception
    {
        // ordinal 0 -> [0], ordinal 1 -> [1, 3], ordinal 2 -> [2], ordinal 3 -> [3]
        // extra row 3 of ordinal 1 is within [0, maxVectorOrdinal=3]
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0),
                1, array(1, 3),
                2, array(2),
                3, array(3)));
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(3, postings)
                                                         .structure(Structure.ONE_TO_MANY);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ONE_TO_MANY, map.structure);
            assertArrayEquals(new int[]{ 0 },    rowIds(map, 0));
            assertArrayEquals(new int[]{ 1, 3 }, rowIds(map, 1));
            assertArrayEquals(new int[]{ 2 },    rowIds(map, 2));
            assertArrayEquals(new int[]{ 3 },    rowIds(map, 3));

            try (OrdinalsView ov = map.getOrdinalsView())
            {
                assertEquals(0, ov.getOrdinalForRowId(0));
                assertEquals(1, ov.getOrdinalForRowId(1));
                assertEquals(2, ov.getOrdinalForRowId(2));
                assertEquals(1, ov.getOrdinalForRowId(3));
            }
        }
    }

    @Test
    public void testRoundTripZeroOrOneToMany() throws Exception
    {
        // ordinal 0 -> [0], ordinal 1 -> [2] (rowId 1 has no vector: gap)
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0),
                1, array(2)));
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(1, postings)
                                                         .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                         .maxRowId(2);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure);
            assertArrayEquals(new int[]{ 0 }, rowIds(map, 0));
            assertArrayEquals(new int[]{ 2 }, rowIds(map, 1));

            try (OrdinalsView ov = map.getOrdinalsView())
            {
                assertEquals(0,  ov.getOrdinalForRowId(0));
                assertEquals(-1, ov.getOrdinalForRowId(1)); // gap
                assertEquals(1,  ov.getOrdinalForRowId(2));
            }
        }
    }

    @Test
    public void testRoundTripOneToManyWithHole() throws Exception
    {
        // ordinal 0 -> [0, 1], ordinal 1 is a hole (absent), ordinal 2 -> [2], ordinal 3 -> [3]
        // extra row 1 of ordinal 0 is within [0, maxVectorOrdinal=3]; ordinal 1 is a hole
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0, 1),
                3, array(3)));  // ordinal 1 is absent → hole; ordinal 2 is absent → hole
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(3, postings)
                                                         .structure(Structure.ONE_TO_MANY);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ONE_TO_MANY, map.structure);
            assertArrayEquals(new int[]{ 0, 1 }, rowIds(map, 0));
            assertArrayEquals(ArrayUtils.EMPTY_INT_ARRAY, rowIds(map, 1));
            assertArrayEquals(ArrayUtils.EMPTY_INT_ARRAY, rowIds(map, 2));
            assertArrayEquals(new int[]{ 3 },    rowIds(map, 3));
        }
    }

    @Test
    public void testNullRowIdsAreTreatedAsHoles() throws Exception
    {
        // ordinal 1 returns null, which must behave exactly like an empty list, for both structures
        IntFunction<IntArrayList> postings = i -> i == 1 ? null : array(i);
        for (Structure structure : new Structure[]{ Structure.ZERO_OR_ONE_TO_MANY, Structure.ONE_TO_MANY })
        {
            V5VectorPostingsWriter w = V5VectorPostingsWriter.create(2, postings)
                                                             .structure(structure)
                                                             .maxRowId(2);
            try (V5OnDiskOrdinalsMap map = roundtrip(w))
            {
                assertEquals(structure, map.structure);
                assertArrayEquals(new int[]{ 0 }, rowIds(map, 0));
                assertArrayEquals(ArrayUtils.EMPTY_INT_ARRAY, rowIds(map, 1));
                assertArrayEquals(new int[]{ 2 }, rowIds(map, 2));
            }
        }
    }

    @Test
    public void testOneToManyRandomized() throws Exception
    {
        // ONE_TO_MANY invariant: min(rowIds[ordinal]) == ordinal, i.e. each ordinal owns its own
        // index as its primary rowId.  Extra rowIds are additional rows above the primary.
        // maxVectorOrdinal must cover every rowId that appears as a primary, so we set it to
        // totalRows - 1 and let some ordinals be holes (no rows at all).
        int totalRows = 10000 + getRandom().nextIntBetween(0, 2000);

        // expectedOrdinal[rowId]: ordinal that owns this rowId
        // By default every rowId is its own primary (identity mapping).
        int[] expectedOrdinal = new int[totalRows];
        for (int i = 0; i < totalRows; i++)
            expectedOrdinal[i] = i;

        // Randomly merge ~9 000 rowIds as extras into a random lower ordinal, so that
        // the extras-per-ordinal distribution is spread across many ordinals and the
        // written order (by ordinal) differs from sorted-by-rowId order.
        Map<Integer, IntArrayList> postingsMap = new HashMap<>();
        for (int i = 0; i < totalRows; i++)
        {
            IntArrayList list = new IntArrayList();
            list.add(i);
            postingsMap.put(i, list);
        }
        int numMerges = totalRows - 1000 - getRandom().nextIntBetween(0, 200);
        for (int m = 0; m < numMerges; m++)
        {
            // Pick a donor rowId that is still its own sole entry (not yet an extra of anyone,
            // and hasn't accumulated its own extras), then merge it into a random lower ordinal.
            int donor = getRandom().nextIntBetween(1, totalRows - 1);
            if (postingsMap.get(donor).size() != 1 || expectedOrdinal[donor] != donor)
                continue; // already reassigned or has accumulated extras — skip
            int target = getRandom().nextIntBetween(0, donor - 1);
            // target must still be its own primary so that min(rowIds[target]) == target holds
            if (expectedOrdinal[target] != target)
                continue;
            postingsMap.get(target).add(donor);
            postingsMap.get(donor).clear(); // donor becomes a hole
            expectedOrdinal[donor] = target;
        }

        IntFunction<IntArrayList> postings = i -> postingsMap.getOrDefault(i, array());
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(totalRows - 1, postings)
                                                         .structure(Structure.ONE_TO_MANY);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ONE_TO_MANY, map.structure);
            try (OrdinalsView ov = map.getOrdinalsView())
            {
                for (int rowId = 0; rowId < totalRows; rowId++)
                    assertEquals("rowId " + rowId, expectedOrdinal[rowId], ov.getOrdinalForRowId(rowId));
            }
        }
    }

    @Test
    public void testZeroOrOneToManyRandomized() throws Exception
    {
        // ~1 000 ordinals, ~10 000 rowIds, with random gaps (rows with no vector) and random
        // multi-row ordinals.  Verifies both directions of the mapping round-trip correctly.
        int numOrdinals = 1000 + getRandom().nextIntBetween(0, 200);
        int totalRows   = 10000 + getRandom().nextIntBetween(0, 2000);

        // expectedOrdinal[rowId]: ordinal for that row, or -1 if it has no vector (gap)
        int[] expectedOrdinal = new int[totalRows];
        Arrays.fill(expectedOrdinal, -1);

        Map<Integer, IntArrayList> postingsMap = new HashMap<>();
        for (int i = 0; i < numOrdinals; i++)
            postingsMap.put(i, new IntArrayList());

        // Assign each rowId to exactly one ordinal, chosen at random; some rowIds are left
        // unassigned (gaps) by randomly skipping them with ~10% probability.
        for (int rowId = 0; rowId < totalRows; rowId++)
        {
            if (getRandom().nextIntBetween(0, 9) == 0)
                continue; // gap row — no vector
            int ordinal = getRandom().nextIntBetween(0, numOrdinals - 1);
            postingsMap.get(ordinal).add(rowId);
            expectedOrdinal[rowId] = ordinal;
        }

        // expectedRowIds[ordinal] = sorted rowIds assigned to that ordinal
        int[][] expectedRowIds = new int[numOrdinals][];
        for (int i = 0; i < numOrdinals; i++)
        {
            int[] ids = postingsMap.get(i).toIntArray();
            Arrays.sort(ids);
            expectedRowIds[i] = ids;
        }

        IntFunction<IntArrayList> postings = i -> postingsMap.getOrDefault(i, array());
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(numOrdinals - 1, postings)
                                                         .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                         .maxRowId(totalRows - 1);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure);
            try (OrdinalsView ov = map.getOrdinalsView())
            {
                for (int rowId = 0; rowId < totalRows; rowId++)
                    assertEquals("rowId " + rowId, expectedOrdinal[rowId], ov.getOrdinalForRowId(rowId));
            }
            try (RowIdsView rv = map.getRowIdsView())
            {
                for (int ordinal = 0; ordinal < numOrdinals; ordinal++)
                {
                    PrimitiveIterator.OfInt it = rv.getSegmentRowIdsMatching(ordinal);
                    int[] got = StreamSupport.intStream(
                            Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false
                    ).toArray();
                    Arrays.sort(got);
                    assertArrayEquals("ordinal " + ordinal, expectedRowIds[ordinal], got);
                }
            }
        }
    }

    // =========================================================================
    // Structure validation tests
    // =========================================================================

    @Test
    public void testDefaultStructureIsZeroOrOneToMany() throws Exception
    {
        // No structure() call → ZERO_OR_ONE_TO_MANY
        try (V5OnDiskOrdinalsMap map = roundtrip(V5VectorPostingsWriter.create(0, asFunction(Map.of(
                0, array(0)))).maxRowId(0)))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOneToManyValidationThrowsOnMinMismatch() throws Exception
    {
        // ordinal 1 has min rowId 2, not 1 → ONE_TO_MANY invariant violated
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0),
                1, array(2, 3)));
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(3, postings)
                                                         .structure(Structure.ONE_TO_MANY);
        roundtrip(w).close();
    }

    @Test
    public void testZeroOrOneToManyAcceptsDataThatFitsSmallerStructure() throws Exception
    {
        // data is ONE_TO_ONE, but we force ZERO_OR_ONE_TO_MANY — no error
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0),
                1, array(1),
                2, array(2)));
        try (V5OnDiskOrdinalsMap map = roundtrip(V5VectorPostingsWriter.create(2, postings)
                                                                          .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                                          .maxRowId(2)))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure);
        }
    }

    // =========================================================================
    // Header check tests
    // =========================================================================

    @Test(expected = IllegalStateException.class)
    public void testOneToOneMaxRowIdMismatchThrows() throws Exception
    {
        // ONE_TO_ONE never calls rowIdsOf — identity lambda is fine
        V5VectorPostingsWriter w = V5VectorPostingsWriter
                .create(3, V5VectorPostingsWriterTest::array)
                .structure(Structure.ONE_TO_ONE)
                .maxRowId(99); // != maxVectorOrdinal (3)
        roundtrip(w).close();
    }

    @Test
    public void testOneToOneWithoutMaxRowIdWritesMaxOrdinal() throws Exception
    {
        int max = 4;
        try (V5OnDiskOrdinalsMap map = roundtrip(V5VectorPostingsWriter.create(max, V5VectorPostingsWriterTest::array)
                                                                          .structure(Structure.ONE_TO_ONE)))
        {
            assertEquals(Structure.ONE_TO_ONE, map.structure);
            assertArrayEquals(new int[]{ max }, rowIds(map, max));
        }
    }

    @Test
    public void testOneToOneNeverCallsRowIdsOf() throws Exception
    {
        // rowIdsOf must not be invoked for ONE_TO_ONE; this lambda throws if it is
        V5VectorPostingsWriter w = V5VectorPostingsWriter
                .create(3, _ -> { throw new AssertionError("rowIdsOf must not be called for ONE_TO_ONE"); })
                .structure(Structure.ONE_TO_ONE);
        try (V5OnDiskOrdinalsMap map = roundtrip(w))
        {
            assertEquals(Structure.ONE_TO_ONE, map.structure);
        }
        // no AssertionError → rowIdsOf was not invoked
    }

    @Test(expected = IllegalStateException.class)
    public void testOneToManyMaxRowIdBelowMaxOrdinalThrows() throws Exception
    {
        // maxRowId(1) < maxVectorOrdinal(3) → must throw
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0),
                1, array(1),
                2, array(2),
                3, array(3)));
        V5VectorPostingsWriter w = V5VectorPostingsWriter.create(3, postings)
                                                         .structure(Structure.ONE_TO_MANY)
                                                         .maxRowId(1);
        roundtrip(w).close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOneToManyMaxRowIdAboveMaxOrdinalThrows() throws Exception
    {
        // extra row 5 of ordinal 0 exceeds maxVectorOrdinal=4 → must throw
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(0, 5),
                1, array(1),
                2, array(2),
                3, array(3),
                4, array(4)));
        V5VectorPostingsWriter.create(4, postings)
                              .structure(Structure.ONE_TO_MANY)
                              .maxRowId(5)
                              .writeTo(null); // throws before any I/O
    }

    // =========================================================================
    // Sorting tests
    // =========================================================================

    @Test
    public void testSortingByDefault() throws Exception
    {
        // Pass unsorted rowIds; reader should return them sorted
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(3, 1, 2),
                1, array(4)));
        try (V5OnDiskOrdinalsMap map = roundtrip(V5VectorPostingsWriter.create(1, postings)
                                                                          .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                                          .maxRowId(4)))
        {
            assertArrayEquals(new int[]{ 1, 2, 3 }, rowIds(map, 0));
        }
    }

    @Test
    public void testSkipSortingWritesAsGiven() throws Exception
    {
        // skipRowIdSorting(): lists are already ascending
        IntFunction<IntArrayList> postings = asFunction(Map.of(
                0, array(1, 2, 3),
                1, array(4)));
        try (V5OnDiskOrdinalsMap map = roundtrip(V5VectorPostingsWriter.create(1, postings)
                                                                          .structure(Structure.ZERO_OR_ONE_TO_MANY)
                                                                          .maxRowId(4)
                                                                          .skipRowIdSorting()))
        {
            // reader returns stored order; after sorting both match
            assertArrayEquals(new int[]{ 1, 2, 3 }, rowIds(map, 0));
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Wrap varargs ints in an IntArrayList without copying. */
    private static IntArrayList array(int... values)
    {
        return new IntArrayList(values, values.length, IntArrayList.DEFAULT_NULL_VALUE);
    }

    /**
     * Build a function from a map of ordinal → rowIds.
     * Ordinals absent from the map return an empty list (holes).
     */
    private static IntFunction<IntArrayList> asFunction(Map<Integer, IntArrayList> postings)
    {
        return i -> postings.getOrDefault(i, array());
    }

    private static File createTempFile()
    {
        File f = FileUtils.createTempFile("V5VectorPostingsWriterTest", "tmp");
        f.deleteOnExit();
        return f;
    }

    /**
     * Write via {@link V5VectorPostingsWriter#create} and open the result as a V5OnDiskOrdinalsMap.
     * The caller is responsible for closing the returned map.
     */
    private static V5OnDiskOrdinalsMap roundtrip(V5VectorPostingsWriter w) throws IOException
    {
        File f = createTempFile();
        SequentialWriter sw = new SequentialWriter(f, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        long start = sw.position();
        long end = w.writeTo(sw);
        sw.close();
        FileHandle fh = new FileHandle.Builder(f).withCompressionMetadata(null).complete();
        return new V5OnDiskOrdinalsMap(fh, start, end - start);
    }

    /** Return sorted rowIds from the RowIdsView for a given ordinal. */
    private static int[] rowIds(V5OnDiskOrdinalsMap map, int ordinal) throws IOException
    {
        try (RowIdsView view = map.getRowIdsView())
        {
            PrimitiveIterator.OfInt it = view.getSegmentRowIdsMatching(ordinal);
            int[] arr = StreamSupport.intStream(
                    Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false
            ).toArray();
            Arrays.sort(arr);
            return arr;
        }
    }
}
