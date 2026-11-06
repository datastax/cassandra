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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.commons.lang3.ArrayUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link RemappedPostings#writer}: the bridge from a {@link RemappedPostings}, the vector values and the
 * vector to postings map to a configured {@link V5VectorPostingsWriter}. The output is checked through
 * {@link V5OnDiskOrdinalsMap}, i.e. what the reader returns, not file bytes.
 */
public class RemappedPostingsTest extends VectorTester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final int DIMENSION = 2; // irrelevant to postings mapping

    @Override
    @Before
    public void setup() throws Throwable
    {
        super.setup();
        SAIUtil.setCurrentVersion(Version.DC);
    }

    // =========================================================================
    // Tests
    // =========================================================================

    @Test
    public void testHolesAndPresentOrdinals() throws Exception
    {
        // old ordinals 0 and 2 are kept as new ordinals 0 and 2; new ordinal 1 is a hole.
        // rows: 0 -> vector 0, 1 and 3 -> vector 2, rows 2 and 4 have no vector.
        ConcurrentVectorValues vectors = vectors(3);
        Map<VectorFloat<?>, VectorPostings<Integer>> postings = new IdentityHashMap<>();
        postings.put(vectors.getVector(0), postings(0, 0));
        postings.put(vectors.getVector(2), postings(2, 3, 1)); // deliberately unsorted

        OrdinalMapper mapper = new OrdinalMapper.MapMapper(Map.of(0, 0, 2, 2));
        RemappedPostings remapped = new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY, 2, 4, null, mapper);

        try (V5OnDiskOrdinalsMap map = roundtrip(remapped.writer(vectors, postings)))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure);
            assertArrayEquals(new int[]{ 0 }, rowIds(map, 0));
            assertArrayEquals(ArrayUtils.EMPTY_INT_ARRAY, rowIds(map, 1));
            assertArrayEquals(new int[]{ 1, 3 }, rowIds(map, 2));

            // maxRowId comes from the RemappedPostings: row 4 is covered although no vector has it
            try (OrdinalsView ov = map.getOrdinalsView())
            {
                assertEquals(0, ov.getOrdinalForRowId(0));
                assertEquals(2, ov.getOrdinalForRowId(1));
                assertEquals(-1, ov.getOrdinalForRowId(2));
                assertEquals(2, ov.getOrdinalForRowId(3));
                assertEquals(-1, ov.getOrdinalForRowId(4));
            }
        }
    }

    @Test
    public void testZeroOrOneToManyFromRemapForMemtable() throws Exception
    {
        // vector 0 -> rows 2 and 0, vector 1 -> row 1, vector 2 -> row 3.
        // remapForMemtable now always goes to ZERO_OR_ONE_TO_MANY when extra postings exist.
        // Ordinals are renumbered densely: new ordinal 0 = vector 0, 1 = vector 1, 2 = vector 2.
        ConcurrentVectorValues vectors = vectors(3);
        Map<VectorFloat<?>, VectorPostings<Integer>> postings = new IdentityHashMap<>();
        postings.put(vectors.getVector(0), postings(0, 2, 0));
        postings.put(vectors.getVector(1), postings(1, 1));
        postings.put(vectors.getVector(2), postings(2, 3));

        RemappedPostings remapped = RemappedPostings.remapForMemtable(postings, Version.DC);
        assertEquals(Structure.ZERO_OR_ONE_TO_MANY, remapped.structure);

        try (V5OnDiskOrdinalsMap map = roundtrip(remapped.writer(vectors, postings)))
        {
            assertEquals(Structure.ZERO_OR_ONE_TO_MANY, map.structure);
            // ordinals are dense [0,2]; no holes
            assertArrayEquals(new int[]{ 0, 2 }, rowIds(map, 0));
            assertArrayEquals(new int[]{ 1 }, rowIds(map, 1));
            assertArrayEquals(new int[]{ 3 }, rowIds(map, 2));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOneToOneDoesNotTouchVectorsOrPostings() throws Exception
    {
        RandomAccessVectorValues noVectors = Mockito.mock(RandomAccessVectorValues.class, invocation -> {
            throw new AssertionError("vector values must not be used for ONE_TO_ONE: " + invocation.getMethod());
        });
        Map<VectorFloat<?>, VectorPostings<Integer>> noPostings = Mockito.mock(Map.class, invocation -> {
            throw new AssertionError("postings map must not be used for ONE_TO_ONE: " + invocation.getMethod());
        });

        RemappedPostings remapped = new RemappedPostings(Structure.ONE_TO_ONE, 3, 3, null,
                                                         new OrdinalMapper.IdentityMapper(3));

        try (V5OnDiskOrdinalsMap map = roundtrip(remapped.writer(noVectors, noPostings)))
        {
            assertEquals(Structure.ONE_TO_ONE, map.structure);
            try (OrdinalsView ov = map.getOrdinalsView())
            {
                for (int i = 0; i <= 3; i++)
                {
                    assertArrayEquals("ordinal " + i, new int[]{ i }, rowIds(map, i));
                    assertEquals("rowId " + i, i, ov.getOrdinalForRowId(i));
                }
            }
        }
    }

    @Test
    public void testEmpty() throws Exception
    {
        // for an empty map remapForMemtable leaves both maxima at Integer.MIN_VALUE; the writer expects -1
        Map<VectorFloat<?>, VectorPostings<Integer>> postings = new IdentityHashMap<>();
        RemappedPostings remapped = RemappedPostings.remapForMemtable(postings, Version.DC);
        assertEquals(Integer.MIN_VALUE, remapped.maxNewOrdinal);

        try (V5OnDiskOrdinalsMap ignored = roundtrip(remapped.writer(vectors(0), postings)))
        {
            // writing and opening an empty relation must not throw
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Vectors for ordinals {@code 0..count-1}; each ordinal gets its own instance. */
    private static ConcurrentVectorValues vectors(int count)
    {
        ConcurrentVectorValues vectors = new ConcurrentVectorValues(DIMENSION);
        for (int i = 0; i < count; i++)
        {
            float[] raw = new float[DIMENSION];
            Arrays.fill(raw, (float) i);
            vectors.add(i, vts.createFloatVector(raw));
        }
        return vectors;
    }

    /** Postings for one vector. Keys are the row ids themselves, so {@code computeRowIds} is the identity. */
    private static VectorPostings<Integer> postings(int ordinal, Integer... rows)
    {
        VectorPostings<Integer> vp = new VectorPostings<>(List.of(rows));
        vp.setOrdinal(ordinal);
        vp.computeRowIds(x -> x);
        return vp;
    }

    private static V5OnDiskOrdinalsMap roundtrip(V5VectorPostingsWriter writer) throws IOException
    {
        File f = FileUtils.createTempFile("RemappedPostingsTest", "tmp");
        f.deleteOnExit();
        SequentialWriter sw = new SequentialWriter(f, SequentialWriterOption.newBuilder().finishOnClose(true).build());
        long start = sw.position();
        long end = writer.writeTo(sw);
        sw.close();
        FileHandle fh = new FileHandle.Builder(f).withCompressionMetadata(null).complete();
        return new V5OnDiskOrdinalsMap(fh, start, end - start);
    }

    /** Sorted rowIds that the reader returns for an ordinal. */
    private static int[] rowIds(V5OnDiskOrdinalsMap map, int ordinal) throws IOException
    {
        try (RowIdsView view = map.getRowIdsView())
        {
            PrimitiveIterator.OfInt it = view.getSegmentRowIdsMatching(ordinal);
            int[] arr = StreamSupport.intStream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
                                     .toArray();
            Arrays.sort(arr);
            return arr;
        }
    }
}
