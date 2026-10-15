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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.util.BitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VectorMergePostingsTest
{
    private static final int[] ID_UPPER_BOUNDS = { 100, 40, 200 };

    private VectorMergePostings postings;

    @Before
    public void setUp() throws IOException
    {
        postings = new VectorMergePostings(ID_UPPER_BOUNDS, 1000);
    }

    @After
    public void tearDown() throws IOException
    {
        if (postings != null)
            postings.close();
    }

    @Test
    public void emptyHasNothing()
    {
        assertTrue(postings.isEmpty());
        assertEquals(0, postings.size());
        assertEquals(-1, postings.maxRowId());
        assertNull(postings.get(new VectorHandle(0, 5)));
        assertEquals(ID_UPPER_BOUNDS.length, postings.sourceCount());
        for (int s = 0; s < postings.sourceCount(); s++)
        {
            assertEquals(ID_UPPER_BOUNDS[s], postings.liveNodes(s).length());
            assertEquals(0, postings.liveNodes(s).cardinality());
        }
    }

    @Test
    public void putMarksNodeLiveAndRecordsRow()
    {
        postings.put(new VectorHandle(1, 7), 3);

        assertFalse(postings.isEmpty());
        assertEquals(1, postings.size());
        assertEquals(3, postings.maxRowId());
        assertTrue(postings.liveNodes(1).get(7));
        assertEquals(1, postings.liveNodes(1).cardinality());
        assertEquals(0, postings.liveNodes(0).cardinality());

        assertNotNull(postings.get(new VectorHandle(1, 7)));
        assertEquals(List.of(3), postings.get(new VectorHandle(1, 7)).getPostings());
    }

    @Test
    public void rowsSharingANodeAreCountedOnce()
    {
        VectorHandle node = new VectorHandle(0, 10);
        postings.put(node, 4);
        postings.put(node, 9);
        postings.put(node, 2);

        assertEquals("one surviving node, however many rows point at it", 1, postings.size());
        assertEquals(9, postings.maxRowId());
        assertEquals(1, postings.liveNodes(0).cardinality());
        assertEquals(List.of(4, 9, 2), postings.get(node).getPostings());
    }

    @Test
    public void sameNodeIdInDifferentSourcesAreDistinct()
    {
        postings.put(new VectorHandle(0, 5), 0);
        postings.put(new VectorHandle(1, 5), 1);
        postings.put(new VectorHandle(2, 5), 2);

        assertEquals(3, postings.size());
        for (int s = 0; s < 3; s++)
            assertTrue(postings.liveNodes(s).get(5));
        assertEquals(List.of(1), postings.get(new VectorHandle(1, 5)).getPostings());
    }

    @Test
    public void liveNodesAreExactlyTheMapKeys()
    {
        List<VectorHandle> handles = new ArrayList<>();
        for (int s = 0; s < ID_UPPER_BOUNDS.length; s++)
            for (int n = s; n < ID_UPPER_BOUNDS[s]; n += 7)
                handles.add(new VectorHandle(s, n));

        int row = 0;
        for (VectorHandle h : handles)
            postings.put(h, row++);

        assertEquals(handles.size(), postings.size());
        int bits = 0;
        for (int s = 0; s < ID_UPPER_BOUNDS.length; s++)
        {
            BitSet live = postings.liveNodes(s);
            bits += live.cardinality();
            for (int n = 0; n < ID_UPPER_BOUNDS[s]; n++)
                assertEquals("source " + s + " node " + n,
                             postings.get(new VectorHandle(s, n)) != null,
                             live.get(n));
        }
        assertEquals("bit count matches the incrementally maintained size", postings.size(), bits);
    }

    @Test
    public void concurrentPutsKeepSizeAndLiveSetsConsistent() throws Exception
    {
        int threads = 8;
        int nodesPerSource = ID_UPPER_BOUNDS[1]; // smallest bound, so every handle is valid for every source
        try (ExecutorService pool = Executors.newFixedThreadPool(threads))
        {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++)
            {
                final int thread = t;
                futures.add(pool.submit(() -> {
                    // every thread hits every node, so each node is contended by all threads
                    for (int s = 0; s < ID_UPPER_BOUNDS.length; s++)
                        for (int n = 0; n < nodesPerSource; n++)
                            postings.put(new VectorHandle(s, n), thread * 1000 + s * nodesPerSource + n);
                }));
            }
            for (Future<?> f : futures)
                f.get(60, TimeUnit.SECONDS);
        }

        int expected = ID_UPPER_BOUNDS.length * nodesPerSource;
        assertEquals(expected, postings.size());
        int bits = 0;
        for (int s = 0; s < postings.sourceCount(); s++)
            bits += postings.liveNodes(s).cardinality();
        assertEquals(expected, bits);
        assertEquals("no row lost under contention", threads, postings.get(new VectorHandle(0, 0)).size());
        assertEquals((threads - 1) * 1000 + (long) (ID_UPPER_BOUNDS.length - 1) * nodesPerSource + (nodesPerSource - 1),
                     postings.maxRowId());
    }

    @Test
    public void rejectsHandlesOutsideTheSourceGraphs()
    {
        assertRejected(new VectorHandle(-1, 0));
        assertRejected(new VectorHandle(ID_UPPER_BOUNDS.length, 0));
        assertRejected(new VectorHandle(0, -1));
        assertRejected(new VectorHandle(0, ID_UPPER_BOUNDS[0]));
        assertRejected(new VectorHandle(1, ID_UPPER_BOUNDS[1]));

        assertTrue("a rejected handle must leave no trace", postings.isEmpty());
        assertEquals(-1, postings.maxRowId());
    }

    @Test
    public void idUpperBoundReportsPerSourceBound()
    {
        for (int s = 0; s < ID_UPPER_BOUNDS.length; s++)
            assertEquals(ID_UPPER_BOUNDS[s], postings.idUpperBound(s));
    }

    @Test
    public void closeDeletesBackingFile() throws IOException
    {
        postings.put(new VectorHandle(0, 0), 0);
        java.io.File[] files = postings.backingFiles();
        for (java.io.File f : files)
            assertTrue(f.exists());

        postings.close();
        postings = null;

        for (java.io.File f : files)
            assertFalse(f.exists());
    }

    @Test
    public void ordinalMappersOfEmptyPostingsMapNothing()
    {
        List<OrdinalMapper> mappers = postings.ordinalMapping();

        assertEquals(ID_UPPER_BOUNDS.length, mappers.size());
        for (OrdinalMapper mapper : mappers)
            assertEquals(-1, mapper.maxOrdinal());
    }

    @Test
    public void ordinalMappersAssignOrdinalsInAscendingRowIdOrder()
    {
        // ordinals are assigned in the order each (source, nodeId) pair is first seen by rowId
        postings.put(new VectorHandle(1, 20), 0);  // (1,20) → ordinal 0
        postings.put(new VectorHandle(0, 9), 1);   // (0,9)  → ordinal 1
        postings.put(new VectorHandle(2, 5), 2);   // (2,5)  → ordinal 2
        postings.put(new VectorHandle(1, 5), 3);   // (1,5)  → ordinal 3
        postings.put(new VectorHandle(0, 5), 4);   // (0,5)  → ordinal 4
        postings.put(new VectorHandle(2, 3), 5);   // (2,3)  → ordinal 5

        List<OrdinalMapper> mappers = postings.ordinalMapping();

        assertEquals(3, mappers.size());
        assertEquals(0, mappers.get(1).oldToNew(20));
        assertEquals(1, mappers.get(0).oldToNew(9));
        assertEquals(2, mappers.get(2).oldToNew(5));
        assertEquals(3, mappers.get(1).oldToNew(5));
        assertEquals(4, mappers.get(0).oldToNew(5));
        assertEquals(5, mappers.get(2).oldToNew(3));
        // maxOrdinal is the highest ordinal assigned within each source
        assertEquals(4, mappers.get(0).maxOrdinal());
        assertEquals(3, mappers.get(1).maxOrdinal());
        assertEquals(5, mappers.get(2).maxOrdinal());
    }

    @Test
    public void ordinalMappersAreInverseOfEachOtherAndCoverLiveNodes()
    {
        int row = 0;
        for (int s = 0; s < ID_UPPER_BOUNDS.length; s++)
            for (int n = s; n < ID_UPPER_BOUNDS[s]; n += 3 + s)
                postings.put(new VectorHandle(s, n), row++);

        List<OrdinalMapper> mappers = postings.ordinalMapping();

        boolean[] ordinalUsed = new boolean[postings.size()];
        for (int s = 0; s < ID_UPPER_BOUNDS.length; s++)
        {
            OrdinalMapper mapper = mappers.get(s);
            BitSet live = postings.liveNodes(s);
            int previous = -1;
            for (int n = 0; n < ID_UPPER_BOUNDS[s]; n++)
            {
                if (!live.get(n))
                    continue;
                int ordinal = mapper.oldToNew(n);
                assertTrue("ordinal in [0, size)", ordinal >= 0 && ordinal < postings.size());
                assertFalse("ordinal " + ordinal + " assigned twice", ordinalUsed[ordinal]);
                ordinalUsed[ordinal] = true;
                assertEquals(n, mapper.newToOld(ordinal));
                assertTrue("ordinals increase with node id within a source", ordinal > previous);
                previous = ordinal;
            }
        }
        for (int i = 0; i < ordinalUsed.length; i++)
            assertTrue("ordinal " + i + " unassigned", ordinalUsed[i]);
    }

    @Test
    public void ordinalMappersOrderByRowId()
    {
        // ordinals follow the rowId order of each vector's first appearance, regardless of source or node id
        postings.put(new VectorHandle(0, 30), 0);   // (0,30)  → ordinal 0
        postings.put(new VectorHandle(1, 10), 1);   // (1,10)  → ordinal 1
        postings.put(new VectorHandle(2, 150), 2);  // (2,150) → ordinal 2

        List<OrdinalMapper> mappers = postings.ordinalMapping();

        assertEquals(0, mappers.get(0).oldToNew(30));
        assertEquals(1, mappers.get(1).oldToNew(10));
        assertEquals(2, mappers.get(2).oldToNew(150));
    }

    @Test
    public void ordinalMappersGiveOmittedForOrdinalsOwnedByOtherSources()
    {
        postings.put(new VectorHandle(0, 5), 0);
        postings.put(new VectorHandle(1, 5), 1);

        List<OrdinalMapper> mappers = postings.ordinalMapping();

        assertEquals(0, mappers.get(0).oldToNew(5));
        assertEquals(1, mappers.get(1).oldToNew(5));
        assertEquals(OrdinalMapper.OMITTED, mappers.get(0).newToOld(1));
        assertEquals(OrdinalMapper.OMITTED, mappers.get(1).newToOld(0));
        assertEquals("a source without survivors maps nothing", -1, mappers.get(2).maxOrdinal());
    }

    @Test
    public void ordinalMappersIgnoreRowMultiplicity()
    {
        VectorHandle shared = new VectorHandle(0, 4);
        postings.put(shared, 0);
        postings.put(shared, 1);
        postings.put(shared, 2);
        postings.put(new VectorHandle(1, 4), 3);

        List<OrdinalMapper> mappers = postings.ordinalMapping();

        assertEquals("a node shared by many rows takes a single ordinal", 0, mappers.get(0).oldToNew(4));
        assertEquals(1, mappers.get(1).oldToNew(4));
        assertEquals(0, mappers.get(0).maxOrdinal());
        assertEquals(1, mappers.get(1).maxOrdinal());
    }

    // ---- rowIdsByMergedOrdinal tests ----

    @Test
    public void rowIdsByMergedOrdinalReturnsCorrectListForEachOrdinal() throws IOException
    {
        // Two sources: source 0 node 0 → ordinal 0, source 1 node 5 → ordinal 1, source 0 node 7 → ordinal 2
        postings.put(new VectorHandle(0, 0), 10);
        postings.put(new VectorHandle(1, 5), 20);
        postings.put(new VectorHandle(0, 7), 30);

        // Build mappers matching ordinalMappers() ordering: node-id ascending across sources
        // node 0 from s0 → ordinal 0, node 5 from s1 → ordinal 1, node 7 from s0 → ordinal 2
        List<OrdinalMapper> mappers = postings.ordinalMapping();
        IntFunction<IntArrayList> fn = postings.rowIdsByMergedOrdinal(mappers);

        assertNotNull(fn.apply(0));
        assertEquals(1, fn.apply(0).size());
        assertEquals(10, fn.apply(0).getInt(0));

        assertNotNull(fn.apply(1));
        assertEquals(1, fn.apply(1).size());
        assertEquals(20, fn.apply(1).getInt(0));

        assertNotNull(fn.apply(2));
        assertEquals(1, fn.apply(2).size());
        assertEquals(30, fn.apply(2).getInt(0));
    }

    @Test
    public void rowIdsByMergedOrdinalMultipleRowsForOneOrdinal() throws IOException
    {
        VectorHandle h = new VectorHandle(0, 3);
        postings.put(h, 0);
        postings.put(h, 1);
        postings.put(h, 2);

        List<OrdinalMapper> mappers = postings.ordinalMapping();
        IntFunction<IntArrayList> fn = postings.rowIdsByMergedOrdinal(mappers);

        IntArrayList ids = fn.apply(0);
        assertNotNull(ids);
        assertEquals(3, ids.size());
        // row ids returned in insertion order from ChronicleMap; just verify they contain 0,1,2
        assertTrue(ids.contains(0));
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(2));
    }

    @Test(expected = IllegalStateException.class)
    public void rowIdsByMergedOrdinalThrowsOnWrongMapperCount() throws IOException
    {
        postings.put(new VectorHandle(0, 0), 0);
        // sourceCount() == 3, so passing 2 mappers must throw
        List<OrdinalMapper> twoMappers = List.of(identityMapper(), identityMapper());
        postings.rowIdsByMergedOrdinal(twoMappers);
    }

    @Test
    public void rowIdsByMergedOrdinalReturnsNullForUnmappedOrdinal() throws IOException
    {
        postings.put(new VectorHandle(0, 0), 0);
        List<OrdinalMapper> mappers = postings.ordinalMapping();
        IntFunction<IntArrayList> fn = postings.rowIdsByMergedOrdinal(mappers);
        assertNotNull(fn.apply(0));
        assertNull(fn.apply(99));
    }

    // ---- discoverStructure tests ----

    @Test
    public void discoverStructureOneToOneWhenRowIdEqualsOrdinal()
    {
        // Each row has a unique vector and the effective mapper assigns ordinal == rowId → ONE_TO_ONE.
        postings.put(new VectorHandle(0, 0), 0);
        postings.put(new VectorHandle(0, 1), 1);
        List<OrdinalMapper> mappers = postings.ordinalMapping();
        assertEquals(Structure.ONE_TO_ONE, postings.discoverStructure(mappers));
    }

    @Test
    public void discoverStructureZeroOrOneToManyWhenReordered()
    {
        // The effective mapper swaps the two ordinals: rowId 0 → ordinal 1 and rowId 1 → ordinal 0.
        // discoverStructure must see that rowId != ordinal and return ZERO_OR_ONE_TO_MANY.
        postings.put(new VectorHandle(0, 0), 0);
        postings.put(new VectorHandle(0, 1), 1);
        // Build a mapper that inverts the natural assignment: nodeId 0 → ordinal 1, nodeId 1 → ordinal 0.
        Map<Integer, Integer> swapped = new HashMap<>();
        swapped.put(0, 1);
        swapped.put(1, 0);
        List<OrdinalMapper> reorderedMappers = List.of(
                new OrdinalMapper.MapMapper(swapped),
                new OrdinalMapper.MapMapper(new HashMap<>()),
                new OrdinalMapper.MapMapper(new HashMap<>()));
        assertEquals(Structure.ZERO_OR_ONE_TO_MANY, postings.discoverStructure(reorderedMappers));
    }

    @Test
    public void discoverStructureZeroOrOneToManyWhenGapInRowIds()
    {
        // Row 1 is missing (no vector); maxRowId=2 but totalRows=2 → gap detected → ZERO_OR_ONE_TO_MANY.
        postings.put(new VectorHandle(0, 0), 0);
        postings.put(new VectorHandle(0, 1), 2);
        List<OrdinalMapper> mappers = postings.ordinalMapping();
        assertEquals(Structure.ZERO_OR_ONE_TO_MANY, postings.discoverStructure(mappers));
    }

    @Test
    public void discoverStructureZeroOrOneToManyWhenDuplicateVector()
    {
        // hasMultiRowVectors tracking: second put() to the same handle sets the flag → ZERO_OR_ONE_TO_MANY.
        VectorHandle h = new VectorHandle(0, 0);
        postings.put(h, 0);
        postings.put(h, 5);
        List<OrdinalMapper> mappers = postings.ordinalMapping();
        assertEquals(Structure.ZERO_OR_ONE_TO_MANY, postings.discoverStructure(mappers));
    }

    // ---- helper factories ----
    /** Returns an OrdinalMapper that maps nothing (empty). */
    private static OrdinalMapper identityMapper()
    {
        return new OrdinalMapper.MapMapper(new HashMap<>());
    }

    private void assertRejected(VectorHandle handle)
    {
        try
        {
            postings.put(handle, 0);
            fail("expected " + handle + " to be rejected");
        }
        catch (IllegalArgumentException _)
        {
            // expected
        }
    }
}
