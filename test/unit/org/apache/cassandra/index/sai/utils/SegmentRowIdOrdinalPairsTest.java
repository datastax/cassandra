/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.utils;

import io.github.jbellis.jvector.graph.NodeQueue;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SegmentRowIdOrdinalPairsTest
{
    @Test
    public void testBasicOperations()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(3);

        // Test initial state
        assertEquals(0, pairs.size());

        // Test adding pairs
        pairs.add(1, 10);
        pairs.add(2, 20);
        pairs.add(3, 30);

        assertEquals(3, pairs.size());

        // Test getting values
        assertEquals(1, pairs.getSegmentRowId(0));
        assertEquals(2, pairs.getSegmentRowId(1));
        assertEquals(3, pairs.getSegmentRowId(2));

        assertEquals(10, pairs.getOrdinal(0));
        assertEquals(20, pairs.getOrdinal(1));
        assertEquals(30, pairs.getOrdinal(2));
    }

    @Test
    public void testForEachOrdinal()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(3);
        pairs.add(1, 10);
        pairs.add(2, 20);
        pairs.add(3, 30);

        List<Integer> ordinals = new ArrayList<>();
        pairs.forEachOrdinal(ordinals::add);

        assertEquals(3, ordinals.size());
        assertEquals(Integer.valueOf(10), ordinals.get(0));
        assertEquals(Integer.valueOf(20), ordinals.get(1));
        assertEquals(Integer.valueOf(30), ordinals.get(2));
    }

    @Test
    public void testForEachSegmentRowIdOrdinalPair()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(3);
        pairs.add(1, 10);
        pairs.add(2, 20);
        pairs.add(3, 30);

        List<Integer> rowIds = new ArrayList<>();
        List<Integer> ordinals = new ArrayList<>();

        pairs.forEachSegmentRowIdOrdinalPair((rowId, ordinal) -> {
            rowIds.add(rowId);
            ordinals.add(ordinal);
        });

        assertEquals(3, rowIds.size());
        assertEquals(3, ordinals.size());
        assertEquals(Integer.valueOf(1), rowIds.get(0));
        assertEquals(Integer.valueOf(10), ordinals.get(0));
        assertEquals(Integer.valueOf(2), rowIds.get(1));
        assertEquals(Integer.valueOf(20), ordinals.get(1));
        assertEquals(Integer.valueOf(3), rowIds.get(2));
        assertEquals(Integer.valueOf(30), ordinals.get(2));
    }

    @Test
    public void testGetSegmentRowIdAndOrdinalBoundaryChecks()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(2);
        pairs.add(1, 10);

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> pairs.getSegmentRowId(-1));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> pairs.getSegmentRowId(1));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> pairs.getOrdinal(-1));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> pairs.getOrdinal(1));
    }

    @Test
    public void testAddToFullArray()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(1);
        pairs.add(1, 10);
        assertThrows(IndexOutOfBoundsException.class, () -> pairs.add(2, 20));
    }

    @Test
    public void testCapacityTooLarge()
    {
        assertThrows(AssertionError.class, () -> new SegmentRowIdOrdinalPairs(Integer.MAX_VALUE / 2 + 1));
    }

    @Test
    public void testOperationsOnEmptyArray()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(1);
        AtomicInteger count = new AtomicInteger(0);

        pairs.forEachOrdinal(i -> count.incrementAndGet());
        assertEquals(0, count.get());

        pairs.forEachSegmentRowIdOrdinalPair((x, y) -> count.incrementAndGet());
        assertEquals(0, count.get());
    }

    @Test
    public void testZeroCapacity()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(0);
        assertEquals(0, pairs.size());
        assertThrows(IndexOutOfBoundsException.class, () -> pairs.add(1, 10));
    }

    @Test
    public void testMapToSegmentRowIdScoreIterator()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(3);
        pairs.add(1, 10);
        pairs.add(2, 20);
        pairs.add(3, 30);

        // Create a simple score function that returns the ordinal value divided by 10 as the score
        ScoreFunction.ExactScoreFunction scoreFunction = ordinal -> ordinal / 10.0f;

        NodeQueue.NodeScoreIterator iterator = pairs.mapToSegmentRowIdScoreIterator(scoreFunction);

        // Test first pair
        assertTrue(iterator.hasNext());
        assertEquals(1.0f, iterator.topScore(), 0.001f);
        assertEquals(1, iterator.pop());

        // Test second pair
        assertTrue(iterator.hasNext());
        assertEquals(2.0f, iterator.topScore(), 0.001f);
        assertEquals(2, iterator.pop());

        // Test third pair
        assertTrue(iterator.hasNext());
        assertEquals(3.0f, iterator.topScore(), 0.001f);
        assertEquals(3, iterator.pop());

        // Test end of iteration
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testMapToIndexScoreIterator()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(3);
        pairs.add(1, 10);
        pairs.add(2, 20);
        pairs.add(3, 30);

        // Create a simple score function that returns the ordinal value divided by 10 as the score
        ScoreFunction.ExactScoreFunction scoreFunction = ordinal -> ordinal / 10.0f;

        NodeQueue.NodeScoreIterator iterator = pairs.mapToIndexScoreIterator(scoreFunction);

        // Test first pair
        assertTrue(iterator.hasNext());
        assertEquals(1.0f, iterator.topScore(), 0.001f);
        assertEquals(0, iterator.pop());

        // Test second pair
        assertTrue(iterator.hasNext());
        assertEquals(2.0f, iterator.topScore(), 0.001f);
        assertEquals(1, iterator.pop());

        // Test third pair
        assertTrue(iterator.hasNext());
        assertEquals(3.0f, iterator.topScore(), 0.001f);
        assertEquals(2, iterator.pop());

        // Test end of iteration
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testEmptyIterators()
    {
        SegmentRowIdOrdinalPairs pairs = new SegmentRowIdOrdinalPairs(0);
        ScoreFunction.ExactScoreFunction scoreFunction = ordinal -> ordinal / 10.0f;

        NodeQueue.NodeScoreIterator segmentRowIdIterator = pairs.mapToSegmentRowIdScoreIterator(scoreFunction);
        assertFalse(segmentRowIdIterator.hasNext());

        NodeQueue.NodeScoreIterator indexIterator = pairs.mapToIndexScoreIterator(scoreFunction);
        assertFalse(indexIterator.hasNext());
    }
}
