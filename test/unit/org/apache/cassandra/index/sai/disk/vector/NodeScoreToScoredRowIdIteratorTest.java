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

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import org.junit.Test;

import io.github.jbellis.jvector.graph.SearchResult;
import org.apache.cassandra.utils.CloseableIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NodeScoreToScoredRowIdIteratorTest
{
    @Test
    public void testEmptyIterator()
    {
        var rowIdsView = new CustomRowIdsView();
        var iter = new NodeScoreToScoredRowIdIterator(CloseableIterator.emptyIterator(), rowIdsView);
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
        assertFalse(rowIdsView.isClosed);
        iter.close();
        assertTrue(rowIdsView.isClosed);
    }

    @Test
    public void testIterator()
    {
        var rowIdsView = new CustomRowIdsView();
        // Note that the score is ignored at this stage because NodeScores are assumed to be in order
        var nodeScores = IntStream.range(0, 3).mapToObj(i -> new SearchResult.NodeScore(i, 1f)).iterator();
        var iter = new NodeScoreToScoredRowIdIterator(CloseableIterator.wrap(nodeScores), rowIdsView);

        assertTrue(iter.hasNext());
        // See CustomRowIdsView for the mapping
        assertEquals(1, iter.next().segmentRowId);
        assertEquals(3, iter.next().segmentRowId);
        assertEquals(4, iter.next().segmentRowId);
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
        assertFalse(rowIdsView.isClosed);
        iter.close();
        assertTrue(rowIdsView.isClosed);
    }

    private static class CustomRowIdsView implements RowIdsView
    {
        boolean isClosed = false;

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal)
        {
            if (vectorOrdinal == 0)
                return IntStream.empty().iterator();
            else if (vectorOrdinal == 1)
                return IntStream.range(1, 2).iterator();
            else if (vectorOrdinal == 2)
                return IntStream.range(3, 5).iterator();
            else
                throw new IllegalArgumentException("Unexpected vector ordinal: " + vectorOrdinal);
        }

        @Override
        public void close()
        {
            isClosed = true;
        }
    }

}
