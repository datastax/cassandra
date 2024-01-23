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

package org.apache.cassandra.index.sai.disk.v2.hnsw;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.lucene.util.hnsw.NeighborQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests to improve coverage for code that isn't exercised in end to end testing
 */
public class ReorderingNodeScoresIteratorTest
{
    @Test
    public void ensureCorrectOrderTest()
    {
        // Use false here because that is what lucene does, and we're verifying NodeScoresIterator correctly
        // handles the ordering
        var queue = new NeighborQueue(5, false);
        // Add nodes out of order (the queue should sort them)
        queue.add(1, 0.1f);
        queue.add(4, 0.4f);
        queue.add(3, 0.3f);
        queue.add(2, 0.2f);
        queue.add(5, 0.5f);

        // Peek at the top node and verify it is the one with the lowest score
        assertEquals(1, queue.topNode());

        var nodeScoresIterator = Iterators.peekingIterator(new CassandraOnDiskHnsw.ReorderingNodeScoresIterator(queue));

        // Ensure correct ordering
        assertTrue(nodeScoresIterator.hasNext());
        assertEquals(5, nodeScoresIterator.peek().node);
        assertEquals(0.5f, nodeScoresIterator.next().score, 0.0f);
        assertTrue(nodeScoresIterator.hasNext());
        assertEquals(4, nodeScoresIterator.peek().node);
        assertEquals(0.4f, nodeScoresIterator.next().score, 0.0f);
        assertTrue(nodeScoresIterator.hasNext());
        assertEquals(3, nodeScoresIterator.peek().node);
        assertEquals(0.3f, nodeScoresIterator.next().score, 0.0f);
        assertTrue(nodeScoresIterator.hasNext());
        assertEquals(2, nodeScoresIterator.peek().node);
        assertEquals(0.2f, nodeScoresIterator.next().score, 0.0f);
        assertTrue(nodeScoresIterator.hasNext());
        assertEquals(1, nodeScoresIterator.peek().node);
        assertEquals(0.1f, nodeScoresIterator.next().score, 0.0f);
        assertFalse(nodeScoresIterator.hasNext());
    }

    @Test
    public void ensureHandlesEmptyQueueTest()
    {
        // Use false here because that is what lucene does, and we're verifying NodeScoresIterator correctly
        // handles the ordering
        var queue = new NeighborQueue(1, false);

        var nodeScoresIterator = Iterators.peekingIterator(new CassandraOnDiskHnsw.ReorderingNodeScoresIterator(queue));

        assertFalse(nodeScoresIterator.hasNext());
    }
}
