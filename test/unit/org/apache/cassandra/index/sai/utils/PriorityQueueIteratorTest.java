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

package org.apache.cassandra.index.sai.utils;

import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class PriorityQueueIteratorTest
{
    @Test
    public void testEmptyPriorityQueue()
    {
        // Should work for an empty pq
        try (var iter = new PriorityQueueIterator<>(new PriorityQueue<>()))
        {
            assertFalse(iter.hasNext());
            assertThrows(NoSuchElementException.class, iter::next);
        }
    }

    @Test
    public void testPriorityQueueIteratorProducesElementsInPriorityOrder()
    {
        var pq = new PriorityQueue<>();
        // Add elements out of order
        pq.add(2);
        pq.add(1);
        pq.add(4);
        pq.add(3);
        // Should work for an empty pq
        try (var iter = new PriorityQueueIterator<>(pq))
        {
            assertEquals(1, iter.next());
            assertEquals(2, iter.next());
            assertEquals(3, iter.next());
            assertEquals(4, iter.next());
            assertFalse(iter.hasNext());
            assertThrows(NoSuchElementException.class, iter::next);
        }
    }
}
