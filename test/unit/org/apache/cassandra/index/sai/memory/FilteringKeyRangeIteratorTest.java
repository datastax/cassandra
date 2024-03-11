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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v2.RowAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
public class FilteringKeyRangeIteratorTest
{

    protected PrimaryKey.Factory primaryKeyFactory;

    @Before
    public void setup()
    {
        primaryKeyFactory = new RowAwarePrimaryKeyFactory(SAITester.EMPTY_COMPARATOR);
    }

    @Test
    public void testFilteringKeyRangeIterator() throws Exception
    {
        var keys = new TreeSet<PrimaryKey>();
        for (long token = 0; token < 100; token++)
            keys.add(keyForToken(token));
        var bounds = AbstractBounds.bounds(decoratedKeyForToken(20), true, decoratedKeyForToken(50), true);
        try (var iter = new FilteringKeyRangeIterator(keys, bounds))
        {
            for (long token = 20; token <= 50; token++)
            {
                assertTrue(iter.hasNext());
                assertEquals(token, iter.next().token().getLongValue());
            }
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testIntersectingTwoFilteringKeyRangeIterators() throws Exception
    {
        var keys = new TreeSet<PrimaryKey>();
        for (long token = 0; token < 100; token++)
            keys.add(keyForToken(token));
        var iter1 = new FilteringKeyRangeIterator(keys, boundsInclusive(20, 60));
        var iter2 = new FilteringKeyRangeIterator(keys, boundsInclusive(30, 50));
        try (var intersection = RangeIntersectionIterator.builder().add(iter1).add(iter2).build())
        {
            // Do an initial skip to that shouldn't change the result
            intersection.skipTo(new Murmur3Partitioner.LongToken(10));
            for (long token = 30; token <= 40; token++)
            {
                assertTrue(intersection.hasNext());
                assertEquals(token, intersection.next().token().getLongValue());
            }
            // Do skip 5 tokens ahead, which should skip over the intersection
            intersection.skipTo(new Murmur3Partitioner.LongToken(45));
            for (long token = 45; token <= 50; token++)
            {
                assertTrue(intersection.hasNext());
                assertEquals(token, intersection.next().token().getLongValue());
            }

            assertFalse(intersection.hasNext());
        }
    }

    private PrimaryKey keyForToken(long token)
    {
        return primaryKeyFactory.create(decoratedKeyForToken(token), Clustering.EMPTY);
    }

    private AbstractBounds<PartitionPosition> boundsInclusive(long start, long end)
    {
        return AbstractBounds.bounds(decoratedKeyForToken(start), true, decoratedKeyForToken(end), true);
    }

    private DecoratedKey decoratedKeyForToken(long token)
    {
        var buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, token);
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(token), buffer);
    }
}
