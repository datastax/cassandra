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

import java.util.ArrayList;

import org.junit.Test;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.disk.v2.RowAwarePrimaryKeyFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataRangeFilterIteratorTest
{
    private final RowAwarePrimaryKeyFactory primaryKeyFactory = new RowAwarePrimaryKeyFactory(new ClusteringComparator());

    @Test
    public void testEmptyInputIterator()
    {
        var dataRanges = new ArrayList<DataRange>();
        dataRanges.add(DataRange.allData(Murmur3Partitioner.instance));
        try (var filterIterator = new DataRangeFilterIterator(dataRanges, primaryKeyFactory, RangeIterator.empty()))
        {
            assertFalse(filterIterator.hasNext());
        }
    }

    @Test
    public void testExpectWholeInputIterator()
    {
        var dataRanges = new ArrayList<DataRange>();
        dataRanges.add(DataRange.allData(Murmur3Partitioner.instance));
        var iter = new LongIterator(new long[]{1, 2, 3, 4, 5});
        try (var filterIterator = new DataRangeFilterIterator(dataRanges, primaryKeyFactory, iter))
        {
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(1), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(2), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(3), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(4), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(5), filterIterator.next());
            assertFalse(filterIterator.hasNext());
        }
    }

    @Test
    public void testExpectParitialResult()
    {
        var dataRanges = new ArrayList<DataRange>();
        dataRanges.add(buildDataRange(2, 4));
        var iter = new LongIterator(new long[]{1, 2, 3, 4, 5});
        try (var filterIterator = new DataRangeFilterIterator(dataRanges, primaryKeyFactory, iter))
        {
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(3), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(4), filterIterator.next());
        }
    }

    @Test
    public void testMultipleDataRanges()
    {
        var dataRanges = new ArrayList<DataRange>();
        dataRanges.add(buildDataRange(1, 3));
        dataRanges.add(buildDataRange(6, 8));
        var iter = new LongIterator(new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        try (var filterIterator = new DataRangeFilterIterator(dataRanges, primaryKeyFactory, iter))
        {
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(2), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(3), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(7), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(8), filterIterator.next());
            assertFalse(filterIterator.hasNext());
        }
    }

    @Test
    public void testDisjointIteratorAndDataRange()
    {
        var dataRanges = new ArrayList<DataRange>();
        dataRanges.add(buildDataRange(4, 10));
        var iter = new LongIterator(new long[]{1,2,3,4});
        try (var filterIterator = new DataRangeFilterIterator(dataRanges, primaryKeyFactory, iter))
        {
            assertFalse(filterIterator.hasNext());
        }
    }


    @Test
    public void testSkipToForFirstPrimaryKey()
    {
        var dataRanges = new ArrayList<DataRange>();
        dataRanges.add(buildDataRange(2, 5));
        var iter = new LongIterator(new long[]{1,2,3,4});
        try (var filterIterator = new DataRangeFilterIterator(dataRanges, primaryKeyFactory, iter))
        {
            // We always skip to the first token, so just do that here
            filterIterator.skipTo(LongIterator.fromToken(2));
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(3), filterIterator.next());
            assertTrue(filterIterator.hasNext());
            assertEquals(LongIterator.fromToken(4), filterIterator.next());
            assertFalse(filterIterator.hasNext());
        }
    }

    private DataRange buildDataRange(long start, long end)
    {
        return DataRange.forTokenRange(new Range<>(LongIterator.fromToken(start).token(), LongIterator.fromToken(end).token()));
    }
}
