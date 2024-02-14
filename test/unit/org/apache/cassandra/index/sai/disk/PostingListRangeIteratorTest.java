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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.v1.kdtree.KDTreeIndexBuilder;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PostingListRangeIteratorTest
{
    private static final PrimaryKeyMap pkm = KDTreeIndexBuilder.TEST_PRIMARY_KEY_MAP;

    @Test
    public void testRemoveDuplicatePostings() throws IOException
    {
        @SuppressWarnings("resource")
        var postingList = new ArrayPostingList(new int[]{1,1,2,2,3});
        var mockIndexContext = mock(IndexContext.class);
        var indexContext = new IndexSearcherContext(pkm.primaryKeyFromRowId(1),
                                                    pkm.primaryKeyFromRowId(3),
                                                    0,
                                                    3,
                                                    0,
                                                    new QueryContext(10000),
                                                    postingList.peekable());
        try (var iterator = new PostingListRangeIterator(mockIndexContext, pkm, indexContext))
        {
            assertEquals(pkm.primaryKeyFromRowId(1), iterator.next());
            assertEquals(pkm.primaryKeyFromRowId(2), iterator.next());
            assertEquals(pkm.primaryKeyFromRowId(3), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void testContrivedScenariosUnion() throws IOException
    {
        var postingList1 = new ArrayPostingList(new int[]{3});
        var postingList2 = new ArrayPostingList(new int[]{1});
        var postingList3 = new ArrayPostingList(new int[]{3});
        var mockIndexContext = mock(IndexContext.class);
        var mpl = MergePostingList.merge(Lists.newArrayList(postingList1.peekable(), postingList2.peekable()));
        var indexContext1 = buildIndexContext(1, 3, mpl.peekable());
        var indexContext2 = buildIndexContext(3, 3, postingList3.peekable());
        var plri1 = new PostingListRangeIterator(mockIndexContext, pkm, indexContext1);
        var plri2 = new PostingListRangeIterator(mockIndexContext, pkm, indexContext2);
        try (var union = RangeUnionIterator.builder().add(plri1).add(plri2).build();)
        {
            union.skipTo(new Murmur3Partitioner.LongToken(2));
            assertTrue(union.hasNext());
            union.next();
            union.skipTo(new Murmur3Partitioner.LongToken(3));
            assertFalse(union.hasNext());
        }
    }

    @Test
    public void testSeveralIntersectionsOnSameIterator() throws IOException
    {
        @SuppressWarnings("resource")
        var postingList = new ArrayPostingList(new int[]{1,2,4,7,10});
        var mockIndexContext = mock(IndexContext.class);
        var indexContext = new IndexSearcherContext(pkm.primaryKeyFromRowId(1),
                                                    pkm.primaryKeyFromRowId(11),
                                                    1,
                                                    11,
                                                    0,
                                                    new QueryContext(10000),
                                                    postingList.peekable());
        try (var iterator = new PostingListRangeIterator(mockIndexContext, pkm, indexContext))
        {
            assertEquals(RangeIterator.IntersectionResult.MISS, iterator.intersect(pkm.primaryKeyFromRowId(0)));
            assertEquals(RangeIterator.IntersectionResult.MATCH, iterator.intersect(pkm.primaryKeyFromRowId(1)));
            assertEquals(RangeIterator.IntersectionResult.MATCH, iterator.intersect(pkm.primaryKeyFromRowId(4)));
            assertEquals(RangeIterator.IntersectionResult.MISS, iterator.intersect(pkm.primaryKeyFromRowId(5)));
            assertEquals(RangeIterator.IntersectionResult.MISS, iterator.intersect(pkm.primaryKeyFromRowId(6)));
            assertEquals(RangeIterator.IntersectionResult.MATCH, iterator.intersect(pkm.primaryKeyFromRowId(7)));
            assertEquals(RangeIterator.IntersectionResult.EXHAUSTED, iterator.intersect(pkm.primaryKeyFromRowId(11)));
            // Calling after we've already exhausted must not throw an exception.
            assertEquals(RangeIterator.IntersectionResult.EXHAUSTED, iterator.intersect(pkm.primaryKeyFromRowId(12)));
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testMixingIntersectionNextAndSkipToOnSameIterator() throws IOException
    {
        @SuppressWarnings("resource")
        var postingList = new ArrayPostingList(new int[]{1,2,4,7,9,10});
        var mockIndexContext = mock(IndexContext.class);
        var indexContext = new IndexSearcherContext(pkm.primaryKeyFromRowId(1),
                                                    pkm.primaryKeyFromRowId(11),
                                                    1,
                                                    11,
                                                    0,
                                                    new QueryContext(10000),
                                                    postingList.peekable());
        try (var iterator = new PostingListRangeIterator(mockIndexContext, pkm, indexContext))
        {
            assertEquals(RangeIterator.IntersectionResult.MISS, iterator.intersect(pkm.primaryKeyFromRowId(0)));
            assertTrue(iterator.hasNext());
            assertEquals(pkm.primaryKeyFromRowId(1), iterator.next());
            // Intersecting with 4 consumes it, so the next entry is 7
            assertEquals(RangeIterator.IntersectionResult.MATCH, iterator.intersect(pkm.primaryKeyFromRowId(4)));
            assertTrue(iterator.hasNext());
            assertEquals(pkm.primaryKeyFromRowId(7), iterator.next());
            // Skipping to 9 doesn't consume it
            iterator.skipTo(new Murmur3Partitioner.LongToken(9));
            assertEquals(RangeIterator.IntersectionResult.MATCH, iterator.intersect(pkm.primaryKeyFromRowId(9)));
            assertTrue(iterator.hasNext());
            assertEquals(pkm.primaryKeyFromRowId(10), iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    private IndexSearcherContext buildIndexContext(int minRowId, int maxRowId, PostingList.PeekablePostingList list) throws IOException
    {
        return new IndexSearcherContext(pkm.primaryKeyFromRowId(minRowId),
                                        pkm.primaryKeyFromRowId(maxRowId),
                                        minRowId,
                                        maxRowId,
                                        0,
                                        new QueryContext(10000),
                                        list);
    }
}
