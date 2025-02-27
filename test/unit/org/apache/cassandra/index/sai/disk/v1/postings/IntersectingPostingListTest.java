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

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IntersectingPostingListTest extends SaiRandomizedTest
{
    private Map<ByteBuffer, PostingList> createPostingMap(PostingList... lists)
    {
        Map<ByteBuffer, PostingList> map = new HashMap<>();
        for (int i = 0; i < lists.length; i++)
        {
            map.put(ByteBufferUtil.bytes(String.valueOf((char) ('A' + i))), lists[i]);
        }
        return map;
    }

    @Test
    public void shouldIntersectOverlappingPostingLists() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{ 1, 4, 6, 8 }),
                                   new IntArrayPostingList(new int[]{ 2, 4, 6, 9 }),
                                   new IntArrayPostingList(new int[]{ 4, 6, 7 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);
        assertPostingListEquals(new IntArrayPostingList(new int[]{ 4, 6 }), intersected);
    }

    @Test
    public void shouldIntersectDisjointPostingLists() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{ 1, 3, 5 }),
                                   new IntArrayPostingList(new int[]{ 2, 4, 6 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);
        assertPostingListEquals(new IntArrayPostingList(new int[]{}), intersected);
    }

    @Test
    public void shouldIntersectSinglePostingList() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{ 1, 4, 6 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);
        assertPostingListEquals(new IntArrayPostingList(new int[]{ 1, 4, 6 }), intersected);
    }

    @Test
    public void shouldIntersectIdenticalPostingLists() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{ 1, 2, 3 }),
                                   new IntArrayPostingList(new int[]{ 1, 2, 3 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);
        assertPostingListEquals(new IntArrayPostingList(new int[]{ 1, 2, 3 }), intersected);
    }

    @Test
    public void shouldAdvanceAllIntersectedLists() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{ 1, 3, 5, 7, 9 }),
                                   new IntArrayPostingList(new int[]{ 2, 3, 5, 7, 8 }),
                                   new IntArrayPostingList(new int[]{ 3, 5, 7, 10 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);
        final PostingList expected = new IntArrayPostingList(new int[]{ 3, 5, 7 });

        assertEquals(expected.advance(5), intersected.advance(5));
        assertPostingListEquals(expected, intersected);
    }

    @Test
    public void shouldHandleEmptyList() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{}),
                                   new IntArrayPostingList(new int[]{ 1, 2, 3 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);
        assertEquals(PostingList.END_OF_STREAM, intersected.advance(1));
    }

    @Test
    public void shouldInterleaveNextAndAdvance() throws IOException
    {
        var map = createPostingMap(new IntArrayPostingList(new int[]{ 1, 3, 5, 7, 9 }),
                                   new IntArrayPostingList(new int[]{ 1, 3, 5, 7, 9 }),
                                   new IntArrayPostingList(new int[]{ 1, 3, 5, 7, 9 }));

        final PostingList intersected = IntersectingPostingList.intersect(map);

        assertEquals(1, intersected.nextPosting());
        assertEquals(5, intersected.advance(5));
        assertEquals(7, intersected.nextPosting());
        assertEquals(9, intersected.advance(9));
    }

    @Test
    public void shouldInterleaveNextAndAdvanceOnRandom() throws IOException
    {
        for (int i = 0; i < 1000; ++i)
        {
            testAdvancingOnRandom();
        }
    }

    private void testAdvancingOnRandom() throws IOException
    {
        final int postingsCount = nextInt(1, 50_000);
        final int postingListCount = nextInt(2, 10);

        final AtomicInteger rowId = new AtomicInteger();
        final int[] commonPostings = IntStream.generate(() -> rowId.addAndGet(nextInt(1, 10)))
                                              .limit(postingsCount / 4)
                                              .toArray();

        var splitPostingLists = new ArrayList<PostingList>();
        for (int i = 0; i < postingListCount; i++)
        {
            final int[] uniquePostings = IntStream.generate(() -> rowId.addAndGet(nextInt(1, 10)))
                                                  .limit(postingsCount)
                                                  .toArray();
            int[] combined = IntStream.concat(IntStream.of(commonPostings),
                                              IntStream.of(uniquePostings))
                                      .distinct()
                                      .sorted()
                                      .toArray();
            splitPostingLists.add(new IntArrayPostingList(combined));
        }

        final PostingList intersected = IntersectingPostingList.intersect(createPostingMap(splitPostingLists.toArray(new PostingList[0])));
        final PostingList expected = new IntArrayPostingList(commonPostings);

        final List<PostingListAdvance> actions = new ArrayList<>();
        for (int idx = 0; idx < commonPostings.length; idx++)
        {
            if (nextInt(0, 8) == 0)
            {
                actions.add((postingList) -> {
                    try
                    {
                        return postingList.nextPosting();
                    }
                    catch (IOException e)
                    {
                        fail(e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
            }
            else
            {
                final int skips = nextInt(0, 5);
                idx = Math.min(idx + skips, commonPostings.length - 1);
                final int rowID = commonPostings[idx];
                actions.add((postingList) -> {
                    try
                    {
                        return postingList.advance(rowID);
                    }
                    catch (IOException e)
                    {
                        fail(e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        for (PostingListAdvance action : actions)
        {
            assertEquals(action.advance(expected), action.advance(intersected));
        }
    }

    private interface PostingListAdvance
    {
        long advance(PostingList list) throws IOException;
    }
}
