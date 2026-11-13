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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;

public class SortedRowIdsBuilderTest extends SaiRandomizedTest
{
    @Test
    public void testEmpty() throws IOException
    {
        SortedRowIdsBuilder builder = new SortedRowIdsBuilder(Integer.MAX_VALUE - 1);
        assertTrue(builder.isEmpty());
        assertSame(PostingList.EMPTY, builder.build());
    }

    @Test
    public void testSortsAndRemovesDuplicates() throws IOException
    {
        SortedRowIdsBuilder builder = new SortedRowIdsBuilder(Integer.MAX_VALUE - 1);
        for (int rowId : new int[]{ 7, 3, 3, 0, 12, 7, 5, Integer.MAX_VALUE - 1, 0 })
            builder.add(rowId);
        assertFalse(builder.isEmpty());

        PostingList postings = builder.build();
        assertEquals(6, postings.size());
        assertEquals(List.of(0, 3, 5, 7, 12, Integer.MAX_VALUE - 1), drain(postings));
        assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
        assertEquals(PostingList.END_OF_STREAM, postings.advance(0));
    }

    @Test
    public void testAlreadySorted() throws IOException
    {
        SortedRowIdsBuilder builder = new SortedRowIdsBuilder(Integer.MAX_VALUE - 1);
        for (int rowId = 0; rowId < 1000; rowId++)
        {
            builder.add(rowId * 2);
            builder.add(rowId * 2);
        }
        PostingList postings = builder.build();
        assertEquals(1000, postings.size());
        for (int rowId = 0; rowId < 1000; rowId++)
            assertEquals(rowId * 2, postings.nextPosting());
        assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
    }

    @Test
    public void testAdvance() throws IOException
    {
        SortedRowIdsBuilder builder = new SortedRowIdsBuilder(Integer.MAX_VALUE - 1);
        for (int rowId : new int[]{ 50, 10, 30, 20, 40, 60 })
            builder.add(rowId);
        PostingList postings = builder.build();

        assertEquals(10, postings.advance(0));
        assertEquals(20, postings.advance(5)); // never returns a row id twice
        assertEquals(40, postings.advance(31));
        assertEquals(50, postings.nextPosting());
        assertEquals(60, postings.advance(60));
        assertEquals(PostingList.END_OF_STREAM, postings.advance(61));
        assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
    }

    @Test
    public void testRandom() throws IOException
    {
        for (int round = 0; round < 50; round++)
        {
            // with the small segments the row ids end up in a bit set, with the large ones they stay in the array
            int count = between(1, 5000);
            int maxRowId = between(1, 200000);
            TreeSet<Integer> expected = new TreeSet<>();
            SortedRowIdsBuilder builder = new SortedRowIdsBuilder(maxRowId);
            for (int i = 0; i < count; i++)
            {
                int rowId = between(0, maxRowId);
                expected.add(rowId);
                builder.add(rowId);
            }

            int maxArrayLength = Math.max(SortedRowIdsBuilder.MIN_ROW_IDS_IN_BIT_SET, maxRowId / 32 + 1);
            assertEquals(count > maxArrayLength, builder.usesBitSet());
            PostingList postings = builder.build();
            assertEquals(expected.size(), postings.size());

            // a mix of nextPosting and advance, against the same moves on the sorted set
            int last = -1;
            while (true)
            {
                Integer expectedRowId;
                int actual;
                if (randomBoolean())
                {
                    expectedRowId = expected.higher(last);
                    actual = postings.nextPosting();
                }
                else
                {
                    int target = last + between(-5, 200);
                    expectedRowId = expected.ceiling(Math.max(target, last + 1));
                    actual = postings.advance(target);
                }
                if (expectedRowId == null)
                {
                    assertEquals(PostingList.END_OF_STREAM, actual);
                    break;
                }
                assertEquals(expectedRowId.intValue(), actual);
                last = actual;
            }
        }
    }

    /**
     * The row ids move to a bit set when the array is as large as it, and the result is the same.
     */
    @Test
    public void testBitSet() throws IOException
    {
        int maxRowId = 100_000; // the bit set has 3126 ints
        SortedRowIdsBuilder builder = new SortedRowIdsBuilder(maxRowId);
        // descending, with duplicates
        for (int rowId = maxRowId; rowId >= 0; rowId -= 10)
        {
            builder.add(rowId);
            builder.add(rowId);
            assertEquals(2 * (maxRowId - rowId) / 10 + 2 > maxRowId / 32 + 1, builder.usesBitSet());
        }
        assertFalse(builder.isEmpty());

        PostingList postings = builder.build();
        assertEquals(maxRowId / 10 + 1, postings.size());
        assertEquals(0, postings.nextPosting());
        assertEquals(10, postings.advance(0)); // never returns a row id twice
        assertEquals(50, postings.advance(41));
        assertEquals(60, postings.nextPosting());
        assertEquals(maxRowId, postings.advance(maxRowId - 9));
        assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
        assertEquals(PostingList.END_OF_STREAM, postings.advance(0));

        // row ids above the expected maximum
        builder = new SortedRowIdsBuilder(10);
        for (int i = 0; i <= SortedRowIdsBuilder.MIN_ROW_IDS_IN_BIT_SET; i++)
            builder.add(i % 5);
        assertTrue(builder.usesBitSet());
        builder.add(5000);
        builder.add(5_000_000);
        postings = builder.build();
        assertEquals(List.of(0, 1, 2, 3, 4, 5000, 5_000_000), drain(postings));
        assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
    }

    private static List<Integer> drain(PostingList postings) throws IOException
    {
        List<Integer> rowIds = new ArrayList<>();
        for (int rowId = postings.nextPosting(); rowId != PostingList.END_OF_STREAM; rowId = postings.nextPosting())
            rowIds.add(rowId);
        return rowIds;
    }
}
