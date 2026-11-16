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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;

public class SmallestFirstRowIdsTest extends SaiRandomizedTest
{
    @Test
    public void testKeepsTheSmallest()
    {
        SmallestFirstRowIds smallest = new SmallestFirstRowIds(new int[]{ 9, 4, 7 }, new long[]{ 109, 104, 107 }, 3);
        assertFalse(smallest.leftOut());
        assertEquals(9, smallest.largestFirstRowId());

        smallest.offer(12, 112);
        assertTrue(smallest.leftOut());
        assertEquals(9, smallest.largestFirstRowId());

        smallest.offer(5, 105);
        assertEquals(7, smallest.largestFirstRowId());
        smallest.offer(7, 207);
        assertEquals(7, smallest.largestFirstRowId());
        smallest.offer(0, 100);
        assertEquals(5, smallest.largestFirstRowId());

        assertEquals(List.of(100L, 104L, 105L), sortedOffsets(smallest));
    }

    @Test
    public void testEmptyPostingListsAreNotLeftOut()
    {
        int empty = PostingList.END_OF_STREAM;
        SmallestFirstRowIds smallest = new SmallestFirstRowIds(new int[]{ empty, 4, empty }, new long[]{ 1, 2, 3 }, 3);
        smallest.offer(empty, 4);
        smallest.offer(8, 5);
        smallest.offer(6, 6);
        assertFalse(smallest.leftOut());
        assertEquals(8, smallest.largestFirstRowId());
        assertEquals(List.of(2L, 5L, 6L), sortedOffsets(smallest));

        smallest.offer(empty, 7);
        assertFalse(smallest.leftOut());
        smallest.offer(8, 8);
        assertTrue(smallest.leftOut());
    }

    @Test
    public void testRandom()
    {
        for (int round = 0; round < 200; round++)
        {
            int size = between(1, 40);
            int lists = between(size, 200);
            int maxRowId = between(1, 300);

            // the first row id of a list is recoverable from its offset
            int[] all = new int[lists];
            for (int i = 0; i < lists; i++)
                all[i] = between(0, 20) == 0 ? PostingList.END_OF_STREAM : between(0, maxRowId);

            int[] firstRowIds = Arrays.copyOf(all, size);
            long[] offsets = new long[size];
            for (int i = 0; i < size; i++)
                offsets[i] = i;
            SmallestFirstRowIds smallest = new SmallestFirstRowIds(firstRowIds, offsets, size);
            for (int i = size; i < lists; i++)
                smallest.offer(all[i], i);

            int[] sorted = all.clone();
            Arrays.sort(sorted);
            int[] kept = new int[size];
            for (int i = 0; i < size; i++)
            {
                kept[i] = smallest.firstRowId(i);
                assertEquals(all[(int) smallest.offset(i)], kept[i]);
            }
            Arrays.sort(kept);
            assertArrayEquals(Arrays.copyOf(sorted, size), kept);
            assertEquals(sorted[size - 1], smallest.largestFirstRowId());
            assertEquals(lists > size && sorted[size] != PostingList.END_OF_STREAM, smallest.leftOut());
            List<Long> offsetsKept = sortedOffsets(smallest);
            assertEquals(offsetsKept.size(), offsetsKept.stream().distinct().count());
        }
    }

    private static List<Long> sortedOffsets(SmallestFirstRowIds smallest)
    {
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < smallest.size(); i++)
            if (smallest.firstRowId(i) != PostingList.END_OF_STREAM)
                offsets.add(smallest.offset(i));
        offsets.sort(null);
        return offsets;
    }
}
