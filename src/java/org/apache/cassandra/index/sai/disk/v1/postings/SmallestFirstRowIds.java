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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;

/**
 * Keeps, out of any number of posting lists, a fixed number of them: those with the smallest first row ids. A
 * posting list is represented by its first row id and by the offset of its summary in the postings file.
 * <p>
 * It is a binary heap with the largest first row id at the top, which is the one replaced when a posting list with
 * a smaller first row id is offered. The empty posting lists, whose first row id is
 * {@link PostingList#END_OF_STREAM}, are the first to go.
 */
@NotThreadSafe
public class SmallestFirstRowIds
{
    private final int[] firstRowIds;
    private final long[] offsets;
    private final int size;
    private boolean leftOut;

    /**
     * @param firstRowIds the first row ids of the initial posting lists; the array is used, not copied
     * @param offsets the offsets of the same posting lists; the array is used, not copied
     * @param size the number of posting lists to keep, which is also the number of the initial ones; not zero
     */
    public SmallestFirstRowIds(int[] firstRowIds, long[] offsets, int size)
    {
        assert size > 0 && size <= firstRowIds.length && size <= offsets.length;
        this.firstRowIds = firstRowIds;
        this.offsets = offsets;
        this.size = size;
        for (int i = (size >>> 1) - 1; i >= 0; i--)
            siftDown(i);
    }

    /**
     * Offers a posting list, which replaces the kept one with the largest first row id if its own is smaller.
     */
    public void offer(int firstRowId, long offset)
    {
        if (firstRowId == PostingList.END_OF_STREAM)
            return;

        int largest = firstRowIds[0];
        if (firstRowId >= largest)
        {
            leftOut = true;
            return;
        }

        leftOut |= largest != PostingList.END_OF_STREAM;
        firstRowIds[0] = firstRowId;
        offsets[0] = offset;
        siftDown(0);
    }

    /**
     * @return whether a posting list that is not empty was left out. If so, all the row ids of the posting lists
     * left out are greater than or equal to {@link #largestFirstRowId()}, which is not
     * {@link PostingList#END_OF_STREAM}.
     */
    public boolean leftOut()
    {
        return leftOut;
    }

    /**
     * @return the largest first row id of the posting lists kept
     */
    public int largestFirstRowId()
    {
        return firstRowIds[0];
    }

    /**
     * @return the number of posting lists kept, some of which can be empty
     */
    public int size()
    {
        return size;
    }

    /**
     * @return the first row id of a posting list kept, in no particular order
     */
    public int firstRowId(int index)
    {
        return firstRowIds[index];
    }

    /**
     * @return the offset of a posting list kept, in no particular order
     */
    public long offset(int index)
    {
        return offsets[index];
    }

    private void siftDown(int index)
    {
        int firstRowId = firstRowIds[index];
        long offset = offsets[index];
        while (true)
        {
            int child = 2 * index + 1;
            if (child >= size)
                break;
            if (child + 1 < size && firstRowIds[child + 1] > firstRowIds[child])
                child++;
            if (firstRowIds[child] <= firstRowId)
                break;
            firstRowIds[index] = firstRowIds[child];
            offsets[index] = offsets[child];
            index = child;
        }
        firstRowIds[index] = firstRowId;
        offsets[index] = offset;
    }
}
