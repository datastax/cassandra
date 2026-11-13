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

import java.util.Arrays;
import java.util.BitSet;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.PostingList;

/**
 * Collects the row ids of many posting lists and exposes their union as a single {@link PostingList}.
 * <p>
 * A query over a range of terms merges one posting list per term. Keeping a reader open for each of them costs
 * a few hundred bytes per term, without bound when the range is wide. Here the posting lists are read to the end
 * when their term is found, see {@link EagerPostingsReader}.
 * <p>
 * The memory is bounded by the size of the segment, not by the number of row ids collected: they are kept in an
 * array, four bytes each, until the array is a quarter of a bit set over all the row ids of the segment, and from
 * there in that bit set. That is one bit per row of the segment at most, 12.5 MB for 100 million rows.
 */
@NotThreadSafe
public class SortedRowIdsBuilder
{
    private static final int INITIAL_CAPACITY = 16;

    /** Below this number of row ids the array is too small for a bit set to be worth it */
    @VisibleForTesting
    static final int MIN_ROW_IDS_IN_BIT_SET = 1024;

    private final int maxRowId;
    /** The length at which the array is left for the bit set */
    private final int maxArrayLength;

    private int[] rowIds = new int[INITIAL_CAPACITY];
    private int count;
    private boolean sorted = true;

    /** Replaces the array once it has reached its maximum length */
    private BitSet bits;

    /**
     * @param maxRowId the largest row id of the segment, which sizes the bit set
     */
    public SortedRowIdsBuilder(int maxRowId)
    {
        this.maxRowId = maxRowId;
        this.maxArrayLength = maxArrayLength(maxRowId);
    }

    /**
     * The array is left for the bit set when it has one row id out of 128 of the segment: it is then a quarter
     * of the size of the bit set, which bounds what is allocated while both exist.
     */
    @VisibleForTesting
    static int maxArrayLength(int maxRowId)
    {
        return Math.max(MIN_ROW_IDS_IN_BIT_SET, (maxRowId >>> 7) + 1);
    }

    public void add(int rowId)
    {
        if (bits != null)
        {
            bits.set(rowId);
            return;
        }

        if (count == rowIds.length)
        {
            if (count >= maxArrayLength)
            {
                switchToBitSet();
                bits.set(rowId);
                return;
            }
            rowIds = Arrays.copyOf(rowIds, (int) Math.min(count * 2L, maxArrayLength));
        }
        if (count > 0 && rowId < rowIds[count - 1])
            sorted = false;
        rowIds[count++] = rowId;
    }

    private void switchToBitSet()
    {
        // the bit set grows by itself, should a row id be above the expected maximum
        bits = new BitSet(maxRowId + 1);
        for (int i = 0; i < count; i++)
            bits.set(rowIds[i]);
        rowIds = null;
        count = 0;
    }

    public boolean isEmpty()
    {
        return count == 0 && bits == null;
    }

    @VisibleForTesting
    boolean usesBitSet()
    {
        return bits != null;
    }

    /**
     * @return the collected row ids in ascending order and without duplicates, {@link PostingList#EMPTY} if there are
     * none. This builder must not be used afterwards.
     */
    public PostingList build()
    {
        if (bits != null)
            return new BitSetRowIds(bits);

        if (count == 0)
            return PostingList.EMPTY;

        if (!sorted)
            Arrays.sort(rowIds, 0, count);

        int unique = 1;
        for (int i = 1; i < count; i++)
        {
            if (rowIds[i] != rowIds[unique - 1])
                rowIds[unique++] = rowIds[i];
        }
        return new SortedRowIds(rowIds, unique);
    }

    private static class BitSetRowIds implements PostingList
    {
        private final BitSet bits;
        private final int size;
        /** The first row id that was not returned yet, if it is set */
        private int next;

        BitSetRowIds(BitSet bits)
        {
            this.bits = bits;
            this.size = bits.cardinality();
        }

        @Override
        public int nextPosting()
        {
            return advance(next);
        }

        @Override
        public int advance(int targetRowID)
        {
            if (next < 0)
                return END_OF_STREAM;

            int rowId = bits.nextSetBit(Math.max(next, targetRowID));
            if (rowId < 0)
            {
                next = -1;
                return END_OF_STREAM;
            }
            next = rowId + 1; // a row id is below END_OF_STREAM, the largest int
            return rowId;
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    private static class SortedRowIds implements PostingList
    {
        private final int[] rowIds;
        private final int size;
        private int next;

        SortedRowIds(int[] rowIds, int size)
        {
            this.rowIds = rowIds;
            this.size = size;
        }

        @Override
        public int nextPosting()
        {
            return next < size ? rowIds[next++] : END_OF_STREAM;
        }

        @Override
        public int advance(int targetRowID)
        {
            if (next >= size)
                return END_OF_STREAM;

            // the target is usually close: look at the next row id before searching
            if (rowIds[next] < targetRowID)
            {
                int found = Arrays.binarySearch(rowIds, next + 1, size, targetRowID);
                next = found >= 0 ? found : -found - 1;
            }
            return nextPosting();
        }

        @Override
        public int size()
        {
            return size;
        }
    }
}
