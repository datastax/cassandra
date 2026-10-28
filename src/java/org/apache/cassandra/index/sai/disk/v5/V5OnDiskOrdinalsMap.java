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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.index.sai.utils.SingletonIntIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class V5OnDiskOrdinalsMap implements OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(V5OnDiskOrdinalsMap.class);

    private static final OneToOneRowIdsView ONE_TO_ONE_ROW_IDS_VIEW = new OneToOneRowIdsView();
    private static final EmptyOrdinalsView EMPTY_ORDINALS_VIEW = new EmptyOrdinalsView();
    private static final EmptyRowIdsView EMPTY_ROW_IDS_VIEW = new EmptyRowIdsView();

    private final FileHandle fh;
    private final long ordToRowOffset;
    private final long segmentEnd;
    private final int maxOrdinal;
    private final int maxRowId;
    private final long rowToOrdinalOffset;
    @VisibleForTesting
    final V5VectorPostingsWriter.Structure structure;

    private final Supplier<OrdinalsView> ordinalsViewSupplier;
    private final Supplier<RowIdsView> rowIdsViewSupplier;

    // cached values for OneToMany structure
    private Int2ObjectHashMap<int[]> extraRowsByOrdinal = null;
    private int[] extraRowIds = null;
    private int[] extraOrdinals = null;

    // immutable, I/O-free views for ZERO_OR_ONE_TO_MANY (null if hole density exceeds the threshold)
    private DeltaOrdinalsView deltaOrdinalsView = null;
    private DeltaRowIdsView deltaRowIdsView = null;


    public V5OnDiskOrdinalsMap(FileHandle fh, long segmentOffset, long segmentLength)
    {
        this.segmentEnd = segmentOffset + segmentLength;
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            reader.seek(segmentOffset);
            int magic = reader.readInt();
            if (magic != V5VectorPostingsWriter.MAGIC)
            {
                throw new RuntimeException("Invalid magic number in V5OnDiskOrdinalsMap");
            }
            this.structure = V5VectorPostingsWriter.Structure.values()[reader.readInt()];
            this.maxOrdinal = reader.readInt();
            this.maxRowId = reader.readInt();
            this.ordToRowOffset = reader.getFilePointer();
            if (structure == V5VectorPostingsWriter.Structure.ONE_TO_ONE)
            {
                this.rowToOrdinalOffset = segmentEnd;
            }
            else
            {
                reader.seek(segmentEnd - 8);
                this.rowToOrdinalOffset = reader.readLong();
            }

            if (maxOrdinal < 0)
            {
                this.rowIdsViewSupplier = () -> EMPTY_ROW_IDS_VIEW;
                this.ordinalsViewSupplier = () -> EMPTY_ORDINALS_VIEW;
            }
            else if (structure == V5VectorPostingsWriter.Structure.ONE_TO_ONE)
            {
                this.rowIdsViewSupplier = () -> ONE_TO_ONE_ROW_IDS_VIEW;
                this.ordinalsViewSupplier = () -> new OneToOneOrdinalsView(maxOrdinal + 1);
            }
            else if (structure == V5VectorPostingsWriter.Structure.ONE_TO_MANY)
            {
                cacheExtraRowIds(reader);
                cacheExtraRowOrdinals(reader);
                this.rowIdsViewSupplier = OneToManyRowIdsView::new;
                this.ordinalsViewSupplier = OneToManyOrdinalsView::new;
            }
            else if (tryBuildDeltaIndex(reader))
            {
                this.rowIdsViewSupplier = () -> deltaRowIdsView;
                this.ordinalsViewSupplier = () -> deltaOrdinalsView;
            }
            else
            {
                this.rowIdsViewSupplier = GenericRowIdsView::new;
                this.ordinalsViewSupplier = GenericOrdinalsView::new;
            }

            assert rowToOrdinalOffset <= segmentEnd : "rowOrdinalOffset " + rowToOrdinalOffset + " is not less than or equal to segmentEnd " + segmentEnd;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error initializing OnDiskOrdinalsMap at segment " + segmentOffset, e);
        }
    }

    @Override
    public V5VectorPostingsWriter.Structure getStructure()
    {
        return structure;
    }

    @Override
    public boolean requiresDiskReads()
    {
        // The views are created lazily, so we can't ask them. Only ZERO_OR_ONE_TO_MANY with a non-empty graph
        // falls back to disk-based views, and only when the in-memory delta index could not be built.
        return structure == V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY
               && maxOrdinal >= 0
               && deltaOrdinalsView == null;
    }

    private void cacheExtraRowIds(RandomAccessReader reader) throws IOException
    {
        extraRowsByOrdinal = new Int2ObjectHashMap<>();
        reader.seek(ordToRowOffset);
        int entryCount = reader.readInt();
        for (int i = 0; i < entryCount; i++)
        {
            int ordinal = reader.readInt();
            int postingsSize = reader.readInt();
            if (postingsSize > 0)
                postingsSize++; // add the ordinal itself
            int[] rowIds = new int[postingsSize];
            if (postingsSize > 0)
            {
                rowIds[0] = ordinal;
                for (int j = 1; j < postingsSize; j++)
                    rowIds[j] = reader.readInt();
            }
            extraRowsByOrdinal.put(ordinal, rowIds);
        }
    }

    private void cacheExtraRowOrdinals(RandomAccessReader reader) throws IOException
    {
        var extraRowIdsList = new IntArrayList();
        var extraOrdinalsList = new IntArrayList();
        reader.seek(rowToOrdinalOffset);
        while (reader.getFilePointer() < segmentEnd - 8)
        {
            extraRowIdsList.add(reader.readInt());
            extraOrdinalsList.add(reader.readInt());
        }

        extraRowIds = extraRowIdsList.toIntArray();
        extraOrdinals = extraOrdinalsList.toIntArray();
    }

    /**
     * Scans the dense rowId-&gt;ordinal table of a ZERO_OR_ONE_TO_MANY segment once and builds compact in-memory
     * views whose size is proportional to the number of holes h; nothing proportional to the row or ordinal count
     * is kept. If the table cannot be encoded (see below) or has too many holes, returns false and leaves the
     * delta views null, so the generic disk-based views are used.
     *
     * <h4>What a hole is</h4>
     * In the ideal case every row has its own vector and the ordinals are handed out in rowId order, so row
     * {@code r} maps to ordinal {@code r}. Every row is then the <i>primary</i> row of a new ordinal, and nothing
     * needs to be stored. A <b>hole</b> is a row that breaks this pattern by <i>not</i> introducing a new ordinal.
     * There are two kinds:
     * <ul>
     * <li>a row without a vector, which maps to ordinal {@code -1};</li>
     * <li>an <i>extra</i> row, whose vector is identical to the one of an earlier row, so it maps to that row's
     * existing ordinal (and the ordinal then has several rows).</li>
     * </ul>
     * Each hole shifts the ordinals of all later rows down by one relative to their rowIds, which is why a hole
     * is all that has to be remembered: every row that is not a hole is the primary row of the next unseen
     * ordinal, so its ordinal can be computed as {@code rowId - (number of holes before it)}. Such rows are
     * called <i>implicit</i>.
     *
     * <h4>Classifying rows</h4>
     * Walk the rows in rowId order, and let {@code expected = rowId - holesSoFar}. Invariant: before each row, the
     * ordinals seen so far are exactly {@code [0, expected)}, so {@code expected} is the next unseen ordinal.
     * Each row is then one of:
     * <ul>
     * <li>{@code ordinal == expected}: the primary row of a new ordinal. It is <i>implicit</i>: nothing is
     * stored.</li>
     * <li>{@code ordinal == -1}: a row without a vector. This is a hole.</li>
     * <li>{@code 0 <= ordinal < expected}: an extra row of an ordinal that already has a primary row. This is
     * also a hole.</li>
     * <li>{@code ordinal > expected}: an ordinal that appears before smaller ones have. The model cannot encode
     * this; the build gives up.</li>
     * </ul>
     *
     * <h4>The three arrays</h4>
     * Given this table, the holes are rows 3 and 8 (extra rows of ordinals 1 and 0) and row 4 (no vector); rows
     * 3 and 4 are consecutive holes, every other row is implicit:
     * <pre>{@code
     * rowId:     0  1  2  3   4  5  6  7  8  9
     * ordinal:   0  1  2  1  -1  3  4  5  0  6
     * }</pre>
     * we store, for the holes k = 0..2 in rowId order:
     * <pre>{@code
     * k                 0   1   2
     * rowIds[k]         3   4   8     rowId of the k-th hole (ascending)
     * vectorOrdinals[k] 1  -1   0     ordinal that row really maps to: -1, or any earlier ordinal
     * expected[k]       3   3   6     = rowIds[k] - k; computed on the fly, NOT stored
     * holesByOrdinal    2   0         indexes k with vectorOrdinals[k] >= 0, sorted by (vectorOrdinals[k], k)
     * }</pre>
     * <ul>
     * <li>{@code rowIds}: the position {@code k} of a hole is the number of holes before it, i.e. the amount by
     * which every later implicit row's ordinal lags behind its rowId. No delta needs to be stored.</li>
     * <li>{@code vectorOrdinals}: parallel to {@code rowIds}, only ever read by position, so it is not sorted
     * and can hold any order of values.</li>
     * <li>{@code expected[k]} is the ordinal that row {@code rowIds[k]} <i>would</i> have received had it been
     * a normal row, which is also the first ordinal whose primary row is shifted by this hole. It is
     * non-decreasing in {@code k} (it repeats, as {@code 3, 3} above, for consecutive holes) so it can be
     * binary searched without being materialized.</li>
     * <li>{@code holesByOrdinal}: the indexes {@code k} of the extra-row holes only (missing vectors are
     * omitted), ordered by ordinal and then by {@code k}, so the extra rows of one ordinal are contiguous and
     * ascending. Its own index has no meaning.</li>
     * </ul>
     *
     * <h4>Lookups</h4>
     * <b>rowId to ordinal:</b> binary search {@code rowIds} for the rowId. On a hit at {@code k} the answer is
     * {@code vectorOrdinals[k]}. On a miss the row is implicit, and the insertion point is the number of holes
     * before it, so the answer is {@code rowId - insertionPoint}. For example, rowId 2 is a miss with insertion
     * point 0 and so maps to ordinal 2; rowId 6 is a miss with insertion point 2 and so maps to ordinal 4;
     * rowId 9 is a miss with insertion point 3 and so maps to ordinal 6; rowId 4 is a hit at k=1 and so maps to
     * {@code -1}; rowId 8 is a hit at k=2 and so maps to ordinal 0.
     * <p>
     * <b>ordinal to rowIds:</b> the primary row is {@code ordinal + count(expected[k] <= ordinal)}. For ordinal 2
     * that count is 0 ({@code 3, 3, 6} are all larger), so its primary row is 2. For ordinal 3 the count is 2
     * ({@code 3, 3}), so its primary row is 5. For ordinal 6 the count is 3, so its primary row is 9. Any extra
     * rows are found by binary searching {@code holesByOrdinal} by ordinal and mapping each {@code k} back
     * through {@code rowIds}. For ordinal 1 the primary row is 1 and {@code holesByOrdinal} yields k=0, i.e. row
     * 3, so the result is {@code [1, 3]}. For ordinal 0 the primary row is 0 and it yields k=2, i.e. row 8, so
     * the result is {@code [0, 8]}.
     */
    private boolean tryBuildDeltaIndex(RandomAccessReader reader) throws IOException
    {
        double maxHoles = V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED * (maxRowId + 1L);
        // rowIds of discontinuities (ascending) and the ordinal each really maps to (-1 or an earlier shared ordinal)
        var rowIdsList = new IntArrayList();
        var vectorOrdinalsList = new IntArrayList();
        int holeCount = 0;
        reader.seek(rowToOrdinalOffset);
        while (reader.getFilePointer() < segmentEnd - 8)
        {
            int rowId = reader.readInt();
            int ordinal = reader.readInt();
            int expected = rowId - holeCount;
            if (ordinal == expected)
                continue;

            if (ordinal > expected)
            {
                logger.debug("ZERO_OR_ONE_TO_MANY segment: row {} has ordinal {} but the next unseen ordinal is {}, " +
                             "so ordinals do not first appear in row order; using generic disk path",
                             rowId, ordinal, expected);
                return false;
            }

            holeCount++;
            if (holeCount > maxHoles)
            {
                logger.debug("ZERO_OR_ONE_TO_MANY segment: too many holes ({} / {}), using generic disk path",
                             holeCount, maxRowId + 1);
                return false;
            }

            // The position in these lists equals the number of discontinuities before this row (the delta).
            rowIdsList.add(rowId);
            vectorOrdinalsList.add(ordinal);
        }

        // every ordinal must have a row, i.e. the ordinals seen are exactly [0, maxOrdinal]
        if (maxRowId + 1 - holeCount != maxOrdinal + 1)
        {
            logger.debug("ZERO_OR_ONE_TO_MANY segment: {} rows and {} holes imply {} ordinals but the header says {}, " +
                         "using generic disk path",
                         maxRowId + 1, holeCount, maxRowId + 1 - holeCount, maxOrdinal + 1);
            return false;
        }

        int[] rowIds = rowIdsList.toIntArray();
        int[] vectorOrdinals = vectorOrdinalsList.toIntArray();

        // Indexes of the holes that map to a real ordinal (extra rows), sorted by (ordinal, index). Within one
        // ordinal this keeps the extra rowIds ascending. The temporary (ordinal << 32 | index) keys are only used
        // to sort.
        long[] keys = new long[vectorOrdinals.length];
        int extraCount = 0;
        for (int k = 0; k < vectorOrdinals.length; k++)
            if (vectorOrdinals[k] >= 0)
                keys[extraCount++] = ((long) vectorOrdinals[k] << 32) | k;
        Arrays.sort(keys, 0, extraCount);
        int[] holesByOrdinal = new int[extraCount];
        for (int j = 0; j < extraCount; j++)
            holesByOrdinal[j] = (int) keys[j];

        logger.debug("ZERO_OR_ONE_TO_MANY segment: built delta index with {} holes out of {} rows (ratio {})",
                     holeCount, maxRowId + 1, (double) holeCount / (maxRowId + 1));
        this.deltaOrdinalsView = new DeltaOrdinalsView(rowIds, vectorOrdinals, maxRowId);
        this.deltaRowIdsView = new DeltaRowIdsView(rowIds, vectorOrdinals, holesByOrdinal, maxOrdinal);
        return true;
    }

    public RowIdsView getRowIdsView()
    {
        return rowIdsViewSupplier.get();
    }

    private class GenericRowIdsView implements RowIdsView
    {
        RandomAccessReader reader = fh.createReader();

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
        {
            Preconditions.checkArgument(vectorOrdinal <= maxOrdinal, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, maxOrdinal);

            // read index entry
            try
            {
                reader.seek(ordToRowOffset + vectorOrdinal * 8L);
            }
            catch (Exception e)
            {
                throw new RuntimeException(String.format("Error seeking to index offset for ordinal %d with ordToRowOffset %d",
                                                         vectorOrdinal, ordToRowOffset), e);
            }
            var offset = reader.readLong();
            // seek to and read rowIds
            try
            {
                reader.seek(offset);
            }
            catch (Exception e)
            {
                throw new RuntimeException(String.format("Error seeking to rowIds offset for ordinal %d with ordToRowOffset %d",
                                                         vectorOrdinal, ordToRowOffset), e);
            }
            var postingsSize = reader.readInt();

            // Optimize for the most common case
            if (postingsSize == 1)
                return new SingletonIntIterator(reader.readInt());

            var rowIds = new int[postingsSize];
            for (var i = 0; i < rowIds.length; i++)
            {
                rowIds[i] = reader.readInt();
            }
            return Arrays.stream(rowIds).iterator();
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    public OrdinalsView getOrdinalsView()
    {
        return ordinalsViewSupplier.get();
    }

    @NotThreadSafe
    private class GenericOrdinalsView implements OrdinalsView
    {
        RandomAccessReader reader = fh.createReader();

        /**
         * @return ordinal if given row id is found; otherwise return -1
         * rowId must increase
         */
        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            long offset = rowToOrdinalOffset + (long) rowId * 8;
            if (offset >= segmentEnd - 8)
                return -1;

            reader.seek(offset);
            int foundRowId = reader.readInt();
            assert foundRowId == rowId : "foundRowId=" + foundRowId + " instead of rowId=" + rowId;

            return reader.readInt();
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            long startOffset = max(rowToOrdinalOffset, rowToOrdinalOffset + (long) startRowId * 8);
            if (startOffset >= segmentEnd - 8)
                return;  // start rowid is larger than any rowId that has an associated vector ordinal

            reader.seek(startOffset);

            while (reader.getFilePointer() < segmentEnd - 8)
            {
                int rowId = reader.readInt();
                int ordinal = reader.readInt();

                if (rowId > endRowId)
                    break;

                if (ordinal != -1)
                    consumer.accept(rowId, ordinal);
            }
        }

        @Override
        public void close()
        {
            reader.close();
        }
    }

    public void close()
    {
        fh.close();
    }

    private class OneToManyRowIdsView implements RowIdsView
    {
        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int ordinal)
        {
            Preconditions.checkArgument(ordinal <= maxOrdinal, "vectorOrdinal %s is out of bounds %s", ordinal, maxOrdinal);

            int[] rowIds = extraRowsByOrdinal.get(ordinal);
            // no entry means there is just one rowid matching the ordinal
            if (rowIds == null)
                return new SingletonIntIterator(ordinal);
            // zero-length entry means it's a hole
            if (rowIds.length == 0)
                return IntStream.empty().iterator();
            // otherwise return the rowIds
            return Arrays.stream(rowIds).iterator();
        }

        @Override
        public void close()
        {
            // no-op
        }
    }

    private class OneToManyOrdinalsView implements OrdinalsView {
        @Override
        public int getOrdinalForRowId(int rowId)
        {
            assert rowId >= 0 : rowId;
            if (rowId > maxRowId) {
                return -1;
            }

            int index = Arrays.binarySearch(extraRowIds, rowId);
            if (index >= 0) {
                // Found in extra rows
                return extraOrdinals[index];
            }

            // If it's not an "extra" row then the ordinal is the same as the rowId
            return rowId;
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            int rawIndex = Arrays.binarySearch(extraRowIds, startRowId);
            int extraIndex = rawIndex >= 0 ? rawIndex : ~rawIndex;
            for (int rowId = max(0, startRowId); rowId <= min(endRowId, maxRowId); rowId++)
            {
                if (extraIndex < extraRowIds.length && extraRowIds[extraIndex] == rowId)
                {
                    consumer.accept(extraRowIds[extraIndex], extraOrdinals[extraIndex]);
                    extraIndex++;
                }
                else
                {
                    consumer.accept(rowId, rowId);
                }
            }
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /** rowId -&gt; ordinal for ZERO_OR_ONE_TO_MANY without disk I/O. Immutable, so a single instance is shared. */
    private static class DeltaOrdinalsView implements OrdinalsView
    {
        // rowIds of discontinuities, ascending; the index of an entry is the number of discontinuities before it
        private final int[] rowIds;
        // parallel to rowIds: the ordinal the row really maps to (-1, or an earlier shared ordinal).
        // Unsorted; only ever read by position.
        private final int[] vectorOrdinals;
        private final int maxRowId;

        DeltaOrdinalsView(int[] rowIds, int[] vectorOrdinals, int maxRowId)
        {
            this.rowIds = rowIds;
            this.vectorOrdinals = vectorOrdinals;
            this.maxRowId = maxRowId;
        }

        long bytesUsed()
        {
            return (rowIds.length + vectorOrdinals.length) * 4L;
        }

        @Override
        public int getOrdinalForRowId(int rowId)
        {
            assert rowId >= 0 : rowId;
            if (rowId > maxRowId)
                return -1;

            int i = Arrays.binarySearch(rowIds, rowId);
            if (i >= 0)
                return vectorOrdinals[i];

            // miss: insertion point is the number of discontinuities before rowId
            return rowId - ~i;
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            int start = max(0, startRowId);
            int end = min(endRowId, maxRowId);
            if (start > end)
                return;

            int i = Arrays.binarySearch(rowIds, start);
            int ptr = i >= 0 ? i : ~i;
            int delta = ptr;
            for (int rowId = start; rowId <= end; rowId++)
            {
                if (ptr < rowIds.length && rowIds[ptr] == rowId)
                {
                    int ordinal = vectorOrdinals[ptr];
                    if (ordinal != -1) // rows without a vector are not reported, matching GenericOrdinalsView
                        consumer.accept(rowId, ordinal);
                    ptr++;
                    delta++;
                }
                else
                {
                    consumer.accept(rowId, rowId - delta);
                }
            }
        }

        @Override
        public void close()
        {
            // no-op
        }
    }

    /** ordinal -&gt; rowIds for ZERO_OR_ONE_TO_MANY without disk I/O. Immutable, so a single instance is shared. */
    private static class DeltaRowIdsView implements RowIdsView
    {
        // both shared with DeltaOrdinalsView (not counted in bytesUsed here)
        private final int[] rowIds;
        private final int[] vectorOrdinals;
        // indexes k of the holes with vectorOrdinals[k] >= 0 (extra rows), sorted by (ordinal, k)
        private final int[] holesByOrdinal;
        private final int maxOrdinal;

        DeltaRowIdsView(int[] rowIds,
                        int[] vectorOrdinals,
                        int[] holesByOrdinal,
                        int maxOrdinal)
        {
            this.rowIds = rowIds;
            this.vectorOrdinals = vectorOrdinals;
            this.holesByOrdinal = holesByOrdinal;
            this.maxOrdinal = maxOrdinal;
        }

        long bytesUsed()
        {
            return holesByOrdinal.length * 4L;
        }

        /**
         * Primary rowId of an ordinal: ordinal + delta, where delta is the number of discontinuities k with
         * {@code rowIds[k] - k <= ordinal}. {@code rowIds[k] - k} is the ordinal the k-th
         * discontinuity row would have had if it were a normal row, i.e. the first ordinal it shifts. This virtual
         * sequence is non-decreasing (repeats for consecutive discontinuities), so we binary search it for the
         * upper bound without storing it.
         */
        private int primaryRowId(int ordinal)
        {
            // Hand-written binary search rather than Arrays.binarySearch because the sequence being searched,
            // rowIds[k] - k, is computed per probe and not stored in an array, and because we need the upper bound
            // (first index with value greater than ordinal), which Arrays.binarySearch does not give for repeated
            // values.
            int lo = 0;
            int hi = rowIds.length;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (rowIds[mid] - mid <= ordinal)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            return ordinal + lo;
        }

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int ordinal)
        {
            Preconditions.checkArgument(ordinal <= maxOrdinal, "vectorOrdinal %s is out of bounds %s", ordinal, maxOrdinal);

            int primary = primaryRowId(ordinal);

            // Lower bound of the first extra row mapping to this ordinal. Hand-written binary search because the key
            // is read indirectly (vectorOrdinals[holesByOrdinal[mid]]), so there is no array of ordinals to pass to
            // Arrays.binarySearch, and with repeated ordinals it would return an arbitrary match rather than the
            // first one.
            int lo = 0;
            int hi = holesByOrdinal.length;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (vectorOrdinals[holesByOrdinal[mid]] < ordinal)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            int end = lo;
            while (end < holesByOrdinal.length && vectorOrdinals[holesByOrdinal[end]] == ordinal)
                end++;

            if (lo == end)
                return new SingletonIntIterator(primary);

            // multi-row ordinal: primary row first, then the extra rows in ascending order, read directly from the
            // backing arrays without materializing an intermediate result array
            final int first = lo;
            final int last = end;
            return new PrimitiveIterator.OfInt()
            {
                private int next = first - 1; // first - 1 denotes the primary row

                @Override
                public boolean hasNext()
                {
                    return next < last;
                }

                @Override
                public int nextInt()
                {
                    if (next >= last)
                        throw new NoSuchElementException();
                    int i = next++;
                    return i < first ? primary : rowIds[holesByOrdinal[i]];
                }
            };
        }

        @Override
        public void close()
        {
            // no-op
        }
    }

    @Override
    public long cachedBytesUsed()
    {
        if (deltaOrdinalsView != null)
            return deltaOrdinalsView.bytesUsed() + deltaRowIdsView.bytesUsed();

        if (structure != V5VectorPostingsWriter.Structure.ONE_TO_MANY) {
            return 0;
        }

        long bytes = 0;
        if (extraRowIds != null) {
            bytes += extraRowIds.length * 4L;
        }
        if (extraOrdinals != null) {
            bytes += extraOrdinals.length * 4L;
        }
        if (extraRowsByOrdinal != null) {
            for (int[] rowIds : extraRowsByOrdinal.values()) {
                bytes += rowIds.length * 4L;
            }
        }
        return bytes;
    }
}
