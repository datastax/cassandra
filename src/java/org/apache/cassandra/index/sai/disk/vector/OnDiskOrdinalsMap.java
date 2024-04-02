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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.BitSet;
import org.apache.cassandra.index.sai.disk.v2.hnsw.DiskBinarySearch;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import io.github.jbellis.jvector.util.Bits;

public class OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskOrdinalsMap.class);

    private final OrdinalsView fastOrdinalsView;
    private static final OrdinalsMatchingRowIdsView ordinalsMatchingRowIdsView = new OrdinalsMatchingRowIdsView();
    private final FileHandle fh;
    private final long ordToRowOffset;
    private final long segmentEnd;
    private final int size;
    // the offset where we switch from recording ordinal -> rows, to row -> ordinal
    private final long rowOrdinalOffset;
    private final Set<Integer> deletedOrdinals;

    private final boolean canFastMapOrdinalsView;
    private final boolean canFastMapRowIdsView;

    public OnDiskOrdinalsMap(FileHandle fh, long segmentOffset, long segmentLength)
    {
        deletedOrdinals = new HashSet<>();

        this.segmentEnd = segmentOffset + segmentLength;
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            reader.seek(segmentOffset);
            int deletedCount = reader.readInt();
            for (var i = 0; i < deletedCount; i++)
            {
                int ordinal = reader.readInt();
                deletedOrdinals.add(ordinal);
            }

            this.ordToRowOffset = reader.getFilePointer();
            this.size = reader.readInt();
            reader.seek(segmentEnd - 8);
            this.rowOrdinalOffset = reader.readLong();

            // When rowOrdinalOffset + 8 is equal to segmentEnd, the segment has no postings. Therefore,
            // we use the EmptyView. That case does not get a fastRowIdsView because we only hit that code after
            // getting ordinals from the graph, and an EmptyView will not produce any ordinals to search. Importantly,
            // the file format for the RowIdsView is correct, even if there are no postings.
            this.canFastMapRowIdsView = deletedCount == -1;
            this.canFastMapOrdinalsView = deletedCount == -1 || rowOrdinalOffset + 8 == segmentEnd;
            this.fastOrdinalsView = deletedCount == -1 ? new RowIdMatchingOrdinalsView(size) : new EmptyView();
            assert rowOrdinalOffset < segmentEnd : "rowOrdinalOffset " + rowOrdinalOffset + " is not less than segmentEnd " + segmentEnd;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error initializing OnDiskOrdinalsMap at segment " + segmentOffset, e);
        }
    }

    public RowIdsView getRowIdsView()
    {
        if (canFastMapRowIdsView) {
            return ordinalsMatchingRowIdsView;
        }

        return new FileReadingRowIdsView();
    }

    public Bits ignoringDeleted(Bits acceptBits)
    {
        return BitsUtil.bitsIgnoringDeleted(acceptBits, deletedOrdinals);
    }


    /**
     * Singleton int iterator used to prevent unnecessary object creation
     */
    private static class SingletonIntIterator implements PrimitiveIterator.OfInt
    {
        private final int value;
        private boolean hasNext = true;

        public SingletonIntIterator(int value)
        {
            this.value = value;
        }

        @Override
        public boolean hasNext()
        {
            return hasNext;
        }

        @Override
        public int nextInt()
        {
            if (!hasNext)
                throw new NoSuchElementException();
            hasNext = false;
            return value;
        }
    }

    private static class OrdinalsMatchingRowIdsView implements RowIdsView {

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
        {
            return new SingletonIntIterator(vectorOrdinal);
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    private class FileReadingRowIdsView implements RowIdsView
    {
        RandomAccessReader reader = fh.createReader();

        @Override
        public PrimitiveIterator.OfInt getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
        {
            Preconditions.checkArgument(vectorOrdinal < size, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, size);

            // read index entry
            try
            {
                reader.seek(ordToRowOffset + 4L + vectorOrdinal * 8L);
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
        if (canFastMapOrdinalsView) {
            return fastOrdinalsView;
        }

        return new FileReadingOrdinalsView();
    }

    /** Bits matching the given range, inclusively. */
    public static class MatchRangeBits extends BitSet
    {
        final int lowerBound;
        final int upperBound;

        public MatchRangeBits(int lowerBound, int upperBound) {
            // bitset is empty if lowerBound > upperBound
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public boolean get(int index) {
            return lowerBound <= index && index <= upperBound;
        }

        @Override
        public int length() {
            if (lowerBound > upperBound)
                return 0;
            return upperBound - lowerBound + 1;
        }

        @Override
        public void set(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public boolean getAndSet(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public void clear(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public void clear(int i, int i1)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public int cardinality()
        {
            return length();
        }

        @Override
        public int approximateCardinality()
        {
            return length();
        }

        @Override
        public int prevSetBit(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public int nextSetBit(int i)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public long ramBytesUsed()
        {
            return 2 * Integer.BYTES;
        }
    }

    private static class RowIdMatchingOrdinalsView implements OrdinalsView
    {
        // The number of ordinals in the segment. If we see a rowId greater than or equal to this, we know it's not in
        // the graph.
        private final int size;

        RowIdMatchingOrdinalsView(int size)
        {
            this.size = size;
        }

        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            if (rowId >= size)
                return -1;
            return rowId;
        }

        @Override
        public boolean forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            // risk of overflow
            assert endRowId < Integer.MAX_VALUE : "endRowId must be less than Integer.MAX_VALUE";
            assert endRowId >= startRowId : "endRowId must be greater than or equal to startRowId";

            int start = Math.max(startRowId, 0);
            int end = Math.min(endRowId + 1, size);
            for (int rowId = start; rowId < end; rowId++)
                consumer.accept(rowId, rowId);
            // Returns true if we called the consumer at least once.
            return end > start;
        }

        @Override
        public BitSet buildOrdinalBitSet(int startRowId, int endRowId, Supplier<BitSet> unused) throws IOException
        {
            int start = Math.max(startRowId, 0);
            int end = Math.min(endRowId + 1, size);

            return new MatchRangeBits(start, end);
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    /**
     * An OrdinalsView that always returns -1 for all rowIds. This is used when the segment has no postings, which
     * can happen if all the graph's ordinals are in the deletedOrdinals set.
     */
    private static class EmptyView implements OrdinalsView
    {
        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            return -1;
        }

        @Override
        public boolean forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            return false;
        }

        @Override
        public BitSet buildOrdinalBitSet(int startRowId, int endRowId, Supplier<BitSet> supplier) throws IOException
        {
            // Get an empty bitset
            return supplier.get();
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    /**
     * not thread safe
     */
    private class FileReadingOrdinalsView implements OrdinalsView
    {
        RandomAccessReader reader = fh.createReader();
        private final long high = (segmentEnd - 8 - rowOrdinalOffset) / 8;
        private int lastFoundRowId = -1;
        private long lastFoundRowIdIndex = -1;

        private int lastRowId = -1;

        /**
         * @return order if given row id is found; otherwise return -1
         * rowId must increase
         */
        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            if (rowId <= lastRowId)
                throw new IllegalArgumentException("rowId " + rowId + " is less than or equal to lastRowId " + lastRowId);
            lastRowId = rowId;

            if (rowId < lastFoundRowId) // skipped row, no need to search
                return -1;

            long low = 0;
            if (lastFoundRowId > -1 && lastFoundRowIdIndex < high)
            {
                low = lastFoundRowIdIndex;

                if (lastFoundRowId == rowId) // "lastFoundRowId + 1 == rowId" case that returned -1 likely moved use here
                {
                    long offset = rowOrdinalOffset + lastFoundRowIdIndex * 8;
                    reader.seek(offset);
                    int foundRowId = reader.readInt();
                    assert foundRowId == rowId : "expected rowId " + rowId + " but found " + foundRowId;
                    return reader.readInt();
                }
                else if (lastFoundRowId + 1 == rowId) // sequential read, skip binary search
                {
                    long offset = rowOrdinalOffset + (lastFoundRowIdIndex + 1) * 8;
                    reader.seek(offset);
                    int foundRowId = reader.readInt();
                    lastFoundRowId = foundRowId;
                    lastFoundRowIdIndex++;
                    if (foundRowId == rowId)
                        return reader.readInt();
                    else
                        return -1;
                }
            }
            final AtomicLong lastRowIdIndex = new AtomicLong(-1L);
            // Compute the offset of the start of the rowId to vectorOrdinal mapping
            long index = DiskBinarySearch.searchInt(low, high, rowId, i -> {
                try
                {
                    lastRowIdIndex.set(i);
                    long offset = rowOrdinalOffset + i * 8;
                    reader.seek(offset);
                    return reader.readInt();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });

            // not found
            if (index < 0)
                return -1;

            lastFoundRowId = rowId;
            lastFoundRowIdIndex = lastRowIdIndex.get();
            return reader.readInt();
        }

        @Override
        public boolean forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            boolean called = false;

            long start = DiskBinarySearch.searchFloor(0, high, startRowId, i -> {
                try
                {
                    long offset = rowOrdinalOffset + i * 8;
                    reader.seek(offset);
                    return reader.readInt();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });

            if (start < 0 || start >= high)
                return false;

            reader.seek(rowOrdinalOffset + start * 8);
            // sequential read without seeks should be fast, we expect OS to prefetch data from the disk
            // binary search for starting offset of min rowid >= startRowId unlikely to be faster
            for (long idx = start; idx < high; idx ++)
            {
                int rowId = reader.readInt();
                if (rowId > endRowId)
                    break;

                int ordinal = reader.readInt();
                if (rowId >= startRowId)
                {
                    called = true;
                    consumer.accept(rowId, ordinal);
                }
            }
            return called;
        }

        @Override
        public BitSet buildOrdinalBitSet(int startRowId, int endRowId, Supplier<BitSet> bitsetSupplier) throws IOException
        {
            BitSet bits = bitsetSupplier.get();
            this.forEachOrdinalInRange(startRowId, endRowId, (segmentRowId, ordinal) -> {
                bits.set(ordinal);
            });
            return bits;
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
}
