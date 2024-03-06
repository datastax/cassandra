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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.v2.hnsw.DiskBinarySearch;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import io.github.jbellis.jvector.util.Bits;

public class OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskOrdinalsMap.class);

    private final RowIdMatchingOrdinalsView rowIdMatchingOrdinalsView;
    private static final OrdinalsMatchingRowIdsView ordinalsMatchingRowIdsView = new OrdinalsMatchingRowIdsView();
    private final FileHandle fh;
    private final long ordToRowOffset;
    private final long segmentEnd;
    private final int size;
    // the offset where we switch from recording ordinal -> rows, to row -> ordinal
    private final long rowOrdinalOffset;
    private final Set<Integer> deletedOrdinals;

    private boolean rowIdsMatchOrdinals = false;

    public OnDiskOrdinalsMap(FileHandle fh, long segmentOffset, long segmentLength)
    {
        deletedOrdinals = new HashSet<>();

        this.segmentEnd = segmentOffset + segmentLength;
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            reader.seek(segmentOffset);
            int deletedCount = reader.readInt();
            if (deletedCount == -1) {
                rowIdsMatchOrdinals = true;
            }
            for (var i = 0; i < deletedCount; i++)
            {
                int ordinal = reader.readInt();
                deletedOrdinals.add(ordinal);
            }

            this.ordToRowOffset = reader.getFilePointer();
            this.size = reader.readInt();
            this.rowIdMatchingOrdinalsView = new RowIdMatchingOrdinalsView(size);
            reader.seek(segmentEnd - 8);
            this.rowOrdinalOffset = reader.readLong();
            assert rowOrdinalOffset < segmentEnd : "rowOrdinalOffset " + rowOrdinalOffset + " is not less than segmentEnd " + segmentEnd;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error initializing OnDiskOrdinalsMap at segment " + segmentOffset, e);
        }
    }

    public RowIdsView getRowIdsView()
    {
        if (rowIdsMatchOrdinals) {
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
        if (rowIdsMatchOrdinals) {
            return rowIdMatchingOrdinalsView;
        }

        return new FileReadingOrdinalsView();
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
            boolean called = false;
            int start = Math.max(startRowId, 0);
            int end = Math.min(endRowId, size);
            for (int rowId = start; rowId < end; rowId++)
            {
                called = true;
                consumer.accept(rowId, rowId);
            }
            return called;
        }

        @Override
        public void close()
        {
            // noop
        }
    }

    private class FileReadingOrdinalsView implements OrdinalsView
    {
        RandomAccessReader reader = fh.createReader();
        private final long high = (segmentEnd - 8 - rowOrdinalOffset) / 8;

        /**
         * @return order if given row id is found; otherwise return -1
         */
        @Override
        public int getOrdinalForRowId(int rowId) throws IOException
        {
            // Compute the offset of the start of the rowId to vectorOrdinal mapping
            long index = DiskBinarySearch.searchInt(0, Math.toIntExact(high), rowId, i -> {
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

            // not found
            if (index < 0)
                return -1;

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
