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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final long rowOrdinalOffset;
    private final List<ExtraRow> extraRows;
    @VisibleForTesting
    final V5VectorPostingsWriter.Structure structure;

    private final Supplier<OrdinalsView> ordinalsViewSupplier;
    private final Supplier<RowIdsView> rowIdsViewSupplier;

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
                this.rowOrdinalOffset = segmentEnd;
            }
            else
            {
                reader.seek(segmentEnd - 8);
                this.rowOrdinalOffset = reader.readLong();
            }

            if (maxOrdinal < 0)
            {
                extraRows = null;
                this.rowIdsViewSupplier = () -> EMPTY_ROW_IDS_VIEW;
                this.ordinalsViewSupplier = () -> EMPTY_ORDINALS_VIEW;
            }
            else if (structure == V5VectorPostingsWriter.Structure.ONE_TO_ONE)
            {
                extraRows = null;
                this.rowIdsViewSupplier = () -> ONE_TO_ONE_ROW_IDS_VIEW;
                this.ordinalsViewSupplier = () -> new OneToOneOrdinalsView(maxOrdinal + 1);
            }
            else if (structure == V5VectorPostingsWriter.Structure.ONE_TO_MANY)
            {
                extraRows = cacheExtraRowOrdinals(reader);
                this.rowIdsViewSupplier = GenericRowIdsView::new;
                this.ordinalsViewSupplier = OneToManyOrdinalsView::new;
            }
            else
            {
                extraRows = null;
                this.rowIdsViewSupplier = GenericRowIdsView::new;
                this.ordinalsViewSupplier = GenericOrdinalsView::new;
            }

            assert rowOrdinalOffset <= segmentEnd : "rowOrdinalOffset " + rowOrdinalOffset + " is not less than or equal to segmentEnd " + segmentEnd;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error initializing OnDiskOrdinalsMap at segment " + segmentOffset, e);
        }
    }

    private List<ExtraRow> cacheExtraRowOrdinals(RandomAccessReader reader) throws IOException
    {
        var extra = new ArrayList<ExtraRow>();
        reader.seek(rowOrdinalOffset);
        while (reader.getFilePointer() < segmentEnd - 8)
        {
            int rowId = reader.readInt();
            int ordinal = reader.readInt();
            extra.add(new ExtraRow(rowId, ordinal));
        }
        return extra;
    }

    public RowIdsView getRowIdsView()
    {
        return rowIdsViewSupplier.get();
    }

    // VSTODO is it worth optimizing this for one-to-many case as well?
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
            long offset = rowOrdinalOffset + (long) rowId * 8;
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
            long startOffset = max(rowOrdinalOffset, rowOrdinalOffset + (long) startRowId * 8);
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

    private class OneToManyOrdinalsView implements OrdinalsView
    {
        @Override
        public int getOrdinalForRowId(int rowId)
        {
            assert rowId >= 0 : rowId;
            if (rowId > maxRowId) {
                return -1;
            }

            // Binary search for the ExtraRow with the given rowId
            int index = Collections.binarySearch(extraRows, new ExtraRow(rowId, 0),
                                                 Comparator.comparingInt(er -> er.rowId));
            if (index >= 0) {
                // Found in extra rows
                return extraRows.get(index).ordinal;
            }

            // if it's not an "extra" row then the ordinal is the same as the rowId
            return rowId;
        }

        @Override
        public void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException
        {
            int rawIndex = Collections.binarySearch(extraRows, new ExtraRow(startRowId, 0),
                                                    Comparator.comparingInt(er -> er.rowId));
            int extraIndex = rawIndex >= 0 ? rawIndex : -rawIndex - 1;
            for (int rowId = max(0, startRowId); rowId <= min(endRowId, maxRowId); rowId++)
            {
                if (extraIndex < extraRows.size() && extraRows.get(extraIndex).rowId == rowId)
                {
                    ExtraRow extraRow = extraRows.get(extraIndex);
                    consumer.accept(extraRow.rowId, extraRow.ordinal);
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
            // no-op, as we no longer need to close any resources
        }
    }

    private static class ExtraRow
    {
        public final int rowId;
        public final int ordinal;

        private ExtraRow(int rowId, int ordinal)
        {
            this.rowId = rowId;
            this.ordinal = ordinal;
        }
    }
}
