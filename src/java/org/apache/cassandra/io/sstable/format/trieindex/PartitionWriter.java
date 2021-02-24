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

package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Partition writer used by {@link TrieIndexSSTableWriter}.
 *
 * Writes all passed data to the given SequentialWriter and if necessary builds a RowIndex by constructing an entry
 * for each row within a partition that follows {@link org.apache.cassandra.config.Config#column_index_cache_size_in_kb}
 * kilobytes of written data.
 */
class PartitionWriter implements AutoCloseable
{
    public int rowIndexCount;

    private final UnfilteredSerializer unfilteredSerializer;
    private final SerializationHeader header;
    private final SequentialWriter writer;
    private final Collection<SSTableFlushObserver> observers;
    private final RowIndexWriter rowTrie;

    private long initialPosition;
    private long startPosition;

    private int written;
    private long previousRowStart;

    private ClusteringPrefix firstClustering;
    private ClusteringPrefix lastClustering;

    private DeletionTime openMarker = DeletionTime.LIVE;
    private DeletionTime startOpenMarker = DeletionTime.LIVE;

    // Sequence control, also used to add empty static row if `addStaticRow` is not called.
    private enum State
    {
        AWAITING_PARTITION_HEADER,
        AWAITING_STATIC_ROW,
        AWAITING_ROWS
    }
    State state;

    PartitionWriter(SerializationHeader header,
                    ClusteringComparator comparator,
                    SequentialWriter writer,
                    SequentialWriter indexWriter,
                    Version version,
                    Collection<SSTableFlushObserver> observers)
    {
        this.header = header;
        this.writer = writer;
        EncodingVersion version1 = version.encodingVersion();
        this.observers = observers;
        this.rowTrie = new RowIndexWriter(comparator, indexWriter);
        this.unfilteredSerializer = UnfilteredSerializer.serializers.get(version1);
    }

    public void reset()
    {
        this.initialPosition = writer.position();
        this.startPosition = -1;
        this.previousRowStart = 0;
        this.rowIndexCount = 0;
        this.written = 0;
        this.firstClustering = null;
        this.lastClustering = null;
        this.openMarker = DeletionTime.LIVE;
        this.state = State.AWAITING_PARTITION_HEADER;
        rowTrie.reset();
    }

    @Override
    public void close()
    {
        rowTrie.close();
    }

    void writePartitionHeader(DecoratedKey partitionKey, DeletionTime partitionLevelDeletion) throws IOException
    {
        assert state == State.AWAITING_PARTITION_HEADER;
        ByteBufferUtil.writeWithShortLength(partitionKey.getTempKey(), writer);

        long deletionTimePosition = writer.position();
        DeletionTime deletionTime = partitionLevelDeletion;
        DeletionTime.serializer.serialize(deletionTime, writer);
        if (!observers.isEmpty())
            observers.forEach(o -> o.partitionLevelDeletion(deletionTime, deletionTimePosition));
        state = header.hasStatic() ? State.AWAITING_STATIC_ROW : State.AWAITING_ROWS;
    }

    private void doWriteStaticRow(Row staticRow) throws IOException
    {
        assert state == State.AWAITING_STATIC_ROW;
        long staticRowPosition = writer.position();
        unfilteredSerializer.serializeStaticRow(staticRow, header, writer);
        if (!observers.isEmpty())
            observers.forEach(o -> o.staticRow(staticRow, staticRowPosition));
        state = State.AWAITING_ROWS;
    }

    void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        if (state == State.AWAITING_STATIC_ROW)
        {
            if (unfiltered.isRow() && ((Row) unfiltered).isStatic())
            {
                doWriteStaticRow((Row) unfiltered);
                return;
            }

            doWriteStaticRow(Rows.EMPTY_STATIC_ROW);
        }

        assert state == State.AWAITING_ROWS;

        long pos = currentPosition();

        if (firstClustering == null)
        {
            // Beginning of an index block. Remember the start and position
            firstClustering = unfiltered.clustering();
            startOpenMarker = openMarker;
            startPosition = pos;
        }

        long unfilteredPosition = writer.position();
        unfilteredSerializer.serialize(unfiltered, header, writer, pos - previousRowStart);

        // notify observers about each new row
        if (!observers.isEmpty())
            observers.forEach(o -> o.nextUnfilteredCluster(unfiltered, unfilteredPosition));

        lastClustering = unfiltered.clustering();
        previousRowStart = pos;
        ++written;

        if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
        }

        // if we hit the row index size that we have to index after, go ahead and index it.
        if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            addIndexBlock();
    }

    long finish() throws IOException
    {
        if (state == State.AWAITING_STATIC_ROW)
            doWriteStaticRow(Rows.EMPTY_STATIC_ROW);

        long endPosition = currentPosition();
        unfilteredSerializer.writeEndOfPartition(writer);

        // It's possible we add no rows, just a top level deletion
        if (written == 0)
            return -1;

        long trieRoot = -1;
        // the last row may have fallen on an index boundary already.  if not, index it explicitly.
        if (firstClustering != null && rowIndexCount > 0)
            addIndexBlock();
        if (rowIndexCount > 1)
            trieRoot = rowTrie.complete(endPosition);
        // Otherwise we don't complete the trie. Even if we did write something (which shouldn't be the case as the
        // first entry has an empty key and root isn't filled), that's not a problem.

        return trieRoot;
    }

    private long currentPosition()
    {
        return writer.position() - initialPosition;
    }

    private void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(startPosition,
                                             startOpenMarker);
        rowTrie.add(firstClustering, lastClustering, cIndexInfo);
        firstClustering = null;
        ++rowIndexCount;
    }
}
