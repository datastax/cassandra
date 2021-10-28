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

package org.apache.cassandra.db.memtable.pmem;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import javax.annotation.Nullable;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import com.intel.pmem.llpl.Transaction;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.PersistentMemoryMemtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class PmemPartition implements Partition
{
    private static final Logger logger = LoggerFactory.getLogger(PmemPartition.class);
    /*
        binary format:
        - address of maps of rows - 8 bytes
        - address of static row - 8 bytes
        - tombstone information - 1 byte
        - partition key size - 4 bytes
        - partition key
     */
    private static final long ROW_MAP_OFFSET = 0; // long, handle
    private static final long STATIC_ROW_OFFSET = 8; // long, handle
    private static final long DELETION_INFO_OFFSET = 16; // long, tombstone
    private static final long DECORATED_KEY_SIZE_OFFSET = 24; //TODO: this is probably not needed, revisit
    private static final long HEADER_SIZE = 28;

    /**
     * The block in memory where the high-level data for this partition is stored.
     */
    private TransactionalMemoryBlock block;
    private final TableMetadata tableMetadata;
    private  Token token;

    // memoized DK
    private DecoratedKey key;
    private AutoCloseableIterator cartIterator;

    protected static final class Holder
    {
        final RegularAndStaticColumns columns;
        final DeletionInfo deletionInfo;
        final Row staticRow;
        final EncodingStats stats;

        Holder(RegularAndStaticColumns columns, DeletionInfo deletionInfo, Row staticRow, EncodingStats stats)
        {
            this.columns = columns;
        //    this.delTime = delTime;
            this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            this.stats = stats;
            this.deletionInfo = deletionInfo;
        }
    }
    // memoized map  Clustering -> Row, where Row is serialized in to one blob
    // - allows compression (debatable if needed)
    protected static final Holder EMPTY = new Holder(RegularAndStaticColumns.NONE, DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
    private final TransactionalHeap heap;
    private volatile Holder ref;
    Row staticRow = Rows.EMPTY_STATIC_ROW;
    PmemRowMap pmemRowMap;

    public PmemPartition(TransactionalHeap heap, DecoratedKey dkey, TableMetadata tableMetadata, AutoCloseableIterator<LongART.Entry> cartIterator)
    {
        this.heap = heap;
        this.token= dkey.getToken();
        this.key = dkey;
        this.tableMetadata = tableMetadata;
        this.ref = EMPTY;
        this.cartIterator = cartIterator;
    }
    public PmemPartition(TransactionalHeap heap, TableMetadata tableMetadata)
    {
        this.heap = heap;
        this.tableMetadata = tableMetadata;
    }

    //This load function is called during puts
    public void load(TransactionalHeap heap, long partitionHandle)
    {
        block = heap.memoryBlockFromHandle(partitionHandle);

        long staticRowAddress = block.getLong(STATIC_ROW_OFFSET);
        if(staticRowAddress != 0)
        {
            staticRow = getStaticRow(staticRowAddress);
        }
        long rowMapAddress = block.getLong(ROW_MAP_OFFSET);
        if(rowMapAddress != 0)
        {
            pmemRowMap = PmemRowMap.loadFromAddress(heap,rowMapAddress, tableMetadata,DeletionTime.LIVE);
        }
        ref = new Holder(tableMetadata.regularAndStaticColumns(), DeletionInfo.LIVE, staticRow, EncodingStats.NO_STATS);//TODO:revisit deletioninfo
    }

    public  void initialize(PartitionUpdate update, TransactionalHeap heap) throws IOException
    {
        //TODO: Revisit holder
        Holder current = ref;
        RegularAndStaticColumns columns = update.columns().mergeTo(current.columns);
        staticRow = update.staticRow();
        EncodingStats newStats = current.stats.mergeWith(update.stats());
        ref = new Holder(columns, update.deletionInfo(), staticRow, newStats);

        ByteBuffer key = update.partitionKey().getKey();
        int keySize =  key.limit();
        long mblockSize = HEADER_SIZE + keySize;
        block = heap.allocateMemoryBlock(mblockSize);
        // handle static row seperate from regular row(s)
        if (!staticRow.isEmpty())
        {
            staticRow = updateStaticRow(staticRow);
        }
        /// ROWS!!!
        final long rowMapAddress;

        pmemRowMap = PmemRowMap.create(heap, update.metadata(), update.partitionLevelDeletion());
        rowMapAddress = pmemRowMap.getHandle(); //gets address of arTree
        for (Row r : update)
        {
            pmemRowMap.put(r, update);
        }
        block.setLong(ROW_MAP_OFFSET, rowMapAddress);
        //TOMBSTONE
        DeletionTime partitionDeleteTime = update.deletionInfo().getPartitionDeletion();
        long partitionDeleteSize = partitionDeleteTime.isLive() ? 0 : DeletionTime.serializer.serializedSize(partitionDeleteTime);//TODO:Fix this later
        if (partitionDeleteSize != 0)
        {
            updatePartitionDelete(partitionDeleteSize);
        }
        block.setInt(DECORATED_KEY_SIZE_OFFSET, key.remaining());
        block.copyFromArray(key.array(),0,HEADER_SIZE , key.array().length);
    }

    public void update(PartitionUpdate update,TransactionalHeap heap) throws IOException
    {
        DeletionTime partitionDeleteTime = update.deletionInfo().getPartitionDeletion();
        if (!update.staticRow().isEmpty())
        {
            staticRow = updateStaticRow(update.staticRow());
        }

        /// ROWS!!!
        //TODO: Do we need to merge deletion info?
        // build the headers data from this
       for (Row r : update)
            pmemRowMap.put(r, update);
       long partitionDeleteSize = partitionDeleteTime.isLive() ? 0 : DeletionTime.serializer.serializedSize(partitionDeleteTime);
       if(partitionDeleteSize > 0) //need to check if partitionDelete exists if so free it, can we inline it!!
       {
           updatePartitionDelete(partitionDeleteSize);
       }
       else
       {
           block.setLong(DELETION_INFO_OFFSET, 0);
       }
    }

    // AtomicBTreePartition.addAllWithSizeDelta & RowUpdater are the places to look to see how classic storage engine stashes things
    private Row updateStaticRow(Row staticRow) throws IOException
    {
        Row newStaticRow = staticRow ;
        long oldStaticRowHandle = 0;
        TransactionalMemoryBlock oldStaticBlock = null;
        try(DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {

            oldStaticRowHandle = block.getLong(STATIC_ROW_OFFSET);
            if (oldStaticRowHandle != 0)
            {
                oldStaticBlock = heap.memoryBlockFromHandle(oldStaticRowHandle);
                newStaticRow = getMergedStaticRow(staticRow, oldStaticRowHandle);
            }

            SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                              tableMetadata,
                                                                              tableMetadata.regularAndStaticColumns(),
                                                                              EncodingStats.NO_STATS);
            // statsCollector.get());
            SerializationHelper helper = new SerializationHelper(serializationHeader);
            int version = PersistentMemoryMemtable.getTablesMetadataMap().get(tableMetadata.id).getSerializationHeaderMap().size(); //added by ssuvarna

            PmemRowSerializer.serializer.serializeStaticRow(newStaticRow, helper, dob, version);
            int size = dob.getLength();

            TransactionalMemoryBlock staticRowBlock;
            if (oldStaticRowHandle == 0)
                staticRowBlock = heap.allocateMemoryBlock(size);
            else if (oldStaticBlock.size() < size)
            {
                staticRowBlock = heap.allocateMemoryBlock(size);
                oldStaticBlock.free();
            }
            else
            {
                staticRowBlock = oldStaticBlock;//TODO: should we zero out if size is greater ?
            }
            MemoryBlockDataOutputPlus cellsOutputPlus = new MemoryBlockDataOutputPlus(staticRowBlock, 0);
            cellsOutputPlus.write(dob.getData(), 0, dob.getLength());
            dob.clear();
            dob.close();
            block.setLong(STATIC_ROW_OFFSET, staticRowBlock.handle());
            return newStaticRow;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public  Row getMergedStaticRow(Row newRow, Long mb)
    {
        Row currentRow = getStaticRow(mb);
        return(Rows.merge(currentRow, newRow));
    }

    private Row getStaticRow(long staticRowAddress)
    {
        TransactionalMemoryBlock staticRowMemoryBlock = heap.memoryBlockFromHandle(staticRowAddress);

        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(staticRowMemoryBlock, heap);

        try
        {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader;
            Map<Integer, SerializationHeader> sHeaderMap = PersistentMemoryMemtable.getTablesMetadataMap().get(tableMetadata.id).getSerializationHeaderMap();
            if (sHeaderMap.size() > 1)
            {
                serializationHeader = sHeaderMap.get(savedVersion);
            }
            else
            {
                serializationHeader = new SerializationHeader(false,
                                                              tableMetadata,
                                                              tableMetadata.regularAndStaticColumns(),
                                                              EncodingStats.NO_STATS);
            }
            DeserializationHelper helper = new DeserializationHelper(tableMetadata, -1, DeserializationHelper.Flag.LOCAL);
            return PmemRowSerializer.serializer.deserializeStaticRow(memoryBlockDataInputPlus, serializationHeader, helper);
        }
        catch (IOException e)
        {
            closeCartIterator();
            throw new IOError(e);
        }
    }

    private void updatePartitionDelete(long partitionDeleteSize)
    {
        //TODO: Need to handle deletion time
     //  DeletionTime.serializer.serialize(partitionDeleteTime, deletionBlockOutputPlus);
            block.setLong(DELETION_INFO_OFFSET, -1); //This indicates it is a Tombstone
    }

    public long getAddress()
    {
        return block.handle();
    }
    public long getRowMapAddress()
    {
        return pmemRowMap.getHandle();
    }

    @Override
    public TableMetadata metadata()
    {
        return tableMetadata;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return this.key;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return ref.deletionInfo.getPartitionDeletion();
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return ref.columns;
    }

    @Override
    public EncodingStats stats()
    {
        return ref.stats;
    }

    @Override
    public boolean isEmpty()
    {
        return ref.deletionInfo.isLive() && pmemRowMap.size() == 0 && ref.staticRow.isEmpty();
    }

    @Override
    public boolean hasRows()
    {
        return pmemRowMap.size() != 0;
    }

    @Nullable
    @Override
    public Row getRow(Clustering<?> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            return staticRow;
        }
        return pmemRowMap.getRow(clustering,tableMetadata);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
    {
        return unfilteredIterator(ref, columns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
       // if (clusteringsInQueryOrder.isEmpty() || block.getLong(DELETION_INFO_OFFSET) == -1)
        if (block.getLong(DELETION_INFO_OFFSET) == -1)
        {
            closeCartIterator();
            return EmptyIterators.unfilteredRow(tableMetadata, key, reversed);
        }
        return new PmemRowMapClusteringIterator(columns,clusteringsInQueryOrder,pmemRowMap.getHandle(),
                                                ref.deletionInfo.getPartitionDeletion(), reversed);
    }

    public UnfilteredRowIterator unfilteredIterator(Holder current, ColumnFilter columns, Slices slices, boolean reversed)
    {
        if(block.getLong(DELETION_INFO_OFFSET) == -1)
        {
            closeCartIterator();
            return EmptyIterators.unfilteredRow(tableMetadata, key, reversed);
        }
        else if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
            closeCartIterator();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return slices.size() == 1
               ? PmemRowMapIterator.create(tableMetadata, pmemRowMap.getRowMapTree(), heap, key,
                                                  current.deletionInfo.getPartitionDeletion(), slices.get(0), reversed, staticRow, cartIterator)
               :new SlicesIterator(current, columns, slices, reversed);
    }



    private void closeCartIterator()
    {
        try
        {
            if(cartIterator != null)
            {
                cartIterator.close();
                cartIterator = null;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    //Addresses cases when read happens over multiple slices
    private class SlicesIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator //extends AbstractBTreePartition.AbstractIterator
    {
        private final Slices slices;
        private int idx;
        private Iterator<Unfiltered> currentSlice;
        private ColumnFilter columnFilter;
        boolean isReversed;
        Holder current;

        private SlicesIterator(Holder current, ColumnFilter selection, Slices slices, boolean isReversed)
        {
            this.isReversed = isReversed;
            this.columnFilter = selection;
            this.current = current;
            this.slices = slices;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = isReversed ? slices.size() - idx - 1 : idx;
                    currentSlice = PmemRowMapIterator.create(tableMetadata, pmemRowMap.getRowMapTree(), heap, key,
                                                             current.deletionInfo.getPartitionDeletion(), slices.get(sliceIdx), isReversed, staticRow, cartIterator);
                    idx++;
                }
                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return current.deletionInfo.getPartitionDeletion();
        }

        @Override
        public EncodingStats stats()
        {
            return current.stats;
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return this.isReversed;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return tableMetadata.regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public void close()
        {
            closeCartIterator();
        }
    }

    //Addresses cases where there are multiple values for a clustering key
    private class PmemRowMapClusteringIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        private final Iterator<Clustering<?>> clusteringsIterator;
        private Iterator<Row> currentIterator;
        private final LongART pmemRowTree;
        private  Iterator pmemRowTreeIterator;
        private boolean reversed = false;
        SerializationHeader header;
        DeletionTime deletionTime = DeletionTime.LIVE;

        private PmemRowMapClusteringIterator(ColumnFilter selection,
                                    NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                    long pmemRowMapTreeAddr,
                                    DeletionTime deletionTime,
                                             boolean isReversed)
        {
            this.header =  SerializationHeader.makeWithoutStats(tableMetadata);
            this.pmemRowTree = LongART.fromHandle(heap, pmemRowMapTreeAddr);
            this.deletionTime = deletionTime;
            this.pmemRowTreeIterator =  pmemRowTree.getEntryIterator();
            this.clusteringsIterator = clusteringsInQueryOrder.iterator();
            this.reversed = isReversed;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentIterator == null)
                {
                    if (!clusteringsIterator.hasNext())
                        return endOfData();
                    currentIterator = nextIterator(clusteringsIterator.next());
                }
                if (currentIterator != null && currentIterator.hasNext())
                    return currentIterator.next();
                currentIterator = null;
            }
        }

        private Iterator<Row> nextIterator(Clustering<?> clustering)
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            ClusteringComparator clusteringComparator = tableMetadata.comparator;
            Row unfiltered = null;
            Iterator<Row> rowIterator;
            ByteSource clusteringByteSource = clusteringComparator.asByteComparable(clustering).asComparableBytes(ByteComparable.Version.OSS41);
            byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
            long valueBlockAddr = pmemRowTree.get(clusteringBytes);
            if (valueBlockAddr == 0)
                return null;

            TransactionalMemoryBlock cellMemoryBlock = heap.memoryBlockFromHandle(valueBlockAddr);
            DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(cellMemoryBlock, heap);
            builder.newRow(clustering);
            try
            {
                int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
                SerializationHeader serializationHeader;
                Map<Integer, SerializationHeader> sHeaderMap = PersistentMemoryMemtable.getTablesMetadataMap().get(tableMetadata.id).getSerializationHeaderMap();
                if (sHeaderMap.size() > 1)
                {
                    serializationHeader = sHeaderMap.get(savedVersion);
                }
                else
                {
                    serializationHeader = new SerializationHeader(false,
                                                                  tableMetadata,
                                                                  tableMetadata.regularAndStaticColumns(),
                                                                  EncodingStats.NO_STATS);
                }
                DeserializationHelper helper = new DeserializationHelper(tableMetadata, -1, DeserializationHelper.Flag.LOCAL);
                unfiltered = (Row) PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
            }
            catch (IOException e)
            {
                closeCartIterator();
            }
            rowIterator = unfiltered == null ? Collections.emptyIterator() : Iterators.singletonIterator(unfiltered);
            return rowIterator;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return deletionTime;
        }

        @Override
        public EncodingStats stats()
        {
            return ref.stats;
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return reversed;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return tableMetadata.regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public void close()
        {
            closeCartIterator();
        }
    }
}



