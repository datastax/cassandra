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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.memtable.PersistentMemoryMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

// keys are Clusterings, and are stored in a MemoryRegion seperate from values
// values are Rows, with the columns serialized (with assocaited header and column information) stored contiguously
// in one MemoryRegion
// basically, this is a sorted hash map, persistent and unsafe, specific to a given partition
public class PmemRowMap
{
    private static TransactionalHeap heap;
    private final LongART rowMapTree;
    private TableMetadata tableMetadata;
    private DeletionTime partitionLevelDeletion;
    private final StatsCollector statsCollector = new StatsCollector();
    private static final Logger logger = LoggerFactory.getLogger(PmemRowMap.class);

    private PmemRowMap(TransactionalHeap heap, LongART arTree, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion)
    {
        this.heap = heap;
        this.rowMapTree = arTree;
        this.tableMetadata = tableMetadata;
        this.partitionLevelDeletion = partitionLevelDeletion;
    }

    public static PmemRowMap create(TransactionalHeap heap, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion)
    {
        LongART arTree = new LongART(heap);
        return (new PmemRowMap(heap, arTree, tableMetadata, partitionLevelDeletion));
    }

    public static PmemRowMap loadFromAddress(TransactionalHeap heap, long address, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion)
    {
        LongART arTree = LongART.fromHandle(heap, address);
        return (new PmemRowMap(heap, arTree, tableMetadata, partitionLevelDeletion));
    }

    public Row getRow(Clustering clustering, TableMetadata metadata)
    {
        Row row = null;
        Row.Builder builder = BTreeRow.sortedBuilder();

        ClusteringComparator clusteringComparator = metadata.comparator;
        ByteSource clusteringByteSource = clusteringComparator.asByteComparable(clustering).asComparableBytes(ByteComparable.Version.OSS41);
        byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
        long rowHandle = rowMapTree.get(clusteringBytes);
        TransactionalMemoryBlock block = heap.memoryBlockFromHandle(rowHandle);
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(block, heap); //TODO: Pass the heap as parameter for now until memoryblock.copyfromArray is sorted out
        try
        {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader;
            Map<Integer, SerializationHeader> sHeaderMap = PersistentMemoryMemtable.getTablesMetadataMap().get(metadata.id).getSerializationHeaderMap();
            if (sHeaderMap.size() > 1)
            {
                serializationHeader = sHeaderMap.get(savedVersion);
            }
            else
            {
                serializationHeader = new SerializationHeader(false,
                                                              metadata,
                                                              metadata.regularAndStaticColumns(),
                                                              EncodingStats.NO_STATS);
            }
            DeserializationHelper helper = new DeserializationHelper(metadata, -1, DeserializationHelper.Flag.LOCAL);
            row = (Row) PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return row;
    }

    public long getHandle()
    {
        return rowMapTree.handle();
    }

    public Row getMergedRow(Row newRow, Long mb)
    {

        Clustering clustering = newRow.clustering();
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(clustering);

        TransactionalMemoryBlock oldBlock = heap.memoryBlockFromHandle(mb);
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(oldBlock, heap); //TODO: Pass the heap as parameter for now until memoryblock.copyfromArray is sorted out

        Row currentRow;
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
            currentRow = (Row) PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        return (Rows.merge(currentRow, newRow));
    }

    Long merge(Object newRow, Long mb)
    {
        Row row = (Row) newRow;
        TransactionalMemoryBlock oldBlock = null;
        TransactionalMemoryBlock cellMemoryRegion;

        if (mb != 0) //Get the existing row to merge with update
        {
            oldBlock = heap.memoryBlockFromHandle(mb);
            row = getMergedRow((Row) newRow, mb);
        }
        SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                          tableMetadata,
                                                                          tableMetadata.regularAndStaticColumns(),
                                                                          EncodingStats.NO_STATS);
        // statsCollector.get());
        SerializationHelper helper = new SerializationHelper(serializationHeader);

        int version = PersistentMemoryMemtable.getTablesMetadataMap().get(tableMetadata.id).getSerializationHeaderMap().size();
        long size = PmemRowSerializer.serializer.serializedSize(row,helper.header,0,version);

        try{

            if (mb == 0)
                cellMemoryRegion = heap.allocateMemoryBlock(size);
            else if (oldBlock.size() < size)
            {
                cellMemoryRegion = heap.allocateMemoryBlock(size);
                oldBlock.free();
            }
            else
            {
                cellMemoryRegion = oldBlock;//TODO: should we zero out if size is greater ?
            }

            DataOutputPlus memoryBlockDataOutputPlus = new MemoryBlockDataOutputPlus(cellMemoryRegion, 0);
            PmemRowSerializer.serializer.serialize(row, helper, memoryBlockDataOutputPlus, 0, version);

        }
        catch (IOException e)
        {
            throw new RuntimeException(e.getMessage()
                                   + " Failed to reload partition ", e);
        }
        long rowAddress = cellMemoryRegion.handle();
        return rowAddress;
    };

    public void put(Row row, PartitionUpdate update)
    {
        /// KEY
        ClusteringComparator clusteringComparator = update.metadata().comparator;
        ByteSource clusteringByteSource = clusteringComparator.asByteComparable(row.clustering()).asComparableBytes(ByteComparable.Version.OSS41);
        byte[] clusteringBytes= ByteSourceInverse.readBytes(clusteringByteSource);
        statsCollector.update(update.stats());
        // need to get version number and list of absolute types here ...
        rowMapTree.put(clusteringBytes, row, this::merge);
    }

    static void clearData(Long mb)
    {
        TransactionalMemoryBlock memoryBlock = heap.memoryBlockFromHandle(mb);
        memoryBlock.free();
    }

    public LongART getRowMapTree()
    {
        return rowMapTree;
    }

    public long size()
    {
        return rowMapTree.size();
    }

    public void delete()
    {
        rowMapTree.clear(PmemRowMap::clearData);
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);
        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }
}
