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
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.memtable.PersistentMemoryMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

// keys are Clusterings, and are stored in a MemoryRegion seperate from values
// values are Rows, with the columns serialized (with assocaited header and column information) stored contiguously
// in one MemoryRegion
// basically, this is a sorted hash map, persistent and unsafe, specific to a given partition
public class PmemRowMap {
    private static TransactionalHeap heap;
    private final LongART rowMapTree;
    private TableMetadata tableMetadata;
    private DeletionTime partitionLevelDeletion;
    private static final Logger logger = LoggerFactory.getLogger(PmemRowMap.class);
    private final LongART rangeTombstoneMarkerTree;
    private final UpdateTransaction indexer;

    private PmemRowMap(TransactionalHeap heap, LongART rangeTombstoneMarkerTree, LongART arTree, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion, UpdateTransaction indexer) {
        this.heap = heap;
        this.rowMapTree = arTree;
        this.tableMetadata = tableMetadata;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.rangeTombstoneMarkerTree = rangeTombstoneMarkerTree;
        this.indexer = indexer;
    }

    public static PmemRowMap create(TransactionalHeap heap, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion, UpdateTransaction indexer) {
        LongART arTree = new LongART(heap);
        return (new PmemRowMap(heap, null, arTree, tableMetadata, partitionLevelDeletion, indexer));
    }

    public static PmemRowMap createForTombstone(TransactionalHeap heap, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion, UpdateTransaction indexer) {
        LongART arTree = new LongART(heap);
        return (new PmemRowMap(heap, arTree, null, tableMetadata, partitionLevelDeletion, indexer));
    }

    public static PmemRowMap loadFromAddress(TransactionalHeap heap, long address, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion, UpdateTransaction indexer) {
        LongART arTree = LongART.fromHandle(heap, address);
        return (new PmemRowMap(heap, null, arTree, tableMetadata, partitionLevelDeletion, indexer));
    }

    public static PmemRowMap loadFromRtmHandle(TransactionalHeap heap, long handle, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion, UpdateTransaction indexer) {
        LongART arTree = LongART.fromHandle(heap, handle);
        return (new PmemRowMap(heap, arTree, null, tableMetadata, partitionLevelDeletion, indexer));
    }

    public Row getRow(Clustering clustering, TableMetadata metadata) {
        Row row = null;
        Row.Builder builder = BTreeRow.sortedBuilder();

        ClusteringComparator clusteringComparator = metadata.comparator;
        ByteSource clusteringByteSource = clusteringComparator.asByteComparable(clustering).asComparableBytes(ByteComparable.Version.OSS41);
        byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
        long rowHandle = rowMapTree.get(clusteringBytes);
        TransactionalMemoryBlock block = heap.memoryBlockFromHandle(rowHandle);
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(block, heap);
        try {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader;
            TableId id = metadata.isIndex() ? TableId.fromUUID(UUID.nameUUIDFromBytes(metadata.indexName().get().getBytes())) : metadata.id;
            Map<Integer, SerializationHeader> sHeaderMap = PersistentMemoryMemtable.getTablesMetadataMap().get(id).getSerializationHeaderMap();
            if (sHeaderMap.size() > 1) {
                serializationHeader = sHeaderMap.get(savedVersion);
            } else {
                serializationHeader = new SerializationHeader(false,
                        metadata,
                        metadata.regularAndStaticColumns(),
                        EncodingStats.NO_STATS);
            }
            DeserializationHelper helper = new DeserializationHelper(metadata, -1, DeserializationHelper.Flag.LOCAL);
            row = (Row) PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return row;
    }

    public long getHandle() {
        return rowMapTree.handle();
    }

    public long getRtmTreeHandle() {
        return rangeTombstoneMarkerTree.handle();
    }

    public Row getMergedRow(Row newRow, Long mb) {

        Clustering clustering = newRow.clustering();
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(clustering);

        TransactionalMemoryBlock oldBlock = heap.memoryBlockFromHandle(mb);
        DataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(oldBlock, heap);

        Row currentRow;
        try {
            int savedVersion = (int) memoryBlockDataInputPlus.readUnsignedVInt();
            SerializationHeader serializationHeader;
            TableId id = tableMetadata.isIndex() ? TableId.fromUUID(UUID.nameUUIDFromBytes(tableMetadata.indexName().get().getBytes())) : tableMetadata.id;
            Map<Integer, SerializationHeader> sHeaderMap = PersistentMemoryMemtable.getTablesMetadataMap().get(id).getSerializationHeaderMap();
            if (sHeaderMap.size() > 1) {
                serializationHeader = sHeaderMap.get(savedVersion);
            } else {
                serializationHeader = new SerializationHeader(false,
                        tableMetadata,
                        tableMetadata.regularAndStaticColumns(),
                        EncodingStats.NO_STATS);
            }
            DeserializationHelper helper = new DeserializationHelper(tableMetadata, -1, DeserializationHelper.Flag.LOCAL);
            currentRow = (Row) PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
        } catch (IOException e) {
            throw new IOError(e);
        }
        if (currentRow != null) {
            Row reconciled = Rows.merge(currentRow, newRow);
            indexer.onUpdated(currentRow, reconciled);
            return reconciled;
        } else
            return currentRow;
    }

    Long merge(Object newRow, Long mb) {
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
        SerializationHelper helper = new SerializationHelper(serializationHeader);

        //Since index table cannot be altered, the version will be always 1.
        int version = tableMetadata.isIndex() ? 1 : PersistentMemoryMemtable.getTablesMetadataMap().get(tableMetadata.id).getSerializationHeaderMap().size();
        long size = PmemRowSerializer.serializer.serializedSize(row, helper.header, 0, version);

        try {

            if (mb == 0) {
                cellMemoryRegion = heap.allocateMemoryBlock(size);
                indexer.onInserted(row);
            } else if (oldBlock.size() < size) {
                cellMemoryRegion = heap.allocateMemoryBlock(size);
                oldBlock.free();
            } else {
                cellMemoryRegion = oldBlock;
            }

            DataOutputPlus memoryBlockDataOutputPlus = new MemoryBlockDataOutputPlus(cellMemoryRegion, 0);
            PmemRowSerializer.serializer.serialize(row, helper, memoryBlockDataOutputPlus, 0, version);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage()
                    + " Failed to reload partition ", e);
        }
        long rowAddress = cellMemoryRegion.handle();
        return rowAddress;
    }

    ;

    Long saveRTM(Object newRTM, Long mb) {
        Unfiltered unfiltered = (Unfiltered) newRTM;
        TransactionalMemoryBlock oldBlock = null;
        TransactionalMemoryBlock rtmMemoryRegion;
        SerializationHeader serializationHeader = new SerializationHeader(false,
                tableMetadata,
                tableMetadata.regularAndStaticColumns(),
                EncodingStats.NO_STATS);
        SerializationHelper helper = new SerializationHelper(serializationHeader);

        //Since index table cannot be altered, the version will be always 1.
        int version = tableMetadata.isIndex() ? 1 : PersistentMemoryMemtable.getTablesMetadataMap().get(tableMetadata.id).getSerializationHeaderMap().size();
        long size = PmemRowSerializer.serializer.serializedSize(unfiltered, helper.header, 0, version);

        try {
            if (mb != 0) {
                oldBlock = heap.memoryBlockFromHandle(mb);
                if (oldBlock.size() < size) {
                    rtmMemoryRegion = heap.allocateMemoryBlock(size);
                    oldBlock.free();
                } else {
                    rtmMemoryRegion = oldBlock;
                }
            } else {
                rtmMemoryRegion = heap.allocateMemoryBlock(size);
            }

            DataOutputPlus memoryBlockDataOutputPlus = new MemoryBlockDataOutputPlus(rtmMemoryRegion, 0);
            PmemRowSerializer.serializer.serialize(unfiltered, helper, memoryBlockDataOutputPlus, 0, version);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage()
                    + " Failed to save RTM ", e);
        }

        return rtmMemoryRegion.handle();
    }

    ;

    public void put(Row row, PartitionUpdate update) {
        ClusteringComparator clusteringComparator = update.metadata().comparator;
        ByteSource clusteringByteSource = clusteringComparator.asByteComparable(row.clustering()).asComparableBytes(ByteComparable.Version.OSS41);
        byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
        //Calling the merge method to insert/update rows
        rowMapTree.put(clusteringBytes, row, this::merge);
    }

    public void putRangeTombstoneMarker(Unfiltered rtm, PartitionUpdate update) {
        ClusteringComparator clusteringComparator = update.metadata().comparator;
        ByteSource clusteringByteSource = clusteringComparator.asByteComparable(rtm.clustering()).asComparableBytes(ByteComparable.Version.OSS41);
        byte[] clusteringBytes = ByteSourceInverse.readBytes(clusteringByteSource);
        rangeTombstoneMarkerTree.put(clusteringBytes, rtm, this::saveRTM);
    }

    static void clearData(Long mb) {
        TransactionalMemoryBlock memoryBlock = heap.memoryBlockFromHandle(mb);
        memoryBlock.free();
    }

    public LongART getRowMapTree() {
        return rowMapTree;
    }

    public LongART getRangeTombstoneMarkerTree() {
        return rangeTombstoneMarkerTree;
    }

    public long size() {
        return rowMapTree.size();
    }

    public void delete() {
        rowMapTree.clear(PmemRowMap::clearData);
    }
}
