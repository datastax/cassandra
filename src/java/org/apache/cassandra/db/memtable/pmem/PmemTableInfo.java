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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

//Contains TableID & Cart pointer
// TableID - 36bytes
// Cart pointer - 8 bytes

public class PmemTableInfo {
    public static final PmemTableInfo.Serializer serializer = new PmemTableInfo.Serializer();
    private static final int TABLE_ID_OFFSET = 0;
    private static final int TABLE_ID_SIZE = 36;
    private TransactionalMemoryBlock block;

    //Save version and respective SerializationHeader
    private Map<Integer, SerializationHeader> serializationHeaderMap = new ConcurrentHashMap<>();
    private static final int METADATA_OFFSET = 44;
    private TransactionalMemoryBlock tableMetadatablock;

    public PmemTableInfo(TransactionalHeap heap, long cartAddress, TableId id, Map<Integer, SerializationHeader> sHeaderMap) {
        block = heap.allocateMemoryBlock(TABLE_ID_SIZE + Long.SIZE + Long.SIZE);
        byte[] tableUUIDBytes = id.toString().getBytes();
        block.copyFromArray(tableUUIDBytes, 0, TABLE_ID_OFFSET, tableUUIDBytes.length);
        block.setLong(TABLE_ID_SIZE, cartAddress);

        this.serializationHeaderMap = sHeaderMap;
        serializeSerializationHeader(sHeaderMap, heap);
    }

    public PmemTableInfo(TransactionalMemoryBlock block, TransactionalHeap heap) {
        this.block = block;
        this.tableMetadatablock = heap.memoryBlockFromHandle(getMetadataBlockAddress());
    }

    public static PmemTableInfo fromHandle(TransactionalHeap heap, long tableHandle) {
        return new PmemTableInfo(heap.memoryBlockFromHandle(tableHandle), heap);
    }

    public long handle() {
        return block.handle();
    }

    public long metadataBlockHandle() {
        return tableMetadatablock.handle();
    }

    public TableId getTableID() {
        int len = TABLE_ID_SIZE;

        byte[] b = new byte[len];
        block.copyToArray(TABLE_ID_OFFSET, b, 0, len);
        String str = new String(b);
        return TableId.fromUUID(UUID.fromString(str));
    }

    public long getCartAddress() {
        return block.getLong(TABLE_ID_SIZE);
    }

    public void updateCartAddress(long handle) {
        block.setLong(TABLE_ID_SIZE, handle);
    }

    public long getMetadataBlockAddress() {
        return block.getLong(METADATA_OFFSET);
    }

    public Map<Integer, SerializationHeader> getSerializationHeaderMap() {
        return serializationHeaderMap;
    }

    public Map<Integer, SerializationHeader> deserializeHeader(TableMetadata metadata, TransactionalHeap heap) {

        DataInputPlus in = new MemoryBlockDataInputPlus(tableMetadatablock, heap);

        try {
            serializationHeaderMap = PmemTableInfo.serializer.deserialize(in, metadata);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return serializationHeaderMap;
    }

    public void serializeSerializationHeader(Map<Integer, SerializationHeader> tableMetaDataMap, TransactionalHeap heap) {
        long blockSize = serializer.serializedSize(tableMetaDataMap);
        tableMetadatablock = heap.allocateMemoryBlock(blockSize);
        block.setLong(METADATA_OFFSET, tableMetadatablock.handle());
        DataOutputPlus out = new MemoryBlockDataOutputPlus(tableMetadatablock, 0);
        try {
            PmemTableInfo.serializer.serialize(tableMetaDataMap, out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Serializer {
        public void serialize(Map<Integer, SerializationHeader> tableMetaDataMap, DataOutputPlus out) throws IOException {
            out.writeUnsignedVInt(tableMetaDataMap.size());
            for (Map.Entry<Integer, SerializationHeader> entry : tableMetaDataMap.entrySet()) {
                out.writeUnsignedVInt(entry.getKey());
                SerializationHeader.serializer.serialize(BigFormat.latestVersion, entry.getValue().toComponent(), out);
            }
        }

        public Map<Integer, SerializationHeader> deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
            int count = (int) in.readUnsignedVInt();
            Map<Integer, SerializationHeader> headerMap = new ConcurrentHashMap<>();
            for (int k = 0; k < count; k++) {
                Integer savedVesrion = (int) in.readUnsignedVInt();
                SerializationHeader.Component component = SerializationHeader.serializer.deserialize(BigFormat.latestVersion, in);

                SerializationHeader header = toSerializationHeader(component, metadata);
                headerMap.putIfAbsent(savedVesrion, header);
            }
            return headerMap;
        }

        public long serializedSize(Map<Integer, SerializationHeader> tableMetaDataMap) {
            long size = 0;
            size += TypeSizes.sizeofUnsignedVInt(tableMetaDataMap.size());

            for (Map.Entry<Integer, SerializationHeader> entry : tableMetaDataMap.entrySet()) {
                size += TypeSizes.sizeofUnsignedVInt(entry.getKey());
                size += SerializationHeader.serializer.serializedSize(BigFormat.latestVersion, entry.getValue().toComponent());
            }
            return size;
        }

        // Construct SerializationHeader  from  SerializationHeader.Component
        private static SerializationHeader toSerializationHeader(SerializationHeader.Component component, TableMetadata metadata) {

            TableMetadata.Builder metadataBuilder = TableMetadata.builder(metadata.keyspace, metadata.name, metadata.id);
            component.getStaticColumns().entrySet().stream()
                    .forEach(entry -> {
                        ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                        metadataBuilder.addStaticColumn(ident, entry.getValue());
                    });
            component.getRegularColumns().entrySet().stream()
                    .forEach(entry -> {
                        ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                        metadataBuilder.addRegularColumn(ident, entry.getValue());
                    });
            for (ColumnMetadata columnMetadata : metadata.partitionKeyColumns())
                metadataBuilder.addPartitionKeyColumn(columnMetadata.name, columnMetadata.cellValueType());
            for (ColumnMetadata columnMetadata : metadata.clusteringColumns()) {
                metadataBuilder.addClusteringColumn(columnMetadata.name, columnMetadata.cellValueType());
            }

            TableMetadata tableMetadata = metadataBuilder.build();
            SerializationHeader serializationHeader = new SerializationHeader(false,
                    tableMetadata,
                    tableMetadata.regularAndStaticColumns(),
                    EncodingStats.NO_STATS);
            return serializationHeader;
        }
    }
}

