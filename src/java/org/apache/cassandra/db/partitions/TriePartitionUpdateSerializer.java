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

package org.apache.cassandra.db.partitions;

import java.io.IOException;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.db.tries.ContentManagerPojo;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Serializer for {@link TriePartitionUpdate} across messaging versions (VERSION_DS_21+).
 * Encodes partition key, row and tombstone counts, data size footprint, {@link SerializationHeader},
 * and serializes the underlying {@link InMemoryDeletionAwareTrie} structure using a type-tagged {@link ContentManagerPojo.PojoSerializer}.
 */
public class TriePartitionUpdateSerializer
{
    private static final int TYPE_NULL = 0;
    private static final int TYPE_ROW = 1;
    private static final int TYPE_MARKER = 2;
    private static final int TYPE_CELL = 3;
    private static final int TYPE_LIVENESS = 4;
    private static final int TYPE_COMPLEX_MARKER = 5;
    private static final int TYPE_PARTITION_MARKER = 6;
    private static final int TYPE_TRIE_TOMBSTONE_MARKER = 7;

    private TriePartitionUpdateSerializer()
    {
    }

    /**
     * Serializes a partition update using the trie-native wire format.
     * Converts {@link PartitionUpdate} to {@link TriePartitionUpdate} if needed, writes partition key and counters,
     * emits messaging {@link SerializationHeader}, and writes the deletion-aware trie structure.
     */
    public static void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
    {
        TriePartitionUpdate trieUpdate = TriePartitionUpdate.asTrieUpdate(update);

        ByteBufferUtil.writeWithVIntLength(trieUpdate.partitionKey().getKey(), out);
        out.writeInt(trieUpdate.rowCountIncludingStatic);
        out.writeInt(trieUpdate.tombstoneCount);
        out.writeInt(trieUpdate.dataSize());

        SerializationHeader header = new SerializationHeader(false, trieUpdate.metadata(), trieUpdate.columns(), trieUpdate.stats());
        SerializationHeader.serializer.serializeForMessaging(header, null, out, true);
        SerializationHelper helper = new SerializationHelper(header);

        ContentManagerPojo.PojoSerializer<Object> pojoSerializer = createPojoSerializer(trieUpdate.metadata(), helper, header, version, null);

        @SuppressWarnings("unchecked")
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie =
            (InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>) trieUpdate.trie();

        trie.serialize(out, pojoSerializer);
    }

    /**
     * Deserializes a {@link TriePartitionUpdate} from an input stream using messaging version, table metadata, and default ON_HEAP buffer type.
     */
    public static TriePartitionUpdate deserialize(DataInputPlus in, int version, DeserializationHelper.Flag flag, TableMetadata metadata) throws IOException
    {
        return deserialize(in, version, flag, metadata, BufferType.ON_HEAP);
    }

    /**
     * Deserializes a {@link TriePartitionUpdate} from an input stream using messaging version, table metadata, and target buffer type.
     * Reconstructs decorated key, header, and underlying {@link InMemoryDeletionAwareTrie} state.
     */
    public static TriePartitionUpdate deserialize(DataInputPlus in, int version, DeserializationHelper.Flag flag, TableMetadata metadata, BufferType bufferType) throws IOException
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
        int rowCountIncludingStatic = in.readInt();
        int tombstoneCount = in.readInt();
        int dataSize = in.readInt();

        if (rowCountIncludingStatic < 0 || tombstoneCount < 0 || dataSize < 0)
            throw new IOException(String.format("Corrupt TriePartitionUpdate header counts: rows=%d, tombstones=%d, dataSize=%d",
                                                rowCountIncludingStatic, tombstoneCount, dataSize));

        SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, null, true);
        DeserializationHelper desHelper = new DeserializationHelper(metadata, version, flag);

        ContentManagerPojo.PojoSerializer<Object> pojoSerializer = createPojoSerializer(metadata, null, header, version, desHelper);

        try
        {
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie =
                InMemoryDeletionAwareTrie.deserialize(in, TrieBackedRow::shouldPreserveContentWithoutChildren, bufferType, InMemoryBaseTrie.ExpectedLifetime.SHORT, null, pojoSerializer);

            return new TriePartitionUpdate(metadata, key, header.columns(), header.stats(), rowCountIncludingStatic, tombstoneCount, dataSize, trie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new IOException("Trie space exhausted during deserialization", e);
        }
    }

    /**
     * Computes serialized size of {@link TriePartitionUpdate} wire format.
     */
    public static long serializedSize(PartitionUpdate update, int version)
    {
        TriePartitionUpdate trieUpdate = TriePartitionUpdate.asTrieUpdate(update);
        SerializationHeader header = new SerializationHeader(false, trieUpdate.metadata(), trieUpdate.columns(), trieUpdate.stats());
        SerializationHelper helper = new SerializationHelper(header);
        ContentManagerPojo.PojoSerializer<Object> pojoSerializer = createPojoSerializer(trieUpdate.metadata(), helper, header, version, null);

        return ByteBufferUtil.serializedSizeWithVIntLength(trieUpdate.partitionKey().getKey())
               + 3L * Integer.BYTES
               + SerializationHeader.serializer.serializedSizeForMessaging(header, null, true)
               + serializedTrieSize(trieUpdate, pojoSerializer);
    }

    private static long serializedTrieSize(TriePartitionUpdate trieUpdate, ContentManagerPojo.PojoSerializer<Object> pojoSerializer)
    {
        @SuppressWarnings("unchecked")
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie =
            (InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>) trieUpdate.trie();
        return trie.serializedSize(pojoSerializer);
    }

    private static final ColumnMetadata[] EMPTY_COLS = new ColumnMetadata[0];

    /// Locates a column index within the serialization header columns.
    ///
    /// @param targetCols regular or static column metadata from the serialization header
    /// @param col column metadata to search for
    /// @return 0-based index of the column in the header array
    /// @throws IllegalArgumentException if the column is missing from the header
    private static int indexOfColumn(ColumnMetadata[] targetCols, ColumnMetadata col)
    {
        int len = targetCols.length;
        if (len == 1)
        {
            if (targetCols[0] == col || targetCols[0].equals(col))
                return 0;
        }
        else if (len == 2)
        {
            if (targetCols[0] == col || targetCols[0].equals(col)) return 0;
            if (targetCols[1] == col || targetCols[1].equals(col)) return 1;
        }
        else
        {
            for (int i = 0; i < len; i++)
            {
                if (targetCols[i] == col)
                    return i;
            }
            for (int i = 0; i < len; i++)
            {
                if (targetCols[i].equals(col))
                    return i;
            }
        }
        throw new IllegalArgumentException("Column not found in serialization header: " + col.name);
    }

    /**
     * Creates a {@link ContentManagerPojo.PojoSerializer} adapter for trie node payloads.
     */
    private static ContentManagerPojo.PojoSerializer<Object> createPojoSerializer(TableMetadata metadata,
                                                                                  SerializationHelper helper,
                                                                                  SerializationHeader header,
                                                                                  int version,
                                                                                  DeserializationHelper desHelper)
    {
        return new TriePayloadSerializer(header, helper, version, desHelper);
    }

    /**
     * Static PojoSerializer implementation for heterogeneous trie node payloads.
     * Encodes and decodes {@link Cell}, {@link Row}, {@link RangeTombstoneMarker}, {@link LivenessInfo}, and {@link TrieTombstoneMarker}.
     */
    private static class TriePayloadSerializer implements ContentManagerPojo.PojoSerializer<Object>
    {
        private final SerializationHeader header;
        private final SerializationHelper helper;
        private final int version;
        private final DeserializationHelper desHelper;
        private final ColumnMetadata[] regularCols;
        private final ColumnMetadata[] staticCols;

        TriePayloadSerializer(SerializationHeader header, SerializationHelper helper, int version, DeserializationHelper desHelper)
        {
            this.header = header;
            this.helper = helper;
            this.version = version;
            this.desHelper = desHelper;

            Columns regs = header.columns().regulars;
            Columns stats = header.columns().statics;
            this.regularCols = regs.isEmpty() ? EMPTY_COLS : regs.toArray(new ColumnMetadata[regs.size()]);
            this.staticCols = stats.isEmpty() ? EMPTY_COLS : stats.toArray(new ColumnMetadata[stats.size()]);
        }

        @Override
        public void serialize(Object content, DataOutputPlus out) throws IOException
        {
            if (content == null)                                 { out.writeByte(TYPE_NULL); return; }
            if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)  { out.writeByte(TYPE_COMPLEX_MARKER); return; }
            if (content == TrieBackedPartition.PARTITION_MARKER) { out.writeByte(TYPE_PARTITION_MARKER); return; }

            if (content instanceof Cell)                 { out.writeByte(TYPE_CELL); serializeCell((Cell<?>) content, out); return; }
            if (content instanceof Row)                  { out.writeByte(TYPE_ROW); UnfilteredSerializer.serializer.serialize((Row) content, helper, out, version); return; }
            if (content instanceof RangeTombstoneMarker) { out.writeByte(TYPE_MARKER); UnfilteredSerializer.serializer.serialize((RangeTombstoneMarker) content, helper, out, version); return; }
            if (content instanceof LivenessInfo)         { out.writeByte(TYPE_LIVENESS); serializeLiveness((LivenessInfo) content, out); return; }
            if (content instanceof TrieTombstoneMarker)  { out.writeByte(TYPE_TRIE_TOMBSTONE_MARKER); serializeTrieTombstoneMarker((TrieTombstoneMarker) content, out); return; }

            throw new IllegalArgumentException("Unknown content type in trie: " + content.getClass().getName());
        }

        @Override
        public Object deserialize(DataInputPlus in) throws IOException
        {
            int type = in.readByte();
            switch (type)
            {
                case TYPE_NULL:                  return null;
                case TYPE_CELL:                  return deserializeCell(in);
                case TYPE_ROW:                   return UnfilteredSerializer.serializer.deserialize(in, header, desHelper, BTreeRow.sortedBuilder());
                case TYPE_MARKER:                return UnfilteredSerializer.serializer.deserialize(in, header, desHelper, BTreeRow.sortedBuilder());
                case TYPE_COMPLEX_MARKER:        return TrieBackedRow.COMPLEX_COLUMN_MARKER;
                case TYPE_PARTITION_MARKER:      return TrieBackedPartition.PARTITION_MARKER;
                case TYPE_LIVENESS:              return deserializeLiveness(in);
                case TYPE_TRIE_TOMBSTONE_MARKER: return deserializeTrieTombstoneMarker(in);
                default:                         throw new IOException("Unknown content type tag: " + type);
            }
        }

        @Override
        public long serializedSize(Object content)
        {
            if (content == null || content == TrieBackedRow.COMPLEX_COLUMN_MARKER || content == TrieBackedPartition.PARTITION_MARKER)
                return 1L;

            if (content instanceof Cell)                 return 1L + serializedSizeCell((Cell<?>) content);
            if (content instanceof Row)                  return 1L + UnfilteredSerializer.serializer.serializedSize((Row) content, helper, version);
            if (content instanceof RangeTombstoneMarker) return 1L + UnfilteredSerializer.serializer.serializedSize((RangeTombstoneMarker) content, helper, version);
            if (content instanceof LivenessInfo)         return 1L + serializedSizeLiveness((LivenessInfo) content);
            if (content instanceof TrieTombstoneMarker)  return 1L + serializedSizeTrieTombstoneMarker((TrieTombstoneMarker) content);

            throw new IllegalArgumentException("Unknown content type in trie: " + content.getClass().getName());
        }

        private void serializeCell(Cell<?> cell, DataOutputPlus out) throws IOException
        {
            ColumnMetadata col = cell.column();
            boolean isStatic = col.isStatic();
            ColumnMetadata[] targetCols = isStatic ? staticCols : regularCols;
            int idx = indexOfColumn(targetCols, col);
            out.writeUnsignedVInt32((idx << 1) | (isStatic ? 1 : 0));
            Cell.serializer.serialize(cell, cell.column(), out, LivenessInfo.EMPTY, header);
        }

        private Cell<?> deserializeCell(DataInputPlus in) throws IOException
        {
            int encoded = in.readUnsignedVInt32();
            boolean isStatic = (encoded & 1) != 0;
            int idx = encoded >>> 1;
            ColumnMetadata[] targetCols = isStatic ? staticCols : regularCols;
            if (idx < 0 || idx >= targetCols.length)
                throw new IOException(String.format("Invalid column index %d for %s static columns (total %d)", idx, isStatic ? "" : "non-", targetCols.length));
            ColumnMetadata column = targetCols[idx];
            return Cell.serializer.deserialize(in, LivenessInfo.EMPTY, column, header, desHelper, ByteBufferAccessor.instance);
        }

        private long serializedSizeCell(Cell<?> cell)
        {
            ColumnMetadata col = cell.column();
            boolean isStatic = col.isStatic();
            ColumnMetadata[] targetCols = isStatic ? staticCols : regularCols;
            int idx = indexOfColumn(targetCols, col);
            return TypeSizes.sizeofUnsignedVInt((idx << 1) | (isStatic ? 1 : 0))
                   + Cell.serializer.serializedSize(cell, cell.column(), LivenessInfo.EMPTY, header);
        }

        private void serializeLiveness(LivenessInfo info, DataOutputPlus out) throws IOException
        {
            boolean isExpiring = info.isExpiring();
            out.writeBoolean(isExpiring);
            out.writeLong(info.timestamp());
            if (isExpiring)
            {
                out.writeInt(info.ttl());
                out.writeLong(info.localExpirationTime());
            }
        }

        private LivenessInfo deserializeLiveness(DataInputPlus in) throws IOException
        {
            boolean isExpiring = in.readBoolean();
            long ts = in.readLong();
            if (isExpiring)
            {
                int ttl = in.readInt();
                long localExp = in.readLong();
                return LivenessInfo.withExpirationTime(ts, ttl, localExp);
            }
            return LivenessInfo.create(ts, 0);
        }

        private long serializedSizeLiveness(LivenessInfo info)
        {
            return 1L + 8L + (info.isExpiring() ? (4L + 8L) : 0L);
        }
    }

    /**
     * Serializes a {@link TrieTombstoneMarker.Covering} instance (boolean presence tag, {@link DeletionTime}, and {@link TrieTombstoneMarker.Kind} ordinal).
     */
    private static void serializeCovering(TrieTombstoneMarker.Covering covering, DataOutputPlus out) throws IOException
    {
        if (covering == null)
        {
            out.writeBoolean(false);
        }
        else
        {
            out.writeBoolean(true);
            DeletionTime.serializer.serialize(covering, out);
            out.writeByte(covering.deletionKind().ordinal());
        }
    }

    private static final TrieTombstoneMarker.Kind[] KINDS = TrieTombstoneMarker.Kind.values();

    /**
     * Deserializes a {@link TrieTombstoneMarker.Covering} instance from stream.
     */
    private static TrieTombstoneMarker.Covering deserializeCovering(DataInputPlus in) throws IOException
    {
        if (!in.readBoolean())
            return null;
        DeletionTime dt = DeletionTime.serializer.deserialize(in);
        int ordinal = in.readByte() & 0xFF;
        if (ordinal >= KINDS.length)
            throw new IOException("Invalid TrieTombstoneMarker.Kind ordinal: " + ordinal);
        TrieTombstoneMarker.Kind kind = KINDS[ordinal];
        return TrieTombstoneMarker.covering(dt, kind);
    }

    /**
     * Computes serialized size of {@link TrieTombstoneMarker.Covering}.
     */
    private static long serializedSizeCovering(TrieTombstoneMarker.Covering covering)
    {
        if (covering == null)
            return 1L;
        return 1L + DeletionTime.serializer.serializedSize(covering) + 1L;
    }

    /**
     * Serializes a {@link TrieTombstoneMarker} across left, right, and point deletion bounds.
     */
    private static void serializeTrieTombstoneMarker(TrieTombstoneMarker marker, DataOutputPlus out) throws IOException
    {
        serializeCovering(marker.leftDeletion(), out);
        serializeCovering(marker.rightDeletion(), out);
        serializeCovering(marker.pointDeletion(), out);
        out.writeBoolean(marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW));
    }

    /**
     * Deserializes a {@link TrieTombstoneMarker} from stream.
     */
    private static TrieTombstoneMarker deserializeTrieTombstoneMarker(DataInputPlus in) throws IOException
    {
        TrieTombstoneMarker.Covering left = deserializeCovering(in);
        TrieTombstoneMarker.Covering right = deserializeCovering(in);
        TrieTombstoneMarker.Covering point = deserializeCovering(in);
        boolean hasRowLevelMarker = in.readBoolean();
        TrieTombstoneMarker.LevelMarker levelMarker = hasRowLevelMarker ? TrieTombstoneMarker.LevelMarker.ROW : null;

        if (point != null)
            return new TrieTombstoneMarker.Point(point, left, right);

        return TrieTombstoneMarker.make(left, right, levelMarker);
    }

    /**
     * Computes serialized size of {@link TrieTombstoneMarker}.
     */
    private static long serializedSizeTrieTombstoneMarker(TrieTombstoneMarker marker)
    {
        return serializedSizeCovering(marker.leftDeletion())
               + serializedSizeCovering(marker.rightDeletion())
               + serializedSizeCovering(marker.pointDeletion())
               + 1L;
    }
}
