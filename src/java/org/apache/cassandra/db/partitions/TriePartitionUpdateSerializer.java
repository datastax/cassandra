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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.BufferCellData;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.OnDiskCursor;
import org.apache.cassandra.db.tries.OnDiskDeletionAwareTrie;
import org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieWriter;
import org.apache.cassandra.db.tries.OnDiskTrieWriter;
import org.apache.cassandra.io.util.CountingDataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.LimitedDataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Serializer for {@link TriePartitionUpdate} across messaging versions (VERSION_DS_21+).
 * Encodes partition key, whether the update is empty, row and tombstone counts, data size footprint,
 * {@link SerializationHeader}, the index the trie keys each of the header's columns by, and writes the update's
 * deletion-aware trie in the on-disk trie format
 * with {@link OnDiskDeletionAwareTrieWriter}, node payloads being encoded by {@link ContentSerDe} in the live branch
 * and by {@link MarkerSerDe} in the deletion branches.
 * <p>
 * Clusterings and cell paths are written only as the trie's keys, and are read back from them through byte-comparable
 * decoding, which gives a value equal to the one written under its type's comparison but not always the same bytes:
 * {@code DecimalType} drops trailing zeros, so a decimal clustering or a {@code set<decimal>} element written as 1.00
 * is read back as 1.
 */
public class TriePartitionUpdateSerializer
{
    // Tags 0 to 3 are unused, and 3 is reserved: do not reuse it.
    private static final int TYPE_LIVENESS = 4;
    private static final int TYPE_COMPLEX_MARKER = 5;
    private static final int TYPE_PARTITION_MARKER = 6;
    private static final int TYPE_TRIE_TOMBSTONE_MARKER = 7;
    /// [LivenessInfo#EMPTY], which marks a row written without primary key liveness (an UPDATE rather than an
    /// INSERT). Readers compare against it by identity, so it must come back as that instance, and it carries
    /// nothing worth encoding.
    private static final int TYPE_EMPTY_LIVENESS = 8;
    private static final int TYPE_CELL = 9;

    /// The flags a cell's payload starts with after its tag. Its timestamp follows, then its local deletion time if it
    /// is deleted or expiring and its TTL if it is expiring, and its value takes the rest of the payload, whose length
    /// the trie records. Its column and cell path are not written: the cell's key in the trie gives them. Nor is whether
    /// it is a counter: a cell is one if it is live and its table is a counter table, as [Cell#isCounterCell] has it. A
    /// cell is never both deleted and expiring, and no other flag is used.
    private static final int CELL_IS_DELETED = 0x01;
    private static final int CELL_IS_EXPIRING = 0x02;

    /// The flags an update starts with after its partition key. The only one is set if the update is empty, as
    /// [TriePartitionUpdate#isEmpty] has it, so that whether a serialized update is empty can be read from its bytes
    /// without its table or its trie ([PartitionUpdate.PartitionUpdateSerializer#isEmpty]), as Paxos does to tell
    /// whether a proposal it keeps is empty. The row and tombstone counts do not tell: neither counts a row whose only
    /// content is the deletion of a complex column.
    static final int UPDATE_IS_EMPTY = 0x01;

    private TriePartitionUpdateSerializer()
    {
    }

    /// The trie encoding exists at the versions whose partition updates carry the format byte that says which encoding
    /// follows, and only there.
    private static void checkVersion(int version)
    {
        if (!PartitionUpdate.PartitionUpdateSerializer.hasFormatByte(version))
            throw new IllegalArgumentException("No trie encoding of a partition update at messaging version " + version);
    }

    /**
     * Serializes a partition update using the trie-native wire format.
     * Writes partition key and counters, emits messaging {@link SerializationHeader}, and writes the deletion-aware
     * trie structure.
     */
    public static void serialize(TriePartitionUpdate trieUpdate, DataOutputPlus out, int version) throws IOException
    {
        checkVersion(version);
        ByteBufferUtil.writeWithVIntLength(trieUpdate.partitionKey().getKey(), out);
        out.writeByte(trieUpdate.isEmpty() ? UPDATE_IS_EMPTY : 0);
        out.writeUnsignedVInt32(trieUpdate.rowCountIncludingStatic);
        out.writeUnsignedVInt32(trieUpdate.tombstoneCount);
        out.writeUnsignedVInt32(trieUpdate.dataSize());

        SerializationHeader header = new SerializationHeader(false, trieUpdate.metadata(), trieUpdate.columns(), trieUpdate.stats());
        SerializationHeader.serializer.serializeForMessaging(header, null, out, true);
        serializeColumnIndexes(trieUpdate.metadata(), header.columns(), out);

        writeTrie(trieUpdate, header, out);
    }

    /// Writes the index the update's trie keys each of the header's columns by, static ones first: the column's position
    /// among all the static or regular columns of the update's table, as [TrieBackedRow] numbers them. A receiver
    /// whose schema numbers the columns differently -- one that has added or dropped a column the sender has not, or
    /// the other way round -- cannot use the trie's keys as they are, and needs these to translate them.
    private static void serializeColumnIndexes(TableMetadata metadata, RegularAndStaticColumns columns, DataOutputPlus out) throws IOException
    {
        for (ColumnMetadata column : columns.statics)
            out.writeUnsignedVInt32(columnIndex(metadata, column));
        for (ColumnMetadata column : columns.regulars)
            out.writeUnsignedVInt32(columnIndex(metadata, column));
    }

    private static long columnIndexesSerializedSize(TableMetadata metadata, RegularAndStaticColumns columns)
    {
        long size = 0;
        for (ColumnMetadata column : columns.statics)
            size += TypeSizes.sizeofUnsignedVInt(columnIndex(metadata, column));
        for (ColumnMetadata column : columns.regulars)
            size += TypeSizes.sizeofUnsignedVInt(columnIndex(metadata, column));
        return size;
    }

    private static int columnIndex(TableMetadata metadata, ColumnMetadata column)
    {
        int index = localIndex(metadata, column);
        if (index < 0)
            throw new IllegalStateException("Column " + column.name + " of a partition update is not a column of " + metadata);
        return index;
    }

    /// The column's position among the static or regular columns of this node's version of the table, or a negative
    /// number if this node does not have the column.
    private static int localIndex(TableMetadata metadata, ColumnMetadata column)
    {
        return TrieBackedRow.columnIdsMap(metadata.regularAndStaticColumns().columns(column.isStatic())).getValue(column.name);
    }

    private static int[] deserializeColumnIndexes(DataInputPlus in, Columns columns) throws IOException
    {
        int[] indexes = new int[columns.size()];
        for (int i = 0; i < indexes.length; ++i)
            indexes[i] = in.readUnsignedVInt32();
        return indexes;
    }

    /// Whether the sender keyed the given columns by the indices this node gives them, so that the trie can be read
    /// as it is. A column this node does not have, i.e. one dropped here, has no index of its own.
    private static boolean hasLocalIndexes(TableMetadata metadata, Columns columns, int[] indexes)
    {
        int i = 0;
        for (ColumnMetadata column : columns)
        {
            if (localIndex(metadata, column) != indexes[i++])
                return false;
        }
        return true;
    }

    /// Writes the trie in the on-disk trie format, length-prefixed.
    ///
    /// The length is needed because the reader walks the serialized form in place rather than
    /// consuming it from the stream, so it has to be handed the trie's bytes as a block. The
    /// writer emits the root last and pointers run backwards, so the trie is only readable once
    /// its extent is known.
    ///
    /// An output that stops at a limit, as a mutation is written through to be kept only if it is smaller than the
    /// cacheable size, would refuse a trie too long for it only once all of it was laid out. The trie is then laid out
    /// under the room the output has, less the one byte its length takes at the least, so that the layout stops as
    /// soon as the trie cannot fit, and the output decides on one that can.
    private static void writeTrie(TriePartitionUpdate trieUpdate,
                                  SerializationHeader header,
                                  DataOutputPlus out) throws IOException
    {
        try (DataOutputBuffer trieBytes = new DataOutputBuffer())
        {
            layOut(trieUpdate,
                   header,
                   out instanceof LimitedDataOutputPlus
                   ? new LimitedDataOutputPlus(trieBytes, ((LimitedDataOutputPlus) out).remaining() - 1)
                   : trieBytes);
            // out.write copies the buffer synchronously and leaves its position alone, and nothing
            // touches trieBytes after this, so the trie does not have to be copied out first.
            ByteBufferUtil.writeWithVIntLength(trieBytes.unsafeGetBufferAndFlip(), out);
        }
    }

    /// Lays the update's trie out in the on-disk trie format.
    private static void layOut(TriePartitionUpdate trieUpdate, SerializationHeader header, DataOutputPlus out) throws IOException
    {
        OnDiskDeletionAwareTrieWriter.write(trieUpdate.trie(), new ContentSerDe(header), new MarkerSerDe(header), out);
    }

    /**
     * Deserializes a {@link TriePartitionUpdate} from an input stream using messaging version and table metadata.
     * Reconstructs the decorated key and header, and opens the trie bytes as an
     * {@link OnDiskDeletionAwareTrie} that is walked in place rather than rebuilt.
     */
    public static TriePartitionUpdate deserialize(DataInputPlus in, int version, DeserializationHelper.Flag flag, TableMetadata metadata) throws IOException
    {
        checkVersion(version);
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
        int flags = in.readUnsignedByte();
        int rowCountIncludingStatic = in.readUnsignedVInt32();
        int tombstoneCount = in.readUnsignedVInt32();
        int dataSize = in.readUnsignedVInt32();

        // dataSize is an estimate that nothing on the read path depends on, so it is not validated here.
        if (rowCountIncludingStatic < 0 || tombstoneCount < 0)
            throw new IOException(String.format("Corrupt TriePartitionUpdate header counts: rows=%d, tombstones=%d",
                                                rowCountIncludingStatic, tombstoneCount));
        if ((flags & ~UPDATE_IS_EMPTY) != 0)
            throw new IOException("Unknown partition update flags: " + flags);
        if ((flags & UPDATE_IS_EMPTY) != 0 && (rowCountIncludingStatic != 0 || tombstoneCount != 0))
            throw new IOException(String.format("Partition update flagged as empty counts rows=%d, tombstones=%d",
                                                rowCountIncludingStatic, tombstoneCount));

        SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, null, true);
        Columns statics = header.columns().statics;
        Columns regulars = header.columns().regulars;
        int[] staticIndexes = deserializeColumnIndexes(in, statics);
        int[] regularIndexes = deserializeColumnIndexes(in, regulars);
        DeserializationHelper desHelper = new DeserializationHelper(metadata, version, flag);
        // Only an update holding some timestamp not newer than the latest drop of its columns can hold data of a
        // dropped column to leave out. The minimum timestamp of its stats bounds those of its cells, liveness and
        // deletions (the memtable's minimum timestamp is merged from it, and compaction uses that to decide which
        // tombstones it may purge), except TIMESTAMP_EPOCH, which NO_STATS and stats that saw no timestamp carry.
        long latestDropTime = latestDropTime(header.columns(), metadata);
        long minTimestamp = header.stats().minTimestamp;
        boolean mayHoldDroppedData = latestDropTime != Long.MIN_VALUE
                                     && (minTimestamp == EncodingStats.TIMESTAMP_EPOCH || minTimestamp <= latestDropTime);

        // The trie is walked in place over these bytes rather than rebuilt, so the update holds a
        // view over the buffer. TrieBackedPartition stores a DeletionAwareTrie, not specifically an
        // in-memory one, which is what makes reading without materialising possible.
        ByteBuffer trieBytes = ByteBufferUtil.readWithVIntLength(in);
        DeletionAwareTrie<Object, TrieTombstoneMarker> trie =
            OnDiskDeletionAwareTrie.open(trieBytes,
                                         new ContentSerDe(header, desHelper, metadata.isCounter()),
                                         new MarkerSerDe(header),
                                         TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                         -1);

        // If the sender's schema numbers the columns differently from this node's, the trie's keys point at the wrong
        // columns here, and the update is rebuilt with this node's numbering; the result is in memory. An update that
        // may hold data of a column dropped here not newer than the drop is rebuilt as well, to leave that data out:
        // the rebuild filters each row by the table's dropped columns, which leaves out the column's cells and complex
        // deletions, and the rows left empty, and counts the rows and tombstones that remain. A column that is dropped
        // here and not added again has no index here, so the update is rebuilt whatever its timestamps. A schema change
        // can still come between reading an update and merging it, so the memtable checks the numbering again
        // (TrieMemtable.put).
        if (mayHoldDroppedData
            || !hasLocalIndexes(metadata, statics, staticIndexes)
            || !hasLocalIndexes(metadata, regulars, regularIndexes))
        {
            TriePartitionUpdate senderKeyed = new TriePartitionUpdate(metadata, key, header.columns(), header.stats(), rowCountIncludingStatic, tombstoneCount, dataSize, trie);
            return TriePartitionUpdate.withColumnIndexes(senderKeyed,
                                                         metadata,
                                                         TrieBackedRow.columnIds(statics, staticIndexes),
                                                         TrieBackedRow.columnIds(regulars, regularIndexes));
        }

        // The counter leader rewrites the counter cells of the update it receives in place (CounterMark.setValue),
        // which bytes read in place cannot take.
        if (metadata.isCounter())
            trie = TriePartitionUpdate.copyToMemory(trie);

        return new TriePartitionUpdate(metadata, key, header.columns(), header.stats(), rowCountIncludingStatic, tombstoneCount, dataSize, trie);
    }

    /// The latest time any of the given columns was dropped here, or `Long.MIN_VALUE` if none of them was.
    private static long latestDropTime(RegularAndStaticColumns columns, TableMetadata metadata)
    {
        long latest = Long.MIN_VALUE;
        if (metadata.droppedColumns.isEmpty())
            return latest;
        for (ColumnMetadata column : columns)
        {
            DroppedColumn dropped = metadata.droppedColumns.get(column.name.bytes);
            if (dropped != null)
                latest = Math.max(latest, dropped.droppedTime);
        }
        return latest;
    }

    /**
     * Computes serialized size of {@link TriePartitionUpdate} wire format.
     */
    public static long serializedSize(TriePartitionUpdate trieUpdate, int version)
    {
        checkVersion(version);
        SerializationHeader header = new SerializationHeader(false, trieUpdate.metadata(), trieUpdate.columns(), trieUpdate.stats());

        return ByteBufferUtil.serializedSizeWithVIntLength(trieUpdate.partitionKey().getKey())
               + TypeSizes.BYTE_SIZE
               + TypeSizes.sizeofUnsignedVInt(trieUpdate.rowCountIncludingStatic)
               + TypeSizes.sizeofUnsignedVInt(trieUpdate.tombstoneCount)
               + TypeSizes.sizeofUnsignedVInt(trieUpdate.dataSize())
               + SerializationHeader.serializer.serializedSizeForMessaging(header, null, true)
               + columnIndexesSerializedSize(trieUpdate.metadata(), header.columns())
               + serializedTrieSize(trieUpdate, header);
    }

    /// Sizing the trie means writing it: [OnDiskTrieWriter] emits nodes as the walk ascends and the
    /// total is only known at the end, so unlike the previous format there is no cheap size to read
    /// off the structure. It is written to an output that only counts the bytes.
    private static long serializedTrieSize(TriePartitionUpdate trieUpdate, SerializationHeader header)
    {
        try
        {
            CountingDataOutputPlus trieBytes = new CountingDataOutputPlus();
            layOut(trieUpdate, header, trieBytes);
            long trieLength = trieBytes.position();
            return TypeSizes.sizeofUnsignedVInt(trieLength) + trieLength;
        }
        catch (IOException e)
        {
            // This is where a checked exception has to stop: serializedSize implements a contract
            // that cannot report one. The destination only counts bytes, so the only way here
            // is a failure to encode a payload, which the write path reports as IOException.
            throw new UncheckedIOException(e);
        }
    }

    /// A payload is read to the end of its slot and no further, so a read that stops anywhere else has been given bytes
    /// the writer did not write.
    private static void checkReadToEnd(DataInputPlus in, long start, int length) throws IOException
    {
        long read = OnDiskCursor.position(in) - start;
        if (read != length)
            throw new IOException("Read " + read + " bytes of a payload of " + length);
    }

    /**
     * Encodes and decodes the payloads of the trie's live branch, each behind a tag: {@link CellData},
     * {@link LivenessInfo}, and the row and partition markers.
     */
    private static final class ContentSerDe implements OnDiskTrieWriter.DataSerializer<Object>, OnDiskCursor.DataDeserializer<Object>
    {
        private final SerializationHeader header;
        /// Only set for reading.
        private final DeserializationHelper desHelper;
        /// Only set for reading.
        private final boolean isCounterTable;

        /// For writing.
        ContentSerDe(SerializationHeader header)
        {
            this.header = header;
            this.desHelper = null;
            this.isCounterTable = false;
        }

        /// For reading.
        ContentSerDe(SerializationHeader header, DeserializationHelper desHelper, boolean isCounterTable)
        {
            this.header = header;
            this.desHelper = desHelper;
            this.isCounterTable = isCounterTable;
        }

        @Override
        public void serialize(DataOutputPlus out, Object content) throws IOException
        {
            if (content instanceof CellData)                     { out.writeByte(TYPE_CELL); serializeCell((CellData<?, ?>) content, out); return; }
            if (content instanceof LivenessInfo)                 { serializeLiveness((LivenessInfo) content, out); return; }
            if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)  { out.writeByte(TYPE_COMPLEX_MARKER); return; }
            if (content == TrieBackedPartition.PARTITION_MARKER) { out.writeByte(TYPE_PARTITION_MARKER); return; }

            throw new IllegalArgumentException("Unknown content type in trie: " + content.getClass().getName());
        }

        @Override
        public Object deserialize(DataInputPlus in, int length) throws IOException
        {
            long start = OnDiskCursor.position(in);
            Object content = deserializeContent(in, start + length);
            checkReadToEnd(in, start, length);
            return content;
        }

        /// `end` is the position the payload ends at.
        private Object deserializeContent(DataInputPlus in, long end) throws IOException
        {
            int type = in.readByte();
            switch (type)
            {
                case TYPE_CELL:                  return deserializeCell(in, end);
                case TYPE_LIVENESS:              return deserializeLiveness(in);
                case TYPE_EMPTY_LIVENESS:        return LivenessInfo.EMPTY;
                case TYPE_COMPLEX_MARKER:        return TrieBackedRow.COMPLEX_COLUMN_MARKER;
                case TYPE_PARTITION_MARKER:      return TrieBackedPartition.PARTITION_MARKER;
                default:                         throw new IOException("Unknown content type tag: " + type);
            }
        }

        private <V> void serializeCell(CellData<V, ?> cell, DataOutputPlus out) throws IOException
        {
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
            int flags = 0;
            if (isDeleted)
                flags |= CELL_IS_DELETED;
            else if (isExpiring)
                flags |= CELL_IS_EXPIRING;
            out.writeByte(flags);

            header.writeTimestamp(cell.timestamp(), out);
            if (isDeleted || isExpiring)
                header.writeLocalDeletionTime(cell.localDeletionTime(), out);
            if (isExpiring)
                header.writeTTL(cell.ttl(), out);
            cell.accessor().write(cell.value(), out);
        }

        /// Reads the cell's data as [Cell.Serializer#deserialize] reads a cell's, from the layout [#CELL_IS_DELETED]
        /// describes.
        private CellData<?, ?> deserializeCell(DataInputPlus in, long end) throws IOException
        {
            int flags = in.readUnsignedByte();
            if ((flags & ~(CELL_IS_DELETED | CELL_IS_EXPIRING)) != 0)
                throw new IOException("Unknown cell flags: " + flags);
            boolean isDeleted = (flags & CELL_IS_DELETED) != 0;
            boolean isExpiring = (flags & CELL_IS_EXPIRING) != 0;
            if (isDeleted && isExpiring)
                throw new IOException("Cell flagged as both deleted and expiring");
            boolean isCounter = isCounterTable && !isDeleted;

            long timestamp = header.readTimestamp(in);
            long localDeletionTime = isDeleted || isExpiring ? header.readLocalDeletionTime(in) : CellData.NO_DELETION_TIME;
            int ttl = isExpiring ? header.readTTL(in) : CellData.NO_TTL;
            if (ttl < 0)
                throw new IOException("Invalid TTL: " + ttl);

            long valueLength = end - OnDiskCursor.position(in);
            if (valueLength < 0)
                throw new IOException("Cell read " + -valueLength + " bytes past the end of its payload");
            ByteBuffer value = ByteBufferUtil.read(in, (int) valueLength);
            // An empty value has no shards to clear, and Cell.Serializer does not clear one either.
            if (isCounter && valueLength > 0)
                value = desHelper.maybeClearCounterValue(value, ByteBufferAccessor.instance);

            localDeletionTime = Cell.decodeLocalDeletionTime(localDeletionTime, ttl, desHelper);
            return new BufferCellData(value, timestamp, localDeletionTime, ttl, isCounter);
        }

        /// Writes the tag as well, as there is one for [LivenessInfo#EMPTY] and one for any other liveness. Any empty
        /// liveness is written as [LivenessInfo#EMPTY], so that only that instance can come back.
        private void serializeLiveness(LivenessInfo info, DataOutputPlus out) throws IOException
        {
            if (info.isEmpty())
            {
                out.writeByte(TYPE_EMPTY_LIVENESS);
                return;
            }

            out.writeByte(TYPE_LIVENESS);
            boolean isExpiring = info.isExpiring();
            out.writeBoolean(isExpiring);
            header.writeTimestamp(info.timestamp(), out);
            if (isExpiring)
            {
                header.writeTTL(info.ttl(), out);
                header.writeLocalDeletionTime(info.localExpirationTime(), out);
            }
        }

        private LivenessInfo deserializeLiveness(DataInputPlus in) throws IOException
        {
            boolean isExpiring = in.readBoolean();
            long ts = header.readTimestamp(in);
            if (ts == LivenessInfo.NO_TIMESTAMP)
                throw new IOException("Empty liveness under the tag of a non-empty one");
            if (isExpiring)
            {
                int ttl = header.readTTL(in);
                long localExp = header.readLocalDeletionTime(in);
                return LivenessInfo.withExpirationTime(ts, ttl, localExp);
            }
            return LivenessInfo.create(ts, 0);
        }
    }

    /**
     * Encodes and decodes the deletion markers of the trie's deletion branches, each behind
     * {@link #TYPE_TRIE_TOMBSTONE_MARKER}, the only tag a deletion branch can hold.
     */
    private static final class MarkerSerDe implements OnDiskTrieWriter.DataSerializer<TrieTombstoneMarker>, OnDiskCursor.DataDeserializer<TrieTombstoneMarker>
    {
        static
        {
            // MarkerSerDe records the level marker as one boolean, so it can only encode ROW. A second constant needs a format change.
            assert TrieTombstoneMarker.LevelMarker.values().length == 1 : "MarkerSerDe encodes only the ROW level marker";
        }

        private final SerializationHeader header;

        MarkerSerDe(SerializationHeader header)
        {
            this.header = header;
        }

        @Override
        public void serialize(DataOutputPlus out, TrieTombstoneMarker marker) throws IOException
        {
            out.writeByte(TYPE_TRIE_TOMBSTONE_MARKER);
            serializeTrieTombstoneMarker(marker, out);
        }

        @Override
        public TrieTombstoneMarker deserialize(DataInputPlus in, int length) throws IOException
        {
            long start = OnDiskCursor.position(in);
            int type = in.readByte();
            if (type != TYPE_TRIE_TOMBSTONE_MARKER)
                throw new IOException("Unexpected content type tag " + type + " in a deletion branch");
            TrieTombstoneMarker marker = deserializeTrieTombstoneMarker(in);
            checkReadToEnd(in, start, length);
            return marker;
        }

        /**
         * Serializes a {@link TrieTombstoneMarker.Covering} instance (boolean presence tag, {@link DeletionTime}, and {@link TrieTombstoneMarker.Kind} ordinal).
         */
        private void serializeCovering(TrieTombstoneMarker.Covering covering, DataOutputPlus out) throws IOException
        {
            if (covering == null)
            {
                out.writeBoolean(false);
            }
            else
            {
                out.writeBoolean(true);
                header.writeDeletionTime(covering, out);
                out.writeByte(covering.deletionKind().ordinal());
            }
        }

        private static final TrieTombstoneMarker.Kind[] KINDS = TrieTombstoneMarker.Kind.values();

        /**
         * Deserializes a {@link TrieTombstoneMarker.Covering} instance from stream.
         */
        private TrieTombstoneMarker.Covering deserializeCovering(DataInputPlus in) throws IOException
        {
            if (!in.readBoolean())
                return null;
            DeletionTime dt = header.readDeletionTime(in);
            if (dt.isLive())
                throw new IOException("Live deletion time in a deletion marker");
            int ordinal = in.readByte() & 0xFF;
            if (ordinal >= KINDS.length)
                throw new IOException("Invalid TrieTombstoneMarker.Kind ordinal: " + ordinal);
            return TrieTombstoneMarker.covering(dt, KINDS[ordinal]);
        }

        /**
         * Serializes a {@link TrieTombstoneMarker} across left, right, and point deletion bounds.
         */
        private void serializeTrieTombstoneMarker(TrieTombstoneMarker marker, DataOutputPlus out) throws IOException
        {
            serializeCovering(marker.leftDeletion(), out);
            serializeCovering(marker.rightDeletion(), out);
            serializeCovering(marker.pointDeletion(), out);
            boolean hasRowLevelMarker = marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW);
            // A point deletion is read back without a level marker; one that had it would lose it unnoticed.
            assert marker.pointDeletion() == null || !hasRowLevelMarker : "Point deletion with a row level marker: " + marker;
            out.writeBoolean(hasRowLevelMarker);
        }

        /**
         * Deserializes a {@link TrieTombstoneMarker} from stream.
         */
        private TrieTombstoneMarker deserializeTrieTombstoneMarker(DataInputPlus in) throws IOException
        {
            TrieTombstoneMarker.Covering left = deserializeCovering(in);
            TrieTombstoneMarker.Covering right = deserializeCovering(in);
            TrieTombstoneMarker.Covering point = deserializeCovering(in);
            boolean hasRowLevelMarker = in.readBoolean();
            TrieTombstoneMarker.LevelMarker levelMarker = hasRowLevelMarker ? TrieTombstoneMarker.LevelMarker.ROW : null;

            if (point != null)
            {
                // A point deletion with the same deletion on both sides is built with one instance for both.
                if (left != null && left.equals(right))
                    return new TrieTombstoneMarker.Point(point, left);
                return new TrieTombstoneMarker.Point(point, left, right);
            }

            // Every marker the writer writes changes something; make would return a bare covering deletion, or nothing,
            // for one that does not.
            if (levelMarker == null && Objects.equals(left, right))
                throw new IOException("Deletion marker with the same deletion on both sides and nothing else: " + left);

            return TrieTombstoneMarker.make(left, right, levelMarker);
        }
    }
}
