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
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;

import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Stores updates made on a partition.
 * <p>
 * A PartitionUpdate object requires that all writes/additions are performed before we
 * try to read the updates (attempts to write to the PartitionUpdate after a read method
 * has been called will result in an exception being thrown). In other words, a Partition
 * is mutable while it's written but becomes immutable as soon as it is read.
 * <p>
 * A typical usage is to create a new update ({@code new PartitionUpdate(metadata, key, columns, capacity)})
 * and then add rows and range tombstones through the {@code add()} methods (the partition
 * level deletion time can also be set with {@code addPartitionDeletion()}). However, there
 * is also a few static helper constructor methods for special cases ({@code emptyUpdate()},
 * {@code fullPartitionDelete} and {@code singleRowUpdate}).
 */
public interface PartitionUpdate extends Partition
{
    @SuppressWarnings("Convert2MethodRef")
    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer(tableId -> Schema.instance.getExistingTableMetadata(tableId));

    DeletionInfo deletionInfo();

    /**
     * The number of "operations" contained in the update.
     * <p>
     * This is used by {@code Memtable} to approximate how much work this update does. In practice, this
     * count how many rows are updated and how many ranges are deleted by the partition update.
     *
     * @return the number of "operations" performed by the update.
     */
    int operationCount();

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    int dataSize();

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    long unsharedHeapSizeExcludingData();

    @Override
    RegularAndStaticColumns columns();

    @Override
    EncodingStats stats();

    Row staticRow();

    int rowCount();

    /**
     * Validates the data contained in this update.
     *
     * @throws org.apache.cassandra.serializers.MarshalException if some of the data contained in this update is corrupted.
     */
    default void validate()
    {
        for (Row row : rows())
        {
            metadata().comparator.validate(row.clustering());
            for (ColumnData cd : row)
                cd.validate();
        }
    }

    /**
     * The maximum timestamp used in this update.
     *
     * @return the maximum timestamp used in this update.
     */
    long maxTimestamp();

    /**
     * For an update on a counter table, returns a list containing a {@code CounterMark} for
     * every counter contained in the update.
     *
     * @return a list with counter marks for every counter in this update.
     */
    List<CounterMark> collectCounterMarks();

    default void validateIndexedColumns()
    {
        IndexRegistry.obtain(metadata()).validate(this);
    }

    PartitionUpdate withUpdatedTimestamps(long timestamp);

    /**
     * Merge the provided updates into a single update. `this` is ignored, it is only used as an indicator of the
     * desired type of partition update to return. The method must also work (possibly inefficiently) when the given
     * updates do not match that type.
     */
    PartitionUpdate merge(List<? extends PartitionUpdate> updates);

    static Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
    {
        return metadata.partitionUpdateFactory().builder(metadata, partitionKey, columns, initialRowCapacity);
    }

    static Builder builder(TableMetadata metadata, ByteBuffer partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
    {
        return builder(metadata, metadata.partitioner.decorateKey(partitionKey), columns, initialRowCapacity);
    }

    static PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey)
    {
        return metadata.partitionUpdateFactory().emptyUpdate(metadata, partitionKey);
    }

    static PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey valueKey, Row row)
    {
        return metadata.partitionUpdateFactory().singleRowUpdate(metadata, valueKey, row);
    }

    static PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec)
    {
        return metadata.partitionUpdateFactory().fullPartitionDelete(metadata, key, timestamp, nowInSec);
    }

    static PartitionUpdate fullPartitionDelete(TableMetadata metadata, ByteBuffer key, long timestamp, int nowInSec)
    {
        return fullPartitionDelete(metadata, metadata.partitioner.decorateKey(key), timestamp, nowInSec);
    }

    static PartitionUpdate fromIterator(UnfilteredRowIterator partition, ColumnFilter filter)
    {
        return partition.metadata().partitionUpdateFactory().fromIterator(partition, filter);
    }

    /**
     * Creates a new simple partition update builder.
     *
     * @param metadata the metadata for the table this is a partition of.
     * @param partitionKeyValues the values for partition key columns identifying this partition. The values for each
     * partition key column can be passed either directly as {@code ByteBuffer} or using a "native" value (int for
     * Int32Type, string for UTF8Type, ...). It is also allowed to pass a single {@code DecoratedKey} value directly.
     * @return a newly created builder.
     */
    public static SimpleBuilder simpleBuilder(TableMetadata metadata, Object... partitionKeyValues)
    {
        return new SimpleBuilders.PartitionUpdateBuilder(metadata, partitionKeyValues);
    }

    /**
     * Deserialize a partition update from a provided byte buffer.
     *
     * @param bytes the byte buffer that contains the serialized update.
     * @param version the version with which the update is serialized.
     *
     * @return the deserialized update or {@code null} if {@code bytes == null}.
     */
    @SuppressWarnings("resource")
    static PartitionUpdate fromBytes(ByteBuffer bytes, int version)
    {
        if (bytes == null)
            return null;

        try
        {
            return serializer.deserialize(new DataInputBuffer(bytes, true),
                                          version,
                                          DeserializationHelper.Flag.LOCAL);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serialize a partition update as a byte buffer.
     *
     * @param update the partition update to serialize.
     * @param version the version to serialize the update into.
     *
     * @return a newly allocated byte buffer containing the serialized update.
     */
    static ByteBuffer toBytes(PartitionUpdate update, int version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(update, out, version);
            return out.buffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Interface for building partition updates geared towards human.
     * <p>
     * This should generally not be used when performance matters too much, but provides a more convenient interface to
     * build an update than using the class constructor when performance is not of the utmost importance.
     */
    public interface SimpleBuilder
    {
        /**
         * The metadata of the table this is a builder on.
         */
        public TableMetadata metadata();

        /**
         * Sets the timestamp to use for the following additions to this builder or any derived (row) builder.
         *
         * @param timestamp the timestamp to use for following additions. If that timestamp hasn't been set, the current
         * time in microseconds will be used.
         * @return this builder.
         */
        public SimpleBuilder timestamp(long timestamp);

        /**
         * Sets the ttl to use for the following additions to this builder or any derived (row) builder.
         *
         * @param ttl the ttl to use for following additions. If that ttl hasn't been set, no ttl will be used.
         * @return this builder.
         */
        public SimpleBuilder ttl(int ttl);

        /**
         * Sets the current time to use for the following additions to this builder or any derived (row) builder.
         *
         * @param nowInSec the current time to use for following additions. If the current time hasn't been set, the current
         * time in seconds will be used.
         * @return this builder.
         */
        public SimpleBuilder nowInSec(int nowInSec);

        /**
         * Adds the row identifier by the provided clustering and return a builder for that row.
         *
         * @param clusteringValues the value for the clustering columns of the row to add to this build. There may be no
         * values if either the table has no clustering column, or if you want to edit the static row. Note that as a
         * shortcut it is also allowed to pass a {@code Clustering} object directly, in which case that should be the
         * only argument.
         * @return a builder for the row identified by {@code clusteringValues}.
         */
        public Row.SimpleBuilder row(Object... clusteringValues);

        /**
         * Deletes the partition identified by this builder (using a partition level deletion).
         *
         * @return this builder.
         */
        public SimpleBuilder delete();

        /**
         * Adds a new range tombstone to this update, returning a builder for that range.
         *
         * @return the range tombstone builder for the newly added range.
         */
        public RangeTombstoneBuilder addRangeTombstone();

        /**
         * Adds a new range tombstone to this update
         *
         * @return this builder
         */
        public SimpleBuilder addRangeTombstone(RangeTombstone rt);

        /**
         * Build the update represented by this builder.
         *
         * @return the built update.
         */
        public PartitionUpdate build();

        /**
         * As shortcut for {@code new Mutation(build())}.
         *
         * @return the built update, wrapped in a {@code Mutation}.
         */
        public Mutation buildAsMutation();

        /**
         * Interface to build range tombstone.
         *
         * By default, if no other methods are called, the represented range is inclusive of both start and end and
         * includes everything (its start is {@code BOTTOM} and it's end is {@code TOP}).
         */
        public interface RangeTombstoneBuilder
        {
            /**
             * Sets the start for the built range using the provided values.
             *
             * @param values the value for the start of the range. They act like the {@code clusteringValues} argument
             * of the {@link SimpleBuilder#row(Object...)} method, except that it doesn't have to be a full
             * clustering, it can only be a prefix.
             * @return this builder.
             */
            public RangeTombstoneBuilder start(Object... values);

            /**
             * Sets the end for the built range using the provided values.
             *
             * @param values the value for the end of the range. They act like the {@code clusteringValues} argument
             * of the {@link SimpleBuilder#row(Object...)} method, except that it doesn't have to be a full
             * clustering, it can only be a prefix.
             * @return this builder.
             */
            public RangeTombstoneBuilder end(Object... values);

            /**
             * Sets the start of this range as inclusive.
             * <p>
             * This is the default and don't need to be called, but can for explicitness.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder inclStart();

            /**
             * Sets the start of this range as exclusive.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder exclStart();

            /**
             * Sets the end of this range as inclusive.
             * <p>
             * This is the default and don't need to be called, but can for explicitness.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder inclEnd();

            /**
             * Sets the end of this range as exclusive.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder exclEnd();
        }
    }

    class PartitionUpdateSerializer
    {
        private final ThrowingFunction<? super TableId, ? extends TableMetadata, ? extends UnknownTableException> tableMetadataResolver;

        public PartitionUpdateSerializer(ThrowingFunction<? super TableId, ? extends TableMetadata, ? extends UnknownTableException> tableMetadataResolver)
        {
            this.tableMetadataResolver = tableMetadataResolver;
        }

        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(version != MessagingService.VERSION_DSE_68,
                                        "Can't serialize to version " + version);
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                assert !iter.isReverseOrder();

                update.metadata().id.serialize(out);
                UnfilteredRowIteratorSerializer.serializer.serialize(iter, null, out, version, update.rowCount());
            }
        }

        public PartitionUpdate deserialize(DataInputPlus in, int version, DeserializationHelper.Flag flag) throws IOException
        {
            TableMetadata metadata = tableMetadataResolver.apply(TableId.deserialize(in));
            if (version == MessagingService.VERSION_DSE_68)
            {
                // ignore maxTimestamp
                in.readLong();
            }
            Factory factory = metadata.partitionUpdateFactory();
            UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, null, in, version, flag);
            if (header.isEmpty)
                return factory.emptyUpdate(metadata, header.key);

            assert !header.isReversed;
            assert header.rowEstimate >= 0;
            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, flag, header))
            {
                return factory.fromIterator(partition);
            }
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                return update.metadata().id.serializedSize()
                       + (version == MessagingService.VERSION_DSE_68 ? TypeSizes.LONG_SIZE : 0)
                       + UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, null, version, update.rowCount());
            }
        }
    }

    /**
     * A counter mark is basically a pointer to a counter update inside this partition update. That pointer allows
     * us to update the counter value based on the pre-existing value read during the read-before-write that counters
     * do. See {@link CounterMutation} to understand how this is used.
     */
    class CounterMark
    {
        private final Row row;
        private final ColumnMetadata column;
        private final CellPath path;

        protected CounterMark(Row row, ColumnMetadata column, CellPath path)
        {
            this.row = row;
            this.column = column;
            this.path = path;
        }

        public Clustering<?> clustering()
        {
            return row.clustering();
        }

        public ColumnMetadata column()
        {
            return column;
        }

        public CellPath path()
        {
            return path;
        }

        public ByteBuffer value()
        {
            return path == null
                 ? row.getCell(column).buffer()
                 : row.getCell(column, path).buffer();
        }

        public void setValue(ByteBuffer value)
        {
            // This is a bit of a giant hack as this is the only place where we mutate a Row object. This makes it more efficient
            // for counters however and this won't be needed post-#6506 so that's probably fine.
            assert row instanceof BTreeRow;
            ((BTreeRow)row).setValue(column, path, value);
        }
    }

    /**
     * Builder for PartitionUpdates
     *
     * This class is not thread safe, but the PartitionUpdate it produces is (since it is immutable).
     */
    interface Builder
    {
        /**
         * Adds a row to this update.
         *
         * There is no particular assumption made on the order of row added to a partition update. It is further
         * allowed to add the same row (more precisely, multiple row objects for the same clustering).
         *
         * Note however that the columns contained in the added row must be a subset of the columns used when
         * creating this update.
         *
         * @param row the row to add.
         */
        void add(Row row);

        void addPartitionDeletion(DeletionTime deletionTime);

        void add(RangeTombstone range);

        DecoratedKey partitionKey();

        TableMetadata metadata();

        PartitionUpdate build();

        RegularAndStaticColumns columns();

        DeletionTime partitionLevelDeletion();
    }
    interface Factory
    {
        Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity);
        PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey);
        PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey valueKey, Row row);
        PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec);
        PartitionUpdate fromIterator(UnfilteredRowIterator iterator);
        PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter);
    }
}
