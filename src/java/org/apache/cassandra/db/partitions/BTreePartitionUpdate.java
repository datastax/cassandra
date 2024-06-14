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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

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
public class BTreePartitionUpdate extends AbstractBTreePartition implements PartitionUpdate
{
    protected static final Logger logger = LoggerFactory.getLogger(BTreePartitionUpdate.class);

    public static final BTreeFactory FACTORY = new BTreeFactory();

    private final BTreePartitionData holder;
    private final DeletionInfo deletionInfo;
    private final TableMetadata metadata;

    private final boolean canHaveShadowedData;

    private BTreePartitionUpdate(TableMetadata metadata,
                                 DecoratedKey key,
                                 BTreePartitionData holder,
                                 MutableDeletionInfo deletionInfo,
                                 boolean canHaveShadowedData)
    {
        super(key);
        this.metadata = metadata;
        this.holder = holder;
        this.deletionInfo = deletionInfo;
        this.canHaveShadowedData = canHaveShadowedData;
    }

    /**
     * Creates a empty immutable partition update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the created update.
     *
     * @return the newly created empty (and immutable) update.
     */
    public static BTreePartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey key)
    {
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        BTreePartitionData holder = new BTreePartitionData(RegularAndStaticColumns.NONE, BTree.empty(), deletionInfo, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
        return new BTreePartitionUpdate(metadata, key, holder, deletionInfo, false);
    }

    /**
     * Creates an immutable partition update that entirely deletes a given partition.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition that the created update should delete.
     * @param timestamp the timestamp for the deletion.
     * @param nowInSec the current time in seconds to use as local deletion time for the partition deletion.
     *
     * @return the newly created partition deletion update.
     */
    public static BTreePartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(timestamp, nowInSec);
        BTreePartitionData holder = new BTreePartitionData(RegularAndStaticColumns.NONE, BTree.empty(), deletionInfo, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
        return new BTreePartitionUpdate(metadata, key, holder, deletionInfo, false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update (may be null).
     * @param row the static row for the update (may be null).
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static BTreePartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row, Row staticRow)
    {
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        BTreePartitionData holder = new BTreePartitionData(
            new RegularAndStaticColumns(
                staticRow == null ? Columns.NONE : Columns.from(staticRow),
                row == null ? Columns.NONE : Columns.from(row)
            ),
            row == null ? BTree.empty() : BTree.singleton(row),
            deletionInfo,
            staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow,
            EncodingStats.NO_STATS
        );
        return new BTreePartitionUpdate(metadata, key, holder, deletionInfo, false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update (may be static).
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static BTreePartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row)
    {
        return singleRowUpdate(metadata, key, row.isStatic() ? null : row, row.isStatic() ? row : null);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update.
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static BTreePartitionUpdate singleRowUpdate(TableMetadata metadata, ByteBuffer key, Row row)
    {
        return singleRowUpdate(metadata, metadata.partitioner.decorateKey(key), row);
    }

    /**
     * Turns the given iterator into an update.
     *
     * @param iterator the iterator to turn into updates.
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    @SuppressWarnings("resource")
    public static BTreePartitionUpdate fromIterator(UnfilteredRowIterator iterator)
    {
        BTreePartitionData holder = build(iterator, 16);
        MutableDeletionInfo deletionInfo = (MutableDeletionInfo) holder.deletionInfo;
        return new BTreePartitionUpdate(iterator.metadata(), iterator.partitionKey(), holder, deletionInfo, false);
    }

    /**
     * Turns the given iterator into an update.
     *
     * @param iterator the iterator to turn into updates.
     * @param filter the column filter used when querying {@code iterator}. This is used to make
     * sure we don't include data for which the value has been skipped while reading (as we would
     * then be writing something incorrect).
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    @SuppressWarnings("resource")
    public static BTreePartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
    {
        return fromIterator(UnfilteredRowIterators.withOnlyQueriedData(iterator, filter));
    }

    protected boolean canHaveShadowedData()
    {
        return canHaveShadowedData;
    }

    /**
     * Creates a partition update that entirely deletes a given partition.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition that the created update should delete.
     * @param timestamp the timestamp for the deletion.
     * @param nowInSec the current time in seconds to use as local deletion time for the partition deletion.
     *
     * @return the newly created partition deletion update.
     */
    public static BTreePartitionUpdate fullPartitionDelete(TableMetadata metadata, ByteBuffer key, long timestamp, int nowInSec)
    {
        return fullPartitionDelete(metadata, metadata.partitioner.decorateKey(key), timestamp, nowInSec);
    }

    public static BTreePartitionUpdate asBTreeUpdate(PartitionUpdate update)
    {
        if (update instanceof BTreePartitionUpdate)
            return (BTreePartitionUpdate) update;

        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            return fromIterator(iterator);
        }
    }

    /**
     * Merges the provided updates, yielding a new update that incorporates all those updates.
     *
     * @param updates the collection of updates to merge. This shouldn't be empty.
     *
     * @return a partition update that include (merge) all the updates from {@code updates}.
     */
    public PartitionUpdate merge(List<? extends PartitionUpdate> updates)
    {
        assert !updates.isEmpty();
        final int size = updates.size();

        if (size == 1)
            return Iterables.getOnlyElement(updates);

        List<UnfilteredRowIterator> asIterators = Lists.transform(updates, Partition::unfilteredIterator);
        try (UnfilteredRowIterator merge = UnfilteredRowIterators.merge(asIterators))
        {
            return fromIterator(merge, ColumnFilter.all(updates.get(0).metadata()));
        }
    }

    // We override this, because the version in the super-class calls holder(), which build the update preventing
    // further updates, but that's not necessary here and being able to check at least the partition deletion without
    // "locking" the update is nice (and used in DataResolver.RepairMergeListener.MergeListener).
    @Override
    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
    }

    /**
     * The number of "operations" contained in the update.
     * <p>
     * This is used by {@code Memtable} to approximate how much work this update does. In practice, this
     * count how many rows are updated and how many ranges are deleted by the partition update.
     *
     * @return the number of "operations" performed by the update.
     */
    public int operationCount()
    {
        return rowCount()
             + (staticRow().isEmpty() ? 0 : 1)
             + deletionInfo.rangeCount()
             + (deletionInfo.getPartitionDeletion().isLive() ? 0 : 1);
    }

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    public int dataSize()
    {
        return Ints.saturatedCast(BTree.<Row>accumulate(holder.tree, (row, value) -> row.dataSize() + value, 0L)
                + holder.staticRow.dataSize() + holder.deletionInfo.dataSize());
    }

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    public long unsharedHeapSizeExcludingData()
    {
        return BTree.<Row>accumulate(holder.tree, (row, value) -> row.unsharedHeapSizeExcludingData() + value, 0L)
                + holder.staticRow.unsharedHeapSizeExcludingData() + holder.deletionInfo.unsharedHeapSize();
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        // The superclass implementation calls holder(), but that triggers a build of the PartitionUpdate. But since
        // the columns are passed to the ctor, we know the holder always has the proper columns even if it doesn't have
        // the built rows yet, so just bypass the holder() method.
        return holder.columns;
    }

    protected BTreePartitionData holder()
    {
        return holder;
    }

    public EncodingStats stats()
    {
        return holder().stats;
    }

    /**
     * Validates the data contained in this update.
     *
     * @throws org.apache.cassandra.serializers.MarshalException if some of the data contained in this update is corrupted.
     */
    public void validate()
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
    public long maxTimestamp()
    {
        long maxTimestamp = deletionInfo.maxTimestamp();
        for (Row row : rows())
        {
            maxTimestamp = Math.max(maxTimestamp, row.primaryKeyLivenessInfo().timestamp());
            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    maxTimestamp = Math.max(maxTimestamp, ((Cell<?>)cd).timestamp());
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    maxTimestamp = Math.max(maxTimestamp, complexData.complexDeletion().markedForDeleteAt());
                    for (Cell<?> cell : complexData)
                        maxTimestamp = Math.max(maxTimestamp, cell.timestamp());
                }
            }
        }

        if (this.holder.staticRow != null)
        {
            for (ColumnData cd : this.holder.staticRow.columnData())
            {
                if (cd.column().isSimple())
                {
                    maxTimestamp = Math.max(maxTimestamp, ((Cell<?>) cd).timestamp());
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData) cd;
                    maxTimestamp = Math.max(maxTimestamp, complexData.complexDeletion().markedForDeleteAt());
                    for (Cell<?> cell : complexData)
                        maxTimestamp = Math.max(maxTimestamp, cell.timestamp());
                }
            }
        }
        return maxTimestamp;
    }

    /**
     * For an update on a counter table, returns a list containing a {@code CounterMark} for
     * every counter contained in the update.
     *
     * @return a list with counter marks for every counter in this update.
     */
    public List<CounterMark> collectCounterMarks()
    {
        assert metadata().isCounter();
        // We will take aliases on the rows of this update, and update them in-place. So we should be sure the
        // update is now immutable for all intent and purposes.
        List<CounterMark> marks = new ArrayList<>();
        addMarksForRow(staticRow(), marks);
        for (Row row : rows())
            addMarksForRow(row, marks);
        return marks;
    }

    private static void addMarksForRow(Row row, List<CounterMark> marks)
    {
        for (Cell<?> cell : row.cells())
        {
            if (cell.isCounterCell())
                marks.add(new CounterMark(row, cell.column(), cell.path()));
        }
    }

    public void validateIndexedColumns()
    {
        IndexRegistry.obtain(metadata()).validate(this);
    }

    @VisibleForTesting
    public static BTreePartitionUpdate unsafeConstruct(TableMetadata metadata,
                                                       DecoratedKey key,
                                                       BTreePartitionData holder,
                                                       MutableDeletionInfo deletionInfo,
                                                       boolean canHaveShadowedData)
    {
        return new BTreePartitionUpdate(metadata, key, holder, deletionInfo, canHaveShadowedData);
    }

    public BTreePartitionUpdate withUpdatedTimestamps(long timestamp)
    {
        return new Builder(this, rowCount()).updateAllTimestamp(timestamp).build();
    }


    /**
     * Builder for PartitionUpdates
     *
     * This class is not thread safe, but the PartitionUpdate it produces is (since it is immutable).
     */
    public static class Builder implements PartitionUpdate.Builder
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final MutableDeletionInfo deletionInfo;
        private final boolean canHaveShadowedData;
        private Object[] tree = BTree.empty();
        private final BTree.Builder<Row> rowBuilder;
        private Row staticRow = Rows.EMPTY_STATIC_ROW;
        private final RegularAndStaticColumns columns;
        private boolean isBuilt = false;

        public Builder(TableMetadata metadata,
                       DecoratedKey key,
                       RegularAndStaticColumns columns,
                       int initialRowCapacity,
                       boolean canHaveShadowedData)
        {
            this(metadata, key, columns, initialRowCapacity, canHaveShadowedData, Rows.EMPTY_STATIC_ROW, MutableDeletionInfo.live(), BTree.empty());
        }

        private Builder(TableMetadata metadata,
                       DecoratedKey key,
                       RegularAndStaticColumns columns,
                       int initialRowCapacity,
                       boolean canHaveShadowedData,
                       BTreePartitionData holder)
        {
            this(metadata, key, columns, initialRowCapacity, canHaveShadowedData, holder.staticRow, holder.deletionInfo, holder.tree);
        }

        private Builder(TableMetadata metadata,
                        DecoratedKey key,
                        RegularAndStaticColumns columns,
                        int initialRowCapacity,
                        boolean canHaveShadowedData,
                        Row staticRow,
                        DeletionInfo deletionInfo,
                        Object[] tree)
        {
            this.metadata = metadata;
            this.key = key;
            this.columns = columns;
            this.rowBuilder = rowBuilder(initialRowCapacity);
            this.canHaveShadowedData = canHaveShadowedData;
            this.deletionInfo = deletionInfo.mutableCopy();
            this.staticRow = staticRow;
            this.tree = tree;
        }

        public Builder(TableMetadata metadata, DecoratedKey key, RegularAndStaticColumns columnDefinitions, int size)
        {
            this(metadata, key, columnDefinitions, size, true);
        }

        public Builder(BTreePartitionUpdate base, int initialRowCapacity)
        {
            this(base.metadata, base.partitionKey, base.columns(), initialRowCapacity, base.canHaveShadowedData, base.holder);
        }

        public Builder(TableMetadata metadata,
                        ByteBuffer key,
                        RegularAndStaticColumns columns,
                        int initialRowCapacity)
        {
            this(metadata, metadata.partitioner.decorateKey(key), columns, initialRowCapacity, true);
        }

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
        public void add(Row row)
        {
            if (row.isEmpty())
                return;

            if (row.isStatic())
            {
                // this assert is expensive, and possibly of limited value; we should consider removing it
                // or introducing a new class of assertions for test purposes
                assert columns().statics.containsAll(row.columns()) : columns().statics + " is not superset of " + row.columns();
                staticRow = staticRow.isEmpty()
                            ? row
                            : Rows.merge(staticRow, row);
            }
            else
            {
                // this assert is expensive, and possibly of limited value; we should consider removing it
                // or introducing a new class of assertions for test purposes
                assert columns().regulars.containsAll(row.columns()) : columns().regulars + " is not superset of " + row.columns();
                rowBuilder.add(row);
            }
        }

        public void addPartitionDeletion(DeletionTime deletionTime)
        {
            deletionInfo.add(deletionTime);
        }

        public void add(RangeTombstone range)
        {
            deletionInfo.add(range, metadata.comparator);
        }

        public DecoratedKey partitionKey()
        {
            return key;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public BTreePartitionUpdate build()
        {
            // assert that we are not calling build() several times
            assert !isBuilt : "A PartitionUpdate.Builder should only get built once";
            Object[] add = rowBuilder.build();
            Object[] merged = BTree.<Row, Row, Row>update(tree, add, metadata.comparator,
                                                          UpdateFunction.Simple.of(Rows::merge));

            EncodingStats newStats = EncodingStats.Collector.collect(staticRow, BTree.iterator(merged), deletionInfo);

            isBuilt = true;
            return new BTreePartitionUpdate(metadata,
                                            partitionKey(),
                                            new BTreePartitionData(columns,
                                                              merged,
                                                              deletionInfo,
                                                              staticRow,
                                                              newStats),
                                            deletionInfo,
                                            canHaveShadowedData);
        }

        public RegularAndStaticColumns columns()
        {
            return columns;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return deletionInfo.getPartitionDeletion();
        }

        private BTree.Builder<Row> rowBuilder(int initialCapacity)
        {
            return BTree.<Row>builder(metadata.comparator, initialCapacity)
                   .setQuickResolver(Rows::merge);
        }
        /**
         * Modify this update to set every timestamp for live data to {@code newTimestamp} and
         * every deletion timestamp to {@code newTimestamp - 1}.
         *
         * There is no reason to use that expect on the Paxos code path, where we need ensure that
         * anything inserted use the ballot timestamp (to respect the order of update decided by
         * the Paxos algorithm). We use {@code newTimestamp - 1} for deletions because tombstones
         * always win on timestamp equality and we don't want to delete our own insertions
         * (typically, when we overwrite a collection, we first set a complex deletion to delete the
         * previous collection before adding new elements. If we were to set that complex deletion
         * to the same timestamp that the new elements, it would delete those elements). And since
         * tombstones always wins on timestamp equality, using -1 guarantees our deletion will still
         * delete anything from a previous update.
         */
        public Builder updateAllTimestamp(long newTimestamp)
        {
            deletionInfo.updateAllTimestamp(newTimestamp - 1);
            tree = BTree.<Row, Row>transformAndFilter(tree, (x) -> x.updateAllTimestamp(newTimestamp));
            staticRow = this.staticRow.updateAllTimestamp(newTimestamp);
            return this;
        }

        @Override
        public String toString()
        {
            return "Builder{" +
                   "metadata=" + metadata +
                   ", key=" + key +
                   ", deletionInfo=" + deletionInfo +
                   ", canHaveShadowedData=" + canHaveShadowedData +
                   ", staticRow=" + staticRow +
                   ", columns=" + columns +
                   ", isBuilt=" + isBuilt +
                   '}';
        }

    }

    public static class BTreeFactory implements PartitionUpdate.Factory
    {

        @Override
        public PartitionUpdate.Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
        {
            return new Builder(metadata, partitionKey, columns, initialRowCapacity);
        }

        @Override
        public PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey)
        {
            return BTreePartitionUpdate.emptyUpdate(metadata, partitionKey);
        }

        @Override
        public PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey valueKey, Row row)
        {
            return BTreePartitionUpdate.singleRowUpdate(metadata, valueKey, row);
        }

        @Override
        public PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec)
        {
            return BTreePartitionUpdate.fullPartitionDelete(metadata, key, timestamp, nowInSec);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator)
        {
            return BTreePartitionUpdate.fromIterator(iterator);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
        {
            return BTreePartitionUpdate.fromIterator(iterator, filter);
        }
    }
}
