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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * A trie-backed PartitionUpdate. Immutable.
 * <p>
 * Provides factories for simple variations (e.g. singleRowUpdate) and a mutable builder for constructing one.
 * The builder holds a mutable trie to which content may be added in any order, also taking care of
 * merging any duplicate rows, and keeping track of statistics and column coverage.
 */
public class TriePartitionUpdate extends TrieBackedPartition implements PartitionUpdate
{
    protected static final Logger logger = LoggerFactory.getLogger(TriePartitionUpdate.class);

    public static final Factory FACTORY = new TrieFactory();

    final int dataSize;

    private TriePartitionUpdate(TableMetadata metadata,
                                DecoratedKey key,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                int rowCountIncludingStatic,
                                int tombstoneCount,
                                int dataSize,
                                DeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        super(key, columns, stats, rowCountIncludingStatic, tombstoneCount, trie, metadata);
        this.dataSize = dataSize;
    }

    private static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie()
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        try
        {
            trie.putRecursive(ByteComparable.EMPTY, PARTITION_MARKER, noConflictInData());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /**
     * Creates a empty immutable partition update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the created update.
     *
     * @return the newly created empty (and immutable) update.
     */
    public static TriePartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey key)
    {
        return new TriePartitionUpdate(metadata,
                                       key,
                                       RegularAndStaticColumns.NONE,
                                       EncodingStats.NO_STATS,
                                       0,
                                       0,
                                       0,
                                       newTrie());
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
    public static TriePartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = newTrie();
        putPartitionDeletionInTrie(trie, new DeletionTime(timestamp, nowInSec));
        return new TriePartitionUpdate(metadata,
                                       key,
                                       RegularAndStaticColumns.NONE,
                                       new EncodingStats(timestamp, nowInSec, LivenessInfo.NO_TTL),
                                       0,
                                       1,
                                       0,
                                       trie);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update, may be a regular or static row and cannot be null.
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static TriePartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row)
    {
        EncodingStats stats = EncodingStats.Collector.forRow(row);
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = newTrie();

        RegularAndStaticColumns columns;
        if (row.isStatic())
            columns = new RegularAndStaticColumns(Columns.from(row.columns()), Columns.NONE);
        else
            columns = new RegularAndStaticColumns(Columns.NONE, Columns.from(row.columns()));

        try
        {
            putInTrie(metadata.comparator, trie, row);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        return new TriePartitionUpdate(metadata, key, columns, stats, 1, row.deletion().isLive() ? 0 : 1, row.dataSize(), trie);
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
    public static TriePartitionUpdate singleRowUpdate(TableMetadata metadata, ByteBuffer key, Row row)
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
    public static TriePartitionUpdate fromIterator(UnfilteredRowIterator iterator)
    {
        ContentBuilder builder = build(iterator, true);

        return new TriePartitionUpdate(iterator.metadata(),
                                       iterator.partitionKey(),
                                       iterator.columns(),
                                       iterator.stats(),
                                       builder.rowCountIncludingStatic(),
                                       builder.tombstoneCount(),
                                       builder.dataSize(),
                                       builder.trie());
    }

    public static TriePartitionUpdate asTrieUpdate(PartitionUpdate update)
    {
        if (update instanceof TriePartitionUpdate)
            return (TriePartitionUpdate) update;

        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            return fromIterator(iterator);
        }
    }

    public static DeletionAwareTrie<Object, TrieTombstoneMarker> asMergableTrie(PartitionUpdate update)
    {
        return asTrieUpdate(update).trie.prefixedBy(update.partitionKey());
    }

    /**
     * Modify this update to set every timestamp for live data to {@code newTimestamp} and
     * every deletion timestamp to {@code newTimestamp - 1}.
     *
     * There is no reason to use that except on the Paxos code path, where we need to ensure that
     * anything inserted uses the ballot timestamp (to respect the order of updates decided by
     * the Paxos algorithm). We use {@code newTimestamp - 1} for deletions because tombstones
     * always win on timestamp equality and we don't want to delete our own insertions
     * (typically, when we overwrite a collection, we first set a complex deletion to delete the
     * previous collection before adding new elements. If we were to set that complex deletion
     * to the same timestamp that the new elements, it would delete those elements). And since
     * tombstones always wins on timestamp equality, using -1 guarantees our deletion will still
     * delete anything from a previous update.
     */
    @Override
    public TriePartitionUpdate withUpdatedTimestamps(long newTimestamp)
    {

        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> t = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        try
        {
            t.apply(trie,
                    (shouldBeNull, o) ->
                    {
                        assert shouldBeNull == null;
                        if (!(o instanceof RowData))
                            return o;
                        RowData update = (RowData) o;

                        LivenessInfo newInfo = update.livenessInfo.isEmpty()
                                               ? update.livenessInfo
                                               : update.livenessInfo.withUpdatedTimestamp(newTimestamp);

                        return new RowData(BTree.transformAndFilter(update.columnsBTree,
                                                                    (ColumnData cd) -> cd.updateAllTimestamp(newTimestamp)),
                                           newInfo);
                    },
                    (shouldBeNull, o) ->
                    {
                        assert shouldBeNull == null;
                        return o.withUpdatedTimestamp(newTimestamp - 1);
                    },
                    noIncomingSelfDeletion(),
                    noExistingSelfDeletion(),
                    true,
                    x -> false);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TriePartitionUpdate(metadata, partitionKey, columns, stats, rowCountIncludingStatic, tombstoneCount, dataSize, t);
    }

    @Override
    public DeletionInfo deletionInfo()
    {
        // Collect deletion info from the trie.
        DeletionTime partitionLevelDeletion = partitionLevelDeletion();
        MutableDeletionInfo.Builder builder = MutableDeletionInfo.builder(partitionLevelDeletion, metadata.comparator, false);
        for (Map.Entry<ByteComparable.Preencoded, TrieTombstoneMarker> entry : trie.deletionOnlyTrie().entrySet())
        {
            RangeTombstoneMarker marker = entry.getValue().toRangeTombstoneMarker(entry.getKey(), BYTE_COMPARABLE_VERSION, metadata.comparator, partitionLevelDeletion);
            if (marker != null)
                builder.add(marker);
        }
        return builder.build();
    }

    /**
     * The number of "operations" contained in the update.
     * <p>
     * This is used by {@code Memtable} to approximate how much work this update does. In practice, this
     * count how many rows are updated and how many ranges are deleted by the partition update.
     *
     * @return the number of "operations" performed by the update.
     */
    @Override
    public int operationCount()
    {
        return rowCountIncludingStatic + tombstoneCount;
    }

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    @Override
    public int dataSize()
    {
        return dataSize;
    }

    /**
     * Validates the data contained in this update.
     *
     * @throws org.apache.cassandra.serializers.MarshalException if some of the data contained in this update is corrupted.
     */
    @Override
    public void validate()
    {
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
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
    @Override
    public long maxTimestamp()
    {
        long maxTimestamp = LivenessInfo.NO_TIMESTAMP;
        for (Iterator<TrieTombstoneMarker> it = trie.deletionOnlyTrie().valueIterator(); it.hasNext();)
            maxTimestamp = Math.max(maxTimestamp, it.next().deletionTime().markedForDeleteAt());
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
            maxTimestamp = Math.max(maxTimestamp, Rows.collectMaxTimestamp(it.next()));

        return maxTimestamp;
    }

    /**
     * For an update on a counter table, returns a list containing a {@code CounterMark} for
     * every counter contained in the update.
     *
     * @return a list with counter marks for every counter in this update.
     */
    @Override
    public List<CounterMark> collectCounterMarks()
    {
        assert metadata().isCounter();
        // We will take aliases on the rows of this update, and update them in-place. So we should be sure the
        // update is now immutable for all intent and purposes.
        List<CounterMark> marks = new ArrayList<>();
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            addMarksForRow(row, marks);
        }
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

    /**
     * Builder for PartitionUpdates
     *
     * This class is not thread safe, but the PartitionUpdate it produces is (since it is immutable).
     */
    public static class Builder implements PartitionUpdate.Builder
    {
        private final TableMetadata metadata;
        private final ColumnFilter cf;
        private final DecoratedKey key;
        private final RegularAndStaticColumns columns;
        private final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        private final EncodingStats.Collector statsCollector = new EncodingStats.Collector();
        private int rowCountIncludingStatic;
        private int tombstoneCount;
        private long dataSize;

        public Builder(TableMetadata metadata,
                       DecoratedKey key,
                       RegularAndStaticColumns columns)
        {
            this.metadata = metadata;
            this.key = key;
            this.columns = columns;
            rowCountIncludingStatic = 0;
            tombstoneCount = 0;
            dataSize = 0;
            cf = ColumnFilter.all(metadata);
        }

        /**
         * Adds a row to this update.
         * <p>
         * There is no particular assumption made on the order of row added to a partition update. It is further
         * allowed to add the same row (more precisely, multiple row objects for the same clustering).
         * <p>
         * Note however that the columns contained in the added row must be a subset of the columns used when
         * creating this update.
         *
         * @param row the row to add.
         */
        public void add(Row row)
        {
            if (row.isEmpty())
                return;

            // this assert is expensive, and possibly of limited value; we should consider removing it
            // or introducing a new class of assertions for test purposes
            assert (row.isStatic() ? columns().statics : columns().regulars).containsAll(row.columns())
            : (row.isStatic() ? columns().statics : columns().regulars) + " is not superset of " + row.columns();

            try
            {
                // We do not look for atomicity here, so can do the two steps separately.
                // TODO: Direct insertion methods (singleton known to not be deleted, deletion known to not delete anything)
                Clustering<?> clustering = row.clustering();
                DeletionTime deletionTime = row.deletion().time();

                ByteComparable comparableClustering = metadata.comparator.asByteComparable(clustering);
                if (!deletionTime.isLive())
                {
                    putDeletionInTrie(comparableClustering,
                                      comparableClustering,
                                      deletionTime);
                }
                if (!row.isEmptyAfterDeletion())
                {
                    trie.apply(DeletionAwareTrie.<Row, TrieTombstoneMarker>singleton(comparableClustering,
                                                                                     BYTE_COMPARABLE_VERSION,
                                                                                     row),
                               this::mergeIncomingRow,
                               this::mergeTombstones,
                               this::applyIncomingTombstone,
                               this::applyExistingTombstoneToIncomingRow,
                               true,
                               x -> false);
                }
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
            Rows.collectStats(row, statsCollector);
        }

        private void putDeletionInTrie(ByteComparable start, ByteComparable end, DeletionTime deletionTime)
        {
            try
            {
                trie.apply(DeletionAwareTrie.deletion(ByteComparable.EMPTY,
                                                      start,
                                                      end,
                                                      BYTE_COMPARABLE_VERSION,
                                                      TrieTombstoneMarker.covering(deletionTime)),
                           this::mergeIncomingRow,
                           this::mergeTombstones,
                           this::applyIncomingTombstone,
                           this::applyExistingTombstoneToIncomingRow,
                           true,
                           x -> false);
                statsCollector.update(deletionTime);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        public void addPartitionDeletion(DeletionTime deletionTime)
        {
            if (!deletionTime.isLive())
                putDeletionInTrie(PARTITION_DELETION_START, PARTITION_DELETION_END, deletionTime);
        }

        public void add(RangeTombstone range)
        {
            putDeletionInTrie(metadata.comparator.asByteComparable(range.deletedSlice().start()),
                              metadata.comparator.asByteComparable(range.deletedSlice().end()),
                              range.deletionTime());
        }

        public DecoratedKey partitionKey()
        {
            return key;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public TriePartitionUpdate build()
        {
            try
            {
                trie.putRecursive(ByteComparable.EMPTY, PARTITION_MARKER, noConflictInData());
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
            TriePartitionUpdate pu = new TriePartitionUpdate(metadata,
                                                             partitionKey(),
                                                             columns,
                                                             statsCollector.get(),
                                                             rowCountIncludingStatic,
                                                             tombstoneCount,
                                                             Ints.saturatedCast(dataSize),
                                                             trie);

            return pu;
        }

        RowData mergeIncomingRow(Object existing, Row update)
        {
            if (existing != null)
            {
                // this is not expected to happen much, so going through toRow and the existing size is okay
                RowData rowData = (RowData) existing;
                update = Rows.merge(rowData.toRow(update.clustering(), DeletionTime.LIVE), update);
                dataSize += update.dataSize() - rowData.dataSize();
            }
            else
            {
                ++rowCountIncludingStatic;
                dataSize += update.dataSize();
            }

            return rowToData(update);
        }

        private Row applyExistingTombstoneToIncomingRow(TrieTombstoneMarker trieTombstoneMarker, Row o)
        {
            return o.filter(cf, trieTombstoneMarker.deletionTime(), false, metadata);
        }

        private Object applyIncomingTombstone(Object o, TrieTombstoneMarker trieTombstoneMarker)
        {
            RowData row = (RowData) o;
            return row.delete(trieTombstoneMarker.deletionTime());
        }

        private TrieTombstoneMarker mergeTombstones(TrieTombstoneMarker existing, TrieTombstoneMarker update)
        {
            if (existing == null)
            {
                // We are adding a new tombstone.
                ++tombstoneCount;
                return update;
            }
            else
            {
                TrieTombstoneMarker merged = update.mergeWith(existing);
                if (merged == null || !merged.isBoundary())
                    --tombstoneCount;   // dropped the existing tombstone (covered by a newer one)
                return merged;
            }
        }

        public RegularAndStaticColumns columns()
        {
            return columns;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            TrieTombstoneMarker applicableRange = trie.deletionOnlyTrie().applicableRange(STATIC_CLUSTERING_PATH);
            return applicableRange != null ? applicableRange.deletionTime() : DeletionTime.LIVE;
        }

        @Override
        public String toString()
        {
            return "Builder{" +
                   "metadata=" + metadata +
                   ", key=" + key +
                   ", columns=" + columns +
                   '}';
        }
    }

    public static class TrieFactory implements PartitionUpdate.Factory
    {

        @Override
        public PartitionUpdate.Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
        {
            return new TriePartitionUpdate.Builder(metadata, partitionKey, columns);
        }

        @Override
        public PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey)
        {
            return TriePartitionUpdate.emptyUpdate(metadata, partitionKey);
        }

        @Override
        public PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey valueKey, Row row)
        {
            return TriePartitionUpdate.singleRowUpdate(metadata, valueKey, row);
        }

        @Override
        public PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, int nowInSec)
        {
            return TriePartitionUpdate.fullPartitionDelete(metadata, key, timestamp, nowInSec);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator)
        {
            return TriePartitionUpdate.fromIterator(iterator);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
        {
            return TriePartitionUpdate.fromIterator(UnfilteredRowIterators.withOnlyQueriedData(iterator, filter));
        }
    }
}
