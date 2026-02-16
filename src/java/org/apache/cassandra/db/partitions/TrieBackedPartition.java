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

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.IDataSize;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieSet;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;

/// In-memory partition backed by a deletion-aware trie. The rows of the partition are values in the leaves of the trie,
/// where the key to the row is only stored as the path to reach that leaf; static rows are also treated as a row with
/// `STATIC_CLUSTERING` path; the deletion information is placed in a deletion branch of the trie which starts at the
/// root of the partition. This matches how `TrieMemtable` stores partitions within the larger map, so that
/// `TrieBackedPartition` objects can be created directly from `TrieMemtable` tail tries.
///
/// This object also holds the partition key, as well as some metadata (columns and statistics).
/// Currently, all descendants and instances of this class are immutable (even tail tries from mutable memtables are
/// guaranteed to not change as we use forced copying below the partition level), though this may change in the future.
public class TrieBackedPartition implements Partition
{
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS50;

    /** Pre-made path for STATIC_CLUSTERING, to avoid creating path object when querying static path. */
    public static final ByteComparable STATIC_CLUSTERING_PATH = v -> ByteSource.oneByte(ClusteringPrefix.Kind.STATIC_CLUSTERING.asByteComparableValue(v));
    /** Pre-made path for BOTTOM, to avoid creating path object when iterating rows. */
    public static final ByteComparable BOTTOM_PATH = v -> ByteSource.oneByte(ClusteringPrefix.Kind.INCL_START_BOUND.asByteComparableValue(v));

    /// Pre-made path for partition deletions
    public static final ByteComparable PARTITION_DELETION_START = v -> ByteSource.oneByte(ByteSource.LT_EXCLUDED);
    public static final ByteComparable PARTITION_DELETION_END = v -> ByteSource.oneByte(ByteSource.GT_NEXT_COMPONENT);

    /// Interface implemented by partition markers, both the singleton below used for standalone [TrieBackedPartition],
    /// and the marker used in tail tries in `TrieMemtable`s.
    public interface PartitionMarker {}

    /// Singleton partition marker used for standalone [TrieBackedPartition] and [TriePartitionUpdate] objects.
    public static final PartitionMarker PARTITION_MARKER = new PartitionMarker()
    {
        public String toString()
        {
            return "PARTITION_MARKER";
        }
    };

    /// Predicate to identify partition boundaries in tries. This accepts any [PartitionMarker], not just the
    /// [#PARTITION_MARKER] used for standalone trie-backed partitions.
    public static final Predicate<Object> IS_PARTITION_BOUNDARY = TrieBackedPartition::isPartitionBoundary;

    /// Returns true if the given content is a partition marker.
    public static boolean isPartitionBoundary(Object content)
    {
        return content instanceof TrieBackedPartition.PartitionMarker;
    }

    /// The representation of a row stored at the leaf of a trie. Does not contain the row key.
    ///
    /// The method [#toRow] combines this with a clustering for the represented [Row].
    public static class RowData implements IDataSize
    {
        final Object[] columnsBTree;
        final LivenessInfo livenessInfo;
        final int minLocalDeletionTime;

        RowData(Object[] columnsBTree, LivenessInfo livenessInfo)
        {
            this(columnsBTree, livenessInfo, BTreeRow.minDeletionTime(columnsBTree, livenessInfo, DeletionTime.LIVE));
        }

        RowData(Object[] columnsBTree, LivenessInfo livenessInfo, int minLocalDeletionTime)
        {
            this.columnsBTree = columnsBTree;
            this.livenessInfo = livenessInfo;
            this.minLocalDeletionTime = minLocalDeletionTime;
        }

        Row toRow(Clustering<?> clustering, DeletionTime deletion)
        {
            return BTreeRow.create(clustering,
                                   livenessInfo,
                                   Row.Deletion.regular(deletion),
                                   columnsBTree,
                                   minLocalDeletionTime);
        }

        public int dataSize()
        {
            int dataSize = livenessInfo.dataSize();

            return Ints.checkedCast(BTree.accumulate(columnsBTree, (ColumnData cd, long v) -> v + cd.dataSize(), dataSize));
        }

        public long unsharedHeapSizeExcludingData()
        {
            long heapSize = EMPTY_ROWDATA_SIZE
                            + BTree.sizeOfStructureOnHeap(columnsBTree)
                            + livenessInfo.unsharedHeapSize();

            return BTree.accumulate(columnsBTree, (ColumnData cd, long v) -> v + cd.unsharedHeapSizeExcludingData(), heapSize);
        }

        public String toString()
        {
            return "row " + livenessInfo + " size " + dataSize() + ": " + BTree.toString(columnsBTree);
        }

        public RowData clone(Cloner cloner)
        {
            Object[] tree = BTree.<ColumnData, ColumnData>transform(columnsBTree, c -> c.clone(cloner));
            return new RowData(tree, livenessInfo, minLocalDeletionTime);
        }

        public RowData delete(DeletionTime activeDeletion)
        {
            LivenessInfo newLiveness = livenessInfo;
            if (activeDeletion.deletes(livenessInfo.timestamp()))
                newLiveness = LivenessInfo.EMPTY;

            Object[] newBTree = BTree.<ColumnData, ColumnData>transformAndFilter(columnsBTree, cd ->
            {
                ColumnMetadata column = cd.column();
                if (column.isComplex())
                    return ((ComplexColumnData) cd).delete(activeDeletion);

                Cell<?> cell = (Cell<?>) cd;
                return activeDeletion.deletes(cell) ? null : cell;
            });

            if (newLiveness == livenessInfo && newBTree == columnsBTree)
                return this;
            if (newLiveness.isEmpty() && newBTree == BTree.empty())
                return null;
            return new RowData(newBTree, newLiveness);
        }
    }

    private static final long EMPTY_ROWDATA_SIZE = ObjectSizes.measure(new RowData(null, null, 0));

    protected final DeletionAwareTrie<Object, TrieTombstoneMarker> trie;
    protected final DecoratedKey partitionKey;
    protected final TableMetadata metadata;
    protected final RegularAndStaticColumns columns;
    protected final EncodingStats stats;
    protected final int rowCountIncludingStatic;
    protected final int tombstoneCount;

    public TrieBackedPartition(DecoratedKey partitionKey,
                               RegularAndStaticColumns columns,
                               EncodingStats stats,
                               int rowCountIncludingStatic,
                               int tombstoneCount,
                               DeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                               TableMetadata metadata)
    {
        this.partitionKey = partitionKey;
        this.trie = trie;
        this.metadata = metadata;
        this.columns = columns;
        this.stats = stats;
        this.rowCountIncludingStatic = rowCountIncludingStatic;
        this.tombstoneCount = tombstoneCount;
        // There must always be a partition marker.
        assert trie.get(ByteComparable.EMPTY) != null;
        assert stats != null;
    }

    public static TrieBackedPartition fromIterator(UnfilteredRowIterator iterator)
    {
        ContentBuilder builder = build(iterator, false);
        return new TrieBackedPartition(iterator.partitionKey(),
                                       iterator.columns(),
                                       iterator.stats(),
                                       builder.rowCountIncludingStatic(),
                                       builder.tombstoneCount(),
                                       builder.trie(),
                                       iterator.metadata());
    }

    protected static ContentBuilder build(UnfilteredRowIterator iterator, boolean collectDataSize)
    {
        try
        {
            ContentBuilder builder = new ContentBuilder(iterator.metadata(), iterator.partitionLevelDeletion(), iterator.isReverseOrder(), collectDataSize);

            builder.addStatic(iterator.staticRow());

            while (iterator.hasNext())
                builder.addUnfiltered(iterator.next());

            return builder.complete();
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    /// Create a row with the given properties and content, making sure to copy all off-heap data to keep it alive when
    /// the given access mode requires it.
    public static TrieBackedPartition create(DecoratedKey partitionKey,
                                             RegularAndStaticColumns columnMetadata,
                                             EncodingStats encodingStats,
                                             int rowCountIncludingStatic,
                                             int tombstoneCount,
                                             DeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                             TableMetadata metadata,
                                             EnsureOnHeap ensureOnHeap)
    {
        return ensureOnHeap == EnsureOnHeap.NOOP
               ? new TrieBackedPartition(partitionKey, columnMetadata, encodingStats, rowCountIncludingStatic, tombstoneCount, trie, metadata)
               : new WithEnsureOnHeap(partitionKey, columnMetadata, encodingStats, rowCountIncludingStatic, tombstoneCount, trie, metadata, ensureOnHeap);
    }

    class RowIterator extends TrieEntriesIterator.WithNullFiltering<Object, Row>
    {
        public RowIterator(DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
        {
            super(trie.mergedTrie(TrieBackedPartition::combineDataAndDeletion), direction);
        }

        @Override
        protected Row mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (content instanceof RowData)
                return toRow((RowData) content,
                             getClustering(bytes, byteLength));
            if (content instanceof Row)
            {
                BTreeRow row = (BTreeRow) content;
                return BTreeRow.create(getClustering(bytes, byteLength),
                                       row.primaryKeyLivenessInfo(),
                                       row.deletion(),
                                       row.getBTree(),
                                       row.getMinLocalDeletionTime());
            }

            TrieTombstoneMarker marker = (TrieTombstoneMarker) content;
            if (marker.hasPointData())
                return BTreeRow.emptyDeletedRow(getClustering(bytes, byteLength),
                                                Row.Deletion.regular(marker.deletionTime()));
            else
                return null;
        }
    }

    private Iterator<Row> rowIterator(DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
    {
        return new RowIterator(trie, direction);
    }

    static RowData rowToData(Row row)
    {
        BTreeRow brow = (BTreeRow) row;
        return new RowData(brow.getBTree(), row.primaryKeyLivenessInfo(), brow.getMinLocalDeletionTime());
    }

    /// Conversion from [RowData] to [Row]. [WithEnsureOnHeap] overrides this to do the necessary copying
    /// (hence the non-static method).
    Row toRow(RowData data, Clustering<?> clustering)
    {
        return data.toRow(clustering, DeletionTime.LIVE);
    }

    /// Put the given unfiltered in the trie, used by methods to build stand-alone partitions.
    ///
    /// @param comparator for converting key to byte-comparable
    /// @param trie destination
    /// @param row content to put
    protected static void putInTrie(ClusteringComparator comparator, InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie, Row row)
    throws TrieSpaceExhaustedException
    {
        // We do not look for atomicity here, so can do the two steps separately.
        Clustering<?> clustering = row.clustering();
        DeletionTime deletionTime = row.deletion().time();

        ByteComparable comparableClustering = comparator.asByteComparable(clustering);
        if (!deletionTime.isLive())
        {
            putDeletionInTrie(trie,
                              comparableClustering,
                              comparableClustering,
                              deletionTime);
        }
        if (!row.isEmptyAfterDeletion())
        {
            trie.apply(DeletionAwareTrie.singleton(comparableClustering,
                                                   BYTE_COMPARABLE_VERSION,
                                                   rowToData(row)),
                       noConflictInData(),
                       mergeTombstoneRanges(),
                       noIncomingSelfDeletion(),
                       noExistingSelfDeletion(),
                       true,
                       x -> false);
        }
    }

    protected static void putMarkerInTrie(ClusteringComparator comparator, 
                                          InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                          RangeTombstoneMarker openMarker,
                                          RangeTombstoneMarker closeMarker)
    {
        DeletionTime deletionTime = openMarker.openDeletionTime(false);
        assert deletionTime.equals(closeMarker.closeDeletionTime(false));
        putDeletionInTrie(trie,
                          comparator.asByteComparable(openMarker.clustering()),
                          comparator.asByteComparable(closeMarker.clustering()),
                          deletionTime);
    }

    protected static void putPartitionDeletionInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                                     DeletionTime deletionTime)
    {
        putDeletionInTrie(trie,
                          PARTITION_DELETION_START,
                          PARTITION_DELETION_END,
                          deletionTime);
    }

    static void putDeletionInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                  ByteComparable start,
                                  ByteComparable end,
                                  DeletionTime deletionTime)
    {
        try
        {
            trie.apply(DeletionAwareTrie.deletion(ByteComparable.EMPTY,
                                                  start,
                                                  end,
                                                  BYTE_COMPARABLE_VERSION,
                                                  TrieTombstoneMarker.covering(deletionTime)),
                       noConflictInData(),
                       mergeTombstoneRanges(),
                       noIncomingSelfDeletion(),
                       noExistingSelfDeletion(),
                       true,
                       x -> false);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }


    public TableMetadata metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        // Since static rows can only be deleted via the partition deletion, getting the applicable deletion of the
        // static row (or a logical position for it if a static row is not defined) gives us the applicable partition
        // deletion.
        TrieTombstoneMarker applicableRange = trie.deletionOnlyTrie().applicableRange(STATIC_CLUSTERING_PATH);
        return applicableRange != null ? applicableRange.deletionTime() : DeletionTime.LIVE;
    }

    public RegularAndStaticColumns columns()
    {
        return columns;
    }

    public EncodingStats stats()
    {
        return stats;
    }

    public int rowCount()
    {
        return rowCountIncludingStatic - (hasStaticRow() ? 1 : 0);
    }

    public ByteComparable path(ClusteringPrefix<?> clustering)
    {
        return metadata.comparator.asByteComparable(clustering);
    }

    public Row staticRow()
    {
        // Static rows can only be deleted via the partition deletion. There is no need to check and apply that here.
        RowData staticRow = (RowData) trie.get(STATIC_CLUSTERING_PATH);
        return staticRow != null ? staticRow.toRow(Clustering.STATIC_CLUSTERING, DeletionTime.LIVE) : Rows.EMPTY_STATIC_ROW;
    }

    public boolean isEmpty()
    {
        return rowCountIncludingStatic + tombstoneCount == 0;
    }

    private boolean hasStaticRow()
    {
        return trie.get(STATIC_CLUSTERING_PATH) != null;
    }

    public boolean hasRows()
    {
        return rowCountIncludingStatic > 1 || rowCountIncludingStatic > 0 && !hasStaticRow();
    }

    /// Provides read access to the trie for users that can take advantage of it directly (e.g. `TrieMemtable`).
    public DeletionAwareTrie<Object, TrieTombstoneMarker> trie()
    {
        return trie;
    }

    private DeletionAwareTrie<Object, TrieTombstoneMarker> nonStaticSubtrie()
    {
        // skip static row if present - the static clustering sorts before BOTTOM so that it's never included in
        // any slices (we achieve this by using the byte ByteSource.EXCLUDED for its representation, which is lower
        // than BOTTOM's ByteSource.LT_NEXT_COMPONENT).
        return trie.subtrie(BOTTOM_PATH, null);
    }

    public Iterator<Row> rowIterator()
    {
        return rowIterator(nonStaticSubtrie(), Direction.FORWARD);
    }

    public Iterator<Row> rowsIncludingStatic()
    {
        return rowIterator(trie, Direction.FORWARD);
    }

    @Override
    public Row lastRow()
    {
        Iterator<Row> reverseIterator = rowIterator(nonStaticSubtrie(), Direction.REVERSE);
        return reverseIterator.hasNext() ? reverseIterator.next() : null;
    }

    public Row getRow(Clustering<?> clustering)
    {
        return getRow(clustering, path(clustering));
    }

    public Row getRow(Clustering<?> clustering, ByteComparable path)
    {
        RowData data = (RowData) trie.get(path);
        TrieTombstoneMarker marker = trie.applicableDeletion(path);
        if (data != null)
            return data.toRow(clustering, marker != null ? marker.deletionTime() : DeletionTime.LIVE);
        else if (marker != null)
            return BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(marker.deletionTime()));
        else
            return null;
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    public static Object combineDataAndDeletion(Object data, TrieTombstoneMarker deletion)
    {
        if (data == null)
            return deletion; // Range or partitions tombstones will follow this path.
        // drop the PartitionMarker
        if (data instanceof PartitionMarker)
            return deletion;

        if (deletion == null)
            return data;
        // mergedTrie will give the covering deletion for any row it reports (i.e. active range or partition deletion);
        // ignore it as we don't want to change rows' deletion time to apply it.
        if (!deletion.isBoundary())
            return data;
        // Tombstone boundaries have different clustering positions than rows; the only boundary that can match the
        // position of a row is a point deletion.
        assert deletion.hasPointData() : "Deletion tombstone boundary " + deletion + " clashes with row " + data;

        // This is a row combined with a point deletion.
        RowData rowData = (RowData) data;
        return rowData.toRow(Clustering.EMPTY, deletion.deletionTime());
    }

    private Clustering<?> getClustering(byte[] bytes, int byteLength)
    {
        return metadata.comparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                ByteComparable.preencoded(BYTE_COMPARABLE_VERSION,
                                                                                          bytes, 0, byteLength),
                                                                BYTE_COMPARABLE_VERSION);
    }

    /// Implementation of [UnfilteredRowIterator] for this partition.
    ///
    /// Currently, this implementation has to revert the transformation done to partition-level deletions. To do that,
    /// we extract the partition-level deletion from its coverage of the static row and filter out tombstone ranges that
    /// switch to it.
    class UnfilteredIterator
    extends TrieEntriesIterator.WithNullFiltering<Object, Unfiltered>
    implements UnfilteredRowIterator
    {
        final boolean reversed;
        final ColumnFilter selection;
        final DeletionTime partitionLevelDeletion;
        final DeletionAwareTrie<Object, TrieTombstoneMarker> trie;
        final Row staticRow;

        protected UnfilteredIterator(ColumnFilter selection, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, boolean reversed)
        {
            this(selection, trie, reversed, TrieBackedPartition.this.partitionLevelDeletion());
        }

        private UnfilteredIterator(ColumnFilter selection, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, boolean reversed, DeletionTime partitionLevelDeletion)
        {
            super(trie.mergedTrieSwitchable(TrieBackedPartition::combineDataAndDeletion),
                  Direction.fromBoolean(reversed));
            this.trie = trie;
            this.selection = selection;
            this.reversed = reversed;
            this.partitionLevelDeletion = partitionLevelDeletion;
            Row staticRow = TrieBackedPartition.this.staticRow().filter(selection, metadata());
            this.staticRow = staticRow != null ? staticRow : Rows.EMPTY_STATIC_ROW;
        }

        @Override
        protected Unfiltered mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (content instanceof RowData)
                return toRow((RowData) content,
                             getClustering(bytes, byteLength))    // deletion is given as range tombstone
                       .filter(selection, metadata());
            if (content instanceof Row)
            {
                BTreeRow row = (BTreeRow) content;
                return BTreeRow.create(getClustering(bytes, byteLength),
                                       row.primaryKeyLivenessInfo(),
                                       row.deletion(),
                                       row.getBTree(),
                                       row.getMinLocalDeletionTime())
                       .filter(selection, metadata());
            }

            TrieTombstoneMarker marker = (TrieTombstoneMarker) content;
            if (marker.hasPointData())
                return BTreeRow.emptyDeletedRow(getClustering(bytes, byteLength),
                                                Row.Deletion.regular(marker.deletionTime()));
            else
                return ((TrieTombstoneMarker) content).toRangeTombstoneMarker(
                    ByteComparable.preencoded(BYTE_COMPARABLE_VERSION, bytes, 0, byteLength),
                    BYTE_COMPARABLE_VERSION,
                    metadata.comparator,
                    partitionLevelDeletion);
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        @Override
        public EncodingStats stats()
        {
            return stats;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return reversed;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return selection.fetchedColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public void close()
        {
            // nothing to close
        }

        @Override
        public boolean stopIssuingTombstones()
        {
            ((DeletionAwareTrie.DeletionsStopControl) cursor).stopIssuingDeletions(this);

            Unfiltered next = peekNextIfAvailable();
            if (next != null && next.isRangeTombstoneMarker())
                consumeNext();
            return true;
        }
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, ByteComparable[] bounds, boolean reversed)
    {
        if (bounds.length == 0)
            return UnfilteredRowIterators.noRowsIterator(metadata, partitionKey, staticRow(), partitionLevelDeletion(), reversed);

        DeletionAwareTrie<Object, TrieTombstoneMarker> slicedTrie = trie.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, bounds));
        return new UnfilteredIterator(selection, slicedTrie, reversed);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        ByteComparable[] bounds = new ByteComparable[slices.size() * 2];
        int index = 0;
        for (Slice slice : slices)
        {
            bounds[index++] = metadata.comparator.asByteComparable(slice.start());
            bounds[index++] = metadata.comparator.asByteComparable(slice.end());
        }
        return unfilteredIterator(selection, bounds, reversed);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        ByteComparable[] bounds = new ByteComparable[clusteringsInQueryOrder.size() * 2];
        // Trie intersection requires the boundaries to be given in forward order. Our clusterings are given in query
        // order, which is why we have to reverse them if we are making a reversed iterator.
        int index = reversed ? (clusteringsInQueryOrder.size() - 1) * 2 : 0;
        int indexInc = reversed ? -2 : +2;
        for (Clustering<?> clustering : clusteringsInQueryOrder)
        {
            bounds[index + 0] = metadata.comparator.asByteComparable(clustering.asStartBound());
            bounds[index + 1] = metadata.comparator.asByteComparable(clustering.asEndBound());
            index += indexInc;
        }
        return unfilteredIterator(selection, bounds, reversed);
    }

    @Override
    public String toString()
    {
        return Partition.toString(this);
    }

    /// A snapshot of the current [TrieBackedPartition] data, copied on heap when retrieved.
    private static final class WithEnsureOnHeap extends TrieBackedPartition
    {
        EnsureOnHeap ensureOnHeap;

        public WithEnsureOnHeap(DecoratedKey partitionKey,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                int rowCountIncludingStatic,
                                int tombstoneCount,
                                DeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                TableMetadata metadata,
                                EnsureOnHeap ensureOnHeap)
        {
            super(partitionKey, columns, stats, rowCountIncludingStatic, tombstoneCount, trie, metadata);
            this.ensureOnHeap = ensureOnHeap;
        }

        @Override
        public Row toRow(RowData data, Clustering<?> clustering)
        {
            return ensureOnHeap.applyToRow(super.toRow(data, clustering));
        }
    }

    /// Resolver for operations with trie-backed partitions. We don't permit any overwrites/merges.
    @SuppressWarnings("rawtypes")
    private static final InMemoryTrie.UpsertTransformer NO_CONFLICT_RESOLVER =
            (existing, update) ->
            {
                if (existing != null)
                    throw new AssertionError("Unique rows expected.");
                return update;
            };

    /// Resolver for data in trie-backed partitions. We don't permit any overwrites/merges.
    @SuppressWarnings("unchecked")
    public static InMemoryTrie.UpsertTransformer<Object, Object> noConflictInData()
    {
        return NO_CONFLICT_RESOLVER;
    }

    /// Tombstone merging resolver. Even though we don't support overwrites, we get requests to add the two sides
    /// of a boundary separately and must join them.
    private static final InMemoryTrie.UpsertTransformer<TrieTombstoneMarker, TrieTombstoneMarker> MERGE_TOMBSTONE_RANGES =
        (existing, update) -> existing != null ? existing.mergeWith(update) : update;

    /// Tombstone merging resolver. Even though we don't support overwrites, we get requests to add the two sides
    /// of a boundary separately and must join them.
    public static InMemoryTrie.UpsertTransformer<TrieTombstoneMarker, TrieTombstoneMarker> mergeTombstoneRanges()
    {
        return MERGE_TOMBSTONE_RANGES;
    }

    private static final InMemoryBaseTrie.UpsertTransformer<Object, TrieTombstoneMarker> IGNORE_UPDATE = (left, right) -> left;

    /// Resolver for applying incoming deletions to existing data in trie-backed partitions. We assume that the data is
    /// not affected by the deletion.
    public static InMemoryTrie.UpsertTransformer<Object, TrieTombstoneMarker> noIncomingSelfDeletion()
    {
        return IGNORE_UPDATE;
    }

    private static final BiFunction<TrieTombstoneMarker, Object, Object> IGNORE_EXISTING = (left, right) -> right;

    /// Resolver for applying existing deletions to incoming data in trie-backed partitions. We assume that the data is
    /// not affected by the deletion.
    public static BiFunction<TrieTombstoneMarker, Object, Object> noExistingSelfDeletion()
    {
        return IGNORE_EXISTING;
    }

    /// Helper class for constructing tries and deletion info from an iterator.
    public static class ContentBuilder
    {
        final TableMetadata metadata;
        final ClusteringComparator comparator;

        private final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie;

        private final boolean collectDataSize;

        private int rowCountIncludingStatic;
        private int tombstoneCount;
        private long dataSize;

        private RangeTombstoneMarker openMarker = null;
        private final boolean isReverseOrder;

        public ContentBuilder(TableMetadata metadata, DeletionTime partitionLevelDeletion, boolean isReverseOrder, boolean collectDataSize)
        {
            this.metadata = metadata;
            this.comparator = metadata.comparator;

            this.trie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);

            this.collectDataSize = collectDataSize;

            rowCountIncludingStatic = 0;
            tombstoneCount = 0;
            dataSize = 0;
            this.isReverseOrder = isReverseOrder;

            if (!partitionLevelDeletion.isLive())
            {
                putPartitionDeletionInTrie(trie, partitionLevelDeletion);
                ++tombstoneCount;
            }
        }

        public ContentBuilder addStatic(Row staticRow) throws TrieSpaceExhaustedException
        {
            if (!staticRow.isEmpty())
                return addRow(staticRow);
            else
                return this;
        }

        public ContentBuilder addRow(Row row) throws TrieSpaceExhaustedException
        {
            putInTrie(comparator, trie, row);
            ++rowCountIncludingStatic;
            if (collectDataSize)
                dataSize += row.dataSize();
            if (!row.deletion().isLive())
                ++tombstoneCount;
            return this;
        }

        public ContentBuilder addRangeTombstoneMarker(RangeTombstoneMarker unfiltered)
        {
            if (openMarker != null)
            {
                // This will check that unfiltered closes openMarker
                putMarkerInTrie(comparator, trie,
                                isReverseOrder ? unfiltered : openMarker,
                                isReverseOrder ? openMarker : unfiltered);
                ++tombstoneCount;
                if (unfiltered.isOpen(isReverseOrder))
                    openMarker = unfiltered;
                else
                    openMarker = null;
            }
            else
            {
                assert unfiltered.isOpen(isReverseOrder);
                openMarker = unfiltered;
            }
            return this;
        }

        public ContentBuilder addUnfiltered(Unfiltered unfiltered) throws TrieSpaceExhaustedException
        {
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                return addRow((Row) unfiltered);
            else
                return addRangeTombstoneMarker((RangeTombstoneMarker) unfiltered);
        }

        public ContentBuilder complete() throws TrieSpaceExhaustedException
        {
            assert openMarker == null;
            trie.putRecursive(ByteComparable.EMPTY, PARTITION_MARKER, noConflictInData());    // will throw if called more than once
            return this;
        }

        public DeletionAwareTrie<Object, TrieTombstoneMarker> trie()
        {
            return trie;
        }

        public int rowCountIncludingStatic()
        {
            return rowCountIncludingStatic;
        }

        public int tombstoneCount()
        {
            return tombstoneCount;
        }

        public int dataSize()
        {
            assert collectDataSize;
            return Ints.saturatedCast(dataSize);
        }
    }
}
