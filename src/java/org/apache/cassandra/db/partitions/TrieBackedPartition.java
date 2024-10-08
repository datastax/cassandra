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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;

/**
 * In-memory partition backed by a trie. The rows of the partition are values in the leaves of the trie, where the key
 * to the row is only stored as the path to reach that leaf; static rows are also treated as a row with STATIC_CLUSTERING
 * path; the deletion information is placed as a metadata object at the root of the trie -- this matches how Memtable
 * stores partitions within the larger map, so that TrieBackedPartition objects can be created directly from Memtable
 * tail tries.
 *
 * This object also holds the partition key, as well as some metadata (columns and statistics).
 *
 * Currently all descendants and instances of this class are immutable (even tail tries from mutable memtables are
 * guaranteed to not change as we use forced copying below the partition level), though this may change in the future.
 */
public class TrieBackedPartition implements Partition
{
    /**
     * If keys are below this length, we will use a recursive procedure for inserting data when building the backing
     * trie.
     */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS50;

    /** Pre-made path for STATIC_CLUSTERING, to avoid creating path object when querying static path. */
    public static final ByteComparable STATIC_CLUSTERING_PATH = v -> ByteSource.oneByte(ClusteringPrefix.Kind.STATIC_CLUSTERING.asByteComparableValue(v));
    /** Pre-made path for BOTTOM, to avoid creating path object when iterating rows. */
    public static final ByteComparable BOTTOM_PATH = v -> ByteSource.oneByte(ClusteringPrefix.Kind.INCL_START_BOUND.asByteComparableValue(v));

    /**
     * The representation of a row stored at the leaf of a trie. Does not contain the row key.
     *
     * The methods toRow and copyToOnHeapRow combine this with a clustering for the represented Row.
     */
    public static class RowData
    {
        final Object[] columnsBTree;
        final LivenessInfo livenessInfo;
        final DeletionTime deletion;
        final int minLocalDeletionTime;

        RowData(Object[] columnsBTree, LivenessInfo livenessInfo, DeletionTime deletion)
        {
            this(columnsBTree, livenessInfo, deletion, BTreeRow.minDeletionTime(columnsBTree, livenessInfo, deletion));
        }

        RowData(Object[] columnsBTree, LivenessInfo livenessInfo, DeletionTime deletion, int minLocalDeletionTime)
        {
            this.columnsBTree = columnsBTree;
            this.livenessInfo = livenessInfo;
            this.deletion = deletion;
            this.minLocalDeletionTime = minLocalDeletionTime;
        }

        Row toRow(Clustering<?> clustering)
        {
            return BTreeRow.create(clustering,
                                   livenessInfo,
                                   Row.Deletion.regular(deletion),
                                   columnsBTree,
                                   minLocalDeletionTime);
        }

        public int dataSize()
        {
            int dataSize = livenessInfo.dataSize() + deletion.dataSize();

            return Ints.checkedCast(BTree.accumulate(columnsBTree, (ColumnData cd, long v) -> v + cd.dataSize(), dataSize));
        }

        public long unsharedHeapSizeExcludingData()
        {
            long heapSize = EMPTY_ROWDATA_SIZE
                            + BTree.sizeOfStructureOnHeap(columnsBTree)
                            + livenessInfo.unsharedHeapSize()
                            + deletion.unsharedHeapSize();

            return BTree.accumulate(columnsBTree, (ColumnData cd, long v) -> v + cd.unsharedHeapSizeExcludingData(), heapSize);
        }

        public String toString()
        {
            return "row " + livenessInfo + " size " + dataSize();
        }

        public RowData clone(Cloner cloner)
        {
            Object[] tree = BTree.<ColumnData, ColumnData>transform(columnsBTree, c -> c.clone(cloner));
            return new RowData(tree, livenessInfo, deletion, minLocalDeletionTime);
        }
    }

    private static final long EMPTY_ROWDATA_SIZE = ObjectSizes.measure(new RowData(null, null, null, 0));

    protected final Trie<Object> trie;
    protected final DecoratedKey partitionKey;
    protected final TableMetadata metadata;
    protected final RegularAndStaticColumns columns;
    protected final EncodingStats stats;
    protected final int rowCountIncludingStatic;
    protected final boolean canHaveShadowedData;

    public TrieBackedPartition(DecoratedKey partitionKey,
                               RegularAndStaticColumns columns,
                               EncodingStats stats,
                               int rowCountIncludingStatic,
                               Trie<Object> trie,
                               TableMetadata metadata,
                               boolean canHaveShadowedData)
    {
        this.partitionKey = partitionKey;
        this.trie = trie;
        this.metadata = metadata;
        this.columns = columns;
        this.stats = stats;
        this.rowCountIncludingStatic = rowCountIncludingStatic;
        this.canHaveShadowedData = canHaveShadowedData;
        // There must always be deletion info metadata.
        // Note: we can't use deletionInfo() because WithEnsureOnHeap's override is not yet set up.
        assert trie.get(ByteComparable.EMPTY) != null;
        assert stats != null;
    }

    public static TrieBackedPartition create(UnfilteredRowIterator iterator)
    {
        ContentBuilder builder = build(iterator, false);
        return new TrieBackedPartition(iterator.partitionKey(),
                                       iterator.columns(),
                                       iterator.stats(),
                                       builder.rowCountIncludingStatic(),
                                       builder.trie(),
                                       iterator.metadata(),
                                       false);
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

    /**
     * Create a row with the given properties and content, making sure to copy all off-heap data to keep it alive when
     * the given access mode requires it.
     */
    public static TrieBackedPartition create(DecoratedKey partitionKey,
                                             RegularAndStaticColumns columnMetadata,
                                             EncodingStats encodingStats,
                                             int rowCountIncludingStatic,
                                             Trie<Object> trie,
                                             TableMetadata metadata,
                                             EnsureOnHeap ensureOnHeap)
    {
        return ensureOnHeap == EnsureOnHeap.NOOP
               ? new TrieBackedPartition(partitionKey, columnMetadata, encodingStats, rowCountIncludingStatic, trie, metadata, true)
               : new WithEnsureOnHeap(partitionKey, columnMetadata, encodingStats, rowCountIncludingStatic, trie, metadata, true, ensureOnHeap);
    }

    class RowIterator extends TrieEntriesIterator<Object, Row>
    {
        public RowIterator(Trie<Object> trie, Direction direction)
        {
            super(trie, direction, RowData.class::isInstance);
        }

        @Override
        protected Row mapContent(Object content, byte[] bytes, int byteLength)
        {
            var rd = (RowData) content;
            return toRow(rd,
                         metadata.comparator.clusteringFromByteComparable(
                             ByteBufferAccessor.instance,
                             ByteComparable.preencoded(BYTE_COMPARABLE_VERSION, bytes, 0, byteLength)));
        }
    }

    private Iterator<Row> rowIterator(Trie<Object> trie, Direction direction)
    {
        return new RowIterator(trie, direction);
    }

    static RowData rowToData(Row row)
    {
        BTreeRow brow = (BTreeRow) row;
        return new RowData(brow.getBTree(), row.primaryKeyLivenessInfo(), row.deletion().time(), brow.getMinLocalDeletionTime());
    }

    /**
     * Conversion from RowData to Row. TrieBackedPartitionOnHeap overrides this to do the necessary copying
     * (hence the non-static method).
     */
    Row toRow(RowData data, Clustering clustering)
    {
        return data.toRow(clustering);
    }

    /**
     * Put the given unfiltered in the trie.
     * @param comparator for converting key to byte-comparable
     * @param useRecursive whether the key length is guaranteed short and recursive put can be used
     * @param trie destination
     * @param row content to put
     */
    protected static void putInTrie(ClusteringComparator comparator, boolean useRecursive, InMemoryTrie<Object> trie, Row row) throws TrieSpaceExhaustedException
    {
        trie.putSingleton(comparator.asByteComparable(row.clustering()), rowToData(row), NO_CONFLICT_RESOLVER, useRecursive);
    }

    /**
     * Check if we can use recursive operations when putting a value in tries.
     * True if all types in the clustering keys are fixed length, and total size is small enough.
     */
    protected static boolean useRecursive(ClusteringComparator comparator)
    {
        int length = 1; // terminator
        for (AbstractType type : comparator.subtypes())
            if (!type.isValueLengthFixed())
                return false;
            else
                length += 1 + type.valueLengthIfFixed();    // separator + value

        return length <= MAX_RECURSIVE_KEY_LENGTH;
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
        return deletionInfo().getPartitionDeletion();
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

    public DeletionInfo deletionInfo()
    {
        return (DeletionInfo) trie.get(ByteComparable.EMPTY);
    }

    public ByteComparable path(ClusteringPrefix clustering)
    {
        return metadata.comparator.asByteComparable(clustering);
    }

    public Row staticRow()
    {
        RowData staticRow = (RowData) trie.get(STATIC_CLUSTERING_PATH);

        if (staticRow != null)
            return toRow(staticRow, Clustering.STATIC_CLUSTERING);
        else
            return Rows.EMPTY_STATIC_ROW;
    }

    public boolean isEmpty()
    {
        return rowCountIncludingStatic == 0 && deletionInfo().isLive();
    }

    private boolean hasStaticRow()
    {
        return trie.get(STATIC_CLUSTERING_PATH) != null;
    }

    public boolean hasRows()
    {
        return rowCountIncludingStatic > 1 || rowCountIncludingStatic > 0 && !hasStaticRow();
    }

    /**
     * Provides read access to the trie for users that can take advantage of it directly (e.g. Memtable).
     */
    public Trie<Object> trie()
    {
        return trie;
    }

    private Trie<Object> nonStaticSubtrie()
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

    public Row getRow(Clustering clustering)
    {
        RowData data = (RowData) trie.get(path(clustering));

        DeletionInfo deletionInfo = deletionInfo();
        RangeTombstone rt = deletionInfo.rangeCovering(clustering);

        // The trie only contains rows, so it doesn't allow to directly account for deletion that should apply to row
        // (the partition deletion or the deletion of a range tombstone that covers it). So if needs be, reuse the row
        // deletion to carry the proper deletion on the row.
        DeletionTime partitionDeletion = deletionInfo.getPartitionDeletion();
        DeletionTime activeDeletion = partitionDeletion;
        if (rt != null && rt.deletionTime().supersedes(activeDeletion))
            activeDeletion = rt.deletionTime();

        if (data == null)
        {
            // this means our partition level deletion supersedes all other deletions and we don't have to keep the row deletions
            if (activeDeletion == partitionDeletion)
                return null;
            // no need to check activeDeletion.isLive here - if anything superseedes the partitionDeletion
            // it must be non-live
            return BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion));
        }

        Row row = toRow(data, clustering);
        if (!activeDeletion.isLive())
            row = row.filter(ColumnFilter.selection(columns()), activeDeletion, true, metadata());
        return row;
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        Row staticRow = staticRow(selection, false);
        if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = deletionInfo().getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return slices.size() == 1
               ? sliceIterator(selection, slices.get(0), reversed, staticRow)
               : new SlicesIterator(selection, slices, reversed, staticRow);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        Row staticRow = staticRow(selection, false);
        if (clusteringsInQueryOrder.isEmpty())
        {
            DeletionTime partitionDeletion = deletionInfo().getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        Iterator<Row> rowIter = new AbstractIterator<Row>() {

            Iterator<Clustering<?>> clusterings = clusteringsInQueryOrder.iterator();

            @Override
            protected Row computeNext()
            {
                while (clusterings.hasNext())
                {
                    Clustering<?> clustering = clusterings.next();
                    Object rowData = trie.get(path(clustering));
                    if (rowData instanceof RowData)
                        return toRow((RowData) rowData, clustering);
                }
                return endOfData();
            }
        };

        // not using DeletionInfo.rangeCovering(Clustering), because it returns the original range tombstone,
        // but we need DeletionInfo.rangeIterator(Set<Clustering>) that generates tombstones based on given clustering bound.
        Iterator<RangeTombstone> deleteIter = deletionInfo().rangeIterator(clusteringsInQueryOrder, reversed);

        return merge(rowIter, deleteIter, selection, reversed, staticRow);
    }

    private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, Row staticRow)
    {
        ClusteringBound start = slice.start();
        ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();
        Iterator<Row> rowIter = slice(start, end, reversed);
        Iterator<RangeTombstone> deleteIter = deletionInfo().rangeIterator(slice, reversed);
        return merge(rowIter, deleteIter, selection, reversed, staticRow);
    }

    private Iterator<Row> slice(ClusteringBound start, ClusteringBound end, boolean reversed)
    {
        ByteComparable endPath = end != null ? path(end) : null;
        // use BOTTOM as bound to skip over static rows
        ByteComparable startPath = start != null ? path(start) : BOTTOM_PATH;
        return rowIterator(trie.subtrie(startPath, endPath), Direction.fromBoolean(reversed));
    }

    private Row staticRow(ColumnFilter columns, boolean setActiveDeletionToRow)
    {
        DeletionTime partitionDeletion = deletionInfo().getPartitionDeletion();
        Row staticRow = staticRow();
        if (columns.fetchedColumns().statics.isEmpty() || (staticRow.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, metadata());
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter, Iterator<RangeTombstone> deleteIter,
                                              ColumnFilter selection, boolean reversed, Row staticRow)
    {
        return new RowAndDeletionMergeIterator(metadata(), partitionKey(), deletionInfo().getPartitionDeletion(),
                                               selection, staticRow, reversed, stats(),
                                               rowIter, deleteIter, canHaveShadowedData);
    }


    @Override
    public String toString()
    {
        return Partition.toString(this);
    }

    class SlicesIterator extends AbstractUnfilteredRowIterator
    {
        private final Slices slices;

        private int idx;
        private Iterator<Unfiltered> currentSlice;
        private final ColumnFilter selection;

        private SlicesIterator(ColumnFilter selection,
                               Slices slices,
                               boolean isReversed,
                               Row staticRow)
        {
            super(TrieBackedPartition.this.metadata(), TrieBackedPartition.this.partitionKey(),
                  TrieBackedPartition.this.partitionLevelDeletion(),
                  selection.fetchedColumns(), staticRow, isReversed, TrieBackedPartition.this.stats());
            this.selection = selection;
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

                    int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                    currentSlice = sliceIterator(selection, slices.get(sliceIdx), isReverseOrder, Rows.EMPTY_STATIC_ROW);
                    idx++;
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }


    /**
     * An snapshot of the current TrieBackedPartition data, copied on heap when retrieved.
     */
    private static final class WithEnsureOnHeap extends TrieBackedPartition
    {
        final DeletionInfo onHeapDeletion;
        EnsureOnHeap ensureOnHeap;

        public WithEnsureOnHeap(DecoratedKey partitionKey,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                int rowCountIncludingStatic,
                                Trie<Object> trie,
                                TableMetadata metadata,
                                boolean canHaveShadowedData,
                                EnsureOnHeap ensureOnHeap)
        {
            super(partitionKey, columns, stats, rowCountIncludingStatic, trie, metadata, canHaveShadowedData);
            this.ensureOnHeap = ensureOnHeap;
            this.onHeapDeletion = ensureOnHeap.applyToDeletionInfo(super.deletionInfo());
        }

        @Override
        public Row toRow(RowData data, Clustering clustering)
        {
            return ensureOnHeap.applyToRow(super.toRow(data, clustering));
        }

        @Override
        public DeletionInfo deletionInfo()
        {
            return onHeapDeletion;
        }
    }

    /**
     * Resolver for operations with trie-backed partitions. We don't permit any overwrites/merges.
     */
    public static final InMemoryTrie.UpsertTransformer<Object, Object> NO_CONFLICT_RESOLVER =
            (existing, update) ->
            {
                if (existing != null)
                    throw new AssertionError("Unique rows expected.");
                return update;
            };

    /**
     * Helper class for constructing tries and deletion info from an iterator or flowable partition.
     *
     * Note: This is not collecting any stats or columns!
     */
    public static class ContentBuilder
    {
        final TableMetadata metadata;
        final ClusteringComparator comparator;

        private final MutableDeletionInfo.Builder deletionBuilder;
        private final InMemoryTrie<Object> trie;

        private final boolean useRecursive;
        private final boolean collectDataSize;

        private int rowCountIncludingStatic;
        private long dataSize;

        public ContentBuilder(TableMetadata metadata, DeletionTime partitionLevelDeletion, boolean isReverseOrder, boolean collectDataSize)
        {
            this.metadata = metadata;
            this.comparator = metadata.comparator;

            this.deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion,
                                                               comparator,
                                                               isReverseOrder);
            this.trie = InMemoryTrie.shortLived(BYTE_COMPARABLE_VERSION);

            this.useRecursive = useRecursive(comparator);
            this.collectDataSize = collectDataSize;

            rowCountIncludingStatic = 0;
            dataSize = 0;
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
            putInTrie(comparator, useRecursive, trie, row);
            ++rowCountIncludingStatic;
            if (collectDataSize)
                dataSize += row.dataSize();
            return this;
        }

        public ContentBuilder addRangeTombstoneMarker(RangeTombstoneMarker unfiltered)
        {
            deletionBuilder.add(unfiltered);
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
            MutableDeletionInfo deletionInfo = deletionBuilder.build();
            trie.putRecursive(ByteComparable.EMPTY, deletionInfo, NO_CONFLICT_RESOLVER);    // will throw if called more than once
            // dataSize does not include the deletion info bytes
            return this;
        }

        public Trie<Object> trie()
        {
            return trie;
        }

        public int rowCountIncludingStatic()
        {
            return rowCountIncludingStatic;
        }

        public int dataSize()
        {
            assert collectDataSize;
            return Ints.saturatedCast(dataSize);
        }
    }
}
