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
import java.util.Map;
import java.util.NavigableSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.memory.HeapCloner;

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
 *
 * @typeparam D The actual deletion info type stored in the trie. Typically just DeletionInfo.
 */
public class TrieBackedPartition implements Partition
{
    private final static Logger logger = LoggerFactory.getLogger(TrieBackedPartition.class);

    /**
     * If keys is below this length, we will use a recursive procedure for inserting data in the memtable trie.
     */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    /**
     * The representation of a row stored at the leaf of a trie. Does not contain the row key.
     *
     * The methods toRow and copyToOnHeapRow combine this with a clustering for the represented Row.
     */
    static public class RowData
    {
        final Object[] columnsBTree;
        final LivenessInfo livenessInfo;
        final Row.Deletion deletion;

        RowData(Object[] columnsBTree, LivenessInfo livenessInfo, Row.Deletion deletion)
        {
            this.columnsBTree = columnsBTree;
            this.livenessInfo = livenessInfo;
            this.deletion = deletion;
        }

        Row toRow(Clustering clustering)
        {
            return BTreeRow.create(clustering,
                                   livenessInfo,
                                   deletion,
                                   columnsBTree);
        }

        Row copyToOnHeapRow(Clustering clustering)
        {
            // TODO: maybe avoid the first row object
            return toRow(clustering).clone(HeapCloner.instance);
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
    }

    static private final long EMPTY_ROWDATA_SIZE = ObjectSizes.measure(new RowData(null, null, null));

    protected final Trie<Object> trie;
    protected final DecoratedKey partitionKey;
    protected final TableMetadata metadata;
    protected final RegularAndStaticColumns columns;
    protected final EncodingStats stats;
    protected final boolean canHaveShadowedData;

    public TrieBackedPartition(DecoratedKey partitionKey,
                               RegularAndStaticColumns columns,
                               EncodingStats stats,
                               Trie<Object> trie,
                               TableMetadata metadata,
                               boolean canHaveShadowedData)
    {
        this.partitionKey = partitionKey;
        this.trie = trie;
        this.metadata = metadata;
        this.columns = columns;
        this.stats = stats;
        this.canHaveShadowedData = canHaveShadowedData;
        assert deletionInfo() != null; // There must be always be deletion info metadata
        assert stats != null;
    }

    public static TrieBackedPartition create(UnfilteredRowIterator iterator)
    {
        return new TrieBackedPartition(iterator.partitionKey(),
                                       iterator.metadata().regularAndStaticColumns(),
                                       iterator.stats(),
                                       build(iterator),
                                       iterator.metadata(),
                                       false);
    }

    protected static InMemoryTrie<Object> build(UnfilteredRowIterator iterator)
    {
        try
        {
            ContentBuilder builder = new ContentBuilder(iterator.metadata(), iterator.partitionLevelDeletion(), iterator.isReverseOrder());

            builder.addStatic(iterator.staticRow());

            while (iterator.hasNext())
                builder.addUnfiltered(iterator.next());

            return builder.buildTrie();
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static Iterator<Map.Entry<ByteComparable, RowData>> rowDataIterator(Trie<Object> trie, Direction direction)
    {
        return trie.filteredEntryIterator(direction, RowData.class);
    }

    private Iterator<Row> rowIterator(Trie<Object> trie, Direction direction)
    {
        // TODO: Maybe replace with custom TrieEntriesIterator descendant to avoid some copying and extra objects.
        return Iterators.transform(rowDataIterator(trie, direction),
                                   e -> toRow(e.getValue(), metadata.comparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                                             e.getKey())));
    }

    static RowData rowToData(Row row)
    {
        return new RowData(((BTreeRow) row).getBTree(), row.primaryKeyLivenessInfo(), row.deletion());
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
     * Put the given static row in the trie. Does nothing if the row is empty.
     */
    protected static void putStaticInTrie(ClusteringComparator comparator, InMemoryTrie<Object> trie, Row staticRow) throws TrieSpaceExhaustedException
    {
        if (!staticRow.isEmpty())
            trie.putRecursive(comparator.asByteComparable(Clustering.STATIC_CLUSTERING), rowToData(staticRow), NO_CONFLICT_RESOLVER);
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

    protected boolean canHaveShadowedData()
    {
        return canHaveShadowedData;
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
        // TODO: Not very efficient. Perhaps we could cache the value as this object is immutable.
        // Note: InMemoryTrie.valuesCount() is not good enough here as values will remain in the trie if overwritten
        // with forceCopy, and we may be holding a Memtable tail trie which only has a portion of the values.
        return Iterables.size(nonStaticSubtrie().filteredValues(RowData.class));
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
        RowData staticRow = (RowData) trie.get(path(Clustering.STATIC_CLUSTERING));

        if (staticRow != null)
            return toRow(staticRow, Clustering.STATIC_CLUSTERING);
        else
            return Rows.EMPTY_STATIC_ROW;
    }

    public boolean isEmpty()
    {
        Iterator<Object> it = trie.valueIterator(Direction.FORWARD);
        assert it.hasNext(); // root metadata must be present
        DeletionInfo info = (DeletionInfo) it.next();
        // If empty the trie only contains the root metadata which is a live deletion time.
        return !it.hasNext() && info.isLive();
    }

    public boolean hasRows()
    {
        return rowDataIterator(nonStaticSubtrie(), Direction.FORWARD).hasNext();
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
        return trie.subtrie(path(ClusteringBound.BOTTOM), null);
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
        DeletionTime activeDeletion = deletionInfo.getPartitionDeletion();
        if (rt != null && rt.deletionTime().supersedes(activeDeletion))
            activeDeletion = rt.deletionTime();

        if (data == null)
        {
            return activeDeletion.isLive()
                   ? null
                   : BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion));
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
        // TODO: Use trie slices.
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
                    if (rowData != null && rowData instanceof RowData)
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
        ByteComparable startPath = start != null ? path(start) : path(ClusteringBound.BOTTOM);
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
                                               rowIter, deleteIter, canHaveShadowedData());
    }


    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("[%s] key=%s partition_deletion=%s columns=%s",
                                metadata(),
                                metadata().partitionKeyType.getString(partitionKey().getKey()),
                                partitionLevelDeletion(),
                                columns()));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata(), true));

        try (UnfilteredRowIterator iter = unfilteredIterator())
        {
            while (iter.hasNext())
                sb.append("\n    ").append(iter.next().toString(metadata(), true));
        }

        return sb.toString();
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

        public ContentBuilder(TableMetadata metadata, DeletionTime partitionLevelDeletion, boolean isReverseOrder)
        {
            this.metadata = metadata;
            this.comparator = metadata.comparator;

            this.deletionBuilder = MutableDeletionInfo.builder(partitionLevelDeletion,
                                                               comparator,
                                                               isReverseOrder);
            this.trie = InMemoryTrie.shortLived();

            this.useRecursive = useRecursive(comparator);
        }

        public ContentBuilder addStatic(Row staticRow) throws TrieSpaceExhaustedException
        {
            putStaticInTrie(comparator, trie, staticRow);
            return this;
        }

        public ContentBuilder addRow(Row row) throws TrieSpaceExhaustedException
        {
            putInTrie(comparator, useRecursive, trie, row);
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

        public InMemoryTrie<Object> buildTrie() throws TrieSpaceExhaustedException
        {
            trie.putRecursive(ByteComparable.EMPTY, deletionBuilder.build(), NO_CONFLICT_RESOLVER);
            return trie;
        }
    }
}
