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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import org.agrona.collections.Object2IntHashMap;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MultiCellCapableType;
import org.apache.cassandra.db.memtable.TrieCellData;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieSet;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.BYTE_COMPARABLE_VERSION;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.mergeTombstoneRanges;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noConflictInData;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noExistingSelfDeletion;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noIncomingSelfDeletion;

/// Immutable implementation of a [Row] object, where the data structure is represented by a trie.
/// Stores either [Cell], or [TrieCellData] (if the row comes from a memtable trie).
///
/// Trie-backed rows using a mapping of column ids to numbers that is to be obtained from the table metadata. Currently,
/// we don't support mixing different column sets in trie rows, thus all rows in use in one object (partition, memtable)
/// must share the same column set.
public class TrieBackedRow extends AbstractRow
{
    public static final Object COMPLEX_COLUMN_MARKER = new Object()
    {
        @Override
        public String toString()
        {
            return "COMPLEX_COLUMN_MARKER";
        }
    };

    static final DeletionAwareTrie<Object, TrieTombstoneMarker> EMPTY_ROW = DeletionAwareTrie.singleton(ByteComparable.EMPTY,
                                                                                                        BYTE_COMPARABLE_VERSION,
                                                                                                        LivenessInfo.EMPTY);
    static final Object2IntHashMap<ColumnIdentifier> EMPTY_COLUMN_IDS = makeColumnIdsMap(Columns.NONE);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, Clustering.EMPTY, EMPTY_ROW));

    private static final int COLUMN_NOT_PRESENT = -1;

    // A column with this ID cannot exist. Used to make sure we don't find anything when we are passed an unknown column definition.
    public static final ByteComparable MISSING_COLUMN_KEY = encodeUnsignedInt(Long.MAX_VALUE);

    public static final int MAX_RECURSIVE_LENGTH = 128;

    /// The row's clustering key.
    private final Clustering<?> clustering;

    /// Mapping from column id to its numeric index, used to reach the column in the trie.
    private final Object2IntHashMap<ColumnIdentifier> columnIds;
    /// List of columns in this row, also used a map from column index to column id/definition.
    private final Columns columns;

    /// Pre-computed column maps for each `Columns` instance.
    private static final Map<Columns, Object2IntHashMap<ColumnIdentifier>> columnsMapCache = new ConcurrentHashMap<>();

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Integer.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Integer.MAX_VALUE;
    private int minLocalDeletionTime;
    boolean minLocalDeletionTimeSet = false;

    ///  Data trie contains:
    ///  - LivenessInfo header at the root
    ///  - A Cell for each (simple or complex) cell of the row
    ///    - Cells may be expiring or even expired (not really expected for memtables but possible)
    ///  - Deletion branch with tombstones
    private final DeletionAwareTrie<Object, TrieTombstoneMarker> data;

    /// Create a trie-backed version of a row, using the column definitions of the given table metadata.
    public static TrieBackedRow from(TableMetadata metadata, Row row)
    {
        Builder builder = builder(metadata, row.clustering());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        builder.addRowDeletion(row.deletion());
        for (ColumnData cd : row)
        {
            if (cd.column.isSimple())
                builder.addCell((Cell<?>) cd);
            else
            {
                var ccd = (ComplexColumnData) cd;
                builder.addComplexDeletion(ccd.column, ccd.complexDeletion());
                for (Cell<?> cell : ccd)
                    builder.addCell(cell);
            }
        }
        return builder.build();
    }

    /// Returns false if the given object is a level marker with no meaning of its own. Used to drop unproductive
    /// markers that can remain after deletions.
    public static boolean shouldPreserveContentWithoutChildren(Object o)
    {
        if (o == LivenessInfo.EMPTY || o == COMPLEX_COLUMN_MARKER || o == TrieTombstoneMarker.LevelMarker.ROW)
            return false;
        if (!(o instanceof TrieTombstoneMarker))
            return true;
        TrieTombstoneMarker m = (TrieTombstoneMarker) o;
        if (!m.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
            return true;
        return !Objects.equals(m.leftDeletion(), m.rightDeletion());
    }

    public static TrieBackedRow create(TableMetadata tableMetadata, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return new TrieBackedRow(tableMetadata, clustering, data);
    }

    TrieBackedRow(TableMetadata tableMetadata, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this(tableMetadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING), clustering, data);
    }

    TrieBackedRow(Columns columns, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this(columns, columnsMapCache.computeIfAbsent(columns, TrieBackedRow::makeColumnIdsMap), clustering, data);
    }

    private TrieBackedRow(Columns columns,
                          Object2IntHashMap<ColumnIdentifier> columnIds,
                          Clustering<?> clustering,
                          DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        assert data != null;
        this.columns = columns;
        this.columnIds = columnIds;
        this.clustering = clustering;
        this.data = data;
    }

    private static Object2IntHashMap<ColumnIdentifier> makeColumnIdsMap(Columns columns)
    {
        Object2IntHashMap<ColumnIdentifier> columnIds = new Object2IntHashMap<>(COLUMN_NOT_PRESENT);
        for (int i = 0; i < columns.size(); i++)
            columnIds.put(columns.getSimple(i).name, i);
        return columnIds;
    }

    private static RangeTrie<TrieTombstoneMarker> rowDeletionTrie(DeletionTime deletion)
    {
        return deletionTrie(ByteComparable.EMPTY, deletion, TrieTombstoneMarker.Kind.ROW);
    }

    private static RangeTrie<TrieTombstoneMarker> deletionTrie(ByteComparable prefix, DeletionTime deletion, TrieTombstoneMarker.Kind kind)
    {
        return withDeletionRoot(RangeTrie.branch(prefix,
                                                 BYTE_COMPARABLE_VERSION,
                                                 TrieTombstoneMarker.covering(deletion, kind)));
    }

    public static TrieBackedRow emptyDeletedRow(Clustering<?> clustering, DeletionTime deletion)
    {
        assert !deletion.isLive();
        RangeTrie<TrieTombstoneMarker> deletionTrie = rowDeletionTrie(deletion);
        return new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, clustering,
                                 DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, BYTE_COMPARABLE_VERSION, deletionTrie));
    }

    /// Adjust the given deletion trie to include a row-level marker.
    private static RangeTrie<TrieTombstoneMarker> withDeletionRoot(RangeTrie<TrieTombstoneMarker> trie)
    {
        // Range tries present separate content in the two directions. We need to add a marker in both.
        return trie.mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              true,
                                              TrieTombstoneMarker.LevelMarker.ROW),
                              TrieTombstoneMarker::mergeUpdate)
                   .mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              false,
                                              TrieTombstoneMarker.LevelMarker.ROW),
                              TrieTombstoneMarker::mergeUpdate);
    }

    @Override
    public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue)
    {
        // TODO: this isn't efficient at all
        long v = initialValue;
        for (ColumnData c : this)
            v = accumulator.apply(c, v);
        return v;
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue)
    {
        // TODO: this isn't efficient at all
        long v = initialValue;
        for (ColumnData c : this)
            v = accumulator.apply(arg, c, v);
        return v;
    }

    private static class Accumulator implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
    {
        final LongAccumulator<CellData<?, ?>> cellAccumulator;
        final LongAccumulator<LivenessInfo> livenessAccumulator;
        final LongAccumulator<DeletionTime> markerAccumulator;
        long value;

        Accumulator(long initialValue,
                    LongAccumulator<CellData<?, ?>> cellAccumulator,
                    LongAccumulator<LivenessInfo> livenessAccumulator,
                    LongAccumulator<DeletionTime> markerAccumulator)
        {
            this.cellAccumulator = cellAccumulator;
            this.livenessAccumulator = livenessAccumulator;
            this.markerAccumulator = markerAccumulator;
            this.value = initialValue;
        }

        @Override
        public void content(Object content)
        {
            if (content instanceof LivenessInfo)
                value = livenessAccumulator.apply((LivenessInfo) content, value);
            else if (content instanceof CellData)
                value = cellAccumulator.apply((CellData<?, ?>) content, value);
            else if (content != COMPLEX_COLUMN_MARKER)
                throw new AssertionError("Unexpected content type: " + content);
        }

        @Override
        public void deletionMarker(TrieTombstoneMarker marker)
        {
            if (marker.isBoundary())
            {
                // We only apply the function to one side of the marker; the other has to be already seen as a
                // succeeding side of a different marker.
                TrieTombstoneMarker.Covering applicableState = marker.applicableToPointForward();
                if (applicableState != null)
                    value = markerAccumulator.apply(applicableState, value);
            }
        }
    }

    /// Accumulate a long value, using the given cell-level functions.
    ///
    /// Note: For efficiency, the cell accumulator is given cells without path. If the path is needed, use a different
    /// `accumulate` method.
    long accumulate(long initialValue,
                    LongAccumulator<LivenessInfo> livenessAccumulator,
                    LongAccumulator<CellData<?, ?>> cellAccumulator,
                    LongAccumulator<DeletionTime> markerAccumulator)
    {
        Accumulator accumulator = new Accumulator(initialValue, cellAccumulator, livenessAccumulator, markerAccumulator);
        data.process(Direction.FORWARD, accumulator);
        return accumulator.value;
    }

    @Override
    public long maxTimestamp()
    {
        return accumulate(Long.MIN_VALUE,
                          (livenessInfo, maxTimestamp) -> Math.max(maxTimestamp, livenessInfo.timestamp()),
                          (cell, maxTimestamp) -> Math.max(maxTimestamp, cell.timestamp()),
                          (marker, maxTimestamp) -> Math.max(maxTimestamp, marker.markedForDeleteAt()));
    }

    @Override
    public long minTimestamp()
    {
        return accumulate(Long.MAX_VALUE,
                          (livenessInfo, minTimestamp) -> Math.min(minTimestamp, livenessInfo.timestamp()),
                          (cell, minTimestamp) -> Math.min(minTimestamp, cell.timestamp()),
                          (marker, minTimestamp) -> Math.min(minTimestamp, marker.markedForDeleteAt()));
    }

    @Override
    public Clustering<?> clustering()
    {
        return clustering;
    }

    @Override
    public LivenessInfo primaryKeyLivenessInfo()
    {
        LivenessInfo info = (LivenessInfo) data.get(ByteComparable.EMPTY);
        return info != null ? info : LivenessInfo.EMPTY;
    }

    @Override
    public boolean isEmpty()
    {
        // Empty has no live or deletion branch but may have an empty row marker.
        return isEmpty(data);
    }

    public static boolean isEmpty(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        if (data == null)
            return true;

        if (!isEmptyAfterDeletion(data))
            return false;

        if (data instanceof InMemoryDeletionAwareTrie)
        {
            // the row deletion marker will only be present if there is a deletion present
            return data.applicableDeletion(ByteComparable.EMPTY) == null;
        }
        else
        {
            // The row deletion marker may remain even if the data is deleted/filtered out.
            // Check for the existence of a deletion boundary
            return !data.deletionOnlyTrie().filteredValuesIterator(Direction.FORWARD, TrieTombstoneMarker.Boundary.class).hasNext();
        }
    }

    @Override
    public boolean isEmptyAfterDeletion()
    {
        return isEmptyAfterDeletion(data);
    }

    public static boolean isEmptyAfterDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        if (data instanceof InMemoryDeletionAwareTrie)
        {
            // the liveness marker will be dropped if there are no cells
            return data.get(ByteComparable.EMPTY) == null;
        }
        else
        {
            // The liveness marker may remain even if the data is deleted/filtered out.
            // Check for the existence of:
            // - non-empty liveness
            LivenessInfo info = (LivenessInfo) data.get(ByteComparable.EMPTY);
            if (info != null && info != LivenessInfo.EMPTY)
                return false;

            // - a cell
            return !data.contentOnlyTrie().filteredValuesIterator(Direction.FORWARD, CellData.class).hasNext();
        }
    }

    @Override
    public Deletion deletion()
    {
        DeletionTime delTime = TrieTombstoneMarker.applicableDeletion(data, ByteComparable.EMPTY);
        if (delTime == null)
            return Deletion.LIVE;
        else
            return Deletion.regular(delTime);
    }

    public static ByteSource columnKey(Columns columns, ColumnMetadata column)
    {
        return ByteSource.variableLengthUnsignedInteger(columns.simpleIdx(column));
    }

    public static ByteSource cellPathKey(ColumnMetadata column, CellPath path, ByteComparable.Version version)
    {
        return ByteSource.withTerminator(ByteSource.TERMINATOR,
                                         getCellPathType(column).asComparableBytes(path.get(0), version));
    }

    /// Return a cell key (i.e. path in the trie) for the given column and cell path.
    private static ByteComparable cellKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column, CellPath path)
    {
        int id = columnIds.getValue(column.name);
        if (id == COLUMN_NOT_PRESENT)
            return MISSING_COLUMN_KEY; // SAI can call cellKey for static columns on regular rows and vice versa
        if (!column.isComplex())
            return encodeUnsignedInt(id);
        else
            return cellKey(id, column, path);
    }

    private static ByteSource columnIdPrefix(int columnId)
    {
        if (columnId < 0)
            return ByteSource.EMPTY;
        else
            return ByteSource.variableLengthUnsignedInteger(columnId);
    }

    /// Return a cell key (i.e. path in the trie) for the given column index and cell path.
    ///
    /// Note: this method is also used by [TrieBackedComplexColumn] where the column index is in the path leading to the
    /// complex column trie. To support this, a `columnId` of -1 is used to skip the column index.
    static ByteComparable cellKey(int columnId, ColumnMetadata column, CellPath path)
    {
        if (path == CellPath.BOTTOM)
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          ByteSource.oneByte(ByteSource.LT_NEXT_COMPONENT));
        else if (path == CellPath.TOP)
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          ByteSource.oneByte(ByteSource.GT_NEXT_COMPONENT));
        else
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          cellPathKey(column, path, v));
        // TODO: figure out a better way to do path slices and remove the leading path byte as
//        return v -> ByteSource.concat(columnIdPrefix(columnId),
//                                      ((MultiCellCapableType<Object>)column.type).nameComparator().asComparableBytes(path.get(0), v),
//                                      ByteSource.oneByte(ByteSource.TERMINATOR));
    }

    private static AbstractType<?> getCellPathType(ColumnMetadata column)
    {
        assert column.isComplex();
        return ((MultiCellCapableType<Object>) column.type).nameComparator();
    }

    /// Returns the column key, i.e. the column index path in the trie without the part corresponding to the cell path.
    private static ByteComparable columnKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column)
    {
        int id = columnIds.getValue(column.name);
        assert id != COLUMN_NOT_PRESENT;
        return encodeUnsignedInt(id);
    }

    /// Convert the cell-path part of the trie path into a cell path to use in a [Cell].
    static CellPath cellPath(ColumnMetadata column, ByteSource.Peekable src)
    {
        int next = src.next();
        if (next == ByteSource.LT_NEXT_COMPONENT)
            return CellPath.BOTTOM;
        else if (next == ByteSource.GT_NEXT_COMPONENT)
            return CellPath.TOP;

        ByteSource.Peekable componentSource = ByteSourceInverse.nextComponentSource(src, next);
        ByteBuffer path = getCellPathType(column).fromComparableBytes(componentSource, BYTE_COMPARABLE_VERSION);
        return CellPath.create(path);
        // TODO: figure out a better way to do path slices and remove the leading path byte as
//        return CellPath.create(getCellPathType(column).fromComparableBytes(src, BYTE_COMPARABLE_VERSION));
    }

    @Override
    public Cell<?> getCell(ColumnMetadata c)
    {
        assert !c.isComplex();
        return getCellInternal(c, null);
    }

    @Override
    public Cell<?> getCell(ColumnMetadata c, CellPath path)
    {
        assert c.isComplex();
        return getCellInternal(c, path);
    }

    private Cell<?> getCellInternal(ColumnMetadata c, CellPath path)
    {
        Object o = data.get(cellKey(columnIds, c, path));
        if (o == null || o instanceof Cell)
            return (Cell<?>) o;
        CellData<?, ?> cellData = (CellData<?, ?>) o;
        return cellData.toCell(c, path);
    }

    @Override
    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        assert c.isComplex();
        DeletionAwareTrie<Object, TrieTombstoneMarker> tail = data.tailTrie(columnKey(columnIds, c));
        if (!isColumnDataTrieEmpty(tail))
            return new TrieBackedComplexColumn(c, tail);
        else
            return null;
    }

    private static boolean isColumnDataTrieEmpty(DeletionAwareTrie<Object, TrieTombstoneMarker> tail)
    {
        if (tail == null)
            return true;
        // We may be left with only a COMPLEX_COLUMN_MARKER after some transformation.
        if (tail instanceof InMemoryDeletionAwareTrie)
            return false; // in-memory trie will drop the marker
        if (TrieTombstoneMarker.applicableDeletion(tail, ByteComparable.EMPTY) != null)
            return false;
        // otherwise it's empty if it has no cells
        return !tail.filteredValuesIterator(Direction.FORWARD, CellData.class).hasNext();
    }

    @Override
    public ColumnData getColumnData(ColumnMetadata c)
    {
        return c.isComplex() ? getComplexColumnData(c) : getCell(c);
    }

    @Override
    public Collection<ColumnMetadata> columns()
    {
        return new AbstractCollection<ColumnMetadata>()
        {
            @Override public Iterator<ColumnMetadata> iterator()
            {
                return Iterators.transform(TrieBackedRow.this.iterator(), ColumnData::column);
            }
            @Override public int size()
            {
                return columnCount();
            }
        };
    }

    /// Combine data in the live and deletion branches to identify column roots.
    private static Object combineDataAndDeletionForColumnIterator(Object content, TrieTombstoneMarker marker, Direction direction)
    {
        if (content instanceof CellData)
            return content;
        if (content == COMPLEX_COLUMN_MARKER)
            return content;
        if (content instanceof LivenessInfo)
            return null; // skip row marker
        // skip any deletion that may be covering the row
        TrieTombstoneMarker.Covering introducedDeletion = marker.succedingState(direction);
        if (introducedDeletion == null || introducedDeletion.deletionKind() != TrieTombstoneMarker.Kind.COLUMN)
            return null;
        // This is a complex column deletion marker. Return it, which will also result in skipping the return path
        // marker.
        return marker;
    }

    private static Object combineDataAndDeletionForColumnIteratorForward(Object content, TrieTombstoneMarker marker)
    {
        return combineDataAndDeletionForColumnIterator(content, marker, Direction.FORWARD);
    }

    private static Object combineDataAndDeletionForColumnIteratorReverse(Object content, TrieTombstoneMarker marker)
    {
        return combineDataAndDeletionForColumnIterator(content, marker, Direction.REVERSE);
    }

    static class ColumnDataIterator extends TrieTailsIterator.DeletionAware<Object, TrieTombstoneMarker, Object, ColumnData>
    {
        private final Columns columns;

        ColumnDataIterator(Columns columns, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
        {
            super(trie,
                  direction,
                  direction.select(TrieBackedRow::combineDataAndDeletionForColumnIteratorForward,
                                   TrieBackedRow::combineDataAndDeletionForColumnIteratorReverse),
                  false);
            this.columns = columns;
        }

        @Override
        protected ColumnData mapContent(Object value, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            // value is given by combineDataAndDeletionForColumnIterator above
            if (value instanceof CellData)
                return cellFromCellData((CellData<?, ?>) value, bytes, byteLength, columns);

            return new TrieBackedComplexColumn(columnMetadataFromPath(bytes, byteLength, columns), tailTrie);
        }

    }

    public static ColumnMetadata columnMetadataFromPath(byte[] bytes, int byteLength, Columns columns)
    {
        long columnIndex = ByteSourceInverse.getVariableLengthUnsignedInteger(ByteSource.preencoded(bytes, 0, byteLength));
        assert ((int) columnIndex) == columnIndex;
        return columns.getSimple((int) columnIndex);
    }

    public static Cell<?> cellFromCellData(CellData<?, ?> value, byte[] bytes, int byteLength, Columns columns)
    {
        if (value instanceof Cell)
            return (Cell<?>) value;
        ByteSource.Peekable pathBytes = ByteSource.preencoded(bytes, 0, byteLength);
        long columnIdx = ByteSourceInverse.getVariableLengthUnsignedInteger(pathBytes);
        ColumnMetadata column = columns.getSimple((int) columnIdx);
        return value.toCell(column, column.isComplex() ? cellPath(column, pathBytes) : null);
    }

    @Override
    public int columnCount()
    {
        return Iterators.size(new ColumnDataIterator(columns, data, Direction.FORWARD));
    }

    @Override
    public Iterator<ColumnData> iterator()
    {
        return new ColumnDataIterator(columns, data, Direction.FORWARD);
    }

    @Override
    public Iterable<Cell<?>> cells()
    {
        return () -> new CellsWithPath(data.contentOnlyTrie(), Direction.FORWARD);
    }

    @Override
    public Row filter(ColumnFilter filter, TableMetadata metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    @Override
    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata)
    {
        Map<ByteBuffer, DroppedColumn> droppedColumns = metadata.droppedColumns;

        boolean mayFilterColumns = !filter.fetchesAllColumns(isStatic()) || !filter.allFetchedColumnsAreQueried();
        // When merging sstable data in Row.Merger#merge(), rowDeletion is removed if it doesn't supersede activeDeletion.
        boolean mayHaveDeleted = !activeDeletion.isLive();
        DeletionAwareTrie<Object, TrieTombstoneMarker> filteredData = data;
        if (!mayFilterColumns && !mayHaveDeleted && droppedColumns.isEmpty())
            return this;

        // Metadata changes are not something we can handle.
        if (!columns.equals(metadata.regularAndStaticColumns().columns(isStatic())))
            throw new IllegalArgumentException("Metadata columns do not match");

        if (!droppedColumns.isEmpty())
        {
            // Filter dropped columns by adding a deletion with the drop time, so that data before the drop time is not
            // returned.
            List<RangeTrie<TrieTombstoneMarker>> drops = new ArrayList<>();
            for (ColumnMetadata c : columns)
            {
                DroppedColumn dropped = droppedColumns.get(c.name.bytes);
                if (dropped != null)
                {
                    drops.add(RangeTrie.branch(columnKey(columnIds, c),
                                               BYTE_COMPARABLE_VERSION,
                                               TrieTombstoneMarker.covering(dropped.droppedTime, Integer.MIN_VALUE, TrieTombstoneMarker.Kind.COLUMN)));
                }
            }
            if (!drops.isEmpty())
                filteredData = filteredData.mappingMergeWithDeletion(RangeTrie.merge(drops, TrieTombstoneMarker::merge),
                                                                     TrieBackedRow::deleteData,
                                                                     TrieTombstoneMarker::dropShadowedUpdate,
                                                                     true);
        }

        if (mayFilterColumns)
        {
            // TODO: Column filter may include cell-level filters for complex columns, in both fetched and queried
            Columns queried = filter.queriedColumns().columns(isStatic());
            BitSet queriedIds = getColumnIds(queried);

            // Getting queried and fetchedButNotQueried separately and merging looks more efficient, but in general
            // most of the time we'll either have fetched getting all columns or fetched equal to queried.
            // Filtering by fetched first avoids one operation in the former case.
            Columns fetched = filter.fetchedColumns().columns(isStatic());
            BitSet fetchedIds = getColumnIds(fetched);
            if (fetchedIds.cardinality() != columns.size())
                filteredData = restrictToColumnSet(filteredData, fetchedIds);

            BitSet fetchedButNotQueried = fetchedIds;
            fetchedButNotQueried.andNot(queriedIds);

            if (!fetchedButNotQueried.isEmpty())
            {
                DeletionAwareTrie<Object, TrieTombstoneMarker> fetchedButNotQueriedData =
                filteredData.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, true, true, mapIdsToColumnKeys(fetchedButNotQueried)))
                            .mapValues(TrieBackedRow::dropCellValue);

                filteredData = filteredData.mergeWith(fetchedButNotQueriedData,
                                                      (x, y) -> y, // fetchedButNotQueried overrides data cells
                                                      TrieTombstoneMarker::mergeUpdate,
                                                      noExistingSelfDeletion(),
                                                      true);
            }
        }

        if (mayHaveDeleted)
        {
            // Apply the given active deletion. If asked to set, add it as a deletion. If not, only drop content that
            // it deletes.
            if (setActiveDeletionToRow)
                filteredData = filteredData.mergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                                               BYTE_COMPARABLE_VERSION,
                                                                               TrieTombstoneMarker.covering(activeDeletion, TrieTombstoneMarker.Kind.ROW)),
                                                              TrieBackedRow::deleteData,
                                                              TrieTombstoneMarker::mergeUpdate,
                                                              true);
            else // we need mappingMerge to make sure that the resolver is called for all update markers so that we can drop them
                filteredData = filteredData.mappingMergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                                                      BYTE_COMPARABLE_VERSION,
                                                                                      TrieTombstoneMarker.covering(activeDeletion, TrieTombstoneMarker.Kind.ROW)),
                                                                     TrieBackedRow::deleteData,
                                                                     TrieTombstoneMarker::dropShadowedUpdate,
                                                                     true);
        }

        // TODO: Should we use `fetched` for `columns`? Note the ids cannot change.

        if (isEmpty(filteredData))
            return null;

        return new TrieBackedRow(columns, columnIds, clustering, filteredData);
    }

    private static DeletionAwareTrie<Object, TrieTombstoneMarker> restrictToColumnSet(DeletionAwareTrie<Object, TrieTombstoneMarker> data, BitSet fetchedIds)
    {
        // Because we do not support column-level deletions for simple columns, we need to keep the row-level deletion
        // at the root. The intersection below moves it down to the cell level.
        TrieTombstoneMarker.Covering rowDeletion = TrieTombstoneMarker.applicableDeletion(data, ByteComparable.EMPTY);

        // Restrict to the fetched columns with the liveness info.
        if (!fetchedIds.isEmpty())
            data = data.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, true, true, mapIdsToColumnKeys(fetchedIds)));
        else
            data = DeletionAwareTrie.singleton(ByteComparable.EMPTY, BYTE_COMPARABLE_VERSION, data.get(ByteComparable.EMPTY));

        // Re-add the row deletion and the ascent-side Level.ROW marker, which we lose in the intersection above.
        if (rowDeletion != null)
            data = data.mergeWithDeletion(rowDeletionTrie(rowDeletion),
                                                          (x, y) -> y, // Row deletion is already applied
                                                          TrieTombstoneMarker::mergeUpdate,
                                                          true);
        return data;
    }

    private static Object deleteData(Object existing, TrieTombstoneMarker marker)
    {
        return deleteData(marker, existing);
    }

    private static Object deleteData(TrieTombstoneMarker marker, Object existing)
    {
        DeletionTime deletion = marker.applicableToPointForward();
        if (deletion == null)
            return existing;
        if (existing == COMPLEX_COLUMN_MARKER)
            return existing;
        if (existing instanceof LivenessInfo)
        {
            if (deletion.deletes(((LivenessInfo) existing).timestamp()))
                return LivenessInfo.EMPTY;
            else
                return existing;
        }
        if (existing instanceof CellData)
        {
            if (deletion.deletes((CellData<?, ?>) existing))
                return null;
            else
                return existing;
        }
        throw new AssertionError("Unknown content type: " + existing);
    }

    private static Object dropCellValue(Object existing)
    {
        if (!(existing instanceof CellData))
            return existing;
        return ((CellData<?, ?>) existing).withSkippedValue();
    }

    private static ByteComparable[] mapIdsToColumnKeys(BitSet fetchedIds)
    {
        ByteComparable[] keys = new ByteComparable[fetchedIds.cardinality() * 2];
        int keyPos = 0;
        for (int i = fetchedIds.nextSetBit(0); i >= 0; i = fetchedIds.nextSetBit(i + 1))
        {
            final int id = i;
            ByteComparable columnKey = encodeUnsignedInt(id);
            keys[keyPos++] = columnKey; // add twice for inclusive start and end
            keys[keyPos++] = columnKey;
        }
        assert keyPos == keys.length;
        return keys;
    }

    private static ByteComparable encodeUnsignedInt(long id)
    {
        return v -> ByteSource.variableLengthUnsignedInteger(id);
    }

    private BitSet getColumnIds(Columns fetched)
    {
        BitSet fetchedIds = new BitSet();
        for (ColumnMetadata c : fetched)
        {
            int idx = columnIds.getValue(c.name);
            if (idx == COLUMN_NOT_PRESENT)
                continue;
            fetchedIds.set(idx);
        }
        return fetchedIds;
    }

    @Override
    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return this;

        // TODO: Column filter may include cell-level filters for complex columns
        Columns queried = filter.queriedColumns().columns(isStatic());
        BitSet queriedIds = getColumnIds(queried);
        if (queriedIds.cardinality() != columns.size())
            return new TrieBackedRow(columns, columnIds, clustering, restrictToColumnSet(data, queriedIds));
        else
            return this;
    }

    @Override
    public boolean hasComplexDeletion()
    {
        for (Map.Entry<ByteComparable.Preencoded, TrieTombstoneMarker> entry : data.deletionOnlyTrie().entrySet())
        {
            if (entry.getKey().getPreencodedBytes().peek() != ByteSource.END_OF_STREAM)
                return true; // entry below the root level exists, this must be a complex column deletion
        }
        return false;
    }

    @Override
    public Row markCounterLocalToBeCleared()
    {
        return transformAndFilter(x -> x, CellData::markCounterLocalToBeCleared);
    }

    @Override
    public boolean hasDeletion(long nowInSec)
    {
        return nowInSec >= getMinLocalDeletionTime();
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        return accumulate(0,
                          (liveness, v) -> (liveness.isExpiring() && (liveness.ttl() < 0 || liveness.localExpirationTime() < 0)) ? 1 : v,
                          (cell, v) -> cell.hasInvalidDeletions() ? 1 : v,
                          (marker, v) -> !marker.validate() ? 1 : v)
               != 0;
    }

    /// Return the backing trie for merging into a partition or memtable.
    public DeletionAwareTrie<Object, TrieTombstoneMarker> trie()
    {
        return data;
    }

    @Override
    public Row updateAllTimestamp(long newTimestamp)
    {
        return transformAndFilter(liveness -> liveness.withUpdatedTimestamp(newTimestamp),
                                  cell -> cell.updateAllTimestamp(newTimestamp),
                                  dt -> dt.isLive() ? dt : DeletionTime.build(newTimestamp - 1, dt.localDeletionTime()));
    }

    @Override
    public Row withRowDeletion(DeletionTime newDeletion)
    {
        // Applies the deletion to the branch, removing any shadowed data (caller should ensure there isn't any, but
        // we do this properly for safety).
        if (newDeletion.isLive())
            return this;

        return new TrieBackedRow(columns, columnIds, clustering,
                                 data.mergeWithDeletion(rowDeletionTrie(newDeletion),
                                                        TrieBackedRow::deleteData,
                                                        TrieTombstoneMarker::mergeUpdate,
                                                        true));
    }

    @Override
    public Row purge(DeletionPurger purger, long nowInSec, boolean enforceStrictLiveness)
    {
        // TODO: evaluate need/performance effect
        if (!hasDeletion(nowInSec))
            return this;

        if (enforceStrictLiveness)
        {
            // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
            LivenessInfo primaryLiveness = primaryKeyLivenessInfo();
            primaryLiveness = purger.shouldPurge(primaryLiveness, nowInSec) ? LivenessInfo.EMPTY : primaryLiveness;
            DeletionTime rowDeletion = TrieTombstoneMarker.applicableDeletion(data, ByteComparable.EMPTY);
            rowDeletion = rowDeletion != null && !purger.shouldPurge(rowDeletion) ? rowDeletion : null;
            if (primaryLiveness.isEmpty() && rowDeletion == null)
                return null;
        }

        return transformAndFilter(primaryKeyLivenessInfo -> purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo,
                                  cell -> cell.purge(purger, nowInSec),
                                  deletion -> purger.shouldPurge(deletion) ? null : deletion);
    }

    public Row purgeDataOlderThan(long timestamp, boolean enforceStrictLiveness)
    {
        if (enforceStrictLiveness)
        {
            // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
            DeletionTime rowDeletion = TrieTombstoneMarker.applicableDeletion(data, ByteComparable.EMPTY);
            if (primaryKeyLivenessInfo().timestamp() < timestamp && rowDeletion == null || rowDeletion.markedForDeleteAt() < timestamp)
                return null;
        }

        return transformAndFilter(primaryKeyLivenessInfo -> primaryKeyLivenessInfo.timestamp() < timestamp ? LivenessInfo.EMPTY : primaryKeyLivenessInfo,
                                  c -> c.purgeDataOlderThan(timestamp),
                                  deletion -> deletion.markedForDeleteAt() < timestamp ? null : deletion);
    }


    @Override
    public Row transformAndFilter(Function<LivenessInfo, LivenessInfo> livenessInfoFunction,
                                  CellTransformer cellFunction)
    {
        return new TrieBackedRow(columns, columnIds, clustering, data.mapValues(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return livenessInfoFunction.apply((LivenessInfo) x);
                }
                else if (x instanceof CellData)
                {
                    //noinspection rawtypes
                    return cellFunction.apply((CellData) x);
                }
                else
                    return x;   // complex column marker
            }));
    }

    Row transformAndFilter(Function<LivenessInfo, LivenessInfo> livenessInfoFunction,
                           Function<CellData<?, ?>, CellData<?, ?>> cellFunction,
                           Function<DeletionTime, DeletionTime> markerFunction)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return (livenessInfoFunction.apply((LivenessInfo) x));
                }
                else if (x instanceof CellData)
                {
                    return cellFunction.apply((CellData<?, ?>) x);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(markerFunction));
        if (isEmpty(mappedData))
            return null;

        return new TrieBackedRow(columns, columnIds, clustering, mappedData);
    }

    @Override
    public Row clone(Cloner cloner)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie = newTrie();
        try
        {
            newTrie.mutator(((ex, toClone) -> toClone instanceof CellData ? ((CellData<?, ?>) toClone).clone(cloner) : toClone),
                            mergeTombstoneRanges(),
                            noIncomingSelfDeletion(),
                            noExistingSelfDeletion(),
                            true,
                            Predicates.alwaysFalse())
            .apply(data);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TrieBackedRow(columns, columnIds, cloner.clone(clustering), newTrie);
    }

    // TODO: Redo size collection to be more direct.

    @Override
    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                     + primaryKeyLivenessInfo().dataSize()
                     + deletion().dataSize();

        return Ints.checkedCast(accumulate((cd, v) -> v + cd.dataSize(), dataSize));
    }

    @Override
    public int liveDataSize(long nowInSec)
    {
        int dataSize = clustering.dataSize()
                       + primaryKeyLivenessInfo().dataSize()
                       + deletion().dataSize();

        return Ints.checkedCast(accumulate((cd, v) -> v + cd.liveDataSize(nowInSec), dataSize));
    }

    @Override
    public long unsharedHeapSize()
    {
        long heapSize = EMPTY_SIZE + clustering.unsharedHeapSizeExcludingData();
        if (data instanceof InMemoryDeletionAwareTrie)
            heapSize += ((InMemoryDeletionAwareTrie) data).usedSizeOnHeap();

        return accumulate(heapSize,
                          (liveness, v) -> v + liveness.unsharedHeapSize(),
                          (cell, v) -> v + cell.unsharedHeapSize(),
                          (marker, v) -> v + marker.unsharedHeapSize());
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE + clustering.unsharedHeapSizeExcludingData();
        if (data instanceof InMemoryDeletionAwareTrie)
            heapSize += ((InMemoryDeletionAwareTrie<?, ?>) data).usedSizeOnHeap();

        return accumulate(heapSize,
                          (liveness, v) -> v + liveness.unsharedHeapSize(),
                          (cell, v) -> v + cell.unsharedHeapSizeExcludingData(),
                          (marker, v) -> v + marker.unsharedHeapSize());
    }

    @Override
    public void apply(Consumer<ColumnData> function)
    {
        for (ColumnData cd : this)
            function.accept(cd);
    }

    @Override
    public <A> void apply(BiConsumer<A, ColumnData> function, A arg)
    {
        for (ColumnData cd : this)
            function.accept(arg, cd);
    }

    private static Builder builder(TableMetadata metadata, Clustering<?> clustering)
    {
        Builder builder = new Builder(metadata.regularAndStaticColumns());
        builder.newRow(clustering);
        return builder;
    }

    public static Row.Builder builder(RegularAndStaticColumns regularAndStaticColumns)
    {
        return new Builder(regularAndStaticColumns);
    }

    private static Object mergeData(Object existing, Object update)
    {
        if (update instanceof LivenessInfo)
            return LivenessInfo.merge((LivenessInfo) existing, (LivenessInfo) update);
        else if (update instanceof CellData)
        {
            CellData<?, ?> existingCell = (CellData<?, ?>) existing;
            CellData<?, ?> updateCell = (CellData<?, ?>) update;
            //noinspection unchecked
            return Cells.<CellData>reconcile(existingCell, updateCell);
        }
        else
        {
            assert existing == COMPLEX_COLUMN_MARKER;
            return existing;
        }
    }

    public static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie()
    {
        return InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION, TrieBackedRow::shouldPreserveContentWithoutChildren);
    }

    public Row mergeWith(Row updateAsRow)
    {
        if (!(updateAsRow instanceof TrieBackedRow))
            throw new IllegalArgumentException("Merging different row types.");
        TrieBackedRow update = (TrieBackedRow) updateAsRow;
        if (!this.columns.containsAll(update.columns))
            throw new IllegalArgumentException("Can't handle varying column lists.");

        try
        {
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> mergedData = newTrie();
            makeMutator(mergedData)
                .apply(this.data.mergeWith(update.data,
                                           TrieBackedRow::mergeData,
                                           TrieTombstoneMarker::mergeUpdate,
                                           TrieBackedRow::deleteData,
                                           true
                ));
            return new TrieBackedRow(this.columns,
                                     this.columnIds,
                                     this.clustering,
                                     mergedData);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static long minLocalDeletionTime(CellData<?, ?> cell)
    {
        return cell.isTombstone() ? Long.MIN_VALUE : cell.localDeletionTime();
    }

    private static long minLocalDeletionTime(LivenessInfo info)
    {
        return info.isExpiring() ? info.localExpirationTime() : Long.MAX_VALUE;
    }

    private static long minLocalDeletionTime(DeletionTime dt)
    {
        return dt.isLive() ? Long.MAX_VALUE : Long.MIN_VALUE;
    }

    public int getMinLocalDeletionTime()
    {
        if (!minLocalDeletionTimeSet)
        {
            long accumulated = accumulate(Integer.MAX_VALUE,
                                         (livenessInfo, mldt) -> Math.min(mldt, minLocalDeletionTime(livenessInfo)),
                                         (cell, mldt) -> Math.min(mldt, minLocalDeletionTime(cell)),
                                         (marker, mldt) -> Math.min(mldt, minLocalDeletionTime(marker)));
            minLocalDeletionTime = (int) accumulated;
            minLocalDeletionTimeSet = true;
        }
        return minLocalDeletionTime;
    }

    class CellsWithPath extends TrieEntriesIterator.WithNullFiltering<Object, Cell<?>>
    {
        protected CellsWithPath(Trie<Object> trie, Direction direction)
        {
            super(trie, direction);
        }

        @Override
        protected Cell<?> mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (!(content instanceof CellData))
                return null;
            return cellFromCellData((CellData<?, ?>) content, bytes, byteLength, columns);
        }
    }

    static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker>
    makeMutator(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return data.mutator(noConflictInData(),
                            TrieBackedPartition.mergeTombstoneRanges(),
                            TrieBackedRow::deleteData,
                            TrieBackedRow::deleteData,
                            true,
                            Predicates.alwaysFalse(),
                            Predicates.alwaysFalse());
    }

    public static class Builder implements Row.Builder
    {
        protected final RegularAndStaticColumns regularAndStaticColumns;
        protected final Object2IntHashMap<ColumnIdentifier> regularColumnIds;
        protected final Object2IntHashMap<ColumnIdentifier> staticColumnIds;
        protected Clustering<?> clustering;
        protected Object2IntHashMap<ColumnIdentifier> columnIds;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.

        protected Builder(RegularAndStaticColumns regularAndStaticColumns)
        {
            this.regularAndStaticColumns = regularAndStaticColumns;
            regularColumnIds = columnsMapCache.computeIfAbsent(regularAndStaticColumns.regulars, TrieBackedRow::makeColumnIdsMap);
            staticColumnIds = columnsMapCache.computeIfAbsent(regularAndStaticColumns.statics, TrieBackedRow::makeColumnIdsMap);
            reset();
        }

        protected Builder(Builder builder)
        {
            this.regularAndStaticColumns = builder.regularAndStaticColumns;
            this.regularColumnIds = builder.regularColumnIds;
            this.staticColumnIds = builder.staticColumnIds;
            this.clustering = builder.clustering;
            this.columnIds = builder.columnIds;
            reset();
            try
            {
                mutator.apply(builder.data);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Builder copy()
        {
            return new Builder(this);
        }

        @Override
        public boolean isSorted()
        {
            return true;
        }

        @Override
        public void newRow(Clustering<?> clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
            this.columnIds = clustering == Clustering.STATIC_CLUSTERING ? staticColumnIds : regularColumnIds;
        }

        @Override
        public Clustering<?> clustering()
        {
            return clustering;
        }

        protected void reset()
        {
            this.clustering = null;
            data = newTrie();
            mutator = makeMutator(data);
        }

        @Override
        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            DeletionTime rowDeletion = TrieTombstoneMarker.applicableDeletion(data, ByteComparable.EMPTY);
            if (rowDeletion != null && rowDeletion.deletes(info))
                return;

            try
            {
                data.putRecursive(ByteComparable.EMPTY, (info), (x, y) -> y);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addRowDeletion(Deletion deletion)
        {
            if (deletion.isLive())
                return;

            try
            {
                mutator.delete(rowDeletionTrie(deletion.time()));
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;
            CellPath path = cell.path();
            ByteComparable key = cellKey(columnIds, cell.column, path);

            // TODO: Use apply to take care of this?
            DeletionTime cellDeletion = TrieTombstoneMarker.applicableDeletion(data, key);
            if (cellDeletion != null && cellDeletion.deletes(cell))
                return;

            try
            {
                if (path == null || path.dataSize() <= MAX_RECURSIVE_LENGTH)
                    data.putRecursive(key, cell, (x, y) -> Cells.reconcile((Cell<?>) x, y));
                else // long path, avoid stack overflow by using the apply path
                    mutator.apply(DeletionAwareTrie.singleton(key, BYTE_COMPARABLE_VERSION, cell));

                if (path != null)
                    data.putRecursive(columnKey(columnIds, cell.column), COMPLEX_COLUMN_MARKER, (x, y) -> y);

                if (data.get(ByteComparable.EMPTY) == null)
                    data.putRecursive(ByteComparable.EMPTY, LivenessInfo.EMPTY, (x, y) -> y);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime deletion)
        {
            if (deletion.isLive())
                return;

            ByteComparable key = columnKey(columnIds, column);
            try
            {
                mutator.delete(deletionTrie(key, deletion, TrieTombstoneMarker.Kind.COLUMN));
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TrieBackedRow build()
        {
            TrieBackedRow row = new TrieBackedRow(regularAndStaticColumns.columns(clustering == Clustering.STATIC_CLUSTERING),
                                                  columnIds,
                                                  clustering,
                                                  data);
            reset();
            return row;
        }
    }
}
