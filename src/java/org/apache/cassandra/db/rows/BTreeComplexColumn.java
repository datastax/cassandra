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
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.memory.Cloner;

/**
 * The data for a complex column, that is its cells and potential complex
 * deletion time.
 */
public class BTreeComplexColumn extends ComplexColumnData
{
    static final Cell<?>[] NO_CELLS = new Cell<?>[0];

    private static final long EMPTY_SIZE = ObjectSizes.measure(
        new BTreeComplexColumn(ColumnMetadata.regularColumn("",
                                                            "",
                                                            "",
                                                            SetType.getInstance(ByteType.instance, true)),
                               NO_CELLS,
                               DeletionTime.build(0, 0)));

    // The cells for 'column' sorted by cell path.
    private final Object[] cells;

    private final DeletionTime complexDeletion;

    BTreeComplexColumn(ColumnMetadata column, Object[] cells, DeletionTime complexDeletion)
    {
        super(column);
        assert column.isComplex();
        assert cells.length > 0 || !complexDeletion.isLive();
        this.cells = cells;
        this.complexDeletion = complexDeletion;
    }

    @Override
    public boolean hasCells() {
        return !BTree.isEmpty(this.cells);
    }

    @Override
    public int cellsCount()
    {
        return BTree.size(cells);
    }

    @Override
    public Cell<?> getCell(CellPath path)
    {
        return (Cell<?>) BTree.<Object>find(cells, column.asymmetricCellPathComparator(), path);
    }

    public <R> R reduce(R seed, BTree.ReduceFunction<R, Cell> reducer)
    {
        return BTree.reduce(cells, seed, reducer);
    }

    @Override
    public Cell<?> getCellByIndex(int idx)
    {
        return BTree.findByIndex(cells, idx);
    }

    @Override
    public DeletionTime complexDeletion()
    {
        return complexDeletion;
    }

    Object[] tree()
    {
        return cells;
    }

    @Override
    public Iterator<Cell<?>> iterator()
    {
        return BTree.iterator(cells);
    }

    @Override
    public Iterator<Cell<?>> reverseIterator()
    {
        return BTree.iterator(cells, BTree.Dir.DESC);
    }

    @Override
    public long accumulate(LongAccumulator<Cell<?>> accumulator, long initialValue)
    {
        return BTree.accumulate(cells, accumulator, initialValue);
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, Cell<?>> accumulator, A arg, long initialValue)
    {
        return BTree.accumulate(cells, accumulator, arg, initialValue);
    }

    @Override
    public int dataSize()
    {
        int size = complexDeletion.dataSize();
        for (Cell<?> cell : this)
            size += cell.dataSize();
        return size;
    }

    @Override
    public int liveDataSize(long nowInSec)
    {
        return complexDeletion.isLive() ? dataSize() : 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        long heapSize = EMPTY_SIZE + BTree.sizeOnHeapOf(cells) + complexDeletion.unsharedHeapSize();
        return BTree.<Cell>accumulate(cells, (cell, value) -> value + cell.unsharedHeapSize(), heapSize);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE + BTree.sizeOnHeapOf(cells);
        // TODO: this can be turned into a simple multiplication, at least while we have only one Cell implementation
        for (Cell<?> cell : this)
            heapSize += cell.unsharedHeapSizeExcludingData();
        return heapSize;
    }

    @Override
    public void validate()
    {
        for (Cell<?> cell : this)
            cell.validate();
    }

    @Override
    public void digest(Digest digest)
    {
        if (!complexDeletion.isLive())
            complexDeletion.digest(digest);

        for (Cell<?> cell : this)
            cell.digest(digest);
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        if (!complexDeletion.validate())
            return true;
        for (Cell<?> cell : this)
            if (cell.hasInvalidDeletions())
                return true;
        return false;
    }

    @Override
    public BTreeComplexColumn markCounterLocalToBeCleared()
    {
        return transformAndFilter(complexDeletion, Cell::markCounterLocalToBeCleared);
    }

    public BTreeComplexColumn filter(ColumnFilter filter, DeletionTime activeDeletion, DroppedColumn dropped, LivenessInfo rowLiveness)
    {
        ColumnFilter.Tester cellTester = filter.newTester(column);
        boolean isQueriedColumn = filter.fetchedColumnIsQueried(column);
        if (cellTester == null && activeDeletion.isLive() && dropped == null && isQueriedColumn)
            return this;

        DeletionTime newDeletion = activeDeletion.supersedes(complexDeletion) ? DeletionTime.LIVE : complexDeletion;
        return transformAndFilter(newDeletion, (cell) ->
        {
            CellPath path = cell.path();
            boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
            boolean isShadowed = activeDeletion.deletes(cell);
            boolean isFetchedCell = cellTester == null || cellTester.fetches(path);
            boolean isQueriedCell = isQueriedColumn && isFetchedCell && (cellTester == null || cellTester.fetchedCellIsQueried(path));
            boolean isSkippableCell = !isFetchedCell || (!isQueriedCell && cell.timestamp() < rowLiveness.timestamp());
            if (isForDropped || isShadowed || isSkippableCell)
                return null;
            // We should apply the same "optimization" as in Cell.deserialize to avoid discrepances
            // between sstables and memtables data, i.e resulting in a digest mismatch.
            return isQueriedCell ? cell : cell.withSkippedValue();
        });
    }

    public BTreeComplexColumn delete(DeletionTime activeDeletion)
    {
        // Assuming this column does not keep any shadowed data, there's nothing to change if we already apply the same
        // deletion or newer.
        if (activeDeletion.isLive() || !activeDeletion.supersedes(complexDeletion))
            return this;

        // The new deletion supersedes the existing, thus we can drop the complex deletion (the assumption is that the
        // deletion that is being applied here will be stored at a higher level of the hierarchy (e.g. row/partition)).
        return transformAndFilter(DeletionTime.LIVE, (cell) -> activeDeletion.deletes(cell) ? null : cell);
    }

    @Override
    public BTreeComplexColumn purge(DeletionPurger purger, long nowInSec)
    {
        DeletionTime newDeletion = complexDeletion.isLive() || purger.shouldPurge(complexDeletion) ? DeletionTime.LIVE : complexDeletion;
        return transformAndFilter(newDeletion, (cell) -> cell.purge(purger, nowInSec));
    }

    @Override
    public BTreeComplexColumn purgeDataOlderThan(long timestamp)
    {
        DeletionTime newDeletion = complexDeletion.markedForDeleteAt() < timestamp ? DeletionTime.LIVE : complexDeletion;
        return transformAndFilter(newDeletion, (cell) -> cell.purgeDataOlderThan(timestamp));
    }

    public BTreeComplexColumn withOnlyQueriedData(ColumnFilter filter)
    {
        return transformAndFilter(complexDeletion, (cell) -> filter.fetchedCellIsQueried(column, cell.path()) ? null : cell);
    }

    private BTreeComplexColumn update(DeletionTime newDeletion, Object[] newCells)
    {
        if (cells == newCells && newDeletion == complexDeletion)
            return this;

        if (newDeletion == DeletionTime.LIVE && BTree.isEmpty(newCells))
            return null;

        return new BTreeComplexColumn(column, newCells, newDeletion);
    }

    public BTreeComplexColumn transformAndFilter(Row.CellTransformer function)
    {
        return update(complexDeletion, BTree.<Cell<?>, Cell<?>>transformAndFilter(cells, function::apply));
    }

    public BTreeComplexColumn transformAndFilter(DeletionTime newDeletion, Function<? super Cell, ? extends Cell> function)
    {
        return update(newDeletion, BTree.transformAndFilter(cells, function));
    }

    public <V> BTreeComplexColumn transform(Function<? super Cell<?>, ? extends Cell<?>> function)
    {
        return update(complexDeletion, BTree.transform(cells, function));
    }

    @Override
    public ColumnData clone(Cloner cloner)
    {
        return transform(c -> cloner.clone(c));
    }

    @Override
    public BTreeComplexColumn updateAllTimestamp(long newTimestamp)
    {
        DeletionTime newDeletion = complexDeletion.isLive() ? complexDeletion : DeletionTime.build(newTimestamp - 1, complexDeletion.localDeletionTime());
        return transformAndFilter(newDeletion, (cell) -> (Cell<?>) cell.updateAllTimestamp(newTimestamp));
    }

    @Override
    public long maxTimestamp()
    {
        long timestamp = complexDeletion.markedForDeleteAt();
        for (Cell<?> cell : this)
            timestamp = Math.max(timestamp, cell.timestamp());
        return timestamp;
    }

    @Override
    public long minTimestamp()
    {
        long timestamp = complexDeletion.isLive()
                         ? Long.MAX_VALUE
                         : complexDeletion.markedForDeleteAt();
        for (Cell cell : this)
            timestamp = Math.min(timestamp, cell.timestamp());
        return timestamp;
    }

    // This is the partner in crime of ArrayBackedRow.setValue. The exact warning apply. The short
    // version is: "don't use that method".
    void setValue(CellPath path, ByteBuffer value)
    {
        Cell<?> current = (Cell<?>) BTree.<Object>find(cells, column.asymmetricCellPathComparator(), path);
        BTree.replaceInSitu(cells, column.cellComparator(), current, current.withUpdatedValue(value));
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if(!(other instanceof BTreeComplexColumn))
            return false;

        BTreeComplexColumn that = (BTreeComplexColumn)other;
        return this.column().equals(that.column())
            && this.complexDeletion().equals(that.complexDeletion)
            && BTree.equals(this.cells, that.cells);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column(), complexDeletion(), BTree.hashCode(cells));
    }

    @Override
    public String toString()
    {
        return String.format("[%s=%s %s]",
                             column().name,
                             complexDeletion.toString(),
                             BTree.toString(cells));
    }

    @VisibleForTesting
    public static BTreeComplexColumn unsafeConstruct(ColumnMetadata column, Object[] cells, DeletionTime complexDeletion)
    {
        return new BTreeComplexColumn(column, cells, complexDeletion);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DeletionTime complexDeletion;
        private ColumnMetadata column;
        private BTree.Builder<Cell<?>> builder;

        public void newColumn(ColumnMetadata column)
        {
            this.column = column;
            this.complexDeletion = DeletionTime.LIVE; // default if writeComplexDeletion is not called
            if (builder == null)
                builder = BTree.builder(column.cellComparator());
            else
                builder.reuse(column.cellComparator());
        }

        public void addComplexDeletion(DeletionTime complexDeletion)
        {
            this.complexDeletion = complexDeletion;
        }

        public void addCell(Cell<?> cell)
        {
            builder.add(cell);
        }

        public BTreeComplexColumn build()
        {
            if (complexDeletion.isLive() && builder.isEmpty())
                return null;

            return new BTreeComplexColumn(column, builder.build(), complexDeletion);
        }
    }
}
