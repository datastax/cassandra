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

package org.apache.cassandra.index.sai.utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.memory.Cloner;

/**
 * A Row wrapper that has a source object that gets added to cell as part of the getCell call. This can only be used
 * validly when all the cells share a common source object.
 */
public class RowWithSourceTable implements Row
{
    private final Row row;
    private final Object source;

    public RowWithSourceTable(Row row, Object source)
    {
        this.row = row;
        this.source = source;
    }

    @Override
    public Kind kind()
    {
        return row.kind();
    }

    @Override
    public Clustering<?> clustering()
    {
        return row.clustering();
    }

    @Override
    public void digest(Digest digest)
    {
        row.digest(digest);
    }

    @Override
    public void validateData(TableMetadata metadata)
    {
        row.validateData(metadata);
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        return row.hasInvalidDeletions();
    }

    @Override
    public Collection<ColumnMetadata> columns()
    {
        return row.columns();
    }

    @Override
    public int columnCount()
    {
        return row.columnCount();
    }

    @Override
    public Deletion deletion()
    {
        return row.deletion();
    }

    @Override
    public LivenessInfo primaryKeyLivenessInfo()
    {
        return row.primaryKeyLivenessInfo();
    }

    @Override
    public boolean isStatic()
    {
        return row.isStatic();
    }

    @Override
    public boolean isEmpty()
    {
        return row.isEmpty();
    }

    @Override
    public String toString(TableMetadata metadata)
    {
        return row.toString(metadata);
    }

    @Override
    public boolean hasLiveData(int nowInSec, boolean enforceStrictLiveness)
    {
        return row.hasLiveData(nowInSec, enforceStrictLiveness);
    }

    @Override
    public Cell<?> getCell(ColumnMetadata c)
    {
        var cell = row.getCell(c);
        if (cell == null)
            return null;
        return new CellWithSourceTable<>(c, cell, source);
    }

    @Override
    public Cell<?> getCell(ColumnMetadata c, CellPath path)
    {
        var cell = row.getCell(c, path);
        if (cell == null)
            return null;
        return new CellWithSourceTable<>(c, cell, source);
    }

    @Override
    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        return row.getComplexColumnData(c);
    }

    @Override
    public ColumnData getColumnData(ColumnMetadata c)
    {
        return row.getColumnData(c);
    }

    @Override
    public Iterable<Cell<?>> cells()
    {
        return Iterables.transform(row.cells(), c -> c != null ? new CellWithSourceTable<>(c.column(), c, source)
                                                               : null);
    }

    @Override
    public Collection<ColumnData> columnData()
    {
        // TODO does this need to be wrapped?
        return row.columnData();
    }

    @Override
    public Iterable<Cell<?>> cellsInLegacyOrder(TableMetadata metadata, boolean reversed)
    {
        return row.cellsInLegacyOrder(metadata, reversed);
    }

    @Override
    public boolean hasComplexDeletion()
    {
        return row.hasComplexDeletion();
    }

    @Override
    public boolean hasComplex()
    {
        return row.hasComplex();
    }

    @Override
    public boolean hasDeletion(int nowInSec)
    {
        return row.hasDeletion(nowInSec);
    }

    @Override
    public SearchIterator<ColumnMetadata, ColumnData> searchIterator()
    {
        return row.searchIterator();
    }

    @Override
    public Row filter(ColumnFilter filter, TableMetadata metadata)
    {
        return new RowWithSourceTable(row.filter(filter, metadata), source);
    }

    @Override
    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata)
    {
        return new RowWithSourceTable(row.filter(filter, activeDeletion, setActiveDeletionToRow, metadata), source);
    }

    @Override
    public Row transformAndFilter(LivenessInfo info, Deletion deletion, Function<ColumnData, ColumnData> function)
    {
        return new RowWithSourceTable(row.transformAndFilter(info, deletion, function), source);
    }

    @Override
    public Row transformAndFilter(Function<ColumnData, ColumnData> function)
    {
        return new RowWithSourceTable(row.transformAndFilter(function), source);
    }

    @Override
    public Row clone(Cloner cloner)
    {
        // todo need me??
        return new RowWithSourceTable(row.clone(cloner), source);
    }

    @Override
    public Row purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness)
    {
        return new RowWithSourceTable(row.purge(purger, nowInSec, enforceStrictLiveness), source);
    }

    @Override
    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        return new RowWithSourceTable(row.withOnlyQueriedData(filter), source);
    }

    @Override
    public Row markCounterLocalToBeCleared()
    {
        return new RowWithSourceTable(row.markCounterLocalToBeCleared(), source);
    }

    @Override
    public Row updateAllTimestamp(long newTimestamp)
    {
        return new RowWithSourceTable(row.updateAllTimestamp(newTimestamp), source);
    }

    @Override
    public Row withRowDeletion(DeletionTime deletion)
    {
        return new RowWithSourceTable(row.withRowDeletion(deletion), source);
    }

    @Override
    public int dataSize()
    {
        return row.dataSize();
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return row.unsharedHeapSizeExcludingData();
    }

    @Override
    public String toString(TableMetadata metadata, boolean fullDetails)
    {
        return row.toString(metadata, fullDetails);
    }

    @Override
    public String toString(TableMetadata metadata, boolean includeClusterKeys, boolean fullDetails)
    {
        return row.toString(metadata, includeClusterKeys, fullDetails);
    }

    @Override
    public long minTimestamp()
    {
        return row.minTimestamp();
    }

    @Override
    public long maxTimestamp()
    {
        return row.maxTimestamp();
    }

    @Override
    public void apply(Consumer<ColumnData> function)
    {
        // todo me??
        row.apply(function);
    }

    @Override
    public <A> void apply(BiConsumer<A, ColumnData> function, A arg)
    {
        row.apply(function, arg);
    }

    @Override
    public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue)
    {
        return row.accumulate(accumulator, initialValue);
    }

    @Override
    public long accumulate(LongAccumulator<ColumnData> accumulator, Comparator<ColumnData> comparator, ColumnData from, long initialValue)
    {
        return row.accumulate(accumulator, comparator, from, initialValue);
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue)
    {
        return row.accumulate(accumulator, arg, initialValue);
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, Comparator<ColumnData> comparator, ColumnData from, long initialValue)
    {
        return row.accumulate(accumulator, arg, comparator, from, initialValue);
    }

    @Override
    public Iterator<ColumnData> iterator()
    {
        return Iterators.transform(row.iterator(), c -> {
            if (c == null)
                return null;
            if (c instanceof Cell<?>)
                return new CellWithSourceTable<>(c.column(), (Cell<?>) c, source);
            if (c instanceof ComplexColumnData)
                return ((ComplexColumnData) c).transform(c1 -> new CellWithSourceTable<>(c1.column(), c1, source));
            throw new IllegalStateException("Unexpected ColumnData type: " + c.getClass().getName());
        });
    }
}
