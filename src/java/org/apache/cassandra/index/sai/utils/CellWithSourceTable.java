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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * A wrapped {@link Cell} that includes a reference to the cell's source table.
 * @param <T> the type of the cell's value
 */
public class CellWithSourceTable<T> extends Cell<T>
{
    private final Cell<T> cell;
    private final Object sourceTable;

    public CellWithSourceTable(Cell<T> cell, Object sourceTable)
    {
        super(cell.column());
        this.cell = cell;
        this.sourceTable = sourceTable;
    }

    public Object sourceTable()
    {
        return sourceTable;
    }

    @Override
    public boolean isCounterCell()
    {
        return cell.isCounterCell();
    }

    @Override
    public T value()
    {
        return cell.value();
    }

    @Override
    public ValueAccessor<T> accessor()
    {
        return cell.accessor();
    }

    @Override
    public long timestamp()
    {
        return cell.timestamp();
    }

    @Override
    public int ttl()
    {
        return cell.ttl();
    }

    @Override
    public int localDeletionTime()
    {
        return cell.localDeletionTime();
    }

    @Override
    public boolean isTombstone()
    {
        return cell.isTombstone();
    }

    @Override
    public boolean isExpiring()
    {
        return cell.isExpiring();
    }

    @Override
    public boolean isLive(int nowInSec)
    {
        return cell.isLive(nowInSec);
    }

    @Override
    public CellPath path()
    {
        return cell.path();
    }

    @Override
    public Cell<?> withUpdatedColumn(ColumnMetadata newColumn)
    {
        return wrapIfNew(cell.withUpdatedColumn(newColumn));
    }

    @Override
    public Cell<?> withUpdatedValue(ByteBuffer newValue)
    {
        return wrapIfNew(cell.withUpdatedValue(newValue));
    }

    @Override
    public Cell<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime)
    {
        return wrapIfNew(cell.withUpdatedTimestampAndLocalDeletionTime(newTimestamp, newLocalDeletionTime));
    }

    @Override
    public Cell<?> withSkippedValue()
    {
        return wrapIfNew(cell.withSkippedValue());
    }

    @Override
    public Cell<?> clone(ByteBufferCloner cloner)
    {
        return wrapIfNew(cell.clone(cloner));
    }

    @Override
    public int dataSize()
    {
        return cell.dataSize();
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return cell.unsharedHeapSizeExcludingData();
    }

    @Override
    public void validate()
    {
        cell.validate();
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        return cell.hasInvalidDeletions();
    }

    @Override
    public void digest(Digest digest)
    {
        cell.digest(digest);
    }

    @Override
    public ColumnData updateAllTimestamp(long newTimestamp)
    {
        var maybeNewCell = cell.updateAllTimestamp(newTimestamp);
        if (maybeNewCell instanceof Cell)
            return wrapIfNew((Cell<?>) maybeNewCell);
        if (maybeNewCell instanceof ComplexColumnData)
            return ((ComplexColumnData) maybeNewCell).transform(this::wrapIfNew);
        // It's not clear when we would hit this code path, but it seems we should not
        // hit this from SAI.
        throw new IllegalStateException("Expected a Cell instance, but got " + maybeNewCell);
    }

    @Override
    public Cell<?> markCounterLocalToBeCleared()
    {
        return wrapIfNew(cell.markCounterLocalToBeCleared());
    }

    @Override
    public Cell<?> purge(DeletionPurger purger, int nowInSec)
    {
        return wrapIfNew(cell.purge(purger, nowInSec));
    }

    @Override
    public long maxTimestamp()
    {
        return cell.maxTimestamp();
    }

    @Override
    public long minTimestamp()
    {
        return cell.minTimestamp();
    }

    private Cell<?> wrapIfNew(Cell<?> maybeNewCell)
    {
        if (maybeNewCell == null)
            return null;
        // If the cell's method returned a reference to the same cell, then
        // we can skip creating a new wrapper.
        if (maybeNewCell == this.cell)
            return this;
        return new CellWithSourceTable<>(maybeNewCell, sourceTable);
    }
}
