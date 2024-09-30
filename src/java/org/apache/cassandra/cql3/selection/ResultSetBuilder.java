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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MultiCellCapableType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class ResultSetBuilder
{
    private final ProtocolVersion protocolVersion;
    private final ResultMetadata metadata;
    private final SortedRowsBuilder rows;

    /**
     * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
     * its own <code>Selectors</code> instance.
     */
    private final Selectors selectors;

    /**
     * The <code>GroupMaker</code> used to build the aggregates.
     */
    private final GroupMaker groupMaker;

    /*
     * We'll build CQL3 row one by one.
     * The currentRow is the values for the (CQL3) columns we've fetched.
     * We also collect timestamps and ttls for the case where the writetime and
     * ttl functions are used. Note that we might collect timestamp and/or ttls
     * we don't care about, but since the array below are allocated just once,
     * it doesn't matter performance wise.
     */
    List<ByteBuffer> current;
    RowTimestamps timestamps;
    RowTimestamps ttls;

    private boolean hasResults = false;
    private int readRowsSize = 0;

    public ResultSetBuilder(ProtocolVersion protocolVersion, ResultMetadata metadata, Selectors selectors)
    {
        this(protocolVersion, metadata, selectors, null, SortedRowsBuilder.create());
    }

    public ResultSetBuilder(ProtocolVersion protocolVersion,
                            ResultMetadata metadata,
                            Selectors selectors,
                            GroupMaker groupMaker,
                            SortedRowsBuilder rows)
    {
        this.protocolVersion = protocolVersion;
        this.metadata = metadata.copy();
        this.rows = rows;
        this.selectors = selectors;
        this.groupMaker = groupMaker;

        timestamps = initTimestamps(ColumnTimestamps.TimestampsType.WRITETIMES, selectors.collectTimestamps(), selectors.getColumns());
        ttls = initTimestamps(ColumnTimestamps.TimestampsType.TTLS, selectors.collectTTLs(), selectors.getColumns());
    }

    public ProtocolVersion getProtocolVersion()
    {
        return protocolVersion;
    }

    private RowTimestamps initTimestamps(ColumnTimestamps.TimestampsType type,
                                         boolean collectWritetimes,
                                         List<ColumnMetadata> columns)
    {
        return collectWritetimes ? RowTimestamps.newInstance(type, columns)
                                 : RowTimestamps.NOOP_ROW_TIMESTAMPS;
    }

    public void add(ByteBuffer v)
    {
        current.add(v);

        if (v != null)
        {
            timestamps.addNoTimestamp(current.size() - 1);
            ttls.addNoTimestamp(current.size() - 1);
        }
    }

    public void add(ColumnData columnData, int nowInSec)
    {
        ColumnMetadata column = selectors.getColumns().get(current.size());
        if (columnData == null)
        {
            add(null);
        }
        else
        {
            if (column.isComplex())
            {
                assert column.type.isMultiCell();
                ComplexColumnData complexData = (ComplexColumnData) columnData;
                add(complexData, nowInSec);
            }
            else
            {
                add((Cell<?>) columnData, nowInSec);
            }
        }
    }

    private void add(Cell<?> c, int nowInSec)
    {
        if (c == null)
        {
            current.add(null);
            return;
        }

        current.add(value(c));

        timestamps.addTimestamp(current.size() - 1, c, nowInSec);
        ttls.addTimestamp(current.size() - 1, c, nowInSec);
    }

    private void add(ComplexColumnData ccd, int nowInSec)
    {
        int index = current.size();
        AbstractType<?> type = selectors.getColumns().get(index).type;
        if (type.isCollection())
        {
            current.add(((MultiCellCapableType<?>) type).serializeForNativeProtocol(ccd.iterator(), protocolVersion));

            for (Cell<?> cell : ccd)
            {
                timestamps.addTimestamp(index, cell, nowInSec);
                ttls.addTimestamp(index, cell, nowInSec);
            }
        }
        else
        {
            UserType udt = (UserType) type;
            int size = udt.size();

            current.add(udt.serializeForNativeProtocol(ccd.iterator(), protocolVersion));

            short fieldPosition = 0;
            for (Cell<?> cell : ccd)
            {
                // handle null fields that aren't at the end
                short fieldPositionOfCell = ByteBufferUtil.toShort(cell.path().get(0));
                while (fieldPosition < fieldPositionOfCell)
                {
                    fieldPosition++;
                    timestamps.addNoTimestamp(index);
                    ttls.addNoTimestamp(index);
                }

                fieldPosition++;
                timestamps.addTimestamp(index, cell, nowInSec);
                ttls.addTimestamp(index, cell, nowInSec);
            }

            // append nulls for missing cells
            while (fieldPosition < size)
            {
                fieldPosition++;
                timestamps.addNoTimestamp(index);
                ttls.addNoTimestamp(index);
            }
        }
    }

    private <V> ByteBuffer value(Cell<V> c)
    {
        return c.isCounterCell()
             ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value(), c.accessor()))
             : c.buffer();
    }

    /**
     * Notifies this <code>Builder</code> that a new row is being processed.
     *
     * @param partitionKey the partition key of the new row
     * @param clustering the clustering of the new row
     */
    public void newRow(DecoratedKey partitionKey, Clustering<?> clustering)
    {
        // The groupMaker needs to be called for each row
        boolean isNewAggregate = groupMaker == null || groupMaker.isNewGroup(partitionKey, clustering);
        if (current != null)
        {
            selectors.addInputRow(this);
            if (isNewAggregate)
            {
                addRow();
            }
        }
        current = new ArrayList<>(selectors.numberOfFetchedColumns());

        // Timestamps and TTLs are arrays per row, we must null them out between rows
        this.timestamps = initTimestamps(ColumnTimestamps.TimestampsType.WRITETIMES, selectors.collectTimestamps(), selectors.getColumns());
        this.ttls = initTimestamps(ColumnTimestamps.TimestampsType.TTLS, selectors.collectTTLs(), selectors.getColumns());
    }

    /**
     * Builds the <code>ResultSet</code>
     */
    public ResultSet build()
    {
        if (current != null)
        {
            selectors.addInputRow(this);
            addRow();
            current = null;
        }

        // For aggregates we need to return a row even it no records have been found
        if (!hasResults && groupMaker != null && groupMaker.returnAtLeastOneRow())
        {
            addRow();
        }

        return new ResultSet(metadata, rows.build());
    }

    public int readRowsSize()
    {
        return readRowsSize;
    }

    private List<ByteBuffer> getOutputRow()
    {
        return selectors.getOutputRow();
    }

    private void addRow()
    {
        List<ByteBuffer> row = getOutputRow();
        selectors.reset();

        hasResults = true;
        for (int i = 0, isize = row.size(); i < isize; i++)
        {
            ByteBuffer value = row.get(i);
            readRowsSize += value != null ? value.remaining() : 0;
        }

        rows.add(row);
    }
}
