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

package org.apache.cassandra.db.memtable;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

class MemtableAverageRowSize
{
    private final static long MAX_ROWS = 100;

    public final long rowSize;
    public final long operations;


    public MemtableAverageRowSize(Memtable memtable)
    {
        DataRange range = DataRange.allData(memtable.metadata().partitioner);
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(memtable.metadata(), true).build();

        long rowCount = 0;
        long totalSize = 0;

        try (var partitionsIter = memtable.makePartitionIterator(columnFilter, range))
        {
            while (partitionsIter.hasNext() && rowCount < MAX_ROWS)
            {
                UnfilteredRowIterator rowsIter = partitionsIter.next();
                while (rowsIter.hasNext() && rowCount < MAX_ROWS)
                {
                    Unfiltered uRow = rowsIter.next();
                    if (uRow.isRow())
                    {
                        rowCount++;
                        totalSize += ((Row) uRow).dataSize();
                    }
                }
            }
        }
        this.operations = memtable.getOperations();
        this.rowSize = (rowCount > 0)
                       ? totalSize / rowCount
                       : 0;
    }
}
