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
import org.apache.cassandra.db.IDataSize;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.BaseTrie;
import org.apache.cassandra.db.tries.Trie;

class MemtableAverageRowSize
{
    private final static long MAX_ROWS = 100;

    public final long rowSize;
    public final long operations;

    public MemtableAverageRowSize(Memtable memtable, Trie<?> trie)
    {
        // If this is a trie-based memtable, get the row sizes from the trie elements. This achieves two things:
        // - makes sure the size used is the size reflected in the memtable's dataSize
        //   (which e.g. excludes clustering keys)
        // - avoids the conversion to Row, which has non-trivial cost

        class SizeCalculator implements BaseTrie.ValueConsumer<Object>
        {
            long totalSize = 0;
            long count = 0;

            @Override
            public void accept(Object o)
            {
                if (o instanceof IDataSize)
                {
                    totalSize += ((IDataSize) o).dataSize();
                    ++count;
                }
            }
        }

        SizeCalculator sizeCalculator = new SizeCalculator();
        trie.forEachValue(sizeCalculator);

        this.rowSize = sizeCalculator.count > 0 ? sizeCalculator.totalSize / sizeCalculator.count : 0;
        this.operations = memtable.getOperations();
    }

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
