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

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.utils.memory.Cloner;

public class BasePartitionUpdater implements ColumnData.PostReconciliationFunction
{
    final Cloner cloner;
    public long dataSize = 0;
    public long heapSize = 0;
    public long colUpdateTimeDelta = Long.MAX_VALUE;

    public BasePartitionUpdater(Cloner cloner)
    {
        this.cloner = cloner;
    }

    public Cell<?> merge(Cell<?> previous, Cell<?> insert)
    {
        if (insert == previous)
            return insert;
        long timeDelta = Math.abs(insert.timestamp() - previous.timestamp());
        if (timeDelta < colUpdateTimeDelta)
            colUpdateTimeDelta = timeDelta;
        if (cloner != null)
            insert = cloner.clone(insert);
        dataSize += insert.dataSize() - previous.dataSize();
        heapSize += insert.unsharedHeapSizeExcludingData() - previous.unsharedHeapSizeExcludingData();
        return insert;
    }

    public ColumnData insert(ColumnData insert)
    {
        if (cloner != null)
            insert = insert.clone(cloner);
        dataSize += insert.dataSize();
        heapSize += insert.unsharedHeapSizeExcludingData();
        return insert;
    }

    public void delete(ColumnData existing)
    {
        dataSize -= existing.dataSize();
        heapSize -= existing.unsharedHeapSizeExcludingData();
    }

    public void onAllocatedOnHeap(long heapSize)
    {
        this.heapSize += heapSize;
    }
}
