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

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 *  the function we provide to the trie and btree utilities to perform any row and column replacements
 */
public class BTreePartitionUpdater extends BasePartitionUpdater implements UpdateFunction<Row, Row>
{
    final MemtableAllocator allocator;
    final OpOrder.Group writeOp;
    final UpdateTransaction indexer;
    public int partitionsAdded = 0;

    public BTreePartitionUpdater(MemtableAllocator allocator, Cloner cloner, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        super(cloner);
        this.allocator = allocator;
        this.writeOp = writeOp;
        this.indexer = indexer;
    }

    @Override
    public Row insert(Row insert)
    {
        Row data = insert.clone(cloner);
        indexer.onInserted(insert);

        this.dataSize += data.dataSize();
        this.heapSize += data.unsharedHeapSizeExcludingData();
        return data;
    }

    @Override
    public Row merge(Row existing, Row update)
    {
        Row reconciled = Rows.merge(existing, update, this);
        indexer.onUpdated(existing, reconciled);

        return reconciled;
    }

    private DeletionInfo apply(DeletionInfo existing, DeletionInfo update)
    {
        if (update.isLive() || !update.mayModify(existing))
            return existing;

        if (!update.getPartitionDeletion().isLive())
            indexer.onPartitionDeletion(update.getPartitionDeletion());

        if (update.hasRanges())
            update.rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);

        // Like for rows, we have to clone the update in case internal buffers (when it has range tombstones) reference
        // memory we shouldn't hold into. But we don't ever store this off-heap currently so we just default to the
        // HeapAllocator (rather than using 'allocator').
        DeletionInfo newInfo = existing.mutableCopy().add(update.clone(HeapCloner.instance));
        onAllocatedOnHeap(newInfo.unsharedHeapSize() - existing.unsharedHeapSize());
        return newInfo;
    }

    public BTreePartitionData mergePartitions(BTreePartitionData current, final BTreePartitionUpdate update)
    {
        if (current == null)
        {
            current = BTreePartitionData.EMPTY;
            this.onAllocatedOnHeap(BTreePartitionData.UNSHARED_HEAP_SIZE);
            ++partitionsAdded;
        }

        try
        {
            indexer.start();

            return makeMergedPartition(current, update);
        }
        finally
        {
            indexer.commit();
            reportAllocatedMemory();
        }
    }

    protected BTreePartitionData makeMergedPartition(BTreePartitionData current, BTreePartitionUpdate update)
    {
        DeletionInfo newDeletionInfo = apply(current.deletionInfo, update.deletionInfo());

        RegularAndStaticColumns columns = current.columns;
        RegularAndStaticColumns newColumns = update.columns().mergeTo(columns);
        onAllocatedOnHeap(newColumns.unsharedHeapSize() - columns.unsharedHeapSize());
        Row newStatic = update.staticRow();
        newStatic = newStatic.isEmpty()
                    ? current.staticRow
                    : (current.staticRow.isEmpty()
                       ? this.insert(newStatic)
                       : this.merge(current.staticRow, newStatic));

        Object[] tree = BTree.update(current.tree, update.holder().tree, update.metadata().comparator, this);
        EncodingStats newStats = current.stats.mergeWith(update.stats());
        onAllocatedOnHeap(newStats.unsharedHeapSize() - current.stats.unsharedHeapSize());

        return new BTreePartitionData(newColumns, tree, newDeletionInfo, newStatic, newStats);
    }

    public void reportAllocatedMemory()
    {
        allocator.onHeap().adjust(heapSize, writeOp);
    }
}
