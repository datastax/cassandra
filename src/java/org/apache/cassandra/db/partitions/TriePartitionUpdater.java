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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.RowData;

/**
 *  The function we provide to the trie utilities to perform any partition and row inserts and updates
 */
public final class TriePartitionUpdater implements InMemoryTrie.UpsertTransformerWithKeyProducer<Object, Object>, ColumnData.PostReconciliationFunction
{
    private final Cloner cloner;
    private final UpdateTransaction indexer;
    private final TableMetadata metadata;
    private final TrieMemtable.MemtableShard owner;
    public long dataSize;
    public long heapSize;
    public int partitionsAdded = 0;
    public int rowsAdded = 0;
    public long colUpdateTimeDelta = Long.MAX_VALUE;

    public TriePartitionUpdater(Cloner cloner,
                                UpdateTransaction indexer,
                                TableMetadata metadata,
                                TrieMemtable.MemtableShard owner)
    {
        this.indexer = indexer;
        this.metadata = metadata;
        this.cloner = cloner;
        this.owner = owner;
    }

    public Object apply(Object existing, Object update, InMemoryTrie.KeyProducer keyState)
    {
        if (update instanceof RowData)
        {
            RowData rowData = (RowData) update;
            return applyRow((RowData) existing, rowData, keyState);
        }
        else if (update instanceof DeletionInfo)
        {
            return applyDeletion((TrieMemtable.PartitionData) existing, (DeletionInfo) update);
        }
        {
            throw new AssertionError("Unexpected update type: " + update.getClass());
        }
    }

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing RowData for this clustering, or null if there isn't any.
     * @param insert RowData to be inserted.
     * @param keyState Used to obtain the path through which this node was reached.
     * @return the insert row, or the merged row, copied using our allocator
     */
    public RowData applyRow(RowData existing, RowData insert, InMemoryTrie.KeyProducer keyState)
    {
        if (existing == null)
        {
            RowData data = insert.clone(cloner);

            if (indexer != UpdateTransaction.NO_OP)
                indexer.onInserted(data.toRow(clusteringFor(keyState)));

            this.dataSize += data.dataSize();
            this.heapSize += data.unsharedHeapSizeExcludingData();
            ++this.rowsAdded;
            return data;
        }
        else
        {
            // TODO: Avoid going through Row.
            Clustering<?> clustering;
            // We need the real clustering only if we have indexing.
            if (indexer != UpdateTransaction.NO_OP)
                clustering = clusteringFor(keyState);
            else
                clustering = Clustering.EMPTY;

            Row existingRow = existing.toRow(clustering);
            Row insertRow = insert.toRow(clustering);
            Row reconciledRow = Rows.merge(existingRow, insertRow, this);
            // data and heap size are updated during merge through the PostReconciliationFunction interface
            RowData reconciled = TrieBackedPartition.rowToData(reconciledRow);

            if (indexer != UpdateTransaction.NO_OP)
                indexer.onUpdated(existingRow, reconciledRow);

            return reconciled;
        }
    }

    private Clustering<?> clusteringFor(InMemoryTrie.KeyProducer<Object> keyState)
    {
        return metadata.comparator.clusteringFromByteComparable(ByteArrayAccessor.instance,
                                                                ByteComparable.fixedLength(keyState.getBytes(TrieMemtable.IS_PARTITION_BOUNDARY)));
    }

    /**
     * Called at the partition boundary to merge the existing and new metadata associated with the partition. This needs
     * to update the deletion time with any new deletion introduced by the update, but also make sure that the
     * statistics we track for the partition (dataSize) are updated for the changes caused by merging the update's rows
     * (note that this is called _after_ the rows of the partition have been merged, on the return path of the
     * recursion).
     *
     * @param existing Any partition data already associated with the partition.
     * @param update The update, always non-null.
     * @return the combined partition data, copying any updated deletion information to heap.
     */
    public TrieMemtable.PartitionData applyDeletion(TrieMemtable.PartitionData existing, DeletionInfo update)
    {
        if (indexer != UpdateTransaction.NO_OP)
        {
            if (!update.getPartitionDeletion().isLive())
                indexer.onPartitionDeletion(update.getPartitionDeletion());
            if (update.hasRanges())
                update.rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);
        }

        if (existing == null)
        {
            // Note: Always on-heap, regardless of cloner
            TrieMemtable.PartitionData newRef = new TrieMemtable.PartitionData(update, owner);
            this.heapSize += newRef.unsharedHeapSize();
            ++this.partitionsAdded;
            return newRef;
        }

        assert owner == existing.owner;
        if (update.isLive() || !update.mayModify(existing))
            return existing;

        // Note: Always on-heap, regardless of cloner
        TrieMemtable.PartitionData merged = new TrieMemtable.PartitionData(existing, update);
        this.heapSize += merged.unsharedHeapSize() - existing.unsharedHeapSize();
        return merged;
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

    @Override
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
