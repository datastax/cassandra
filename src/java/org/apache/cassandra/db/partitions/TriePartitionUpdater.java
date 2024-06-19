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
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.RowData;

/**
 *  The function we provide to the trie utilities to perform any partition and row inserts and updates
 */
public final class TriePartitionUpdater
extends BasePartitionUpdater
implements InMemoryTrie.UpsertTransformerWithKeyProducer<Object, Object>
{
    private final UpdateTransaction indexer;
    private final TableMetadata metadata;
    private TrieMemtable.PartitionData currentPartition;
    private final TrieMemtable.MemtableShard owner;
    public int partitionsAdded = 0;

    public TriePartitionUpdater(Cloner cloner,
                                UpdateTransaction indexer,
                                TableMetadata metadata,
                                TrieMemtable.MemtableShard owner)
    {
        super(cloner);
        this.indexer = indexer;
        this.metadata = metadata;
        this.owner = owner;
    }

    @Override
    public Object apply(Object existing, Object update, InMemoryTrie.KeyProducer<Object> keyState)
    {
        if (update instanceof RowData)
            return applyRow((RowData) existing, (RowData) update, keyState);
        else if (update instanceof DeletionInfo)
            return applyDeletion((TrieMemtable.PartitionData) existing, (DeletionInfo) update);
        else
            throw new AssertionError("Unexpected update type: " + update.getClass());
    }

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing RowData for this clustering, or null if there isn't any.
     * @param insert RowData to be inserted.
     * @param keyState Used to obtain the path through which this node was reached.
     * @return the insert row, or the merged row, copied using our allocator
     */
    private RowData applyRow(RowData existing, RowData insert, InMemoryTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            RowData data = insert.clone(cloner);

            if (indexer != UpdateTransaction.NO_OP)
                indexer.onInserted(data.toRow(clusteringFor(keyState)));

            this.dataSize += data.dataSize();
            this.heapSize += data.unsharedHeapSizeExcludingData();
            currentPartition.markInsertedRows(1);  // null pointer here means a problem in applyDeletion
            return data;
        }
        else
        {
            // data and heap size are updated during merge through the PostReconciliationFunction interface
            RowData reconciled = merge(existing, insert);

            if (indexer != UpdateTransaction.NO_OP)
            {
                Clustering<?> clustering = clusteringFor(keyState);
                indexer.onUpdated(existing.toRow(clustering), reconciled.toRow(clustering));
            }

            return reconciled;
        }
    }

    private RowData merge(RowData existing, RowData update)
    {

        LivenessInfo livenessInfo = LivenessInfo.merge(update.livenessInfo, existing.livenessInfo);
        DeletionTime deletion = DeletionTime.merge(update.deletion, existing.deletion);
        if (deletion.deletes(livenessInfo))
            livenessInfo = LivenessInfo.EMPTY;

        Object[] tree = BTreeRow.mergeRowBTrees(this,
                                                existing.columnsBTree, update.columnsBTree,
                                                deletion, existing.deletion);
        return new RowData(tree, livenessInfo, deletion);
    }

    private Clustering<?> clusteringFor(InMemoryTrie.KeyProducer<Object> keyState)
    {
        return metadata.comparator.clusteringFromByteComparable(
            ByteArrayAccessor.instance,
            ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                      keyState.getBytes(TrieMemtable.IS_PARTITION_BOUNDARY)));
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
    private TrieMemtable.PartitionData applyDeletion(TrieMemtable.PartitionData existing, DeletionInfo update)
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
            return currentPartition = newRef;
        }

        assert owner == existing.owner;
        if (update.isLive() || !update.mayModify(existing))
            return currentPartition = existing;

        // Note: Always on-heap, regardless of cloner
        TrieMemtable.PartitionData merged = new TrieMemtable.PartitionData(existing, update);
        this.heapSize += merged.unsharedHeapSize() - existing.unsharedHeapSize();
        return currentPartition = merged;
    }
}
