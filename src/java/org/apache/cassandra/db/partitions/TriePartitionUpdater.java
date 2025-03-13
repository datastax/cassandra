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

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
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
implements InMemoryBaseTrie.UpsertTransformerWithKeyProducer<Object, Object>
{
    private final UpdateTransaction indexer;
    private final TableMetadata metadata;
    private TrieMemtable.PartitionData currentPartition;
    private final TrieMemtable.MemtableShard owner;
    private ClusteringBound<byte[]> rangeTombstoneOpenPosition = null;
    private final DeletionTime partitionLevelDeletion;
    public int partitionsAdded = 0;

    public TriePartitionUpdater(Cloner cloner,
                                UpdateTransaction indexer,
                                DeletionTime partitionLevelDeletion,
                                TableMetadata metadata,
                                TrieMemtable.MemtableShard owner)
    {
        super(cloner);
        this.indexer = indexer;
        this.metadata = metadata;
        this.owner = owner;
        this.partitionLevelDeletion = partitionLevelDeletion;
        if (!partitionLevelDeletion.isLive())
            indexer.onPartitionDeletion(partitionLevelDeletion);
    }

    @Override
    public Object apply(@Nullable Object existing, Object update, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (update == TrieBackedPartition.PARTITION_MARKER)
            return mergePartitionMarkers((TrieMemtable.PartitionData) existing);
        else if (update instanceof RowData)
            return applyIncomingRow((RowData) existing, (RowData) update, keyState);
        else
            throw new AssertionError("Unexpected update type: " + update.getClass());
    }

    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update, InMemoryBaseTrie.KeyProducer<TrieTombstoneMarker> keyState)
    {
        if (indexer != UpdateTransaction.NO_OP)
        {
            if (update.hasPointData())
            {
                Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(
                    ByteArrayAccessor.instance,
                    ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                              keyState.getBytes()));
                if (existing != null)
                    indexer.onUpdated(BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(existing.deletionTime())),
                                      BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(update.deletionTime())));
                else
                    indexer.onInserted(BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(update.deletionTime())));
            }
            else if (update.isBoundary())
            {
                if (rangeTombstoneOpenPosition != null)
                {
                    // We have an active range. The incoming marker's left side (preceding in forward direction) must
                    // close it. Combine with the start position to form the tombstone range we report to the indexer.
                    TrieTombstoneMarker preceding = update.precedingState(Direction.FORWARD);
                    assert preceding != null; // open markers are always closed
                    DeletionTime deletionTime = preceding.deletionTime();
                    ClusteringBound<?> bound = metadata.comparator.boundFromByteComparable(
                        ByteArrayAccessor.instance,
                        ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                  keyState.getBytes()),
                        true);
                    indexer.onRangeTombstone(new RangeTombstone(Slice.make(rangeTombstoneOpenPosition,
                                                                           bound),
                                                                deletionTime));
                }

                // The right side (preceding in reverse direction) of the marker tells us if this boundary opens a new
                // deletion. If so, store the position to report the range when it closes.
                // Note: we don't need to save the deletion time as the closing side will repeat it.
                TrieTombstoneMarker succeeding = update.precedingState(Direction.REVERSE);
                // Ignore the partition deletion.
                if (succeeding != null && !succeeding.deletionTime().equals(partitionLevelDeletion))
                {
                    rangeTombstoneOpenPosition = metadata.comparator.boundFromByteComparable(
                        ByteArrayAccessor.instance,
                        ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                  keyState.getBytes()),
                        false);
                }
                else
                {
                    rangeTombstoneOpenPosition = null;
                }
            }
        }

        if (existing == null)
        {
            currentPartition.markAddedTombstones(1);
            this.heapSize += update.unsharedHeapSize();
            return update;
        }
        else
        {
            TrieTombstoneMarker merged = update.mergeWith(existing);
            this.heapSize += (merged != null ? merged.unsharedHeapSize() : 0) - existing.unsharedHeapSize();
            return merged;
        }
    }

    public Object applyIncomingMarker(Object existingContent, TrieTombstoneMarker updateMarker, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        assert existingContent instanceof RowData; // must be non-null, and can't be partition root
        RowData existing = (RowData) existingContent;
        RowData updated = existing.delete(updateMarker.deletionTime());
        if (updated != existing)
            this.heapSize += (updated != null ? updated.unsharedHeapSizeExcludingData() : 0) - existing.unsharedHeapSizeExcludingData();
        if (updated == null)
            currentPartition.markInsertedRows(-1);

        if (indexer != UpdateTransaction.NO_OP && updated != existingContent)
        {
            Clustering<?> clustering = clusteringFor(keyState);
            if (updated != null)
                indexer.onUpdated(existing.toRow(clustering, DeletionTime.LIVE),
                                  updated.toRow(clustering, DeletionTime.LIVE));
            else if (clustering != Clustering.STATIC_CLUSTERING)
                indexer.onUpdated(existing.toRow(clustering, DeletionTime.LIVE),
                                  BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(updateMarker.deletionTime())));
            else
                indexer.onPartitionDeletion(updateMarker.deletionTime()); // static clustering is deleted only on partition deletion
        }
        return updated;
    }

    public Object applyExistingMarkerToIncomingRow(TrieTombstoneMarker marker, Object content)
    {
        // This is called to apply an existing tombstone to incoming data, before applyRow is called on the result.
        // No size tracking is needed, because the result of this then gets applied to the trie with applyRow.
        assert content instanceof RowData; // must be non-null, and can't be partition root
        return ((RowData) content).delete(marker.deletionTime());
    }

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing RowData for this clustering, or null if there isn't any.
     * @param insert RowData to be inserted.
     * @param keyState Used to obtain the path through which this node was reached.
     * @return the insert row, or the merged row, copied using our allocator
     */
    private RowData applyIncomingRow(@Nullable RowData existing, RowData insert, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            RowData data = insert.clone(cloner);

            if (indexer != UpdateTransaction.NO_OP)
                indexer.onInserted(data.toRow(clusteringFor(keyState), DeletionTime.LIVE));

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
                indexer.onUpdated(existing.toRow(clustering, DeletionTime.LIVE),
                                  reconciled.toRow(clustering, DeletionTime.LIVE));
            }

            return reconciled;
        }
    }

    private RowData merge(RowData existing, RowData update)
    {

        LivenessInfo existingLiveness = existing.livenessInfo;
        LivenessInfo livenessInfo = LivenessInfo.merge(update.livenessInfo, existingLiveness);
        this.heapSize += livenessInfo.unsharedHeapSize() - existingLiveness.unsharedHeapSize();

        Object[] tree = BTreeRow.mergeRowBTrees(this,
                                                existing.columnsBTree, update.columnsBTree,
                                                DeletionTime.LIVE, DeletionTime.LIVE);
        return new RowData(tree, livenessInfo);
    }

    private Clustering<?> clusteringFor(InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        return metadata.comparator.clusteringFromByteComparable(
            ByteArrayAccessor.instance,
            ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                      keyState.getBytes(TrieBackedPartition.IS_PARTITION_BOUNDARY)));
    }

    /**
     * Called at the partition boundary to merge the existing and new metadata associated with the partition. This needs
     * to make sure that the statistics we track for the partition (dataSize) are updated for the changes caused by
     * merging the update's rows.
     *
     * @param existing Any partition data already associated with the partition.
     * @return the combined partition data, creating a new marker if one did not already exist.
     */
    private TrieMemtable.PartitionData mergePartitionMarkers(@Nullable TrieMemtable.PartitionData existing)
    {
        if (existing == null)
        {
            // Note: Always on-heap, regardless of cloner
            TrieMemtable.PartitionData newRef = new TrieMemtable.PartitionData(owner);
            this.heapSize += newRef.unsharedHeapSize();
            ++this.partitionsAdded;
            return currentPartition = newRef;
        }

        assert owner == existing.owner;
        return currentPartition = existing;
    }
}
