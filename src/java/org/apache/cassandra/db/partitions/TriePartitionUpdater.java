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

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/// The function we provide to the trie utilities to perform any partition and row inserts and updates.
/// This version is used when no secondary index is applied, which makes the process quite a bit simpler.
public class TriePartitionUpdater
{
    private final TrieMemtable.MemtableShard owner;
    protected final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

    private Cloner cloner;
    /// Holds a reference to the current partition's statistics, used to update them when merging data.
    private PartitionData currentPartition;

    public long dataSize;
    public long heapSize;
    public long colUpdateTimeDelta;
    public int partitionsAdded;

    public TriePartitionUpdater(TrieMemtable.MemtableShard owner,
                                InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this.owner = owner;
        this.mutator = data.mutator(this::mergeData,
                                    this::mergeMarkers,
                                    this::applyIncomingMarker,
                                    this::applyExistingMarkerToIncomingRow,
                                    true,
                                    TrieMemtable.FORCE_COPY_PARTITION_BOUNDARY,
                                    x -> { throw new AssertionError("Force copy should already be in effect for all range tries"); },
                                    TrieBackedRow::isDroppableMarker,
                                    TrieBackedRow::isDroppableMarker);
    }

    /// Merge the given update into the data trie.
    public void mergeUpdate(Cloner cloner, DeletionAwareTrie<Object, TrieTombstoneMarker> update) throws TrieSpaceExhaustedException
    {
        this.cloner = cloner;
        this.currentPartition = null;
        this.partitionsAdded = 0;
        this.dataSize = 0;
        this.heapSize = 0;
        this.colUpdateTimeDelta = Long.MAX_VALUE;

        mutator.apply(update);
    }

    /// Merge incoming live data (cell, liveness info or various level markers) with existing content.
    Object mergeData(@Nullable Object existing, Object update)
    {
        // Most common case first
        if (update instanceof Cell)
            return applyCell((Cell<?>) existing, (Cell<?>) update);
        else if (update == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return update;
        else if (update instanceof LivenessInfo)
            return applyIncomingRowMarker((LivenessInfo) existing, (LivenessInfo) update);
        else if (update == TrieBackedPartition.PARTITION_MARKER)
            return mergePartitionMarkers((PartitionData) existing);
        else
            throw new AssertionError("Unexpected update type: " + update.getClass());
    }

    /// Merge an incoming tombstone with existing deletions.
    /// This will be called for all boundary tombstones in the update, but also for all existing boundaries that are
    /// covered by an incoming range.
    TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update)
    {
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

    /// Apply an incoming tombstone to existing data, possibly removing it from the trie.
    Object applyIncomingMarker(Object existingContent, TrieTombstoneMarker updateMarker)
    {
        DeletionTime deletion = updateMarker.applicableToPointForward();
        if (deletion == null)
            return existingContent;

        // Most common case first
        if (existingContent instanceof Cell)
            return applyCellDeletion((Cell<?>) existingContent, deletion);
        else if (existingContent == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return existingContent;
        else if (existingContent instanceof LivenessInfo)
            return applyRowDeletion((LivenessInfo) existingContent, deletion);
        else if (existingContent instanceof PartitionData)
            return applyPartitionDeletion((PartitionData) existingContent, deletion);
        else
            throw new AssertionError("Unexpected content in trie " + existingContent + " for deletion " + updateMarker);
    }

    private Cell<?> applyCellDeletion(Cell<?> existingContent, DeletionTime deletion)
    {
        if (!deletion.deletes(existingContent))
            return existingContent;
        heapSize -= existingContent.unsharedHeapSizeExcludingData();
        dataSize -= existingContent.dataSize();
        return null;
    }

    Object applyPartitionDeletion(PartitionData existing, DeletionTime unused)
    {
        existing.clearStats();
        return existing;
    }

    Object applyRowDeletion(LivenessInfo existing, DeletionTime deletion)
    {
        if (deletion.deletes(existing))
        {
            this.heapSize -= existing.unsharedHeapSize();
            return LivenessInfo.EMPTY;
            // TODO: and also do currentPartition.markInsertedRows(-1) in that case?
            // TODO: Does strict row liveness apply here? How do we drop tail trie if it does?
        }
        return existing;
    }

    /// Apply an existing tombstone to incoming data before merging that data in the trie.
    Object applyExistingMarkerToIncomingRow(TrieTombstoneMarker marker, Object content)
    {
        DeletionTime rowDeletion = marker.applicableToPointForward();
        if (rowDeletion == null)
            return content; // there is no row deletion here

        // No size tracking is needed, because the result of this gets applied to the trie with applyRow.
        if (content instanceof Cell)
            return rowDeletion.deletes((Cell<?>) content) ? null : content;
        else if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return content;
        else if (content instanceof LivenessInfo)
        {
            if (!rowDeletion.deletes((LivenessInfo) content))
                return content;
            else
                return LivenessInfo.EMPTY;
        }
        else if (content instanceof PartitionData)
            return content;
        else
            throw new AssertionError("Unexpected content in trie " + content + " for deletion " + marker);
    }

    LivenessInfo applyIncomingRowMarker(@Nullable LivenessInfo existing, LivenessInfo insert)
    {
        if (existing == null)
        {
            this.dataSize += insert.dataSize();
            this.heapSize += insert.unsharedHeapSize();
            currentPartition.markInsertedRows(1);  // null pointer here means a problem in applyDeletion
            return insert;
        }
        else
        {
            LivenessInfo reconciled = LivenessInfo.merge(existing, insert);
            if (reconciled != existing)
            {
                this.dataSize += reconciled.dataSize() - existing.dataSize();
                this.heapSize += reconciled.unsharedHeapSize() - existing.unsharedHeapSize();
            }
            return reconciled;
        }
    }

    Cell<?> applyCell(@Nullable Cell<?> existing, Cell<?> update)
    {
        if (existing == null)
        {
            if (cloner != null)
                update = cloner.clone(update);
            this.dataSize += update.dataSize();
            this.heapSize += update.unsharedHeapSizeExcludingData();
            return update;
        }
        else
        {
            Cell<?> reconciled = Cells.reconcile(existing, update);
            if (reconciled != existing)
            {
                long timeDelta = Math.abs(reconciled.timestamp() - existing.timestamp());
                if (timeDelta < colUpdateTimeDelta)
                    colUpdateTimeDelta = timeDelta;
                if (cloner != null)
                    reconciled = cloner.clone(reconciled);
                this.dataSize += reconciled.dataSize() - existing.dataSize();
                this.heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
            }
            return reconciled;
        }
    }

    /// Called at the partition boundary to merge the existing and new metadata associated with the partition. This needs
    /// to make sure that the statistics we track for the partition (dataSize) are updated for the changes caused by
    /// merging the update's rows.
    ///
    /// @param existing Any partition data already associated with the partition.
    /// @return the combined partition data, creating a new marker if one did not already exist.
    protected PartitionData mergePartitionMarkers(@Nullable PartitionData existing)
    {
        if (existing == null)
        {
            // Note: Always on-heap, regardless of cloner
            PartitionData newRef = new PartitionData(owner);
            this.heapSize += newRef.unsharedHeapSize();
            ++this.partitionsAdded;
            return currentPartition = newRef;
        }

        assert owner == existing.owner;
        return currentPartition = existing;
    }
}
