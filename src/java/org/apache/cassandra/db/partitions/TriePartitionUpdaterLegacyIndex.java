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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/// The function we provide to the trie utilities to perform any partition and row inserts and updates when a legacy
/// secondary index is in use. This builds on the plain [TriePartitionUpdater] and prepares legacy interpretations of
/// the changes for the indexing calls.
public final class TriePartitionUpdaterLegacyIndex extends TriePartitionUpdater
{
    private UpdateTransaction indexer;
    private TableMetadata metadata;
    /// When a tombstone boundary opens a range, we store the position here to report a range tombstone when it closes.
    private ClusteringBound<byte[]> rangeTombstoneOpenPosition;
    /// The depth at which we saw the root of the current partition. Used to obtain clustering keys of modified rows.
    private int currentPartitionDepth;

    public TriePartitionUpdaterLegacyIndex(TrieMemtable.MemtableShard owner,
                                           InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data,
                                           TableMetadata metadata)
    {
        super(owner, data);
        this.metadata = metadata;
    }

    /// Set the update transaction to use
    public void setIndexContext(UpdateTransaction indexer)
    {
        this.indexer = indexer;
        this.rangeTombstoneOpenPosition = null;
        assert indexer != UpdateTransaction.NO_OP;
    }

    @Override
    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update)
    {
        boolean existingIsRow = existing != null && existing.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW);
        boolean updateIsRow = update.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW);

        if (existingIsRow || updateIsRow)
            processRowDeletion(update, updateIsRow, existingIsRow);
        else if (update.isBoundary())
            processMarkerBoundary(update);

        return super.mergeMarkers(existing, update);
    }

    private void processRowDeletion(TrieTombstoneMarker update, boolean updateIsRow, boolean existingIsRow)
    {
        Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(
            ByteArrayAccessor.instance,
            byteComparableForCurrentDeletionBranchKey());

        Row updateRow;
        if (updateIsRow)
        {
            updateRow = TrieBackedRow.create(metadata,
                                             clustering,
                                             DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                                              TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                              mutator.getMutationDeletionTailTrie()));
        }
        else
        {
            DeletionTime rowDeletion = update.applicableToPointForward();
            if (rowDeletion != null)
                updateRow = TrieBackedRow.emptyDeletedRow(clustering, rowDeletion);
            else
                return; // nothing to issue as there is no row and no applicable deletion
        }

        if (existingIsRow)
        {
            Row existingRow = TrieBackedRow.create(metadata,
                                                   clustering,
                                                   DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                                                    TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                    mutator.getExistingDeletionTailTrie()));
            indexer.onUpdated(existingRow, updateRow);
        }
        else
            indexer.onInserted(updateRow);
    }

    private void processMarkerBoundary(TrieTombstoneMarker update)
    {
        TrieTombstoneMarker.Covering leftSide = update.leftDeletion();
        TrieTombstoneMarker.Covering rightSide = update.rightDeletion();
        TrieTombstoneMarker.Kind leftKind = leftSide != null ? leftSide.deletionKind() : null;
        TrieTombstoneMarker.Kind rightKind = rightSide != null ? rightSide.deletionKind() : null;

        assert leftKind != TrieTombstoneMarker.Kind.ROW && rightKind != TrieTombstoneMarker.Kind.ROW
            : "Row deletion without row level marker: " + update;

        // For range tombstones, we should only report when they start and stop. This means ignoring all switches
        // that include a lower-level change.
        if (leftKind == TrieTombstoneMarker.Kind.COLUMN || rightKind == TrieTombstoneMarker.Kind.COLUMN)
            return;
        // We should also skip the sides that switch to or from the partition deletion.
        if (leftKind == TrieTombstoneMarker.Kind.PARTITION)
            leftSide = null;
        if (rightKind == TrieTombstoneMarker.Kind.PARTITION)
            rightSide = null;

        if (leftSide != null || rightSide != null)
            processRangeTombstoneMarker(leftSide, rightSide);
    }

    private void processRangeTombstoneMarker(TrieTombstoneMarker.Covering leftSide, TrieTombstoneMarker.Covering rightSide)
    {
        ByteComparable deletionBranchKey = byteComparableForCurrentDeletionBranchKey();
        if (rangeTombstoneOpenPosition != null)
        {
            // We have an active range. The incoming marker's left side must close it. Combine with the start
            // position to form the tombstone range we report to the indexer.
            assert leftSide != null; // open markers are always closed
            ClusteringBound<?> bound = metadata.comparator.boundFromByteComparable(
                ByteArrayAccessor.instance,
                deletionBranchKey,
                true);
            indexer.onRangeTombstone(new RangeTombstone(Slice.make(rangeTombstoneOpenPosition,
                                                                   bound),
                                                        leftSide));
        }
        else
            assert leftSide == null;

        if (rightSide != null)
        {
            // The right side of the marker tells us if this boundary opens a new deletion. If so, store the
            // position to report the range when it closes.
            // Note: we don't need to save the deletion time as the closing side will repeat it.
            rangeTombstoneOpenPosition = metadata.comparator.boundFromByteComparable(
                ByteArrayAccessor.instance,
                deletionBranchKey,
                false);
        }
        else
            rangeTombstoneOpenPosition = null;
    }

    @Override
    public Object applyRowDeletion(LivenessInfo existing, DeletionTime deletion)
    {
        // Note: We only apply the full row deletion here (this may be the result of a range or partition tombstone or
        // a deletion of the specific row) to give the indexer the previous content of the row.
        // The incoming deletion may be more complex than just a row deletion. If that is the case, we will also
        // call indexer.onInserted with the full content of the deletion branch when the deletion branches are merged.
        // It is very hard to combine the data and deletion branches in one operation; on the other hand, the indexer
        // needs to be able to receive these updates separately because the database can receive them separately and
        // have, for example, the data written in one memtable and a deletion in another.
        Clustering<?> clustering = clusteringForCurrentKey();
        indexer.onUpdated(TrieBackedRow.create(metadata, clustering, mutator.getExistingTailTrie()),
                          TrieBackedRow.emptyDeletedRow(clustering, deletion));

        return super.applyRowDeletion(existing, deletion);
    }

    @Override
    Object applyPartitionDeletion(PartitionData existing, DeletionTime deletion)
    {
        indexer.onPartitionDeletion(deletion);
        return super.applyPartitionDeletion(existing, deletion);
    }

    @Override
    protected PartitionData mergePartitionMarkers(@Nullable PartitionData existing)
    {
        currentPartitionDepth = mutator.currentDepth();
        return super.mergePartitionMarkers(existing);
    }

    @Override
    LivenessInfo applyIncomingRowMarker(@Nullable LivenessInfo existing, LivenessInfo insert)
    {
        Clustering<?> clustering = clusteringForCurrentKey();
        if (existing == null)
        {
            indexer.onInserted(TrieBackedRow.create(metadata, clustering, mutator.getMutationTailTrie()));
        }
        else
        {
            indexer.onUpdated(TrieBackedRow.create(metadata, clustering, mutator.getExistingTailTrie()),
                              TrieBackedRow.create(metadata, clustering, mutator.getMutationTailTrie()));
        }
        return super.applyIncomingRowMarker(existing, insert);
    }

    private ByteComparable byteComparableForCurrentDeletionBranchKey()
    {
        return ByteComparable.preencoded(mutator.byteComparableVersion(),
                                         mutator.getDeletionBranchKeyBytes());
    }

    private Clustering<?> clusteringForCurrentKey()
    {
        return metadata.comparator.clusteringFromByteComparable(
            ByteArrayAccessor.instance,
            ByteComparable.preencoded(mutator.byteComparableVersion(),
                                      mutator.getCurrentKeyBytes(currentPartitionDepth)));
    }
}
