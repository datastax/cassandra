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
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;

/// An [UnfilteredRowIterator] that recombines sequences of range tombstones for the same key into deleted rows.
///
/// The objective of this class is to reverse the transformation made by [TriePartitionUpdate] that implements row
/// deletions as pairs of tombstones around the row (which are then placed in the deletion branch of the trie).
/// This transformation is valid, but a lot of tests rely on row deletions being represented by empty deleted rows.
/// For the time being we thus do the reverse transformation on conversion from trie to unfiltered iterator.
class RecombiningUnfilteredRowIterator extends WrappingUnfilteredRowIterator
{
    Unfiltered bufferedOne;
    Unfiltered bufferedTwo;
    Unfiltered next;
    boolean nextPrepared;

    protected RecombiningUnfilteredRowIterator(UnfilteredRowIterator wrapped)
    {
        super(wrapped);
        bufferedOne = null;
        nextPrepared = false;
    }

    @Override
    public boolean hasNext()
    {
        return computeNext() != null;
    }

    @Override
    public Unfiltered next()
    {
        Unfiltered item = computeNext();
        nextPrepared = false;
        return item;
    }

    private Unfiltered computeNext()
    {
        if (nextPrepared)
            return next;

        // If we have two buffered entries, report the first one directly (there's no need to process it as we already
        // know it is a row) and shift the second one to the first position.
        if (bufferedTwo != null)
        {
            Unfiltered unfiltered2 = bufferedTwo;
            bufferedTwo = null;
            return setNextAndBufferedAndReturn(bufferedOne, unfiltered2);
        }

        // If we have a buffered entry, use it for the following processing, otherwise get one from the source.
        Unfiltered unfiltered1;
        if (bufferedOne != null)
        {
            unfiltered1 = bufferedOne;
            bufferedOne = null;
        }
        else
        {
            if (!wrapped.hasNext())
                return setNextAndReturn(null);

            unfiltered1 = wrapped.next();
        }

        // The pattern we are looking for is
        //   open_incusive(clustering, del) + row(clustering) + close_inclusive(clustering, del)
        // where the row is optional

        if (unfiltered1.isRow())
            return setNextAndReturn(unfiltered1);

        RangeTombstoneMarker marker1 = (RangeTombstoneMarker) unfiltered1;
        boolean reversed = isReverseOrder();
        int clusteringSize = metadata().comparator.size();
        // The first marker must be open, inclusive, and a fully specified clustering.
        if (!marker1.isOpen(reversed)
            || !marker1.openIsInclusive(reversed)
            || marker1.clustering().size() != clusteringSize
            || (clusteringSize > 0 && marker1.clustering().get(clusteringSize - 1) == null))
            return setNextAndReturn(marker1);

        if (!wrapped.hasNext())
            return setNextAndReturn(marker1);

        Unfiltered unfiltered2 = wrapped.next();
        final DeletionTime deletionTime = marker1.openDeletionTime(reversed);
        if (unfiltered2.isRangeTombstoneMarker())
        {
            RangeTombstoneMarker marker2 = (RangeTombstoneMarker) unfiltered2;
            assert marker2.isClose(reversed);
            assert marker2.closeDeletionTime(reversed).equals(deletionTime);
            if (!marker2.closeIsInclusive(reversed) || !clusteringPositionsEqual(marker1, marker2))
                return setNextAndBufferedAndReturn(marker1, marker2);

            // The recombination applies. We have to transform the open side of marker1 and the close side
            // of marker2 into an empty row with deletion time.
            return processOtherSidesAndReturn(BTreeRow.emptyDeletedRow(clusteringPositionOf(marker1), Row.Deletion.regular(deletionTime)),
                                              reversed, marker1, marker2, deletionTime);
        }

        BTreeRow row2 = (BTreeRow) unfiltered2;

        if (!clusteringPositionsEqual(marker1, row2))
            return setNextAndBufferedAndReturn(marker1, row2);

        if (!wrapped.hasNext())
            return setNextAndBufferedAndReturn(marker1, row2);

        Unfiltered unfiltered3 = wrapped.next();
        if (unfiltered3.isRow())
            return setNextAndBufferedAndReturn(marker1, row2, unfiltered3);

        RangeTombstoneMarker marker3 = (RangeTombstoneMarker) unfiltered3;
        assert marker3.isClose(reversed);
        assert marker3.closeDeletionTime(reversed).equals(deletionTime);
        if (!marker3.closeIsInclusive(reversed) || !clusteringPositionsEqual(marker1, marker3))
            return setNextAndBufferedAndReturn(marker1, row2, marker3);

        // The recombination applies. We have to transform the open side of marker1 and the close side
        // of marker3 into a deletion time for row2.
        return processOtherSidesAndReturn(BTreeRow.create(row2.clustering(), row2.primaryKeyLivenessInfo(), Row.Deletion.regular(deletionTime), row2.getBTree()),
                                          reversed, marker1, marker3, deletionTime);
    }

    private Unfiltered processOtherSidesAndReturn(Row row,
                                                  boolean reversed,
                                                  RangeTombstoneMarker markerLeft,
                                                  RangeTombstoneMarker markerRight,
                                                  DeletionTime deletionTime)
    {
        // Check if any of the markers is a boundary, and if so, report the other side.
        if (!markerLeft.isClose(reversed))
        {
            if (!markerRight.isOpen(reversed))
                return setNextAndReturn(row);

            return setNextAndBufferedAndReturn(row,
                                               ((RangeTombstoneBoundaryMarker) markerRight).createCorrespondingOpenMarker(reversed));
        }

        if (!markerRight.isOpen(reversed))
            return setNextAndBufferedAndReturn(((RangeTombstoneBoundaryMarker) markerLeft).createCorrespondingCloseMarker(reversed),
                                               row);

        // We have surviving markers on both sides.
        final DeletionTime closeDeletionTime = markerLeft.closeDeletionTime(reversed);
        if (markerRight.openDeletionTime(reversed).equals(closeDeletionTime) && !closeDeletionTime.supersedes(deletionTime))
        {
            // The row interrupts a covering deletion, we can still drop both markers and report a deleted row.
            return setNextAndReturn(row);
        }

        return setNextAndBufferedAndReturn(((RangeTombstoneBoundaryMarker) markerLeft).createCorrespondingCloseMarker(reversed),
                                           row,
                                           ((RangeTombstoneBoundaryMarker) markerRight).createCorrespondingOpenMarker(reversed));
    }

    private Unfiltered setNextAndReturn(Unfiltered next)
    {
        this.next = next;
        this.nextPrepared = true;
        return next;
    }

    private Unfiltered setNextAndBufferedAndReturn(Unfiltered next, Unfiltered bufferedOne)
    {
        this.bufferedOne = bufferedOne;
        return setNextAndReturn(next);
    }

    private Unfiltered setNextAndBufferedAndReturn(Unfiltered next, Row bufferedOne, Unfiltered bufferedTwo)
    {
        this.bufferedTwo = bufferedTwo;
        return setNextAndBufferedAndReturn(next, bufferedOne);
    }

    static boolean clusteringPositionsEqual(Unfiltered l, Unfiltered r)
    {
        return clusteringPositionsEqual(l.clustering(), r.clustering());
    }

    static <L, R> boolean clusteringPositionsEqual(ClusteringPrefix<L> cl, ClusteringPrefix<R> cr)
    {
        if (cl.size() != cr.size())
            return false;
        for (int i = cl.size() - 1; i >= 0; --i)
            if (cl.accessor().compare(cl.get(i), cr.get(i), cr.accessor()) != 0)
                return false;
        return true;
    }

    static Clustering<?> clusteringPositionOf(Unfiltered unfiltered)
    {
        return clusteringPositionOf(unfiltered.clustering());
    }

    static <V> Clustering<V> clusteringPositionOf(ClusteringPrefix<V> prefix)
    {
        return prefix.accessor().factory().clustering(prefix.getRawValues());
    }
}
