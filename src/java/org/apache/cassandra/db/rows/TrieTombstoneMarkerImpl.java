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

package org.apache.cassandra.db.rows;

import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// The implementation of trie tombstone markers.
///
/// To save some object creation, the `Covering` subtype extends `DeletionTime`, and the `Boundary` subtypes stores the
/// sides as instances of `Covering`.
interface TrieTombstoneMarkerImpl extends TrieTombstoneMarker
{
    Covering leftDeletion();
    Covering rightDeletion();

    static Covering covering(DeletionTime deletionTime)
    {
        return new Covering(deletionTime);
    }

    static Covering combine(Covering left, Covering right)
    {
        if (left == null)
            return right;
        if (right == null)
            return left;
        if (right.supersedes(left))
            return right;
        else
            return left;
    }

    static TrieTombstoneMarker make(Covering left, Covering right)
    {
        if (left == right) // includes both being null
            return left;

        if (left != null && left.equals(right))
            return left;

        return new Boundary(left, right);
    }

    static class Covering extends DeletionTime implements TrieTombstoneMarkerImpl
    {
        static final long HEAP_SIZE = ObjectSizes.measure(new Covering(DeletionTime.LIVE));

        private Covering(DeletionTime deletionTime)
        {
            super(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime());
        }

        private Covering(long markedForDeleteAt, int localDeletionTime)
        {
            super(markedForDeleteAt, localDeletionTime);
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator,
                                                           DeletionTime deletionToOmit)
        {
            throw new AssertionError("Covering trie tombstone cannot be converted to a RangeTombstoneMarker");
        }

        @Override
        public Covering leftDeletion()
        {
            return this;
        }

        @Override
        public Covering rightDeletion()
        {
            return this;
        }

        @Override
        public boolean hasPointData()
        {
            return false;
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker other)
        {
            if (other instanceof Boundary)
                return other.mergeWith(this);
            if (other instanceof Point)
                return other.mergeWith(this);

            return combine(this, (Covering) other);
        }

        @Override
        public Covering withUpdatedTimestamp(long newTimestamp)
        {
            return new Covering(newTimestamp, localDeletionTime());
        }

        @Override
        public boolean isBoundary()
        {
            return false;
        }

        @Override
        public TrieTombstoneMarker precedingState(Direction direction)
        {
            return this;
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            throw new AssertionError("Restrict is only applicable to boundary markers");
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            return direction.isForward() ? new Boundary(null, this) : new Boundary(this, null);
        }

        @Override
        public TrieTombstoneMarker asPoint()
        {
            return new Point(this, null);
        }

        @Override
        public DeletionTime deletionTime()
        {
            return this;
        }

        @Override
        public long unsharedHeapSize()
        {
            // Note: HEAP_SIZE is used directly by Point and Boundary. Make sure to apply any changes there too.
            return HEAP_SIZE;
        }
    }

    static class Boundary implements TrieTombstoneMarkerImpl
    {
        // Every boundary contains one side of a deletion, and for simplicity we assume that any covering deletion we
        // interrupt is already accounted for by its end boundaries, so with every new Boundary we add this object's
        // size plus one half of a Covering.
        static final long UNSHARED_HEAP_SIZE =
            ObjectSizes.measure(new Boundary(new Covering(0, 0), null)) +
            Covering.HEAP_SIZE / 2;

        final @Nullable Covering leftDeletion;
        final @Nullable Covering rightDeletion;

        private Boundary(@Nullable Covering left, @Nullable Covering right)
        {
            assert left != null || right != null;
            assert left == null || !left.isLive();
            assert right == null || !right.isLive();
            this.leftDeletion = left;
            this.rightDeletion = right;
        }

        @Override
        public DeletionTime deletionTime()
        {
            // Report the higher deletion, to avoid dropping the other side of boundaries that switch to any omitted
            // deletion time.
            if (leftDeletion == null)
                return rightDeletion;
            if (rightDeletion == null)
                return leftDeletion;
            return rightDeletion.supersedes(leftDeletion) ? rightDeletion : leftDeletion;
        }

        @Override
        public boolean hasPointData()
        {
            return false;
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator,
                                                           DeletionTime deletionToOmit)
        {
            assert byteComparableVersion == ByteComparable.Version.OSS50;
            if (leftDeletion == null || leftDeletion.equals(deletionToOmit))
            {
                if (rightDeletion == null || rightDeletion.equals(deletionToOmit))
                    return null;
                else
                    return new RangeTombstoneBoundMarker(comparator.boundFromByteComparable(ByteArrayAccessor.instance,
                                                                                            clusteringPrefixAsByteComparable,
                                                                                            false),
                                                         rightDeletion);
            }

            if (rightDeletion == null || rightDeletion.equals(deletionToOmit))
                return new RangeTombstoneBoundMarker(comparator.boundFromByteComparable(ByteArrayAccessor.instance,
                                                                                        clusteringPrefixAsByteComparable,
                                                                                        true),
                                                     leftDeletion);

            return new RangeTombstoneBoundaryMarker(comparator.boundaryFromByteComparable(ByteArrayAccessor.instance,
                                                                                          clusteringPrefixAsByteComparable),
                                                    leftDeletion,
                                                    rightDeletion);
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing)
        {
            if (existing == null)
                return this;

            assert !existing.hasPointData() : "Boundary cannot be merged with point deletion";
            TrieTombstoneMarkerImpl other = (TrieTombstoneMarkerImpl) existing;
            Covering otherLeft = other.leftDeletion();
            Covering newLeft = combine(leftDeletion, otherLeft);
            Covering otherRight = other.rightDeletion();
            Covering newRight = combine(rightDeletion, otherRight);
            if (leftDeletion == newLeft && rightDeletion == newRight)
                return this;
            if (otherLeft == newLeft && otherRight == newRight)
                return other;
            return make(newLeft, newRight);
        }

        @Override
        public TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp)
        {
            Covering newLeft = leftDeletion != null ? leftDeletion.withUpdatedTimestamp(newTimestamp) : null;
            Covering newRight = rightDeletion != null ? rightDeletion.withUpdatedTimestamp(newTimestamp) : null;
            if (Objects.equals(newLeft, newRight))
                return null;
            return new Boundary(newLeft, newRight);
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public TrieTombstoneMarker precedingState(Direction dir)
        {
            return dir.isForward() ? leftDeletion : rightDeletion;
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            if (!applicableAfter && leftDeletion == null || !applicableBefore && rightDeletion == null)
                return null;
            if (applicableBefore && applicableAfter)
                return this;
            return new Boundary(applicableBefore ? leftDeletion : null,
                                applicableAfter ? rightDeletion : null);
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            throw new AssertionError("Already a boundary");
        }

        @Override
        public Covering leftDeletion()
        {
            return leftDeletion;
        }

        @Override
        public Covering rightDeletion()
        {
            return rightDeletion;
        }

        @Override
        public String toString()
        {
            return (leftDeletion != null ? leftDeletion : "LIVE") + " -> " + (rightDeletion != null ? rightDeletion : "LIVE");
        }

        @Override
        public long unsharedHeapSize()
        {
            return UNSHARED_HEAP_SIZE;
        }
    }

    /// Point deletion. Marks a deletion at the lowest points of the represented data hierarchy where no further
    /// complexity can exist below the marked point to improve efficiency compared to bracketing the point with
    /// boundaries on both sides.
    ///
    /// Both sides of a point deletion are the same (null if no covering deletion applies), and the point deletion
    /// applies only to the exact position of the marker (i.e. if there is substructure, this deletion will not be
    /// covering for the branch). `isBoundary` returns true even though the applicable covering deletion does not
    /// change, because the point must be reported as content.
    static class Point implements TrieTombstoneMarkerImpl
    {
        // Every point deletion introduces a new deletion time. If it interrupts an existing deletion, it will reuse
        // the Covering object provided by its end bounds. Thus, the unshared size is this object + the size of
        // one Covering.
        static final long UNSHARED_HEAP_SIZE = ObjectSizes.measure(new Point(new Covering(0, 0),
                                                                             null)) +
                                               Covering.HEAP_SIZE;

        final @Nullable Covering coveringDeletion;
        final Covering pointDeletion;

        public Point(Covering pointDeletion, @Nullable Covering coveringDeletion)
        {
            assert pointDeletion != null;
            this.coveringDeletion = coveringDeletion;
            this.pointDeletion = pointDeletion;
        }

        @Override
        public Covering leftDeletion()
        {
            return coveringDeletion;
        }

        @Override
        public Covering rightDeletion()
        {
            return coveringDeletion;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return pointDeletion;
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator,
                                                           DeletionTime deletionToOmit)
        {
            throw new AssertionError("Point trie tombstone cannot be converted to a RangeTombstoneMarker");
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing)
        {
            if (existing == null)
                return this;

            if (existing instanceof Covering)
            {
                Covering existingCovering = (Covering) existing;
                if (!pointDeletion.supersedes(existingCovering))
                {
                    if (coveringDeletion == null || !coveringDeletion.supersedes(existingCovering))
                        return existingCovering; // This point is fully superseded/deleted.
                    else
                        return coveringDeletion;
                }

                Covering newCovering = combine(coveringDeletion, existingCovering);
                if (newCovering == coveringDeletion)
                    return this;
                else
                    return new Point(pointDeletion, newCovering);
            }
            else if (existing instanceof Point)
            {
                Point existingPoint = (Point) existing;
                Covering newCovering = combine(coveringDeletion, existingPoint.coveringDeletion);
                Covering newPoint = combine(pointDeletion, existingPoint.pointDeletion);
                if (newCovering == coveringDeletion && newPoint == pointDeletion)
                    return this;
                if (newCovering == existingPoint.coveringDeletion && newPoint == existingPoint.pointDeletion)
                    return existingPoint;

                return new Point(newPoint, newCovering);
            }
            else
                throw new AssertionError("Boundaries cannot be positioned on row clusterings.");
        }

        @Override
        public boolean hasPointData()
        {
            return true;
        }

        @Override
        public TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp)
        {
            if (coveringDeletion != null)
                return new Covering(newTimestamp, coveringDeletion.localDeletionTime()); // subsumed by range deletion
            return new Point(new Covering(newTimestamp, pointDeletion.localDeletionTime()), null);
        }

        @Override
        public boolean isBoundary()
        {
            // Must be reported.
            return true;
        }

        @Override
        public TrieTombstoneMarker precedingState(Direction direction)
        {
            return coveringDeletion;
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            throw new AssertionError("Cannot have a row clustering as slice bound.");
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            throw new AssertionError("Cannot have a row clustering as slice bound.");
        }

        @Override
        public String toString()
        {
            return pointDeletion + (coveringDeletion != null ? "(under " + coveringDeletion + ")" : "");
        }

        @Override
        public long unsharedHeapSize()
        {
            return UNSHARED_HEAP_SIZE;
        }
    }
}
