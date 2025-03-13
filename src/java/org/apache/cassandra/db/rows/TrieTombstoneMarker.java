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

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.tries.RangeState;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A tombstone marker used in trie-backed structures. Unlike [RangeTombstoneMarker], this does not include a position
/// in the clustering order, only deletion times.
/// There are two kinds of trie markers:
/// - covering markers, which have a deletion time applicable to some position that is not a boundary in the deletions
///   branch,
/// - boundary markers, which switch from one deletion time to a different one (either of which may be null).
///
/// See [RangeState] for further explanation of the types of markers.
///
/// In addition to these, we have a special form of boundary for point deletions, where the left and right side are the
/// same (often null), but there is a deletion time applicable to the exact point. These are used to mark deletions at
/// the lowest points of the represented data hierarchy where no further complexity can exist below the marked point
/// (currently point markers are used to mark deleted rows, which is the lowest level reached by `TrieMemtable`) to
/// improve efficiency compared to bracketing the point with boundaries on both sides.
public interface TrieTombstoneMarker extends RangeState<TrieTombstoneMarker>, IMeasurableMemory
{
    /// Returns the deletion time applicable at this point. Normally this would be called on non-boundary states only,
    /// but generally makes sense on any boundary as the boundary itself is included.
    DeletionTime deletionTime();

    /// Converts this marker to [RangeTombstoneMarker], assigning it a clustering position from its byte-comparable
    /// path in the trie. This is only applicable to boundary markers and will throw if called on covering ones.
    ///
    /// The given `deletionToOmit` can be used to omit deletion times that are already covered by some higher-level
    /// marker (e.g. partition deletion).
    RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                ByteComparable.Version byteComparableVersion,
                                                ClusteringComparator comparator,
                                                DeletionTime deletionToOmit);


    /// Combine two markers and return the applicable combined state, obtained by getting the higher of the deletion
    /// times on both sides of the marker. For boundaries this may result in a covering state (when both sides become
    /// equal) which is not stored or reported.
    TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing);

    /// Returns true if this marker contains a point deletion. The point deletion applies only to the position of the
    /// marker, and it only makes sense for points on the lowest level of the trie hierarchy.
    /// See [TrieTombstoneMarkerImpl.Point] for further details.
    boolean hasPointData();

    static TrieTombstoneMarker covering(DeletionTime deletionTime)
    {
        return TrieTombstoneMarkerImpl.covering(deletionTime);
    }

    TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp);
}
