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

package org.apache.cassandra.db.tries;

/// The implementation of a [TrieSet].
///
/// In addition to the functionality of normal trie cursors, set cursors also produce a [#state] that describes the
/// coverage of trie sections to the left, right and below the cursor position. This is necessary to be able to identify
/// coverage after a [#skipTo] operation, where the set cursor jumps to a position beyond the requested one.
interface TrieSetCursor extends Cursor<TrieSetCursor.RangeState>
{
    /// This type describes the state at a given cursor position. It describes the coverage of the positions before and
    /// after the current in forward order, whether the node is boundary (and thus applies to this point and all its
    /// descendants) and also describes the type of boundary (e.g. start/end).
    enum RangeState
    {
        // Note: the states must be ordered so that
        //   `values()[applicableBefore * 1 + applicableAfter * 2 + applicableAtPoint * 4]`
        // produces a state with the requested flags

        /// The cursor is at a prefix of a contained range, and neither the branches to the left or right are contained.
        START_END_PREFIX(false, false, false),
        /// The cursor is positioned at a prefix of an end boundary, inside a covered range on the left.
        END_PREFIX(true, false, false),
        /// The cursor is positioned at a prefix of a start boundary. The branches to the right are covered.
        START_PREFIX(false, true, false),
        /// The cursor is positioned inside a covered range, on a prefix of an excluded sub-range.
        END_START_PREFIX(true, true, false),
        /// The cursor is positioned at a "point" boundary, i.e. only the descendants of the boundary are covered,
        /// branches to the left or right are not contained.
        POINT(false, false, true),
        /// The cursor is positioned at an end boundary. Branches to the left, as well as descendants of this point are
        /// covered by the set.
        END(true, false, true),
        /// The cursor is positioned at a start boundary. Branches to the right, as well as descendants of this point
        /// are covered by the set.
        START(false, true, true),
        /// The cursor is positioned at a non-effective boundary (an end boundary for the previous range, as well as
        /// a start for the next). Branches before, after and below this point is covered.
        COVERED(true, true, true);

        /// Whether the set applied to positions before the cursor's in forward order.
        final boolean applicableBefore;
        /// Whether the set applied to positions after the cursor's in forward order.
        final boolean applicableAfter;
        /// The state to report as content. This converts prefix states to null to report only the boundaries
        /// (e.g. for dumping to text).
        final RangeState asContent;

        RangeState(boolean applicableBefore, boolean applicableAfter, boolean applicableAtPoint)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAfter = applicableAfter;
            this.asContent = applicableAtPoint ? this : null;
        }

        /// Whether the positions preceding the current in iteration order are included in the set.
        public boolean precedingIncluded(Direction direction)
        {
            return direction.select(applicableBefore, applicableAfter);
        }

        /// Whether the descendant branch is fully included in the set.
        public boolean branchIncluded()
        {
            return asContent != null;
        }

        public RangeState toContent()
        {
            return asContent;
        }

        /// Return an "intersection" state for the combination of two states, i.e. the ranges covered by both states.
        public RangeState intersect(RangeState other)
        {
            return values()[ordinal() & other.ordinal()];
        }

        /// Return a "union" state for the combination of two states, i.e. the ranges covered by at least one of the states.
        public RangeState union(RangeState other)
        {
            return values()[ordinal() | other.ordinal()];
        }

        /// Return the "weakly negated" state, i.e. the state that corresponds to flipped areas of coverage to the left
        /// and right, and the boundary points. See [TrieSet#weakNegation] for more details.
        public RangeState weakNegation()
        {
            return values()[ordinal() ^ 3];
        }

        public static RangeState fromProperties(boolean applicableBefore, boolean applicableAfter, boolean applicableAtPoint)
        {
            return values()[(applicableBefore ? 1 : 0) + (applicableAfter ? 2 : 0) + (applicableAtPoint ? 4 : 0)];
        }
    }

    /// The range state of the trie cursor at this point.
    RangeState state();

    /// Returns whether the set includes the positions before the current in iteration order, but after any earlier
    /// position of this cursor, including any position requested by a [#skipTo] call, where this cursor advanced beyond
    /// that position.
    ///
    /// Note that this may also be true when the cursor is in an exhausted state, as well as immediately
    /// after cursor construction, signifying, respectively, right and left unbounded ranges.
    default boolean precedingIncluded()
    {
        return state().precedingIncluded(direction());
    }

    /// Returns whether the set fully includes all descendants of the current position. This is true for all boundary
    /// points.
    default boolean branchIncluded()
    {
        return state().asContent != null;
    }

    @Override
    default RangeState content()
    {
        return state().toContent();
    }

    @Override
    TrieSetCursor tailCursor(Direction direction);
}
