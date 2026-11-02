/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A tail of an on-disk range trie: a range cursor rooted at a node inside the trie, as [RangeCursor#tailCursor]
/// returns it. Deletion ranges active where the walk enters the node's branch and where it leaves it must be presented
/// by the tail at its root, so the tail presents there, on each side, the node's own content restricted to the branch,
/// or else the range open on that side as a boundary, in place of what the node itself holds.
///
/// The tail works that content out itself once it is positioned on its root, reading the file with its own rebufferer:
/// the cursor it is taken from may have been closed and have given its buffer back (see [Cursor#close]).
class OnDiskRangeTailCursor<S extends RangeState<S>> extends OnDiskBaseRangeCursor<S>
{
    /// Where the backtrack entry that presents [#rootAscentContent] points. No read of the file produces it --
    /// [#readContentLength] keeps every item's start at or after 0 -- so every other read goes to the file, and one
    /// that is corrupt, at position 0 included, is rejected as it is in any other cursor.
    private static final long ROOT_ASCENT_CONTENT_POS = -1;

    /// The content presented on the root's return path in place of the node's own, if not null. Worked out once the
    /// tail is positioned on its root, and set before the backtrack entry that presents it is recorded.
    private final S rootAscentContent;

    /// A tail rooted at the node given as the pair that identifies it (see [#currentFullNodePostCodePos]).
    ///
    /// When `precedingStateKnown` is set, `precedingState` is the range state active before the root in `direction`,
    /// as the cursor the tail is taken from has already worked it out; it applies only if the root has no content of
    /// its own to present first.
    OnDiskRangeTailCursor(DataDeserializer<S> deserializer,
                          RebuffererFactory rebuffererFactory,
                          ByteComparable.Version byteComparableVersion,
                          Direction direction,
                          long rootPostCodePos,
                          int rootNodeCode,
                          boolean precedingStateKnown,
                          S precedingState)
    {
        // The superclass leaves the descent to us, so that getContentAtPos below is never called from inside its
        // constructor, and only answers for ROOT_ASCENT_CONTENT_POS once rootAscentContent has been set.
        super(deserializer, rebuffererFactory, byteComparableVersion, direction);
        try
        {
            descendInto(Cursor.rootPosition(direction), rootPostCodePos, rootNodeCode);
            S rootDescentContent = getTailRootContent(direction, content, precedingStateKnown, precedingState);
            rootAscentContent = getTailRootContent(direction.opposite(), rootReturnPathContent(), false, null);

            // Present those in place of the root's own: drop the backtrack entry the root may have recorded for its
            // return path, and record one for rootAscentContent instead.
            stackLength = 0;
            content = rootDescentContent;
            currentEncodedPosition = rootDescentContent != null
                                     ? currentEncodedPosition | MAY_HAVE_CONTENT_BIT
                                     : currentEncodedPosition & ~MAY_HAVE_CONTENT_BIT;
            if (rootAscentContent != null)
                OnDiskReadNodeType.addReturnPathBacktrack(this, ROOT_ASCENT_CONTENT_POS);
        }
        catch (Throwable t)
        {
            releaseBuffers();
            throw t;
        }
        // The state follows from the content set above, not from the node's own: with no root content the tail
        // starts with no active range.
        initActiveState();
    }

    /// The content the tail presents on one side of its root: the root's own content on that side, restricted to the
    /// tail's branch, or else the range that is open there, as a boundary. `activeRangeKnown` says whether
    /// `activeRange` already holds the range state active before the root in `direction`; if not, it is worked out from
    /// the nearest content.
    private S getTailRootContent(Direction direction, S contentAtRoot, boolean activeRangeKnown, S activeRange)
    {
        if (contentAtRoot != null)
            return contentAtRoot.restrict(!direction.isForward(), direction.isForward());
        if (!activeRangeKnown)
            activeRange = getNearestContent(direction);
        if (activeRange == null)
            return null;
        activeRange = activeRange.precedingState(direction);
        if (activeRange == null)
            return null;
        return activeRange.asBoundary(direction);
    }

    /// The content the root presents on its return path. The root records it as a backtrack entry as it is loaded
    /// (see [OnDiskReadNodeType#addReturnPathBacktrack]), and until the tail first moves, such entries are all the
    /// stack holds: the root has nothing above it, and loading a node records no other (see [OnDiskReadNodeType#load]).
    private S rootReturnPathContent()
    {
        if (stackLength == 0)
            return null;
        assert OnDiskReadNodeType.isReturnPathEntry(getStackNodeCode(stackLength - 1));
        return readContentAtPos(getStackPostCodePos(stackLength - 1));
    }

    @Override
    long getContentAtPos(long currentPos)
    {
        if (currentPos != ROOT_ASCENT_CONTENT_POS)
            return super.getContentAtPos(currentPos);

        content = rootAscentContent;
        return currentPos;
    }
}
