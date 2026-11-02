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

/// What the two cursors over an on-disk range trie have in common: [OnDiskRangeCursor], which walks a range trie from
/// its root, and [OnDiskRangeTailCursor], which walks a tail of one. Both track the range state active at their
/// position the way the in-memory range cursor does (see [InMemoryRangeTrie]), and take tails the same way.
///
/// The two differ only in where they start and what they present there, which each subclass sets up once, in its own
/// constructor.
abstract class OnDiskBaseRangeCursor<S extends RangeState<S>> extends OnDiskCursor<S> implements RangeCursor<S>
{
    boolean activeIsSet;
    S activeRange;  // only non-null if activeIsSet
    S prevContent;  // can only be non-null if activeIsSet

    /// The failure of the last move, if it failed. A move that fails can leave the cursor part way, e.g. on a node whose
    /// content it has read without the state that follows from it, so [#state] and [#tailCursor] throw this rather
    /// than report a state or take a tail at wherever the move stopped.
    RuntimeException moveFailure;

    /// A cursor positioned at `root`, the root of a range trie.
    OnDiskBaseRangeCursor(DataDeserializer<S> deserializer, RebuffererFactory rebuffererFactory, ByteComparable.Version byteComparableVersion, Direction direction, long root)
    {
        super(deserializer, rebuffererFactory, byteComparableVersion, direction, true, false, root);
    }

    /// A cursor that is not yet positioned; the subclass descends into its root itself.
    OnDiskBaseRangeCursor(DataDeserializer<S> deserializer, RebuffererFactory rebuffererFactory, ByteComparable.Version byteComparableVersion, Direction direction)
    {
        super(deserializer, rebuffererFactory, byteComparableVersion, direction, true, false);
    }

    /// Set the active state from the content at the cursor's root, which the cursor must be positioned on.
    void initActiveState()
    {
        activeIsSet = true;
        activeRange = null;
        prevContent = null;
        updateActiveAndReturn(encodedPosition());
    }

    @Override
    public long advance()
    {
        try
        {
            return updateActiveAndReturn(super.advance());
        }
        catch (RuntimeException e)
        {
            throw failed(e);
        }
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        try
        {
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }
        catch (RuntimeException e)
        {
            throw failed(e);
        }
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        activeIsSet = false;    // since we are skipping, we have no idea where we will end up
        activeRange = null;
        prevContent = null;
        try
        {
            return updateActiveAndReturn(super.skipTo(encodedSkipPosition));
        }
        catch (RuntimeException e)
        {
            throw failed(e);
        }
    }

    /// Record a failed move in [#moveFailure], and drop the active state so that [#state] throws it too: an advance
    /// keeps the state it starts from, which no longer applies wherever the move stopped.
    private RuntimeException failed(RuntimeException e)
    {
        moveFailure = e;
        activeIsSet = false;
        activeRange = null;
        prevContent = null;
        return e;
    }

    @Override
    public S state()
    {
        if (!activeIsSet)
        {
            if (moveFailure != null)
                throw moveFailure;
            // The tries made from a closed cursor make all their cursors from it and may be read from several
            // threads, so a closed cursor works the state out each time it is asked and stores nothing.
            if (rebufferer == null)
                return stateFromNearestContent();
            setActiveState();
        }
        return activeRange;
    }

    long updateActiveAndReturn(long position)
    {
        if (!Cursor.isExhausted(position))
        {
            // Always check if we are seeing new content; if we do, that's an easy state update.
            S content = content();
            if (content != null)
            {
                activeRange = content;
                prevContent = content;
                activeIsSet = true;
            }
            else if (prevContent != null)
            {
                // If the previous state was exact, its right side is what we now have.
                activeRange = prevContent.succedingState(direction());
                prevContent = null;
                assert activeIsSet;
            }
            // otherwise the active state is either not set or still valid.
        }
        else
        {
            // exhausted
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
        }
        return position;
    }

    private void setActiveState()
    {
        activeRange = stateFromNearestContent();
        prevContent = null;
        activeIsSet = true;
    }

    /// The state at a position without content: the preceding state of the nearest content below it.
    private S stateFromNearestContent()
    {
        assert content() == null;
        S nearestContent = getNearestContent(direction());
        // Note: the nearest content may change between the time we fetch it and when we reach that node, e.g.
        // if someone deletes aa-cd where there existed an abc-acd deletion, and we fetched the latter while at "a".
        // This, though, should only be possible if the preceding state of the nearest content is null.
        return nearestContent != null ? nearestContent.precedingState(direction()) : null;
    }

    /// The first content below the current node in the given direction, the node's own included.
    ///
    /// A closed cursor no longer holds a buffer to read it with, but it can still be asked for it:
    /// [RangeCursor#precedingStateCursor], which asks for the state, remains callable after [#close]. It then reads
    /// it with a reader of its own, which it closes straight after, as a tail reads its root content with a
    /// rebufferer of its own (see [#tailCursor]).
    S getNearestContent(Direction direction)
    {
        if (rebufferer != null)
            return readNearestContent(this, direction);

        try (OnDiskCursor<S> reader = new OnDiskCursor<>(rdr.deserializer, rebuffererFactory, byteComparableVersion,
                                                         direction, isOrdered, alternateInAscentSlot))
        {
            return readNearestContent(reader, direction);
        }
    }

    /// [#getNearestContent], reading through `reader`: this cursor, or, when it is closed, one that is not positioned
    /// anywhere. The nodes it reads are given to it, from this cursor's position down.
    private S readNearestContent(OnDiskCursor<S> reader, Direction direction)
    {
        long postCodePos = currentFullNodePostCodePos;
        int code = currentFullNodeCode;
        while (true)
        {
            OnDiskReadNodeType type = OnDiskReadNodeType.selectNodeImpl(code);
            S content = type.getContent(reader, direction, code, postCodePos);
            if (content != null)
                return content;
            long node = type.getFirstChild(reader, direction, code, postCodePos);
            if (node <= 0)
                throw corrupt("node at " + postCodePos + " has neither content nor children");
            postCodePos = node - 1;
            code = reader.readByteBefore(node);
        }
    }

    /// The tail must present at its root the ranges active where the walk enters and leaves it, which come out of the
    /// file. It reads them itself, with a rebufferer of its own, once it is positioned on its root: this cursor may
    /// have been closed and given its buffer back (see [Cursor#close]).
    ///
    /// The one thing worth passing on from here is the state active at this position, if it is known, when the tail
    /// goes the same way and there is no content here to present instead; it saves the tail a descent to the nearest
    /// content.
    @Override
    public RangeCursor<S> tailCursor(Direction direction)
    {
        if (moveFailure != null)
            throw moveFailure;
        boolean precedingStateKnown = direction == direction() && activeIsSet && content == null;
        return new OnDiskRangeTailCursor<>(rdr.deserializer, rebuffererFactory, byteComparableVersion, direction,
                                           currentFullNodePostCodePos, currentFullNodeCode,
                                           precedingStateKnown, precedingStateKnown ? activeRange : null);
    }
}
