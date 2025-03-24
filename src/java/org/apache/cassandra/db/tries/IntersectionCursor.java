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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// The implementation of the intersection of a trie with a set
abstract class IntersectionCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    enum State
    {
        /// The exact position is inside the set, source and set cursors are at the same position.
        MATCHING,
        /// The set cursor is ahead; the current position, as well as any before the set cursor's are inside the set.
        SET_AHEAD
    }

    final C source;
    final TrieSetCursor set;
    final Direction direction;
    State state;

    IntersectionCursor(C source, TrieSetCursor set)
    {
        this.direction = source.direction();
        this.source = source;
        this.set = set;
        matchingPosition(depth());
    }

    @Override
    public int depth()
    {
        return source.depth();
    }

    @Override
    public int incomingTransition()
    {
        return source.incomingTransition();
    }

    @Override
    public int advance()
    {
        if (state == State.SET_AHEAD)
            return advanceInCoveredBranch(set.depth(), source.advance());

        // The set is assumed sparser, so we advance that first.
        int setDepth = set.advance();
        if (set.precedingIncluded())
            return advanceInCoveredBranch(setDepth, source.advance());
        else
            return advanceSourceToIntersection(setDepth, set.incomingTransition());
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        if (state == State.SET_AHEAD)
            return advanceInCoveredBranch(set.depth(), source.skipTo(skipDepth, skipTransition));

        int setDepth = set.skipTo(skipDepth, skipTransition);
        if (set.precedingIncluded())
            return advanceInCoveredBranch(setDepth, source.skipTo(skipDepth, skipTransition));
        else
            return advanceSourceToIntersection(setDepth, set.incomingTransition());
    }

    @Override
    public int advanceMultiple(Cursor.TransitionsReceiver receiver)
    {
        // We can only apply advanceMultiple if we are fully inside a covered branch.
        if (state == State.SET_AHEAD)
            return advanceInCoveredBranch(set.depth(), source.advanceMultiple(receiver));

        int setDepth = set.advance();
        if (set.precedingIncluded())
            return advanceInCoveredBranch(setDepth, source.advance());
        else
            return advanceSourceToIntersection(setDepth, set.incomingTransition());
    }

    private int advanceInCoveredBranch(int setDepth, int sourceDepth)
    {
        // Check if the advanced source is still in the covered area.
        if (sourceDepth > setDepth) // most common fast path
            return coveredAreaWithSetAhead(sourceDepth);
        if (sourceDepth < 0)
            return exhausted();

        int sourceTransition = source.incomingTransition();
        if (sourceDepth == setDepth)
        {
            int setTransition = set.incomingTransition();
            if (direction.lt(sourceTransition, setTransition))
                return coveredAreaWithSetAhead(sourceDepth);
            if (sourceTransition == setTransition)
                return matchingPosition(sourceDepth);
        }

        // Source moved beyond the set position. Advance the set too.
        setDepth = set.skipTo(sourceDepth, sourceTransition);
        int setTransition = set.incomingTransition();
        if (setDepth == sourceDepth && setTransition == sourceTransition)
            return matchingPosition(sourceDepth);

        // At this point set is ahead. Check content to see if we are in a covered branch.
        // If not, we need to skip the source as well and repeat the process.
        if (set.precedingIncluded())
            return coveredAreaWithSetAhead(sourceDepth);
        else
            return advanceSourceToIntersection(setDepth, setTransition);
    }

    private int advanceSourceToIntersection(int setDepth, int setTransition)
    {
        while (true)
        {
            // Set is ahead of source, but outside the covered area. Skip source to the set's position.
            int sourceDepth = source.skipTo(setDepth, setTransition);
            int sourceTransition = source.incomingTransition();
            if (sourceDepth < 0)
                return exhausted();
            if (sourceDepth == setDepth && sourceTransition == setTransition)
                return matchingPosition(setDepth);

            // Source is now ahead of the set.
            setDepth = set.skipTo(sourceDepth, sourceTransition);
            setTransition = set.incomingTransition();
            if (setDepth == sourceDepth && setTransition == sourceTransition)
                return matchingPosition(setDepth);

            // At this point set is ahead. Check content to see if we are in a covered branch.
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(sourceDepth);
        }
    }

    private int coveredAreaWithSetAhead(int depth)
    {
        state = State.SET_AHEAD;
        return depth;
    }

    private int matchingPosition(int depth)
    {
        // If we are matching a boundary of the set, include all its children by using a set-ahead state, ensuring that
        // the set will only be advanced once the source ascends to its depth again.
        if (set.branchIncluded())
            state = State.SET_AHEAD;
        else
            state = State.MATCHING;
        return depth;
    }

    private int exhausted()
    {
        state = State.MATCHING;
        return -1;
    }

    @Override
    public Direction direction()
    {
        return source.direction();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return source.byteComparableVersion();
    }

    /// Intersection cursor for [Trie].
    static class Plain<T> extends IntersectionCursor<T, Cursor<T>>
    {
        public Plain(Cursor<T> source, TrieSetCursor set)
        {
            super(source, set);
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new Plain<>(source.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return source.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }
    }
}
