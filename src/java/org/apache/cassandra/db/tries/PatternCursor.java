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

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.PatternMatcher.EMPTY;
import static org.apache.cassandra.db.tries.PatternMatcher.HAS_ANY_STRING;
import static org.apache.cassandra.db.tries.PatternMatcher.HAS_WILDCARD;
import static org.apache.cassandra.db.tries.PatternMatcher.MATCH;
import static org.apache.cassandra.db.tries.PatternMatcher.MATCHES_ALL_EXTENSIONS;

/// A cursor that presents the content of the source cursor whose keys match a [TriePattern], see
/// [Trie#patternMatched(TriePattern)]. The matching is done by a [PatternMatcher] while the source is walked, which lets
/// the cursor skip the branches of the source that cannot match. See `PatternMatching.md` for details.
class PatternCursor<T> implements Cursor<T>, Cursor.TransitionsReceiver
{
    final Cursor<T> wrapped;
    final PatternMatcher matcher;

    // The depth of the current matcher state. Normally the depth of the current position, but positions below the
    // point where a trailing star matched (see trailingStarMatchDepth) don't track the state.
    int curDepth;
    // The flags of the current matcher state, see PatternMatcher.flags
    int flags;

    // When a trailing star is matched, everything below that depth matches and the state is not tracked.
    int trailingStarMatchDepth = Integer.MAX_VALUE;

    // The receiver given to advanceMultiple, to which we pass the bytes after processing them.
    TransitionsReceiver chainedReceiver;

    static <T> Cursor<T> create(Cursor<T> wrapped, TriePattern pattern)
    {
        return new PatternCursor<>(wrapped, PatternMatcher.create(pattern));
    }

    PatternCursor(Cursor<T> wrapped, PatternMatcher matcher)
    {
        this.wrapped = wrapped;
        this.matcher = matcher;
        curDepth = 0;
        flags = matcher.flags(0);
    }

    @Override
    public long encodedPosition()
    {
        return (flags & MATCH) != 0 ? wrapped.encodedPosition() : wrapped.encodedPosition() & ~MAY_HAVE_CONTENT_BIT;
    }

    @Override
    public T content()
    {
        return (flags & MATCH) != 0 ? wrapped.content() : null;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return wrapped.byteComparableVersion();
    }

    private long advanceTo(long encodedPosition)
    {
        while (true)
        {
            int depth = Cursor.depth(encodedPosition);
            if (depth > trailingStarMatchDepth || depth < 0)
                return encodedPosition;

            trailingStarMatchDepth = Integer.MAX_VALUE;
            curDepth = depth;
            // At depth 0 (the root on the return path) keep the initial (or inherited for tails) state.
            int flags = depth == 0 ? matcher.flags(0) : matcher.descend(depth, Cursor.incomingTransition(encodedPosition));
            if (flags != EMPTY)
            {
                this.flags = flags;
                if ((flags & MATCHES_ALL_EXTENSIONS) != 0)
                    trailingStarMatchDepth = depth;
                return (flags & MATCH) != 0 ? encodedPosition : encodedPosition & ~MAY_HAVE_CONTENT_BIT;
            }

            // No key in this branch can match. We won't end up here very often (only if there's no star in the path
            // so far). If we do, skip to the next byte that the pattern can accept.
            Direction direction = direction();
            int nextByte = matcher.nextAcceptedByte(depth - 1, Cursor.incomingTransition(encodedPosition), direction);
            encodedPosition = wrapped.skipTo(Cursor.encodeWithOverflow(depth, nextByte, direction));
        }
    }

    @Override
    public long advance()
    {
        // if we have never seen a star, and the current position is not a wildcard, use the next pattern byte
        if ((flags & HAS_WILDCARD) == 0)
            return advanceToPatternByte();
        else
            return advanceTo(wrapped.advance());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        int flags = this.flags;
        // if we have never seen a star, and the current position is not a wildcard, use the next pattern byte
        if ((flags & HAS_WILDCARD) == 0)
            return advanceToPatternByte();

        // Only single-character wildcards in the state, no star: advance one step, so that we go back to skipping by
        // pattern byte as soon as the next pattern position is a literal.
        if ((flags & HAS_ANY_STRING) == 0)
            return advanceTo(wrapped.advance());

        // we don't need to track anything after a trailing star, only check if we have left the branch
        if ((flags & MATCHES_ALL_EXTENSIONS) != 0)
            return advanceTo(wrapped.advanceMultiple(receiver));

        // If there is a star in our state, we have to walk the full trie, so take advantage of advanceMultiple,
        // updating our state for each character.
        chainedReceiver = receiver;
        return advanceTo(wrapped.advanceMultiple(this));
    }

    private long advanceToPatternByte()
    {
        int nextByte = matcher.singleLiteral(curDepth);
        // Don't descend if no single byte can follow (the pattern is fully matched, or more than one position is
        // active, possible after a single-character wildcard), or if we are on the return path (in reverse direction
        // ordered tries present prefix content after the node's children, which have already been visited).
        if (nextByte < 0 || Cursor.isOnReturnPath(wrapped.encodedPosition()))
            return advanceTo(wrapped.advance());
        return advanceTo(wrapped.skipTo(Cursor.encode(curDepth + 1, nextByte, direction())));
    }

    @Override
    public long skipTo(long skipPosition)
    {
        return advanceTo(wrapped.skipTo(skipPosition));
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        if ((flags & MATCHES_ALL_EXTENSIONS) != 0)
            return wrapped.tailCursor(direction);
        return new PatternCursor<>(wrapped.tailCursor(direction), matcher.tail(curDepth));
    }

    @Override
    public void addPathByte(int nextByte)
    {
        matcher.descend(++curDepth, nextByte);
        if (chainedReceiver != null)
            chainedReceiver.addPathByte(nextByte);
    }

    @Override
    public void addPathBytes(DirectBuffer buffer, int pos, int count)
    {
        matcher.descend(curDepth + 1, buffer, pos, count);
        curDepth += count;
        if (chainedReceiver != null)
            chainedReceiver.addPathBytes(buffer, pos, count);
    }
}
