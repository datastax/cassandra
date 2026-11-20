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
package org.apache.cassandra.io.tries;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.PatternMatcher;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.PatternMatcher.EMPTY;
import static org.apache.cassandra.db.tries.PatternMatcher.HAS_ANY_STRING;
import static org.apache.cassandra.db.tries.PatternMatcher.HAS_WILDCARD;
import static org.apache.cassandra.db.tries.PatternMatcher.MATCH;
import static org.apache.cassandra.db.tries.PatternMatcher.MATCHES_ALL_EXTENSIONS;

/**
 * Thread-unsafe iterator over the payloaded nodes of an on-disk trie whose keys match a pattern, in forward or reverse
 * order. Uses the assumptions of {@link Walker}.
 * <p>
 * The pattern is matched by a {@link PatternMatcher} while the trie is walked, which lets the iterator skip the
 * branches that cannot contain a match: a child is only entered if the pattern can accept its transition byte, and
 * when one is rejected the walk continues with the next byte the pattern can accept. See
 * {@code org/apache/cassandra/db/tries/PatternMatching.md} for a description of the algorithm.
 */
@NotThreadSafe
public class PatternValueIterator<CONCRETE extends PatternValueIterator<CONCRETE>> extends BaseValueIterator<CONCRETE>
{
    private final PatternMatcher matcher;
    private final boolean forward;

    /** The current path, from the current node up to the root. */
    private Position path;

    /**
     * The depth at which a trailing any-string wildcard was matched: every key in that branch matches, and the state
     * of the matcher is not tracked below this depth.
     */
    private int trailingStarDepth = Integer.MAX_VALUE;

    /** The flags given to the nodes below a matched trailing any-string wildcard, whose state is not tracked. */
    private static final int MATCHES_ALL_FLAGS = MATCH | MATCHES_ALL_EXTENSIONS | HAS_ANY_STRING | HAS_WILDCARD;

    /**
     * In reverse order the payload of a node is returned when the walk ascends from it, while the collector still holds
     * its transition byte. It is removed on the next advance.
     */
    private boolean popOnAdvance = false;

    private static final class Position
    {
        final long node;
        final int depth;
        /** The flags of the matcher's state at the node, see {@link PatternMatcher#flags}. */
        final int flags;
        final Position prev;
        int childIndex;

        Position(long node, int depth, int flags, int childIndex, Position prev)
        {
            this.node = node;
            this.depth = depth;
            this.flags = flags;
            this.childIndex = childIndex;
            this.prev = prev;
        }

        @Override
        public String toString()
        {
            return String.format("[Node %d, depth %d, flags %x, child %d]", node, depth, flags, childIndex);
        }
    }

    /**
     * Creates an iterator over the payloaded nodes whose keys match the given matcher's pattern.
     *
     * @param matcher a matcher for the pattern, in its initial state; the iterator updates its state as it walks
     * @param direction the order in which to list the matching nodes
     * @param collecting whether to collect the keys of the nodes (see {@link #collectedKey()})
     */
    protected PatternValueIterator(Rebufferer source, long root, PatternMatcher matcher, Direction direction, boolean collecting, ByteComparable.Version version)
    {
        super(source, root, null, collecting, version);
        this.matcher = matcher;
        this.forward = direction.isForward();

        try
        {
            go(root);
            int flags = matcher.flags(0);
            if ((flags & MATCHES_ALL_EXTENSIONS) != 0)
                trailingStarDepth = 0;
            if (forward)
            {
                path = new Position(root, 0, flags, -1, null);
                // In forward order the payload of a node comes before its children.
                next = (flags & MATCH) != 0 && hasPayload() ? root : NOT_PREPARED;
            }
            else
            {
                path = new Position(root, 0, flags, transitionRange(), null);
                next = NOT_PREPARED;
            }
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
    }

    @Override
    protected long advanceNode()
    {
        return forward ? advanceForward() : advanceReverse();
    }

    private long advanceForward()
    {
        if (path == null)
            return NONE;

        go(path.node);
        while (true)
        {
            Position top = path;
            int childIndex = nextChildIndex(top);
            // transitionByte is Integer.MAX_VALUE past the last child
            int transitionByte = childIndex == Integer.MAX_VALUE ? Integer.MAX_VALUE : transitionByte(childIndex);
            if (transitionByte == Integer.MAX_VALUE)
            {
                // All children are done, ascend.
                if (top.depth == trailingStarDepth)
                    trailingStarDepth = Integer.MAX_VALUE;
                path = top.prev;
                if (path == null)
                    return NONE;
                if (collector != null)
                    collector.pop();
                go(path.node);
                continue;
            }

            top.childIndex = childIndex;
            long child = transition(childIndex);
            if (child == NONE)
                continue; // missing transition in a dense node

            int depth = top.depth + 1;
            int flags;
            if (depth > trailingStarDepth)
                flags = MATCHES_ALL_FLAGS;
            else
            {
                flags = matcher.descend(depth, transitionByte);
                if (flags == EMPTY)
                {
                    // No key in this branch can match. Continue with the next byte the pattern can accept.
                    int nextByte = matcher.nextAcceptedByte(depth - 1, transitionByte, Direction.FORWARD);
                    int index = search(nextByte);
                    top.childIndex = (index >= 0 ? index : -1 - index) - 1;
                    continue;
                }
                if ((flags & MATCHES_ALL_EXTENSIONS) != 0)
                    trailingStarDepth = depth;
            }

            go(child);
            path = new Position(child, depth, flags, -1, top);
            if (collector != null)
                collector.add(transitionByte);

            if ((flags & MATCH) != 0 && hasPayload())
                return child;
        }
    }

    /**
     * Returns the index of the next child of the current node (described by {@code top}) to visit in forward order,
     * or {@code Integer.MAX_VALUE} or an index past the last child if there are no more children to visit.
     */
    private int nextChildIndex(Position top)
    {
        if ((top.flags & HAS_WILDCARD) == 0)
        {
            int literal = matcher.singleLiteral(top.depth);
            if (literal >= 0)
            {
                // Only one byte can follow, go directly to it, and finish after it.
                if (top.childIndex >= 0)
                    return Integer.MAX_VALUE;
                int index = search(literal);
                return index >= 0 ? index : Integer.MAX_VALUE;
            }
        }
        return top.childIndex + 1;
    }

    private long advanceReverse()
    {
        if (path == null)
            return NONE;

        if (popOnAdvance)
        {
            popOnAdvance = false;
            if (collector != null)
                collector.pop();
        }

        go(path.node);
        while (true)
        {
            Position top = path;
            int childIndex = previousChildIndex(top);
            if (childIndex < 0)
            {
                // All children are done. In reverse order the payload of a node comes after its children, so this is
                // when we report it, before ascending.
                boolean match = (top.flags & MATCH) != 0;
                if (top.depth == trailingStarDepth)
                    trailingStarDepth = Integer.MAX_VALUE;
                path = top.prev;
                if (match && hasPayload())
                {
                    popOnAdvance = path != null;
                    return top.node;
                }

                if (path == null)
                    return NONE;
                if (collector != null)
                    collector.pop();
                go(path.node);
                continue;
            }

            top.childIndex = childIndex;
            long child = transition(childIndex);
            if (child == NONE)
                continue; // missing transition in a dense node

            int transitionByte = transitionByte(childIndex);
            int depth = top.depth + 1;
            int flags;
            if (depth > trailingStarDepth)
                flags = MATCHES_ALL_FLAGS;
            else
            {
                flags = matcher.descend(depth, transitionByte);
                if (flags == EMPTY)
                {
                    // No key in this branch can match. Continue with the previous byte the pattern can accept.
                    int nextByte = matcher.nextAcceptedByte(depth - 1, transitionByte, Direction.REVERSE);
                    int index = search(nextByte);
                    top.childIndex = (index >= 0 ? index : -1 - index - 1) + 1;
                    continue;
                }
                if ((flags & MATCHES_ALL_EXTENSIONS) != 0)
                    trailingStarDepth = depth;
            }

            go(child);
            path = new Position(child, depth, flags, transitionRange(), top);
            if (collector != null)
                collector.add(transitionByte);
        }
    }

    /**
     * Returns the index of the next child of the current node (described by {@code top}) to visit in reverse order,
     * or a negative value if there are no more children to visit.
     */
    private int previousChildIndex(Position top)
    {
        if ((top.flags & HAS_WILDCARD) == 0)
        {
            int literal = matcher.singleLiteral(top.depth);
            if (literal >= 0)
            {
                // Only one byte can follow, go directly to it, and finish after it.
                if (top.childIndex < transitionRange())
                    return -1;
                return search(literal); // negative if not found
            }
        }
        return top.childIndex - 1;
    }
}
