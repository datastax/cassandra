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

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Thread-unsafe reverse value iterator for on-disk tries. Uses the assumptions of {@link Walker}.
 * <p>
 * The main utility of this class is the {@link #nextPayloadedNode()} method, which lists all nodes that contain a
 * payload within the requested bounds. The treatment of the bounds is non-standard (see
 * {@link #ReverseValueIterator(Rebufferer, long, ByteComparable, ByteComparable, LeftBoundTreatment, ByteComparable.Version)}), necessary to
 * properly walk tries of prefixes and separators.
 */
@NotThreadSafe
public class ReverseValueIterator<Concrete extends ReverseValueIterator<Concrete>> extends BaseValueIterator<Concrete>
{
    static final int NOT_AT_LIMIT = Integer.MIN_VALUE;
    private LeftBoundTreatment reportingPrefixes;
    private boolean popOnAdvance = true;

    protected ReverseValueIterator(Rebufferer source, long root, ByteComparable.Version version)
    {
        this(source, root, false, version);
    }

    protected ReverseValueIterator(Rebufferer source, long root, boolean collecting, ByteComparable.Version version)
    {
        super(source, root, null, collecting, version);
        initializeNoRightBound(root, NOT_AT_LIMIT, LeftBoundTreatment.GREATER);
    }

    protected ReverseValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, LeftBoundTreatment admitPrefix, ByteComparable.Version version)
    {
        this(source, root, start, end, admitPrefix, false, version);
    }

    /**
     * Constrained iterator. The end position is always treated as inclusive, and we have two possible treatments for
     * the start:
     * <ul>
     *   <li> When {@code admitPrefix=false}, exact matches and any prefixes of the start are excluded.
     *   <li> When {@code admitPrefix=true}, the longest prefix of the start present in the trie is also included,
     *        provided that there is no entry in the trie between that prefix and the start. An exact match also
     *        satisfies this and is included.
     * </ul>
     * This behaviour is shared with the forward counterpart {@link ValueIterator}.
     */
    protected ReverseValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, LeftBoundTreatment admitPrefix, boolean collecting, ByteComparable.Version version)
    {
        super(source, root, start != null ? start.asComparableBytes(version) : null, collecting, version);

        if (end != null)
            initializeWithRightBound(root, end.asComparableBytes(version), admitPrefix, limit != null);
        else
            initializeNoRightBound(root, limit != null ? limit.next() : NOT_AT_LIMIT, admitPrefix);
    }

    void initializeWithRightBound(long root, ByteSource endStream, LeftBoundTreatment admitPrefix, boolean hasLimit)
    {
        IterationPosition prev = null;
        boolean atLimit = hasLimit;
        int childIndex;
        int limitByte;
        reportingPrefixes = admitPrefix;

        // Follow end position while we still have a prefix, stacking path.
        go(root);
        while (true)
        {
            int s = endStream.next();
            childIndex = search(s);

            limitByte = NOT_AT_LIMIT;
            if (atLimit)
            {
                limitByte = limit.next();
                if (s > limitByte)
                    atLimit = false;
            }
            if (childIndex < 0)
                break;

            prev = new IterationPosition(position, childIndex, limitByte, prev);
            if (collector != null)
                collector.add(s);
            go(transition(childIndex)); // childIndex is positive, this transition must exist
        }

        // Advancing now gives us first match.
        childIndex = -1 - childIndex;
        stack = new IterationPosition(position, childIndex, limitByte, prev);
        next = NOT_PREPARED;
        popOnAdvance = false;
    }

    private void initializeNoRightBound(long root, int limitByte, LeftBoundTreatment admitPrefix)
    {
        go(root);
        stack = new IterationPosition(root, -1 - search(256), limitByte, null);
        if (hasPayload())
            next = root;
        else
            next = NOT_PREPARED;
        popOnAdvance = false;
        reportingPrefixes = admitPrefix;
    }

    protected long advanceNode()
    {
        if (stack == null)
            return -1;

        long child;
        int transitionByte;

        if (collector != null)
        {
            // We need to pop the last character unless we have not yet advanced to an entry.
            if (popOnAdvance)
                collector.pop();
            else
                popOnAdvance = true;
        }

        go(stack.node);
        while (true)
        {
            // advance position in node
            int childIdx = stack.childIndex - 1;
            boolean beyondLimit = true;
            if (childIdx >= 0)
            {
                transitionByte = transitionByte(childIdx);
                beyondLimit = transitionByte < stack.limit;
                if (beyondLimit)
                {
                    assert stack.limit >= 0;    // we are at a limit position (not in a node that's completely within the span)
                    reportingPrefixes = null;  // there exists a smaller child than limit, no longer should report prefixes
                }
            }
            else
                transitionByte = Integer.MIN_VALUE;

            if (beyondLimit)
            {
                // ascend to parent, remove from stack
                IterationPosition stackTop = stack;
                stack = stack.prev;

                // Report payloads on the way up
                // unless we are at limit and there has been a smaller child
                if (payloadFlags() != 0)
                {
                    // If we are fully inside the covered space, report.
                    // Note that on the exact match of the limit, stackTop.limit would be END_OF_STREAM.
                    // This comparison rejects the exact match; if we wanted to include it, we could test < 0 instead.
                    if (stackTop.limit == NOT_AT_LIMIT || stackTop.limit == ByteSource.END_OF_STREAM && reportingPrefixes == LeftBoundTreatment.ADMIT_EXACT)
                        return stackTop.node;
                    else if (reportingPrefixes == LeftBoundTreatment.ADMIT_PREFIXES)
                    {
                        reportingPrefixes = null; // if we are at limit position only report one prefix, the closest
                        return stackTop.node;
                    }
                    // else skip this payload
                }

                if (collector != null)
                    collector.pop();

                if (stack == null)        // exhausted whole trie
                    return NONE;
                go(stack.node);
                continue;
            }

            child = transition(childIdx);
            if (child != NONE)
            {
                go(child);

                stack.childIndex = childIdx;

                // descend, stack up position
                int l = NOT_AT_LIMIT;
                if (transitionByte == stack.limit)
                    l = limit.next();

                stack = new IterationPosition(child, transitionRange(), l, stack);
                if (collector != null)
                    collector.add(transitionByte);
            }
            else
            {
                stack.childIndex = childIdx;
            }
        }
    }
}
