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

package org.apache.cassandra.index.sai.disk.v1;
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

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryReader.NOT_FOUND;

/**
 * Thread-unsafe term -> long trie range iterator.
 * The payload may be changed to something else.
 */
public class TermsRangeIterator<Concrete extends TermsRangeIterator<Concrete>> extends Walker<Concrete>
{
    protected final ByteSource limit;
    protected IterationPosition stack;
    protected long next;
    final TrieTermsDictionaryReader.TransitionBytesCollector collector = new TrieTermsDictionaryReader.TransitionBytesCollector();
    final boolean inclEnd;

    public static class IterationPosition
    {
        public long node;
        public int childIndex;
        public int limit;
        public IterationPosition prev;

        public IterationPosition(long node, int childIndex, int limit, IterationPosition prev)
        {
            super();
            this.node = node;
            this.childIndex = childIndex;
            this.limit = limit;
            this.prev = prev;
        }

        @Override
        public String toString()
        {
            return String.format("[Node %d, child %d, limit %d]", node, childIndex, limit);
        }
    }

    public TermsRangeIterator(Rebufferer source,
                              long root,
                              ByteComparable start,
                              ByteComparable end,
                              boolean admitPrefix,
                              boolean inclEnd)
    {
        super(source, root);
        limit = end != null ? end.asComparableBytes(BYTE_COMPARABLE_VERSION) : null;
        this.inclEnd = inclEnd;

        if (start != null)
            initializeWithLeftBound(root, start.asComparableBytes(BYTE_COMPARABLE_VERSION), admitPrefix, limit != null);
        else
            initializeNoLeftBound(root, limit != null ? limit.next() : 256);
    }

    boolean first = true;

    public Iterator<Pair<ByteSource, Long>> iterator()
    {
        return new AbstractIterator<Pair<ByteSource, Long>>()
        {
            @Override
            protected Pair<ByteSource, Long> computeNext()
            {
                final long node = nextPayloadedNode();
                if (node == -1 || getCurrentPayload() == -1)
                {
                    return endOfData();
                }
                ByteSource byteSource = collector.toByteComparable().asComparableBytes(ByteComparable.Version.OSS41);
                return Pair.create(byteSource, getCurrentPayload());
            }
        };
    }

    public long getCurrentPayload()
    {
        return getPayload(buf, payloadPosition(), payloadFlags());
    }

    private long getPayload(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
        {
            return NOT_FOUND;
        }
        return SizedInts.read(contents, payloadPos, bytes);
    }

    protected long nextPayloadedNode()
    {
        if (first)
        {
            first = false;
            if (stack == null) return -1;
            return stack.node;
        }
        long toReturn = next;
        if (next != -1)
            next = advanceNode();
        return toReturn;
    }

    private void initializeWithLeftBound(long root, ByteSource startStream, boolean admitPrefix, boolean atLimit)
    {
        IterationPosition prev = null;
        int childIndex;
        int limitByte;
        long payloadedNode = -1;

        try
        {
            // Follow start position while we still have a prefix, stacking path and saving prefixes.
            go(root);
            while (true)
            {
                int s = startStream.next();
                childIndex = search(s);

                // For a separator trie the latest payload met along the prefix is a potential match for start
                if (admitPrefix)
                    if (childIndex == 0 || childIndex == -1)
                    {
                        if (payloadFlags() != 0)
                            payloadedNode = position;
                    }
                    else
                        payloadedNode = -1;

                limitByte = 256;
                if (atLimit)
                {
                    limitByte = limit.next();
                    if (s < limitByte)
                        atLimit = false;
                }
                if (childIndex < 0)
                    break;

                int transitionByte = transitionByte(childIndex);
                collector.add(transitionByte);

                prev = new IterationPosition(position, childIndex, limitByte, prev);
                go(transition(childIndex));
            }

            childIndex = -1 - childIndex - 1;

            stack = new IterationPosition(position, childIndex, limitByte, prev);

            // Advancing now gives us first match if we didn't find one already.
            if (payloadedNode != -1)
                next = payloadedNode;
            else {
                next = advanceNode();
            }
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
    }

    protected long advanceNode()
    {
        long child;
        int transitionByte;

        go(stack.node); // can throw NotInCacheException, OK no state modified yet

        while (true)
        {
            // advance position in node but don't change the stack just yet due to NotInCacheExceptions
            int childIndex = stack.childIndex + 1;
            transitionByte = transitionByte(childIndex);

            if (transitionByte > stack.limit)
            {
                // ascend
                stack = stack.prev;
                if (stack == null)        // exhausted whole trie
                    return -1;
                collector.pop();
                go(stack.node); // can throw NotInCacheException, OK - stack ready to re-enter loop with parent
                continue;
            }

            child = transition(childIndex);

            if (child != -1)
            {
                assert child >= 0 : String.format("Expected value >= 0 but got %d - %s", child, this);

                // descend
                go(child); // can throw NotInCacheException, OK - stack not yet changed, limit not yet incremented

                int l = 256;
                if (transitionByte == stack.limit)
                    l = limit.next();

                stack.childIndex = childIndex;
                stack = new IterationPosition(child, -1, l, stack);

                collector.add(transitionByte);

                if (payloadFlags() != 0 && (inclEnd || l >= 0))
                    return child;
            }
            else
            {
                stack.childIndex = childIndex;
            }
        }
    }

    private void initializeNoLeftBound(long root, int limitByte)
    {
        stack = new IterationPosition(root, -1, limitByte, null);

        try
        {
            go(root);
            if (payloadFlags() != 0)
                next = root;
            else
                next = advanceNode();
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
    }
}