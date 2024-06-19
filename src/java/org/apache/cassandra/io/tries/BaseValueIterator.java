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

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public abstract class BaseValueIterator<CONCRETE extends BaseValueIterator<CONCRETE>> extends Walker<CONCRETE>
{
    protected static final long NOT_PREPARED = -2;
    protected final ByteSource limit;
    protected final TransitionBytesCollector collector;
    protected IterationPosition stack;
    protected long next;

    public BaseValueIterator(Rebufferer source, long root, ByteSource limit, boolean collecting, ByteComparable.Version version)
    {
        super(source, root, version);
        this.limit = limit;
        collector = collecting ? new TransitionBytesCollector(byteComparableVersion) : null;
    }

    /**
     * Returns the payload node position.
     * <p>
     * This method must be async-read-safe, see {@link #advanceNode()}.
     */
    protected long nextPayloadedNode()
    {
        if (next != NOT_PREPARED)
        {
            long toReturn = next;
            next = NOT_PREPARED;
            return toReturn;
        }
        else
            return advanceNode();
    }

    protected boolean hasNext()
    {
        if (next == NOT_PREPARED)
            next = advanceNode();
        return next != NONE;
    }

    protected <VALUE> VALUE nextValue(Supplier<VALUE> supplier)
    {
        long node = nextPayloadedNode();
        if (node == NONE)
            return null;
        go(node);
        return supplier.get();
    }

    protected long nextValueAsLong(LongSupplier supplier, long valueIfNone)
    {
        long node = nextPayloadedNode();
        if (node == NONE)
            return valueIfNone;
        go(node);
        return supplier.getAsLong();
    }

    protected ByteComparable collectedKey()
    {
        assert collector != null : "Cannot get a collected value from a non-collecting iterator";
        return collector.toByteComparable();
    }

    protected abstract long advanceNode();

    protected enum LeftBoundTreatment
    {
        ADMIT_PREFIXES,
        ADMIT_EXACT,
        GREATER
    }

    protected static class IterationPosition
    {
        final long node;
        final int limit;
        final IterationPosition prev;
        int childIndex;

        IterationPosition(long node, int childIndex, int limit, IterationPosition prev)
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
}
