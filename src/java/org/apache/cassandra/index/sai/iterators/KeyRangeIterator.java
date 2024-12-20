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
package org.apache.cassandra.index.sai.iterators;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.AbstractGuavaIterator;

/**
 * Range iterators contain primary keys, in sorted order, with no duplicates.  They also
 * know their minimum and maximum keys, and an upper bound on the number of keys they contain.
 */
public abstract class KeyRangeIterator extends AbstractGuavaIterator<PrimaryKey> implements Closeable
{
    private static final Builder.EmptyRangeIterator EMPTY = new Builder.EmptyRangeIterator();

    private final PrimaryKey min, max;
    private final long count;

    protected KeyRangeIterator(Builder.Statistics statistics)
    {
        this(statistics.min, statistics.max, statistics.tokenCount);
    }

    public KeyRangeIterator(KeyRangeIterator range)
    {
        this(range == null ? null : range.min, range == null ? null : range.max, range == null ? -1 : range.count);
    }

    public KeyRangeIterator(PrimaryKey min, PrimaryKey max, long count)
    {
        if (min == null || max == null || count == 0)
        {
            assert min == null && max == null && (count == 0 || count == -1) : min + " - " + max + " " + count;
            endOfData();
        }

        this.min = min;
        this.max = max;
        this.count = count;
    }

    public final PrimaryKey getMinimum()
    {
        return min;
    }

    public final PrimaryKey getMaximum()
    {
        return max;
    }

    /**
     * @return an upper bound on the number of keys that can be returned by this iterator.
     */
    public final long getMaxKeys()
    {
        return count;
    }

    public final PrimaryKey nextOrNull()
    {
        return hasNext() ? next() : null;
    }

    /**
     * When called, this iterators current position will
     * be skipped forwards until finding either:
     *   1) an element equal to or bigger than next
     *   2) the end of the iterator
     *
     * @param nextToken value to skip the iterator forward until matching
     */
    public final void skipTo(PrimaryKey nextToken)
    {
        if (state == State.DONE)
            return;

        if (state == State.READY && next.compareTo(nextToken) >= 0)
            return;

        performSkipTo(nextToken);
        state = State.NOT_READY;
    }

    /**
     * Skip up to nextKey, but leave the internal state in a position where
     * calling computeNext() will return nextKey or the first one after it.
     */
    protected abstract void performSkipTo(PrimaryKey nextKey);

    public static KeyRangeIterator empty()
    {
        return EMPTY;
    }

    public static abstract class Builder
    {
        public enum IteratorType
        {
            CONCAT,
            UNION,
            INTERSECTION
        }

        @VisibleForTesting
        protected final Statistics statistics;


        public Builder(IteratorType type)
        {
            statistics = new Statistics(type);
        }

        public PrimaryKey getMinimum()
        {
            return statistics.min;
        }

        public PrimaryKey getMaximum()
        {
            return statistics.max;
        }

        public long getTokenCount()
        {
            return statistics.tokenCount;
        }

        public abstract int rangeCount();

        public abstract Collection<KeyRangeIterator> ranges();

        // Implementation takes ownership of the range iterator. If the implementation decides not to include it, such
        // that `rangeCount` may return 0, it must close the range iterator.
        public abstract Builder add(KeyRangeIterator range);

        public abstract Builder add(List<KeyRangeIterator> ranges);

        public final KeyRangeIterator build()
        {
            if (rangeCount() == 0)
                return new Builder.EmptyRangeIterator();
            else
                return buildIterator();
        }

        public static class EmptyRangeIterator extends KeyRangeIterator
        {
            EmptyRangeIterator() { super(null, null, 0); }
            public org.apache.cassandra.index.sai.utils.PrimaryKey computeNext() { return endOfData(); }
            protected void performSkipTo(org.apache.cassandra.index.sai.utils.PrimaryKey nextToken) { }
            public void close() { }
        }

        protected abstract KeyRangeIterator buildIterator();

        public static class Statistics
        {
            protected final IteratorType iteratorType;

            protected org.apache.cassandra.index.sai.utils.PrimaryKey min, max;
            protected long tokenCount;

            // iterator with the least number of items
            protected KeyRangeIterator minRange;
            // iterator with the most number of items
            protected KeyRangeIterator maxRange;


            private boolean hasRange = false;

            public Statistics(IteratorType iteratorType)
            {
                this.iteratorType = iteratorType;
            }

            /**
             * Update statistics information with the given range.
             *
             * Updates min/max of the combined range, token count and
             * tracks range with the least/most number of tokens.
             *
             * @param range The range to update statistics with.
             */
            public void update(KeyRangeIterator range)
            {
                switch (iteratorType)
                {
                    case CONCAT:
                        // range iterators should be sorted, but previous max must not be greater than next min.
                        if (range.getMaxKeys() > 0)
                        {
                            if (tokenCount == 0)
                            {
                                min = range.getMinimum();
                            }
                            else if (tokenCount > 0 && max.compareTo(range.getMinimum()) > 0)
                            {
                                throw new IllegalArgumentException("KeyRangeIterator must be sorted, previous max: " + max + ", next min: " + range.getMinimum());
                            }

                            max = range.getMaximum();
                        }
                        tokenCount += range.getMaxKeys();
                        break;

                    case UNION:
                        min = nullSafeMin(min, range.getMinimum());
                        max = nullSafeMax(max, range.getMaximum());
                        tokenCount += range.getMaxKeys();
                        break;

                    case INTERSECTION:
                        // minimum of the intersection is the biggest minimum of individual iterators
                        min = nullSafeMax(min, range.getMinimum());
                        // maximum of the intersection is the smallest maximum of individual iterators
                        max = nullSafeMin(max, range.getMaximum());
                        if (hasRange)
                            tokenCount = Math.min(tokenCount, range.getMaxKeys());
                        else
                            tokenCount = range.getMaxKeys();

                        break;

                    default:
                        throw new IllegalStateException("Unknown iterator type: " + iteratorType);
                }

                minRange = minRange == null ? range : min(minRange, range);
                maxRange = maxRange == null ? range : max(maxRange, range);

                hasRange = true;
            }

            private KeyRangeIterator min(KeyRangeIterator a, KeyRangeIterator b)
            {
                return a.getMaxKeys() > b.getMaxKeys() ? b : a;
            }

            private KeyRangeIterator max(KeyRangeIterator a, KeyRangeIterator b)
            {
                return a.getMaxKeys() > b.getMaxKeys() ? a : b;
            }

            /**
             * Returns true if the final range is not going to produce any results,
             * so we can cleanup range storage and never added anything to it.
             */
            public boolean isEmptyOrDisjoint()
            {
                // max < min if intersected ranges are disjoint
                return tokenCount == 0 || min.compareTo(max) > 0;
            }

            public double sizeRatio()
            {
                return minRange.getMaxKeys() * 1d / maxRange.getMaxKeys();
            }
        }
    }

    @VisibleForTesting
    protected static <U extends Comparable<U>> boolean isOverlapping(KeyRangeIterator a, KeyRangeIterator b)
    {
        return isOverlapping(a.peek(), a.getMaximum(), b);
    }

    /**
     * Ranges are overlapping the following cases:
     *
     *   * When they have a common subrange:
     *
     *   min       b.current      max          b.max
     *   +---------|--------------+------------|
     *
     *   b.current      min       max          b.max
     *   |--------------+---------+------------|
     *
     *   min        b.current     b.max        max
     *   +----------|-------------|------------+
     *
     *
     *  If either range is empty, they're disjoint.
     */
    @VisibleForTesting
    protected static boolean isOverlapping(PrimaryKey min, PrimaryKey max, KeyRangeIterator b)
    {
        return (min != null && max != null) &&
               b.hasNext() && min.compareTo(b.getMaximum()) <= 0 && b.peek().compareTo(max) <= 0;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable> T nullSafeMin(T a, T b)
    {
        if (a == null) return b;
        if (b == null) return a;

        return a.compareTo(b) > 0 ? b : a;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable> T nullSafeMax(T a, T b)
    {
        if (a == null) return b;
        if (b == null) return a;

        return a.compareTo(b) > 0 ? a : b;
    }
}
