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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit which limits
 * the number of ranges that will be included in the intersection. This currently defaults to 2.
 */
@SuppressWarnings({"resource", "RedundantSuppression"})
public class KeyRangeIntersectionIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(KeyRangeIntersectionIterator.class);

    // The cassandra.sai.intersection.clause.limit (default: 2) controls the maximum number of range iterator that
    // will be used in the final intersection of a query operation.
    private static final int INTERSECTION_CLAUSE_LIMIT = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    private final List<KeyRangeIterator> ranges;

    private KeyRangeIntersectionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
    }

    @Override
    protected PrimaryKey computeNext()
    {
        // Range iterator that has been advanced in the previous cycle of the outer loop.
        // Initially there hasn't been the previous cycle, so set to null.
        int alreadyAvanced = -1;

        // The highest primary key seen on any range iterator so far.
        // It can become null when we reach the end of the iterator.
        PrimaryKey highestKey = getCurrent();

        outer:
        // We need to check if highestKey exceeds the maximum because the maximum is
        // the lowest value maximum of all the ranges. As a result any value could
        // potentially exceed it.
        while (highestKey != null && highestKey.compareTo(getMaximum()) <= 0)
        {
            // Try advance all iterators to the highest key seen so far.
            // Once this inner loop finishes normally, all iterators are guaranteed to be at the same value.
            for (int index = 0; index < ranges.size(); index++)
            {
                if (index != alreadyAvanced)
                {
                    KeyRangeIterator range = ranges.get(index);
                    PrimaryKey nextKey = nextOrNull(range, highestKey);
                    if (nextKey == null || nextKey.compareTo(highestKey) > 0)
                    {
                        // We jumped over the highest key seen so far, so make it the new highest key.
                        highestKey = nextKey;
                        // Remember this iterator to avoid advancing it again, because it is already at the highest key
                        alreadyAvanced = index;
                        // This iterator jumped over, so the other iterators are lagging behind now,
                        // including the ones already advanced in the earlier cycles of the inner loop.
                        // Therefore, restart the inner loop in order to advance
                        // the other iterators except this one to match the new highest key.
                        continue outer;
                    }
                    assert nextKey.compareTo(highestKey) == 0:
                    String.format("skipped to an item smaller than the target; " +
                                  "iterator: %s, target key: %s, returned key: %s", range, highestKey, nextKey);
                }
            }
            // If we reached here, next() has been called at least once on each range iterator and
            // the last call to next() on each iterator returned a value equal to the highestKey.
            return highestKey;
        }
        return endOfData();
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        for (KeyRangeIterator range : ranges)
            if (range.hasNext())
                range.skipTo(nextKey);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(ranges);
    }

    /**
     * Fetches the next available item from the iterator, such that the item is not lower than the given key.
     * If no such items are available, returns null.
     */
    private PrimaryKey nextOrNull(KeyRangeIterator iterator, PrimaryKey minKey)
    {
        iterator.skipTo(minKey);
        return iterator.hasNext() ? iterator.next() : null;
    }

    public static KeyRangeIntersectionIterator build(List<KeyRangeIterator> subIterators)
    {
        return new KeyRangeIntersectionIterator(new IntersectionStatistics(), subIterators);
    }

    private static class IntersectionStatistics extends KeyRangeIterator.Builder.Statistics
    {
        @Override
        public void update(KeyRangeIterator range)
        {
            // minimum of the intersection is the biggest minimum of individual iterators
            min = nullSafeMax(min, range.getMinimum());
            // maximum of the intersection is the smallest maximum of individual iterators
            max = nullSafeMin(max, range.getMaximum());
            count += range.getCount();
        }
    }

    @VisibleForTesting
    protected static boolean isDisjoint(KeyRangeIterator a, KeyRangeIterator b)
    {
        return isDisjointInternal(a.getCurrent(), a.getMaximum(), b);
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
    private static boolean isDisjointInternal(PrimaryKey min, PrimaryKey max, KeyRangeIterator b)
    {
        return min == null || max == null || b.getCount() == 0 || min.compareTo(b.getMaximum()) > 0 || b.getCurrent().compareTo(max) > 0;
    }
}
