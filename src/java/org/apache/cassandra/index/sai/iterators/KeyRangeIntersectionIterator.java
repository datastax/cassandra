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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKey.Kind;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

import javax.annotation.Nullable;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit which limits
 * the number of ranges that will be included in the intersection. This currently defaults to 2.
 */
@SuppressWarnings("resource")
public class KeyRangeIntersectionIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // The cassandra.sai.intersection.clause.limit (default: 2) controls the maximum number of range iterator that
    // will be used in the final intersection of a query operation.
    public static final int INTERSECTION_CLAUSE_LIMIT = Integer.getInteger("cassandra.sai.intersection.clause.limit", 2);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    public static boolean shouldDefer(int numberOfExpressions)
    {
        return (INTERSECTION_CLAUSE_LIMIT <= 0) || (numberOfExpressions <= INTERSECTION_CLAUSE_LIMIT);
    }

    public final List<KeyRangeIterator> ranges;
    private final int[] rangeStats;
    private PrimaryKey highestKey;

    private KeyRangeIntersectionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.rangeStats = new int[ranges.size()];
        this.highestKey = null;
    }

    protected PrimaryKey computeNext()
    {
        if (highestKey == null)
            highestKey = computeHighestKey();

        outer:
        // After advancing one iterator, we must try to advance all the other iterators that got behind,
        // so they catch up to it. Note that we will not advance the iterators for static columns
        // as long as they point to the partition of the highest key. (This is because STATIC primary keys
        // compare to other keys only by partition.) This loop continues until all iterators point to the same key,
        // or if we run out of keys on any of them, or if we exceed the maximum key.
        // There is no point in iterating after maximum, because no keys will match beyond that point.
        while (highestKey != null && highestKey.compareTo(getMaximum()) <= 0)
        {
            // Try to advance all iterators to the highest key seen so far.
            // Once this inner loop finishes normally, all iterators are guaranteed to be at the same value.
            for (int index = 0; index < ranges.size(); index++)
            {
                KeyRangeIterator range = ranges.get(index);
                if (!range.hasNext())
                    return endOfData();

                if (range.peek().compareTo(highestKey) < 0)
                {
                    // If we advance a STATIC key, then we must advance it to the same partition as the highestKey.
                    // Advancing a STATIC key to a WIDE key directly (without throwing away the clustering) would
                    // go too far, as WIDE keys are stored after STATIC in the posting list.
                    PrimaryKey nextKey = range.peek().kind() == Kind.STATIC
                                         ? skipAndPeek(range, highestKey.toStatic())
                                         : skipAndPeek(range, highestKey);
                    rangeStats[index]++;

                    // We use strict comparison here, since it orders WIDE primary keys after STATIC primary keys
                    // in the same partition. When WIDE keys are present, we want to return them rather than STATIC
                    // keys to avoid retrieving and post-filtering entire partitions.
                    if (nextKey == null || nextKey.compareToStrict(highestKey) > 0)
                    {
                        // We jumped over the highest key seen so far, so make it the new highest key.
                        highestKey = nextKey;

                        // This iterator jumped over, so the other iterators might be lagging behind now,
                        // including the ones already advanced in the earlier cycles of the inner loop.
                        // Therefore, restart the inner loop in order to advance the lagging iterators.
                        continue outer;
                    }
                    assert nextKey.compareTo(highestKey) == 0 :
                        String.format("Skipped to a key smaller than the target! " +
                                      "iterator: %s, target key: %s, returned key: %s", range, highestKey, nextKey);
                }
            }
            // If we get here, all iterators have been advanced to the same key. When STATIC and WIDE keys are
            // mixed, this means WIDE keys point to exactly the same row, and STATIC keys the same partition.
            PrimaryKey result = highestKey;

            // Advance one iterator to the next key and remember the key as the highest seen so far.
            // It can become null when we reach the end of the iterator.
            // If there are both static and non-static keys being iterated here, we advance a non-static one,
            // regardless of the order of ranges in the ranges list.
            highestKey = advanceOneRange();

            // If we reached here, next() has been called at least once on each range iterator and
            // the last call to next() on each iterator returned a value equal to the highestKey.

            // Move the iterator that was called the least times to the start of the list.
            // This is an optimisation assuming that iterator is likely a more selective one.
            // E.g.if the first range produces (1, 2, 3, ... 100) and the second one (10, 20, 30, .. 100)
            // we'd want to start with the second.
            int idxOfSmallest = getIdxOfSmallest(rangeStats);

            if (idxOfSmallest != 0)
            {
                Collections.swap(ranges, 0, idxOfSmallest);
                // swap stats as well
                int a = rangeStats[0];
                int b = rangeStats[idxOfSmallest];
                rangeStats[0] = b;
                rangeStats[idxOfSmallest] = a;
            }

            return result;
        }

        return endOfData();
    }

    private static int getIdxOfSmallest(int[] rangeStats)
    {
        int idxOfSmallest = 0;
        for (int i = 1; i < rangeStats.length; i++)
        {
            if (rangeStats[i] < rangeStats[idxOfSmallest])
                idxOfSmallest = i;
        }
        return idxOfSmallest;
    }

    /**
     * Advances the iterator of one range to the next item, which becomes the highest seen so far.
     * Iterators pointing to STATIC keys are advanced only if no non-STATIC keys have been advanced.
     *
     * @return the next highest key or null if the iterator has reached the end
     */
    private @Nullable PrimaryKey advanceOneRange()
    {
        for (KeyRangeIterator range : ranges)
            if (range.peek().kind() != Kind.STATIC)
            {
                range.next();
                return range.hasNext() ? range.peek() : null;
            }

        for (KeyRangeIterator range : ranges)
            if (range.peek().kind() == Kind.STATIC)
            {
                range.next();
                return range.hasNext() ? range.peek() : null;
            }

        throw new IllegalStateException("There should be at least one range to advance!");
    }

    private @Nullable PrimaryKey computeHighestKey()
    {
        PrimaryKey max = getMinimum();
        for (KeyRangeIterator range : ranges)
        {
            if (!range.hasNext())
                return null;
            if (range.peek().compareToStrict(max) > 0)
                max = range.peek();
        }
        return max;
    }

    protected void performSkipTo(PrimaryKey nextToken)
    {
        // Resist the temptation to call range.hasNext before skipTo: this is a pessimisation, hasNext will invoke
        // computeNext under the hood, which is an expensive operation to produce a value that we plan to throw away.
        // Instead, it is the responsibility of the child iterators to make skipTo fast when the iterator is exhausted.
        for (var range : ranges)
            range.skipTo(nextToken);

        // Force recomputing the highest key on the next call to computeNext()
        highestKey = null;
    }

    /**
     * Fetches the next available item from the iterator, such that the item is not lower than the given key.
     * If no such items are available, returns null.
     */
    private PrimaryKey skipAndPeek(KeyRangeIterator iterator, PrimaryKey minKey)
    {
        iterator.skipTo(minKey);
        return iterator.hasNext() ? iterator.peek() : null;
    }

    public void close() throws IOException
    {
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder(List<KeyRangeIterator> ranges, int limit)
    {
        var builder = new Builder(ranges.size(), limit);
        for (var range : ranges)
            builder.add(range);
        return builder;
    }

    public static Builder builder(int size, int limit)
    {
        return new Builder(size, limit);
    }

    public static Builder builder(int limit)
    {
        return builder(10, limit);
    }

    public static Builder sizedBuilder(int size)
    {
        return builder(size, INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder builder()
    {
        return builder(Integer.MAX_VALUE);
    }

    public static class Builder extends KeyRangeIterator.Builder
    {
        private final int limit;
        protected List<KeyRangeIterator> rangeIterators;

        private Builder(int size, int limit)
        {
            super(IteratorType.INTERSECTION);
            rangeIterators = new ArrayList<>(size);
            this.limit = limit;
        }

        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getMaxKeys() > 0)
                rangeIterators.add(range);
            else
                FileUtils.closeQuietly(range);
            statistics.update(range);

            return this;
        }

        public KeyRangeIterator.Builder add(List<KeyRangeIterator> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public int rangeCount()
        {
            return rangeIterators.size();
        }

        protected KeyRangeIterator buildIterator()
        {
            rangeIterators.sort(Comparator.comparingLong(KeyRangeIterator::getMaxKeys));
            int initialSize = rangeIterators.size();
            // all ranges will be included
            if (limit >= rangeIterators.size() || limit <= 0)
                return buildIterator(statistics, rangeIterators);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            Statistics selectiveStatistics = new Statistics(IteratorType.INTERSECTION);
            for (int i = rangeIterators.size() - 1; i >= 0 && i >= limit; i--)
                FileUtils.closeQuietly(rangeIterators.remove(i));

            for (var iterator : rangeIterators)
                selectiveStatistics.update(iterator);

            if (Tracing.isTracing())
                Tracing.trace("Selecting {} {} of {} out of {} indexes",
                              rangeIterators.size(),
                              rangeIterators.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                              rangeIterators.stream().map(KeyRangeIterator::getMaxKeys).map(Object::toString).collect(Collectors.joining(", ")),
                              initialSize);

            return buildIterator(selectiveStatistics, rangeIterators);
        }

        private static KeyRangeIterator buildIterator(Statistics statistics, List<KeyRangeIterator> ranges)
        {
            // if the range is disjoint or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (statistics.isDisjoint())
            {
                // release posting lists
                FileUtils.closeQuietly(ranges);
                return KeyRangeIterator.empty();
            }

            if (ranges.size() == 1)
                return ranges.get(0);

            return new KeyRangeIntersectionIterator(statistics, ranges);
        }
    }
}
