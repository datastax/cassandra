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
package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit which limits
 * the number of ranges that will be included in the intersection. This currently defaults to 2.
 */
@SuppressWarnings("resource")
public class RangeIntersectionIterator extends RangeIterator
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

    public final List<RangeIterator> ranges;
    private final int[] rangeStats;

    private RangeIntersectionIterator(Builder.Statistics statistics, List<RangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.rangeStats = new int[ranges.size()];
    }

    protected PrimaryKey computeNext()
    {
        outer:
        while (true)
        {
            if (!ranges.get(0).hasNext())
                return endOfData();

            // VSTODO - when we skip on a PostingsList, we know how many postings we skipped. Can we tie that
            // information back into the IntersectionResult and then order the ranges based on the remaining
            // number of postings? Then, we would always call next on the range with the fewest remaining postings.
            PrimaryKey maybeNextKey = ranges.get(0).next();

            // Iterate over each range and check if each range contains the maybeNextKey. If it does, the
            // maybeNextKey is our next key. Otherwise, we continue to the next key in the first range until
            // one of the ranges runs out of keys.
            for (int index = 1; index < ranges.size(); index++)
            {
                RangeIterator range = ranges.get(index);
                switch (range.intersect(maybeNextKey))
                {
                    case MATCH:
                        continue;
                    case MISS:
                        continue outer;
                    case EXHAUSTED:
                        return endOfData();
                }
            }
            // If we reached here, maybeNextKey is our nextKey.
            return maybeNextKey;
        }
    }

    @Override
    protected IntersectionResult performIntersect(PrimaryKey otherKey)
    {
        boolean isMiss = false;
        for (var range : ranges)
        {
            switch(range.intersect(otherKey))
            {
                case MISS:
                    // VSTODO is it worth returning early here? It will add complexity, but will make this a less
                    // eager. See comment in RangeUnionIterator for details.
                    isMiss = true;
                case MATCH:
                    continue;
                case EXHAUSTED:
                    return IntersectionResult.EXHAUSTED;
            }
        }
        return isMiss ? IntersectionResult.MISS : IntersectionResult.MATCH;
    }

    protected void performSkipTo(Token nextToken)
    {
        // Resist the temptation to call range.hasNext before skipTo: this is a pessimisation, hasNext will invoke
        // computeNext under the hood, which is an expensive operation to produce a value that we plan to throw away.
        // Instead, it is the responsibility of the child iterators to make skipTo fast when the iterator is exhausted.
        for (var range : ranges)
            range.skipTo(nextToken);
    }

    public void close() throws IOException
    {
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder(List<RangeIterator> ranges, int limit)
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

    public static class Builder extends RangeIterator.Builder
    {
        private final int limit;
        protected List<RangeIterator> rangeIterators;

        private Builder(int size, int limit)
        {
            super(IteratorType.INTERSECTION);
            rangeIterators = new ArrayList<>(size);
            this.limit = limit;
        }

        public RangeIterator.Builder add(RangeIterator range)
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

        public RangeIterator.Builder add(List<RangeIterator> ranges)
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

        protected RangeIterator buildIterator()
        {
            rangeIterators.sort(Comparator.comparingLong(RangeIterator::getMaxKeys));
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
                              rangeIterators.stream().map(RangeIterator::getMaxKeys).map(Object::toString).collect(Collectors.joining(", ")),
                              initialSize);

            return buildIterator(selectiveStatistics, rangeIterators);
        }

        private static RangeIterator buildIterator(Statistics statistics, List<RangeIterator> ranges)
        {
            // if the range is disjoint or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (statistics.isDisjoint())
            {
                // release posting lists
                FileUtils.closeQuietly(ranges);
                return RangeIterator.empty();
            }

            if (ranges.size() == 1)
                return ranges.get(0);

            return new RangeIntersectionIterator(statistics, ranges);
        }
    }
}
