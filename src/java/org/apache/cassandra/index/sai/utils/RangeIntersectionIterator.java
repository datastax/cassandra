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
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.Token;
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
    private static final int INTERSECTION_CLAUSE_LIMIT = Integer.getInteger("cassandra.sai.intersection.clause.limit", 2);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    private final List<RangeIterator> ranges;
    private final List<RangeIterator> toRelease;
    private final List<RangeIterator> processedRanges;

    private RangeIntersectionIterator(Builder.Statistics statistics, List<RangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.toRelease = new ArrayList<>(ranges);
        this.processedRanges = new ArrayList<>(ranges.size());
    }

    protected PrimaryKey computeNext()
    {
        processedRanges.clear();
        RangeIterator head = null;
        PrimaryKey candidate = null;
        while (!ranges.isEmpty())
        {
            RangeIterator range = ranges.remove(0);
            if (!range.hasNext())
                return endOfData();
            if (range.getCurrent().compareTo(getMinimum()) < 0)
            {
                range.skipTo(getMinimum());
            }
            if (candidate == null)
            {
                candidate = range.hasNext() ? range.next() : null;
                if (candidate == null || candidate.compareTo(getMaximum()) > 0)
                    return endOfData();
                head = range;
            }
            else
            {
                if (!isOverlapping(head, range) || (range.skipTo(candidate) == null))
                {
                    return endOfData();
                }
                int cmp = candidate.compareTo(range.getCurrent());
                if (cmp == 0)
                {
                    range.hasNext();
                    processedRanges.add(range);
                }
                else if (cmp < 0)
                {
                    candidate = range.next();
                    ranges.add(head);
                    ranges.addAll(processedRanges);
                    processedRanges.clear();
                    head = range;
                }
                else
                    return endOfData();
            }
        }
        ranges.add(head);
        ranges.addAll(processedRanges);
        return candidate;
    }

    protected void performSkipTo(PrimaryKey nextToken)
    {
        for (RangeIterator range : ranges)
            if (range.hasNext())
                range.skipTo(nextToken);
    }

    public void close() throws IOException
    {
        toRelease.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder selectiveBuilder()
    {
        return selectiveBuilder(INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder selectiveBuilder(int limit)
    {
        return new Builder(limit);
    }

    public static class Builder extends RangeIterator.Builder
    {
        private final int limit;
        protected List<RangeIterator> rangeIterators = new ArrayList<>();

        public Builder()
        {
            this(Integer.MAX_VALUE);
        }

        public Builder(int limit)
        {
            super(IteratorType.INTERSECTION);
            this.limit = limit;
        }

        public RangeIterator.Builder add(RangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
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
            rangeIterators.sort(Comparator.comparingLong(RangeIterator::getCount));
            int initialSize = rangeIterators.size();
            // all ranges will be included
            if (limit >= rangeIterators.size() || limit <= 0)
                return buildIterator(statistics, rangeIterators);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            Statistics selectiveStatistics = new Statistics(IteratorType.INTERSECTION);
            for (int i = rangeIterators.size() - 1; i >= 0 && i >= limit; i--)
                FileUtils.closeQuietly(rangeIterators.remove(i));

            for (RangeIterator iterator : rangeIterators)
                selectiveStatistics.update(iterator);

            if (Tracing.isTracing())
                Tracing.trace("Selecting {} {} of {} out of {} indexes",
                              rangeIterators.size(),
                              rangeIterators.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                              rangeIterators.stream().map(RangeIterator::getCount).map(Object::toString).collect(Collectors.joining(", ")),
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
