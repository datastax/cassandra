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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit which limits
 * the number of ranges that will be included in the intersection. This currently defaults to 2.
 */
public class KeyRangeIntersectionIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d",
                                  CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt()));
    }

    public final List<KeyRangeIterator> ranges;
    private final int[] rangeStats;

    private KeyRangeIntersectionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.rangeStats = new int[ranges.size()];
    }

    protected PrimaryKey computeNext()
    {
        // The highest primary key seen on any range iterator so far.
        // It can become null when we reach the end of the iterator.
        PrimaryKey highestKey = ranges.get(0).hasNext() ? ranges.get(0).next() : null;
        // Index of the range iterator that has advanced beyond the others
        int alreadyAdvanced = 0;
        rangeStats[0]++;

        outer:
        while (highestKey != null)
        {
            // Try advance all iterators to the highest key seen so far.
            // Once this inner loop finishes normally, all iterators are guaranteed to be at the same value.
            for (int index = 0; index < ranges.size(); index++)
            {
                if (index != alreadyAdvanced)
                {
                    KeyRangeIterator range = ranges.get(index);
                    PrimaryKey nextKey = nextOrNull(range, highestKey);
                    rangeStats[index]++;
                    int comparisonResult;
                    if (nextKey == null || (comparisonResult = nextKey.compareTo(highestKey)) > 0)
                    {
                        // We jumped over the highest key seen so far, so make it the new highest key.
                        highestKey = nextKey;
                        // Remember this iterator to avoid advancing it again, because it is already at the highest key
                        alreadyAdvanced = index;
                        // This iterator jumped over, so the other iterators are lagging behind now,
                        // including the ones already advanced in the earlier cycles of the inner loop.
                        // Therefore, restart the inner loop in order to advance
                        // the other iterators except this one to match the new highest key.
                        continue outer;
                    }
                    assert comparisonResult == 0 :
                           String.format("skipTo skipped to an item smaller than the target; " +
                                         "iterator: %s, target key: %s, returned key: %s", range, highestKey, nextKey);
                }
            }
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

            return highestKey;
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

    protected void performSkipTo(PrimaryKey nextToken)
    {
        // Resist the temptation to call range.hasNext before skipTo: this is a pessimisation, hasNext will invoke
        // computeNext under the hood, which is an expensive operation to produce a value that we plan to throw away.
        // Instead, it is the responsibility of the child iterators to make skipTo fast when the iterator is exhausted.
        for (var range : ranges)
            range.skipTo(nextToken);
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

    public void close() throws IOException
    {
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder(List<KeyRangeIterator> ranges)
    {
        var builder = new Builder(ranges.size());
        for (var range : ranges)
            builder.add(range);
        return builder;
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    public static Builder builder()
    {
        return builder(4);
    }

    public static class Builder extends KeyRangeIterator.Builder
    {
        protected List<KeyRangeIterator> rangeIterators;

        private Builder(int size)
        {
            super(IteratorType.INTERSECTION);
            rangeIterators = new ArrayList<>(size);
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

        @Override
        public Collection<KeyRangeIterator> ranges()
        {
            return rangeIterators;
        }

        protected KeyRangeIterator buildIterator()
        {
            // if the range is disjoint or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (statistics.isEmptyOrDisjoint())
            {
                // release posting lists
                FileUtils.closeQuietly(rangeIterators);
                return KeyRangeIterator.empty();
            }

            if (rangeCount() == 1)
                return rangeIterators.get(0);

            return new KeyRangeIntersectionIterator(statistics, rangeIterators);
        }
    }
}
