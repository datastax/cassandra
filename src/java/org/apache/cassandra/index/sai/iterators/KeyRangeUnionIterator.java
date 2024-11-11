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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Range Union Iterator is used to return sorted stream of elements from multiple KeyRangeIterator instances.
 */
@SuppressWarnings("resource")
public class KeyRangeUnionIterator extends KeyRangeIterator
{
    public final List<KeyRangeIterator> ranges;

    private KeyRangeUnionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);
        this.ranges = new ArrayList<>(ranges);
    }

    public PrimaryKey computeNext()
    {
        // Keep track of the next best candidate. If another candidate has the same value, advance it to prevent
        // duplicate results. This design avoids unnecessary list operations.
        KeyRangeIterator candidate = null;
        for (KeyRangeIterator range : ranges)
        {
            if (!range.hasNext())
                continue;

            if (candidate == null)
            {
                candidate = range;
            }
            else
            {
                int cmp = candidate.peek().compareTo(range.peek());
                if (cmp == 0)
                    range.next();
                else if (cmp > 0)
                    candidate = range;
            }
        }
        if (candidate == null)
            return endOfData();
        return candidate.next();
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        // Resist the temptation to call range.hasNext before skipTo: this is a pessimisation, hasNext will invoke
        // computeNext under the hood, which is an expensive operation to produce a value that we plan to throw away.
        // Instead, it is the responsibility of the child iterators to make skipTo fast when the iterator is exhausted.
        for (KeyRangeIterator range : ranges)
            range.skipTo(nextKey);
    }

    public void close() throws IOException
    {
        // Due to lazy key fetching, we cannot close iterator immediately
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    public static Builder builder()
    {
        return builder(10);
    }

    public static KeyRangeIterator build(Iterable<KeyRangeIterator> tokens)
    {
        return KeyRangeUnionIterator.builder(Iterables.size(tokens)).add(tokens).build();
    }

    public static class Builder extends KeyRangeIterator.Builder
    {
        protected List<KeyRangeIterator> rangeIterators;

        public Builder(int size)
        {
            super(IteratorType.UNION);
            this.rangeIterators = new ArrayList<>(size);
        }

        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getMaxKeys() > 0)
            {
                rangeIterators.add(range);
                statistics.update(range);
            }
            else
                FileUtils.closeQuietly(range);

            return this;
        }

        @Override
        public KeyRangeIterator.Builder add(List<KeyRangeIterator> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public KeyRangeIterator.Builder add(Iterable<KeyRangeIterator> ranges)
        {
            if (ranges == null || Iterables.isEmpty(ranges))
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
            switch (rangeCount())
            {
                case 1:
                    return rangeIterators.get(0);

                default:
                    //TODO Need to test whether an initial sort improves things
                    return new KeyRangeUnionIterator(statistics, rangeIterators);
            }
        }
    }
}
