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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * {@link KeyRangeConcatIterator} takes a list of sorted range iterator and concatenates them, leaving duplicates in
 * place, to produce a new stably sorted iterator. Duplicates are eliminated later in
 * {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}
 * as results from multiple SSTable indexes and their respective segments are consumed.
 *
 * ex. (1, 2, 3) + (3, 3, 4, 5) -> (1, 2, 3, 3, 3, 4, 5)
 * ex. (1, 2, 2, 3) + (3, 4, 4, 6, 6, 7) -> (1, 2, 2, 3, 3, 4, 4, 6, 6, 7)
 *
 */
public class KeyRangeConcatIterator extends KeyRangeIterator
{
    private final Iterator<KeyRangeIterator> ranges;
    private KeyRangeIterator currentRange;
    private final List<KeyRangeIterator> toRelease;

    protected KeyRangeConcatIterator(KeyRangeIterator.Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);

        if (ranges.isEmpty())
            throw new IllegalArgumentException("Cannot concatenate empty list of ranges");
        this.ranges = ranges.iterator();
        currentRange = this.ranges.next();
        this.toRelease = ranges;
    }

    @Override
    protected void performSkipTo(PrimaryKey primaryKey)
    {
        while (true)
        {
            if (currentRange.getMaximum().compareTo(primaryKey) >= 0)
            {
                currentRange.skipTo(primaryKey);
                return;
            }
            if (!ranges.hasNext())
            {
                currentRange.skipTo(primaryKey);
                return;
            }
            currentRange = ranges.next();
        }
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (!currentRange.hasNext())
        {
            if (!ranges.hasNext())
                return endOfData();

            currentRange = ranges.next();
        }
        return currentRange.next();
    }

    @Override
    public void close() throws IOException
    {
        // due to lazy key fetching, we cannot close iterator immediately
        toRelease.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder()
    {
        return builder(1);
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    public static KeyRangeIterator build(List<KeyRangeIterator> tokens)
    {
        return new Builder(tokens.size()).add(tokens).build();
    }

    public static class Builder extends KeyRangeIterator.Builder
    {
        // We can use a list because the iterators are already in order
        private final List<KeyRangeIterator> rangeIterators;
        public Builder(int size)
        {
            super(IteratorType.CONCAT);
            this.rangeIterators = new ArrayList<>(size);
        }

        @Override
        public int rangeCount()
        {
            return rangeIterators.size();
        }

        @Override
        public Collection<KeyRangeIterator> ranges()
        {
            return rangeIterators;
        }

        @Override
        public Builder add(KeyRangeIterator range)
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

        protected KeyRangeIterator buildIterator()
        {
            if (rangeCount() == 0)
                return empty();
            if (rangeCount() == 1)
                return rangeIterators.get(0);
            return new KeyRangeConcatIterator(statistics, rangeIterators);
        }
    }
}
