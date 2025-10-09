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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Range Union Iterator is used to return sorted stream of elements from multiple KeyRangeIterator instances.
 * Keys are sorted by natural order of PrimaryKey, however if two keys are equal by their natural order,
 * the one with an empty clustering always wins.
 */
@SuppressWarnings("resource")
public class KeyRangeUnionIterator extends KeyRangeIterator
{
    public final List<KeyRangeIterator> ranges;

    // If set, we must first skip this partition.
    private DecoratedKey partitionToSkip = null;

    private KeyRangeUnionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);
        this.ranges = new ArrayList<>(ranges);
    }

    public PrimaryKey computeNext()
    {
        // If we already emitted a partition key for the whole partition (== pk with empty clustering),
        // we should not emit any more keys from this partition.
        maybeSkipCurrentPartition();

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
                {
                    // Due to the way how we compare PrimaryKeys with empty clusterings which is hard to change now,
                    // the fact that two primary keys compare the same doesn't guarantee they have the same clustering.
                    // The clustering information is ignored if one key has empty clustering, so a key with an empty
                    // clustering will match any key with a non-empty clustering (as long as the partition keys are the same).
                    // So we may end up in a situation when one or more ranges have empty clustering and the others
                    // have non-empty. This situation is likely if we mix row-aware (DC, EC, ...) indexes with older
                    // non-row-aware (AA) indexes.
                    // In that case we absolutely *must* pick the key with an empty clustering,
                    // as it matches all rows in the partition
                    // (and hence, it includes all the rows matched by the keys from the other candidates).
                    // Thanks to postfiltering, we are allowed to return more rows than necessary in SAI, but not less.
                    // If we chose one of the specific keys with non-empty clustering (e.g. pick the first one we see),
                    // we may miss rows matched by the non-row-aware index, as well as the rows matched by
                    // the other row-aware indexes.
                    if (range.peek().hasEmptyClustering() && !candidate.peek().hasEmptyClustering())
                        candidate = range;
                    else
                        range.next();   // truly equal by partition and clustering, so we can just get rid of one
                }
                else if (cmp > 0)
                {
                    candidate = range;
                }
            }
        }

        if (candidate == null)
            return endOfData();

        var result = candidate.next();

        // If the winning candidate has an empty clustering, this means it selects the whole partition, so
        // advance all other ranges to the end of this partition to avoid duplicates.
        // We delay that to the next call to computeNext() though, because if we have a wide partition, it's better
        // to first let the caller consume all the rows from this partition - maybe they won't call again.
        if (result.hasEmptyClustering())
            partitionToSkip = result.partitionKey();

        return result;
    }

    private void maybeSkipCurrentPartition()
    {
        if (partitionToSkip != null)
        {
            for (KeyRangeIterator range : ranges)
                skipPartition(range, partitionToSkip);

            partitionToSkip = null;
        }
    }

    private void skipPartition(KeyRangeIterator iterator, DecoratedKey partitionKey)
    {
        // TODO: Push this logic down to the iterator where it can be more efficient
        while (iterator.hasNext() && iterator.peek().partitionKey() != null && iterator.peek().partitionKey().compareTo(partitionKey) <= 0)
            iterator.next();
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
                    rangeIterators.sort((a, b) -> a.getMinimum().compareTo(b.getMinimum()));
                    boolean isDisjoint = true;
                    for (int i = 0; i < rangeIterators.size() - 1; i++)
                    {
                        // If a's max is greater than or equal to b's min, then the ranges are not disjoint
                        var a = rangeIterators.get(i);
                        var b = rangeIterators.get(i + 1);
                        if (a.getMaximum().compareTo(b.getMinimum()) >= 0)
                        {
                            isDisjoint = false;
                            break;
                        }
                    }
                    // If the iterators are not overlapping, then we can use the concat iterator which is more efficient
                    return isDisjoint ? new KeyRangeConcatIterator(statistics, rangeIterators)
                                      : new KeyRangeUnionIterator(statistics, rangeIterators);
            }
        }
    }
}
