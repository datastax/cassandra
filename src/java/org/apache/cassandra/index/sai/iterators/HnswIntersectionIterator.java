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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * The HnswIntersectionIterator is an implementation of the KeyRangeIterator
 * that handles intersection operations between normal SAI predicates and Hierarchical
 * Navigable Small World (HNSW) graphs.
 *
 * HNSW is special because it returns results in similarity order, not in key order,
 * and because it's really more of an order by than a where clause; HNSW doesn't
 * "eliminate" any keys, they will all be in the results if you iterate far down enough.
 *
 * HnswIntersectionIterator performs what the Pinterest guys call Post Filtering:
 * we perform the HNSW search, then go through the keys from the other predicates
 * looking for matches.  If we don't get enough matches, we expand the HNSW search
 * window and perform the search over again.
 *
 * To attempt to minimize the passes, we double the HNSW window with each iteration.
 */
public class HnswIntersectionIterator extends KeyRangeIterator
{
    private final Builder builder;
    private final KeyRangeIterator hnswIterator;
    private HnswOnePassIterator onePassIterator;
    private final Set<PrimaryKey> hnswMatches = new HashSet<>();
    private final Set<PrimaryKey> seenMatchingKeys = new HashSet<>();
    private int hnswBatchSize;

    protected HnswIntersectionIterator(Builder builder)
    {
        super(builder.statistics);
        this.builder = builder;
        this.hnswIterator = builder.hnswIterator;
        hnswBatchSize = limit();
        this.onePassIterator = newOnePassIterator(builder);
        loadMoreHnswMatches();
    }

    private HnswOnePassIterator newOnePassIterator(Builder builder)
    {
        return new HnswOnePassIterator(builder.limit,
                                       builder.otherSuppliers.stream().map(Supplier::get).collect(Collectors.toList()));
    }

    @Override
    public PrimaryKey skipTo(PrimaryKey nextKey)
    {
        return onePassIterator.skipTo(nextKey);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        // external callers will call skipTo()
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        onePassIterator.close();
    }

    // TODO do we need to compute all the rows, and then re-sort by PK?
    @Override
    protected PrimaryKey computeNext()
    {
        // TODO integrate this better with the actual HNSW batch window
        while (seenMatchingKeys.size() < limit())
        {
            while (onePassIterator.hasNext())
            {
                var next = onePassIterator.next();
                if (seenMatchingKeys.add(next))
                {
                    return next;
                }
            }

            if (!loadMoreHnswMatches()) {
                return endOfData();
            } else {
                hnswBatchSize *= 2;
            }
            onePassIterator = newOnePassIterator(builder);
        }
        return endOfData();
    }

    private boolean loadMoreHnswMatches()
    {
        for (int i = 0; i < hnswBatchSize; i++)
        {
            if (!hnswIterator.hasNext())
            {
                return i > 0;
            }
            hnswMatches.add(hnswIterator.next());
        }
        return true;
    }

    private int limit()
    {
        return builder.limit;
    }

    public static KeyRangeIterator.Builder builder(int otherExpressionsSize)
    {
        return new Builder(otherExpressionsSize);
    }

    /**
     * Performs a single pass over the other predicates, looking for matches.
     */
    private class HnswOnePassIterator extends KeyRangeIterator
    {
        private final KeyRangeIterator otherIterator;

        public HnswOnePassIterator(int limit, List<KeyRangeIterator> otherIterators)
        {
            super(new PlaceholderStatistics());
            var builder = KeyRangeIntersectionIterator.builder(otherIterators.size(), limit);
            otherIterator = builder.add(otherIterators).build();
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            otherIterator.performSkipTo(nextKey);
        }

        @Override
        public void close() throws IOException
        {
            otherIterator.close();
        }

        @Override
        protected PrimaryKey computeNext()
        {
            while (otherIterator.hasNext())
            {
                var next = otherIterator.next();
                if (hnswMatches.contains(next))
                {
                    return next;
                }
            }
            return endOfData();
        }
    }

    // FIXME
    private static class PlaceholderStatistics extends KeyRangeIterator.Builder.Statistics
    {
        @Override
        public void update(KeyRangeIterator range)
        {
        }
    }

    public static class Builder extends KeyRangeIterator.Builder
    {
        private List<Supplier<KeyRangeIterator>> otherSuppliers;
        private KeyRangeIterator hnswIterator;
        private int limit;

        public Builder(int expressionCount)
        {
            super(new PlaceholderStatistics());
            otherSuppliers = new ArrayList<>(expressionCount);
        }

        @Override
        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            throw new UnsupportedOperationException("Use the Supplier overload");
        }

        @Override
        public KeyRangeIterator.Builder add(Supplier<KeyRangeIterator> iteratorSupplier, Expression expression, int limit)
        {
            if (expression.getOp() == Expression.IndexOperator.ANN)
            {
                this.limit = limit;
                this.hnswIterator = iteratorSupplier.get();
            }
            else
            {
                otherSuppliers.add(iteratorSupplier);
            }
            return this;
        }

        @Override
        public int rangeCount()
        {
            return otherSuppliers.size();
        }

        @Override
        public void cleanup()
        {
        }

        @Override
        protected KeyRangeIterator buildIterator()
        {
            return new HnswIntersectionIterator(this);
        }
    }
}
