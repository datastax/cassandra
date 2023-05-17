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
    protected void performSkipTo(PrimaryKey nextKey)
    {
        onePassIterator.performSkipTo(nextKey);
    }

    @Override
    public void close() throws IOException
    {
        onePassIterator.close();
    }

    @Override
    protected PrimaryKey computeNext()
    {
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
        private Expression hnswExpression;
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
