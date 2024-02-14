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

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator wrapper that wraps two iterators (left and right) and returns the primary keys from the left iterator
 * that do not match the primary keys from the right iterator. The keys returned by the wrapped iterators must
 * follow token-clustering order.
 */
public class RangeAntiJoinIterator extends RangeIterator
{
    final RangeIterator left;
    final RangeIterator right;

    private PrimaryKey nextKeyToSkip = null;

    private RangeAntiJoinIterator(RangeIterator left, RangeIterator right)
    {
        super(left.getMinimum(), left.getMaximum(), left.getMaxKeys());
        this.left = left;
        this.right = right;
    }

    public static RangeAntiJoinIterator create(RangeIterator left, RangeIterator right)
    {
        return new RangeAntiJoinIterator(left, right);
    }

    protected void performSkipTo(Token nextToken)
    {
        left.skipTo(nextToken);
        right.skipTo(nextToken);
    }

    @Override
    protected IntersectionResult performIntersect(PrimaryKey otherKey)
    {
        var leftResult = left.intersect(otherKey);
        // Only do the right intersection if the left iterator matches.
        if (leftResult == IntersectionResult.MATCH && right.intersect(otherKey) == IntersectionResult.MATCH)
            return IntersectionResult.MISS;
        return leftResult;
    }

    public void close() throws IOException
    {
        FileUtils.close(left, right);
    }

    protected PrimaryKey computeNext()
    {
        while (left.hasNext())
        {
            var key = left.next();
            switch (right.intersect(key))
            {
                case MATCH:
                    continue;
                case MISS:
                case EXHAUSTED:
                    return key;
            }
        }
        return endOfData();
    }
}
