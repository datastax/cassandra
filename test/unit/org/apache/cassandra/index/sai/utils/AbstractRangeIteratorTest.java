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

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.junit.Assert;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AbstractRangeIteratorTest extends SaiRandomizedTest
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    final RangeIterator buildIntersection(RangeIterator... ranges)
    {
        return RangeIntersectionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildSelectiveIntersection(int limit, RangeIterator... ranges)
    {
        return RangeIntersectionIterator.<PrimaryKey>builder(limit).add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final RangeIterator buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    final RangeIterator buildUnion(RangeIterator... ranges)
    {
        return RangeUnionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    final RangeIterator buildConcat(RangeIterator... ranges)
    {
        return RangeConcatIterator.builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private RangeIterator[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(this::build).toArray(RangeIterator[]::new);
    }

    protected LongIterator build(long... tokens)
    {
        return new LongIterator(tokens);
    }

    protected RangeIterator build(RangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        RangeIterator rangeA = new LongIterator(tokensA);
        RangeIterator rangeB = new LongIterator(tokensB);

        switch (type)
        {
            case INTERSECTION:
                return buildIntersection(rangeA, rangeB);
            case UNION:
                return buildUnion(rangeA, rangeB);
            case CONCAT:
                return buildConcat(rangeA, rangeB);
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    static void validateWithSkipping(RangeIterator tokens, long[] totalOrdering)
    {
        var R = ThreadLocalRandom.current();

        int count = 0;
        while (tokens.hasNext())
        {
            if (R.nextDouble() < 0.1)
            {
                int n = R.nextInt(1, 3);
                // ensure we skip to a different value, otherwise skipTo is a no-op
                while (count + n < totalOrdering.length && totalOrdering[count] == totalOrdering[count + n])
                    n++;
                if (count + n < totalOrdering.length)
                {
                    count += n;
                    tokens.skipTo(LongIterator.fromToken(totalOrdering[count]));
                }
            }
            Assert.assertEquals(totalOrdering[count++], tokens.next().token().getLongValue());
        }
        Assert.assertEquals(totalOrdering.length, count);
    }

    static Set<Long> toSet(long[] tokens)
    {
        return Arrays.stream(tokens).boxed().collect(Collectors.toSet());
    }
}
