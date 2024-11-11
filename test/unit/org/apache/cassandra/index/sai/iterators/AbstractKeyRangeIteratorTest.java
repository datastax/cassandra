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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.utils.Pair;

public class AbstractKeyRangeIteratorTest extends SaiRandomizedTest
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    final KeyRangeIterator buildIntersection(KeyRangeIterator... ranges)
    {
        return KeyRangeIntersectionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildSelectiveIntersection(int limit, KeyRangeIterator... ranges)
    {
        return KeyRangeIntersectionIterator.<PrimaryKey>builder(limit).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final KeyRangeIterator buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    static KeyRangeIterator buildUnion(KeyRangeIterator... ranges)
    {
        return KeyRangeUnionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    static KeyRangeIterator buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    static KeyRangeIterator buildConcat(KeyRangeIterator... ranges)
    {
        return KeyRangeConcatIterator.builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    static KeyRangeIterator buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private static KeyRangeIterator[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(AbstractKeyRangeIteratorTest::build).toArray(KeyRangeIterator[]::new);
    }

    protected static LongIterator build(long... tokens)
    {
        return new LongIterator(tokens);
    }

    protected KeyRangeIterator build(KeyRangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        KeyRangeIterator rangeA = new LongIterator(tokensA);
        KeyRangeIterator rangeB = new LongIterator(tokensB);

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

    static void validateWithSkipping(KeyRangeIterator ri, long[] totalOrdering)
    {
        int count = 0;
        while (ri.hasNext())
        {
            // make sure hasNext plays nice with skipTo
            if (randomBoolean())
                ri.hasNext();

            // skipping to the same element should also be a no-op
            if (randomBoolean())
                ri.skipTo(LongIterator.fromToken(totalOrdering[count]));

            // skip a few elements
            if (nextDouble() < 0.1)
            {
                int n = nextInt(1, 3);
                if (count + n < totalOrdering.length)
                {
                    count += n;
                    ri.skipTo(LongIterator.fromToken(totalOrdering[count]));
                }
            }
            Assert.assertEquals(totalOrdering[count++], ri.next().token().getLongValue());
        }
        Assert.assertEquals(totalOrdering.length, count);
    }

    static Set<Long> toSet(long[] tokens)
    {
        return Arrays.stream(tokens).boxed().collect(Collectors.toSet());
    }

    /**
     * @return a random {Concat,Intersection, Union} iterator, and a long[] of the elements in the iterator.
     *         elements will range from 0..1024.
     */
    static Pair<KeyRangeIterator, long[]> createRandomIterator()
    {
        var n = randomIntBetween(0, 3);
        switch (n)
        {
            case 0:
                return KeyRangeConcatIteratorTest.createRandom();
            case 1:
                return KeyRangeIntersectionIteratorTest.createRandom(nextInt(1, 16));
            case 2:
                return KeyRangeUnionIteratorTest.createRandom(nextInt(1, 16));
            default:
                throw new AssertionError();
        }
    }
}
