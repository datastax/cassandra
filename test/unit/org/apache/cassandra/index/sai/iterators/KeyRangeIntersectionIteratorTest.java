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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class KeyRangeIntersectionIteratorTest extends AbstractKeyRangeIteratorTester
{
    @Test
    public void testNoOverlappingValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        assertEquals(convert(), convert(builder.build()));

        builder = KeyRangeIntersectionIterator.builder();
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 6L }));

        KeyRangeIterator range = builder.build();

        assertNotNull(range);
        assertFalse(range.hasNext());

        builder = KeyRangeIntersectionIterator.builder();
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 0L, 10L, 12L }));

        range = builder.build();

        assertNotNull(range);
        assertFalse(range.hasNext());
    }

    @Test
    public void testOverlappingValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        assertEquals(convert(4L, 6L), convert(builder.build()));
    }

    @Test
    public void testSameValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));

        assertEquals(convert(1L, 2L, 3L, 4L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testSkipTo()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L, 9L, 10L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L, 7L, 10L, 12L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 7L, 9L, 10L }));

        KeyRangeIterator range = builder.build();
        assertNotNull(range);

        // first let's skipTo something before range
        range.skipTo(LongIterator.fromToken(3L));
        assertEquals(4L, range.peek().token().getLongValue());

        // now let's skip right to the send value
        range.skipTo(LongIterator.fromToken(5L));
        assertEquals(6L, range.peek().token().getLongValue());

        // now right to the element
        range.skipTo(LongIterator.fromToken(7L));
        assertEquals(7L, range.peek().token().getLongValue());
        assertEquals(7L, range.next().token().getLongValue());

        Assert.assertTrue(range.hasNext());
        assertEquals(10L, range.peek().token().getLongValue());

        // now right after the last element
        range.skipTo(LongIterator.fromToken(11L));
        assertFalse(range.hasNext());
    }

    @Test
    public void testMinMaxAndCount()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 9L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 9L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(3L, builder.getTokenCount());

        KeyRangeIterator tokens = builder.build();

        assertNotNull(tokens);
        assertEquals(7L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(3L, tokens.getMaxKeys());

        assertEquals(convert(9L), convert(builder.build()));
    }

    @Test
    public void testBuilder()
    {
        KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        assertEquals(0L, builder.getTokenCount());
        assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(6L, builder.getMinimum().token().getLongValue());
        assertEquals(6L, builder.getMaximum().token().getLongValue());
        assertEquals(3L, builder.getTokenCount());
        assertEquals(3L, builder.rangeCount());
        assertFalse(builder.statistics.isDisjoint());

        assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        assertEquals(6L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(convert(6L), convert(builder.build()));

        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[]{ 1L, 5L, 6L }));
        builder.add(new LongIterator(new long[]{ 3L, 5L, 6L }));

        var tokens = builder.build();

        assertEquals(convert(5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        var emptyTokens = KeyRangeIntersectionIterator.builder().build();
        assertEquals(0, emptyTokens.getMaxKeys());

        builder = KeyRangeIntersectionIterator.builder();
        assertEquals(0L, builder.add((KeyRangeIterator) null).rangeCount());
        assertEquals(0L, builder.add((List<KeyRangeIterator>) null).getTokenCount());
        assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        var single = new LongIterator(new long[] { 1L, 2L, 3L });
        var range = KeyRangeIntersectionIterator.<PrimaryKey>builder().add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        assertEquals(range, single);

        // Make a difference between empty and null ranges.
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        assertEquals(0L, builder.rangeCount());
        builder.add(single);
        assertEquals(1L, builder.rangeCount());
        range = builder.build();
        assertEquals(0, range.getMaxKeys());

        // disjoint case
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));

        Assert.assertTrue(builder.statistics.isDisjoint());

        var disjointIntersection = builder.build();
        assertNotNull(disjointIntersection);
        assertFalse(disjointIntersection.hasNext());

    }

    @Test
    public void emptyRangeTest()
    {
        KeyRangeIterator.Builder builder;

        // empty, then non-empty
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());

        // non-empty, then empty
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));

        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // empty, then non-empty then empty again
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // non-empty, empty, then non-empty again
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());
    }

    public static void assertEmpty(KeyRangeIterator range)
    {
        Assert.assertNull(range.getMinimum());
        Assert.assertNull(range.getMaximum());
        assertFalse(range.hasNext());
        assertEquals(0, range.getMaxKeys());
    }

    @Test
    public void testClose() throws IOException
    {
        var tokens = KeyRangeIntersectionIterator.<PrimaryKey>builder()
                                                 .add(new LongIterator(new long[] { 1L, 2L, 3L }))
                                                 .build();

        assertNotNull(tokens);
        tokens.close();
    }

    @Test
    public void testIsOverlapping()
    {
        KeyRangeIterator rangeA, rangeB;

        rangeA = new LongIterator(new long[] { 1L, 5L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 6L });
        Assert.assertTrue(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 1L, 4L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        assertFalse(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 6L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 4L });
        assertFalse(KeyRangeIterator.isOverlapping(rangeA, rangeB));
    }

    @Test
    public void testIntersectionOfRandomRanges()
    {
        for (int attempt = 0; attempt < 16; attempt++)
        {
            var p = createRandom(nextInt(2, 16));
            validateWithSkipping(p.left, p.right);
        }
    }

    /**
     * @return a long[][] of random elements, and a long[] of the intersection of those elements
     */
    static Pair<KeyRangeIterator, long[]> createRandom(int nRanges)
    {
        // generate randomize ranges
        long[][] ranges = new long[nRanges][];
        for (int i = 0; i < ranges.length; i++)
        {
            int rangeSize = nextInt(16, 512);
            LongSet range = new LongHashSet(rangeSize);

            for (int j = 0; j < rangeSize; j++)
                range.add(nextInt(1024));

            ranges[i] = range.toArray();
            Arrays.sort(ranges[i]);
        }
        var builder = KeyRangeIntersectionIterator.builder();
        for (long[] range : ranges)
            builder.add(new LongIterator(range));

        Set<Long> expectedSet = toSet(ranges[0]);
        IntStream.range(1, ranges.length).forEach(i -> expectedSet.retainAll(toSet(ranges[i])));
        return Pair.create(builder.build(), expectedSet.stream().mapToLong(Long::longValue).sorted().toArray());
    }

    // SAI specific tests
    @Test
    public void testSelectiveIntersection()
    {
        var intersection = buildSelectiveIntersection(2,
                                                      arr(1L, 4L, 6L, 7L),
                                                      arr(1L, 4L, 5L, 6L),
                                                      arr(4L, 6L, 8L, 9L, 10L)); // skipped

        assertEquals(convert(1L, 4L, 6L), convert(intersection));

        intersection = buildSelectiveIntersection(1,
                                                  arr(2L, 4L, 6L),
                                                  arr(1L, 4L, 5L, 6L),       // skipped
                                                  arr(4L, 6L, 8L, 9L, 10L)); // skipped

        assertEquals(convert(2L, 4L, 6L), convert(intersection));
    }
}
