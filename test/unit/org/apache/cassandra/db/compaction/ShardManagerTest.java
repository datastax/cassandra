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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ShardManagerTest
{
    final IPartitioner partitioner = Murmur3Partitioner.instance;
    final Token minimumToken = partitioner.getMinimumToken();

    SortedLocalRanges localRanges;
    List<Splitter.WeightedRange> weightedRanges;

    static final double delta = 1e-15;

    @Before
    public void setUp()
    {
        weightedRanges = new ArrayList<>();
        var realm = Mockito.mock(CompactionRealm.class);
        localRanges = Mockito.mock(SortedLocalRanges.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(localRanges.getRanges()).thenAnswer(invocation -> weightedRanges);
        Mockito.when(localRanges.getRealm()).thenReturn(realm);
        Mockito.when(realm.estimatedPartitionCount()).thenReturn(10000L);
    }

    @Test
    public void testRangeSpannedFullOwnership()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(minimumToken, minimumToken)));
        ShardManager shardManager = new ShardManagerNoDisks(localRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.5, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.2, shardManager.rangeSpanned(range(0.3, 0.5)), delta);

        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, Double.NaN)), delta);
        // single-token-span correction
        assertEquals(0.02, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN, 200)), delta);
        // small partition count correction
        assertEquals(0.0001, shardManager.rangeSpanned(mockedTable(0.3, 0.30001, Double.NaN, 1)), delta);
        assertEquals(0.001, shardManager.rangeSpanned(mockedTable(0.3, 0.30001, Double.NaN, 10)), delta);
        assertEquals(0.01, shardManager.rangeSpanned(mockedTable(0.3, 0.31, Double.NaN, 10)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.0)), delta);
        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, -1)), delta);

        // correction over coverage
        assertEquals(0.02, shardManager.rangeSpanned(mockedTable(0.3, 0.5, 1e-50, 200)), delta);
    }

    @Test
    public void testRangeSpannedPartialOwnership()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.05), tokenAt(0.15))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.3), tokenAt(0.4))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.45), tokenAt(0.5))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.7), tokenAt(0.75))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.75), tokenAt(0.85))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.90), tokenAt(0.91))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.92), tokenAt(0.94))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.98), tokenAt(1.0))));
        double total = weightedRanges.stream().mapToDouble(wr -> wr.range().left.size(wr.range().right)).sum();

        ShardManager shardManager = new ShardManagerNoDisks(localRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.15, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.15, shardManager.rangeSpanned(range(0.3, 0.5)), delta);
        assertEquals(0.0, shardManager.rangeSpanned(range(0.5, 0.7)), delta);
        assertEquals(total, shardManager.rangeSpanned(range(0.0, 1.0)), delta);


        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, Double.NaN)), delta);

        // single-partition correction
        assertEquals(0.02 * total, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN, 200)), delta);
        // out-of-local-range correction
        assertEquals(0.03, shardManager.rangeSpanned(mockedTable(0.6, 0.73, Double.NaN, 200)), delta);
        // completely outside should use partition-based count
        assertEquals(0.02 * total, shardManager.rangeSpanned(mockedTable(0.6, 0.7, Double.NaN, 200)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 0.0)), delta);
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, -1)), delta);

        // correction over coverage, no recalculation
        assertEquals(0.02 * total, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 1e-50, 200)), delta);
    }

    @Test
    public void testRangeSpannedWeighted()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.05), tokenAt(0.15))));
        weightedRanges.add(new Splitter.WeightedRange(0.5, new Range<>(tokenAt(0.3), tokenAt(0.4))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.45), tokenAt(0.5))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.7), tokenAt(0.75))));
        weightedRanges.add(new Splitter.WeightedRange(0.2, new Range<>(tokenAt(0.75), tokenAt(0.85))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.90), tokenAt(0.91))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.92), tokenAt(0.94))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.98), tokenAt(1.0))));
        double total = weightedRanges.stream().mapToDouble(wr -> wr.size()).sum();

        ShardManager shardManager = new ShardManagerNoDisks(localRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.10, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.10, shardManager.rangeSpanned(range(0.3, 0.5)), delta);
        assertEquals(0.0, shardManager.rangeSpanned(range(0.5, 0.7)), delta);
        assertEquals(total, shardManager.rangeSpanned(range(0.0, 1.0)), delta);


        assertEquals(0.06, shardManager.rangeSpanned(mockedTable(0.5, 0.8, Double.NaN)), delta);

        // single-partition correction
        assertEquals(0.02 * total, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN, 200)), delta);
        // out-of-local-range correction
        assertEquals(0.02 * total, shardManager.rangeSpanned(mockedTable(0.6, 0.7, Double.NaN, 200)), delta);
        assertEquals(0.03, shardManager.rangeSpanned(mockedTable(0.6, 0.73, Double.NaN)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.06, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 0.0)), delta);
        assertEquals(0.06, shardManager.rangeSpanned(mockedTable(0.5, 0.8, -1)), delta);

        // correction over coverage, no recalculation
        assertEquals(0.02 * total, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 1e-50, 200)), delta);
    }

    Token tokenAt(double pos)
    {
        return partitioner.split(minimumToken, minimumToken, pos);
    }

    Range<Token> range(double start, double end)
    {
        return new Range<>(tokenAt(start), tokenAt(end));
    }

    CompactionSSTable mockedTable(double start, double end, double reportedCoverage)
    {
        return mockedTable(start, end, reportedCoverage, ShardManager.PER_PARTITION_SPAN_THRESHOLD * 2);
    }

    CompactionSSTable mockedTable(double start, double end, double reportedCoverage, long estimatedKeys)
    {
        CompactionSSTable mock = Mockito.mock(CompactionSSTable.class);
        Mockito.when(mock.getFirst()).thenReturn(tokenAt(start).minKeyBound());
        Mockito.when(mock.getLast()).thenReturn(tokenAt(end).minKeyBound());
        Mockito.when(mock.tokenSpaceCoverage()).thenReturn(reportedCoverage);
        Mockito.when(mock.estimatedKeys()).thenReturn(estimatedKeys);
        return mock;
    }

    @Test
    public void testShardBoundaries()
    {
        // no shards
        testShardBoundaries(ints(), 1, 1, ints(10, 50));
        // split on disks at minimum
        testShardBoundaries(ints(30), 1, 2, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50), 1, 5, ints(10, 51, 61, 70));

        // no disks
        testShardBoundaries(ints(30), 2, 1, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50), 5, 1, ints(10, 51, 61, 70));

        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 80), 3, 3, ints(0, 90));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(0, 51, 61, 100));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90), 3, 3, ints(0, 49, 59, 100));
        testShardBoundaries(ints(12, 23, 33, 45, 56, 70, 80, 90), 3, 3, ints(0, 9, 11, 20, 21, 39, 41, 50, 51, 60, 64, 68, 68, 100));

        // uneven
        testShardBoundaries(ints(8, 16, 24, 32, 42, 52, 62, 72, 79, 86, 93), 4, ints(32, 72, 100), ints(0, 100));
        testShardBoundaries(ints(1, 2, 3, 4, 6, 8, 10, 12, 34, 56, 78), 4, ints(4, 12, 100), ints(0, 100));
    }

    @Test
    public void testShardBoundariesWraparound()
    {
        // no shards
        testShardBoundaries(ints(), 1, 1, ints(50, 10));
        // split on disks at minimum
        testShardBoundaries(ints(70), 1, 2, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70), 1, 5, ints(91, 31, 61, 71));
        // no disks
        testShardBoundaries(ints(70), 2, 1, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70), 5, 1, ints(91, 31, 61, 71));
        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90), 3, 3, ints(81, 71));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90), 3, 3, ints(51, 41));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90), 3, 3, ints(21, 11));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90), 3, 3, ints(89, 79));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90), 3, 3, ints(59, 49));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90), 3, 3, ints(29, 19));

        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(91, 51, 61, 91));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(21, 51, 61, 21));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(71, 51, 61, 71));
    }

    @Test
    public void testShardBoundariesWeighted()
    {
        // no shards
        testShardBoundariesWeighted(ints(), 1, 1, ints(10, 50));
        // split on disks at minimum
        testShardBoundariesWeighted(ints(30), 1, 2, ints(10, 50));
        testShardBoundariesWeighted(ints(22, 34, 45, 64), 1, 5, ints(10, 51, 61, 70));

        // no disks
        testShardBoundariesWeighted(ints(30), 2, 1, ints(10, 50));
        testShardBoundariesWeighted(ints(22, 34, 45, 64), 5, 1, ints(10, 51, 61, 70));

        // split
        testShardBoundariesWeighted(ints(10, 20, 30, 40, 50, 60, 70, 80), 3, 3, ints(0, 90));
        testShardBoundariesWeighted(ints(14, 29, 43, 64, 71, 79, 86, 93), 3, 3, ints(0, 51, 61, 100));
        testShardBoundariesWeighted(ints(18, 36, 50, 63, 74, 83, 91, 96), 3, 3, ints(0, 40, 40, 70, 70, 90, 90, 100));
    }

    private int[] ints(int... values)
    {
        return values;
    }

    private void testShardBoundaries(int[] expected, int numShards, int numDisks, int[] rangeBounds)
    {
        CompactionRealm realm = Mockito.mock(CompactionRealm.class);
        when(realm.getPartitioner()).thenReturn(partitioner);

        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(1.0, new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTesting(realm, ranges);

        List<Token> diskBoundaries = sortedRanges.split(numDisks);
        int[] result = getShardBoundaries(numShards, diskBoundaries, sortedRanges);
        Assert.assertArrayEquals("Disks " + numDisks + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private void testShardBoundariesWeighted(int[] expected, int numShards, int numDisks, int[] rangeBounds)
    {
        CompactionRealm realm = Mockito.mock(CompactionRealm.class);
        when(realm.getPartitioner()).thenReturn(partitioner);

        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(2.0 / (rangeBounds.length - i), new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTesting(realm, ranges);

        List<Token> diskBoundaries = sortedRanges.split(numDisks);
        int[] result = getShardBoundaries(numShards, diskBoundaries, sortedRanges);
        Assert.assertArrayEquals("Disks " + numDisks + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private void testShardBoundaries(int[] expected, int numShards, int[] diskPositions, int[] rangeBounds)
    {
        CompactionRealm realm = Mockito.mock(CompactionRealm.class);
        when(realm.getPartitioner()).thenReturn(partitioner);

        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(1.0, new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTesting(realm, ranges);

        List<Token> diskBoundaries = Arrays.stream(diskPositions).mapToObj(this::getToken).collect(Collectors.toList());
        int[] result = getShardBoundaries(numShards, diskBoundaries, sortedRanges);
        Assert.assertArrayEquals("Disks " + Arrays.toString(diskPositions) + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private int[] getShardBoundaries(int numShards, List<Token> diskBoundaries, SortedLocalRanges sortedRanges)
    {
        DiskBoundaries db = Mockito.mock(DiskBoundaries.class);
        when(db.getLocalRanges()).thenReturn(sortedRanges);
        when(db.getPositions()).thenReturn(diskBoundaries);

        var rs = Mockito.mock(AbstractReplicationStrategy.class);
        final ShardTracker shardTracker = ShardManager.create(db, rs, false)
                                                      .boundaries(numShards);
        IntArrayList list = new IntArrayList();
        for (int i = 0; i < 100; ++i)
        {
            if (shardTracker.advanceTo(getToken(i)))
                list.addInt(fromToken(shardTracker.shardStart()));
        }
        return list.toIntArray();
    }

    private Token getToken(int x)
    {
        return tokenAt(x / 100.0);
    }

    private int fromToken(Token t)
    {
        return (int) Math.round(partitioner.getMinimumToken().size(t) * 100.0);
    }

    @Test
    public void testRangeEnds()
    {
        CompactionRealm realm = Mockito.mock(CompactionRealm.class);
        when(realm.getPartitioner()).thenReturn(partitioner);
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTestingFull(realm);

        for (int numDisks = 1; numDisks <= 3; ++numDisks)
        {
            List<Token> diskBoundaries = sortedRanges.split(numDisks);
            DiskBoundaries db = Mockito.mock(DiskBoundaries.class);
            when(db.getLocalRanges()).thenReturn(sortedRanges);
            when(db.getPositions()).thenReturn(diskBoundaries);

            var rs = Mockito.mock(AbstractReplicationStrategy.class);

            ShardManager shardManager = ShardManager.create(db, rs, false);
            for (int numShards = 1; numShards <= 3; ++numShards)
            {
                ShardTracker iterator = shardManager.boundaries(numShards);
                iterator.advanceTo(partitioner.getMinimumToken());

                int count = 1;
                for (Token end = iterator.shardEnd(); end != null; end = iterator.shardEnd())
                {
                    assertFalse(iterator.advanceTo(end));
                    assertTrue(iterator.advanceTo(end.nextValidToken()));
                    ++count;
                }
                assertEquals(numDisks * numShards, count);

                assertEquals(numDisks * numShards, iterator.count());
            }
        }
    }

    @Test
    public void testSpannedShardCount()
    {
        CompactionRealm realm = Mockito.mock(CompactionRealm.class);
        when(realm.getPartitioner()).thenReturn(partitioner);
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTestingFull(realm);

        for (int numDisks = 1; numDisks <= 3; ++numDisks)
        {
            List<Token> diskBoundaries = sortedRanges.split(numDisks);
            DiskBoundaries db = Mockito.mock(DiskBoundaries.class);
            when(db.getLocalRanges()).thenReturn(sortedRanges);
            when(db.getPositions()).thenReturn(diskBoundaries);

            var rs = Mockito.mock(AbstractReplicationStrategy.class);

            ShardManager shardManager = ShardManager.create(db, rs, false);
            for (int numShards = 1; numShards <= 3; ++numShards)
            {
                checkCoveredCount(shardManager, numDisks, numShards, 0.01, 0.99);
                checkCoveredCount(shardManager, numDisks, numShards, 0.01, 0.49);
                checkCoveredCount(shardManager, numDisks, numShards, 0.51, 0.99);
                checkCoveredCount(shardManager, numDisks, numShards, 0.26, 0.74);

                for (double l = 0; l <= 1; l += 0.05)
                    for (double r = l; r <= 1; r += 0.05)
                        checkCoveredCount(shardManager, numDisks, numShards, l, r);
            }
        }
    }

    private void checkCoveredCount(ShardManager shardManager, int numDisks, int numShards, double left, double right)
    {
        Token min = partitioner.getMinimumToken();
        int totalSplits = numDisks * numShards;
        int leftIdx = left == 0 ? 0 : (int) Math.ceil(left * totalSplits) - 1; // to reflect end-inclusiveness of ranges
        int rightIdx = right == 0 ? 0 : (int) Math.ceil(right * totalSplits) - 1;
        assertEquals(String.format("numDisks %d numShards %d left %f right %f", numDisks, numShards, left, right),
                     rightIdx - leftIdx + 1,
                     shardManager.coveredShardCount(partitioner.split(min, min, left).maxKeyBound(),
                                                    partitioner.split(min, min, right).maxKeyBound(),
                                                    numShards));
    }

    @Test
    public void testSplitSSTablesInRanges()
    {
        testSplitSSTablesInRanges(8, ints(1, 2, 4));
        testSplitSSTablesInRanges(4, ints(1, 2, 4));
        testSplitSSTablesInRanges(2, ints(1, 2, 4));
        testSplitSSTablesInRanges(5, ints(1, 2, 4));
        testSplitSSTablesInRanges(5, ints(2, 4, 8));
        testSplitSSTablesInRanges(3, ints(1, 3, 5));
        testSplitSSTablesInRanges(3, ints(3, 3, 3));

        testSplitSSTablesInRanges(1, ints(1, 2, 3));

        testSplitSSTablesInRanges(3, ints());
    }

    @Test
    public void testSplitSSTablesInRangesMissingParts()
    {
        // Drop some sstables without losing ranges
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1));

        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(0), ints(2, 7));

        testSplitSSTablesInRanges(5, ints(2, 4, 8),
                                ints(1), ints(0), ints(2, 7));
    }

    @Test
    public void testSplitSSTablesInRangesOneRange()
    {
        // Drop second half
        testSplitSSTablesInRanges(2, ints(2, 4, 8),
                                ints(1), ints(2, 3), ints(4, 5, 6, 7));
        // Drop all except center, within shard
        testSplitSSTablesInRanges(3, ints(5, 7, 9),
                                ints(0, 1, 3, 4), ints(0, 1, 2, 4, 5, 6), ints(0, 1, 2, 6, 7, 8));
    }

    @Test
    public void testSplitSSTablesInRangesSkippedRange()
    {
        // Drop all sstables containing the 4/8-5/8 range.
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(2), ints(4));
        // Drop all sstables containing the 4/8-6/8 range.
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(2), ints(4, 5));
        // Drop all sstables containing the 4/8-8/8 range.
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(2, 3), ints(4, 5, 6, 7));

        // Drop all sstables containing the 0/8-2/8 range.
        testSplitSSTablesInRanges(5, ints(2, 4, 8),
                                ints(0), ints(0), ints(0, 1));
        // Drop all sstables containing the 6/8-8/8 range.
        testSplitSSTablesInRanges(5, ints(2, 4, 8),
                                ints(1), ints(3), ints(6, 7));
        // Drop sstables on both ends.
        testSplitSSTablesInRanges(5, ints(3, 4, 8),
                                ints(0, 2), ints(0, 3), ints(0, 1, 6, 7));
    }

    public void testSplitSSTablesInRanges(int numShards, int[] perLevelCounts, int[]... dropsPerLevel)
    {
        weightedRanges.clear();
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(minimumToken, minimumToken)));
        ShardManager manager = new ShardManagerNoDisks(localRanges);

        Set<CompactionSSTable> allSSTables = new HashSet<>();
        int levelNum = 0;
        for (int perLevelCount : perLevelCounts)
        {
            List<CompactionSSTable> ssTables = mockNonOverlappingSSTables(perLevelCount);
            if (levelNum < dropsPerLevel.length)
            {
                for (int i = dropsPerLevel[levelNum].length - 1; i >= 0; i--)
                    ssTables.remove(dropsPerLevel[levelNum][i]);
            }
            allSSTables.addAll(ssTables);
            ++levelNum;
        }

        var results = new ArrayList<Pair<Range<Token>, Set<CompactionSSTable>>>();
        manager.splitSSTablesInShards(allSSTables, numShards, (sstables, range) -> results.add(Pair.create(range, Set.copyOf(sstables))));
        int i = 0;
        int[] expectedSSTablesInTasks = new int[results.size()];
        int[] collectedSSTablesPerTask = new int[results.size()];
        for (var t : results)
        {
            collectedSSTablesPerTask[i] = t.right().size();
            expectedSSTablesInTasks[i] = (int) allSSTables.stream().filter(x -> intersects(x, t.left())).count();
            ++i;
        }
        Assert.assertEquals(Arrays.toString(expectedSSTablesInTasks), Arrays.toString(collectedSSTablesPerTask));
        System.out.println(Arrays.toString(expectedSSTablesInTasks));
    }

    private boolean intersects(CompactionSSTable r, Range<Token> range)
    {
        if (range == null)
            return true;
        return range.intersects(range(r));
    }


    private Bounds<Token> range(CompactionSSTable x)
    {
        return new Bounds<>(x.getFirst().getToken(), x.getLast().getToken());
    }

    List<CompactionSSTable> mockNonOverlappingSSTables(int numSSTables)
    {
        if (!partitioner.splitter().isPresent())
            throw new IllegalStateException(String.format("Cannot split ranges with current partitioner %s", partitioner));

        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

        List<CompactionSSTable> sstables = new ArrayList<>(numSSTables);
        for (int i = 0; i < numSSTables; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(numSSTables, i).nextValidToken(), emptyBuffer);
            DecoratedKey last =  new BufferDecoratedKey(boundary(numSSTables, i+1), emptyBuffer);
            sstables.add(mockSSTable(first, last));
        }

        return sstables;
    }

    private Token boundary(int numSSTables, int i)
    {
        return partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), i * 1.0 / numSSTables);
    }

    private CompactionSSTable mockSSTable(DecoratedKey first, DecoratedKey last)
    {
        CompactionSSTable sstable = Mockito.mock(CompactionSSTable.class);
        when(sstable.getFirst()).thenReturn(first);
        when(sstable.getLast()).thenReturn(last);
        return sstable;
    }
}
