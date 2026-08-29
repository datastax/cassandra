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

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.LIVE;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.MARKER;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// [DeletionAwareTrie#mapValues] and [DeletionAwareTrie#mapValuesAndDeletions] had no tests at all. [MapValuesTest]
/// contains no occurrence of `DeletionAware`: it covers [Trie#mapValues] only, and a plain trie has no deletion
/// branch to forward.
///
/// The deletion-aware mapping cursors are where the gap bites. `ContentMappingCursor.DeletionAwareDataOnly` must
/// hand the source's deletion branch through untouched, and `ContentMappingCursor.DeletionAware` must wrap it in a
/// mapping range cursor that maps not only the boundaries a walk reports but also the `state` and `precedingState` a
/// merge or an intersection reads. Nothing checked either. `TrieBackedRow.transformAndFilter` and
/// `TrieBackedComplexColumn.purge` are the callers; a deletion branch dropped by a mapping is a tombstone that stops
/// being applied.
///
/// The projections are checked separately rather than only through the merged view, because a mapping that wrongly
/// also mapped deletions -- or wrongly did not -- is invisible in the merged list for a mapper that leaves the
/// ordering alone.
///
/// One restriction the tests establish and do not work around: the deletion mapper is applied independently to
/// `content`, `state` and `precedingState`, so it must be a function of the deletion state, not of the boundary it
/// appears in. A mapper that drops one side of a range while keeping its partner leaves a range that is opened and
/// never closed. Both production mappers are `t -> t.map(f)` over a deletion time and are consistent by
/// construction, so what is exercised here is that shape: a purge that removes a deletion time removes the whole
/// range it covers.
@RunWith(Parameterized.class)
public class DeletionAwareMapValuesTest extends DeletionAwareTestBase
{
    /// Added to every timestamp by the mapping under test. It is larger than every deletion time in the data set, so
    /// mapped live content survives exactly the deletions unmapped live content does -- which is what makes mapping
    /// commute with a merge.
    private static final int SHIFT = 1000;

    /// The deletion time the purge test removes.
    private static final int PURGED = 21;

    @Parameterized.Parameters(name = "bits per transition {0} deletions at root {1}")
    public static List<Object[]> mapData()
    {
        List<Object[]> list = new ArrayList<>();
        for (int bits : IntStream.rangeClosed(1, bitsNeeded).toArray())
            for (boolean deletionsAtRoot : new boolean[]{ false, true })
                list.add(new Object[]{ bits, deletionsAtRoot });
        return list;
    }

    @Parameterized.Parameter(1)
    public boolean deletionsAtRoot = false;

    @BeforeClass
    public static void setUpOnDisk()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    private final List<OnDiskDeletionAwareTrie<LivePoint, DeletionMarker>> opened = new ArrayList<>();

    @After
    public void closeOpened()
    {
        opened.forEach(OnDiskDeletionAwareTrie::close);
        opened.clear();
    }

    private static final Function<LivePoint, LivePoint> SHIFT_LIVE =
        live -> new LivePoint(live.position, live.timestamp + SHIFT);

    private static final Function<DeletionMarker, DeletionMarker> SHIFT_DELETION =
        marker -> new DeletionMarker(marker.position, shift(marker.leftSide), shift(marker.rightSide));

    /// A purge in the shape the production mappers take: a function of the deletion time, applied to whichever
    /// sides of a state carry one, dropping the state entirely when nothing is left of it.
    private static final Function<DeletionMarker, DeletionMarker> PURGE_DELETION =
        marker -> {
            int left = marker.leftSide == PURGED ? -1 : marker.leftSide;
            int right = marker.rightSide == PURGED ? -1 : marker.rightSide;
            return left < 0 && right < 0 ? null : new DeletionMarker(marker.position, left, right);
        };

    private static int shift(int deletionTime)
    {
        return deletionTime < 0 ? deletionTime : deletionTime + SHIFT;
    }

    /// Live points interleaved with deleted points at two different deletion times, so that a mapping of one time
    /// can be told apart from a mapping of the other. Live timestamps are all above both deletion times.
    private List<DataPoint> dataSet()
    {
        return flatten(asList(livePoint(19, 30),
                              deletedPoint(22, PURGED),
                              livePoint(23, 31),
                              deletedPoint(28, 23),
                              livePoint(29, 32),
                              livePoint(35, 33)));
    }

    /// A second data set with deletions of its own, used as the other operand of the merge that mapping must
    /// commute with.
    private List<DataPoint> otherDataSet()
    {
        return flatten(asList(livePoint(20, 34),
                              deletedPoint(25, 22),
                              livePoint(31, 35)));
    }

    /// `mapValues` maps live content and must leave deletions completely alone. Checked through the deletion-only
    /// projection as well as the merged one: with a mapper that only shifts timestamps, a bug that also mapped the
    /// deletions would not move a single entry in the merged view.
    @Test
    public void testMapValuesLeavesDeletionsAlone()
    {
        List<DataPoint> data = dataSet();
        List<DataPoint> expected = mapList(data, SHIFT_LIVE, Function.identity());

        assertDeletionAwareEqual(qualify("in memory"), expected, inMemory(data).mapValues(SHIFT_LIVE));
        assertDeletionAwareEqual(qualify("on disk"), expected, onDisk(data).mapValues(SHIFT_LIVE));

        // The deletion-only projection must be identical to the source's, entry for entry.
        assertEquals(qualify("deletions untouched"),
                     DataPoint.deletionOnlyList(inMemory(data)),
                     DataPoint.deletionOnlyList(inMemory(data).mapValues(SHIFT_LIVE)));
    }

    /// `mapValuesAndDeletions` maps both. The deletion mapper shifts the deletion time, which moves every marker in
    /// the deletion-only projection and nothing in the content-only one.
    @Test
    public void testMapValuesAndDeletions()
    {
        List<DataPoint> data = dataSet();
        List<DataPoint> expected = mapList(data, SHIFT_LIVE, SHIFT_DELETION);

        assertDeletionAwareEqual(qualify("in memory"), expected, inMemory(data).mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION));
        assertDeletionAwareEqual(qualify("on disk"), expected, onDisk(data).mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION));

        // Mapping deletions only must leave the content-only projection alone.
        assertEquals(qualify("content untouched"),
                     DataPoint.contentOnlyList(inMemory(data)),
                     DataPoint.contentOnlyList(inMemory(data).mapValuesAndDeletions(Function.identity(), SHIFT_DELETION)));
    }

    /// A content mapper that returns null must drop that content, the way [Trie#mapValues] does, and must not
    /// disturb the deletions -- including a deletion branch rooted at a position whose content was just dropped.
    @Test
    public void testNullContentMapperDropsContent()
    {
        List<DataPoint> data = dataSet();
        Function<LivePoint, LivePoint> dropOne = live -> live.timestamp == 31 ? null : live;
        List<DataPoint> expected = mapList(data, dropOne, Function.identity());

        assertDeletionAwareEqual(qualify("in memory"), expected, inMemory(data).mapValues(dropOne));
        assertDeletionAwareEqual(qualify("on disk"), expected, onDisk(data).mapValues(dropOne));
        assertEquals(qualify("deletions untouched"),
                     DataPoint.deletionOnlyList(inMemory(data)),
                     DataPoint.deletionOnlyList(inMemory(data).mapValues(dropOne)));
    }

    /// A deletion mapper that returns null must drop the range it covers, both of its boundaries and the covering
    /// state between them, leaving the other ranges and all live content in place. This is what
    /// `TrieBackedComplexColumn.purge` does when a tombstone is old enough to go.
    @Test
    public void testNullDeletionMapperDropsWholeRanges()
    {
        List<DataPoint> data = dataSet();
        List<DataPoint> expected = mapList(data, Function.identity(), PURGE_DELETION);

        assertDeletionAwareEqual(qualify("in memory"), expected, inMemory(data).mapValuesAndDeletions(Function.identity(), PURGE_DELETION));
        assertDeletionAwareEqual(qualify("on disk"), expected, onDisk(data).mapValuesAndDeletions(Function.identity(), PURGE_DELETION));
    }

    /// Mapping must commute with merging. The mapper shifts live timestamps by more than every deletion time in
    /// either operand, so it does not change which content the merge's deleter removes and the two orders must
    /// produce the same trie -- position for position, in both directions.
    @Test
    public void testMappingCommutesWithMerge()
    {
        List<DataPoint> left = dataSet();
        List<DataPoint> right = otherDataSet();

        TrieUtil.assertTriesEqual(merge(inMemory(left), inMemory(right)).mapValues(SHIFT_LIVE),
                                  merge(inMemory(left).mapValues(SHIFT_LIVE), inMemory(right).mapValues(SHIFT_LIVE)));
        TrieUtil.assertTriesEqual(merge(onDisk(left), inMemory(right)).mapValues(SHIFT_LIVE),
                                  merge(onDisk(left).mapValues(SHIFT_LIVE), inMemory(right).mapValues(SHIFT_LIVE)));

        // The same with deletions mapped as well: shifting both by the same amount leaves the deleter's verdict
        // unchanged, so the orders must still agree.
        TrieUtil.assertTriesEqual(merge(inMemory(left), inMemory(right)).mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION),
                                  merge(inMemory(left).mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION),
                                        inMemory(right).mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION)));
    }

    /// Mapping must commute with taking a tail, with and without the covering deletion. The covering-deletion
    /// branch of a tail is built from the source's deletion cursor, so this is the path on which a mapping cursor
    /// has to survive being asked for a preceding state rather than for content.
    @Test
    public void testMappingCommutesWithTailTrie()
    {
        List<DataPoint> data = dataSet();
        for (int value = 18; value <= 36; ++value)
            for (var prefix : asList(before(value), at(value), after(value)))
                for (boolean includeCoveringDeletions : new boolean[]{ true, false })
                {
                    assertTailsEqual(inMemory(data), prefix, includeCoveringDeletions);
                    assertTailsEqual(onDisk(data), prefix, includeCoveringDeletions);
                }
    }

    /// Two mappings applied one after the other must be the same as the single mapping that composes them, which is
    /// what [MapValuesTest#testChainedMapValues] checks for a plain trie. The chain also has to keep forwarding the
    /// deletion branch through two layers of mapping cursor.
    @Test
    public void testChainedMapValues()
    {
        List<DataPoint> data = dataSet();
        Function<LivePoint, LivePoint> twice = SHIFT_LIVE.andThen(SHIFT_LIVE);

        TrieUtil.assertTriesEqual(inMemory(data).mapValues(twice),
                                  inMemory(data).mapValues(SHIFT_LIVE).mapValues(SHIFT_LIVE));
        TrieUtil.assertTriesEqual(onDisk(data).mapValues(twice),
                                  onDisk(data).mapValues(SHIFT_LIVE).mapValues(SHIFT_LIVE));
        TrieUtil.assertTriesEqual(inMemory(data).mapValuesAndDeletions(twice, SHIFT_DELETION.andThen(SHIFT_DELETION)),
                                  inMemory(data).mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION)
                                                .mapValuesAndDeletions(SHIFT_LIVE, SHIFT_DELETION));
    }

    private void assertTailsEqual(DeletionAwareTrie<LivePoint, DeletionMarker> source,
                                  ByteComparable prefix,
                                  boolean includeCoveringDeletions)
    {
        DeletionAwareTrie<LivePoint, DeletionMarker> mappedTail =
            source.mapValues(SHIFT_LIVE).tailTrie(prefix, includeCoveringDeletions);
        DeletionAwareTrie<LivePoint, DeletionMarker> tailMapped =
            tailOrNull(source.tailTrie(prefix, includeCoveringDeletions));
        TrieUtil.assertTriesEqual(mappedTail, tailMapped);
    }

    private static DeletionAwareTrie<LivePoint, DeletionMarker> tailOrNull(DeletionAwareTrie<LivePoint, DeletionMarker> tail)
    {
        return tail == null ? null : tail.mapValues(SHIFT_LIVE);
    }

    private DeletionAwareTrie<LivePoint, DeletionMarker> merge(DeletionAwareTrie<LivePoint, DeletionMarker> left,
                                                               DeletionAwareTrie<LivePoint, DeletionMarker> right)
    {
        return left.mergeWith(right, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, deletionsAtRoot);
    }

    /// The list oracle: the same two mappings applied to the expected data points, dropping the points the mappers
    /// leave nothing of.
    private static List<DataPoint> mapList(List<DataPoint> points,
                                           Function<LivePoint, LivePoint> liveMapper,
                                           Function<DeletionMarker, DeletionMarker> deletionMapper)
    {
        return verify(points.stream()
                            .map(point -> DataPoint.resolve(point.live() != null ? liveMapper.apply(point.live()) : null,
                                                            point.marker() != null ? deletionMapper.apply(point.marker()) : null))
                            .filter(point -> point != null)
                            .collect(Collectors.toList()));
    }

    private String qualify(String message)
    {
        return message + " b" + bits + " deletionsAtRoot " + deletionsAtRoot;
    }

    private InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> inMemory(List<DataPoint> points)
    {
        return DataPoint.fromList(points, false, deletionsAtRoot);
    }

    private OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> onDisk(List<DataPoint> points)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DeletionAwareFileWriter.write(inMemory(points), LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            opened.add(read);
            return read;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
