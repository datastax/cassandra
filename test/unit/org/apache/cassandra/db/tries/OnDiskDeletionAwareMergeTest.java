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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DeletionAwareMergeTest.mergeLists;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.LIVE;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.MARKER;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

/// Merges with a deletion-aware trie read back off disk as an operand.
///
/// This is the gap the on-disk suites leave: they write a trie, read it back, walk it and compare it with the
/// in-memory one it came from, and stop there. A walk asks a cursor for its content and its children. A merge asks it
/// where it is once it has ended, and asks its deletion branch for the range active at a position -- [MergeCursor],
/// [CollectionMergeCursor], [RangeIntersectionCursor] and [DeletionAwareMergeSource] all call `precedingState` on
/// their sources. Both of the reader defects that reached review, and the one this class was written alongside, sit
/// in that second set of calls, and all three produce a wrong answer rather than a failure: rows that are in neither
/// operand, or a tombstone applied to a branch it does not cover.
///
/// The sides are parameterised because they are not symmetric in the cursor code -- a merge advances whichever source
/// is behind, so which one is read back off disk decides which paths are taken -- and `bits` because it is what
/// selects the node type the reader has to decode, which is how the bitmap and dense readers came to ship untested.
@RunWith(Parameterized.class)
public class OnDiskDeletionAwareMergeTest extends DeletionAwareTestBase
{
    @Parameterized.Parameters(name = "bits per transition {0} deletions at root {1}")
    public static List<Object[]> mergeData()
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

    /// Live points interleaved with ranges, ranges that switch deletion time inside a branch, and points that carry
    /// live content and a deletion at the same key. The last is the shape that fills both the descent and the ascent
    /// content slot of one node, which is the only place the on-disk payload order can be observed.
    private List<DataPoint> dataSet()
    {
        return flatten(asList(deletedPoint(17, 20),
                              livePoint(19, 30),
                              from(21, 10), deletedPointInside(22, 21, 10), livePoint(23, 31), to(24, 10),
                              from(26, 11), livePoint(27, 32), change(28, 11, 12).withPoint(22), livePoint(29, 33), to(30, 12),
                              livePoint(32, 34), from(33, 13).withPoint(23), to(34, 13),
                              from(36, 14), to(38, 14).withPoint(24), livePoint(39, 35)));
    }

    /// A set of ranges that overlaps the data set on both sides of several of its boundaries, so the merge has to
    /// resolve boundaries that fall inside a range of the other source as well as at its edges.
    private List<DataPoint> overlappingRanges()
    {
        return DataPoint.verify(new ArrayList<>(asList(new DeletionMarker(before(14), -1, 40),
                                                       new DeletionMarker(before(22), 40, -1),
                                                       new DeletionMarker(before(28), -1, 40),
                                                       new DeletionMarker(before(37), 40, -1))));
    }

    /// A source with no deletions at all: its deletion branch is absent everywhere, which is the operand shape a
    /// merge takes the shortest path through and therefore the one least likely to be covered elsewhere.
    private List<DataPoint> liveOnly()
    {
        return flatten(asList(livePoint(18, 50), livePoint(25, 51), livePoint(35, 52)));
    }

    /// Both directions of `a.mergeWith(b)` with one side read back off disk, and with both sides read back.
    @Test
    public void testMergeWithAReadBackOperand()
    {
        for (List<DataPoint> right : asList(overlappingRanges(), liveOnly(), List.<DataPoint>of()))
        {
            List<DataPoint> left = dataSet();
            List<DataPoint> expected = mergeLists(left, right);

            assertMergeMatches("on-disk left", expected, onDisk(left), inMemory(right));
            assertMergeMatches("on-disk right", expected, inMemory(left), onDisk(right));
            assertMergeMatches("on-disk both", expected, onDisk(left), onDisk(right));
            // The oracle is order-independent, so the same expectation covers the merge taken the other way round.
            assertMergeMatches("on-disk left reversed sides", expected, onDisk(right), inMemory(left));
            assertMergeMatches("on-disk right reversed sides", expected, inMemory(right), onDisk(left));
        }
    }

    /// [DeletionAwareTrie#merge] over a collection, with the on-disk operand in each position in turn. The collection
    /// merge keeps its sources in a heap ordered by position, so which slot the read-back trie occupies decides when
    /// its position is read and which of its cursor's paths the merge takes.
    @Test
    public void testCollectionMergeWithAReadBackOperand()
    {
        List<DataPoint> left = dataSet();
        List<DataPoint> middle = overlappingRanges();
        List<DataPoint> right = liveOnly();
        List<DataPoint> expected = mergeLists(mergeLists(left, middle), right);

        for (int onDiskIndex = 0; onDiskIndex < 3; ++onDiskIndex)
        {
            List<List<DataPoint>> sources = asList(left, middle, right);
            List<DeletionAwareTrie<LivePoint, DeletionMarker>> tries = new ArrayList<>();
            for (int i = 0; i < sources.size(); ++i)
                tries.add(i == onDiskIndex ? onDisk(sources.get(i)) : inMemory(sources.get(i)));

            assertResultMatches("collection merge with source " + onDiskIndex + " on disk",
                                expected,
                                DeletionAwareTrie.merge(tries,
                                                        LivePoint::combineCollection,
                                                        DeletionMarker::combineCollection,
                                                        DeletionMarker::applyTo,
                                                        deletionsAtRoot),
                                DeletionAwareTrie.merge(sources.stream()
                                                               .map(this::inMemory)
                                                               .collect(Collectors.toList()),
                                                        LivePoint::combineCollection,
                                                        DeletionMarker::combineCollection,
                                                        DeletionMarker::applyTo,
                                                        deletionsAtRoot));
        }
    }

    /// Merging a read-back trie with itself. Two cursors are live over the same file at once, each holding on to the
    /// bytes it was given while the other reads elsewhere, and every position of one is compared with the same
    /// position of the other -- so any state the reader keeps per file rather than per cursor shows up as a
    /// disagreement between two views that must be identical.
    @Test
    public void testMergeOfAReadBackTrieWithItself()
    {
        List<DataPoint> data = dataSet();
        OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read = onDisk(data);
        assertMergeMatches("self merge", mergeLists(data, data), read, read);
    }

    /// A merge is often taken apart again by the caller: the memtable merges sources and then walks tails of the
    /// result per partition. A tail of a merge whose operand is read back off disk is rooted at a position the
    /// on-disk cursor has to reproduce from the merge's request, not from a descent of its own.
    @Test
    public void testTailsOfAMergeWithAReadBackOperand()
    {
        List<DataPoint> left = dataSet();
        List<DataPoint> right = overlappingRanges();
        DeletionAwareTrie<LivePoint, DeletionMarker> expected = merge(inMemory(left), inMemory(right));
        DeletionAwareTrie<LivePoint, DeletionMarker> actual = merge(onDisk(left), inMemory(right));

        for (int value = 14; value <= 40; ++value)
            for (var prefix : asList(before(value), at(value), after(value)))
                for (boolean includeCoveringDeletions : new boolean[]{ true, false })
                    TrieUtil.assertTriesEqual(expected.tailTrie(prefix, includeCoveringDeletions),
                                              actual.tailTrie(prefix, includeCoveringDeletions));
    }

    private DeletionAwareTrie<LivePoint, DeletionMarker> merge(DeletionAwareTrie<LivePoint, DeletionMarker> left,
                                                               DeletionAwareTrie<LivePoint, DeletionMarker> right)
    {
        return left.mergeWith(right, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, deletionsAtRoot);
    }

    private void assertMergeMatches(String message,
                                    List<DataPoint> expectedList,
                                    DeletionAwareTrie<LivePoint, DeletionMarker> left,
                                    DeletionAwareTrie<LivePoint, DeletionMarker> right)
    {
        assertResultMatches(message, expectedList, merge(left, right), null);
    }

    /// Checks the merge against the list the oracle produces -- which also covers the content-only and deletion-only
    /// projections -- and, where one is given, against the same merge taken over in-memory operands only. The latter
    /// is the part that walks both directions and descends into every deletion branch.
    private void assertResultMatches(String message,
                                     List<DataPoint> expectedList,
                                     DeletionAwareTrie<LivePoint, DeletionMarker> actual,
                                     DeletionAwareTrie<LivePoint, DeletionMarker> expectedTrie)
    {
        String qualified = message + " b" + bits + " deletionsAtRoot " + deletionsAtRoot;
        assertDeletionAwareEqual(qualified, expectedList, actual);
        if (expectedTrie != null)
            TrieUtil.assertTriesEqual(expectedTrie, actual);
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
