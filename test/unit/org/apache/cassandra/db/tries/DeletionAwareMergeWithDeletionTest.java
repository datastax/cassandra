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

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
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
import static org.apache.cassandra.db.tries.DeletionAwareMergeTest.mergeLists;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.LIVE;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.MARKER;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// [DeletionAwareTrie#mergeWithDeletion] merges a bare [RangeTrie] into a deletion-aware trie, hoisting the deletion
/// branch to the root if it is not there already. Nothing tested it, on either side.
///
/// It is how `TrieBackedRow` applies a row deletion, and the shape it produces -- a deletion branch rooted at the
/// root, with the trie's own deletions pulled up into it -- is the one that hid several of the reader defects this
/// branch has already fixed. It also carries a `TODO: Optimize/simplify`, so pinning what it does today is most of
/// the value of the class.
///
/// The oracle is [DeletionAwareMergeTest#mergeLists] at the [DataPoint] list level, which is independent of the trie
/// code. Deliberately *not* `mergeWith(deletionBranch(EMPTY, ...))`: that is the implementation, so it would assert
/// nothing.
///
/// `deletionsAtRoot` drives both the shape of the trie being merged into and the flag the method is given, because
/// the flag is a promise that no source has a deletion branch above or below another's. With it, the trie already
/// has a branch at the root and the two must merge rather than replace one another; without it, the trie's branches
/// sit below the root and hoisting has to pull them up past the level they were introduced at -- the case most
/// likely to be wrong.
@RunWith(Parameterized.class)
public class DeletionAwareMergeWithDeletionTest extends DeletionAwareTestBase
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

    /// Live content only: the trie has no deletion branch anywhere, so the merged deletion has nowhere to go but a
    /// branch created at the root. This is the shape the hoisting exists for.
    private List<DataPoint> liveOnly()
    {
        return flatten(asList(livePoint(19, 30), livePoint(23, 31), livePoint(29, 32), livePoint(35, 33)));
    }

    /// Live content plus deletions of its own. A deleted point opens and closes at the same value, so its two
    /// markers always share a prefix and the branch lands below the root whenever `deletionsAtRoot` is false.
    private List<DataPoint> withOwnDeletions()
    {
        return flatten(asList(livePoint(19, 30),
                              deletedPoint(22, 21),
                              livePoint(23, 31),
                              deletedPoint(28, 23),
                              livePoint(29, 32),
                              livePoint(35, 33)));
    }

    /// A deletion that does not reach any of the trie's own deletions, so the result is the two sets of ranges side
    /// by side in one hoisted branch.
    @Test
    public void testDisjointDeletion()
    {
        assertMergeWithDeletionMatches("disjoint", liveOnly(), 15, 18, 9);
        assertMergeWithDeletionMatches("disjoint", withOwnDeletions(), 15, 18, 9);
    }

    /// A deletion that overlaps the trie's own ranges on both sides of their boundaries, which is what forces the
    /// merge to resolve boundaries falling inside a range of the other source rather than only at its edges.
    @Test
    public void testOverlappingDeletion()
    {
        assertMergeWithDeletionMatches("overlapping", liveOnly(), 18, 25, 9);
        assertMergeWithDeletionMatches("overlapping", withOwnDeletions(), 18, 25, 9);
        assertMergeWithDeletionMatches("overlapping later", withOwnDeletions(), 22, 30, 9);
        // A deletion time above the trie's own, so the incoming range wins where they overlap.
        assertMergeWithDeletionMatches("overlapping stronger", withOwnDeletions(), 18, 25, 25);
    }

    /// A deletion covering every key in the trie -- the row-deletion shape `TrieBackedRow` applies. The live points
    /// carry timestamps above the deletion time so they survive it and the result still has content to compare.
    @Test
    public void testCoveringDeletion()
    {
        assertMergeWithDeletionMatches("covering", liveOnly(), 10, 45, 9);
        assertMergeWithDeletionMatches("covering", withOwnDeletions(), 10, 45, 9);
    }

    /// Two disjoint incoming ranges, so the hoisted branch has to interleave with the trie's own deletions rather
    /// than sit before or after them.
    @Test
    public void testTwoIncomingRanges()
    {
        RangeTrie<DeletionMarker> deletion = deletionRange(15, 20, 9).mergeWith(deletionRange(27, 31, 9),
                                                                               DeletionMarker::combine);
        List<DataPoint> deletionList = verify(new ArrayList<>(asList(from(15, 9), to(20, 9), from(27, 9), to(31, 9))));
        assertMergeWithDeletionMatches("two ranges", liveOnly(), deletion, deletionList);
        assertMergeWithDeletionMatches("two ranges", withOwnDeletions(), deletion, deletionList);
    }

    /// An empty deletion. The merge must leave the trie exactly as it was, including leaving a branch that sits
    /// below the root where it was -- hoisting an empty branch to the root would move deletions to a level the
    /// caller did not ask for.
    @Test
    public void testEmptyDeletion()
    {
        RangeTrie<DeletionMarker> empty = dir -> RangeCursor.empty(dir, VERSION);
        assertMergeWithDeletionMatches("empty deletion", liveOnly(), empty, List.of());
        assertMergeWithDeletionMatches("empty deletion", withOwnDeletions(), empty, List.of());
    }

    /// The hoisting itself: whatever level the trie's own deletion branches were introduced at, after a merge with
    /// a deletion rooted at the root every deletion in the result must be reachable from the root branch. A caller
    /// that then reads `deletionBranchAtRoot()` -- which is what the partition writers do -- must see all of them,
    /// not only the incoming one.
    @Test
    public void testDeletionsAreHoistedToTheRoot()
    {
        for (List<DataPoint> base : asList(liveOnly(), withOwnDeletions()))
        {
            DeletionAwareTrie<LivePoint, DeletionMarker> merged = mergeWithDeletion(inMemory(base), deletionRange(18, 25, 9));
            assertEquals("every deletion must be reachable from the root branch",
                         markerEntries(merged.deletionOnlyTrie()),
                         markerEntries(merged.deletionBranchAtRoot()));
            assertTrue("the hoisted branch is not empty", !markerEntries(merged.deletionBranchAtRoot()).isEmpty());
        }
    }

    private void assertMergeWithDeletionMatches(String message, List<DataPoint> base, int from, int to, int time)
    {
        assertMergeWithDeletionMatches(message,
                                       base,
                                       deletionRange(from, to, time),
                                       verify(new ArrayList<>(asList(from(from, time), to(to, time)))));
    }

    /// Checks the merge against the list oracle -- which also covers the content-only and deletion-only projections
    /// -- with the deletion-aware operand in memory and read back off disk, with the deletion operand read back off
    /// disk, and in both walk directions.
    private void assertMergeWithDeletionMatches(String message,
                                                List<DataPoint> base,
                                                RangeTrie<DeletionMarker> deletion,
                                                List<DataPoint> deletionList)
    {
        String qualified = message + " b" + bits + " deletionsAtRoot " + deletionsAtRoot;
        List<DataPoint> expected = verify(mergeLists(base, deletionList));
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = inMemory(base);

        DeletionAwareTrie<LivePoint, DeletionMarker> merged = mergeWithDeletion(source, deletion);
        assertDeletionAwareEqual(qualified, expected, merged);
        assertEquals(qualified + " reversed", Lists.reverse(expected), toList(merged, Direction.REVERSE));

        assertDeletionAwareEqual(qualified + " source on disk", expected, mergeWithDeletion(onDisk(source), deletion));
        assertDeletionAwareEqual(qualified + " deletion on disk", expected, mergeWithDeletion(source, onDiskRange(deletion)));
    }

    private DeletionAwareTrie<LivePoint, DeletionMarker> mergeWithDeletion(DeletionAwareTrie<LivePoint, DeletionMarker> source,
                                                                          RangeTrie<DeletionMarker> deletion)
    {
        return source.mergeWithDeletion(deletion, DeletionMarker::applyTo, DeletionMarker::combine, deletionsAtRoot);
    }

    /// A deletion of everything between `before(from)` (inclusive) and `before(to)` (exclusive), the range the pair
    /// `from(from, time)`, `to(to, time)` describes at the list level.
    private RangeTrie<DeletionMarker> deletionRange(int from, int to, int time)
    {
        return RangeTrie.range(before(from), true, before(to), false, VERSION, new DeletionMarker(before(from), time, time));
    }

    private static List<DataPoint> toList(DeletionAwareTrie<LivePoint, DeletionMarker> trie, Direction direction)
    {
        return Streams.stream(trie.mergedTrie(DataPoint::resolve).entryIterator(direction))
                      .map(entry -> entry.getValue().remap(entry.getKey()))
                      .collect(Collectors.toList());
    }

    /// The markers of a range trie as `key -> marker` strings. [DeletionMarker#equals] ignores the position it
    /// carries, so comparing marker lists alone would not notice a deletion hoisted to the wrong key.
    private static List<String> markerEntries(RangeTrie<DeletionMarker> trie)
    {
        return Streams.stream(trie.entryIterator())
                      .map(entry -> entry.getValue().remap(entry.getKey()).toString())
                      .collect(Collectors.toList());
    }

    private InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> inMemory(List<DataPoint> points)
    {
        return DataPoint.fromList(points, false, deletionsAtRoot);
    }

    private OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> onDisk(DeletionAwareTrie<LivePoint, DeletionMarker> source)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DeletionAwareFileWriter.write(source, LIVE, MARKER, out);
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

    /// A range trie backed by the on-disk reader, obtained by writing it as the root deletion branch of a
    /// deletion-aware trie and reading that branch back. There is no buffer-backed writer for a plain range trie,
    /// and this keeps the operand on the same fast path the rest of the suite uses.
    private RangeTrie<DeletionMarker> onDiskRange(RangeTrie<DeletionMarker> deletion)
    {
        return onDisk(DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, deletion)).deletionBranchAtRoot();
    }
}
