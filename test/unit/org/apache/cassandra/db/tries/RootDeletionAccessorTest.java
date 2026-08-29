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
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.LIVE;
import static org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieTest.MARKER;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// The two root-deletion accessors, [DeletionAwareTrie#deletionBranchAtRoot] and [DeletionAwareTrie#deletionAtRoot],
/// have two implementations and nothing compared them until this class existed.
///
/// [InMemoryDeletionAwareTrie] overrides both with a structural shortcut that reads the root's alternate branch
/// directly and never positions a cursor. [OnDiskDeletionAwareTrie] does not override, so the reader -- and every
/// other view built by the algebra -- runs the cursor-based defaults in [DeletionAwareTrie]. The two answers are
/// consumed interchangeably: `TriePartitionUpdate` and `TrieBackedPartitionStage3` iterate `deletionBranchAtRoot()`
/// to emit partition deletions, and `TrieBackedRow` uses both to decide whether a row is empty. A disagreement is
/// therefore a silent wrong answer -- a partition deletion that vanishes on write, or a row wrongly reported empty --
/// rather than a failure, which is why the central assertion here is the differential between the override and the
/// default rather than a comparison with an expected list.
///
/// The second trap the accessors carry is the distinction from [DeletionAwareTrie#deletionOnlyTrie]:
/// `deletionBranchAtRoot` reports only the branch rooted at the root and must be empty when the deletions sit lower,
/// while `deletionOnlyTrie` reports all of them wherever they are. Reading one for the other is over- or
/// under-deletion at the partition level, so both directions of that distinction are asserted.
///
/// `bits` is parameterised because it selects the node type the on-disk reader has to decode; `deletionsAtRoot`
/// because it is what decides whether the deletion branch of the built trie sits at the root or below it.
@RunWith(Parameterized.class)
public class RootDeletionAccessorTest extends DeletionAwareTestBase
{
    /// A covering deletion at a timestamp all the live points in this suite survive, so that a whole-partition
    /// deletion branch can be built without emptying the data trie.
    private static final DeletionMarker PARTITION_DELETION = new DeletionMarker(ByteComparable.EMPTY, 5, 5);

    @Parameterized.Parameters(name = "bits per transition {0} deletions at root {1}")
    public static List<Object[]> accessorData()
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

    /// Live content only: the accessors must report the absence of a deletion branch, and must do it in the two
    /// different ways the callers depend on -- an empty trie for the branch (`TriePartitionUpdate` iterates the
    /// result without a null check) and a null for the deletion (`TrieBackedRow` tests it against null).
    private List<DataPoint> liveOnly()
    {
        return flatten(asList(livePoint(19, 30), livePoint(23, 31), livePoint(29, 32)));
    }

    /// Live content plus two deleted points. A deleted point opens and closes its range at the same value, so the
    /// two markers differ only in their terminator and always share a prefix; the branch it produces is therefore
    /// below the root for every `bits` when `deletionsAtRoot` is false, and hoisted to the root when it is true.
    private List<DataPoint> pointDeletions()
    {
        return flatten(asList(livePoint(19, 30),
                              deletedPoint(22, 21),
                              livePoint(23, 31),
                              deletedPoint(28, 23),
                              livePoint(29, 32)));
    }

    /// The empty trie -- the shape whose root has no children at all, where a cursor-based default has nothing to
    /// descend into and the structural override has no root node to read.
    @Test
    public void testEmptyTrie()
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = inMemory(List.of());
        assertAccessorsAgree("empty", trie);
        assertNoRootDeletion("empty", trie);
        assertNoRootDeletion("empty on disk", onDisk(trie));
    }

    /// No deletion branch anywhere. `deletionBranchAtRoot()` must be an empty trie rather than null, and
    /// `deletionAtRoot()` must be null; getting either the other way round is an immediate NPE in a caller.
    @Test
    public void testNoDeletionBranch()
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = inMemory(liveOnly());
        assertAccessorsAgree("live only", trie);
        assertNoRootDeletion("live only", trie);
        assertNoRootDeletion("live only on disk", onDisk(trie));
        // With nothing to report at the root, the deletion-only view must be empty too.
        assertTrue(markerEntries(trie.deletionOnlyTrie(), Direction.FORWARD).isEmpty());
    }

    /// The contract distinction between the two views, in both of its directions. With `deletionsAtRoot` the branch
    /// is at the root and the two views must report exactly the same entries; without it the branches sit below the
    /// root and `deletionBranchAtRoot()` must report nothing at all while `deletionOnlyTrie()` reports all four
    /// markers. Confusing the two is over-deletion (a lower range applied to the whole partition) or under-deletion
    /// (a partition deletion dropped).
    @Test
    public void testRootBranchVersusAllBranches()
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = inMemory(pointDeletions());
        assertAccessorsAgree("point deletions", trie);

        List<String> allBranches = markerEntries(trie.deletionOnlyTrie(), Direction.FORWARD);
        assertEquals(4, allBranches.size());

        if (deletionsAtRoot)
        {
            assertEquals("branch at root must report all deletions",
                         allBranches,
                         markerEntries(trie.deletionBranchAtRoot(), Direction.FORWARD));
            assertEquals("branch at root must report all deletions on disk",
                         allBranches,
                         markerEntries(onDisk(trie).deletionBranchAtRoot(), Direction.FORWARD));
        }
        else
        {
            assertTrue("no branch is rooted at the root",
                       markerEntries(trie.deletionBranchAtRoot(), Direction.FORWARD).isEmpty());
            assertTrue("no branch is rooted at the root on disk",
                       markerEntries(onDisk(trie).deletionBranchAtRoot(), Direction.FORWARD).isEmpty());
        }

        // Either way the branch root itself carries no marker, so there is no deletion applicable at the root.
        assertNull(trie.deletionAtRoot());
        assertNull(onDisk(trie).deletionAtRoot());
    }

    /// A whole-partition deletion: a branch at the root whose root node carries the opening marker. This is what
    /// separates `deletionAtRoot()` from "there is a branch" -- every other shape in this suite has a branch whose
    /// root is empty -- and it is the shape `TriePartitionUpdate` writes for `DELETE FROM t WHERE k = ?`.
    @Test
    public void testWholePartitionDeletion()
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = wholePartitionDeleted();
        assertAccessorsAgree("whole partition", trie);

        assertNotNull("a deletion covers the root", trie.deletionAtRoot());
        assertEquals(PARTITION_DELETION.rightSide, trie.deletionAtRoot().rightSide);
        assertEquals("a branch rooted at the root reports every marker it holds",
                     markerEntries(trie.deletionOnlyTrie(), Direction.FORWARD),
                     markerEntries(trie.deletionBranchAtRoot(), Direction.FORWARD));

        OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read = onDisk(trie);
        assertEquals("read back", trie.deletionAtRoot(), read.deletionAtRoot());
        assertEquals("read back",
                     markerEntries(trie.deletionBranchAtRoot(), Direction.FORWARD),
                     markerEntries(read.deletionBranchAtRoot(), Direction.FORWARD));
        assertEquals("read back reversed",
                     markerEntries(trie.deletionBranchAtRoot(), Direction.REVERSE),
                     markerEntries(read.deletionBranchAtRoot(), Direction.REVERSE));
    }

    /// The differential itself, over every shape: the structural override that [InMemoryDeletionAwareTrie] provides
    /// must agree with the cursor-based default of the interface, which is what the on-disk reader and every
    /// transformed view actually run. The default is forced by widening the trie to the interface through a method
    /// reference, which hides the concrete type and therefore the overrides.
    @Test
    public void testOverrideAgreesWithCursorDefault()
    {
        assertAccessorsAgree("empty", inMemory(List.of()));
        assertAccessorsAgree("live only", inMemory(liveOnly()));
        assertAccessorsAgree("point deletions", inMemory(pointDeletions()));
        assertAccessorsAgree("whole partition", wholePartitionDeleted());
    }

    /// `deletionAtRoot()` is documented as an efficient alternative of `applicableDeletion(EMPTY)`. The two take
    /// completely different routes -- one reads the branch root's content, the other descends a cursor along an
    /// empty key and reports the state it lands on -- so the identity is worth asserting on the reader as well as
    /// in memory.
    @Test
    public void testDeletionAtRootMatchesApplicableDeletion()
    {
        for (InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie
             : asList(inMemory(List.of()), inMemory(liveOnly()), inMemory(pointDeletions()), wholePartitionDeleted()))
        {
            assertEquals("override", trie.deletionAtRoot(), trie.applicableDeletion(ByteComparable.EMPTY));
            DeletionAwareTrie<LivePoint, DeletionMarker> viaDefault = trie::makeCursor;
            assertEquals("default", viaDefault.deletionAtRoot(), viaDefault.applicableDeletion(ByteComparable.EMPTY));
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read = onDisk(trie);
            assertEquals("on disk", read.deletionAtRoot(), read.applicableDeletion(ByteComparable.EMPTY));
            assertEquals("on disk against in memory", trie.deletionAtRoot(), read.deletionAtRoot());
        }
    }

    /// Documents a divergence between the two implementations that the shape tests above cannot see because they
    /// build the trie before they read it.
    ///
    /// Trie views are documented as live -- a write to the source is reflected in the view -- and the cursor-based
    /// default is, because it opens its cursor when it is walked. The [InMemoryDeletionAwareTrie] override is not:
    /// it reads the root's alternate branch when `deletionBranchAtRoot()` is called and closes over the result, so a
    /// view taken before a root deletion is written keeps reporting no deletion afterwards. This test asserts the
    /// behaviour as it stands today, not as it should be; every current caller reads the view immediately, so the
    /// difference is unobservable in production, but it is a real disagreement between an override and the default
    /// it stands in for.
    @Test
    public void testRootBranchViewIsCapturedEagerlyByTheOverride()
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = trie.mutator(DataPoint::combineLive,
                                   DataPoint::combineDeletion,
                                   DataPoint::deleteLive,
                                   DataPoint::deleteLive,
                                   true,
                                   v -> false);
        DeletionAwareTrie<LivePoint, DeletionMarker> viaDefault = trie::makeCursor;
        RangeTrie<DeletionMarker> fromOverride = trie.deletionBranchAtRoot();
        RangeTrie<DeletionMarker> fromDefault = viaDefault.deletionBranchAtRoot();
        try
        {
            mutator.apply(DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, null, true, null, true, VERSION, PARTITION_DELETION));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        List<String> written = markerEntries(trie.deletionBranchAtRoot(), Direction.FORWARD);
        assertEquals("a view taken after the write reports it", 2, written.size());
        assertEquals("the default is live", written, markerEntries(fromDefault, Direction.FORWARD));
        assertTrue("current behaviour: the override captured the absent branch when it was called",
                   markerEntries(fromOverride, Direction.FORWARD).isEmpty());
    }

    /// Asserts the two implementations of both accessors agree, in both directions and down to the range states a
    /// merge or an intersection would read off the returned cursor, not just the markers a walk reports.
    private void assertAccessorsAgree(String message, InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        String qualified = message + " b" + bits + " deletionsAtRoot " + deletionsAtRoot;
        DeletionAwareTrie<LivePoint, DeletionMarker> viaDefault = trie::makeCursor;

        RangeTrie<DeletionMarker> fromOverride = trie.deletionBranchAtRoot();
        RangeTrie<DeletionMarker> fromDefault = viaDefault.deletionBranchAtRoot();
        assertNotNull(qualified + ": the override must return an empty trie, not null", fromOverride);
        assertNotNull(qualified + ": the default must return an empty trie, not null", fromDefault);
        TrieUtil.assertTriesEqual(fromOverride, fromDefault);
        assertRangeStatesEqual(qualified, fromOverride, fromDefault);

        assertEquals(qualified + ": deletionAtRoot", trie.deletionAtRoot(), viaDefault.deletionAtRoot());
    }

    /// Asserts that neither accessor reports a deletion, in the two different ways their callers check.
    private void assertNoRootDeletion(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        String qualified = message + " b" + bits + " deletionsAtRoot " + deletionsAtRoot;
        RangeTrie<DeletionMarker> branch = trie.deletionBranchAtRoot();
        assertNotNull(qualified + ": must be an empty trie, not null", branch);
        for (Direction direction : Direction.values())
            assertTrue(qualified + ' ' + direction, markerEntries(branch, direction).isEmpty());
        assertNull(qualified, trie.deletionAtRoot());
    }

    /// Walks two range tries in lockstep and compares the `state` and `precedingState` they report at every
    /// position. A content walk only sees the boundaries; a merge or an intersection reads the covering state at
    /// positions it skips to, which is where two implementations of the same trie can silently differ.
    private static void assertRangeStatesEqual(String message, RangeTrie<DeletionMarker> expected, RangeTrie<DeletionMarker> actual)
    {
        for (Direction direction : Direction.values())
        {
            RangeCursor<DeletionMarker> expectedCursor = expected.cursor(direction);
            RangeCursor<DeletionMarker> actualCursor = actual.cursor(direction);
            long expectedPos = expectedCursor.encodedPosition();
            long actualPos = actualCursor.encodedPosition();
            while (true)
            {
                String at = message + ' ' + direction + " at " + Cursor.toString(expectedPos);
                assertEquals(at + " position", 0, Cursor.compare(expectedPos, actualPos));
                assertEquals(at + " state", expectedCursor.state(), actualCursor.state());
                assertEquals(at + " preceding state", expectedCursor.precedingState(), actualCursor.precedingState());
                if (Cursor.isExhausted(expectedPos))
                    break;
                assertEquals(at + " content", expectedCursor.content(), actualCursor.content());
                expectedPos = expectedCursor.advance();
                actualPos = actualCursor.advance();
            }
        }
    }

    /// The markers of a range trie as `key -> marker` strings. [DeletionMarker#equals] ignores the position it
    /// carries, so comparing marker lists alone would not notice a branch reported at the wrong key -- which is
    /// exactly the failure mode when a root branch and a lower one are confused.
    private static List<String> markerEntries(RangeTrie<DeletionMarker> trie, Direction direction)
    {
        return Streams.stream(trie.entryIterator(direction))
                      .map(entry -> entry.getValue().remap(entry.getKey()).toString())
                      .collect(Collectors.toList());
    }

    /// A trie whose entire content is covered by one deletion rooted at the trie root, built the way a partition
    /// deletion is: an unbounded deleted range applied at the empty prefix.
    private InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> wholePartitionDeleted()
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = trie.mutator(DataPoint::combineLive,
                                   DataPoint::combineDeletion,
                                   DataPoint::deleteLive,
                                   DataPoint::deleteLive,
                                   true,
                                   v -> false);
        try
        {
            for (DataPoint point : liveOnly())
                mutator.apply(DeletionAwareTrie.singleton(point.position(), VERSION, point.live()));
            mutator.apply(DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, null, true, null, true, VERSION, PARTITION_DELETION));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
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
}
