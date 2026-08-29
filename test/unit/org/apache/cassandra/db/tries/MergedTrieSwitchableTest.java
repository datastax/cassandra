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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Streams;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import static org.junit.Assert.assertTrue;

/// [DeletionAwareTrie#mergedTrieSwitchable] and the [DeletionAwareTrie.DeletionsStopControl] it exposes had no
/// trie-level test; they were only reachable through partition-level ones.
///
/// The cursor behind them, `DeletionAwareCursor.SwitchableLiveAndDeletionsMergeCursor`, is a merge cursor whose
/// `tailCursor` is a four-branch state machine keyed on whether deletions have been switched off and on which of
/// its two sources is at the current position. `TrieBackedPartitionStage3` reaches the switch through a cast on
/// every read that hits a tombstone limit, and `TrieBackedPartition` through the tails iterator. Getting a branch of
/// that state machine wrong is a row that keeps or loses a tombstone it should not, in the middle of a page.
///
/// Three things are checked: that the switchable view is indistinguishable from [DeletionAwareTrie#mergedTrie]
/// before anything is switched; that after the switch the rest of the walk carries only live content while
/// everything reported before it is untouched, for a switch taken at every position of the walk including inside a
/// deletion branch; and that the tails taken on either side of the switch match the merged and the content-only
/// tails they stand for.
@RunWith(Parameterized.class)
public class MergedTrieSwitchableTest extends DeletionAwareTestBase
{
    @Parameterized.Parameters(name = "bits per transition {0} deletions at root {1}")
    public static List<Object[]> switchData()
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

    /// Live points interleaved with deleted points, so that the walk alternates between the live trie and a
    /// deletion branch and a switch can be taken on either side of every boundary. Live and marker keys are
    /// distinct -- markers sit at `before`/`after` positions, content at `at` positions -- which is what lets the
    /// expectation below be stated as "the live entries past the last one reported".
    private List<DataPoint> dataSet()
    {
        return flatten(asList(livePoint(19, 30),
                              deletedPoint(22, 21),
                              livePoint(23, 31),
                              deletedPoint(28, 23),
                              livePoint(29, 32),
                              deletedPoint(33, 24),
                              livePoint(35, 33)));
    }

    /// A data set whose live content sits inside a deleted range, so that a tail taken at a live position carries a
    /// covering deletion. Without one, every tail the tails iterator hands out is deletion-free before the switch as
    /// well as after it, and the test below would assert nothing.
    private List<DataPoint> coveredDataSet()
    {
        return flatten(asList(livePoint(19, 30),
                              from(21, 10), livePoint(23, 31), to(24, 10),
                              livePoint(29, 32)));
    }

    /// Before anything is switched off, the switchable view must be the merged view -- same positions, same
    /// content, both directions. Cheap, and it is what makes the switch tests below a statement about the switch
    /// rather than about the merge.
    @Test
    public void testMatchesMergedTrieBeforeAnySwitch()
    {
        for (DeletionAwareTrie<LivePoint, DeletionMarker> source : sources())
            TrieUtil.assertTriesEqual(source.mergedTrie(DataPoint::resolve),
                                      source.mergedTrieSwitchable(DataPoint::resolve));
    }

    /// The switch, taken at every position of the walk in turn: before the first entry, at each live entry, at each
    /// deletion boundary, and after the last of them. Everything reported before the switch must be unaffected, and
    /// everything after it must be live content only.
    @Test
    public void testSwitchAtEveryPosition()
    {
        for (DeletionAwareTrie<LivePoint, DeletionMarker> source : sources())
            for (Direction direction : Direction.values())
            {
                List<Map.Entry<ByteComparable.Preencoded, DataPoint>> merged =
                    entries(source.mergedTrie(DataPoint::resolve), direction);
                List<Map.Entry<ByteComparable.Preencoded, LivePoint>> live =
                    entries(source.contentOnlyTrie(), direction);

                for (int switchAfter = 0; switchAfter <= merged.size(); ++switchAfter)
                    assertEquals(qualify("switch after " + switchAfter + ' ' + direction),
                                 expectedAfterSwitch(merged, live, direction, switchAfter),
                                 walkWithSwitch(source, direction, switchAfter));
            }
    }

    /// The tails the state machine builds, on both sides of the switch.
    ///
    /// Before it, the switchable cursor must be indistinguishable from the plain merge cursor: the two are walked in
    /// lockstep and their tails compared at every position, in the walk direction and in the opposite one, which
    /// covers the branches for a live position, a position inside a deletion branch, and a position where both
    /// sources are present. The comparison is against the plain cursor at the same point of the same walk rather
    /// than against `tailTrie(key)`, because the covering state a range cursor carries into a tail is by definition
    /// a function of the direction it was reached from, so a tail from a forward descent is not the same object as a
    /// tail taken during a reverse walk.
    ///
    /// After it, a tail must carry live content only. That includes the assumption the code records at the `AT_C2`
    /// branch, that a switch taken inside a deletion branch leaves an empty cursor behind: a deletion boundary key
    /// has no live descendants, so the content-only tail at that key is empty too.
    @Test
    public void testTailsAcrossTheSwitch()
    {
        for (DeletionAwareTrie<LivePoint, DeletionMarker> source : sources())
            for (Direction direction : Direction.values())
            {
                int positions = entries(source.mergedTrie(DataPoint::resolve), direction).size();

                assertTailsMatchMergedTails(source, direction);
                for (int switchAfter = 0; switchAfter <= positions; ++switchAfter)
                    assertTailsAreLiveOnlyAfterSwitch(source, direction, switchAfter);
            }
    }

    /// [TrieTailsIterator.DeletionAware#stopIssuingDeletions] is the other way into the same control, and the one
    /// `TrieBackedPartition` uses. Once it has been called, no tail the iterator hands out may carry a deletion
    /// branch, while the keys it selects -- which come from live content only -- must not change.
    ///
    /// The data set has live content inside a deleted range, so that the tails an unswitched run produces do carry
    /// covering deletions; without that the assertion below would hold for a run in which the switch did nothing.
    @Test
    public void testTailsIteratorStopsIssuingDeletions()
    {
        List<DataPoint> data = coveredDataSet();
        for (DeletionAwareTrie<LivePoint, DeletionMarker> source : sources(data))
            for (Direction direction : Direction.values())
            {
                List<String> unswitchedKeys = new ArrayList<>();
                List<Boolean> unswitched = tailDeletions(source, direction, -1, unswitchedKeys);
                List<String> switchedKeys = new ArrayList<>();
                List<Boolean> switched = tailDeletions(source, direction, 0, switchedKeys);

                assertEquals(qualify("the selected keys must not change " + direction), unswitchedKeys, switchedKeys);
                assertTrue(qualify("no tail after the switch may carry deletions " + direction),
                           switched.subList(1, switched.size()).stream().noneMatch(x -> x));
                assertTrue(qualify("without the switch some of them do " + direction),
                           unswitched.subList(1, unswitched.size()).stream().anyMatch(x -> x));
            }
    }

    /// Iterates the deletion-aware tails, switching deletions off after `switchAfter` entries (never if it is
    /// negative), and reports for each tail whether it carries any deletion.
    private List<Boolean> tailDeletions(DeletionAwareTrie<LivePoint, DeletionMarker> source,
                                        Direction direction,
                                        int switchAfter,
                                        List<String> keys)
    {
        var iterator = new TrieTailsIterator.AsEntriesDeletionAware<>(source.cursor(direction), x -> true, true);
        List<Boolean> result = new ArrayList<>();
        while (iterator.hasNext())
        {
            var entry = iterator.next();
            keys.add(TrieUtil.asString(entry.getKey()));
            result.add(entry.getValue() != null && !DataPoint.deletionOnlyList(entry.getValue()).isEmpty());
            if (result.size() - 1 == switchAfter)
                iterator.stopIssuingDeletions(x -> false);
        }
        return result;
    }

    /// A tail taken at a position that has a deletion branch, after deletions have been switched off, must carry
    /// live content only. It does not, and this test records that.
    ///
    /// `SwitchableLiveAndDeletionsMergeCursor` passes the flag to the constructor of the tail it builds, but the
    /// base `LiveAndDeletionsMergeCursor` constructor runs `postAdvance` before the subclass field is assigned. The
    /// override of `postAdvance` therefore reads `false` and opens the deletion branch at the root of the tail; the
    /// assignment that follows cannot close it again. The tails the suite takes elsewhere do not show it because
    /// they are rooted below every deletion branch, so there is nothing for the constructor to re-open -- it takes a
    /// branch at the trie root, which is how a partition-level deletion is stored, for the flag to be lost.
    ///
    /// Consequence: a reader that has stopped issuing tombstones and then takes a tail of the switched view starts
    /// issuing them again. `TrieBackedPartition` and `TrieBackedPartitionStage3` reach the switch through the tails
    /// iterator and through a cursor walk respectively, neither of which takes a tail of the switched cursor, so
    /// this looks latent today rather than live.
    @Ignore("Tail of a switched cursor re-opens the deletion branch; see the comment above")
    @Test
    public void testTailAtTheRootAfterSwitchingDeletionsOff()
    {
        for (DeletionAwareTrie<LivePoint, DeletionMarker> source : sources())
            for (Direction direction : Direction.values())
            {
                Cursor<DataPoint> cursor = source.<DataPoint>mergedTrieSwitchable(DataPoint::resolve).makeCursor(direction);
                cursor.getPositionAndAssertFresh();
                stopIssuingDeletions(cursor, new TriePathReconstructor());
                assertEquals(qualify("tail at the root after switching deletions off " + direction),
                             entryStrings(source.contentOnlyTrie(), direction),
                             entryStrings(tailOf(cursor, direction), direction));
            }
    }

    /// The way `TrieBackedPartitionStage3` reaches the control: a cast of the cursor the trie hands out.
    ///
    /// Ignored because it does not hold when `cassandra.debug_tries` is set, which this suite sets and which the
    /// partition-level tests do not. [Trie#cursor] then wraps the cursor in a `VerificationCursor.Plain`, which does
    /// not implement [DeletionAwareTrie.DeletionsStopControl], so the cast throws. The tests above reach the same
    /// cursor through [Trie#makeCursor], which is what production gets with verification off. Enabling verification
    /// for a read that hits a tombstone limit would fail with a ClassCastException as things stand.
    @Ignore("VerificationCursor.Plain does not forward DeletionsStopControl; fails with cassandra.debug_tries set")
    @Test
    public void testControlIsReachableFromTheCursorTheTrieHandsOut()
    {
        Trie<DataPoint> switchable = inMemory(dataSet()).mergedTrieSwitchable(DataPoint::resolve);
        Cursor<DataPoint> cursor = switchable.cursor(Direction.FORWARD);
        assertTrue(cursor instanceof DeletionAwareTrie.DeletionsStopControl);
    }

    /// Walks the switchable view, switching deletions off once `switchAfter` entries have been reported, and
    /// returns everything it saw.
    private List<String> walkWithSwitch(DeletionAwareTrie<LivePoint, DeletionMarker> source,
                                        Direction direction,
                                        int switchAfter)
    {
        Cursor<DataPoint> cursor = source.<DataPoint>mergedTrieSwitchable(DataPoint::resolve).makeCursor(direction);
        TriePathReconstructor path = new TriePathReconstructor();
        List<String> result = new ArrayList<>();

        if (switchAfter == 0)
            stopIssuingDeletions(cursor, path);

        DataPoint content = Cursor.content(cursor, cursor.getPositionAndAssertFresh());
        while (true)
        {
            if (content != null)
            {
                result.add(asEntry(ByteComparable.preencoded(VERSION, path.getTrimmedPathBytes()), content));
                if (result.size() == switchAfter)
                    stopIssuingDeletions(cursor, path);
            }
            content = cursor.advanceToContent(path);
            if (content == null)
                return result;
        }
    }

    /// The entries the walk above must produce: everything the merged view reports up to the switch, then the live
    /// content that follows the last entry reported.
    private static List<String> expectedAfterSwitch(List<? extends Map.Entry<ByteComparable.Preencoded, ? extends DataPoint>> merged,
                                                    List<? extends Map.Entry<ByteComparable.Preencoded, ? extends DataPoint>> live,
                                                    Direction direction,
                                                    int switchAfter)
    {
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < switchAfter; ++i)
            expected.add(asEntry(merged.get(i).getKey(), merged.get(i).getValue()));

        ByteComparable last = switchAfter > 0 ? merged.get(switchAfter - 1).getKey() : null;
        for (var entry : live)
            if (last == null || direction.gt(ByteComparable.compare(entry.getKey(), last, VERSION), 0))
                expected.add(asEntry(entry.getKey(), entry.getValue()));
        return expected;
    }

    /// Every tail the switchable cursor produces before the switch must be the one the plain merge cursor produces
    /// at the same point of the same walk, read in either direction.
    private void assertTailsMatchMergedTails(DeletionAwareTrie<LivePoint, DeletionMarker> source, Direction direction)
    {
        Cursor<DataPoint> plain = source.<DataPoint>mergedTrie(DataPoint::resolve).makeCursor(direction);
        Cursor<DataPoint> switchable = source.<DataPoint>mergedTrieSwitchable(DataPoint::resolve).makeCursor(direction);
        TriePathReconstructor path = new TriePathReconstructor();
        DataPoint content = Cursor.content(plain, plain.getPositionAndAssertFresh());
        switchable.getPositionAndAssertFresh();
        while (true)
        {
            if (content != null)
            {
                ByteComparable.Preencoded key = ByteComparable.preencoded(VERSION, path.getTrimmedPathBytes());
                for (Direction readIn : Direction.values())
                    assertEquals(qualify("tail at " + TrieUtil.asString(key) + " walked " + direction + " read " + readIn),
                                 entryStrings(tailOf(plain, direction), readIn),
                                 entryStrings(tailOf(switchable, direction), readIn));
            }
            content = plain.advanceToContent(path);
            switchable.advanceToContent(null);
            if (content == null)
                return;
        }
    }

    /// Every tail taken after the switch must contain the live content of that branch and nothing else.
    private void assertTailsAreLiveOnlyAfterSwitch(DeletionAwareTrie<LivePoint, DeletionMarker> source,
                                                   Direction direction,
                                                   int switchAfter)
    {
        Cursor<DataPoint> cursor = source.<DataPoint>mergedTrieSwitchable(DataPoint::resolve).makeCursor(direction);
        TriePathReconstructor path = new TriePathReconstructor();
        Trie<LivePoint> contentOnly = source.contentOnlyTrie();
        int reported = 0;
        boolean switched = false;

        long position = cursor.getPositionAndAssertFresh();
        if (switchAfter == 0)
        {
            stopIssuingDeletions(cursor, path);
            switched = true;
        }

        DataPoint content = Cursor.content(cursor, position);
        while (true)
        {
            if (content != null)
            {
                if (switched)
                {
                    ByteComparable.Preencoded key = ByteComparable.preencoded(VERSION, path.getTrimmedPathBytes());
                    assertEquals(qualify("live-only tail at " + TrieUtil.asString(key) + " after " + switchAfter),
                                 entryStrings(contentOnly.tailTrie(key), direction),
                                 entryStrings(tailOf(cursor, direction), direction));
                }
                if (++reported == switchAfter)
                {
                    stopIssuingDeletions(cursor, path);
                    switched = true;
                    // The tail taken immediately after the switch is the branch the AT_C2 case documents.
                    ByteComparable.Preencoded key = ByteComparable.preencoded(VERSION, path.getTrimmedPathBytes());
                    assertEquals(qualify("tail immediately after the switch at " + TrieUtil.asString(key)),
                                 entryStrings(contentOnly.tailTrie(key), direction),
                                 entryStrings(tailOf(cursor, direction), direction));
                }
            }
            content = cursor.advanceToContent(path);
            if (content == null)
                return;
        }
    }

    private static void stopIssuingDeletions(Cursor<DataPoint> cursor, TriePathReconstructor path)
    {
        ((DeletionAwareTrie.DeletionsStopControl) cursor).stopIssuingDeletions(path);
    }

    /// The branch rooted at the cursor's current position, as a trie. Taking a tail does not disturb the cursor, so
    /// the walk that produced it can carry on afterwards.
    private static Trie<DataPoint> tailOf(Cursor<DataPoint> cursor, Direction direction)
    {
        Cursor<DataPoint> tail = cursor.tailCursor(direction);
        return tail::tailCursor;
    }

    private static <T extends DataPoint> List<Map.Entry<ByteComparable.Preencoded, T>> entries(Trie<T> trie, Direction direction)
    {
        return Streams.stream(trie.entryIterator(direction)).collect(Collectors.toList());
    }

    private static <T extends DataPoint> List<String> entryStrings(Trie<T> trie, Direction direction)
    {
        if (trie == null)
            return List.of();
        return Streams.stream(trie.entryIterator(direction))
                      .map(entry -> asEntry(entry.getKey(), entry.getValue()))
                      .collect(Collectors.toList());
    }

    private static String asEntry(ByteComparable key, DataPoint value)
    {
        return value.remap(key).toString();
    }

    private List<DeletionAwareTrie<LivePoint, DeletionMarker>> sources()
    {
        return sources(dataSet());
    }

    private List<DeletionAwareTrie<LivePoint, DeletionMarker>> sources(List<DataPoint> data)
    {
        return asList(inMemory(data), onDisk(data));
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
