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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Predicates;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.RANGE_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.STRING_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.directComparable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// Runs the whole of [DeletionAwareTailTrieTest] against the same trie after it has been written out with
/// [DeletionAwareFileWriter] and read back, and adds the checks that only make sense for the on-disk reader.
///
/// The on-disk suites only ever walk a trie and compare it with the in-memory one it was written from. A walk asks a
/// cursor for its content and for its children; it never asks a [RangeCursor] for [RangeCursor#state] or
/// [RangeCursor#precedingState], and it never takes a tail. Both of the worst defects found on the on-disk reader --
/// a range tail presenting unrestricted content at its root, and a range branch tail leaving the active range set
/// from the node's unrestricted content -- lived in exactly that combination, and both would have been reported as a
/// deletion covering a whole branch that it does not apply to. Every merge and intersection asks its sources for the
/// deletion active at their root, so a wrong answer there is a silent over- or under-deletion rather than a crash.
///
/// The tests below therefore compare states as well as walks, and take tails at every prefix rather than only at the
/// keys the data was built from -- including prefixes that fall in the middle of a chain node, which is the case
/// [OnDiskDeletionAwareTrieTest#testTailsOfADeletionBranch] deliberately keeps out of its own data.
public class OnDiskDeletionAwareTailTrieTest extends DeletionAwareTailTrieTest
{
    static OnDiskDeletionAwareTrie<String, TestRangeState> onDisk;

    @BeforeClass
    public static void writeAndRead() throws IOException
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
        DataOutputBuffer out = new DataOutputBuffer();
        DeletionAwareFileWriter.write(trie, STRING_SERDE, RANGE_SERDE, out);
        onDisk = OnDiskDeletionAwareTrie.open(out.asNewBuffer(), STRING_SERDE, RANGE_SERDE, VERSION, -1);
    }

    @Override
    DeletionAwareTrie<String, TestRangeState> trieUnderTest()
    {
        return onDisk;
    }

    /// Every prefix of every key, not only the ones the data was described with. The keys here are long, so most of
    /// these fall in the middle of a chain node, where the tail's root is a position the file has no code byte for
    /// and the reader has to carry how much of the chain is left. A tail rooted there that reads the wrong node
    /// returns a branch of the trie that is not the one that was asked for.
    private static List<ByteComparable> allPrefixes()
    {
        List<ByteComparable> prefixes = new ArrayList<>();
        for (ByteComparable key : new ByteComparable[]{ partition, key1, key2, key4, key5, key7 })
        {
            int length = key.asByteComparableArray(VERSION).length;
            for (int i = 0; i <= length; ++i)
                prefixes.add(ByteComparable.cut(key, i));
        }
        // A key that leaves the trie part-way down a chain node, and one that leaves it at a branching node.
        prefixes.add(directComparable("partition/kex"));
        prefixes.add(directComparable("partition/key8"));
        prefixes.add(directComparable("partitiox"));
        return prefixes;
    }

    /// A tail of the read-back trie must present exactly what the tail of the in-memory trie presents, at every
    /// prefix and with covering deletions both included and excluded. Compared with states, not only with a walk:
    /// `includeCoveringDeletions` synthesises a deletion branch at the tail's root out of the deletion that covers
    /// the prefix, and a reader that gets that root state wrong reports a range that does not apply to the branch it
    /// heads. That is invisible to a walk and is what every merge source is asked for.
    @Test
    public void testTailsAtEveryPrefix()
    {
        for (ByteComparable prefix : allPrefixes())
            for (boolean includeCoveringDeletions : new boolean[]{ true, false })
            {
                String message = "tailTrie(" + prefix.byteComparableAsString(VERSION) + ", " + includeCoveringDeletions + ')';
                assertTailsEqual(message,
                                 trie.tailTrie(prefix, includeCoveringDeletions),
                                 onDisk.tailTrie(prefix, includeCoveringDeletions));
            }
    }

    /// The same over a tail of a tail. A tail taken from a tail is rooted at a node the second cursor was handed
    /// rather than one it descended to from the root of the file, which is the shape a query that drills into a
    /// partition and then into a row takes.
    @Test
    public void testTailsOfTails()
    {
        for (boolean includeCoveringDeletions : new boolean[]{ true, false })
        {
            DeletionAwareTrie<String, TestRangeState> expectedHead = trie.tailTrie(partition, includeCoveringDeletions);
            DeletionAwareTrie<String, TestRangeState> actualHead = onDisk.tailTrie(partition, includeCoveringDeletions);
            assertNotNull(actualHead);
            for (String suffix : new String[]{ "", "/", "/k", "/key", "/key1", "/key5", "/key7", "/key8" })
            {
                ByteComparable prefix = directComparable(suffix);
                String message = "tailTrie(partition, " + includeCoveringDeletions + ").tailTrie(" + suffix + ')';
                assertTailsEqual(message,
                                 expectedHead.tailTrie(prefix, includeCoveringDeletions),
                                 actualHead.tailTrie(prefix, includeCoveringDeletions));
            }
        }
    }

    /// [Cursor#tailCursor] taken at every position a walk passes through, which is the entry point
    /// [DeletionAwareTrie#tailTrie] and [DeletionAwareTrie#prefixedBySeparately] are built on. A walk of the whole
    /// trie stops in the middle of the chain nodes that spell out the keys, so this reaches the mid-chain roots that
    /// no other on-disk test takes a trie-level tail at.
    @Test
    public void testTailCursorsAtEveryPosition()
    {
        for (Direction direction : Direction.values())
        {
            DeletionAwareCursor<String, TestRangeState> expected = trie.cursor(direction);
            DeletionAwareCursor<String, TestRangeState> actual = onDisk.cursor(direction);
            long position = expected.encodedPosition();
            assertEquals(Cursor.toString(position), Cursor.toString(actual.encodedPosition()));
            while (!Cursor.isExhausted(position))
            {
                if (!Cursor.isOnReturnPath(position))
                    for (Direction tailDirection : Direction.values())
                        assertDeletionAwareCursorsEqual("tailCursor(" + tailDirection + ") at " + Cursor.toString(position),
                                                        expected.tailCursor(tailDirection),
                                                        actual.tailCursor(tailDirection));
                position = expected.advance();
                assertEquals(Cursor.toString(position), Cursor.toString(actual.advance()));
            }
        }
    }

    /// The deletion that applies at a key is read by descending along it and then asking the cursor for its state --
    /// the one query that consists of nothing but the calls a walk never makes. It is also what a merge does with
    /// every source it is given, so a wrong answer here is applied to data the source does not cover.
    @Test
    public void testApplicableDeletionAtEveryPrefix()
    {
        for (ByteComparable prefix : allPrefixes())
            assertEquals("applicableDeletion(" + prefix.byteComparableAsString(VERSION) + ')',
                         trie.applicableDeletion(prefix),
                         onDisk.applicableDeletion(prefix));
    }

    /// [DeletionAwareTrie#tailTries] over the read-back trie, against the in-memory tails for the same keys. The
    /// inherited tests check the tails against a hand-written expectation; this checks that the reader's tails and
    /// the in-memory ones are the same object graph, states included.
    @Test
    public void testTailTriesMatchInMemory()
    {
        for (Direction direction : Direction.values())
            for (boolean includeCoveringDeletions : new boolean[]{ true, false })
            {
                var expectedTails = trie.tailTries(direction, Predicates.alwaysTrue(), includeCoveringDeletions).iterator();
                for (var actualTail : onDisk.tailTries(direction, Predicates.alwaysTrue(), includeCoveringDeletions))
                {
                    assertTrue("Fewer tails in memory than on disk", expectedTails.hasNext());
                    var expectedTail = expectedTails.next();
                    assertEquals(0, ByteComparable.compare(expectedTail.getKey(), actualTail.getKey(), VERSION));
                    assertTailsEqual("tailTries(" + direction + ", " + includeCoveringDeletions + ") at "
                                     + actualTail.getKey().byteComparableAsString(VERSION),
                                     expectedTail.getValue(),
                                     actualTail.getValue());
                }
                assertTrue("More tails in memory than on disk", !expectedTails.hasNext());
            }
    }

    /// [DeletionAwareTrie#prefixedBy] and [DeletionAwareTrie#prefixedBySeparately] over the read-back trie.
    /// `prefixedBySeparately` takes the deletion side from `deletionBranchCursor` or from a `tailCursor` of the
    /// source depending on `deletionsMustBeAtRoot`, so it exercises the reader's branch-at-root handling from the
    /// opposite side to `tailTrie`, and its result is what a prefixed source contributes to a merge.
    @Test
    public void testPrefixedByOverAReadBackTrie()
    {
        for (String prefixString : new String[]{ "", "a", "partition", "zzz" })
        {
            ByteComparable prefix = directComparable(prefixString);
            assertTailsEqual("prefixedBy(" + prefixString + ')',
                             trie.prefixedBy(prefix),
                             onDisk.prefixedBy(prefix));
            // The deletion branch of this trie is at "partition", not at the root, so only the below-root form
            // describes it; the at-root form is checked on the tail at "partition", which does have it at its root.
            assertTailsEqual("prefixedBySeparately(" + prefixString + ", false)",
                             trie.prefixedBySeparately(prefix, false),
                             onDisk.prefixedBySeparately(prefix, false));

            DeletionAwareTrie<String, TestRangeState> expectedHead = trie.tailTrie(partition, true);
            DeletionAwareTrie<String, TestRangeState> actualHead = onDisk.tailTrie(partition, true);
            assertTailsEqual("tailTrie(partition).prefixedBySeparately(" + prefixString + ", true)",
                             expectedHead.prefixedBySeparately(prefix, true),
                             actualHead.prefixedBySeparately(prefix, true));
        }
    }

    private static void assertTailsEqual(String message,
                                         DeletionAwareTrie<String, TestRangeState> expected,
                                         DeletionAwareTrie<String, TestRangeState> actual)
    {
        if (expected == null || actual == null)
        {
            assertEquals(message, expected == null, actual == null);
            return;
        }
        for (Direction direction : Direction.values())
            assertDeletionAwareCursorsEqual(message + ' ' + direction,
                                            expected.cursor(direction),
                                            actual.cursor(direction));
    }

    /// [TrieUtil#assertCursorWalksEqual] compares the deletion branches of two deletion-aware cursors as plain
    /// cursors, which compares their content and their shape but not the range state they report. The state is the
    /// part a merge reads and the part the reader gets wrong, so it is compared here.
    private static void assertDeletionAwareCursorsEqual(String message,
                                                        DeletionAwareCursor<String, TestRangeState> expected,
                                                        DeletionAwareCursor<String, TestRangeState> actual)
    {
        long expectedPosition = expected.encodedPosition();
        long actualPosition = actual.encodedPosition();
        while (true)
        {
            assertEquals(message + " position", Cursor.toString(expectedPosition), Cursor.toString(actualPosition));
            if (Cursor.isExhausted(expectedPosition))
                return;
            assertEquals(message + " content at " + Cursor.toString(expectedPosition),
                         expected.content(), actual.content());
            Direction direction = Cursor.direction(expectedPosition);
            RangeCursor<TestRangeState> expectedBranch = expected.deletionBranchCursor(direction);
            RangeCursor<TestRangeState> actualBranch = actual.deletionBranchCursor(direction);
            assertEquals(message + " deletion branch present at " + Cursor.toString(expectedPosition),
                         expectedBranch != null, actualBranch != null);
            if (expectedBranch != null)
                assertRangeCursorsEqual(message + " deletion branch at " + Cursor.toString(expectedPosition),
                                        expectedBranch, actualBranch);
            expectedPosition = expected.advance();
            actualPosition = actual.advance();
        }
    }

    private static void assertRangeCursorsEqual(String message,
                                                RangeCursor<TestRangeState> expected,
                                                RangeCursor<TestRangeState> actual)
    {
        long expectedPosition = expected.encodedPosition();
        long actualPosition = actual.encodedPosition();
        while (true)
        {
            assertEquals(message + " position", Cursor.toString(expectedPosition), Cursor.toString(actualPosition));
            if (Cursor.isExhausted(expectedPosition))
                return;
            String at = message + " at " + Cursor.toString(expectedPosition);
            assertEquals(at + " content", expected.content(), actual.content());
            assertEquals(at + " state", expected.state(), actual.state());
            assertEquals(at + " preceding state", expected.precedingState(), actual.precedingState());
            // equals() on TestRangeState only compares the two sides, so check the boundary flag separately: a
            // covering state reported as a boundary is the shape both root-content defects produced.
            assertSameBoundedness(at + " state", expected.state(), actual.state());
            assertSameBoundedness(at + " preceding state", expected.precedingState(), actual.precedingState());
            expectedPosition = expected.advance();
            actualPosition = actual.advance();
        }
    }

    private static void assertSameBoundedness(String message, TestRangeState expected, TestRangeState actual)
    {
        if (expected == null)
        {
            assertNull(message, actual);
            return;
        }
        assertNotNull(message, actual);
        assertEquals(message + " boundary", expected.isBoundary(), actual.isBoundary());
    }
}
