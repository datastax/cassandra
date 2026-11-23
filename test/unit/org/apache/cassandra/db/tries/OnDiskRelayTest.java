/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/// Covers how [OnDiskCursor] and the cursors built on it handle relay nodes (code `11111bbb`). The round-trip tests
/// only see the relays that a page-aware writer chooses to place, so the reader is given hand-written bytes here,
/// derived from the format description in `OnDiskTrie.md` and independent of any writer. They place relays everywhere
/// a node position can be expected to be found: as the implicit first child of a sparse, bitmap and chain node, as
/// the node a prefix augments, as the target of an explicit pointer and of a deletion branch pointer, and as the
/// root. The one place a relay cannot be is where another relay leads, which is among the sequences of nodes that
/// the reader must reject.
///
/// It also covers what the tail of a range cursor presents at its root when it is taken on a boundary that ends a
/// range without reaching into the branch below it, given hand-written bytes as well.
///
/// ## Reading the vectors
///
/// A vector is listed node by node, each piece followed by the position it ends at. The position of a node is the
/// one just after its code byte, i.e. the end of its piece; a node is read backwards from there and "starts" where
/// its piece begins, which is the position its pointers are taken from. Multi-byte pointers have their least
/// significant byte first in the file. [#vector] checks the listed positions, so that the arithmetic in the comments
/// (`starts 4 → 2` stands for "a relay whose data starts at 4 and whose pointer of 2 gives the target 4 - 2 = 2")
/// can be trusted.
///
/// Plain content is one byte holding a number (see [#INT_CONTENT]). Range content is two bytes, the left and right
/// side of a [TestRangeState] with `ff` for a side without a range (see [#RANGE_CONTENT]); `1<X<2` below stands for
/// a boundary with 1 on its left and 2 on its right, `X<1` and `1<X` for the start and the end of a range. The
/// padding between nodes is `ff`, which a reader that strays into it decodes as a relay it cannot follow, rather
/// than `00`, which is a valid empty leaf.
public class OnDiskRelayTest
{
    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    // ---- Relays: plain tries -------------------------------------------------------------------------------------

    /// Code `11111bbb` is a relay and not a reserved code any more: a cursor that is given one follows it, and
    /// presents the node it stands for in its place.
    @Test
    public void testRelayIsFollowed() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("05 01", 2,      // leaf 5
                                   "00 f8", 4);     // relay with a one-byte pointer; starts 2 → 2, the node just before it
        assertWalks(vector,
                    List.of("=5"),
                    List.of("=5"),
                    List.of("", "↑=5"));
        assertReadsAsTrie(vector, "", 5);
    }

    /// The first child of a sparse node has no pointer: it is expected to end where the node starts. A relay there
    /// supplies the position of a child that is somewhere else.
    @Test
    public void testSparseFirstChildThroughRelay() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("01 01", 2,           // leaf 1                                       ("a")
                                   "02 01", 4,           // leaf 2                                       ("b")
                                   "02 f8", 6,           // relay; starts 4 → 2
                                   "02 62 61 80", 10);   // sparse {a, b}, one-byte pointers; starts 6:
                                                         //   a → 6 (the relay), b → 6 - 2 = 4
        assertWalks(vector,
                    List.of("", "a=1", "b=2"),
                    List.of("", "b=2", "a=1"),
                    List.of("", "b↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "b", 2);

        // The node that the cursor is on after it has been taken through the relay is the leaf: this is what a tail
        // is rooted at, and where a deletion branch pointer is looked for.
        OnDiskCursor<Integer> cursor = plainCursor(vector, Direction.FORWARD, false);
        cursor.advance();
        assertEquals(1, cursor.currentFullNodePostCodePos);
        assertEquals(0x01, cursor.currentFullNodeCode);

        // The same with more children, padding between the relay's target and the relay, and a wider pointer than
        // the distance needs.
        vector = vector("01 01", 2,                      // leaf 1                                       ("a")
                        pad(3), 5,
                        "03 01", 7,                      // leaf 3                                       ("c")
                        "02 01", 9,                      // leaf 2                                       ("b")
                        "07 00 f9", 12,                  // relay, two-byte pointer; starts 9 → 9 - 0x0007 = 2
                        "05 03 63 62 61 84", 18);        // sparse {a, b, c}; starts 12:
                                                         //   a → 12 (the relay), b → 12 - 3 = 9, c → 12 - 5 = 7
        assertWalks(vector,
                    List.of("", "a=1", "b=2", "c=3"),
                    List.of("", "c=3", "b=2", "a=1"),
                    List.of("", "c↑=3", "b↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "b", 2, "c", 3);
    }

    /// A relay stands for a node and is followed once: one that leads to another relay is rejected, wherever it is
    /// met. No writer produces such a pair (see [OnDiskNodeWriterPageAware#complete]).
    @Test
    public void testRelayToRelayIsRejected()
    {
        // at the root
        assertRootRejectedAsCorruptRelay(vector("02 01", 2,           // leaf 2
                                                "00 f8", 4,           // relay B; starts 2 → 2 (the leaf)
                                                "00 f8", 6),          // relay A; starts 4 → 4 (relay B)
                                         RELAY_CHAIN);
        // with wider pointers and padding between the two
        assertRootRejectedAsCorruptRelay(vector("02 01", 2,           // leaf 2
                                                pad(3), 5,
                                                "03 00 f9", 8,        // relay B; starts 5 → 5 - 0x0003 = 2 (the leaf)
                                                pad(2), 10,
                                                "02 00 00 fa", 14),   // relay A; starts 10 → 10 - 0x000002 = 8 (relay B)
                                         RELAY_CHAIN);

        // as the first child of a sparse node, and as a child it has a pointer to
        ByteBuffer sparse = vector("01 01", 2,           // leaf 1                                       ("a", "c")
                                   "02 01", 4,           // leaf 2                                       ("b")
                                   "02 f8", 6,           // relay B; starts 4 → 2 (leaf 1)
                                   "00 f8", 8,           // relay A; starts 6 → 6 (relay B)
                                   "00 04 63 62 61 84", 14);  // sparse {a, b, c}; starts 8:
                                                         //   a → 8 (relay A), b → 8 - 4 = 4, c → 8 - 0 = 8 (relay A)
        for (Direction direction : Direction.values())
        {
            OnDiskCursor<Integer> advancing = plainCursor(sparse, direction, false);
            assertRejectedAsCorruptRelay(direction + " advance", RELAY_CHAIN, advancing::advance);
            OnDiskCursor<Integer> skipping = plainCursor(sparse, direction, false);
            assertRejectedAsCorruptRelay(direction + " skipTo", RELAY_CHAIN,
                                         () -> skipping.skipTo(Cursor.positionForDescentWithByte(skipping.encodedPosition(),
                                                                                                 direction.select('a', 'c'))));
        }

        // as the child of a chain, also when the chain is stepped over in one move
        ByteBuffer chain = vector("01 01", 2,            // leaf 1                                       ("ab")
                                  "00 f8", 4,            // relay B; starts 2 → 2 (the leaf)
                                  "00 f8", 6,            // relay A; starts 4 → 4 (relay B)
                                  "62 61 41", 9);        // chain "ab"; child at 8 - 2 = 6 (relay A)
        for (Direction direction : Direction.values())
        {
            OnDiskCursor<Integer> cursor = plainCursor(chain, direction, false);
            cursor.advance();
            assertRejectedAsCorruptRelay(direction + " advance", RELAY_CHAIN, cursor::advance);
            OnDiskCursor<Integer> jumping = plainCursor(chain, direction, false);
            assertRejectedAsCorruptRelay(direction + " advanceMultiple", RELAY_CHAIN, () -> jumping.advanceMultiple(null));
        }

        // behind a prefix node
        ByteBuffer prefix = vector("02 01", 2,           // leaf 2                                       ("a")
                                   "61 40", 4,           // chain "a"; child at 3 - 1 = 2
                                   "00 f8", 6,           // relay B; starts 4 → 4 (chain "a")
                                   "00 f8", 8,           // relay A; starts 6 → 6 (relay B)
                                   "01 01 f5", 11);      // prefix with content 1; child at 11 - 3 = 8 (relay A)
        for (Direction direction : Direction.values())
            for (boolean ordered : new boolean[]{ false, true })
                assertRejectedAsCorruptRelay(direction + " prefix", RELAY_CHAIN, () -> plainCursor(prefix, direction, ordered));

        // in the walk of a range trie
        ByteBuffer range = vector("01 02 02", 3,         // leaf 1<X<2                                   ("ab")
                                  "00 f8", 5,            // relay B; starts 3 → 3 (the leaf)
                                  "00 f8", 7,            // relay A; starts 5 → 5 (relay B)
                                  "62 61 41", 10);       // chain "ab"; child at 9 - 2 = 7 (relay A)
        for (Direction direction : Direction.values())
        {
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(range, direction);
            cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'a'));
            assertRejectedAsCorruptRelay(direction + " advance", RELAY_CHAIN, cursor::advance);
        }

        // at the root of a deletion branch
        ByteBuffer branch = vector("ff 01 02", 3,        // leaf X<1
                                   "00 f8", 5,           // relay B; starts 3 → 3 (the leaf)
                                   "00 f8", 7,           // relay A; starts 5 → 5 (relay B)
                                   "00 f2", 9);          // prefix with an empty ascent slot, i.e. with its branch
                                                         //   root where it starts, at 7 (relay A)
        OnDiskDeletionAwareTrie<Integer, TestRangeState> trie = OnDiskDeletionAwareTrie.open(branch, INT_CONTENT, RANGE_CONTENT, VERSION, -1);
        for (Direction direction : Direction.values())
        {
            DeletionAwareCursor<Integer, TestRangeState> cursor = trie.makeCursor(direction);
            for (Direction branchDirection : Direction.values())
                assertRejectedAsCorruptRelay(direction + " " + branchDirection + " branch", RELAY_CHAIN,
                                             () -> cursor.deletionBranchCursor(branchDirection));
        }
    }

    /// A prefix node holds the content of a node whose children are listed by the node that follows it, i.e. the one
    /// that ends where the prefix node's content starts, or the one a relay in that place leads to. That node can be
    /// neither a leaf nor another prefix node: the node the cursor is on would then have two contents. The reader
    /// rejects a prefix node that follows a prefix node, directly or behind a relay, and a leaf that follows one
    /// directly.
    ///
    /// The content is two bytes here so that the same vector can be opened as every kind of trie.
    @Test
    public void testInvalidNodeAfterPrefixIsRejected()
    {
        // prefix → leaf
        assertRootRejectedAsCorrupt(vector("ff 01 02", 3,             // leaf X<1
                                           "ff 02 02 f5", 7),         // prefix with content X<2 and a child, which is
                                                                      //   at 7 - 4 = 3 (the leaf)
                                    PREFIX_TO_LEAF);
        // prefix → prefix, without a child and with one
        assertRootRejectedAsCorrupt(vector("ff 01 02 f4", 4,          // prefix with content X<1, no child
                                           "ff 02 02 f5", 8),         // prefix with content X<2; child at 8 - 4 = 4
                                    CHAINED_PREFIXES);
        assertRootRejectedAsCorrupt(vector("ff 03 02", 3,             // leaf X<3                             ("a")
                                           "61 40", 5,                // chain "a"; child at 4 - 1 = 3
                                           "ff 01 02 f5", 9,          // prefix with content X<1; child at 9 - 4 = 5
                                           "ff 02 02 f5", 13),        // prefix with content X<2; child at 13 - 4 = 9
                                    CHAINED_PREFIXES);
        // prefix → relay -> leaf
        assertRootRejectedAsCorrupt(vector("ff 01 02", 3,             // leaf X<1
                                           "00 f8", 5,                // relay; starts 3 → 3 (the leaf)
                                           "ff 02 02 f5", 9),         // prefix with content X<2 and a child, which is
                                    //   at 9 - 4 = 5 (the relay)
                                    PREFIX_TO_LEAF);

        // prefix → relay → prefix
        assertRootRejectedAsCorrupt(vector("ff 01 02 f4", 4,          // prefix with content X<1, no child
                                           "00 f8", 6,                // relay; starts 4 → 4 (the prefix)
                                           "ff 02 02 f5", 10),        // prefix with content X<2; child at 10 - 4 = 6
                                    CHAINED_PREFIXES);

        // The same with content on the ascent side, and on both, in the tries that can have it: range tries, as plain
        // tries only use the descent side, and deletion-aware ones, where the ascent side is the pointer to the
        // deletion branch, here with the distance 2.
        ByteBuffer ascentToLeaf = vector("ff 01 02", 3,               // leaf X<1
                                         "01 ff 02 f3", 7);           // prefix with ascent content 1<X and a child,
                                                                      //   which is at 7 - 4 = 3 (the leaf)
        ByteBuffer branchToLeaf = vector("ff 01 02", 3,               // leaf X<1
                                         "02 01 f3", 6);              // prefix with branch pointer 2 and a child,
                                                                      //   which is at 6 - 3 = 3 (the leaf)
        ByteBuffer bothToPrefix = vector("ff 01 02 f4", 4,            // prefix with content X<1, no child
                                         "03 ff 02 02 03 02 f7", 11); // prefix with ascent content 3<X, descent
                                                                      //   content 2<X<3 and a child at 11 - 7 = 4
        for (Direction direction : Direction.values())
        {
            assertRejectedAsCorrupt(direction + " range", PREFIX_TO_LEAF, () -> rangeCursor(ascentToLeaf, direction));
            assertRejectedAsCorrupt(direction + " data", PREFIX_TO_LEAF, () -> dataCursor(branchToLeaf, direction));
            assertRejectedAsCorrupt(direction + " range", CHAINED_PREFIXES, () -> rangeCursor(bothToPrefix, direction));
        }

        // below the root, where it is a move of the cursor that comes across it
        ByteBuffer prefixToLeaf = vector("01 01", 2,                  // leaf 1
                                         "02 01 f5", 5,               // prefix with content 2; child at 5 - 3 = 2
                                         "61 40", 7);                 // chain "a"; child at 6 - 1 = 5
        ByteBuffer prefixToPrefix = vector("01 01 f4", 3,             // prefix with content 1, no child
                                           "00 f8", 5,                // relay; starts 3 → 3 (the prefix)
                                           "02 01 f5", 8,             // prefix with content 2; child at 8 - 3 = 5
                                           "61 40", 10);              // chain "a"; child at 9 - 1 = 8
        for (Direction direction : Direction.values())
            for (boolean ordered : new boolean[]{ false, true })
            {
                String description = direction + (ordered ? " ordered" : " unordered");
                OnDiskCursor<Integer> toLeaf = plainCursor(prefixToLeaf, direction, ordered);
                assertRejectedAsCorrupt(description + " advance", PREFIX_TO_LEAF, toLeaf::advance);
                OnDiskCursor<Integer> toPrefix = plainCursor(prefixToPrefix, direction, ordered);
                assertRejectedAsCorrupt(description + " advance", CHAINED_PREFIXES, toPrefix::advance);
                OnDiskCursor<Integer> skipping = plainCursor(prefixToPrefix, direction, ordered);
                assertRejectedAsCorrupt(description + " skipTo", CHAINED_PREFIXES,
                                        () -> skipping.skipTo(Cursor.positionForDescentWithByte(skipping.encodedPosition(), 'a')));
            }
    }

    /// What may follow a prefix node: any node that lists children, and a relay to one. In particular the node a
    /// prefix node is followed by can itself be reached through a relay, i.e. relay → prefix → relay is not a chain
    /// of relays.
    @Test
    public void testRelayBeforeAndAfterPrefix() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("02 01", 2,           // leaf 2                                       ("ab")
                                   "62 40", 4,           // chain "b"; child at 3 - 1 = 2                 ("a")
                                   pad(1), 5,
                                   "01 f8", 7,           // relay B; starts 5 → 4 (chain "b")
                                   "01 01 f5", 10,       // prefix with content 1; child at 10 - 3 = 7 (relay B)
                                   pad(2), 12,
                                   "02 f8", 14,          // relay A; starts 12 → 10 (the prefix)
                                   "61 40", 16);         // chain "a"; child at 15 - 1 = 14 (relay A)
        assertWalks(vector,
                    List.of("", "a=1", "ab=2"),
                    List.of("", "a=1", "ab=2"),
                    List.of("", "a", "ab↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "ab", 2);

        // The node the cursor is on at "a" is the prefix node: the relay that led to it is left behind, the one that
        // follows it is not what a tail is rooted at.
        OnDiskCursor<Integer> cursor = plainCursor(vector, Direction.FORWARD, false);
        cursor.advance();
        assertEquals(9, cursor.currentFullNodePostCodePos);
        assertEquals(0xf5, cursor.currentFullNodeCode);
    }

    /// The first child of a bitmap node is implicit like the one of a sparse node.
    @Test
    public void testBitmapFirstChildThroughRelay() throws TrieSpaceExhaustedException
    {
        // Transition t is bit t % 8 of the bitmap's byte 31 - t / 8 in file order: a (0x61) and b (0x62) are bits 1
        // and 2 of byte 31 - 12 = 19.
        ByteBuffer vector = vector("01 01", 2,           // leaf 1                                       ("a")
                                   "02 01", 4,           // leaf 2                                       ("b")
                                   "02 f8", 6,           // relay; starts 4 → 2
                                   join("02",            // bitmap {a, b}, one-byte pointers; starts 6:
                                        zeros(19),       //   a → 6 (the relay), b → 6 - 2 = 4
                                        "06",
                                        zeros(12),
                                        "e0"), 40);
        assertWalks(vector,
                    List.of("", "a=1", "b=2"),
                    List.of("", "b=2", "a=1"),
                    List.of("", "b↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "b", 2);
    }

    /// A dense node has no implicit child, but its pointers can lead to relays like any other.
    @Test
    public void testDensePointersToRelays() throws TrieSpaceExhaustedException
    {
        // The pointer for transition t is at 255 - t in file order: 157 for b (0x62) and 158 for a (0x61).
        ByteBuffer vector = vector("01 01", 2,           // leaf 1                                       ("a")
                                   "02 01", 4,           // leaf 2                                       ("b")
                                   "02 f8", 6,           // relay A; starts 4 → 2 (leaf 1)
                                   "04 f8", 8,           // relay B; starts 6 → 2 (leaf 1)
                                   "04 f8", 10,          // relay C; starts 8 → 4 (leaf 2)
                                   join(pad(157),        // dense, one-byte pointers, ff for no child; starts 10:
                                        "00 02",         //   a → 10 - 2 = 8 (relay B → leaf 1)
                                        pad(97),         //   b → 10 - 0 = 10 (relay C → leaf 2)
                                        "e8"), 267);
        assertWalks(vector,
                    List.of("", "a=1", "b=2"),
                    List.of("", "b=2", "a=1"),
                    List.of("", "b↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "b", 2);
    }

    /// The node a chain leads to is expected to end where the chain's transitions start.
    @Test
    public void testChainChildThroughRelay() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("01 01", 2,           // leaf 1                                       ("a")
                                   pad(2), 4,
                                   "02 f8", 6,           // relay; starts 4 → 2
                                   "61 40", 8);          // chain "a"; child at 7 - 1 = 6 (the relay)
        assertWalks(vector,
                    List.of("", "a=1"),
                    List.of("", "a=1"),
                    List.of("", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1);

        // A chain that leads to a chain through a relay, which is what the two pieces of a chain of more than 64
        // transitions look like when they are not adjacent.
        vector = vector("01 01", 2,                      // leaf 1                                       ("ab")
                        "62 40", 4,                      // chain "b"; child at 3 - 1 = 2
                        pad(1), 5,
                        "01 f8", 7,                      // relay; starts 5 → 4
                        "61 40", 9);                     // chain "a"; child at 8 - 1 = 7 (the relay)
        assertWalks(vector,
                    List.of("", "a", "ab=1"),
                    List.of("", "a", "ab=1"),
                    List.of("", "a", "ab↑=1"));
        assertReadsAsTrie(vector, "ab", 1);
    }

    /// A chain of several transitions is stepped through without reading a node code, so the relay only comes into
    /// play at its end. That end can be reached one transition at a time, in one jump by [Cursor#advanceMultiple],
    /// and from a tail that starts in the middle of the chain.
    @Test
    public void testLongerChainChildThroughRelay() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("01 01", 2,              // leaf 1                                    ("abcd")
                                   pad(3), 5,
                                   "03 f8", 7,              // relay; starts 5 → 2
                                   "64 63 62 61 43", 12);   // chain "abcd"; child at 11 - 4 = 7 (the relay)
        assertWalks(vector,
                    List.of("", "a", "ab", "abc", "abcd=1"),
                    List.of("", "a", "ab", "abc", "abcd=1"),
                    List.of("", "a", "ab", "abc", "abcd↑=1"));
        assertReadsAsTrie(vector, "abcd", 1);

        for (Direction direction : Direction.values())
            for (boolean ordered : new boolean[]{ false, true })
            {
                String context = direction + (ordered ? " ordered" : " unordered");
                boolean onReturnPath = ordered && !direction.isForward();

                // in one jump, with and without someone to take the transitions jumped over
                StringBuilder received = new StringBuilder();
                OnDiskCursor<Integer> cursor = plainCursor(vector, direction, ordered);
                long position = cursor.advanceMultiple(pathReceiver(received));
                assertEquals(context, "abc", received.toString());
                assertEquals(context, 4, Cursor.depth(position));
                assertEquals(context, 'd', Cursor.incomingTransition(position));
                assertEquals(context, onReturnPath, Cursor.isOnReturnPath(position));
                assertEquals(context, Integer.valueOf(1), cursor.content());
                assertNotOnRelay(cursor);
                assertTrue(context, Cursor.isExhausted(cursor.advanceMultiple(pathReceiver(received))));

                cursor = plainCursor(vector, direction, ordered);
                position = cursor.advanceMultiple(null);
                assertEquals(context, 4, Cursor.depth(position));
                assertEquals(context, Integer.valueOf(1), cursor.content());

                // in a jump that starts in the middle of the chain
                received.setLength(0);
                cursor = plainCursor(vector, direction, ordered);
                cursor.advance();
                position = cursor.advanceMultiple(pathReceiver(received));
                assertEquals(context, "bc", received.toString());
                assertEquals(context, 4, Cursor.depth(position));
                assertEquals(context, Integer.valueOf(1), cursor.content());

                // from a tail taken in the middle of the chain, which has no code byte of its own in the file
                cursor = plainCursor(vector, direction, ordered);
                cursor.advance();
                cursor.advance();
                assertEquals(context, 9, cursor.currentFullNodePostCodePos);
                assertEquals(context, 0x41, cursor.currentFullNodeCode);
                for (Direction tailDirection : Direction.values())
                {
                    boolean tailOnReturnPath = ordered && !tailDirection.isForward();
                    assertEquals(context + ", " + tailDirection + " tail",
                                 List.of("", "c", tailOnReturnPath ? "cd↑=1" : "cd=1"),
                                 walk((OnDiskCursor<Integer>) cursor.tailCursor(tailDirection), String::valueOf));
                }
            }
    }

    /// The node that a prefix node augments is expected to end where the prefix node's content starts.
    @Test
    public void testPrefixChildThroughRelay() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("02 01", 2,           // leaf 2                                       ("cd")
                                   "64 63 41", 5,        // chain "cd"; child at 4 - 2 = 2
                                   pad(2), 7,
                                   "02 f8", 9,           // relay; starts 7 → 5
                                   "01 01 f5", 12);      // prefix with content 1 (and its length) and a child,
                                                         //   which is at 12 - 3 = 9 (the relay)             ("")
        // In the reverse walk of an ordered trie the content of the prefix is presented on the way back, by which
        // time the cursor has been through the relay and the nodes beyond it.
        assertWalks(vector,
                    List.of("=1", "c", "cd=2"),
                    List.of("=1", "c", "cd=2"),
                    List.of("", "c", "cd↑=2", "↑=1"));
        assertReadsAsTrie(vector, "", 1, "cd", 2);

        // The node the cursor is on remains the prefix node.
        OnDiskCursor<Integer> cursor = plainCursor(vector, Direction.FORWARD, false);
        assertEquals(11, cursor.currentFullNodePostCodePos);
        assertEquals(0xf5, cursor.currentFullNodeCode);

        // A relay that leads to a relay in that place.
        vector = vector("02 01", 2,                      // leaf 2                                       ("c")
                        "63 40", 4,                      // chain "c"; child at 3 - 1 = 2
                        pad(1), 5,
                        "01 f8", 7,                      // relay A; starts 5 → 4 (chain "c")
                        "03 f8", 9,                      // relay B; starts 7 → 4 (as relay A)
                        "01 01 f5", 12);                 // prefix with content 1; child at 12 - 3 = 9 (relay B)   ("")
        assertWalks(vector,
                    List.of("=1", "c=2"),
                    List.of("=1", "c=2"),
                    List.of("", "c↑=2", "↑=1"));
        assertReadsAsTrie(vector, "", 1, "c", 2);

        // The same below the root, where the prefix node is itself reached through a relay, and with a sparse node
        // as the one that is augmented.
        vector = vector("02 01", 2,                      // leaf 2                                       ("ab")
                        "03 01", 4,                      // leaf 3                                       ("ac")
                        pad(1), 5,
                        "03 f8", 7,                      // relay A; starts 5 → 2 (leaf 2)
                        "03 63 62 80", 11,               // sparse {b, c}; starts 7: b → 7 (relay A), c → 7 - 3 = 4
                        "00 f8", 13,                     // relay B; starts 11 → 11 (the sparse node)
                        "01 01 f5", 16,                  // prefix with content 1; child at 16 - 3 = 13 (relay B)  ("a")
                        "04 01", 18,                     // leaf 4                                       ("d")
                        "02 f8", 20,                     // relay C; starts 18 → 16 (the prefix)
                        "02 64 61 80", 24);              // sparse {a, d}; starts 20: a → 20 (relay C), d → 20 - 2 = 18
        assertWalks(vector,
                    List.of("", "a=1", "ab=2", "ac=3", "d=4"),
                    List.of("", "d=4", "a=1", "ac=3", "ab=2"),
                    List.of("", "d↑=4", "a", "ac↑=3", "ab↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "ab", 2, "ac", 3, "d", 4);
    }

    /// The root position that a trie is opened with can be that of a relay.
    @Test
    public void testRootIsRelay() throws TrieSpaceExhaustedException
    {
        // In the reverse walk of an ordered trie the root's content is presented on the return path, i.e. the
        // cursor has to come back to a node that it did not enter by its own position.
        ByteBuffer vector = vector("01 01", 2,           // leaf 1                                       ("")
                                   "00 f8", 4);           // relay; starts 2 → 2
        assertWalks(vector,
                    List.of("=1"),
                    List.of("=1"),
                    List.of("", "↑=1"));
        assertReadsAsTrie(vector, "", 1);

        vector = vector("01 01", 2,                      // leaf 1                                       ("")
                        pad(3), 5,
                        "03 f8", 7);                     // relay; starts 5 → 2
        assertWalks(vector,
                    List.of("=1"),
                    List.of("=1"),
                    List.of("", "↑=1"));
        assertReadsAsTrie(vector, "", 1);

        // a root with content and children
        vector = vector("02 01", 2,                      // leaf 2                                       ("a")
                        "61 40", 4,                      // chain "a"; child at 3 - 1 = 2
                        "01 01 f5", 7,                   // prefix with content 1; child at 7 - 3 = 4    ("")
                        "00 f8", 9);                     // relay; starts 7 → 7
        assertWalks(vector,
                    List.of("=1", "a=2"),
                    List.of("=1", "a=2"),
                    List.of("", "a↑=2", "↑=1"));
        assertReadsAsTrie(vector, "", 1, "a", 2);

        // a root with children only
        vector = vector("01 01", 2,                      // leaf 1                                       ("a")
                        "02 01", 4,                      // leaf 2                                       ("b")
                        "02 f8", 6,                      // relay; starts 4 → 2
                        "02 62 61 80", 10,               // sparse {a, b}; starts 6: a → 6 (the relay), b → 6 - 2 = 4
                        pad(4), 14,
                        "04 00 00 fa", 18);              // relay, three-byte pointer; starts 14 → 10
        assertWalks(vector,
                    List.of("", "a=1", "b=2"),
                    List.of("", "b=2", "a=1"),
                    List.of("", "b↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "b", 2);
    }

    /// A trie in which no node is where the node that refers to it expects it to be.
    @Test
    public void testRelaysEverywhere() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = relaysEverywhere();
        assertWalks(vector,
                    List.of("", "a=1", "ac", "acd=2", "ax=3", "b=4", "by", "byz=5", "c", "cq=6"),
                    List.of("", "c", "cq=6", "b=4", "by", "byz=5", "a=1", "ax=3", "ac", "acd=2"),
                    List.of("", "c", "cq↑=6", "b", "by", "byz↑=5", "b↑=4", "a", "ax↑=3", "ac", "acd↑=2", "a↑=1"));
        assertReadsAsTrie(vector, "a", 1, "acd", 2, "ax", 3, "b", 4, "byz", 5, "cq", 6);
    }

    private static ByteBuffer relaysEverywhere()
    {
        return vector("02 01", 2,                 // leaf 2                                                ("acd")
                      pad(1), 3,
                      "01 f8", 5,                 // relay 1; starts 3 → 2 (leaf 2)
                      "64 40", 7,                 // chain "d"; child at 6 - 1 = 5 (relay 1)               ("ac")
                      "03 01", 9,                 // leaf 3                                                ("ax")
                      "00 f8", 11,                // relay 2; starts 9 → 9 (leaf 3)
                      "04 f8", 13,                // relay 3; starts 11 → 7 (chain "d")
                      "02 78 63 80", 17,          // sparse {c, x}; starts 13:
                                                  //   c → 13 (relay 3), x → 13 - 2 = 11 (relay 2)
                      pad(2), 19,
                      "02 f8", 21,                // relay 4; starts 19 → 17 (sparse {c, x})
                      "01 01 f5", 24,             // prefix with content 1; child at 24 - 3 = 21 (relay 4)  ("a")
                      "05 01", 26,                // leaf 5                                                ("byz")
                      "7a 79 41", 29,             // chain "yz"; child at 28 - 2 = 26
                      "00 f8", 31,                // relay 5; starts 29 → 29 (chain "yz")
                      "04 01 f5", 34,             // prefix with content 4; child at 34 - 3 = 31 (relay 5)  ("b")
                      "00 f8", 36,                // relay 6; starts 34 → 34 (prefix "b")
                      "02 f8", 38,                // relay 7; starts 36 → 36 (as relay 6)
                      "06 01 f4", 41,             // content 6 as a generic content node without a child   ("cq")
                      pad(3), 44,
                      "03 f8", 46,                // relay 8; starts 44 → 41 (content 6)
                      "71 40", 48,                // chain "q"; child at 47 - 1 = 46 (relay 8)             ("c")
                      "18 f8", 50,                // relay 9; starts 48 → 48 - 0x18 = 24 (prefix "a")
                      "02 0c 63 62 61 84", 56,    // sparse {a, b, c}; starts 50:
                                                  //   a → 50 (relay 9)
                                                  //   b → 50 - 0x0c = 38 (relay 7 → relay 6 → prefix "b")
                                                  //   c → 50 - 2 = 48 (chain "q")
                      "00 f8", 58);               // relay 10; starts 56 → 56 (the root)
    }

    /// The pointer of a relay takes one to eight bytes, as given by the low bits of its code, of which the first
    /// in the file is the least significant.
    @Test
    public void testRelayPointerWidths() throws TrieSpaceExhaustedException
    {
        for (int width = 1; width <= 8; ++width)
        {
            String code = String.format("%02x", 0xf8 | (width - 1));

            // a distance that fits one byte
            ByteBuffer vector = vector("07 01", 2,                                          // leaf 7
                                       pad(3), 5,
                                       join("03", zeros(width - 1), code), 6 + width);      // relay; starts 5 → 2
            assertWalks(vector, List.of("=7"), List.of("=7"), List.of("", "↑=7"));
            assertReadsAsTrie(vector, "", 7);

            // a distance that takes two: the padding is 0x0102 bytes long
            if (width >= 2)
            {
                vector = vector("07 01", 2,
                                pad(0x0102), 0x0104,
                                join("02 01", zeros(width - 2), code), 0x0105 + width);     // starts 0x0104 → 2
                assertWalks(vector, List.of("=7"), List.of("=7"), List.of("", "↑=7"));
                assertReadsAsTrie(vector, "", 7);
            }

            // and one that takes three
            if (width >= 3)
            {
                vector = vector("07 01", 2,
                                pad(0x010203), 0x010205,
                                join("03 02 01", zeros(width - 3), code), 0x010206 + width);  // starts 0x010205 → 2
                assertWalks(vector, List.of("=7"), List.of("=7"), List.of("", "↑=7"));
                assertReadsAsTrie(vector, "", 7);
            }

            // The bytes above that cannot be given a value that leads somewhere without a vector of many megabytes.
            // Show that they are taken into account by making them lead out of the data instead.
            for (int high = 3; high < width; ++high)
            {
                byte[] pointer = new byte[width];
                pointer[high] = 1;
                ByteBuffer corrupt = vector("07 01", 2,
                                            pad(3), 5,
                                            join(pointer, code), 6 + width);
                assertRejectedAsCorruptRelay("relay of " + width + " bytes with 1 in byte " + high,
                                             () -> plainCursor(corrupt, Direction.FORWARD, false));
            }
        }
    }

    /// A pointer that ends less than eight bytes into a buffer cannot be read as part of a long.
    @Test
    public void testRelayInFirstBytesOfBuffer() throws TrieSpaceExhaustedException
    {
        for (int width = 1; width <= 5; ++width)
        {
            String code = String.format("%02x", 0xf8 | (width - 1));

            // The relay's code is at 2 + width, i.e. no further than 7.
            ByteBuffer vector = vector("07 01", 2,                                  // leaf 7
                                       join(zeros(width), code), 3 + width);        // relay; starts 2 → 2
            assertWalks(vector, List.of("=7"), List.of("=7"), List.of("", "↑=7"));
            assertReadsAsTrie(vector, "", 7);

            if (width == 5)
                continue;
            vector = vector("07 01", 2,                                             // leaf 7
                            pad(1), 3,
                            join("01", zeros(width - 1), code), 4 + width);         // relay; starts 3 → 2
            assertWalks(vector, List.of("=7"), List.of("=7"), List.of("", "↑=7"));
            assertReadsAsTrie(vector, "", 7);
        }

        // The order of the bytes: read the other way around, this pointer is 0x0100 and leads out of the data, and
        // read as nothing it leads into the padding.
        ByteBuffer vector = vector("05 01", 2,           // leaf 5
                                   "06 01", 4,           // leaf 6
                                   pad(1), 5,
                                   "01 00 f9", 8);       // relay, two-byte pointer; starts 5 → 5 - 0x0001 = 4
        assertWalks(vector, List.of("=6"), List.of("=6"), List.of("", "↑=6"));
        assertReadsAsTrie(vector, "", 6);
    }

    // ---- Relays: range tries -------------------------------------------------------------------------------------

    /// A range trie whose root is given as a relay, where the root has content on both sides and the chain it
    /// augments is reached through one relay and leads to another.
    @Test
    public void testRangeTrieRootThroughRelay()
    {
        ByteBuffer vector = rangeRootThroughRelay();
        assertRangeWalks(vector,
                         List.of("=X<1", "a", "ab=1<X<2", "↑=2<X"),
                         List.of("=2<X", "a", "ab↑=1<X<2", "↑=X<1"));
        assertReadsAsRangeTrie(vector,
                               before("", -1, 1),
                               before("ab", 1, 2),
                               after("", 2, -1));
    }

    private static ByteBuffer rangeRootThroughRelay()
    {
        return vector("01 02 02", 3,                  // leaf 1<X<2                                        ("ab")
                      pad(1), 4,
                      "01 f8", 6,                     // relay 1; starts 4 → 3
                      "62 61 41", 9,                  // chain "ab"; child at 8 - 2 = 6 (relay 1)
                      pad(2), 11,
                      "02 f8", 13,                    // relay 2; starts 11 → 9
                      "02 ff 02 ff 01 02 f7", 20,     // prefix with ascent content 2<X, descent content X<1 (each
                                                      //   followed by its length) and a child, which is at
                                                      //   20 - 7 = 13 (relay 2)                           ("")
                      "00 f8", 22);                   // relay 3; starts 20 → 20
    }

    /// After a skip, a range cursor does not know which range covers the position it arrived at, and finds out when
    /// asked by descending to the nearest content. This is a walk of its own which does not move the cursor, and
    /// has to get through the relays just the same.
    @Test
    public void testRangeStateAfterSkipToMiddleOfChainBeforeRelay()
    {
        ByteBuffer vector = rangeRootThroughRelay();

        // Forward, the nearest content after "a" is the boundary at "ab", whose left side applies.
        OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
        long position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'a'));
        assertEquals(1, Cursor.depth(position));
        assertNull(cursor.content());
        assertCovering(1, cursor.state());

        // In reverse it is the right side.
        cursor = rangeCursor(vector, Direction.REVERSE);
        position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'a'));
        assertEquals(1, Cursor.depth(position));
        assertNull(cursor.content());
        assertCovering(2, cursor.state());
    }

    /// A range trie with a relay in front of every node that can have one.
    @Test
    public void testRangeTrieThroughRelays()
    {
        ByteBuffer vector = rangeRelaysEverywhere();
        assertRangeWalks(vector,
                         List.of("", "a=X<1", "c", "cd=1<X<2", "cg", "cgh=2<X<3", "cgj↑=3<X<4", "c↑=4<X<5",
                                 "e", "ef", "efg↑=5<X"),
                         List.of("", "e", "ef", "efg=5<X", "c=4<X<5", "cg", "cgj=3<X<4", "cgh↑=2<X<3", "cd↑=1<X<2",
                                 "a↑=X<1"));
        assertReadsAsRangeTrie(vector,
                               before("a", -1, 1),
                               before("cd", 1, 2),
                               before("cgh", 2, 3),
                               after("cgj", 3, 4),
                               after("c", 4, 5),
                               after("efg", 5, -1));
    }

    private static ByteBuffer rangeRelaysEverywhere()
    {
        return vector("ff 01 02", 3,              // leaf X<1                                              ("a")
                      "01 02 02", 6,              // leaf 1<X<2                                            ("cd")
                      "02 03 02", 9,              // leaf 2<X<3                                            ("cgh")
                      "03 04 02 f2", 13,          // ascent content 3<X<4 and its length, no child         ("cgj")
                      "05 ff 02 f2", 17,          // ascent content 5<X, no child                          ("efg")
                      pad(2), 19,
                      "02 f8", 21,                // relay 1; starts 19 → 17 (content "efg")
                      "67 66 41", 24,             // chain "fg"; child at 23 - 2 = 21 (relay 1)            ("e")
                      "00 f8", 26,                // relay 2; starts 24 → 24 (chain "fg")
                      "0d f8", 28,                // relay 3; starts 26 → 26 - 0x0d = 13 (content "cgj")
                      "0f f8", 30,                // relay 4; starts 28 → 13 (as relay 3)
                      "15 f8", 32,                // relay 5; starts 30 → 30 - 0x15 = 9 (leaf "cgh")
                      "02 6a 68 80", 36,          // sparse {h, j}; starts 32:                             ("cg")
                                                  //   h → 32 (relay 5), j → 32 - 2 = 30 (relay 4)
                      pad(1), 37,
                      "1f f8", 39,                // relay 6; starts 37 → 37 - 0x1f = 6 (leaf "cd")
                      "03 67 64 80", 43,          // sparse {d, g}; starts 39:
                                                  //   d → 39 (relay 6), g → 39 - 3 = 36 (sparse {h, j})
                      pad(3), 46,
                      "03 f8", 48,                // relay 7; starts 46 → 43 (sparse {d, g})
                      "04 05 02 f3", 52,          // prefix with ascent content 4<X<5 and a child, which is at
                                                  //   52 - 4 = 48 (relay 7)                               ("c")
                      "00 f8", 54,                // relay 8; starts 52 → 52 (prefix "c")
                      "33 f8", 56,                // relay 9; starts 54 → 54 - 0x33 = 3 (leaf "a")
                      "1e 02 65 63 61 84", 62,    // sparse {a, c, e}; starts 56:
                                                  //   a → 56 (relay 9)
                                                  //   c → 56 - 2 = 54 (relay 8)
                                                  //   e → 56 - 0x1e = 26 (relay 2)
                      "00 f8", 64);               // relay 10; starts 62 → 62 (the root)
    }

    /// The descents to the nearest content (see [#testRangeStateAfterSkipToMiddleOfChainBeforeRelay]) through the
    /// relays in front of the node a prefix augments, of the first and the last child of a sparse node, and of the
    /// node a chain leads to.
    @Test
    public void testRangeStateAfterSkipThroughRelays()
    {
        ByteBuffer vector = rangeRelaysEverywhere();

        // Forward, skipping the (missing) "b" lands on "c", a prefix node reached through relay 8 whose only
        // content is on the ascent side. The nearest content is that of "cd", found through relays 7 and 6.
        OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
        long position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'b'));
        assertEquals(1, Cursor.depth(position));
        assertEquals('c', Cursor.incomingTransition(position));
        assertNull(cursor.content());
        assertCovering(1, cursor.state());

        // A tail taken here presents the range it is entered in and the one it is left in on its root.
        assertEquals(List.of("=X<1", "d=1<X<2", "g", "gh=2<X<3", "gj↑=3<X<4", "↑=4<X"),
                     walkRange(cursor.tailCursor(Direction.FORWARD)));
        assertEquals(List.of("=4<X", "g", "gj=3<X<4", "gh↑=2<X<3", "d↑=1<X<2", "↑=X<1"),
                     walkRange(cursor.tailCursor(Direction.REVERSE)));

        // From there, skipping "ce" lands on "cg", whose first child is behind relay 5.
        position = cursor.skipTo(Cursor.positionForDescentWithByte(position, 'e'));
        assertEquals(2, Cursor.depth(position));
        assertEquals('g', Cursor.incomingTransition(position));
        assertNull(cursor.content());
        assertCovering(2, cursor.state());
        assertEquals(List.of("=X<2", "h=2<X<3", "j↑=3<X<4", "↑=4<X"), walkRange(cursor.tailCursor(Direction.FORWARD)));

        // Forward again, skipping "d" lands on "e", the start of a chain reached through relay 2. The content it
        // leads to through relay 1 is on the ascent side, and is nevertheless the nearest.
        cursor = rangeCursor(vector, Direction.FORWARD);
        position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'd'));
        assertEquals(1, Cursor.depth(position));
        assertEquals('e', Cursor.incomingTransition(position));
        assertNull(cursor.content());
        assertCovering(5, cursor.state());
        // and the same from the middle of the chain
        position = cursor.skipTo(Cursor.positionForDescentWithByte(position, 'f'));
        assertEquals(2, Cursor.depth(position));
        assertNull(cursor.content());
        assertCovering(5, cursor.state());

        // In reverse, skipping "f" lands on "e", and what is active there is what follows the end of the last range.
        cursor = rangeCursor(vector, Direction.REVERSE);
        position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'f'));
        assertEquals(1, Cursor.depth(position));
        assertEquals('e', Cursor.incomingTransition(position));
        assertNull(cursor.content());
        assertNull(cursor.state());

        // In reverse, skipping "d" lands on "c", which presents its ascent content right away.
        cursor = rangeCursor(vector, Direction.REVERSE);
        position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'd'));
        assertEquals(1, Cursor.depth(position));
        assertEquals('c', Cursor.incomingTransition(position));
        assertEquals("4<X<5", cursor.content().toStringNoPosition());
        // From there, skipping "ci" lands on "cg", whose last child is pointed to explicitly and found through
        // relays 4 and 3. The right side of the boundary there is what applies.
        position = cursor.skipTo(Cursor.positionForDescentWithByte(position, 'i'));
        assertEquals(2, Cursor.depth(position));
        assertEquals('g', Cursor.incomingTransition(position));
        assertNull(cursor.content());
        assertCovering(4, cursor.state());
        assertEquals(List.of("=4<X", "j=3<X<4", "h↑=2<X<3", "↑=X<2"), walkRange(cursor.tailCursor(Direction.REVERSE)));
    }

    /// The descents to the nearest content from a bitmap and from a dense node whose children are found through
    /// relays: the first child of the bitmap node, which is implicit, and the others through pointers.
    @Test
    public void testRangeStateThroughRelaysBelowBitmapAndDenseNodes()
    {
        // The bitmap is the one of testBitmapFirstChildThroughRelay.
        ByteBuffer bitmap = vector("03 ff 02 f2", 4,          // ascent content 3<X, no child                ("n")
                                   "02 03 02 f2", 8,          // ascent content 2<X<3, no child              ("mb")
                                   "01 02 02", 11,            // leaf 1<X<2                                  ("ma")
                                   "ff 01 02", 14,            // leaf X<1                                    ("k")
                                   "06 f8", 16,               // relay 1; starts 14 → 14 - 6 = 8 (content "mb")
                                   "05 f8", 18,               // relay 2; starts 16 → 16 - 5 = 11 (leaf "ma")
                                   join("02",                 // bitmap {a, b}, one-byte pointers; starts 18:  ("m")
                                        zeros(19),            //   a → 18 (relay 2), b → 18 - 2 = 16 (relay 1)
                                        "06",
                                        zeros(12),
                                        "e0"), 52,
                                   "00 f8", 54,               // relay 3; starts 52 → 52 (the bitmap)
                                   "28 f8", 56,               // relay 4; starts 54 → 54 - 0x28 = 14 (leaf "k")
                                   "34 02 6e 6d 6b 84", 62);  // sparse {k, m, n}; starts 56:
                                                              //   k → 56 (relay 4)
                                                              //   m → 56 - 2 = 54 (relay 3)
                                                              //   n → 56 - 0x34 = 4

        // The dense node is the one of testDensePointersToRelays. It makes the distances take two bytes.
        ByteBuffer dense = vector("03 ff 02 f2", 4,           // ascent content 3<X, no child                ("n")
                                  "02 03 02 f2", 8,           // ascent content 2<X<3, no child              ("mb")
                                  "01 02 02", 11,             // leaf 1<X<2                                  ("ma")
                                  "ff 01 02", 14,             // leaf X<1                                    ("k")
                                  "06 f8", 16,                // relay 1; starts 14 → 14 - 6 = 8 (content "mb")
                                  "05 f8", 18,                // relay 2; starts 16 → 16 - 5 = 11 (leaf "ma")
                                  join(pad(157),              // dense, one-byte pointers, ff for no child;
                                       "02 00",               //   starts 18:                                ("m")
                                       pad(97),               //   a → 18 - 0 = 18 (relay 2)
                                       "e8"), 275,            //   b → 18 - 2 = 16 (relay 1)
                                  "00 f8", 277,               // relay 3; starts 275 → 275 (the dense node)
                                  "07 01 f9", 280,            // relay 4, two-byte pointer;
                                                              //   starts 277 → 277 - 0x0107 = 14 (leaf "k")
                                  "14 01 03 00 6e 6d 6b 85", 288);    // sparse {k, m, n}, two-byte pointers; starts 280:
                                                              //   k → 280 (relay 4)
                                                              //   m → 280 - 0x0003 = 277 (relay 3)
                                                              //   n → 280 - 0x0114 = 4

        for (ByteBuffer vector : List.of(bitmap, dense))
        {
            String context = vector == bitmap ? "bitmap" : "dense";
            assertRangeWalks(vector,
                             List.of("", "k=X<1", "m", "ma=1<X<2", "mb↑=2<X<3", "n↑=3<X"),
                             List.of("", "n=3<X", "m", "mb=2<X<3", "ma↑=1<X<2", "k↑=X<1"));

            // Forward, skipping the (missing) "l" lands on "m", where the nearest content is that of its first child.
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
            long position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'l'));
            assertEquals(context, 1, Cursor.depth(position));
            assertEquals(context, 'm', Cursor.incomingTransition(position));
            assertNull(context, cursor.content());
            assertCovering(1, cursor.state());
            assertEquals(context,
                         List.of("=X<1", "a=1<X<2", "b↑=2<X<3", "↑=3<X"),
                         walkRange(cursor.tailCursor(Direction.FORWARD)));

            // In reverse, skipping to "m" passes over "n", and the nearest content is that of the last child.
            cursor = rangeCursor(vector, Direction.REVERSE);
            position = cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'm'));
            assertEquals(context, 1, Cursor.depth(position));
            assertEquals(context, 'm', Cursor.incomingTransition(position));
            assertNull(context, cursor.content());
            assertCovering(3, cursor.state());
            assertEquals(context,
                         List.of("=3<X", "b=2<X<3", "a↑=1<X<2", "↑=X<1"),
                         walkRange(cursor.tailCursor(Direction.REVERSE)));

            assertReadsAsRangeTrie(vector,
                                   before("k", -1, 1),
                                   before("ma", 1, 2),
                                   after("mb", 2, 3),
                                   after("n", 3, -1));
        }
    }

    /// A cursor that has been closed can still be asked for a tail of the position it was closed on. A plain cursor
    /// has nothing to read for that. A range cursor has to find out, while it still can, which ranges the tail is
    /// entered and left in, i.e. [OnDiskRangeCursor#close] makes the descents to the nearest content, and has to
    /// get through the relays like they do.
    @Test
    public void testTailAfterCloseThroughRelays()
    {
        ByteBuffer plain = relaysEverywhere();
        Set<String> plainProbes = probes(List.of("a", "acd", "ax", "b", "byz", "cq"));
        ByteBuffer range = rangeRelaysEverywhere();
        Set<String> rangeProbes = probes(List.of("a", "cd", "cgh", "cgj", "c", "efg"));

        for (Direction direction : Direction.values())
            for (Direction tailDirection : Direction.values())
            {
                String context = tailDirection + " tail of closed " + direction + " cursor at ";
                for (boolean ordered : new boolean[]{ false, true })
                    for (String probe : plainProbes)
                        assertSameTailAfterClose(() -> plainCursor(plain, direction, ordered),
                                                 probe,
                                                 tailDirection,
                                                 String::valueOf,
                                                 (ordered ? "ordered, " : "unordered, ") + context);

                for (String probe : rangeProbes)
                    assertSameTailAfterClose(() -> rangeCursor(range, direction),
                                             probe,
                                             tailDirection,
                                             TestRangeState::toStringNoPosition,
                                             "range, " + context);
            }
    }

    /// Takes two cursors to where a descent along the given key ends up, see [#descendAlong], and checks that the
    /// tail of the one that is closed there is the tail of the one that is not.
    @SuppressWarnings("unchecked")
    private static <T> void assertSameTailAfterClose(Supplier<OnDiskCursor<T>> cursors,
                                                     String probe,
                                                     Direction tailDirection,
                                                     Function<? super T, String> show,
                                                     String context)
    {
        OnDiskCursor<T> open = cursors.get();
        long position = descendAlong(open, probe);
        if (Cursor.isExhausted(position) || Cursor.isOnReturnPath(position))
            return;     // no tail can be taken here
        List<String> expected = walk((OnDiskCursor<T>) open.tailCursor(tailDirection), show);

        OnDiskCursor<T> closed = cursors.get();
        assertEquals(context + '"' + probe + '"', position, descendAlong(closed, probe));
        closed.close();
        assertEquals(context + '"' + probe + '"',
                     expected,
                     walk((OnDiskCursor<T>) closed.tailCursor(tailDirection), show));
    }

    // ---- Relays: deletion-aware tries ------------------------------------------------------------------------------

    /// A node of a deletion-aware trie that has a deletion branch carries its position in the ascent-side content
    /// slot of its prefix node. The pointer is not read when the node is entered, but on demand, from the node that
    /// the cursor knows itself to be on -- which has to be the prefix node also when that was found through a relay.
    @Test
    public void testDeletionBranchOfNodeEnteredThroughRelay() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("05 ff 02 f2", 4,         // ascent content 5<X, no child               (branch "y")
                                   "ff 05 02", 7,            // leaf X<5                                   (branch "x")
                                   "03 79 78 80", 11,        // sparse {x, y}; starts 7: x → 7, y → 7 - 3 = 4
                                                             //   the root of the branch: 11 = 0x0b
                                   "02 01", 13,              // leaf 2                                     ("ab")
                                   "62 40", 15,              // chain "b"; child at 14 - 1 = 13
                                   "04 01 01 01 f7", 20,     // prefix with the pointer to the branch root in the ascent
                                                             //   slot (starts 15 → 15 - 4 = 11), content 1 (each
                                                             //   followed by its length) and a child, which is at
                                                             //   20 - 5 = 15                              ("a")
                                   "03 01", 22,              // leaf 3                                     ("c")
                                   pad(1), 23,
                                   "03 f8", 25,              // relay; starts 23 → 20 (prefix "a")
                                   "03 63 61 80", 29);       // sparse {a, c}; starts 25:
                                                             //   a → 25 (the relay), c → 25 - 3 = 22
        for (Direction direction : Direction.values())
            assertEquals(direction.toString(),
                         direction.isForward() ? List.of("", "a=1 branch at 11", "ab=2", "c=3")
                                               : List.of("", "c=3", "a=1 branch at 11", "ab=2"),
                         walkWithBranches(dataCursor(vector, direction)));

        ExpectedDeletionAwareTrie expected = new ExpectedDeletionAwareTrie()
                                             .live("a", 1)
                                             .live("ab", 2)
                                             .live("c", 3)
                                             .branch("a", before("x", -1, 5), after("y", 5, -1));
        assertReadsAsDeletionAwareTrie(vector, expected);
    }

    /// The root of a deletion-aware trie given as a relay, with a deletion branch whose root is given as a relay too.
    @Test
    public void testDeletionBranchAtRootThroughRelays() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector("07 ff 02 f2", 4,         // ascent content 7<X, no child               (branch "n")
                                   "ff 07 02", 7,            // leaf X<7                                   (branch "m")
                                   "03 6e 6d 80", 11,        // sparse {m, n}; starts 7: m → 7, n → 7 - 3 = 4
                                   pad(2), 13,
                                   "02 f8", 15,              // relay; starts 13 → 11
                                                             //   the root of the branch, as recorded: 15 = 0x0f
                                   "02 01", 17,              // leaf 2                                     ("a")
                                   "61 40", 19,              // chain "a"; child at 18 - 1 = 17
                                   "04 01 01 01 f7", 24,     // prefix with the pointer to the branch root (starts
                                                             //   19 → 19 - 4 = 15), content 1 and a child, which is
                                                             //   at 24 - 5 = 19                           ("")
                                   "00 f8", 26);             // relay; starts 24 → 24
        for (Direction direction : Direction.values())
            assertEquals(direction.toString(),
                         List.of("=1 branch at 15", "a=2"),
                         walkWithBranches(dataCursor(vector, direction)));

        ExpectedDeletionAwareTrie expected = new ExpectedDeletionAwareTrie()
                                             .live("", 1)
                                             .live("a", 2)
                                             .branch("", before("m", -1, 7), after("n", 7, -1));
        assertReadsAsDeletionAwareTrie(vector, expected);
    }

    /// Nodes with deletion branches reached through a relay in every way a node can be: as the first child of a
    /// sparse node, through a pointer that leads to a relay that leads to a relay, and from a chain; with and without
    /// content and children; with a branch whose root is a relay, one that has relays inside it, and one without any.
    @Test
    public void testDeletionBranchesThroughRelays() throws TrieSpaceExhaustedException
    {
        ByteBuffer vector = vector(// the branch of "a"
                                   "05 ff 02 f2", 4,         // ascent content 5<X, no child               (branch "y")
                                   "ff 05 02", 7,            // leaf X<5                                   (branch "x")
                                   pad(1), 8,
                                   "01 f8", 10,              // relay 1; starts 8 → 7 (leaf "x")
                                   "06 79 78 80", 14,        // sparse {x, y}; starts 10:
                                                             //   x → 10 (relay 1), y → 10 - 6 = 4
                                   "00 f8", 16,              // relay 2; starts 14 → 14
                                                             //   the root of the branch, as recorded: 16 = 0x10
                                   // the branch of "c"
                                   "06 ff 02 f2", 20,        // ascent content 6<X, no child               (branch "q")
                                   "ff 06 02", 23,           // leaf X<6                                   (branch "p")
                                   "03 71 70 80", 27,        // sparse {p, q}; starts 23: p → 23, q → 23 - 3 = 20
                                                             //   the root of the branch: 27 = 0x1b
                                   // the branch of "ef"
                                   "03 ff 02 ff 03 02 f6", 34,   // ascent content 3<X, descent content X<3,
                                                             //   no child                                 (branch "k")
                                   pad(2), 36,
                                   "02 f8", 38,              // relay 3; starts 36 → 34
                                   "6b 40", 40,              // chain "k"; child at 39 - 1 = 38 (relay 3)
                                                             //   the root of the branch: 40 = 0x28
                                   // the data
                                   "02 01", 42,              // leaf 2                                     ("ab")
                                   "62 40", 44,              // chain "b"; child at 43 - 1 = 42
                                   "00 f8", 46,              // relay 4; starts 44 → 44 (chain "b")
                                   "1e 01 01 01 f7", 51,     // prefix with the pointer to the branch root (starts
                                                             //   46 → 46 - 0x1e = 16), content 1 and a child, which
                                                             //   is at 51 - 5 = 46 (relay 4)              ("a")
                                   "18 01 f2", 54,           // prefix with the pointer to the branch root only
                                                             //   (starts 51 → 51 - 0x18 = 27)             ("c")
                                   "00 f8", 56,              // relay 5; starts 54 → 54 (prefix "c")
                                   "02 f8", 58,              // relay 6; starts 56 → 56 (relay 5)
                                   "12 01 05 01 f6", 63,     // prefix with the pointer to the branch root (starts
                                                             //   58 → 58 - 0x12 = 40) and content 5, no child
                                                             //                                            ("ef")
                                   pad(3), 66,
                                   "03 f8", 68,              // relay 7; starts 66 → 63 (prefix "ef")
                                   "66 40", 70,              // chain "f"; child at 69 - 1 = 68 (relay 7)  ("e")
                                   "09 01", 72,              // leaf 9                                     ("g")
                                   "15 f8", 74,              // relay 8; starts 72 → 72 - 0x15 = 51 (prefix "a")
                                   "02 04 10 67 65 63 61 88", 82);   // sparse {a, c, e, g}; starts 74:
                                                             //   a → 74 (relay 8)
                                                             //   c → 74 - 0x10 = 58 (relay 6 → prefix "c")
                                                             //   e → 74 - 4 = 70 (chain "f")
                                                             //   g → 74 - 2 = 72
        for (Direction direction : Direction.values())
            assertEquals(direction.toString(),
                         direction.isForward()
                         ? List.of("", "a=1 branch at 16", "ab=2", "c branch at 27", "e", "ef=5 branch at 40", "g=9")
                         : List.of("", "g=9", "e", "ef=5 branch at 40", "c branch at 27", "a=1 branch at 16", "ab=2"),
                         walkWithBranches(dataCursor(vector, direction)));

        ExpectedDeletionAwareTrie expected = new ExpectedDeletionAwareTrie()
                                             .live("a", 1)
                                             .live("ab", 2)
                                             .live("ef", 5)
                                             .live("g", 9)
                                             .branch("a", before("x", -1, 5), after("y", 5, -1))
                                             .branch("c", before("p", -1, 6), after("q", 6, -1))
                                             .branch("ef", before("k", -1, 3), after("k", 3, -1));
        assertReadsAsDeletionAwareTrie(vector, expected);
    }

    // ---- Relays: corrupt data --------------------------------------------------------------------------------------

    /// A relay stands for a node placed before it. A pointer that leads to the start of the data or beyond it must
    /// be reported as corruption, like the bad lengths above, rather than become a position to read from.
    @Test
    public void testRelayTargetOutsideDataThrows()
    {
        // The target is before the start of the data: 0 - 5.
        assertRootRejectedAsCorruptRelay(vector("05 f8", 2), OUTSIDE);
        // The target is the start of the data, which is not the position of any node: 2 - 2, and 0 - 0.
        assertRootRejectedAsCorruptRelay(vector("01 01", 2,
                                                "02 f8", 4), OUTSIDE);
        assertRootRejectedAsCorruptRelay(vector("00 f8", 2), OUTSIDE);
        // With wider pointers: 8 - 0x0109, and 9 - 0x7fffffffffffffff.
        assertRootRejectedAsCorruptRelay(vector("01 01", 2,
                                                pad(6), 8,
                                                "09 01 f9", 11), OUTSIDE);
        assertRootRejectedAsCorruptRelay(vector("01 01", 2,
                                                pad(7), 9,
                                                "ff ff ff ff ff ff ff 7f ff", 18), OUTSIDE);
    }

    /// The pointer of a relay is unsigned, but eight bytes of it do not fit a positive long. A negative value read
    /// from them is a pointer to somewhere after the relay, which would let relays form a loop.
    @Test(timeout = 60000)
    public void testRelayPointerWithTopBitSetThrows()
    {
        // The smallest such: Long.MIN_VALUE.
        assertRootRejectedAsCorruptRelay(vector("01 01", 2,
                                                "00 00 00 00 00 00 00 80 ff", 11), OUTSIDE);
        // -9, which taken as a distance from the start of the relay at 2 gives 11, the relay itself.
        assertRootRejectedAsCorruptRelay(vector("01 01", 2,
                                                "f7 ff ff ff ff ff ff ff ff", 11), OUTSIDE);
        // This is also what padding with ff looks like.
        assertRootRejectedAsCorruptRelay(vector("01 01", 2,
                                                pad(9), 11), OUTSIDE);
    }

    /// A relay closer to the start of the data than the size of its pointer.
    @Test
    public void testRelayWithoutRoomForPointerThrows()
    {
        assertRootRejectedAsCorruptRelay(vector("f8", 1), NO_ROOM);
        assertRootRejectedAsCorruptRelay(vector("f9", 1), NO_ROOM);
        assertRootRejectedAsCorruptRelay(vector("00 f9", 2), NO_ROOM);
        assertRootRejectedAsCorruptRelay(vector("00 00 00 fb", 4), NO_ROOM);
        assertRootRejectedAsCorruptRelay(vector("01 01 00 00 00 00 00 ff", 8), NO_ROOM);
        assertRootRejectedAsCorruptRelay(vector(pad(8), 8), NO_ROOM);
        assertRootRejectedAsCorruptRelay(vector(pad(1), 1), NO_ROOM);
    }

    /// The same rejection in the other places where a relay can be met: below a node, behind a prefix node, on the
    /// descent to the nearest content of a range trie, and at the root of a deletion branch.
    @Test
    public void testCorruptRelayBelowRootThrows()
    {
        // as the child of a chain
        ByteBuffer chain = vector("05 f8", 2,            // relay; starts 0 → 0 - 5
                                  "61 40", 4);           // chain "a"; child at 3 - 1 = 2
        for (Direction direction : Direction.values())
        {
            OnDiskCursor<Integer> cursor = plainCursor(chain, direction, false);
            assertRejectedAsCorruptRelay(direction + " advance", cursor::advance);
            OnDiskCursor<Integer> skipping = plainCursor(chain, direction, false);
            assertRejectedAsCorruptRelay(direction + " skipTo",
                                         () -> skipping.skipTo(Cursor.positionForDescentWithByte(skipping.encodedPosition(), 'a')));
            OnDiskCursor<Integer> jumping = plainCursor(chain, direction, false);
            assertRejectedAsCorruptRelay(direction + " advanceMultiple", () -> jumping.advanceMultiple(null));
        }

        // as the first child of a sparse node
        ByteBuffer sparse = vector("01 01", 2,           // leaf 1                                       ("b")
                                   "7f f8", 4,           // relay; starts 2 → 2 - 0x7f
                                   "02 62 61 80", 8);    // sparse {a, b}; starts 4: a → 4 (the relay), b → 4 - 2 = 2
        OnDiskCursor<Integer> forward = plainCursor(sparse, Direction.FORWARD, false);
        assertRejectedAsCorruptRelay("forward advance", forward::advance);
        OnDiskCursor<Integer> reverse = plainCursor(sparse, Direction.REVERSE, false);
        reverse.advance();
        assertEquals(Integer.valueOf(1), reverse.content());
        assertRejectedAsCorruptRelay("reverse advance", reverse::advance);

        // as the node a prefix node augments
        ByteBuffer prefix = vector("09 f8", 2,           // relay; starts 0 → 0 - 9
                                   "01 01 f5", 5);       // prefix with content 1; child at 5 - 3 = 2
        for (Direction direction : Direction.values())
            for (boolean ordered : new boolean[]{ false, true })
                assertRejectedAsCorruptRelay(direction + " prefix", () -> plainCursor(prefix, direction, ordered));

        // on the descent to the nearest content
        ByteBuffer range = vector(pad(2), 2,
                                  "7f f8", 4,            // relay; starts 2 → 2 - 0x7f
                                  "62 61 41", 7);        // chain "ab"; child at 6 - 2 = 4
        for (Direction direction : Direction.values())
        {
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(range, direction);
            cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'a'));
            assertRejectedAsCorruptRelay(direction + " state", cursor::state);
            for (Direction tailDirection : Direction.values())
                assertRejectedAsCorruptRelay(direction + " " + tailDirection + " tail", () -> cursor.tailCursor(tailDirection));
        }

        // at the root of a deletion branch
        ByteBuffer branch = vector("05 f8", 2,           // relay; starts 0 → 0 - 5
                                   "00 f2", 4);          // prefix with an empty ascent slot, i.e. with its branch
                                                         //   root where it starts, at 2
        OnDiskDeletionAwareTrie<Integer, TestRangeState> trie = OnDiskDeletionAwareTrie.open(branch, INT_CONTENT, RANGE_CONTENT, VERSION, -1);
        for (Direction direction : Direction.values())
        {
            DeletionAwareCursor<Integer, TestRangeState> cursor = trie.makeCursor(direction);
            assertTrue((cursor.encodedPosition() & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0);
            for (Direction branchDirection : Direction.values())
                assertRejectedAsCorruptRelay(direction + " " + branchDirection + " branch",
                                             () -> cursor.deletionBranchCursor(branchDirection));
        }
    }

    // ---- Range tails where a range ends ----------------------------------------------------------------------------

    // The vectors of this section have no relays. They pin something the vectors above came across in the tails of
    // range cursors, which has nothing to do with relays: a boundary that ends a range _before_ the branch of its node
    // (or, seen from the other side, starts one _after_ it) has nothing to say about that branch, because no range is
    // active inside it. A tail taken on such a boundary must not present anything of it at its root, although the
    // boundary is the content that the root node of the tail has in the file.

    /// `p` starts a range and `q` ends it, both before their (empty) branches.
    private static ByteBuffer rangeEndingBeforeBranch()
    {
        return vector("06 ff 02", 3,              // leaf 6<X                                              ("q")
                      "ff 06 02", 6,              // leaf X<6                                              ("p")
                      "03 71 70 80", 10);         // sparse {p, q}; starts 6: p → 6, q → 6 - 3 = 3
    }

    /// `p` starts a range and `q` ends it, both after their (empty) branches.
    private static ByteBuffer rangeStartingAfterBranch()
    {
        return vector("06 ff 02 f2", 4,           // ascent content 6<X, no child                          ("q")
                      "ff 06 02 f2", 8,           // ascent content X<6, no child                          ("p")
                      "04 71 70 80", 12);         // sparse {p, q}; starts 8: p → 8, q → 8 - 4 = 4
    }

    /// As [#rangeEndingBeforeBranch], with another range inside the branch of `q`.
    private static ByteBuffer rangeEndingBeforeBranchWithChildren()
    {
        return vector("07 ff 02", 3,              // leaf 7<X                                              ("qs")
                      "ff 07 02", 6,              // leaf X<7                                              ("qr")
                      "03 73 72 80", 10,          // sparse {r, s}; starts 6: r → 6, s → 6 - 3 = 3
                      "06 ff 02 f5", 14,          // prefix with descent content 6<X and a child, which is at
                                                  //   14 - 4 = 10                                         ("q")
                      "ff 06 02", 17,             // leaf X<6                                              ("p")
                      "03 71 70 80", 21);         // sparse {p, q}; starts 17: p → 17, q → 17 - 3 = 14
    }

    /// As [#rangeStartingAfterBranch], with another range inside the branch of `p`.
    private static ByteBuffer rangeStartingAfterBranchWithChildren()
    {
        return vector("06 ff 02 f2", 4,           // ascent content 6<X, no child                          ("q")
                      "07 ff 02 f2", 8,           // ascent content 7<X, no child                          ("ps")
                      "ff 07 02", 11,             // leaf X<7                                              ("pr")
                      "03 73 72 80", 15,          // sparse {r, s}; starts 11: r → 11, s → 11 - 3 = 8
                      "ff 06 02 f3", 19,          // prefix with ascent content X<6 and a child, which is at
                                                  //   19 - 4 = 15                                         ("p")
                      "0f 71 70 80", 23);         // sparse {p, q}; starts 19: p → 19, q → 19 - 0x0f = 4
    }

    /// The root of a tail taken where a range ends, in the direction that the range ends in, has neither content
    /// nor a state: in particular, its state is not the boundary that the cursor it was taken from is on.
        @Test
    public void testTailRootWhereRangeEndsHasNoState()
    {
        ByteBuffer vector = rangeEndingBeforeBranch();
        assertRangeWalks(vector,
                         List.of("", "p=X<6", "q=6<X"),
                         List.of("", "q↑=6<X", "p↑=X<6"));
        OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
        cursor.advance();
        cursor.advance();
        assertEquals("6<X", cursor.content().toStringNoPosition());
        RangeCursor<TestRangeState> tail = cursor.tailCursor(Direction.FORWARD);
        assertNull(tail.content());
        assertNull(tail.state());
        assertTrue(Cursor.isExhausted(tail.advance()));

        // with something to walk in the tail
        vector = rangeEndingBeforeBranchWithChildren();
        assertRangeWalks(vector,
                         List.of("", "p=X<6", "q=6<X", "qr=X<7", "qs=7<X"),
                         List.of("", "q", "qs↑=7<X", "qr↑=X<7", "q↑=6<X", "p↑=X<6"));
        cursor = rangeCursor(vector, Direction.FORWARD);
        cursor.advance();
        cursor.advance();
        assertEquals("6<X", cursor.content().toStringNoPosition());
        tail = cursor.tailCursor(Direction.FORWARD);
        assertNull(tail.content());
        assertNull(tail.state());
        assertEquals(List.of("", "r=X<7", "s=7<X"), walkRange(tail));

        // In reverse, the boundary that ends a range is the one where it starts.
        vector = rangeStartingAfterBranch();
        assertRangeWalks(vector,
                         List.of("", "p↑=X<6", "q↑=6<X"),
                         List.of("", "q=6<X", "p=X<6"));
        cursor = rangeCursor(vector, Direction.REVERSE);
        cursor.advance();
        cursor.advance();
        assertEquals("X<6", cursor.content().toStringNoPosition());
        tail = cursor.tailCursor(Direction.REVERSE);
        assertNull(tail.content());
        assertNull(tail.state());
        assertTrue(Cursor.isExhausted(tail.advance()));
    }

    /// A tail taken at the root of a tail is the same branch again. The boundary that the root node has in the file
    /// is not part of it when the first tail did not present it.
        @Test
    public void testTailOfTailWhereRangeEnds()
    {
        // A reverse tail at "q" does not have the boundary, which is before the branch, and neither have its tails.
        ByteBuffer vector = rangeEndingBeforeBranch();
        for (Direction direction : Direction.values())
        {
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
            cursor.advance();
            cursor.advance();
            assertEquals("6<X", cursor.content().toStringNoPosition());
            RangeCursor<TestRangeState> tail = cursor.tailCursor(Direction.REVERSE);
            assertNull(tail.content());
            assertNull(tail.state());
            assertEquals(direction.toString(), List.of(""), walkRange(tail.tailCursor(direction)));
            assertTrue(Cursor.isExhausted(tail.advance()));
        }

        // with something to walk in the tails
        vector = rangeEndingBeforeBranchWithChildren();
        for (Direction direction : Direction.values())
        {
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
            cursor.advance();
            cursor.advance();
            assertEquals("6<X", cursor.content().toStringNoPosition());
            RangeCursor<TestRangeState> tail = cursor.tailCursor(Direction.REVERSE);
            assertEquals(direction.toString(),
                         direction.isForward() ? List.of("", "r=X<7", "s=7<X") : List.of("", "s↑=7<X", "r↑=X<7"),
                         walkRange(tail.tailCursor(direction)));
            assertEquals(List.of("", "s↑=7<X", "r↑=X<7"), walkRange(tail));
        }

        // The same from the other side: a forward tail at "p" does not have the boundary, which is after the branch.
        vector = rangeStartingAfterBranch();
        for (Direction direction : Direction.values())
        {
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.REVERSE);
            cursor.advance();
            cursor.advance();
            assertEquals("X<6", cursor.content().toStringNoPosition());
            RangeCursor<TestRangeState> tail = cursor.tailCursor(Direction.FORWARD);
            assertNull(tail.content());
            assertNull(tail.state());
            assertEquals(direction.toString(), List.of(""), walkRange(tail.tailCursor(direction)));
            assertTrue(Cursor.isExhausted(tail.advance()));
        }

        // And with nothing but forward cursors, as when a tail is taken of a branch and then of its root again to
        // walk it a second time: the range that starts after the branch of "p" is not one that is ended in it.
        vector = rangeStartingAfterBranchWithChildren();
        assertRangeWalks(vector,
                         List.of("", "p", "pr=X<7", "ps↑=7<X", "p↑=X<6", "q↑=6<X"),
                         List.of("", "q=6<X", "p=X<6", "ps=7<X", "pr↑=X<7"));
        for (Direction direction : Direction.values())
        {
            OnDiskRangeCursor<TestRangeState> cursor = rangeCursor(vector, Direction.FORWARD);
            cursor.advance();
            assertNull(cursor.content());
            RangeCursor<TestRangeState> tail = cursor.tailCursor(Direction.FORWARD);
            assertEquals(direction.toString(),
                         direction.isForward() ? List.of("", "r=X<7", "s↑=7<X") : List.of("", "s=7<X", "r↑=X<7"),
                         walkRange(tail.tailCursor(direction)));
            assertEquals(List.of("", "r=X<7", "s↑=7<X"), walkRange(tail));
        }
    }

    /// The tails, and the tails of tails, of every position of these tries, compared with the in-memory tries that
    /// hold the same.
        @Test
    public void testRangeTriesWhereRangesEndBeforeBranches()
    {
        assertReadsAsRangeTrie(rangeEndingBeforeBranch(),
                               before("p", -1, 6),
                               before("q", 6, -1));
        assertReadsAsRangeTrie(rangeEndingBeforeBranchWithChildren(),
                               before("p", -1, 6),
                               before("q", 6, -1),
                               before("qr", -1, 7),
                               before("qs", 7, -1));
        assertReadsAsRangeTrie(rangeStartingAfterBranch(),
                               after("p", -1, 6),
                               after("q", 6, -1));
        assertReadsAsRangeTrie(rangeStartingAfterBranchWithChildren(),
                               before("pr", -1, 7),
                               after("ps", 7, -1),
                               after("p", -1, 6),
                               after("q", 6, -1));
    }

    // ---- Vectors -------------------------------------------------------------------------------------------------

    /// Puts a hand-written vector together. The arguments alternate between a piece of the vector, given as a string
    /// of hex bytes or as a byte array, and the position that the piece must end at.
    private static ByteBuffer vector(Object... piecesAndEnds)
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < piecesAndEnds.length; i += 2)
        {
            byte[] piece = bytes(piecesAndEnds[i]);
            bytes.write(piece, 0, piece.length);
            assertEquals("End of piece " + i / 2 + " of the vector, " + describe(piece),
                         ((Number) piecesAndEnds[i + 1]).longValue(),
                         bytes.size());
        }
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private static byte[] bytes(Object piece)
    {
        if (piece instanceof byte[])
            return (byte[]) piece;

        String hex = ((String) piece).trim();
        if (hex.isEmpty())
            return new byte[0];
        String[] hexBytes = hex.split("\\s+");
        byte[] bytes = new byte[hexBytes.length];
        for (int i = 0; i < bytes.length; ++i)
        {
            assertEquals("Hex byte " + hexBytes[i], 2, hexBytes[i].length());
            bytes[i] = (byte) Integer.parseInt(hexBytes[i], 16);
        }
        return bytes;
    }

    private static String describe(byte[] piece)
    {
        StringBuilder description = new StringBuilder();
        for (int i = 0; i < Math.min(piece.length, 16); ++i)
            description.append(String.format("%02x ", piece[i]));
        return description + (piece.length > 16 ? "... (" + piece.length + " bytes)" : "");
    }

    /// The concatenation of the given pieces, each a string of hex bytes or a byte array.
    private static byte[] join(Object... pieces)
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (Object piece : pieces)
        {
            byte[] pieceBytes = bytes(piece);
            bytes.write(pieceBytes, 0, pieceBytes.length);
        }
        return bytes.toByteArray();
    }

    private static byte[] repeat(String hex, int count)
    {
        byte[] piece = bytes(hex);
        byte[] bytes = new byte[piece.length * count];
        for (int i = 0; i < count; ++i)
            System.arraycopy(piece, 0, bytes, i * piece.length, piece.length);
        return bytes;
    }

    /// Bytes that no pointer leads to. A reader that takes one of them for a node sees a relay with an eight-byte
    /// pointer, which it cannot follow (see [#testRelayPointerWithTopBitSetThrows]).
    private static byte[] pad(int count)
    {
        byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) 0xFF);
        return bytes;
    }

    private static byte[] zeros(int count)
    {
        return new byte[count];
    }

    /// The content of the plain vectors: the bytes of the content as an unsigned number, most significant first.
    private static final OnDiskCursor.DataDeserializer<Integer> INT_CONTENT = (rdr, length) ->
    {
        int value = 0;
        for (int i = 0; i < length; ++i)
            value = (value << 8) | rdr.readUnsignedByte();
        return value;
    };

    /// The content of the range vectors: two signed bytes, the left and the right side of a [TestRangeState], with
    /// -1 (`ff`) for a side that no range applies to.
    private static final OnDiskCursor.DataDeserializer<TestRangeState> RANGE_CONTENT = (rdr, length) ->
    {
        if (length != 2)
            throw new IOException("Range content of " + length + " bytes");
        int left = rdr.readByte();
        int right = rdr.readByte();
        return new TestRangeState(ByteComparable.EMPTY, left, right);
    };

    private static TestRangeState before(String key, int left, int right)
    {
        return new TestRangeState(TrieUtil.directComparable(key), false, left, right);
    }

    private static TestRangeState after(String key, int left, int right)
    {
        return new TestRangeState(TrieUtil.directComparable(key), true, left, right);
    }

    private static String key(TestRangeState marker)
    {
        byte[] bytes = ByteSourceInverse.readBytes(marker.position.asComparableBytes(VERSION));
        StringBuilder key = new StringBuilder();
        for (byte b : bytes)
            key.append((char) (b & 0xFF));
        return key.toString();
    }

    private static ByteSource source(String key)
    {
        return TrieUtil.directComparable(key).asComparableBytes(VERSION);
    }

    // ---- Cursors over the vectors, and what they are expected to present ---------------------------------------------

    private static OnDiskCursor<Integer> plainCursor(ByteBuffer vector, Direction direction, boolean ordered)
    {
        return new OnDiskCursor<>(INT_CONTENT, new ByteBufferRebufferer(vector), VERSION, direction, ordered ? OnDiskCursor.ContentStyle.ORDERED : OnDiskCursor.ContentStyle.UNORDERED, vector.limit());
    }

    private static OnDiskRangeCursor<TestRangeState> rangeCursor(ByteBuffer vector, Direction direction)
    {
        return new OnDiskRangeCursor<>(RANGE_CONTENT, new ByteBufferRebufferer(vector), VERSION, direction, vector.limit());
    }

    /// A cursor over the data trie of a deletion-aware trie, as [OnDiskDeletionAwareTrie] makes it.
    private static OnDiskCursor<Integer> dataCursor(ByteBuffer vector, Direction direction)
    {
        return new OnDiskCursor<>(INT_CONTENT, new ByteBufferRebufferer(vector), VERSION, direction, OnDiskCursor.ContentStyle.ALTERNATE_BRANCH, vector.limit());
    }

    /// Relays are followed before a node is entered, so that no part of the state of a cursor ever describes one:
    /// neither the node it is on, nor the one whose children it is iterating, nor the ones it will come back to.
    private static void assertNotOnRelay(OnDiskCursor<?> cursor)
    {
        assertFalse("On a relay", OnDiskReadNodeType.selectNodeImpl(cursor.currentFullNodeCode) == OnDiskReadNodeType.RELAY);
        assertFalse("Iterating a relay", OnDiskReadNodeType.selectNodeImpl(cursor.nodeCode) == OnDiskReadNodeType.RELAY);
        for (int i = 0; i < cursor.stackLength; ++i)
            assertFalse("Relay to come back to", OnDiskReadNodeType.selectNodeImpl(cursor.getStackNodeCode(i)) == OnDiskReadNodeType.RELAY);
    }

    /// Walks the cursor to exhaustion and lists the positions it stops at, each as the path to it, followed by `↑` if
    /// the position is on the return path and by `=` and the content if it has any.
    private static <T> List<String> walk(OnDiskCursor<T> cursor, Function<? super T, String> show)
    {
        return walk(cursor, show, false);
    }

    /// As [#walk] for the data trie of a deletion-aware trie, also listing the positions of the deletion branches.
    private static List<String> walkWithBranches(OnDiskCursor<Integer> cursor)
    {
        return walk(cursor, String::valueOf, true);
    }

    private static <T> List<String> walk(OnDiskCursor<T> cursor, Function<? super T, String> show, boolean withBranches)
    {
        List<String> stops = new ArrayList<>();
        StringBuilder path = new StringBuilder();
        long position = cursor.encodedPosition();
        while (!Cursor.isExhausted(position))
        {
            assertNotOnRelay(cursor);
            int depth = Cursor.depth(position);
            path.setLength(Math.max(depth - 1, 0));
            if (depth > 0)
                path.append((char) Cursor.incomingTransition(position));

            StringBuilder stop = new StringBuilder(path);
            if (Cursor.isOnReturnPath(position))
                stop.append('↑');
            T content = cursor.content();
            assertEquals("Content flag at " + path, content != null, (position & Cursor.MAY_HAVE_CONTENT_BIT) != 0);
            if (content != null)
                stop.append('=').append(show.apply(content));
            if (withBranches)
            {
                long branch = cursor.alternateBranch();
                assertEquals("Deletion branch flag at " + path, branch >= 0, (position & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0);
                if (branch >= 0)
                    stop.append(" branch at ").append(branch);
            }
            stops.add(stop.toString());
            position = cursor.advance();
        }
        return stops;
    }

    private static Cursor.TransitionsReceiver pathReceiver(StringBuilder path)
    {
        return new Cursor.TransitionsReceiver()
        {
            @Override
            public void addPathByte(int nextByte)
            {
                path.append((char) nextByte);
            }

            @Override
            public void addPathBytes(DirectBuffer buffer, int pos, int count)
            {
                for (int i = 0; i < count; ++i)
                    path.append((char) (buffer.getByte(pos + i) & 0xFF));
            }
        };
    }

    /// Checks what plain cursors over the vector present, walking it forward, in reverse as an unordered trie and in
    /// reverse as an ordered one. (An ordered trie only differs from an unordered one when walked in reverse.)
    private static void assertWalks(ByteBuffer vector, List<String> forward, List<String> reverse, List<String> orderedReverse)
    {
        assertEquals("forward, unordered", forward, walk(plainCursor(vector, Direction.FORWARD, false), String::valueOf));
        assertEquals("forward, ordered", forward, walk(plainCursor(vector, Direction.FORWARD, true), String::valueOf));
        assertEquals("reverse, unordered", reverse, walk(plainCursor(vector, Direction.REVERSE, false), String::valueOf));
        assertEquals("reverse, ordered", orderedReverse, walk(plainCursor(vector, Direction.REVERSE, true), String::valueOf));
    }

    private static void assertRangeWalks(ByteBuffer vector, List<String> forward, List<String> reverse)
    {
        assertEquals("forward", forward, walkRange(rangeCursor(vector, Direction.FORWARD)));
        assertEquals("reverse", reverse, walkRange(rangeCursor(vector, Direction.REVERSE)));
    }

    /// As [#walk] for a range cursor over a vector, which a tail of one is too.
    private static List<String> walkRange(RangeCursor<TestRangeState> cursor)
    {
        return walk((OnDiskCursor<TestRangeState>) cursor, TestRangeState::toStringNoPosition);
    }

    private static void assertCovering(int expected, TestRangeState state)
    {
        assertNotNull("No range, expected " + expected, state);
        assertEquals(state.toStringNoPosition(), expected, state.leftSide);
        assertEquals(state.toStringNoPosition(), expected, state.rightSide);
    }

    /// The two things a relay is rejected for, as the message puts them: being too close to the start of the data to
    /// hold the pointer that its code announces, and having a pointer that does not lead to a node before the relay.
    private static final String NO_ROOM = "no room";
    private static final String OUTSIDE = "outside";

    private static void assertRejectedAsCorruptRelay(String description, ThrowingRunnable read)
    {
        assertRejectedAsCorruptRelay(description, "", read);
    }

    private static void assertRejectedAsCorruptRelay(String description, String reason, ThrowingRunnable read)
    {
        UncheckedIOException thrown = assertThrows(description, UncheckedIOException.class, read);
        String message = thrown.getCause().getMessage();
        assertTrue(description + ": " + message, message.startsWith("Corrupt serialized trie: relay "));
        assertTrue(description + ": " + message + " instead of " + reason, message.contains(reason));
    }

    /// A third: leading to another relay.
    private static final String RELAY_CHAIN = "relay chain";

    /// What the node that follows a prefix node is rejected for, as the message starts.
    private static final String PREFIX_TO_LEAF = "chained prefix to leaf";
    private static final String CHAINED_PREFIXES = "chained prefix nodes";

    private static void assertRejectedAsCorrupt(String description, String problem, ThrowingRunnable read)
    {
        UncheckedIOException thrown = assertThrows(description, UncheckedIOException.class, read);
        String message = thrown.getCause().getMessage();
        assertTrue(description + ": " + message + " instead of " + problem,
                   message.startsWith("Corrupt serialized trie: " + problem));
    }

    /// Opening the vector at its end must fail with the given problem, whatever kind of trie it is opened as.
    private static void assertRootRejectedAsCorrupt(ByteBuffer vector, String problem)
    {
        String description = describe(vector.array());
        for (Direction direction : Direction.values())
        {
            for (boolean ordered : new boolean[]{ false, true })
                assertRejectedAsCorrupt(description, problem, () -> plainCursor(vector, direction, ordered));
            assertRejectedAsCorrupt(description, problem, () -> rangeCursor(vector, direction));
            assertRejectedAsCorrupt(description, problem, () -> dataCursor(vector, direction));
        }
    }

    /// Opening the vector at its end must fail for the given reason, whatever kind of trie it is opened as.
    private static void assertRootRejectedAsCorruptRelay(ByteBuffer vector, String reason)
    {
        String description = describe(vector.array());
        for (Direction direction : Direction.values())
        {
            for (boolean ordered : new boolean[]{ false, true })
                assertRejectedAsCorruptRelay(description, reason, () -> plainCursor(vector, direction, ordered));
            assertRejectedAsCorruptRelay(description, reason, () -> rangeCursor(vector, direction));
            assertRejectedAsCorruptRelay(description, reason, () -> dataCursor(vector, direction));
        }
    }

    // ---- Comparison with in-memory tries -----------------------------------------------------------------------------

    /// The sizes of the chunks in which the vectors are also given to the reader, see [ChunkedRebufferer].
    private static final int[] CHUNK_SIZES = { 1, 3, 8, 13 };

    /// The ways in which the bytes of a vector are given to the reader: as a whole, like a commit-log record or a
    /// message payload, and in chunks like the pages of a file.
    private static Map<String, RebuffererFactory> sources(ByteBuffer vector)
    {
        Map<String, RebuffererFactory> sources = new LinkedHashMap<>();
        sources.put("one buffer", new ByteBufferRebufferer(vector));
        for (int chunkSize : CHUNK_SIZES)
            sources.put("chunks of " + chunkSize, new ChunkedRebufferer(vector, chunkSize));
        return sources;
    }

    /// Serves a vector in the way a file is served, in chunks of a fixed size that are buffers of their own. The
    /// chunks are much smaller than a page so that relays, their pointers and their targets end up in different
    /// chunks, and pointers get split between two.
    private static class ChunkedRebufferer implements Rebufferer, RebuffererFactory
    {
        private final ByteBuffer vector;
        private final int chunkSize;

        ChunkedRebufferer(ByteBuffer vector, int chunkSize)
        {
            this.vector = vector;
            this.chunkSize = chunkSize;
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            if (position < 0 || position >= vector.limit())
                throw new IndexOutOfBoundsException("Position " + position + " is outside the " + vector.limit() + " bytes of the vector");

            int start = (int) (position - position % chunkSize);
            ByteBuffer chunk = vector.duplicate();
            chunk.limit(Math.min(start + chunkSize, vector.limit()));
            chunk.position(start);
            ByteBuffer slice = chunk.slice();
            return new BufferHolder()
            {
                @Override
                public ByteBuffer buffer()
                {
                    return slice.duplicate();
                }

                @Override
                public long offset()
                {
                    return start;
                }

                @Override
                public void release()
                {
                    // nothing to release
                }
            };
        }

        @Override
        public Rebufferer instantiateRebufferer(boolean isScan)
        {
            return this;
        }

        @Override
        public int chunkSize()
        {
            return chunkSize;
        }

        @Override
        public void invalidateIfCached(long position)
        {
            // nothing is cached
        }

        @Override
        public long remainingBytes(long position)
        {
            return Math.max(0, fileLength() - position);
        }

        @Override
        public long positionForSkip(long currentPosition, int bytesToSkip)
        {
            return currentPosition + bytesToSkip;
        }

        @Override
        public long fileLength()
        {
            return vector.limit();
        }

        @Override
        public ChannelProxy channel()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;
        }

        @Override
        public long adjustPosition(long position)
        {
            return position;
        }

        @Override
        public void closeReader()
        {
            // nothing to close
        }

        @Override
        public void close()
        {
            // nothing to close
        }
    }

    /// What is compared between two cursors that are expected to be in the same place, for one kind of trie.
    ///
    /// The positions of the cursors are given separately, as returned by the calls that took the cursors there.
    private abstract static class Checks<C extends Cursor<?>>
    {
        abstract C tail(C cursor, Direction direction);

        String show(Object content)
        {
            return String.valueOf(content);
        }

        /// Compares the positions and, unless the cursors are exhausted, what the cursors present there.
        /// Returns false if they are exhausted.
        boolean assertSame(C expected, long expectedPosition, C actual, long actualPosition, String context)
        {
            assertEquals(context + ": at " + Cursor.toString(actualPosition) + " instead of " + Cursor.toString(expectedPosition),
                         expectedPosition,
                         actualPosition);
            if (Cursor.isExhausted(expectedPosition))
                return false;

            String place = Cursor.toString(expectedPosition);
            assertEquals(context + ": position reported at " + place, actualPosition, actual.encodedPosition());
            assertEquals(context + ": content at " + place, expected.content(), actual.content());
            return true;
        }
    }

    private static final Checks<Cursor<Integer>> PLAIN = new Checks<>()
    {
        @Override
        Cursor<Integer> tail(Cursor<Integer> cursor, Direction direction)
        {
            return cursor.tailCursor(direction);
        }
    };

    private static final Checks<RangeCursor<TestRangeState>> RANGE = new Checks<>()
    {
        @Override
        RangeCursor<TestRangeState> tail(RangeCursor<TestRangeState> cursor, Direction direction)
        {
            return cursor.tailCursor(direction);
        }

        @Override
        String show(Object content)
        {
            return ((TestRangeState) content).toStringNoPosition();
        }

        @Override
        boolean assertSame(RangeCursor<TestRangeState> expected, long expectedPosition,
                           RangeCursor<TestRangeState> actual, long actualPosition,
                           String context)
        {
            if (!super.assertSame(expected, expectedPosition, actual, actualPosition, context))
                return false;

            assertEquals(context + ": state at " + Cursor.toString(expectedPosition), expected.state(), actual.state());
            return true;
        }
    };

    private static final Checks<DeletionAwareCursor<Integer, TestRangeState>> DELETION_AWARE = new Checks<>()
    {
        @Override
        DeletionAwareCursor<Integer, TestRangeState> tail(DeletionAwareCursor<Integer, TestRangeState> cursor, Direction direction)
        {
            return cursor.tailCursor(direction);
        }

        @Override
        boolean assertSame(DeletionAwareCursor<Integer, TestRangeState> expected, long expectedPosition,
                           DeletionAwareCursor<Integer, TestRangeState> actual, long actualPosition,
                           String context)
        {
            if (!super.assertSame(expected, expectedPosition, actual, actualPosition, context))
                return false;

            for (Direction direction : Direction.values())
            {
                String branchContext = context + ", " + direction + " deletion branch at " + Cursor.toString(expectedPosition);
                RangeCursor<TestRangeState> expectedBranch = expected.deletionBranchCursor(direction);
                RangeCursor<TestRangeState> actualBranch = actual.deletionBranchCursor(direction);
                assertEquals(branchContext + " present", expectedBranch != null, actualBranch != null);
                if (expectedBranch != null)
                    assertWalksEqual(RANGE, expectedBranch, actualBranch, 1, branchContext);
            }
            return true;
        }
    };

    /// Walks the two cursors, which must not have been advanced yet, to exhaustion in parallel, see below.
    private static <C extends Cursor<?>> void assertWalksEqual(Checks<C> checks, C expected, C actual, int tailLevels, String context)
    {
        assertWalksEqual(checks, expected, expected.encodedPosition(), actual, actual.encodedPosition(), tailLevels, context);
    }

    /// Walks the two cursors to exhaustion in parallel from the given positions, which are the ones that they are
    /// at, comparing them at every step. For `tailLevels > 0` it does the same with the tails in both directions at
    /// every position that permits taking one, and with `tailLevels - 1` further levels of tails of those tails.
    private static <C extends Cursor<?>> void assertWalksEqual(Checks<C> checks,
                                                               C expected, long expectedPosition,
                                                               C actual, long actualPosition,
                                                               int tailLevels,
                                                               String context)
    {
        while (checks.assertSame(expected, expectedPosition, actual, actualPosition, context))
        {
            if (tailLevels > 0 && !Cursor.isOnReturnPath(expectedPosition))
                for (Direction tailDirection : Direction.values())
                    assertWalksEqual(checks,
                                     checks.tail(expected, tailDirection),
                                     checks.tail(actual, tailDirection),
                                     tailLevels - 1,
                                     context + ", " + tailDirection + " tail at " + Cursor.toString(expectedPosition));

            expectedPosition = expected.advance();
            actualPosition = actual.advance();
        }
    }

    /// Descends along the given key like [Cursor#descendAlong], which the cursor must not have been advanced before,
    /// and returns the position the cursor ends up at: the one for the key if the trie has a path for it, and the
    /// first that follows it otherwise.
    private static long descendAlong(Cursor<?> cursor, String key)
    {
        long position = cursor.encodedPosition();
        for (char next : key.toCharArray())
        {
            long nextPosition = Cursor.positionForDescentWithByte(position, next);
            long arrived = cursor.skipTo(nextPosition);
            if (Cursor.compare(arrived, nextPosition) != 0)
                return arrived;
            position = arrived;
        }
        return position;
    }

    /// Lists the content that a walk done with [Cursor#advanceMultiple] finds, with the paths as put together from
    /// the transitions that it reports. Where such a walk stops between items of content is up to the cursor, and
    /// is thus not something that can be compared.
    private static <C extends Cursor<?>> List<String> contentByAdvanceMultiple(Checks<C> checks, C cursor)
    {
        List<String> found = new ArrayList<>();
        StringBuilder path = new StringBuilder();
        Cursor.TransitionsReceiver receiver = pathReceiver(path);
        long position = cursor.encodedPosition();
        while (!Cursor.isExhausted(position))
        {
            Object content = cursor.content();
            if (content != null)
                found.add(path + (Cursor.isOnReturnPath(position) ? "↑" : "") + '=' + checks.show(content));

            long next = cursor.advanceMultiple(receiver);
            if (Cursor.isExhausted(next))
                break;
            if (Cursor.ascended(next, position))
                path.setLength(Math.max(Cursor.depth(next) - 1, 0));
            if (Cursor.depth(next) > 0)
                path.append((char) Cursor.incomingTransition(next));
            assertEquals("Transitions received on the way to " + Cursor.toString(next), Cursor.depth(next), path.length());
            position = next;
        }
        return found;
    }

    /// The keys to try descents with: every prefix of the given keys, on its own and continued with every byte from
    /// the one below the smallest that the keys use to the one above the largest.
    private static Set<String> probes(Collection<String> keys)
    {
        int min = 'a';
        int max = 'a';
        for (String key : keys)
            for (char c : key.toCharArray())
            {
                min = Math.min(min, c);
                max = Math.max(max, c);
            }

        Set<String> probes = new LinkedHashSet<>();
        probes.add("");
        for (String key : keys)
            for (int length = 0; length <= key.length(); ++length)
            {
                String prefix = key.substring(0, length);
                probes.add(prefix);
                for (int next = min - 1; next <= max + 1; ++next)
                    probes.add(prefix + (char) next);
            }
        return probes;
    }

    /// Checks that the two tries cannot be told apart by their cursors:
    /// - by walking them in both directions, and the tails and tails of tails taken at every position on the way,
    /// - by collecting their content with [Cursor#advanceMultiple],
    /// - by descending along each of the given keys with [Cursor#skipTo], taking the tails there, skipping over the
    ///   branch that was reached and walking what remains.
    private static <C extends Cursor<?>> void assertTriesEqual(Checks<C> checks,
                                                               Function<Direction, C> expected,
                                                               Function<Direction, C> actual,
                                                               Collection<String> probes,
                                                               String context)
    {
        for (Direction direction : Direction.values())
        {
            String walkContext = context + ", " + direction + " walk";
            assertWalksEqual(checks, expected.apply(direction), actual.apply(direction), 2, walkContext);

            assertEquals(walkContext + " with advanceMultiple",
                         contentByAdvanceMultiple(checks, expected.apply(direction)),
                         contentByAdvanceMultiple(checks, actual.apply(direction)));

            for (String probe : probes)
            {
                String probeContext = context + ", " + direction + " descent along \"" + probe + '"';
                C expectedCursor = expected.apply(direction);
                C actualCursor = actual.apply(direction);
                long expectedPosition = descendAlong(expectedCursor, probe);
                long actualPosition = descendAlong(actualCursor, probe);
                if (!checks.assertSame(expectedCursor, expectedPosition, actualCursor, actualPosition, probeContext))
                    continue;

                if (!Cursor.isOnReturnPath(expectedPosition))
                    for (Direction tailDirection : Direction.values())
                        assertWalksEqual(checks,
                                         checks.tail(expectedCursor, tailDirection),
                                         checks.tail(actualCursor, tailDirection),
                                         1,
                                         probeContext + ", " + tailDirection + " tail");

                long skipPosition = Cursor.positionForSkippingBranch(expectedPosition);
                assertWalksEqual(checks,
                                 expectedCursor, expectedCursor.skipTo(skipPosition),
                                 actualCursor, actualCursor.skipTo(skipPosition),
                                 1,
                                 probeContext + ", after skipping the branch");
            }
        }
    }

    /// Checks that the vector reads as a trie with the given keys and values (listed alternately), both as an
    /// unordered and as an ordered trie, from each of the [#sources].
    private static void assertReadsAsTrie(ByteBuffer vector, Object... keysAndValues) throws TrieSpaceExhaustedException
    {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < keysAndValues.length; i += 2)
            keys.add((String) keysAndValues[i]);
        Set<String> probes = probes(keys);

        for (boolean ordered : new boolean[]{ false, true })
        {
            InMemoryTrie<Integer> expected = ordered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
            for (int i = 0; i < keysAndValues.length; i += 2)
                expected.putRecursive(TrieUtil.directComparable((String) keysAndValues[i]),
                                      (Integer) keysAndValues[i + 1],
                                      (existing, update) -> update);

            for (Map.Entry<String, RebuffererFactory> source : sources(vector).entrySet())
            {
                Trie<Integer> actual = new OnDiskTrie<>(source.getValue(), INT_CONTENT, VERSION, ordered, vector.limit());
                assertTriesEqual(PLAIN, expected::cursor, actual::cursor, probes,
                                 (ordered ? "ordered" : "unordered") + " trie in " + source.getKey());
            }
        }
    }

    /// Checks that the vector reads as a range trie with the given markers, from each of the [#sources].
    private static void assertReadsAsRangeTrie(ByteBuffer vector, TestRangeState... markers)
    {
        List<String> keys = new ArrayList<>();
        for (TestRangeState marker : markers)
            keys.add(key(marker));
        Set<String> probes = probes(keys);
        RangeTrie<TestRangeState> expected = expectedRangeTrie(markers);

        for (Map.Entry<String, RebuffererFactory> source : sources(vector).entrySet())
        {
            RangeTrie<TestRangeState> actual = new OnDiskRangeTrie<>(source.getValue(), RANGE_CONTENT, VERSION, vector.limit());
            assertTriesEqual(RANGE, expected::cursor, actual::cursor, probes, "range trie in " + source.getKey());
        }
    }

    /// The in-memory range trie with the given markers, which is what a vector is compared with.
    ///
    /// The markers are applied as a mutation rather than put one by one, as [TestRangeState#fromList] does: a
    /// boundary that is put after the branch of a node with children is stored where the in-memory cursor does not
    /// look for it when it descends to the nearest content, and the state after a skip and the roots of tails would
    /// then be wrong in the trie that we compare with.
    private static RangeTrie<TestRangeState> expectedRangeTrie(TestRangeState... markers)
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        try
        {
            trie.apply(TestRangeState.fromList(Arrays.asList(markers)), (existing, update) -> update, features -> false);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /// The deletion-aware trie that a vector is expected to read as, put together from its live data and its
    /// deletion branches.
    private static class ExpectedDeletionAwareTrie
    {
        final InMemoryDeletionAwareTrie<Integer, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        final InMemoryDeletionAwareTrie<Integer, TestRangeState>.Mutator<Integer, TestRangeState> mutator =
            trie.<Integer, TestRangeState>mutator((existing, update) -> update,
                                                  (existing, update) -> update,
                                                  (existing, deletion) -> existing,
                                                  (deletion, update) -> update,
                                                  true,
                                                  features -> false);
        final List<String> keys = new ArrayList<>();
        final Map<String, RangeTrie<TestRangeState>> branches = new LinkedHashMap<>();

        ExpectedDeletionAwareTrie live(String key, int value) throws TrieSpaceExhaustedException
        {
            mutator.apply(DeletionAwareTrie.singleton(TrieUtil.directComparable(key), VERSION, value));
            keys.add(key);
            return this;
        }

        /// A deletion branch at the given key, with markers whose keys are relative to it.
        ExpectedDeletionAwareTrie branch(String at, TestRangeState... markers) throws TrieSpaceExhaustedException
        {
            RangeTrie<TestRangeState> branch = expectedRangeTrie(markers);
            mutator.apply(DeletionAwareTrie.deletionBranch(TrieUtil.directComparable(at), VERSION, branch));
            keys.add(at);
            branches.put(at, branch);
            return this;
        }
    }

    /// Checks that the vector reads as the given deletion-aware trie, from each of the [#sources].
    private static void assertReadsAsDeletionAwareTrie(ByteBuffer vector, ExpectedDeletionAwareTrie expected)
    {
        Set<String> probes = probes(expected.keys);
        for (Map.Entry<String, RebuffererFactory> source : sources(vector).entrySet())
        {
            String context = "deletion-aware trie in " + source.getKey();
            // The vector as a whole is opened the way a commit-log record or a message payload is, without giving
            // the position of the root, which is then taken to be the end of the buffer.
            DeletionAwareTrie<Integer, TestRangeState> actual =
                source.getValue() instanceof ByteBufferRebufferer
                ? OnDiskDeletionAwareTrie.open(vector, INT_CONTENT, RANGE_CONTENT, VERSION, -1)
                : new OnDiskDeletionAwareTrie<>(source.getValue(), INT_CONTENT, RANGE_CONTENT, VERSION, vector.limit());

            // The comparison below cannot tell a branch that is missing on both sides, so make sure that the ones
            // we expect are there, also in the tails taken where they are.
            for (Map.Entry<String, RangeTrie<TestRangeState>> branch : expected.branches.entrySet())
                for (Direction direction : Direction.values())
                {
                    String branchContext = context + ", " + direction + " deletion branch at \"" + branch.getKey() + '"';
                    DeletionAwareCursor<Integer, TestRangeState> cursor = actual.cursor(direction);
                    assertTrue(branchContext, cursor.descendAlong(source(branch.getKey())));
                    assertTrue(branchContext, (cursor.encodedPosition() & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0);
                    for (Direction branchDirection : Direction.values())
                    {
                        RangeCursor<TestRangeState> branchCursor = cursor.deletionBranchCursor(branchDirection);
                        assertNotNull(branchContext, branchCursor);
                        assertWalksEqual(RANGE, branch.getValue().cursor(branchDirection), branchCursor, 2, branchContext);

                        DeletionAwareCursor<Integer, TestRangeState> tail = cursor.tailCursor(branchDirection);
                        assertTrue(branchContext + " of tail", (tail.encodedPosition() & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0);
                        for (Direction tailBranchDirection : Direction.values())
                        {
                            branchCursor = tail.deletionBranchCursor(tailBranchDirection);
                            assertNotNull(branchContext + " of tail", branchCursor);
                            assertWalksEqual(RANGE, branch.getValue().cursor(tailBranchDirection), branchCursor, 1, branchContext + " of tail");
                        }
                    }
                }

            assertTriesEqual(DELETION_AWARE, expected.trie::cursor, actual::cursor, probes, context);
        }
    }
}
