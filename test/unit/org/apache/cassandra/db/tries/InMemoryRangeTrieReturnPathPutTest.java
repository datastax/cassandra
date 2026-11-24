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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// Tests single-key puts of range markers on the return path of a node, i.e. after its branch, into an
/// [InMemoryRangeTrie], in every order the markers of a trie can be put in.
///
/// A prefix node has a slot for the content presented before its branch and one for the content presented after
/// it. The walks present a content according to its own before/after flag, whichever slot it is in, so a marker in the
/// wrong slot still walks correctly. The state a cursor takes after a skip, and with it
/// [RangeTrie#applicableRange], is looked up in the slot for the cursor's direction, though, and finds the wrong
/// marker. The tests thus check the applicable ranges against values worked out by hand, and the walks and the states
/// after skips against the trie built in walk order.
///
/// The puts cover a node that has a child, more than one child, or content before its branch when the marker after
/// it arrives, and the same nodes getting those after the marker has been put, at the root and below it.
///
/// Each of these is also checked putting both markers of a key with one call of the two-payload `putRecursive`, and
/// further tests cover what that call gives the transformer for each slot, removals and long keys.
public class InMemoryRangeTrieReturnPathPutTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    /// The keys whose applicable range is checked, and whose positions the skips go to.
    static final String[] PROBES = { "", "a", "aa", "ab", "abb", "abc", "abd", "ac", "ad", "b" };

    /// A marker after the branch of a node with one child.
    @Test
    public void testAfterBranchWithChild()
    {
        check(new String[]{ "-", "-", "-", "-1/1", "1/1", "1/1", "1/1", "1/1", "1/1", "-" },
              before("ab", -1, 1),
              after("a", 1, -1));
    }

    /// A marker after the branch of a node whose child is a chain.
    @Test
    public void testAfterBranchWithChainChild()
    {
        check(new String[]{ "-", "-", "-", "-", "-", "-1/1", "1/1", "1/1", "1/1", "-" },
              before("abc", -1, 1),
              after("a", 1, -1));
    }

    /// A marker after the branch of a node with two children, which makes it a split node.
    @Test
    public void testAfterBranchWithTwoChildren()
    {
        check(new String[]{ "-", "-", "-", "-1/1", "1/1", "1/1", "1/1", "1/2", "2/2", "-" },
              before("ab", -1, 1),
              before("ac", 1, 2),
              after("a", 2, -1));
    }

    /// Markers before and after the branch of a node that has no children.
    @Test
    public void testBeforeAndAfterBranch()
    {
        check(new String[]{ "-", "-1/1", "1/1", "1/1", "1/1", "1/1", "1/1", "1/1", "1/1", "-" },
              before("a", -1, 1),
              after("a", 1, -1));
    }

    /// The same one level down, so that the skips to "a" look for the state below them.
    @Test
    public void testBeforeAndAfterBranchBelowSkip()
    {
        check(new String[]{ "-", "-", "-", "-1/1", "1/1", "1/1", "1/1", "-", "-", "-" },
              before("ab", -1, 1),
              after("ab", 1, -1));
    }

    /// Markers before and after the branch of a node, and one in that branch.
    @Test
    public void testBeforeAndAfterBranchWithChild()
    {
        check(new String[]{ "-", "-1/1", "1/1", "1/2", "2/2", "2/2", "2/2", "2/2", "2/2", "-" },
              before("a", -1, 1),
              before("ab", 1, 2),
              after("a", 2, -1));
    }

    /// The same one level down.
    @Test
    public void testBeforeAndAfterBranchWithChildBelowSkip()
    {
        check(new String[]{ "-", "-", "-", "-1/1", "1/1", "1/2", "2/2", "-", "-", "-" },
              before("ab", -1, 1),
              before("abc", 1, 2),
              after("ab", 2, -1));
    }

    /// Markers before and after the branch of the root, and one in that branch.
    @Test
    public void testBeforeAndAfterRootBranch()
    {
        check(new String[]{ "-1/1", "1/1", "1/1", "1/2", "2/2", "2/2", "2/2", "2/2", "2/2", "2/2" },
              before("", -1, 1),
              before("ab", 1, 2),
              after("", 2, -1));
    }

    final static class Marker
    {
        private final String key;
        private final boolean afterBranch;
        private final int left;
        private final int right;

        Marker(String key, boolean afterBranch, int left, int right)
        {
            this.key = key;
            this.afterBranch = afterBranch;
            this.left = left;
            this.right = right;
        }

        TestRangeState state()
        {
            return new TestRangeState(comparable(key), afterBranch, left, right);
        }

        @Override
        public String toString()
        {
            return '"' + key + '"' + (afterBranch ? "↑" : "") + ' ' + left + '/' + right;
        }

        public String key()
        {
            return key;
        }

        public boolean afterBranch()
        {
            return afterBranch;
        }

        public int left()
        {
            return left;
        }

        public int right()
        {
            return right;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (Marker) obj;
            return Objects.equals(this.key, that.key) &&
                   this.afterBranch == that.afterBranch &&
                   this.left == that.left &&
                   this.right == that.right;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, afterBranch, left, right);
        }
    }

    static Marker before(String key, int left, int right)
    {
        return new Marker(key, false, left, right);
    }

    static Marker after(String key, int left, int right)
    {
        return new Marker(key, true, left, right);
    }

    static ByteComparable comparable(String key)
    {
        return ByteComparable.preencoded(VERSION, key.getBytes(StandardCharsets.UTF_8));
    }

    /// Puts the markers, given in the order a forward walk presents them, in every order, and checks the walks, the
    /// applicable ranges of the [#PROBES] against `expectedApplicable`, and the states after skips against the trie
    /// built in walk order.
    ///
    /// Then does the same putting the markers of each key with a single call of the two-payload `putRecursive`, which
    /// is given `null` for a slot the key has no marker for.
    static void check(String[] expectedApplicable, Marker... markersInWalkOrder)
    {
        List<String> expectedForward = new ArrayList<>();
        for (Marker marker : markersInWalkOrder)
            expectedForward.add('"' + marker.key + "\" " + marker.state());
        List<String> expectedReverse = new ArrayList<>(expectedForward);
        Collections.reverse(expectedReverse);

        List<String> referenceSkips = null;
        for (List<Marker> order : permutations(Arrays.asList(markersInWalkOrder)))
        {
            InMemoryRangeTrie<TestRangeState> trie = build(order);
            String what = "Markers put in the order " + order + '\n' + trie.dump();
            List<String> skips = verify(trie, what, expectedForward, expectedReverse, expectedApplicable);
            if (referenceSkips == null)
                referenceSkips = skips; // the first permutation is the walk order
            else
                assertEquals(what, referenceSkips, skips);
        }

        for (List<Marker[]> order : permutations(slotsByKey(markersInWalkOrder)))
        {
            InMemoryRangeTrie<TestRangeState> trie = buildBothSlots(order);
            String what = "Markers put with both slots of a key at once in the order " + toString(order) + '\n' + trie.dump();
            assertEquals(what, referenceSkips, verify(trie, what, expectedForward, expectedReverse, expectedApplicable));
        }
    }

    /// Checks the walks and applicable ranges of the trie, and returns the states after skips.
    static List<String> verify(InMemoryRangeTrie<TestRangeState> trie,
                               String what,
                               List<String> expectedForward,
                               List<String> expectedReverse,
                               String[] expectedApplicable)
    {
        assertEquals(what, expectedForward, entries(trie, Direction.FORWARD));
        assertEquals(what, expectedReverse, entries(trie, Direction.REVERSE));

        for (int i = 0; i < PROBES.length; ++i)
            assertEquals("applicableRange(\"" + PROBES[i] + "\")\n" + what,
                         expectedApplicable[i],
                         sides(trie.applicableRange(comparable(PROBES[i]))));

        return skips(trie);
    }

    /// Groups the markers by key, as pairs of the marker before the branch and the one after it, either of which may be
    /// `null`.
    static List<Marker[]> slotsByKey(Marker... markers)
    {
        Map<String, Marker[]> slots = new LinkedHashMap<>();
        for (Marker marker : markers)
        {
            Marker[] pair = slots.computeIfAbsent(marker.key, k -> new Marker[2]);
            assertNull("Two markers on the same side of " + marker.key, pair[marker.afterBranch ? 1 : 0]);
            pair[marker.afterBranch ? 1 : 0] = marker;
        }
        return new ArrayList<>(slots.values());
    }

    static String toString(List<Marker[]> pairs)
    {
        return pairs.stream().map(Arrays::toString).collect(Collectors.joining(", ", "[", "]"));
    }

    static TestRangeState state(Marker marker)
    {
        return marker == null ? null : marker.state();
    }

    static InMemoryRangeTrie<TestRangeState> buildBothSlots(List<Marker[]> pairs)
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        try
        {
            for (Marker[] pair : pairs)
            {
                String key = pair[0] != null ? pair[0].key : pair[1].key;
                trie.putRecursive(comparable(key), state(pair[0]), state(pair[1]), (existing, update) -> update);
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /// The node shapes a two-payload put can find at "a", each given as the markers put before it, in walk order. The
    /// markers at "a" give the content the put finds in each slot.
    static final Marker[][] EXISTING_SHAPES = {
        { before("a", -1, 1), after("b", 1, -1) },                          // leaf with content before the branch
        { before("", -1, 1), after("a", 1, -1) },                           // leaf with content after the branch
        { before("a", -1, 1), after("a", 1, -1) },                          // prefix node with both, without a child
        { before("a", -1, 1), before("ab", 1, 2), after("a", 2, -1) },      // prefix node with both and a child
        { before("a", -1, 1), before("ab", 1, -1) },                        // content before the branch and a child
        { before("ab", -1, 1), after("a", 1, -1) },                         // content after the branch and a child
        { before("a", -1, 1), before("ab", 1, 2), before("ac", 2, 3), after("a", 3, -1) }, // both, two children
    };

    /// A two-payload put gives the transformer the existing content of each slot with the update for that slot, and
    /// leaves the other content of the node and its branch as they were.
    @Test
    public void testBothSlotsUpdateGetsTheContentOfEachSlot() throws TrieSpaceExhaustedException
    {
        for (Marker[] shape : EXISTING_SHAPES)
        {
            InMemoryRangeTrie<TestRangeState> trie = build(Arrays.asList(shape));
            Marker[] existing = new Marker[2];
            for (Marker marker : shape)
                if (marker.key.equals("a"))
                    existing[marker.afterBranch ? 1 : 0] = marker;

            // Updates with the same sides, so that the ranges stay valid, as distinct objects.
            TestRangeState updateBefore = existing[0] != null ? existing[0].state() : null;
            TestRangeState updateAfter = existing[1] != null ? existing[1].state() : null;
            for (int slots = 1; slots <= 3; ++slots)
            {
                TestRangeState putBefore = (slots & 1) != 0 ? updateBefore : null;
                TestRangeState putAfter = (slots & 2) != 0 ? updateAfter : null;
                if (putBefore == null && putAfter == null)
                    continue;

                List<String> calls = new ArrayList<>();
                trie.putRecursive(comparable("a"), putBefore, putAfter, (e, u) -> {
                    calls.add(e + " -> " + u);
                    assertTrue("Update " + u + " given for the wrong slot", u == putBefore || u == putAfter);
                    return u;
                });

                String what = "Shape " + Arrays.toString(shape) + " slots " + slots + '\n' + trie.dump();
                List<String> expectedCalls = new ArrayList<>();
                if (putBefore != null)
                    expectedCalls.add(state(existing[0]) + " -> " + putBefore);
                if (putAfter != null)
                    expectedCalls.add(state(existing[1]) + " -> " + putAfter);
                assertEquals(what, expectedCalls, calls);

                List<String> expectedForward = new ArrayList<>();
                for (Marker marker : shape)
                    expectedForward.add('"' + marker.key + "\" " + marker.state());
                assertEquals(what, expectedForward, entries(trie, Direction.FORWARD));
                assertEquals(what, Arrays.asList(shape).size(), trie.valuesCount());
            }
        }
    }

    /// Putting both slots of a key that has no content gives the node both, whatever kind of node it is, and calls the
    /// transformer with `null` as the existing content of each.
    @Test
    public void testBothSlotsIntoNodeWithoutContent() throws TrieSpaceExhaustedException
    {
        // The branch of "a" before the put: none, the end of a chain, the middle of a chain, a split node.
        Marker[][] branches = {
            {},
            { before("ab", 3, 4), after("ab", 4, 3) },
            { before("abc", 3, 4), after("abc", 4, 3) },
            { before("ab", 3, 4), after("ab", 4, 3), before("ac", 3, 5), after("ac", 5, 3) },
        };
        for (Marker[] branch : branches)
        {
            InMemoryRangeTrie<TestRangeState> trie = build(Arrays.asList(branch));
            // a sibling with content, so that the path to "a" exists before the put for an empty branch
            trie.putRecursive(comparable("b"), before("b", -1, 6).state(), after("b", 6, -1).state(), (e, u) -> u);

            List<String> calls = new ArrayList<>();
            Marker start = before("a", -1, 3);
            Marker end = after("a", 3, -1);
            trie.putRecursive(comparable("a"), start.state(), end.state(), (e, u) -> {
                calls.add(e + " -> " + u);
                return u;
            });
            String what = Arrays.toString(branch) + '\n' + trie.dump();
            assertEquals(what, Arrays.asList("null -> " + start.state(), "null -> " + end.state()), calls);

            List<String> expectedForward = new ArrayList<>();
            expectedForward.add("\"a\" " + start.state());
            for (Marker marker : branch)
                expectedForward.add('"' + marker.key + "\" " + marker.state());
            expectedForward.add("\"a\" " + end.state());
            expectedForward.add("\"b\" " + before("b", -1, 6).state());
            expectedForward.add("\"b\" " + after("b", 6, -1).state());
            assertEquals(what, expectedForward, entries(trie, Direction.FORWARD));
            List<String> expectedReverse = new ArrayList<>(expectedForward);
            Collections.reverse(expectedReverse);
            assertEquals(what, expectedReverse, entries(trie, Direction.REVERSE));
            assertEquals(what, "-1/3", sides(trie.applicableRange(comparable("a"))));
            assertEquals(what, "3/3", sides(trie.applicableRange(comparable("aa"))));
            assertEquals(what, "3/3", sides(trie.applicableRange(comparable("ad"))));
        }
    }

    /// A transformer that returns `null` for both slots removes the node's content, and the node if it has no branch,
    /// cleaning up the path that led to it.
    @Test
    public void testBothSlotsRemoval() throws TrieSpaceExhaustedException
    {
        // Both slots of "a" with a child: the prefix node goes, the child stays.
        InMemoryRangeTrie<TestRangeState> trie = build(Arrays.asList(before("a", -1, 1), before("ab", 1, 2), after("ab", 2, 1), after("a", 1, -1)));
        trie.putRecursive(comparable("a"), before("a", -1, 1).state(), after("a", 1, -1).state(), (e, u) -> {
            assertNotNull(e);
            return null;
        });
        // "ab" now has to start and end the range itself.
        trie.putRecursive(comparable("ab"), before("ab", -1, 2).state(), after("ab", 2, -1).state(), (e, u) -> u);
        assertEquals(trie.dump(), Arrays.asList("\"ab\" " + before("ab", -1, 2).state(), "\"ab\" " + after("ab", 2, -1).state()), entries(trie, Direction.FORWARD));
        assertEquals(2, trie.valuesCount());

        // Removing those, which sit in a prefix node with no child, at the end of a chain, empties the trie.
        trie.putRecursive(comparable("ab"), before("ab", -1, 2).state(), after("ab", 2, -1).state(), (e, u) -> null);
        assertTrue(trie.dump(), trie.isEmpty());
        assertEquals(0, trie.valuesCount());

        // Removing the range at "a" leaves its sibling, in a node that has only one child left.
        trie = build(Arrays.asList(before("a", -1, 1), after("a", 1, -1), before("b", -1, 2), after("b", 2, -1)));
        trie.putRecursive(comparable("a"), before("a", -1, 1).state(), after("a", 1, -1).state(), (e, u) -> null);
        assertEquals(trie.dump(), Arrays.asList("\"b\" " + before("b", -1, 2).state(), "\"b\" " + after("b", 2, -1).state()), entries(trie, Direction.FORWARD));
        assertEquals("2/2", sides(trie.applicableRange(comparable("ba"))));
        assertEquals("-", sides(trie.applicableRange(comparable("ab"))));
        assertEquals(2, trie.valuesCount());

        // Removing the only child of a node with content after its branch keeps that content.
        for (Marker[] shape : new Marker[][]{ { before("ab", -1, 1), after("a", 1, -1) },
                                              { before("abc", -1, 1), after("a", 1, -1) },
                                              { before("a", -1, 1), before("ab", 1, 2), after("a", 2, -1) } })
        {
            trie = build(Arrays.asList(shape));
            Marker child = shape[shape.length - 2];
            trie.putRecursive(comparable(child.key), child.state(), null, (e, u) -> null);
            String what = Arrays.toString(shape) + '\n' + trie.dump();
            assertEquals(what, shape.length - 1, trie.valuesCount());
            // Make the range valid again with a start at the root if needed, and check the walks.
            List<String> expected = new ArrayList<>();
            if (shape.length == 2)
            {
                trie.putRecursive(comparable(""), before("", -1, 1).state(), null, (e, u) -> u);
                expected.add("\"\" " + before("", -1, 1).state());
            }
            else
                expected.add("\"a\" " + before("a", -1, 1).state());
            Marker end = shape[shape.length - 1];
            if (shape.length == 3) // the end of the range moves to the left side of the start
                trie.putRecursive(comparable("a"), null, after("a", 1, -1).state(), (e, u) -> u);
            expected.add("\"a\" " + (shape.length == 3 ? after("a", 1, -1) : end).state());
            what = Arrays.toString(shape) + '\n' + trie.dump();
            assertEquals(what, expected, entries(trie, Direction.FORWARD));
            Collections.reverse(expected);
            assertEquals(what, expected, entries(trie, Direction.REVERSE));
            assertEquals(what, "1/1", sides(trie.applicableRange(comparable("aa"))));
            assertEquals(what, "-", sides(trie.applicableRange(comparable("b"))));

            // and the content can still be removed, emptying the trie
            trie.putRecursive(comparable("a"), before("a", -1, 1).state(), after("a", 1, -1).state(), (e, u) -> null);
            trie.putRecursive(comparable(""), before("", -1, 1).state(), null, (e, u) -> null);
            assertTrue(trie.dump(), trie.isEmpty());
        }
    }

    /// Removing one of the two markers of a node without children must leave the node in the shape a put of the other
    /// marker alone would give it (a leaf), which walks and skips in both directions rely on.
    @Test
    public void testRemovingOneSideOfNodeWithoutChildren() throws TrieSpaceExhaustedException
    {
        for (boolean removeAfter : new boolean[]{ false, true })
        {
            for (String key : new String[]{ "", "a", "ab" })
            {
                InMemoryRangeTrie<TestRangeState> trie = build(Arrays.asList(before(key, -1, 1), after(key, 1, -1)));
                List<Marker> expected;
                if (removeAfter)
                {
                    trie.putRecursive(comparable(key), null, after(key, 1, -1).state(), (e, u) -> null);
                    // end the range somewhere after the key's branch instead
                    trie.putRecursive(comparable("b"), null, after("b", 1, -1).state(), (e, u) -> u);
                    expected = Arrays.asList(before(key, -1, 1), after("b", 1, -1));
                }
                else
                {
                    trie.putRecursive(comparable(key), before(key, -1, 1).state(), null, (e, u) -> null);
                    // start the range at the root instead
                    if (key.isEmpty())
                        continue; // nothing left to start the range before
                    trie.putRecursive(comparable(""), before("", -1, 1).state(), null, (e, u) -> u);
                    expected = Arrays.asList(before("", -1, 1), after(key, 1, -1));
                }
                assertSameAs(build(expected), trie, (removeAfter ? "after" : "before") + " removed at \"" + key + '"');
            }
        }
    }

    /// Checks that the two tries walk, skip and report applicable ranges the same way.
    static void assertSameAs(InMemoryRangeTrie<TestRangeState> expected, InMemoryRangeTrie<TestRangeState> actual, String msg)
    {
        String what = msg + "\nexpected " + expected.dump() + "\nactual " + actual.dump();
        assertEquals(what, entries(expected, Direction.FORWARD), entries(actual, Direction.FORWARD));
        assertEquals(what, entries(expected, Direction.REVERSE), entries(actual, Direction.REVERSE));
        for (String probe : PROBES)
            assertEquals("applicableRange(\"" + probe + "\")\n" + what,
                         sides(expected.applicableRange(comparable(probe))),
                         sides(actual.applicableRange(comparable(probe))));
        assertEquals(what, skips(expected), skips(actual));
        assertEquals(what, expected.valuesCount(), actual.valuesCount());
    }

    /// The two-payload put walks keys of any length without recursion.
    @Test
    public void testBothSlotsLongKey() throws TrieSpaceExhaustedException
    {
        String key = "a".repeat(100_000);
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        trie.putRecursive(comparable(key), before(key, -1, 1).state(), after(key, 1, -1).state(), (e, u) -> u);
        trie.putRecursive(comparable(key + "b"), before(key + "b", 1, 2).state(), after(key + "b", 2, 1).state(), (e, u) -> u);
        assertEquals(4, trie.valuesCount());
        assertEquals("2/2", sides(trie.applicableRange(comparable(key + "bc"))));
        assertEquals("1/1", sides(trie.applicableRange(comparable(key + "c"))));

        trie.putRecursive(comparable(key + "b"), before(key + "b", 1, 2).state(), after(key + "b", 2, 1).state(), (e, u) -> null);
        trie.putRecursive(comparable(key), before(key, -1, 1).state(), after(key, 1, -1).state(), (e, u) -> null);
        assertTrue(trie.isEmpty());
        assertEquals(0, trie.valuesCount());
    }

    static InMemoryRangeTrie<TestRangeState> build(List<Marker> markers)
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        try
        {
            for (Marker marker : markers)
                trie.putRecursive(comparable(marker.key), marker.state(), marker.afterBranch, (existing, update) -> update);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    static List<String> entries(InMemoryRangeTrie<TestRangeState> trie, Direction direction)
    {
        List<String> entries = new ArrayList<>();
        trie.forEachEntry(direction, (key, state) -> entries.add('"' + new String(key.asByteComparableArray(VERSION), StandardCharsets.UTF_8) + "\" " + state));
        return entries;
    }

    static String sides(TestRangeState state)
    {
        return state == null ? "-" : state.leftSide + "/" + state.rightSide;
    }

    /// Skips, in both directions, along the path of each probe and to the return path of its last byte, one byte at
    /// a time, and lists where each cursor stops and the state it reports there.
    static List<String> skips(InMemoryRangeTrie<TestRangeState> trie)
    {
        List<String> skips = new ArrayList<>();
        for (String probe : PROBES)
        {
            if (probe.isEmpty())
                continue;
            for (boolean toReturnPath : new boolean[]{ false, true })
                for (Direction direction : Direction.values())
                {
                    RangeCursor<TestRangeState> cursor = trie.cursor(direction);
                    long position = cursor.encodedPosition();
                    for (int i = 0; i < probe.length(); ++i)
                    {
                        long target = Cursor.positionForDescentWithByte(position, probe.charAt(i));
                        if (toReturnPath && i == probe.length() - 1)
                            target |= Cursor.ON_RETURN_PATH_BIT;
                        position = cursor.skipTo(target);
                        if (Cursor.compare(position, target) != 0)
                            break;
                    }
                    skips.add(direction + " to \"" + probe + '"' + (toReturnPath ? "↑" : "") + ": "
                              + Cursor.toString(position).trim() + " state " + sides(cursor.state()));
                }
        }
        return skips;
    }

    static <T> List<List<T>> permutations(List<T> items)
    {
        List<List<T>> permutations = new ArrayList<>();
        permute(new ArrayList<>(items), 0, permutations);
        return permutations;
    }

    private static <T> void permute(List<T> items, int start, List<List<T>> permutations)
    {
        if (start == items.size())
        {
            permutations.add(new ArrayList<>(items));
            return;
        }
        for (int i = start; i < items.size(); ++i)
        {
            Collections.swap(items, start, i);
            permute(items, start + 1, permutations);
            Collections.swap(items, start, i);
        }
    }
}
