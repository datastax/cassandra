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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

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

    record Marker(String key, boolean afterBranch, int left, int right)
    {
        TestRangeState state()
        {
            return new TestRangeState(comparable(key), afterBranch, left, right);
        }

        @Override
        public String toString()
        {
            return '"' + key + '"' + (afterBranch ? "↑" : "") + ' ' + left + '/' + right;
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

            assertEquals(what, expectedForward, entries(trie, Direction.FORWARD));
            assertEquals(what, expectedReverse, entries(trie, Direction.REVERSE));

            for (int i = 0; i < PROBES.length; ++i)
                assertEquals("applicableRange(\"" + PROBES[i] + "\")\n" + what,
                             expectedApplicable[i],
                             sides(trie.applicableRange(comparable(PROBES[i]))));

            List<String> skips = skips(trie);
            if (referenceSkips == null)
                referenceSkips = skips; // the first permutation is the walk order
            else
                assertEquals(what, referenceSkips, skips);
        }
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
                    try (RangeCursor<TestRangeState> cursor = trie.cursor(direction))
                    {
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
