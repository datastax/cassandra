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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/// Tests for moving deletion branches of an [InMemoryDeletionAwareTrie] up when a deletion is applied at a higher
/// level and deletions are not at fixed points.
public class InMemoryDeletionAwareTrieHoistingTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Test(timeout = 10000)
    public void testHoistingPastLastTransitionOfSplitNode() throws TrieSpaceExhaustedException
    {
        // Node 40 is split (7 children) and its last child is 0xFF. Each child has live data and a deletion branch
        // rooted at it, and there is more data after 40. A deletion applied at the root must hoist all deletion
        // branches to the root, walking past 40's 0xFF child without wrapping around to its first child.
        String[] children = new String[]{ "10", "20", "30", "40", "50", "60", "ff" };
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = trie.mutator(DataPoint::combineLive,
                                   DataPoint::combineDeletion,
                                   DataPoint::deleteLive,
                                   DataPoint::deleteLive,
                                   false,
                                   v -> false);

        List<DataPoint> expected = new ArrayList<>();
        for (String child : children)
        {
            String prefix = "40" + child;
            mutator.apply(DeletionAwareTrie.singleton(hex(prefix + "15"), VERSION, new LivePoint(hex(prefix + "15"), 10)));
            mutator.apply(DeletionAwareTrie.singleton(hex(prefix + "30"), VERSION, new LivePoint(hex(prefix + "30"), 1)));
            mutator.apply(DeletionAwareTrie.deletedRange(hex(prefix), hex("10"), hex("20"), VERSION, new DeletionMarker(hex(prefix + "10"), 5, 5)));

            expected.add(new DeletionMarker(hex(prefix + "10"), -1, 5));
            expected.add(livePointInside(hex(prefix + "15"), 10, 5));
            expected.add(new DeletionMarker(hex(prefix + "20"), 5, -1));
            expected.add(new LivePoint(hex(prefix + "30"), 1));
        }
        mutator.apply(DeletionAwareTrie.singleton(hex("4110"), VERSION, new LivePoint(hex("4110"), 1)));
        mutator.apply(DeletionAwareTrie.singleton(hex("4130"), VERSION, new LivePoint(hex("4130"), 1)));

        assertNull(trie.cursor(Direction.FORWARD).deletionBranchCursor(Direction.FORWARD));
        // Deletion at the root, which is after node 40 so that only the hoisting walk goes over it.
        mutator.apply(DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, hex("4100"), hex("4120"), VERSION, new DeletionMarker(hex("4100"), 7, 7)));

        // All boundaries, including the ones previously under 40's children, must now be in the root branch.
        RangeTrie<DeletionMarker> rootBranch = dir -> trie.cursor(dir).deletionBranchCursor(dir);
        assertEquals(children.length * 2 + 2, Iterables.size(rootBranch.values()));

        expected.add(new DeletionMarker(hex("4100"), -1, 7));
        expected.add(new DeletionMarker(hex("4120"), 7, -1));
        expected.add(new LivePoint(hex("4130"), 1));
        verify(expected);

        List<DataPoint> actual = toList(trie);
        assertEquals(expected, actual);
        assertEquals(expected.toString(), actual.toString()); // marker equality does not check positions
        DeletionAwareTestBase.assertDeletionAwareEqual("hoisted", expected, trie);
    }

    /// `putRecursive` at the node where a deletion branch is rooted adds, changes and removes the node's live content
    /// and leaves the deletion branch in place.
    @Test
    public void testPutRecursiveAtDeletionBranchRoot() throws TrieSpaceExhaustedException
    {
        for (String at : new String[]{ "", "40", "4050" })
        {
            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
            var mutator = trie.mutator(DataPoint::combineLive,
                                       DataPoint::combineDeletion,
                                       DataPoint::deleteLive,
                                       DataPoint::deleteLive,
                                       false,
                                       v -> false);
            mutator.apply(DeletionAwareTrie.deletedRange(hex(at), hex("10"), hex("20"), VERSION, new DeletionMarker(hex(at + "10"), 5, 5)));
            mutator.apply(DeletionAwareTrie.singleton(hex(at + "30"), VERSION, new LivePoint(hex(at + "30"), 1)));

            List<DataPoint> rest = List.of(new DeletionMarker(hex(at + "10"), -1, 5),
                                           new DeletionMarker(hex(at + "20"), 5, -1),
                                           new LivePoint(hex(at + "30"), 1));
            assertPoints("before put at " + at, rest, trie);

            trie.putRecursive(hex(at), new LivePoint(hex(at), 2), (e, u) -> u);
            List<DataPoint> withContent = new ArrayList<>();
            withContent.add(new LivePoint(hex(at), 2));
            withContent.addAll(rest);
            assertPoints("put at " + at, withContent, trie);

            trie.putRecursive(hex(at), new LivePoint(hex(at), 3), (e, u) -> u);
            withContent.set(0, new LivePoint(hex(at), 3));
            assertPoints("update at " + at, withContent, trie);

            trie.putRecursive(hex(at), new LivePoint(hex(at), 4), (e, u) -> null);
            assertPoints("removal at " + at, rest, trie);

            // Remove the child too; the node keeps the deletion branch.
            trie.putRecursive(hex(at + "30"), new LivePoint(hex(at + "30"), 1), (e, u) -> null);
            assertPoints("removal of child of " + at, rest.subList(0, 2), trie);
            trie.putRecursive(hex(at), new LivePoint(hex(at), 5), (e, u) -> u);
            withContent = new ArrayList<>();
            withContent.add(new LivePoint(hex(at), 5));
            withContent.addAll(rest.subList(0, 2));
            assertPoints("put at childless " + at, withContent, trie);
        }
    }

    private static void assertPoints(String msg, List<DataPoint> expected, InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        String what = msg + '\n' + trie.dump();
        List<DataPoint> actual = toList(trie);
        assertEquals(what, expected, actual);
        assertEquals(what, expected.toString(), actual.toString()); // marker equality does not check positions
        DeletionAwareTestBase.assertDeletionAwareEqual(what, expected, trie);
    }

    private static DataPoint livePointInside(ByteComparable position, int timestamp, int covering)
    {
        return new CombinedDataPoint(new LivePoint(position, timestamp), new DeletionMarker(position, covering, covering));
    }

    private static ByteComparable hex(String s)
    {
        return ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s));
    }
}
