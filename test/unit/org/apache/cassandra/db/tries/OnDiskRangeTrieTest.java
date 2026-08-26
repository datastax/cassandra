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
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.RANGE_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// Covers the state machine of [OnDiskCursor.Range] over nodes with more children than a sparse node
/// can hold.
///
/// The round-trip comparisons in the other on-disk tests only walk both directions, which never asks
/// for the active range state. That state is computed by descending to the nearest content with
/// [OnDiskReadNodeType#getFirstChild], reached only when [RangeCursor#state] is asked for after a
/// skip, which is what [RangeTrie#applicableRange] does. Getting to the bitmap, dense and relay
/// implementations of it needs a wider alphabet than the other tests use, transitions at or above
/// 0xE0 to tell a full bitmap cardinality from a truncated one, and a trie spanning several pages.
public class OnDiskRangeTrieTest
{
    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// 30 transitions, a third of them at or above 0xE0. [OnDiskWriteNodeType#selectChildrenType]
    /// picks a bitmap node for this many children.
    private static final int[] BITMAP_TRANSITIONS =
    { 0x00, 0x01, 0x07, 0x0d, 0x1f, 0x32, 0x40, 0x4d, 0x64, 0x78, 0x7f, 0x96, 0xaa, 0xbe, 0xc8, 0xd2,
      0xdc, 0xdf, 0xe0, 0xe1, 0xe6, 0xeb, 0xf0, 0xf5, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff };

    private static int[] denseTransitions()
    {
        int[] transitions = new int[256];
        for (int i = 0; i < transitions.length; ++i)
            transitions[i] = i;
        return transitions;
    }

    @Test
    public void testBitmapAtRoot() throws IOException
    {
        testWideNode(0, BITMAP_TRANSITIONS, 1);
    }

    @Test
    public void testBitmapBelowRoot() throws IOException
    {
        testWideNode(2, BITMAP_TRANSITIONS, 1);
    }

    /// A dense root is the only node that can have a child at transition 0xFF and be the last thing in
    /// the file, because the writer emits the root last.
    @Test
    public void testDenseAtRoot() throws IOException
    {
        testWideNode(0, denseTransitions(), 1);
    }

    @Test
    public void testDenseBelowRoot() throws IOException
    {
        testWideNode(2, denseTransitions(), 1);
    }

    /// Long keys make the trie span several pages, which is what makes the writer lay out relay nodes.
    @Test
    public void testWideNodesOverSeveralPages() throws IOException
    {
        testWideNode(2, BITMAP_TRANSITIONS, 128);
    }

    /// Builds a range trie whose keys are `prefixLength` fixed bytes, one of the given transitions,
    /// then `suffixLength` fixed bytes, with a range spanning every consecutive pair of transitions,
    /// and compares the on-disk read of it against the in-memory original.
    private void testWideNode(int prefixLength, int[] transitions, int suffixLength) throws IOException
    {
        InMemoryRangeTrie<TestRangeState> expected = TestRangeState.fromList(markers(prefixLength, transitions, suffixLength));
        File file = FileWriter.write(expected, true, RANGE_SERDE, new File(java.io.File.createTempFile("rangetrie", ".trie")));

        try (OnDiskRangeTrie<TestRangeState> actual = OnDiskRangeTrie.open(file, RANGE_SERDE, VERSION, -1))
        {
            TrieUtil.assertTriesEqual(expected, actual);
            assertApplicableRangesEqual(expected, actual, prefixLength, suffixLength);
        }
    }

    /// [RangeTrie#applicableRange] descends along the key and then asks for the state, which is the
    /// path a memtable merge takes. Probes stop above the wide node, on it, inside a key and past the
    /// end of one.
    private void assertApplicableRangesEqual(RangeTrie<TestRangeState> expected,
                                             RangeTrie<TestRangeState> actual,
                                             int prefixLength,
                                             int suffixLength)
    {
        if (prefixLength > 0)
            assertApplicableRangeEqual(expected, actual, key(prefixLength, -1, 0));
        for (int transition = 0; transition <= 0xFF; ++transition)
        {
            assertApplicableRangeEqual(expected, actual, key(prefixLength, transition, 0));
            assertApplicableRangeEqual(expected, actual, key(prefixLength, transition, suffixLength / 2));
            assertApplicableRangeEqual(expected, actual, key(prefixLength, transition, suffixLength));
            assertApplicableRangeEqual(expected, actual, key(prefixLength, transition, suffixLength + 1));
        }
    }

    private void assertApplicableRangeEqual(RangeTrie<TestRangeState> expected,
                                            RangeTrie<TestRangeState> actual,
                                            ByteComparable key)
    {
        assertEquals("Applicable range at " + key.byteComparableAsString(VERSION),
                     expected.applicableRange(key),
                     actual.applicableRange(key));
    }

    /// A tail has to present the ranges that are active when its branch is entered and when it is left,
    /// as content on its root. Neither the walks in [TrieUtil#assertTriesEqual] nor the applicable-range
    /// probes above ask for that, and the flag on the root position that tells a consumer to look for it
    /// is separate from the content itself.
    @Test
    public void testTailsAlongAForwardWalk() throws IOException
    {
        assertTailsEqual(Direction.FORWARD);
    }

    /// The same from a reverse walk. The ascent side of a tail's root is the side the walk has not yet
    /// presented, which for a reverse walk is the one a node carries its content on, so the two
    /// directions reach it by different routes.
    @Test
    public void testTailsAlongAReverseWalk() throws IOException
    {
        assertTailsEqual(Direction.REVERSE);
    }

    /// Takes the tail in both directions at every position of a walk in the given one, and compares it
    /// against the tail the in-memory trie the file was written from gives at the same place.
    private void assertTailsEqual(Direction walkDirection) throws IOException
    {
        InMemoryRangeTrie<TestRangeState> expected = TestRangeState.fromList(markers(0, BITMAP_TRANSITIONS, 1));
        try (OnDiskRangeTrie<TestRangeState> actual = TrieUtil.onDiskRoundtrip(expected))
        {
            RangeCursor<TestRangeState> expectedCursor = expected.cursor(walkDirection);
            RangeCursor<TestRangeState> actualCursor = actual.cursor(walkDirection);
            long position = expectedCursor.encodedPosition();
            assertEquals(Cursor.toString(position), Cursor.toString(actualCursor.encodedPosition()));
            while (!Cursor.isExhausted(position))
            {
                // Taking a tail on the return path is not permitted.
                if (!Cursor.isOnReturnPath(position))
                    for (Direction tailDirection : Direction.values())
                        assertTailsEqual(expectedCursor.tailCursor(tailDirection),
                                         actualCursor.tailCursor(tailDirection));
                position = expectedCursor.advance();
                assertEquals(Cursor.toString(position), Cursor.toString(actualCursor.advance()));
            }
        }
    }

    /// Compares the two tails, and before walking them (which consumes them) the tails they in turn give
    /// at their root: a branch tail holds its root's content in the cursor rather than in the file, and
    /// a tail taken from it has to pick it up from there.
    private void assertTailsEqual(RangeCursor<TestRangeState> expected, RangeCursor<TestRangeState> actual)
    {
        for (Direction nestedDirection : Direction.values())
            TrieUtil.assertCursorWalksEqual(expected.tailCursor(nestedDirection),
                                            actual.tailCursor(nestedDirection));
        TrieUtil.assertCursorWalksEqual(expected, actual);
    }

    /// A range that covers a whole branch puts a marker on both sides of the same node: one presented
    /// on descent and one on the way back up. That is the only shape that fills both content slots of
    /// a generic-content node, and it is what a partition-level deletion looks like in a trie-backed
    /// partition update.
    @Test
    public void testContentOnBothSidesOfANode() throws IOException
    {
        assertBranchRoundTrips(ByteComparable.EMPTY);       // whole trie, i.e. content on the root
        assertBranchRoundTrips(key(3, -1, 0));              // content on a node below the root
    }

    private void assertBranchRoundTrips(ByteComparable branchKey) throws IOException
    {
        TestRangeState covering = new TestRangeState(branchKey, false, 1, 1);
        RangeTrie<TestRangeState> expected = RangeTrie.branch(branchKey, VERSION, covering);

        try (OnDiskRangeTrie<TestRangeState> actual = TrieUtil.onDiskRoundtrip(expected))
        {
            TrieUtil.assertTriesEqual(expected, actual);
        }
    }

    /// A range spanning every consecutive pair of the given transitions, over the keys [#key] builds.
    private static List<TestRangeState> markers(int prefixLength, int[] transitions, int suffixLength)
    {
        List<TestRangeState> markers = new ArrayList<>();
        for (int i = 0; i + 1 < transitions.length; i += 2)
        {
            int value = i / 2 + 1;
            markers.add(new TestRangeState(key(prefixLength, transitions[i], suffixLength), -1, value));
            markers.add(new TestRangeState(key(prefixLength, transitions[i + 1], suffixLength), value, -1));
        }
        TestRangeState.verify(markers);
        return markers;
    }

    /// A key of `prefixLength` bytes of 0x21, the given transition, and `suffixLength` bytes of 0x42.
    /// A negative transition gives the prefix alone, i.e. a key that stops above the wide node.
    private static ByteComparable.Preencoded key(int prefixLength, int transition, int suffixLength)
    {
        byte[] bytes = new byte[transition < 0 ? prefixLength : prefixLength + 1 + suffixLength];
        Arrays.fill(bytes, 0, prefixLength, (byte) 0x21);
        if (transition >= 0)
        {
            bytes[prefixLength] = (byte) transition;
            Arrays.fill(bytes, prefixLength + 1, bytes.length, (byte) 0x42);
        }
        return ByteComparable.preencoded(VERSION, bytes);
    }
}
