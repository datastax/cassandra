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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertTriesEqual;
import static org.apache.cassandra.io.util.RandomAccessReader.DEFAULT_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// Round-trips deletion-aware tries through [DeletionAwareFileWriter] and
/// [OnDiskDeletionAwareTrie], which is the only real check that the deletion-branch pointer
/// written into the payload is recovered and that the branch bytes land where the pointer says.
public class OnDiskDeletionAwareTrieTest
{
    @BeforeClass
    public static void setUp()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// [LivePoint] and [DeletionMarker] both carry their own position, so the key bytes have to be
    /// serialized alongside the values for the round trip to compare equal.
    static class LiveSerDe implements FileWriter.DataSerializer<LivePoint>, OnDiskCursor.DataDeserializer<LivePoint>
    {
        @Override
        public int serializedSize(LivePoint value)
        {
            return 4 + 4 + positionBytes(value.position).length;
        }

        @Override
        public int serialize(DataOutputPlus out, LivePoint value) throws IOException
        {
            byte[] pos = positionBytes(value.position);
            out.writeInt(value.timestamp);
            out.writeInt(pos.length);
            out.write(pos);
            return serializedSize(value);
        }

        @Override
        public LivePoint deserialize(DataInputPlus rdr, int length) throws IOException
        {
            int timestamp = rdr.readInt();
            byte[] pos = new byte[rdr.readInt()];
            rdr.readFully(pos);
            return new LivePoint(ByteComparable.preencoded(VERSION, pos), timestamp);
        }
    }

    static class MarkerSerDe implements FileWriter.DataSerializer<DeletionMarker>, OnDiskCursor.DataDeserializer<DeletionMarker>
    {
        @Override
        public int serializedSize(DeletionMarker value)
        {
            return 4 + 4 + 4 + positionBytes(value.position).length;
        }

        @Override
        public int serialize(DataOutputPlus out, DeletionMarker value) throws IOException
        {
            byte[] pos = positionBytes(value.position);
            out.writeInt(value.leftSide);
            out.writeInt(value.rightSide);
            out.writeInt(pos.length);
            out.write(pos);
            return serializedSize(value);
        }

        @Override
        public DeletionMarker deserialize(DataInputPlus rdr, int length) throws IOException
        {
            int left = rdr.readInt();
            int right = rdr.readInt();
            byte[] pos = new byte[rdr.readInt()];
            rdr.readFully(pos);
            return new DeletionMarker(ByteComparable.preencoded(VERSION, pos), left, right);
        }
    }

    static byte[] positionBytes(ByteComparable position)
    {
        return ByteSourceInverse.readBytes(position.asComparableBytes(VERSION));
    }

    static final LiveSerDe LIVE = new LiveSerDe();
    static final MarkerSerDe MARKER = new MarkerSerDe();

    private void assertRoundTrips(List<DataPoint> points) throws IOException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points);

        File file = new File(java.io.File.createTempFile("deletionaware", ".trie"));
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            DeletionAwareFileWriter.write(source, false, LIVE, MARKER, writer);
            writer.finish();
        }

        try (OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                 OnDiskDeletionAwareTrie.open(file, LIVE, MARKER, VERSION, -1))
        {
            assertTriesEqual(source, read);
        }

        assertRoundTripsInMemory(source);
    }

    /// The same round trip without a file, which is how the commit log and messaging will use this:
    /// serialize into a buffer, then read the trie straight back out of that buffer.
    private void assertRoundTripsInMemory(InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DeletionAwareFileWriter.write(source, false, LIVE, MARKER, out);
            ByteBuffer buffer = out.asNewBuffer();

            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(buffer, LIVE, MARKER, VERSION, -1);
            assertTriesEqual(source, read);
            read.close();
        }
    }

    @Test
    public void testLiveOnly() throws IOException
    {
        assertRoundTrips(points(live("abc", 1), live("abd", 2), live("xyz", 3)));
    }

    @Test
    public void testDeletionOnly() throws IOException
    {
        assertRoundTrips(points(marker("abc", -1, 5), marker("abd", 5, -1)));
    }

    /// The case the payload encoding exists for: a node carrying both live content and a branch.
    @Test
    public void testLiveAndDeletionsTogether() throws IOException
    {
        assertRoundTrips(points(live("abc", 1),
                                marker("abd", -1, 7),
                                live("abe", 2),
                                marker("abf", 7, -1),
                                live("xyz", 3)));
    }

    @Test
    public void testEmpty() throws IOException
    {
        assertRoundTrips(new ArrayList<>());
    }

    /// Ordering and pointer mistakes show up here rather than in the hand-built cases.
    @Test
    public void testRandomized() throws IOException
    {
        Random rand = new Random(1);
        for (int iter = 0; iter < 20; ++iter)
        {
            // Keys first, distinct and in order: the deletion chain below is only valid when
            // built in key order, since each marker's left side must equal the level active
            // when the walk reaches it.
            java.util.TreeSet<String> keys = new java.util.TreeSet<>();
            while (keys.size() < 40)
                keys.add(String.format("%04d", rand.nextInt(2000)));

            List<DataPoint> points = new ArrayList<>();
            int active = -1;
            for (String key : keys)
            {
                if (active == -1 && rand.nextBoolean())
                {
                    points.add(live(key, rand.nextInt(100)));
                }
                else
                {
                    // Open a range when none is active, otherwise close or change the active one.
                    int next = active == -1 ? rand.nextInt(100)
                                            : (rand.nextBoolean() ? -1 : rand.nextInt(100));
                    points.add(marker(key, active, next));
                    active = next;
                }
            }
            if (active != -1)
                points.add(marker(String.format("%04d", 9999), active, -1));  // close the last range

            assertRoundTrips(points);
        }
    }

    /// 30 transitions, a third of them at or above 0xE0, all diverging at the same byte so that the
    /// deletion branch they form has a bitmap node at its root. The `%04d` keys the tests above use
    /// only ever give a node ten children, i.e. always a sparse one.
    private static final int[] WIDE_TRANSITIONS =
    { 0x00, 0x01, 0x07, 0x0d, 0x1f, 0x32, 0x40, 0x4d, 0x64, 0x78, 0x7f, 0x96, 0xaa, 0xbe, 0xc8, 0xd2,
      0xdc, 0xdf, 0xe0, 0xe1, 0xe6, 0xeb, 0xf0, 0xf5, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff };

    /// Every transition. [OnDiskWriteNodeType#selectChildrenType] picks a dense node once a bitmap one
    /// would no longer save anything over it.
    private static int[] allTransitions()
    {
        int[] transitions = new int[256];
        for (int i = 0; i < transitions.length; ++i)
            transitions[i] = i;
        return transitions;
    }

    private static final int KEY_LENGTH = 5;

    /// A deletion branch over a wide alphabet: its root node is a bitmap one rather than the sparse
    /// nodes the `%04d` keys above can produce, and the deletion state that applies to a key is asked
    /// for, which the walks in [TrieUtil#assertTriesEqual] never do.
    @Test
    public void testWideDeletionBranch() throws IOException
    {
        testWideDeletionBranch(WIDE_TRANSITIONS);
    }

    /// The same with a dense branch root, which reads its children through a different node
    /// implementation.
    @Test
    public void testDenseDeletionBranch() throws IOException
    {
        testWideDeletionBranch(allTransitions());
    }

    private void testWideDeletionBranch(int[] transitions) throws IOException
    {
        List<DataPoint> points = wideBranchPoints(transitions, KEY_LENGTH);

        assertRoundTrips(points);

        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DeletionAwareFileWriter.write(source, false, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            for (int transition = 0; transition <= 0xFF; ++transition)
                for (int length = 3; length <= KEY_LENGTH + 1; ++length)
                    assertApplicableDeletionEqual(source, read, wideKey(transition, length));
            read.close();
        }
    }

    /// The range cursor over a deletion branch has a state machine that the walks above never drive:
    /// the tail it gives at a position, and the deletion that is active when that tail's branch is
    /// entered and left. Take the tail in both directions at every position of the branch and compare
    /// it against the one the in-memory trie the file was written from gives at the same place.
    @Test
    public void testTailsOfADeletionBranch() throws IOException
    {
        // One byte of key below the branching one, so that no position inside the branch falls in the
        // middle of a chain node; a tail taken there is a separate, open question.
        List<DataPoint> points = wideBranchPoints(WIDE_TRANSITIONS, 4);
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            DeletionAwareFileWriter.write(source, false, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            for (Direction direction : Direction.values())
                assertDeletionBranchTailsEqual(source.cursor(direction), read.cursor(direction));
            read.close();
        }
    }

    /// Two cursors are live over one trie at the same time: the parent walking the data trie, and the deletion
    /// branch cursor taken from it. Each holds on to the data it was given while the other reads elsewhere in the
    /// file. The trie built here deliberately spans more than one buffer, which is the condition the tests above
    /// miss -- theirs all fit in a single one, where any two cursors are handed the same bytes anyway.
    @Test
    public void testConcurrentCursorsOverALargeFile() throws IOException
    {
        List<DataPoint> points = largeMixedPoints(4000);
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points);

        File file = new File(java.io.File.createTempFile("deletionawarelarge", ".trie"));
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            DeletionAwareFileWriter.write(source, false, LIVE, MARKER, writer);
            writer.finish();
        }
        assertTrue("The trie must not fit in one buffer, was " + file.length() + " bytes",
                   file.length() > DEFAULT_BUFFER_SIZE);

        try (OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                 OnDiskDeletionAwareTrie.open(file, LIVE, MARKER, VERSION, -1))
        {
            assertTriesEqual(source, read);
            for (Direction direction : Direction.values())
                assertDeletionBranchesEqual(source.cursor(direction), read.cursor(direction));
        }
    }

    /// The randomized shape of [#testRandomized], scaled up so that the written trie takes many buffers and the
    /// deletion branches end up far from the data nodes that introduce them.
    private static List<DataPoint> largeMixedPoints(int count)
    {
        Random rand = new Random(5);
        java.util.TreeSet<String> keys = new java.util.TreeSet<>();
        while (keys.size() < count)
            keys.add(String.format("%06d", rand.nextInt(count * 4)));

        List<DataPoint> points = new ArrayList<>();
        int active = -1;
        for (String key : keys)
        {
            if (active == -1 && rand.nextBoolean())
            {
                points.add(live(key, rand.nextInt(100)));
            }
            else
            {
                int next;
                do
                {
                    next = active == -1 ? rand.nextInt(100)
                                        : (rand.nextBoolean() ? -1 : rand.nextInt(100));
                }
                while (next == active);
                points.add(marker(key, active, next));
                active = next;
            }
        }
        if (active != -1)
            points.add(marker(String.format("%06d", count * 4 + 1), active, -1));
        DataPoint.verify(points);
        return points;
    }

    /// Walks the data trie, and at every position walks to exhaustion the deletion branch it introduces, while the
    /// data cursor stays where it is. Deliberately does not take tails: those are covered by
    /// [#testTailsOfADeletionBranch], and mid-chain tails are a separate open question.
    private static void assertDeletionBranchesEqual(DeletionAwareCursor<LivePoint, DeletionMarker> expected,
                                                    DeletionAwareCursor<LivePoint, DeletionMarker> actual)
    {
        long position = expected.encodedPosition();
        assertEquals(Cursor.toString(position), Cursor.toString(actual.encodedPosition()));
        while (!Cursor.isExhausted(position))
        {
            Direction direction = Cursor.direction(position);
            RangeCursor<DeletionMarker> expectedBranch = expected.deletionBranchCursor(direction);
            RangeCursor<DeletionMarker> actualBranch = actual.deletionBranchCursor(direction);
            assertEquals("Deletion branch present", expectedBranch != null, actualBranch != null);
            if (expectedBranch != null)
                TrieUtil.assertCursorWalksEqual(expectedBranch, actualBranch);
            position = expected.advance();
            assertEquals(Cursor.toString(position), Cursor.toString(actual.advance()));
        }
    }

    private static void assertDeletionBranchTailsEqual(DeletionAwareCursor<LivePoint, DeletionMarker> expected,
                                                       DeletionAwareCursor<LivePoint, DeletionMarker> actual)
    {
        long position = expected.encodedPosition();
        assertEquals(Cursor.toString(position), Cursor.toString(actual.encodedPosition()));
        while (!Cursor.isExhausted(position))
        {
            Direction direction = Cursor.direction(position);
            RangeCursor<DeletionMarker> expectedBranch = expected.deletionBranchCursor(direction);
            RangeCursor<DeletionMarker> actualBranch = actual.deletionBranchCursor(direction);
            assertEquals("Deletion branch present", expectedBranch != null, actualBranch != null);
            if (expectedBranch != null)
                assertTailsEqual(expectedBranch, actualBranch);
            position = expected.advance();
            assertEquals(Cursor.toString(position), Cursor.toString(actual.advance()));
        }
    }

    /// Walks both branches in lockstep, taking the tail in both directions at every position.
    private static void assertTailsEqual(RangeCursor<DeletionMarker> expected, RangeCursor<DeletionMarker> actual)
    {
        long position = expected.encodedPosition();
        assertEquals(Cursor.toString(position), Cursor.toString(actual.encodedPosition()));
        while (!Cursor.isExhausted(position))
        {
            // Taking a tail on the return path is not permitted.
            if (!Cursor.isOnReturnPath(position))
                for (Direction tailDirection : Direction.values())
                    TrieUtil.assertCursorWalksEqual(expected.tailCursor(tailDirection),
                                                    actual.tailCursor(tailDirection));
            position = expected.advance();
            assertEquals(Cursor.toString(position), Cursor.toString(actual.advance()));
        }
    }

    /// A deletion spanning every consecutive pair of the given transitions, over the keys [#wideKey]
    /// builds. All of them diverge at the same byte, so they end up in one branch.
    private static List<DataPoint> wideBranchPoints(int[] transitions, int keyLength)
    {
        List<DataPoint> points = new ArrayList<>();
        for (int i = 0; i + 1 < transitions.length; i += 2)
        {
            int value = i / 2 + 1;
            points.add(new DeletionMarker(wideKey(transitions[i], keyLength), -1, value));
            points.add(new DeletionMarker(wideKey(transitions[i + 1], keyLength), value, -1));
        }
        DataPoint.verify(points);
        return points;
    }

    private static void assertApplicableDeletionEqual(DeletionAwareTrie<LivePoint, DeletionMarker> expected,
                                                      DeletionAwareTrie<LivePoint, DeletionMarker> actual,
                                                      ByteComparable key)
    {
        assertEquals("Applicable deletion at " + key.byteComparableAsString(VERSION),
                     expected.applicableDeletion(key),
                     actual.applicableDeletion(key));
    }

    /// The first `length` bytes of a key made of two fixed bytes, so that the deletion branch is not
    /// rooted at the trie's root, the given transition, then a fixed suffix. Shorter lengths give a
    /// probe that stops above or inside the branch.
    private static ByteComparable wideKey(int transition, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i)
            bytes[i] = i < 2 ? 0x21 : i == 2 ? (byte) transition : 0x42;
        return ByteComparable.preencoded(VERSION, bytes);
    }

    private static List<DataPoint> points(DataPoint... p)
    {
        List<DataPoint> list = new ArrayList<>();
        for (DataPoint x : p)
            list.add(x);
        return list;
    }

    private static LivePoint live(String key, int timestamp)
    {
        return new LivePoint(TrieUtil.comparable(key), timestamp);
    }

    private static DeletionMarker marker(String key, int left, int right)
    {
        return new DeletionMarker(TrieUtil.comparable(key), left, right);
    }
}
