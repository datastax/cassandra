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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChannelProxy;
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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Round-trips deletion-aware tries through [OnDiskDeletionAwareTrieWriter] and
/// [OnDiskDeletionAwareTrie], which is the only real check that the deletion-branch pointer
/// written into a node's ascent-side slot is recovered and that the branch bytes land where the
/// pointer says.
public class OnDiskDeletionAwareTrieTest
{
    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// [LivePoint] and [DeletionMarker] both carry their own position, so the key bytes have to be
    /// serialized alongside the values for the round trip to compare equal.
    static class LiveSerDe implements OnDiskTrieWriter.DataSerializer<LivePoint>, OnDiskCursor.DataDeserializer<LivePoint>
    {
        @Override
        public void serialize(DataOutputPlus out, LivePoint value) throws IOException
        {
            byte[] pos = positionBytes(value.position);
            out.writeInt(value.timestamp);
            out.writeInt(pos.length);
            out.write(pos);
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

    static class MarkerSerDe implements OnDiskTrieWriter.DataSerializer<DeletionMarker>, OnDiskCursor.DataDeserializer<DeletionMarker>
    {
        @Override
        public void serialize(DataOutputPlus out, DeletionMarker value) throws IOException
        {
            byte[] pos = positionBytes(value.position);
            out.writeInt(value.leftSide);
            out.writeInt(value.rightSide);
            out.writeInt(pos.length);
            out.write(pos);
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
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, writer);
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
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
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

    /// A node carrying both live content and a branch, which fills both content slots of a generic-content node.
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

    /// A deletion branch hanging off the root, i.e. a root node with an ascent-side slot and no descent-side one.
    /// Without the branch pointer in that slot, [OnDiskReadNodeType#PREFIX] would present a node's lone slot on the
    /// return path, which a pointer must never be.
    @Test
    public void testDeletionBranchAtRoot() throws IOException
    {
        List<DataPoint> points = points(marker("abc", -1, 5), marker("abd", 5, -1), live("xyz", 3));
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points, false, true);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            assertTriesEqual(source, read);
            for (Direction d : Direction.values())
                assertDeletionBranchesEqual(source.cursor(d), read.cursor(d));
            read.close();
        }
    }

    /// Live content presented on the return path has no slot to go to: the ascent-side one holds the branch pointer.
    /// An ordered plain trie wrapped as deletion-aware presents its content there when walked in reverse, as the writer
    /// walks. Writing it must fail rather than put the content where the reader expects a pointer; an unordered one,
    /// whose content is on the descent path, writes and reads back.
    @Test
    public void testLiveContentOnTheReturnPathIsRejected() throws IOException, TrieSpaceExhaustedException
    {
        for (boolean ordered : new boolean[]{ false, true })
        {
            InMemoryTrie<LivePoint> plain = ordered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
            for (String key : new String[]{ "a", "ab", "ac" })
            {
                LivePoint point = live(key, 1);
                plain.putRecursive(point.position, point, (x, y) -> y);
            }
            DeletionAwareTrie<LivePoint, DeletionMarker> wrapped = DeletionAwareTrie.wrap(plain);
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                if (ordered)
                {
                    assertThrows(IllegalArgumentException.class, () -> OnDiskDeletionAwareTrieWriter.write(wrapped, LIVE, MARKER, out));
                }
                else
                {
                    OnDiskDeletionAwareTrieWriter.write(wrapped, LIVE, MARKER, out);
                    assertTriesEqual(wrapped, OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1));
                }
            }
        }
    }

    /// The deletion-aware counterpart of [OnDiskTrieWriterTest#testWriteAtNonZeroOffset]: an empty trie reports root 0,
    /// which the reader takes as the empty trie, and a non-empty one the position the stream is left at.
    @Test
    public void testWriteAtNonZeroOffset() throws IOException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source =
            DataPoint.fromList(points(live("aa", 1), marker("abc", -1, 5), marker("abd", 5, -1), live("xyz", 3)));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeLong(0);
            long emptyRoot = OnDiskDeletionAwareTrieWriter.write(DataPoint.fromList(new ArrayList<>()), LIVE, MARKER, out);
            assertEquals(0, emptyRoot);
            assertEquals(8, out.getLength());

            long root = OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
            assertEquals(out.getLength(), root);

            ByteBuffer buffer = out.asNewBuffer();
            assertTriesEqual(DataPoint.fromList(new ArrayList<>()), OnDiskDeletionAwareTrie.open(buffer, LIVE, MARKER, VERSION, emptyRoot));
            assertTriesEqual(source, OnDiskDeletionAwareTrie.open(buffer, LIVE, MARKER, VERSION, root));
        }
    }

    /// The shape of a partition deletion: no live content at all, so the data trie is a single node that carries a
    /// deletion branch and has no children. Walking it, the cursor runs out of the data node at once, and the walks that
    /// drive several cursors in parallel read the exhausted position back from [Cursor#encodedPosition] rather than
    /// from the return value -- a cursor still reporting the node there makes them go round forever.
    @Test(timeout = 60_000)
    public void testMergedTrieOverChildlessBranchNode() throws IOException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(partitionDeletionPoints());
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            for (Direction direction : Direction.values())
                assertEquals(mergedValues(source, direction), mergedValues(read, direction));
            read.close();
        }
    }

    /// The same shape applied to an in-memory trie, which is how a deserialized partition deletion reaches a memtable.
    @Test(timeout = 60_000)
    public void testApplyChildlessBranchNode() throws IOException, TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(partitionDeletionPoints());
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);

            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> expected = applyTo(liveOnlyTrie(live("010", 1)), source);
            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> actual = applyTo(liveOnlyTrie(live("010", 1)), read);
            assertTriesEqual(expected, actual);
            read.close();
        }
    }

    /// The deletion-only shapes a partition update carries for a range tombstone, a row deletion and a partition
    /// deletion: no live content, and a deletion branch at the root, as partition updates keep them. Each is written,
    /// reopened and walked the ways the partition iterators walk it -- merged with its deletions, sliced by a set and
    /// split into tails -- and must give what the trie it was written from gives.
    @Test(timeout = 60_000)
    public void testDeletionOnlyShapes() throws IOException, TrieSpaceExhaustedException
    {
        DeletionMarker covering = new DeletionMarker(TrieUtil.directComparable("x"), 5, 5);
        ByteComparable prefix = TrieUtil.directComparable("ab");
        List<DeletionAwareTrie<LivePoint, DeletionMarker>> shapes = new ArrayList<>();
        // range tombstone
        shapes.add(DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, TrieUtil.directComparable("abc"), TrieUtil.directComparable("abe"), VERSION, covering));
        // row deletion
        shapes.add(DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, RangeTrie.branch(prefix, VERSION, covering)));
        // partition deletion
        shapes.add(DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, RangeTrie.branch(ByteComparable.EMPTY, VERSION, covering)));

        List<TrieSet> sets = new ArrayList<>();
        sets.add(TrieSet.full(VERSION));
        sets.add(TrieSet.range(VERSION, TrieUtil.directComparable("a"), true, TrieUtil.directComparable("b"), true));
        sets.add(TrieSet.range(VERSION, null, true, TrieUtil.directComparable("abd"), false));
        sets.add(TrieSet.range(VERSION, TrieUtil.directComparable("abd"), true, null, true));
        sets.add(TrieSet.branch(VERSION, prefix));

        // Each shape on its own, and next to a live row outside the deletion, so that the row iterators also skip past
        // a returned tail.
        LivePoint row = new LivePoint(TrieUtil.directComparable("zz"), 1);
        List<InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>> sources = new ArrayList<>();
        for (DeletionAwareTrie<LivePoint, DeletionMarker> shape : shapes)
        {
            sources.add(applyTo(InMemoryDeletionAwareTrie.shortLived(VERSION), shape));
            sources.add(applyTo(applyTo(InMemoryDeletionAwareTrie.shortLived(VERSION), shape),
                                DeletionAwareTrie.singleton(row.position, VERSION, row)));
        }

        for (InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source : sources)
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
                OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                    OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
                assertTriesEqual(source, read);
                for (TrieSet set : sets)
                {
                    DeletionAwareTrie<LivePoint, DeletionMarker> expected = source.intersect(set);
                    DeletionAwareTrie<LivePoint, DeletionMarker> actual = read.intersect(set);
                    for (Direction direction : Direction.values())
                    {
                        List<String> expectedValues = mergedValues(expected, direction);
                        assertEquals(expectedValues, mergedValues(actual, direction));
                        List<String> expectedSwitchableValues = switchableMergedValues(expected, direction);
                        assertEquals(expectedSwitchableValues, switchableMergedValues(actual, direction));
                        List<String> expectedTails = mergedTails(expected, direction);
                        assertEquals(expectedTails, mergedTails(actual, direction));
                    }
                }
                read.close();
            }
        }
    }

    private static List<String> switchableMergedValues(DeletionAwareTrie<LivePoint, DeletionMarker> trie, Direction direction)
    {
        List<String> values = new ArrayList<>();
        trie.mergedTrieSwitchable((live, deletion) -> live + "|" + deletion).values(direction).forEach(values::add);
        return values;
    }

    /// The tails of the switchable merged view at live content, the way the partition row iterators split a partition
    /// into rows. (A tail cannot be taken at a deletion boundary, which may be on the return path.)
    private static List<String> mergedTails(DeletionAwareTrie<LivePoint, DeletionMarker> trie, Direction direction)
    {
        List<String> tails = new ArrayList<>();
        for (Map.Entry<ByteComparable.Preencoded, Trie<String>> entry :
             trie.mergedTrieSwitchable((live, deletion) -> live != null ? live + "|" + deletion : null)
                 .tailTries(direction, x -> true))
        {
            List<String> values = new ArrayList<>();
            entry.getValue().values(direction).forEach(values::add);
            tails.add(entry.getKey().byteComparableAsString(VERSION) + " -> " + values);
        }
        return tails;
    }

    private static List<DataPoint> partitionDeletionPoints()
    {
        return points(marker("006", -1, 90), marker("032", 90, -1));
    }

    private static List<String> mergedValues(DeletionAwareTrie<LivePoint, DeletionMarker> trie, Direction direction)
    {
        List<String> values = new ArrayList<>();
        trie.mergedTrie((live, deletion) -> live + "|" + deletion).values(direction).forEach(values::add);
        return values;
    }

    private static InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> applyTo(InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> target,
                                                                              DeletionAwareTrie<LivePoint, DeletionMarker> mutation)
    throws TrieSpaceExhaustedException
    {
        target.mutator(DataPoint::combineLive,
                       DataPoint::combineDeletion,
                       DataPoint::deleteLive,
                       DataPoint::deleteLive,
                       false,
                       v -> false)
              .apply(mutation);
        return target;
    }

    /// A deletion branch the walk enters but which turns out to have nothing to write. `intersect` gives such a
    /// branch when only a prefix of the branch's data is matched by the set: the walk descends into the branch,
    /// which the set covers at that point, and then finds no marker below.
    ///
    /// [OnDiskDeletionAwareTrieWriter#write] must record no root for it. The position it would otherwise
    /// record is that of whatever node was written last -- here the live point's, written before the walk
    /// reaches the branch -- and the reader has no way to tell that apart from a real root: it sets
    /// [Cursor#MAY_HAVE_DELETION_BRANCH_BIT] and decodes the byte there as a node code.
    @Test
    public void testDeletionBranchEmptiedByIntersection() throws IOException, TrieSpaceExhaustedException
    {
        // Raw keys, because one has to be a prefix of another and TrieUtil.comparable appends a terminator.
        LivePoint live = new LivePoint(TrieUtil.directComparable("zzzz"), 1);
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = liveOnlyTrie(live);
        source.mutator(DataPoint::combineLive, DataPoint::combineDeletion, DataPoint::deleteLive,
                       DataPoint::deleteLive, false, v -> false)
              .apply(DeletionAwareTrie.deletedRange(TrieUtil.directComparable("pre"),
                                                    TrieUtil.directComparable("abc"),
                                                    TrieUtil.directComparable("abd"),
                                                    VERSION,
                                                    new DeletionMarker(TrieUtil.directComparable("preabc"), 7, 7)));

        // "preabqu" shares "preab" with the branch's only path and diverges at the byte after it, so the branch is
        // entered and produces nothing. "zzz" keeps the live point, whose node the writer emits before it gets to
        // the branch, and which is therefore what an empty branch would report as its root.
        DeletionAwareTrie<LivePoint, DeletionMarker> intersected =
            source.intersect(TrieSet.branch(VERSION, TrieUtil.directComparable("preabqu"))
                                    .union(TrieSet.branch(VERSION, TrieUtil.directComparable("zzz"))));
        assertHasEmptyDeletionBranch(intersected);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(intersected, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            assertNoDeletionBranchRecorded(read);
            // A branch that writes nothing leaves no node behind either, so what is left is the live point alone.
            assertTriesEqual(liveOnlyTrie(live), read);
            read.close();
        }
    }

    private static InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> liveOnlyTrie(LivePoint live)
    throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        trie.mutator(DataPoint::combineLive, DataPoint::combineDeletion, DataPoint::deleteLive,
                     DataPoint::deleteLive, false, v -> false)
            .apply(DeletionAwareTrie.singleton(live.position, VERSION, live));
        return trie;
    }

    /// The premise of the test above: the trie really does present a deletion branch, and walking it really does
    /// yield no marker. Without this the test would still pass if `intersect` stopped producing such a branch, and
    /// it would no longer be testing anything.
    private static void assertHasEmptyDeletionBranch(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        int emptyBranches = 0;
        try (DeletionAwareCursor<LivePoint, DeletionMarker> cursor = trie.cursor(Direction.REVERSE))
        {
            long position = cursor.encodedPosition();
            while (!Cursor.isExhausted(position))
            {
                RangeCursor<DeletionMarker> branch = DeletionAwareCursor.deletionBranchCursor(cursor, position);
                if (branch != null)
                {
                    try
                    {
                        long branchPosition = branch.encodedPosition();
                        while (!Cursor.isExhausted(branchPosition))
                        {
                            assertNull("Deletion branch must have no content", branch.content());
                            branchPosition = branch.advance();
                        }
                        ++emptyBranches;
                    }
                    finally
                    {
                        branch.close();
                    }
                }
                position = cursor.advance();
            }
        }
        assertEquals("Empty deletion branches presented", 1, emptyBranches);
    }

    /// No node of the trie read back may claim a deletion branch. Comparing the walks alone would not catch a root
    /// recorded for an empty branch, since a pointer to an unrelated node can decode into something consistent.
    private static void assertNoDeletionBranchRecorded(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        for (Direction direction : Direction.values())
        {
            try (DeletionAwareCursor<LivePoint, DeletionMarker> cursor = trie.cursor(direction))
            {
                long position = cursor.encodedPosition();
                while (!Cursor.isExhausted(position))
                {
                    assertEquals("Deletion branch flagged at " + Cursor.toString(position),
                                 0L, position & Cursor.MAY_HAVE_DELETION_BRANCH_BIT);
                    RangeCursor<DeletionMarker> branch = cursor.deletionBranchCursor(Cursor.direction(position));
                    if (branch != null)
                    {
                        branch.close();
                        fail("Deletion branch present at " + Cursor.toString(position));
                    }
                    position = cursor.advance();
                }
            }
        }
    }

    /// A node with content and no deletion branch must cost exactly what a plain trie's node costs: the branch
    /// pointer occupies the ascent-side content slot, which live-only data never fills. Writing the same keys and
    /// values both ways must therefore produce the same bytes.
    @Test
    public void testLiveOnlyMatchesPlainTrieByteForByte() throws IOException, TrieSpaceExhaustedException
    {
        Random rand = new Random(3);
        java.util.TreeSet<String> keys = new java.util.TreeSet<>();
        while (keys.size() < 200)
            keys.add(String.format("%04d", rand.nextInt(2000)));

        List<DataPoint> points = new ArrayList<>();
        InMemoryTrie<LivePoint> plain = InMemoryTrie.shortLived(VERSION);
        for (String key : keys)
        {
            LivePoint point = live(key, rand.nextInt(100));
            points.add(point);
            plain.putRecursive(TrieUtil.comparable(key), point, (x, y) -> y);
        }

        File deletionAwareFile = new File(java.io.File.createTempFile("deletionawarelive", ".trie"));
        try (SequentialWriter writer = new SequentialWriter(deletionAwareFile))
        {
            OnDiskDeletionAwareTrieWriter.write(DataPoint.fromList(points), LIVE, MARKER, writer);
            writer.finish();
        }

        File plainFile = new File(java.io.File.createTempFile("plainlive", ".trie"));
        OnDiskTrieWriter.write(plain, false, LIVE, plainFile);

        assertArrayEquals("Live-only data must serialize identically with and without deletion awareness",
                          Files.readAllBytes(plainFile.toPath()),
                          Files.readAllBytes(deletionAwareFile.toPath()));
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
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);
            for (int transition = 0; transition <= 0xFF; ++transition)
                for (int length = 3; length <= KEY_LENGTH + 1; ++length)
                    assertApplicableDeletionEqual(source, read, wideKey(transition, length));
            read.close();
        }
    }

    /// Every pointer in the format is a backward delta from the node that holds it, so a serialized trie does not
    /// depend on where it starts and can be read from any offset it is embedded at -- the property that lets a
    /// branch be copied unchanged. The deletion branch pointer used to be the one absolute position in the format;
    /// shifting the bytes moved every node but left that pointer aiming at the old place. The larger shift also
    /// makes an absolute position need a wider integer than the unshifted one.
    @Test
    public void testDeletionBranchesSurviveRelocation() throws IOException
    {
        List<InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>> sources = new ArrayList<>();
        sources.add(DataPoint.fromList(points(live("abc", 1),
                                              marker("abd", -1, 7),
                                              live("abe", 2),
                                              marker("abf", 7, -1),
                                              live("xyz", 3))));
        sources.add(DataPoint.fromList(points(marker("abc", -1, 5), marker("abd", 5, -1), live("xyz", 3)),
                                       false, true));
        sources.add(DataPoint.fromList(wideBranchPoints(WIDE_TRANSITIONS, KEY_LENGTH)));
        sources.add(DataPoint.fromList(wideBranchPoints(allTransitions(), KEY_LENGTH)));

        for (InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source : sources)
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
                ByteBuffer serialized = out.asNewBuffer();
                for (int shift : new int[]{ 7, 300 })
                {
                    ByteBuffer shifted = ByteBuffer.allocate(shift + serialized.remaining());
                    for (int i = 0; i < shift; ++i)
                        shifted.put((byte) (i * 31 + 17));
                    shifted.put(serialized.duplicate());
                    shifted.flip();

                    // The root node ends where the buffer does, as it did before the shift.
                    OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                        OnDiskDeletionAwareTrie.open(shifted, LIVE, MARKER, VERSION, -1);
                    assertTriesEqual(source, read);
                    for (Direction d : Direction.values())
                        assertDeletionBranchesEqual(source.cursor(d), read.cursor(d));
                    read.close();
                }
            }
        }
    }

    /// A deletion branch pointer whose delta would put the branch root at or before the start of the data must be
    /// rejected as corruption, not followed to a position the reader cannot seek to. The bytes can be a corrupt
    /// commit-log record or message payload.
    ///
    /// The trie has its deletion branch at the root and no live content there, so the root node is a prefix node
    /// with an ascent-side slot and no descent-side one, and it ends the buffer:
    /// `... <pointer> <slot length 01> <node code>`. The trie is small enough for the pointer to take one byte.
    @Test
    public void testCorruptDeletionBranchPointerThrows() throws IOException
    {
        List<DataPoint> points = points(marker("abc", -1, 5), marker("abd", 5, -1), live("xyz", 3));
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points, false, true);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
            ByteBuffer buffer = out.asNewBuffer();
            int codePos = buffer.limit() - 1;
            int code = buffer.get(codePos) & 0xFF;
            assertEquals("root is a prefix node", OnDiskWriteNodeType.PREFIX.bits, code & ~0x7);
            assertEquals("root has an ascent-side slot", OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT,
                         code & OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT);
            assertEquals("root has no descent-side slot", 0, code & OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT);
            assertEquals("pointer is one byte", 1, buffer.get(codePos - 1));
            int pointerPos = codePos - 2;
            int slotStart = pointerPos;
            assertTrue("slot start must fit a one-byte delta", slotStart <= 0xFF);

            // Delta slotStart would put the root at 0, where no branch can end; anything larger is negative.
            for (int delta : new int[]{ slotStart, 0xFF })
            {
                ByteBuffer corrupt = buffer.duplicate();
                corrupt.put(pointerPos, (byte) delta);
                OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                    OnDiskDeletionAwareTrie.open(corrupt, LIVE, MARKER, VERSION, -1);
                // With verification on, taking the cursor already reads the branch pointer.
                UncheckedIOException e = assertThrows(UncheckedIOException.class,
                                                      () -> read.cursor(Direction.FORWARD).deletionBranchCursor(Direction.FORWARD));
                assertTrue(e.getMessage(), e.getMessage().contains("Corrupt serialized trie"));
                read.close();
            }
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
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, out);
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
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, writer);
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

    /// [BaseTrie#tailTrie] hands back a trie that keeps a cursor as the position to make its cursors from, and has
    /// no close of its own for the caller to reach that cursor with. The cursor must therefore be released before the
    /// trie is returned, and must still produce working tails afterwards.
    ///
    /// The deletion-aware form is the intricate one: when the descent passes a deletion branch, the tail is built out
    /// of a data cursor and a deletion cursor combined, and every cursor made along the way is ours to release. The
    /// probes below are prefixes of the trie's own keys, so they stop above, at and below the points where the
    /// branches hang.
    @Test
    public void testTailTrieReleasesItsCursor() throws IOException
    {
        List<DataPoint> points = largeMixedPoints(600);
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points);

        File file = new File(java.io.File.createTempFile("deletionawaretails", ".trie"));
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, writer);
            writer.finish();
        }

        ChannelProxy channel = new ChannelProxy(file);
        TrieUtil.CountingRebuffererFactory factory =
            new TrieUtil.CountingRebuffererFactory(OnDiskBaseTrie.openChunkReader(channel));
        try (OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                 new OnDiskDeletionAwareTrie<>(factory, LIVE, MARKER, VERSION, factory.fileLength(), channel))
        {
            for (DataPoint point : points)
            {
                byte[] key = positionBytes(point.position());
                for (int length = 0; length <= key.length; ++length)
                {
                    ByteComparable prefix = ByteComparable.preencoded(VERSION, key, 0, length);
                    int outstandingBefore = factory.outstanding;
                    DeletionAwareTrie<LivePoint, DeletionMarker> tail = read.tailTrie(prefix);
                    assertEquals("tailTrie must not keep a rebufferer, at " + prefix.byteComparableAsString(VERSION),
                                 outstandingBefore, factory.outstanding);
                    if (tail != null)
                        walkAndClose(tail.cursor(Direction.FORWARD));
                    assertEquals("Walking a tail must give its rebufferers back, at " + prefix.byteComparableAsString(VERSION),
                                 outstandingBefore, factory.outstanding);
                }
            }
        }
    }

    /// Every way of reading a deletion-aware trie that does not hand a cursor to its caller must give back the buffers
    /// of the cursors it takes, including those of the deletion branches it visits, once the walk has run to its end.
    @Test
    public void testReadsReleaseTheirCursors() throws IOException
    {
        List<DataPoint> points = points(live("aa", 1), marker("abc", -1, 5), marker("abd", 5, -1), live("abz", 3),
                                        marker("xbc", -1, 6), marker("xbd", 6, -1), live("xyz", 3));
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> source = DataPoint.fromList(points);
        File file = new File(java.io.File.createTempFile("deletionawarerelease", ".trie"));
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            OnDiskDeletionAwareTrieWriter.write(source, LIVE, MARKER, writer);
            writer.finish();
        }

        ChannelProxy channel = new ChannelProxy(file);
        TrieUtil.CountingRebuffererFactory factory =
            new TrieUtil.CountingRebuffererFactory(OnDiskBaseTrie.openChunkReader(channel));
        try (OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                 new OnDiskDeletionAwareTrie<>(factory, LIVE, MARKER, VERSION, factory.fileLength(), channel))
        {
            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> rootDeletion = DataPoint.fromList(points, false, true);
            Map<String, Runnable> reads = new LinkedHashMap<>();
            for (Direction direction : Direction.values())
            {
                reads.put("values " + direction, () -> read.values(direction).forEach(v -> {}));
                reads.put("entrySet " + direction, () -> read.entrySet(direction).forEach(e -> {}));
                reads.put("forEachEntry " + direction, () -> read.forEachEntry(direction, (k, v) -> {}));
                reads.put("mergedTrie values " + direction, () -> mergedValues(read, direction));
                reads.put("deletionOnlyTrie values " + direction, () -> read.deletionOnlyTrie().values(direction).forEach(v -> {}));
                reads.put("tailTries " + direction, () -> read.tailTries(direction, v -> true)
                                                              .forEach(e -> mergedValues(e.getValue(), direction)));
                reads.put("mergeWith values " + direction,
                          () -> mergedValues(read.mergeWith(source, DataPoint::combineLive, DataPoint::combineDeletion, DataPoint::deleteLive, false),
                                             direction));
                reads.put("mergeWithDeletion values " + direction,
                          () -> mergedValues(read.mergeWithDeletion(rootDeletion.deletionBranchAtRoot(), DataPoint::deleteLive, DataPoint::combineDeletion, false),
                                             direction));
            }
            reads.put("dump", read::dump);
            reads.put("deletionAtRoot", read::deletionAtRoot);
            reads.put("deletionBranchAtRoot values", () -> read.deletionBranchAtRoot().values().forEach(v -> {}));
            reads.put("mergeWithDeletion from file", () -> mergedValues(source.mergeWithDeletion(read.deletionOnlyTrie(), DataPoint::deleteLive, DataPoint::combineDeletion, false),
                                                                        Direction.FORWARD));
            reads.put("write", () -> {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    OnDiskDeletionAwareTrieWriter.write(read, LIVE, MARKER, out);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });
            reads.put("apply", () -> {
                try
                {
                    applyTo(InMemoryDeletionAwareTrie.shortLived(VERSION), read);
                }
                catch (TrieSpaceExhaustedException e)
                {
                    throw new AssertionError(e);
                }
            });

            List<String> leaking = new ArrayList<>();
            for (Map.Entry<String, Runnable> r : reads.entrySet())
            {
                int before = factory.outstanding;
                r.getValue().run();
                if (factory.outstanding != before)
                    leaking.add(r.getKey() + " (" + (factory.outstanding - before) + ")");
            }
            assertEquals("Reads leaving rebufferers outstanding", new ArrayList<>(), leaking);
        }
    }

    /// A mutator is kept for further mutations -- a partition updater holds one per shard -- so once a mutation has
    /// been applied it must not keep any of its cursors, including the deletion branch cursors handed to the mutators
    /// it uses for the branches. A closed cursor over a file-backed trie still references that trie.
    @Test
    public void testMutatorDoesNotKeepTheMutationCursors() throws IOException, TrieSpaceExhaustedException
    {
        List<DataPoint> points = points(live("aa", 1), marker("abc", -1, 5), marker("abd", 5, -1), live("abz", 3),
                                        marker("xbc", -1, 6), marker("xbd", 6, -1), live("xyz", 3));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(DataPoint.fromList(points), LIVE, MARKER, out);
            OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(), LIVE, MARKER, VERSION, -1);

            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> target = InMemoryDeletionAwareTrie.shortLived(VERSION);
            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>.Mutator<LivePoint, DeletionMarker> mutator =
                target.mutator(DataPoint::combineLive,
                               DataPoint::combineDeletion,
                               DataPoint::deleteLive,
                               DataPoint::deleteLive,
                               false,
                               v -> false);
            mutator.apply(read);
            assertNull("data mutation cursor", mutator.mutationCursor);
            assertNull("deletion branch cursor applied to the data", mutator.deleter.mutationCursor);
            assertNull("deletion branch cursor merged into the deletions", mutator.deletionMutator.mutationCursor);
            read.close();
        }
    }

    /// Corrupt bytes, as a damaged commit-log record or message payload would present them, must be reported as
    /// corruption, as [OnDiskTrieTest#testCorruptTriesFailAsCorruption] checks for plain tries: a walk over them must
    /// neither hang nor fail with any other exception. The walk here also skips, closes cursors early and takes tails
    /// of closed ones, and walks every deletion branch it meets the same way, asking for the active state on the way.
    @Test
    public void testCorruptTriesFailAsCorruption() throws IOException
    {
        final int maxSteps = 1_000_000;
        Random rand = new Random(7);
        int rejected = 0;
        for (int iteration = 0; iteration < 500; ++iteration)
        {
            byte[] bytes;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                OnDiskDeletionAwareTrieWriter.write(DataPoint.fromList(mixedPoints(rand, 1 + rand.nextInt(30))), LIVE_UNTRUSTED, MARKER_UNTRUSTED, out);
                bytes = out.toByteArray();
            }
            // Over the bytes as written the walk must go through, which also shows that the skips it makes are valid.
            walkRandomly(rand, bytes, maxSteps);

            int flips = 1 + rand.nextInt(3);
            for (int i = 0; i < flips; ++i)
                bytes[rand.nextInt(bytes.length)] = (byte) rand.nextInt(256);
            try
            {
                walkRandomly(rand, bytes, maxSteps);
            }
            catch (UncheckedIOException e)
            {
                assertTrue(e.getMessage(), e.getCause() instanceof IOException);
                ++rejected;
            }
        }
        // Make sure the test is not passing because no corruption is detected. Fewer are than for plain tries: the keys
        // here are strings of six digits, and a changed byte in a chain is a different key rather than corruption.
        assertTrue("Only " + rejected + " corrupt tries rejected", rejected > 50);
    }

    private static void walkRandomly(Random rand, byte[] bytes, int maxSteps)
    {
        OnDiskDeletionAwareTrie<LivePoint, DeletionMarker> trie =
            OnDiskDeletionAwareTrie.open(ByteBuffer.wrap(bytes), LIVE_UNTRUSTED, MARKER_UNTRUSTED, VERSION, -1);
        Direction direction = rand.nextBoolean() ? Direction.FORWARD : Direction.REVERSE;
        int[] steps = new int[1];
        // Not verified: corrupt bytes can list transitions out of order, which the reader does not check.
        TrieUtil.walkRandomlyAndClose(rand, trie.makeCursor(direction), steps, maxSteps, (cursor, position) -> {
            RangeCursor<DeletionMarker> branch = DeletionAwareCursor.deletionBranchCursor(cursor, position);
            if (branch != null)
                TrieUtil.walkRandomlyAndClose(rand, branch, steps, maxSteps, (b, p) -> b.state());
        });
    }

    /// Content for walks over corrupt data: only the values, so that most of the bytes that get corrupted are the trie's
    /// own, and read without trusting a length inside them, as [LiveSerDe] and [MarkerSerDe] do -- a corrupt one there
    /// would ask for an array of any size. The content's bytes are taken out of the reader's stream as given; getting
    /// them right is the reader's job.
    static class UntrustedLiveSerDe implements OnDiskTrieWriter.DataSerializer<LivePoint>, OnDiskCursor.DataDeserializer<LivePoint>
    {
        @Override
        public void serialize(DataOutputPlus out, LivePoint value) throws IOException
        {
            out.writeInt(value.timestamp);
        }

        @Override
        public LivePoint deserialize(DataInputPlus rdr, int length) throws IOException
        {
            return new LivePoint(ByteComparable.EMPTY, readContent(rdr, length, 4).getInt());
        }
    }

    static class UntrustedMarkerSerDe implements OnDiskTrieWriter.DataSerializer<DeletionMarker>, OnDiskCursor.DataDeserializer<DeletionMarker>
    {
        @Override
        public void serialize(DataOutputPlus out, DeletionMarker value) throws IOException
        {
            out.writeInt(value.leftSide);
            out.writeInt(value.rightSide);
        }

        @Override
        public DeletionMarker deserialize(DataInputPlus rdr, int length) throws IOException
        {
            ByteBuffer content = readContent(rdr, length, 8);
            return new DeletionMarker(ByteComparable.EMPTY, content.getInt(), content.getInt());
        }
    }

    private static ByteBuffer readContent(DataInputPlus rdr, int length, int minLength) throws IOException
    {
        byte[] bytes = new byte[length];
        rdr.readFully(bytes);
        if (length < minLength)
            throw new IOException("Content of " + length + " bytes, at least " + minLength + " expected");
        return ByteBuffer.wrap(bytes);
    }

    static final UntrustedLiveSerDe LIVE_UNTRUSTED = new UntrustedLiveSerDe();
    static final UntrustedMarkerSerDe MARKER_UNTRUSTED = new UntrustedMarkerSerDe();

    /// Walks the cursor and every deletion branch it presents to the end, then gives all of them back. The trie a
    /// [BaseTrie#tailTrie] hands out makes its cursors from a closed one, so a mistake in that hand-over surfaces
    /// here as a read through a buffer that has already gone back to the pool.
    private static void walkAndClose(DeletionAwareCursor<LivePoint, DeletionMarker> cursor)
    {
        try
        {
            long position = cursor.encodedPosition();
            while (!Cursor.isExhausted(position))
            {
                RangeCursor<DeletionMarker> branch = DeletionAwareCursor.deletionBranchCursor(cursor, position);
                if (branch != null)
                {
                    try
                    {
                        long branchPosition = branch.encodedPosition();
                        while (!Cursor.isExhausted(branchPosition))
                            branchPosition = branch.advance();
                    }
                    finally
                    {
                        branch.close();
                    }
                }
                position = cursor.advance();
            }
        }
        finally
        {
            cursor.close();
        }
    }

    /// The randomized shape of [#testRandomized], scaled up so that the written trie takes many buffers and the
    /// deletion branches end up far from the data nodes that introduce them.
    private static List<DataPoint> largeMixedPoints(int count)
    {
        return mixedPoints(new Random(5), count);
    }

    private static List<DataPoint> mixedPoints(Random rand, int count)
    {
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

    /// Walks the data trie, and at every position walks to exhaustion the deletion branch it introduces and the tails
    /// of both, while the data cursor stays where it is. The keys here are long enough for the walk to stop in the
    /// middle of a chain node, where the tail's root has no code byte of its own in the file; the shorter keys used
    /// by [#testTailsOfADeletionBranch] never land there.
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
                assertTailsEqual(expectedBranch, actualBranch);
            if (!Cursor.isOnReturnPath(position))
                for (Direction tailDirection : Direction.values())
                    TrieUtil.assertCursorWalksEqual(expected.tailCursor(tailDirection),
                                                    actual.tailCursor(tailDirection));
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

    /// Walks both branches in lockstep, taking the tail in both directions at every position. A tail must also start
    /// with the same active deletion: the walk does not compare states, and the deletion a tail reports at its root is
    /// what a merge applies to everything below it.
    private static void assertTailsEqual(RangeCursor<DeletionMarker> expected, RangeCursor<DeletionMarker> actual)
    {
        long position = expected.encodedPosition();
        assertEquals(Cursor.toString(position), Cursor.toString(actual.encodedPosition()));
        while (!Cursor.isExhausted(position))
        {
            // Taking a tail on the return path is not permitted.
            if (!Cursor.isOnReturnPath(position))
                for (Direction tailDirection : Direction.values())
                {
                    RangeCursor<DeletionMarker> expectedTail = expected.tailCursor(tailDirection);
                    RangeCursor<DeletionMarker> actualTail = actual.tailCursor(tailDirection);
                    assertEquals("State at the root of the tail at " + Cursor.toString(position),
                                 expectedTail.state(), actualTail.state());
                    TrieUtil.assertCursorWalksEqual(expectedTail, actualTail);
                }
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
