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
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertTriesEqual;

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
