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
import java.util.Arrays;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/// Covers the back-to-front integer decoding of [OnDiskCursor], which the round-trip tests only reach
/// for the small content sizes they produce, its behaviour on lengths that a corrupt commit-log
/// record or message payload can present, and the parts of the cursor contract that a walk comparison
/// against the in-memory trie does not check.
public class OnDiskCursorTest
{
    @BeforeClass
    public static void setUp()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// One value per encoded length the writer can produce, on both sides of every length boundary,
    /// including the nine-byte encoding, which no content can be long enough to need but which a
    /// corrupt leading byte still asks the reader to decode.
    private static final long[] VALUES =
    { 0, 1, 127, 128, (1L << 14) - 1, 1L << 14, (1L << 21) - 1, 1L << 21, (1L << 28) - 1, 1L << 28,
      (1L << 35) - 1, 1L << 35, (1L << 42) - 1, 1L << 42, (1L << 49) - 1, 1L << 49, (1L << 56) - 1,
      1L << 56, Long.MAX_VALUE };

    @Test
    public void testVIntRoundTrip() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long[] ends = new long[VALUES.length];
            for (int i = 0; i < VALUES.length; ++i)
            {
                FileWriter.writeReversedVint(out, VALUES[i]);
                ends[i] = out.position();
            }

            OnDiskCursor<Void> cursor = cursorOver(out);
            for (int i = 0; i < VALUES.length; ++i)
            {
                int vintLength = cursor.readVIntLength(ends[i]);
                assertEquals("Encoded length of " + VALUES[i],
                             VIntCoding.computeUnsignedVIntSize(VALUES[i]),
                             vintLength);
                assertEquals("Round trip of " + VALUES[i], VALUES[i], cursor.readVInt(ends[i], vintLength));
            }
        }
    }

    /// A length that does not fit in the space before it must be rejected instead of being subtracted
    /// from the position, which puts the read outside the data.
    @Test
    public void testContentLengthPastStartOfDataThrows() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(new byte[8]);
            FileWriter.writeReversedVint(out, 1 << 20);
            long pos = out.position();

            OnDiskCursor<Void> cursor = cursorOver(out);
            assertThrows(UncheckedIOException.class, () -> cursor.readContentAtPos(pos));
        }
    }

    /// A corrupt leading byte gives the longest encoding, whose value uses all 64 bits and can be
    /// negative.
    @Test
    public void testCorruptLengthByteThrows() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            byte[] allOnes = new byte[9];
            Arrays.fill(allOnes, (byte) 0xFF);
            out.write(allOnes);
            long pos = out.position();

            OnDiskCursor<Void> cursor = cursorOver(out);
            assertThrows(UncheckedIOException.class, () -> cursor.readContentAtPos(pos));
        }
    }

    /// There is no byte to read a length from when the position is at or past the end of the data.
    @Test
    public void testLengthPastEndOfDataThrows() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(new byte[8]);
            OnDiskCursor<Void> cursor = cursorOver(out);
            long pastEnd = cursor.rebufferer.fileLength() + 1;
            assertThrows(UncheckedIOException.class, () -> cursor.readVIntLength(pastEnd));
        }
    }

    /// The bytes handed to the payload deserializer can be corrupt, and the deserializer is often the
    /// first thing that can tell -- a content type tag it does not know, for instance. Its rejection has
    /// to reach the caller as the same kind of failure the length checks above produce: a commit-log
    /// replay classifies the error it gets for a record, and an Error is not something it can classify.
    @Test
    public void testDeserializerFailureIsReportedAsIOException() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // The root leaf that cursorOver appends carries content, which the cursor reads as it is
            // constructed. The deserializer below rejects it the way one that meets a tag it does not
            // know does.
            UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
                                                       () -> cursorOver(out, (rdr, length) -> {
                                                           throw new IOException("Unknown content type tag: 18");
                                                       }));
            assertEquals("Unknown content type tag: 18", thrown.getCause().getMessage());
        }
    }

    /// A cursor that has run out of positions must keep reporting the exhausted position from
    /// [Cursor#encodedPosition], not the last live one it was on.
    ///
    /// The round-trip tests all drive a cursor by the position `advance` returns and stop as soon as that is
    /// exhausted, so none of them asks an exhausted cursor where it is. The merge cursors do: they hold two or more
    /// sources, advance the one that is behind, and then read the position of each to decide which to take next. A
    /// source that answers with a live position after it has ended is advanced again and again, and the merge
    /// republishes the content it ended on -- so a trie read back off disk silently gains rows when it is merged with
    /// anything, and the merge only terminates because the other source does.
    @Test
    public void testExhaustedCursorReportsTheExhaustedPosition() throws IOException, TrieSpaceExhaustedException
    {
        InMemoryTrie<String> plain = InMemoryTrie.shortLived(VERSION);
        plain.putRecursive(TrieUtil.directComparable("abc"), "one", (x, y) -> y);
        plain.putRecursive(TrieUtil.directComparable("abd"), "two", (x, y) -> y);

        try (OnDiskTrie<String> read = TrieUtil.onDiskRoundtripStrings(plain, false))
        {
            for (Direction direction : Direction.values())
            {
                assertExhaustedPositionReported("advanced to the end", read.cursor(direction), Cursor::advance);
                assertExhaustedPositionReported("advanceMultiple to the end",
                                                read.cursor(direction),
                                                c -> c.advanceMultiple(null));
                // Skipping past everything at the top level leaves the cursor exhausted through the ascent path
                // rather than through a node running out of children.
                assertExhaustedPositionReported("skipped past the end",
                                                read.cursor(direction),
                                                c -> c.skipTo(Cursor.encode(1, direction.select(0xFF, 0x00), direction)));
                // A tail cursor ends the same way and is what tailTrie hands to a merge.
                assertExhaustedPositionReported("tail advanced to the end",
                                                read.cursor(direction).tailCursor(direction),
                                                Cursor::advance);
            }
        }
    }

    private static void assertExhaustedPositionReported(String message,
                                                       Cursor<String> cursor,
                                                       java.util.function.ToLongFunction<Cursor<String>> step)
    {
        long position = cursor.encodedPosition();
        while (!Cursor.isExhausted(position))
            position = step.applyAsLong(cursor);

        assertEquals(message,
                     Cursor.toString(position),
                     Cursor.toString(cursor.encodedPosition()));
    }

    /// A cursor over the given bytes. A leaf node with no content is appended as the root so that the
    /// cursor can be constructed; the tests read positions within the bytes before it directly.
    private static OnDiskCursor<Void> cursorOver(DataOutputBuffer out) throws IOException
    {
        return cursorOver(out, (rdr, length) -> null);
    }

    private static OnDiskCursor<Void> cursorOver(DataOutputBuffer out, OnDiskCursor.DataDeserializer<Void> deserializer) throws IOException
    {
        out.writeByte(OnDiskWriteNodeType.LEAF.bits);
        ByteBuffer buffer = out.asNewBuffer();
        return new OnDiskCursor<>(deserializer,
                                  sourceOver(buffer),
                                  VERSION,
                                  Direction.FORWARD,
                                  true,
                                  false,
                                  buffer.limit());
    }

    /// The buffer source a trie would give its cursors, for a trie that is only these bytes. A
    /// [ByteBufferRebufferer] hands itself to every cursor, so there is nothing per-cursor to track.
    private static OnDiskCursor.RebuffererSource sourceOver(ByteBuffer buffer)
    {
        ByteBufferRebufferer rebufferer = new ByteBufferRebufferer(buffer);
        return new OnDiskCursor.RebuffererSource()
        {
            @Override
            public RebuffererFactory rebuffererFactory()
            {
                return rebufferer;
            }

            @Override
            public Set<Rebufferer> outstandingRebufferers()
            {
                return null;
            }
        };
    }
}
