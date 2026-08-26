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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/// Covers the back-to-front integer decoding of [OnDiskCursor], which the round-trip tests only reach
/// for the small content sizes they produce, and its behaviour on lengths that a corrupt commit-log
/// record or message payload can present.
public class OnDiskCursorTest
{
    @BeforeClass
    public static void setUp()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// One value per encoded length the writer can produce, on both sides of every length boundary.
    ///
    /// Nine-byte encodings are left out: [FileWriter#writeReversedVint] emits their leading byte first
    /// rather than last, so they do not round-trip through [OnDiskCursor#readVIntLength], which reads
    /// the byte immediately before the value as the leading one. Nothing can write a content that
    /// large, but a corrupt leading byte still asks the reader to decode one; that case is below.
    private static final long[] VALUES =
    { 0, 1, 127, 128, (1L << 14) - 1, 1L << 14, (1L << 21) - 1, 1L << 21, (1L << 28) - 1, 1L << 28,
      (1L << 35) - 1, 1L << 35, (1L << 42) - 1, 1L << 42, (1L << 49) - 1, 1L << 49, (1L << 56) - 1 };

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

    /// A cursor over the given bytes. A leaf node with no content is appended as the root so that the
    /// cursor can be constructed; the tests read positions within the bytes before it directly.
    private static OnDiskCursor<Void> cursorOver(DataOutputBuffer out) throws IOException
    {
        out.writeByte(OnDiskWriteNodeType.LEAF.bits);
        ByteBuffer buffer = out.asNewBuffer();
        return new OnDiskCursor<>((rdr, length) -> null,
                                  new ByteBufferRebufferer(buffer),
                                  VERSION,
                                  Direction.FORWARD,
                                  true,
                                  buffer.limit());
    }
}
