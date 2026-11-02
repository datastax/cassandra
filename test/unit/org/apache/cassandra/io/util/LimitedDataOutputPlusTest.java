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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LimitedDataOutputPlusTest
{
    /// What the output holds before the write, so that the limit is seen to apply to its position.
    private static final int WRITTEN_BEFORE = 5;

    private interface Write
    {
        void to(DataOutputPlus out) throws IOException;
    }

    /// Every write is made if it takes the output exactly to its limit, and refused if it would take it one byte past.
    @Test
    public void testEveryWriteStopsAtTheLimit() throws IOException
    {
        assertStopsAtLimit("write(int)", out -> out.write(0x1ff));
        assertStopsAtLimit("write(byte[])", out -> out.write(new byte[13]));
        assertStopsAtLimit("write(byte[], int, int)", out -> out.write(new byte[13], 3, 7));
        assertStopsAtLimit("write(ByteBuffer) of a heap buffer", out -> out.write((ByteBuffer) ByteBuffer.allocate(20).position(5)));
        assertStopsAtLimit("write(ByteBuffer) of a direct buffer", out -> out.write((ByteBuffer) ByteBuffer.allocateDirect(20).limit(9)));
        assertStopsAtLimit("writeBoolean", out -> out.writeBoolean(true));
        assertStopsAtLimit("writeByte", out -> out.writeByte(-1));
        assertStopsAtLimit("writeShort", out -> out.writeShort(-1));
        assertStopsAtLimit("writeChar", out -> out.writeChar('☃'));
        assertStopsAtLimit("writeInt", out -> out.writeInt(-1));
        assertStopsAtLimit("writeLong", out -> out.writeLong(-1));
        assertStopsAtLimit("writeFloat", out -> out.writeFloat(1.5f));
        assertStopsAtLimit("writeDouble", out -> out.writeDouble(1.5));
        assertStopsAtLimit("writeBytes", out -> out.writeBytes("abc☃"));
        assertStopsAtLimit("writeChars", out -> out.writeChars("abc☃"));
        // Modified UTF-8 takes two bytes for the null character and three for each half of a surrogate pair.
        assertStopsAtLimit("writeUTF", out -> out.writeUTF("a\u0000é☃😀"));
        for (int bytes = 1; bytes <= 8; ++bytes)
        {
            int count = bytes;
            assertStopsAtLimit("writeMostSignificantBytes of " + bytes, out -> out.writeMostSignificantBytes(-1L, count));
        }
        // Every length of vint, from one byte to nine.
        for (int shift = 0; shift < 64; shift += 7)
        {
            long value = 1L << shift;
            assertStopsAtLimit("writeUnsignedVInt of " + value, out -> out.writeUnsignedVInt(value));
            assertStopsAtLimit("writeVInt of " + -value, out -> out.writeVInt(-value));
        }
        assertStopsAtLimit("writeUnsignedVInt of -1", out -> out.writeUnsignedVInt(-1L));
        assertStopsAtLimit("writeUnsignedVInt32", out -> out.writeUnsignedVInt32(Integer.MAX_VALUE));
        assertStopsAtLimit("writeVInt32", out -> out.writeVInt32(Integer.MIN_VALUE));
        assertStopsAtLimit("write(ReadableMemory)", out -> {
            try (Memory memory = Memory.allocate(11))
            {
                out.write(memory, 2, 9);
            }
        });
    }

    /// Writing nothing is never refused, not even when the output is at its limit.
    @Test
    public void testEmptyWriteAtTheLimitIsMade() throws IOException
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            LimitedDataOutputPlus limited = new LimitedDataOutputPlus(buffer, 0);
            limited.write(new byte[0]);
            limited.write(ByteBuffer.allocate(0));
            limited.writeMostSignificantBytes(-1L, 0);
            assertEquals(0, buffer.position());
            assertEquals(0, limited.remaining());
            assertThrows(LimitedDataOutputPlus.LimitReachedException.class, () -> limited.writeByte(0));
        }
    }

    /// An output that was given past its limit has less than no room left, and takes no write.
    @Test
    public void testOutputPastTheLimit() throws IOException
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            buffer.write(new byte[WRITTEN_BEFORE]);
            LimitedDataOutputPlus limited = new LimitedDataOutputPlus(buffer, WRITTEN_BEFORE - 2);
            assertEquals(-2, limited.remaining());
            assertThrows(LimitedDataOutputPlus.LimitReachedException.class, () -> limited.writeByte(0));
            assertEquals(WRITTEN_BEFORE, buffer.position());
        }
    }

    /// Reaching the limit is how the output reports a serialization too large to want, and can happen for every one,
    /// so it costs no stack trace.
    @Test
    public void testLimitReachedHasNoStackTrace() throws IOException
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            LimitedDataOutputPlus limited = new LimitedDataOutputPlus(buffer, 0);
            LimitedDataOutputPlus.LimitReachedException e = assertThrows(LimitedDataOutputPlus.LimitReachedException.class, () -> limited.writeLong(0));
            assertEquals(0, e.getStackTrace().length);
        }
    }

    private static void assertStopsAtLimit(String write, Write w) throws IOException
    {
        byte[] expected;
        try (DataOutputBuffer plain = new DataOutputBuffer())
        {
            w.to(plain);
            expected = plain.toByteArray();
        }
        assertTrue(write, expected.length > 0);

        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            buffer.write(new byte[WRITTEN_BEFORE]);
            long limit = WRITTEN_BEFORE + expected.length;
            LimitedDataOutputPlus limited = new LimitedDataOutputPlus(buffer, limit);
            assertTrue(limited.hasPosition());
            assertEquals(write, expected.length, limited.remaining());

            w.to(limited);
            assertEquals(write, limit, limited.position());
            assertEquals(write, 0, limited.remaining());
            assertArrayEquals(write, expected, Arrays.copyOfRange(buffer.toByteArray(), WRITTEN_BEFORE, (int) limit));
        }

        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            buffer.write(new byte[WRITTEN_BEFORE]);
            long limit = WRITTEN_BEFORE + expected.length - 1;
            LimitedDataOutputPlus limited = new LimitedDataOutputPlus(buffer, limit);

            assertThrows(write, LimitedDataOutputPlus.LimitReachedException.class, () -> w.to(limited));
            // A write the output takes in parts, as a nine-byte vint, may have made its first part.
            assertTrue(write, buffer.position() <= limit);
            byte[] made = Arrays.copyOfRange(buffer.toByteArray(), WRITTEN_BEFORE, (int) buffer.position());
            assertArrayEquals(write, Arrays.copyOf(expected, made.length), made);
        }
    }
}
