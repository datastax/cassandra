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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CountingDataOutputPlusTest
{
    private interface Write
    {
        void to(DataOutputPlus out) throws IOException;
    }

    /// Every write moves the position by the number of bytes a real output writes for it.
    @Test
    public void testPositionIsTheNumberOfBytesWritten() throws IOException
    {
        assertCounted("write(int)", out -> out.write(0x1ff));
        assertCounted("write(byte[])", out -> out.write(new byte[13]));
        assertCounted("write(byte[], int, int)", out -> out.write(new byte[13], 3, 7));
        assertCounted("write(ByteBuffer) of a heap buffer", out -> out.write((ByteBuffer) ByteBuffer.allocate(20).position(5)));
        assertCounted("write(ByteBuffer) of a direct buffer", out -> out.write((ByteBuffer) ByteBuffer.allocateDirect(20).limit(9)));
        assertCounted("writeBoolean", out -> out.writeBoolean(true));
        assertCounted("writeByte", out -> out.writeByte(-1));
        assertCounted("writeShort", out -> out.writeShort(-1));
        assertCounted("writeChar", out -> out.writeChar('☃'));
        assertCounted("writeInt", out -> out.writeInt(-1));
        assertCounted("writeLong", out -> out.writeLong(-1));
        assertCounted("writeFloat", out -> out.writeFloat(1.5f));
        assertCounted("writeDouble", out -> out.writeDouble(1.5));
        assertCounted("writeBytes", out -> out.writeBytes("abc☃"));
        assertCounted("writeChars", out -> out.writeChars("abc☃"));
        // Modified UTF-8 takes two bytes for the null character and three for each half of a surrogate pair.
        assertCounted("writeUTF", out -> out.writeUTF("a\u0000é☃😀"));
        for (int bytes = 0; bytes <= 8; ++bytes)
        {
            int count = bytes;
            assertCounted("writeMostSignificantBytes of " + bytes, out -> out.writeMostSignificantBytes(-1L, count));
        }
        for (int shift = 0; shift < 64; ++shift)
        {
            long value = 1L << shift;
            assertCounted("writeUnsignedVInt of " + value, out -> out.writeUnsignedVInt(value));
            assertCounted("writeVInt of " + -value, out -> out.writeVInt(-value));
        }
    }

    private static void assertCounted(String write, Write w) throws IOException
    {
        CountingDataOutputPlus counting = new CountingDataOutputPlus();
        assertTrue(counting.hasPosition());
        assertEquals(0, counting.position());
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            // Twice, as the position moves on from wherever the previous write left it.
            for (int repeat = 0; repeat < 2; ++repeat)
            {
                w.to(counting);
                w.to(buffer);
                assertEquals(write, buffer.getLength(), counting.position());
            }
        }
    }
}
