/*
 * Copyright DataStax, Inc.
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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.io.LittleEndianDataInputStream;
import org.junit.Test;

import org.apache.cassandra.utils.AssertUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RebufferingInputStreamTest
{
    static final int STREAM_LEN = 1024 * 1024 + 15;

    @Test
    public void testZeroLenRead() throws IOException
    {
        try (var r = new TestRebufferingInputStream(new RandomBytesInputStream(0, STREAM_LEN)))
        {
            byte[] buffer = new byte[8];
            assertEquals(0, r.read(buffer, 0, 0));

            final ByteBuffer targetBuffer = ByteBuffer.allocate(4);
            targetBuffer.limit(0);
            r.readFully(targetBuffer);
            assertEquals(0, targetBuffer.position());
        }
    }

    @Test
    public void testOutOfBounds() throws IOException
    {
        try (var r = new TestRebufferingInputStream(new RandomBytesInputStream(0, STREAM_LEN)))
        {
            byte[] buffer = new byte[8];
            assertThrows(IndexOutOfBoundsException.class, () -> r.read(buffer, 0, buffer.length + 1));
            assertThrows(IndexOutOfBoundsException.class, () -> r.read(buffer, buffer.length, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> r.read(buffer, buffer.length + 1, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> r.read(buffer, -1, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> r.read(buffer, 0, -1));

            assertThrows(IndexOutOfBoundsException.class, () -> r.readFully(buffer, 0, buffer.length + 1));
            assertThrows(IndexOutOfBoundsException.class, () -> r.readFully(buffer, buffer.length, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> r.readFully(buffer, buffer.length + 1, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> r.readFully(buffer, -1, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> r.readFully(buffer, 0, -1));
        }
    }

    @Test
    public void testRead() throws IOException
    {
        final int SEED = 0;
        var sliceSizeRng = new Random(1);
        try (
        var ref = new RandomBytesInputStream(SEED, STREAM_LEN);
        var test = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
        {
            while (true)
            {
                int toRead = sliceSizeRng.nextInt(16);
                for (int i = 0; i < toRead; i++)
                {
                    int refByte = ref.read();
                    int testByte = test.read();
                    assertEquals(refByte, testByte);
                    if (refByte == -1)
                        return;
                }
            }
        }
    }

    @Test
    public void testReadIntoArray() throws IOException
    {
        final int READ_BUF_SIZE = 1024;
        final int SEED = 0;
        var dataRng = new Random(SEED);
        var sliceSizeRng = new Random(1);
        try (var stream = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
        {
            byte[] readBuffer = new byte[READ_BUF_SIZE];
            byte[] refBuffer = new byte[READ_BUF_SIZE];

            while (true)
            {
                int toRead = sliceSizeRng.nextInt(READ_BUF_SIZE);
                int read = stream.read(readBuffer, 0, toRead);
                if (read == -1)  // EOF
                    break;

                randomFill(dataRng, refBuffer, read);
                assert Arrays.equals(refBuffer, 0, read, readBuffer, 0, read)
                    : "Read data does not match reference data";
            }
        }
    }

    @Test
    public void testReadFullyIntoArray() throws IOException
    {
        final int READ_BUF_SIZE = 1024;
        final int SEED = 0;
        var dataRng = new Random(SEED);
        var sliceSizeRng = new Random(1);
        try (var stream = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
        {
            byte[] refBuffer = new byte[READ_BUF_SIZE];
            byte[] readBuffer = new byte[READ_BUF_SIZE];
            int totalRead = 0;
            while (totalRead < STREAM_LEN)
            {
                int toRead = Math.min(sliceSizeRng.nextInt(READ_BUF_SIZE), STREAM_LEN - totalRead);
                stream.readFully(readBuffer, 0, toRead);
                totalRead += toRead;
                randomFill(dataRng, refBuffer, toRead);
                assert Arrays.equals(refBuffer, 0, toRead, readBuffer, 0, toRead)
                    : "Read data does not match reference data";
            }
        }
    }

    @Test
    public void testReadFullyIntoBuffer() throws IOException
    {
        final int READ_BUF_SIZE = 11;  // An arbitrary size that is not a multiple of the internal buffer size
        final int SEED = 0;
        var dataRng = new Random(SEED);
        var sliceSizeRng = new Random(1);
        try (var stream = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
        {
            ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUF_SIZE);
            byte[] refBuffer = new byte[READ_BUF_SIZE];
            int totalRead = 0;
            while (totalRead < STREAM_LEN)
            {
                int toRead = Math.min(sliceSizeRng.nextInt(readBuffer.capacity()), STREAM_LEN - totalRead);
                readBuffer.clear();
                readBuffer.limit(toRead);
                stream.readFully(readBuffer);
                totalRead += toRead;
                randomFill(dataRng, refBuffer, toRead);
                assert Arrays.equals(refBuffer, 0, toRead, readBuffer.array(), 0, toRead)
                    : "Read data does not match reference data";
            }
        }
    }

    @Test
    public void testEOF()
    {
        assertThrows(EOFException.class, () -> {
            try (var stream = new TestRebufferingInputStream(new RandomBytesInputStream(0, STREAM_LEN)))
            {
                for (int i = 0; i < STREAM_LEN / 128 + 1; i++)
                {
                    stream.readFully(new byte[128]);
                }
            }
        });
        assertThrows(EOFException.class, () -> {
            try (var stream = new TestRebufferingInputStream(new RandomBytesInputStream(0, STREAM_LEN)))
            {
                ByteBuffer buffer = ByteBuffer.allocate(128);
                for (int i = 0; i < STREAM_LEN / 128 + 1; i++)
                {
                    buffer.clear();
                    stream.readFully(buffer);
                }
            }
        });
    }

    @Test
    public void testReadWithSkipping() throws IOException
    {
        final int READ_BUF_SIZE = 1024;
        final int SEED = 0;
        var dataRng = new Random(SEED);
        var opRng = new Random(1);
        try (var stream = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
        {
            byte[] readBuffer = new byte[READ_BUF_SIZE];
            byte[] refBuffer = new byte[READ_BUF_SIZE];

            while (true)
            {
                boolean shouldSkip = opRng.nextBoolean();
                int toRead = opRng.nextInt(READ_BUF_SIZE);

                if (shouldSkip)
                {
                    int skipped = stream.skipBytes(toRead);
                    randomFill(dataRng, refBuffer, skipped);  // read from the ref data stream to keep insync
                }
                else
                {
                    int read = stream.read(readBuffer, 0, toRead);
                    if (read == -1)  // EOF
                        break;

                    randomFill(dataRng, refBuffer, read);
                    assert Arrays.equals(refBuffer, 0, read, readBuffer, 0, read)
                        : "Read data does not match reference data";
                }
            }
        }
    }

    @Test
    public void testBigEndianMultiByteReads() throws IOException
    {
        var SEED = 0;
        for (Validator validator : VALIDATORS)
        {
            try (RandomBytesInputStream ref = new RandomBytesInputStream(SEED, STREAM_LEN);
                 DataInputStream refData = new DataInputStream(ref);
                 TestRebufferingInputStream testData = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
            {
                while (true)
                {
                    try
                    {
                        validator.validate(refData, testData);
                    }
                    catch (EOFException e)
                    {
                        break;
                    }
                }
            }
        }

        // Run with mixed types in a single sequence
        Random validatorSelectRng = new Random(1);
        try (RandomBytesInputStream ref = new RandomBytesInputStream(SEED, STREAM_LEN);
             DataInputStream refData = new DataInputStream(ref);
             TestRebufferingInputStream testData = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN)))
        {
            while (true)
            {
                try
                {
                    Validator validator = VALIDATORS[validatorSelectRng.nextInt(VALIDATORS.length)];
                    validator.validate(refData, testData);
                }
                catch (EOFException e)
                {
                    break;
                }
            }
        }
    }

    @Test
    public void testLittleEndianMultiByteReads() throws IOException
    {
        var SEED = 0;
        for (Validator validator : VALIDATORS)
        {
            try (RandomBytesInputStream ref = new RandomBytesInputStream(SEED, STREAM_LEN);
                 LittleEndianDataInputStream refData = new LittleEndianDataInputStream(ref);
                 TestRebufferingInputStream testData = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN),
                                                                                      ByteOrder.LITTLE_ENDIAN))
            {
                while (true)
                {
                    try
                    {
                        validator.validate(refData, testData);
                    }
                    catch (EOFException e)
                    {
                        break;
                    }
                }
            }
        }

        // Run with mixed types in a single sequence
        Random validatorSelectRng = new Random(1);
        try (RandomBytesInputStream ref = new RandomBytesInputStream(SEED, STREAM_LEN);
             LittleEndianDataInputStream refData = new LittleEndianDataInputStream(ref);
             TestRebufferingInputStream testData = new TestRebufferingInputStream(new RandomBytesInputStream(SEED, STREAM_LEN),
                                                                                  ByteOrder.LITTLE_ENDIAN))
        {
            while (true)
            {
                try
                {
                    Validator validator = VALIDATORS[validatorSelectRng.nextInt(VALIDATORS.length)];
                    validator.validate(refData, testData);
                }
                catch (EOFException e)
                {
                    break;
                }
            }
        }
    }

    @Test
    public void testUtf8Reads() throws IOException
    {
        final int SEED = 0;
        try (RandomUtf8InputStream ref = new RandomUtf8InputStream(SEED, STREAM_LEN);
             DataInputStream refData = new DataInputStream(ref);
             TestRebufferingInputStream testData = new TestRebufferingInputStream(new RandomUtf8InputStream(SEED, STREAM_LEN)))
        {
            while (true)
            {
                try
                {
                    validate(refData::readUTF, testData::readUTF);
                }
                catch (EOFException e)
                {
                    break;
                }
            }
        }
    }

    /// Helper interface to perform read validations of data of different types (short, long, double, vint  etc.)
    /// with the same code
    interface Validator
    {
        /// Performs reading operation on both streams and checks if the results match.
        /// Expected to throw AssertionError if the results do not match.
        /// Expected to throw EOFException when the end of streams is reached.
        void validate(DataInput ref, RebufferingInputStream test) throws IOException;
    }

    private static final Validator[] VALIDATORS =
    {
        (ref, test) -> validate(ref::readByte, test::readByte),
        (ref, test) -> validate(ref::readShort, test::readShort),
        (ref, test) -> validate(ref::readInt, test::readInt),
        (ref, test) -> validate(ref::readLong, test::readLong),
        (ref, test) -> validate(ref::readFloat, test::readFloat),
        (ref, test) -> validate(ref::readDouble, test::readDouble),
        (ref, test) -> validate(ref::readBoolean, test::readBoolean),
        (ref, test) -> validate(ref::readChar, test::readChar),
        (ref, test) -> validate(ref::readUnsignedByte, test::readUnsignedByte),
        (ref, test) -> validate(ref::readUnsignedShort, test::readUnsignedShort),
        (ref, test) -> validate(() -> VIntCoding.readVInt(ref), test::readVInt),
        (ref, test) -> validate(() -> VIntCoding.readUnsignedVInt(ref), test::readUnsignedVInt),
    };

    /// Performs reading operation on both streams and checks if the results match.
    /// If the first stream hits EOF, the second stream is still read to check if it also hits EOF.
    /// If both streams hit EOF, an EOFException is thrown.
    /// If one stream hits EOF and another does not, then an assertion error is thrown.
    private static <T> void validate(AssertUtil.ThrowingSupplier<T> ref,
                                     AssertUtil.ThrowingSupplier<T> test) throws IOException
    {
        EOFException eof1 = null;
        EOFException eof2 = null;
        T refValue = null;
        T testValue = null;

        try
        {
            refValue = ref.get();
        }
        catch (EOFException e)
        {
            eof1 = e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            testValue = test.get();
        }
        catch (EOFException e)
        {
            eof2 = e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }

        if (eof1 != null && eof2 == null)
            throw new AssertionError("Reference stream hit EOF, but test stream did not");
        if (eof1 == null && eof2 != null)
            throw new AssertionError("Test stream hit EOF, but reference stream did not");

        assertEquals(refValue, testValue);

        if (eof1 != null)
            throw eof1;
    }

    /// Fills the given `ByteBuffer` with random bytes from the given Random generator.
    /// We are deliberately not using `rng.nextBytes()` because it does not guarantee generating
    /// the same sequence of data if buffers are of random sizes (i.e. if we slice two streams of data
    /// differently, we want to get the same data sequence, but `rng.nextBytes()` could generate different sequences).
    public static void randomFill(Random rng, byte[] buffer, int len)
    {
        for (int i = 0; i < len; i++)
            buffer[i] = (byte) rng.nextInt(256);
    }

    /// A test RebufferingInputStream that fills the buffer with data read from another reference InputStream.
    /// The buffer is filled with random sizes of data, to make sure the rebuffering logic works correctly regardless
    /// of how data are sliced - e.g. the boundary between buffers may fall in the middle of a multibyte data type.
    static class TestRebufferingInputStream extends RebufferingInputStream
    {
        static final int BUFFER_SIZE = 64;
        final Random bufFillRng;
        final ReadableByteChannel source;

        public TestRebufferingInputStream(InputStream source)
        {
            this(source, ByteOrder.BIG_ENDIAN);
        }

        public TestRebufferingInputStream(InputStream source, ByteOrder order)
        {
            super(makeBuffer(order), order.equals(ByteOrder.BIG_ENDIAN));
            this.source = Channels.newChannel(source);
            bufFillRng = new Random(0);
        }

        private static ByteBuffer makeBuffer(ByteOrder order)
        {
            ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
            buf.order(order);
            buf.flip();
            return buf;
        }

        @Override
        protected void reBuffer() throws IOException
        {
            Preconditions.checkState(!buffer.hasRemaining());
            int toFill = Math.max(1, bufFillRng.nextInt(buffer.capacity()));
            buffer.clear();
            buffer.limit(toFill);
            source.read(buffer);
            buffer.flip();
        }

        @Override
        public void close() throws IOException
        {
            source.close();
            super.close();
        }
    }

    /// Data stream for producing reference input.
    /// Creates a random sequence of bytes.
    static class RandomBytesInputStream extends InputStream
    {
        final Random rng;
        final int length;

        int position = 0;

        RandomBytesInputStream(int seed, int length)
        {
            this.rng = new Random(seed);
            this.length = length;
        }

        @Override
        public int read() throws IOException
        {
            if (position >= length)
                return -1;

            position++;
            return rng.nextInt(256);
        }
    }

    /// Generates a random sequence of UTF8 strings as an InputStream.
    /// The sequence may be slightly longer than the specified length because of the variable-length
    /// nature of UTF8 encoding.
    static class RandomUtf8InputStream extends InputStream
    {
        final Random rng;
        final int length;

        int remaining;

        ByteBuffer buffer;
        DataOutputBufferFixed dataOutput;

        RandomUtf8InputStream(int seed, int length)
        {
            this.rng = new Random(seed);
            this.length = length;
            this.remaining = length;
            buffer = ByteBuffer.allocate(4096);
            dataOutput = new DataOutputBufferFixed(buffer);
        }

        @Override
        public int read() throws IOException
        {
            // with UTF8 we can slightly exceed the desired stream length so remaining can get negative,
            // but we don't want to cut the stream in the middle of the UTF8 string
            if (remaining <= 0)
                return -1;

            if (!buffer.hasRemaining())
                writeRandomUtf8String();

            return buffer.get() & 0xFF;
        }

        private void writeRandomUtf8String() throws IOException
        {
            dataOutput.clear();
            // The string length must be much smaller thant the buffer size,
            // because UTF8 generates multiple bytes per character
            var stringLen = rng.nextInt(buffer.remaining() / 8 - 4);
            String str = randomString(stringLen);
            dataOutput.writeUTF(str);
            remaining -= dataOutput.getLength();
            buffer.flip();
        }

        private String randomString(int length)
        {
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++)
            {
                int codePoint;
                do
                {
                    codePoint = rng.nextInt(Character.MAX_CODE_POINT + 1);
                } while (Character.isSurrogate((char) codePoint));

                sb.appendCodePoint(codePoint);
            }
            return sb.toString();
        }
    }
}