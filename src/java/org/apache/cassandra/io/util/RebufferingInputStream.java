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
package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.base.Preconditions;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.lang.Math.min;

/**
 * Rough equivalent of BufferedInputStream and DataInputStream wrapping a ByteBuffer that can be refilled
 * via rebuffer. Implementations provide this buffer from various channels (socket, file, memory, etc).
 *
 * RebufferingInputStream is not thread safe.
 */
public abstract class RebufferingInputStream extends InputStream implements DataInputPlus, Closeable
{
    protected ByteBuffer buffer;

    protected RebufferingInputStream(ByteBuffer buffer)
    {
        this(buffer, true);
    }

    protected RebufferingInputStream(ByteBuffer buffer, boolean validateByteOrder)
    {
        if (validateByteOrder)
            Preconditions.checkArgument(buffer == null || buffer.order() == ByteOrder.BIG_ENDIAN,
                                        "Buffer must have BIG ENDIAN byte ordering");
        this.buffer = buffer;
    }

    /// Refills the buffer with new data.
    /// The buffer must be empty when this method is invoked.
    /// The buffer must be filled with at least 1 byte of data unless EOF is reached.
    ///
    /// EOF is indicated by not writing any bytes to the buffer and leaving the buffer with no remaining content.
    /// The implementations must not throw `EOFException` on EOF.
    ///
    /// Callers must not rely on the identity of the buffer object to stay the same after this call returns.
    /// The buffer reference may be switched to a different buffer instance in order to provide new data, and the
    /// previous buffer may be released if applicable.
    /// The buffer reference may be switched to a static empty buffer in case of EOF, in order to release the current
    /// exhausted buffer and to free up memory.
    /// The buffer is not allowed to be set to null if the call to this method exits normally (no exception thrown).
    ///
    /// @throws IOException when data is expected but could not be read due to an I/O error
    /// @throws IllegalStateException if the buffer hasn't been exhausted when this method is invoked
    protected abstract void reBuffer() throws IOException;

    // This is final because it is a convenience method that simply delegates to readFully(byte[], int, int).
    // Override that method instead if you want to change the behavior.
    @Override
    public final void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        // avoid int overflow
        if (off < 0 || off > b.length || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        int copied = 0;
        while (copied < len)
        {
            int read = readInternal(b, off, len - copied);
            if (read == -1)
                throw new EOFException("EOF after " + copied + " bytes out of " + len);
            copied += read;
            off += read;
        }

        assert copied == len;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        // avoid int overflow
        if (off < 0 || off > b.length || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        return readInternal(b, off, len);
    }

    /// Reads up to `len` bytes into `b` at offset `off` from the current buffer.
    /// Returns number of bytes read, or -1 if EOF is reached before reading any bytes.
    /// If the buffer is empty, it will be refilled via `reBuffer()` once.
    /// If EOF is not reached, reads at least one byte.
    private int readInternal(byte[] b, int off, int len) throws IOException
    {
        if (len == 0)
            return 0;

        if (!buffer.hasRemaining())
        {
            reBuffer();
            if (!buffer.hasRemaining())
                return -1; // EOF
        }

        int toRead = min(len, buffer.remaining());
        assert toRead > 0 : "toRead must be > 0";
        FastByteOperations.copy(buffer, buffer.position(), b, off, toRead);
        buffer.position(buffer.position() + toRead);
        return toRead;
    }

    /**
     * Equivalent to {@link #read(byte[], int, int)}, where offset is {@code dst.position()} and length is {@code dst.remaining()}
     */
    public void readFully(ByteBuffer dst) throws IOException
    {
        int offset = dst.position();
        int len = dst.limit() - offset;

        int copied = 0;
        while (copied < len)
        {
            int position = buffer.position();
            int remaining = buffer.limit() - position;

            if (remaining == 0)
            {
                reBuffer();

                position = buffer.position();
                remaining = buffer.limit() - position;

                if (remaining == 0)
                    throw new EOFException("EOF after " + copied + " bytes out of " + len);
            }

            int toCopy = min(len - copied, remaining);
            FastByteOperations.copy(buffer, position, dst, offset + copied, toCopy);
            buffer.position(position + toCopy);
            copied += toCopy;
        }

        assert copied == len;
    }

    @DontInline
    protected long readBigEndianPrimitiveSlowly(int bytes) throws IOException
    {
        long result = 0;
        for (int i = 0; i < bytes; i++)
            result = (result << 8) | (readByte() & 0xFFL);
        return result;
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        // Note: This implementation works correctly with files that may have holes between buffers
        // (see EncryptedChunkReader). If changing this code, make sure to account for that.
        if (n <= 0)
            return 0;
        int requested = n;
        int position = buffer.position(), limit = buffer.limit(), remaining;
        while ((remaining = limit - position) < n)
        {
            n -= remaining;
            buffer.position(limit);
            reBuffer();
            position = buffer.position();
            limit = buffer.limit();
            if (position == limit)
                return requested - n;
        }
        buffer.position(position + n);
        return requested;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        if (!buffer.hasRemaining())
        {
            reBuffer();
            if (!buffer.hasRemaining())
                throw new EOFException();
        }

        return buffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getShort();
        var result = (short) readBigEndianPrimitiveSlowly(2);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            return Short.reverseBytes(result);
        return result;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getChar();
        var result = (char) readBigEndianPrimitiveSlowly(2);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            return Character.reverseBytes(result);
        return result;
    }

    @Override
    public int readInt() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getInt();
        var result = (int) readBigEndianPrimitiveSlowly(4);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            return Integer.reverseBytes(result);
        return result;
    }

    @Override
    public long readLong() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getLong();
        var result = readBigEndianPrimitiveSlowly(8);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            return Long.reverseBytes(result);
        return result;
    }

    public long readVInt() throws IOException
    {
        return VIntCoding.decodeZigZag64(readUnsignedVInt());
    }

    public long readUnsignedVInt() throws IOException
    {
        //If 9 bytes aren't available use the slow path in VIntCoding
        if (buffer.remaining() < 9)
            return VIntCoding.readUnsignedVInt(this);

        byte firstByte = buffer.get();

        //Bail out early if this is one byte, necessary or it fails later
        if (firstByte >= 0)
            return firstByte;

        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);

        int position = buffer.position();
        int extraBits = extraBytes * 8;

        long retval = buffer.getLong(position);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            retval = Long.reverseBytes(retval);
        buffer.position(position + extraBytes);

        // truncate the bytes we read in excess of those we needed
        retval >>>= 64 - extraBits;
        // remove the non-value bits from the first byte
        firstByte &= VIntCoding.firstByteValueMask(extraBytes);
        // shift the first byte up to its correct position
        retval |= (long) firstByte << extraBits;
        return retval;
    }

    @Override
    public float readFloat() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getFloat();
        var intBits = (int) readBigEndianPrimitiveSlowly(4);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            intBits = Integer.reverseBytes(intBits);
        return Float.intBitsToFloat(intBits);
    }

    @Override
    public double readDouble() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getDouble();
        var longBits = readBigEndianPrimitiveSlowly(8);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            longBits = Long.reverseBytes(longBits);
        return Double.longBitsToDouble(longBits);
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    @Override
    public int read() throws IOException
    {
        try
        {
            return readUnsignedByte();
        }
        catch (EOFException ex)
        {
            return -1;
        }
    }
}
