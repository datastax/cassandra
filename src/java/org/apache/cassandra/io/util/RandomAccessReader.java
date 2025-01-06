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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.Rebufferer.BufferHolder;

@NotThreadSafe
public class RandomAccessReader extends RebufferingInputStream implements FileDataInput, io.github.jbellis.jvector.disk.RandomAccessReader
{
    // The default buffer size when the client doesn't specify it
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    // offset of the last file mark
    private long markedPointer;

    final Rebufferer rebufferer;
    private final ByteOrder order;
    private BufferHolder bufferHolder;

    /**
     * Only created through Builder
     *
     * @param rebufferer Rebufferer to use
     */
    RandomAccessReader(Rebufferer rebufferer, ByteOrder order, BufferHolder bufferHolder)
    {
        super(bufferHolder.buffer(), false);
        this.bufferHolder = bufferHolder;
        this.rebufferer = rebufferer;
        this.order = order;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    public void reBuffer()
    {
        if (isEOF())
            return;

        reBufferAt(current());
    }

    private void reBufferAt(long position)
    {
        bufferHolder.release();
        if (position == length())
        {
            bufferHolder = Rebufferer.emptyBufferHolderAt(position);
            buffer = bufferHolder.buffer();
        }
        else
        {
            bufferHolder = Rebufferer.EMPTY; // prevents double release if the call below fails
            bufferHolder = rebufferer.rebuffer(position);
            buffer = bufferHolder.buffer();
            buffer.position(Ints.checkedCast(position - bufferHolder.offset()));
            buffer.order(order);
        }
    }

    public ByteOrder order()
    {
        return order;
    }

    @Override
    public void read(float[] dest, int offset, int count) throws IOException
    {
        for (int inBuffer = buffer.remaining() / Float.BYTES;
             inBuffer < count;
             inBuffer = buffer.remaining() / Float.BYTES)
        {
            if (inBuffer >= 1)
            {
                // read as much as we can from the buffer
                readFloats(buffer, order, dest, offset, inBuffer);
                offset += inBuffer;
                count -= inBuffer;
            }

            if (buffer.remaining() > 0)
            {
                // read the buffer-spanning value using the slow path
                dest[offset++] = readFloat();
                --count;
            }
            else
                reBuffer();
        }

        readFloats(buffer, order, dest, offset, count);
    }

    @Override
    public void readFully(long[] dest) throws IOException
    {
        read(dest, 0, dest.length);
    }

    public void read(long[] dest, int offset, int count) throws IOException
    {
        for (int inBuffer = buffer.remaining() / Long.BYTES;
             inBuffer < count;
             inBuffer = buffer.remaining() / Long.BYTES)
        {
            if (inBuffer >= 1)
            {
                // read as much as we can from the buffer
                readLongs(buffer, order, dest, offset, inBuffer);
                offset += inBuffer;
                count -= inBuffer;
            }

            if (buffer.remaining() > 0)
            {
                // read the buffer-spanning value using the slow path
                dest[offset++] = readLong();
                --count;
            }
            else
                reBuffer();
        }

        readLongs(buffer, order, dest, offset, count);
    }

    @Override
    public void read(int[] dest, int offset, int count) throws IOException
    {
        for (int inBuffer = buffer.remaining() / Integer.BYTES;
             inBuffer < count;
             inBuffer = buffer.remaining() / Integer.BYTES)
        {
            if (inBuffer >= 1)
            {
                // read as much as we can from the buffer
                readInts(buffer, order, dest, offset, inBuffer);
                offset += inBuffer;
                count -= inBuffer;
            }

            if (buffer.remaining() > 0)
            {
                // read the buffer-spanning value using the slow path
                dest[offset++] = readInt();
                --count;
            }
            else
                reBuffer();
        }

        readInts(buffer, order, dest, offset, count);
    }

    private static void readFloats(ByteBuffer buffer, ByteOrder order, float[] dest, int offset, int count)
    {
        FloatBuffer floatBuffer = updateBufferByteOrderIfNeeded(buffer, order).asFloatBuffer();
        floatBuffer.get(dest, offset, count);
        buffer.position(buffer.position() + count * Float.BYTES);
    }

    private static void readLongs(ByteBuffer buffer, ByteOrder order, long[] dest, int offset, int count)
    {
        LongBuffer longBuffer = updateBufferByteOrderIfNeeded(buffer, order).asLongBuffer();
        longBuffer.get(dest, offset, count);
        buffer.position(buffer.position() + count * Long.BYTES);
    }

    private static void readInts(ByteBuffer buffer, ByteOrder order, int[] dest, int offset, int count)
    {
        IntBuffer intBuffer = updateBufferByteOrderIfNeeded(buffer, order).asIntBuffer();
        intBuffer.get(dest, offset, count);
        buffer.position(buffer.position() + count * Integer.BYTES);
    }

    private static ByteBuffer updateBufferByteOrderIfNeeded(ByteBuffer buffer, ByteOrder order)
    {
        return buffer.order() != order
               ? buffer.duplicate().order(order)
               : buffer;    // Note: ?: rather than if to hit one-liner inlining path
    }

    @Override
    public long getFilePointer()
    {
        if (buffer == null)     // closed already
            return rebufferer.fileLength();
        return current();
    }

    protected long current()
    {
        return bufferHolder.offset() + buffer.position();
    }

    @Override
    public File getFile()
    {
        return getChannel().getFile();
    }

    public ChannelProxy getChannel()
    {
        return rebufferer.channel();
    }

    @Override
    public void reset() throws IOException
    {
        seek(markedPointer);
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public DataPosition mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(DataPosition mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(DataPosition mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return current() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    @Override
    public int available() throws IOException
    {
        return Ints.saturatedCast(bytesRemaining());
    }

    @Override
    public void close()
    {
        // close needs to be idempotent.
        if (buffer == null)
            return;
        bufferHolder.release();
        rebufferer.closeReader();
        buffer = null;
        bufferHolder = null;

        //For performance reasons we don't keep a reference to the file
        //channel so we don't close it
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ':' + rebufferer;
    }

    /**
     * Class to hold a mark to the position of the file
     */
    private static class BufferedRandomAccessFileMark implements DataPosition
    {
        final long pointer;

        private BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (buffer == null)
            throw new IllegalStateException("Attempted to seek in a closed RAR");

        long bufferOffset = bufferHolder.offset();
        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }

        if (newPosition > length())
            throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getFile(), length()));
        reBufferAt(newPosition);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        if (n <= 0)
            return 0;
        if (buffer == null)
            throw new IOException("Attempted skipBytes() on a closed RAR");
        long current = current();
        long newPosition = Math.min(current + n, length());
        n = (int)(newPosition - current);
        seek(newPosition);
        return n;
    }

    /**
     * Reads a line of text form the current position in this file. A line is
     * represented by zero or more characters followed by {@code '\n'}, {@code
     * '\r'}, {@code "\r\n"} or the end of file marker. The string does not
     * include the line terminating sequence.
     * <p>
     * Blocks until a line terminating sequence has been read, the end of the
     * file is reached or an exception is thrown.
     * </p>
     * @return the contents of the line or {@code null} if no characters have
     * been read before the end of the file has been reached.
     * @throws IOException if this file is closed or another I/O error occurs.
     */
    public final String readLine() throws IOException
    {
        StringBuilder line = new StringBuilder(80); // Typical line length
        boolean foundTerminator = false;
        long unreadPosition = -1;
        while (true)
        {
            int nextByte = read();
            switch (nextByte)
            {
                case -1:
                    return line.length() != 0 ? line.toString() : null;
                case (byte) '\r':
                    if (foundTerminator)
                    {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    foundTerminator = true;
                    /* Have to be able to peek ahead one byte */
                    unreadPosition = getPosition();
                    break;
                case (byte) '\n':
                    return line.toString();
                default:
                    if (foundTerminator)
                    {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    line.append((char) nextByte);
            }
        }
    }

    public long length()
    {
        return rebufferer.fileLength();
    }

    public long getPosition()
    {
        return current();
    }

    public double getCrcCheckChance()
    {
        return rebufferer.getCrcCheckChance();
    }

    // A wrapper of the RandomAccessReader that closes the channel when done.
    // For performance reasons RAR does not increase the reference count of
    // a channel but assumes the owner will keep it open and close it,
    // see CASSANDRA-9379, this thin class is just for those cases where we do
    // not have a shared channel.
    static class RandomAccessReaderWithOwnChannel extends RandomAccessReader
    {
        RandomAccessReaderWithOwnChannel(Rebufferer rebufferer)
        {
            super(rebufferer, ByteOrder.BIG_ENDIAN, Rebufferer.EMPTY);
        }

        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            finally
            {
                try
                {
                    rebufferer.close();
                }
                finally
                {
                    getChannel().close();
                }
            }
        }
    }

    /**
     * Open a RandomAccessReader (not compressed, not mmapped, no read throttling) that will own its channel.
     *
     * @param file File to open for reading
     * @return new RandomAccessReader that owns the channel opened in this method.
     */
    @SuppressWarnings("resource")
    public static RandomAccessReader open(File file)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            ChunkReader reader = new SimpleChunkReader(channel, -1, BufferType.OFF_HEAP, DEFAULT_BUFFER_SIZE);
            Rebufferer rebufferer = reader.instantiateRebufferer();
            return new RandomAccessReaderWithOwnChannel(rebufferer);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

}
