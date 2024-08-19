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
        var copied = 0;
        while (copied < count)
        {
            var bh = bufferHolder;
            long position = getPosition();

            FloatBuffer floatBuffer;
            if (bh.offset() == 0 && position % Float.BYTES == 0 && bh.order() == order)
            {
                // this is a separate code path because buffer() and asFloatBuffer() both allocate
                // new and relatively expensive xBuffer objects, so we want to avoid doing that
                // twice, where possible. If the BufferHandler has a different underlying
                // byte order, we duplicate first because there is not yet a way to configure
                // the buffer handler to use the correct byte order.
                floatBuffer = bh.floatBuffer();
                floatBuffer.position(Ints.checkedCast(position / Float.BYTES));
            }
            else
            {
                // bufferHolder offset is non-zero, and probably not aligned to Float.BYTES, so
                // set the position before converting to FloatBuffer.
                var bb = bh.buffer();
                bb.order(order);
                bb.position(Ints.checkedCast(position - bh.offset()));
                floatBuffer = bb.asFloatBuffer();
            }

            var remaining = floatBuffer.remaining();
            if (remaining == 0)
            {
                // slow path -- the next float bytes are across a buffer boundary (we never start a loop iteration with
                // the "current" buffer fully exhausted, so `remaining == 0` truly means "some bytes remains, but not
                // enough for a float"), so we read that float individually (which will read it byte by byte,
                // reBuffering as needed). After that we loop, which will switch back to the faster path for any
                // remaining floats in the newly reloaded buffer.
                dest[offset + copied] = readFloat();
                seek(position + Float.BYTES);
                copied++;
            }
            else
            {
                var elementsToRead = Math.min(remaining, count - copied);
                floatBuffer.get(dest, offset + copied, elementsToRead);
                seek(position + ((long) elementsToRead * Float.BYTES));
                copied += elementsToRead;
            }
        }
    }

    @Override
    public void readFully(long[] dest) throws IOException {
        int copied = 0;
        while (copied < dest.length)
        {
            var bh = bufferHolder;
            long position = getPosition();

            LongBuffer longBuffer;
            if (bh.offset() == 0 && position % Long.BYTES == 0 && bh.order() == order)
            {
                // this is a separate code path because buffer() and asLongBuffer() both allocate
                // new and relatively expensive xBuffer objects, so we want to avoid doing that
                // twice, where possible. If the BufferHandler has a different underlying
                // byte order, we duplicate first because there is not yet a way to configure
                // the buffer handler to use the correct byte order.
                longBuffer = bh.longBuffer();
                longBuffer.position(Ints.checkedCast(position / Long.BYTES));
            }
            else
            {
                // offset is non-zero, and probably not aligned to Long.BYTES, so
                // set the position before converting to LongBuffer.
                var bb = bh.buffer();
                bb.order(order);
                bb.position(Ints.checkedCast(position - bh.offset()));
                longBuffer = bb.asLongBuffer();
            }

            var remaining = longBuffer.remaining();
            if (remaining == 0)
            {
                // slow path -- the next long bytes are across a buffer boundary (we never start a loop iteration with
                // the "current" buffer fully exhausted, so `remaining == 0` truly means "some bytes remains, but not
                // enough for a long"), so we read that long individually (which will read it byte by byte,
                // reBuffering as needed). After that we loop, which will switch back to the faster path for any
                // remaining longs in the newly reloaded buffer.
                dest[copied] = readLong();
                seek(position + Long.BYTES);
                copied++;
            }
            else
            {
                var elementsToRead = Math.min(remaining, dest.length - copied);
                longBuffer.get(dest, copied, elementsToRead);
                seek(position + ((long) elementsToRead * Long.BYTES));
                copied += elementsToRead;
            }
        }
    }

    /**
     * Read ints into an int[], starting at the current position.
     *
     * @param dest the array to read into
     * @param offset the offset in the array at which to start writing ints
     * @param count the number of ints to read
     *
     * Will change the buffer position.
     */
    @Override
    public void read(int[] dest, int offset, int count) throws IOException
    {
        int copied = 0;
        while (copied < count)
        {
            var bh = bufferHolder;
            long position = getPosition();

            IntBuffer intBuffer;
            if (bh.offset() == 0 && position % Integer.BYTES == 0 && bh.order() == order)
            {
                // this is a separate code path because buffer() and asIntBuffer() both allocate
                // new and relatively expensive xBuffer objects, so we want to avoid doing that
                // twice, where possible. If the BufferHandler has a different underlying
                // byte order, we duplicate first because there is not yet a way to configure
                // the buffer handler to use the correct byte order.
                intBuffer = bh.intBuffer();
                intBuffer.position(Ints.checkedCast(position / Integer.BYTES));
            }
            else
            {
                // offset is non-zero, and probably not aligned to Integer.BYTES, so
                // set the position before converting to IntBuffer.
                var bb = bh.buffer();
                bb.order(order);
                bb.position(Ints.checkedCast(position - bh.offset()));
                intBuffer = bb.asIntBuffer();
            }

            var remaining = intBuffer.remaining();
            if (remaining == 0)
            {
                // slow path -- the next int bytes are across a buffer boundary (we never start a loop iteration with
                // the "current" buffer fully exhausted, so `remaining == 0` truly means "some bytes remains, but not
                // enough for an int"), so we read that int individually (which will read it byte by byte,
                // reBuffering as needed). After that we loop, which will switch back to the faster path for any
                // remaining ints in the newly reloaded buffer.
                dest[offset + copied] = readInt();
                seek(position + Integer.BYTES);
                copied++;
            }
            else
            {
                var elementsToRead = Math.min(remaining, count - copied);
                intBuffer.get(dest, offset + copied, elementsToRead);
                seek(position + ((long) elementsToRead * Integer.BYTES));
                copied += elementsToRead;
            }
        }
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
