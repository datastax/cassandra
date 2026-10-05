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
    private BufferHolder bufferHolder;
    private final ByteOrder order;

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

        // Not at EOF, so there is content after current(): its adjusted position is before length() (unlike in seek,
        // it cannot be a length() at the start of the last chunk's hole, which adjustPosition would move past it).
        reBufferAt(rebufferer.adjustPosition(current()));
    }

    /**
     * Moves the file pointer to the given position, which must not be after {@link #length()}. The position is not
     * adjusted (see {@link ReaderFileProxy#adjustPosition}): at the start of a hole before {@link #length()} this loads
     * the chunk ending there, with nothing remaining in the buffer, where reading the chunk's last byte leaves the
     * reader.
     */
    private void reBufferAt(long position)
    {
        assert position <= length() : position + " > " + length();
        bufferHolder.release();
        if (position == length())
        {
            bufferHolder = Rebufferer.emptyBufferHolderAt(position);
            buffer = bufferHolder.buffer();
        }
        else
        {
            try
            {
                bufferHolder = rebufferer.rebuffer(position);
            }
            catch (Throwable t)
            {
                // The reader must neither release the holder again nor read from the buffer of the released one, whose
                // memory may have been reused (by the chunk cache, or by the failed read itself). It stays at the
                // position it failed to load, so that a later read tries to load it again.
                bufferHolder = Rebufferer.emptyBufferHolderAt(position);
                buffer = bufferHolder.buffer();
                throw t;
            }
            buffer = bufferHolder.buffer();
            // The buffer may hold data past length(), e.g. a chunk read in full when length() is in its middle, or a
            // chunk cached (or a region mapped) for a handle of the same file with a longer length. Reads stop at
            // length(); the limit of the buffer, which is this reader's own view, is the place to enforce it.
            long lengthInBuffer = length() - bufferHolder.offset();
            if (buffer.limit() > lengthInBuffer)
                buffer.limit(Ints.checkedCast(lengthInBuffer));
            long positionInBuffer = position - bufferHolder.offset();
            // e.g. a position in the padding of a chunk of an encryption-only file (see bytesRemaining); the holder
            // stays referenced by this reader, which releases it on the next rebuffer or close
            if (positionInBuffer > buffer.limit())
                throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode: past the end of the data at %d",
                                                                 position, getFile(), length(), bufferHolder.offset() + buffer.limit()));
            buffer.position(Ints.checkedCast(positionInBuffer));
        }
        buffer.order(order);
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
     * @return true if there is no more data to read, i.e. if {@link #bytesRemaining()} is 0 (see
     * {@link #bytesRemaining()} for chunks padded before {@link #length()})
     */
    public boolean isEOF()
    {
        return rebufferer.remainingBytes(current()) == 0;
    }

    /**
     * @return the number of bytes between the file pointer and {@link #length()}, not counting holes (see
     * {@link ReaderFileProxy#remainingBytes}), or 0 after {@link #length()}; this is the number of bytes reads return
     * before reporting EOF.
     * <p>
     * In an encryption-only file a chunk that the writer padded before {@link #length()} (see
     * {@link org.apache.cassandra.io.compress.EncryptedSequentialWriter#padToPageBoundary}) has less content than its
     * usable size, which only reading the chunk reveals: the rest of its usable size is counted here, while a read
     * that reaches it reports EOF. Nothing is written to be read across such padding.
     */
    public long bytesRemaining()
    {
        return rebufferer.remainingBytes(getFilePointer());
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
        // For files with holes (see EncryptedChunkReader) length() may be the start of the last chunk's hole, which
        // adjustPosition would move to the physical end of the file, where there is no chunk to read.
        reBufferAt(newPosition == length() ? newPosition : rebufferer.adjustPosition(newPosition));
    }

    /**
     * Skips {@code n} bytes, or up to {@link #length()} if fewer remain, leaving the file pointer where reading the same
     * bytes would leave it.
     * <p>
     * For files with holes (see {@link EncryptedChunkReader}) this matters when the skipped bytes end exactly at the
     * usable end of a chunk: a read leaves the pointer there (at the start of the hole), while {@link #seek} moves on to
     * the start of the next chunk. Callers compare file pointers with positions recorded by the writer (e.g.
     * {@code TrieIndexEntry.deserialize}), so the pointer is moved without adjusting the position.
     * <p>
     * A skip ending inside the padding of a chunk padded before {@link #length()} (see {@link #bytesRemaining()}) is an
     * error.
     *
     * @return the number of bytes skipped, i.e. {@code n} unless the end of the file was reached first
     */
    @Override
    public int skipBytes(int n) throws IOException
    {
        if (n <= 0)
            return 0;
        if (buffer == null)
            throw new IOException("Attempted skipBytes() on a closed RAR");
        if (n <= buffer.remaining())
        {
            buffer.position(buffer.position() + n);
            return n;
        }

        long current = current();
        long remaining = rebufferer.remainingBytes(current);
        if (remaining <= 0)
            return 0;
        if (n > remaining)
            n = (int) remaining;
        // n is more than the buffer holds: the target is after the buffer, and not after length()
        reBufferAt(rebufferer.positionForSkip(current, n));
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
    @SuppressWarnings({ "resource", "RedundantSuppression" }) // reader is closed along with the returned RandomAccessReader instance
    public static RandomAccessReader open(File file)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            ChunkReader reader = new SimpleChunkReader(channel, -1, BufferType.OFF_HEAP, DEFAULT_BUFFER_SIZE);
            Rebufferer rebufferer = reader.instantiateRebufferer(false);
            return new RandomAccessReaderWithOwnChannel(rebufferer);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

}
