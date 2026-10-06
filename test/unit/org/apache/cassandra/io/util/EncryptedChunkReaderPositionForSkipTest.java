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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_RANDOM_SEED;
import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.CHUNK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Checks that {@link EncryptedChunkReader#positionForSkip}, computed through the conversion to and from "non-holed
 * space", gives the same results as the chunk-by-chunk loop it replaced; that {@link EncryptedChunkReader#remainingBytes}
 * counts the usable bytes before the length; and that {@link RandomAccessReader#skipBytes} over holes of any size
 * leaves the position a read leaves.
 */
public class EncryptedChunkReaderPositionForSkipTest
{
    private static final int[] MAX_BYTES_IN_PAGE = { 1, 7, CHUNK_SIZE / 2, CHUNK_SIZE - 64, CHUNK_SIZE - 33, CHUNK_SIZE - 1 };

    /**
     * The original implementation, moving one chunk per iteration.
     */
    private static long positionForSkipLoop(long currentPosition, int bytesToSkip, int maxBytesInPage)
    {
        long currentOffset = currentPosition & (CHUNK_SIZE - 1);
        while (currentOffset + bytesToSkip > maxBytesInPage)
        {
            long len = maxBytesInPage - currentOffset;
            bytesToSkip -= len;
            currentPosition += CHUNK_SIZE - maxBytesInPage + len;
            currentOffset = 0;
        }
        return currentPosition + bytesToSkip;
    }

    private static void check(long position, int bytesToSkip, int maxBytesInPage)
    {
        assertEquals(String.format("skip %d from %d with %d usable bytes per chunk", bytesToSkip, position, maxBytesInPage),
                     positionForSkipLoop(position, bytesToSkip, maxBytesInPage),
                     EncryptedChunkReader.positionForSkip(position, bytesToSkip, maxBytesInPage));
    }

    @Test
    public void testChunkBoundaries()
    {
        for (int maxBytesInPage : MAX_BYTES_IN_PAGE)
        {
            for (long chunk : new long[]{ 0, 1, 5, 1L << 20 })
            {
                long chunkStart = chunk * CHUNK_SIZE;
                // positions at the start, before, at and just after the usable end of a chunk
                for (long position : new long[]{ chunkStart, chunkStart + 1, chunkStart + maxBytesInPage - 1, chunkStart + maxBytesInPage })
                {
                    long offset = position - chunkStart;
                    // skips ending just before, exactly at and just after the usable end of this and further chunks
                    for (long chunksCrossed = 0; chunksCrossed < 4; ++chunksCrossed)
                    {
                        long toUsableEnd = maxBytesInPage - offset + chunksCrossed * maxBytesInPage;
                        for (long delta = -1; delta <= 1; ++delta)
                        {
                            long bytesToSkip = toUsableEnd + delta;
                            if (bytesToSkip >= 0)
                                check(position, (int) bytesToSkip, maxBytesInPage);
                        }
                    }
                    check(position, 0, maxBytesInPage);
                }
            }
        }
    }

    @Test
    public void testRandom()
    {
        long seed = TEST_RANDOM_SEED.getLong(System.nanoTime());
        Random random = new Random(seed);
        for (int i = 0; i < 100_000; ++i)
        {
            int maxBytesInPage = random.nextBoolean() ? MAX_BYTES_IN_PAGE[random.nextInt(MAX_BYTES_IN_PAGE.length)]
                                                      : 1 + random.nextInt(CHUNK_SIZE - 1);
            // a usable position (in-chunk offset up to maxBytesInPage inclusive, i.e. at most the hole start)
            long position = (long) random.nextInt(1 << 20) * CHUNK_SIZE + random.nextInt(maxBytesInPage + 1);
            // up to 64 chunks (the loop takes one iteration per chunk), half of the time ending at a usable end
            int bytesToSkip = random.nextInt(Math.min(64 * maxBytesInPage, 64 * CHUNK_SIZE) + 1);
            if (random.nextBoolean())
            {
                long offset = position & (CHUNK_SIZE - 1);
                bytesToSkip = (int) (maxBytesInPage - offset + (long) random.nextInt(64) * maxBytesInPage);
            }
            try
            {
                check(position, bytesToSkip, maxBytesInPage);
            }
            catch (AssertionError e)
            {
                throw new AssertionError("Failed with seed " + seed + " (rerun with -D" + TEST_RANDOM_SEED.getKey() + '=' + seed + "): " + e.getMessage(), e);
            }
        }
    }

    @Test
    public void testLargestSkip()
    {
        for (int maxBytesInPage : MAX_BYTES_IN_PAGE)
        {
            if (maxBytesInPage < 64)
                continue; // the loop would take too long
            for (long position : new long[]{ 0, maxBytesInPage - 1, maxBytesInPage, 3L * CHUNK_SIZE + 17 })
                check(position, Integer.MAX_VALUE, maxBytesInPage);
        }
    }

    /**
     * A position strictly inside a hole counts as the usable end of its chunk: a skip from there continues at the start
     * of the next chunk. This intentionally differs from the original loop, which overshot by the distance between
     * the position and the hole start (such positions are never file pointers: reads and seeks move past holes).
     */
    @Test
    public void testSkipFromInsideHole()
    {
        for (int maxBytesInPage : MAX_BYTES_IN_PAGE)
        {
            if (maxBytesInPage > CHUNK_SIZE - 2)
                continue; // no position strictly inside a hole of a single byte
            long chunkStart = 5L * CHUNK_SIZE;
            long nextChunkStart = chunkStart + CHUNK_SIZE;
            for (long position : new long[]{ chunkStart + maxBytesInPage + 1, nextChunkStart - 1 })
            {
                assertEquals(nextChunkStart + 1, EncryptedChunkReader.positionForSkip(position, 1, maxBytesInPage));
                assertEquals(nextChunkStart + maxBytesInPage, EncryptedChunkReader.positionForSkip(position, maxBytesInPage, maxBytesInPage));
                assertEquals(nextChunkStart + CHUNK_SIZE + 1, EncryptedChunkReader.positionForSkip(position, maxBytesInPage + 1, maxBytesInPage));
                assertEquals(position, EncryptedChunkReader.positionForSkip(position, 0, maxBytesInPage));
                assertEquals(maxBytesInPage, EncryptedChunkReader.remainingBytes(position, nextChunkStart + CHUNK_SIZE, maxBytesInPage));
            }
        }
    }

    /**
     * {@code remainingBytes(p)} must be the number of usable bytes between {@code p} and the length (counted one by
     * one), and skipping them all must land at the last usable position before the length: the length itself, or the
     * hole start of the previous chunk when the length is a chunk start.
     */
    @Test
    public void testRemainingBytes()
    {
        int chunks = 4;
        for (int maxBytesInPage : MAX_BYTES_IN_PAGE)
        {
            for (int chunk = 0; chunk < chunks; ++chunk)
            {
                long chunkStart = (long) chunk * CHUNK_SIZE;
                long[] lengths = { chunkStart, chunkStart + 1, chunkStart + maxBytesInPage / 2, chunkStart + maxBytesInPage };
                for (long length : lengths)
                {
                    long lastUsable = length > 0 && (length & (CHUNK_SIZE - 1)) == 0 ? length - CHUNK_SIZE + maxBytesInPage
                                                                                     : length;
                    // count the usable bytes from the end, one position at a time
                    long usableAfter = 0;
                    for (long position = length + CHUNK_SIZE; position >= 0; --position)
                    {
                        if (position < length && (position & (CHUNK_SIZE - 1)) < maxBytesInPage)
                            ++usableAfter;
                        long remaining = EncryptedChunkReader.remainingBytes(position, length, maxBytesInPage);
                        long afterSkip = remaining > 0 ? EncryptedChunkReader.positionForSkip(position, (int) remaining, maxBytesInPage) : lastUsable;
                        if (remaining != usableAfter || afterSkip != lastUsable)
                        {
                            String context = String.format("from %d to %d with %d usable bytes per chunk", position, length, maxBytesInPage);
                            assertEquals("Remaining bytes " + context, usableAfter, remaining);
                            assertEquals("Skip of all bytes " + context, lastUsable, afterSkip);
                        }
                    }
                }
            }
        }
    }

    /**
     * An in-memory file with the chunk layout of an encryption-only file: every chunk of {@code CHUNK_SIZE} bytes
     * holds {@code maxBytesInPage} usable bytes followed by a hole. Usable bytes hold their logical index.
     */
    private static class HoleyRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        final byte[] data;
        final int maxBytesInPage;
        final long length;
        ByteBuffer buffer;
        long offset;

        HoleyRebufferer(int chunks, int maxBytesInPage)
        {
            this.maxBytesInPage = maxBytesInPage;
            this.data = new byte[chunks * CHUNK_SIZE];
            int logical = 0;
            for (int i = 0; i < data.length; ++i)
                data[i] = (i & (CHUNK_SIZE - 1)) < maxBytesInPage ? (byte) logical++ : (byte) 0xFF;
            this.length = (long) (chunks - 1) * CHUNK_SIZE + maxBytesInPage; // the hole start of the last chunk
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            offset = position & -CHUNK_SIZE;
            buffer = ByteBuffer.wrap(data, (int) offset, maxBytesInPage).slice();
            return this;
        }

        @Override
        public long adjustPosition(long position)
        {
            return (position & (CHUNK_SIZE - 1)) < maxBytesInPage ? position : position - maxBytesInPage + CHUNK_SIZE;
        }

        @Override
        public long positionForSkip(long currentPosition, int bytesToSkip)
        {
            return EncryptedChunkReader.positionForSkip(currentPosition, bytesToSkip, maxBytesInPage);
        }

        @Override
        public long remainingBytes(long position)
        {
            return EncryptedChunkReader.remainingBytes(position, length, maxBytesInPage);
        }

        @Override
        public ByteBuffer buffer()
        {
            return buffer;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public void release()
        {
            buffer = null;
        }

        @Override
        public void closeReader()
        {
            // no underlying reader to close
        }

        @Override
        public void close()
        {
            // nothing to close: the data is an in-memory array
        }

        @Override
        public ChannelProxy channel()
        {
            return null;
        }

        @Override
        public long fileLength()
        {
            return length;
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;
        }
    }

    /**
     * A skip must leave the pointer where a read of the same bytes leaves it, also with holes of a single byte (where
     * the position after the start of a hole is the start of the next chunk).
     */
    @Test
    public void testReaderSkipMatchesReadWithSmallHoles() throws IOException
    {
        int chunks = 4;
        for (int maxBytesInPage : new int[]{ CHUNK_SIZE - 1, CHUNK_SIZE - 2, CHUNK_SIZE - 33 })
        {
            HoleyRebufferer rebufferer = new HoleyRebufferer(chunks, maxBytesInPage);
            try (RandomAccessReader reader = new RandomAccessReader(rebufferer, ByteOrder.BIG_ENDIAN, Rebufferer.EMPTY))
            {
                for (int chunk = 0; chunk < chunks - 1; ++chunk)
                {
                    long usableEnd = (long) chunk * CHUNK_SIZE + maxBytesInPage;
                    for (long start = usableEnd - 3; start <= usableEnd; ++start)
                    {
                        long logicalStart = (long) chunk * maxBytesInPage + (start - (long) chunk * CHUNK_SIZE);
                        long remaining = (long) chunks * maxBytesInPage - logicalStart;
                        for (int n : new int[]{ 1, 2, 3, 4, 5, maxBytesInPage - 1, maxBytesInPage, maxBytesInPage + 1, maxBytesInPage + 2 })
                        {
                            if (n > remaining)
                                continue;
                            String context = String.format("maxBytesInPage %d, start %d, %d bytes", maxBytesInPage, start, n);
                            reader.seek(start);
                            assertEquals("Remaining " + context, remaining, reader.bytesRemaining());
                            reader.readFully(new byte[n]);
                            long afterRead = reader.getFilePointer();
                            assertEquals("Remaining after read " + context, remaining - n, reader.bytesRemaining());
                            assertEquals("EOF after read " + context, remaining == n, reader.isEOF());
                            int nextAfterRead = nextByte(reader);

                            reader.seek(start);
                            assertEquals(context, n, reader.skipBytes(n));
                            assertEquals("Position " + context, afterRead, reader.getFilePointer());
                            assertEquals("Remaining after skip " + context, remaining - n, reader.bytesRemaining());
                            assertEquals("Next byte " + context, nextAfterRead, nextByte(reader));
                        }

                        // a skip longer than what remains stops at length(), the hole start of the last chunk
                        reader.seek(start);
                        assertEquals(remaining, reader.skipBytes(Integer.MAX_VALUE));
                        assertEquals(rebufferer.length, reader.getFilePointer());
                        assertTrue(reader.isEOF());
                        assertEquals(0, reader.skipBytes(1));
                    }
                }
            }
        }
    }

    private static int nextByte(RandomAccessReader reader) throws IOException
    {
        try
        {
            return reader.readByte() & 0xFF;
        }
        catch (EOFException e)
        {
            return Integer.MAX_VALUE;
        }
    }
}
