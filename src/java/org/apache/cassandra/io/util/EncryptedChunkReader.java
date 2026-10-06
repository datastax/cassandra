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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.CHUNK_SIZE;
import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.FOOTER_LENGTH;

/**
 * Reader for encryption-only files written using EncryptedSequentialWriter.
 *
 * These files are written in chunks, where each page has some of its size reserved for metadata (e.g. CRC, length,
 * IV). The metadata is visible only to this class, but to avoid having to define a mapping between file and logical
 * positions, the file skips over the space assigned to the metadata.
 *
 * In other words, to access e.g. content at position 0x12E34F with chunk size 0x1000, we read 0x1000 encrypted chunk
 * bytes at position 0x12E000 in the file, decrypt the content and then position the buffer on offset 0x34F. The
 * buffer's limit will be lower than 0x1000 (typically by at least 33 bytes) and if we read (or skip over) a sequence
 * that reaches this limit the position will jump to the beginning of the next chunk (see adjustPosition).
 *
 * Both the encrypted chunk size (given by the CHUNK_SIZE constant) and the decrypted (i.e. usable) size (calculated as
 * the largest that must fit CHUNK_SIZE) are fixed.
 *
 * For comparison, in compressed files the chunk size is equal to the uncompressed/usable size, while the
 * compressed size varies. There are unrelated compressed and uncompressed positions which are resolved
 * using an in-memory offsets mapping.
 *
 * Used for primary indices (both partition and row) where most pages store page-packed tries, where compression is
 * not beneficial and in-memory offset overhead (given the chunk size of 4k) would be prohibitive.
 */
public abstract class EncryptedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedChunkReader.class);

    final int maxBytesInPage;

    final CompressionParams compressionParams;
    final ICompressor encryptor;

    EncryptedChunkReader(ChannelProxy channel, long fileLength, CompressionParams params, ICompressor encryptor, int maxBytesInPage)
    {
        super(channel, fileLength);
        this.compressionParams = params;
        this.encryptor = encryptor;
        this.maxBytesInPage = maxBytesInPage;
    }

    public long adjustPosition(long position)
    {
        if (inChunkOffset(position) < maxBytesInPage)
            return position;

        return position - maxBytesInPage + CHUNK_SIZE;
    }

    @Override
    public long positionForSkip(long currentPosition, int bytesToSkip)
    {
        return positionForSkip(currentPosition, bytesToSkip, maxBytesInPage);
    }

    @Override
    public long remainingBytes(long position)
    {
        return remainingBytes(position, fileLength(), maxBytesInPage);
    }

    /**
     * The position {@code bytesToSkip} usable bytes after {@code currentPosition}, in a file whose chunks of
     * {@code CHUNK_SIZE} bytes hold {@code maxBytesInPage} usable bytes each. A skip of at least one byte that ends
     * exactly at the usable end of a chunk returns that end, i.e. the start of the chunk's hole (see
     * {@link ReaderFileProxy#positionForSkip}).
     */
    @VisibleForTesting
    static long positionForSkip(long currentPosition, int bytesToSkip, int maxBytesInPage)
    {
        if (bytesToSkip == 0)
            return currentPosition;
        return toPhysical(toLogical(currentPosition, maxBytesInPage) + bytesToSkip, maxBytesInPage);
    }

    /**
     * The number of usable bytes between {@code position} and {@code fileLength}, in a file whose chunks of
     * {@code CHUNK_SIZE} bytes hold {@code maxBytesInPage} usable bytes each (see
     * {@link ReaderFileProxy#remainingBytes}).
     */
    @VisibleForTesting
    static long remainingBytes(long position, long fileLength, int maxBytesInPage)
    {
        if (position >= fileLength)
            return 0;
        return toLogical(fileLength, maxBytesInPage) - toLogical(position, maxBytesInPage);
    }

    /**
     * Converts a file position to "non-holed space", i.e. to the number of usable bytes before it. A position inside
     * a hole maps to the usable end of its chunk.
     */
    private static long toLogical(long position, int maxBytesInPage)
    {
        return position / CHUNK_SIZE * maxBytesInPage + Math.min(inChunkOffset(position), maxBytesInPage);
    }

    /**
     * Converts a positive number of usable bytes back to a file position. At a chunk boundary there are two such
     * positions, the start of a chunk's hole and the start of the next chunk: this returns the former, which is where
     * reading that many bytes from the start of the file leaves the file pointer.
     */
    private static long toPhysical(long logicalPosition, int maxBytesInPage)
    {
        assert logicalPosition > 0 : logicalPosition;
        long lastByte = logicalPosition - 1; // the physical position of the last byte is unambiguous
        return lastByte / maxBytesInPage * CHUNK_SIZE + lastByte % maxBytesInPage + 1;
    }

    private static long inChunkOffset(long position)
    {
        return position & (CHUNK_SIZE - 1);
    }

    public ReaderType type()
    {
        return ReaderType.COMPRESSED;
    }

    public boolean shouldCheckCrc()
    {
        return compressionParams.shouldCheckCrc();
    }

    protected ByteBuffer decrypt(ByteBuffer input, int start, ByteBuffer output, long position) throws CorruptBlockException
    {
        assert output.capacity() == CHUNK_SIZE;

        if (shouldCheckCrc())
        {
            input.position(start).limit(start + CHUNK_SIZE - 4);
            int checksum = (int) ChecksumType.CRC32.of(input);

            //Change the limit to include the checksum
            input.limit(start + CHUNK_SIZE);
            int storedChecksum = input.getInt();
            if (storedChecksum != checksum)
                throw new CorruptBlockException(channel.getFile(), position, CHUNK_SIZE, storedChecksum, checksum);
        }

        int length = input.getInt(start + CHUNK_SIZE - FOOTER_LENGTH);
        if (length < 0 || length > CHUNK_SIZE - FOOTER_LENGTH)
            throw new CorruptBlockException(channel.getFile(), position, CHUNK_SIZE);
        output.clear();
        input.position(start).limit(start + length);
        try
        {
            encryptor.uncompress(input, output);
        }
        catch (IOException e)
        {
            throw new CorruptBlockException(channel.getFile(), position, CHUNK_SIZE, e);
        }
        output.flip();
        // more content than the chunk can hold, e.g. a chunk written for another encryptor
        if (output.limit() > maxBytesInPage)
            throw new CorruptBlockException(channel.getFile(), position, CHUNK_SIZE);

        return output;
    }

    @Override
    public int chunkSize()
    {
        return CHUNK_SIZE;
    }

    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Aligned(this);
    }

    @Override
    public Rebufferer instantiateRebufferer(boolean isScan)
    {
        return instantiateRebufferer();
    }

    @Override
    public void invalidateIfCached(long position)
    {
        // Nothing to do: this reader holds no cached data. When the chunk cache is in use, this reader is wrapped by
        // ChunkCache.CachingRebufferer, which performs the invalidation.
    }

    @Override
    public String toString()
    {
        return String.format("EncryptedChunkReader.%s(%s - %s, chunk length %d, data length %d)",
                getClass().getSimpleName(),
                channel.filePath(),
                encryptor.getClass().getSimpleName(),
                CHUNK_SIZE,
                fileLength);
    }

    /**
     * The logical length of a file read without a length override. Files written by {@link EncryptedSequentialWriter}
     * end with a full chunk, i.e. right after a hole: their length is the usable end of the last chunk (the start of
     * its hole), which allows partition index readers to find their metadata at the end of the file (see
     * {@link SequentialWriter#establishEndAddressablePosition}). This is exact for files whose last chunk is filled up
     * to its usable end (the partition index), but only an upper bound for files whose last chunk is partially filled
     * and padded on disk (the row index), like for any padded chunk (see {@link RandomAccessReader#bytesRemaining()}):
     * finding the end of the content would need decrypting the last chunk when the file is opened.
     * <p>
     * A file that does not end with a full chunk has been truncated. If it ends in the usable part of a chunk its
     * length is left unchanged, and reading the incomplete chunk reports it as corrupted; if it ends inside a hole
     * (i.e. there is no valid logical length), it is reported as corrupted here.
     */
    private static long defaultLength(ChannelProxy channel, long fileLength, int maxBytesInPage)
    {
        long inChunkOffset = inChunkOffset(fileLength);
        if (inChunkOffset == 0)
            return Math.max(0, fileLength - (CHUNK_SIZE - maxBytesInPage));
        if (inChunkOffset <= maxBytesInPage)
            return fileLength;
        throw new CorruptSSTableException(new CorruptBlockException(channel.getFile(), fileLength - inChunkOffset, CHUNK_SIZE),
                                          channel.filePath());
    }

    public static Standard createStandard(ChannelProxy channel,
            ICompressor encryptor,
            CompressionParams compressionParams,
            long fileLength,
            long overrideLength)
    {
        int maxBytesInPage = EncryptedSequentialWriter.maxBytesInPage(encryptor);

        if (overrideLength <= 0)
            overrideLength = defaultLength(channel, fileLength, maxBytesInPage);

        return new Standard(channel, compressionParams, encryptor, overrideLength, maxBytesInPage);
    }

    public static Mmap createMmap(ChannelProxy channel,
            MmappedRegions regions,
            ICompressor encryptor,
            CompressionParams compressionParams,
            long fileLength,
            long overrideLength)
    {
        int maxBytesInPage = EncryptedSequentialWriter.maxBytesInPage(encryptor);

        if (overrideLength <= 0)
            overrideLength = defaultLength(channel, fileLength, maxBytesInPage);

        return new Mmap(channel, regions, compressionParams, encryptor, overrideLength, maxBytesInPage);
    }

    static class Standard extends EncryptedChunkReader
    {
        Standard(ChannelProxy channel, CompressionParams params, ICompressor encryptor, long dataLength, int maxBytesInPage)
        {
            super(channel, dataLength, params, encryptor, maxBytesInPage);
        }

        @Override
        public void readChunk(long position, ByteBuffer buffer)
        {
            assert inChunkOffset(position) == 0 : "Access must always be aligned";
            assert buffer.capacity() >= CHUNK_SIZE;

            ByteBuffer input;

            if (encryptor.canDecompressInPlace())
            {
                input = buffer.duplicate();
            }
            else
            {
                input = BufferPools.forNetworking().get(CHUNK_SIZE, BufferType.preferredForCompression());
            }

            try
            {
                input.position(0).limit(CHUNK_SIZE);
                if (channel.read(input, position) != CHUNK_SIZE)
                    throw new CorruptBlockException(channel.getFile(), position, CHUNK_SIZE);
                decrypt(input, 0, buffer, position);
            }
            catch (CorruptBlockException e)
            {
                StorageProvider.instance.invalidateFileSystemCache(channel.getFile());

                // Make sure reader does not see stale data.
                buffer.position(0).limit(0);
                throw new CorruptSSTableException(e, channel.filePath());
            }
            finally
            {
                if (!encryptor.canDecompressInPlace())
                {
                    BufferPools.forNetworking().put(input);
                }
            }
        }

        @Override
        public BufferType preferredBufferType()
        {
            return BufferType.preferredForCompression();
        }
    }

    static class Mmap extends EncryptedChunkReader
    {
        final MmappedRegions regions;

        Mmap(ChannelProxy channel, MmappedRegions regions, CompressionParams params, ICompressor encryptor, long dataLength, int maxBytesInPage)
        {
            super(channel, dataLength, params, encryptor, maxBytesInPage);
            this.regions = regions;
        }

        @Override
        public void readChunk(long position, ByteBuffer buffer)
        {
            assert inChunkOffset(position) == 0 : "Access must always be aligned";
            assert buffer.capacity() >= CHUNK_SIZE;

            MmappedRegions.Region r = regions.floor(position);
            try
            {
                ByteBuffer input = r.buffer();
                int start = (int) (position - r.offset());
                if (start + CHUNK_SIZE > input.capacity())
                    throw new CorruptBlockException(channel.getFile(), position, CHUNK_SIZE);
                decrypt(input, start, buffer, position);
            }
            catch (CorruptBlockException e)
            {
                // Make sure reader does not see stale data.
                buffer.position(0).limit(0);
                throw new CorruptSSTableException(e, channel.filePath());
            }
        }

        @Override
        public BufferType preferredBufferType()
        {
            return BufferType.preferredForCompression();
        }

        @Override
        public void close()
        {
            regions.closeQuietly();
            super.close();
        }
    }
}
