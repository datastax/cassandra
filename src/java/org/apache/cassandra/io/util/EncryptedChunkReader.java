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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
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

    protected ByteBuffer decrypt(ByteBuffer input, int start, ByteBuffer output, long position) throws IOException
    {
        assert output.capacity() == CHUNK_SIZE;

        if (shouldCheckCrc())
        {
            input.position(start).limit(start + CHUNK_SIZE - 4);
            int checksum = (int) ChecksumType.CRC32.of(input);

            //Change the limit to include the checksum
            input.limit(start + CHUNK_SIZE);
            if (input.getInt() != checksum)
                throw new CorruptBlockException(channel.filePath(), position, CHUNK_SIZE);
        }

        int length = input.getInt(start + CHUNK_SIZE - FOOTER_LENGTH);
        output.clear();
        input.position(start).limit(start + length);
        encryptor.uncompress(input, output);
        output.flip();

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
        // Encrypted chunks are not cached, so nothing to invalidate
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

    public static Standard createStandard(ChannelProxy channel,
            ICompressor encryptor,
            CompressionParams compressionParams,
            long fileLength,
            long overrideLength)
    {
        int maxBytesInPage = EncryptedSequentialWriter.maxBytesInPage(encryptor);

        if (overrideLength <= 0)
        {
            // For encrypted files, we need to calculate the logical data length
            // Each chunk can hold maxBytesInPage of actual data
            // Calculate how many complete chunks we have
            long numChunks = fileLength / CHUNK_SIZE;
            // Calculate the logical data that can be stored
            overrideLength = numChunks * maxBytesInPage;
            // If there's a partial last chunk, add its data
            long lastChunkSize = fileLength % CHUNK_SIZE;
            if (lastChunkSize > 0) {
                // The last chunk might have less data
                overrideLength += Math.max(0, lastChunkSize - (CHUNK_SIZE - maxBytesInPage));
            }
        }

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
        {
            // For encrypted files, we need to calculate the logical data length
            // Each chunk can hold maxBytesInPage of actual data
            // Calculate how many complete chunks we have
            long numChunks = fileLength / CHUNK_SIZE;
            // Calculate the logical data that can be stored
            overrideLength = numChunks * maxBytesInPage;
            // If there's a partial last chunk, add its data
            long lastChunkSize = fileLength % CHUNK_SIZE;
            if (lastChunkSize > 0) {
                // The last chunk might have less data
                overrideLength += Math.max(0, lastChunkSize - (CHUNK_SIZE - maxBytesInPage));
            }
        }
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
                channel.read(input, position);
                decrypt(input, 0, buffer, position);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
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
                decrypt(r.buffer(), (int) (position - r.offset()), buffer, position);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
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
