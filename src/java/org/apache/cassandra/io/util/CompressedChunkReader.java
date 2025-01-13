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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.memory.BufferPools;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    final CompressionMetadata metadata;
    final int maxCompressedLength;
    protected final long startOffset;
    protected final long onDiskStartOffset;

    protected CompressedChunkReader(ChannelProxy channel, CompressionMetadata metadata, long startOffset)
    {
        super(channel, metadata.dataLength + startOffset);
        this.metadata = metadata;
        this.maxCompressedLength = metadata.maxCompressedLength();
        this.startOffset = startOffset;
        assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
        this.onDiskStartOffset = startOffset == 0 ? 0 : metadata.chunkFor(startOffset).offset;
    }

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return metadata.parameters.getCrcCheckChance();
    }

    boolean shouldCheckCrc()
    {
        return metadata.parameters.shouldCheckCrc();
    }

    @Override
    public String toString()
    {
        return String.format(startOffset > 0
                             ? "CompressedChunkReader.%s(%s - %s, chunk length %d, data length %d, slice offset %s)"
                             : "CompressedChunkReader.%s(%s - %s, chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             metadata.compressor().getClass().getSimpleName(),
                             metadata.chunkLength(),
                             metadata.dataLength,
                             startOffset);
    }

    @Override
    public int chunkSize()
    {
        return metadata.chunkLength();
    }

    @Override
    public BufferType preferredBufferType()
    {
        return metadata.compressor().preferredBufferType();
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Aligned(this);
    }

    public ReaderType type()
    {
        return ReaderType.COMPRESSED;
    }

    public static class Standard extends CompressedChunkReader
    {
        // we read the raw compressed bytes into this buffer, then uncompressed them into the provided one.
        public Standard(ChannelProxy channel, CompressionMetadata metadata)
        {
            this(channel, metadata, 0);
        }

        public Standard(ChannelProxy channel, CompressionMetadata metadata, long startOffset)
        {
            super(channel, metadata, startOffset);
        }

        @Override
        public void readChunk(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
                boolean shouldCheckCrc = shouldCheckCrc();
                int length = shouldCheckCrc ? chunk.length + Integer.BYTES // compressed length + checksum length
                                            : chunk.length;

                long chunkOffset = chunk.offset - onDiskStartOffset;
                boolean shouldDecompress = chunk.length < maxCompressedLength;
                if (shouldDecompress || shouldCheckCrc) // when we need to read the CRC too, follow the decompression path to avoid a second channel read call
                {
                    ByteBuffer compressed = BufferPools.forNetworking().getAtLeast(length, metadata.compressor().preferredBufferType());

                    try
                    {
                        compressed.limit(length);
                        if (channel.read(compressed, chunkOffset) != length)
                            throw new CorruptBlockException(channel.filePath(), chunk);

                        if (shouldCheckCrc)
                        {
                            // compute checksum of the compressed data
                            compressed.position(0).limit(chunk.length);
                            int checksum = (int) ChecksumType.CRC32.of(compressed);
                            // the remaining bytes are the checksum
                            compressed.limit(length);
                            int storedChecksum = compressed.getInt();
                            if (storedChecksum != checksum)
                                throw new CorruptBlockException(channel.filePath(), chunk, storedChecksum, checksum);
                        }

                        compressed.position(0).limit(chunk.length);
                        uncompressed.clear();

                        try
                        {
                            if (shouldDecompress)
                                metadata.compressor().uncompress(compressed, uncompressed);
                            else
                                uncompressed.put(compressed);
                        }
                        catch (IOException e)
                        {
                            throw new CorruptBlockException(channel.filePath(), chunk, e);
                        }
                    }
                    finally
                    {
                        BufferPools.forNetworking().put(compressed);
                    }
                }
                else
                {
                    uncompressed.position(0).limit(chunk.length);
                    if (channel.read(uncompressed, chunkOffset) != chunk.length)
                        throw new CorruptBlockException(channel.filePath(), chunk);
                }
                uncompressed.flip();
            }
            catch (CorruptBlockException e)
            {
                StorageProvider.instance.invalidateFileSystemCache(channel.getFile());

                // Make sure reader does not see stale data.
                uncompressed.position(0).limit(0);
                throw new CorruptSSTableException(e, channel.filePath());
            }
        }

        @Override
        public void invalidateIfCached(long position)
        {
        }
    }

    public static class Mmap extends CompressedChunkReader
    {
        protected final MmappedRegions regions;

        public Mmap(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
        {
            this(channel, metadata, regions, 0);
        }

        public Mmap(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions, long startOffset)
        {
            super(channel, metadata, startOffset);
            this.regions = regions;
        }

        @Override
        public void readChunk(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                long segmentOffset = region.offset();
                int chunkOffsetInSegment = Ints.checkedCast(chunk.offset - segmentOffset);
                ByteBuffer compressedChunk = region.buffer();

                try
                {
                    if (shouldCheckCrc())
                    {
                        compressedChunk.position(chunkOffsetInSegment).limit(chunkOffsetInSegment + chunk.length);
                        int checksum = (int) ChecksumType.CRC32.of(compressedChunk);

                        compressedChunk.limit(compressedChunk.capacity());
                        int storedChecksum = compressedChunk.getInt();
                        if (storedChecksum != checksum)
                            throw new CorruptBlockException(channel.filePath(), chunk, storedChecksum, checksum);
                    }

                    compressedChunk.position(chunkOffsetInSegment).limit(chunkOffsetInSegment + chunk.length);
                    uncompressed.clear();

                    if (chunk.length < maxCompressedLength)
                        metadata.compressor().uncompress(compressedChunk, uncompressed);
                    else
                        uncompressed.put(compressedChunk);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                uncompressed.flip();
            }
            catch (CorruptBlockException e)
            {
                // Make sure reader does not see stale data.
                uncompressed.position(0).limit(0);
                throw new CorruptSSTableException(e, channel.filePath());
            }

        }

        public void close()
        {
            regions.closeQuietly();
            super.close();
        }

        @Override
        public void invalidateIfCached(long position)
        {
        }
    }
}
