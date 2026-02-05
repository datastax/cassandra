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

package org.apache.cassandra.io.compress;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;

public class CompressionChunkOffsetCache
{
    private static final class Holder
    {
        private static final CompressionChunkOffsetCache INSTANCE = new CompressionChunkOffsetCache(CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_CACHE_IN_MB.getInt());
    }

    private static final int DEFAULT_BLOCK_BYTES = 64 * 1024;

    private final int offsetsPerBlock;
    private final Cache<BlockKey, long[]> cache;

    CompressionChunkOffsetCache(int maxSizeInMB)
    {
        this(maxSizeInMB, DEFAULT_BLOCK_BYTES);
    }

    CompressionChunkOffsetCache(int maxSizeInMB, int blockSize)
    {
        offsetsPerBlock = Math.max(1, blockSize / Long.BYTES);
        long maxBytes = (long) maxSizeInMB * 1024L * 1024L;
        if (maxBytes <= 0)
            cache = null;
        else
            cache = Caffeine.newBuilder()
                            .maximumWeight(maxBytes)
                            .weigher((BlockKey key, long[] value) -> Math.min(Integer.MAX_VALUE, value.length * Long.BYTES))
                            .build();
    }

    public static CompressionChunkOffsetCache get()
    {
        return CompressionChunkOffsetCache.Holder.INSTANCE;
    }

    long getOffset(File indexFilePath,
                   long offsetsStart,
                   int offsetIndex,
                   int chunkCount,
                   CompressionMetadataReaderType readerType)
    {
        if (offsetIndex < 0 || offsetIndex >= chunkCount)
            throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d",
                                                                             offsetIndex,
                                                                             chunkCount)),
                                              indexFilePath);

        int blockIndex = offsetIndex / offsetsPerBlock;
        int offsetInBlock = offsetIndex % offsetsPerBlock;
        BlockKey key = new BlockKey(indexFilePath, offsetsStart, blockIndex);
        long[] block = cache == null
                       ? loadBlock(key, chunkCount, readerType)
                       : cache.get(key, k -> loadBlock(k, chunkCount, readerType));
        if (offsetInBlock >= block.length)
            throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d",
                                                                             offsetIndex,
                                                                             chunkCount)),
                                              indexFilePath);
        return block[offsetInBlock];
    }

    private long[] loadBlock(BlockKey key, int chunkCount, CompressionMetadataReaderType readerType)
    {
        int blockStartIndex = key.blockIndex * offsetsPerBlock;
        int remaining = chunkCount - blockStartIndex;
        if (remaining <= 0)
            return new long[0];

        int count = Math.min(offsetsPerBlock, remaining);
        int bytesToRead = count * Long.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(bytesToRead);
        // FIXME store channel in CompressionMetadata
        try (FileChannel channel = openChannel(key.file, readerType))
        {
            long position = key.offsetsStart + ((long) blockStartIndex * Long.BYTES);
            int read = 0;
            while (read < bytesToRead)
            {
                int n = channel.read(buffer, position + read);
                if (n < 0)
                    throw new EOFException("EOF reading compression offsets from " + key.file);
                if (n == 0)
                    continue;
                read += n;
            }

            buffer.flip();
            long[] offsets = new long[count];
            buffer.asLongBuffer().get(offsets);
            return offsets;
        }
        catch (EOFException e)
        {
            throw new CorruptSSTableException(e, key.file);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, key.file);
        }
    }

    private static FileChannel openChannel(File indexFilePath, CompressionMetadataReaderType readerType) throws IOException
    {
        if (readerType == CompressionMetadataReaderType.WRITE_TIME)
            return StorageProvider.instance.writeTimeReadFileChannelFor(indexFilePath);
        return indexFilePath.newReadChannel();
    }

    // FIXME this is memory consuming
    public static final class BlockKey
    {
        private final File file;
        private final long offsetsStart;
        private final int blockIndex;

        private BlockKey(File file, long offsetsStart, int blockIndex)
        {
            this.file = file;
            this.offsetsStart = offsetsStart;
            this.blockIndex = blockIndex;
        }

        public boolean equals(Object other)
        {
            if (this == other)
                return true;
            if (!(other instanceof BlockKey))
                return false;
            BlockKey that = (BlockKey) other;
            return offsetsStart == that.offsetsStart
                   && blockIndex == that.blockIndex
                   && Objects.equals(file, that.file);
        }

        public int hashCode()
        {
            return Objects.hash(file, offsetsStart, blockIndex);
        }
    }
}
