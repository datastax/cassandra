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

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_FACTORY;
import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

/**
 * Factory for {@link CompressionChunkOffsets} implementations.
 * <p>
 * Compression chunk offsets selection is controlled by a single configuration property:
 * {@link CassandraRelevantProperties#COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE}
 * </p>
 * <p>
 * When cache size is positive (default: 10% of max direct memory):
 * - Uses {@link BlockCacheChunkOffsets} that loads offsets in 64KB blocks on-demand
 * - Blocks are cached with LRU eviction, preventing OOM
 * - Recommended for production use
 * </p>
 * <p>
 * When cache size is zero or negative:
 * - Falls back to legacy {@link InMemoryCompressionChunkOffsets} implementation
 * - Loads all offsets into a single Memory.LongArray
 * - Kept for rollback purposes only in case of issues with block-cached implementation
 * </p>
 */
public interface CompressionChunkOffsetsFactory
{
    CompressionChunkOffsetsFactory instance = COMPRESSION_CHUNK_OFFSETS_FACTORY.isPresent()
                                              ? FBUtilities.construct(COMPRESSION_CHUNK_OFFSETS_FACTORY.getString(), "Compression Chunk Offsets Factory")
                                              : new CompressionChunkOffsetsFactory() {};

    /**
     * Create a {@link CompressionChunkOffsets} implementation from the provided compression metadata stream.
     *
     * @param indexFilePath        path to the compression info file
     * @param input                stream positioned at the start of chunk offsets
     * @param offsetsStart         position in the metadata file where chunk offsets begin
     * @param startIndex           first chunk index to include (inclusive)
     * @param endIndex             last chunk index to include (exclusive)
     * @param chunkCount           total number of chunks in the file
     * @param compressedFileLength compressed data file length
     * @param readerType           read-time or write-time access mode
     * @return a chunk offsets implementation
     */
    default CompressionChunkOffsets getInstance(File indexFilePath, TrackedDataInputPlus input, long offsetsStart,
                                                int startIndex, int endIndex,
                                                int chunkCount, long compressedFileLength,
                                                CompressionMetadataReaderType readerType) throws IOException
    {
        if (chunkCount == 0)
            return new CompressionChunkOffsets.Empty();

        CompressionChunkOffsetCache cache = CompressionChunkOffsetCache.get();
        
        // If cache is enabled (size > 0), use block-cached implementation
        if (cache != null && cache.capacity() > 0)
            return new BlockCacheChunkOffsets(indexFilePath, offsetsStart, startIndex, endIndex - startIndex, 
                                             endIndex, chunkCount, compressedFileLength, readerType, cache);
        
        // Otherwise fall back to legacy in-memory implementation
        return createInMemoryOffsets(indexFilePath, input, startIndex, endIndex, chunkCount, compressedFileLength);
    }

    /**
     * Creates a {@link CompressionChunkOffsets} for metadata when opening the CompressionMetadata.Writer for early-open
     * SSTableReader or final SSTableReader.
     * <p>
     * Note that compression info file is written when completing the sstable writing. During early-open, we expect
     * the compression info file is not present; in that case this method always uses in-memory offsets.
     * <p>
     * When it's called after completing sstable writing with compression info, in-memory limit admission is applied and
     * the writer's in-memory offsets may be released if on-disk offsets are selected.
     * </p>
     * @param indexFilePath path to the compression info file
     * @param memoryChunkOffsets in-memory offsets buffer built by the writer
     * @param offsetsStart position of the offsets table in the compression info file
     * @param startIndex first chunk index to include (inclusive)
     * @param endIndex last chunk index to include (exclusive)
     * @param chunkCount total number of chunks in the file
     * @param compressedFileLength compressed data file length
     * @param isCompressionInfoWritten whether the compression info file is fully written
     * @return a chunk offsets implementation for use in early-open or final SSTableReader
     */
    default CompressionChunkOffsets getInstanceOnWriterComplete(File indexFilePath, Memory.LongArray memoryChunkOffsets,
                                                                long offsetsStart, int startIndex, int endIndex, int chunkCount,
                                                                long compressedFileLength, boolean isCompressionInfoWritten) throws IOException
    {
        if (chunkCount == 0)
            return new CompressionChunkOffsets.Empty();

        // When CompressionMetadata.Writer completes, the opened CompressionMetadata is used into the final SSTableReader.
        // This is considered READ_TIME as sstable has completed writing and the file will be used during READ_TIME
        CompressionMetadataReaderType readerType = CompressionMetadataReaderType.READ_TIME;
        
        // During early open, compression info file is not written yet, so use in-memory offsets
        if (!isCompressionInfoWritten)
        {
            NATIVE_MEMORY_USAGE.addAndGet(memoryChunkOffsets.memoryUsed());
            return new CompressionChunkOffsetsFactory.InMemoryCompressionChunkOffsets(memoryChunkOffsets, compressedFileLength);
        }

        CompressionChunkOffsetCache cache = CompressionChunkOffsetCache.get();
        
        // If cache is enabled (size > 0), use block-cached implementation
        if (cache != null && cache.capacity() > 0)
        {
            // Release writer's in-memory offsets since we'll use block-cached implementation
            memoryChunkOffsets.close();
            return new BlockCacheChunkOffsets(indexFilePath, offsetsStart, startIndex, endIndex - startIndex,
                                             endIndex, chunkCount, compressedFileLength, readerType, cache);
        }
        
        // Otherwise use legacy in-memory implementation with writer's offsets
        NATIVE_MEMORY_USAGE.addAndGet(memoryChunkOffsets.memoryUsed());
        return new CompressionChunkOffsetsFactory.InMemoryCompressionChunkOffsets(memoryChunkOffsets, compressedFileLength);
    }



    static CompressionChunkOffsets createInMemoryOffsets(File indexFilePath, TrackedDataInputPlus input,
                                                         int startIndex, int endIndex, int chunkCount,
                                                         long compressedFileLength)
    {
        Preconditions.checkState(startIndex < chunkCount, "The start index %s has to be < chunk count %s", startIndex, chunkCount);
        Preconditions.checkState(endIndex <= chunkCount, "The end index %s has to be <= chunk count %s", endIndex, chunkCount);
        Preconditions.checkState(startIndex <= endIndex, "The start index %s has to be < end index %s", startIndex, endIndex);

        int chunksToRead = endIndex - startIndex;
        if (chunksToRead == 0)
            return new CompressionChunkOffsets.Empty();

        Memory.LongArray offsets = new Memory.LongArray(chunksToRead);
        long i = 0;
        try
        {
            input.skipBytes(startIndex * 8);
            long lastOffset;
            for (i = 0; i < chunksToRead; i++)
            {
                lastOffset = input.readLong();
                offsets.set(i, lastOffset);
            }

            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last chunk).
            // Obvously, we could use the compressed file length for that purpose but unfortunately, sometimes there is
            // an empty chunk added to the end of the file thus we cannot rely on the file length.
            lastOffset = endIndex < chunkCount ? input.readLong() - offsets.get(0) : compressedFileLength;
            NATIVE_MEMORY_USAGE.getAndAdd(offsets.memoryUsed());
            return new InMemoryCompressionChunkOffsets(offsets, lastOffset);
        }
        catch (EOFException e)
        {
            offsets.close();
            String msg = String.format("Corrupted Index File %s: read %d but expected at least %d chunks.", input, i, chunksToRead);
            throw new CorruptSSTableException(new IOException(msg, e), indexFilePath.toPath());
        }
        catch (IOException e)
        {
            offsets.close();
            throw new FSReadError(e, indexFilePath.toPath());
        }
        catch (RuntimeException e)
        {
            offsets.close();
            throw e;
        }
    }

    class InMemoryCompressionChunkOffsets implements CompressionChunkOffsets
    {
        private final Memory.LongArray offsets;
        private final long compressedOffsetLength;

        public InMemoryCompressionChunkOffsets(Memory.LongArray offsets, long compressedOffsetLength)
        {
            this.offsets = offsets;
            this.compressedOffsetLength = compressedOffsetLength;
        }

        public long get(int index)
        {
            return offsets.get(index);
        }

        public int size()
        {
            return Math.toIntExact(offsets.size());
        }

        public long offHeapMemoryUsed()
        {
            return offsets.memoryUsed();
        }

        public void addTo(Ref.IdentityCollection identities)
        {
            if (offsets.memory != null)
                identities.add(offsets.memory);
        }

        @Override
        public long compressedFileLength()
        {
            return compressedOffsetLength;
        }

        public void close()
        {
            NATIVE_MEMORY_USAGE.addAndGet(-offsets.memory.size());
            offsets.close();
        }
    }

    class BlockCacheChunkOffsets implements CompressionChunkOffsets
    {
        // Num of bytes per cache block. This value divides by 8 is the num of chunk offsets per cache block.
        public static final int DEFAULT_BLOCK_BYTES = Integer.parseInt(System.getProperty("cassandra.compression_chunk_offsets_block_size", "65536")); // 64 * 1024
        // Used to retry fetching cache from disk when cache referencing races with concurrent cache eviction. This should be rare and 10 should be sufficient.
        private static final int MAX_RETIRES = 10;

        private final File file;
        private final FileChannel fileChannel;
        private final long offsetsStart;
        private final int baseChunkIndex;
        private final int size;
        private final int chunkCount;
        private final long compressedFileLength;
        private final int offsetsPerBlock;
        private final CompressionChunkOffsetCache cache;

        public BlockCacheChunkOffsets(File file,
                                      long offsetsStart,
                                      int baseChunkIndex,
                                      int size,
                                      int endIndex,
                                      int chunkCount,
                                      long compressedFileLength,
                                      CompressionMetadataReaderType readerType,
                                      CompressionChunkOffsetCache cache) throws IOException
        {
            this.file = file;
            this.fileChannel = openChannel(file, readerType);
            this.offsetsStart = offsetsStart;
            this.baseChunkIndex = baseChunkIndex;
            this.size = size;
            this.chunkCount = chunkCount;
            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last chunk).
            // Obviously, we could use the compressed file length for that purpose but unfortunately, sometimes there is
            // an empty chunk added to the end of the file thus we cannot rely on the file length.
            long lastOffset = endIndex < chunkCount ? getFromDisk(endIndex - baseChunkIndex) - getFromDisk(0) : compressedFileLength;
            this.compressedFileLength = lastOffset;
            this.offsetsPerBlock = Math.max(1, DEFAULT_BLOCK_BYTES / Long.BYTES);
            this.cache = cache;
        }

        private static FileChannel openChannel(File indexFilePath, CompressionMetadataReaderType readerType) throws IOException
        {
            if (readerType == CompressionMetadataReaderType.WRITE_TIME)
                return StorageProvider.instance.writeTimeReadFileChannelFor(indexFilePath);
            return indexFilePath.newReadChannel();
        }

        @Override
        public long get(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            int blockIndex = absoluteIndex / offsetsPerBlock;
            int offsetInBlock = absoluteIndex % offsetsPerBlock;

            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            int retries = MAX_RETIRES;
            while (retries-- > 0)
            {
                CompressionChunkOffsetCache.OffsetsBlock block = cache.getBlock(file, blockIndex, () -> loadBlock(blockIndex, offsetsPerBlock, offsetsStart, chunkCount));
                if (block.ref())
                {
                    try
                    {
                        int count = block.count();
                        if (offsetInBlock >= count)
                            throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", offsetInBlock, count)), file);
                        return block.getLongAtIndex(offsetInBlock);
                    }
                    finally
                    {
                        block.release();
                    }
                }
                // let's retry, if the block is concurrently evicted from cache and fails to ref
            }
            // fall to read from on-disk offset directly without caching
            return getFromDisk(index);
        }

        private long getFromDisk(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
            try
            {
                int read = 0;
                long position = offsetsStart + (long) absoluteIndex * Long.BYTES;
                while (read < Long.BYTES)
                {
                    int n = fileChannel.read(buffer, position + read);
                    if (n < 0)
                        throw new EOFException("EOF reading compression offsets from " + file);
                    if (n == 0)
                        continue;
                    read += n;
                }
                buffer.flip();
                return buffer.getLong();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int size()
        {
            return size;
        }

        private CompressionChunkOffsetCache.OffsetsBlock loadBlock(int blockIndex, int offsetsPerBlock, long offsetsStart, int chunkCount)
        {
            try
            {
                int blockStartIndex = blockIndex * offsetsPerBlock;
                int remaining = chunkCount - blockStartIndex;
                if (remaining <= 0)
                    return new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocateDirect(0));

                int count = Math.min(offsetsPerBlock, remaining);
                int bytesToRead = count * Long.BYTES;
                long position = offsetsStart + ((long) blockStartIndex * Long.BYTES);
                ByteBuffer buffer = ByteBuffer.allocateDirect(bytesToRead);
                int read = 0;
                while (read < bytesToRead)
                {
                    int n = fileChannel.read(buffer, position + read);
                    if (n < 0)
                        throw new EOFException("EOF reading compression offsets from " + file);
                    if (n == 0)
                        continue;
                    read += n;
                }
                buffer.flip();
                return new CompressionChunkOffsetCache.OffsetsBlock(buffer);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long offHeapMemoryUsed()
        {
            // offheap memory usage per file is not tracked in cache. return 0 here.
            return 0;
        }

        @Override
        public void addTo(Ref.IdentityCollection identities)
        {
            // no-op - cache manages its own memory
        }

        @Override
        public long compressedFileLength()
        {
            return compressedFileLength;
        }

        @Override
        public void close()
        {
            // Invalidate all blocks from cache because we don't track what blocks are cached
            int blockCount = (int) Math.ceil(chunkCount * 1.0 / offsetsPerBlock);
            for (int i = 0; i < blockCount; i++)
                cache.invalidate(new CompressionChunkOffsetCache.BlockKey(file, i));

            FileUtils.closeQuietly(fileChannel);
        }
    }
}
