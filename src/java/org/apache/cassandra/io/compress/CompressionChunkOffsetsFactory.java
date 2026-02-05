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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import io.netty.util.concurrent.FastThreadLocal;
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
 * Based on {@link CassandraRelevantProperties#COMPRESSION_CHUNK_OFFSETS_TYPE},
 * - When {@code IN_MEMORY} is selected, all offsets are loaded into off-heap memory.</li>
 * - When {@code ON_DISK} is selected, offsets are read directly from the compression info file.</li>
 * - When {@code ON_DISK_WITH_CACHE} is selected, offsets are read in blocks and cached.</li>
 * </p>
 * Configs:
 * - {@link CassandraRelevantProperties#COMPRESSION_CHUNK_OFFSETS_TYPE} - type of compression offset implementation
 * - {@link CassandraRelevantProperties#COMPRESSION_CHUNK_OFFSETS_CACHE_IN_MB}: cache size for block caching.
 *       Non-positive value to disable caching and fallck back to {@link CompressionChunkOffsets.Type#ON_DISK}
 * <p>
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

        CompressionChunkOffsets.Type type = CompressionChunkOffsets.Type.valueOf(CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_TYPE.getString());
        switch (type)
        {
            case IN_MEMORY:
                return createInMemoryOffsets(indexFilePath, input, startIndex, endIndex, chunkCount, compressedFileLength);
            case ON_DISK:
                return createOnDiskOffsets(indexFilePath, offsetsStart, startIndex, endIndex, chunkCount, compressedFileLength, readerType, null);
            case ON_DISK_WITH_CACHE:
                CompressionChunkOffsetCache cache = CompressionChunkOffsetCache.get();
                return createOnDiskOffsets(indexFilePath, offsetsStart, startIndex, endIndex, chunkCount, compressedFileLength, readerType, cache);
            default:
                throw new UnsupportedOperationException(type + " is not supported");
        }
    }

    /**
     * Creates a {@link CompressionChunkOffsets} for metadata when opening the CompressionMetadata.Writer for early-open
     * SSTableReader or final SSTableReader.
     * <p>
     * Note that compression info file is written when completing the sstable writing. During early-open, we expect
     * the compression info file is not present; in that case this method forces {@link CompressionChunkOffsets.Type#IN_MEMORY}
     * regardless of the configured type.
     * <p>
     * When it's called after completing sstable writing with compression info, the configured type is honored and the in-memory
     * offsets are closed.
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

        // if it's early open, compression info file is not written yet, fall back to IN_MEMORY
        CompressionChunkOffsets.Type type = isCompressionInfoWritten
                                            ? CompressionChunkOffsets.Type.valueOf(CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_TYPE.getString())
                                            : CompressionChunkOffsets.Type.IN_MEMORY;
        // When CompressionMetadata.Writer completes, the opened CompressionMetadata is used into the final SSTableReader.
        // This is considered READ_TIME as sstable has completed writing and the file will be used during READ_TIME
        CompressionMetadataReaderType readerType = CompressionMetadataReaderType.READ_TIME;
        switch (type)
        {
            case IN_MEMORY:
                NATIVE_MEMORY_USAGE.addAndGet(memoryChunkOffsets.memoryUsed());
                return new CompressionChunkOffsetsFactory.InMemoryCompressionChunkOffsets(memoryChunkOffsets, compressedFileLength);
            case ON_DISK:
                // release IN_MEMORY offsets if ON_DISK is used
                memoryChunkOffsets.close();
                return createOnDiskOffsets(indexFilePath, offsetsStart, startIndex, endIndex, chunkCount, compressedFileLength, readerType, null);
            case ON_DISK_WITH_CACHE:
                // release IN_MEMORY offsets if ON_DISK_WITH_CACHE is used
                memoryChunkOffsets.close();
                CompressionChunkOffsetCache cache = CompressionChunkOffsetCache.get();
                return createOnDiskOffsets(indexFilePath, offsetsStart, startIndex, endIndex, chunkCount, compressedFileLength, readerType, cache);
            default:
                throw new UnsupportedOperationException(type + " is not supported");
        }
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
            NATIVE_MEMORY_USAGE.addAndGet(offsets.memoryUsed());
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
            NATIVE_MEMORY_USAGE.addAndGet(-offHeapMemoryUsed());
            offsets.close();
        }
    }


    static CompressionChunkOffsets createOnDiskOffsets(File indexFilePath,
                                                       long offsetsStart,
                                                       int startIndex,
                                                       int endIndex,
                                                       int chunkCount,
                                                       long compressedFileLength,
                                                       CompressionMetadataReaderType readerType,
                                                       @Nullable CompressionChunkOffsetCache cache) throws IOException
    {
        Preconditions.checkState(chunkCount >= 0, "Negative chunk count %s for %s", chunkCount, indexFilePath);
        Preconditions.checkState(startIndex < chunkCount || chunkCount == 0,
                                 "The start index %s has to be < chunk count %s", startIndex, chunkCount);
        Preconditions.checkState(endIndex <= chunkCount,
                                 "The end index %s has to be <= chunk count %s", endIndex, chunkCount);
        Preconditions.checkState(startIndex <= endIndex,
                                 "The start index %s has to be < end index %s", startIndex, endIndex);
        int chunksToRead = Math.max(0, endIndex - startIndex);
        if (chunksToRead == 0)
        {
            return new CompressionChunkOffsets.Empty();
        }
        else if (cache == null)
        {
            return new OnDiskChunkOffsets(indexFilePath, offsetsStart, startIndex, chunksToRead, endIndex, chunkCount, compressedFileLength, readerType);
        }
        else
        {
            return new BlockCacheChunkOffsets(indexFilePath, offsetsStart, startIndex, chunksToRead, endIndex, chunkCount, compressedFileLength, readerType, cache);
        }
    }

    class OnDiskChunkOffsets implements CompressionChunkOffsets
    {
        static FastThreadLocal<ByteBuffer> buffers = new FastThreadLocal<>()
        {
            protected ByteBuffer initialValue()
            {
                return ByteBuffer.allocateDirect(Long.BYTES);
            }
        };

        final File file;
        final FileChannel fileChannel;
        final long offsetsStart;
        final int baseChunkIndex;
        final int size;
        final int chunkCount;
        final long compressedFileLength;

        public OnDiskChunkOffsets(File file,
                                  long offsetsStart,
                                  int baseChunkIndex,
                                  int size,
                                  int endIndex,
                                  int chunkCount,
                                  long compressedFileLength,
                                  CompressionMetadataReaderType readerType) throws IOException
        {
            this.file = file;
            this.fileChannel = openChannel(file, readerType);
            this.offsetsStart = offsetsStart;
            this.baseChunkIndex = baseChunkIndex;
            this.size = size;
            this.chunkCount = chunkCount;
            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last chunk).
            // Obvously, we could use the compressed file length for that purpose but unfortunately, sometimes there is
            // an empty chunk added to the end of the file thus we cannot rely on the file length.
            long lastOffset = endIndex < chunkCount ? get(endIndex - baseChunkIndex) - get(0) : compressedFileLength;
            this.compressedFileLength = lastOffset;
        }

        public long get(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            ByteBuffer buffer = buffers.get();
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
            finally
            {
                buffer.clear();
            }
        }

        private static FileChannel openChannel(File indexFilePath, CompressionMetadataReaderType readerType) throws IOException
        {
            if (readerType == CompressionMetadataReaderType.WRITE_TIME)
                return StorageProvider.instance.writeTimeReadFileChannelFor(indexFilePath);
            return indexFilePath.newReadChannel();
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public long offHeapMemoryUsed()
        {
            return 0;
        }

        @Override
        public void addTo(Ref.IdentityCollection identities)
        {
            // no-op
        }

        @Override
        public long compressedFileLength()
        {
            return compressedFileLength;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(fileChannel);
        }
    }

    class BlockCacheChunkOffsets extends OnDiskChunkOffsets
    {
        // Num of bytes per cache block. This value divides by 8 is the num of chunk offsets per cache block.
        private static final int DEFAULT_BLOCK_BYTES = 64 * 1024;
        private static final int MAX_RETIRES = 10;

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
            super(file, offsetsStart, baseChunkIndex, size, endIndex, chunkCount, compressedFileLength, readerType);
            this.offsetsPerBlock = Math.max(1, DEFAULT_BLOCK_BYTES / Long.BYTES);
            this.cache = cache;
        }

        public long get(int index)
        {
            // During initialization, cache is not setup yet.
            if (cache == null)
                return super.get(index);

            int absoluteIndex = baseChunkIndex + index;
            int blockIndex = absoluteIndex / offsetsPerBlock;
            int offsetInBlock = absoluteIndex % offsetsPerBlock;

            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            int retries = MAX_RETIRES;
            while (retries-- > 0)
            {
                CompressionChunkOffsetCache.OffsetsBlock block = cache.getBlock(file, offsetsStart, blockIndex, () -> loadBlock(blockIndex, offsetsPerBlock, offsetsStart, chunkCount));
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
            return super.get(index);
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
    }
}
