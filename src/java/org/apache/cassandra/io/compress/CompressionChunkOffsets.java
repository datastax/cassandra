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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.AbstractReaderFileProxy;
import org.apache.cassandra.io.util.BufferManagingRebufferer;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.INativeLibrary;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

/**
 * Abstraction over chunk offset storage for compression metadata.
 * <p>
 * Implementations may keep offsets in memory, read them on-demand from disk, or use a cache.
 * Offsets are indexed by chunk index relative to the metadata's start chunk.
 * </p>
 */
public interface CompressionChunkOffsets extends AutoCloseable
{
    /**
     * Return the chunk offset at the provided index (0-based within this metadata instance).
     *
     * @param index chunk index relative to the start chunk
     * @return offset of the chunk in the compressed file
     */
    long get(int index);

    /**
     * @return number of offsets in this instance.
     */
    int size();

    /**
     * @return off-heap bytes used by this instance. return 0 if it's backed by cache.
     */
    long offHeapMemoryUsed();

    /**
     * Add any off-heap identities to the provided collection.
     *
     * @param identities collection of tracked identities
     */
    void addTo(Ref.IdentityCollection identities);

    /**
     * @return position after the last chunk in the compressed file, adjusted for partial files.
     */
    long compressedFileLength();

    /**
     * Release any resources held by this instance.
     */
    void close();

    /**
     * Open a read channel for the compression info file, honouring the reader type (read-time vs write-time access).
     */
    static FileChannel openChannel(File indexFilePath, CompressionMetadataReaderType readerType) throws IOException
    {
        if (readerType == CompressionMetadataReaderType.WRITE_TIME)
            return StorageProvider.instance.writeTimeReadFileChannelFor(indexFilePath);
        return indexFilePath.newReadChannel();
    }

    class Empty implements CompressionChunkOffsets
    {
        public long get(int index)
        {
            throw new IndexOutOfBoundsException("Chunk index " + index + " out of bounds for empty offsets");
        }

        public int size()
        {
            return 0;
        }

        public long offHeapMemoryUsed()
        {
            return 0;
        }

        public void addTo(Ref.IdentityCollection identities)
        {
        }

        @Override
        public long compressedFileLength()
        {
            return 0;
        }

        public void close()
        {
        }
    }

    class InMemory implements CompressionChunkOffsets
    {
        private final Memory.LongArray offsets;
        private final long compressedOffsetLength;

        public InMemory(Memory.LongArray offsets, long compressedOffsetLength)
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


    class BlockCache implements CompressionChunkOffsets
    {
        private static final Logger logger = LoggerFactory.getLogger(BlockCache.class);
        private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

        // Used to retry fetching cache from disk when cache referencing races with concurrent cache eviction. This should be rare and 10 should be sufficient.
        private static final int MAX_RETRIES = 10;

        private static final AtomicLong nextFileId = new AtomicLong();

        private final File file;
        // Used to reduce cost of hashing for block cache key. Note that different instances of CompressionChunkOffsets
        // on the same file will result in different cache key.
        private final long fileId;
        private final FileChannel fileChannel;
        private final long offsetsStart;
        private final int baseChunkIndex;
        private final int size;
        private final int chunkCount;
        private final long compressedFileLength;
        private final int offsetsPerBlock;
        private final CompressionChunkOffsetCache cache;
        private final Function<CompressionChunkOffsetCache.BlockKey, CompressionChunkOffsetCache.OffsetsBlock> blockLoader;

        public BlockCache(File file,
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
            this.fileId = nextFileId.incrementAndGet();
            this.fileChannel = CompressionChunkOffsets.openChannel(file, readerType);
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
            this.offsetsPerBlock = CompressionChunkOffsetCache.blockBufferSize() / Long.BYTES;
            this.cache = cache;
            this.blockLoader = this::loadBlock;
        }

        @Override
        public long get(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            int blockIndex = absoluteIndex / offsetsPerBlock;
            int offsetInBlock = absoluteIndex % offsetsPerBlock;

            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            int retries = MAX_RETRIES;
            while (retries-- > 0)
            {
                CompressionChunkOffsetCache.OffsetsBlock block = cache.getBlock(fileId, blockIndex, blockLoader);
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
            noSpamLogger.warn("Failed to reference cached compression chunk offset block for {} block {} after {} retries; reading chunk {} directly from disk",
                              file, blockIndex, MAX_RETRIES, absoluteIndex);
            return getFromDisk(index);
        }

        private long getFromDisk(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
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

        private CompressionChunkOffsetCache.OffsetsBlock loadBlock(CompressionChunkOffsetCache.BlockKey key)
        {
            try
            {
                int blockIndex = key.blockIndex();
                int blockStartIndex = blockIndex * offsetsPerBlock;
                int remaining = chunkCount - blockStartIndex;
                if (remaining <= 0)
                    return new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocate(0));

                int count = Math.min(offsetsPerBlock, remaining);
                int bytesToRead = count * Long.BYTES;
                long position = offsetsStart + ((long) blockStartIndex * Long.BYTES);
                ByteBuffer buffer = cache.allocateBlockBuffer(bytesToRead);
                boolean success = false;
                try
                {
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
                    success = true;
                    return cache.wrapBlockBuffer(buffer);
                }
                finally
                {
                    if (!success)
                        cache.releaseBlockBuffer(buffer);
                }
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
                cache.invalidate(new CompressionChunkOffsetCache.BlockKey(fileId, i));

            FileUtils.closeQuietly(fileChannel);
        }
    }

    class ChunkCache implements CompressionChunkOffsets
    {
        private final File file;
        private final int baseChunkIndex;
        private final int size;
        private final int chunkCount;
        private final long compressedFileLength;
        private final Rebufferer rebufferer;

        public ChunkCache(File file,
                          long offsetsStart,
                          int baseChunkIndex,
                          int size,
                          int endIndex,
                          int chunkCount,
                          long compressedFileLength,
                          CompressionMetadataReaderType readerType) throws IOException
        {
            org.apache.cassandra.cache.ChunkCache cache = org.apache.cassandra.cache.ChunkCache.instance;
            if (cache == null || !cache.isEnabled())
            {
                String msg = "Compression chunk offsets type 'chunk_cache' requires the global chunk cache " +
                             "to be enabled.";
                throw new ConfigurationException(msg);
            }

            this.file = file;
            this.baseChunkIndex = baseChunkIndex;
            this.size = size;
            this.chunkCount = chunkCount;

            OffsetChunkReader chunkReader = new OffsetChunkReader(file,
                                                                  offsetsStart,
                                                                  (long) chunkCount * Long.BYTES,
                                                                  readerType,
                                                                  chunkCacheBlockBufferSize());
            boolean success = false;
            try
            {
                this.rebufferer = cache.maybeWrap(chunkReader).instantiateRebufferer();
                // Save the post-last-chunk position to calculate the last compressed chunk length.
                // Some files add an empty trailing chunk, so we cannot rely on file length here.
                this.compressedFileLength = endIndex < chunkCount
                                            ? getAbsolute(endIndex) - getAbsolute(baseChunkIndex)
                                            : compressedFileLength;
                success = true;
            }
            finally
            {
                if (!success)
                    chunkReader.close();
            }
        }

        @Override
        public long get(int index)
        {
            return getAbsolute(baseChunkIndex + index);
        }

        private long getAbsolute(int absoluteIndex)
        {
            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
            {
                String msg = String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount);
                throw new CorruptSSTableException(new EOFException(msg), file);
            }

            long position = (long) absoluteIndex * Long.BYTES;
            Rebufferer.BufferHolder holder = rebufferer.rebuffer(position);
            try
            {
                ByteBuffer buffer = holder.buffer();
                int offset = Math.toIntExact(position - holder.offset());
                if (offset < 0 || offset + Long.BYTES > buffer.limit())
                {
                    String msg = String.format("Chunk %d offset is outside cached buffer: offset=%d limit=%d",
                                               absoluteIndex, offset, buffer.limit());
                    throw new CorruptSSTableException(new EOFException(msg), file);
                }
                return buffer.getLong(offset);
            }
            finally
            {
                holder.release();
            }
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public long offHeapMemoryUsed()
        {
            // Chunk cache memory is shared and reported through chunk cache metrics.
            return 0;
        }

        @Override
        public void addTo(Ref.IdentityCollection identities)
        {
            // no-op - the chunk cache manages its own memory
        }

        @Override
        public long compressedFileLength()
        {
            return compressedFileLength;
        }

        @Override
        public void close()
        {
            rebufferer.close();
        }

        @VisibleForTesting
        static int chunkCacheBlockBufferSize()
        {
            int configuredSize = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_CHUNK_CACHE_BLOCK_SIZE
                                                            .getInt();
            int roundedSize = Math.max(Long.BYTES, (configuredSize / Long.BYTES) * Long.BYTES);
            if (Integer.bitCount(roundedSize) != 1)
            {
                CassandraRelevantProperties blockSizeProperty =
                    CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_CHUNK_CACHE_BLOCK_SIZE;
                String blockSizeKey = blockSizeProperty.getKey();
                String typeKey = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_TYPE.getKey();
                String msg = String.format("%s must resolve to a power of two when %s is 'chunk_cache'; " +
                                           "got %d",
                                           blockSizeKey, typeKey, roundedSize);
                throw new ConfigurationException(msg);
            }
            return roundedSize;
        }

        private static final class OffsetChunkReader extends AbstractReaderFileProxy implements ChunkReader
        {
            private final long offsetsStart;
            private final long offsetsLength;
            private final int chunkSize;

            private OffsetChunkReader(File file,
                                      long offsetsStart,
                                      long offsetsLength,
                                      CompressionMetadataReaderType readerType,
                                      int chunkSize) throws IOException
            {
                super(new ChannelProxy(file, CompressionChunkOffsets.openChannel(file, readerType)),
                      offsetsLength);
                this.offsetsStart = offsetsStart;
                this.offsetsLength = offsetsLength;
                this.chunkSize = chunkSize;
            }

            @Override
            public void readChunk(long position, ByteBuffer buffer)
            {
                if (position < 0 || position >= offsetsLength)
                {
                    buffer.position(0).limit(0);
                    return;
                }

                int bytesToRead = Math.toIntExact(Math.min(buffer.capacity(), offsetsLength - position));
                buffer.clear().limit(bytesToRead);

                int read = 0;
                long filePosition = offsetsStart + position;
                while (read < bytesToRead)
                {
                    int n = channel.read(buffer, filePosition + read);
                    if (n < 0)
                    {
                        String msg = "EOF reading compression offsets from " + channel.getFile();
                        throw new CorruptSSTableException(new EOFException(msg), channel.getFile());
                    }
                    if (n == 0)
                        continue;
                    read += n;
                }
                buffer.flip();
            }

            @Override
            public int chunkSize()
            {
                return chunkSize;
            }

            @Override
            public BufferType preferredBufferType()
            {
                return BufferType.OFF_HEAP;
            }

            @Override
            public Rebufferer instantiateRebufferer()
            {
                return new BufferManagingRebufferer.Aligned(this);
            }

            @Override
            public ReaderType type()
            {
                return ReaderType.COMPRESSION_OFFSET;
            }

            @Override
            public void close()
            {
                channel.close();
            }
        }
    }

    class Mmap implements CompressionChunkOffsets
    {
        private final File file;
        private final int baseChunkIndex;
        private final int size;
        private final int chunkCount;
        private final long compressedFileLength;
        // Bytes per segment, captured once at construction. A single MappedByteBuffer can map at most
        // Integer.MAX_VALUE bytes, so larger offset sections are split into multiple segments. Rounded down to a whole
        // number of 8-byte offsets so that no offset straddles a segment boundary, and floored at one offset so a
        // misconfigured value cannot reach 0.
        private final int segmentBytes;
        private final MappedByteBuffer[] segments;

        public Mmap(File file,
                    long offsetsStart,
                    int baseChunkIndex,
                    int size,
                    int endIndex,
                    int chunkCount,
                    long compressedFileLength,
                    CompressionMetadataReaderType readerType) throws IOException
        {
            this.file = file;
            this.baseChunkIndex = baseChunkIndex;
            this.size = size;
            this.chunkCount = chunkCount;
            int maxSegmentBytes = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_MMAP_SEGMENT_SIZE.getInt();
            this.segmentBytes = Math.max(Long.BYTES, (maxSegmentBytes / Long.BYTES) * Long.BYTES);
            this.segments = mapOffsets(file, offsetsStart, chunkCount, readerType, segmentBytes);

            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last
            // chunk). Obviously, we could use the compressed file length for that purpose but unfortunately, sometimes
            // there is an empty chunk added to the end of the file thus we cannot rely on the file length.
            this.compressedFileLength = endIndex < chunkCount ? getAbsolute(endIndex) - getAbsolute(baseChunkIndex)
                                                              : compressedFileLength;
        }

        private static MappedByteBuffer[] mapOffsets(File file, long offsetsStart, int chunkCount,
                                                     CompressionMetadataReaderType readerType, int segmentBytes) throws IOException
        {
            long offsetsBytes = (long) chunkCount * Long.BYTES;
            int segmentCount = Math.toIntExact((offsetsBytes + segmentBytes - 1) / segmentBytes);
            MappedByteBuffer[] segments = new MappedByteBuffer[segmentCount];

            // The mapping remains valid after the channel is closed, so we don't keep a file descriptor open.
            try (FileChannel channel = CompressionChunkOffsets.openChannel(file, readerType))
            {
                for (int i = 0; i < segmentCount; i++)
                {
                    long position = offsetsStart + (long) i * segmentBytes;
                    long segmentSize = Math.min(segmentBytes, offsetsBytes - (long) i * segmentBytes);
                    MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, segmentSize);
                    // Offsets are accessed randomly, so disable OS read-ahead for the mapped region.
                    INativeLibrary.instance.adviseRandom(buffer, segmentSize, file.path());
                    segments[i] = buffer;
                }
            }
            catch (IOException | RuntimeException e)
            {
                for (MappedByteBuffer buffer : segments)
                    FileUtils.clean(buffer);
                throw e;
            }
            return segments;
        }

        private long getAbsolute(int absoluteIndex)
        {
            long byteOffset = (long) absoluteIndex * Long.BYTES;
            int segmentIndex = (int) (byteOffset / segmentBytes);
            int offsetInSegment = (int) (byteOffset % segmentBytes);
            return segments[segmentIndex].getLong(offsetInSegment);
        }

        @Override
        public long get(int index)
        {
            int absoluteIndex = baseChunkIndex + index;
            if (absoluteIndex < 0 || absoluteIndex >= chunkCount)
                throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", absoluteIndex, chunkCount)), file);

            return getAbsolute(absoluteIndex);
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public long offHeapMemoryUsed()
        {
            // memory-mapped pages are managed by the OS page cache, not tracked here.
            return 0;
        }

        @Override
        public void addTo(Ref.IdentityCollection identities)
        {
            // no-op - mapped memory is not Cassandra-managed off-heap memory
        }

        @Override
        public long compressedFileLength()
        {
            return compressedFileLength;
        }

        @Override
        public void close()
        {
            for (MappedByteBuffer buffer : segments)
                FileUtils.clean(buffer);
        }
    }
}
