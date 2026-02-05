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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;
import org.apache.cassandra.metrics.MicrometerCompressionChunkOffsetCacheMetrics;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

/**
 * Block cache for compression chunk offsets stored in the compression info file.
 * <p>
 * The cache stores blocks of offsets in direct {@link ByteBuffer} instances. Each block is
 * reference-counted and cleaned on eviction. This cache is intended for use by
 * {@link org.apache.cassandra.io.compress.CompressionChunkOffsets.BlockCache}.
 * <p>
 * Note that Blocks are weighted by direct buffer capacity; key overhead is not included.
 */
public class CompressionChunkOffsetCache implements CacheSize
{
    private static volatile CompressionChunkOffsetCache INSTANCE;

    public static CompressionChunkOffsetCache get()
    {
        // do not initialize the cache if cache size is non-positive
        long cacheSizeInBytes = getCacheSizeInBytes(PlatformDependent.maxDirectMemory());
        Preconditions.checkArgument(cacheSizeInBytes > 0, "CompressionChunkOffsetCache size must be postive, but got " + cacheSizeInBytes);

        if (INSTANCE == null)
        {
            synchronized (CompressionChunkOffsetCache.class)
            {
                if (INSTANCE == null)
                    INSTANCE = new CompressionChunkOffsetCache(cacheSizeInBytes);
            }
        }
        return INSTANCE;
    }

    @VisibleForTesting
    static synchronized void resetCache()
    {
        if (INSTANCE != null)
            INSTANCE.clear();

        INSTANCE = null;
    }

    static long getCacheSizeInBytes(long maxDirectMemoryInSize)
    {
        String configString = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.getString();
        return FBUtilities.parseHumanReadableConfig(maxDirectMemoryInSize, configString);
    }

    private final Cache<BlockKey, OffsetsBlock> cache;
    private final MicrometerCompressionChunkOffsetCacheMetrics metrics;
    private final long maxSizeInBytes;
    private final SimpleCachedBufferPool bufferPool;
    private final int blockBufferSize;

    CompressionChunkOffsetCache(long maxSizeInBytes)
    {
        this.maxSizeInBytes = maxSizeInBytes;
        this.metrics = new MicrometerCompressionChunkOffsetCacheMetrics(this, "compression_chunk_offsets_cache");
        this.blockBufferSize = blockBufferSize();
        this.bufferPool = createBufferPool(maxSizeInBytes, blockBufferSize);
        RemovalListener<BlockKey, OffsetsBlock> remover = (key, value, cause) ->
        {
            if (value != null)
                value.release();
        };
        cache = Caffeine.newBuilder()
                        .maximumWeight(maxSizeInBytes)
                        .weigher((BlockKey key, OffsetsBlock value) -> Math.min(Integer.MAX_VALUE, value.capacity()))
                        .removalListener(remover)
                        .recordStats(() -> metrics)
                        .build();
    }

    /**
     * Get a cached offsets block for the provided file id and block index, loading it from disk if absent.
     *
     * @param fileId local id for the compression info file
     * @param blockIndex index of the offsets block
     * @param blockLoader loader used to create a block on cache miss. The cache key is provided to the loader so
     *                    callers can reuse one loader instead of allocating a closure per lookup.
     * @return cached or newly loaded offsets block
     */
    OffsetsBlock getBlock(long fileId, int blockIndex, Function<BlockKey, OffsetsBlock> blockLoader)
    {
        BlockKey key = new BlockKey(fileId, blockIndex);
        return cache.get(key, blockLoader);
    }

    public MicrometerCompressionChunkOffsetCacheMetrics getMetrics()
    {
        return metrics;
    }

    ByteBuffer allocateBlockBuffer(int bytesToRead)
    {
        ByteBuffer buffer = bufferPool.createBuffer();
        Preconditions.checkArgument(bytesToRead <= buffer.capacity());
        buffer.limit(bytesToRead);
        return buffer;
    }

    OffsetsBlock wrapBlockBuffer(ByteBuffer buffer)
    {
        return new OffsetsBlock(buffer, bufferPool);
    }

    void releaseBlockBuffer(ByteBuffer buffer)
    {
        bufferPool.releaseBuffer(buffer);
    }

    void invalidate(BlockKey blockKey)
    {
        cache.invalidate(blockKey);
    }

    @Override
    public long capacity()
    {
        return maxSizeInBytes;
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Compression chunk offsets cache size cannot be changed.");
    }

    @Override
    public int size()
    {
        return cache.asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return cache.policy()
                    .eviction()
                    .map(policy -> policy.weightedSize().orElseGet(cache::estimatedSize))
                    .orElseGet(cache::estimatedSize);
    }

    @VisibleForTesting
    public void clear()
    {
        cache.invalidateAll();
        cache.cleanUp();
        bufferPool.emptyBufferPool();
    }

    private static SimpleCachedBufferPool createBufferPool(long maxSizeInBytes, int blockBufferSize)
    {
        long blockCount = maxSizeInBytes / blockBufferSize + (maxSizeInBytes % blockBufferSize == 0 ? 0 : 1);
        long poolSize = blockCount == Long.MAX_VALUE ? Long.MAX_VALUE : blockCount + 1;
        int maxBufferPoolSize = (int) Math.min(Integer.MAX_VALUE, Math.max(1, poolSize));
        return new SimpleCachedBufferPool(maxBufferPoolSize, blockBufferSize, BufferType.OFF_HEAP);
    }

    public static final class BlockKey
    {
        private static final Hasher64 hasher = Hashing.metroHash64();

        private final long fileId;
        private final int blockIndex;
        private final int hashcode;

        public BlockKey(long fileId, int blockIndex)
        {
            this.fileId = fileId;
            this.blockIndex = blockIndex;
            this.hashcode = hasher.hashLongLongToInt(fileId, blockIndex);
        }

        /**
         * @return index of the offsets block within the compression info file
         */
        int blockIndex()
        {
            return blockIndex;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;
            BlockKey that = (BlockKey) other;
            return blockIndex == that.blockIndex && fileId == that.fileId;
        }

        @Override
        public int hashCode()
        {
            return hashcode;
        }
    }

    @VisibleForTesting
    static int blockBufferSize()
    {
        int configuredSize = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_CACHE_BLOCK_SIZE.getInt();
        // round to multiple of 8 bytes
        return Math.max(Long.BYTES, (configuredSize / Long.BYTES) * Long.BYTES);
    }

    /**
     * A cached block of chunk offsets. It owns off-heap buffer and is reference-counted to ensure the memory
     * is released only after eviction and last user release.
     */
    public static class OffsetsBlock
    {
        private static final AtomicIntegerFieldUpdater<OffsetsBlock> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(OffsetsBlock.class, "references");
        private final ByteBuffer offsetsBuffer;
        private final int capacity;
        private final int length;
        @Nullable
        private final SimpleCachedBufferPool bufferPool;

        public volatile int references;

        public OffsetsBlock(ByteBuffer offsetsBuffer)
        {
            this(offsetsBuffer, null);
        }

        private OffsetsBlock(ByteBuffer offsetsBuffer, SimpleCachedBufferPool bufferPool)
        {
            this.offsetsBuffer = offsetsBuffer;
            this.length = offsetsBuffer.remaining();
            this.capacity = offsetsBuffer.capacity();
            this.bufferPool = bufferPool;
            this.references = 1;
            NATIVE_MEMORY_USAGE.addAndGet(capacity);
        }

        /**
         * Increment the reference count if the block has not been released.
         *
         * @return true if the reference was acquired; false if already released
         */
        boolean ref()
        {
            int refCount;
            do
            {
                refCount = references;

                if (refCount == 0)
                    return false; // Payload was released before we managed to reference it.
            }
            while (!referencesUpdater.compareAndSet(this, refCount, refCount + 1));

            return true;
        }

        /**
         * Decrement the reference count and free off-heap memory when it reaches zero.
         */
        void release()
        {
            if (referencesUpdater.decrementAndGet(this) == 0)
            {
                if (bufferPool == null)
                    FileUtils.clean(offsetsBuffer);
                else
                    bufferPool.releaseBuffer(offsetsBuffer);
                NATIVE_MEMORY_USAGE.addAndGet(-capacity);
            }
        }

        /**
         * @return off-heap capacity of this block in bytes
         */
        public int capacity()
        {
            return capacity;
        }

        /**
         * @return number of chunk offsets stored in this block
         */
        public int count()
        {
            return length / Long.BYTES;
        }

        /**
         * Return the chunk offset at the given index within this block. Must be called after successful {@link #ref()}.
         *
         * @param index offset index within this block
         * @return chunk offset value
         */
        public long getLongAtIndex(int index)
        {
            return offsetsBuffer.getLong(index * Long.BYTES);
        }
    }
}
