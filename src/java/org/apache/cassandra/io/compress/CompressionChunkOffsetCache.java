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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

/**
 * Block cache for compression chunk offsets stored in the compression info file.
 * <p>
 * The cache stores blocks of offsets in direct {@link ByteBuffer} instances. Each block is
 * reference-counted and cleaned on eviction. This cache is intended for use by
 * {@link CompressionChunkOffsetsFactory.BlockCacheChunkOffsets}.
 * <p>
 * Note that Blocks are weighted by direct buffer capacity; key overhead is not included.
 */
public class CompressionChunkOffsetCache
{
    private static volatile CompressionChunkOffsetCache INSTANCE;

    public static CompressionChunkOffsetCache get()
    {
        // do not initialize the cache if cache size is non-positive
        long cacheSizeInBytes = getCacheSizeInBytes(PlatformDependent.maxDirectMemory());
        if (cacheSizeInBytes <= 0)
            return null;

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
        String configString = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_ON_DISK_CACHE_SIZE.getString();
        return FBUtilities.parseHumanReadableConfig(maxDirectMemoryInSize, configString);
    }

    private final Cache<BlockKey, OffsetsBlock> cache;

    CompressionChunkOffsetCache(long maxSizeInBytes)
    {
        RemovalListener<BlockKey, OffsetsBlock> remover = (key, value, cause) ->
        {
            if (value != null)
                value.release();
        };
        cache = Caffeine.newBuilder()
                        .maximumWeight(maxSizeInBytes)
                        .weigher((BlockKey key, OffsetsBlock value) -> Math.min(Integer.MAX_VALUE, value.capacity()))
                        .removalListener(remover)
                        .build();
    }

    OffsetsBlock getBlock(File indexFilePath, long offsetsStart, int blockIndex, Supplier<OffsetsBlock> blockLoader)
    {
        BlockKey key = new BlockKey(indexFilePath, offsetsStart, blockIndex);
        return cache.get(key, k -> blockLoader.get());
    }

    public long offHeapMemoryUsage()
    {
        return cache.policy()
                    .eviction()
                    .map(eviction -> eviction.weightedSize().orElse(0L))
                    .orElse(0L);
    }

    @VisibleForTesting
    public void clear()
    {
        cache.invalidateAll();
    }

    public static final class BlockKey
    {
        private final File file;
        private final long offsetsStart;
        private final int blockIndex;
        private final int hashcode;

        private BlockKey(File file, long offsetsStart, int blockIndex)
        {
            this.file = file;
            this.offsetsStart = offsetsStart;
            this.blockIndex = blockIndex;
            this.hashcode = Objects.hash(file, offsetsStart, blockIndex);
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;
            if (!(other instanceof BlockKey))
                return false;
            BlockKey that = (BlockKey) other;
            return offsetsStart == that.offsetsStart
                   && blockIndex == that.blockIndex
                   // file object should be the same
                   && file == that.file;
        }

        @Override
        public int hashCode()
        {
            return hashcode;
        }
    }

    public static class OffsetsBlock
    {
        private static final AtomicIntegerFieldUpdater<OffsetsBlock> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(OffsetsBlock.class, "references");
        private final ByteBuffer offsetsBuffer;
        private final int capacity;
        private final int count;

        public volatile int references;

        public OffsetsBlock(ByteBuffer offsetsBuffer)
        {
            this.offsetsBuffer = offsetsBuffer;
            this.capacity = offsetsBuffer.capacity();
            this.count = capacity / Long.BYTES;
            this.references = 1;
            NATIVE_MEMORY_USAGE.addAndGet(capacity);
        }

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

        void release()
        {
            if (referencesUpdater.decrementAndGet(this) == 0)
            {
                FileUtils.clean(offsetsBuffer);
                NATIVE_MEMORY_USAGE.addAndGet(-capacity);
            }
        }

        public int capacity()
        {
            return capacity;
        }

        public int count()
        {
            return count;
        }

        public long getLongAtIndex(int index)
        {
            return offsetsBuffer.getLong(index * Long.BYTES);
        }

        public ByteBuffer buffer()
        {
            return offsetsBuffer;
        }
    }
}
