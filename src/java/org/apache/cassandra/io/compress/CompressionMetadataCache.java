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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.Memory;

/**
 * Cache for compression metadata chunk offsets. This cache stores blocks of chunk offsets
 * using off-heap memory (Memory.LongArray) to reduce memory usage while maintaining good
 * performance for compressed SSTable reads.
 *
 * The cache uses an LRU eviction policy and is bounded by a configurable size limit.
 * All CompressionMetadata instances use this cache (always-cache approach) to ensure
 * predictable memory usage.
 *
 * Design follows ChunkCache pattern: Caffeine cache structure is on-heap, but actual
 * chunk offset data is stored off-heap in Memory.LongArray blocks.
 *
 * Note: Cache misses trigger asynchronous I/O operations to avoid blocking the calling thread.
 * This prevents latency spikes on the read hot path, though cache misses will still incur
 * I/O overhead. Monitor p99 latency in production to assess impact.
 */
public class CompressionMetadataCache
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionMetadataCache.class);

    // Singleton instance
    public static final CompressionMetadataCache instance = new CompressionMetadataCache();

    // Track native memory usage (similar to CompressionMetadata.NATIVE_MEMORY_USAGE)
    private static final AtomicLong NATIVE_MEMORY_USAGE = new AtomicLong(0);

    // Default cache size: 2048MB (2GB)
    private static final long DEFAULT_CACHE_SIZE_MB =
        Long.getLong("cassandra.compression_metadata_cache_size_mb", 2048);

    // Number of chunks per cache block (trade-off: memory vs I/O)
    // Default: 1024 chunks per block
    private static final int BLOCK_SIZE =
        Integer.getInteger("cassandra.compression_metadata_cache_block_size", 1024);

    // Feature flag to enable/disable the cache (default: disabled for gradual rollout)
    private static final boolean CACHE_ENABLED =
        Boolean.getBoolean("cassandra.compression_metadata_cache_enabled");

    private final AsyncCache<ChunkBlockKey, ChunkBlock> cache;
    private final Cache<ChunkBlockKey, ChunkBlock> synchronousCache;
    private final long cacheSizeBytes;
    private final int blockSize;
    private boolean enabled; // Not final - can be toggled for testing
    private final CompressionMetadataCacheMetrics metrics;
    private final Executor executor;

    private CompressionMetadataCache()
    {
        boolean cacheEnabled = DatabaseDescriptor.getCompressionMetadataCacheEnabled();
        int cacheSizeMB = DatabaseDescriptor.getCompressionMetadataCacheSizeMB();
        this.blockSize = DatabaseDescriptor.getCompressionMetadataCacheBlockSize();

        this.cacheSizeBytes = cacheSizeMB * 1024L * 1024L;
        this.enabled = cacheEnabled && cacheSizeBytes > 0;

        if (enabled)
        {
            logger.info("Initializing CompressionMetadataCache: enabled={}, size={}MB ({}bytes), block_size={} chunks",
                       true, cacheSizeMB, cacheSizeBytes, blockSize);

            this.metrics = new CompressionMetadataCacheMetrics();
            this.executor = java.util.concurrent.ForkJoinPool.commonPool();

            this.cache = Caffeine.newBuilder()
                .maximumWeight(cacheSizeBytes)
                .weigher((Weigher<ChunkBlockKey, ChunkBlock>) (key, value) -> value.memorySize())
                .removalListener(this::onEviction)
                .recordStats(() -> metrics)
                .buildAsync();
            this.synchronousCache = cache.synchronous();

            logger.info("CompressionMetadataCache initialized successfully with async loading");
        }
        else
        {
            logger.info("CompressionMetadataCache: enabled={}, cache_size={}MB - cache is DISABLED",
                       cacheEnabled, cacheSizeMB);
            this.cache = null;
            this.synchronousCache = null;
            this.metrics = null;
            this.executor = null;
        }
    }

    /**
     * Check if the cache is enabled and properly initialized.
     * Returns true only if the cache was initialized at startup with proper configuration.
     */
    public boolean isEnabled()
    {
        return enabled && cache != null;
    }

    /**
     * Enable or disable the cache at runtime (for testing).
     * Clears all cached entries when toggling.
     */
    @VisibleForTesting
    public void enable(boolean enabled)
    {
        this.enabled = enabled;
        clear();
        if (metrics != null)
        {
            metrics.reset();
        }
    }

    /**
     * Get native memory usage by cached blocks.
     */
    public static long nativeMemoryUsage()
    {
        return NATIVE_MEMORY_USAGE.get();
    }

    /**
     * Get a single chunk's offset and length.
     *
     * @param metadataFile Path to the compression metadata file
     * @param chunkIndex Index of the chunk to retrieve
     * @return Chunk with offset and length
     */
    public CompressionMetadata.Chunk getChunk(Path metadataFile, int chunkIndex) throws IOException
    {
        if (!isEnabled())
            throw new IllegalStateException("CompressionMetadataCache is not enabled");

        int blockIndex = chunkIndex / blockSize;
        ChunkBlockKey key = new ChunkBlockKey(metadataFile, blockIndex);

        ChunkBlock block = getBlockFromCache(key);

        int indexInBlock = chunkIndex - block.startChunkIndex;
        if (indexInBlock < 0 || indexInBlock >= block.count)
        {
            throw new CorruptSSTableException(
                new EOFException(String.format("Chunk index %d out of bounds in block (start=%d, count=%d)",
                                              chunkIndex, block.startChunkIndex, block.count)),
                metadataFile.toString());
        }

        long offset = block.offsets.get(indexInBlock);
        long nextOffset = indexInBlock < block.count - 1
            ? block.offsets.get(indexInBlock + 1)
            : block.nextOffset;

        int length = (int) (nextOffset - offset - 4); // -4 for checksum
        return new CompressionMetadata.Chunk(offset, length);
    }

    /**
     * Get multiple chunks' offsets and lengths in a batch operation.
     * This is more efficient than calling getChunk() multiple times when chunks
     * are in the same cache block, as it reduces cache lookups.
     *
     * @param metadataFile Path to the compression metadata file
     * @param chunkIndices Array of chunk indices to retrieve (must be sorted)
     * @return Array of Chunks corresponding to the requested indices
     */
    public CompressionMetadata.Chunk[] getChunks(Path metadataFile, int[] chunkIndices) throws IOException
    {
        if (!isEnabled())
            throw new IllegalStateException("CompressionMetadataCache is not enabled");

        if (chunkIndices.length == 0)
            return new CompressionMetadata.Chunk[0];

        CompressionMetadata.Chunk[] result = new CompressionMetadata.Chunk[chunkIndices.length];

        // Process chunks by block to minimize cache lookups
        int currentBlockIndex = -1;
        ChunkBlock currentBlock = null;

        for (int i = 0; i < chunkIndices.length; i++)
        {
            int chunkIndex = chunkIndices[i];
            int blockIndex = chunkIndex / blockSize;

            // Load new block if needed
            if (blockIndex != currentBlockIndex)
            {
                ChunkBlockKey key = new ChunkBlockKey(metadataFile, blockIndex);
                currentBlock = getBlockFromCache(key);
                currentBlockIndex = blockIndex;
            }

            // Extract chunk from current block
            int indexInBlock = chunkIndex - currentBlock.startChunkIndex;
            if (indexInBlock < 0 || indexInBlock >= currentBlock.count)
            {
                throw new CorruptSSTableException(
                    new EOFException(String.format("Chunk index %d out of bounds in block (start=%d, count=%d)",
                                                  chunkIndex, currentBlock.startChunkIndex, currentBlock.count)),
                    metadataFile.toString());
            }

            long offset = currentBlock.offsets.get(indexInBlock);
            long nextOffset = indexInBlock < currentBlock.count - 1
                ? currentBlock.offsets.get(indexInBlock + 1)
                : currentBlock.nextOffset;

            int length = (int) (nextOffset - offset - 4); // -4 for checksum
            result[i] = new CompressionMetadata.Chunk(offset, length);
        }

        return result;
    }

    /**
     * Helper method to get a chunk block from cache, handling async loading and exception unwrapping.
     */
    private ChunkBlock getBlockFromCache(ChunkBlockKey key) throws IOException
    {
        try
        {
            return cache.get(key, (k, exec) ->
                CompletableFuture.supplyAsync(() -> {
                    try
                    {
                        return loadChunkBlock(k);
                    }
                    catch (IOException e)
                    {
                        throw new UncheckedIOException("Failed to load chunk block: " + k, e);
                    }
                }, exec)
            ).get();
        }
        catch (java.util.concurrent.ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof UncheckedIOException)
                throw ((UncheckedIOException) cause).getCause();
            if (cause instanceof IOException)
                throw (IOException) cause;
            throw new IOException("Failed to load chunk block", e);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while loading chunk block", e);
        }
    }

    /**
     * Load a block of chunk offsets from the compression metadata file.
     *
     * @param key The cache key containing file path and block index
     * @return ChunkBlock containing the loaded offsets
     */
    private ChunkBlock loadChunkBlock(ChunkBlockKey key) throws IOException
    {
        File file = new File(key.metadataFile);
        if (key.blockIndex > Integer.MAX_VALUE / blockSize)
            throw new IllegalArgumentException("Block index too large");
        int startChunkIndex = key.blockIndex * blockSize;

        try (FileInputStreamPlus stream = file.newInputStream())
        {
            // Read header to get to chunk offsets
            String compressorName = stream.readUTF();
            int optionCount = stream.readInt();
            for (int i = 0; i < optionCount; i++)
            {
                stream.readUTF(); // key
                stream.readUTF(); // value
            }
            int chunkLength = stream.readInt();
            stream.readInt(); // maxCompressedSize
            long dataLength = stream.readLong();
            int totalChunks = stream.readInt();

            if (totalChunks <= 0)
                throw new IOException("Invalid chunk count: " + totalChunks);

            if (startChunkIndex >= totalChunks)
            {
                throw new CorruptSSTableException(
                    new EOFException(String.format("Start chunk index %d >= total chunks %d",
                                                  startChunkIndex, totalChunks)),
                    file.path());
            }

            // Calculate how many chunks to read in this block
            int chunksToRead = Math.min(blockSize, totalChunks - startChunkIndex);

            // Skip to the start of our block (each offset is 8 bytes)
            long bytesToSkip = startChunkIndex * 8L;
            while (bytesToSkip > 0)
            {
                int toSkip = (int) Math.min(bytesToSkip, Integer.MAX_VALUE);
                stream.skipBytes(toSkip);
                bytesToSkip -= toSkip;
            }

            // Allocate off-heap memory for offsets
            Memory.LongArray offsets = new Memory.LongArray(chunksToRead);
            NATIVE_MEMORY_USAGE.addAndGet(offsets.memoryUsed());

            try
            {
                // Read chunk offsets into off-heap memory
                for (int i = 0; i < chunksToRead; i++)
                {
                    offsets.set(i, stream.readLong());
                }

                // Read one more offset to calculate the last chunk's length
                long nextOffset;
                if (startChunkIndex + chunksToRead < totalChunks)
                {
                    nextOffset = stream.readLong();
                }
                else
                {
                    // Last block - get actual compressed file length for accurate last chunk size
                    // Use Descriptor API to get the data file path from the compression metadata file path
                    Descriptor desc = Descriptor.fromFilename(file.path());
                    File dataFile = desc.fileFor(Component.DATA);
                    nextOffset = dataFile.length();
                }

                return new ChunkBlock(offsets, startChunkIndex, chunksToRead, nextOffset);
            }
            catch (Exception e)
            {
                // Clean up on error
                NATIVE_MEMORY_USAGE.addAndGet(-offsets.memoryUsed());
                offsets.close();
                throw e;
            }
        }
    }

    /**
     * Clear all cache entries. Used for testing.
     */
    @VisibleForTesting
    public void clear()
    {
        if (synchronousCache != null)
        {
            synchronousCache.invalidateAll();
        }
        if (metrics != null)
        {
            metrics.reset();
        }
    }

    /**
     * Get the metrics object for testing.
     */
    @VisibleForTesting
    CompressionMetadataCacheMetrics getMetricsForTesting()
    {
        return metrics;
    }

    /**
     * Get cache statistics for monitoring.
     */
    public CacheStats getStats()
    {
        if (!enabled)
            return new CacheStats(0, 0, 0, 0, 0);

        com.github.benmanes.caffeine.cache.stats.CacheStats stats = synchronousCache.stats();
        return new CacheStats(
            stats.hitCount(),
            stats.missCount(),
            stats.evictionCount(),
            synchronousCache.estimatedSize(),
            synchronousCache.policy().eviction().map(e -> e.weightedSize().orElse(0L)).orElse(0L)
        );
    }

    /**
     * Get the metrics object for this cache.
     */
    public CompressionMetadataCacheMetrics getMetrics()
    {
        return metrics;
    }

    /**
     * Get current cache size (number of entries).
     */
    public long size()
    {
        return enabled ? synchronousCache.estimatedSize() : 0;
    }

    /**
     * Get current cache weight (bytes).
     */
    public long weightBytes()
    {
        if (!enabled)
            return 0;

        return synchronousCache.policy().eviction()
            .map(e -> e.weightedSize().orElse(0L))
            .orElse(0L);
    }

    private void onEviction(ChunkBlockKey key, ChunkBlock value, RemovalCause cause)
    {
        // Release off-heap memory
        try
        {
            value.close();
        }
        catch (Exception e)
        {
            logger.error("Failed to close chunk block", e);
        }
        logger.trace("Evicted chunk block {} (cause: {})", key, cause);
    }

    /**
     * Cache key for chunk blocks.
     */
    public static class ChunkBlockKey
    {
        final Path metadataFile;
        final int blockIndex;

        public ChunkBlockKey(Path metadataFile, int blockIndex)
        {
            this.metadataFile = metadataFile;
            this.blockIndex = blockIndex;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChunkBlockKey that = (ChunkBlockKey) o;
            return blockIndex == that.blockIndex && Objects.equals(metadataFile, that.metadataFile);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(metadataFile, blockIndex);
        }

        @Override
        public String toString()
        {
            return String.format("ChunkBlockKey{file=%s, block=%d}", metadataFile, blockIndex);
        }
    }

    /**
     * A block of chunk offsets stored in off-heap memory.
     * Uses Memory.LongArray for efficient off-heap storage, similar to CompressionMetadata.
     */
    public static class ChunkBlock implements AutoCloseable
    {
        // Approximate JVM object overhead
        private static final int OBJECT_OVERHEAD_BYTES = 64;
        
        final Memory.LongArray offsets;  // Off-heap storage
        final int startChunkIndex;
        final int count;
        final long nextOffset;  // Offset after the last chunk in this block

        public ChunkBlock(Memory.LongArray offsets, int startChunkIndex, int count, long nextOffset)
        {
            this.offsets = offsets;
            this.startChunkIndex = startChunkIndex;
            this.count = count;
            this.nextOffset = nextOffset;
        }

        /**
         * Calculate memory size of this block for cache weighing.
         */
        int memorySize()
        {
            // Memory.LongArray size + object overhead
            return (int) offsets.memoryUsed() + OBJECT_OVERHEAD_BYTES;
        }

        @Override
        public void close()
        {
            NATIVE_MEMORY_USAGE.addAndGet(-offsets.memoryUsed());
            offsets.close();
        }
    }

    /**
     * Cache statistics for monitoring.
     */
    public static class CacheStats
    {
        public final long hitCount;
        public final long missCount;
        public final long evictionCount;
        public final long size;
        public final long weightedSize;

        public CacheStats(long hitCount, long missCount, long evictionCount, long size, long weightedSize)
        {
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.size = size;
            this.weightedSize = weightedSize;
        }

        public double hitRate()
        {
            long total = hitCount + missCount;
            return total == 0 ? 0.0 : (double) hitCount / total;
        }
    }
}
