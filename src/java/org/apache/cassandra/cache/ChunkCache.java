/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cache;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

public class ChunkCache implements CacheSize
{
    private static final int MAX_BUFFER_POOL_SIZE_IN_MB = Math.toIntExact(BufferPools.forChunkCache().maxPoolSize() / 1024 / 1024);

    // Leave a bit of space for buffers that might still be in use but already evicted from the cache (in-flight data)
    // We pick 5% of the total cache size, and at least 32 MB (by default). This is somewhat arbitrary but no other sensible
    // limits are evident at the moment.
    private static final int INFLIGHT_DATA_OVERHEAD_IN_MB = Integer.getInteger("dse.cache.inflight_data_overhead_in_mb", 32);

    // number of bits required to store the log2 of the chunk size (highestOneBit(highestOneBit(Integer.MAX_VALUE)))
    private final static int CHUNK_SIZE_LOG2_BITS = 5;


    private static final long cacheSize = 1024L * 1024L * Math.max(0, MAX_BUFFER_POOL_SIZE_IN_MB - INFLIGHT_DATA_OVERHEAD_IN_MB);
    public static final boolean roundUp = DatabaseDescriptor.getFileCacheRoundUp();
    private static final DiskOptimizationStrategy diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();
    public static final ChunkCache instance = cacheSize > 0 ? new ChunkCache() : null;
    private Function<ChunkReader, RebuffererFactory> wrapper = this::wrap;

    public final ChunkCacheMetrics metrics;
    private final ChunkCacheImpl pageCache;

    // Global file id management
    private static final ConcurrentHashMap<String, Long> fileId = new ConcurrentHashMap<>();
    private static final AtomicLong nextFileId = new AtomicLong(0);

    public ChunkCache()
    {
        this.metrics = ChunkCacheMetrics.create(this, "chunk-cache");
        this.pageCache = new ChunkCacheImpl(metrics,
                                            cacheSize, BufferPools.forChunkCache());
    }

    public ChunkCache(BufferPool bufferPool, int cacheSize, Function<ChunkCache, ChunkCacheMetrics> metrics)
    {
        this.metrics = metrics.apply(this);
        this.pageCache = new ChunkCacheImpl(this.metrics, cacheSize, bufferPool);
    }

    private RebuffererFactory wrap(ChunkReader file)
    {
        return pageCache.wrap(file);
    }

    public RebuffererFactory maybeWrap(ChunkReader file)
    {
        return wrapper.apply(file);
    }

    @VisibleForTesting
    public void enable(boolean enabled)
    {
        wrapper = enabled ? this::wrap : x -> x;
        pageCache.reset();
    }

    @VisibleForTesting
    public void intercept(Function<RebuffererFactory, RebuffererFactory> interceptor)
    {
        final Function<ChunkReader, RebuffererFactory> prevWrapper = wrapper;
        wrapper = rdr -> interceptor.apply(prevWrapper.apply(rdr));
    }


    /**
     * Removes FileId for given file if cache is initialized.
     * @param file file to remove from the map.
     */
    public static void removeFileIdFromCache(String file)
    {
        if (instance != null) instance.invalidateFile(file);
    }

    /**
     * Invalidate all buffers from the given file, i.e. make sure they can not be accessed by any reader using a
     * FileHandle opened after this call. The buffers themselves will remain in the cache until they get normally
     * evicted, because it is too costly to remove them.
     *
     * Note that this call has no effect of handles that are already opened. The correct usage is to call this when
     * a file is deleted, or when a file is created for writing. It cannot be used to update and resynchronize the
     * cached view of an existing file.
     */
    public void invalidateFile(String file)
    {
        // Removing the name from the id map suffices -- the next time someone wants to read this file, it will get
        // assigned a fresh id.
        fileId.remove(file);
    }

    protected static long assignFileId(String file)
    {
        return nextFileId.getAndIncrement();
    }

    /**
     * Maps a reader to a file id, used by the cache to find content.
     *
     * Uses the file name (through the fileId map), reader type and chunk size to define the id.
     * the next {@link #CHUNK_SIZE_LOG2_BITS}
     * are occupied by log 2 of chunk size (we assume the chunk size is the power of 2), and the rest of the bits
     * are occupied by fileId counter which is incremented with for each unseen file name)
     */
    protected static long fileIdFor(String file, int chunkSize)
    {
        return (((fileId.computeIfAbsent(file, ChunkCache::assignFileId)
                  << CHUNK_SIZE_LOG2_BITS) | Integer.numberOfTrailingZeros(chunkSize)));
    }

    /**
     * Maps a reader to a file id, used by the cache to find content.
     */
    protected static long fileIdFor(ChunkReader source)
    {
        return fileIdFor(source.channel().filePath(), source.chunkSize());
    }

    @Override
    public long capacity()
    {
        return pageCache.capacity();
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new AssertionError("Can't set capacity of chunk cache");
    }

    @Override
    public int size()
    {
        return pageCache.size();
    }

    @Override
    public long weightedSize()
    {
        return pageCache.weightedSize();
    }

    @VisibleForTesting // see javadoc in ChunkCacheImpl
    public void checkForLeakedChunks()
    {
        pageCache.checkForLeakedChunks();
    }

    @VisibleForTesting
    public Set<String> getFileIdMapKeysForTable(String keyspace, String table)
    {
        return fileId.keySet().stream()
                     .filter(x -> x.contains(keyspace))
                     .filter(x -> x.contains(table))
                     .collect(Collectors.toSet());
    }
}
