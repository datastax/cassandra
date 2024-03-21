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

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.cliffc.high_scale_lib.NonBlockingIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ParkedExecutor;
import org.apache.cassandra.concurrent.ShutdownableExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

public class ChunkCache
        implements RemovalListener<ChunkCache.Key, ChunkCache.Buffer>, CacheSize
{
    private final static Logger logger = LoggerFactory.getLogger(ChunkCache.class);

    public static final int RESERVED_POOL_SPACE_IN_MB = 32;
    private static final int INITIAL_CAPACITY = Integer.getInteger("cassandra.chunkcache_initialcapacity", 16);
    private static final boolean ASYNC_CLEANUP = Boolean.parseBoolean(System.getProperty("cassandra.chunkcache.async_cleanup", "true"));
    private static final int CLEANER_THREADS = Integer.getInteger("dse.chunk.cache.cleaner.threads",1);

    private static final Class PERFORM_CLEANUP_TASK_CLASS;

    static
    {
        try
        {
            logger.info("-Dcassandra.chunkcache.async_cleanup={} dse.chunk.cache.cleaner.threads={}", ASYNC_CLEANUP, CLEANER_THREADS);
            PERFORM_CLEANUP_TASK_CLASS = Class.forName("com.github.benmanes.caffeine.cache.BoundedLocalCache$PerformCleanupTask");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static final boolean roundUp = DatabaseDescriptor.getFileCacheRoundUp();

    public static final ChunkCache instance = DatabaseDescriptor.getFileCacheEnabled()
                                              ? new ChunkCache(BufferPools.forChunkCache(), DatabaseDescriptor.getFileCacheSizeInMB(), ChunkCacheMetrics::create)
                                              : null;

    private final BufferPool bufferPool;

    // Relies on the implementation detail that keys are interned strings and can be compared by reference.
    // Because compute optimistically updates the set without acquiring a lock, we must use a set that is
    // safe for concurrent access.
    private final NonBlockingIdentityHashMap<String, NonBlockingHashSet<Key>> keysByFile;
    private final AsyncCache<Key, Buffer> cache;
    private final Cache<Key, Buffer> synchronousCache;
    private final ConcurrentMap<Key, CompletableFuture<Buffer>> cacheAsMap;
    private final long cacheSize;
    public final ChunkCacheMetrics metrics;
    private final ShutdownableExecutor cleanupExecutor;

    private boolean enabled;
    private Function<ChunkReader, RebuffererFactory> wrapper = this::wrap;

    static class Key
    {
        final ChunkReader file;
        final String internedPath;
        final long position;
        final int hashCode;

        /**
         * Attention!  internedPath must be interned by caller -- intern() is too expensive
         * to be done for every Key instantiation.
         */
        private Key(ChunkReader file, String internedPath, long position)
        {
            super();
            this.file = file;
            this.position = position;
            this.internedPath = internedPath;
            hashCode = hashCodeInternal();
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private int hashCodeInternal()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + internedPath.hashCode();
            result = prime * result + Long.hashCode(position);
            result = prime * result + Integer.hashCode(file.chunkSize());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;

            Key other = (Key) obj;
            return (position == other.position)
                   && internedPath == other.internedPath // == is okay b/c we explicitly intern
                   && file.chunkSize() == other.file.chunkSize(); // TODO we should not allow different chunk sizes
        }
    }

    class Buffer implements Rebufferer.BufferHolder
    {
        private final ByteBuffer buffer;
        private final long offset;
        private final AtomicInteger references;

        public Buffer(ByteBuffer buffer, long offset)
        {
            this.buffer = buffer;
            this.offset = offset;
            references = new AtomicInteger(1);  // start referenced.
        }

        Buffer reference()
        {
            int refCount;
            do
            {
                refCount = references.get();
                if (refCount == 0)
                    // Buffer was released before we managed to reference it.
                    return null;
            } while (!references.compareAndSet(refCount, refCount + 1));

            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            assert references.get() > 0;
            return buffer.duplicate();
        }

        @Override
        public FloatBuffer floatBuffer()
        {
            assert references.get() > 0;
            // this does an implicit duplicate(), so we need to expose it directly to avoid doing it twice unnecessarily
            return buffer.asFloatBuffer();
        }

        @Override
        public IntBuffer intBuffer()
        {
            assert references.get() > 0;
            // this does an implicit duplicate(), so we need to expose it directly to avoid doing it twice unnecessarily
            return buffer.asIntBuffer();
        }

        @Override
        public LongBuffer longBuffer()
        {
            assert references.get() > 0;
            // this does an implicit duplicate(), so we need to expose it directly to avoid doing it twice unnecessarily
            return buffer.asLongBuffer();
        }


        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public void release()
        {
            if (references.decrementAndGet() == 0)
                bufferPool.put(buffer);
        }
    }

    public ChunkCache(BufferPool pool, int cacheSizeInMB, Function<ChunkCache, ChunkCacheMetrics> createMetrics)
    {
        cacheSize = 1024L * 1024L * Math.max(0, cacheSizeInMB - RESERVED_POOL_SPACE_IN_MB);
        cleanupExecutor = ParkedExecutor.createParkedExecutor("ChunkCacheCleanup", CLEANER_THREADS);
        enabled = cacheSize > 0;
        bufferPool = pool;
        metrics = createMetrics.apply(this);
        keysByFile = new NonBlockingIdentityHashMap<>();
        cache = Caffeine.newBuilder()
                        .maximumWeight(cacheSize)
                        .initialCapacity(INITIAL_CAPACITY)
                        .executor(r -> {
                            if (ASYNC_CLEANUP && r.getClass() == PERFORM_CLEANUP_TASK_CLASS)
                                cleanupExecutor.execute(r);
                            else
                                r.run();
                        })
                        .weigher((key, buffer) -> ((Buffer) buffer).buffer.capacity())
                        .removalListener(this)
                        .recordStats(() -> metrics)
                        .buildAsync();
        synchronousCache = cache.synchronous();
        cacheAsMap = cache.asMap();
    }


    private Buffer load(Key key)
    {
        ByteBuffer buffer = bufferPool.get(key.file.chunkSize(), key.file.preferredBufferType());
        assert buffer != null;
        try
        {
            key.file.readChunk(key.position, buffer);
        }
        catch (RuntimeException t)
        {
            bufferPool.put(buffer);
            throw t;
        }
        // Complete addition within compute remapping function to ensure there is no race condition with removal.
        keysByFile.compute(key.internedPath, (k, v) -> {
            if (v == null)
                v = new NonBlockingHashSet<>();
            v.add(key);
            return v;
        });
        return new Buffer(buffer, key.position);
    }

    @Override
    public void onRemoval(Key key, Buffer buffer, RemovalCause cause)
    {
        buffer.release();
        // Complete addition within compute remapping function to ensure there is no race condition with load.
        keysByFile.compute(key.internedPath, (k, v) -> {
            if (v == null)
                return null;
            v.remove(key);
            return v.isEmpty() ? null : v;
        });
    }

    /**
     * Clears the cache, used in the CNDB Writer for testing purposes.
     */
    public void clear() {
        // Clear keysByFile first to prevent unnecessary computation in onRemoval method.
        keysByFile.clear();
        synchronousCache.invalidateAll();
    }

    public void close()
    {
        clear();
        try
        {
            cleanupExecutor.shutdown();
        }
        catch (InterruptedException e)
        {
            logger.debug("Interrupted during shutdown: ", e);
        }
    }

    private RebuffererFactory wrap(ChunkReader file)
    {
        return new CachingRebufferer(file);
    }

    public RebuffererFactory maybeWrap(ChunkReader file)
    {
        if (!enabled)
            return file;

        return wrapper.apply(file);
    }

    public void invalidateFile(String filePath)
    {
        var internedPath = filePath.intern();
        var keys = keysByFile.remove(internedPath);
        if (keys == null)
            return;
        keys.forEach(synchronousCache::invalidate);
    }

    @VisibleForTesting
    public void enable(boolean enabled)
    {
        this.enabled = enabled;
        wrapper = this::wrap;
        synchronousCache.invalidateAll();
        metrics.reset();
    }

    @VisibleForTesting
    public void intercept(Function<RebuffererFactory, RebuffererFactory> interceptor)
    {
        final Function<ChunkReader, RebuffererFactory> prevWrapper = wrapper;
        wrapper = rdr -> interceptor.apply(prevWrapper.apply(rdr));
    }

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified ChunkReader.
     * Thread-safe. One instance per SegmentedFile, created by ChunkCache.maybeWrap if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer, RebuffererFactory
    {
        private final ChunkReader source;
        private final String internedPath;
        final long alignmentMask;

        public CachingRebufferer(ChunkReader file)
        {
            source = file;
            internedPath = source.channel().filePath().intern();
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1 : String.format("%d must be a power of two", chunkSize);
            alignmentMask = -chunkSize;
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            try
            {
                long pageAlignedPos = position & alignmentMask;
                BufferHolder buf = null;
                Key chunkKey = new Key(source, internedPath, pageAlignedPos);

                int spin = 0;
                //There is a small window when a released buffer/invalidated chunk
                //is still in the cache. In this case it will return null
                //so we spin loop while waiting for the cache to re-populate
                while (buf == null)
                {
                    Buffer chunk;
                    // Using cache.get(k, compute) results in lots of allocation, rather risk the unlikely race...
                    CompletableFuture<Buffer> cachedValue = cache.getIfPresent(chunkKey);
                    if (cachedValue == null)
                    {
                        CompletableFuture<Buffer> entry = new CompletableFuture<>();
                        CompletableFuture<Buffer> existing = cacheAsMap.putIfAbsent(chunkKey, entry);
                        if (existing == null)
                        {
                            try
                            {
                                chunk = load(chunkKey);
                            }
                            catch (Throwable t)
                            {
                                // please note that we don't need to remove the entry from the cache here
                                // because Caffeine automatically removes entries that complete exceptionally

                                // also signal other waiting readers
                                entry.completeExceptionally(t);
                                throw t;
                            }
                            entry.complete(chunk);
                        }
                        else
                        {
                            chunk = existing.join();
                        }
                    }
                    else
                    {
                        chunk = cachedValue.join();
                    }

                    buf = chunk.reference();

                    if (buf == null && ++spin == 1000)
                    {
                        String msg = String.format("Could not acquire a reference to for %s after 1000 attempts. " +
                                                   "This is likely due to the chunk cache being too small for the " +
                                                   "number of concurrently running requests.", chunkKey);
                        throw new RuntimeException(msg);
                        // Note: this might also be caused by reference counting errors, especially double release of
                        // chunks.
                    }
                }
                return buf;
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
        }

        @Override
        public Rebufferer instantiateRebufferer()
        {
            return this;
        }

        @Override
        public void invalidateIfCached(long position)
        {
            long pageAlignedPos = position & alignmentMask;
            synchronousCache.invalidate(new Key(source, internedPath, pageAlignedPos));
        }

        @Override
        public void close()
        {
            source.close();
        }

        @Override
        public void closeReader()
        {
            // Instance is shared among readers. Nothing to release.
        }

        @Override
        public ChannelProxy channel()
        {
            return source.channel();
        }

        @Override
        public long fileLength()
        {
            return source.fileLength();
        }

        @Override
        public double getCrcCheckChance()
        {
            return source.getCrcCheckChance();
        }

        @Override
        public String toString()
        {
            return "CachingRebufferer:" + source;
        }
    }

    @Override
    public long capacity()
    {
        return cacheSize;
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Chunk cache size cannot be changed.");
    }

    @Override
    public int size()
    {
        return cache.asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return synchronousCache.policy().eviction()
                .map(policy -> policy.weightedSize().orElseGet(synchronousCache::estimatedSize))
                .orElseGet(synchronousCache::estimatedSize);
    }

    /**
     * Returns the number of cached chunks of given file.
     */
    @VisibleForTesting
    public int sizeOfFile(String filePath) {
        var internedPath = filePath.intern();
        return (int) cacheAsMap.keySet().stream().filter(x -> x.internedPath == internedPath).count();
    }
}
