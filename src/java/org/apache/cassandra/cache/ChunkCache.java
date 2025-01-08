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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.cliffc.high_scale_lib.NonBlockingIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.cassandra.concurrent.ParkedExecutor;
import org.apache.cassandra.concurrent.ShutdownableExecutor;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.ThreadLocalByteBufferHolder;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

public class ChunkCache
        implements RemovalListener<ChunkCache.Key, ChunkCache.Chunk>, CacheSize
{
    private final static Logger logger = LoggerFactory.getLogger(ChunkCache.class);

    public static final int RESERVED_POOL_SPACE_IN_MB = 32;
    private static final int INITIAL_CAPACITY = Integer.getInteger("cassandra.chunkcache_initialcapacity", 16);
    private static final boolean ASYNC_CLEANUP = Boolean.parseBoolean(System.getProperty("cassandra.chunkcache.async_cleanup", "true"));
    private static final int CLEANER_THREADS = Integer.getInteger("dse.chunk.cache.cleaner.threads",1);

    private static final Class PERFORM_CLEANUP_TASK_CLASS;
    // cached value in order to not call System.getProperty on a hotpath
    private static final int CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS = CassandraRelevantProperties.CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS.getInt();

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
    private final AsyncCache<Key, Chunk> cache;
    private final Cache<Key, Chunk> synchronousCache;
    private final ConcurrentMap<Key, CompletableFuture<Chunk>> cacheAsMap;
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

    abstract static class Chunk
    {
        volatile int references = 1; // Start referenced

        /**
         * Return the correct buffer depending on the position requested by the client, also taking a reference to this
         * chunk.
         *
         * @param position the position requested by the client, this has not been aligned to neither the chunk nor the page
         * @return the buffer covering this position, or null if no reference can be taken
         */
        @Nullable
        Rebufferer.BufferHolder getReferencedBuffer(long position)
        {
            int refCount;
            do
            {
                refCount = references;

                if (refCount == 0)
                    return null; // Buffer was released before we managed to reference it.

            } while (!referencesUpdater.compareAndSet(this, refCount, refCount + 1));

            return getBuffer(position);
        }

        /**
         * Release the chunk when the cache or a reader no longer needs it.
         */
        public void release()
        {
            if (referencesUpdater.decrementAndGet(this) == 0)
                releaseBuffers();
        }

        <B> Chunk handleLoadResult(B loadResult, Throwable loadError)
        {
            if (loadError == null)
                return this;

            release();
            throw Throwables.propagate(loadError);
        }

        /**
         * Load the data for this chunk.
         *
         * @param key The key / file & position that this chunk corresponds to.
         * @return a future holding the loaded chunk when it completes.
         */
        abstract void read(Key key);

        /**
         * Return the correct buffer depending on the position requested by the client. The returned buffer cannot be
         * null, but may be empty and must contain the given position
         * (i.e. buf.offset <= position <= buf.offset + buf.buffer.limit where the latter can only be == if at the end
         * of the file and buffer is empty).
         *
         * @param position the position requested by the client, this has not been aligned to neither the chunk nor the page
         * @return the buffer covering this position, or null if no reference can be taken
         */
        abstract Rebufferer.BufferHolder getBuffer(long position);

        /**
         * @return the space taken by this chunk
         */
        abstract int capacity();

        /**
         * Release the buffers when this chunk is no longer used. Called when all references are released.
         */
        abstract void releaseBuffers();
    }

    private static final AtomicIntegerFieldUpdater<Chunk> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "references");

    /**
     * A chunk is a group of buffers of size {@link PageAware#PAGE_SIZE} that will be allocated and released
     * at the same time. The {@link ChunkReader} will read into the buffers of a chunk.
     */
    public class MultiBufferChunk extends Chunk
    {
        private final ByteBuffer[] buffers;
        private final long offset;

        public MultiBufferChunk(Key key)
        {
            this.offset = key.position;

            int numPages = PageAware.numPages(key.file.chunkSize());
            this.buffers = bufferPool.getMultiple(PageAware.PAGE_SIZE, key.file.preferredBufferType(), numPages);
        }

        void releaseBuffers()
        {
            bufferPool.putMultiple(buffers);
        }

        void read(Key key)
        {
            readScattering(key.file, offset, capacity(), buffers);
        }

        @Nullable
        Buffer getBuffer(long position)
        {
            int index = PageAware.pageNum(position - offset);
            Preconditions.checkArgument(index >= 0 && index < buffers.length, "Invalid position: %s, index: %s", position, index);

            long pageAlignedPosition = PageAware.pageStart(position);
            return new Buffer(buffers[index], pageAlignedPosition);
        }

        public int capacity()
        {
            return buffers.length * PageAware.PAGE_SIZE;
        }

        class Buffer implements Rebufferer.BufferHolder
        {
            private final ByteBuffer buffer;
            private final long offset;

            public Buffer(ByteBuffer buffer, long offset)
            {
                this.buffer = buffer;
                this.offset = offset;
            }

            @Override
            public ByteBuffer buffer()
            {
                assert references > 0 : "Already unreferenced";
                return buffer.duplicate();
            }

            @Override
            public long offset()
            {
                return offset;
            }

            @Override
            public void release()
            {
                MultiBufferChunk.this.release();
            }
        }
    }

    class SingleBuffer extends Chunk implements Rebufferer.BufferHolder
    {
        private final ByteBuffer buffer;
        private final long offset;

        public SingleBuffer(Key key)
        {
            this.buffer = bufferPool.get(PageAware.PAGE_SIZE, key.file.preferredBufferType());
            assert key.file.chunkSize() <= PageAware.PAGE_SIZE;
            this.offset = key.position;
        }

        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        public long offset()
        {
            return offset;
        }

        void releaseBuffers()
        {
            bufferPool.put(buffer);
        }

        void read(Key key)
        {
            assert offset == key.position;

            buffer.limit(key.file.chunkSize());
            key.file.readChunk(offset, buffer);
        }

        @Nullable
        Rebufferer.BufferHolder getBuffer(long position)
        {
            return this;
        }

        int capacity()
        {
            return PageAware.PAGE_SIZE;
        }
    }

    Chunk newChunk(Key key)
    {
        if (key.file.chunkSize() > PageAware.PAGE_SIZE)
            return new MultiBufferChunk(key);
        else
            return new SingleBuffer(key);
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
                        .weigher((key, buffer) -> ((Chunk) buffer).capacity())
                        .removalListener(this)
                        .recordStats(() -> metrics)
                        .buildAsync();
        synchronousCache = cache.synchronous();
        cacheAsMap = cache.asMap();
    }


    private Chunk load(Key key)
    {
        Chunk chunk = null;
        try
        {
            chunk = newChunk(key);  // Note: we need `chunk` to be assigned before we call read to release on error
            chunk.read(key);
        }
        catch (RuntimeException t)
        {
            if (chunk != null)
                chunk.release();
            throw t;
        }
        // Complete addition within compute remapping function to ensure there is no race condition with removal.
        keysByFile.compute(key.internedPath, (k, v) -> {
            if (v == null)
                v = new NonBlockingHashSet<>();
            v.add(key);
            return v;
        });
        return chunk;
    }

    @Override
    public void onRemoval(Key key, Chunk chunk, RemovalCause cause)
    {
        chunk.release();
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

    /**
     * Read a chunk to be stored in the chunk cache.
     * <p/>
     * The chunk cache will only store buffers of a fixed size, known as pages. If a chunk only has a single page,
     * then this method will read directly into this page. Otherwise this method will use a temporary scratch buffer
     * to read all the pages at once and will then copy the data back into the individual pages.
     *
     * @param position the start position of the chunk
     * @param chunkSize the amount of data to read
     * @param buffers an array of smaller-sized buffers whose total capacity needs to be >= chunkSize
     *
     * @return a future that will complete when all the buffers in the chunk have been read.
     */
    void readScattering(ChunkReader file, long position, int chunkSize, ByteBuffer[] buffers)
    {
        // Note: We cannot use ThreadLocalByteBufferHolder because readChunk uses it for its temporary buffer.
        // Note: This uses the "Networking" buffer pool, which is meant to serve short-term buffers. Using
        // our buffer pool can cause problems due to the difference in buffer sizes and lifetime.
        ByteBuffer scratchBuffer = BufferPools.forNetworking().get(chunkSize, file.preferredBufferType());
        try
        {
            file.readChunk(position, scratchBuffer);
            int remaining = scratchBuffer.remaining();
            int pos = 0;
            for (ByteBuffer buf : buffers)
            {
                int len = Math.min(remaining, buf.capacity());
                FastByteOperations.copy(scratchBuffer, pos, buf, 0, len);
                buf.position(0).limit(len);
                pos += len;
                remaining -= len;
            }
        }
        finally
        {
            bufferPool.put(scratchBuffer);
        }
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
                    Chunk chunk;
                    // Using cache.get(k, compute) results in lots of allocation, rather risk the unlikely race...
                    CompletableFuture<Chunk> cachedValue = cache.getIfPresent(chunkKey);
                    if (cachedValue == null)
                    {
                        CompletableFuture<Chunk> entry = new CompletableFuture<>();
                        CompletableFuture<Chunk> existing = cacheAsMap.putIfAbsent(chunkKey, entry);
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
                            chunk = existing.get(CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        }
                    }
                    else
                    {
                        chunk = cachedValue.get(CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    }

                    buf = chunk.getReferencedBuffer(position);

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
