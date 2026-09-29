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
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
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
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPoolExhaustedException;
import org.apache.cassandra.utils.memory.BufferPools;
import org.github.jamm.Unmetered;

public class ChunkCache
        implements RemovalListener<ChunkCache.Key, ChunkCache.Chunk>, CacheSize
{
    private final static Logger logger = LoggerFactory.getLogger(ChunkCache.class);

    /**
     * Bytes withheld from Caffeine {@code maximumWeight} but still part of the chunk-cache {@link BufferPool}.
     * <p>
     * Steady-state cache residents are capped at {@code file_cache_size - RESERVED}. The reserved slice is not a
     * separate allocator; it is headroom in the <em>same</em> pool so that under pressure we can still
     * {@code tryGet} short-lived <em>bypass</em> pages.
     * <p>
     * 32MiB ≈ {@code 2 (infligh + bypass) × 250 readers/threads × 64KiB} as a shared budget for in-flight + bypass.
     */
    public static final int RESERVED_POOL_SPACE_IN_MB = 32;
    private static final int INITIAL_CAPACITY = Integer.getInteger("cassandra.chunkcache_initialcapacity", 16);
    private static final boolean ASYNC_CLEANUP = Boolean.parseBoolean(System.getProperty("cassandra.chunkcache.async_cleanup", "true"));
    private static final int CLEANER_THREADS = Integer.getInteger("dse.chunk.cache.cleaner.threads",1);

    private static final Class PERFORM_CLEANUP_TASK_CLASS;
    // cached value in order to not call System.getProperty on a hotpath
    private static final int CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS = CassandraRelevantProperties.CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS.getInt();

    /** When true on the current thread, Caffeine PerformCleanupTask runs inline instead of on cleanupExecutor. */
    private static final ThreadLocal<Boolean> FORCE_INLINE_CLEANUP = ThreadLocal.withInitial(() -> Boolean.FALSE);

    static
    {
        try
        {
            logger.info("-Dcassandra.chunkcache.async_cleanup={} dse.chunk.cache.cleaner.threads={}",
                        ASYNC_CLEANUP, CLEANER_THREADS);
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

    @Unmetered
    private final BufferPool bufferPool;

    private final AsyncCache<Key, Chunk> cache;
    private final Cache<Key, Chunk> synchronousCache;
    private final ConcurrentMap<Key, CompletableFuture<Chunk>> cacheAsMap;
    private final long cacheSize;
    @Unmetered
    public final ChunkCacheMetrics metrics;
    @Unmetered
    private final ShutdownableExecutor cleanupExecutor;

    private boolean enabled;
    private Function<ChunkReader, RebuffererFactory> wrapper = this::wrap;

    // File id management
    private final ConcurrentHashMap<File, Long> fileIdMap = new ConcurrentHashMap<>();
    private final AtomicLong nextFileId = new AtomicLong(0);

    // number of bits required to store the log2 of the chunk size
    private final static int CHUNK_SIZE_LOG2_BITS = Integer.numberOfTrailingZeros(Integer.SIZE);

    // number of bits required to store the ready type
    private final static int READER_TYPE_BITS = Integer.SIZE - Integer.numberOfLeadingZeros(ChunkReader.ReaderType.COUNT - 1);

    public ChunkCache(BufferPool pool, int cacheSizeInMB, Function<ChunkCache, ChunkCacheMetrics> createMetrics)
    {
        cacheSize = 1024L * 1024L * Math.max(0, cacheSizeInMB - RESERVED_POOL_SPACE_IN_MB);
        cleanupExecutor = ParkedExecutor.createParkedExecutor("ChunkCacheCleanup", CLEANER_THREADS);
        enabled = cacheSize > 0;
        bufferPool = pool;
        metrics = createMetrics.apply(this);
        cache = Caffeine.newBuilder()
                        .maximumWeight(cacheSize)
                        .initialCapacity(INITIAL_CAPACITY)
                        .executor(this::executeCleanup)
                        .weigher((key, buffer) -> ((Chunk) buffer).capacity())
                        .removalListener(this)
                        .recordStats(() -> metrics)
                        .buildAsync();
        synchronousCache = cache.synchronous();
        cacheAsMap = cache.asMap();
    }

    /**
     * Caffeine executor: async cleanup by default; inline under {@link #FORCE_INLINE_CLEANUP} (reclaim path)
     * so eviction/onRemoval can free pool pages before tryGet is retried.
     */
    private void executeCleanup(Runnable r)
    {
        if (ASYNC_CLEANUP && r.getClass() == PERFORM_CLEANUP_TASK_CLASS && !FORCE_INLINE_CLEANUP.get())
            cleanupExecutor.execute(r);
        else
            r.run();
    }

    /**
     * Load a chunk for the Caffeine cache path: allocate from the pool, read, return ready Chunk.
     * <p>
     * On pool pressure returns {@code null} so the caller can
     * {@link #bypassLoad} instead of completing a cache future with a transient chunk
     * (that would re-admit pressure allocations into Caffeine and erase the reserve).
     *
     * @return loaded chunk, or {@code null} if the pool could not supply pages after reclaim
     */
    @Nullable
    private Chunk load(ChunkReader file, long position)
    {
        Chunk chunk = null;
        try
        {
            chunk = tryAllocateChunk(file.chunkSize(), position);
            if (chunk == null)
                return null;

            chunk.read(file);
            return chunk;
        }
        catch (RuntimeException | Error t)
        {
            chunk.release();
            throw t;
        }
    }

    /**
     * Uncached load for bypass: pool {@code tryGet} only (no second {@link #reclaimSync}), then
     * {@link Chunk#read}. Result must <b>never</b> be completed into Caffeine / {@code cacheAsMap}.
     * Caller wraps it in {@link TransientBufferHolder} and releases as soon as the reader is done.
     * <p>
     * Invoked only after {@link #load} already ran tryGet → reclaimSync → tryGet and still failed.
     * Another reclaim here would mostly re-wait the same pending cleanups and add read latency without
     * much extra freeable cache memory. If this tryGet fails → {@link BufferPoolExhaustedException}.
     */
    private Chunk bypassLoad(ChunkReader file, long position)
    {
        // tryGet once only — cache path already reclaimed once on this miss.
        Chunk chunk = allocateChunk(file.chunkSize(), position);
        if (chunk == null)
        {
            metrics.recordPoolExhausted();
            throw new BufferPoolExhaustedException(
                    String.format("Chunk cache buffer pool exhausted during bypass (pool used=%s, overflow=%s, cache capacity=%s). " +
                                  "Increase file_cache_size_in_mb or reduce concurrent reads; " +
                                  "chunk cache does not allocate outside the pool.",
                                  prettyUsed(), prettyOverflow(), prettyCapacity()));
        }
        try
        {
            chunk.read(file);
            metrics.recordBypass(chunk.capacity());
            return chunk;
        }
        catch (RuntimeException | Error t)
        {
            chunk.release();
            throw t;
        }
    }

    /**
     * Allocate for the Caffeine path: tryGet, then {@link #reclaimSync} + tryGet once.
     * Returns null if still unavailable (caller should {@link #bypassLoad} without reclaiming again).
     */
    @Nullable
    private Chunk tryAllocateChunk(int chunkSize, long position)
    {
        Chunk chunk = allocateChunk(chunkSize, position);
        if (chunk != null)
            return chunk;

        reclaimSync();
        chunk = allocateChunk(chunkSize, position);
        if (chunk != null)
            metrics.recordReclaimRetrySuccess();
        return chunk;
    }

    private String prettyUsed()
    {
        return org.apache.cassandra.utils.FBUtilities.prettyPrintMemory(bufferPool.usedSizeInBytes());
    }

    private String prettyOverflow()
    {
        return org.apache.cassandra.utils.FBUtilities.prettyPrintMemory(bufferPool.overflowMemoryInBytes());
    }

    private String prettyCapacity()
    {
        return org.apache.cassandra.utils.FBUtilities.prettyPrintMemory(cacheSize);
    }

    /**
     * When the pool cannot satisfy tryGet: run Caffeine maintenance inline so eviction/onRemoval can
     * {@code put} pages on this thread, recycle free local slabs, then caller retries tryGet once.
     * <p>
     * Does <b>not</b> wait for already-queued async cleanups: under load that backlog can stay non-empty
     * and a bounded wait mostly adds read latency. Inline {@link Cache#cleanUp()} already blocks for
     * work started here; remaining recovery is bypass / fail if tryGet still misses.
     */
    @VisibleForTesting
    void reclaimSync()
    {
        metrics.recordSyncReclaim();
        long t0 = System.nanoTime();
        FORCE_INLINE_CLEANUP.set(Boolean.TRUE);
        try
        {
            synchronousCache.cleanUp();
        }
        finally
        {
            FORCE_INLINE_CLEANUP.set(Boolean.FALSE);
        }

        bufferPool.recycleFreeLocalChunks();
        metrics.recordReclaimLatency(System.nanoTime() - t0);
    }

    /**
     * Try to allocate a chunk from the pool only (no overflow). Returns null if the pool is exhausted.
     * Used by both cache and bypass paths; callers decide admission (Caffeine vs transient).
     */
    @Nullable
    Chunk allocateChunk(int chunkSize, long position)
    {
        if (chunkSize <= PageAware.PAGE_SIZE)
        {
            // Always reserve a full page from the pool, even when the reader requests a smaller chunk.
            // Encode the logical chunk size in the owned buffer's limit (capacity stays PAGE_SIZE so
            // BufferPool.put sees the size it handed out). buffer() builds a transient capacity-narrowed
            // view from that limit; releasing a slice/duplicate confuses slot/size accounting.
            ByteBuffer allocated = bufferPool.tryGet(PageAware.PAGE_SIZE);
            if (allocated == null)
                return null;
            // position must remain 0: buffer() uses slice(), which bases capacity on remaining.
            assert allocated.position() == 0 : "pool buffer position must be 0";
            allocated.limit(chunkSize);
            return new SingleRegionChunk(position, allocated);
        }

        ByteBuffer[] buffers = bufferPool.tryGetMultiple(chunkSize, PageAware.PAGE_SIZE);
        if (buffers == null)
            return null;
        if (buffers.length > 1)
            return new MultiRegionChunk(position, buffers);
        else
            return new SingleRegionChunk(position, buffers[0]);
    }

    /**
     * Test helper: allocate for the cache path (reclaim + retry). Returns null if pool exhausted
     */
    @VisibleForTesting
    @Nullable
    Chunk newChunk(int chunkSize, long position)
    {
        return tryAllocateChunk(chunkSize, position);
    }

    @VisibleForTesting
    BufferPool bufferPool()
    {
        return bufferPool;
    }

    @Override
    public void onRemoval(Key key, Chunk chunk, RemovalCause cause)
    {
        chunk.release();
    }

    /**
     * Clears the cache, used in the CNDB Writer for testing purposes.
     */
    public void clear() {
        // Clear keysByFile first to prevent unnecessary computation in onRemoval method.
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

    @VisibleForTesting
    public void enable(boolean enabled)
    {
        this.enabled = enabled;
        wrapper = this::wrap;
        synchronousCache.invalidateAll();
        metrics.reset();
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @VisibleForTesting
    public void intercept(Function<RebuffererFactory, RebuffererFactory> interceptor)
    {
        final Function<ChunkReader, RebuffererFactory> prevWrapper = wrapper;
        wrapper = rdr -> interceptor.apply(prevWrapper.apply(rdr));
    }

    /**
     * Maps a reader to a reader id, used by the cache to find content.
     *
     * Uses the file name (through the fileIdMap), reader type and chunk size to define the id.
     * The lowest {@link #READER_TYPE_BITS} are occupied by reader type, then the next {@link #CHUNK_SIZE_LOG2_BITS}
     * are occupied by log 2 of chunk size (we assume the chunk size is the power of 2), and the rest of the bits
     * are occupied by fileId counter which is incremented for each unseen file name.
     */
    private long readerIdFor(File file, ChunkReader.ReaderType type, int chunkSize)
    {
        return (((fileIdMap.computeIfAbsent(file, this::assignFileId)
                  << CHUNK_SIZE_LOG2_BITS) | Integer.numberOfTrailingZeros(chunkSize))
                << READER_TYPE_BITS) | type.ordinal();
    }

    /**
     * Maps a reader to a file id, used by the cache to find content.
     */
    protected long readerIdFor(ChunkReader source)
    {
        return readerIdFor(source.channel().getFile(), source.type(), source.chunkSize());
    }

    private long assignFileId(File file)
    {
        return nextFileId.getAndIncrement();
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
    public void invalidateFile(File file)
    {
        // Removing the name from the id map suffices -- the next time someone wants to read this file, it will get
        // assigned a fresh id.
        fileIdMap.remove(file);
    }

    /**
     * Invalidate all buffers for the given file, including handles that are already opened. This is a very costly
     * operation and is only intended to be used by tests.
     */
    @VisibleForTesting
    public void invalidateFileNow(File file)
    {
        Long fileIdMaybeNull = fileIdMap.get(file);
        if (fileIdMaybeNull == null)
            return;
        long fileId = fileIdMaybeNull << (CHUNK_SIZE_LOG2_BITS + READER_TYPE_BITS);
        long mask = - (1 << (CHUNK_SIZE_LOG2_BITS + READER_TYPE_BITS));
        synchronousCache.invalidateAll(Iterables.filter(cache.asMap().keySet(), x -> (x.readerId & mask) == fileId));
    }

    @VisibleForTesting
    public static class Key
    {
        private static final Hasher64 hasher = Hashing.metroHash64();

        final long readerId;
        final long position;

        @VisibleForTesting
        public Key(long readerId, long position)
        {
            super();
            this.position = position;
            this.readerId = readerId;
        }

        @Override
        public int hashCode()
        {
            return hasher.hashLongLongToInt(readerId, position);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;

            Key other = (Key) obj;
            return (position == other.position)
                   && readerId == other.readerId;
        }
    }

    /**
     * An abstract chunk contains the common implementation for the single and multi-regions chunks, normally
     * this is related to ref counting and calling the read methods in the chunk reader. The chunk implementations
     * will then take care of implementing the read target so that they can accommodate the data that was read into
     * either a single memory region or into multiple memory regions.
     */
    abstract static class Chunk
    {
        /** The offset in the file where the chunk is read */
        final long offset;

        /** The number of bytes read from disk, this could be less than the memory space allocated */
        int bytesRead;

        private volatile int references;
        private static final AtomicIntegerFieldUpdater<Chunk> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "references");

        Chunk(long offset)
        {
            this.offset = offset;
            this.bytesRead = 0; // To be filled by the read method
            this.references = 1; // Start referenced
        }

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

        public long offset()
        {
            return offset;
        }

        /**
         * Used in assertions, returns false if the chunk is not still referenced.
         */
        boolean isReferenced()
        {
            return references > 0;
        }

        /**
         * Load the data for this chunk.
         */
        abstract void read(ChunkReader file);

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
         * Release the addresses when this chunk is no longer used. Called when all references are released.
         */
        abstract void releaseBuffers();
    }

    /**
     * A chunk is a group of buffers of size {@link PageAware#PAGE_SIZE} that will be allocated and released
     * at the same time. The {@link ChunkReader} will read into the buffers of a chunk.
     */
    public class MultiRegionChunk extends Chunk
    {
        private final ByteBuffer[] buffers;

        public MultiRegionChunk(long offset, ByteBuffer[] buffers)
        {
            super(offset);
            this.buffers = buffers;
        }

        void releaseBuffers()
        {
            for (int i = 0; i < buffers.length; ++i)
                bufferPool.put(buffers[i]);
        }

        void read(ChunkReader file)
        {
            // Note: We cannot use ThreadLocalByteBufferHolder because readChunk uses it for its temporary buffer.
            // Note: This uses the "Networking" buffer pool, which is meant to serve short-term buffers. Using
            // the cache's buffer pool can cause problems due to the difference in buffer sizes and lifetime.
            // Note: As this buffer is not retained in the cache, it can use the chunk reader's preferred buffer type.
            ByteBuffer scratchBuffer = BufferPools.forNetworking().get(capacity(), file.preferredBufferType());
            try
            {
                file.readChunk(offset, scratchBuffer);
                int limit = scratchBuffer.limit();
                int idx = 0;
                int pageStart;
                for (pageStart = 0; pageStart + PageAware.PAGE_SIZE <= limit; pageStart += PageAware.PAGE_SIZE)
                    FastByteOperations.copy(scratchBuffer, pageStart, buffers[idx++], 0, PageAware.PAGE_SIZE);

                if (pageStart < limit)   // if the limit is not a multiple of the page size
                    FastByteOperations.copy(scratchBuffer, pageStart, buffers[idx++], 0, limit - pageStart);

                bytesRead = limit;
            }
            finally
            {
                BufferPools.forNetworking().put(scratchBuffer);
            }
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
                buffer.order(ByteOrder.BIG_ENDIAN);
            }

            @Override
            public ByteBuffer buffer()
            {
                assert isReferenced() : "Already unreferenced";
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
                MultiRegionChunk.this.release();
            }
        }
    }

    /**
     * A chunk with a single memory region. This is always used for reading chunks of up to PageAware.PAGE_SIZE (note
     * that the memory allocated will be always at least PageAware.PAGE_SIZE even if the reader requests a smaller
     * buffer), and may also be used for larger chunks if the buffer pool can produce a contiguous memory buffer.
     * See {@link this#newChunk}.
     * <p/>
     * This class is a chunk but also behaves as a {@link Rebufferer.BufferHolder} to save an allocation when
     * {@link this#getBuffer(long)} is invoked.
     * <p/>
     * Only one {@link ByteBuffer} is owned: the object returned by the buffer pool (typically a full page).
     * Logical chunk size is encoded in {@code buffer.limit()} (with {@code position == 0}); capacity stays at the
     * allocated size so the pool sees what it handed out on release. {@link #buffer()} builds a transient
     * {@code slice()} view whose capacity equals that logical size so {@code readChunk}'s {@code clear()}
     * semantics stay correct — the slice is never returned to the pool.
     */
    class SingleRegionChunk extends Chunk implements Rebufferer.BufferHolder
    {
        /**
         * Pool-owned buffer; always what is returned from {@link #bufferPool#put} on release.
         * {@code limit} holds the logical chunk size for reads (may be smaller than {@link #capacity()});
         * {@code position} must stay 0 for the lifetime of the chunk.
         */
        private final ByteBuffer buffer;

        public SingleRegionChunk(long offset, ByteBuffer buffer)
        {
            super(offset);
            this.buffer = buffer;
            buffer.order(ByteOrder.BIG_ENDIAN);
        }

        public ByteBuffer buffer()
        {
            assert isReferenced() : "Already unreferenced";
            // Always apply bytesRead as the view limit. It starts at 0 so a failed/incomplete load cannot
            // expose stale data. Chunk readers (e.g. SimpleChunkReader, CompressedChunkReader) expect that
            // and call clear() (or otherwise reset limit) before filling, so a 0-limit buffer is fine for read().
            ByteBuffer view = buffer.slice();
            view.limit(bytesRead);
            return view;
        }

        public long offset()
        {
            return offset;
        }

        void releaseBuffers()
        {
            bufferPool.put(buffer);
        }

        void read(ChunkReader file)
        {
            ByteBuffer buffer = buffer();
            file.readChunk(offset, buffer);
            bytesRead = buffer.limit();
        }

        @Nullable
        Rebufferer.BufferHolder getBuffer(long position)
        {
            return this;
        }

        int capacity()
        {
            // Weight / pool accounting use allocated size, not the logical read size in buffer.limit().
            return buffer.capacity();
        }
    }

    /**
     * Short-lived {@link Rebufferer.BufferHolder} for <em>bypass</em> (uncached) serves.
     * <p>
     * <b>Lifetime:</b> created only inside {@link CachingRebufferer#bypassServe}, handed to the caller of
     * {@link Rebufferer#rebuffer(long)}, and must be {@link #release()}'d as soon as that rebuffer window is
     * done—same contract as cached holders (typically when the reader advances or closes the current buffer).
     * Do not stash on long-lived structures; pinning bypass pages defeats
     * {@link #RESERVED_POOL_SPACE_IN_MB} and starves both cache admits and further bypass.
     * <p>
     * <b>Who releases:</b> the reader that obtained the holder from {@code rebuffer} (RandomAccessReader /
     * upper layers), exactly once. This class is not installed in Caffeine; {@link ChunkCache#onRemoval} will
     * not run for it.
     * <p>
     * <b>Do NOT:</b>
     * <ul>
     *   <li>complete a {@code CompletableFuture} in {@code cacheAsMap} with the wrapped {@link Chunk}</li>
     *   <li>call {@link ChunkCache#onRemoval} / treat as a cache resident</li>
     *   <li>skip {@link #release()} (leaks pool pages until process death)</li>
     * </ul>
     * Implementation reuses the same {@link Chunk} allocate+read path as cache loads; only admission differs.
     */
    static final class TransientBufferHolder implements Rebufferer.BufferHolder
    {
        private final Rebufferer.BufferHolder delegate;
        private boolean released;

        TransientBufferHolder(Rebufferer.BufferHolder delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public ByteBuffer buffer()
        {
            assert !released : "Already released";
            return delegate.buffer();
        }

        @Override
        public long offset()
        {
            return delegate.offset();
        }

        @Override
        public void release()
        {
            if (released)
                return;
            released = true;
            delegate.release();
        }
    }

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified ChunkReader.
     * Thread-safe. One instance per SegmentedFile, created by ChunkCache.maybeWrap if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer, RebuffererFactory
    {
        private final ChunkReader source;
        private final long readerId;
        final long alignmentMask;

        public CachingRebufferer(ChunkReader file)
        {
            source = file;
            readerId = readerIdFor(file);
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
                Key chunkKey = new Key(readerId, pageAlignedPos);

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
                                chunk = load(source, pageAlignedPos);
                                if (chunk == null)
                                {
                                    // The chunk cache is full and could not admit a cache resident after reclaim.
                                    // We will attempt an uncached bypass.
                                    //
                                    // Remove the incomplete entry, wake waiters with null which should then uncached bypass.
                                    // If there are hot keys under pressure returning null means that other threads waiting
                                    // will also attempt an uncached bypass (allocating from the pool).
                                    // This is suboptimal and handling hot key under pool pressure would require more complexity
                                    // in tracking bypass serve requests. We don't want to pre maturely optimize for this case,
                                    // unless it starts happening more often than expected.
                                    cacheAsMap.remove(chunkKey, entry);
                                    entry.complete(null);
                                    return bypassServe(position, pageAlignedPos);
                                }
                            }
                            catch (Throwable t)
                            {
                                // Caffeine automatically removes entries that complete exceptionally
                                entry.completeExceptionally(t);
                                throw t;
                            }
                            // Only complete successfully with a chunk that is intended as a cache resident.
                            entry.complete(chunk);
                        }
                        else
                        {
                            chunk = awaitCacheChunk(existing);
                            if (chunk == null)
                                return bypassServe(position, pageAlignedPos);
                        }
                    }
                    else
                    {
                        chunk = awaitCacheChunk(cachedValue);
                        if (chunk == null)
                            return bypassServe(position, pageAlignedPos);
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
                // Bypass already threw BufferPoolExhaustedException — do not catch and retry.
                Throwables.propagateIfInstanceOf(t, BufferPoolExhaustedException.class);
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                // Timeout, disk/load errors, etc.: same as before — surface to the caller (not a bypass signal).
                throw Throwables.propagate(t);
            }
        }

        /**
         * Await a cache load future.
         * <ul>
         *   <li>{@code null} — we aborted cache admission and completed the future with null after
         *       removing the map entry; this waiter should {@link #bypassServe}.</li>
         *   <li>chunk — normal load.</li>
         *   <li>{@link java.util.concurrent.TimeoutException} — wait exceeded
         *       {@code CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS} (slow/stuck IO or loader)
         *   <li>other load failures — propagate (e.g. corrupt / FS errors via the future).</li>
         * </ul>
         */
        @Nullable
        private Chunk awaitCacheChunk(CompletableFuture<Chunk> pending) throws Exception
        {
            return pending.get(CHUNK_CACHE_REBUFFER_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }

        /**
         * Uncached rebuffer using the same Chunk {@code allocate + read} machinery as {@link #load}, without
         * installing the result into Caffeine. Memory comes from the chunk-cache pool (reserve headroom);
         * the holder must be released ASAP by the reader (same contract as any {@link BufferHolder}).
         */
        private BufferHolder bypassServe(long position, long pageAlignedPos)
        {
            Chunk chunk = bypassLoad(source, pageAlignedPos);
            // DO NOT put chunk into cacheAsMap / complete a future with it.
            //
            // Chunk starts with references=1 (initial ref). For cache loads that ref is owned by Caffeine
            // until onRemoval. For bypass there is no cache entry: take a reader ref, drop the initial ref,
            // and wrap so the reader's release() returns pages to the pool immediately.
            BufferHolder holder = chunk.getReferencedBuffer(position);
            if (holder == null)
            {
                chunk.release();
                throw new RuntimeException("bypass chunk could not be referenced");
            }
            chunk.release(); // drop production ref; holder keeps refs until reader release
            return new TransientBufferHolder(holder);
        }

        @Override
        public int chunkSize()
        {
            return source.chunkSize();
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
            synchronousCache.invalidate(new Key(readerId, pageAlignedPos));
        }

        @Override
        public long adjustPosition(long position)
        {
            return source.adjustPosition(position);
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
    public int sizeOfFile(File file) {
        Long fileIdMaybeNull = fileIdMap.get(file);
        if (fileIdMaybeNull == null)
            return 0;
        long fileId = fileIdMaybeNull << (CHUNK_SIZE_LOG2_BITS + READER_TYPE_BITS);
        long mask = - (1 << (CHUNK_SIZE_LOG2_BITS + READER_TYPE_BITS));
        return (int) cacheAsMap.keySet().stream().filter(x -> (x.readerId & mask) == fileId).count();
    }
}
