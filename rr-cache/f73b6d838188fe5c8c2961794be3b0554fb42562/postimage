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
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;

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
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.config.CassandraRelevantProperties.CHUNKCACHE_ASYNC_CLEANUP;
import static org.apache.cassandra.config.CassandraRelevantProperties.CHUNKCACHE_CLEANER_THREADS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CHUNKCACHE_INITIAL_CAPACITY;

public class ChunkCache
        implements RemovalListener<ChunkCache.Key, ChunkCache.Chunk>, CacheSize
{
    private final static Logger logger = LoggerFactory.getLogger(ChunkCache.class);

    public static final int RESERVED_POOL_SPACE_IN_MiB = 32;
    private static final int INITIAL_CAPACITY = CHUNKCACHE_INITIAL_CAPACITY.getInt();;
    private static final boolean ASYNC_CLEANUP =CHUNKCACHE_ASYNC_CLEANUP.getBoolean();
    private static final int CLEANER_THREADS = CHUNKCACHE_CLEANER_THREADS.getInt();

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
                                              ? new ChunkCache(BufferPools.forChunkCache(), DatabaseDescriptor.getFileCacheSizeInMiB(), ChunkCacheMetrics::create)
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
        cacheSize = 1024L * 1024L * Math.max(0, cacheSizeInMB - RESERVED_POOL_SPACE_IN_MiB);
        cleanupExecutor = ParkedExecutor.createParkedExecutor("ChunkCacheCleanup", CLEANER_THREADS);
        enabled = cacheSize > 0;
        bufferPool = pool;
        metrics = createMetrics.apply(this);
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


    private Chunk load(ChunkReader file, long position)
    {
        Chunk chunk = null;
        try
        {
            chunk = newChunk(file.chunkSize(), position);  // Note: we need `chunk` to be assigned before we call read to release on error
            chunk.read(file);
        }
        catch (RuntimeException t)
        {
            if (chunk != null)
                chunk.release();
            throw t;
        }
        return chunk;
    }

    Chunk newChunk(int chunkSize, long position)
    {
        if (chunkSize == PageAware.PAGE_SIZE)
            return new SingleRegionChunk(position, bufferPool.get(PageAware.PAGE_SIZE, BufferType.OFF_HEAP));
        if (chunkSize < PageAware.PAGE_SIZE)
            return new SingleRegionChunk(position, bufferPool.get(PageAware.PAGE_SIZE, BufferType.OFF_HEAP).limit(chunkSize).slice());

        ByteBuffer[] buffers = bufferPool.getMultiple(chunkSize, PageAware.PAGE_SIZE, BufferType.OFF_HEAP);
        if (buffers.length > 1)
            return new MultiRegionChunk(position, buffers);
        else
            return new SingleRegionChunk(position, buffers[0]);
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

    public RebuffererFactory wrap(ChunkReader file)
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

    static class Key
    {
        final long readerId;
        final long position;

        private Key(long readerId, long position)
        {
            super();
            this.position = position;
            this.readerId = readerId;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + Long.hashCode(readerId);
            result = prime * result + Long.hashCode(position);
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
        /** A list of memory addresses, each page has capacity of PageAware.PAGE_SIZE */
        private final long[] pages;
        /** List of attachments, necessary to be able to release pool buffers properly */
        private final Object[] attachments;

        public MultiRegionChunk(long offset, ByteBuffer[] buffers)
        {
            super(offset);
            this.pages = new long[buffers.length];
            this.attachments = new Object[buffers.length];
            for (int i = 0; i < buffers.length; ++i)
            {
                pages[i] = MemoryUtil.getAddress(buffers[i]);
                attachments[i] = MemoryUtil.getAttachment(buffers[i]);
            }
        }

        void releaseBuffers()
        {
            for (int i = 0; i < pages.length; ++i)
                bufferPool.put(MemoryUtil.allocateByteBuffer(pages[i], PageAware.PAGE_SIZE, PageAware.PAGE_SIZE, ByteOrder.BIG_ENDIAN, attachments[i]));
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
                int pageEnd;
                for (pageEnd = PageAware.PAGE_SIZE; pageEnd <= limit; pageEnd += PageAware.PAGE_SIZE)
                {
                    scratchBuffer.limit(pageEnd);
                    MemoryUtil.setBytes(pages[idx++], scratchBuffer);
                    scratchBuffer.position(pageEnd);
                }
                if (scratchBuffer.position() < limit)   // if the limit is not a multiple of the page size
                {
                    scratchBuffer.limit(limit);
                    MemoryUtil.setBytes(pages[idx], scratchBuffer);
                }
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
            Preconditions.checkArgument(index >= 0 && index < pages.length, "Invalid position: %s, index: %s", position, index);

            long pageAlignedPosition = PageAware.pageStart(position);
            int bufferSize = Math.min(PageAware.PAGE_SIZE, Math.toIntExact(bytesRead - (index * PageAware.PAGE_SIZE)));
            assert bufferSize > 0 && bufferSize <= PageAware.PAGE_SIZE : "Wrong buffer size: " + bufferSize + " at position " + position;

            return new Buffer(pages[index], pageAlignedPosition, bufferSize);
        }

        public int capacity()
        {
            return pages.length * PageAware.PAGE_SIZE;
        }

        class Buffer implements Rebufferer.BufferHolder
        {
            private final long address;
            private final long offset;
            private final int limit;


            public Buffer(long address, long offset, int limit)
            {
                this.address = address;
                this.offset = offset;
                this.limit = limit;
            }

            @Override
            public ByteBuffer buffer()
            {
                assert isReferenced() : "Already unreferenced";
                return MemoryUtil.allocateByteBuffer(address, limit, PageAware.PAGE_SIZE, ByteOrder.BIG_ENDIAN, null);
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
     */
    class SingleRegionChunk extends Chunk implements Rebufferer.BufferHolder
    {
        private final long address;
        private final int capacity;
        private final Object attachment;

        public SingleRegionChunk(long offset, ByteBuffer buffer)
        {
            super(offset);
            this.address = MemoryUtil.getAddress(buffer);
            this.capacity = buffer.capacity();
            this.attachment = MemoryUtil.getAttachment(buffer);
        }

        public ByteBuffer buffer()
        {
            assert isReferenced() : "Already unreferenced";
            return MemoryUtil.allocateByteBuffer(address, bytesRead, capacity, ByteOrder.BIG_ENDIAN, null);
        }

        public long offset()
        {
            return offset;
        }

        void releaseBuffers()
        {
            bufferPool.put(MemoryUtil.allocateByteBuffer(address, capacity, capacity, ByteOrder.BIG_ENDIAN, attachment));
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
            return capacity;
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
                if (t.getCause() instanceof CorruptSSTableException)
                    throw (CorruptSSTableException)t.getCause();
                Throwables.throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }

        @Override
        public void invalidateIfCached(long position)
        {
            long pageAlignedPos = position & alignmentMask;
            synchronousCache.invalidate(new Key(readerId, pageAlignedPos));
        }

        @Override
        public Rebufferer instantiateRebufferer(boolean isScan)
        {
            return this;
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
