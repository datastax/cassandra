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

package org.apache.cassandra.cache;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.utils.leaks.detection.LeakLevel;
import com.datastax.bdp.db.utils.leaks.detection.LeaksDetector;
import com.datastax.bdp.db.utils.leaks.detection.LeaksDetectorFactory;
import com.datastax.bdp.db.utils.leaks.detection.LeaksTracker;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.concurrent.ParkedExecutor;
import org.apache.cassandra.utils.concurrent.ShutdownableExecutor;
import org.apache.cassandra.utils.memory.BufferPool;

public class ChunkCacheImpl
implements AsyncCacheLoader<ChunkCacheImpl.Key, ChunkCacheImpl.Chunk>, RemovalListener<ChunkCacheImpl.Key, ChunkCacheImpl.Chunk>, CacheSize
{
    private static final Class PERFORM_CLEANUP_TASK_CLASS;

    static
    {
        try
        {
            PERFORM_CLEANUP_TASK_CLASS = Class.forName("com.github.benmanes.caffeine.cache.BoundedLocalCache$PerformCleanupTask");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static final int CLEANER_THREADS = Integer.getInteger("dse.chunk.cache.cleaner.threads",1);
    private static final Logger logger = LoggerFactory.getLogger(ChunkCacheImpl.class);

    private final AsyncLoadingCache<Key, Chunk> cache;
    private final ChunkCacheMetrics metrics;
    private final LeaksDetector<Chunk> leakDetector;
    private final long cacheSize;
    private final BufferPool bufferPool;
    private final ShutdownableExecutor cleanupExecutor;
    private final LeaksDetector.Cleaner<long[]> leaksCleaner;

    @VisibleForTesting
    public static class Key
    {
        final ChunkReader file;
        final long fileId;
        final long position;
        final int hashCode;

        Key(ChunkReader file, long fileId, long position)
        {
            super();
            this.file = file;
            this.fileId = fileId;
            this.position = position;
            this.hashCode = computeHashCode();
        }

        private int computeHashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + Long.hashCode(fileId);
            result = prime * result + Long.hashCode(position);
            return result;
        }

        @Override
        public int hashCode()
        {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;

            if (obj == null || !(obj instanceof Key))
                return false;

            Key other = (Key) obj;
            return (fileId == other.fileId) && (position == other.position);
        }

        public String path()
        {
            return file.channel().filePath();
        }

        public String toString()
        {
            return path() + '@' + position;
        }
    }

    /**
     * An abstract chunk contains the common implementation for the single and multi-regions chunks, normally
     * this is related to ref counting and calling the read methods in the chunk reader. The chunk implementations
     * will then take care of implementing the read target so that they can accommodate the data that was read into
     * either a single memory region or into multiple memory regions.
     */
    abstract class Chunk implements ChunkReader.ReadTarget<Chunk>
    {
        /** The offset in the file where the chunk is read */
        final long offset;

        /** The number of bytes read from disk, this could be less than the memory space allocated */
        int bytesRead;

        @VisibleForTesting
        public volatile int references;

        @Nullable
        protected volatile LeaksTracker<Chunk> leak;

        Chunk(long offset)
        {
            this.offset = offset;
            this.bytesRead = 0; // Updated by ChunkReader.ReadTarget.setReadResult
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

            if (leak != null)
                leak.record();

            return getBuffer(position);
        }

        /**
         * Release the chunk when the cache or a reader no longer needs it.
         */
        public void release()
        {
            if (referencesUpdater.decrementAndGet(this) == 0)
            {
                if (leak != null)
                    leak.close(this);

                releaseBuffers();
            }
        }

        /**
         * Called when the chunk is evicted from the cache. It needs to release it but also to install
         * auto-cleaning if not already done in the constructor.
         * <p/>
         * If the refCount is > 1, then we can create a leak detector that will run the cleaner on GC
         * knowing that until we've released the reference count for the cache, the code in the {@link this#release()}
         * method that runs when the ref count reaches zero cannot execute. So it's safe to install a new leak
         * detector before calling {@link this#release()}.
         */
        void evicted()
        {
            int refCount = references;

            // no leak detection and some one else has a reference, install a leak detector that will call the cleaner
            // if the chunk is not released, if this other thread races us and calls release in the meantime, we know
            // that the leak will be closed by release() when the ref count goes to zero by our call to release() just below
            if (this.leak == null && refCount > 1)
                this.leak = trackForCleaning();

            // It's possible that a thread that has been preempted after taking a chunk, may take a reference before
            // we release ours, e.g. in rebuffer(long position, ReaderConstraint rc). In this case the chunk won't be auto-cleaned.
            // We could close this race by taking a reference again and retrying if after a release the ref count is > 0.
            // However because the risk of a race is low, and auto-cleaning is only a last resort measure in case of bugs that
            // we should aim at fixing, I prefer not to add code that executes only in extremely rare cases and it's hard to properly
            // test. Each time a chunk is auto-cleaned an error will be logged, and this should alert us on fixing any bugs causing
            // a chunk not to be cleaned manually. If the vast majority of leaked chunks are auto-cleaned, clients should not suffer
            // from memory problems because of this race and so I prefer to avoid the risk.
            release();
        }

        public Throwable onError(Throwable error, ByteBuffer buffer)
        {
            release();
            return error;
        }

        public long offset()
        {
            return offset;
        }

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

        /**
         * @return a tracker that will automatically clean the chunk's buffer(s).
         */
        abstract LeaksTracker<Chunk> trackForCleaning();
    }

    private static final AtomicIntegerFieldUpdater<Chunk> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "references");

    /**
     * A chunk is a group of memory regions of size {@link PageAware#PAGE_SIZE} that will be allocated and released
     * at the same time. The {@link ChunkReader} will read into a temporary buffer and then pass the results to the chunk
     * via the implementation of {@link ChunkReader.ReadTarget} and specifically {@link ChunkReader.ReadTarget#setReadResult(ByteBuffer)}.
     * The memory regions are taken from the buffer pool.
     */
    public class MultiRegionChunk extends Chunk
    {
        /** A list of memory addresses, each page has capacity of PageAware.PAGE_SIZE */
        final long[] pages;

        MultiRegionChunk(Key key, long[] pages)
        {
            super(key.position);
            this.pages = pages;
            this.leak = leakDetector.trackForDebug(this, leaksCleaner, pages);
        }

        @Override
        public ByteBuffer constructTarget(int chunkSize)
        {
            return bufferPool.allocate(chunkSize);
        }

        @Override
        public Throwable onError(Throwable error, ByteBuffer buffer)
        {
            if (buffer != null)
                bufferPool.release(buffer);
            return super.onError(error, buffer);
        }

        //@Override
        public Chunk setReadResult(ByteBuffer result)
        {
            // copy the bytes read from the scratch buffer to the individual memory regions

            int totRead = 0;
            for (long page : pages)
            {
                int len = Math.min(PageAware.PAGE_SIZE, result.limit() - totRead);
                if (len == 0)
                    break; // nothing left to copy into remaining pages

                UnsafeCopy.copyBufferToMemory(result, totRead, page, len);
                totRead += len;
            }

            this.bytesRead = totRead;
            bufferPool.release(result);
            return this;
        }

        @Override
        void releaseBuffers()
        {
            bufferPool.release(PageAware.PAGE_SIZE, pages);
        }

        LeaksTracker<Chunk> trackForCleaning()
        {
            return leakDetector.trackForCleaning(this, leaksCleaner, pages);
        }

        @Nullable
        Buffer getBuffer(long position)
        {
            int index = PageAware.pageNum(position - offset);
            if (index < 0 || index >= pages.length)
                throw new IllegalArgumentException(String.format("Invalid position: %s, or index: %s (pages count: %s, position: %s, offset: %s", position - offset, index, pages.length, position, offset));

            long pageAlignedPosition = PageAware.pageStart(position);
            int bufferSize = Math.min(PageAware.PAGE_SIZE, Math.toIntExact(bytesRead - (index * PageAware.PAGE_SIZE)));
            assert bufferSize > 0 && bufferSize <= PageAware.PAGE_SIZE : "Wrong buffer size: " + bufferSize + " at position " + position;

            return new Buffer(pageAlignedPosition, pages[index], bufferSize);
        }

        int capacity()
        {
            return pages.length * PageAware.PAGE_SIZE;
        }

        /**
         * A buffer holder that wraps an address of memory of a specific size. It creates a byte buffer on the fly
         * when requested. The capacity of the memory region is assumed to be {@link PageAware#PAGE_SIZE}.
         */
        @VisibleForTesting
        public class Buffer implements Rebufferer.BufferHolder
        {
            private final long offset;
            private final long address;
            private final int bufferSize;

            Buffer(long offset, long address, int bufferSize)
            {
                this.offset = offset;
                this.address = address;
                this.bufferSize = bufferSize;
            }

            @Override
            public ByteBuffer buffer()
            {
                return unsafeBuffer(); // no need to duplicate since unsafeBuffer() already creates a new hollow instance
            }

            @Override
            public FloatBuffer floatBuffer()
            {
                return buffer().asFloatBuffer();
            }

            @Override
            public IntBuffer intBuffer()
            {
                return buffer().asIntBuffer();
            }

            @Override
            public LongBuffer longBuffer()
            {
                return buffer().asLongBuffer();
            }

            //@Override
            public ByteBuffer unsafeBuffer()
            {
                assert references > 0 : "Already unreferenced";
                return UnsafeByteBufferAccess.allocateByteBuffer(address, bufferSize, PageAware.PAGE_SIZE, ByteOrder.BIG_ENDIAN, null);
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

            /*
            @Override
            public ByteBuffer buffer(ByteBuffer buffer)
            {
                if (buffer == null || !buffer.isDirect())
                    return buffer();

                UnsafeByteBufferAccess.initByteBufferInstance(buffer, address, bufferSize, PageAware.PAGE_SIZE);
                return buffer;
            }*/
        }
    }

    /**
     * A chunk with a single memory region. This can be used for reading chunks of up to PageAware.PAGE_SIZE but note
     * that the memory allocated will be always at least PageAware.PAGE_SIZE. It can also be used for larger chunks if
     * the individual memory regions are contiguous, see {@link this#newChunk(Key)}. The memory region is taken from the pool.
     * <p/>
     * This class is a chunk but it behaves also as a {@link Rebufferer.BufferHolder} to save an allocation when {@link this#getBuffer(long)}
     * is invoked.
     */
    @VisibleForTesting
    public class SingleRegionChunk extends Chunk implements Rebufferer.BufferHolder
    {
        /** The memory address of the single page */
        private final long address;

        /** The number of pages behind the single memory region */
        private final int numPages;

        SingleRegionChunk(Key key, long pages[])
        {
            super(key.position);
            // note we don't check again that the pages are contiguous because it is expensive
            this.address = pages[0];
            this.numPages = pages.length;
            this.leak = leakDetector.trackForDebug(this, leaksCleaner, pages);
        }

        //@Override
        public ByteBuffer constructTarget(int chunkSize)
        {
            // return the address of the contiguous memory region so that no temporary buffer will be created
            assert chunkSize <= numPages * PageAware.PAGE_SIZE : "Received chunk too large: " + chunkSize + " for " + numPages + " pages.";
            return UnsafeByteBufferAccess.allocateByteBuffer(address, chunkSize, ByteOrder.BIG_ENDIAN);
        }

        //@Override
        public Chunk setReadResult(ByteBuffer result)
        {
            // the read result buffer should be a wrapper around the address we returned in targetAddress(), so no need to copy anything
            assert UnsafeByteBufferAccess.getAddress(result) == address : "Expected result with same address";
            this.bytesRead = result.limit();
            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            return unsafeBuffer(); // no need to duplicate since unsafeBuffer() already creates a new hollow instance
        }

        //@Override
        public ByteBuffer unsafeBuffer()
        {
            assert references > 0 : "Already unreferenced";
            return UnsafeByteBufferAccess.allocateByteBuffer(address, bytesRead, capacity(), ByteOrder.BIG_ENDIAN, null);
        }

        @Override
        void releaseBuffers()
        {
            bufferPool.release(PageAware.PAGE_SIZE, UnsafeByteBufferAccess.splitContiguousRegion(address, PageAware.PAGE_SIZE, capacity()));
        }

        LeaksTracker<Chunk> trackForCleaning()
        {
            return leakDetector.trackForCleaning(this, leaksCleaner, UnsafeByteBufferAccess.splitContiguousRegion(address, PageAware.PAGE_SIZE, capacity()));
        }

        @Nullable
        Rebufferer.BufferHolder getBuffer(long position)
        {
            return this;
        }

        int capacity()
        {
            return numPages * PageAware.PAGE_SIZE;
        }
    }

    /**
     * A single direct buffer chunk is a chunk with a single buffer that is allocated directly, without
     * using the buffer pool. This is normally preferable for larger buffers and is used for chunks
     * larger than or equal to {@link BufferPool#CACHED_FILE_READS_MBEAN_NAME}.
     */
    @VisibleForTesting
    public class SingleDirectBuffer extends Chunk implements Rebufferer.BufferHolder
    {
        private final ByteBuffer buffer;

        SingleDirectBuffer(Key key, int chunkSize)
        {
            super(key.position);
            this.buffer = BufferType.OFF_HEAP_ALIGNED.allocate(chunkSize);
            this.leak = leakDetector.trackForDebug(this, FileUtils::clean, buffer);
        }

        //@Override
        public ByteBuffer constructTarget(int chunkSize)
        {
            // return the address of the contiguous memory region so that no temporary buffer will be created
            assert chunkSize <= buffer.capacity(): "Received chunk too large: " + chunkSize + " for direct buffer of " + buffer.capacity() + " bytes.";
            return buffer;
        }

        //@Override
        public Chunk setReadResult(ByteBuffer result)
        {
            // the read result buffer should be the same buffer returned by constructTarget()
            assert result == this.buffer : "Expected result to be the same buffer";
            this.bytesRead = result.limit();
            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        @Override
        public FloatBuffer floatBuffer()
        {
            return buffer.asFloatBuffer();
        }

        @Override
        public IntBuffer intBuffer()
        {
            return buffer.asIntBuffer();
        }

        @Override
        public LongBuffer longBuffer()
        {
            return buffer.asLongBuffer();
        }

        //@Override
        public ByteBuffer unsafeBuffer()
        {
            assert references > 0 : "Already unreferenced";
            return buffer;
        }

        @Override
        void releaseBuffers()
        {
            FileUtils.clean(buffer);
        }

        LeaksTracker<Chunk> trackForCleaning()
        {
            return leakDetector.trackForCleaning(this, FileUtils::clean, buffer);
        }

        //@Override
        public ByteBuffer buffer(ByteBuffer buffer)
        {
            if (buffer == null || !buffer.isDirect())
                return buffer();

            UnsafeByteBufferAccess.initByteBufferInstance(buffer,
                                                          UnsafeByteBufferAccess.getAddress(this.buffer),
                                                          bytesRead,
                                                          ByteOrder.BIG_ENDIAN);
            return buffer;
        }

        @Nullable
        Rebufferer.BufferHolder getBuffer(long position)
        {
            return this;
        }

        int capacity()
        {
            return buffer.capacity();
        }
    }

    Chunk newChunk(Key key)
    {
        int chunkLength = key.file.chunkSize();

        if (chunkLength >= BufferPool.CHUNK_DIRECT_ALLOC_SIZE_BYTES || BufferPool.DISABLED)
            return new SingleDirectBuffer(key, chunkLength);

        // Allocate a sufficient number of 4k pages to cover the chunk length, if the chunk length is zero
        // then allocate at least one page (this was the existing behavior and at least in unit tests it can happen)
        long[] pages = bufferPool.allocate(PageAware.PAGE_SIZE, Math.max(1, PageAware.numPages(chunkLength)));

        if (pages.length == 1 || UnsafeByteBufferAccess.regionsAreContiguous(pages, PageAware.PAGE_SIZE, chunkLength))
            return new SingleRegionChunk(key, pages);
        else
            return new MultiRegionChunk(key, pages);
    }


    public ChunkCacheImpl(ChunkCacheMetrics metrics, long cacheSize, BufferPool bufferPool)
    {
        this.cacheSize = cacheSize;
        this.metrics = metrics;
        cleanupExecutor = ParkedExecutor.createParkedExecutor("ChunkCacheCleanup", CLEANER_THREADS);

        cache = Caffeine.newBuilder()
                        .maximumWeight(cacheSize)
                        .executor(r -> {
                            if (r.getClass() == PERFORM_CLEANUP_TASK_CLASS)
                                cleanupExecutor.execute(r);
                            else
                                r.run();
                        })
                        .weigher((key, chunk) -> ((Chunk) chunk).capacity())
                        .recordStats(() -> metrics)
                        .removalListener(this)
                        .buildAsync(this);

        this.bufferPool = bufferPool;
        this.leakDetector = LeaksDetectorFactory.create("ChunkCache", Chunk.class, LeakLevel.FIRST_LEVEL);
        this.leaksCleaner = addresses -> bufferPool.release(PageAware.PAGE_SIZE, addresses);

        logger.info("Created chunk cache of size {}, allocating page-aligned buffers from cached reads pool except for sizes >= {}",
                    FBUtilities.prettyPrintMemory(cacheSize),
                    FBUtilities.prettyPrintMemory(BufferPool.CHUNK_DIRECT_ALLOC_SIZE_BYTES));

        //if (BufferPool.CHUNK_DIRECT_ALLOC_SIZE_BYTES <= NativeMemoryMetrics.smallBufferThreshold)
        //    logger.warn("Using direct aligned allocations for chunks as small as {} is not recommended",
        //                FBUtilities.prettyPrintMemory(BufferPool.CHUNK_DIRECT_ALLOC_SIZE_BYTES));
    }

    @Override
    @SuppressWarnings("resource")
    public CompletableFuture<Chunk> asyncLoad(Key key, Executor executor)
    {
        Chunk chunk = newChunk(key);
        return key.file.readChunk(chunk.offset(), chunk.capacity(), chunk);
    }

    public Chunk syncLoad(Key key)
    {
        Chunk chunk = newChunk(key);
        return key.file.readChunkBlocking(chunk.offset(), chunk.capacity(), chunk);
    }

    //@Override
    public void onRemoval(Key key, Chunk chunk, RemovalCause cause)
    {
        chunk.evicted();
    }

    public void reset()
    {
        cache.synchronous().invalidateAll();
        metrics.reset();
    }

    /**
     * Manually ask the leak detector to run its checks so that any chunks that might have been leaked
     * can be automatically cleaned.
     *
     * Normally these checks are run automatically by the leak detector. This method is here for tests that
     * need to force a check at a specific time.
     */
    @VisibleForTesting
    public void checkForLeakedChunks()
    {
        leakDetector.checkForLeaks();
    }

    public void close()
    {
        cache.synchronous().invalidateAll();
        try
        {
            cleanupExecutor.shutdown();
        }
        catch (InterruptedException e)
        {
            logger.error("Interrupted during shutdown: ", e);
        }
    }

    public RebuffererFactory wrap(ChunkReader file)
    {
        return new CachingRebufferer(file);
    }

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified ChunkReader.
     * <p>
     * Thread-safe. One instance per SegmentedFile, created by FileHandle.Builder#maybeCached
     * if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer, RebuffererFactory
    {
        private final ChunkReader source;
        final long chunkAlignmentMask;
        final long fileId;

        public CachingRebufferer(ChunkReader file)
        {
            source = file;
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1 : String.format("%d must be a power of two", chunkSize);
            chunkAlignmentMask = -chunkSize;
            fileId = ChunkCache.fileIdFor(file);
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            try
            {
                long chunkAlignedPos = position & chunkAlignmentMask;
                BufferHolder buf;
                Key chunkKey = new Key(source, fileId, chunkAlignedPos);

                Chunk chunk;

                int spin = 0;
                //There is a small window when a released buffer/invalidated chunk
                //is still in the cache. In this case it will return null
                //so we spin loop while waiting for the cache to re-populate
                while(true)
                {
                    // Using cache.get(k, compute) results in lots of allocation, rather risk the unlikely race...
                    CompletableFuture<Chunk> cachedValue = cache.getIfPresent(chunkKey);
                    if (cachedValue == null)
                    {
                        // this blocking read might compete with an another read, but the race is benign (under the assumption that caching is valid)
                        CompletableFuture<Chunk> entry = new CompletableFuture<>();
                        // Put the future in the cache first so the window of racing (and loading the same chunk twice) is small
                        cache.put(chunkKey, entry);

                        try
                        {
                            chunk = syncLoad(chunkKey);
                        }
                        catch (Throwable t)
                        {
                            // also signal other waiting readers
                            entry.completeExceptionally(t);
                            throw t;
                        }
                        entry.complete(chunk);
                    }
                    else
                    {
                        chunk = cachedValue.join();
                    }

                    buf = chunk.getReferencedBuffer(position);
                    if (buf != null)
                        return buf;

                    if (++spin == 1024)
                        logger.error("Spinning for {}", chunkKey);
                }
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
        }


        // This convoluted and racy loading is done in order to avoid nested read problems (see DB-3050 and JDK-8062841).
        // These problems can actually happen in reality due to ReplicatedKeyProvider issuing reads during uncompression.
        private CompletableFuture<Chunk> racyGetOrLoad(Key chunkKey)
        {
            CompletableFuture<Chunk> chunkFut = cache.getIfPresent(chunkKey);
            if (chunkFut == null || chunkFut.isCompletedExceptionally())
            {
                final CompletableFuture<Chunk> finalChunkFut = new CompletableFuture<>();
                cache.put(chunkKey, finalChunkFut);
                try
                {
                    asyncLoad(chunkKey, null).handle((res, err) ->
                                                     {
                                                         if (err != null)
                                                             // Ideally the now stale, exceptionally completed future should be
                                                             // purged from the chunk cache immediately. As there's no nice way to
                                                             // do that though (no remove(chunkKey) method, put(chunkKey, null)
                                                             // throwing), it will have to be replaced on the next request for that
                                                             // chunk.
                                                             finalChunkFut.completeExceptionally(err);
                                                         else if (res != null)
                                                             finalChunkFut.complete(res);
                                                         return null;
                                                     });
                }
                catch (Throwable t)
                {
                    finalChunkFut.completeExceptionally(t);
                    throw t;
                }
                chunkFut = finalChunkFut;
            }
            // This is guaranteed not to be null, and to be completed with the result of an async read of the chunk referenced
            // by the key, but it's not guaranteed to be present (as in exactly the same future) in the chunk cache, or that
            // the chunk with which it will be resolved will still have a "referencable" buffer.
            return chunkFut;
        }

        //@Override
        public int rebufferSize()
        {
            return Math.min(PageAware.PAGE_SIZE, source.chunkSize());
        }

        @Override
        public void invalidateIfCached(long position)
        {
            long pageAlignedPos = position & chunkAlignmentMask;
            cache.synchronous().invalidate(new Key(source, fileId, pageAlignedPos));
        }

        @Override
        @SuppressWarnings("resource") // channel closed by the PrefetchingRebufferer
        public Rebufferer instantiateRebufferer()
        {

            // Returning the cache content only if the file id matches
            if (fileId == ChunkCache.fileIdFor(source))
                return this;
            return new CachingRebufferer(source);
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
        return cache.synchronous().asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return cache.synchronous().policy().eviction()
                    .map(policy -> policy.weightedSize().orElseGet(cache.synchronous()::estimatedSize))
                    .orElseGet(cache.synchronous()::estimatedSize);
    }

}
