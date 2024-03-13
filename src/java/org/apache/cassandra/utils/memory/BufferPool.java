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

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.datastax.bdp.db.utils.leaks.detection.LeakLevel;
import com.datastax.bdp.db.utils.leaks.detection.LeaksDetector;
import com.datastax.bdp.db.utils.leaks.detection.LeaksDetectorFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.BufferPoolMetrics;

import org.apache.cassandra.utils.UnsafeByteBufferAccess;



public abstract class BufferPool implements BufferPoolMXBean
{
    /** The size for direct allocations performed by the chunk cache. Chunks with size >= of this value will be allocated directly
     * and not taken from the pool. This property is defined here rather than in {@link ChunkCache} because it is used by
     * the {@link DiskOptimizationStrategy}, which is part of client initialization. We don't want to start server components
     * for clients. */
    public static final int CHUNK_DIRECT_ALLOC_SIZE_BYTES = Math.max(0, Integer.getInteger("dse.chunk_cache.direct_alloc_size_in_kb", 256)) * 1024;
    /**
     * Set this to true to avoid using buffer pools, see {@link BufferPoolDisabled}. When buffer pools are
     * disabled, each buffer is allocated using {@link org.apache.cassandra.io.compress.BufferType#OFF_HEAP_ALIGNED}.
     * This is not recommended and should be used only for tests.
     */
    public static final boolean DISABLED = Boolean.getBoolean("cassandra.test.disable_buffer_pool");

    /**
     * A cleanup operation will run periodically to try and release memory that is no longer used by tiered pools, this
     * parameter controls how frequently the cleanup operation runs.
     */
    static final int CLEANUP_INTERVAL_MILLIS = Integer.getInteger("dse.buffer_pool_cleanup_interval_ms", 5000);

    /**
     * A thread waiting for another thread to allocate a shared slab will wait for at most this time before giving up.
     */
    static final int CONTENTION_WAIT_TIME_MILLIS =  Integer.getInteger("dse.buffer_pool_contention_wait_time_ms", 5000);

    /** The minimum size of a page aligned buffer, 1Kib. Below this the requested size will be rounded up. Must be a power of two. */
    public static final int MIN_DIRECT_READS_BUFFER_SIZE = 1 << 10;

    /** The maximum size of a page aligned buffer, 64KiB. Beyond this a direct allocation will be performed. Must be a power of two. */
    public static final int MAX_DIRECT_READS_BUFFER_SIZE = 64 << 10;

    /** The size of a thread local slab to serve short lived buffers, 1MB. */
    public static final int THREAD_LOCAL_SLAB_SIZE = 1 << 20;

    /** The number of buffers that will be served by a single bitmap slab in sized tiered pool. This should be a multiple of 32,
     * see {@link MemorySlabWithBitmap#MemorySlabWithBitmap(int, int)} for the reason, and how to remove this limitation. */
    public static final int NUM_BUFFERS_PER_SLAB = 1024;

    /**
     * The maximum number of buffers that will be stashed by a thread for long lived sized tired pools.
     */
    public static final int MAX_THREAD_LOCAL_BUFFERS = 64;

    /**
     * The maximum number of free buffers in a size tiered pool.
     *
     * When there are more than MAX_NUM_FREE_BUFFERS in a pool, a periodic cleanup operation
     * will check for any slabs that are completely free, and if it finds any, they will be released
     * so that the memory can be re-used. See {@link #CLEANUP_INTERVAL_MILLIS}.
     * */
    public static final int MAX_NUM_FREE_BUFFERS = NUM_BUFFERS_PER_SLAB * 2;

    static
    {
        Preconditions.checkArgument(CONTENTION_WAIT_TIME_MILLIS > 0,
                                    "CONTENTION_WAIT_TIME_MILLIS must be positive, set it to a large value if it's causing problems: %s",
                                    CONTENTION_WAIT_TIME_MILLIS);
    }

    private final LeaksDetector leaksDetector;
    protected final BufferPoolMetrics metrics;
    protected final long maxPoolSize;

    protected BufferPool(String name, long maxPoolSize)
    {
        this.leaksDetector = LeaksDetectorFactory.create(name, ByteBuffer.class, LeakLevel.FIRST_LEVEL);
        this.metrics = BufferPoolMetrics.create(name, this);
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Return a new buffer, either freshly allocated or taken from an existing pool.
     *
     * @param size the buffer size
     *
     * @return the buffer
     */
    public ByteBuffer allocate(int size)
    {
        if (size < 0)
            throw new IllegalArgumentException("Size must be >=0: " + size);

        if (size == 0)
            return UnsafeByteBufferAccess.EMPTY_BUFFER;

        ByteBuffer ret = allocate0(size);

        if (ret != null)
            AttachmentMarker.mark(ret, this, leaksDetector);

        return ret;
    }

    public ByteBuffer get(int size, BufferType bufferType) {
        return allocate(size);
    }

    public ByteBuffer getAtLeast(int size, BufferType bufferType) {
        return allocate(size);
    }

    public void put(ByteBuffer buffer) {
        release(buffer);
    }

    public void putUnusedPortion(ByteBuffer buffer) {
        release(buffer);
    }

    abstract ByteBuffer allocate0(int size);

    /**
     * Return the buffer to the pool, or release its memory if it's not part of the pool.
     *
     * @param buffer - the buffer to return
     */
    public void release(ByteBuffer buffer)
    {
        assert buffer != null : "Buffer should not be null";
        assert buffer.isDirect() : "Buffer should not be on heap";

        AttachmentMarker marker = AttachmentMarker.unmark(buffer, this);

        if (marker == null)
        {
            // Not our buffer
            if (buffer != UnsafeByteBufferAccess.EMPTY_BUFFER)
                FileUtils.clean(buffer);
        }
        else
        {
            release0(buffer, marker);
        }
    }

    /**
     * Release the buffer or return it to the pool.
     *
     * @param buffer the buffer
     */
    abstract void release0(ByteBuffer buffer, AttachmentMarker marker);

    /**
     * Bulk allocate multiple memory regions.
     *
     * @param size The size of each individual memory region
     * @param numAddresses the number of memory regions to allocateion
     *
     * @return an array containing the addresses of the memory regions allocated
     */
    public long[] allocate(int size, int numAddresses)
    {
        throw new IllegalStateException("This buffer pool does not support this functionality");
    }

    /**
     * Bulk release multiple addresses, only {@link PermanentBufferPool} implements this.
     *
     * @param size The size of each individual memory region
     * @param addresses The memory addresses to be released.
     */
    public void release(int size, long[] addresses)
    {
        throw new IllegalStateException("This buffer pool does not support this functionality");
    }

    /**
     * Forces a cleanup operation. This is only visible for testing, since there is a private periodic
     * task that runs cleanup. Cleanup must be performed only by a single thread at a time.
     */
    @VisibleForTesting
    public abstract void cleanup();

    /**
     * @return the metrics for this buffer pool
     */
    public BufferPoolMetrics metrics()
    {
        return metrics;
    }

    /**
     * @return the maximum memory that this buffer pool can allocate permanently.
     */
    public long maxPoolSize()
    {
        return maxPoolSize;
    }


}
