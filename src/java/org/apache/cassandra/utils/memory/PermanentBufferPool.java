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

/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.concurrent.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A buffer pool suitable for buffers that can potentially be long lived or permanent.
 * <p/>
 * This pool is typically used by a cache, such as the {@link org.apache.cassandra.cache.ChunkCache},
 * where some buffers may be retained for a long time whilst other buffers may be released sooner.
 * <p/>
 * It uses an inner pool, {@link SizeTieredPool} that contains sub-pools, one for each buffer size
 * rounded to a power of two. This class takes care of allocating memory directly if the inner pool
 * is exhausted or if the buffer is larger than {@link SizeTieredPool#maxBufferSize()}.
 * <p/>
 * To avoid fragmentation because of the long lived nature of buffers, this pool only supports
 * a limited set of sizes, from a minimum to a maximum size, refer to {@link SizeTieredPool } for more details.
 */
@ThreadSafe
class PermanentBufferPool extends BufferPool
{
    private static final Logger logger = LoggerFactory.getLogger(PermanentBufferPool.class);
    private final static NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS);

    private final SizeTieredPool pool;
    private final LongAdder overflowMemoryUsage;

    PermanentBufferPool(String name, int minBufferSize, int maxBufferSize, long maxPoolSize)
    {
        super(name, maxPoolSize);

        this.pool = new SizeTieredPool(minBufferSize, maxBufferSize, NUM_BUFFERS_PER_SLAB, maxPoolSize, CLEANUP_INTERVAL_MILLIS);
        this.overflowMemoryUsage = new LongAdder();

        logger.info("{} is enabled, max is {}", name, FBUtilities.prettyPrintMemory(maxPoolSize));
    }

    @Override
    public ByteBuffer allocate0(int size)
    {
        ByteBuffer buf;
        if (size > pool.maxBufferSize())
        {
            noSpamLogger.debug("Requested buffer size {} that is bigger than {}, allocating directly",
                               FBUtilities.prettyPrintMemory(size),
                               FBUtilities.prettyPrintMemory(pool.maxBufferSize()));

            buf = realAllocate(size);
        }
        else
        {
            long address = pool.allocate(size, 1)[0];
            if (address == -1)
            {
                noSpamLogger.debug("Requested buffer size {} has been allocated directly due to buffer pool exhausted",
                                   FBUtilities.prettyPrintMemory(size));

                buf = realAllocate(size);
            }
            else
            {
                buf = UnsafeByteBufferAccess.allocateByteBuffer(address, size, pool.order(), pool);
            }
        }

        return buf;
    }

    private ByteBuffer realAllocate(int size)
    {
        //metrics.overflowAllocs.mark();

        ByteBuffer ret = BufferType.OFF_HEAP_ALIGNED.allocate(size);
        overflowMemoryUsage.add(ret.capacity());

        return ret;
    }

    @Override
    public void release0(ByteBuffer buffer, AttachmentMarker marker)
    {
        if (marker.attachment() instanceof SizeTieredPool)
        {
            pool.release(buffer.capacity(), new long[] { UnsafeByteBufferAccess.getAddress(buffer) });
        }
        else
        {
            overflowMemoryUsage.add(-buffer.capacity());
            FileUtils.clean(buffer);
        }
    }

    @Override
    public long[] allocate(int size, int numRegions)
    {
        if (size > pool.maxBufferSize())
            throw new IllegalStateException("Expected size to be less than " + pool.maxBufferSize());

        long[] ret = pool.allocate(size, numRegions);

        try
        {
            for (int i = 0; i < ret.length; i++)
            {
                if (ret[i] == -1)
                    throw new IllegalStateException(String.format("Failed to allocate address nr. %d of size %d: buffer pool is probably exhausted, " +
                                                                "consider setting file_cache_size_in_mb and inflight_data_overhead_in_mb in the yaml", i, size));
            }
        }
        catch (Throwable t)
        {
            release(size, ret);
            throw t;
        }

        return ret;
    }

    @Override
    public void release(int size, long[] addresses)
    {
        pool.release(size, addresses);
    }

    @Override
    public void cleanup()
    {
        pool.cleanup();
    }

    /**
     * @return The total memory currently used by clients.
     */
    @Override
    public long usedMemoryBytes()
    {
        return pool.usedMemoryBytes() + overflowMemoryBytes();
    }

    /**
     * @return The total memory currently allocated by the pool.
     */
    @Override
    public long allocatedMemoryBytes()
    {
        return pool.allocatedMemoryBytes() + overflowMemoryBytes();
    }

    /**
     * @return The total memory allocated for buffers that are not in the pool.
     */
    @Override
    public long overflowMemoryBytes()
    {
        return overflowMemoryUsage.longValue();
    }

    /**
     * @return the number of allocations performed directly
     */
    public double missedAllocationsMeanRate()
    {
        return -1;
    }

    /**
     * @return the detailed status of the buffer pool
     */
    public String status()
    {
        return toString();
    }

    @Override
    public String toString()
    {
       return String.format("BufferPool for long lived buffers: %s%sOverflow: %s",
                            pool.toString(),
                            System.lineSeparator(),
                            FBUtilities.prettyPrintMemory(overflowMemoryUsage.longValue()));
    }
}
