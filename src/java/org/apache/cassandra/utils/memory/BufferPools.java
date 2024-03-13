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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.PageAware;
import org.xerial.snappy.buffer.BufferAllocatorFactory;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;
import static org.apache.cassandra.utils.memory.BufferPool.MAX_DIRECT_READS_BUFFER_SIZE;
import static org.apache.cassandra.utils.memory.BufferPool.THREAD_LOCAL_SLAB_SIZE;

public class BufferPools
{
    private static final Logger logger = LoggerFactory.getLogger(BufferPools.class);

    /**
     * Used by chunk cache to store decompressed data and buffers may be held by chunk cache for arbitrary period.
     */
    private static final long FILE_MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;

    /**
     * Used by client-server or inter-node requests, buffers should be released immediately after use.
     */
    private static final long NETWORKING_MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getNetworkingCacheSizeInMB() * 1024L * 1024L;


    /**
     * The buffer pool instance used by the chunk cache. It's created with a maximum pool size of
     * {@link DatabaseDescriptor#getFileCacheSizeInMB()} Mib.
     */
    private final static BufferPool CHUNK_CACHE_POOL = createPermanent("CachedReadsBufferPool",
                                                                       BufferPoolMXBean.CACHED_FILE_READS_MBEAN_NAME,
                                                                       PageAware.PAGE_SIZE, // file cache only uses PAGE_SIZE buffer sizes
                                                                       PageAware.PAGE_SIZE,
                                                                       FILE_MEMORY_USAGE_THRESHOLD);

    /**
     * The buffer pool instance used for direct reads. It's created with a maximum pool size of
     * {@link DatabaseDescriptor#getNetworkingCacheSizeInMB()} and it can be used for reading hints, streaming checksummed
     * files or by the chunk readers when the cache is disabled.
     */
    private final static BufferPool NETWORKING_POOL = createTemporary("DirectReadsBufferPool",
                                                                      BufferPoolMXBean.DIRECT_FILE_READS_MBEAN_NAME,
                                                                      THREAD_LOCAL_SLAB_SIZE,
                                                                      MAX_DIRECT_READS_BUFFER_SIZE * 2,
                                                                      NETWORKING_MEMORY_USAGE_THRESHOLD);

    static
    {
        logger.info("Global buffer pool limit is {} for {} and {} for {}",
                    prettyPrintMemory(FILE_MEMORY_USAGE_THRESHOLD),
                    BufferPoolMXBean.CACHED_FILE_READS_MBEAN_NAME,
                    prettyPrintMemory(NETWORKING_MEMORY_USAGE_THRESHOLD),
                    BufferPoolMXBean.DIRECT_FILE_READS_MBEAN_NAME);

        CHUNK_CACHE_POOL.metrics().register3xAlias();
    }
    /**
     * Long-lived buffers used for chunk cache and other disk access
     */
    public static BufferPool forChunkCache()
    {
        return CHUNK_CACHE_POOL;
    }

    /**
     * Short-lived buffers used for internode messaging or client-server connections.
     */
    public static BufferPool forNetworking()
    {
        return NETWORKING_POOL;
    }

    public static void shutdownLocalCleaner(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException
    {

    }


    /**
     * Create a buffer pool suitable for potentially long-lived buffers, such as those stored in a cache.
     *
     * @param name the name of the pool, for the metrics
     * @param mbeanName the mbean name, for nodetool bufferpool status
     * @param minBufferSize the minimum buffer size, below this buffer sizes are rounded up
     * @param maxBufferSize the maximum buffer size, beyond this direct allocations are performed
     * @param maxPoolSize the maximum size of the pool, memory below this value is not released but kept for later
     *
     * @return the buffer pool just created
     */
    public static BufferPool createPermanent(String name, String mbeanName, int minBufferSize, int maxBufferSize, long maxPoolSize)
    {
        BufferPool ret = BufferPool.DISABLED ? new BufferPoolDisabled(name, maxPoolSize) : new PermanentBufferPool(name, minBufferSize, maxBufferSize, maxPoolSize);

        return ret;
    }

    /**
     * Create a buffer pool suitable for temporary buffers, that must be released quickly.
     *
     * @param name the name of the pool, for the metrics
     * @param mbeanName the mbean name, for nodetool bufferpool status
     * @param slabSize the size of the slab for slicing temporary buffers from
     * @param maxBufferSize the maximum buffer size, beyond this direct allocations are performed
     * @param maxPoolSize the maximum size of the pool, memory below this value is not released but kept for later
     *
     * @return the buffer pool just created
     */
    public static BufferPool createTemporary(String name, String mbeanName, int slabSize, int maxBufferSize, long maxPoolSize)
    {
        BufferPool ret = BufferPool.DISABLED ? new BufferPoolDisabled(name, maxPoolSize) : new TemporaryBufferPool(name, slabSize, maxBufferSize, maxPoolSize);

        return ret;
    }

}
