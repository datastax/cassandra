/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

public interface BufferPoolMXBean
{
    /**
     * The name of the buffer pool used by the file cache, see {@link org.apache.cassandra.cache.ChunkCache}.
     */
    String CACHED_FILE_READS_MBEAN_NAME = "org.apache.cassandra.utils.memory.buffer:type=BufferPool,name=CachedFileReads";

    /**
     * The name of the buffer pool used by direct reads, such as for streaming, hints,  or when the chunk cache is disabled.
     */
    String DIRECT_FILE_READS_MBEAN_NAME = "org.apache.cassandra.utils.memory.buffer:type=BufferPool,name=DirectFileReads";

    /**
     * @return The total memory currently used by clients.
    */
    long usedMemoryBytes();

    /**
     * @return The total memory currently allocated by the pool.
     */
    long allocatedMemoryBytes();

    /**
     * @return The total memory allocated for buffers that are not in the pool.
     */
    long overflowMemoryBytes();

    /**
     * @return the mean rate of allocations performed directly
     */
    double missedAllocationsMeanRate();

    /**
     * @return the detailed status of the buffer pool
     */
    String status();
}
