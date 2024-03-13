/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.concurrent.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * A buffer pool that does no pooling at all, it simply allocates directly.
 */
class BufferPoolDisabled extends BufferPool
{
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolDisabled.class);

    private final LongAdder memory;

    BufferPoolDisabled(String name, long maxPoolSize)
    {
        super(name, maxPoolSize);
        this.memory = new LongAdder();

        logger.info("{} pool is disabled", name);
    }

    /**
     * Return a buffer of the specified size by allocating it directly using {@link BufferType#OFF_HEAP_ALIGNED}.
     * <p/>
     * The buffer will always be off heap and page aligned, even when it is very small, which could be wasteful.
     * <p/>
     * @param size  - the size of the buffer to return
     *
     * @return a buffer of the specified size either taken from the pool or allocated directly
     */
    @Override
    public ByteBuffer allocate0(int size)
    {
        memory.add(size);
        //metrics.overflowAllocs.mark();
        return BufferType.OFF_HEAP_ALIGNED.allocate(size);
    }

    /**
     * Release the memory allocated for this buffer.
     *
     * @param buffer - the buffer to release
     */
    @Override
    public void release0(ByteBuffer buffer, AttachmentMarker marker)
    {
        memory.add(-buffer.capacity());  // this may be wrong if the buffer was not allocated with this pool
        FileUtils.clean(buffer);
    }

    @Override
    public void cleanup()
    {
    }

    /**
     * @return The total memory currently used by clients.
     */
    @Override
    public long usedMemoryBytes()
    {
        return memory.longValue();
    }

    /**
     * @return The total memory currently allocated by the pool.
     */
    @Override
    public long allocatedMemoryBytes()
    {
        return usedMemoryBytes();
    }

    /**
     * @return The total memory allocated for buffers that are not in the pool.
     */
    @Override
    public long overflowMemoryBytes()
    {
        return usedMemoryBytes();
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
        return "Buffer pool disabled";
    }
}
