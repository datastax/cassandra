/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.DseThreadLocal;
import org.apache.cassandra.utils.concurrent.ThreadLocals;

/**
 * A buffer pool suitable for temporary buffers that are released quickly afterwards and ideally in temporal order,
 * that is first allocated first released.
 * <p/>
 * This pool is typically used to obtain short lived buffers for read operations in order to reuse the buffers
 * multiple times.
 * <p/>
 * It uses a list of bump the pointer slabs, {@link MemorySlabWithBumpPtr}. TPC threads have a dedicated slab
 * stored in a thread local. Other threads share a global slab instead. This is because IO threads may need
 * buffers from this pool and the number of IO threads is currently unbounded, see DB-3124.
 * <p/>
 * A slab will be used as long as it has space. When the slab is created, and each time a buffer is taken, a counter
 * is increased. When the slab has reached the end, or each time a buffer is returned, this counter is decreased.
 * When this counter reaches zero, the slab will be returned to the queue if the total allocated
 * memory is below the limit, otherwise the slab will be released.
 */
@ThreadSafe
class TemporaryBufferPool extends BufferPool
{
    private static final Logger logger = LoggerFactory.getLogger(TemporaryBufferPool.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS);

    private final ConcurrentLinkedQueue<MemorySlabWithBumpPtr> slabs;
    private final DseThreadLocal<MemorySlabWithBumpPtr> threadLocalSlab;
    private final int slabSize;
    private final int maxBufferSize;
    private final AtomicLong allocated;
    private final AtomicLong used;

    private AtomicReference<MemorySlabWithBumpPtr> sharedSlab;

    TemporaryBufferPool(String name, int slabSize, int maxBufferSize, long maxPoolSize)
    {
        super(name, maxPoolSize);

        Preconditions.checkArgument(slabSize > 0, "Slab size should be positive: %s", slabSize);
        Preconditions.checkArgument(maxBufferSize > 0, "Max buffer size should be positive: %s", maxBufferSize);
        Preconditions.checkArgument(maxPoolSize > 0, "Max pool size should be positive: %s", maxPoolSize);
        Preconditions.checkArgument(maxBufferSize < slabSize, "Max buffer size %s should be less than slab size %s", maxBufferSize, slabSize);
        Preconditions.checkArgument(slabSize < maxPoolSize, "Slab size %s should be less than max pool size %s", slabSize, maxPoolSize);


        this.slabs = new ConcurrentLinkedQueue<>();
        this.threadLocalSlab = ThreadLocals.withInitial(this::newSlab);
        this.slabSize = slabSize;
        this.maxBufferSize = maxBufferSize;
        this.allocated = new AtomicLong(0);
        this.used = new AtomicLong(0);

        logger.info("{} is enabled, max is {}", name, FBUtilities.prettyPrintMemory(maxPoolSize));

        // Create an initial slab to be shared among all non-TPC threads
        this.sharedSlab = new AtomicReference<>(newSlab());
    }

    @Override
    public ByteBuffer allocate0(int bufferSize)
    {
        ByteBuffer ret;
        if (bufferSize > maxBufferSize)
        {
            noSpamLogger.debug("Requested buffer size {} that is bigger than {}",
                               FBUtilities.prettyPrintMemory(bufferSize),
                               FBUtilities.prettyPrintMemory(maxBufferSize));

            ret = allocateDirect(bufferSize);
        }
        else
        {
            //if (Thread.currentThread() instanceof TPCThread)
            //    ret = allocateFromThreadLocal(bufferSize);
            //else
                ret = allocateFromShared(bufferSize);
        }

        if (ret != null)
            used.getAndAdd(bufferSize);

        return ret;
    }

    private ByteBuffer allocateDirect(int bufferSize)
    {
        noSpamLogger.debug(("Allocating buffer of {} bytes directly, current status: [{}]"), bufferSize, toString());

        ByteBuffer ret = BufferType.OFF_HEAP_ALIGNED.allocate(bufferSize); // may throw an OOM
        //metrics.overflowAllocs.mark(); // direct allocations are marked as overflow allocations
        return ret;
    }

    /**
     * Takes a buffer from the thread local slab if this thread does have a thread local slab.
     */
    private ByteBuffer allocateFromThreadLocal(int bufferSize)
    {
        MemorySlabWithBumpPtr slab = threadLocalSlab.get();
        if (slab == null)
        {   // this occurs if there was an OOM preventing a new slab from being installed
            slab = newSlab(); // may throw an OOM
            threadLocalSlab.set(slab);
        }

        ByteBuffer ret = slab.allocate(bufferSize);
        assert ret != null : "Expected valid buffer or boundary crossed: " + slab + " size requested: " + bufferSize;

        if (ret == MemorySlabWithBumpPtr.BOUNDARY_CROSSED)
        {
            threadLocalSlab.set(null);

            if (slab.unreference())
                returnSlab(slab);

            slab = newSlab(); // may throw an OOM
            threadLocalSlab.set(slab);

            ret = slab.allocate(bufferSize);
        }

        return ret;
    }

    /**
     * Takes a buffer from the shared slab. This method could fail due to too much contention.
     */
    private ByteBuffer allocateFromShared(int bufferSize)
    {
        ByteBuffer ret;
        long start = MonotonicClock.preciseTime.now();

        MemorySlabWithBumpPtr current;
        do
        {
            current = sharedSlab.get();
            if (current == null)
                current = createSharedSlabFollowingOOM(); // may throw, in which case sharedSlab will be unchanged

            ret = current.allocate(bufferSize);

            if (ret == MemorySlabWithBumpPtr.BOUNDARY_CROSSED)
            {
                // need to switch shared, only one thread can get this result
                switchSharedSlab(current); // may throw, but will set sharedSlab to null first
            }
            else if (ret == null)
            {
                Thread.yield(); // yield a bit so that another thread replaces the slab
            }
            else
            {
                // valid buffer, return it
                return ret;
            }

        } while (MonotonicClock.preciseTime.now() - start <= CONTENTION_WAIT_TIME_MILLIS);

        throw new OutOfMemoryError(String.format("Failed to allocate byte buffer from temporary pool, waited for too long (%d ms), current shared slab: %s",
                                                 MonotonicClock.preciseTime.now()  - start,
                                                 current));
    }

    /**
     * If a thread receives a {@link MemorySlabWithBumpPtr#BOUNDARY_CROSSED}, then this thread will call
     * this method to create a new shared slab. Only one thread can execute this method. The current slab
     * will be unreferenced.
     *
     * @param current  the current slab to be unreferenced
     * @return the new shared slab
     * @throws OutOfMemoryError
     */
    @Nullable
    private MemorySlabWithBumpPtr switchSharedSlab(MemorySlabWithBumpPtr current)
    {
        MemorySlabWithBumpPtr replacement = null;
        try
        {
            replacement = newSlab(); // may throw an OOM, in which case we want to publish a null replacement
        }
        finally
        {
            boolean res = sharedSlab.compareAndSet(current, replacement);
            assert res : "Multiple threads raced on boundary, this was not expected: " + current;

            // Unreference the current only after having replaced it otherwise a TPC thread could recycle and put it
            // in its thread local whilst the other threads are still trying to use it
            if (current.unreference())
                returnSlab(current); // This would throw only if it fails the assertion
        }

        return replacement;
    }

    /**
     * If there is an OOM {@link this#switchSharedSlab(MemorySlabWithBumpPtr)} will publish a null slab. When
     * this happens, this method is called to try and allocate a new slab. Unlike {@link this#switchSharedSlab(MemorySlabWithBumpPtr)},
     * several threads may execute this method.
     *
     * @return the new shared slab
     * @throws OutOfMemoryError
     */
    private MemorySlabWithBumpPtr createSharedSlabFollowingOOM()
    {
        MemorySlabWithBumpPtr slab = newSlab(); // may throw an OOM
        if (!sharedSlab.compareAndSet(null, slab))
        {
            // another thread beat us
            slab.unreference();
            returnSlab(slab);
            slab = sharedSlab.get();
        }

        return slab;
    }

    @Override
    public void release0(ByteBuffer buffer, AttachmentMarker marker)
    {
        used.getAndAdd(-buffer.capacity());

        if (marker.attachment() instanceof MemorySlabWithBumpPtr)
        {
            MemorySlabWithBumpPtr slab = (MemorySlabWithBumpPtr) marker.attachment();

            // Return the buffer to the slab, if release returns true then
            // the owning thread is no longer using the slab and all buffers
            // have been returned, we must therefore recycle it
            if (slab.release(buffer))
                returnSlab(slab);
        }
        else
        {
            FileUtils.clean(buffer);
        }
}

    /**
     * Either create a new slab or recycle an existing one.
     *
     * @return a slab ready to be used for allocating buffers
     *
     * @throws OutOfMemoryError if OOM
     */
    private MemorySlabWithBumpPtr newSlab()
    {
        MemorySlabWithBumpPtr ret = slabs.poll();
        if (ret == null)
        {
            ret = new MemorySlabWithBumpPtr(slabSize); // may throw OOM
            allocated.getAndAdd(slabSize);
            if (allocated.get() > maxPoolSize)
            {
                noSpamLogger.debug("Allocated slab beyond maxPoolSize {}, allocated: {}",
                                   FBUtilities.prettyPrintMemory(maxPoolSize),
                                   FBUtilities.prettyPrintMemory(allocated.get()));

                //metrics.overflowAllocs.mark();
            }
        }

        return ret;
    }

    /**
     * Either add the slab to the queue or destroy it.
     *
     * @param slab the slab to re-use or destroy
     */
    private void returnSlab(MemorySlabWithBumpPtr slab)
    {
        assert slab.numReferences() == 0 : "Slab is still referenced:" +  slab;

        if (allocated.get() > maxPoolSize)
        {
            slab.destroy();
            allocated.getAndAdd(-slabSize);
        }
        else
        {
            slab = slab.recycle();
            if (!slabs.offer(slab)) // cannot return false but just in case
            {
                slab.unreference();
                slab.destroy();
                allocated.getAndAdd(-slabSize);
                assert false : "Unbounded queue failed to add recycled slab";
            }
        }
    }

    @Override
    public void cleanup()
    {
        // small pool, no op
    }

    @Override
    public long usedMemoryBytes()
    {
        return used.get();
    }

    @Override
    public long allocatedMemoryBytes()
    {
        return allocated.get();
    }

    @Override
    public long overflowMemoryBytes()
    {
        return Math.max(0, allocatedMemoryBytes() - maxPoolSize);
    }

    @Override
    public double missedAllocationsMeanRate()
    {
        return -1;
    }

    @Override
    public String status()
    {
        return toString();
    }

    @Override
    public String toString()
    {
        return String.format("BufferPool for temporary buffers: allocated %s, used %s, overflow %s, overflow allocations mean rate %f",
                             FBUtilities.prettyPrintMemory(allocatedMemoryBytes()),
                             FBUtilities.prettyPrintMemory(usedMemoryBytes()),
                             FBUtilities.prettyPrintMemory(overflowMemoryBytes()),
                             missedAllocationsMeanRate());
    }
}
