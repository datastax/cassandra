/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;

/**
 * A memory slab with a simple bump-the-pointer strategy: it allocates a large buffer (the slab) and it slices it
 * into smaller buffers when requested. A pointer tracks the position of the memory in the slab that is still free.
 * Once the end of the slab is reached, no more buffers are served. Different buffer sizes are supported and buffer
 * start positions are always page aligned.
 * <p/>
 * Once the end of the slab is reached and no more buffers can be issued, then the slab can no longer be used
 * but it's memory can be recycled by creating a new slab with the same memory. This can only be done when all
 * the served buffers have been released. For this purpose a reference counter is incremented when the slab is created
 * and each time a buffer is returned. When the reference count has reached zero, the memory can be recycled.
 * <p/>
 * This class assumes that only one thread can release the initial reference count in the constructor and destroy
 * or recycle the memory. Multiple threads can however take and return buffers. The thread that must release the initial
 * reference count receives a special buffer from {@link this#allocate(int)}: {@link this#BOUNDARY_CROSSED}.
 * The thread that receives this buffer, must unreference the slab and retry the allocation with a new slab.
 * {@link this#unreference()} must be called only once.
 * <p/>
 * Both {@link #unreference()} and {@link #release(ByteBuffer)} return a boolean. Only one caller will
 * receive true, when the reference count reaches zero. The caller that receives
 * true must either destroy this slab or recycle it into a new one by calling {@link #destroy()} or {@link #recycle()}
 * respectively. {@link #unreference()} must be called exactly once per byte buffer returned and once for the entire slab,
 * it cannot be called more times than this.
 * <p/>
 * Once destroyed or recycled, the number of references is set to a negative value. At this point the
 * slab cannot be used and {@link #allocate(int)} will return null. {@link #recycle()} and  {@link #destroy()} will
 * throw an {@link AssertionError} or an {@link IllegalStateException} if called again.
 * */
class MemorySlabWithBumpPtr
{
    /** A sentinel value that is returned by {@link this#allocate(int)} to indicate to the caller that
     * the boundary was crossed.
     */
    static final ByteBuffer BOUNDARY_CROSSED = ByteBuffer.allocate(0);

    private final ByteBuffer slab;
    private final long baseAddress;
    private final AtomicInteger position;
    private final AtomicInteger numReferences;
    private final long createdAt;

    MemorySlabWithBumpPtr(int capacity)
    {
        this(BufferType.OFF_HEAP_ALIGNED.allocate(capacity));
    }

    private MemorySlabWithBumpPtr(ByteBuffer slab)
    {
        this.slab = slab;
        this.baseAddress = UnsafeByteBufferAccess.getAddress(slab);
        this.position = new AtomicInteger(0);
        this.numReferences = new AtomicInteger(1); // start referenced, calling thread owns this slab
        this.createdAt = MonotonicClock.preciseTime.now();
    }

    /**
     * Allocates a new buffer by bumping the current position forward.
     *
     * If the pointer crosses the boundary, then {@link this#BOUNDARY_CROSSED} is returned. This signals to the caller that it's time
     * to use a new slab. If the pointer is already beyond the boundary then null is returned.
     *
     * @param bufferSize the buffer size
     * @return a buffer of the requested size, or {@link this#BOUNDARY_CROSSED} if the end was reached or null if
     *         the original position was already beyond the end.
     *
     * @throws AssertionError if the buffer size is <= 0 or if the slab is not referenced (numReferences <= 0)
     */
    public ByteBuffer allocate(int bufferSize)
    {
        assert bufferSize > 0 : "BufferSize must be positive:" + bufferSize;

        if (numReferences.get() <= 0 || position.get() > slab.capacity())
            return null; // prevent incrementing position unnecessarily with the getAndAdd()

        int padded = Math.toIntExact(PageAware.padded(bufferSize));
        int bufferPos = position.getAndAdd(padded);

        if (bufferPos < 0)
        {
            // overflow only happened with utests without the  if (numReferences.get() <= 0 || position.get() > slab.capacity()) above
            return null;
        }
        else if ((bufferPos + padded) <= slab.capacity())
        {
            // Landed within the slab, we get a chance to allocate a buffer
            if (reference())
            {
                // returning a valid buffer
                return UnsafeByteBufferAccess.allocateByteBuffer(baseAddress + bufferPos, bufferSize, slab.order(), this);
            }
            else
            {
                // the thread that received the boundary beat us and has already unreferenced this slab
                return null;
            }
        }
        else if (bufferPos <= slab.capacity())
        {
            // landed beyond the slab but started within the slab
            return BOUNDARY_CROSSED;
        }
        else
        {
            // landed and started beyond the slab
            return null;
        }
    }

    /**
     * Increase the reference count but only if in the meantime no other thread has set the count to zero or neg.ve.
     *
     * @return true if the reference count was incremented, false otherwise
     */
    private boolean reference()
    {
        int refCount;
        do
        {
            refCount = numReferences.get();

            if (refCount <= 0)
                return false;

        } while (!numReferences.compareAndSet(refCount, refCount + 1));

        return true;
    }

    /**
     * Return the buffer to the slab: check the address is valid and release the reference count by calling
     * {@link this#unreference()}.
     *
     * @param buffer the buffer that is being released
     * @return true if the last reference was released and the caller must destroy or recycle the slab.
     */
    public boolean release(ByteBuffer buffer)
    {
        long address = UnsafeByteBufferAccess.getAddress(buffer);
        assert (address >= baseAddress) && (address <= baseAddress + slab.capacity()) :
                String.format("Buffer address out of range: %s, %d", this, address);

        // prevent further use without re-initialization
        UnsafeByteBufferAccess.resetByteBufferInstance(buffer);

        return unreference();
    }

    /**
     * Called by the owner of this slab to indicate that no more buffers will be taken from this slab.
     * Release the reference count by one and return true if the last reference was released and the
     * caller must destroy or recycle the slab.
     *
     * @return true if the last reference was released and the caller must destroy or recycle the slab.
     */
    boolean unreference()
    {
        long numReferences = this.numReferences.decrementAndGet();
        assert numReferences >= 0 : "unreference() or release(ByteBuffer) have been called too many times, refer to class javadoc";
        return numReferences == 0;
    }

    /**
     * Return the free space still available for allocations. If the slab is being accessed by concurrent threads,
     * then this result may not be accurate if another thread allocates a buffer in parallel. It should only therefore
     * be used for testing.
     *
     * @return the free space still available for allocations
     */
    int free()
    {
        return Math.max(0, slab.capacity() - position.get());
    }

    /**
     * @return the base address
     */
    long baseAddress()
    {
        return baseAddress;
    }

    /**
     * @return the maximum capacity of this slab
     */
    int capacity()
    {
        return slab.capacity();
    }

    /**
     * @return the number of references to this slab
     */
    int numReferences()
    {
        return numReferences.get();
    }

    /**
     * @return a new slab using the same memory
     *
     * @throws IllegalStateException if the slab was already recycled or destroyed.
     */
    MemorySlabWithBumpPtr recycle()
    {
        int numReferences = this.numReferences.get();
        assert numReferences == 0 : "Slab should have been unreferenced and all buffers returned before recycling: " + toString(numReferences);

        if (this.numReferences.compareAndSet(0, -1))
        {
            return new MemorySlabWithBumpPtr(slab);
        }

        throw new IllegalStateException("Failed to recycle slab, wrong ref. count or concurrent access: " + toString());
    }

    /**
     * Destroy the slab by releasing its memory and setting the reference count to -1.
     *
     * @throws IllegalStateException if the slab was already referenced or destroyed.
     */
    void destroy()
    {
        int numReferences = this.numReferences.get();
        assert numReferences == 0 : "Slab should have been unreferenced and all buffers returned before destroying: " + toString(numReferences);

        if (this.numReferences.compareAndSet(0, -2))
            FileUtils.clean(slab);
        else
            throw new IllegalStateException("Failed to destroy slab, wrong ref. count or concurrent access: " + toString());
    }

    @Override
    public String toString()
    {
        return toString(numReferences.get());
    }

    private String toString(int numReferences)
    {
        return String.format("[Addr: %d, Pos %d, capacity %d, num. references: %d, created: %d ms ago]",
                             baseAddress,
                             position.get(),
                             slab.capacity(),
                             numReferences,
                             MonotonicClock.preciseTime.now()- createdAt);
    }


}
