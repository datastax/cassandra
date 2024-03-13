/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.DseThreadLocal;
import org.apache.cassandra.utils.concurrent.InlinedThreadLocalThread;
import org.apache.cassandra.utils.concurrent.LongAdder;
import org.apache.cassandra.utils.concurrent.TPCUtils;
import org.apache.cassandra.utils.concurrent.ThreadLocals;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.apache.cassandra.utils.memory.BufferPool.CONTENTION_WAIT_TIME_MILLIS;

/**
 * A pool of buffers organized by buffer size.
 * </p>
 * For each power of two between the minimum and maximum buffer sizes, this pool contains a sub-pool
 * for that size. Each sub-pool contains a queue of {@link MemorySlabWithBitmap} that have some space,
 * enough to allocate at least one buffer.
 * Because the subPools are organized by size, the first slab found on the queue will return a buffer of that size,
 * unless there is a race with another thread, in which case the operation is retried.
 * </p>
 * Periodically a cleanup operation will release any additional free memory that is no longer used by a sub-pool.
 * </p>
 * This class is a global class, accessed by multiple threads, and as such all operations must be thread safe
 * without the need for external synchronization.
 * </p>
 * The minimum and maximum buffer sizes should be powers of two.
 */
@ThreadSafe
final class SizeTieredPool
{
    private final static Logger logger = LoggerFactory.getLogger(SizeTieredPool.class);
    private final static NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS);
    private final static MemorySlabWithBitmap MEMORY_SLAB_SENTINEL = new MemorySlabWithBitmap(1, 4);

    private final int minBufferSize;
    private final int minLog2Ceil;
    private final int maxBufferSize;
    private final int numBuffersPerSlab;
    private final long maxPoolSize;

    /** This arrays contains all powers of twos starting at minBufferSize (index zero)
     * and ending at maxBufferSize (last valid index). */
    private final SubPool[] subPools;

    /** This is the memory that has been reserved so far by all sub-pools. It cannot exceed {@link this#maxPoolSize}. */
    private final AtomicLong reservedMemory;

    SizeTieredPool(int minBufferSize, int maxBufferSize, int numBuffersPerSlab, long maxPoolSize, long cleanupIntervalMillis)
    {
        Preconditions.checkArgument(Integer.bitCount(minBufferSize) == 1, "minBufferSize must be a power of two (%s)", minBufferSize);
        Preconditions.checkArgument(Integer.bitCount(maxBufferSize) == 1, "maxBufferSize must be a power of two (%s)", maxBufferSize);

        this.minBufferSize = minBufferSize;
        this.minLog2Ceil = 32 - Integer.numberOfLeadingZeros(minBufferSize - 1); // perf. optimization
        this.maxBufferSize = maxBufferSize;
        this.numBuffersPerSlab = numBuffersPerSlab;
        this.maxPoolSize = maxPoolSize;
        this.reservedMemory = new AtomicLong(0L);

        this.subPools = new SubPool[toIndex(maxBufferSize) + 1];
        for (int i = 0; i < subPools.length; i++)
            this.subPools[i] = new SubPool(toSize(i));

        if (cleanupIntervalMillis > 0)
            ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(this::cleanup, cleanupIntervalMillis, cleanupIntervalMillis, TimeUnit.MILLISECONDS);
    }

    long[] allocate(int size, int numRegions)
    {
        int index = toIndex(size);

        SubPool subPool = subPools[index];
        return subPool.allocate(size, numRegions);
    }

    ByteOrder order()
    {
        return MEMORY_SLAB_SENTINEL.order();
    }

    void release(int size, long[] addresses)
    {
        int index = toIndex(size); // because there is a MemorySlabWithBitmap, index will be valid
        SubPool subPool = subPools[index];
        subPool.release(size, addresses);
    }

    long usedMemoryBytes()
    {
        return Arrays.stream(subPools).mapToLong(SubPool::usedMemoryBytes).sum();
    }

    long allocatedMemoryBytes()
    {
        return Arrays.stream(subPools).mapToLong(SubPool::allocatedMemoryBytes).sum();
    }

    int maxBufferSize()
    {
        return maxBufferSize;
    }

    /** Convert the size into an index in the subPools array, no preconditions checks on the size here for
     * performance reasons, callers should pass a size >= . {@link this#minBufferSize}. */
    private int toIndex(int size)
    {
        int log2Ceil = 32 - Integer.numberOfLeadingZeros(size - 1);
        return Math.max(0, log2Ceil - minLog2Ceil);
    }

    /** Convert the index into the subPools array into the corresponding size*/
    private int toSize(int index)
    {
        return 1 << minLog2Ceil + index;
    }

    private boolean reserveMemory(int capacity)
    {
        while (true)
        {
            long cur = reservedMemory.get();
            if (cur + capacity > maxPoolSize)
            {
                noSpamLogger.info("Maximum memory usage reached ({}), cannot reserve size of {}", maxPoolSize, capacity);
                return false;
            }

            if (reservedMemory.compareAndSet(cur, cur + capacity))
                return true;
        }
    }

    long reservedMemory()
    {
        return reservedMemory.get();
    }

    @VisibleForTesting
    int numSlabs(int size)
    {
        int index = toIndex(size);
        return subPools[index].numFreeSlabs();
    }

    @VisibleForTesting
    void cleanup()
    {
        for (SubPool subPool : subPools)
            subPool.cleanup();
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("Size-tiered from ")
           .append(FBUtilities.prettyPrintMemory(minBufferSize))
           .append(" to ")
           .append(FBUtilities.prettyPrintMemory(maxBufferSize))
           .append(" buffers, using ")
           .append(numBuffersPerSlab)
           .append(" buffers per slab.")
           .append(System.lineSeparator())
           .append("Sub pools:");

        for (int i = 0; i < subPools.length; i++)
        {
            long numSlabs = subPools[i].numSlabs();

            str.append(System.lineSeparator())
               .append("Buffer size ")
               .append(FBUtilities.prettyPrintMemory(toSize(i)))
               .append(": ")
               .append(FBUtilities.prettyPrintMemory(subPools[i].memoryInUse.longValue()))
               .append(" used, ")
               .append(FBUtilities.prettyPrintMemory(subPools[i].memoryAllocated.longValue()))
               .append(" allocated, ")
               .append(numSlabs)
               .append(" slabs.");
        }
        return str.toString();
    }

    @ThreadSafe
    private class SubPool
    {
        final int bufferSize;
        final int maxFreeMemory;
        final ConcurrentLinkedQueue<MemorySlabWithBitmap> free;
        final ConcurrentSkipListMap<Long, MemorySlabWithBitmap> slabs;
        final ConcurrentMap<Long, ThreadLocalStash> threadLocalStashes;
        final AtomicReference<MemorySlabWithBitmap> current;
        final DseThreadLocal<ThreadLocalStash> threadLocalStash;
        final LongAdder memoryAllocated;
        final LongAdder memoryInUse;
        final AtomicLong lastCleanupTime;

        SubPool(int bufferSize)
        {
            Preconditions.checkArgument(((long)bufferSize * (long)numBuffersPerSlab) == bufferSize * numBuffersPerSlab,
                                        "bufferSize x numBuffersPerSlab should not overflow a max int size: %s x %s",
                                        bufferSize, numBuffersPerSlab);

            this.bufferSize = bufferSize;
            this.maxFreeMemory = BufferPool.MAX_NUM_FREE_BUFFERS * bufferSize;
            this.free = new ConcurrentLinkedQueue<>();
            this.slabs = new ConcurrentSkipListMap<>();
            this.threadLocalStashes = new ConcurrentHashMap<>();
            this.current = new AtomicReference<>(null);
            this.threadLocalStash = ThreadLocals.withInitial(this::createThreadLocalStash);
            this.memoryAllocated = new LongAdder();
            this.memoryInUse = new LongAdder();
            this.lastCleanupTime = new AtomicLong(-1L);
        }

        ThreadLocalStash createThreadLocalStash()
        {
            // InlinedThreadLocalThread will call close() to cleanup on exit, other threads will not
            Thread thread = Thread.currentThread();
            //assert thread instanceof TPCThread : "Only TPC threads should use thread local";
            return new ThreadLocalStash(thread, this, BufferPool.MAX_THREAD_LOCAL_BUFFERS);

        }

        /**
         * Allocate new memory regions (a region is a slice in a direct byte buffer slab). If the memory is
         * available on the current slab, then this is all that is required. Otherwise the slab must be switched
         * with a new slab.
         *
         * @param size the size of the memory regions to be allocated
         * @param numRegions the number of memory regions to be allocated
         *
         * @return the address of the memory regions of size {@code size} that were allocated
         */
        long[] allocate(final int size, final int numRegions)
        {
            assert size <= this.bufferSize : "Expected size to be the same or smaller than sub-pool size";

            int coreId = TPCUtils.getCoreId();
            long[] ret = new long[numRegions];
            MemorySlabWithBitmap current = this.current.get();

            // First try to allocate from the current slab
            int allocated = allocateFromCurrent(current, ret, 0, coreId);
            if (allocated == ret.length)
                return increaseMemUsed(ret, size);

            do
            {
                // The slab must be replaced, either we replace it or wait for a replacement
                if (current != MEMORY_SLAB_SENTINEL && this.current.compareAndSet(current, MEMORY_SLAB_SENTINEL))
                {
                    if (current != null)
                        current.setOffline();

                    current = getNextSlab();
                    this.current.set(current); // publish to other threads, even if null
                }
                else
                {
                    current = this.current.get();

                    long start = approxTime.now();
                    long now = start;
                    while (current == MEMORY_SLAB_SENTINEL && (now - start <= CONTENTION_WAIT_TIME_MILLIS))
                    {
                        // wait for next slab, make non-TPC threads yield immediately and TPC threads yield after 100 millis
                        if (!TPCUtils.isValidCoreId(coreId) || (now - start >= 100))
                            Thread.yield();

                        now = approxTime.now();
                        current = this.current.get();
                    }
                }

                if (current == null || current == MEMORY_SLAB_SENTINEL)
                    break; // OOM or Failed to wait

                allocated += allocateFromCurrent(current, ret, allocated, coreId);

            } while (allocated < ret.length);

            return increaseMemUsed(ret, size);
        }

        /**
         * Allocate memory regions from the thread local stash, if available, and if this fails then from the current slab,
         * if the slab is valid (not a sentinel and not null).
         * <p/>
         *
         * @param current - the current slab
         * @param addresses - the array of addresses where to store the allocated region addresses
         * @param alreadyAllocated - the number of addresses already allocated in the addresses array
         * @param coreId - the thread core id, needed to determine if we can rely on the thread local stash
         *
         * @return the number of regions allocated
         */
        private int allocateFromCurrent(MemorySlabWithBitmap current, final long[] addresses, final int alreadyAllocated, final int coreId)
        {
            // First try the TL stash
            boolean hasThreadLocal = TPCUtils.isValidCoreId(coreId);
            int allocated = alreadyAllocated;
            if (hasThreadLocal)
                allocated += maybeTakeFromLocalStash(addresses, allocated);

            // Then try the current slab if it is a valid slab
            if (current == null || current == MEMORY_SLAB_SENTINEL)
                return allocated - alreadyAllocated;

            while (allocated < addresses.length)
            {
                int newlyAllocated = 0;
                if (hasThreadLocal)
                {
                    // TPC threads
                    if (replenish(current))
                        newlyAllocated = maybeTakeFromLocalStash(addresses, allocated);
                }
                else
                {
                    // Other threads
                    newlyAllocated = current.allocateMemory(addresses, allocated, coreId);
                }

                if (newlyAllocated == 0)
                    break;

                allocated += newlyAllocated;
            }

            return allocated - alreadyAllocated;
        }
		
        /**
         * Increment memory used for the allocated regions.
         */
        private long[] increaseMemUsed(long[] ret, int size)
        {
            int totSize = 0;
            for (int i = 0; i < ret.length; i++)
            {
                if (ret[i] > 0)
                    totSize += size;
                else
                    ret[i] = -1; // this signals an alloc failure
            }

            memoryInUse.add(totSize);
            return ret;
        }

        private int maybeTakeFromLocalStash(final long[] ret, final int idx)
        {
            return threadLocalStash.get().take(ret, idx);
        }

        private boolean replenish(MemorySlabWithBitmap current)
        {
            return threadLocalStash.get().replenish(current);
        }

        @Nullable
        private MemorySlabWithBitmap getNextSlab()
        {
            MemorySlabWithBitmap ret = free.poll();
            if (ret == null)
                ret = allocateSlab();

            return ret;
        }

        void release(int size, long[] addresses)
        {
            int released = release(addresses);
            memoryInUse.add(-(size * released));
        }

        int release(long[] addresses)
        {
            Map.Entry<Long, MemorySlabWithBitmap> entry = null;
            int released = 0;

            for (int i = 0; i < addresses.length; i++)
            {
                if (addresses[i] == -1)
                    continue;

                if (entry == null || !entry.getValue().hasAddress(addresses[i]))
                {
                    entry = slabs.floorEntry(addresses[i]);
                    if (entry == null || !entry.getValue().hasAddress(addresses[i]))
                    {
                        noSpamLogger.error("Failed to release address {}", addresses[i]);
                        continue;
                    }
                }

                MemorySlabWithBitmap memorySlab = entry.getValue();
                memorySlab.release(addresses[i]);

                addresses[i] = -1;
                released++;

                MemorySlabWithBitmap.Status status = memorySlab.status();
                if (status == MemorySlabWithBitmap.Status.OFFLINE && memorySlab.setOnline())
                    free.offer(memorySlab);
            }

            return released;
        }

        long usedMemoryBytes()
        {
            return memoryInUse.longValue();
        }

        long allocatedMemoryBytes()
        {
            return memoryAllocated.longValue();
        }

        /**
         *  Try releasing unused memory.
         *
         *  Normally a sub-pool will always have some memory in use because the chunk cache will have some content.
         *  However, if a buffer size is completely discarded, e.g. the only table using a buffer size has been dropped
         *  or altered, we'd like to detect this. If the sub-pool no longer has any memory in use, we assume this buffer
         *  size is no longer in use and recall the thread-local stashes before attempting to release any slabs that are
         *  completely free.
         *
         *  If we have more than maxFreeMemory already available, we attempt to release any slabs that are completely free.
         *  The reasoning in this case is that we may fail to release slabs due to fragmentation but, if the sub-pool is
         *  winding down due to a change in buffer size, we will succeed with the next attempts whilst if the sub-pool
         *  is still actively used, then the amount of unused memory should be below the threshold and so this condition
         *  should not be triggered too frequently.
         *
         *  It's not safe to free the current slab due to a possible race with allocating threads, so a sub-pool no longer
         *  in use will waste at least one slab. I didn't think it worth it increasing the complexity to release a single slab
         *  since it's not much memory and the subpool may later on become used again.
         */
        void cleanup()
        {
            long totUsed = memoryInUse.longValue();
            long totMemory = memoryAllocated.longValue();

            if (totMemory == 0)
                return; // no memory allocated

            if (totUsed == 0)
            {
                recallThreadLocalStashes();
                releaseFreeSlabs();
            }
            else if (totMemory - totUsed >= maxFreeMemory)
            {
                releaseFreeSlabs();
            }
        }

        /**
         * Recall all the thread local stashes for this sub-pool. Currently only TPC threads can have thread local
         * stashes and so the core id of the stash will always be valid.
         * <p/>
         * Because the thread local stash is not thread safe, {@link ThreadLocalStash#drain()} needs to execute on the actual TPC core
         * and this method must wait until all thread local stashes have been drained. This method should only be called from
         * the cleanup executor and so it should not be already on the TPC thread, and this is checked by the use of
         * blockingAwait(CompletableFuture).
         * */
        private void recallThreadLocalStashes()
        {
            Collection<ThreadLocalStash> stashes = threadLocalStashes.values();
            CompletableFuture<Void>[] futures = new CompletableFuture[stashes.size()];

            int i = 0;
            for (ThreadLocalStash stash : stashes)
                futures[i++] = CompletableFuture.runAsync(stash::drain);

            TPCUtils.blockingAwait(CompletableFuture.allOf(futures));
        }

        private void releaseFreeSlabs()
        {
            boolean done = false;
            MemorySlabWithBitmap memorySlab;
            MemorySlabWithBitmap first = null;

            while(!done && (memorySlab = free.poll()) != null)
            {
                if (first == null)
                    first = memorySlab;
                else if (first == memorySlab)
                    done = true;

                if (memorySlab.isFree())
                {
                    releaseSlab(memorySlab);
                    if (memorySlab == first)
                        first = null;
                }
                else
                {
                    free.offer(memorySlab);
                }
            }
        }

        int numFreeSlabs()
        {
            MemorySlabWithBitmap memorySlab = current.get();
            return free.size() + (memorySlab != null && memorySlab != MEMORY_SLAB_SENTINEL ? 1 : 0);
        }

        int numSlabs()
        {
            return Math.toIntExact(memoryAllocated.longValue() / capacity());
        }

        private int capacity()
        {
            return bufferSize * numBuffersPerSlab;
        }

        /**
         * Allocate a slab if memory can be reserved
         *
         * @return a newly allocated slab or null
         */
        private @Nullable MemorySlabWithBitmap allocateSlab()
        {
            int capacity = capacity();
            try
            {
                if (!reserveMemory(capacity))
                    return null;

                memoryAllocated.add(capacity);
                MemorySlabWithBitmap ret = new MemorySlabWithBitmap(bufferSize, capacity);
                if (slabs.putIfAbsent(ret.baseAddress(), ret) != null)
                { // There can't be an existing slab with the same base address
                    ret.close();
                    throw new IllegalStateException("Found slab with same base address when allocating a new slab");
                }
                return ret;
            }
            catch (Throwable t)
            {
                reservedMemory.addAndGet(-capacity);
                memoryAllocated.add(-capacity);
                throw t;
            }
        }

        private void releaseSlab(MemorySlabWithBitmap memorySlab)
        {
            long capacity = memorySlab.capacity();
            noSpamLogger.debug("Releasing free slab of {}.", FBUtilities.prettyPrintMemory(capacity));

            // the slab must be removed before calling close() because once the memory is released, another slab
            // could be created with the same memory and inserted into the map using the same base address as the key
            MemorySlabWithBitmap removed = slabs.remove(memorySlab.baseAddress());
            boolean closed = memorySlab.close();
            if (closed)
            {
                reservedMemory.addAndGet(-capacity);
                memoryAllocated.add(-capacity);
            }

            assert removed == memorySlab && closed :
                   String.format("Problem releasing memory slab memory: memorySlab=%s, slab removed=%s, closed=%b",
                                 memorySlab, removed, closed);
        }
    }

    /**
     * A stash of thread-local buffers.
     * <p/>
     * Threads that inherit from {@link InlinedThreadLocalThread} will stash a few buffers,
     * in order to reduce contention.
     */
    @NotThreadSafe
    private static class ThreadLocalStash implements Closeable
    {
        final long threadId;
        final SubPool parent;
        final int maxNumAddresses;
        final int coreId;

        volatile Addresses addresses;

        ThreadLocalStash(Thread thread, SubPool parent, int maxNumAddresses)
        {
            this.threadId = thread.getId();
            this.parent = parent;
            this.addresses = new Addresses(maxNumAddresses);
            this.maxNumAddresses = maxNumAddresses;
            this.coreId = TPCUtils.getCoreId(thread);
            parent.threadLocalStashes.putIfAbsent(threadId, this);
        }

        int take(final long[] ret, final int idx)
        {
            return addresses.take(ret, idx);
        }

        boolean replenish(MemorySlabWithBitmap memorySlab)
        {
            return this.addresses.allocate(memorySlab, coreId);
        }

        void drain()
        {
            if (addresses.isEmpty())
                return;

            long[] toFree = new long[addresses.available()];
            addresses.take(toFree, 0);
            assert addresses.isEmpty() : "Expected addresses to be empty";

            int released = parent.release(toFree);
            assert released == toFree.length : "Failed to release some buffers when draining thread local stash";
        }

        @Override
        public void close() throws IOException
        {
            drain();
            parent.threadLocalStashes.remove(threadId);
        }

        /**
         * A wrapper around a list of addresses, which are accesses in FIFO order.
         * <p/>
         * We need a wrapper because we want to avoid boxing the long, therefore
         * we don't use any of the existing Java generic collections or queues.
         */
        @NotThreadSafe
        private final static class Addresses
        {
            private final long[] addresses;
            int prod; // producer index
            int cons; // consumer index

            Addresses(int numAddresses)
            {
                this.addresses = new long[numAddresses];

                reset();
            }

            private void reset()
            {
                prod = cons = 0;
            }

            private boolean isEmpty()
            {
                return cons == prod;
            }

            private boolean allocate(MemorySlabWithBitmap slab, int coreId)
            {
                assert isEmpty() : "allocate should have been called on an empty thread local";
                reset();

                prod = slab.allocateMemory(addresses, 0, coreId);
                return prod > 0;
            }

            // Fill ret with as many addresses as possible starting at idx
            private int take(final long[] ret, final int idx)
            {
                int allocated = idx;
                while (cons < prod && allocated < ret.length)
                    ret[allocated++] = addresses[cons++];
                return allocated - idx;
            }

            private int available()
            {
                return prod - cons;
            }
        }
    }
}
