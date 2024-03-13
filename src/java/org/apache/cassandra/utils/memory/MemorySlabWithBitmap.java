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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;

import static org.apache.cassandra.utils.UnsafeAccess.UNSAFE;

/**
 * A memory slab with a bitmap: it allocates a large buffer (the slab) and it slices it
 * into smaller buffers when requested. The only supported slice size is the bufferSize,
 * where bufferSize is specified in the constructor. The buffer size must be a power of two
 * and the slab size, or the capacity of the slab, should be a multiple of the buffer size
 * and a power of 2.
 *
 * Each bufferSize is allocated to a bit in an integer.
 * A bit is set to 1 to indicate the corresponding buffer is available, and to 0 to indicate that it is in use.
 * Multiple integers are appended into an array, the buffers allocations bitmap, to produce a bitmap that covers
 * the entire slab.
 *
 * When multiple buffers are requested, the pool will try to use consecutive addresses, if they are available.
 *
 * When a buffer is released, the pool works out the position in the bitmap by comparing the buffer
 * address to its base address, and it releases the corresponding bit in the bitmap.
 */
@ThreadSafe // except for closing, only a single thread should own a slab that is about to be closed
final class MemorySlabWithBitmap
{
    private static final long BITMAP_ARRAY_BASE;
    private static final int SHIFT;
    private static final int NUM_BUFFERS_PER_SINGLE_BITMAP;
    private static final int SINGLE_BITMAP_SHIFT;

    /**
     * A state machine for removing slabs that are full from the owning pool, and returning them to the pool when
     * they have space. It is also used for releasing slabs when they are completely free and no longer required.
     * <p/>
     * Slabs that are serving requests are {@link Status#ONLINE}. When they become full, the thread
     * that successfully removes them from the pool, will also set the status as {@link Status#OFFLINE}.
     * When a buffer is returned to the slab, and the slab has therefore regained some space, if the slab is
     * {@link Status#OFFLINE}, the thread that successfully sets the status as {@link Status#ONLINE}, will also add the
     * slab back to pool. Because the slab is set as @link Status#OFFLINE} before the buffer is returned to the user,
     * we're guaranteed that there will be at least one buffer returned with the slab status {@link Status#OFFLINE},
     * and that therefore the slab will be reintegrated into the pool.
     * <p/>
     * During cleanup, if a slab is completely free and taken off the pool, then it can be released. In this case,
     * the cleanup thread will set the status as {@link Status#CLOSED} and the slab memory will also be released.
     * No buffers will be returned from slabs that are {@link Status#CLOSED}. So we are also using the status to
     * prevent allocations after the memory has been released.
     */
    enum Status
    {
        /** The slab is serving requests */
        ONLINE,
        /** The slab is not serving requests (for example because it is full) */
        OFFLINE,
        /** The slab has been permanently released (it's an error to use a slab in this state) */
        CLOSED
    }

    private final ByteBuffer slab;
    private final long baseAddress;
    private final int bufferSize;
    private final int shift;
    private final int[] buffersAllocationBitmap;

    private static final AtomicReferenceFieldUpdater<MemorySlabWithBitmap, Status> statusUpdater =
            AtomicReferenceFieldUpdater.newUpdater(MemorySlabWithBitmap.class, Status.class, "status");
    private volatile Status status;

    static
    {
        try
        {
            BITMAP_ARRAY_BASE = UNSAFE.arrayBaseOffset(int[].class);

            int scale = UNSAFE.arrayIndexScale(int[].class);
            SHIFT = 31 - Integer.numberOfLeadingZeros(scale);

            NUM_BUFFERS_PER_SINGLE_BITMAP = 32; // The number of bits in an integer
            SINGLE_BITMAP_SHIFT = Integer.numberOfTrailingZeros(NUM_BUFFERS_PER_SINGLE_BITMAP); // to avoid a division
        }
        catch (Exception e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    MemorySlabWithBitmap(int bufferSize, int capacity)
    {
        assert bufferSize > 0 && Integer.bitCount(bufferSize) == 1 :
                                    "buffer size must be positive and a power of two: " + bufferSize;
        assert Integer.numberOfTrailingZeros(bufferSize) <= 31 :
                                    "buffer size must be at most 2^31: " + bufferSize;
        assert capacity > bufferSize && Integer.bitCount(capacity) == 1 :
                                    "capacity must be > bufferSize and a power of two:" + capacity;

        this.slab = BufferType.OFF_HEAP_ALIGNED.allocate(capacity);
        this.baseAddress = UnsafeByteBufferAccess.getAddress(slab);
        this.bufferSize = bufferSize;
        this.shift = Integer.numberOfTrailingZeros(bufferSize); // to avoid a division

        int len = capacity >> (shift + SINGLE_BITMAP_SHIFT);
        this.buffersAllocationBitmap = new int[len];
        Arrays.fill(this.buffersAllocationBitmap, -1); // -1 means all free whilst 0 means all in use

        this.status = Status.ONLINE;
    }

    long baseAddress()
    {
        return baseAddress;
    }

    private boolean casBitmap(int current, int update, int index)
    {
        long offset = BITMAP_ARRAY_BASE + (index << SHIFT);
        return UNSAFE.compareAndSwapInt(buffersAllocationBitmap, offset, current, update);
    }

    int capacity()
    {
        return slab.capacity();
    }

    final int bufferSize()
    {
        return bufferSize;
    }

    ByteOrder order()
    {
        return slab.order();
    }

    /**
     * @return true if the slab is completely free, that is all the bits are 1
     *
     * Note that due to concurrent access there is no guarantee that the result returned
     * will be accurate. This method should be used as in indication only, or concurrent
     * access should be prevented.
     */
    final boolean isFree()
    {
        // Because buffersAllocationBitmap is normally quite short, it's probably faster to & all
        // values and only check the final value
        int val = -1;
        for (int bitmap : buffersAllocationBitmap)
            val &= bitmap;

        return val == -1;
    }

    /**
     * @return true if the slab is completely full, that is all the bits are 0
     *
     * Note that due to concurrent access there is no guarantee that the result returned
     * will be accurate. This method should be used as in indication only, or concurrent
     * access should be prevented.
     */
    final boolean isFull()
    {
        // Because buffersAllocationBitmap is normally quite short, it's probably faster to | all
        // values and only check the final value
        int val = 0;
        for (int bitmap : buffersAllocationBitmap)
            val |= bitmap;

        return val == 0;
    }

    /**
     * @return The total free size.
     *
     * Note that due to concurrent access there is no guarantee that the result returned
     * will be accurate. This method should be used as in indication only, or concurrent
     * access should be prevented.
     **/
    int freeSpace()
    {
        int ret = 0;
        for (int bitmap : buffersAllocationBitmap)
            ret += Integer.bitCount(bitmap);

        return ret << shift; // same as ret * bufferSize;
    }

    Status status()
    {
        return status;
    }

    private boolean setStatus(Status current, Status update)
    {
        return statusUpdater.compareAndSet(this, current, update);
    }

    boolean setOnline()
    {
        return setStatus(Status.OFFLINE, Status.ONLINE);
    }

    boolean setOffline()
    {
        return setStatus(Status.ONLINE, Status.OFFLINE);
    }

    boolean close()
    {
        assert isFree() : "Slab must be free before closing it";
        if (setStatus(status(), Status.CLOSED))
        {
            FileUtils.clean(slab);
            return true;
        }

        return false;
    }

    /**
     * Allocate memory addresses of size {@link this#bufferSize} until the addresses array is filled.
     * Return the number of addresses that were added to the array.
     *
     * If possible, addresses are allocated contiguously, in order to perform less work and reduce contention
     * between threads. If this is not possible, then addressed are taken one by one. This is slow but ensures
     * that fragmentation does not prevent allocating memory.
     *
     * @param addresses an array of addresses to fill
     * @param idx the index in the addresses array where to start allocating new regions
     * @param threadId the thread id is used to reduce contention by starting at a different index in {@link this#buffersAllocationBitmap}.
     *                 For TPC threads this can be the TPC core id. If contention is not an issue it can be simply passed in as zero.
     *
     * @return the number of addresses that were allocated from the slab and passed to the array
     */
    int allocateMemory(long[] addresses, int idx, long threadId)
    {
        Status status = status();

        assert threadId >= 0 : "Thread id cannot be negative: " + threadId;
        assert idx < addresses.length : "idx should be less than addresses length: " + idx + " " + addresses.length;
        assert status != Status.CLOSED : "Slab was closed";

        // it's OK to request buffers on OFFLINE slabs due to races, but they are full, so we're wasting our time
        if (status != Status.ONLINE)
            return 0;

        int bitmap = Math.toIntExact((threadId & (buffersAllocationBitmap.length - 1)) - 1); // (threadId % buffersAllocationBitmap.length) -1
        int allocated = idx;

        for (int i = 0; allocated < addresses.length && i < buffersAllocationBitmap.length; i++)
        {
            bitmap++;
            if (bitmap >= buffersAllocationBitmap.length)
                bitmap = 0;

            int reserved = reserveFromBitmap(bitmap, addresses.length - allocated);
            while (reserved != 0)
            {
                // Issue the buffer corresponding to the lowest set bit in reserved
                int index = Integer.numberOfTrailingZeros(reserved);
                int offset = ((bitmap << SINGLE_BITMAP_SHIFT) + index) << shift; // << shift is same as * bufferSize
                addresses[allocated++] = baseAddress + offset;
                reserved ^= 1 << index;
            }
        }

        return allocated - idx;
    }

    /**
     * Tries to reserve howMany bits from the given integer in the bitmap. If fewer than that are available,
     * reserve as many as there are available.
     * <p/>
     * Returns bitset of what we managed to reserve.
     */
    private int reserveFromBitmap(int bitmap, int howMany)
    {
        int current;
        while ((current = buffersAllocationBitmap[bitmap]) != 0)
        {
            int left = current;
            // Clear at most howMany lowest-order bits from the bitmap
            for (int i = howMany; left != 0 && i > 0; --i)
                left &= left - 1;   // strip the lowest-order bit from the number

            // Try to mark these bits as allocated
            if (casBitmap(current, left, bitmap))
                return current ^ left; // Success. The xor returns the bits we managed to clear.

            // Someone beat us to it, try again with the updated bitmap value
        }
        return 0;
    }

    /**
     * Returns true if this address belongs to the slab.
     *
     * @param address the address to check
     *
     * @return true if the address belongs to the slab.
     */
    boolean hasAddress(long address)
    {
        return address >= baseAddress & address < baseAddress + capacity();
    }

    /**
     * Release the memory for this address.
     *
     * @param address the address whose memory is being returned to the slab.
     */
    void release(long address)
    {
        assert hasAddress(address) : "Invalid address: " + address + " - for: " + this.toString();

        int numBitmaps = (int)(address - baseAddress) >> shift;
        int index = numBitmaps >> SINGLE_BITMAP_SHIFT;

        int position = numBitmaps - (index << SINGLE_BITMAP_SHIFT);
        int shiftedBitmapBits = (1 << position);

        while (true)
        {
            int cur = buffersAllocationBitmap[index];
            int next = cur | shiftedBitmapBits;
            assert next == (cur ^ shiftedBitmapBits); // ensure no double free
            if (casBitmap(cur, next, index))
                return;
        }
    }

    @Override
    public String toString()
    {
        return String.format("[%s, capacity %d, free %d]",
                             Arrays.stream(buffersAllocationBitmap)
                                   .mapToObj(i -> String.format("%32s", Integer.toBinaryString(i)).replace(' ', '0'))
                                   .reduce((s1, s2) -> s1.concat(',' + s2))
                                   .orElse(""),
                             capacity(),
                             freeSpace());
    }
}
