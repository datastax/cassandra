/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.concurrent;

import static org.apache.cassandra.utils.UnsafeAccess.UNSAFE;

/**
 * A class equivalent to {@link java.util.concurrent.atomic.LongAdder} except that
 * it creates a Cell per core-thread as soon as needed, and a further Cell to be
 * shared by every other thread. The dedicated cells are updated with an ordered put,
 * aka as lazy set, rather than a full volatile write, whilst the shared Cell is
 * updated with CAS.
 *
 * The ordered put guarantees atomicity, just like a volatile write, but not
 * total ordering. So we may miss the latest value when reading, but we can
 * tolerate this (the reading thread will see the value in a matter of nanoseconds).
 */
public class LongAdder
{
    // to prevent false sharing we need to pad by 2 cache lines, typically 64b per cache line.
    private static final int FALSE_SHARING_PAD = 128;
    private static final int COUNTER_OFFSET_SCALE = Integer.numberOfTrailingZeros(FALSE_SHARING_PAD);
    private static final int SIZEOF_LONG = 8;
    private static final long COUNTER_ARRAY_BASE;

    static
    {
        try
        {
            // first counter offset in array, padded to the 'left'
            COUNTER_ARRAY_BASE = UNSAFE.arrayBaseOffset(long[].class) + FALSE_SHARING_PAD;
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private final int numCores;
    private final long[] values;

    public LongAdder()
    {
        this.numCores = TPCUtils.getNumCores();
        // each counter is padded to the right by 2 cache lines(128b) and we need one extra pad on the left
        this.values = new long[(numCores + 2)*(FALSE_SHARING_PAD / SIZEOF_LONG)];
    }

    public void add(long x)
    {
        int coreId = TPCUtils.getCoreId();

        add(x, coreId);
    }

    public void add(long x, int coreId)
    {
        if (x == 0)
            return;

        // only the last index requires a cas, for the rest an ordered put is sufficient since only one thread will
        // update the value.
        long offset = counterOffset(coreId);
        if (coreId < numCores)
        {
            // This will not work with concurrent reset
            long value = UNSAFE.getLong(values, offset);
            UNSAFE.putOrderedLong(values, offset, value + x);
        }
        else
        {
            UNSAFE.getAndAddLong(values, offset, x);
        }
    }

    private long counterOffset(int coreId)
    {
        return COUNTER_ARRAY_BASE + (coreId << COUNTER_OFFSET_SCALE);
    }

    public void increment() {
        add(1L);
    }

    public void decrement() {
        add(-1L);
    }

    public long sum()
    {
        long sum = 0;
        long[] values = this.values;
        for (int i = 0; i <= numCores;i++)
            sum += UNSAFE.getLongVolatile(values, counterOffset(i));

        return sum;
    }

    public void reset()
    {
        long[] values = this.values;
        for (int i = 0; i <= numCores;i++)
            UNSAFE.getAndSetLong(values, counterOffset(i), 0);
    }

    public String toString() {
        // a breakpoint in the constructor before initializing values will cause a SEGFAULT because the debugger calls toString()
        return values == null ? "0" : Long.toString(sum());
    }

    /**
     * Equivalent to {@link #sum}.
     *
     * @return the sum
     */
    public long longValue() {
        return sum();
    }

    /**
     * Returns the {@link #sum} as an {@code int} after a narrowing
     * primitive conversion.
     */
    public int intValue() {
        return (int)sum();
    }

    /**
     * Returns the {@link #sum} as a {@code float}
     * after a widening primitive conversion.
     */
    public float floatValue() {
        return (float)sum();
    }

    /**
     * Returns the {@link #sum} as a {@code double} after a widening
     * primitive conversion.
     */
    public double doubleValue() {
        return (double)sum();
    }
}
