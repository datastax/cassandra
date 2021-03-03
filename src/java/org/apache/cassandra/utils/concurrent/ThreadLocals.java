/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils.concurrent;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;

public final class ThreadLocals
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadLocals.class);

    private ThreadLocals()
    {
    }

    public static <T> LightweightRecycler<T> createLightweightRecycler(int limit)
    {
        long index = InlinedThreadLocalThread.nextRefIndex();
        if (index != -1)
            return new InlineThreadLocalLightweightRecycler(index, limit);

        logger.warn("InlinedThreadLocal exhausted, falling back to FastThreadLocal. Please report this message to DataStax support. " +
                    "Note that the system will still work and this does not indicate an error.", new Exception("TL allocation site"));

        return new FastThreadLocalLightweightRecycler(limit);
    }

    /**
     * A {@link LightweightRecycler} which is backed by a {@link FastThreadLocal}.
     */
    private static final class FastThreadLocalLightweightRecycler<T> extends FastThreadLocal<ArrayDeque<T>> implements LightweightRecycler<T>
    {
        private final int capacity;

        public FastThreadLocalLightweightRecycler(int capacity)
        {
            super();
            this.capacity = capacity;
        }

        protected ArrayDeque<T> initialValue()
        {
            return new ArrayDeque<>(capacity);
        }

        /**
         * @return maximum capacity of the recycler
         */
        public int capacity()
        {
            return capacity;
        }
    }
}
