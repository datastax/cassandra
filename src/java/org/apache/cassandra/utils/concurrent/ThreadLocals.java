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
        return new FastThreadLocalLightweightRecycler<>(limit);
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


    public static <T> DseThreadLocal<T> withInitial(Supplier<T> initialValueSupplier)
    {
        Objects.requireNonNull(initialValueSupplier);

        long index = InlinedThreadLocalThread.nextRefIndex();
        if (index != -1)
            return new InlinedThreadLocal<>(index, initialValueSupplier);

        logger.warn("InlinedThreadLocal exhausted, falling back to FastThreadLocal. Please report this message to DataStax support. " +
                    "Note that the system will still work and this does not indicate an error.", new Exception("TL allocation site"));

        return new FastThreadLocalFallback<>(initialValueSupplier);
    }


    /**
     * This implementation of {@link DseThreadLocal} uses a {@link FastThreadLocal} as storage. It is intended as a
     * fallback alternative when the {@link InlinedThreadLocalThread} storage has been exhausted. It forces consistent
     * behaviour with regards to null values.
     */
    private static class FastThreadLocalFallback<V> implements DseThreadLocal<V>
    {
        private final Supplier<V> initialValueSupplier;
        private final FastThreadLocal<V> ftl;

        FastThreadLocalFallback(Supplier<V> initialValueSupplier)
        {
            Objects.requireNonNull(initialValueSupplier);
            this.initialValueSupplier = initialValueSupplier;
            this.ftl = new FastThreadLocal<V>(){
                @Override
                protected V initialValue()
                {
                    if (initialValueSupplier == InlinedThreadLocal.EMPTY_SUPPLIER)
                        return null;
                    V v = initialValueSupplier.get();
                    assert v != null;
                    return v;
                }
            };
        }

        @Override
        public V get()
        {
            return ftl.get();
        }

        @Override
        public void set(V value)
        {
            if (value == null)
                ftl.remove();
            else
                ftl.set(value);
        }

        @Override
        public boolean isSet()
        {
            assert initialValueSupplier == InlinedThreadLocal.EMPTY_SUPPLIER;
            return get() != null;
        }

        @Override
        public void remove()
        {
            set(null);
        }
    }

}
