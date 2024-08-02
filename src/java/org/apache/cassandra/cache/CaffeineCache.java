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
package org.apache.cassandra.cache;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;

import com.google.common.util.concurrent.MoreExecutors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.github.benmanes.caffeine.cache.Weigher;

/**
 * An adapter from a Caffeine cache to the ICache interface. This provides an on-heap cache using
 * the W-TinyLFU eviction policy (http://arxiv.org/pdf/1512.00727.pdf), which has a higher hit rate
 * than an LRU.
 */
public class CaffeineCache<K extends IMeasurableMemory, V extends IMeasurableMemory> implements ICache<K, V>
{
    private final Cache<K, V> cache;
    private final Eviction<K, V> policy;

    /**
     * cache capacity cached for performance reasons
     * </p>
     * The need to cache this value arises from the fact that map.capacity() is an expensive operation for
     * Caffeine caches, which use the cache eviction policy to determine the capacity, which, in turn,
     * may take eviction lock and do cache maintenance before returning the actual value.
     * </p>
     * Cassandra frequently uses cache capacity to determine if a cache is enabled.
     * See e.g:
     * {@link org.apache.cassandra.db.ColumnFamilyStore#putCachedCounter},
     * {@link org.apache.cassandra.db.ColumnFamilyStore#getCachedCounter},
     * </p>
     * In more stressful scenarios with many counter cache puts and gets asking the underlying cache to provide the
     * capacity instead of using the cached value leads to a significant performance degradation.
     * </p>
     * No thread-safety guarantees are provided. The value may be a bit stale, and this is good enough.
     */
    private long cacheCapacity;

    private CaffeineCache(Cache<K, V> cache)
    {
        this.cache = cache;
        this.policy = cache.policy().eviction().orElseThrow(() -> 
            new IllegalArgumentException("Expected a size bounded cache"));
        checkState(policy.isWeighted(), "Expected a weighted cache");
        this.cacheCapacity = capacitySlow();
    }

    /**
     * Initialize a cache with initial capacity with weightedCapacity
     */
    public static <K extends IMeasurableMemory, V extends IMeasurableMemory> CaffeineCache<K, V> create(long weightedCapacity, Weigher<K, V> weigher)
    {
        Cache<K, V> cache = Caffeine.newBuilder()
                .maximumWeight(weightedCapacity)
                .weigher(weigher)
                .executor(MoreExecutors.directExecutor())
                .build();
        return new CaffeineCache<>(cache);
    }

    public static <K extends IMeasurableMemory, V extends IMeasurableMemory> CaffeineCache<K, V> create(long weightedCapacity)
    {
        return create(weightedCapacity, (key, value) -> {
            long size = key.unsharedHeapSize() + value.unsharedHeapSize();
            if (size > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Serialized size cannot be more than 2GB/Integer.MAX_VALUE");
            }
            return (int) size;
        });
    }

    @Override
    public long capacity()
    {
        return cacheCapacity;
    }

    public void setCapacity(long capacity)
    {
        policy.setMaximum(capacity);
        cacheCapacity = capacitySlow();
    }

    public boolean isEmpty()
    {
        return cache.asMap().isEmpty();
    }

    public int size()
    {
        return cache.asMap().size();
    }

    public long weightedSize()
    {
        return policy.weightedSize().getAsLong();
    }

    public void clear()
    {
        cache.invalidateAll();
    }

    public V get(K key)
    {
        return cache.getIfPresent(key);
    }

    public void put(K key, V value)
    {
        cache.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return cache.asMap().putIfAbsent(key, value) == null;
    }

    public boolean replace(K key, V old, V value)
    {
        return cache.asMap().replace(key, old, value);
    }

    public void remove(K key)
    {
        cache.invalidate(key);
    }

    public Iterator<K> keyIterator()
    {
        return cache.asMap().keySet().iterator();
    }

    public Iterator<K> hotKeyIterator(int n)
    {
        return policy.hottest(n).keySet().iterator();
    }

    public boolean containsKey(K key)
    {
        return cache.asMap().containsKey(key);
    }

    /**
     * This method is used to get the capacity of the cache. It is a slow method because it may take the eviction lock
     * and perform cache maintenance before returning the actual value.
     * @return the capacity of the cache as determined by the eviction policy
     */
    private long capacitySlow()
    {
        return policy.getMaximum();
    }
}
