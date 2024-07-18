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

import java.util.Iterator;

import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.service.CacheService;

/**
 * Wraps an ICache in requests + hits tracking.
 */
public class InstrumentingCache<K, V>
{
    private final ICache<K, V> map;
    private final CacheService.CacheType type;

    private CacheMetrics metrics;

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

    public InstrumentingCache(CacheService.CacheType type, ICache<K, V> map)
    {
        this.map = map;
        this.type = type;
        this.metrics = CacheMetrics.create(type, map);
        this.cacheCapacity = map.capacity();
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return map.putIfAbsent(key, value);
    }

    public boolean replace(K key, V old, V value)
    {
        return map.replace(key, old, value);
    }

    public V get(K key)
    {
        V v = map.get(key);
        if (v != null)
            metrics.recordHits(1);
        else
            metrics.recordMisses(1);
        return v;
    }

    public V getInternal(K key)
    {
        return map.get(key);
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public long getCapacity()
    {
        return cacheCapacity;
    }

    public void setCapacity(long capacity)
    {
        map.setCapacity(capacity);
        cacheCapacity = map.capacity();
    }

    public int size()
    {
        return map.size();
    }

    public long weightedSize()
    {
        return map.weightedSize();
    }

    public void clear()
    {
        map.clear();

        // this does not clear metered metrics which are defined statically. for testing purposes, these can be
        // cleared by CacheMetrics.reset()
        metrics = CacheMetrics.create(type, map);
    }

    public Iterator<K> keyIterator()
    {
        return map.keyIterator();
    }

    public Iterator<K> hotKeyIterator(int n)
    {
        return map.hotKeyIterator(n);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public CacheMetrics getMetrics()
    {
        return metrics;
    }
}
