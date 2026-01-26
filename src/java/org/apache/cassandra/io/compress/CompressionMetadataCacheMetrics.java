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
package org.apache.cassandra.io.compress;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

/**
 * Metrics for CompressionMetadataCache that implements Caffeine's StatsCounter interface.
 * Tracks hits, misses, load times, and evictions for the compression metadata cache.
 */
public class CompressionMetadataCacheMetrics implements StatsCounter
{
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong loadSuccessCount = new AtomicLong();
    private final AtomicLong loadFailureCount = new AtomicLong();
    private final AtomicLong totalLoadTime = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();
    private final AtomicLong evictionWeight = new AtomicLong();

    @Override
    public void recordHits(int count)
    {
        hitCount.addAndGet(count);
    }

    @Override
    public void recordMisses(int count)
    {
        missCount.addAndGet(count);
    }

    @Override
    public void recordLoadSuccess(long loadTime)
    {
        loadSuccessCount.incrementAndGet();
        totalLoadTime.addAndGet(loadTime);
    }

    @Override
    public void recordLoadFailure(long loadTime)
    {
        loadFailureCount.incrementAndGet();
        totalLoadTime.addAndGet(loadTime);
    }

    @Override
    public void recordEviction(int weight, RemovalCause cause)
    {
        if (cause.wasEvicted())
        {
            evictionCount.incrementAndGet();
            evictionWeight.addAndGet(weight);
        }
    }

    @Override
    public CacheStats snapshot()
    {
        return CacheStats.of(
            hitCount.get(),
            missCount.get(),
            loadSuccessCount.get(),
            loadFailureCount.get(),
            totalLoadTime.get(),
            evictionCount.get(),
            evictionWeight.get()
        );
    }

    /**
     * @return total number of cache hits
     */
    public long hits()
    {
        return hitCount.get();
    }

    /**
     * @return total number of cache misses
     */
    public long misses()
    {
        return missCount.get();
    }

    /**
     * @return total number of requests (hits + misses)
     */
    public long requests()
    {
        return hitCount.get() + missCount.get();
    }

    /**
     * @return cache hit rate (0.0 to 1.0)
     */
    public double hitRate()
    {
        long requests = requests();
        return requests == 0 ? 1.0 : (double) hitCount.get() / requests;
    }

    /**
     * @return total number of successful loads
     */
    public long loadSuccesses()
    {
        return loadSuccessCount.get();
    }

    /**
     * @return total number of failed loads
     */
    public long loadFailures()
    {
        return loadFailureCount.get();
    }

    /**
     * @return average load time in nanoseconds
     */
    public double averageLoadPenalty()
    {
        long totalLoads = loadSuccessCount.get() + loadFailureCount.get();
        return totalLoads == 0 ? 0.0 : (double) totalLoadTime.get() / totalLoads;
    }

    /**
     * @return average load time in milliseconds
     */
    public double averageLoadPenaltyMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis((long) averageLoadPenalty());
    }

    /**
     * @return total number of evictions
     */
    public long evictions()
    {
        return evictionCount.get();
    }

    /**
     * @return total weight of evicted entries (bytes)
     */
    public long evictedWeight()
    {
        return evictionWeight.get();
    }

    /**
     * Reset all metrics to zero. Used for testing.
     */
    public void reset()
    {
        hitCount.set(0);
        missCount.set(0);
        loadSuccessCount.set(0);
        loadFailureCount.set(0);
        totalLoadTime.set(0);
        evictionCount.set(0);
        evictionWeight.set(0);
    }

    @Override
    public String toString()
    {
        CacheStats stats = snapshot();
        return String.format(
            "CompressionMetadataCache metrics: hits=%d, misses=%d, hitRate=%.2f%%, " +
            "loadSuccesses=%d, loadFailures=%d, avgLoadTime=%.2fms, " +
            "evictions=%d, evictedWeight=%d bytes",
            stats.hitCount(),
            stats.missCount(),
            stats.hitRate() * 100,
            stats.loadSuccessCount(),
            stats.loadFailureCount(),
            TimeUnit.NANOSECONDS.toMillis((long) stats.averageLoadPenalty()),
            stats.evictionCount(),
            stats.evictionWeight()
        );
    }
}
