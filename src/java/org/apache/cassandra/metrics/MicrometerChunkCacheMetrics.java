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
package org.apache.cassandra.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Micrometer implementation for the chunk cache metrics.
 */
public class MicrometerChunkCacheMetrics extends MicrometerMetrics implements ChunkCacheMetrics
{
    private final CacheSize cache;
    private final String metricsPrefix;

    private volatile MicrometerCacheMetrics metrics;
    private volatile Timer missLatency;
    private volatile Counter evictions;
    private final ConcurrentHashMap<RemovalCause, Counter> evitictionByRemovalCause = new ConcurrentHashMap<>();

    public MicrometerChunkCacheMetrics(CacheSize cache, String metricsPrefix)
    {
        this.cache = cache;
        this.metricsPrefix = metricsPrefix;

        registerMetrics(registryWithTags().left, registryWithTags().right);
    }

    private void registerMetrics(MeterRegistry registry, Tags tags)
    {
        this.metrics = new MicrometerCacheMetrics(metricsPrefix, cache);
        this.metrics.register(registry, tags);

        this.missLatency = timer(metricsPrefix + "_miss_latency_seconds");
        this.evictions = counter(metricsPrefix + "_evictions");

        for (RemovalCause cause : RemovalCause.values())
        {
            evitictionByRemovalCause.put(cause, counter(metricsPrefix + "_evictions_" + cause.toString().toLowerCase()));
        }
    }

    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags);
        registerMetrics(newRegistry, newTags);
    }

    @Override
    public void recordMisses(int count)
    {
        metrics.recordMisses(count);
    }

    @Override
    public void recordLoadSuccess(long val)
    {
        missLatency.record(val, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoadFailure(long val)
    {
    }

    @Override
    public void recordEviction(int weight, RemovalCause removalCause) {
        if (removalCause.wasEvicted())
        {
            evictions.increment(1);
        }
        Counter counter = evitictionByRemovalCause.get(removalCause);
        if (counter != null) {
            counter.increment(1);
        }
    }

    @Override
    public void recordHits(int count)
    {
        metrics.recordHits(count);
    }

    @Override
    public double hitRate()
    {
        return metrics.hitRate();
    }

    @Override
    public double hitOneMinuteRate()
    {
        return metrics.hitOneMinuteRate();
    }

    @Override
    public double hitFiveMinuteRate()
    {
        return metrics.hitFiveMinuteRate();
    }

    @Override
    public double hitFifteenMinuteRate()
    {
        return metrics.hitFifteenMinuteRate();
    }

    @Override
    public double requestsFifteenMinuteRate()
    {
        return metrics.requestsFifteenMinuteRate();
    }

    @Override
    public long requests()
    {
        return metrics.requests();
    }

    @Override
    public long misses()
    {
        return metrics.misses();
    }

    @Override
    public long hits()
    {
        return metrics.hits();
    }

    @Override
    public double missLatency()
    {
        return missLatency.mean(TimeUnit.NANOSECONDS);
    }

    @Override
    public long capacity()
    {
        return metrics.capacity();
    }

    @Override
    public long size()
    {
        return metrics.size();
    }

    public long entries()
    {
        return metrics.entries();
    }

    @Override
    @VisibleForTesting
    public void reset()
    {
        // This method is only used for unit tests, and unit tests only use the codahale implementation
        throw new UnsupportedOperationException("This was not expected to be called and should be implemented if required");
    }

    @Nonnull
    @Override
    public CacheStats snapshot()
    {
        return CacheStats.of(metrics.hits(), metrics.misses(), missLatency.count(),
                0L, (long) missLatency.totalTime(TimeUnit.NANOSECONDS), (long) evictions.count(), 0L);
    }

    @Override
    public String toString()
    {
        return "Chunk cache metrics: " + System.lineSeparator() +
               "Miss latency in seconds: " + missLatency() + System.lineSeparator() +
               "Misses count: " + misses() + System.lineSeparator() +
               "Hits count: " + hits() + System.lineSeparator() +
               "Cache requests count: " + requests() + System.lineSeparator() +
               "Moving hit rate: " + hitRate() + System.lineSeparator() +
               "Num entries: " + entries() + System.lineSeparator() +
               "Size in memory: " + FBUtilities.prettyPrintMemory(size()) + System.lineSeparator() +
               "Capacity: " + FBUtilities.prettyPrintMemory(capacity());
    }

    public Map<RemovalCause, Double> getEvictionCountByRemovalCause()
    {
        Map<RemovalCause, Double> result = new HashMap<>();
        for (Map.Entry<RemovalCause, Counter> entry : evitictionByRemovalCause.entrySet())
        {
            result.put(entry.getKey(), entry.getValue().count());
        }
        return result;
    }
}
