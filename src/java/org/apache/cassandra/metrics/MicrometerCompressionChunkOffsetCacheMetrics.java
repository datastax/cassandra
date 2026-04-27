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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.io.compress.CompressionChunkOffsetCache;

/**
 * Micrometer implementation for compression chunk offsets cache metrics.
 */
public class MicrometerCompressionChunkOffsetCacheMetrics extends MicrometerMetrics implements StatsCounter, CacheMetrics
{
    private final CacheSize cache;
    private final String metricsPrefix;

    private volatile MicrometerCacheMetrics metrics;
    private volatile Timer missLatency;
    private volatile Counter evictions;
    private final ConcurrentHashMap<RemovalCause, Counter> evictionByRemovalCause = new ConcurrentHashMap<>();

    public MicrometerCompressionChunkOffsetCacheMetrics(CompressionChunkOffsetCache cache, String metricsPrefix)
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
            evictionByRemovalCause.put(cause, counter(metricsPrefix + "_evictions_" + cause.toString().toLowerCase()));
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
    public void recordEviction(int weight, RemovalCause removalCause)
    {
        if (removalCause.wasEvicted())
            evictions.increment(1);

        Counter counter = evictionByRemovalCause.get(removalCause);
        if (counter != null)
            counter.increment(1);
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
    public long capacity()
    {
        return metrics.capacity();
    }

    @Override
    public long size()
    {
        return metrics.size();
    }

    @Override
    public long entries()
    {
        return metrics.entries();
    }

    @Override
    public long hits()
    {
        return metrics.hits();
    }

    @Override
    public long misses()
    {
        return metrics.misses();
    }

    public double missLatency()
    {
        return missLatency.mean(TimeUnit.NANOSECONDS);
    }

    @Override
    public CacheStats snapshot()
    {
        return CacheStats.of(metrics.hits(), metrics.misses(), missLatency.count(),
                0L, (long) missLatency.totalTime(TimeUnit.NANOSECONDS), (long) evictions.count(), 0L);
    }

    @Override
    public String toString()
    {
        return metrics.toString();
    }
}
