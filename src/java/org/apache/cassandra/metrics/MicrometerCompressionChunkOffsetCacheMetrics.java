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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.cassandra.io.compress.CompressionChunkOffsetCache;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Micrometer implementation for compression chunk offsets cache metrics: the standard cache metrics extended with
 * the load and eviction meters required by caffeine's {@link StatsCounter}.
 */
public class MicrometerCompressionChunkOffsetCacheMetrics extends MicrometerCacheMetrics implements StatsCounter
{
    private final String metricsPrefix;
    private final ConcurrentHashMap<RemovalCause, Counter> evictionByRemovalCause = new ConcurrentHashMap<>();

    private volatile Timer missLatency;
    private volatile Counter evictions;

    public MicrometerCompressionChunkOffsetCacheMetrics(CompressionChunkOffsetCache cache, String metricsPrefix)
    {
        super(metricsPrefix, cache);
        this.metricsPrefix = metricsPrefix;
        registerStatsMeters();
    }

    private void registerStatsMeters()
    {
        this.missLatency = timer(metricsPrefix + "_miss_latency_seconds");
        this.evictions = counter(metricsPrefix + "_evictions");

        for (RemovalCause cause : RemovalCause.values())
            evictionByRemovalCause.put(cause, counter(evictionMeterName(cause)));
    }

    private String evictionMeterName(RemovalCause cause)
    {
        return metricsPrefix + "_evictions_" + cause.toString().toLowerCase();
    }

    /**
     * CNDB calls this after construction to replace the registry and tags, so the stats meters have to be recreated
     * on the new registry, like the cache meters of the parent class.
     */
    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags);
        registerStatsMeters();
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

    public double missLatency()
    {
        return missLatency.mean(TimeUnit.NANOSECONDS);
    }

    @Override
    public CacheStats snapshot()
    {
        return CacheStats.of(hits(), misses(), missLatency.count(),
                             0L, (long) missLatency.totalTime(TimeUnit.NANOSECONDS), (long) evictions.count(), 0L);
    }

    @Override
    public String toString()
    {
        return "Compression chunk offsets cache metrics: " + System.lineSeparator() +
               "Miss latency in seconds: " + missLatency() + System.lineSeparator() +
               "Misses count: " + misses() + System.lineSeparator() +
               "Hits count: " + hits() + System.lineSeparator() +
               "Cache requests count: " + requests() + System.lineSeparator() +
               "Moving hit rate: " + hitRate() + System.lineSeparator() +
               "Num entries: " + entries() + System.lineSeparator() +
               "Size in memory: " + FBUtilities.prettyPrintMemory(size()) + System.lineSeparator() +
               "Capacity: " + FBUtilities.prettyPrintMemory(capacity());
    }
}
