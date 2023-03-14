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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class MicrometerNativeMemoryMetrics extends MicrometerMetrics implements NativeMemoryMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(MicrometerNativeMemoryMetrics.class);

    private static final String METRICS_PREFIX = "jvm_native_memory";

    public static final String RAW_NATIVE_MEMORY = METRICS_PREFIX + "_raw_native_memory";
    public static final String BLOOM_FILTER_MEMORY = METRICS_PREFIX + "_bloom_filter_memory";
    public static final String NETWORK_DIRECT_MEMORY = METRICS_PREFIX + "_network_direct_memory";
    public static final String USED_NIO_DIRECT_MEMORY = METRICS_PREFIX + "_used_nio_direct_memory";
    public static final String TOTAL_NIO_MEMORY = METRICS_PREFIX + "_total_nio_direct_memory";
    public static final String NIO_DIRECT_BUFFER_COUNT = METRICS_PREFIX + "_nio_direct_buffer_count";
    public static final String TOTAL_MEMORY = METRICS_PREFIX + "_total_memory";
    public static final String TOTAL_ALIGNED_ALLOCATIONS = METRICS_PREFIX + "_total_aligned_allocations_total";
    public static final String SMALL_ALIGNED_ALLOCATIONS = METRICS_PREFIX + "_small_aligned_allocations_total";

    private volatile Counter totalAlignedAllocations;
    private volatile Counter smallAlignedAllocations;

    public MicrometerNativeMemoryMetrics()
    {
        if (directBufferPool == null)
            logger.error("Direct memory buffer pool MBean not present, native memory metrics will be missing for nio buffers");

        this.totalAlignedAllocations = counter(TOTAL_ALIGNED_ALLOCATIONS);
        this.smallAlignedAllocations = counter(SMALL_ALIGNED_ALLOCATIONS);
    }

    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags);

        gauge(RAW_NATIVE_MEMORY, this, NativeMemoryMetrics::rawNativeMemory);
        gauge(BLOOM_FILTER_MEMORY, this, NativeMemoryMetrics::bloomFilterMemory);
        gauge(NETWORK_DIRECT_MEMORY, this, NativeMemoryMetrics::networkDirectMemory);
        gauge(USED_NIO_DIRECT_MEMORY, this, NativeMemoryMetrics::usedNioDirectMemory);
        gauge(TOTAL_NIO_MEMORY, this, NativeMemoryMetrics::totalNioDirectMemory);
        gauge(NIO_DIRECT_BUFFER_COUNT, this, NativeMemoryMetrics::nioDirectBufferCount);
        gauge(TOTAL_MEMORY, this, NativeMemoryMetrics::totalMemory);

        this.totalAlignedAllocations = counter(TOTAL_ALIGNED_ALLOCATIONS);
        this.smallAlignedAllocations = counter(SMALL_ALIGNED_ALLOCATIONS);
    }

    @Override
    public void alignedAllocation()
    {
        this.totalAlignedAllocations.increment();
    }

    @Override
    public void smallAlignedAllocation()
    {
        this.smallAlignedAllocations.increment();
    }

    @Override
    public long usedNioDirectMemoryValue()
    {
        return usedNioDirectMemory();
    }
}
