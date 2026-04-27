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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.awaitility.Awaitility;

import org.apache.cassandra.metrics.MicrometerCompressionChunkOffsetCacheMetrics;
import static org.assertj.core.api.Assertions.assertThat;

public class CompressionChunkOffsetCacheTest
{
    @Test
    public void offsetsBlockRefCounting()
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(16);
        CompressionChunkOffsetCache.OffsetsBlock block = new CompressionChunkOffsetCache.OffsetsBlock(buffer);

        assertThat(block.references).isEqualTo(1);
        assertThat(block.ref()).isTrue();
        assertThat(block.references).isEqualTo(2);

        block.release();
        assertThat(block.references).isEqualTo(1);

        block.release();
        assertThat(block.references).isEqualTo(0);
        assertThat(block.ref()).isFalse();
    }

    @Test
    public void testOffheapMemoryUsage()
    {
        long before = CompressionMetadata.nativeMemoryAllocated();
        CompressionChunkOffsetCache cache = new CompressionChunkOffsetCache(100 * 1024);
        File file = new File("cache_test");

        CompressionChunkOffsetCache.OffsetsBlock block =
        cache.getBlock(file, 0, () -> new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocateDirect(1024)));

        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
             assertThat(cache.weightedSize()).isEqualTo(1024L);
             assertThat(CompressionMetadata.nativeMemoryAllocated()).isGreaterThanOrEqualTo(before + 1024L);
         });

        cache.clear();
        Awaitility.await("Cache eviction")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(cache.weightedSize()).isEqualTo(0));
        Awaitility.await("Native memory released")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(CompressionMetadata.nativeMemoryAllocated()).isEqualTo(before));
    }

    @Test
    public void testMetrics()
    {
        CompressionChunkOffsetCache cache = new CompressionChunkOffsetCache(1024);
        MicrometerCompressionChunkOffsetCacheMetrics metrics = cache.getMetrics();
        assertThat(metrics.hits()).isEqualTo(0);
        assertThat(metrics.hitRate()).isNaN();
        assertThat(metrics.misses()).isEqualTo(0);
        assertThat(metrics.entries()).isEqualTo(0);
        assertThat(metrics.capacity()).isEqualTo(1024);
        assertThat(metrics.size()).isEqualTo(0);

        File file = new File("cache_test");

        // 1st miss
        cache.getBlock(file, 0, () -> new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocateDirect(1024)));
        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(metrics.hits()).isEqualTo(0);
                      assertThat(metrics.hitRate()).isNaN();
                      assertThat(metrics.misses()).isEqualTo(1);
                      assertThat(metrics.missLatency()).isGreaterThan(0);
                      assertThat(metrics.entries()).isEqualTo(1);
                      assertThat(metrics.capacity()).isEqualTo(1024);
                      assertThat(metrics.size()).isEqualTo(1024);
                  });

        // 1st hit
        cache.getBlock(file, 0, () -> new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocateDirect(1024)));
        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(metrics.hits()).isEqualTo(1);
                      assertThat(metrics.hitRate()).isEqualTo(0.5);
                      assertThat(metrics.misses()).isEqualTo(1);
                      assertThat(metrics.missLatency()).isGreaterThan(0);
                      assertThat(metrics.entries()).isEqualTo(1);
                      assertThat(metrics.capacity()).isEqualTo(1024);
                      assertThat(metrics.size()).isEqualTo(1024);
                  });

        // load another block to evict previous block
        cache.getBlock(file, 1, () -> new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocateDirect(1024)));
        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(metrics.hits()).isEqualTo(1);
                      assertThat(metrics.hitRate()).isLessThan(0.5).isGreaterThan(0.0);
                      assertThat(metrics.misses()).isEqualTo(2);
                      assertThat(metrics.missLatency()).isGreaterThan(0);
                      assertThat(metrics.entries()).isEqualTo(1);
                      assertThat(metrics.capacity()).isEqualTo(1024);
                      assertThat(metrics.size()).isEqualTo(1024);
                  });
    }
}
