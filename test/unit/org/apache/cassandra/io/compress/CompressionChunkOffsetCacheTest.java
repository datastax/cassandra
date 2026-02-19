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
        cache.getBlock(file, 0L, 0, () -> new CompressionChunkOffsetCache.OffsetsBlock(ByteBuffer.allocateDirect(1024)));

        assertThat(block).isNotNull();
        assertThat(cache.offHeapMemoryUsage()).isGreaterThanOrEqualTo(1024L);
        assertThat(CompressionMetadata.nativeMemoryAllocated()).isGreaterThanOrEqualTo(before + 1024L);

        cache.clear();
        Awaitility.await("Cache eviction")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(cache.offHeapMemoryUsage()).isEqualTo(0));
        Awaitility.await("Native memory released")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(CompressionMetadata.nativeMemoryAllocated()).isEqualTo(before));
    }
}
