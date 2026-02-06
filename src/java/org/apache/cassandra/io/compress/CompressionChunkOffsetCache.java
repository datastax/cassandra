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

import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.File;

public class CompressionChunkOffsetCache
{
    private static final class Holder
    {
        private static final CompressionChunkOffsetCache INSTANCE = new CompressionChunkOffsetCache(CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_CACHE_IN_MB.getInt());
    }

    public static CompressionChunkOffsetCache get()
    {
        return CompressionChunkOffsetCache.Holder.INSTANCE;
    }

    private final Cache<BlockKey, long[]> cache;

    CompressionChunkOffsetCache(int maxSizeInMB)
    {
        Preconditions.checkArgument(maxSizeInMB > 0, maxSizeInMB + " must be positive number");
        long maxBytes = (long) maxSizeInMB * 1024L * 1024L;
        cache = Caffeine.newBuilder()
                        .maximumWeight(maxBytes)
                        // TODO include key size
                        .weigher((BlockKey key, long[] value) -> Math.min(Integer.MAX_VALUE, value.length * Long.BYTES))
                        .build();
    }

    long[] getBlock(File indexFilePath, long offsetsStart, int blockIndex, Supplier<long[]> blockLoader)
    {
        BlockKey key = new BlockKey(indexFilePath, offsetsStart, blockIndex);
        return cache.get(key, k -> blockLoader.get());
    }

    public long memoryUsage()
    {
        return cache.policy()
                    .eviction()
                    .map(eviction -> eviction.weightedSize().orElse(0))
                    .orElse(0L);
    }

    public static final class BlockKey
    {
        // TODO this is memory consuming. use file name instead?
        private final File file;
        private final long offsetsStart;
        private final int blockIndex;
        private final int hashcode;

        private BlockKey(File file, long offsetsStart, int blockIndex)
        {
            this.file = file;
            this.offsetsStart = offsetsStart;
            this.blockIndex = blockIndex;
            this.hashcode = Objects.hash(file, offsetsStart, blockIndex);
        }

        public boolean equals(Object other)
        {
            if (this == other)
                return true;
            if (!(other instanceof BlockKey))
                return false;
            BlockKey that = (BlockKey) other;
            return offsetsStart == that.offsetsStart
                   && blockIndex == that.blockIndex
                   && Objects.equals(file, that.file);
        }

        public int hashCode()
        {
            return hashcode;
        }
    }
}
