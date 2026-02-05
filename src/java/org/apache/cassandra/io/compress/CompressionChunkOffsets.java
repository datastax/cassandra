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

import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Abstraction over chunk offset storage for compression metadata.
 * <p>
 * Implementations may keep offsets in memory, read them on-demand from disk, or use a cache.
 * Offsets are indexed by chunk index relative to the metadata's start chunk.
 * </p>
 */
public interface CompressionChunkOffsets extends AutoCloseable
{
    enum Type
    {
        /**
         * All chunk offsets are loaded and kept in off-heap memory. This is the fastest and consume around 75GB memory
         * for 150TB data. This is the default behavior if not specified otherwise.
         */
        IN_MEMORY,
        /**
         * Chunk offsets are read directly from the compression info file on-demand.
         * This mode has worse latency due to disk accesses and it's for demostration only.
         */
        ON_DISK,
        /**
         * Chunk offsets are read from the compression info file in blocks and cached in memory.
         * This mode aims to balance memory usage and access latency.
         */
        ON_DISK_WITH_CACHE
    }

    /**
     * Return the chunk offset at the provided index (0-based within this metadata instance).
     *
     * @param index chunk index relative to the start chunk
     * @return offset of the chunk in the compressed file
     */
    long get(int index);

    /**
     * @return number of offsets in this instance.
     */
    int size();

    /**
     * @return off-heap bytes used by this instance. return 0 if it's backed by cache.
     */
    long offHeapMemoryUsed();

    /**
     * Add any off-heap identities to the provided collection.
     *
     * @param identities collection of tracked identities
     */
    void addTo(Ref.IdentityCollection identities);

    /**
     * @return position after the last chunk in the compressed file, adjusted for partial files.
     */
    long compressedFileLength();

    /**
     * Release any resources held by this instance.
     */
    void close();

    class Empty implements CompressionChunkOffsets
    {
        public long get(int index)
        {
            throw new IndexOutOfBoundsException("Chunk index " + index + " out of bounds for empty offsets");
        }

        public int size()
        {
            return 0;
        }

        public long offHeapMemoryUsed()
        {
            return 0;
        }

        public void addTo(Ref.IdentityCollection identities)
        {
        }

        @Override
        public long compressedFileLength()
        {
            return 0;
        }

        public void close()
        {
        }
    }
}
