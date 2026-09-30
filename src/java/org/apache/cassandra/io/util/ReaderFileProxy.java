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

package org.apache.cassandra.io.util;

/**
 * Base class for the RandomAccessReader components that implement reading.
 */
public interface ReaderFileProxy extends AutoCloseable
{
    void close();               // no checked exceptions

    ChannelProxy channel();

    long fileLength();

    /**
     * Needed for tests. Returns the table's CRC check chance, which is only set for compressed tables.
     */
    double getCrcCheckChance();

    /**
     * Called before rebuffering to allow for position adjustments.
     * This is used to enable files with holes (e.g. encryption data) where we still want to be able to write and read
     * sequences of bytes (e.g. keys) that span over a hole.
     */
    long adjustPosition(long position);

    /**
     * Returns the position {@code bytesToSkip} bytes of content after {@code currentPosition}. For files with holes
     * (see {@link #adjustPosition}) the holes crossed are not counted as skipped bytes.
     * <p>
     * The result is the file pointer that reading the same bytes would leave: when the skipped bytes end exactly at
     * the usable end of a chunk, it is the start of that chunk's hole. Seeking to such a position, however, moves past
     * the hole to the start of the next chunk (see {@link #adjustPosition}), so the result is not always a position
     * to seek to; {@link RandomAccessReader#skipBytes} takes care of that difference.
     * <p>
     * Implementations must be strictly increasing in {@code bytesToSkip}, and a skip ending at the usable end of a
     * chunk must return the start of its hole (not the start of the next chunk): {@link SkipPositions} and
     * {@link TailOverridingRebufferer} rely on both, and {@link RandomAccessReader#skipBytes} relies on the latter to
     * recognize skips ending at the start of a hole.
     * <p>
     * The default implementation, for files without holes, returns {@code currentPosition + bytesToSkip}. Wrappers
     * (e.g. rebufferers built over a {@link ChunkReader}) must delegate to their source. A wrapper that does not falls
     * back to this default, i.e. it only loses the awareness of holes (the behaviour before files with holes were
     * supported): skips across a hole then land at a wrong position.
     */
    default long positionForSkip(long currentPosition, int bytesToSkip)
    {
        return currentPosition + bytesToSkip;
    }
}
