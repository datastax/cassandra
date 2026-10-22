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
     * The result is the file pointer that reading the same bytes would leave: when {@code bytesToSkip > 0} bytes end
     * exactly at the usable end of a chunk, it is the start of that chunk's hole (not the start of the next chunk).
     * Seeking to such a position, however, moves past the hole to the start of the next chunk (see
     * {@link #adjustPosition}), so the result is not always a position to seek to; {@link RandomAccessReader#skipBytes}
     * takes care of that difference.
     * <p>
     * Implementations over files without holes return {@code currentPosition + bytesToSkip}. Wrappers (e.g.
     * rebufferers built over a {@link ChunkReader}) must delegate to their source: otherwise skips across a hole land
     * at a wrong position.
     */
    long positionForSkip(long currentPosition, int bytesToSkip);

    /**
     * Returns the number of bytes of content between {@code position} and {@link #fileLength()}, not counting holes
     * (see {@link #adjustPosition}), or 0 if {@code position} is not before {@link #fileLength()}. This is the largest
     * number of bytes {@link #positionForSkip} can skip from {@code position} without going past the end of the file.
     * <p>
     * Implementations over files without holes return {@code max(0, fileLength() - position)}. The result is measured
     * against this proxy's own {@link #fileLength()}: wrappers that change neither the file length nor the positions
     * delegate to their source, while wrappers that change either (e.g. {@link TailOverridingRebufferer}) must
     * override it.
     */
    long remainingBytes(long position);
}
