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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Throwables;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;

/**
 * RandomFileReader component that reads data from a file into a provided buffer and may have requirements over the
 * size and alignment of reads.
 * A caching or buffer-managing rebufferer will reference one of these to do the actual reading.
 * Note: Implementations of this interface must be thread-safe!
 */
public interface ChunkReader extends RebuffererFactory
{
    /**
     * Read the chunk at the given position, attempting to fill the capacity of the given buffer.
     * The filled buffer must be positioned at 0, with limit set at the size of the available data.
     * The source may have requirements for the positioning and/or size of the buffer (e.g. chunk-aligned and
     * chunk-sized). These must be satisfied by the caller. 
     */
    void readChunk(long position, ByteBuffer buffer);

    default CompletableFuture<ByteBuffer> readChunkAsync(long position, ByteBuffer buffer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                readChunk(position, buffer);
                return buffer;
            } catch (Throwable t) {
                throw Throwables.propagate(t);
            }
        });
    }

    default void readChunkBlocking(long position, ByteBuffer buffer) {
        readChunk(position, buffer);
    }

    /**
     * Buffer size required for this rebufferer. Must be power of 2 if alignment is required.
     */
    int chunkSize();

    /**
     * Specifies type of buffer the caller should attempt to give.
     * This is not guaranteed to be fulfilled.
     */
    BufferType preferredBufferType();

    /**
     * Read a chunk asynchronously using a {@link ReadTarget}. This method is a wrapper of {@link this#readChunk(long, ByteBuffer)},
     * it merely asks the {@link ReadTarget} how the buffer should be provided.
     *
     * @param position the start position of the chunk
     * @param chunkSize the amount of data to read from the file
     * @param target the read target will receive the data read into the temporary buffer
     *
     * @return a future that will complete when the buffers in the chunk have been read
     */
    default <R> CompletableFuture<R> readChunk(long position, int chunkSize, ReadTarget<R> target)
    {
        ByteBuffer bufferToRelease = null;
        try
        {
            ByteBuffer buffer = target.constructTarget(chunkSize);
            bufferToRelease = buffer;
            assert !buffer.isDirect() || (UnsafeByteBufferAccess.getAddress(buffer) & (512 - 1)) == 0 : "Buffer is not properly aligned!";

            return readChunkAsync(position, buffer)
                   .handle((result, err) -> {
                       if (err != null)
                           throw Throwables.propagate(target.onError(err, buffer));

                       return target.setReadResult(result);
                   });
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(target.onError(t, bufferToRelease));
        }
    }

    /**
     * Read a chunk blocking using a {@link ReadTarget}. This method is a wrapper of {@link this#readChunkBlocking(long, ByteBuffer)},
     * it merely asks the {@link ReadTarget} how the buffer should be provided.
     *
     * @param position the start position of the chunk
     * @param chunkSize the amount of data to read from the file
     * @param target the read target will receive the data read into the temporary buffer
     *
     * @return a future that will complete when the buffers in the chunk have been read.
     */
    default <R> R readChunkBlocking(long position, int chunkSize, ReadTarget<R> target)
    {
        ByteBuffer buffer = null;
        try
        {
            buffer = target.constructTarget(chunkSize);
            assert !buffer.isDirect() || (UnsafeByteBufferAccess.getAddress(buffer) & (512 - 1)) == 0 : "Buffer from pool is not properly aligned!";

            readChunkBlocking(position, buffer);
            return target.setReadResult(buffer);
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(target.onError(t, buffer));
        }
    }

    /**
     * A read target provides a byte buffer for the read and receives the data read into the buffer or an error
     * notification.
     */
    interface ReadTarget<R>
    {
        /**
         * Return a buffer that is used as the target for the read.
         *
         * @param chunkSize the size of the memory to read
         *
         * @return a buffer large enough to accommodate chunkSize bytes
         */
        ByteBuffer constructTarget(int chunkSize);

        /**
         * Called when the read completes to do any necessary movement of data and release of temporary buffers.
         *
         * This method is not permitted to throw (since its job is to release buffers, we can't safely call onError
         * if it does).
         *
         * @param result a byte buffer obtained from {@link #constructTarget} filled with the bytes that have been read.
         */
        R setReadResult(ByteBuffer result);

        /**
         * To be called if the loading encounters an error. Clean up any buffers allocated by
         * {@link #constructTarget}.
         *
         * @param error The exception encountered.
         * @param buffer The buffer that was supplied by {@link #constructTarget}.
         * @return the error to pass on, must not be null.
         */
        Throwable onError(Throwable error, ByteBuffer buffer);
    }
}
