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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

public interface ICompressor
{
    /**
     * Ways that a particular instance of ICompressor should be used internally in Cassandra.
     *
     * GENERAL: Suitable for general use
     * FAST_COMPRESSION: Suitable for use in particularly latency sensitive compression situations (flushes).
     */
    enum Uses {
        GENERAL,
        FAST_COMPRESSION
    }

    public int initialCompressedBufferLength(int chunkLength);

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException;

    /**
     * Compression for ByteBuffers.
     *
     * The data between input.position() and input.limit() is compressed and placed into output starting from output.position().
     * Positions in both buffers are moved to reflect the bytes read and written. Limits are not changed.
     */
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException;

    /**
     * Decompression for DirectByteBuffers.
     *
     * The data between input.position() and input.limit() is uncompressed and placed into output starting from output.position().
     * Positions in both buffers are moved to reflect the bytes read and written. Limits are not changed.
     */
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException;

    /**
     * Returns the preferred (most efficient) buffer type for this compressor.
     */
    public BufferType preferredBufferType();

    /**
     * Checks if the given buffer would be supported by the compressor. If a type is supported, the compressor must be
     * able to use it in combination with all other supported types.
     *
     * Direct and memory-mapped buffers must be supported by all compressors.
     */
    public boolean supports(BufferType bufferType);

    public Set<String> supportedOptions();

    /**
     * Hints to Cassandra which uses this compressor is recommended for. For example a compression algorithm which gets
     * good compression ratio may trade off too much compression speed to be useful in certain compression heavy use
     * cases such as flushes or mutation hints.
     *
     * Note that Cassandra may ignore these recommendations, it is not a strict contract.
     */
    default Set<Uses> recommendedUses()
    {
        return ImmutableSet.copyOf(EnumSet.allOf(Uses.class));
    }

    /**
     * Returns the compressor configured for a particular use.
     * Allows creating a compressor implementation that can handle multiple uses but requires different configurations
     * adapted to a particular use.
     * <p>
     * May return this object.
     * May not modify this object.
     * Should return null if the request cannot be satisfied.
     */
    default @Nullable ICompressor forUse(Uses use)
    {
        return recommendedUses().contains(use) ? this : null;
    }
}
