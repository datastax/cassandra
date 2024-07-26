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
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.INativeLibrary;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static java.util.stream.Stream.of;
import static org.apache.cassandra.config.CassandraRelevantProperties.MMAPPED_MAX_SEGMENT_SIZE_IN_MB;
import static org.apache.cassandra.utils.Throwables.perform;

public class MmappedRegions extends SharedCloseableImpl
{
    /** In a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size */
    public static int MAX_SEGMENT_SIZE = MMAPPED_MAX_SEGMENT_SIZE_IN_MB.getInt(Integer.MAX_VALUE);

    /**
     * When we need to grow the arrays, we add this number of region slots
     */
    static final int REGION_ALLOC_SIZE = 15;

    /**
     * The original state, which is shared with the tidier and
     * contains all the regions mapped so far. It also
     * does the actual mapping.
     */
    private final State state;

    /**
     * A copy of the latest state. We update this each time the original state is
     * updated and we share this with copies. If we are a copy, then this
     * is null. Copies can only access existing regions, they cannot create
     * new ones. This is for thread safety and because MmappedRegions is
     * reference counted, only the original state will be cleaned-up,
     * therefore only the original state should create new mapped regions.
     */
    private volatile State copy;

    private final long uncompressedSliceOffset;

    private MmappedRegions(ChannelProxy channel, CompressionMetadata metadata, long length, long uncompressedSliceOffset, boolean adviseRandom)
    {
        this(new State(channel, offsetFrom(metadata, uncompressedSliceOffset), adviseRandom),
             metadata,
             length,
             uncompressedSliceOffset);
    }

    private static long offsetFrom(CompressionMetadata metadata, long uncompressedSliceOffset)
    {
        return metadata == null ? uncompressedSliceOffset : metadata.chunkFor(uncompressedSliceOffset).offset;
    }

    private MmappedRegions(State state, CompressionMetadata metadata, long length, long uncompressedSliceOffset)
    {
        super(new Tidier(state));

        this.state = state;
        this.uncompressedSliceOffset = uncompressedSliceOffset;

        if (metadata != null)
        {
            assert length == 0 : "expected no length with metadata";
            updateState(metadata);
        }
        else if (length > 0)
        {
            updateState(length);
        }

        this.copy = new State(state);
    }

    private MmappedRegions(MmappedRegions original)
    {
        super(original);
        this.state = original.copy;
        this.uncompressedSliceOffset = original.uncompressedSliceOffset;
    }

    public static MmappedRegions empty(ChannelProxy channel)
    {
        return new MmappedRegions(channel, null, 0, 0, false);
    }

    /**
     * Create memory mapped regions for the given compressed file.
     *
     * @param channel     file to map. The {@link MmappedRegions} instance will hold shared copy of given channel.
     * @param metadata    compression metadata for the mapped file, cannot be null. A shared copy of the metadata is not
     *                    created, so it needs to me managed by the caller.
     * @param uncompressedSliceOffset if the file represents a slice of the origial file, this is the offset of the slice in
     *                    the original file (in uncompressed data), namely the value of {@link SliceDescriptor#sliceStart}.
     * @param adviseRandom whether to apply MADV_RANDOM to mapped regions
     * @return new instance
     */
    public static MmappedRegions map(ChannelProxy channel, CompressionMetadata metadata, long uncompressedSliceOffset, boolean adviseRandom)
    {
        if (metadata == null)
            throw new IllegalArgumentException("metadata cannot be null");

        return new MmappedRegions(channel, metadata, 0, uncompressedSliceOffset, adviseRandom);
    }

    public static MmappedRegions map(ChannelProxy channel, long length, long uncompressedSliceOffset, boolean adviseRandom)
    {
        if (length <= 0)
            throw new IllegalArgumentException("Length must be positive");

        return new MmappedRegions(channel, null, length, uncompressedSliceOffset, adviseRandom);
    }

    /**
     * @return a snapshot of the memory mapped regions. The snapshot can
     * only use existing regions, it cannot create new ones.
     */
    public MmappedRegions sharedCopy()
    {
        return new MmappedRegions(this);
    }

    private boolean isCopy()
    {
        return copy == null;
    }

    /**
     * Extends this collection of mmapped regions up to the provided total length.
     *
     * @return {@code true} if new regions have been created
     */
    public boolean extend(long length)
    {
        if (length < 0)
            throw new IllegalArgumentException("Length must not be negative");

        assert !isCopy() : "Copies cannot be extended";

        if (length <= state.length)
            return false;

        int initialRegions = state.last;
        updateState(length);
        copy = new State(state);
        return state.last > initialRegions;
    }

    /**
     * Extends this collection of mmapped regions up to the length of the compressed file described by the provided
     * metadata.
     *
     * @return {@code true} if new regions have been created
     */
    public boolean extend(CompressionMetadata compressionMetadata)
    {
        assert !isCopy() : "Copies cannot be extended";

        if (compressionMetadata.compressedFileLength <= state.length)
            return false;

        int initialRegions = state.last;
        if (compressionMetadata.compressedFileLength - state.length <= MAX_SEGMENT_SIZE)
            updateState(compressionMetadata.compressedFileLength);
        else
            updateState(compressionMetadata);

        copy = new State(state);
        return state.last > initialRegions;
    }

    /**
     * Updates state by adding the remaining segments. It starts with the current state last segment end position and
     * subsequently add new segments until all data up to the provided length are mapped.
     */
    private void updateState(long length)
    {
        state.length = length;
        long pos = state.getPosition();
        while (pos < length)
        {
            long size = Math.min(MAX_SEGMENT_SIZE, length - pos);
            state.add(pos, size);
            pos += size;
        }
    }

    private void updateState(CompressionMetadata metadata)
    {
        long uncompressedPosition = metadata.getDataOffsetForChunkOffset(state.getPosition()); // uncompressed position of the current compressed chunk in the original (compressed) file
        long compressedPosition = state.getPosition();   // position on disk of the current compressed chunk in the original (compressed) file
        long segmentSize = 0;

        assert metadata.chunkFor(uncompressedPosition).offset == compressedPosition;

        while (uncompressedPosition - uncompressedSliceOffset < metadata.dataLength)
        {
            // chunk contains the position on disk in the original file
            CompressionMetadata.Chunk chunk = metadata.chunkFor(uncompressedPosition);

            //Reached a new mmap boundary
            if (segmentSize + chunk.length + 4 > MAX_SEGMENT_SIZE)
            {
                if (segmentSize > 0)
                {
                    state.add(compressedPosition, segmentSize);
                    compressedPosition += segmentSize;
                    segmentSize = 0;
                }
            }

            segmentSize += chunk.length + 4; // compressed size of the chunk including 4 bytes of checksum
            uncompressedPosition += metadata.chunkLength(); // uncompressed size of the chunk
        }

        if (segmentSize > 0)
            state.add(compressedPosition, segmentSize);

        state.length = compressedPosition + segmentSize;
    }

    public boolean isValid(ChannelProxy channel)
    {
        return state.isValid(channel);
    }

    public boolean isEmpty()
    {
        return state.isEmpty();
    }

    /**
     * Get the region containing the given position
     *
     * @param position the position on disk (not in the uncompressed data) in the original file (not in the slice)
     */
    public Region floor(long position)
    {
        assert !isCleanedUp() : "Attempted to use closed region";
        return state.floor(position);
    }

    public void closeQuietly()
    {
        Throwable err = close(null);
        if (err != null)
        {
            JVMStabilityInspector.inspectThrowable(err);

            // This is not supposed to happen
            LoggerFactory.getLogger(getClass()).error("Error while closing mmapped regions", err);
        }
    }

    public static final class Region implements Rebufferer.BufferHolder
    {
        public final long offset;
        public final ByteBuffer buffer;

        public Region(long offset, ByteBuffer buffer)
        {
            this.offset = offset;
            this.buffer = buffer;
        }

        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        @Override
        public ByteOrder order()
        {
            return buffer.order();
        }

        public FloatBuffer floatBuffer()
        {
            // this does an implicit duplicate(), so we need to expose it directly to avoid doing it twice unnecessarily
            return buffer.asFloatBuffer();
        }

        public IntBuffer intBuffer()
        {
            // this does an implicit duplicate(), so we need to expose it directly to avoid doing it twice unnecessarily
            return buffer.asIntBuffer();
        }

        public LongBuffer longBuffer()
        {
            // this does an implicit duplicate(), so we need to expose it directly to avoid doing it twice unnecessarily
            return buffer.asLongBuffer();
        }

        public long offset()
        {
            return offset;
        }

        public long end()
        {
            return offset + buffer.capacity();
        }

        public void release()
        {
            // only released after no readers are present
        }
    }

    private static final class State
    {
        /**
         * The file channel
         */
        private final ChannelProxy channel;

        /**
         * An array of region buffers, synchronized with offsets
         */
        private ByteBuffer[] buffers;

        /**
         * An array of region offsets, synchronized with buffers
         */
        private long[] offsets;

        /**
         * The maximum file length we have mapped
         */
        private long length;

        /**
         * The index to the last region added
         */
        private int last;

        /** The position of the first region of the slice in the original file (if the file is compressed, the offset
          * refers to position on disk, not the uncompressed data) */
        private final long onDiskSliceOffset;

        /** whether to apply fadv_random to mapped regions */
        private boolean adviseRandom;

        private State(ChannelProxy channel, long onDiskSliceOffset, boolean adviseRandom)
        {
            this.channel = channel.sharedCopy();
            this.adviseRandom = adviseRandom;
            this.buffers = new ByteBuffer[REGION_ALLOC_SIZE];
            this.offsets = new long[REGION_ALLOC_SIZE];
            this.length = 0;
            this.last = -1;
            this.onDiskSliceOffset = onDiskSliceOffset;
        }

        private State(State original)
        {
            this.channel = original.channel;
            this.adviseRandom = original.adviseRandom;
            this.buffers = original.buffers;
            this.offsets = original.offsets;
            this.length = original.length;
            this.last = original.last;
            this.onDiskSliceOffset = original.onDiskSliceOffset;
        }

        private boolean isEmpty()
        {
            return last < 0;
        }

        private boolean isValid(ChannelProxy channel)
        {
            // todo maybe extend validation to verify slice offset?
            return this.channel.filePath().equals(channel.filePath());
        }

        private Region floor(long position)
        {
            assert onDiskSliceOffset <= position && position <= length : String.format("%d > %d", position, length);

            int idx = Arrays.binarySearch(offsets, 0, last + 1, position);
            assert idx != -1 : String.format("Bad position %d for regions %s, last %d in %s", position, Arrays.toString(offsets), last, channel);
            if (idx < 0)
                idx = -(idx + 2); // round down to entry at insertion point

            return new Region(offsets[idx], buffers[idx]);
        }

        private long getPosition()
        {
            return last < 0 ? onDiskSliceOffset : offsets[last] + buffers[last].capacity();
        }

        /**
         * Add a new region to the state
         * @param pos the position on disk (not in the uncompressed data) in the original file (not the slice)
         * @param size the size of the region
         */
        private void add(long pos, long size)
        {
            var buffer = channel.map(FileChannel.MapMode.READ_ONLY, pos - onDiskSliceOffset, size);
            if (adviseRandom)
                INativeLibrary.instance.adviseRandom(buffer, size, channel.filePath());

            ++last;

            if (last == offsets.length)
            {
                offsets = Arrays.copyOf(offsets, offsets.length + REGION_ALLOC_SIZE);
                buffers = Arrays.copyOf(buffers, buffers.length + REGION_ALLOC_SIZE);
            }

            offsets[last] = pos;
            buffers[last] = buffer;
        }

        private Throwable close(Throwable accumulate)
        {
            accumulate = channel.close(accumulate);

            return perform(accumulate, channel.filePath(), Throwables.FileOpType.READ,
                           of(buffers)
                           .map((buffer) ->
                                () ->
                                {
                                    if (buffer != null)
                                        FileUtils.clean(buffer);
                                }));
        }
    }

    public static final class Tidier implements RefCounted.Tidy
    {
        final State state;

        Tidier(State state)
        {
            this.state = state;
        }

        public String name()
        {
            return state.channel.filePath();
        }

        public void tidy()
        {
            try
            {
                Throwables.maybeFail(state.close(null));
            }
            catch (Exception e)
            {
                throw new FSReadError(e, state.channel.filePath());
            }
        }
    }
}
