/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.utils.memory.BufferPools;

/// Buffer manager used for reading from a [ChunkReader] when cache is not in use. They use a buffer produced by the
/// "networking" buffer pool, which is the one to be used for buffers that are not to be retained for a long time
/// (the lifetime of this object is contained by the lifetime of a [RandomAccessReader] which is contained in a read
/// operation's lifetime).
///
/// Instances of this class are reader-specific and thus do not need to be thread-safe since the reader itself isn't.
///
/// The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
public abstract class BufferManagingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final ChunkReader source;
    protected final ByteBuffer buffer;
    protected long offset = 0;

    abstract long alignedPosition(long position);

    protected BufferManagingRebufferer(ChunkReader wrapped)
    {
        this.source = wrapped;
        // Note: This class uses the networking buffer pool which makes better sense for short-lifetime buffers.
        // Because this is meant to be used when the chunk cache is disabled, it also makes sense to use any memory
        // that may have been allocated for in-flight data by using the chunk-cache pool.
        // However, if some new functionality decides to use this class in the presence of the chunk cache (e.g.
        // cache-bypassing compaction), using the chunk-cache pool here will certainly cause hard-to-diagnose issues
        // that we would prefer to avoid.
        buffer = BufferPools.forNetworking().get(wrapped.chunkSize(), wrapped.preferredBufferType()).order(ByteOrder.BIG_ENDIAN);
        buffer.limit(0);
    }

    @Override
    public void closeReader()
    {
        BufferPools.forNetworking().put(buffer);
        source.releaseUnderlyingResources();
        offset = -1;
    }

    @Override
    public void close()
    {
        assert offset == -1;    // reader must be closed at this point.
        source.close();
    }

    @Override
    public ChannelProxy channel()
    {
        return source.channel();
    }

    @Override
    public long fileLength()
    {
        return source.fileLength();
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        offset = alignedPosition(position);
        source.readChunk(offset, buffer);
        return this;
    }

    @Override
    public double getCrcCheckChance()
    {
        return source.getCrcCheckChance();
    }

    @Override
    public long adjustPosition(long position)
    {
        return source.adjustPosition(position);
    }

    @Override
    public String toString()
    {
        return "BufferManagingRebufferer." + getClass().getSimpleName() + ":" + source;
    }

    // BufferHolder methods

    public ByteBuffer buffer()
    {
        return buffer.duplicate();
    }

    public long offset()
    {
        return offset;
    }

    @Override
    public void release()
    {
        // nothing to do, we don't delete buffers before we're closed.
    }

    public static class Unaligned extends BufferManagingRebufferer
    {
        public Unaligned(ChunkReader wrapped)
        {
            super(wrapped);
        }

        @Override
        long alignedPosition(long position)
        {
            return position;
        }
    }

    public static class Aligned extends BufferManagingRebufferer
    {
        public Aligned(ChunkReader wrapped)
        {
            super(wrapped);
            assert Integer.bitCount(wrapped.chunkSize()) == 1;
        }

        @Override
        long alignedPosition(long position)
        {
            return position & -buffer.capacity();
        }
    }
}
