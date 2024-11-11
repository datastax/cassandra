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

import org.apache.cassandra.io.compress.BufferType;

class SimpleChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    private final int bufferSize;
    private final BufferType bufferType;
    private final long startOffset;

    SimpleChunkReader(ChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize)
    {
        this(channel, fileLength, bufferType, bufferSize, 0);
    }

    SimpleChunkReader(ChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize, long startOffset)
    {
        super(channel, fileLength);
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
        this.startOffset = startOffset;
    }

    @Override
    public void readChunk(long position, ByteBuffer buffer)
    {
        buffer.clear();
        channel.read(buffer, position - startOffset);
        buffer.flip();
    }

    @Override
    public int chunkSize()
    {
        return bufferSize;
    }

    @Override
    public BufferType preferredBufferType()
    {
        return bufferType;
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        if (Integer.bitCount(bufferSize) == 1)
        {
            assert startOffset == (startOffset & -bufferSize) : "startOffset must be aligned to buffer size";
            return new BufferManagingRebufferer.Aligned(this);
        }
        else
            return new BufferManagingRebufferer.Unaligned(this);
    }

    @Override
    public void invalidateIfCached(long position)
    {
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s - chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             bufferSize,
                             fileLength());
    }
}
