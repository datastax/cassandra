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

/// A [Rebufferer] over a buffer that is already fully in memory.
///
/// The on-disk trie readers are written against [Rebufferer] so they can page a file in as they
/// walk. Serialized tries also arrive already-complete in a buffer — a commit-log record, or a
/// message payload — and this adapts the one to the other: the whole content is a single
/// "chunk", so [#rebuffer] hands back the same buffer whatever position is asked for.
///
/// Owns nothing. The buffer must outlive every cursor reading through it, and closing this does
/// not release it.
public class ByteBufferRebufferer implements Rebufferer
{
    private final ByteBuffer buffer;
    private final BufferHolder holder;

    public ByteBufferRebufferer(ByteBuffer buffer)
    {
        this.buffer = buffer;
        this.holder = new BufferHolder()
        {
            @Override
            public ByteBuffer buffer()
            {
                // Duplicate: readers set position and limit on what they are handed, and must not
                // disturb the buffer the caller still owns.
                return buffer.duplicate();
            }

            @Override
            public long offset()
            {
                return 0;
            }

            @Override
            public void release()
            {
                // Nothing to release; the buffer is the caller's.
            }
        };
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        return holder;
    }

    @Override
    public long fileLength()
    {
        return buffer.limit();
    }

    /// There is no file behind this, so there is no channel. Nothing on the trie read path calls
    /// this — [org.apache.cassandra.db.tries.OnDiskBaseTrie.WithoutChannel] closes only the reader.
    @Override
    public ChannelProxy channel()
    {
        throw new UnsupportedOperationException("ByteBufferRebufferer is not backed by a channel");
    }

    @Override
    public double getCrcCheckChance()
    {
        return 0;
    }

    @Override
    public long adjustPosition(long position)
    {
        return position;
    }

    @Override
    public void closeReader()
    {
        // Nothing to close.
    }

    @Override
    public void close()
    {
        // Nothing to close.
    }
}
