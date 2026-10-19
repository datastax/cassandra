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
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Special rebufferer that replaces the tail of the file (from the specified cutoff point) with the given buffer.
 * <p>
 * Instantiated once per RandomAccessReader, thread-unsafe.
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 * Only one BufferHolder can be active at a time. Calling {@link #rebuffer(long)} before the previously obtained
 * buffer holder is released will throw {@link AssertionError}.
 */
@NotThreadSafe
public class TailOverridingRebufferer extends WrappingRebufferer
{
    private final long cutoff;
    private final ByteBuffer tail;

    public TailOverridingRebufferer(Rebufferer source, long cutoff, ByteBuffer tail)
    {
        super(source);
        this.cutoff = cutoff;
        this.tail = tail;
    }

    @Override
    public Rebufferer.BufferHolder rebuffer(long position)
    {
        assert buffer == null : "Buffer holder has been already acquired and has been not released yet";
        if (position < cutoff)
        {
            super.rebuffer(position);
            if (offset + buffer.limit() > cutoff)
                buffer.limit((int) (cutoff - offset));
        }
        else
        {
            buffer = tail.duplicate();
            offset = cutoff;
        }
        return this;
    }

    @Override
    public long fileLength()
    {
        return cutoff + tail.limit();
    }

    @Override
    public long adjustPosition(long position)
    {
        if (position < cutoff)
            return super.adjustPosition(position);
        else
            return position;
    }

    /**
     * Consistent with {@link #adjustPosition}: the source's arithmetic (e.g. the holes of an encrypted file) applies
     * before the cutoff only, and the tail from the cutoff on is contiguous. See {@link #bytesBeforeCutoff} for the
     * positions where this is defined.
     */
    @Override
    public long positionForSkip(long currentPosition, int bytesToSkip)
    {
        if (currentPosition >= cutoff)
            return currentPosition + bytesToSkip;

        long bytesBeforeCutoff = bytesBeforeCutoff(currentPosition);
        if (bytesToSkip <= bytesBeforeCutoff)
            return super.positionForSkip(currentPosition, bytesToSkip);
        return cutoff + (bytesToSkip - bytesBeforeCutoff);
    }

    @Override
    public long remainingBytes(long position)
    {
        if (position >= cutoff)
            return Math.max(0, fileLength() - position);
        return bytesBeforeCutoff(position) + tail.limit();
    }

    /**
     * The source's content between {@code position} (before the cutoff) and the cutoff, counted with the source's
     * {@link #remainingBytes}, i.e. up to the source's own length.
     * <p>
     * The source's length (the writer's last content position) may be shorter than the cutoff (the writer's padded
     * position): the gap between the two is unaddressable, as nothing points into it, and is not counted as
     * skippable, although reads through this rebufferer would return its bytes for an index without holes. The results
     * of {@link #positionForSkip} and {@link #remainingBytes} are therefore only defined for positions up to the
     * source's length or at/after the cutoff. This rebufferer is only built for the early-open partition index, whose
     * trie walkers seek and never skip.
     */
    private long bytesBeforeCutoff(long position)
    {
        return wrapped.remainingBytes(position) - wrapped.remainingBytes(cutoff);
    }

    @Override
    public String toString()
    {
        return String.format("%s[+%d@%d]:%s", getClass().getSimpleName(), tail.limit(), cutoff, wrapped.toString());
    }
}
