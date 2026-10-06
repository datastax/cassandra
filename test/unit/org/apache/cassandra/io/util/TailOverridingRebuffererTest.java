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

import org.junit.Test;

import org.mockito.Mockito;

import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.CHUNK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TailOverridingRebuffererTest
{
    ByteBuffer head = ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 });
    ByteBuffer tail = ByteBuffer.wrap(new byte[]{ 9, 10 });

    Rebufferer r = Mockito.mock(Rebufferer.class);
    Rebufferer.BufferHolder bh = Mockito.mock(Rebufferer.BufferHolder.class);

    public void before()
    {
        reset(r, bh);
    }

    @Test
    public void testAccessLeftToTailFully()
    {
        when(r.rebuffer(anyLong())).thenReturn(bh);
        when(bh.buffer()).thenReturn(head.duplicate());
        when(bh.offset()).thenReturn(0L);
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        for (int i = 0; i < 8; i++)
        {
            Rebufferer.BufferHolder bh = tor.rebuffer(i);
            assertEquals(head, bh.buffer());
            bh.release();
        }

        assertEquals(10, tor.fileLength());
    }

    @Test
    public void testAccessLeftToTailPartial()
    {
        when(r.rebuffer(anyLong())).thenReturn(bh);
        when(bh.buffer()).thenReturn(head.duplicate());
        when(bh.offset()).thenReturn(2L);
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        for (int i = 2; i < 8; i++)
        {
            Rebufferer.BufferHolder bh = tor.rebuffer(i);
            assertEquals(head.limit(6), bh.buffer());
            bh.release();
        }

        assertEquals(10, tor.fileLength());
    }

    @Test
    public void testAccessRightToTail()
    {
        when(r.rebuffer(anyLong())).thenReturn(bh);
        when(bh.buffer()).thenReturn(head.duplicate());
        when(bh.offset()).thenReturn(0L);
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        for (int i = 8; i < 10; i++)
        {
            Rebufferer.BufferHolder bh = tor.rebuffer(i);
            assertEquals(tail, bh.buffer());
            bh.release();
        }

        assertEquals(10, tor.fileLength());
    }

    @Test
    public void testOtherMethods()
    {
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        File tmp = FileUtils.createTempFile("fakeChannelProxy", "");
        try (ChannelProxy channelProxy = new ChannelProxy(tmp))
        {
            when(r.channel()).thenReturn(channelProxy);
            assertSame(channelProxy, tor.channel());
            verify(r).channel();
            reset(r);
        }

        tor.closeReader();
        verify(r).closeReader();
        reset(r);

        tor.close();
        verify(r).close();
        reset(r);

        when(r.getCrcCheckChance()).thenReturn(0.123d);
        assertEquals(0.123d, tor.getCrcCheckChance(), 0);
        verify(r).getCrcCheckChance();
        reset(r);
    }

    /**
     * The source's skip arithmetic (here the holes of an encrypted file) must only apply before the cutoff; the tail
     * from the cutoff on is contiguous, like in {@link TailOverridingRebufferer#adjustPosition}.
     */
    @Test
    public void testPositionForSkip()
    {
        int maxBytesInPage = CHUNK_SIZE - 40;
        when(r.positionForSkip(anyLong(), anyInt()))
        .thenAnswer(invocation -> EncryptedChunkReader.positionForSkip(invocation.getArgument(0), invocation.getArgument(1), maxBytesInPage));
        // a source extending past both cutoffs below
        when(r.remainingBytes(anyLong()))
        .thenAnswer(invocation -> EncryptedChunkReader.remainingBytes(invocation.getArgument(0), 4L * CHUNK_SIZE, maxBytesInPage));
        when(r.adjustPosition(anyLong())).thenAnswer(invocation -> {
            long position = invocation.getArgument(0);
            return (position & (CHUNK_SIZE - 1)) < maxBytesInPage ? position : position - maxBytesInPage + CHUNK_SIZE;
        });
        long holeStart = CHUNK_SIZE + maxBytesInPage; // the usable end of the second chunk

        // cutoff at a chunk start
        long cutoff = 2L * CHUNK_SIZE;
        Rebufferer tor = new TailOverridingRebufferer(r, cutoff, tail.duplicate());
        // before the cutoff: the source's arithmetic, including holes
        assertEquals(30, tor.positionForSkip(10, 20));
        assertEquals(CHUNK_SIZE + 5, tor.positionForSkip(maxBytesInPage - 5, 10));
        // ending at the usable end of the chunk before the cutoff: the start of its hole, which adjustPosition moves
        // to the cutoff
        assertEquals(holeStart, tor.positionForSkip(holeStart - 10, 10));
        assertEquals(cutoff, tor.adjustPosition(holeStart));
        assertEquals(cutoff + 5, tor.positionForSkip(holeStart - 10, 15));
        // across the cutoff: the bytes after it are contiguous
        assertEquals(cutoff + maxBytesInPage + 7, tor.positionForSkip(holeStart - 10, 10 + maxBytesInPage + 7));
        assertEquals(cutoff + 2L * CHUNK_SIZE, tor.positionForSkip(maxBytesInPage - 5, 5 + maxBytesInPage + 2 * CHUNK_SIZE));
        // after the cutoff
        assertEquals(cutoff + 3 + 2L * CHUNK_SIZE, tor.positionForSkip(cutoff + 3, 2 * CHUNK_SIZE));
        assertEquals(cutoff, tor.positionForSkip(cutoff, 0));

        // a cutoff at the usable end (hole start) of a chunk, which the writer does not produce (the cutoff comes from
        // paddedPosition(), always chunk-aligned), for robustness
        cutoff = holeStart;
        tor = new TailOverridingRebufferer(r, cutoff, tail.duplicate());
        assertEquals(cutoff, tor.positionForSkip(cutoff - 10, 10));
        assertEquals(cutoff + 1, tor.positionForSkip(cutoff - 10, 11));
        assertEquals(cutoff + 2L * CHUNK_SIZE, tor.positionForSkip(cutoff - 10, 10 + 2 * CHUNK_SIZE));
        assertEquals(cutoff + 2L * CHUNK_SIZE, tor.positionForSkip(cutoff, 2 * CHUNK_SIZE));
    }

    /**
     * The content before the cutoff is counted with the source's arithmetic (holes excluded), up to the source's
     * length, and the tail after it is contiguous.
     */
    @Test
    public void testRemainingBytes()
    {
        int maxBytesInPage = CHUNK_SIZE - 40;
        long holeStart = CHUNK_SIZE + maxBytesInPage; // the usable end of the second chunk
        long[] sourceLength = { holeStart }; // the writer's last content position when the index was opened early
        when(r.remainingBytes(anyLong()))
        .thenAnswer(invocation -> EncryptedChunkReader.remainingBytes(invocation.getArgument(0), sourceLength[0], maxBytesInPage));
        when(r.positionForSkip(anyLong(), anyInt()))
        .thenAnswer(invocation -> EncryptedChunkReader.positionForSkip(invocation.getArgument(0), invocation.getArgument(1), maxBytesInPage));
        int tailLength = 3 * CHUNK_SIZE;
        ByteBuffer longTail = ByteBuffer.allocate(tailLength);

        // cutoff at a chunk start, the source ending at the hole start of the previous chunk
        long cutoff = 2L * CHUNK_SIZE;
        Rebufferer tor = new TailOverridingRebufferer(r, cutoff, longTail.duplicate());
        assertEquals(cutoff + tailLength, tor.fileLength());
        // before the cutoff, across the source's holes
        assertEquals(2L * maxBytesInPage - 10 + tailLength, tor.remainingBytes(10));
        assertEquals(5 + maxBytesInPage + tailLength, tor.remainingBytes(maxBytesInPage - 5));
        assertEquals(10 + tailLength, tor.remainingBytes(holeStart - 10));
        assertEquals(tailLength, tor.remainingBytes(holeStart));
        // skipping everything lands at the end of the tail
        assertEquals(cutoff + tailLength, tor.positionForSkip(10, (int) tor.remainingBytes(10)));
        assertEquals(cutoff + 1, tor.positionForSkip(holeStart, 1));
        // from the cutoff on
        assertEquals(tailLength, tor.remainingBytes(cutoff));
        assertEquals(tailLength - 3, tor.remainingBytes(cutoff + 3));
        assertEquals(0, tor.remainingBytes(cutoff + tailLength));
        assertEquals(0, tor.remainingBytes(cutoff + tailLength + 1));
        // a source extending past the cutoff does not change the counts
        sourceLength[0] = 3L * CHUNK_SIZE + maxBytesInPage;
        assertEquals(2L * maxBytesInPage - 10 + tailLength, tor.remainingBytes(10));
        assertEquals(tailLength, tor.remainingBytes(holeStart));

        // a cutoff at the hole start of a chunk, which the writer does not produce (the cutoff comes from
        // paddedPosition(), always chunk-aligned), for robustness
        cutoff = holeStart;
        sourceLength[0] = holeStart;
        tor = new TailOverridingRebufferer(r, cutoff, longTail.duplicate());
        assertEquals(2L * maxBytesInPage - 5 + tailLength, tor.remainingBytes(5));
        assertEquals(10 + tailLength, tor.remainingBytes(cutoff - 10));
        assertEquals(tailLength, tor.remainingBytes(cutoff));
        assertEquals(tailLength - 1, tor.remainingBytes(cutoff + 1));
        assertEquals(cutoff + tailLength, tor.positionForSkip(5, (int) tor.remainingBytes(5)));
        assertEquals(cutoff, tor.positionForSkip(cutoff - 10, 10));
    }
}
