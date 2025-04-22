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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.mockito.Mockito;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class PrefetchingRebuffererTest
{
    private static final int PAGE_SIZE = 4096;

    private RebuffererFactory source;
    private final Map<Long, TestBufferHolder> buffers = new ConcurrentHashMap<>();

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.READ_PREFETCHING_SIZE_KB.setInt((PAGE_SIZE / 1024) * 2);
        CassandraRelevantProperties.READ_PREFETCHING_WINDOW.setDouble(0.5);
        DatabaseDescriptor.daemonInitialization();
    }

    private Rebufferer newMockedRebuffer()
    {
        Rebufferer mock = Mockito.mock(Rebufferer.class);
        when(mock.rebuffer(anyLong())).thenAnswer(inv -> buffer(inv.getArgument(0)));
        return mock;
    }

    Rebufferer.BufferHolder buffer(long offset)
    {
        TestBufferHolder ret = buffers.computeIfAbsent(offset, o -> new TestBufferHolder(ByteBuffer.allocate(PAGE_SIZE), o));
        ret.numRequested++;
        return ret;
    }

    @Before
    public void setUp() throws IOException
    {
        source = Mockito.mock(RebuffererFactory.class);
        when(source.chunkSize()).thenReturn(PAGE_SIZE);
        when(source.fileLength()).thenReturn(Long.MAX_VALUE);
        when(source.instantiateRebufferer(false)).thenAnswer(__ -> newMockedRebuffer());
        buffers.clear();
        PrefetchingRebufferer.metrics.reset();
    }

    @Test
    public void testPrefetchSamePage()
    {
        Rebufferer rebufferer = PrefetchingRebufferer.withPrefetching(source);
        assertNotNull(rebufferer);
        assertSame(PrefetchingRebufferer.class, rebufferer.getClass());

        for (int i = 0; i < PAGE_SIZE; i ++)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(i);
            assertNotNull(buf);
            assertEquals(0L, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();
        }

        rebufferer.closeReader();
        rebufferer.close();

        assertEquals(3, buffers.size()); // 1 requested and 2 prefetched
        assertTrue(buffers.containsKey(0L));
        assertTrue(buffers.containsKey((long)PAGE_SIZE));
        assertTrue(buffers.containsKey((long)PAGE_SIZE * 2));
        assertFalse(buffers.containsKey((long)PAGE_SIZE * 3)); // never prefetched

        // This test is not super realistic: by requesting the same page over and over, it is effectively "seeking
        // backward", which effectively drop all pretch, so we will re-fetch all buffers every time.
        assertEquals(PAGE_SIZE, buffers.get(0L).numRequested); // requested many times
        assertEquals(PAGE_SIZE, buffers.get((long)PAGE_SIZE).numRequested); // prefetched only once
        assertEquals(PAGE_SIZE, buffers.get((long)PAGE_SIZE * 2).numRequested); // prefetched only once

        for (TestBufferHolder buffer : buffers.values())
            assertTrue(buffer.released); // make sure closeReader is effective in releasing prefetched buffers

        verify(source, times(1)).close();
    }

    @Test
    public void testPrefetchAtPageBoundaries()
    {
        final int prefetchSize = 8;
        final int windowSize = 4;
        final int numPages = 10;
        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source, prefetchSize * PAGE_SIZE, (double)windowSize / prefetchSize);
        assertNotNull(rebufferer);

        for (int i = 0; i < numPages; i++)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(i * PAGE_SIZE);
            assertNotNull(buf);
            assertEquals(i * PAGE_SIZE, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();

            // Prefetching is trigger on a separate thread pool, and it should be fast but not necessarily immediate,
            // so give it some time.
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            for (int k = i; k <= i + windowSize; k ++)
            {
                long pos = (long)k * PAGE_SIZE;
                assertTrue("Should have buffer at position " + pos, buffers.containsKey(pos));
                assertEquals(1, buffers.get(pos).numRequested);
            }
        }

        rebufferer.closeReader();
        rebufferer.close();

        assertTrue(String.format("We should have prefetched at least num pages + window size (%d + %d = %d), but instead got %d", numPages, windowSize, numPages + windowSize, PrefetchingRebufferer.metrics.prefetched.getCount()),
                   numPages + windowSize <= PrefetchingRebufferer.metrics.prefetched.getCount());

        assertTrue(String.format("We should have not used at least window size (%d), not %d", windowSize, PrefetchingRebufferer.metrics.unused.getCount()),
                   windowSize <= PrefetchingRebufferer.metrics.unused.getCount());
    }

    @Test
    public void testSequentialAccess()
    {
        int numPages = 20;
        // Simulate a file with exactly that number of pages to make sure we handle a full scan of the file correctly,
        // including the end of the file.
        when(source.fileLength()).thenReturn((long)numPages * PAGE_SIZE);

        Rebufferer rebufferer = PrefetchingRebufferer.withPrefetching(source);
        assertNotNull(rebufferer);

        for (int i = 0; i < numPages; i++)
        {
            long offset = (long)i * PAGE_SIZE;
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(offset);
            assertNotNull(buf);
            assertEquals(offset, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());

            // This "simulate" some "processing" time for the buffer. Mostly, this ensures prefetched buffers are ready
            // when we try using them (prefetching is "near immediate" in those tests, but so is this loop if we don't
            // have that sleep).
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            buf.release();
        }

        rebufferer.closeReader();
        rebufferer.close();

        assertEquals(numPages, buffers.size());

        // We should only request every buffer once.
        for (int i = 0; i < numPages; i++)
        {
            TestBufferHolder buffer = buffers.get((long) i * PAGE_SIZE);
            assertNotNull(buffer);
            assertEquals(1, buffer.numRequested);
            assertTrue(buffer.released);
        }

        assertEquals(numPages, PrefetchingRebufferer.metrics.prefetched.getCount());

        assertEquals(0, PrefetchingRebufferer.metrics.notReady.getCount());

        assertEquals(0, PrefetchingRebufferer.metrics.unused.getCount());
        assertEquals(0, PrefetchingRebufferer.metrics.nonSequentialRequest.getCount());
    }

    @Test
    public void testNonSequentialAccess()
    {
        Rebufferer rebufferer = PrefetchingRebufferer.withPrefetching(source);
        assertNotNull(rebufferer);

        long[] offsets = new long[] { 0, PAGE_SIZE * 2, PAGE_SIZE };

        for (long offset : offsets)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(offset);
            assertNotNull(buf);
            assertEquals(offset, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();
        }

        rebufferer.closeReader();
        rebufferer.close();

        assertEquals(5, buffers.size());

        // requesting PAGE_SIZE after PAGE_SIZE * 2 is non-sequential
        assertEquals(1, PrefetchingRebufferer.metrics.nonSequentialRequest.getCount());

        for (int i = 0; i < 5; i++)
        {
            TestBufferHolder buffer = buffers.get((long) i * PAGE_SIZE);
            assertNotNull(buffer);
            // In order, we've requested indexes 0, 2 and then 1. We expect:
            // - 0 to fetch 0 and prefetch 1 and 2.
            // - 2 to find 2 prefetched above (having discared 1) and prefetch 3 and 4.
            // - 1 is a seek backward so it discard everything, fetch 1 and prefetch 2 and 3.
            // Overall, 0 and 4 are only requested once, but the other indexes are requested twice.
            assertEquals("For index " + i, i == 0 || i == 4 ? 1 : 2, buffer.numRequested);
            assertTrue(buffer.released);
        }

        // As mentioned above, we'd discarded 1 when getting 2, and then discard 3 and 4 when getting 1. And then
        // when we close the rebufferer, we discard 2 and 3.
        assertEquals(5, PrefetchingRebufferer.metrics.unused.getCount());
    }

    @Test
    public void testSkippingBuffers()
    {
        final int prefetchSize = 8;
        final int windowSize = 4;
        final int numPages = 24;
        PrefetchingRebufferer rebufferer = new PrefetchingRebufferer(source,
                                                                     prefetchSize * PAGE_SIZE,
                                                                     (double)windowSize / prefetchSize);
        assertNotNull(rebufferer);

        for (int i = 0; i < numPages; i+=2)
        {
            Rebufferer.BufferHolder buf = rebufferer.rebuffer(i * PAGE_SIZE);
            assertNotNull(buf);
            assertEquals(i * PAGE_SIZE, buf.offset());
            assertEquals(PAGE_SIZE, buf.buffer().capacity());
            buf.release();

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            for (int k = i; k <= i + windowSize; k ++)
            {
                assertTrue(buffers.containsKey((long)k * PAGE_SIZE));
                assertEquals(1, buffers.get((long)k * PAGE_SIZE).numRequested);
            }
        }

        rebufferer.closeReader();
        rebufferer.close();

        // we skip every other page so we requested this many buffers
        int numBuffersRequested = numPages / 2;

        assertTrue(String.format("We should have prefetched at least num pages + window size (%d + %d = %d), but instead got %d",
                                 numPages, windowSize, numPages + windowSize, PrefetchingRebufferer.metrics.prefetched.getCount()),
                   numBuffersRequested + windowSize <= PrefetchingRebufferer.metrics.prefetched.getCount());

        // each request discards 1 buffer
        assertTrue(String.format("We should have not used at least window size (%d), not %d",
                                 windowSize, PrefetchingRebufferer.metrics.unused.getCount()),
                   numBuffersRequested + windowSize <= PrefetchingRebufferer.metrics.unused.getCount());
    }

    @Test(expected = CorruptSSTableException.class)
    public void testExceptionInSourceRebuffer()
    {
        Rebufferer rebufferer = PrefetchingRebufferer.withPrefetching(source);
        assertNotNull(rebufferer);

        when(source.instantiateRebufferer(false)).thenAnswer(__ -> {
            Rebufferer mock = Mockito.mock(Rebufferer.class);
            when(mock.rebuffer(anyLong())).thenThrow(CorruptSSTableException.class);
            return mock;
        });

        rebufferer.rebuffer(0);
    }

    @Test
    public void testExceptionInSourceRebufferDuringPrefetch()
    {
        Rebufferer rebufferer = PrefetchingRebufferer.withPrefetching(source);
        assertNotNull(rebufferer);

        when(source.instantiateRebufferer(false)).thenAnswer(__ -> {
            Rebufferer mock = Mockito.mock(Rebufferer.class);
            when(mock.rebuffer(anyLong())).thenAnswer(inv -> {
                long pos = inv.getArgument(0);
                if (pos == PAGE_SIZE * 2)
                    throw new CorruptSSTableException(new RuntimeException("Corrupted"), "fakeFile");
                return buffer(pos);
            });
            return mock;
        });

        try
        {
            // We should have no issue until we _use_ the position that throw.
            rebufferer.rebuffer(0);
            rebufferer.rebuffer(PAGE_SIZE);

            assertThrows(CorruptSSTableException.class, () -> rebufferer.rebuffer(PAGE_SIZE * 2));
        }
        finally
        {
            rebufferer.closeReader();
            rebufferer.close();
        }
    }

    private static class TestBufferHolder implements Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;
        boolean released;
        long offset;
        int numRequested;

        TestBufferHolder(ByteBuffer buffer, long offset)
        {
            this.buffer = buffer;
            this.released = false;
            this.offset = offset;
        }

        @Override
        public ByteBuffer buffer()
        {
            return buffer;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public void release()
        {
            released = true;
        }

    }
}