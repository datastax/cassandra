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

package org.apache.cassandra.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;
import org.github.jamm.MemoryMeter;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachingRebuffererTest
{
    private final int PAGE_SIZE = 4096;
    private File file;
    private ChunkReader chunkReader;
    private BufferPool bufferPool;
    private ChannelProxy blockingChannel;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        assumeNotNull(ChunkCache.instance);

        file = new File(java.io.File.createTempFile("CachingRebuffererTest", ""));
        file.deleteOnExit();

        blockingChannel = new ChannelProxy(file);
        chunkReader = Mockito.mock(ChunkReader.class);
        bufferPool = BufferPools.forChunkCache();

        when(chunkReader.chunkSize()).thenReturn(PAGE_SIZE);
        when(chunkReader.channel()).thenReturn(blockingChannel);
        when(chunkReader.type()).thenReturn(ChunkReader.ReaderType.SIMPLE);

        ChunkCache.instance.invalidateFile(file);
    }

    // Helper test to estimate the memory overhead caused by buffer cache
    @Ignore
    @Test
    public void calculateMemoryOverhead() throws InterruptedException
    {
        // Allocate 1,5M items
        long count = 1_500_000;

        class EmptyAllocatingChunkReader implements ChunkReader
        {
            public void readChunk(long chunkOffset, ByteBuffer toBuffer)
            {
            }

            public int chunkSize()
            {
                return PAGE_SIZE;
            }

            public BufferType preferredBufferType()
            {
                return BufferType.OFF_HEAP;
            }

            public Rebufferer instantiateRebufferer(boolean isScan)
            {
                return null;
            }

            public void invalidateIfCached(long position)
            {
                // do nothing
            }

            public long adjustPosition(long position)
            {
                return position;
            }

            public void close()
            {

            }

            public ChannelProxy channel()
            {
                return blockingChannel;
            }

            public long fileLength()
            {
                return count * PAGE_SIZE; // enough to cover all keys requested or else the chunk length will overflow
            }

            public double getCrcCheckChance()
            {
                return 0;
            }

            public ReaderType type()
            {
                return ReaderType.SIMPLE;
            }
        }

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(new EmptyAllocatingChunkReader()).instantiateRebufferer(false);
        final MemoryMeter memoryMeter = MemoryMeter.builder()
                                                   .withGuessing(MemoryMeter.Guess.UNSAFE)
                                                   .build();
        final long initialHeap = memoryMeter.measureDeep(ChunkCache.instance);
        System.out.println("initial deepSize = " + FBUtilities.prettyPrintMemory(initialHeap));

        // Cache them count times
        for (long i = 0; i < count; i++)
            rebufferer.rebuffer(i * PAGE_SIZE).release();

        long queriedSize = 1L * PAGE_SIZE * count;
        System.out.println("queriedSize = " + FBUtilities.prettyPrintMemory(queriedSize));

        long cachedCount = ChunkCache.instance.size();
        long cachedSize = ChunkCache.instance.weightedSize();

        System.out.println("cachedCount = " + cachedCount);
        System.out.println("cachedSize = " + FBUtilities.prettyPrintMemory(cachedSize));

        final long populatedHeap = memoryMeter.measureDeep(ChunkCache.instance);
        System.out.println("populated deepSize = " + FBUtilities.prettyPrintMemory(populatedHeap));
        System.out.println("deepSizeDelta/cachedCount = " + FBUtilities.prettyPrintBinary((populatedHeap - initialHeap) * 1.0 / cachedCount, "B", ""));

        ChunkCache.instance.clear();

        System.out.println("cleared deepSize = " + FBUtilities.prettyPrintMemory(memoryMeter.measureDeep(ChunkCache.instance)));
    }

    @Test
    public void testRebufferInSamePage()
    {
        when(chunkReader.chunkSize()).thenReturn(PAGE_SIZE);
        doNothing().when(chunkReader).readChunk(anyLong(), any());

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer(false);
        assertNotNull(rebufferer);

        for (int i = 0; i < PAGE_SIZE; i++)
        {
            Rebufferer.BufferHolder buffer = rebufferer.rebuffer(i);
            assertNotNull(buffer);
            assertEquals(PAGE_SIZE, buffer.buffer().capacity());
            assertEquals(0, buffer.offset());
            buffer.release();
        }

        verify(chunkReader, times(1)).readChunk(anyLong(), any());
    }

    @Test
    public void testRebufferSeveralPages()
    {
        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer(false);
        assertNotNull(rebufferer);

        doNothing().when(chunkReader).readChunk(anyLong(), any());

        final int numPages = 10;

        for (int j = 0; j < numPages; j++)
        {
            for (int i = j * PAGE_SIZE; i < (j + 1) * PAGE_SIZE; i ++)
            {
                Rebufferer.BufferHolder buffer = rebufferer.rebuffer(i);
                assertNotNull(buffer);
                assertEquals(PAGE_SIZE, buffer.buffer().capacity());
                assertEquals(j * PAGE_SIZE, buffer.offset());
                buffer.release();
            }
        }

        verify(chunkReader, times(numPages)).readChunk(anyLong(), any());
    }

    @Test
    public void testRebufferContendedPage() throws InterruptedException
    {
        final int numThreads = 15;
        final int numAttempts = 1024;
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final AtomicInteger numBuffers = new AtomicInteger(0);

        doAnswer(i -> {
            numBuffers.incrementAndGet();
            return null;
        }).when(chunkReader).readChunk(anyLong(), any());

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer(false);
        assertNotNull(rebufferer);

        // using NamedThreadFactory ensures that the threads are InlinedThreadLocalThread, which means the pool will cache buffers in a thread local stash
        ExecutorService executor = Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("testMultipleThreadsOneSizeSepAllocFree"));

        for (int j = 0; j < numThreads; j++)
        {
            executor.submit(() -> {
                try
                {
                    for (int i = 0; i < numAttempts; i++)
                    {
                        Rebufferer.BufferHolder buffer = rebufferer.rebuffer(0);
                        assertNotNull(buffer);
                        assertEquals(PAGE_SIZE, buffer.buffer().capacity());
                        assertEquals(0, buffer.offset());
                        buffer.release();

                        // removes the buffer from the cache, other threads should still be able to create a new one and
                        // insert it into the cache thanks to the ref. counting mechanism
                        ((ChunkCache.CachingRebufferer) rebufferer).invalidateIfCached(0);
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    error.set(t);
                }
                finally
                {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await(1, TimeUnit.MINUTES);
        assertNull(error.get());
        assertTrue(numBuffers.get() > 1); // there should be several buffers created, in the thousands, up to numThreads * numAttempts
    }

    @Test(expected = CorruptSSTableException.class)
    public void testExceptionInReadChunk()
    {
        doThrow(CorruptSSTableException.class).when(chunkReader).readChunk(anyLong(), any());

        Rebufferer rebufferer = ChunkCache.instance.maybeWrap(chunkReader).instantiateRebufferer(false);
        assertNotNull(rebufferer);

        rebufferer.rebuffer(0);
    }
}
