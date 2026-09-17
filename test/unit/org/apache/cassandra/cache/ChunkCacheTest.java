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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Throwables;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.memory.BufferPool;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;

import static org.apache.cassandra.utils.PageAware.PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunkCacheTest
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkCacheTest.class);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.enableChunkCache(512);
    }

    @Test
    public void testRandomAccessReaderCanUseCache() throws IOException
    {
        File file = FileUtils.createTempFile("foo", null);
        file.deleteOnExit();

        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());
        assertEquals(0, ChunkCache.instance.sizeOfFile(file));

        try (SequentialWriter writer = new SequentialWriter(file))
        {
            writer.write(new byte[64]);
            writer.flush();
        }

        try (FileHandle.Builder builder = new FileHandle.Builder(file).withChunkCache(ChunkCache.instance);
             FileHandle h = builder.complete();
             RandomAccessReader r = h.createReader())
        {
            r.reBuffer();

            assertEquals(1, ChunkCache.instance.size());
            assertEquals(1, ChunkCache.instance.sizeOfFile(file));
        }

        // We do not invalidate the file on close
    }

    @Test
    public void testInvalidateFileNotInCache()
    {
        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());
        ChunkCache.instance.invalidateFile(new File("/tmp/does/not/exist/in/cache/or/on/file/system"));
    }

    @Test
    public void testRandomAccessReadersWithUpdatedFileAndMultipleChunksAndCacheInvalidation() throws IOException
    {
        File file = FileUtils.createTempFile("foo", null);
        file.deleteOnExit();

        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());
        assertEquals(0, ChunkCache.instance.sizeOfFile(file));

        writeBytes(file, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE * 3]);

        try (FileHandle.Builder builder1 = new FileHandle.Builder(file).withChunkCache(ChunkCache.instance))
        {
             try (FileHandle handle1 = builder1.complete();
                  RandomAccessReader reader1 = handle1.createReader())
             {
                 // Read 2 chunks and verify contents
                 for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 2; i++)
                     assertEquals((byte) 0, reader1.readByte());

                 // Overwrite the file's contents
                 var bytes = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE * 3];
                 Arrays.fill(bytes, (byte) 1);
                 writeBytes(file, bytes);

                 // Verify rebuffer pulls from cache for first 2 bytes and then from disk for third byte
                 reader1.seek(0);
                 for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 2; i++)
                     assertEquals((byte) 0, reader1.readByte());
                 // Trigger read of next chunk and see it is the new data
                 assertEquals((byte) 1, reader1.readByte());

                 assertEquals(3, ChunkCache.instance.size());
                 assertEquals(3, ChunkCache.instance.sizeOfFile(file));
             }

            // Invalidate cache for both chunks
            ChunkCache.instance.invalidateFile(file);

            // Verify cache does not contain an entry for the file
            assertEquals(0, ChunkCache.instance.sizeOfFile(file));

            // Existing handles and readers keep using the old file id. To make sure we get a new one, recreate the
            // handle and reader.
            try (FileHandle handle2 = builder1.complete();
                 RandomAccessReader reader2 = handle2.createReader())
            {
                for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 3; i++)
                    assertEquals((byte) 1, reader2.readByte());
                assertEquals(3, ChunkCache.instance.sizeOfFile(file));
            }
        }

        // We do not invalidate the file on close
    }

    @Test
    public void testRandomAccessReadersForDifferentFilesWithCacheInvalidation() throws IOException
    {
        File fileFoo = FileUtils.createTempFile("foo", null);
        fileFoo.deleteOnExit();
        File fileBar = FileUtils.createTempFile("bar", null);
        fileBar.deleteOnExit();

        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());
        assertEquals(0, ChunkCache.instance.sizeOfFile(fileFoo));
        assertEquals(0, ChunkCache.instance.sizeOfFile(fileBar));

        writeBytes(fileFoo, new byte[64]);
        // Write different bytes for meaningful content validation
        var barBytes = new byte[64];
        Arrays.fill(barBytes, (byte) 1);
        writeBytes(fileBar, barBytes);

        try (FileHandle.Builder builderFoo = new FileHandle.Builder(fileFoo).withChunkCache(ChunkCache.instance);
             FileHandle handleFoo = builderFoo.complete();
             RandomAccessReader readerFoo = handleFoo.createReader())
        {
            assertEquals((byte) 0, readerFoo.readByte());

            assertEquals(1, ChunkCache.instance.size());
            assertEquals(1, ChunkCache.instance.sizeOfFile(fileFoo));

            try (FileHandle.Builder builderBar = new FileHandle.Builder(fileBar).withChunkCache(ChunkCache.instance);
                 FileHandle handleBar = builderBar.complete();
                 RandomAccessReader readerBar = handleBar.createReader())
            {
                assertEquals((byte) 1, readerBar.readByte());

                assertEquals(2, ChunkCache.instance.size());
                assertEquals(1, ChunkCache.instance.sizeOfFile(fileFoo));
                assertEquals(1, ChunkCache.instance.sizeOfFile(fileBar));

                // Invalidate fileFoo and verify that only fileFoo's chunks are removed
                ChunkCache.instance.invalidateFile(fileFoo);
                assertEquals(0, ChunkCache.instance.sizeOfFile(fileFoo));
                assertEquals(1, ChunkCache.instance.sizeOfFile(fileBar));
            }
        }
        // We do not invalidate the file on close
    }

    private void writeBytes(File file, byte[] bytes) throws IOException
    {
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            writer.write(bytes);
            writer.flush();
        }
    }

    static final class MockFileControl implements AutoCloseable
    {
        final File file;
        final int fileSize;
        FileChannel channel;
        FileHandle fileHandle;
        ChannelProxy proxy;
        RandomAccessReader reader;
        ChunkCache chunkCache;
        volatile boolean reading;

        CompletableFuture<?> waitOnRead = new CompletableFuture<>();

        public MockFileControl(File file, int fileSize, ChunkCache chunkCache) throws Exception
        {
            this.file = file;
            this.fileSize = fileSize;
            this.chunkCache = chunkCache;
        }

        @Override
        public void close() throws Exception
        {
            if (reader != null)
                reader.close();
            if (fileHandle != null)
                fileHandle.close();
            if (channel != null)
                channel.close();
        }

        void createFile() throws Exception
        {
            file.deleteOnExit();

            try (SequentialWriter writer = new SequentialWriter(file))
            {
                writer.write(new byte[fileSize]);
                writer.flush();
            }
        }

        RandomAccessReader openReader() throws Exception
        {
            assert reader == null;
            channel = spy(FileChannel.class);
            when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {

                reading = true;
                logger.info("Waiting on read for file {}", file.path());
                // this allows us to introduce a delay or a failure in the read
                waitOnRead.join();
                logger.info("Read completed for file {}", file.path());
                reading = false;


                ByteBuffer buffer = invocation.getArgument(0);
                long position = invocation.getArgument(1);
                int writen = buffer.remaining();
                buffer.put(new byte[writen]);
                return writen;
            });
            when(channel.size()).thenReturn(Long.valueOf(fileSize));

            proxy = new ChannelProxy(file, channel);
            FileHandle.Builder builder = new FileHandle.Builder(proxy)
                                         .withChunkCache(chunkCache);
            fileHandle = builder.complete();
            reader = fileHandle.createReader();

            return reader;
        }
    }

    /**
     * This test asserts that in case of multiple threads reading from multiple files, the reads for one file
     * are not blocked by the reads for another file.
     * This is something that can happen on CNDB because we read data from the network (S3 or Storage Service)
     * and it can be slow (or fail after some timeout).
     */
    @Test
    public void testBlockReadsMultipleThreads() throws Exception
    {
        ChunkCache chunkCache = ChunkCache.instance;
        chunkCache.clear();
        assertEquals(0, chunkCache.size());
        int numFiles = 64;
        int fileSize = 64;

        // reading from 1 file is very slow (blocked until we signal it to continue)
        int slowFileIndex = 5;

        MockFileControl[] files = new MockFileControl[numFiles];
        try
        {
            for (int i = 0; i < numFiles; i++)
            {
                File file = FileUtils.createTempFile("foo" + i, ".tmp");
                MockFileControl mockFileControl = new MockFileControl(file, fileSize, chunkCache);
                files[i] = mockFileControl;
                mockFileControl.createFile();
                if (i != slowFileIndex)
                {
                    mockFileControl.waitOnRead.complete(null);
                }
                assertEquals(0, chunkCache.sizeOfFile(file));
            }

            ExecutorService threadPool = Executors.newFixedThreadPool(numFiles);

            Future<?>[] results = new Future[numFiles];
            for (int i = 0; i < numFiles; i++)
            {
                MockFileControl mockFileControl = files[i];
                RandomAccessReader r = mockFileControl.openReader();
                File file = mockFileControl.file;

                results[i] = threadPool.submit(() -> {
                    r.reBuffer();
                    assertEquals(1, chunkCache.sizeOfFile(file));
                });
            }

            // ensure that all the threads were able to complete, even if one was slow
            for (int i = 0; i < numFiles; i++)
            {
                if (i != slowFileIndex)
                {
                    results[i].get();
                }
            }

            // let the slow file finish
            files[slowFileIndex].waitOnRead.complete(null);
            results[slowFileIndex].get();
        }
        finally
        {
            for (MockFileControl file : files)
            {
                if (file != null)
                {
                    file.close();
                }
            }
        }
    }

    /**
     * This test asserts that in case of multiple threads reading from multiple files, the reads for one file
     * are not blocked by the reads for another file.
     * This is something that can happen on CNDB because we read data from the network (S3 or Storage Service)
     * and it can be slow (or fail after some timeout).
     *
     * @throws Exception
     */
    @Test
    public void testNotCacheOnReadErrors() throws Exception
    {
        BufferPool pool = mock(BufferPool.class);
        CopyOnWriteArrayList<ByteBuffer> allocated = new CopyOnWriteArrayList<>();
        when(pool.get(anyInt(), any(BufferType.class))).thenAnswer(invocation -> {
            int size = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            allocated.add(buffer);
            return buffer;
        });

        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            allocated.remove(buffer);
            return true;
        }).when(pool).put(any(ByteBuffer.class));
        ChunkCache chunkCache = new ChunkCache(pool, 512, ChunkCacheMetrics::create);

        assertEquals(0, chunkCache.size());
        int fileSize = 64;
        File file1 = FileUtils.createTempFile("foo1", ".tmp");
        File file2 = FileUtils.createTempFile("foo2", ".tmp");
        try (MockFileControl mockFileControl1 = new MockFileControl(file1, fileSize, chunkCache);
             MockFileControl mockFileControl2 = new MockFileControl(file2, fileSize, chunkCache);)
        {

            mockFileControl1.createFile();
            mockFileControl2.createFile();

            // file 1 has an error during read, we shouldn't cache the handle
            mockFileControl1.waitOnRead.completeExceptionally(new RuntimeException("some weird runtime error"));
            RandomAccessReader r1 = mockFileControl1.openReader();
            assertThrows(FSReadError.class, r1::reBuffer);
            assertEquals(0, chunkCache.sizeOfFile(mockFileControl1.file));
            assertEquals(0, chunkCache.size());
            assertEquals(0, allocated.size());

            // file 2 works fine, we should cache the handle
            mockFileControl2.waitOnRead.complete(null);
            RandomAccessReader r2 = mockFileControl2.openReader();
            r2.reBuffer();
            assertEquals(1, chunkCache.sizeOfFile(mockFileControl2.file));
            assertEquals(1, chunkCache.size());
            assertEquals(1, allocated.size());
        }
    }

    @Test
    public void testRacingReaders() throws Exception
    {
        testRacingReaders(false);
    }

    @Test
    public void testRacingReadersWithError() throws Exception
    {
        testRacingReaders(true);
    }

    private void testRacingReaders(boolean injectReadError) throws Exception
    {
        BufferPool pool = mock(BufferPool.class);
        CopyOnWriteArrayList<ByteBuffer> allocated = new CopyOnWriteArrayList<>();
        when(pool.get(anyInt(), any(BufferType.class))).thenAnswer(invocation -> {
            int size = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            allocated.add(buffer);
            return buffer;
        });

        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            allocated.remove(buffer);
            return true;
        }).when(pool).put(any(ByteBuffer.class));

        ChunkCache chunkCache = new ChunkCache(pool, 512, ChunkCacheMetrics::create);
        assertEquals(chunkCache.size(), 0);
        int fileSize = 64;
        File file1 = FileUtils.createTempFile("foo1", ".tmp");
        try (MockFileControl mockFileControl1 = new MockFileControl(file1, fileSize, chunkCache);
             MockFileControl mockFileControl2 = new MockFileControl(file1, fileSize, chunkCache);)
        {

            mockFileControl1.createFile();

            RandomAccessReader r1 = mockFileControl1.openReader();
            RandomAccessReader r2 = mockFileControl2.openReader();

            // start 2 threads that will try to read from the same file, the same chunk
            // they are racing to cache the chunk
            CompletableFuture<?> thread1 = CompletableFuture.runAsync(r1::reBuffer);

            Awaitility.await().until(() -> mockFileControl1.reading);
            assertEquals(allocated.size(), 1);

            CompletableFuture<?> thread2 = CompletableFuture.runAsync(r2::reBuffer);
            if (injectReadError)
            {
                RuntimeException error = new RuntimeException("some weird runtime error");
                mockFileControl1.waitOnRead.completeExceptionally(error);
                assertSame(error, Throwables.getRootCause(assertThrows(CompletionException.class, thread1::join)));
                assertSame(error, Throwables.getRootCause(assertThrows(CompletionException.class, thread2::join)));
                // assert that we didn't leak the buffer
                assertEquals(0, allocated.size());
                assertEquals(0, chunkCache.size());
            }
            else
            {
                mockFileControl1.waitOnRead.complete(null);
                thread1.join();
                thread2.join();
                // assert that we have only 1 buffer allocated
                assertEquals(1, allocated.size());
                assertEquals(1, chunkCache.size());
            }

            assertTrue(mockFileControl1.waitOnRead.isDone());
            // assert that thread2 never performed the read
            assertFalse(mockFileControl2.waitOnRead.isDone());
        }

        assertEquals(0, ChunkCache.instance.sizeOfFile(file1));
    }

    @Test
    public void tstDontCacheErroredReads() throws Exception
    {
        BufferPool pool = mock(BufferPool.class);
        CopyOnWriteArrayList<ByteBuffer> allocated = new CopyOnWriteArrayList<>();
        when(pool.get(anyInt(), any(BufferType.class))).thenAnswer(invocation -> {
            int size = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            allocated.add(buffer);
            return buffer;
        });

        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            allocated.remove(buffer);
            return true;
        }).when(pool).put(any(ByteBuffer.class));

        ChunkCache chunkCache = new ChunkCache(pool, 512, ChunkCacheMetrics::create);
        assertEquals(0, chunkCache.size());
        int fileSize = 64;
        File file1 = FileUtils.createTempFile("foo1", ".tmp");
        try (MockFileControl mockFileControl1 = new MockFileControl(file1, fileSize, chunkCache);
             MockFileControl mockFileControl2 = new MockFileControl(file1, fileSize, chunkCache);)
        {

            mockFileControl1.createFile();

            RandomAccessReader r1 = mockFileControl1.openReader();
            RandomAccessReader r2 = mockFileControl2.openReader();

            // start 2 threads that will try to read from the same file, the same chunk
            // they are racing to cache the chunk
            CompletableFuture<?> thread1 = CompletableFuture.runAsync(r1::reBuffer);

            Awaitility.await().until(() -> mockFileControl1.reading);
            assertEquals(1, allocated.size());

            // in this case thread1 errors before thread2 starts to read
            RuntimeException error = new RuntimeException("some weird runtime error");
            mockFileControl1.waitOnRead.completeExceptionally(error);
            assertThatThrownBy(thread1::join).hasCauseInstanceOf(FSReadError.class);

            // assert that we didn't leak the buffer
            assertEquals(0, allocated.size());
            assertEquals(0, chunkCache.size());

            // assert that the cache didn't cache the CompletableFuture that completed exceptionally the first time
            CompletableFuture<?> thread2 = CompletableFuture.runAsync(r2::reBuffer);
            mockFileControl2.waitOnRead.complete(null);
            // threads2 completes without error
            thread2.join();
            // assert that we have only 1 buffer allocated
            assertEquals(1, allocated.size());
            assertEquals(1, chunkCache.size());

            assertTrue(mockFileControl1.waitOnRead.isDone());
            // assert that thread2 performed the read
            assertTrue(mockFileControl2.waitOnRead.isDone());
        }

        assertEquals(0, ChunkCache.instance.sizeOfFile(file1));
    }

    /**
     * Realistic compression chunk length below {@link PageAware#PAGE_SIZE} (e.g. {@code chunk_length_in_kb: 1}).
     * {@link ChunkCache#newChunk} always reserves a full page for such sizes and must free that full page.
     */
    private static final int SMALL_CHUNK_SIZE = 1024;

    /**
     * For chunks smaller than {@link PageAware#PAGE_SIZE}, {@link ChunkCache#newChunk} still reserves a whole
     * page from the pool, encodes the logical size in the owned buffer's limit, and exposes a
     * {@code duplicate().slice()} view for reads (see {@code SingleRegionChunk}). This test verifies that:
     * <ul>
     *   <li>the pool sees reservation/release of the *full* page (not the narrowed chunk size),</li>
     *   <li>the read view has {@code capacity == chunkSize},</li>
     *   <li>repeated alloc/release cycles leave used-memory and free-slot accounting unchanged
     *       (the pre-fix bug permanently stuck BufferPool slots and drifted {@code memoryInUse}).</li>
     * </ul>
     */
    @Test
    public void testSmallChunkPoolAllocation()
    {
        // Snapshot the released buffer's capacity at call-time before the pool recycles the object
        // (LocalPool.put recycles the buffer object by zeroing its address/capacity via Unsafe, so reading
        // capacity() from the ArgumentCaptor value *after* put() returns always yields 0).
        int[] capturedCapacity = { -1 };
        BufferPool pool = spy(new BufferPool("small_chunk_pool_test", 8 * 1024 * 1024, true));
        doAnswer(invocation -> {
            capturedCapacity[0] = ((ByteBuffer) invocation.getArgument(0)).capacity();
            return invocation.callRealMethod();
        }).when(pool).put(any(ByteBuffer.class));

        ChunkCache chunkCache = new ChunkCache(pool, 512, ChunkCacheMetrics::create);

        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();
        assertEquals(0, overflowBefore);

        ChunkCache.Chunk chunk = chunkCache.newChunk(SMALL_CHUNK_SIZE, 0);
        assertEquals(PAGE_SIZE, chunk.capacity());
        // Read view must stay narrowed to the requested chunk size (alignment / readChunk capacity asserts).
        assertEquals(SMALL_CHUNK_SIZE, ((Rebufferer.BufferHolder) chunk).buffer().capacity());

        // A full page must have been reserved from the pool, not just chunkSize.
        assertEquals(usedBefore + PAGE_SIZE, pool.usedSizeInBytes());
        assertEquals(overflowBefore, pool.overflowMemoryInBytes());

        chunk.release();

        // The buffer handed back to the pool must be the full page-sized buffer, not a narrowed slice.
        // Capacity is snapshotted at call time because the pool zeroes the buffer object on recycle.
        verify(pool).put(any(ByteBuffer.class));
        assertEquals(PAGE_SIZE, capturedCapacity[0]);

        // The pool must be back to its exact pre-allocation state.
        assertEquals(usedBefore, pool.usedSizeInBytes());
        assertEquals(overflowBefore, pool.overflowMemoryInBytes());

        // Churn: the old path only freed roundUp(chunkSize) slots (e.g. 1 of 2 for a 4 KiB page) and
        // under-decremented memoryInUse, so usedSize would climb and slots would remain stuck allocated.
        final int cycles = 256;
        for (int i = 0; i < cycles; i++)
        {
            ChunkCache.Chunk c = chunkCache.newChunk(SMALL_CHUNK_SIZE, i * (long) SMALL_CHUNK_SIZE);
            assertEquals(PAGE_SIZE, c.capacity());
            c.release();
        }
        assertEquals("used memory must not drift after repeated small-chunk cycles",
                     usedBefore, pool.usedSizeInBytes());
        assertEquals("overflow must stay unused on the pooled path",
                     overflowBefore, pool.overflowMemoryInBytes());
        // usedSizeInBytes is driven by buffer.capacity() on put/get; the old under-sized free() left
        // both memoryInUse and free-slot bits stuck. Returning exactly to baseline means full pages were freed.
    }

    /**
     * Same as {@link #testSmallChunkPoolAllocation()}, but forces the pool's overflow (unpooled) allocation
     * path by using a {@link BufferPool} whose memory threshold is {@code 0}, so every {@code get()} falls
     * back to a raw {@code ByteBuffer.allocateDirect()} (see {@code BufferPool.LocalPool.get()}).
     * <p/>
     * Verifies that the buffer released back to the pool is the original, full-page buffer -- which still owns
     * a real {@code Cleaner} -- rather than the narrowed slice used for reads (a slice never has its own
     * {@code Cleaner}, so releasing it would silently fail to free the native memory). Also verifies that the
     * overflow memory accounting returns exactly to its pre-allocation baseline once the chunk is released.
     */
    @Test
    public void testSmallChunkOverflowAllocations() throws Exception
    {
        Method cleanerMethod = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");

        // A pool with a zero memory threshold can never allocate a macro-chunk, so every get() is guaranteed
        // to fall back to the raw, unpooled allocate() overflow path.
        BufferPool overflowPool = spy(new BufferPool("small_chunk_overflow_test", 0, true));
        ChunkCache chunkCache = new ChunkCache(overflowPool, 512, ChunkCacheMetrics::create);

        long usedBefore = overflowPool.usedSizeInBytes();
        long overflowBefore = overflowPool.overflowMemoryInBytes();
        assertEquals(0, usedBefore);
        assertEquals(0, overflowBefore);

        ChunkCache.Chunk chunk = chunkCache.newChunk(SMALL_CHUNK_SIZE, 0);
        assertEquals(PAGE_SIZE, chunk.capacity());
        assertEquals(SMALL_CHUNK_SIZE, ((Rebufferer.BufferHolder) chunk).buffer().capacity());

        // Confirm the overflow path was really taken, and that the full page was tracked as overflow usage.
        assertEquals(overflowBefore + PAGE_SIZE, overflowPool.overflowMemoryInBytes());
        assertEquals(usedBefore + PAGE_SIZE, overflowPool.usedSizeInBytes());

        chunk.release();

        ArgumentCaptor<ByteBuffer> released = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(overflowPool).put(released.capture());
        ByteBuffer releasedBuffer = released.getValue();

        // The released buffer must be the original, full-page buffer, which owns a real Cleaner -- unlike a
        // narrowed slice of it, which would silently fail to free the underlying native memory.
        assertEquals(PAGE_SIZE, releasedBuffer.capacity());
        assertNotNull("The released overflow buffer should own a real Cleaner so put() can actually free it",
                       cleanerMethod.invoke(releasedBuffer));

        // Accounting must return exactly to its pre-allocation baseline.
        assertEquals(overflowBefore, overflowPool.overflowMemoryInBytes());
        assertEquals(usedBefore, overflowPool.usedSizeInBytes());
    }

    /**
     * Production-symptom regression: under sustained overflow-path allocate/release of small chunks,
     * {@code overflowMemoryUsage} must not climb. The pre-fix path released a capacity-{@code chunkSize}
     * slice of an allocated page, so each cycle leaked {@code PAGE_SIZE - chunkSize} from both native
     * memory (slice has no Cleaner) and the overflow counter -- matching weeks-long growth until direct OOM.
     */
    @Test
    public void testSmallChunkOverflowChurnDoesNotGrowOverflowMemory()
    {
        BufferPool overflowPool = new BufferPool("small_chunk_overflow_churn", 0, true);
        ChunkCache chunkCache = new ChunkCache(overflowPool, 512, ChunkCacheMetrics::create);

        long usedBefore = overflowPool.usedSizeInBytes();
        long overflowBefore = overflowPool.overflowMemoryInBytes();
        assertEquals(0, usedBefore);
        assertEquals(0, overflowBefore);

        final int cycles = 512;
        for (int i = 0; i < cycles; i++)
        {
            ChunkCache.Chunk chunk = chunkCache.newChunk(SMALL_CHUNK_SIZE, i * (long) SMALL_CHUNK_SIZE);
            assertEquals(PAGE_SIZE, chunk.capacity());
            // Peak usage during the cycle must be a full page on the overflow path.
            assertEquals(PAGE_SIZE, overflowPool.overflowMemoryInBytes());
            chunk.release();
            assertEquals("overflow must return to baseline after every release",
                         overflowBefore, overflowPool.overflowMemoryInBytes());
            assertEquals(usedBefore, overflowPool.usedSizeInBytes());
        }

        assertEquals(overflowBefore, overflowPool.overflowMemoryInBytes());
        assertEquals(usedBefore, overflowPool.usedSizeInBytes());
    }

    /**
     * End-to-end: small-chunk readers go through the cache, Caffeine weighs entries by full-page
     * {@link ChunkCache.Chunk#capacity()}, and eviction/invalidation returns every reserved page to the
     * pool. Without weighing by the allocated page (and putting that page back), small chunks under-weigh
     * the cache and releasing a narrowed view leaks pool/overflow memory.
     */
    @Test
    public void testSmallChunkCacheEvictionReleasesFullPage() throws IOException
    {
        // Cache large enough that RESERVED_POOL_SPACE_IN_MB still leaves room for a few pages of weight.
        final int cacheSizeMb = 64;
        BufferPool pool = new BufferPool("small_chunk_eviction_test", 8 * 1024 * 1024, true);
        ChunkCache chunkCache = new ChunkCache(pool, cacheSizeMb, ChunkCacheMetrics::create);

        long usedBefore = pool.usedSizeInBytes();
        long overflowBefore = pool.overflowMemoryInBytes();

        // Force SimpleChunkReader.chunkSize() == SMALL_CHUNK_SIZE via SliceDescriptor (see FileHandle.Builder).
        final int fileSize = SMALL_CHUNK_SIZE * 8;
        File file = FileUtils.createTempFile("small-chunk-cache", null);
        file.deleteOnExit();
        writeBytes(file, new byte[fileSize]);

        try (FileHandle.Builder builder = new FileHandle.Builder(file)
                                                        .withChunkCache(chunkCache)
                                                        .bufferSize(SMALL_CHUNK_SIZE)
                                                        .slice(new SliceDescriptor(0, fileSize, SMALL_CHUNK_SIZE));
             FileHandle handle = builder.complete();
             RandomAccessReader reader = handle.createReader())
        {
            for (int pos = 0; pos < fileSize; pos += SMALL_CHUNK_SIZE)
            {
                reader.seek(pos);
                byte[] buf = new byte[SMALL_CHUNK_SIZE];
                reader.readFully(buf);
            }

            assertTrue("expected multiple cached small chunks", chunkCache.sizeOfFile(file) > 1);
            // Weighted size must count full pages (capacity), not the narrowed read view.
            assertEquals((long) chunkCache.sizeOfFile(file) * PAGE_SIZE, chunkCache.weightedSize());
            assertEquals(usedBefore + (long) chunkCache.sizeOfFile(file) * PAGE_SIZE, pool.usedSizeInBytes());
            assertEquals(overflowBefore, pool.overflowMemoryInBytes());
        }

        // Drop every entry for this reader id; onRemoval must put the full-page pool buffer back.
        chunkCache.invalidateFileNow(file);
        Awaitility.await().untilAsserted(() -> assertEquals(0, chunkCache.sizeOfFile(file)));

        assertEquals(usedBefore, pool.usedSizeInBytes());
        assertEquals(overflowBefore, pool.overflowMemoryInBytes());
        chunkCache.close();
    }
}
