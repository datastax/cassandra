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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.metrics.ChunkCacheMetrics;
import org.apache.cassandra.utils.memory.BufferPool;
import org.awaitility.Awaitility;

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

    @Test
    public void testInspectHotEntries() throws IOException
    {
        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());

        // Create multiple files and populate cache
        File file1 = FileUtils.createTempFile("hot1", null);
        file1.deleteOnExit();
        File file2 = FileUtils.createTempFile("hot2", null);
        file2.deleteOnExit();
        File file3 = FileUtils.createTempFile("hot3", null);
        file3.deleteOnExit();

        writeBytes(file1, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);
        writeBytes(file2, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);
        writeBytes(file3, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);

        Set<File> expectedFiles = new HashSet<>(Arrays.asList(file1, file2, file3));

        // Access files to populate cache
        try (FileHandle.Builder builder1 = new FileHandle.Builder(file1).withChunkCache(ChunkCache.instance);
             FileHandle handle1 = builder1.complete();
             RandomAccessReader reader1 = handle1.createReader();
             FileHandle.Builder builder2 = new FileHandle.Builder(file2).withChunkCache(ChunkCache.instance);
             FileHandle handle2 = builder2.complete();
             RandomAccessReader reader2 = handle2.createReader();
             FileHandle.Builder builder3 = new FileHandle.Builder(file3).withChunkCache(ChunkCache.instance);
             FileHandle handle3 = builder3.complete();
             RandomAccessReader reader3 = handle3.createReader())
        {
            reader1.reBuffer();
            reader2.reBuffer();
            reader3.reBuffer();

            assertEquals(3, ChunkCache.instance.size());

            // Inspect hot entries
            List<ChunkCache.ChunkCacheInspectionEntry> hotEntries = new ArrayList<>();
            ChunkCache.instance.inspectHotEntries(10, hotEntries::add);

            // Should have exactly 3 entries
            assertEquals(3, hotEntries.size());

            // Verify entries have valid data and match files we put in cache
            for (ChunkCache.ChunkCacheInspectionEntry entry : hotEntries)
            {
                assertNotNull("File should not be null", entry.file);
                assertTrue("File should be one we added to cache", expectedFiles.contains(entry.file));
                assertTrue("Position should be non-negative", entry.position >= 0);
                assertTrue("Size should be positive", entry.size > 0);
            }

            // Verify all files are represented
            Set<File> observedFiles = hotEntries.stream()
                                                .map(e -> e.file)
                                                .collect(Collectors.toSet());
            assertEquals("All cached files should appear in results", expectedFiles, observedFiles);
        }
    }

    @Test
    public void testInspectColdEntries() throws IOException
    {
        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());

        File file1 = FileUtils.createTempFile("cold1", null);
        file1.deleteOnExit();
        File file2 = FileUtils.createTempFile("cold2", null);
        file2.deleteOnExit();

        writeBytes(file1, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);
        writeBytes(file2, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);

        Set<File> expectedFiles = new HashSet<>(Arrays.asList(file1, file2));

        try (FileHandle.Builder builder1 = new FileHandle.Builder(file1).withChunkCache(ChunkCache.instance);
             FileHandle handle1 = builder1.complete();
             RandomAccessReader reader1 = handle1.createReader();
             FileHandle.Builder builder2 = new FileHandle.Builder(file2).withChunkCache(ChunkCache.instance);
             FileHandle handle2 = builder2.complete();
             RandomAccessReader reader2 = handle2.createReader())
        {
            reader1.reBuffer();
            reader2.reBuffer();

            assertEquals(2, ChunkCache.instance.size());

            // Inspect cold entries
            List<ChunkCache.ChunkCacheInspectionEntry> coldEntries = new ArrayList<>();
            ChunkCache.instance.inspectColdEntries(10, coldEntries::add);

            assertEquals(2, coldEntries.size());

            // Verify entries have valid data and match files we put in cache
            for (ChunkCache.ChunkCacheInspectionEntry entry : coldEntries)
            {
                assertNotNull("File should not be null", entry.file);
                assertTrue("File should be one we added to cache", expectedFiles.contains(entry.file));
                assertTrue("Position should be non-negative", entry.position >= 0);
                assertTrue("Size should be positive", entry.size > 0);
            }

            // Verify all files are represented
            Set<File> observedFiles = coldEntries.stream()
                                                 .map(e -> e.file)
                                                 .collect(Collectors.toSet());
            assertEquals("All cached files should appear in results", expectedFiles, observedFiles);
        }
    }

    @Test
    public void testInspectEntriesWithLimit() throws IOException
    {
        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());

        File file = FileUtils.createTempFile("limitTest", null);
        file.deleteOnExit();
        writeBytes(file, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE * 5]);

        try (FileHandle.Builder builder = new FileHandle.Builder(file).withChunkCache(ChunkCache.instance);
             FileHandle handle = builder.complete();
             RandomAccessReader reader = handle.createReader())
        {
            // Read all chunks to populate cache
            for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 5; i++)
                reader.readByte();

            assertEquals(5, ChunkCache.instance.size());

            // Test with limit smaller than cache size
            List<ChunkCache.ChunkCacheInspectionEntry> limitedEntries = new ArrayList<>();
            ChunkCache.instance.inspectHotEntries(2, limitedEntries::add);

            // Should respect the limit
            assertEquals(2, limitedEntries.size());

            // Verify all entries are from the same file and have valid data
            for (ChunkCache.ChunkCacheInspectionEntry entry : limitedEntries)
            {
                assertNotNull("File should not be null", entry.file);
                assertEquals("Should be from the test file", file, entry.file);
                assertTrue("Position should be non-negative", entry.position >= 0);
                assertTrue("Size should be positive", entry.size > 0);
            }
        }
    }

    @Test
    public void testInspectEntriesWhenCacheDisabled()
    {
        BufferPool pool = mock(BufferPool.class);
        ChunkCache disabledCache = new ChunkCache(pool, 0, ChunkCacheMetrics::create);

        List<ChunkCache.ChunkCacheInspectionEntry> entries = new ArrayList<>();

        // Should not throw and should return no entries
        disabledCache.inspectHotEntries(10, entries::add);
        assertEquals(0, entries.size());

        disabledCache.inspectColdEntries(10, entries::add);
        assertEquals(0, entries.size());
    }

    @Test
    public void testInspectEntriesWithZeroLimit() throws IOException
    {
        ChunkCache.instance.clear();

        File file = FileUtils.createTempFile("zeroLimit", null);
        file.deleteOnExit();
        writeBytes(file, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE]);

        try (FileHandle.Builder builder = new FileHandle.Builder(file).withChunkCache(ChunkCache.instance);
             FileHandle handle = builder.complete();
             RandomAccessReader reader = handle.createReader())
        {
            reader.reBuffer();
            assertEquals(1, ChunkCache.instance.size());

            List<ChunkCache.ChunkCacheInspectionEntry> entries = new ArrayList<>();
            ChunkCache.instance.inspectHotEntries(0, entries::add);

            // Should return no entries when limit is 0
            assertEquals(0, entries.size());
        }
    }

    @Test
    public void testInspectEntriesWithEmptyCache()
    {
        ChunkCache.instance.clear();
        assertEquals(0, ChunkCache.instance.size());

        List<ChunkCache.ChunkCacheInspectionEntry> hotEntries = new ArrayList<>();
        List<ChunkCache.ChunkCacheInspectionEntry> coldEntries = new ArrayList<>();

        // Should not throw when cache is empty
        ChunkCache.instance.inspectHotEntries(10, hotEntries::add);
        ChunkCache.instance.inspectColdEntries(10, coldEntries::add);

        assertEquals(0, hotEntries.size());
        assertEquals(0, coldEntries.size());
    }
}