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
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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

        Assert.assertEquals(ChunkCache.instance.size(), 0);
        Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 0);

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

            Assert.assertEquals(ChunkCache.instance.size(), 1);
            Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 1);
        }

        Assert.assertEquals(ChunkCache.instance.size(), 0);
        Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 0);
    }

    @Test
    public void testInvalidateFileNotInCache()
    {
        Assert.assertEquals(ChunkCache.instance.size(), 0);
        ChunkCache.instance.invalidateFile("/tmp/does/not/exist/in/cache/or/on/file/system");
    }

    @Test
    public void testRandomAccessReadersWithUpdatedFileAndMultipleChunksAndCacheInvalidation() throws IOException
    {
        File file = FileUtils.createTempFile("foo", null);
        file.deleteOnExit();

        Assert.assertEquals(ChunkCache.instance.size(), 0);
        Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 0);

        writeBytes(file, new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE * 3]);

        try (FileHandle.Builder builder1 = new FileHandle.Builder(file).withChunkCache(ChunkCache.instance);
             FileHandle handle1 = builder1.complete();
             RandomAccessReader reader1 = handle1.createReader();
             RandomAccessReader reader2 = handle1.createReader())
        {
            // Read 2 chunks and verify contents
            for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 2; i++)
                Assert.assertEquals((byte) 0, reader1.readByte());

            // Overwrite the file's contents
            var bytes = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE * 3];
            Arrays.fill(bytes, (byte) 1);
            writeBytes(file, bytes);

            // Verify rebuffer pulls from cache for first 2 bytes and then from disk for third byte
            reader1.seek(0);
            for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 2; i++)
                Assert.assertEquals((byte) 0, reader1.readByte());
            // Trigger read of next chunk and see it is the new data
            Assert.assertEquals((byte) 1, reader1.readByte());

            Assert.assertEquals(ChunkCache.instance.size(), 3);
            Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 3);

            // Invalidate cache for both chunks
            ChunkCache.instance.invalidateFile(file.path());

            // Verify cache is empty
            Assert.assertEquals(ChunkCache.instance.size(), 0);
            Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 0);

            // Seek then verify that the new data is read
            reader1.seek(0);
            for (int i = 0; i < RandomAccessReader.DEFAULT_BUFFER_SIZE * 3; i++)
                Assert.assertEquals((byte) 1, reader1.readByte());

            // Verify a second reader gets the new data even though it was created before the cache was invalidated
            Assert.assertEquals((byte) 1, reader2.readByte());
        }

        Assert.assertEquals(ChunkCache.instance.size(), 0);
        Assert.assertEquals(ChunkCache.instance.sizeOfFile(file.path()), 0);
    }

    @Test
    public void testRandomAccessReadersForDifferentFilesWithCacheInvalidation() throws IOException
    {
        File fileFoo = FileUtils.createTempFile("foo", null);
        fileFoo.deleteOnExit();
        File fileBar = FileUtils.createTempFile("bar", null);
        fileBar.deleteOnExit();

        Assert.assertEquals(ChunkCache.instance.size(), 0);
        Assert.assertEquals(ChunkCache.instance.sizeOfFile(fileFoo.path()), 0);
        Assert.assertEquals(ChunkCache.instance.sizeOfFile(fileBar.path()), 0);

        writeBytes(fileFoo, new byte[64]);
        // Write different bytes for meaningful content validation
        var barBytes = new byte[64];
        Arrays.fill(barBytes, (byte) 1);
        writeBytes(fileBar, barBytes);

        try (FileHandle.Builder builderFoo = new FileHandle.Builder(fileFoo).withChunkCache(ChunkCache.instance);
             FileHandle handleFoo = builderFoo.complete();
             RandomAccessReader readerFoo = handleFoo.createReader())
        {
            Assert.assertEquals((byte) 0, readerFoo.readByte());

            Assert.assertEquals(ChunkCache.instance.size(), 1);
            Assert.assertEquals(ChunkCache.instance.sizeOfFile(fileFoo.path()), 1);

            try (FileHandle.Builder builderBar = new FileHandle.Builder(fileBar).withChunkCache(ChunkCache.instance);
                 FileHandle handleBar = builderBar.complete();
                 RandomAccessReader readerBar = handleBar.createReader())
            {
                Assert.assertEquals((byte) 1, readerBar.readByte());

                Assert.assertEquals(ChunkCache.instance.size(), 2);
                Assert.assertEquals(ChunkCache.instance.sizeOfFile(fileBar.path()), 1);

                // Invalidate fileFoo and verify that only fileFoo's chunks are removed
                ChunkCache.instance.invalidateFile(fileFoo.path());
                Assert.assertEquals(ChunkCache.instance.size(), 1);
                Assert.assertEquals(ChunkCache.instance.sizeOfFile(fileBar.path()), 1);
            }
        }
        Assert.assertEquals(ChunkCache.instance.size(), 0);
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

        CompletableFuture<?> waitOnRead = new CompletableFuture<>();

        public MockFileControl(File file, int fileSize) throws Exception
        {
            this.file = file;
            this.fileSize = fileSize;
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

        void open() throws Exception
        {
            file.deleteOnExit();

            try (SequentialWriter writer = new SequentialWriter(file))
            {
                writer.write(new byte[fileSize]);
                writer.flush();
            }

            channel = spy(FileChannel.class);
            when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {

                logger.info("Waiting on read for file {}", file.path());
                // this allows us to introduce a delay or a failure in the read
                waitOnRead.join();
                logger.info("Read completed for file {}", file.path());


                ByteBuffer buffer = invocation.getArgument(0);
                long position = invocation.getArgument(1);
                int writen = buffer.remaining();
                buffer.put(new byte[writen]);
                return writen;
            });
            when(channel.size()).thenReturn(Long.valueOf(fileSize));

            proxy = new ChannelProxy(file, channel);
            FileHandle.Builder builder = new FileHandle.Builder(proxy)
                                         .withChunkCache(ChunkCache.instance);
            fileHandle = builder.complete();
            reader = fileHandle.createReader();
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
        Assert.assertEquals(chunkCache.size(), 0);
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
                MockFileControl mockFileControl = new MockFileControl(file, fileSize);
                files[i] = mockFileControl;
                mockFileControl.open();
                if (i != slowFileIndex)
                {
                    mockFileControl.waitOnRead.complete(null);
                }
                Assert.assertEquals(chunkCache.sizeOfFile(file.path()), 0);
            }

            ExecutorService threadPool = Executors.newFixedThreadPool(numFiles);

            Future<?>[] results = new Future[numFiles];
            for (int i = 0; i < numFiles; i++)
            {
                MockFileControl mockFileControl = files[i];
                RandomAccessReader r = mockFileControl.reader;
                File file = mockFileControl.file;

                results[i] = threadPool.submit(() -> {
                    r.reBuffer();
                    Assert.assertEquals(chunkCache.sizeOfFile(file.path()), 1);
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
        ChunkCache chunkCache = ChunkCache.instance;
        Assert.assertEquals(chunkCache.size(), 0);
        int fileSize = 64;
        File file1 = FileUtils.createTempFile("foo1", ".tmp");
        File file2 = FileUtils.createTempFile("foo2", ".tmp");
        try (MockFileControl mockFileControl1 = new MockFileControl(file1, fileSize);
             MockFileControl mockFileControl2 = new MockFileControl(file2, fileSize);)
        {

            mockFileControl1.open();
            mockFileControl2.open();

            // file 1 has an error during read, we shouldn't cache the handle
            mockFileControl1.waitOnRead.completeExceptionally(new RuntimeException("some weird runtime error"));
            RandomAccessReader r1 = mockFileControl1.reader;
            assertThrows(CompletionException.class, r1::reBuffer);
            Assert.assertEquals(chunkCache.sizeOfFile(mockFileControl1.file.path()), 0);

            // file 2 works fine, we should cache the handle
            mockFileControl2.waitOnRead.complete(null);
            RandomAccessReader r2 = mockFileControl2.reader;
            r2.reBuffer();
            Assert.assertEquals(chunkCache.sizeOfFile(mockFileControl2.file.path()), 1);
        }
    }

}