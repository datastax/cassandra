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
import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

public class ChunkCacheTest
{
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

}