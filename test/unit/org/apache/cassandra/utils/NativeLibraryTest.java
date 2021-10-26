/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;


import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

public class NativeLibraryTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void testIsAvailable()
    {
        Assert.assertTrue(NativeLibrary.instance.isAvailable());
    }

    @Test
    public void testGetfdForAsynchronousFileChannel() throws IOException
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "get_fd_async");

        try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath()))
        {
            Assert.assertTrue(NativeLibrary.instance.getfd(channel) > 0);

            FileDescriptor fileDescriptor = NativeLibrary.instance.getFileDescriptor(channel);
            Assert.assertNotNull(fileDescriptor);
            Assert.assertEquals(NativeLibrary.instance.getfd(channel), NativeLibrary.instance.getfd(fileDescriptor));
        }
    }

    @Test
    public void testGetfdForFileChannel() throws IOException
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "get_fd");

        try (FileChannel channel = FileChannel.open(file.toPath()))
        {
            Assert.assertTrue(NativeLibrary.instance.getfd(channel) > 0);

            FileDescriptor fileDescriptor = NativeLibrary.instance.getFileDescriptor(channel);
            Assert.assertNotNull(fileDescriptor);
            Assert.assertEquals(NativeLibrary.instance.getfd(channel), NativeLibrary.instance.getfd(fileDescriptor));
        }
    }

    @Test
    public void testInvalidFileDescriptor()
    {
        Assert.assertEquals(-1, NativeLibrary.instance.getfd((FileDescriptor) null));
    }

    @Test
    public void testTryFcntlWithIllegalArgument()
    {
        int invalidFd = 199991;
        Assert.assertEquals(-1, NativeLibrary.instance.tryFcntl(invalidFd, -1, -1));
    }

    @Test
    public void testOpenDirectory()
    {
        File file = FileUtils.createDeletableTempFile("testOpenDirectory", "1");

        int fd = NativeLibrary.instance.tryOpenDirectory(file.parent());
        NativeLibrary.instance.tryCloseFD(fd);
    }

    @Test
    public void testOpenDirectoryWithIllegalArgument()
    {
        File file = FileUtils.createDeletableTempFile("testOpenDirectoryWithIllegalArgument", "1");
        Assert.assertEquals(-1, NativeLibrary.instance.tryOpenDirectory(file.resolve("no_existing")));
    }

    @Test
    public void testTrySyncWithIllegalArgument()
    {
        NativeLibrary.instance.trySync(-1);

        int invalidFd = 199991;
        Assert.assertThrows(FSWriteError.class, () -> NativeLibrary.instance.trySync(invalidFd));
    }

    @Test
    public void testTryCloseFDWithIllegalArgument()
    {
        NativeLibrary.instance.tryCloseFD(-1);

        int invalidFd = 199991;
        Assert.assertThrows(FSWriteError.class, () -> NativeLibrary.instance.tryCloseFD(invalidFd));
    }

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        NativeLibrary.instance.trySkipCache(file, 0, 0);
        NativeLibrary.instance.trySkipCache(file.resolve("no_existing"), 0, 0);
    }

    @Test
    public void getPid()
    {
        long pid = NativeLibrary.instance.getProcessID();
        Assert.assertTrue(pid > 0);
    }
}
