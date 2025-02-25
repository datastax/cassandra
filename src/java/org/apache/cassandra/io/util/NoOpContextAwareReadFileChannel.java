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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.utils.INativeLibrary;

/**
 * A trivial implementation of {@link ContextAwareReadFileChannel} that wraps a {@link FileChannel} but ignores the
 * context provided the various methods.
 */
class NoOpContextAwareReadFileChannel implements ContextAwareReadFileChannel
{
    private final File file;
    private final FileChannel channel;

    NoOpContextAwareReadFileChannel(File file, FileChannel channel)
    {
        this.file = file;
        this.channel = channel;
    }

    @Override
    public long size() throws IOException
    {
        return channel.size();
    }

    @Override
    public int read(ByteBuffer buffer, long position, ReadCtx ctx) throws IOException
    {
        return channel.read(buffer, position);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target, ReadCtx ctx) throws IOException
    {
        return channel.transferTo(position, count, target);
    }

    @Override
    public MappedByteBuffer map(FileChannel.MapMode mode, long position, long size) throws IOException
    {
        return channel.map(mode, position, size);
    }

    @Override
    public void trySkipCache(long offset, long length)
    {
        int fd = INativeLibrary.instance.getfd(channel);
        INativeLibrary.instance.trySkipCache(fd, offset, length, file.absolutePath());
    }
}
