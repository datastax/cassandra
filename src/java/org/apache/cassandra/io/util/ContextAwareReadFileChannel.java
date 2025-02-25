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

public interface ContextAwareReadFileChannel
{
    /**
     * Behaves exactly like {@link java.nio.channels.FileChannel#size()}.
     */
    long size() throws IOException;

    /**
     * Behaves exactly like {@link java.nio.channels.FileChannel#read(ByteBuffer, long)} but with the additional
     * context as argument.
     */
    int read(ByteBuffer buffer, long position, ReadCtx ctx) throws IOException;

    /**
     * Behaves exactly like {@link java.nio.channels.FileChannel#transferTo(long, long, WritableByteChannel)} but with
     * the additional context as argument.
     */
    long transferTo(long position, long count, WritableByteChannel target, ReadCtx ctx) throws IOException;

    /**
     * If supported, behaves exactly like {@link java.nio.channels.FileChannel#map(FileChannel.MapMode, long, long)}.
     * <p>
     * Note that truly "context aware" implementations cannot really support mmapping easily with the current
     * abstraction as the context would have to be provided to the methods of the provided {@link MappedByteBuffer}.
     * But doing so would require somewhat extensive changes and as this is not currently strongly needed, this is
     * left as future work if ever desired. So the default here is that this method is unsupported, and only
     * {@link NoOpContextAwareReadFileChannel} which ignore the context anyway implement it.
     */
    default MappedByteBuffer map(FileChannel.MapMode mode, long position, long size) throws IOException
    {
        throw new UnsupportedOperationException("MMapping is not supported by this implementation");
    }

    /**
     * Apply FADV_DONTNEED to the provided file region if supported by this implementation.
     */
    void trySkipCache(long offset, long length);
}
