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
package org.apache.cassandra.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class FileUtils
{

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Clean the buffer if it is a direct byte buffer. If the buffer was sliced from a parent slice, clean the parent
     * if it was marked for cleaning with {@link SlicedBufferCleaner}. This is currently done by page aligned buffers,
     * which are sliced from a larger parent that must be cleaned as well.
     *
     * @param buffer - the buffer to clean
     */
    public static void clean(ByteBuffer buffer)
    {
        if (buffer == null || !buffer.isDirect())
            return;

        if (!JavaVersionDependent.invokeCleaner(buffer))
        {
            ByteBuffer parent = SlicedBufferCleaner.cleanableParent(buffer);
            if (parent != null)
                clean(parent);
        }
    }


    /**
     * A small utility class for buffers that are sliced from larger buffers and that when cleaned require the parent
     * buffer to be cleaned as well. This currently happens for page alignment with buffers allocated by
     * {@link org.apache.cassandra.io.compress.BufferType#OFF_HEAP_ALIGNED}. This method returns a slice but, upon cleaning
     * the slice, the parent actually needs to be cleaned.
     * <p/>
     * {@link org.apache.cassandra.io.util.FileUtils#clean(ByteBuffer)} will recognize this attachment marker and know that
     * it must clean the parent of the sliced buffer.
     */
    public final static class SlicedBufferCleaner
    {
        private final ByteBuffer attachment;
        private volatile Runnable observer;

        private SlicedBufferCleaner(ByteBuffer buffer)
        {
            Object attachment =  UnsafeByteBufferAccess.getAttachment(buffer);
            assert attachment instanceof ByteBuffer : "Sliced buffers with cleanable parents should have ByteBuffer attachments";
            this.attachment = (ByteBuffer) attachment;
        }

        public static ByteBuffer mark(ByteBuffer buffer)
        {
            UnsafeByteBufferAccess.setAttachment(buffer, new SlicedBufferCleaner(buffer));
            return buffer;
        }

        // This is only used by unit tests to check that cleanableParent is called
        @VisibleForTesting
        public static boolean setObserver(ByteBuffer buffer, Runnable observer)
        {
            Object attachment = UnsafeByteBufferAccess.getAttachment(buffer);
            if (attachment == null)
                return false;

            if (attachment instanceof SlicedBufferCleaner)
            {
                ((SlicedBufferCleaner) attachment).observer = observer;
                return true;
            }
            else if (attachment instanceof ByteBuffer)
            {
                return setObserver((ByteBuffer) attachment, observer);
            }

            return false;
        }

        static ByteBuffer cleanableParent(ByteBuffer buffer)
        {
            Object attachment = UnsafeByteBufferAccess.getAttachment(buffer);
            if (attachment == null || !(attachment instanceof SlicedBufferCleaner))
                return null;

            SlicedBufferCleaner cleaner = (SlicedBufferCleaner) attachment;
            if (cleaner.observer != null)
                cleaner.observer.run(); // for tests only

            return cleaner.attachment;
        }
    }


    /**
     * Private constructor as the class contains only static methods.
     */
    private FileUtils()
    {
    }
}
