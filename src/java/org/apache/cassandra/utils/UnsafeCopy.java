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

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import net.nicoulaj.compilecommand.annotations.Inline;

import static org.apache.cassandra.utils.UnsafeAccess.UNSAFE;
import static org.apache.cassandra.utils.UnsafeByteBufferAccess.*;

public abstract class UnsafeCopy
{
    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits

    private static final long MIN_COPY_THRESHOLD = 6;

    @Inline
    public static void copyBufferToMemory(ByteBuffer srcBuffer, long tgtAddress)
    {
        copyBufferToMemory(srcBuffer, srcBuffer.position(), tgtAddress, srcBuffer.remaining());
    }

    @Inline
    public static void copyMemoryToBuffer(long srcAddress, ByteBuffer dstBuffer, int length)
    {
        copyMemoryToBuffer(srcAddress, dstBuffer, dstBuffer.position(), length);
    }

    /**
     * Callers assumed to have sanitized arguments.
     */
    @Inline
    public static void copyMemoryToArray(long srcAddress, byte[] dstArray, int dstOffset, int length)
    {
        copy0(null, srcAddress, dstArray, BYTE_ARRAY_BASE_OFFSET + dstOffset, length);
    }

    @Inline
    public static void copyBufferToArray(ByteBuffer srcBuf, byte[] dstArray, int dstOffset)
    {
        copyBufferToArray(srcBuf, srcBuf.position(), dstArray, dstOffset, srcBuf.remaining());
    }

    @Inline
    public static void copyBufferToArray(ByteBuffer srcBuf, int srcPosition, byte[] dstArray, int dstOffset, int length)
    {
        if (length > (dstArray.length - dstOffset))
        {
            throw new IllegalArgumentException("Cannot copy " + length + " bytes into array of length " + dstArray.length + " at offset " + dstOffset);
        }
        Object src = UnsafeByteBufferAccess.getArray(srcBuf);
        long srcOffset = bufferOffset(srcBuf, src) + srcPosition;
        copy0(src, srcOffset, dstArray, BYTE_ARRAY_BASE_OFFSET + dstOffset, length);
    }

    /**
     * Callers assumed to have sanitized arguments.
     */
    @Inline
    public static void copyBufferToMemory(ByteBuffer srcBuf, int srcPosition, long dstAddress, int length)
    {
        Object src = UnsafeByteBufferAccess.getArray(srcBuf);
        long srcOffset = bufferOffset(srcBuf, src) + srcPosition;
        copy0(src, srcOffset, null, dstAddress, length);
    }

    /**
     * Callers assumed to have sanitized arguments.
     */
    @Inline
    public static void copyMemoryToBuffer(long srcAddress, ByteBuffer dstBuf, int dstPosition, int length)
    {
        if (dstBuf.isReadOnly())
            throw new ReadOnlyBufferException();

        Object dst = UnsafeByteBufferAccess.getArray(dstBuf);
        long dstOffset = bufferOffset(dstBuf, dst) + dstPosition;
        copy0(null, srcAddress, dst, dstOffset, length);
    }

    /**
     * Callers assumed to have sanitized arguments.
     */
    @Inline
    public static void copyArrayToMemory(byte[] src, int srcPosition, long dstAddress, int length)
    {
        long srcOffset = BYTE_ARRAY_BASE_OFFSET + srcPosition;
        copy0(src, srcOffset, null, dstAddress, length);
    }

    /**
     * Callers assumed to have sanitized arguments.
     */
    @Inline
    public static void copyMemoryToMemory(long srcAddress, long dstAddress, long length)
    {
        copy0(null, srcAddress, null, dstAddress, length);
    }

    /**
     * Generic memory copy for copying array/memory to array/memory. Arguments are assumes sanitized by callers. Failure
     * to sanitize arguments MAY CRASH THE VM, or worse, MAY CORRUPT MEMORY. Use with care.
     *
     * @param src       null or a byte[]
     * @param srcOffset address (iff src == null) or offset in bytes within src (index + BYTE_ARRAY_BASE_OFFSET)
     * @param dst       null or a byte[]
     * @param dstOffset address (iff dst == null) or offset in bytes within dst (index + BYTE_ARRAY_BASE_OFFSET)
     * @param length    number of bytes to copy
     */
    @Inline
    public static void copy0(Object src, long srcOffset, Object dst, long dstOffset, long length)
    {
        // TODO: is this a valid optimization? how was this verified and on what machines?
        if (length <= MIN_COPY_THRESHOLD)
        {
            for (int i = 0 ; i < length ; i++)
            {
                byte b = UNSAFE.getByte(src, srcOffset + i);
                UNSAFE.putByte(dst, dstOffset + i, b);
            }
            return;
        }

        // Takes a conservative view of time to safepoint issues which may be induced by large uninterruptible copies,
        // especially when considering offheap mapped buffers. This should be handled by JVM implementation of copy but
        // is not on OpenJDK (is on Zing, maybe others), so we compensate in the same way NIO Bits does.
        while (length > 0)
        {
            long size = (length > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : length;
            // if src or dst are null, the offsets are absolute base addresses
            UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
            length -= size;
            srcOffset += size;
            dstOffset += size;
        }
    }
}
