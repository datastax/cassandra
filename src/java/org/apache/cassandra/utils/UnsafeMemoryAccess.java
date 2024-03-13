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

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.jna.Native;
import net.nicoulaj.compilecommand.annotations.Inline;

import static org.apache.cassandra.utils.Architecture.IS_UNALIGNED;
import static org.apache.cassandra.utils.UnsafeAccess.UNSAFE;

/**
 * This class is used for native memory access, in particular for temporary internal memory access. The methods are
 * therefore optimized for nativeByteOrder access, i.e. the byte order will depend on the platform. This is not suitable
 * for externally visible data (e.g. network/files).
 */
public class UnsafeMemoryAccess
{
    public static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    private static final AtomicLong memoryAllocated = new AtomicLong(0);

    @Inline
    public static void setByte(long address, byte b)
    {
        UNSAFE.putByte(address, b);
    }

    /**
     * For direct memory access ONLY (not for DirectByteBuffer used for files/network), so native byte ordering is used.
     */
    @Inline
    public static void setShort(long address, short s)
    {
        if (IS_UNALIGNED)
            UNSAFE.putShort(address, s);
        else
            putShortByByte(address, s);
    }

    /**
     * For direct memory access ONLY (not for DirectByteBuffer used for files/network), so native byte ordering is used.
     */
    @Inline
    public static void setInt(long address, int i)
    {
        if (IS_UNALIGNED)
            UNSAFE.putInt(address, i);
        else
            putIntByByte(address, i);
    }

    /**
     * For direct memory access ONLY (not for DirectByteBuffer used for files/network), so native byte ordering is used.
     */
    @Inline
    public static void setLong(long address, long l)
    {
        if (IS_UNALIGNED)
            UNSAFE.putLong(address, l);
        else
            putLongByByte(address, l);
    }

    @Inline
    public static byte getByte(long address)
    {
        return UNSAFE.getByte(address);
    }

    /**
     * For direct memory access ONLY (not for DirectByteBuffer used for files/network), so native byte ordering is used.
     */
    @Inline
    public static int getUnsignedShort(long address)
    {
        return (IS_UNALIGNED ? UNSAFE.getShort(address) : getShortByByte(null, address, BIG_ENDIAN)) & 0xffff;
    }

    /**
     * For direct memory access ONLY (not for DirectByteBuffer used for files/network), so native byte ordering is used.
     */
    @Inline
    public static int getInt(long address)
    {
        return IS_UNALIGNED ? UNSAFE.getInt(null, address) : getIntByByte(null, address, BIG_ENDIAN);
    }

    /**
     * For direct memory access ONLY (not for DirectByteBuffer used for files/network), so native byte ordering is used.
     */
    @Inline
    public static long getLong(long address)
    {
        return IS_UNALIGNED ? UNSAFE.getLong(address) : getLongByByte(null, address, BIG_ENDIAN);
    }

    public static long getLongByByte(Object o, long address, boolean bigEndian)
    {
        if (bigEndian)
        {
            return (((long) UNSAFE.getByte(o, address    )       ) << 56) |
                   (((long) UNSAFE.getByte(o, address + 1) & 0xff) << 48) |
                   (((long) UNSAFE.getByte(o, address + 2) & 0xff) << 40) |
                   (((long) UNSAFE.getByte(o, address + 3) & 0xff) << 32) |
                   (((long) UNSAFE.getByte(o, address + 4) & 0xff) << 24) |
                   (((long) UNSAFE.getByte(o, address + 5) & 0xff) << 16) |
                   (((long) UNSAFE.getByte(o, address + 6) & 0xff) << 8) |
                   (((long) UNSAFE.getByte(o, address + 7) & 0xff)      );
        }
        else
        {
            return (((long) UNSAFE.getByte(o, address + 7)       ) << 56) |
                   (((long) UNSAFE.getByte(o, address + 6) & 0xff) << 48) |
                   (((long) UNSAFE.getByte(o, address + 5) & 0xff) << 40) |
                   (((long) UNSAFE.getByte(o, address + 4) & 0xff) << 32) |
                   (((long) UNSAFE.getByte(o, address + 3) & 0xff) << 24) |
                   (((long) UNSAFE.getByte(o, address + 2) & 0xff) << 16) |
                   (((long) UNSAFE.getByte(o, address + 1) & 0xff) << 8) |
                   (((long) UNSAFE.getByte(o, address    ) & 0xff)      );
        }
    }

    public static int getIntByByte(Object o, long address, boolean bigEndian)
    {
        if (bigEndian)
        {
            return (((int) UNSAFE.getByte(o, address    )       ) << 24) |
                   (((int) UNSAFE.getByte(o, address + 1) & 0xff) << 16) |
                   (((int) UNSAFE.getByte(o, address + 2) & 0xff) << 8 ) |
                   (((int) UNSAFE.getByte(o, address + 3) & 0xff)      );
        }
        else
        {
            return (((int) UNSAFE.getByte(o, address + 3)       ) << 24) |
                   (((int) UNSAFE.getByte(o, address + 2) & 0xff) << 16) |
                   (((int) UNSAFE.getByte(o, address + 1) & 0xff) << 8) |
                   (((int) UNSAFE.getByte(o, address    ) & 0xff)      );
        }
    }

    public static short getShortByByte(Object o, long address, boolean bigEndian)
    {
        if (bigEndian)
        {
            return (short) ((((int) UNSAFE.getByte(o, address    )       ) << 8) |
                            (((int) UNSAFE.getByte(o, address + 1) & 0xff)     ));
        }
        else
        {
            return (short) ((((int) UNSAFE.getByte(o, address + 1)       ) << 8) |
                            (((int) UNSAFE.getByte(o, address    ) & 0xff)      ));
        }
    }

    public static void putLongByByte(long address, long value)
    {
        if (BIG_ENDIAN)
        {
            UNSAFE.putByte(address, (byte) (value >> 56));
            UNSAFE.putByte(address + 1, (byte) (value >> 48));
            UNSAFE.putByte(address + 2, (byte) (value >> 40));
            UNSAFE.putByte(address + 3, (byte) (value >> 32));
            UNSAFE.putByte(address + 4, (byte) (value >> 24));
            UNSAFE.putByte(address + 5, (byte) (value >> 16));
            UNSAFE.putByte(address + 6, (byte) (value >> 8));
            UNSAFE.putByte(address + 7, (byte) (value));
        }
        else
        {
            UNSAFE.putByte(address + 7, (byte) (value >> 56));
            UNSAFE.putByte(address + 6, (byte) (value >> 48));
            UNSAFE.putByte(address + 5, (byte) (value >> 40));
            UNSAFE.putByte(address + 4, (byte) (value >> 32));
            UNSAFE.putByte(address + 3, (byte) (value >> 24));
            UNSAFE.putByte(address + 2, (byte) (value >> 16));
            UNSAFE.putByte(address + 1, (byte) (value >> 8));
            UNSAFE.putByte(address, (byte) (value));
        }
    }

    public static void putIntByByte(long address, int value)
    {
        if (BIG_ENDIAN)
        {
            UNSAFE.putByte(address, (byte) (value >> 24));
            UNSAFE.putByte(address + 1, (byte) (value >> 16));
            UNSAFE.putByte(address + 2, (byte) (value >> 8));
            UNSAFE.putByte(address + 3, (byte) (value));
        }
        else
        {
            UNSAFE.putByte(address + 3, (byte) (value >> 24));
            UNSAFE.putByte(address + 2, (byte) (value >> 16));
            UNSAFE.putByte(address + 1, (byte) (value >> 8));
            UNSAFE.putByte(address, (byte) (value));
        }
    }

    public static void putShortByByte(long address, short value)
    {
        if (BIG_ENDIAN)
        {
            UNSAFE.putByte(address, (byte) (value >> 8));
            UNSAFE.putByte(address + 1, (byte) (value));
        }
        else
        {
            UNSAFE.putByte(address + 1, (byte) (value >> 8));
            UNSAFE.putByte(address, (byte) (value));
        }
    }

    public static int pageSize()
    {
        return UNSAFE.pageSize();
    }

    public static long allocate(long size)
    {
        memoryAllocated.addAndGet(size);
        return Native.malloc(size);
    }

    public static void free(long peer, long size)
    {
        memoryAllocated.addAndGet(-size);
        Native.free(peer);
    }

    public static long allocated()
    {
        return memoryAllocated.get();
    }

    public static void fill(long address, long count, byte b)
    {
        UNSAFE.setMemory(address, count, b);
    }
}
