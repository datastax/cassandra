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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.zip.Checksum;

public final class PureJavaCRC64NVME implements Checksum
{
    private static final long POLY = 0x9A6C9329AC4BC9B5L;

    private static final long[][] TABLE = new long[8][256];

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    static
    {
        for (int i = 0; i < 256; i++)
        {
            long crc = i;

            for (int j = 0; j < 8; j++)
            {
                crc = ((crc & 1) != 0)
                      ? (crc >>> 1) ^ POLY
                      : (crc >>> 1);
            }

            TABLE[0][i] = crc;
        }

        for (int t = 1; t < 8; t++)
        {
            for (int i = 0; i < 256; i++)
            {
                long crc = TABLE[t - 1][i];
                TABLE[t][i] = TABLE[0][(int) (crc & 0xFF)] ^ (crc >>> 8);
            }
        }
    }

    private long crc;

    @Override
    public void update(int b)
    {
        long c = crc ^ ~0L;
        c = (c >>> 8) ^ TABLE[0][(int) ((c ^ b) & 0xFF)];
        crc = c ^ ~0L;
    }

    @Override
    public void update(byte[] b, int off, int len)
    {
        long c = crc ^ ~0L;

        int end = off + len;

        while (off + 8 <= end)
        {
            long x = (long) LONG_LE.get(b, off) ^ c;

            c = TABLE[7][(int) (x & 0xFF)] ^
                TABLE[6][(int) ((x >>> 8) & 0xFF)] ^
                TABLE[5][(int) ((x >>> 16) & 0xFF)] ^
                TABLE[4][(int) ((x >>> 24) & 0xFF)] ^
                TABLE[3][(int) ((x >>> 32) & 0xFF)] ^
                TABLE[2][(int) ((x >>> 40) & 0xFF)] ^
                TABLE[1][(int) ((x >>> 48) & 0xFF)] ^
                TABLE[0][(int) ((x >>> 56) & 0xFF)];

            off += 8;
        }

        while (off < end)
        {
            c = (c >>> 8) ^ TABLE[0][(int) ((c ^ b[off++]) & 0xFF)];
        }

        crc = c ^ ~0L;
    }

    @Override
    public long getValue()
    {
        return crc;
    }

    @Override
    public void reset()
    {
        crc = 0;
    }
}
