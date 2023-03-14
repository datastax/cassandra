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

package org.apache.cassandra.metrics;

import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.UnsafeMemoryAccess;

import static org.junit.Assert.assertEquals;

public class NativeMemoryMetricsTest
{
    private final static Logger logger = LoggerFactory.getLogger(NativeMemoryMetricsTest.class);
    private static NativeMemoryMetrics nativeMemoryMetrics;
    private static BufferPoolMXBean directBufferPool;

    @BeforeClass
    public static void setupClass()
    {
        // Meter depends on LongAdder, which depends on TPC, which needs DD
        DatabaseDescriptor.daemonInitialization();

        nativeMemoryMetrics = NativeMemoryMetrics.instance;

        directBufferPool = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)
                                            .stream()
                                            .filter(bpMBean -> bpMBean.getName().equals("direct"))
                                            .findFirst()
                                            .orElse(null);
    }

    @Test
    public void testNioDirectMemory()
    {
        long totalNioDirectMemory = nativeMemoryMetrics.totalNioDirectMemory();
        long usedNioDirectMemory = nativeMemoryMetrics.usedNioDirectMemory();
        long nioDirectBufferCount = nativeMemoryMetrics.nioDirectBufferCount();

        logger.debug("Total Nio Memory: {}, Reserved Nio Memory: {}, Num Nio buffers: {}",
                     totalNioDirectMemory, usedNioDirectMemory, nioDirectBufferCount);

        assertEquals("Total and reserved nio memory should be equal since -Dsun.nio.PageAlignDirectMemory=true should not be set",
                     totalNioDirectMemory, usedNioDirectMemory);

        assertEquals("Total nio memory should be equal to total memory since no unsafe allocations should have been done",
                     totalNioDirectMemory, nativeMemoryMetrics.totalMemory());

        assertEquals(directBufferPool.getMemoryUsed(), nativeMemoryMetrics.usedNioDirectMemory());
        assertEquals(directBufferPool.getTotalCapacity(), nativeMemoryMetrics.totalNioDirectMemory());
        assertEquals(directBufferPool.getCount(), nativeMemoryMetrics.nioDirectBufferCount());

        ByteBuffer buffer = ByteBuffer.allocateDirect(128);

        assertEquals(totalNioDirectMemory + 128, nativeMemoryMetrics.totalNioDirectMemory());
        assertEquals(usedNioDirectMemory + 128, nativeMemoryMetrics.usedNioDirectMemory());
        assertEquals(nioDirectBufferCount + 1, nativeMemoryMetrics.nioDirectBufferCount());

        FileUtils.clean(buffer);

        assertEquals(totalNioDirectMemory, nativeMemoryMetrics.totalNioDirectMemory());
        assertEquals(usedNioDirectMemory, nativeMemoryMetrics.usedNioDirectMemory());
        assertEquals(nioDirectBufferCount, nativeMemoryMetrics.nioDirectBufferCount());
    }

    @Test
    public void testUnsafeNativeMemory()
    {
        assertEquals(0, nativeMemoryMetrics.rawNativeMemory());

        long peer = UnsafeMemoryAccess.allocate(128);
        assertEquals(128, nativeMemoryMetrics.rawNativeMemory());

        UnsafeMemoryAccess.free(peer, 128);
        assertEquals(0, nativeMemoryMetrics.rawNativeMemory());
    }
}
