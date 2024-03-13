/*
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
 *
 */

/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeMemoryAccess;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class BufferPoolTest
{
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolTest.class);

    private final static long MEMORY_USAGE_THRESHOLD = 128 * 1024L * 1024L;

    /** The minimum size of a page aligned buffer, 1Kib. Below this the requested size will be rounded up. Must be a power of two. */
    private final static int MIN_BUFFER_SIZE = BufferPool.MIN_DIRECT_READS_BUFFER_SIZE;

    /** The maximum size of a page aligned buffer, 64KiB. Beyond this a direct allocation will be performed. Must be a power of two. */
    private final static int MAX_BUFFER_SIZE = BufferPool.MAX_DIRECT_READS_BUFFER_SIZE;

    /** The size of a thread local slab to serve short lived buffers, 4MB. */
    private final static int SLAB_SIZE = BufferPool.THREAD_LOCAL_SLAB_SIZE;

    private final static Random rand = new Random(1287502554245L);

    private final BufferPool bufferPool;

    @Parameterized.Parameters()
    public static Collection<Object[]> generateData()
    {
        DatabaseDescriptor.daemonInitialization();

        return Arrays.asList(new Object[][]{
        { new PermanentBufferPool("PermanentBufferPoolTest", MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, MEMORY_USAGE_THRESHOLD) },
        { new TemporaryBufferPool("TemporaryBufferPoolTest", SLAB_SIZE, MAX_BUFFER_SIZE * 2, MEMORY_USAGE_THRESHOLD) },
        });
    }

    public BufferPoolTest(BufferPool bufferPool)
    {
        this.bufferPool = bufferPool;
    }

    @After
    public void cleanUp()
    {
        assertEquals(bufferPool.toString(), 0, bufferPool.usedMemoryBytes());
        bufferPool.cleanup();
    }

    @Test
    public void testStatus()
    {
        assertNotNull(bufferPool.status());
        assertNotNull(bufferPool.toString());
    }

    @Test
    public void testAllocRelease()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;

        ByteBuffer buffer = bufferPool.allocate(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertTrue(buffer.isDirect());
        assertEquals(size, bufferPool.usedMemoryBytes());

        bufferPool.release(buffer);
        assertEquals(0, bufferPool.usedMemoryBytes());
    }

    @Test
    public void testMemoryAddressesAlloc()
    {
        if (bufferPool instanceof TemporaryBufferPool)
            return; // only the permanent pool supports this feature

        final int bufferSize = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        // We want to test a few addresses. Here we test a number of addresses sufficient to cover two slabs
        final int numAddresses = BufferPool.NUM_BUFFERS_PER_SLAB * 2;

        final byte[] written = new byte[bufferSize];
        final byte[] read = new byte[bufferSize];

        long[] addresses = bufferPool.allocate(bufferSize, numAddresses);
        assertNotNull(addresses);
        assertEquals(numAddresses, addresses.length);

        for (long address : addresses)
        {
            assertNotEquals(-1, address);
            assertNotEquals(0, address);

            ByteBuffer buffer = UnsafeByteBufferAccess.allocateByteBuffer(address, bufferSize);
            assertEquals(bufferSize, buffer.capacity());
            assertTrue(buffer.isDirect());

            rand.nextBytes(written);
            buffer.put(written);
            buffer.rewind();

            buffer.get(read);
            assertArrayEquals(written, read);
        }

        assertEquals(numAddresses * bufferSize, bufferPool.usedMemoryBytes());

        bufferPool.release(bufferSize, addresses);

        for (long address : addresses)
        {
            assertEquals(-1, address);
        }

        assertEquals(0, bufferPool.usedMemoryBytes());
    }

    @Test
    public void testMemoryAddressesAllocWithOOM()
    {
        if (bufferPool instanceof TemporaryBufferPool)
            return; // only the permanent pool supports this feature

        // addresses cannot be allocated beyond the max pool size so we should get an OOM without risking exceeding max direct memory
        final long maxSize = MEMORY_USAGE_THRESHOLD;
        final int bufferSize = MAX_BUFFER_SIZE;
        final int numAddresses = Math.toIntExact(maxSize / bufferSize + 2);

        try
        {
            bufferPool.allocate(bufferSize, numAddresses);
            fail("Expected OutOfMemoryError");
        }
        catch (RuntimeException | OutOfMemoryError e)
        {
            // make sure all buffers were returned to the pool
            assertEquals(0, this.bufferPool.usedMemoryBytes());

            if (e instanceof  RuntimeException)
                JVMStabilityInspector.inspectThrowable(e);
        }
    }

    @Test
    public void testPageAligned()
    {
        for (int i = MIN_BUFFER_SIZE; i <= MAX_BUFFER_SIZE; i += MIN_BUFFER_SIZE)
        {
            checkPageAligned(i);
        }
    }

    private void checkPageAligned(int size)
    {
        ByteBuffer buffer = bufferPool.allocate(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertTrue(buffer.isDirect());

        long address = UnsafeByteBufferAccess.getAddress(buffer);
        assertEquals(0, address % UnsafeMemoryAccess.pageSize());

        bufferPool.release(buffer);
    }

    @Test
    public void testDifferentSizes()
    {
        final int size1 = 1024;
        final int size2 = 2048;

        ByteBuffer buffer1 = bufferPool.allocate(size1);
        assertNotNull(buffer1);
        assertEquals(size1, buffer1.capacity());

        ByteBuffer buffer2 = bufferPool.allocate(size2);
        assertNotNull(buffer2);
        assertEquals(size2, buffer2.capacity());

        assertEquals(size1 + size2, bufferPool.usedMemoryBytes());

        bufferPool.release(buffer1);
        bufferPool.release(buffer2);

        assertEquals(0, bufferPool.usedMemoryBytes());
    }

    @Test
    public void testMaxMemoryExceeded()
    {
        final int bufferSize = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        final long totalSize = MEMORY_USAGE_THRESHOLD * 2;
        final int numBuffers = Math.toIntExact(totalSize / bufferSize);
        boolean hitlimit = false;

        assertEquals(0, bufferPool.usedMemoryBytes());
        assertEquals(0, bufferPool.overflowMemoryBytes());

        List<ByteBuffer> buffers = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
        {
            ByteBuffer buffer = bufferPool.allocate(bufferSize);
            assertNotNull(buffer);
            assertEquals(bufferSize, buffer.capacity());

            if (bufferPool.usedMemoryBytes() > MEMORY_USAGE_THRESHOLD)
            {
                assertTrue(bufferPool.overflowMemoryBytes() > 0);
                assertTrue(bufferPool.missedAllocationsMeanRate() > 0);
                hitlimit = true;
            }

            buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            bufferPool.release(buffer);

        assertEquals(bufferPool.toString(), 0, bufferPool.usedMemoryBytes());
        assertEquals(bufferPool.toString(), 0, bufferPool.overflowMemoryBytes());
        assertTrue(hitlimit);
    }

    @Test
    public void testBigRequest()
    {
        final int size = (MAX_BUFFER_SIZE * 2) + 1;

        ByteBuffer buffer = bufferPool.allocate(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        bufferPool.release(buffer);
    }

    @Test
    public void testCompactIfOutOfCapacity()
    {
        if (bufferPool instanceof TemporaryBufferPool)
            return;

        final int size = 4096;
        final int numBuffersInSlab = BufferPool.NUM_BUFFERS_PER_SLAB;

        List<ByteBuffer> buffers = new ArrayList<>(numBuffersInSlab);
        Set<Long> addresses = new HashSet<>(numBuffersInSlab);

        for (int i = 0; i < numBuffersInSlab; i++)
        {
            ByteBuffer buffer = bufferPool.allocate(size);
            buffers.add(buffer);
            addresses.add(UnsafeByteBufferAccess.getAddress(buffer));
        }

        for (int i = numBuffersInSlab - 1; i >= 0; i--)
            bufferPool.release(buffers.get(i));

        buffers.clear();

        for (int i = 0; i < numBuffersInSlab; i++)
        {
            ByteBuffer buffer = bufferPool.allocate(size);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());
            addresses.remove(UnsafeByteBufferAccess.getAddress(buffer));

            buffers.add(buffer);
        }

        assertTrue(addresses.isEmpty()); // all 5 released buffers were used

        for (ByteBuffer buffer : buffers)
            bufferPool.release(buffer);
    }

    @Test
    public void testMultipleBuffersOneChunk()
    {
        checkBuffers(32768, 33553);
        checkBuffers(32768, 32768);
        checkBuffers(48450, 33172);
        checkBuffers(32768, 15682, 33172);
    }

    private void checkBuffers(int ... sizes)
    {
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);

        for (int size : sizes)
        {
            ByteBuffer buffer = bufferPool.allocate(size);
            assertEquals(size, buffer.capacity());

            buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            bufferPool.release(buffer);
    }

    @Test
    public void testZeroSizeRequest()
    {
        ByteBuffer buffer = bufferPool.allocate(0);
        assertNotNull(buffer);
        assertEquals(0, buffer.capacity());
        bufferPool.release(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSizeRequest()
    {
        bufferPool.allocate(-1);
    }

    @Test
    public void testDisabled()
    {
        BufferPoolDisabled bufferPoolDisabled = new BufferPoolDisabled("DisabledBufferPoolTest", MEMORY_USAGE_THRESHOLD);
        assertNotNull(bufferPoolDisabled.allocate(0));

        ByteBuffer buffer = bufferPoolDisabled.allocate(4096);
        assertEquals(4096, buffer.capacity());
        bufferPoolDisabled.release(buffer);
    }

}
