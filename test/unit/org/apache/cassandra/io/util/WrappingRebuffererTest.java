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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import org.apache.cassandra.concurrent.TPCUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WrappingRebuffererTest
{
    @Test
    public void testRecycleSameHolder()
    {
        TestRebufferer mock = new TestRebufferer();
        try(WrappingRebufferer rebufferer = new WrappingRebufferer(mock))
        {
            Rebufferer.BufferHolder ret = rebufferer.rebuffer(0);
            assertNotNull(ret);
            assertEquals(mock.buffer(), ret.buffer());
            assertEquals(mock.offset(), ret.offset());

            ret.release();
            assertTrue(mock.released);

            assertTrue(ret == rebufferer.rebuffer(0)); // same buffer holder was recycled
        }
    }

    @Test
    public void testRecycleTwoHolders()
    {
        TestRebufferer mock = new TestRebufferer();
        try (WrappingRebufferer rebufferer = new WrappingRebufferer(mock))
        {

            Rebufferer.BufferHolder ret1 = rebufferer.rebuffer(0);
            assertNotNull(ret1);
            assertEquals(mock.buffer(), ret1.buffer());
            assertEquals(mock.offset(), ret1.offset());

            Rebufferer.BufferHolder ret2 = rebufferer.rebuffer(1);
            assertNotNull(ret2);
            assertEquals(mock.buffer(), ret2.buffer());
            assertEquals(mock.offset(), ret2.offset());

            ret1.release();
            assertTrue(mock.released);

            mock.released = false;
            ret2.release();
            assertTrue(mock.released);

            assertTrue(ret2 == rebufferer.rebuffer(0)); // first buffer holder was recycled
            assertTrue(ret1 == rebufferer.rebuffer(1)); // second buffer holder was recycled
        }
    }


    class TestRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;
        boolean released;
        long offset;

        TestRebufferer()
        {
            this.buffer = ByteBuffer.allocate(0);
            this.released = false;
            this.offset = 0;
        }

        public AsynchronousChannelProxy asyncChannel()
        {
            return null;
        }

        public ChannelProxy blockingChannel()
        {
            return null;
        }

        public long fileLength()
        {
            return buffer.remaining();
        }

        public boolean isPastEOF(long position)
        {
            return position > fileLength();
        }

        public double getCrcCheckChance()
        {
            return 0;
        }

        public BufferHolder rebuffer(long position, ReaderConstraint constraint)
        {
            offset = position;
            return this;
        }

        public CompletableFuture<BufferHolder> rebufferAsync(long position)
        {
            return TPCUtils.completedFuture(rebuffer(position));
        }

        public int rebufferSize()
        {
            return buffer.capacity();
        }

        public ByteBuffer unsafeBuffer()
        {
            return buffer;
        }

        public long offset()
        {
            return offset;
        }

        public void release()
        {
            released = true;
        }

        public long adjustExternal(long position)
        {
            return position;
        }

        public long adjustInternal(long position)
        {
            return position;
        }

        public void close()
        {
            // nothing
        }

        public void closeReader()
        {
            // nothing
        }
    }
}
