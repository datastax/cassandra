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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.util.BytesRef;

public class OffheapBytesTest extends SaiRandomizedTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testOffheapBytesArray() throws Exception
    {
        OffheapBytesArray array = new OffheapBytesArray();

        List<OffheapBytes> byteBuffers = new ArrayList<>();
        List<BytesRef> bytesRefs = new ArrayList<>();

        for (int x = 0; x < 100_000; x++)
        {
            byte[] bytes = new byte[nextInt(1, 10)];
            nextBytes(bytes);

            BytesRef bytesRef = new BytesRef(bytes);

            OffheapBytes offheapBytes = array.add(bytesRef);

            byte[] bytes2 = ByteBufferUtil.getArray(offheapBytes.buffer);

            assertArrayEquals(bytes, bytes2);

            byteBuffers.add(offheapBytes);
            bytesRefs.add(bytesRef);
        }

        Collections.sort(byteBuffers);
        Collections.sort(bytesRefs);

        for (int x = 0; x < byteBuffers.size(); x++)
        {
            OffheapBytes offheapBytes = byteBuffers.get(x);
            BytesRef bytesRef = bytesRefs.get(x);
            assertArrayEquals(bytesRef.bytes, ByteBufferUtil.getArray(offheapBytes.buffer));
        }
    }

    @Test
    public void testSimple() throws Exception
    {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.flip();

        byte[] bytes2 = ByteBufferUtil.getArray(byteBuffer);

        assertArrayEquals(bytes, bytes2);
    }

    @Test
    public void testSlice() throws Exception
    {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = "hello2".getBytes(StandardCharsets.UTF_8);
        byte[] bytes3 = "hello3".getBytes(StandardCharsets.UTF_8);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(2000);
        byteBuffer.put(bytes);
        byteBuffer.put(bytes2);
        byteBuffer.put(bytes3);

        byteBuffer.flip();

        ByteBuffer slice = byteBuffer.slice();
        slice.position(0);
        slice.limit(bytes.length);

        int position = bytes.length;
        ByteBuffer slice2 = byteBuffer.slice();
        slice2.position(position);
        slice2.limit(position + bytes2.length);

        position = bytes.length + bytes2.length;
        ByteBuffer slice3 = byteBuffer.slice();
        slice3.position(position);
        slice3.limit(position + bytes3.length);

        OffheapBytes offheapBytes2 = new OffheapBytes(slice2);
        OffheapBytes offheapBytes3 = new OffheapBytes(slice3);

        byte[] bytes21 = ByteBufferUtil.getArray(offheapBytes2.buffer);
        assertArrayEquals(bytes2, bytes21);

        byte[] bytes31 = ByteBufferUtil.getArray(offheapBytes3.buffer);
        assertArrayEquals(bytes3, bytes31);
    }

    @Test
    public void test() throws Exception
    {
        List<OffheapBytes> byteBuffers = new ArrayList<>();
        List<BytesRef> bytesRefs = new ArrayList<>();

        for (int x = 0; x < 2000; x++)
        {
            byte[] bytes = new byte[nextInt(1, 10)];
            nextBytes(bytes);

            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            OffheapBytes offheapBytes = new OffheapBytes(byteBuffer);

            BytesRef bytesRef = new BytesRef(bytes);

            byteBuffers.add(offheapBytes);
            bytesRefs.add(bytesRef);
        }

        Collections.sort(byteBuffers);
        Collections.sort(bytesRefs);

        for (int x = 0; x < byteBuffers.size(); x++)
        {
            OffheapBytes offheapBytes = byteBuffers.get(x);
            BytesRef bytesRef = bytesRefs.get(x);
            assertArrayEquals(bytesRef.bytes, ByteBufferUtil.getArray(offheapBytes.buffer));
        }
    }
}
