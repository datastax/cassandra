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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.BytesRef;

/**
 * Offheap unsigned bytes comparable wrapper for ByteBuffer
 */
public class OffheapBytes implements Comparable<OffheapBytes>
{
    public final ByteBuffer buffer;

    public OffheapBytes(byte[] bytes)
    {
        this(new BytesRef(bytes));
    }

    public OffheapBytes(BytesRef bytesRef)
    {
        this.buffer = ByteBuffer.allocateDirect(bytesRef.length);
        this.buffer.put(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        this.buffer.flip();
    }

    public OffheapBytes(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    public ByteComparable toByteComparable()
    {
        return ByteComparable.fixedLength(buffer);
    }

    @Override
    public boolean equals(Object o)
    {
        return compareTo((OffheapBytes) o) == 0;
    }

    @Override
    public int compareTo(OffheapBytes o)
    {
        return ByteBufferUtil.compareUnsigned(buffer, o.buffer);
    }

    // for testing
    BytesRef toBytesRef()
    {
        return new BytesRef(ByteBufferUtil.getArray(buffer));
    }
}
