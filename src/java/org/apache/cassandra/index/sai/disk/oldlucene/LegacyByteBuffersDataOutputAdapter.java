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

package org.apache.cassandra.index.sai.disk.oldlucene;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.DataInput;

/**
 * Minimal wrapper around LegacyByteBufferDataOutput, to allow for mixed callsites of LegacyByteBufferDataOutput
 * and ByteBufferDataOutput.
 */
public class LegacyByteBuffersDataOutputAdapter extends ByteBuffersDataOutputAdapter
{
    private LegacyByteBuffersDataOutput wrapped;

    public LegacyByteBuffersDataOutputAdapter(long expectedSize)
    {
        wrapped = new LegacyByteBuffersDataOutput(expectedSize);
    }

    @Override
    public void reset()
    {
        wrapped.reset();
    }

    @Override
    public long size()
    {
        return wrapped.size();
    }

    @Override
    public byte[] toArrayCopy()
    {
        return wrapped.toArrayCopy();
    }

    @Override
    public void writeBytes(byte[] b, int length) throws IOException
    {
        wrapped.writeBytes(b, length);
    }

    @Override
    public void writeInt(int i) throws IOException
    {
        wrapped.writeInt(i);
    }

    @Override
    public void writeShort(short i) throws IOException
    {
        wrapped.writeShort(i);
    }

    @Override
    public void writeLong(long i) throws IOException
    {
        wrapped.writeLong(i);
    }

    @Override
    public void writeString(String s) throws IOException
    {
        wrapped.writeString(s);
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException
    {
        wrapped.copyBytes(input, numBytes);
    }

    @Override
    public void writeMapOfStrings(Map<String, String> map) throws IOException
    {
        wrapped.writeMapOfStrings(map);
    }

    @Override
    public void writeSetOfStrings(Set<String> set) throws IOException
    {
        wrapped.writeSetOfStrings(set);
    }

    @Override
    public void writeByte(byte b) throws IOException
    {
        wrapped.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] src, int offset, int length) throws IOException
    {
        wrapped.writeBytes(src, offset, length);
    }
}
