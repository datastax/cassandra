/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RandomAccessInput;

/**
 * An {@link IndexInput} implementing {@link RandomAccessInput} and backed
 * by a {@link LegacyByteBuffersDataInput}. Data is read in big-endian byte order,
 * as produced by Lucene 7.5.
 * This file was imported from the Apache Lucene project at commit b5bf70b7e32d7ddd9742cc821d471c5fabd4e3df,
 * tagged as releases/lucene-solr/7.5.0. The following modifications have been made to the original file:
 * <ul>
 * <li>Renamed from ByteBuffersIndexInput to LegacyByteBuffersIndexInput.</li>
 * <li>Implements our IndexInput wrapper, which provides endianness.</li>
 * <li>Wraps LegacyByteBuffersDataInput instead of ByteBuffersDataInput.</li>
 * </ul>
 */
public final class LegacyByteBuffersIndexInput extends IndexInput implements RandomAccessInput
{
    private LegacyByteBuffersDataInput in;

    public LegacyByteBuffersIndexInput(LegacyByteBuffersDataInput in, String resourceDescription)
    {
        super(resourceDescription, ByteOrder.BIG_ENDIAN);
        this.in = in;
    }

    @Override
    public void close() throws IOException
    {
        in = null;
    }

    @Override
    public long getFilePointer()
    {
        ensureOpen();
        return in.position();
    }

    @Override
    public void seek(long pos) throws IOException
    {
        ensureOpen();
        in.seek(pos);
    }

    @Override
    public long length()
    {
        ensureOpen();
        return in.size();
    }

    @Override
    public LegacyByteBuffersIndexInput slice(String sliceDescription, long offset, long length) throws IOException
    {
        ensureOpen();
        return new LegacyByteBuffersIndexInput(in.slice(offset, length),
                                               "(sliced) offset=" + offset + ", length=" + length + " " + toString() + " [slice=" + sliceDescription + "]");
    }

    @Override
    public byte readByte() throws IOException
    {
        ensureOpen();
        return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException
    {
        ensureOpen();
        in.readBytes(b, offset, len);
    }

    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException
    {
        ensureOpen();
        return slice("", offset, length);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException
    {
        ensureOpen();
        in.readBytes(b, offset, len, useBuffer);
    }

    @Override
    public short readShort() throws IOException
    {
        ensureOpen();
        return in.readShort();
    }

    @Override
    public int readInt() throws IOException
    {
        ensureOpen();
        return in.readInt();
    }

    @Override
    public int readVInt() throws IOException
    {
        ensureOpen();
        return in.readVInt();
    }

    @Override
    public int readZInt() throws IOException
    {
        ensureOpen();
        return in.readZInt();
    }

    @Override
    public long readLong() throws IOException
    {
        ensureOpen();
        return in.readLong();
    }

    @Override
    public long readVLong() throws IOException
    {
        ensureOpen();
        return in.readVLong();
    }

    @Override
    public long readZLong() throws IOException
    {
        ensureOpen();
        return in.readZLong();
    }

    @Override
    public String readString() throws IOException
    {
        ensureOpen();
        return in.readString();
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException
    {
        ensureOpen();
        return in.readMapOfStrings();
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException
    {
        ensureOpen();
        return in.readSetOfStrings();
    }

    @Override
    public void skipBytes(long numBytes) throws IOException
    {
        ensureOpen();
        super.skipBytes(numBytes);
    }

    @Override
    public byte readByte(long pos) throws IOException
    {
        ensureOpen();
        return in.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException
    {
        ensureOpen();
        return in.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException
    {
        ensureOpen();
        return in.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException
    {
        ensureOpen();
        return in.readLong(pos);
    }

    @Override
    public IndexInput clone()
    {
        ensureOpen();
        LegacyByteBuffersIndexInput cloned = new LegacyByteBuffersIndexInput(in.slice(0, in.size()), "(clone of) " + toString());
        try
        {
            cloned.seek(getFilePointer());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        return cloned;
    }

    private void ensureOpen()
    {
        if (in == null)
        {
            throw new AlreadyClosedException("Already closed.");
        }
    }
}
