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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataInput;

/**
 * An {@link IndexOutput} writing to a {@link LegacyByteBuffersDataOutput}.
 * This uses the big-endian byte ordering of Lucene 7.5 and is used to write indexes/data compatible with the
 * readers in older Lucene versions.
 * This file was imported from the Apache Lucene project at commit b5bf70b7e32d7ddd9742cc821d471c5fabd4e3df,
 * tagged as releases/lucene-solr/7.5.0. The following modifications have been made to the original file:
 * <ul>
 * <li>Renamed from ByteBuffersIndexOutput to LegacyByteBuffersIndexOutput.</li>
 * <li>Implements our IndexOutput wrapper, which provides endianness.</li>
 * <li>Wraps LegacyByteBuffersDataOutput instead of ByteBuffersDataOutput.</li>
 * </ul>
 */
public final class LegacyByteBuffersIndexOutput extends IndexOutput
{
    private final Consumer<LegacyByteBuffersDataOutput> onClose;

    private final Checksum checksum;
    private long lastChecksumPosition;
    private long lastChecksum;

    private LegacyByteBuffersDataOutput delegate;

    public LegacyByteBuffersIndexOutput(LegacyByteBuffersDataOutput delegate, String resourceDescription, String name)
    {
        this(delegate, resourceDescription, name, new CRC32(), null);
    }

    public LegacyByteBuffersIndexOutput(LegacyByteBuffersDataOutput delegate, String resourceDescription, String name,
                                        Checksum checksum,
                                        Consumer<LegacyByteBuffersDataOutput> onClose)
    {
        super(resourceDescription, name, ByteOrder.BIG_ENDIAN);
        this.delegate = delegate;
        this.checksum = checksum;
        this.onClose = onClose;
    }

    @Override
    public void close() throws IOException
    {
        // No special effort to be thread-safe here since IndexOutputs are not required to be thread-safe.
        LegacyByteBuffersDataOutput local = delegate;
        delegate = null;
        if (local != null && onClose != null)
        {
            onClose.accept(local);
        }
    }

    @Override
    public long getFilePointer()
    {
        ensureOpen();
        return delegate.size();
    }

    @Override
    public long getChecksum() throws IOException
    {
        ensureOpen();

        if (checksum == null)
        {
            throw new IOException("This index output has no checksum computing ability: " + toString());
        }

        // Compute checksum on the current content of the delegate.
        //
        // This way we can override more methods and pass them directly to the delegate for efficiency of writing,
        // while allowing the checksum to be correctly computed on the current content of the output buffer (IndexOutput
        // is per-thread, so no concurrent changes).
        if (lastChecksumPosition != delegate.size())
        {
            lastChecksumPosition = delegate.size();
            checksum.reset();
            byte[] buffer = null;
            for (ByteBuffer bb : delegate.toBufferList())
            {
                if (bb.hasArray())
                {
                    checksum.update(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
                }
                else
                {
                    if (buffer == null) buffer = new byte[1024 * 4];

                    bb = bb.asReadOnlyBuffer();
                    int remaining = bb.remaining();
                    while (remaining > 0)
                    {
                        int len = Math.min(remaining, buffer.length);
                        bb.get(buffer, 0, len);
                        checksum.update(buffer, 0, len);
                        remaining -= len;
                    }
                }
            }
            lastChecksum = checksum.getValue();
        }
        return lastChecksum;
    }

    @Override
    public void writeByte(byte b) throws IOException
    {
        ensureOpen();
        delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException
    {
        ensureOpen();
        delegate.writeBytes(b, offset, length);
    }

    @Override
    public void writeBytes(byte[] b, int length) throws IOException
    {
        ensureOpen();
        delegate.writeBytes(b, length);
    }

    @Override
    public void writeInt(int i) throws IOException
    {
        ensureOpen();
        delegate.writeInt(i);
    }

    @Override
    public void writeShort(short i) throws IOException
    {
        ensureOpen();
        delegate.writeShort(i);
    }

    @Override
    public void writeLong(long i) throws IOException
    {
        ensureOpen();
        delegate.writeLong(i);
    }

    @Override
    public void writeString(String s) throws IOException
    {
        ensureOpen();
        delegate.writeString(s);
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException
    {
        ensureOpen();
        delegate.copyBytes(input, numBytes);
    }

    @Override
    public void writeMapOfStrings(Map<String, String> map) throws IOException
    {
        ensureOpen();
        delegate.writeMapOfStrings(map);
    }

    @Override
    public void writeSetOfStrings(Set<String> set) throws IOException
    {
        ensureOpen();
        delegate.writeSetOfStrings(set);
    }

    private void ensureOpen()
    {
        if (delegate == null)
        {
            throw new AlreadyClosedException("Already closed.");
        }
    }
}
