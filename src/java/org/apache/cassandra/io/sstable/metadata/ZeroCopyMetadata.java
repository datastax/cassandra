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

package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.primitives.Longs;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Metadata related to sstables copied via zero copy, stored in the {@link StatsMetadata}. ZCS allows for streaming
 * part of the data file while streaming the original index and stats files (thus data file positions included by the
 * indexes refer to the orignal data file rather than to the streamed slice). This metadata contains information about
 * slice of the data file that was streamed this way.
 * <ul>
 * <li>{@link #dataStart}:     Disk offset in the original data file where the first streamed partition begins (inclusive)</li>
 * <li>{@link #dataEnd}:       Disk offset in the original data file where the last streamed row ends (exclusive)</li>
 * <li>{@link #firstKey}:      Byte representation of the first key in the streamed sstable slice</li>
 * <li>{@link #lastKey}:       Byte representation of the last key in the streamed sstable slice</li>
 * <li>{@link #chunkSize}:     If the file is written/read in chunks, this is the size in bytes of the chunk, otherwise 0</li>
 * <li>{@link #estimatedKeys}: Estimated keys contained in the sstable</li>
 * </ul>
 * When the sstable is written in chunks, the {@link #dataStart} might not match the actual offset as it needs to be
 * aligned by the {@link #chunkSize}. In such case, if you need to access the actual start offset, please use
 * {@link #sliceStart}. For example:
 * <pre>
 * 0                16               32               48               64               80               96
 * |----------------|----------------|----------------|----------------|----------------|----------------|
 * | chunk 1        | chunk 2        | chunk 3        | chunk 4        | chunk 5        | chunk 6        |
 * |----------------|----------------|----------------|----------------|----------------|----------------|
 *  #key1      #key2                    #key3      #key4                    #key5     #key6     #key7
 * </pre>
 * Say the slice ZCS sends is from chunk 3 to chunk 5. The first key is 3 and the last key is 5. The start offset
 * is 34 (the exact position) and the start offset aligned is 32 (the position aligned to the chunk size). The end
 * offset is 78 (the exact position).
 * The transferred data file looks as follows:
 * <pre>
 * 0                16               32               48
 * |----------------|----------------|----------------|
 * | chunk 3        | chunk 4        | chunk 5        |
 * |----------------|----------------|----------------|
 *    #key3      #key4                    #key5
 * </pre>
 * So to get the actual start offset of the first key, that is 2, you need to calculate is as follows:
 * {@code dataStart - sliceStart}. When you get a position of say key 4, it is 45, so you need to calculate
 * the actual position in slice as follows: {@code 45 - sliceStart}, which gives you 12 - the key 4 position in
 * the local slice.
 */
public class ZeroCopyMetadata extends SliceDescriptor
{
    public static final Serializer serializer = new Serializer();
    public static final ZeroCopyMetadata EMPTY = new ZeroCopyMetadata(0, 0, 0, 0, null, null);

    private final long estimatedKeys;
    private final ByteBuffer firstKey;
    private final ByteBuffer lastKey;

    public ZeroCopyMetadata(long dataStart, long dataEnd, int chunkSize, long estimatedKeys, ByteBuffer firstKey, ByteBuffer lastKey)
    {
        super(dataStart, dataEnd, chunkSize);
        this.estimatedKeys = estimatedKeys;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }

    public ByteBuffer firstKey()
    {
        return this.firstKey.duplicate();
    }

    public ByteBuffer lastKey()
    {
        return this.lastKey.duplicate();
    }

    public long estimatedKeys()
    {
        return this.estimatedKeys;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ZeroCopyMetadata that = (ZeroCopyMetadata) o;
        return estimatedKeys == that.estimatedKeys
               && Objects.equals(firstKey, that.firstKey)
               && Objects.equals(lastKey, that.lastKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), estimatedKeys, firstKey, lastKey);
    }

    public static class Serializer implements ISerializer<ZeroCopyMetadata>
    {
        @Override
        public long serializedSize(ZeroCopyMetadata metadata)
        {
            if (!metadata.exists())
                return 0;

            return 3 * Longs.BYTES + Integer.BYTES + ByteBufferUtil.serializedSizeWithShortLength(metadata.firstKey) + ByteBufferUtil.serializedSizeWithShortLength(metadata.lastKey);
        }

        @Override
        public void serialize(ZeroCopyMetadata component, DataOutputPlus out) throws IOException
        {
            out.writeLong(component.dataStart);
            out.writeLong(component.dataEnd);
            out.writeInt(component.chunkSize);
            out.writeLong(component.estimatedKeys);
            ByteBufferUtil.writeWithShortLength(component.firstKey.duplicate(), out);
            ByteBufferUtil.writeWithShortLength(component.lastKey.duplicate(), out);
        }

        @Override
        public ZeroCopyMetadata deserialize(DataInputPlus in) throws IOException
        {
            return new ZeroCopyMetadata(
                in.readLong(), 
                in.readLong(), 
                in.readInt(), 
                in.readLong(), 
                ByteBufferUtil.readWithShortLength(in), 
                ByteBufferUtil.readWithShortLength(in));
        }
    }
}
