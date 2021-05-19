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

package org.apache.cassandra.db.memtable.pmem;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;

public class MemoryBlockDataOutputPlus implements DataOutputPlus
{
    private final TransactionalMemoryBlock block;
    private final long size;
    private int position;

    public MemoryBlockDataOutputPlus(TransactionalMemoryBlock block, int initialPosition)
    {
        this.block = block;
        size = block.size();
        position = initialPosition;
    }

    public boolean hasPosition()
    {
        return true;
    }

    public long position()
    {
        return position;
    }

    public void position(int position)
    {
        this.position = position;
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        byte[] bufferArray = buffer.array();
        block.copyFromArray(bufferArray,0,position, bufferArray.length);
        position += bufferArray.length;
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    @Override
    public void write(int b) throws IOException
    {
        block.setByte(position, (byte) b);
        position++;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        block.copyFromArray(b, 0, position, b.length);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        block.copyFromArray(b, off, position, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v)
    {
        block.setByte(position, v ? (byte) 1 : (byte) 0);
        position++;
    }

    @Override
    public void writeByte(int v)
    {
        block.setByte(position, (byte) v);
        position++;
    }

    @Override
    public void writeShort(int v)
    {
        block.setShort(position, (short) v);
        position += 2;
    }

    @Override
    public void writeChar(int v)
    {
        block.setByte(position, (byte) v);
        position += 2;
    }

    @Override
    public void writeInt(int v)
    {
        block.setInt(position, v);
        position += 4;
    }

    @Override
    public void writeLong(long v)
    {
        block.setLong(position, v);
        position += 8;
    }

    @Override
    public void writeFloat(float v)
    {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v)
    {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    public void writeBytes(String s)
    {
        for (int index = 0; index < s.length(); index++)
            writeByte(s.charAt(index));
    }

    @Override
    public void writeChars(String s)
    {
        for (int index = 0; index < s.length(); index++)
            writeChar(s.charAt(index));
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        try
        {
            UnbufferedDataOutputStreamPlus.writeUTF(s, this);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
