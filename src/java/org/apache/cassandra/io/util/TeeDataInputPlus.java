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

import java.io.EOFException;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;

/**
 * DataInput that also stores the raw inputs into an output buffer
 * This is useful for storing serialized buffers as they are deserialized.
 *
 * Note: If a non-zero limit is included it is important to for callers to check {@link #isLimitReached()}
 * before using the tee buffer as it could be cropped.
 */
public class TeeDataInputPlus implements DataInputPlus
{
    private final DataInputPlus source;
    private final DataOutputPlus teeBuffer;

    private final long limit;
    private boolean limitReached;

    public TeeDataInputPlus(DataInputPlus source, DataOutputPlus teeBuffer)
    {
        this(source, teeBuffer, 0);
    }

    public TeeDataInputPlus(DataInputPlus source, DataOutputPlus teeBuffer, long limit)
    {
        assert source != null && teeBuffer != null;
        this.source = source;
        this.teeBuffer = teeBuffer;
        this.limit = limit;
        this.limitReached = false;
    }

    /**
     * Checks if writing {@code length} bytes to the tee buffer will remain within the configured {@code limit}.
     * Returns true if writing is permitted, or false if the byte limit would be exceeded.
     * Sets {@code limitReached} to true upon the first boundary breach to fast-path subsequent reads.
     */
    private boolean canWrite(int length)
    {
        if (limit <= 0)
            return true;
        if (limitReached)
            return false;
        if ((teeBuffer.position() + length) <= limit)
            return true;
        limitReached = true;
        return false;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException
    {
        source.readFully(bytes);
        if (canWrite(bytes.length))
            teeBuffer.write(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int offset, int length) throws IOException
    {
        source.readFully(bytes, offset, length);
        if (canWrite(length))
            teeBuffer.write(bytes, offset, length);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        for (int i = 0; i < n; i++)
        {
            try
            {
                byte v = source.readByte();
                if (canWrite(TypeSizes.BYTE_SIZE))
                    teeBuffer.writeByte(v);
            }
            catch (EOFException eof)
            {
                return i;
            }
        }
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        boolean v = source.readBoolean();
        if (canWrite(TypeSizes.BOOL_SIZE))
            teeBuffer.writeBoolean(v);
        return v;
    }

    @Override
    public byte readByte() throws IOException
    {
        byte v = source.readByte();
        if (canWrite(TypeSizes.BYTE_SIZE))
            teeBuffer.writeByte(v);
        return v;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        int v = source.readUnsignedByte();
        if (canWrite(TypeSizes.BYTE_SIZE))
            teeBuffer.writeByte(v);
        return v;
    }

    @Override
    public short readShort() throws IOException
    {
        short v = source.readShort();
        if (canWrite(TypeSizes.SHORT_SIZE))
            teeBuffer.writeShort(v);
        return v;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        int v = source.readUnsignedShort();
        if (canWrite(TypeSizes.SHORT_SIZE))
            teeBuffer.writeShort(v);
        return v;
    }

    @Override
    public char readChar() throws IOException
    {
        char v = source.readChar();
        if (canWrite(TypeSizes.SHORT_SIZE))
            teeBuffer.writeChar(v);
        return v;
    }

    @Override
    public int readInt() throws IOException
    {
        int v = source.readInt();
        if (canWrite(TypeSizes.INT_SIZE))
            teeBuffer.writeInt(v);
        return v;
    }

    @Override
    public long readLong() throws IOException
    {
        long v = source.readLong();
        if (canWrite(TypeSizes.LONG_SIZE))
            teeBuffer.writeLong(v);
        return v;
    }

    @Override
    public float readFloat() throws IOException
    {
        float v = source.readFloat();
        if (canWrite(TypeSizes.FLOAT_SIZE))
            teeBuffer.writeFloat(v);
        return v;
    }

    @Override
    public double readDouble() throws IOException
    {
        double v = source.readDouble();
        if (canWrite(TypeSizes.DOUBLE_SIZE))
            teeBuffer.writeDouble(v);
        return v;
    }

    @Override
    public String readLine() throws IOException
    {
        //This one isn't safe since we know the actual line termination type
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        String v = source.readUTF();
        if (canWrite(TypeSizes.sizeof(v)))
            teeBuffer.writeUTF(v);
        return v;
    }

    @Override
    public long readVInt() throws IOException
    {
        long v = source.readVInt();
        if (canWrite(TypeSizes.sizeofVInt(v)))
            teeBuffer.writeVInt(v);
        return v;
    }

    @Override
    public long readUnsignedVInt() throws IOException
    {
        long v = source.readUnsignedVInt();
        if (canWrite(TypeSizes.sizeofUnsignedVInt(v)))
            teeBuffer.writeUnsignedVInt(v);
        return v;
    }

    @Override
    public void skipBytesFully(int n) throws IOException
    {
        source.skipBytesFully(n);
        if (canWrite(n))
        {
            for (int i = 0; i < n; i++)
                teeBuffer.writeByte(0);
        }
    }

    /**
     * Used to detect if the teeBuffer hit the supplied limit.
     * If true this means the teeBuffer does not contain the full input.
     */
    public boolean isLimitReached()
    {
        return limitReached;
    }
}
