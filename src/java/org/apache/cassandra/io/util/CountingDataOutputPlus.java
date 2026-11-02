/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;

/// A [DataOutputPlus] that writes nothing and only advances its [#position] by the number of bytes each write would
/// have written: for sizing a serialization that can only be sized by performing it, without building its bytes.
/// Starts at position 0.
public class CountingDataOutputPlus implements DataOutputPlus
{
    private long position;

    @Override
    public void write(int b)
    {
        ++position;
    }

    @Override
    public void write(byte[] b)
    {
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        position += len;
    }

    @Override
    public void write(ByteBuffer buffer)
    {
        position += buffer.remaining();
    }

    @Override
    public void writeBoolean(boolean v)
    {
        ++position;
    }

    @Override
    public void writeByte(int v)
    {
        ++position;
    }

    @Override
    public void writeShort(int v)
    {
        position += Short.BYTES;
    }

    @Override
    public void writeChar(int v)
    {
        position += Character.BYTES;
    }

    @Override
    public void writeInt(int v)
    {
        position += Integer.BYTES;
    }

    @Override
    public void writeLong(long v)
    {
        position += Long.BYTES;
    }

    @Override
    public void writeFloat(float v)
    {
        position += Float.BYTES;
    }

    @Override
    public void writeDouble(double v)
    {
        position += Double.BYTES;
    }

    @Override
    public void writeBytes(String s)
    {
        position += s.length();
    }

    @Override
    public void writeChars(String s)
    {
        position += (long) s.length() * Character.BYTES;
    }

    @Override
    public void writeUTF(String s)
    {
        position += Short.BYTES + TypeSizes.encodedUTF8Length(s);
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public boolean hasPosition()
    {
        return true;
    }
}
