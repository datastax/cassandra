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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;

/// A [DataOutputPlus] that writes to another for as long as the other's position stays within a limit, and throws
/// [LimitReachedException] instead of making a write that would take it past the limit: for writing a serialization
/// that is only wanted if it is no larger than the limit, without building more of it than that to find out. The read
/// side of such a limit is [TeeDataInputPlus]'s.
public class LimitedDataOutputPlus implements DataOutputPlus
{
    /// Thrown instead of a write that would take the output past its limit. It is how the output reports reaching the
    /// limit, which is expected and can happen for every serialization, so it does not take a stack trace through the
    /// serializer it is thrown from.
    public static final class LimitReachedException extends IOException
    {
        @Override
        public synchronized Throwable fillInStackTrace()
        {
            return this;
        }
    }

    private final DataOutputPlus out;
    private final long limit;

    /// Writes to `out` up to position `limit`. `out` must have a position (see [DataOutputPlus#hasPosition]).
    public LimitedDataOutputPlus(DataOutputPlus out, long limit)
    {
        assert out.hasPosition() : "A limited output needs the position of the output it writes to";
        this.out = out;
        this.limit = limit;
    }

    /// The number of bytes that can still be written, which is negative if the output was already past the limit
    /// when it was given.
    public long remaining()
    {
        return limit - out.position();
    }

    /// Returns the output to write `length` bytes to, if they keep it within the limit.
    private DataOutputPlus reserve(long length) throws LimitReachedException
    {
        if (length > remaining())
            throw new LimitReachedException();
        return out;
    }

    @Override
    public void write(int b) throws IOException
    {
        reserve(1).write(b);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        reserve(b.length).write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        reserve(len).write(b, off, len);
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        reserve(buffer.remaining()).write(buffer);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        reserve(1).writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        reserve(1).writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        reserve(Short.BYTES).writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        reserve(Character.BYTES).writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        reserve(Integer.BYTES).writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        reserve(Long.BYTES).writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        reserve(Float.BYTES).writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        reserve(Double.BYTES).writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        reserve(s.length()).writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        reserve((long) s.length() * Character.BYTES).writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        reserve(Short.BYTES + TypeSizes.encodedUTF8Length(s)).writeUTF(s);
    }

    @Override
    public void writeMostSignificantBytes(long register, int bytes) throws IOException
    {
        reserve(bytes).writeMostSignificantBytes(register, bytes);
    }

    @Override
    public long position()
    {
        return out.position();
    }

    @Override
    public boolean hasPosition()
    {
        return true;
    }
}
