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

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.util.Locale;

import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * A {@link IndexInput} backed by a byte array.
 *
 * ByteBufferIndexInput is nominally the blessed replacement for this, but
 * it's a pretty different API.
 *
 * @lucene.experimental
 */
public final class ByteArrayIndexInput extends IndexInput implements RandomAccessInput
{
    private byte[] bytes;

    private final int offset;
    private final int length;
    private final boolean isBigEndian;

    private int pos;

    public ByteArrayIndexInput(String description, byte[] bytes, ByteOrder order) {
        this(description, bytes, 0, bytes.length, order);
    }

    public ByteArrayIndexInput(String description, byte[] bytes, int offs, int length, ByteOrder order) {
        super(description, order);
        this.offset = offs;
        this.bytes = bytes;
        this.length = length;
        this.pos = offs;
        this.isBigEndian = order == ByteOrder.BIG_ENDIAN;
    }

    public long getFilePointer() {
        return pos - offset;
    }

    public void seek(long pos) throws EOFException {
        int newPos = Math.toIntExact(pos + offset);
        try {
            if (pos < 0 || pos > length) {
                throw new EOFException();
            }
        } finally {
            this.pos = newPos;
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public short readShort() {
        var b1 = bytes[pos++] & 0xFF;
        var b2 = bytes[pos++] & 0xFF;
        return isBigEndian
               ? (short) (b1 << 8 | b2)
               : (short) (b2 << 8 | b1);
    }

    @Override
    public int readInt() {
        var b1 = bytes[pos++] & 0xFF;
        var b2 = bytes[pos++] & 0xFF;
        var b3 = bytes[pos++] & 0xFF;
        var b4 = bytes[pos++] & 0xFF;
        
        return isBigEndian
               ? b1 << 24 | b2 << 16 | b3 << 8 | b4
               : b4 << 24 | b3 << 16 | b2 << 8 | b1;
    }

    @Override
    public long readLong()
    {
        int i1 = readInt();
        int i2 = readInt();
        return isBigEndian
                ? (long) i1 << 32 | i2 & 0xFFFFFFFFL
                : (long) i2 << 32 | i1 & 0xFFFFFFFFL;
    }

    // NOTE: AIOOBE not EOF if you read too much
    @Override
    public byte readByte() {
        return bytes[pos++];
    }

    // NOTE: AIOOBE not EOF if you read too much
    @Override
    public void readBytes(byte[] b, int offset, int len) {
        System.arraycopy(bytes, pos, b, offset, len);
        pos += len;
    }

    @Override
    public void close() {
        bytes = null;
    }

    @Override
    public IndexInput clone() {
        ByteArrayIndexInput slice = slice("(cloned)" + toString(), 0, length());
        try {
            slice.seek(getFilePointer());
        } catch (EOFException e) {
            throw new UncheckedIOException(e);
        }
        return slice;
    }

    public ByteArrayIndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                                                             "slice(offset=%s, length=%s) is out of bounds: %s",
                                                             offset, length, this));
        }

        return new ByteArrayIndexInput(sliceDescription,
                                       this.bytes,
                                       Math.toIntExact(this.offset + offset),
                                       Math.toIntExact(length),
                                       isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return bytes[Math.toIntExact(offset + pos)];
    }

    @Override
    public short readShort(long pos) throws IOException {
        int i = Math.toIntExact(offset + pos);
        var b1 = bytes[i] & 0xFF;
        var b2 = bytes[i + 1] & 0xFF;
        return isBigEndian
                ? (short) (b1 << 8 | b2)
                : (short) (b2 << 8 | b1);
    }

    @Override
    public int readInt(long pos) throws IOException {
        int i = Math.toIntExact(offset + pos);
        var b1 = bytes[i] & 0xFF;
        var b2 = bytes[i + 1] & 0xFF;
        var b3 = bytes[i + 2] & 0xFF;
        var b4 = bytes[i + 3] & 0xFF;
        return isBigEndian
                ? b1 << 24 | b2 << 16 | b3 << 8 | b4
                : b4 << 24 | b3 << 16 | b2 << 8 | b1;
    }

    @Override
    public long readLong(long pos) throws IOException {
        int i = Math.toIntExact(offset + pos);
        int b1 = readInt(i);
        int b2 = readInt(i + 4);
        return isBigEndian
                ? (long) b1 << 32 | b2 & 0xFFFFFFFFL
                : (long) b2 << 32 | b1 & 0xFFFFFFFFL;
    }
}
