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

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.util.BitSet;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

public enum OnDiskWriteNodeType
{
    LEAF(0b00000000),

    CHAIN(0b01000000)
    {
        @Override
        long sizeChildren(int bytesPerPointer, FileWriter.Node<?>[] children)
        {
            assert children.length == 1;
            return (children[0].firstTransition != -1 ? 1 + 1 : 0) +
                   maybeSizeRelay(children, bytesPerPointer);
        }

        @Override
        void writeChildren(DataOutputPlus out, FileWriter.Node<?>[] children, long basePos, int bytesPerPointer) throws IOException
        {
            assert children.length == 1;
            maybeWriteRelay(out, children, basePos, 1);
            // Node may have moved its transition into the leading chain of its child. If so, there's no node to write
            // here.
            if (children[0].firstTransition != -1)
            {
                out.writeByte(children[0].firstTransition);
                out.writeByte(bits);    // length 1
            }
        }
    },

    PREFIX(0b11110000),

    RELAY(0b11111000),

    DENSE(0b11101000)
    {
        @Override
        long sizeChildren(int bytesPerPointer, FileWriter.Node<?>[] children)
        {
            // last pointer is not implicit here
            return 256 * bytesPerPointer + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, FileWriter.Node<?>[] children, long basePos, int bytesPerPointer) throws IOException
        {
            int size = children.length;
            // last pointer is not implicit here
            int index = size - 1;
            for (int i = 0; i <= 255; ++i)
            {
                if (index >= 0 && i == children[index].firstTransition)
                    FileWriter.writeReversedSized(out, basePos - children[index--].writtenFilePos, bytesPerPointer);
                else
                    FileWriter.writeReversedSized(out, -1L, bytesPerPointer);
            }
            assert index == -1;
            out.writeByte(bits | (bytesPerPointer - 1));
        }
    },

    BITMAP(0b11100000)
    {
        @Override
        long sizeChildren(int bytesPerPointer, FileWriter.Node<?>[] children)
        {
            return (children.length - 1) * bytesPerPointer + 32 + 1 +
                   maybeSizeRelay(children, bytesPerPointer);
        }

        @Override
        void writeChildren(DataOutputPlus out, FileWriter.Node<?>[] children, long basePos, int bytesPerPointer) throws IOException
        {
            int size = writePointers(out, children, basePos, bytesPerPointer);
            BitSet bits = new BitSet(256);
            for (int i = 0; i < size; ++i)
                bits.set(children[i].firstTransition);
            long[] bitsAsLong = bits.toLongArray();
            for (int i = 3; i >= 0; --i)
                out.writeLong(bitsAsLong[i]);   // lowest-order bytes ends up last
            out.writeByte(this.bits | (bytesPerPointer - 1));
        }
    },

    SPARSE(0b10000000)
    {
        @Override
        long sizeChildren(int bytesPerPointer, FileWriter.Node<?>[] children)
        {
            return (children.length - 1) * bytesPerPointer + children.length + 1 +
                   maybeSizeRelay(children, bytesPerPointer);
        }

        @Override
        void writeChildren(DataOutputPlus out, FileWriter.Node<?>[] children, long basePos, int bytesPerPointer) throws IOException
        {
            int size = writePointers(out, children, basePos, bytesPerPointer);
            for (int i = size - 1; i >= 0; --i)
                out.writeByte(children[i].firstTransition);
            out.writeByte(bits | ((size - 2) << SHIFT_SPARSE_LENGTH) | (bytesPerPointer - 1));
        }
    };

    final int bits;

    static final int MAX_LEAF_LENGTH_INCLUSIVE = 63;

    static final int MAX_CHAIN_LENGTH_INCLUSIVE = 64;

    // sparse: 0x80 - 0xE0 (96 positions) 1lllllbb, lllll is count - 2 and must be < 24, bb is bytes per pointer
    static final int MAX_SPARSE_LENGTH_INCLUSIVE = 25;
    static final int MAX_SPARSE_BYTES = 4;
    static final int SHIFT_SPARSE_LENGTH = 2;

    static final int PREFIX_HAS_CHILD = 0b00000001;
    static final int PREFIX_HAS_ASCENT_CONTENT = 0b00000010;
    static final int PREFIX_HAS_DESCENT_CONTENT = 0b00000100;

    OnDiskWriteNodeType(int bits)
    {
        this.bits = bits;
    }

    static <T> long sizePayload(FileWriter.DataSerializer<T> serializer, T descentData, T ascentData, boolean hasChild)
    {
        if (descentData == null && ascentData == null)
            return 0;

        int descentDataSize = -1;
        if (descentData != null)
            descentDataSize = serializer.serializedSize(descentData);

        if (hasChild || ascentData != null || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
        {
            long size = 1;
            if (descentDataSize >= 0)
            {
                size += descentDataSize + VIntCoding.computeUnsignedVIntSize(descentDataSize);
            }

            if (ascentData != null)
            {
                int ascentDataSize = serializer.serializedSize(ascentData);
                size += ascentDataSize + VIntCoding.computeUnsignedVIntSize(ascentDataSize);
            }

            return size;
        }
        else
            return descentDataSize + 1; // certainly smaller than a page
    }

    static <T> void writePayload(DataOutputPlus out, FileWriter.DataSerializer<T> serializer, T descentData, T ascentData, boolean hasChild) throws IOException
    {
        if (descentData == null && ascentData == null)
            return;

        int descentDataSize = -1;
        if (descentData != null)
            descentDataSize = serializer.serialize(out, descentData);

        if (hasChild || ascentData != null || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
        {
            int code = PREFIX.bits;
            if (descentDataSize >= 0)
            {
                FileWriter.writeReversedVint(out, descentDataSize);
                code |= PREFIX_HAS_DESCENT_CONTENT;
            }
            if (ascentData != null)
            {
                int ascentDataSize = serializer.serialize(out, ascentData);
                FileWriter.writeReversedVint(out, ascentDataSize);
                code |= PREFIX_HAS_ASCENT_CONTENT;
            }
            if (hasChild)
                code |= PREFIX_HAS_CHILD;

            out.writeByte(code);
        }
        else
            out.writeByte(LEAF.bits | descentDataSize);
    }

    static OnDiskWriteNodeType selectChildrenType(int bytesPerPointer, int pointerCount)
    {
        if (pointerCount == 1)
            return CHAIN;
        else if (pointerCount <= MAX_SPARSE_LENGTH_INCLUSIVE && bytesPerPointer <= MAX_SPARSE_BYTES)
            return SPARSE;
        else if ((256 - pointerCount) * bytesPerPointer > 32) // if we will save at least one byte over DENSE
            return BITMAP;
        else
            return DENSE;
    }

    long sizeChildren(int bytesPerPointer, FileWriter.Node<?>[] children)
    {
        // Throw by default, only applies to RELAY, SPARSE, BITMAP and DENSE
        throw new AssertionError();
    }

    void writeChildren(DataOutputPlus out, FileWriter.Node<?> children[], long base, int bytesPerPointer) throws IOException
    {
        // Throw by default, only applies to RELAY, SPARSE, BITMAP and DENSE
        throw new AssertionError();
    }

    static long sizeChain(byte[] bytes)
    {
        int length = bytes.length;
        return length + (length + MAX_CHAIN_LENGTH_INCLUSIVE - 1) / MAX_CHAIN_LENGTH_INCLUSIVE;
    }

    static void writeChain(DataOutputPlus out, byte[] bytes) throws IOException
    {
        int remaining = bytes.length;
        int current = remaining - 1;
        while (remaining > 0)
        {
            int len = Math.min(remaining, MAX_CHAIN_LENGTH_INCLUSIVE);
            int stop = remaining - len;
            while (current >= stop)
                out.writeByte(bytes[current--]);
            out.writeByte(CHAIN.bits | (len - 1));
            remaining -= len;
        }
    }

    static long sizeRelay(int bytesPerPointer)
    {
        return bytesPerPointer + 1;
    }

    static long writeRelay(DataOutputPlus out, long filePos, long base) throws IOException
    {
        assert filePos >= 0;
        int bytesPerPointer = FileWriter.bytesFor(base - filePos);
        FileWriter.writeReversedSized(out, base - filePos, bytesPerPointer);
        out.writeByte(RELAY.bits | (bytesPerPointer - 1));
        return base + bytesPerPointer + 1;
    }


    private static boolean implicitFirstChild(FileWriter.Node<?>[] children)
    {
        // If the last child is not written yet, it will be written immediately before parent and won't need a relay.
        return children[children.length - 1].writtenFilePos < 0;
    }

    static long maybeSizeRelay(FileWriter.Node<?>[] children, int bytesPerPointer)
    {
        return implicitFirstChild(children) ? 0 : sizeRelay(bytesPerPointer);
    }

    static int writePointers(DataOutputPlus out, FileWriter.Node<?>[] children, long basePos, int bytesPerPointer) throws IOException
    {
        int size = children.length;
        basePos = maybeWriteRelay(out, children, basePos, size);
        for (int i = size - 2; i >= 0; --i)
            FileWriter.writeReversedSized(out, basePos - children[i].writtenFilePos, bytesPerPointer);
        return size;
    }

    private static long maybeWriteRelay(DataOutputPlus out, FileWriter.Node<?>[] children, long basePos, int size) throws IOException
    {
        long firstNodePos = children[size - 1].writtenFilePos;
        if (firstNodePos != basePos)
            basePos = writeRelay(out, firstNodePos, basePos);
        return basePos;
    }
}
