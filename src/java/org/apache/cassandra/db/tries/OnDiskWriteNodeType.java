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
import java.util.Arrays;
import java.util.BitSet;

import org.apache.cassandra.io.util.DataOutputPlus;

public enum OnDiskWriteNodeType
{
    // Note: Any changes to bits must be reflected in OnDiskReadNodeType.IMPLEMENTATIONS.

    LEAF(0b00000000),

    CHAIN(0b01000000)
    {
        @Override
        void writeChildren(DataOutputPlus out, OnDiskTrieWriter.Node<?> node, long basePos, int bytesPerPointer) throws IOException
        {
            assert node.childCount() == 1;
            out.writeByte(node.childTransition(0));
            out.writeByte(bits);    // length 1
        }
    },

    PREFIX(0b11110000),

    DENSE(0b11101000)
    {
        @Override
        void writeChildren(DataOutputPlus out, OnDiskTrieWriter.Node<?> node, long basePos, int bytesPerPointer) throws IOException
        {
            int size = node.childCount();
            // first pointer is not implicit here
            int index = 0;
            for (int i = 255; i >= 0; --i)
            {
                if (index < size && i == node.childTransition(index))
                    OnDiskTrieWriter.writeReversedSized(out, basePos - node.child(index++), bytesPerPointer);
                else
                    OnDiskTrieWriter.writeReversedSized(out, -1L, bytesPerPointer);
            }
            assert index == size;
            out.writeByte(bits | (bytesPerPointer - 1));
        }
    },

    BITMAP(0b11100000)
    {
        @Override
        void writeChildren(DataOutputPlus out, OnDiskTrieWriter.Node<?> node, long basePos, int bytesPerPointer) throws IOException
        {
            int size = writePointers(out, node, basePos, bytesPerPointer);
            BitSet bits = new BitSet(256);
            for (int i = 0; i < size; ++i)
                bits.set(node.childTransition(i));
            // toLongArray() trims to the highest set bit, so a node whose transitions all fall in the
            // low half returns fewer than 4 longs. The format always reserves 32 bytes here, so pad back out to 4.
            long[] bitsAsLong = Arrays.copyOf(bits.toLongArray(), 4);
            for (int i = 3; i >= 0; --i)
                out.writeLong(bitsAsLong[i]);   // lowest-order bytes ends up last
            out.writeByte(this.bits | (bytesPerPointer - 1));
        }
    },

    SPARSE(0b10000000)
    {
        @Override
        void writeChildren(DataOutputPlus out, OnDiskTrieWriter.Node<?> node, long basePos, int bytesPerPointer) throws IOException
        {
            int size = writePointers(out, node, basePos, bytesPerPointer);
            for (int i = 0; i < size; ++i)
                out.writeByte(node.childTransition(i));
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

    static <T> void writePayload(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer, T descentData, T ascentData, boolean hasChild) throws IOException
    {
        if (descentData == null && ascentData == null)
            return;

        // A node is read backwards from its code byte, and the reader takes the content block
        // adjacent to the code as the descent side (see OnDiskReadNodeType.PREFIX and the layout in
        // OnDiskTrie.md), so the ascent block has to be emitted first.
        int code = PREFIX.bits;
        if (ascentData != null)
        {
            long ascentStart = out.position();
            int ascentDataSize = serializer.serialize(out, ascentData);
            assert ascentDataSize == out.position() - ascentStart
                : "Serializer reported " + ascentDataSize + " bytes but wrote " + (out.position() - ascentStart);
            OnDiskTrieWriter.writeReversedVint(out, ascentDataSize);
            code |= PREFIX_HAS_ASCENT_CONTENT;
        }

        int descentDataSize = -1;
        if (descentData != null)
        {
            long descentStart = out.position();
            descentDataSize = serializer.serialize(out, descentData);
            assert descentDataSize == out.position() - descentStart
                : "Serializer reported " + descentDataSize + " bytes but wrote " + (out.position() - descentStart);
        }

        if (hasChild || ascentData != null || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
        {
            if (descentDataSize >= 0)
            {
                OnDiskTrieWriter.writeReversedVint(out, descentDataSize);
                code |= PREFIX_HAS_DESCENT_CONTENT;
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

    void writeChildren(DataOutputPlus out, OnDiskTrieWriter.Node<?> node, long basePos, int bytesPerPointer) throws IOException
    {
        // Throw by default, only applies to CHAIN, SPARSE, BITMAP and DENSE
        throw new AssertionError();
    }

    /// Write the explicit pointers of a sparse or bitmap node: every child but the last written, whose delta is 0 and
    /// is left implicit.
    static int writePointers(DataOutputPlus out, OnDiskTrieWriter.Node<?> node, long basePos, int bytesPerPointer) throws IOException
    {
        int size = node.childCount();
        for (int i = 0; i < size - 1; ++i)
            OnDiskTrieWriter.writeReversedSized(out, basePos - node.child(i), bytesPerPointer);
        return size;
    }
}
