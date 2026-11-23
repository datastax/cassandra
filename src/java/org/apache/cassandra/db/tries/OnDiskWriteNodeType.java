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

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

/// The node encodings of the on-disk trie format, as used when writing. See [OnDiskTrie.md](./OnDiskTrie.md).
///
/// The methods here only encode: they are given the positions of a node's children and the place the node is written
/// at, and are shared by all [OnDiskNodeWriter] implementations. Deciding when a node is written and where its children
/// end up is the job of the node writer.
///
/// A node's children are passed as parallel arrays of positions (the end of each child's serialization) and transition
/// bytes, ordered by descending transition, i.e. in the order a reverse walk of the trie completes them. The last child
/// in the arrays is thus the first child of the node, the one the sparse, bitmap and chain types do not store a pointer
/// for: it, or a relay to it (see [WithRelay]), must end exactly where the node begins.
abstract class OnDiskWriteNodeType
{
    // Note: Any changes to bits must be reflected in OnDiskReadNodeType.IMPLEMENTATIONS.

    static final int LEAF_BITS = 0b00000000;

    static final int PREFIX_BITS = 0b11110000;

    static class Chain extends OnDiskWriteNodeType
    {
        Chain()
        {
            super(0b01000000);
        }

        @Override
        int sizeChildren(int count)
        {
            return 2;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException
        {
            assert count == 1;
            out.writeByte(transitions[0]);
            out.writeByte(bits);    // length 1
        }

        @Override
        long sizeValidFor(long furthestDelta)
        {
            return Long.MAX_VALUE;
        }
    }

    /// A version of Chain used for situations where the transition byte is folded into the child's leading chain
    static class FoldedChain extends Chain
    {
        FoldedChain()
        {
            super();
        }

        @Override
        int sizeChildren(int count)
        {
            return 0;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException
        {
            // nothing to write
        }

        // inherits sizeValidFor
    }

    static abstract class ThreeBitBPP extends OnDiskWriteNodeType
    {
        ThreeBitBPP(int bitsBase, int bytesPerPointer)
        {
            super(bitsBase | (bytesPerPointer - 1));
        }

        int bpp()
        {
            return (bits & 7) + 1;
        }

        @Override
        long sizeValidFor(long furthestDelta)
        {
            return OnDiskNodeWriterPageAware.positionsWithSameBytes(furthestDelta);
        }
    }

    static class Dense extends ThreeBitBPP
    {
        Dense(int bytesPerPointer)
        {
            super(0b11101000, bytesPerPointer);
        }

        @Override
        int sizeChildren(int count)
        {
            // first pointer is not implicit here
            return 256 * bpp() + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException
        {
            int bytesPerPointer = bpp();
            // bytesPerPointer is sized for one more than the furthest delta, so that no real delta is all-ones, which
            // marks an absent child (see OnDiskReadNodeType.DENSE.notPresent).
            // first pointer is not implicit here
            int index = 0;
            for (int i = 255; i >= 0; --i)
            {
                if (index < count && i == (transitions[index] & 0xFF))
                {
                    long delta = basePos - positions[index++];
                    assert delta >= 0 && OnDiskBaseTrieWriter.bytesFor(delta + 1) <= bytesPerPointer;
                    OnDiskBaseTrieWriter.writeReversedSized(out, delta, bytesPerPointer);
                }
                else
                    OnDiskBaseTrieWriter.writeReversedSized(out, -1L, bytesPerPointer);
            }
            assert index == count;
            out.writeByte(bits);
        }

        @Override
        long sizeValidFor(long furthestDelta, long firstDelta, boolean firstIsWritten)
        {
            return OnDiskNodeWriterPageAware.positionsWithSameBytes(Math.max(firstDelta, furthestDelta) + 1);
        }
    }

    static class Bitmap extends ThreeBitBPP
    {
        Bitmap(int bytesPerPointer)
        {
            super(0b11100000, bytesPerPointer);
        }

        @Override
        int sizeChildren(int count)
        {
            return (count - 1) * bpp() + 32 + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException
        {
            writePointers(out, positions, count, basePos, bpp());
            // Bit t of the bitmap is bit t % 64 of word t / 64, the layout of BitSet.toLongArray.
            long[] bitsAsLong = new long[4];
            for (int i = 0; i < count; ++i)
            {
                int transition = transitions[i] & 0xFF;
                bitsAsLong[Integer.divideUnsigned(transition, 64)] |= 1L << Integer.remainderUnsigned(transition, 64);
            }
            for (int i = 3; i >= 0; --i)
                out.writeLong(bitsAsLong[i]);   // lowest-order bytes ends up last
            out.writeByte(this.bits);
        }

        @Override
        long sizeValidFor(long furthestDelta)
        {
            return OnDiskNodeWriterPageAware.positionsWithSameBytes(furthestDelta);
        }
    }

    static class Sparse extends OnDiskWriteNodeType
    {
        Sparse(int bytesPerPointer)
        {
            super(0b10000000 | (bytesPerPointer - 1));
        }

        int bpp()
        {
            return (bits & 3) + 1;
        }

        @Override
        int sizeChildren(int count)
        {
            return (count - 1) * bpp() + count + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException
        {
            writePointers(out, positions, count, basePos, bpp());
            for (int i = 0; i < count; ++i)
                out.writeByte(transitions[i]);
            out.writeByte(bits | ((count - 2) << SHIFT_SPARSE_LENGTH));
        }

        @Override
        long sizeValidFor(long furthestDelta)
        {
            return OnDiskNodeWriterPageAware.positionsWithSameBytes(furthestDelta);
        }
    }

    /// An epsilon transition to a node placed elsewhere. Written immediately before a node whose first child could not
    /// be placed there, and followed by a parent node listing all other transitions.
    static class WithRelay extends ThreeBitBPP
    {
        private final OnDiskWriteNodeType parent;

        WithRelay(int bytesPerPointer, OnDiskWriteNodeType parent)
        {
            super(0b11111000, bytesPerPointer);
            this.parent = parent;
        }

        @Override
        int sizeChildren(int count)
        {
            return 1 + bpp() + parent.sizeChildren(count);
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException
        {
            int bpp = bpp();
            OnDiskBaseTrieWriter.writeReversedSized(out, basePos - positions[count - 1], bpp);
            out.writeByte(bits);
            parent.writeChildren(out, positions, transitions, count, basePos + bpp + 1);
        }

        @Override
        long sizeValidFor(long furthestDelta, long firstDelta, boolean firstIsWritten)
        {
            long parentValidFor = parent.sizeValidFor(furthestDelta + 1 + bpp());
            if (!firstIsWritten)
                return parentValidFor;

            long relayValidFor = OnDiskNodeWriterPageAware.positionsWithSameBytes(firstDelta);
            return Math.min(relayValidFor, parentValidFor);
        }
    }

    static final int DENSES = 8;
    static final OnDiskWriteNodeType[] DENSE;
    static final int BITMAPS = 8;
    static final OnDiskWriteNodeType[] BITMAP;
    static final int SPARSES = 4;
    static final OnDiskWriteNodeType[] SPARSE;
    static final int CHAIN_BITS;
    static final OnDiskWriteNodeType[] CHAIN;
    static final OnDiskWriteNodeType[] FOLDED_CHAIN;

    static
    {
        int relays = 9;
        DENSE = new OnDiskWriteNodeType[DENSES];
        CHAIN = new OnDiskWriteNodeType[relays];
        FOLDED_CHAIN = new OnDiskWriteNodeType[relays];
        SPARSE = new OnDiskWriteNodeType[relays * SPARSES];
        BITMAP = new OnDiskWriteNodeType[relays * BITMAPS];

        CHAIN[0] = new Chain();
        CHAIN_BITS = CHAIN[0].bits;
        FOLDED_CHAIN[0] = new FoldedChain();
        for (int i = 0; i < SPARSES; ++i)
            SPARSE[i] = new Sparse(i + 1);
        for (int i = 0; i < DENSES; ++i)
            DENSE[i] = new Dense(i + 1);
        for (int i = 0; i < BITMAPS; ++i)
            BITMAP[i] = new Bitmap(i + 1);
        for (int j = 1; j < relays; ++j)
        {
            CHAIN[j] = new WithRelay(j, CHAIN[0]);
            FOLDED_CHAIN[j] = new WithRelay(j, FOLDED_CHAIN[0]);
            for (int i = 0; i < SPARSES; ++i)
                SPARSE[j * SPARSES + i] = new WithRelay(j, SPARSE[i]);
            for (int i = 0; i < BITMAPS; ++i)
                BITMAP[j * BITMAPS + i] = new WithRelay(j, BITMAP[i]);
        }
    }

    final int bits;

    static final int MAX_LEAF_LENGTH_INCLUSIVE = 63;
    static final int MAX_CHAIN_LENGTH_INCLUSIVE = 64;
    // sparse: 0x80 - 0xDF (96 positions) 1lllllbb, lllll is count - 2 and must be < 24, bb is bytes per pointer
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

    /// The size of the payload node [#writePayload] writes for content of the given serialized sizes, where -1 stands
    /// for no content on that side, or 0 if there is nothing to write.
    ///
    /// If the node has an alternate branch, the size does not include the bytes of the pointer to it, whose number
    /// depends on where the branch and the node are placed; the caller must add [#sizeAlternateBranchPointer].
    static int sizePayload(int descentDataSize, int ascentDataSize, boolean hasAlternateBranch, boolean hasChild)
    {
        assert ascentDataSize < 0 || !hasAlternateBranch;
        boolean hasAscentSlot = ascentDataSize >= 0 || hasAlternateBranch;
        if (descentDataSize < 0 && !hasAscentSlot)
            return 0;

        int size = 1;   // the node code
        if (ascentDataSize >= 0)
            size += ascentDataSize + VIntCoding.computeUnsignedVIntSize(ascentDataSize);
        else if (hasAlternateBranch)
            size += 1;  // the length of the pointer, which is at most 8
        if (descentDataSize >= 0)
        {
            size += descentDataSize;
            if (hasChild || hasAscentSlot || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
                size += VIntCoding.computeUnsignedVIntSize(descentDataSize);
        }
        return size;
    }

    /// The number of bytes [#writePayload] uses for the pointer to an alternate branch at the given distance from the
    /// start of the payload. A branch that ends where the payload starts needs none.
    static int sizeAlternateBranchPointer(long delta)
    {
        return delta == 0 ? 0 : OnDiskBaseTrieWriter.bytesFor(delta);
    }

    /// Write the payload of a node: its descent-side content and what goes in its ascent-side slot, which is either
    /// ascent-side content or the pointer to the node's alternate branch.
    ///
    /// The latter is given as `alternateBranch`, the position of the root of the branch, or 0 if there is none. It is
    /// stored like every other pointer in the format, as a delta subtracted from the position where the slot starts
    /// (which is where the payload starts, as the ascent-side slot comes first), in the smallest number of bytes that
    /// can hold it. A branch that ends just where the slot starts has the delta 0, which takes no bytes and leaves
    /// the slot empty. This can only be the case for a node without children: the children of a node (or a relay to
    /// them) lie between its branch and the node.
    static <T> void writePayload(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer, T descentData, T ascentData, long alternateBranch, boolean hasChild) throws IOException
    {
        assert ascentData == null || alternateBranch <= 0;
        boolean hasAscentSlot = ascentData != null || alternateBranch > 0;
        if (descentData == null && !hasAscentSlot)
            return;

        // A node is read backwards from its code byte, and the reader takes the content block
        // adjacent to the code as the descent side (see OnDiskReadNodeType.PREFIX and the layout in
        // OnDiskTrie.md), so the ascent block has to be emitted first.
        int code = PREFIX_BITS;
        if (ascentData != null)
        {
            long ascentStart = out.position();
            serializer.serialize(out, ascentData);
            OnDiskBaseTrieWriter.writeReversedVint(out, out.position() - ascentStart);
            code |= PREFIX_HAS_ASCENT_CONTENT;
        }
        else if (alternateBranch > 0)
        {
            long delta = out.position() - alternateBranch;
            assert delta >= 0 : "Alternate branch root " + alternateBranch + " is after its node at " + out.position();
            assert delta > 0 || !hasChild : "Alternate branch root " + alternateBranch + " is where the children of its node should be";
            int pointerBytes = sizeAlternateBranchPointer(delta);
            if (pointerBytes > 0)
                OnDiskBaseTrieWriter.writeReversedSized(out, delta, pointerBytes);
            OnDiskBaseTrieWriter.writeReversedVint(out, pointerBytes);
            code |= PREFIX_HAS_ASCENT_CONTENT;
        }

        long descentDataSize = -1;
        if (descentData != null)
        {
            long descentStart = out.position();
            serializer.serialize(out, descentData);
            descentDataSize = out.position() - descentStart;
        }

        if (hasChild || hasAscentSlot || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
        {
            if (descentDataSize >= 0)
            {
                OnDiskBaseTrieWriter.writeReversedVint(out, descentDataSize);
                code |= PREFIX_HAS_DESCENT_CONTENT;
            }
            if (hasChild)
                code |= PREFIX_HAS_CHILD;

            out.writeByte(code);
        }
        else
            out.writeByte(LEAF_BITS | (int) descentDataSize);
    }

    /// Choose the node type for the given number of children, and the given maximum distance to a child, when
    /// the first child (i.e. the last entry in the children array) immediately precedes it. This will never return
    /// a type that includes a relay.
    ///
    /// This is the same as `selectChildrenType(furthestDelta, 0, pointerCount, false)`.
    static OnDiskWriteNodeType selectChildrenType(long furthestDelta, int pointerCount)
    {
        if (pointerCount == 1)
            return CHAIN[0];

        int denseBytesPerPointer = OnDiskBaseTrieWriter.bytesFor(furthestDelta + 1);
        int bytesPerPointer = OnDiskBaseTrieWriter.bytesFor(furthestDelta);

        if (pointerCount <= MAX_SPARSE_LENGTH_INCLUSIVE && bytesPerPointer <= MAX_SPARSE_BYTES)
            return SPARSE[bytesPerPointer - 1];
        else if (32 + (pointerCount - 1) * bytesPerPointer < 256 * denseBytesPerPointer)
            return BITMAP[bytesPerPointer - 1];
        else
            return DENSE[denseBytesPerPointer - 1];
    }

    /// Choose the node type or node combination to be used for the given number of children when the first child
    /// may not immediately precede it (i.e. for page-aware layouts).
    /// `furthestDelta` is the distance to the furthest of the children other than the first (not used for a single
    /// child). `firstDelta` specifies the distance to the first child, which is 0 when the child immediately precedes
    /// the node, as it always does when nodes are written at once; if this distance is not zero (i.e. in page-aware
    /// layouts), the returned type will either be Dense or a combination of a relay plus one of the other types.
    /// If `folded` is true (which can only be specified if the node has only one child), an empty node will be selected
    /// (used to fold the transition into the child's leading chain instead of using a separate node).
    static OnDiskWriteNodeType selectChildrenType(long furthestDelta, long firstDelta, int pointerCount, boolean folded)
    {
        int relayBytesPerPointer = 0;
        if (firstDelta > 0)
            relayBytesPerPointer = OnDiskBaseTrieWriter.bytesFor(firstDelta);

        if (pointerCount == 1)
            return folded ? FOLDED_CHAIN[relayBytesPerPointer] : CHAIN[relayBytesPerPointer];
        assert !folded : "Folding can only work with single child";

        int relaySize = relayBytesPerPointer == 0 ? 0 : relayBytesPerPointer + 1;
        int denseBytesPerPointer = OnDiskBaseTrieWriter.bytesFor(Math.max(firstDelta, furthestDelta) + 1);
        int bytesPerPointer = OnDiskBaseTrieWriter.bytesFor(furthestDelta + relaySize);

        if (pointerCount <= MAX_SPARSE_LENGTH_INCLUSIVE && bytesPerPointer <= MAX_SPARSE_BYTES)
            return SPARSE[relayBytesPerPointer * SPARSES + bytesPerPointer - 1];
        else if (relaySize + 32 + (pointerCount - 1) * bytesPerPointer < 256 * denseBytesPerPointer)
            return BITMAP[relayBytesPerPointer * BITMAPS + bytesPerPointer - 1];
        else
            return DENSE[denseBytesPerPointer - 1];
    }

    /// The size of the node [#writeChildren] writes for the given number of children.
    abstract int sizeChildren(int count);

    /// The number of further bytes for which this node type is valid, assuming first child (or relay) is going to be
    /// written with it.
    long sizeValidFor(long furthestDelta)
    {
        throw new AssertionError(); // either overridden (Chain, Sparse, Bitmap) or not called (WithRelay, Dense)
    }

    /// The number of further bytes for which this node type is valid.
    long sizeValidFor(long furthestDelta, long firstDelta, boolean firstIsWritten)
    {
        // Base implementation for non-relays. The selection is only valid when first stays next to the node.
        if (!firstIsWritten)
            return sizeValidFor(furthestDelta);
        assert firstDelta == 0;
        return 1;
    }

    /// Write a node of this type listing the given children. `basePos` is the position the node is written at, from
    /// which all pointers are taken.
    abstract void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos) throws IOException;


    /// Write the explicit pointers of a sparse or bitmap node: every child but the last in the arrays, whose delta is 0
    /// and is left implicit.
    static void writePointers(DataOutputPlus out, long[] positions, int count, long basePos, int bytesPerPointer) throws IOException
    {
        for (int i = 0; i < count - 1; ++i)
        {
            long delta = basePos - positions[i];
            assert delta >= 0 && OnDiskBaseTrieWriter.bytesFor(delta) <= bytesPerPointer;
            OnDiskBaseTrieWriter.writeReversedSized(out, delta, bytesPerPointer);
        }
    }

    /// The size of the chain nodes [#writeChain] writes for the given number of transitions.
    static int sizeChain(int length)
    {
        return length + (length + MAX_CHAIN_LENGTH_INCLUSIVE - 1) / MAX_CHAIN_LENGTH_INCLUSIVE;
    }

    /// Write the transitions `bytes[start, end)` as chain nodes, deepest transition first, in pieces of at most
    /// [#MAX_CHAIN_LENGTH_INCLUSIVE] with the deepest piece the full one. The node the chain leads to must end at the
    /// current position.
    static void writeChain(DataOutputPlus out, byte[] bytes, int start, int end) throws IOException
    {
        int pos = end;
        while (pos > start)
        {
            int written = 0;
            while (pos > start && written < MAX_CHAIN_LENGTH_INCLUSIVE)
            {
                out.writeByte(bytes[--pos]);
                ++written;
            }
            out.writeByte(CHAIN_BITS | (written - 1));
        }
    }

    /// Write the transitions `bytes[start, end) + firstByte`` as chain nodes, deepest transition first, in pieces of
    /// at most [#MAX_CHAIN_LENGTH_INCLUSIVE] with the deepest piece the full one. The node the chain leads to must end
    /// at the current position.
    static void writeChainFolded(DataOutputPlus out, byte[] bytes, int start, int end, int firstByte) throws IOException
    {
        int pos = end;
        int written = 0;
        while (pos > start)
        {
            out.writeByte(bytes[--pos]);
            if (++written == MAX_CHAIN_LENGTH_INCLUSIVE)
            {
                out.writeByte(CHAIN_BITS | (written - 1));
                written = 0;
            }
        }
        out.writeByte(firstByte);
        written++;
        out.writeByte(CHAIN_BITS | (written - 1));
    }

    /// Returns true if the chain in the given array can take another byte without needing a new chain node.
    static boolean isExpandableChain(byte[] bytes)
    {
        return bytes != null && Integer.remainderUnsigned(bytes.length, MAX_CHAIN_LENGTH_INCLUSIVE) > 0;
    }
}
