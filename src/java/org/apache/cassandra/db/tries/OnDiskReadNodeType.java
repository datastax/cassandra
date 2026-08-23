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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.tries.Cursor.TransitionsReceiver;

/**
 * Enum representing different types of trie nodes for reading from disk.
 * Each enum constant implements the logic for deserializing and traversing its specific node type.
 */
public enum OnDiskReadNodeType
{
    LEAF
    {
        private static final int LEAF_LENGTH_MASK = 63;

        @Override
        public void load(OnDiskCursor<?> state)
        {
            if (state.swapContentSides)
            {
                if (Cursor.isRootPosition(state.currentEncodedPosition))
                {
                    // Root content needs to be presented on the way back.
                    // Note: This takes advantage of the fact that leaf codes are 00llllll, which reads correctly as a
                    // varint length.
                    state.addBacktrack(state.postCodePos + 1, ASCENT_LEAF_CODE, state.currentEncodedPosition | Cursor.ON_RETURN_PATH_BIT);
                    return;
                }
                state.currentEncodedPosition |= Cursor.ON_RETURN_PATH_BIT;
            }
            int length = state.nodeCode & LEAF_LENGTH_MASK;
            state.getContentAtPosWithLength(state.postCodePos, length);
        }

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            return state.exhausted;
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            return state.exhausted;
        }

        @Override
        public String dump(OnDiskCursor<?> state)
        {
            return "Leaf: " + state.content;
        }
    },

    CHAIN
    {
        private static final int CHAIN_LENGTH_MASK = 63;

        private int length(int nodeCode)
        {
            return (nodeCode & CHAIN_LENGTH_MASK) + 1;
        }

        @Override
        public void load(OnDiskCursor<?> state)
        {
        }

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            int nextByte = state.readByteBefore(state.postCodePos);
            long nextPos = Cursor.positionForDescentWithByte(state.currentEncodedPosition, nextByte);
            if (length(state.nodeCode) > 1)
                return state.descendInto(nextPos, state.postCodePos - 1, state.nodeCode - 1);
            else
                return state.descendInto(nextPos, state.postCodePos - 1);
        }

        @Override
        public long advanceMultiple(OnDiskCursor<?> state, TransitionsReceiver receiver)
        {
            int length = length(state.nodeCode);
            long postCodePos = state.postCodePos;
            int skippedBytes = length - 1;
            long encodedPosition = state.currentEncodedPosition;
            if (skippedBytes > 0)
            {
                if (receiver != null)
                {
                    while (length > 1)
                    {
                        int nextByte = state.readByteBefore(postCodePos);
                        receiver.addPathByte(nextByte);
                        --postCodePos;
                        --length;
                    }
                }
                else
                    postCodePos -= skippedBytes;

                encodedPosition += Cursor.DEPTH_ADJUSTMENT_ONE * skippedBytes;
            }
            int nextByte = state.readByteBefore(postCodePos);
            long nextPos = Cursor.positionForDescentWithByte(encodedPosition, nextByte);

            return state.descendInto(nextPos, postCodePos - 1);
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            int nextByte = state.readByteBefore(state.postCodePos);
            long nextPos = Cursor.positionForDescentWithByte(state.currentEncodedPosition, nextByte);
            if (Cursor.compare(nextPos, encodedSkipPosition) < 0)
                return state.exhausted;

            if (length(state.nodeCode) > 1)
                return state.descendInto(nextPos, state.postCodePos - 1, state.nodeCode - 1);
            else
                return state.descendInto(nextPos, state.postCodePos - 1);
        }

        @Override
        public String dump(OnDiskCursor<?> state)
        {
            String s = "Chain: ";
            int length = length(state.nodeCode);
            for (int i = 0; i < length; ++i)
                s += String.format("%02x", state.readByteBefore(state.postCodePos - i));
            return s + " --> " + (state.postCodePos - length);
        }
    },

    SPARSE
    {
        private int length(int nodeCode)
        {
            return ((nodeCode >> 2) & 0b11111) + 2;
        }

        private int bytes(int nodeCode)
        {
            return (nodeCode & 0b11) + 1;
        }

        private int index(long implData)
        {
            return (int) implData;
        }

        private long base(long postCodePos, int nodeCode)
        {
            int l = length(nodeCode);
            int b = bytes(nodeCode);
            return postCodePos - (l + (l - 1) * b);
        }

        @Override
        public void load(OnDiskCursor<?> state)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            state.nodeImplData = direction.isForward() ? 0 : length(state.nodeCode) - 1;
        }

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            int currIndex = index(state.nodeImplData);
            int length = length(state.nodeCode);
            long nextPosition = encodedPositionForChild(state, currIndex, length);

            return descendToChild(state, currIndex, direction, length, nextPosition);
        }

        private long descendToChild(OnDiskCursor<?> state, int currIndex, Direction direction, int length, long nextPosition)
        {
            int nextIndex = currIndex + direction.increase;
            long postCodePos = state.postCodePos;
            int nodeCode = state.nodeCode;
            if (direction.inLoop(nextIndex, 0, length - 1))
                state.addBacktrack(postCodePos, nodeCode, nextIndex);
            int bytes = bytes(nodeCode);
            long base = base(postCodePos, nodeCode);
            long childDelta = state.readSizedIntImplicit0(base, currIndex, bytes);
            return state.descendInto(nextPosition, base - childDelta);
        }

        private long encodedPositionForChild(OnDiskCursor<?> state, int index, int length)
        {
            int nextByte = state.readByteBefore(state.postCodePos - (length - 1 - index));
            return Cursor.positionForDescentWithByte(state.currentEncodedPosition, nextByte);
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            int currIndex = index(state.nodeImplData);
            int length = length(state.nodeCode);
            long nextPosition = encodedPositionForChild(state, currIndex, length);
            while (Cursor.compare(nextPosition, encodedSkipPosition) < 0)
            {
                currIndex += direction.increase;
                if (!direction.inLoop(currIndex, 0, length - 1))
                    return state.exhausted;
                nextPosition = encodedPositionForChild(state, currIndex, length);
            }

            return descendToChild(state, currIndex, direction, length, nextPosition);
        }

        @Override
        public String dump(OnDiskCursor<?> state)
        {
            int length = length(state.nodeCode);
            long base = base(state.postCodePos, state.nodeCode);
            int bytes = bytes(state.nodeCode);
            String s = String.format("Sparse%d", bytes);
            for (int i = 0; i < length; ++i)
                s += String.format("\n%02x --> %d", state.readByteBefore(state.postCodePos - length + i),
                                   base - state.readSizedIntImplicit0(base, i, bytes));
            return s;
        }
    },

    DENSE
    {
        private int bytes(int nodeCode)
        {
            return (nodeCode & 0b111) + 1;
        }

        @Override
        public void load(OnDiskCursor<?> state)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            state.nodeImplData = findNext(state, direction, direction.select(0, 255));
        }

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            int transition = (int) state.nodeImplData;
            return descendToChild(state, direction, transition);
        }

        private long descendToChild(OnDiskCursor<?> state, Direction direction, int transition)
        {
            int next = findNext(state, direction, transition + direction.increase);
            if (direction.inLoop(next, 0, 255))
                state.addBacktrack(state.postCodePos, state.nodeCode, next);
            long nextPosition = Cursor.positionForDescentWithByte(state.currentEncodedPosition, transition);
            int bytes = bytes(state.nodeCode);
            long base = state.postCodePos - 256 * bytes;
            long childDelta = state.readSizedInt(base, transition, bytes);
            return state.descendInto(nextPosition, base - childDelta);
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            int transition = Cursor.incomingTransition(encodedSkipPosition);
            if (direction.le(transition, (int) state.nodeImplData))
                return advance(state);
            int currTransition = findNext(state, direction, transition);
            if (!direction.inLoop(currTransition, 0, 255))
                return state.exhausted;
            return descendToChild(state, direction, currTransition);
        }

        private int findNext(OnDiskCursor<?> state, Direction direction, int index)
        {
            int bytes = bytes(state.nodeCode);
            long base = state.postCodePos - 256 * bytes;
            long notPresent = (1L << bytes * 8) - 1;
            do
            {
                long child = state.readSizedInt(base, index, bytes);
                if (child != notPresent)
                    break;

                index += direction.increase;
            }
            while (direction.inLoop(index, 0, 255));

            return index;
        }

        @Override
        public String dump(OnDiskCursor<?> state)
        {
            int bytes = bytes(state.nodeCode);
            long base = state.postCodePos - 256 * bytes;
            long notPresent = (1L << bytes * 8) - 1;
            StringBuilder s = new StringBuilder(String.format("Dense%d", bytes));
            for (int i = 0; i < 256; ++i)
            {
                long child = state.readSizedInt(base, i, bytes);
                if (child != notPresent)
                    s.append(String.format("\n%02x --> %d", i, base - child));
            }
            return s.toString();
        }
    },

    BITMAP
    {
        private int transition(OnDiskCursor<?> state)
        {
            return (int) state.nodeImplData & 0xFF;
        }

        private int childIndex(OnDiskCursor<?> state)
        {
            return (int) (state.nodeImplData >> 8) & 0xFF;
        }

        private int length(OnDiskCursor<?> state)
        {
            return (int) (state.nodeImplData >> 16) & 0xFF;
        }

        private int bytes(int nodeCode)
        {
            return (nodeCode & 0b111) + 1;
        }

        private long encode(int transition, int childIndex, int length)
        {
            return transition | (childIndex << 8) | (length << 16);
        }

        @Override
        public void load(OnDiskCursor<?> state)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            int length = cardinality(state);
            state.nodeImplData = encode(findNextEntry(state,
                                                      state.postCodePos,
                                                      direction,
                                                      direction.select(0, 255)),
                                        direction.select(0, length - 1),
                                        length);
        }

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            return descendToChild(state, Cursor.direction(state.currentEncodedPosition), transition(state), childIndex(state));
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            Direction direction = Cursor.direction(state.currentEncodedPosition);
            int transition = Cursor.incomingTransition(encodedSkipPosition);
            if (direction.le(transition, transition(state)))
                return advance(state);

            int currTransition = findNextEntry(state, state.postCodePos, direction, transition);
            if (!direction.inLoop(currTransition, 0, 255))
                return state.exhausted;
            int currIndex = cardinality(state, state.postCodePos, currTransition);
            return descendToChild(state, direction, currTransition, currIndex);
        }

        private long descendToChild(OnDiskCursor<?> state, Direction direction, int currTransition, int currIndex)
        {
            long postCodePos = state.postCodePos;
            int nextTransition = findNextEntry(state, postCodePos, direction, currTransition + direction.increase);
            int length = length(state);
            if (direction.inLoop(nextTransition, 0, 255))
                state.addBacktrack(postCodePos, state.nodeCode, encode(nextTransition, currIndex + direction.increase, length));
            int bytes = bytes(state.nodeCode);
            long base = postCodePos - 32 - (length - 1) * bytes;
            long childDelta = state.readSizedIntImplicit0(base, currIndex, bytes);
            return state.descendInto(Cursor.positionForDescentWithByte(state.currentEncodedPosition, currTransition), base - childDelta);
        }

        private int cardinality(OnDiskCursor<?> state)
        {
            long postCodePos = state.postCodePos;
            state.seekTo(postCodePos);
            int bits = 0;
            long currentBufferOffset = state.currentBufferOffset;
            if (postCodePos - currentBufferOffset >= 32)
            {
                ByteBuffer currentBuffer = state.currentBuffer;
                for (int i = 1; i <= 4; ++i)
                {
                    bits += Long.bitCount(currentBuffer.getLong((int) (postCodePos - currentBufferOffset - i * 8)));
                }
            }
            else
            {
                for (int i = 0; i <= 31; ++i)
                    bits += Integer.bitCount(state.readByteBefore(postCodePos - i));
            }
            return bits;
        }

        private int cardinality(OnDiskCursor<?> state, long postCodePos, int upToIndex)
        {
            state.seekTo(postCodePos);
            int bits = 0;
            if (postCodePos - state.currentBufferOffset >= 32)
            {
                int i;
                for (i = 1; i <= upToIndex / 64; ++i)
                    bits += Long.bitCount(state.currentBuffer.getLong((int) (postCodePos - state.currentBufferOffset - i * 8)));
                int remainder = upToIndex % 64;
                if (remainder > 0)
                {
                    long l = state.currentBuffer.getLong((int) (postCodePos - state.currentBufferOffset - i * 8));
                    long mask = (1L << remainder) - 1;
                    bits += Long.bitCount(l & mask);
                }
            }
            else
            {
                int i;
                for (i = 0; i < upToIndex / 8; ++i)
                    bits += Integer.bitCount(state.readByteBefore(postCodePos - i));
                int remainder = upToIndex % 8;
                if (remainder > 0)
                {
                    int l = state.readByteBefore(postCodePos - i);
                    int mask = (1 << remainder) - 1;
                    bits += Integer.bitCount(l & mask);
                }
            }
            return bits;
        }

        private int findNextEntry(OnDiskCursor<?> state, long postCodePos, Direction direction, int start)
        {
            if (direction.isForward())
            {
                int i = start / 8;
                int remainder = start % 8;
                int b = state.readByteBefore(postCodePos - i);
                int mask = ~((1 << remainder) - 1);
                b &= mask;
                while (true)
                {
                    int bits = Integer.numberOfTrailingZeros(b);
                    if (bits < 8)
                        return i * 8 + bits;
                    if (++i == 256 / 8)
                        return 256;
                    b = state.readByteBefore(postCodePos - i);
                }
            }
            else
            {
                int i = start / 8;
                int remainder = start % 8;
                int b = state.readByteBefore(postCodePos - i);
                int mask = (1 << (remainder + 1)) - 1;
                b &= mask;
                while (true)
                {
                    int bits = Integer.numberOfLeadingZeros(b);
                    if (bits < 32)
                        return i * 8 + (31 - bits);
                    if (--i < 0)
                        return -1;
                    b = state.readByteBefore(postCodePos - i);
                }
            }
        }

        @Override
        public String dump(OnDiskCursor<?> state)
        {
            int bytes = bytes(state.nodeCode);
            long postCodePos = state.postCodePos;
            int length = cardinality(state);
            long base = postCodePos - 32 - (length - 1) * bytes;
            StringBuilder s = new StringBuilder(String.format("Bitmap%d", bytes));
            
            // Read the bitmap (32 bytes = 256 bits)
            state.seekTo(postCodePos);
            int childIndex = 0;
            for (int byteIdx = 0; byteIdx < 32; ++byteIdx)
            {
                int bitmapByte = state.readByteBefore(postCodePos - byteIdx);
                for (int bitIdx = 0; bitIdx < 8; ++bitIdx)
                {
                    if ((bitmapByte & (1 << bitIdx)) != 0)
                    {
                        int transition = byteIdx * 8 + bitIdx;
                        long childDelta = state.readSizedIntImplicit0(base, childIndex, bytes);
                        s.append(String.format("\n%02x --> %d", transition, base - childDelta));
                        childIndex++;
                    }
                }
            }
            return s.toString();
        }
    },

    PREFIX
    {
        @Override
        public void load(OnDiskCursor<?> state)
        {
            long currentPos = state.postCodePos;
            int nodeCode = state.nodeCode;
            boolean hasAscent = (nodeCode & OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT) != 0;
            boolean hasDescent = (nodeCode & OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT) != 0;
            boolean hasChild = (nodeCode & OnDiskWriteNodeType.PREFIX_HAS_CHILD) != 0;
            boolean swap = state.swapContentSides;
            assert hasAscent | hasDescent;
            state.nodeImplData = -1;

            // ascent or descent-only prefix is presented immediately, possibly by switching position to return path
            if (!hasChild && (hasDescent != hasAscent))
            {
                state.currentImpl = LEAF; // no children
                if (swap == hasDescent) // swap & descent | !swap & ascent
                {
                    if (Cursor.isRootPosition(state.currentEncodedPosition))
                    {
                        // the root position needs to be presented; add backtrack for the content
                        state.addBacktrack(currentPos, ASCENT_LEAF_CODE, state.currentEncodedPosition | Cursor.ON_RETURN_PATH_BIT);
                        return;
                    }

                    state.currentEncodedPosition |= Cursor.ON_RETURN_PATH_BIT;
                }

                state.getContentAtPos(currentPos);
                return;
            }

            if (!swap)
            {
                if (hasDescent)
                    currentPos = state.getContentAtPos(currentPos);
                if (hasAscent)
                    currentPos = addBacktrackAndMaybeAdvanceOver(state, currentPos, hasChild);
            }
            else
            {
                if (hasDescent)
                    currentPos = addBacktrackAndMaybeAdvanceOver(state, currentPos, hasChild || hasAscent);
                if (hasAscent)
                    currentPos = state.getContentAtPos(currentPos);
            }

            if (hasChild)
                state.descendPostPrefixOrRelay(currentPos);
            else
                state.descendPostPrefixToEmpty(); // no children
        }

        private long addBacktrackAndMaybeAdvanceOver(OnDiskCursor<?> state, long currentPos, boolean shouldAdvanceOverContent)
        {
            // Make up a node on the return path; its advance or skipTo will be called
            state.addBacktrack(currentPos, ASCENT_LEAF_CODE, state.currentEncodedPosition | Cursor.ON_RETURN_PATH_BIT);

            if (shouldAdvanceOverContent)
            {
                int vintlen = state.readVIntLength(currentPos);
                int len = (int) state.readVInt(currentPos, vintlen);
                currentPos -= vintlen + len;
            }
            return currentPos;
        }

        // Because load() always moves on to the post-prefix part, the advance and skipTo methods below are only called
        // by backtracking to present return-path content.

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            state.getContentAtPos(state.postCodePos);
            state.descendPostPrefixToEmpty(); // no further children
            // The return-path position was saved before the content above was read, so re-apply the content flag.
            long position = state.nodeImplData;
            if (state.content != null)
                position |= Cursor.MAY_HAVE_CONTENT_BIT;
            return state.currentEncodedPosition = position;
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            assert Cursor.compare(encodedSkipPosition, state.nodeImplData) <= 0;
            return advance(state);
        }

        @Override
        public String dump(OnDiskCursor state)
        {
            int nodeCode = state.nodeCode;
            boolean hasAscent = (nodeCode & OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT) != 0;
            boolean hasDescent = (nodeCode & OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT) != 0;
            boolean hasChild = (nodeCode & OnDiskWriteNodeType.PREFIX_HAS_CHILD) != 0;
            Object saved = state.content;
            long savedPosition = state.currentEncodedPosition;
            String descentContent = "";
            long pos = state.postCodePos;
            if (hasDescent)
            {
                pos = state.getContentAtPos(pos);
                descentContent = "D[" + state.content + "]";
            }
            String ascentContent = "";
            if (hasAscent)
            {
                pos = state.getContentAtPos(pos);
                ascentContent = "A[" + state.content + "]";
            }
            // getContentAtPos above also sets the content flag on the position; this method must not change state.
            state.content = saved;
            state.currentEncodedPosition = savedPosition;
            String children = hasChild ? " --> " + pos : "";
            return "Prefix: " + descentContent + ascentContent + children;
        }
    },

    RELAY
    {
        private int bytes(int nodeCode)
        {
            return (nodeCode & 0b111) + 1;
        }

        @Override
        public void load(OnDiskCursor<?> state)
        {
            int bytes = bytes(state.nodeCode);
            long base = state.postCodePos - bytes;
            state.descendPostPrefixOrRelay(base - state.readSizedInt(state.postCodePos, bytes));
        }

        @Override
        public long advance(OnDiskCursor<?> state)
        {
            state.getContentAtPos(state.postCodePos);
            state.currentImpl = LEAF; // no further children
            return state.currentEncodedPosition = state.nodeImplData;
        }

        @Override
        public long skipTo(OnDiskCursor<?> state, long encodedSkipPosition)
        {
            assert Cursor.compare(encodedSkipPosition, state.nodeImplData) <= 0;
            return advance(state);
        }

        @Override
        public String dump(OnDiskCursor<?> state)
        {
            return "Relay --> " + state.readSizedInt(state.postCodePos, bytes(state.nodeCode));
        }
    };

    // prefix with no content and no child is used for backtrack entry to return ascent-side content
    static final int ASCENT_LEAF_CODE = OnDiskWriteNodeType.PREFIX.bits;

    /**
     * Abstract methods that each enum constant must implement for reading trie nodes from disk.
     */
    public abstract void load(OnDiskCursor<?> state);
    public abstract long advance(OnDiskCursor<?> state);
    public long advanceMultiple(OnDiskCursor<?> state, TransitionsReceiver receiver)
    {
        // only implemented by chain
        return advance(state);
    }
    public abstract long skipTo(OnDiskCursor<?> state, long encodedSkipPosition);
    public abstract String dump(OnDiskCursor<?> state);

    /**
     * Array mapping node codes to their corresponding OnDiskReadNodeType enum constants.
     * Note: Any changes to OnDiskWriteNodeType.bits must be reflected here.
     */
    static final OnDiskReadNodeType[] IMPLEMENTATIONS = new OnDiskReadNodeType[]
    {
        LEAF, LEAF, LEAF, LEAF, LEAF, LEAF, LEAF, LEAF,
        CHAIN, CHAIN, CHAIN, CHAIN, CHAIN, CHAIN, CHAIN, CHAIN,
        SPARSE, SPARSE, SPARSE, SPARSE, SPARSE, SPARSE, SPARSE, SPARSE,
        SPARSE, SPARSE, SPARSE, SPARSE, BITMAP, DENSE, PREFIX, RELAY
    };

    /**
     * Selects the appropriate OnDiskReadNodeType based on the node code.
     */
    static OnDiskReadNodeType selectNodeImpl(int nodeCode)
    {
        return IMPLEMENTATIONS[nodeCode >> 3];
    }
}
