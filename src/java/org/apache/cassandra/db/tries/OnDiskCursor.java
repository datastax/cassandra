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
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.vint.VIntCoding;

public class OnDiskCursor<T> implements Cursor<T>
{
    public OnDiskCursor(DataDeserializer<T> deserializer,
                        Rebufferer rebufferer,
                        ByteComparable.Version byteComparableVersion,
                        Direction direction,
                        boolean isOrdered,
                        long root)
    {
        this.rebufferer = rebufferer;
        this.currentBH = Rebufferer.EMPTY;
        this.currentBuffer = currentBH.buffer();
        this.currentBufferOffset = 0;
        this.rdr = new SharedStream(deserializer);
        this.byteComparableVersion = byteComparableVersion;
        this.swapContentSides = direction.select(false, isOrdered);
        this.exhausted = Cursor.exhaustedPosition(direction);
        descendInto(Cursor.rootPosition(direction), root);
    }

    class SharedStream extends RebufferingInputStream
    {
        final DataDeserializer<T> deserializer;

        protected SharedStream(DataDeserializer<T> deserializer)
        {
            super(currentBuffer);
            this.deserializer = deserializer;
        }

        @Override
        protected void reBuffer() throws IOException
        {
            seekTo(currentBufferOffset + buffer.position());
        }

        private void seekTo(long pos)
        {
            OnDiskCursor.this.seekTo(pos + 1);
            buffer = currentBuffer;
            buffer.position((int) (pos - currentBufferOffset));
        }

        public T deserialize(SharedStream rdr, long startPos, int len)
        {
            seekTo(startPos);
            try
            {
                return deserializer.deserialize(rdr, len);
            }
            catch (IOException e)
            {
                // Rebufferer wraps exception already, this cannot be raised.
                throw new AssertionError(e);
            }
        }
    }

    interface DataDeserializer<T>
    {
        T deserialize(DataInputPlus rdr, int length) throws IOException;
    }

    final SharedStream rdr;
    final Rebufferer rebufferer;
    final ByteComparable.Version byteComparableVersion;
    final boolean swapContentSides;


    Rebufferer.BufferHolder currentBH;
    ByteBuffer currentBuffer;
    long currentBufferOffset;

    long[] stack = new long[3 * 16];
    int stackLength;

    final long exhausted;

    T content = null;
    Node currentImpl;
    long currentEncodedPosition;
    long postCodePos;
    long nodeImplData;
    int nodeCode;

    long currentFullNode;   // for tails

    long descendInto(long encodedPosition, long postCodePos, int nodeCode)
    {
        this.content = null;
        this.currentEncodedPosition = encodedPosition;
        this.postCodePos = postCodePos;
        this.nodeCode = nodeCode;
        this.currentImpl = selectNodeImpl(nodeCode);
        this.currentImpl.load(this); // may modify currentEncodedPosition
        return currentEncodedPosition;
    }

    long descendInto(long encodedPosition, long nodePos)
    {
        currentFullNode = nodePos;
        return descendInto(encodedPosition, nodePos - 1, readByteBefore(nodePos));
    }

    void descendPostPrefixOrRelay(long nodePos)
    {
        // leave content and encodedPosition unchanged
        this.postCodePos = nodePos - 1;
        this.nodeCode = readByteBefore(nodePos);
        this.currentImpl = selectNodeImpl(nodeCode);
        this.currentImpl.load(this); // may modify currentEncodedPosition
    }

    public T content()
    {
        return content;
    }

    @Override
    public long encodedPosition()
    {
        return currentEncodedPosition;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public long advance()
    {
        return returnOrAscend(currentImpl.advance(this));
    }

    private long returnOrAscend(long descend)
    {
        if (descend != exhausted)
            return descend;
        else
            return ascend();
    }

    private long ascend()
    {
        if (stackLength <= 0)
            return exhausted;

        --stackLength;
        currentImpl = loadStackImpl(stackLength);
        return currentImpl.advance(this);
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (Cursor.ascended(encodedSkipPosition, currentEncodedPosition))
        {
            int skipDepth = Cursor.depth(encodedSkipPosition);
            while (--stackLength >= 0 && getStackDepth(stackLength) >= skipDepth)
            {
                // pos decreased in while
            }
            if (stackLength < 0)
                return currentEncodedPosition = Cursor.exhaustedPosition(currentEncodedPosition);

            currentImpl = loadStackImpl(stackLength);

            // if we ascended above the target depth, we only need to advance
            if (Cursor.depth(currentEncodedPosition) < skipDepth - 1)
                return currentImpl.advance(this);
        }

        return returnOrAscend(currentImpl.skipTo(this, encodedSkipPosition));
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        return returnOrAscend(currentImpl.advanceMultiple(this, receiver));
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        return null;
    }

    private long getContentAtPos(long currentPos)
    {
        int vintlen = readVIntLength(currentPos);
        int len = (int) readVInt(currentPos, vintlen);
        currentPos -= vintlen + len;
        content = rdr.deserialize(rdr, currentPos, len);
        return currentPos;
    }

    private void getContentAtPosWithLength(long currentPos, int len)
    {
        content = rdr.deserialize(rdr, currentPos - len, len);
    }


    int getStackDepth(int stackIndex)
    {
        return (int) stack[stackIndex * 3 + 0];
    }
    int getStackNodeCode(int stackIndex)
    {
        return (int) (stack[stackIndex * 3 + 0] >> 32);
    }
    long getStackPostCodePos(int stackIndex)
    {
        return stack[stackIndex * 3 + 1];
    }
    long getStackNodeImplData(int stackIndex)
    {
        return stack[stackIndex * 3 + 2];
    }

    private void addBacktrack(long nodeBasePos, int nodeCode, long nodeImplData)
    {
        // store depth & nodeCode in one long, nodeBasePos, nodeImplData
        if (stack.length <= stackLength * 3)
            stack = Arrays.copyOf(stack, stack.length * 2);
        stack[stackLength * 3 + 0] = Cursor.depth(currentEncodedPosition) | (((long) nodeCode) << 32);
        stack[stackLength * 3 + 1] = nodeBasePos;
        stack[stackLength * 3 + 2] = nodeImplData;
        ++stackLength;
    }

    Node loadStackImpl(int stackIndex)
    {
        nodeCode = getStackNodeCode(stackIndex);
        nodeImplData = getStackNodeImplData(stackIndex);
        postCodePos = getStackPostCodePos(stackIndex);
        currentEncodedPosition = Cursor.encode(getStackDepth(stackIndex), 0, Cursor.direction(currentEncodedPosition));
        return selectNodeImpl(nodeCode);
    }

    static final Node LEAF = new Leaf();
    static final Node CHAIN = new Chain();
    static final Node SPARSE = new Sparse();
    static final Node DENSE = new Dense();
    static final Node BITMAP = new Bitmap();
    static final Node PREFIX = new Prefix();
    static final Node RELAY = new Relay();

    static final Node[] IMPLEMENTATIONS = new Node[]
                                          {
                                          LEAF, LEAF, LEAF, LEAF, LEAF, LEAF, LEAF, LEAF,
                                          CHAIN, CHAIN, CHAIN, CHAIN, CHAIN, CHAIN, CHAIN, CHAIN,
                                          SPARSE, SPARSE, SPARSE, SPARSE, SPARSE, SPARSE, SPARSE, SPARSE,
                                          SPARSE, SPARSE, SPARSE, SPARSE, BITMAP, DENSE, PREFIX, RELAY
                                          };

    Node selectNodeImpl(int nodeCode)
    {
        return IMPLEMENTATIONS[nodeCode >> 3];
    }

    private int readVIntLength(long currentPos)
    {
        seekTo(currentPos);
        return VIntCoding.computeUnsignedVIntSize(currentBuffer, (int) (currentPos - 1 - currentBufferOffset));
    }

    private long readVInt(long pos, int vintLength)
    {
        long withMask = readSizedInt(pos, vintLength);
        return withMask & ((1 << vintLength * 7) - 1);
    }

    private long readSizedInt(long base, int index, int bytes)
    {
        return readSizedInt(base + (index + 1) * bytes, bytes);
    }

    private long readSizedIntImplicit0(long base, int index, int bytes)
    {
        return index > 0 ? readSizedInt(base + index * bytes, bytes) : 0;
    }

    private long readSizedInt(long pos, int bytes)
    {
        seekTo(pos);
        if (pos - currentBufferOffset >= 8)
        {
            long l = currentBuffer.getLong((int) (pos - 8 -  currentBufferOffset));
            return (Long.reverseBytes(l) >> (64 - bytes * 8)) & ((1 << bytes * 8) - 1);
        }
        else
        {
            long l = 0;
            while (bytes-- > 0)
                l = (l << 8) | readByteBefore(pos--);
            return l;
        }
    }

    String dumpNode()
    {
        return currentImpl.dump(this);
    }

    String dumpNode(long node)
    {
        return new OnDiskCursor<>(rdr.deserializer, rebufferer, byteComparableVersion, direction(), swapContentSides, node).dumpNode();
    }

    interface Node
    {
        void load(OnDiskCursor<?> state);
        long advance(OnDiskCursor<?> state);
        default long advanceMultiple(OnDiskCursor<?> state, TransitionsReceiver receiver)
        {
            // only implemented by chain
            return advance(state);
        }
        long skipTo(OnDiskCursor<?> state, long encodedSkipPosition);
        String dump(OnDiskCursor<?> state);
    }

    static class Leaf implements Node
    {
        private static final int LEAF_LENGTH_MASK = 63;

        public void load(OnDiskCursor<?> state)
        {
            if (state.swapContentSides)
            {
                if (Cursor.isRootPosition(state.currentEncodedPosition))
                {
                    // Root content needs to be presented on the way back.
                    // Note: This takes advantage of the fact that leaf codes are 00llllll, which reads correctly as a
                    // varint length.
                    state.addBacktrack(state.postCodePos + 1, ASCENT_LEAF_CODE, state.currentEncodedPosition | ON_RETURN_PATH_BIT);
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
    }

    // prefix with no content and no child is used for backtrack entry to return ascent-side content
    static final int ASCENT_LEAF_CODE = OnDiskNodeType.PREFIX.bits;

    static class Relay implements Node
    {
        private static int bytes(int nodeCode)
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
    }

    static class Prefix implements Node
    {
        public void load(OnDiskCursor<?> state)
        {
            long currentPos = state.postCodePos;
            int nodeCode = state.nodeCode;
            boolean hasAscent = (nodeCode & OnDiskNodeType.PREFIX_HAS_ASCENT_CONTENT) != 0;
            boolean hasDescent = (nodeCode & OnDiskNodeType.PREFIX_HAS_DESCENT_CONTENT) != 0;
            boolean hasChild = (nodeCode & OnDiskNodeType.PREFIX_HAS_CHILD) != 0;
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
                        state.addBacktrack(currentPos, ASCENT_LEAF_CODE, state.currentEncodedPosition | ON_RETURN_PATH_BIT);
                        return;
                    }

                    state.currentEncodedPosition |= ON_RETURN_PATH_BIT;
                }

                state.getContentAtPos(currentPos);
                return;
            }

            if (!swap)
            {
                if (hasDescent)
                {
                    currentPos = state.getContentAtPos(currentPos);
                }
                if (hasAscent)
                {
                    // Make up a node on the return path; its advance or skipTo will be called
                    state.addBacktrack(currentPos, ASCENT_LEAF_CODE, state.currentEncodedPosition | ON_RETURN_PATH_BIT);

                    if (hasChild)
                    {
                        int vintlen = state.readVIntLength(currentPos);
                        int len = (int) state.readVInt(currentPos, vintlen);
                        currentPos -= vintlen + len;
                    }
                }
            }
            else
            {
                if (hasDescent)
                {
                    // Make up a node on the return path; its advance or skipTo will be called
                    state.addBacktrack(currentPos, ASCENT_LEAF_CODE, state.currentEncodedPosition | ON_RETURN_PATH_BIT);

                    if (hasChild || hasAscent)
                    {
                        int vintlen = state.readVIntLength(currentPos);
                        int len = (int) state.readVInt(currentPos, vintlen);
                        currentPos -= vintlen + len;
                    }
                }
                if (hasAscent)
                {
                    currentPos = state.getContentAtPos(currentPos);
                }
            }

            if (hasChild)
                state.descendPostPrefixOrRelay(currentPos);
            else
                state.currentImpl = LEAF; // no children
        }

        // Node: Since we always change the impl, advance and skipTo are only called if this is on the backtrack path
        // to present ascent-side content.

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
        public String dump(OnDiskCursor state)
        {
            int nodeCode = state.nodeCode;
            boolean hasAscent = (nodeCode & OnDiskNodeType.PREFIX_HAS_ASCENT_CONTENT) != 0;
            boolean hasDescent = (nodeCode & OnDiskNodeType.PREFIX_HAS_DESCENT_CONTENT) != 0;
            boolean hasChild = (nodeCode & OnDiskNodeType.PREFIX_HAS_CHILD) != 0;
            Object saved = state.content;
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
            state.content = saved;
            String children = hasChild ? " --> " + pos : "";
            return "Prefix: " + descentContent + ascentContent + children;
        }
    }

    static class Chain implements Node
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
    }

    static class Sparse implements Node
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

        private static long encodedPositionForChild(OnDiskCursor<?> state, int index, int length)
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
    }

    static class Bitmap implements Node
    {
        private static int transition(OnDiskCursor<?> state)
        {
            return (int) state.nodeImplData & 0xFF;
        }

        private static int childIndex(OnDiskCursor<?> state)
        {
            return (int) (state.nodeImplData >> 8) & 0xFF;
        }

        private static int length(OnDiskCursor<?> state)
        {
            return (int) (state.nodeImplData >> 16) & 0xFF;
        }

        private static int bytes(int nodeCode)
        {
            return (nodeCode & 0b111) + 1;
        }

        private static long encode(int transition, int childIndex, int length)
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

        private static long descendToChild(OnDiskCursor<?> state, Direction direction, int currTransition, int currIndex)
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

        private static int cardinality(OnDiskCursor<?> state)
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
                for (int i = 0; i <= 31; --i)
                    bits += Integer.bitCount(state.readByteBefore(postCodePos - i));
            }
            return bits;
        }

        private static int cardinality(OnDiskCursor<?> state, long postCodePos, int upToIndex)
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
                for (i = 0; i < upToIndex / 8; --i)
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

        private static int findNextEntry(OnDiskCursor<?> state, long postCodePos, Direction direction, int start)
        {
            if (direction.isForward())
            {
                // TODO: add long version
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
            // TODO
            return "Bitmap";
        }
    }

    static class Dense implements Node
    {
        private static int bytes(int nodeCode)
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

        private static long descendToChild(OnDiskCursor<?> state, Direction direction, int transition)
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

        private static int findNext(OnDiskCursor<?> state, Direction direction, int index)
        {
            int bytes = bytes(state.nodeCode);
            long base = state.postCodePos - 256 * bytes;
            long notPresent = (1 << bytes * 8) - 1;
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
            // TODO
            return "Dense";
        }
    }

    private int readByteBefore(long filePos)
    {
        seekTo(filePos);
        return currentBuffer.get((int) (filePos - 1 - currentBufferOffset)) & 0xFF;
    }

    // pos is _after_ the byte we want to read
    private void seekTo(long filePos)
    {
        long ofs = filePos - currentBufferOffset;
        if (ofs > 0 && ofs <= currentBuffer.remaining())
            return;
        currentBH.release();
        currentBH = Rebufferer.EMPTY;
        currentBH = rebufferer.rebuffer(filePos - 1);
        currentBuffer = currentBH.buffer();
        currentBufferOffset = currentBH.offset();
    }
}
