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
        this.isOrdered = isOrdered;
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
    final boolean isOrdered; // determines swapContentSides above; needed if tailTrie switches direction


    Rebufferer.BufferHolder currentBH;
    ByteBuffer currentBuffer;
    long currentBufferOffset;

    long[] stack = new long[3 * 16];
    int stackLength;

    final long exhausted;

    T content = null;
    OnDiskReadNodeType currentImpl;
    long currentEncodedPosition;
    long postCodePos;
    long nodeImplData;
    int nodeCode;

    long currentFullNode;   // for tails

    long descendInto(long encodedPosition, long nodePos)
    {
        currentFullNode = nodePos;
        return descendInto(encodedPosition, nodePos - 1, readByteBefore(nodePos));
    }

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

    void descendPostPrefixOrRelay(long nodePos)
    {
        // leave content and encodedPosition unchanged
        this.postCodePos = nodePos - 1;
        this.nodeCode = readByteBefore(nodePos);
        this.currentImpl = selectNodeImpl(nodeCode);
        this.currentImpl.load(this); // may modify currentEncodedPosition
    }

    void descendPostPrefixToEmpty()
    {
        this.currentImpl = OnDiskReadNodeType.LEAF;
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
        return new OnDiskCursor<>(rdr.deserializer, rebufferer, byteComparableVersion, direction, isOrdered, currentFullNode);
    }

    long getContentAtPos(long currentPos)
    {
        int vintlen = readVIntLength(currentPos);
        int len = (int) readVInt(currentPos, vintlen);
        currentPos -= vintlen + len;
        content = rdr.deserialize(rdr, currentPos, len);
        return currentPos;
    }

    void getContentAtPosWithLength(long currentPos, int len)
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

    void addBacktrack(long nodeBasePos, int nodeCode, long nodeImplData)
    {
        // store depth & nodeCode in one long, nodeBasePos, nodeImplData
        if (stack.length <= stackLength * 3)
            stack = Arrays.copyOf(stack, stack.length * 2);
        stack[stackLength * 3 + 0] = Cursor.depth(currentEncodedPosition) | (((long) nodeCode) << 32);
        stack[stackLength * 3 + 1] = nodeBasePos;
        stack[stackLength * 3 + 2] = nodeImplData;
        ++stackLength;
    }

    OnDiskReadNodeType loadStackImpl(int stackIndex)
    {
        nodeCode = getStackNodeCode(stackIndex);
        nodeImplData = getStackNodeImplData(stackIndex);
        postCodePos = getStackPostCodePos(stackIndex);
        currentEncodedPosition = Cursor.encode(getStackDepth(stackIndex), 0, Cursor.direction(currentEncodedPosition));
        return OnDiskReadNodeType.selectNodeImpl(nodeCode);
    }

    OnDiskReadNodeType selectNodeImpl(int nodeCode)
    {
        return OnDiskReadNodeType.selectNodeImpl(nodeCode);
    }

    /// Read the first byte of a variable-length-encoded unsigned integer, positioned immediately before position `pos`
    /// in the file, and return the length of the encoded int.
    int readVIntLength(long pos)
    {
        seekTo(pos);
        return VIntCoding.computeUnsignedVIntSize(currentBuffer, (int) (pos - 1 - currentBufferOffset));
    }

    /// Read a variable-length-encoded unsigned integer with the given length (obtained using [#readVIntLength]),
    /// positioned immediately before position `pos` in the file.
    long readVInt(long pos, int vintLength)
    {
        long withMask = readSizedInt(pos, vintLength);
        // vintLength * 7 + vintLength / 8 is 64 for vintLength == 8 and vintLength * 7 otherwise
        return withMask & ((1 << vintLength * 7 + vintLength / 8) - 1);
    }

    /// Reads a long int from an int array with the given number of bytes per item.
    /// Unlike other read methods, this accepts the position _before_ the array in `base`.
    long readSizedInt(long base, int index, int bytes)
    {
        return readSizedInt(base + (index + 1) * bytes, bytes);
    }

    /// Reads a long int from an int array with the given number of bytes per item, where the first element of the
    /// array is an implicit 0.
    /// Unlike other read methods, this accepts the position _before_ the array in `base`.
    long readSizedIntImplicit0(long base, int index, int bytes)
    {
        return index > 0 ? readSizedInt(base + index * bytes, bytes) : 0;
    }

    /// Read `bytes` many bytes preceding position `pos` in the file into a long unsigned integer.
    long readSizedInt(long pos, int bytes)
    {
        seekTo(pos);
        if (pos - currentBufferOffset >= 8)
        {
            long l = currentBuffer.getLong((int) (pos - 8 -  currentBufferOffset));
            return (Long.reverseBytes(l) >>> (64 - bytes * 8));
        }
        else
        {
            long l = 0;
            while (bytes-- > 0)
                l = (l << 8) | readByteBefore(pos--);
            return l;
        }
    }

    /// Read one byte positioned immediately before `filePos`.
    int readByteBefore(long filePos)
    {
        seekTo(filePos);
        return currentBuffer.get((int) (filePos - 1 - currentBufferOffset)) & 0xFF;
    }

    /// Seek in the file to make the data preceding `pos` available in `currentBuffer`.
    void seekTo(long filePos)
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

    /// Used for debugging.
    String dumpNode()
    {
        return currentImpl.dump(this);
    }

    /// Used for debugging.
    String dumpNode(long node)
    {
        return new OnDiskCursor<>(rdr.deserializer, rebufferer, byteComparableVersion, direction(), swapContentSides, node).dumpNode();
    }
}
