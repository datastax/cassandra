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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.vint.VIntCoding;

public class OnDiskCursor<T> implements Cursor<T>
{
    public OnDiskCursor(DataDeserializer<T> deserializer,
                        RebuffererFactory rebuffererFactory,
                        ByteComparable.Version byteComparableVersion,
                        Direction direction,
                        boolean isOrdered,
                        boolean alternateInAscentSlot,
                        long root)
    {
        this(deserializer, rebuffererFactory, byteComparableVersion, direction, isOrdered, alternateInAscentSlot);
        try
        {
            descendInto(Cursor.rootPosition(direction), root);
        }
        catch (Throwable t)
        {
            releaseBuffers();
            throw t;
        }
    }

    /// Start at a node given as the pair that identifies it, as [#currentFullNodePostCodePos] and
    /// [#currentFullNodeCode] hold it. Used for tails, whose root can be a position inside a chain node, which has
    /// no code byte of its own in the file.
    public OnDiskCursor(DataDeserializer<T> deserializer,
                        RebuffererFactory rebuffererFactory,
                        ByteComparable.Version byteComparableVersion,
                        Direction direction,
                        boolean isOrdered,
                        boolean alternateInAscentSlot,
                        long rootPostCodePos,
                        int rootNodeCode)
    {
        this(deserializer, rebuffererFactory, byteComparableVersion, direction, isOrdered, alternateInAscentSlot);
        try
        {
            descendInto(Cursor.rootPosition(direction), rootPostCodePos, rootNodeCode);
        }
        catch (Throwable t)
        {
            releaseBuffers();
            throw t;
        }
    }

    /// A cursor that is not yet positioned: for a subclass that descends into its root itself, once the fields its
    /// overrides use are set, or to read nodes that are given to it, as a closed range cursor does in
    /// [OnDiskBaseRangeCursor#getNearestContent]. A subclass must release the buffers with [#releaseBuffers] if the
    /// descent fails, as the constructors above do.
    OnDiskCursor(DataDeserializer<T> deserializer,
                 RebuffererFactory rebuffererFactory,
                 ByteComparable.Version byteComparableVersion,
                 Direction direction,
                 boolean isOrdered,
                 boolean alternateInAscentSlot)
    {
        this.rebuffererFactory = rebuffererFactory;
        this.rebufferer = rebuffererFactory.instantiateRebufferer(false);
        this.currentBH = Rebufferer.EMPTY;
        this.currentBuffer = currentBH.buffer();
        this.currentBufferOffset = 0;
        this.rdr = new SharedStream(deserializer);
        this.byteComparableVersion = byteComparableVersion;
        this.isOrdered = isOrdered;
        this.alternateInAscentSlot = alternateInAscentSlot;
        this.swapContentSides = direction.select(false, isOrdered);
        this.exhaustedPosition = Cursor.exhaustedPosition(direction);
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

        long position()
        {
            return currentBufferOffset + buffer.position();
        }

        /// Drop the reference to the cursor's buffer when the cursor gives it back.
        void releaseBuffer()
        {
            buffer = Rebufferer.EMPTY.buffer();
        }

        public T deserialize(long startPos, int len)
        {
            seekTo(startPos);
            try
            {
                return deserializer.deserialize(this, len);
            }
            catch (IOException e)
            {
                // A rebuffering failure arrives already wrapped, so this is the deserializer rejecting the
                // bytes it was given -- which, for a commit-log record or a message payload, can be corrupt.
                // Present it the way the corruption checks in this class do, as an unchecked wrapper the
                // callers can unwrap back into the IOException it is.
                throw new UncheckedIOException(e);
            }
        }
    }

    public interface DataDeserializer<T>
    {
        T deserialize(DataInputPlus rdr, int length) throws IOException;
    }

    /// The position in the trie of the next byte the reader handed to a [DataDeserializer] reads, for a deserializer
    /// that checks how much of its payload it has read.
    ///
    /// Valid only for the stream this cursor hands a [DataDeserializer], which it casts to the cursor's shared
    /// stream; it exists so that a deserializer can check it consumed exactly the payload length it was given. Any
    /// other stream throws a `ClassCastException`.
    public static long position(DataInputPlus rdr)
    {
        return ((OnDiskCursor<?>.SharedStream) rdr).position();
    }

    final SharedStream rdr;
    /// The trie's buffer source, shared by all its cursors and needed to make further cursors over the same trie.
    final RebuffererFactory rebuffererFactory;
    /// This cursor's own rebufferer, taken from [#rebuffererFactory] on construction and given back on [#close].
    ///
    /// A cursor holds on to the data it was given until it has to move off it, and more than one cursor is live at a
    /// time in normal use: a deletion branch cursor is taken while its parent is still walking, and merges and slices
    /// hold several. The rebufferer a [org.apache.cassandra.io.util.ChunkReader] instantiates owns a single buffer and
    /// hands out duplicates of it, so cursors sharing one would overwrite each other's data as soon as the trie is
    /// longer than a chunk; hence one per cursor.
    ///
    /// A factory that hands itself out -- [org.apache.cassandra.io.util.ByteBufferRebufferer] over a commit-log record
    /// or a message payload -- is shared between readers by design and owns no per-cursor buffer; its `closeReader` is
    /// a no-op, so the same take-and-give-back works for it.
    ///
    /// Null once the cursor has been closed, which is what makes [#close] idempotent.
    Rebufferer rebufferer;
    final ByteComparable.Version byteComparableVersion;
    final boolean swapContentSides;
    final boolean isOrdered; // determines swapContentSides above; needed if tailTrie switches direction
    /// True when a node's ascent-side content slot holds an alternate-branch pointer rather than return-path content,
    /// i.e. when this walks the data trie of a deletion-aware trie. The walk then never stops on that slot; it is read
    /// on demand by [#alternateBranch]. See [OnDiskDeletionAwareTrieWriter].
    final boolean alternateInAscentSlot;


    Rebufferer.BufferHolder currentBH;
    ByteBuffer currentBuffer;
    long currentBufferOffset;

    long[] stack = new long[3 * 16];
    int stackLength;

    final long exhaustedPosition;

    T content = null;
    OnDiskReadNodeType currentImpl;
    long currentEncodedPosition;
    long postCodePos;
    long nodeImplData;
    int nodeCode;

    /// The node the cursor is positioned on, as the pair that identifies it: the position just after its code byte,
    /// and that code. A chain node steps through its bytes without moving to a new node in the file, so the position
    /// alone does not identify where a tail taken here must start -- the code carries how much of the chain is left.
    /// Both are left alone by [#descendPostPrefix], which moves the implementation on to the node a prefix points to
    /// while the cursor stays on the prefix.
    long currentFullNodePostCodePos;   // for tails
    int currentFullNodeCode;

    long descendInto(long encodedPosition, long nodePos)
    {
        return descendInto(encodedPosition, nodePos - 1, readByteBefore(nodePos));
    }

    long descendInto(long encodedPosition, long postCodePos, int nodeCode)
    {
        this.currentFullNodePostCodePos = postCodePos;
        this.currentFullNodeCode = nodeCode;
        this.content = null;
        // Both flags are set by the node implementation below; until they are, this node is known to have neither
        // content nor a deletion branch.
        this.currentEncodedPosition = encodedPosition & ~(MAY_HAVE_CONTENT_BIT | MAY_HAVE_DELETION_BRANCH_BIT);
        this.postCodePos = postCodePos;
        this.nodeCode = nodeCode;
        this.currentImpl = OnDiskReadNodeType.selectNodeImpl(nodeCode);
        this.currentImpl.load(this); // may modify currentEncodedPosition
        return currentEncodedPosition;
    }

    void descendPostPrefix(long nodePos)
    {
        // leave content and encodedPosition unchanged
        this.postCodePos = nodePos - 1;
        this.nodeCode = readByteBefore(nodePos);
        this.currentImpl = OnDiskReadNodeType.selectNodeImpl(nodeCode);
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
        if (descend != exhaustedPosition)
            return descend;
        else
            return ascend();
    }

    private long ascend()
    {
        if (stackLength <= 0)
            return exhausted();

        --stackLength;
        currentImpl = loadStackImpl(stackLength);
        return currentImpl.advance(this);
    }

    /// Move to the exhausted position, which [#encodedPosition] must then report like any other position the cursor
    /// returns: the walks that drive several cursors at once read it from there rather than from the return value.
    private long exhausted()
    {
        stackLength = 0;
        content = null;
        return currentEncodedPosition = exhaustedPosition;
    }

    /// Follows the in-memory cursor's `skipTo` (see [InMemoryReadTrie]).
    ///
    /// A node is given only the target's transition, and moves to its first child at or after it. That is the target
    /// or a position after it, except when the target is the return path of the child with that transition: the
    /// return path of a node comes after the node and everything below it, so the descent stops short of the target.
    /// The target is then on the stack if it has return-path content: a node that presents content on its return path
    /// records it, as it is loaded, in a backtrack entry at the depth of its parent (see
    /// [OnDiskReadNodeType#addReturnPathBacktrack]), which is therefore the entry on top of the stack after the
    /// descent, and presents the target. If the node has no such content, the entry on top leads to the first position
    /// after the target.
    @Override
    public long skipTo(long encodedSkipPosition)
    {
        int skipTransition = Cursor.incomingTransition(encodedSkipPosition);
        if (!Cursor.ascended(encodedSkipPosition, currentEncodedPosition))
        {
            // Descent requested. Jump to the given child transition or greater, and backtrack if there's no such.
            long position = currentImpl.skipTo(this, skipTransition);
            if (position != exhaustedPosition && Cursor.compare(position, encodedSkipPosition) >= 0)
                return position;
            // Either there is no such child, or the target is the return path of the child we descended into, and the
            // entry on top of the stack is the target or the first position after it.
            return ascend();
        }

        // Backtrack until we reach the requested depth. A node can leave more than one entry at the depth of its
        // parent (the parent's remaining children, and the node's own return-path content, whose only child is that
        // return path, at the node's transition), so check each in turn.
        int skipDepth = Cursor.depth(encodedSkipPosition);
        while (--stackLength >= 0)
        {
            int depth = getStackDepth(stackLength);
            if (depth < skipDepth - 1)
            {
                // We ascended above the target depth, we only need to advance.
                currentImpl = loadStackImpl(stackLength);
                return currentImpl.advance(this);
            }

            if (depth == skipDepth - 1)
            {
                currentImpl = loadStackImpl(stackLength);
                long position = currentImpl.skipTo(this, skipTransition);
                if (position != exhaustedPosition && Cursor.compare(position, encodedSkipPosition) >= 0)
                    return position;
                // We found nothing here, or descended into the child whose return path is the target and which may
                // just have added a backtrack entry for it. The next iteration of the loop will find it.
            }
        }
        return exhausted();
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        return returnOrAscend(currentImpl.advanceMultiple(this, receiver));
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        return new OnDiskCursor<>(rdr.deserializer, rebuffererFactory, byteComparableVersion, direction, isOrdered, alternateInAscentSlot, currentFullNodePostCodePos, currentFullNodeCode);
    }

    /// The position of the alternate branch the current node carries in its ascent-side content slot, or -1 if it
    /// has none. Only meaningful when [#alternateInAscentSlot] is set.
    long alternateBranch()
    {
        return OnDiskReadNodeType.selectNodeImpl(currentFullNodeCode)
                                 .getAlternateBranch(this, currentFullNodeCode, currentFullNodePostCodePos);
    }

    long getContentAtPos(long currentPos)
    {
        int vintlen = readVIntLength(currentPos);
        int len = readContentLength(currentPos, vintlen);
        currentPos -= vintlen + len;
        content = rdr.deserialize(currentPos, len);
        if (content != null)
            currentEncodedPosition |= MAY_HAVE_CONTENT_BIT;
        return currentPos;
    }

    final T readContentAtPos(long currentPos)
    {
        int vintlen = readVIntLength(currentPos);
        int len = readContentLength(currentPos, vintlen);
        currentPos -= vintlen + len;
        return rdr.deserialize(currentPos, len);
    }

    void getContentAtPosWithLength(long currentPos, int len)
    {
        content = rdr.deserialize(currentPos - len, len);
        if (content != null)
            currentEncodedPosition |= MAY_HAVE_CONTENT_BIT;
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

    /// Record a node to come back to for its remaining children.
    void addBacktrack(long nodeBasePos, int nodeCode, long nodeImplData)
    {
        addBacktrack(Cursor.depth(currentEncodedPosition), nodeBasePos, nodeCode, nodeImplData);
    }

    /// Record an entry to come back to when the walk returns to the given depth. Return-path content is recorded at the
    /// depth of the parent of the position that presents it; see [OnDiskReadNodeType#addReturnPathBacktrack].
    void addBacktrack(int depth, long nodeBasePos, int nodeCode, long nodeImplData)
    {
        // store depth & nodeCode in one long, nodeBasePos, nodeImplData
        if (stack.length <= stackLength * 3)
            stack = Arrays.copyOf(stack, stack.length * 2);
        stack[stackLength * 3 + 0] = (depth & 0xFFFFFFFFL) | (((long) nodeCode) << 32);
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

    /// Read the first byte of a variable-length-encoded unsigned integer, positioned immediately before position `pos`
    /// in the file, and return the length of the encoded int.
    int readVIntLength(long pos)
    {
        seekTo(pos);
        return VIntCoding.computeUnsignedVIntSizeUnchecked(currentBuffer, (int) (pos - 1 - currentBufferOffset));
    }

    /// Read a variable-length-encoded unsigned integer with the given length (obtained using [#readVIntLength]),
    /// positioned immediately before position `pos` in the file.
    long readVInt(long pos, int vintLength)
    {
        // The longest encoding spends its whole leading byte on the length, and the remaining eight bytes carry all
        // 64 bits of the value; the shorter ones leave 8 - vintLength value bits in the leading byte.
        if (vintLength == 9)
            return readSizedInt(pos - 1, 8);
        long withMask = readSizedInt(pos, vintLength);
        return withMask & ((1L << vintLength * 7) - 1);
    }

    /// Read the length of a content item that ends immediately before `pos`, where `vintLength` is the size of the
    /// length's own encoding as given by [#readVIntLength].
    ///
    /// The bytes being read are a commit-log record or a message payload, i.e. they can be corrupt. A length that does
    /// not fit in the space before its encoding would place the item's start outside the data, so reject it here rather
    /// than let it turn into an arbitrary position to seek to.
    int readContentLength(long pos, int vintLength)
    {
        long length = readVInt(pos, vintLength);
        if (length < 0 || length > pos - vintLength || length > Integer.MAX_VALUE)
            throw corrupt("content length " + length + " before position " + pos);
        return (int) length;
    }

    static UncheckedIOException corrupt(String message)
    {
        return new UncheckedIOException(new IOException("Corrupt serialized trie: " + message));
    }

    /// Check a child pointer read from the file. Children are written before their parent, so a child must end at or
    /// before `nodeDataStart`, where the parent's own bytes begin, and after the start of the data. Every descent thus
    /// moves strictly towards the start of the data, which is what guarantees that a walk over corrupt bytes
    /// terminates: a pointer that does not would let a node be its own descendant.
    static long checkChild(long child, long nodeDataStart)
    {
        if (child <= 0 || child > nodeDataStart)
            throw corrupt("child position " + child + " outside of (0, " + nodeDataStart + "]");
        return child;
    }

    /// Reads a long int from an int array with the given number of bytes per item, where the first element of the
    /// array is an implicit 0.
    /// The parameter `pos` must point to the end of the array, where the int with index 1 ends.
    long readSizedIntImplicit0(long pos, int index, int bytes)
    {
        return index > 0 ? readSizedInt(pos - (index - 1) * bytes, bytes) : 0;
    }

    /// Read `bytes` many bytes preceding position `pos` in the file into a long unsigned integer. `bytes` is at most 8:
    /// the callers take it from a node's pointer-size bits or a vint's length, or check it, as the prefix node's
    /// [OnDiskReadNodeType#getAlternateBranch] does.
    long readSizedInt(long pos, int bytes)
    {
        seekTo(pos);
        if (pos - currentBufferOffset >= 8)
        {
            long l = currentBuffer.getLong((int) (pos - 8 -  currentBufferOffset));
            return (Long.reverseBytes(l) >>> (64 - bytes * 8));
        }
        else
            return readSizedIntByBytes(pos, bytes);
    }

    /// [#readSizedInt] for an int that is within 8 bytes of the start of the buffer, where it cannot be read as a long.
    private long readSizedIntByBytes(long pos, int bytes)
    {
        long l = 0;
        while (bytes-- > 0)
            l = (l << 8) | readByteBefore(pos--);
        return l;
    }

    /// Read one byte positioned immediately before `filePos`.
    int readByteBefore(long filePos)
    {
        seekTo(filePos);
        return currentBuffer.get((int) (filePos - 1 - currentBufferOffset)) & 0xFF;
    }

    /// Seek in the file to make the data preceding `pos` available in `currentBuffer`.
    ///
    /// The positions asked for come from the bytes being read, which can be a corrupt commit-log record or message
    /// payload, so one outside the data is rejected as corruption. The check is made again after rebuffering, because
    /// a rebufferer does not necessarily refuse such a position: [org.apache.cassandra.io.util.ByteBufferRebufferer]
    /// returns the same buffer whatever position it is asked for.
    ///
    /// A buffer can also hold data past the file's length: chunks are read in full, and cached chunks and mapped
    /// regions are shared by the readers of a file, which can have different lengths. The buffer's limit is therefore
    /// brought down to the length, so that neither the check above nor the deserializer's stream reads past it.
    void seekTo(long filePos)
    {
        // Compare with the limit, not what remains: the buffer is shared with the deserializer's stream, which moves
        // its position as it reads, and the reads here are absolute.
        long ofs = filePos - currentBufferOffset;
        if (ofs <= 0 || ofs > currentBuffer.limit())
            rebufferTo(filePos);
    }

    /// The part of [#seekTo] that runs when the data is not in the current buffer, kept apart so that the check above
    /// is small enough to be inlined into every read.
    private void rebufferTo(long filePos)
    {
        if (filePos <= 0)
            throw corrupt("read before the start of the data, at position " + filePos);
        long fileLength = rebufferer.fileLength();
        if (filePos > fileLength)
            throw corrupt("read past the end of the data, at position " + filePos + ", length " + fileLength);
        currentBH.release();
        currentBH = Rebufferer.EMPTY;
        currentBH = rebufferer.rebuffer(filePos - 1);
        currentBuffer = currentBH.buffer();
        currentBufferOffset = currentBH.offset();
        long ofs = filePos - currentBufferOffset;
        if (ofs <= 0 || ofs > currentBuffer.limit())
            throw corrupt("read past the end of the data, at position " + filePos);
        if (currentBufferOffset + currentBuffer.limit() > fileLength)
            currentBuffer.limit((int) (fileLength - currentBufferOffset));
    }

    /// Give this cursor's buffer back to the factory it came from. A cursor over a file holds a buffer for as long as
    /// it lives, so it must be closed or the buffer is never returned to the pool.
    ///
    /// That is all closing does. [#tailCursor] remains usable afterwards, as [Cursor#close] requires: the tail is
    /// rooted at [#currentFullNodePostCodePos] and [#currentFullNodeCode], which are plain fields, and it reads the
    /// file with a rebufferer of its own, which it takes from [#rebuffererFactory] -- never with this cursor's.
    @Override
    public void close()
    {
        releaseBuffers();
    }

    /// The part of [#close] that gives up the buffers, separated out so the constructors, here and in subclasses, can
    /// call it on a failed descent without dispatching to a subclass override on a half-built object.
    ///
    /// Idempotent, as [Cursor#close] requires: a cursor can be reached by two wrappers that each close it.
    final void releaseBuffers()
    {
        if (rebufferer == null)
            return;     // already closed
        currentBH.release();
        currentBH = Rebufferer.EMPTY;
        currentBuffer = currentBH.buffer();
        rdr.releaseBuffer();
        Rebufferer toRelease = rebufferer;
        rebufferer = null;
        toRelease.closeReader();
    }

    /// Used for debugging.
    String dumpNode()
    {
        return currentImpl.dump(this);
    }

    /// Used for debugging.
    String dumpNode(long node)
    {
        try (OnDiskCursor<T> cursor = new OnDiskCursor<>(rdr.deserializer, rebuffererFactory, byteComparableVersion, direction(), isOrdered, alternateInAscentSlot, node))
        {
            return cursor.dumpNode();
        }
    }
}
