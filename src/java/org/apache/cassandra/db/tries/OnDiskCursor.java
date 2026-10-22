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
        this(deserializer, rebuffererFactory, byteComparableVersion, direction, isOrdered, alternateInAscentSlot, null);
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
    /// no code byte of its own in the file. `tailRootAscentContent` is null except for a range tail; see
    /// [#tailRootAscentContent].
    public OnDiskCursor(DataDeserializer<T> deserializer,
                        RebuffererFactory rebuffererFactory,
                        ByteComparable.Version byteComparableVersion,
                        Direction direction,
                        boolean isOrdered,
                        boolean alternateInAscentSlot,
                        long rootPostCodePos,
                        int rootNodeCode,
                        T tailRootAscentContent)
    {
        this(deserializer, rebuffererFactory, byteComparableVersion, direction, isOrdered, alternateInAscentSlot, tailRootAscentContent);
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

    private OnDiskCursor(DataDeserializer<T> deserializer,
                         RebuffererFactory rebuffererFactory,
                         ByteComparable.Version byteComparableVersion,
                         Direction direction,
                         boolean isOrdered,
                         boolean alternateInAscentSlot,
                         T tailRootAscentContent)
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
        this.exhausted = Cursor.exhaustedPosition(direction);
        this.tailRootAscentContent = tailRootAscentContent;
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

    interface DataDeserializer<T>
    {
        T deserialize(DataInputPlus rdr, int length) throws IOException;
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

    final long exhausted;

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

    /// The content a range tail presents on its root's ascent side, read in place of the item at
    /// [#TAIL_ROOT_ASCENT_CONTENT_POS], which is where the tail's backtrack entry for it points (see the tail
    /// constructor of [Range]). Null in any other cursor.
    final T tailRootAscentContent;

    /// A content position no read of the file can produce -- [#readContentLength] keeps every item's start at or after
    /// 0 -- so that only the backtrack entry of [#tailRootAscentContent] reaches it, and a corrupt read at position 0
    /// still throws.
    static final long TAIL_ROOT_ASCENT_CONTENT_POS = -1;

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
        if (descend != exhausted)
            return descend;
        else
            return ascend();
    }

    private long ascend()
    {
        if (stackLength <= 0)
            return exhaust();

        --stackLength;
        currentImpl = loadStackImpl(stackLength);
        return currentImpl.advance(this);
    }

    /// Move to the exhausted position, which [#encodedPosition] must then report like any other position the cursor
    /// returns: the walks that drive several cursors at once read it from there rather than from the return value.
    private long exhaust()
    {
        stackLength = 0;
        content = null;
        return currentEncodedPosition = exhausted;
    }

    /// Follows the in-memory cursor's `skipTo` (see [InMemoryReadTrie]). The node implementations' `skipTo` find the
    /// first child whose transition is at or after the target's, ignoring the return-path bit, so when the target is
    /// a node's return-path position the walk descends into that node and then looks for the target among the
    /// backtrack entries the descent added.
    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (!Cursor.ascended(encodedSkipPosition, currentEncodedPosition))
        {
            // Descent requested. Jump to the given child transition or greater, and backtrack if there's no such.
            long position = currentImpl.skipTo(this, encodedSkipPosition);
            if (position != exhausted && Cursor.compare(position, encodedSkipPosition) >= 0)
                return position;
            // Either there is no such child, or the target is the return path of the child we descended into. In the
            // latter case the child's return-path content, if it has any, is the backtrack entry on top of the stack;
            // if it has none, the entry there gives the next position after the target.
            return ascend();
        }

        // Backtrack until we reach the requested depth. A node can leave more than one entry at the depth of its
        // parent (the parent's remaining children, and the node's own return-path content), so check each in turn.
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
                long position = currentImpl.skipTo(this, encodedSkipPosition);
                if (position != exhausted && Cursor.compare(position, encodedSkipPosition) >= 0)
                    return position;
                // We found nothing here, or descended into the child whose return path is the target and which may
                // just have added a backtrack entry for it. The next iteration of the loop will find it.
            }
        }
        return exhaust();
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        return returnOrAscend(currentImpl.advanceMultiple(this, receiver));
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        return new OnDiskCursor<>(rdr.deserializer, rebuffererFactory, byteComparableVersion, direction, isOrdered, alternateInAscentSlot, currentFullNodePostCodePos, currentFullNodeCode, null);
    }

    /// The position of the alternate branch the current node carries in its ascent-side content slot, or -1 if it
    /// has none. Only meaningful when [#alternateInAscentSlot] is set.
    long alternateBranch()
    {
        // Selecting the type also rejects a reserved code.
        return OnDiskReadNodeType.selectNodeImpl(currentFullNodeCode) == OnDiskReadNodeType.PREFIX
               ? OnDiskReadNodeType.prefixAlternateBranch(this, currentFullNodeCode, currentFullNodePostCodePos)
               : -1;
    }

    final long getContentAtPos(long currentPos)
    {
        if (currentPos == TAIL_ROOT_ASCENT_CONTENT_POS && tailRootAscentContent != null)
        {
            content = tailRootAscentContent;
            return currentPos;
        }
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
        if (currentPos == TAIL_ROOT_ASCENT_CONTENT_POS && tailRootAscentContent != null)
            return tailRootAscentContent;
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

    /// Record the content at `contentPos` to be presented on the return path of the current position.
    ///
    /// Like the in-memory cursor, this stores the entry at the depth of the parent: on the parent's level the return
    /// path of a child is ordered after the descent into it and before the next sibling, which is what [#skipTo] relies
    /// on when the target is that return-path position. For the root the depth is -1.
    void addReturnPathBacktrack(long contentPos)
    {
        addBacktrack(Cursor.depth(currentEncodedPosition) - 1,
                     contentPos,
                     OnDiskReadNodeType.ASCENT_LEAF_CODE,
                     currentEncodedPosition | Cursor.ON_RETURN_PATH_BIT);
    }

    private void addBacktrack(int depth, long nodeBasePos, int nodeCode, long nodeImplData)
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
    /// the callers take it from a node's pointer-size bits or a vint's length, or check it, as
    /// [OnDiskReadNodeType#prefixAlternateBranch] does.
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
    /// [#tailCursor] remains usable afterwards, as [Cursor#close] requires: it is built from
    /// [#currentFullNodePostCodePos] and [#currentFullNodeCode], which are plain fields and are not read from the
    /// file. Nothing has to be snapshotted here; [OnDiskCursor.Range] is the one that does.
    @Override
    public void close()
    {
        releaseBuffers();
    }

    /// The part of [#close] that gives up the buffers, separated out so the constructors can call it on a failed
    /// descent without dispatching to a subclass override on a half-built object.
    ///
    /// Idempotent, as [Cursor#close] requires: a cursor can be reached by two wrappers that each close it.
    private void releaseBuffers()
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

    static class Range<S extends RangeState<S>> extends OnDiskCursor<S> implements RangeCursor<S>
    {

        boolean activeIsSet;
        S activeRange;  // only non-null if activeIsSet
        S prevContent;  // can only be non-null if activeIsSet

        /// The ascent-side content a tail taken at the position this cursor was closed on must present at its root,
        /// worked out by [#close] because [#tailCursor] can no longer read the file. Only meaningful after close.
        S closedRootAscentContent;

        /// Why [#close] could not work out what a tail needs, if it could not -- the bytes can be corrupt. A tail asked
        /// for after such a close throws this rather than read a buffer that has already been given back.
        ///
        /// Also set by a move that fails, which can leave the cursor part way, e.g. on a node whose content it has read
        /// without the state that follows from it. [#close] then does not try, and [#state] and [#tailCursor] throw this.
        RuntimeException closeFailure;

        public Range(DataDeserializer<S> deserializer, RebuffererFactory rebuffererFactory, ByteComparable.Version byteComparableVersion, Direction direction, long root)
        {
            super(deserializer, rebuffererFactory, byteComparableVersion, direction, true, false, root);
            initActiveState();
        }

        /// A tail rooted at the given node, presenting the given content on its root's two sides in place of what the
        /// node itself holds; see [#tailCursor].
        public Range(DataDeserializer<S> deserializer, RebuffererFactory rebuffererFactory, ByteComparable.Version byteComparableVersion, Direction direction, long rootPostCodePos, int rootNodeCode, S rootDescentContent, S rootAscentContent)
        {
            super(deserializer, rebuffererFactory, byteComparableVersion, direction, true, false, rootPostCodePos, rootNodeCode, rootAscentContent);
            // LEAF or PREFIX may have put a backtrack entry, remove if so
            this.stackLength = 0;
            this.content = rootDescentContent;
            this.currentEncodedPosition = rootDescentContent != null
                                          ? currentEncodedPosition | MAY_HAVE_CONTENT_BIT
                                          : currentEncodedPosition & ~MAY_HAVE_CONTENT_BIT;
            if (rootAscentContent != null)
                addReturnPathBacktrack(TAIL_ROOT_ASCENT_CONTENT_POS);
            // The state follows from the content set above, not from the node's own: with no root content the tail
            // starts with no active range.
            initActiveState();
        }

        private void initActiveState()
        {
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
            updateActiveAndReturn(encodedPosition());
        }

        /// Take everything a post-close [#tailCursor] or [RangeCursor#precedingStateCursor] would read from the file,
        /// then release the buffer as the base class does.
        ///
        /// Unlike the plain cursor, a range tail is not just a position: it must also present the deletions active at
        /// its root, and both of those come out of the file. The descent side is the active range, which a [#skipTo]
        /// may have left unresolved -- [#state] resolves it into `activeRange`, after which `getTailRootContent` needs
        /// no reads. The ascent side has no field to land in, so it gets one here.
        ///
        /// The ascent side is skipped when the cursor is exhausted or on the return path, because `tailCursor` is an
        /// error in those states anyway, and being exhausted is how a cursor is usually left when the walk that owns
        /// it is done -- there is no reason to make the ordinary end of a walk pay for a descent to nearest content.
        ///
        /// The buffer is given back whether or not that succeeds. If it fails, the failure is kept for a tail asked for
        /// afterwards instead of being thrown from here: the caller is usually done with the cursor and closing it on
        /// its way out, possibly after another failure from the same bytes.
        @Override
        public void close()
        {
            try
            {
                if (rebufferer != null && closeFailure == null)
                {
                    state();
                    if (!Cursor.isExhausted(currentEncodedPosition) && !Cursor.isOnReturnPath(currentEncodedPosition))
                        closedRootAscentContent = getTailRootContent(direction().opposite(), getAscentPathContent(), false, null);
                }
            }
            catch (RuntimeException e)
            {
                closeFailure = e;
            }
            finally
            {
                super.close();
            }
        }

        @Override
        public long advance()
        {
            try
            {
                return updateActiveAndReturn(super.advance());
            }
            catch (RuntimeException e)
            {
                throw failed(e);
            }
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            try
            {
                return updateActiveAndReturn(super.advanceMultiple(receiver));
            }
            catch (RuntimeException e)
            {
                throw failed(e);
            }
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            try
            {
                return updateActiveAndReturn(super.skipTo(encodedSkipPosition));
            }
            catch (RuntimeException e)
            {
                throw failed(e);
            }
        }

        /// Record a failed move in [#closeFailure], and drop the active state so that [#state] throws it too: an
        /// advance keeps the state it starts from, which no longer applies wherever the move stopped.
        private RuntimeException failed(RuntimeException e)
        {
            closeFailure = e;
            activeIsSet = false;
            activeRange = null;
            prevContent = null;
            return e;
        }

        @Override
        public S state()
        {
            if (!activeIsSet)
            {
                if (closeFailure != null)
                    throw closeFailure;
                setActiveState();
            }
            return activeRange;
        }

        long updateActiveAndReturn(long position)
        {
            if (!Cursor.isExhausted(position))
            {
                // Always check if we are seeing new content; if we do, that's an easy state update.
                S content = content();
                if (content != null)
                {
                    activeRange = content;
                    prevContent = content;
                    activeIsSet = true;
                }
                else if (prevContent != null)
                {
                    // If the previous state was exact, its right side is what we now have.
                    activeRange = prevContent.succedingState(direction());
                    prevContent = null;
                    assert activeIsSet;
                }
                // otherwise the active state is either not set or still valid.
            }
            else
            {
                // exhausted
                activeIsSet = true;
                activeRange = null;
                prevContent = null;
            }
            return position;
        }

        private void setActiveState()
        {
            assert content() == null;
            S nearestContent = getNearestContent(direction());
            // Note: the nearest content may change between the time we fetch it and when we reach that node, e.g.
            // if someone deletes aa-cd where there existed an abc-acd deletion, and we fetched the latter while at "a".
            // This, though, should only be possible if the preceding state of the nearest content is null.
            activeRange = nearestContent != null ? nearestContent.precedingState(direction()) : null;
            prevContent = null;
            activeIsSet = true;
        }

        private S getNearestContent(Direction direction)
        {
            long postCodePos = currentFullNodePostCodePos;
            int code = currentFullNodeCode;
            while (true)
            {
                OnDiskReadNodeType type = OnDiskReadNodeType.selectNodeImpl(code);
                S content = type.getContent(this, direction, code, postCodePos);
                if (content != null)
                    return content;
                long node = type.getFirstChild(this, direction, code, postCodePos);
                if (node <= 0)
                    throw corrupt("node at " + postCodePos + " has neither content nor children");
                postCodePos = node - 1;
                code = readByteBefore(node);
            }
        }

        /// The content that will be presented when the walk returns to the current position, which a tail
        /// taken here has to reproduce on its root's ascent side.
        ///
        /// [OnDiskReadNodeType#PREFIX] records it as a backtrack entry when it loads the node, and then moves
        /// the cursor on to the post-prefix node, so `currentImpl`, `nodeCode` and `postCodePos` no longer
        /// describe the node that holds it -- often `currentImpl` is LEAF over bytes that are not a leaf.
        /// The backtrack entry is the only remaining record of it, as it is in the in-memory cursor.
        S getAscentPathContent()
        {
            if (stackLength <= 0)
                return null;
            int index = stackLength - 1;
            if (getStackNodeCode(index) != OnDiskReadNodeType.ASCENT_LEAF_CODE)
                return null;    // a child backtrack entry, not return-path content
            if (getStackDepth(index) != Cursor.depth(currentEncodedPosition) - 1)
                return null;    // recorded by an ancestor, not by this node (see addReturnPathBacktrack)
            return readContentAtPos(getStackPostCodePos(index));
        }

        S getTailRootContent(Direction direction, S contentAtRoot, boolean activeRangeKnown, S activeRange)
        {
            if (contentAtRoot != null)
                return contentAtRoot.restrict(!direction.isForward(), direction.isForward());
            if (!activeRangeKnown)
                activeRange = getNearestContent(direction);
            if (activeRange == null)
                return null;
            activeRange = activeRange.precedingState(direction);
            if (activeRange == null)
                return null;
            return activeRange.asBoundary(direction);
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            if (closeFailure != null)
                throw closeFailure;
            // Deletion ranges active at entry and exit must be presented by the tail at its root. To do this, get
            // the closest content in both forward and reverse direction and adjust the content that the tail reports
            // for them.
            // The root content is applied unconditionally, even when both sides come out null and a plain Range over
            // the same node would walk identically. Guarding on the root node carrying no content of its own on either
            // side would save resetting the root's state per plain range tail, but the guard has to restate what
            // getTailRootContent already worked out, and this path is not on any hot query today. Revisit the
            // narrower guard when it becomes the SSTable read path.
            Direction ourDirection = direction();
            S rootDescentContent = getTailRootContent(ourDirection, content, activeIsSet, activeRange);
            // After close the ascent side can no longer be read; use what close() worked out while it still could.
            S rootAscentContent = rebufferer != null
                                  ? getTailRootContent(ourDirection.opposite(), getAscentPathContent(), false, null)
                                  : closedRootAscentContent;
            if (ourDirection != direction)
            {
                S swap = rootDescentContent;
                rootDescentContent = rootAscentContent;
                rootAscentContent = swap;
            }

            return new Range<>(rdr.deserializer, rebuffererFactory, byteComparableVersion, direction, currentFullNodePostCodePos, currentFullNodeCode, rootDescentContent, rootAscentContent);
        }
    }

}
