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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.vint.VIntCoding;

public class OnDiskCursor<T> implements Cursor<T>
{
    /// The buffer source of a trie, shared by all its cursors.
    ///
    /// A cursor holds on to the data it was given until it has to move off it, and more than one cursor is live at a
    /// time in normal use: a deletion branch cursor is taken while its parent is still walking, and merges and slices
    /// hold several. The rebufferer a [org.apache.cassandra.io.util.ChunkReader] instantiates owns a single buffer and
    /// hands out duplicates of it, so cursors sharing one overwrite each other's data as soon as the trie is longer
    /// than a chunk. Each cursor therefore takes a rebufferer of its own and gives it back on [#close].
    ///
    /// A factory that hands itself out -- [org.apache.cassandra.io.util.ByteBufferRebufferer] over a commit-log record
    /// or a message payload -- is shared between readers by design and owns no per-cursor buffer; it reports a null
    /// [#outstandingRebufferers] and nothing is tracked or released for it.
    ///
    /// Not every cursor can be closed: [BaseTrie#tailTrie] and [BaseTrie#tailTries] hand back a trie that keeps a live
    /// cursor of its own, and [BaseTrie] has no close for the caller to reach it with. The ones that are still out when
    /// the trie itself is closed are released then, which is what the tracking is for.
    interface RebuffererSource
    {
        RebuffererFactory rebuffererFactory();

        /// The rebufferers handed out to cursors that have not given them back, or null if the factory hands itself
        /// out and there is nothing per-cursor to track.
        Set<Rebufferer> outstandingRebufferers();

        /// Hand out a rebufferer for one cursor. Paired with [#releaseRebufferer].
        default Rebufferer takeRebufferer()
        {
            Rebufferer rebufferer = rebuffererFactory().instantiateRebufferer(false);
            Set<Rebufferer> outstanding = outstandingRebufferers();
            if (outstanding != null)
                outstanding.add(rebufferer);
            return rebufferer;
        }

        /// Take back the rebufferer of a cursor that has been closed. The removal from the set is what claims it, so
        /// that this cannot race [#releaseOutstandingRebufferers] into returning the same buffer to the pool twice.
        default void releaseRebufferer(Rebufferer rebufferer)
        {
            Set<Rebufferer> outstanding = outstandingRebufferers();
            if (outstanding != null && outstanding.remove(rebufferer))
                rebufferer.closeReader();
        }

        /// Release whatever the cursors that could not be closed are still holding. Called when the trie is closed, by
        /// which point the caller must have stopped reading through it.
        default void releaseOutstandingRebufferers()
        {
            Set<Rebufferer> outstanding = outstandingRebufferers();
            if (outstanding == null)
                return;
            for (Rebufferer rebufferer : outstanding)
                if (outstanding.remove(rebufferer))
                    rebufferer.closeReader();
        }

        /// The tracking set for a factory that makes a rebufferer per cursor, or null for one that hands itself out.
        static Set<Rebufferer> trackerFor(RebuffererFactory factory)
        {
            return factory instanceof Rebufferer ? null : ConcurrentHashMap.newKeySet();
        }
    }

    public OnDiskCursor(DataDeserializer<T> deserializer,
                        RebuffererSource source,
                        ByteComparable.Version byteComparableVersion,
                        Direction direction,
                        boolean isOrdered,
                        boolean alternateInAscentSlot,
                        long root)
    {
        this(deserializer, source, byteComparableVersion, direction, isOrdered, alternateInAscentSlot);
        try
        {
            descendInto(Cursor.rootPosition(direction), root);
        }
        catch (Throwable t)
        {
            close();
            throw t;
        }
    }

    /// Start at a node given as the pair that identifies it, as [#currentFullNodePostCodePos] and
    /// [#currentFullNodeCode] hold it. Used for tails, whose root can be a position inside a chain node, which has
    /// no code byte of its own in the file.
    public OnDiskCursor(DataDeserializer<T> deserializer,
                        RebuffererSource source,
                        ByteComparable.Version byteComparableVersion,
                        Direction direction,
                        boolean isOrdered,
                        boolean alternateInAscentSlot,
                        long rootPostCodePos,
                        int rootNodeCode)
    {
        this(deserializer, source, byteComparableVersion, direction, isOrdered, alternateInAscentSlot);
        try
        {
            descendInto(Cursor.rootPosition(direction), rootPostCodePos, rootNodeCode);
        }
        catch (Throwable t)
        {
            close();
            throw t;
        }
    }

    private OnDiskCursor(DataDeserializer<T> deserializer,
                         RebuffererSource source,
                         ByteComparable.Version byteComparableVersion,
                         Direction direction,
                         boolean isOrdered,
                         boolean alternateInAscentSlot)
    {
        this.source = source;
        this.rebufferer = source.takeRebufferer();
        this.currentBH = Rebufferer.EMPTY;
        this.currentBuffer = currentBH.buffer();
        this.currentBufferOffset = 0;
        this.rdr = new SharedStream(deserializer);
        this.byteComparableVersion = byteComparableVersion;
        this.isOrdered = isOrdered;
        this.alternateInAscentSlot = alternateInAscentSlot;
        this.swapContentSides = direction.select(false, isOrdered);
        this.exhausted = Cursor.exhaustedPosition(direction);
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
    /// The trie's shared buffer source, needed to make further cursors over the same trie and to give this cursor's
    /// rebufferer back on [#close].
    final RebuffererSource source;
    /// This cursor's own rebufferer. A cursor holds on to the data it was given until it has to move off it, so it
    /// cannot share one with the other cursors of the trie: the rebufferer a chunk reader makes owns a single buffer
    /// and hands out duplicates of it. Released back to [#source] on [#close].
    Rebufferer rebufferer;
    final ByteComparable.Version byteComparableVersion;
    final boolean swapContentSides;
    final boolean isOrdered; // determines swapContentSides above; needed if tailTrie switches direction
    /// True when a node's ascent-side content slot holds an alternate-branch pointer rather than return-path content,
    /// i.e. when this walks the data trie of a deletion-aware trie. The walk then never stops on that slot; it is read
    /// on demand by [#alternateBranch]. See [DeletionAwareFileWriter].
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
    /// Both are left alone by [#descendPostPrefixOrRelay], which moves the implementation on to the node a prefix or
    /// relay points to while the cursor stays on the prefix.
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
        return new OnDiskCursor<>(rdr.deserializer, source, byteComparableVersion, direction, isOrdered, alternateInAscentSlot, currentFullNodePostCodePos, currentFullNodeCode);
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
        content = rdr.deserialize(rdr, currentPos, len);
        if (content != null)
            currentEncodedPosition |= MAY_HAVE_CONTENT_BIT;
        return currentPos;
    }

    T readContentAtPos(long currentPos)
    {
        int vintlen = readVIntLength(currentPos);
        int len = readContentLength(currentPos, vintlen);
        currentPos -= vintlen + len;
        return rdr.deserialize(rdr, currentPos, len);
    }

    void getContentAtPosWithLength(long currentPos, int len)
    {
        content = rdr.deserialize(rdr, currentPos - len, len);
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
        int vintLength = VIntCoding.computeUnsignedVIntSize(currentBuffer, (int) (pos - 1 - currentBufferOffset));
        if (vintLength < 0)
            throw corrupt("no length byte before position " + pos);
        return vintLength;
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
        assert bytes <= 8 : "Cannot read " + bytes + " bytes into a long";
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

    /// Give this cursor's buffer back to the trie it reads from. The cursor cannot be used afterwards.
    ///
    /// In-memory cursors hold nothing to release, so [Cursor#close] does nothing by default; a cursor over a file
    /// holds a buffer for as long as it lives and must be closed, or the buffer is never returned to the pool.
    @Override
    public void close()
    {
        if (rebufferer == null)
            return;     // already closed
        currentBH.release();
        currentBH = Rebufferer.EMPTY;
        currentBuffer = currentBH.buffer();
        Rebufferer toRelease = rebufferer;
        rebufferer = null;
        source.releaseRebufferer(toRelease);
    }

    /// Used for debugging.
    String dumpNode()
    {
        return currentImpl.dump(this);
    }

    /// Used for debugging.
    String dumpNode(long node)
    {
        return new OnDiskCursor<>(rdr.deserializer, source, byteComparableVersion, direction(), swapContentSides, alternateInAscentSlot, node).dumpNode();
    }

    static class Range<S extends RangeState<S>> extends OnDiskCursor<S> implements RangeCursor<S>
    {

        boolean activeIsSet;
        S activeRange;  // only non-null if activeIsSet
        S prevContent;  // can only be non-null if activeIsSet

        public Range(DataDeserializer<S> deserializer, RebuffererSource source, ByteComparable.Version byteComparableVersion, Direction direction, long root)
        {
            super(deserializer, source, byteComparableVersion, direction, true, false, root);
            initActiveState();
        }

        public Range(DataDeserializer<S> deserializer, RebuffererSource source, ByteComparable.Version byteComparableVersion, Direction direction, long rootPostCodePos, int rootNodeCode)
        {
            super(deserializer, source, byteComparableVersion, direction, true, false, rootPostCodePos, rootNodeCode);
            initActiveState();
        }

        private void initActiveState()
        {
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
            updateActiveAndReturn(encodedPosition());
        }

        @Override
        public long advance()
        {
            return updateActiveAndReturn(super.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            return updateActiveAndReturn(super.skipTo(encodedSkipPosition));
        }

        @Override
        public S state()
        {
            if (!activeIsSet)
                setActiveState();
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
//            return tailCursor(direction).advanceToContent(null);

            long postCodePos = currentFullNodePostCodePos;
            int code = currentFullNodeCode;
            while (true)
            {
                OnDiskReadNodeType type = OnDiskReadNodeType.selectNodeImpl(code);
                S content = type.getContent(this, direction, true, code, postCodePos);
                if (content != null)
                    return content;
                long node = type.getFirstChild(this, direction, code, postCodePos);
                assert node > 0;
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
            if (getStackDepth(index) != Cursor.depth(currentEncodedPosition))
                return null;    // recorded by an ancestor, not by this node
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
            // Deletion ranges active at entry and exit must be presented by the tail at its root. To do this, get
            // the closest content in both forward and reverse direction and adjust the content that the tail reports
            // for them.
            // A RangeBranch is built unconditionally, even when both sides come out null and a plain Range over the
            // same node would walk identically. Guarding on the root node carrying no content of its own on either
            // side would save an object and a second updateActiveAndReturn per plain range tail, but the guard has
            // to restate what getTailRootContent already worked out, and this path is not on any hot query today.
            // Revisit the narrower guard when it becomes the SSTable read path.
            Direction ourDirection = direction();
            S rootDescentContent = getTailRootContent(ourDirection, content, activeIsSet, activeRange);
            S rootAscentContent = getTailRootContent(ourDirection.opposite(), getAscentPathContent(), false, null);
            if (ourDirection != direction)
            {
                S swap = rootDescentContent;
                rootDescentContent = rootAscentContent;
                rootAscentContent = swap;
            }

            return new RangeBranch<>(rdr.deserializer, source, byteComparableVersion, direction, currentFullNodePostCodePos, currentFullNodeCode, rootDescentContent, rootAscentContent);
        }
    }

    static class RangeBranch<S extends RangeState<S>> extends Range<S>
    {
        final S rootAscentContent;

        public RangeBranch(DataDeserializer<S> deserializer, RebuffererSource source, ByteComparable.Version byteComparableVersion, Direction direction, long rootPostCodePos, int rootNodeCode, S rootDescentContent, S rootAscentContent)
        {
            super(deserializer, source, byteComparableVersion, direction, rootPostCodePos, rootNodeCode);
            // LEAF or PREFIX may have put a backtrack entry, remove if so
            this.stackLength = 0;
            this.content = rootDescentContent;
            this.currentEncodedPosition = rootDescentContent != null
                                          ? currentEncodedPosition | MAY_HAVE_CONTENT_BIT
                                          : currentEncodedPosition & ~MAY_HAVE_CONTENT_BIT;
            this.rootAscentContent = rootAscentContent;
            if (rootAscentContent != null)
                addBacktrack(0, OnDiskReadNodeType.ASCENT_LEAF_CODE, currentEncodedPosition | ON_RETURN_PATH_BIT);

            // Redo this as we now have different content() value
            prevContent = null;
            updateActiveAndReturn(encodedPosition());
        }

        @Override
        long getContentAtPos(long currentPos)
        {
            if (currentPos > 0)
                return super.getContentAtPos(currentPos);

            content = rootAscentContent;
            return currentPos;
        }

        @Override
        S readContentAtPos(long currentPos)
        {
            return currentPos > 0 ? super.readContentAtPos(currentPos) : rootAscentContent;
        }
    }

}
