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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.ReflectionUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/// Covers the back-to-front integer decoding of [OnDiskCursor], which the round-trip tests only reach
/// for the small content sizes they produce, its behaviour on lengths that a corrupt commit-log
/// record or message payload can present, and the parts of the cursor contract that a walk comparison
/// against the in-memory trie does not check.
public class OnDiskCursorTest
{
    @BeforeClass
    public static void setUp()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// One value per encoded length the writer can produce, on both sides of every length boundary,
    /// including the nine-byte encoding, which no content can be long enough to need but which a
    /// corrupt leading byte still asks the reader to decode.
    private static final long[] VALUES =
    { 0, 1, 127, 128, (1L << 14) - 1, 1L << 14, (1L << 21) - 1, 1L << 21, (1L << 28) - 1, 1L << 28,
      (1L << 35) - 1, 1L << 35, (1L << 42) - 1, 1L << 42, (1L << 49) - 1, 1L << 49, (1L << 56) - 1,
      1L << 56, Long.MAX_VALUE };

    @Test
    public void testVIntRoundTrip() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long[] ends = new long[VALUES.length];
            for (int i = 0; i < VALUES.length; ++i)
            {
                OnDiskBaseTrieWriter.writeReversedVint(out, VALUES[i]);
                ends[i] = out.position();
            }

            OnDiskCursor<Void> cursor = cursorOver(out);
            for (int i = 0; i < VALUES.length; ++i)
            {
                int vintLength = cursor.readVIntLength(ends[i]);
                assertEquals("Encoded length of " + VALUES[i],
                             VIntCoding.computeUnsignedVIntSize(VALUES[i]),
                             vintLength);
                assertEquals("Round trip of " + VALUES[i], VALUES[i], cursor.readVInt(ends[i], vintLength));
            }
        }
    }

    /// A length that does not fit in the space before it must be rejected instead of being subtracted
    /// from the position, which puts the read outside the data.
    @Test
    public void testContentLengthPastStartOfDataThrows() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(new byte[8]);
            OnDiskBaseTrieWriter.writeReversedVint(out, 1 << 20);
            long pos = out.position();

            OnDiskCursor<Void> cursor = cursorOver(out);
            assertThrows(UncheckedIOException.class, () -> cursor.readContentAtPos(pos));
        }
    }

    /// A corrupt leading byte gives the longest encoding, whose value uses all 64 bits and can be
    /// negative.
    @Test
    public void testCorruptLengthByteThrows() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            byte[] allOnes = new byte[9];
            Arrays.fill(allOnes, (byte) 0xFF);
            out.write(allOnes);
            long pos = out.position();

            OnDiskCursor<Void> cursor = cursorOver(out);
            assertThrows(UncheckedIOException.class, () -> cursor.readContentAtPos(pos));
        }
    }

    /// There is no byte to read a length from when the position is at or past the end of the data.
    @Test
    public void testLengthPastEndOfDataThrows() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(new byte[8]);
            OnDiskCursor<Void> cursor = cursorOver(out);
            long pastEnd = cursor.rebufferer.fileLength() + 1;
            assertThrows(UncheckedIOException.class, () -> cursor.readVIntLength(pastEnd));
        }
    }

    /// The bytes handed to the payload deserializer can be corrupt, and the deserializer is often the
    /// first thing that can tell -- a content type tag it does not know, for instance. Its rejection has
    /// to reach the caller as the same kind of failure the length checks above produce: a commit-log
    /// replay classifies the error it gets for a record, and an Error is not something it can classify.
    @Test
    public void testDeserializerFailureIsReportedAsIOException() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // The root leaf that cursorOver appends carries content, which the cursor reads as it is
            // constructed. The deserializer below rejects it the way one that meets a tag it does not
            // know does.
            UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
                                                       () -> cursorOver(out, (rdr, length) -> {
                                                           throw new IOException("Unknown content type tag: 18");
                                                       }));
            assertEquals("Unknown content type tag: 18", thrown.getCause().getMessage());
        }
    }

    /// Code `11111xxx` is reserved. A reader that meets it must fail as on corruption, not dereference nothing.
    @Test
    public void testReservedNodeCodeIsRejected() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(new byte[8]);
            out.writeByte(0b11111000);
            ByteBuffer buffer = out.asNewBuffer();
            assertThrows(UncheckedIOException.class,
                         () -> new OnDiskCursor<Void>((rdr, length) -> null,
                                                      new ByteBufferRebufferer(buffer),
                                                      VERSION,
                                                      Direction.FORWARD,
                                                      true,
                                                      false,
                                                      buffer.limit()));
        }
    }

    /// A child pointer must point before its parent. A dense node with eight-byte pointers can hold any delta, including
    /// one that makes the node its own child; following it would walk the same node forever.
    @Test(timeout = 60_000)
    public void testSelfPointingChildIsRejected()
    {
        ByteBuffer buffer = selfPointingDenseNode();
        OnDiskCursor<Void> cursor = new OnDiskCursor<>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                       Direction.FORWARD, false, false, buffer.limit());
        assertThrows(UncheckedIOException.class, cursor::advance);
    }

    /// A range tail descends to the nearest content to work out the deletions at its root, along the same pointers. A
    /// tail taken from a closed cursor reads with a buffer of its own, which it must give back when it reports the
    /// corruption.
    @Test(timeout = 60_000)
    public void testSelfPointingChildIsRejectedOnRangeTail()
    {
        ByteBuffer buffer = selfPointingDenseNode();
        TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
        OnDiskRangeCursor<TestRangeState> cursor = new OnDiskRangeCursor<>(TrieUtil.RANGE_SERDE, factory,
                                                                           VERSION, Direction.FORWARD, buffer.limit());
        cursor.close();
        assertEquals(0, factory.outstanding);
        assertThrows(UncheckedIOException.class, () -> cursor.tailCursor(Direction.FORWARD));
        assertEquals(0, factory.outstanding);
    }

    /// The same descent, for the state of a closed range cursor: a skip to "a", where there is no content, leaves the
    /// state to be worked out from the nearest content below, and [RangeCursor#precedingStateCursor] asks for it after
    /// [Cursor#close]. The read takes a buffer of its own, which it must give back when it reports the corruption.
    @Test(timeout = 60_000)
    public void testSelfPointingChildIsRejectedForPrecedingStateOfClosedRangeCursor()
    {
        ByteBuffer node = selfPointingDenseNode();
        ByteBuffer buffer = ByteBuffer.allocate(node.limit() + 2);
        buffer.put(node);
        buffer.put((byte) 'a');
        buffer.put((byte) OnDiskWriteNodeType.CHAIN.bits);     // the root, a chain to the node with "a"
        buffer.flip();
        TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
        OnDiskRangeCursor<TestRangeState> cursor = new OnDiskRangeCursor<>(TrieUtil.RANGE_SERDE, factory,
                                                                           VERSION, Direction.FORWARD, buffer.limit());
        cursor.skipTo(Cursor.encode(1, 'a', Direction.FORWARD));
        cursor.close();
        assertEquals(0, factory.outstanding);
        assertThrows(UncheckedIOException.class, () -> cursor.precedingStateCursor(Direction.FORWARD));
        assertEquals(0, factory.outstanding);
    }

    /// A move that fails on corrupt bytes can leave a range cursor part way: here a skip to the return path of "b" has
    /// read the content on the way down into "b" when the content on the way back up is rejected, and the state the
    /// cursor would work out from either is not set. Closing it must still give the buffer back without failing, and
    /// a tail asked for afterwards must report the corruption.
    @Test
    public void testRangeCursorClosesAfterAFailedSkip() throws IOException
    {
        ByteComparable branchKey = TrieUtil.directComparable("b");
        RangeTrie<TestRangeState> trie = RangeTrie.branch(branchKey, VERSION, new TestRangeState(branchKey, false, 1, 1));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, true, TrieUtil.RANGE_SERDE, out);
            ByteBuffer buffer = out.asNewBuffer();
            int[] reads = new int[1];
            OnDiskCursor.DataDeserializer<TestRangeState> deserializer = (rdr, length) -> {
                if (++reads[0] > 1)
                    throw new IOException("Corrupt content");
                return TrieUtil.RANGE_SERDE.deserialize(rdr, length);
            };
            TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
            OnDiskRangeCursor<TestRangeState> cursor = new OnDiskRangeCursor<>(deserializer, factory,
                                                                               VERSION, Direction.FORWARD, buffer.limit());
            long target = Cursor.encode(1, 'b', Direction.FORWARD) | Cursor.ON_RETURN_PATH_BIT;
            assertThrows(UncheckedIOException.class, () -> cursor.skipTo(target));
            assertEquals(2, reads[0]);
            cursor.close();
            assertEquals(0, factory.outstanding);
            assertThrows(UncheckedIOException.class, () -> cursor.tailCursor(Direction.FORWARD));
        }
    }

    /// An advance, unlike a skip, starts from a known state, and one that fails part way must not leave that state
    /// to be reported for wherever the cursor stopped.
    @Test
    public void testRangeCursorStateAfterAFailedAdvance() throws IOException
    {
        testRangeCursorStateAfterAFailedMove(cursor -> cursor.advance());
        testRangeCursorStateAfterAFailedMove(cursor -> cursor.advanceMultiple(null));
    }

    private static void testRangeCursorStateAfterAFailedMove(ToLongFunction<OnDiskRangeCursor<TestRangeState>> move) throws IOException
    {
        RangeTrie<TestRangeState> trie = TestRangeState.fromList(TestRangeState.verify(Arrays.asList(
            new TestRangeState(TrieUtil.directComparable("a"), -1, 1),
            new TestRangeState(TrieUtil.directComparable("c"), 1, -1))));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, true, TrieUtil.RANGE_SERDE, out);
            ByteBuffer buffer = out.asNewBuffer();
            int[] reads = new int[1];
            OnDiskCursor.DataDeserializer<TestRangeState> deserializer = (rdr, length) -> {
                if (++reads[0] > 1)
                    throw new IOException("Corrupt content");
                return TrieUtil.RANGE_SERDE.deserialize(rdr, length);
            };
            TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
            OnDiskRangeCursor<TestRangeState> cursor = new OnDiskRangeCursor<>(deserializer, factory,
                                                                               VERSION, Direction.FORWARD, buffer.limit());
            move.applyAsLong(cursor);     // to "a", whose range is active until the move to "c"
            assertNotNull(cursor.state());
            assertThrows(UncheckedIOException.class, () -> move.applyAsLong(cursor));
            assertEquals(2, reads[0]);
            assertThrows(UncheckedIOException.class, cursor::state);
            cursor.close();
            assertEquals(0, factory.outstanding);
            assertThrows(UncheckedIOException.class, cursor::state);
            assertThrows(UncheckedIOException.class, () -> cursor.tailCursor(Direction.FORWARD));
        }
    }

    /// Closing a range cursor only gives its buffer back. Here a skip has left the state at the cursor's position to be
    /// worked out from the nearest content, and close must not read it on the chance that the state or a tail is asked
    /// for afterwards. Those read the file afresh when they are asked for, each with a buffer of its own that it gives
    /// back in turn.
    @Test
    public void testRangeCursorCloseOnlyReleases() throws IOException
    {
        RangeTrie<TestRangeState> trie = TestRangeState.fromList(TestRangeState.verify(Arrays.asList(
            new TestRangeState(TrieUtil.directComparable("a"), -1, 1),
            new TestRangeState(TrieUtil.directComparable("ba"), 1, 2),
            new TestRangeState(TrieUtil.directComparable("bc"), 2, -1))));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, true, TrieUtil.RANGE_SERDE, out);
            ByteBuffer buffer = out.asNewBuffer();
            int[] reads = new int[1];
            OnDiskCursor.DataDeserializer<TestRangeState> deserializer = (rdr, length) -> {
                ++reads[0];
                return TrieUtil.RANGE_SERDE.deserialize(rdr, length);
            };
            TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
            OnDiskRangeCursor<TestRangeState> cursor = new OnDiskRangeCursor<>(deserializer, factory,
                                                                               VERSION, Direction.FORWARD, buffer.limit());
            RangeCursor<TestRangeState> expected = trie.cursor(Direction.FORWARD);
            long target = Cursor.encode(1, 'b', Direction.FORWARD);   // "b": inside the range "a" opens, no content
            cursor.skipTo(target);
            expected.skipTo(target);

            int readsBeforeClose = reads[0];
            cursor.close();
            assertEquals("Close must not read", readsBeforeClose, reads[0]);
            assertEquals(0, factory.outstanding);

            try (RangeCursor<TestRangeState> expectedPreceding = expected.precedingStateCursor(Direction.FORWARD);
                 RangeCursor<TestRangeState> actualPreceding = cursor.precedingStateCursor(Direction.FORWARD))
            {
                assertEquals(0, factory.outstanding);
                TrieUtil.assertCursorWalksEqual(expectedPreceding, actualPreceding);
            }
            for (Direction direction : Direction.values())
            {
                try (RangeCursor<TestRangeState> expectedTail = expected.tailCursor(direction);
                     RangeCursor<TestRangeState> actualTail = cursor.tailCursor(direction))
                {
                    assertEquals(1, factory.outstanding);
                    TrieUtil.assertCursorWalksEqual(expectedTail, actualTail);
                }
                assertEquals(0, factory.outstanding);
            }
        }
    }

    /// A closed range cursor that is asked for its state reads the file with a reader of its own; it is not given a
    /// rebufferer again for the read, and does not store the state it works out, but reads it again when asked again.
    /// The tries made from a closed cursor make all their cursors from it, and could do so from more than one thread at
    /// a time.
    @Test
    public void testClosedRangeCursorIsNotChangedByItsState() throws IOException
    {
        RangeTrie<TestRangeState> trie = TestRangeState.fromList(TestRangeState.verify(Arrays.asList(
            new TestRangeState(TrieUtil.directComparable("a"), -1, 1),
            new TestRangeState(TrieUtil.directComparable("ba"), 1, 2),
            new TestRangeState(TrieUtil.directComparable("bc"), 2, -1))));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, true, TrieUtil.RANGE_SERDE, out);
            ByteBuffer buffer = out.asNewBuffer();
            AtomicReference<OnDiskRangeCursor<TestRangeState>> closed = new AtomicReference<>();
            List<Rebufferer> closedCursorRebufferers = new ArrayList<>();
            OnDiskCursor.DataDeserializer<TestRangeState> deserializer = (rdr, length) -> {
                if (closed.get() != null)
                    closedCursorRebufferers.add(closed.get().rebufferer);
                return TrieUtil.RANGE_SERDE.deserialize(rdr, length);
            };
            TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
            OnDiskRangeCursor<TestRangeState> cursor = new OnDiskRangeCursor<>(deserializer, factory,
                                                                               VERSION, Direction.FORWARD, buffer.limit());
            cursor.skipTo(Cursor.encode(1, 'b', Direction.FORWARD));   // "b": no content, the state is not known
            cursor.close();
            closed.set(cursor);

            for (int i = 0; i < 2; ++i)
            {
                try (RangeCursor<TestRangeState> preceding = cursor.precedingStateCursor(Direction.FORWARD))
                {
                    assertNotNull(preceding);
                }
                assertFalse("state stored by a closed cursor", cursor.activeIsSet);
                assertNull(cursor.activeRange);
            }
            assertEquals("the nearest content, read each time while the cursor stayed closed",
                         Arrays.asList(null, null), closedCursorRebufferers);
            assertEquals(0, factory.outstanding);
        }
    }

    /// A dense node (code `11101111`: eight bytes per pointer) whose child for transition 0x00 is the node itself and
    /// which has no other children.
    private static ByteBuffer selfPointingDenseNode()
    {
        int size = 256 * 8 + 1;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < 256 * 8; ++i)
            buffer.put(i, (byte) 0xFF);     // absent
        buffer.put(size - 1, (byte) (OnDiskWriteNodeType.DENSE.bits | 7));
        long postCodePos = size - 1;
        long base = postCodePos - 256 * 8;
        long delta = base - size;           // child = base - delta = size, i.e. this node
        // Pointers are stored reversed, the one for transition 0x00 in the eight bytes just before the code.
        for (int i = 0; i < 8; ++i)
            buffer.put((int) postCodePos - 8 + i, (byte) (delta >>> (8 * i)));
        return buffer;
    }

    /// A leaf's length is in its code; one longer than the data before it would place the content before the start.
    @Test
    public void testLeafLongerThanTheDataIsRejected()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 0, 0, (byte) (OnDiskWriteNodeType.LEAF.bits | 5) });
        assertThrows(UncheckedIOException.class,
                     () -> new OnDiskCursor<Void>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                  Direction.FORWARD, false, false, buffer.limit()));
    }

    /// A prefix code with neither content flag is never written; the reader only uses it for its own backtrack entries.
    @Test
    public void testPrefixWithoutContentIsRejected()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ (byte) OnDiskWriteNodeType.LEAF.bits,
                                                        (byte) (OnDiskWriteNodeType.PREFIX.bits | OnDiskWriteNodeType.PREFIX_HAS_CHILD) });
        assertThrows(UncheckedIOException.class,
                     () -> new OnDiskCursor<Void>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                  Direction.FORWARD, false, false, buffer.limit()));
    }

    /// A range tail presents its root's ascent content through a backtrack entry that reads no bytes. A prefix whose
    /// descent content starts the data and which claims ascent content as well asks for that content at position 0,
    /// which must be rejected, not answered with the tail's root content.
    ///
    /// The root here closes on its return path the range that the corrupt prefix below it opens, so a tail taken at the
    /// root presents that content on its own return path, through such an entry.
    @Test
    public void testRangeTailReadAtStartOfDataIsRejected()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 0,                                          // descent content
                                                        1,                                          // its length
                                                        (byte) (OnDiskWriteNodeType.PREFIX.bits
                                                                | OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT
                                                                | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT),
                                                        'a',
                                                        (byte) OnDiskWriteNodeType.CHAIN.bits,      // child "a"
                                                        0,                                          // root's ascent content
                                                        1,                                          // its length
                                                        (byte) (OnDiskWriteNodeType.PREFIX.bits
                                                                | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT
                                                                | OnDiskWriteNodeType.PREFIX_HAS_CHILD) }); // root
        TestRangeState content = new TestRangeState(TrieUtil.directComparable("a"), -1, 1);
        TestRangeState rootAscentContent = new TestRangeState(ByteComparable.EMPTY, 1, -1);
        OnDiskCursor.DataDeserializer<TestRangeState> deserializer =
            (rdr, length) -> OnDiskCursor.position(rdr) == 0 ? content : rootAscentContent;
        try (OnDiskRangeCursor<TestRangeState> parent = new OnDiskRangeCursor<>(deserializer, new ByteBufferRebufferer(buffer),
                                                                                VERSION, Direction.FORWARD, buffer.limit());
             RangeCursor<TestRangeState> cursor = parent.tailCursor(Direction.FORWARD))
        {
            cursor.advance();
            assertEquals(content, cursor.content());
            assertThrows(UncheckedIOException.class, cursor::advance);
        }
    }

    /// A prefix with a child whose content starts the data has its child end at position 0, where no node can end.
    /// Rejecting it must leave the cursor reading the file: the position before the start of the data is the one at
    /// which a range tail presents its root's return-path content, and a move after the failure must not present that
    /// content at the prefix.
    @Test
    public void testRangeTailPrefixWithChildAtStartOfDataIsRejected()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 0,                                          // descent content
                                                        1,                                          // its length
                                                        (byte) (OnDiskWriteNodeType.PREFIX.bits
                                                                | OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT
                                                                | OnDiskWriteNodeType.PREFIX_HAS_CHILD),
                                                        'a',
                                                        (byte) OnDiskWriteNodeType.CHAIN.bits,      // child "a"
                                                        0,                                          // root's ascent content
                                                        1,                                          // its length
                                                        0,                                          // root's descent content
                                                        1,                                          // its length
                                                        (byte) (OnDiskWriteNodeType.PREFIX.bits
                                                                | OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT
                                                                | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT
                                                                | OnDiskWriteNodeType.PREFIX_HAS_CHILD) }); // root
        TestRangeState content = new TestRangeState(TrieUtil.directComparable("a"), -1, 2);
        TestRangeState rootAscentContent = new TestRangeState(ByteComparable.EMPTY, 1, -1);
        TestRangeState rootDescentContent = new TestRangeState(ByteComparable.EMPTY, -1, 1);
        OnDiskCursor.DataDeserializer<TestRangeState> deserializer = (rdr, length) -> {
            long position = OnDiskCursor.position(rdr);
            return position == 0 ? content : position == 5 ? rootAscentContent : rootDescentContent;
        };
        try (OnDiskRangeCursor<TestRangeState> parent = new OnDiskRangeCursor<>(deserializer, new ByteBufferRebufferer(buffer),
                                                                                VERSION, Direction.FORWARD, buffer.limit());
             RangeCursor<TestRangeState> cursor = parent.tailCursor(Direction.FORWARD))
        {
            assertThrows(UncheckedIOException.class, cursor::advance);
            cursor.advance();
            assertNotSame(rootAscentContent, cursor.content());
        }
    }

    /// The deletion branch pointer is a delta back from its slot. Eight bytes of it can read back negative, which would
    /// put the branch after the node that refers to it.
    @Test
    public void testNegativeAlternateBranchPointerIsRejected() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeByte(OnDiskWriteNodeType.LEAF.bits);       // a branch root, at position 1
            OnDiskBaseTrieWriter.writeReversedSized(out, -1L, 8);   // the slot: delta -1
            OnDiskBaseTrieWriter.writeReversedVint(out, 8);
            out.writeByte(OnDiskWriteNodeType.PREFIX.bits | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT);
            ByteBuffer buffer = out.asNewBuffer();
            OnDiskCursor<Void> cursor = new OnDiskCursor<>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                           Direction.FORWARD, false, true, buffer.limit());
            assertThrows(UncheckedIOException.class, cursor::alternateBranch);
        }
    }

    /// A node without children has its deletion branch immediately before it, and the delta 0 to it is written as an
    /// empty slot, which reads back as that branch.
    @Test
    public void testEmptyAlternateBranchSlotIsDeltaZero() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeByte(OnDiskWriteNodeType.LEAF.bits);       // a branch root, at position 1
            OnDiskBaseTrieWriter.writeReversedVint(out, 0);     // the slot: empty
            out.writeByte(OnDiskWriteNodeType.PREFIX.bits | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT);
            ByteBuffer buffer = out.asNewBuffer();
            OnDiskCursor<Void> cursor = new OnDiskCursor<>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                           Direction.FORWARD, false, true, buffer.limit());
            assertEquals(1, cursor.alternateBranch());
        }
    }

    /// The delta 0 of a node without children written as one byte, `00`, as it was before the empty slot, reads back
    /// the same.
    @Test
    public void testOneByteZeroAlternateBranchSlotIsDeltaZero() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeByte(OnDiskWriteNodeType.LEAF.bits);       // a branch root, at position 1
            OnDiskBaseTrieWriter.writeReversedSized(out, 0, 1); // the slot: the delta 0 in one byte
            OnDiskBaseTrieWriter.writeReversedVint(out, 1);
            out.writeByte(OnDiskWriteNodeType.PREFIX.bits | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT);
            ByteBuffer buffer = out.asNewBuffer();
            OnDiskCursor<Void> cursor = new OnDiskCursor<>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                           Direction.FORWARD, false, true, buffer.limit());
            assertEquals(1, cursor.alternateBranch());
        }
    }

    /// Only a node without children has its deletion branch immediately before it: a node's children are written
    /// between its branch and the node. The delta 0 on a node with children, as an empty slot or as the one byte `00`,
    /// would read its child as its branch.
    @Test
    public void testZeroAlternateBranchDeltaWithChildIsRejected() throws IOException
    {
        for (int slotLength = 0; slotLength <= 1; ++slotLength)
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                out.writeByte(OnDiskWriteNodeType.LEAF.bits);       // the child, at position 1
                if (slotLength > 0)
                    OnDiskBaseTrieWriter.writeReversedSized(out, 0, slotLength);   // the slot: the delta 0
                OnDiskBaseTrieWriter.writeReversedVint(out, slotLength);
                out.writeByte(OnDiskWriteNodeType.PREFIX.bits | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT
                              | OnDiskWriteNodeType.PREFIX_HAS_CHILD);
                ByteBuffer buffer = out.asNewBuffer();
                OnDiskCursor<Void> cursor = new OnDiskCursor<>((rdr, length) -> null, new ByteBufferRebufferer(buffer),
                                                               VERSION, Direction.FORWARD, false, true, buffer.limit());
                assertThrows("slot of " + slotLength + " bytes", UncheckedIOException.class, cursor::alternateBranch);
            }
        }
    }

    /// An empty slot at the start of the data would put the branch root at 0, where no branch can end.
    @Test
    public void testEmptyAlternateBranchSlotAtStartIsRejected() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskBaseTrieWriter.writeReversedVint(out, 0);     // the slot: empty, delta 0 back to position 0
            out.writeByte(OnDiskWriteNodeType.PREFIX.bits | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT);
            ByteBuffer buffer = out.asNewBuffer();
            OnDiskCursor<Void> cursor = new OnDiskCursor<>((rdr, length) -> null, new ByteBufferRebufferer(buffer), VERSION,
                                                           Direction.FORWARD, false, true, buffer.limit());
            assertThrows(UncheckedIOException.class, cursor::alternateBranch);
        }
    }

    /// A rebufferer can hand out data past the file's length: chunks are read in full, and cached chunks and mapped
    /// regions are shared by the readers of a file, which can have different lengths. A root past the length must be
    /// rejected even though the buffer holds a valid node there.
    @Test
    public void testRootPastTheFileLengthIsRejected()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ (byte) OnDiskWriteNodeType.LEAF.bits, (byte) OnDiskWriteNodeType.LEAF.bits });
        ByteBufferRebufferer rebufferer = rebuffererWithLength(buffer, 1);
        new OnDiskCursor<Void>((rdr, length) -> null, rebufferer, VERSION, Direction.FORWARD, false, false, 1).close();
        assertThrows(UncheckedIOException.class,
                     () -> new OnDiskCursor<Void>((rdr, length) -> null, rebufferer, VERSION,
                                                  Direction.FORWARD, false, false, 2));
    }

    /// The same for the deserializer's stream, which reads through the buffer the cursor holds: a deserializer that
    /// reads past its content -- as one given corrupt bytes can -- must not be handed the data past the file's length.
    @Test
    public void testContentReadPastTheFileLengthIsRejected()
    {
        // A leaf with one byte of content; the bytes that follow the leaf are past the file's length.
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 0, (byte) (OnDiskWriteNodeType.LEAF.bits | 1), 0, 0 });
        ByteBufferRebufferer rebufferer = rebuffererWithLength(buffer, 2);
        assertThrows(UncheckedIOException.class,
                     () -> new OnDiskCursor<Void>((rdr, length) -> {
                                                      rdr.readFully(new byte[3]);
                                                      return null;
                                                  },
                                                  rebufferer, VERSION, Direction.FORWARD, false, false, 2));
    }

    private static ByteBufferRebufferer rebuffererWithLength(ByteBuffer buffer, long fileLength)
    {
        return new ByteBufferRebufferer(buffer)
        {
            @Override
            public long fileLength()
            {
                return fileLength;
            }
        };
    }

    /// A trie in a single buffer is read with a single rebuffer. The deserializer's stream shares the cursor's buffer
    /// and moves its position as it reads content, which must not make the cursor think the data it still holds is gone.
    @Test
    public void testSingleBufferIsRebufferedOnce() throws IOException, TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLived(VERSION);
        java.util.Random rand = new java.util.Random(1);
        for (int i = 0; i < 2000; ++i)
        {
            StringBuilder key = new StringBuilder();
            for (int j = 0; j < 6; ++j)
                key.append((char) ('a' + rand.nextInt(26)));
            trie.putRecursive(TrieUtil.directComparable(key.toString()), key.toString(), (x, y) -> y);
        }
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, TrieUtil.STRING_SERDE, out);
            ByteBuffer buffer = out.asNewBuffer();
            int[] rebuffers = new int[1];
            ByteBufferRebufferer rebufferer = new ByteBufferRebufferer(buffer)
            {
                @Override
                public BufferHolder rebuffer(long position)
                {
                    ++rebuffers[0];
                    return super.rebuffer(position);
                }
            };
            OnDiskTrie<String> read = new OnDiskTrie<>(rebufferer, TrieUtil.STRING_SERDE, VERSION, false, buffer.limit());
            assertEquals(Iterables.size(trie.values()), Iterables.size(read.values()));
            assertEquals(1, rebuffers[0]);
        }
    }

    /// The deserializer's stream reads through the cursor's buffer. Closing the cursor gives the buffer back, and the
    /// stream must not keep it reachable afterwards.
    @Test
    public void testClosedCursorDoesNotKeepItsBuffer() throws IOException, ReflectiveOperationException, TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLived(VERSION);
        trie.putRecursive(TrieUtil.directComparable("a"), "a", (x, y) -> y);
        trie.putRecursive(TrieUtil.directComparable("b"), "b", (x, y) -> y);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, TrieUtil.STRING_SERDE, out);
            ByteBuffer buffer = out.asNewBuffer();
            OnDiskCursor<String> cursor = new OnDiskCursor<>(TrieUtil.STRING_SERDE, new ByteBufferRebufferer(buffer), VERSION,
                                                             Direction.FORWARD, false, false, buffer.limit());
            long position = cursor.advance();
            while (!Cursor.isExhausted(position) && cursor.content() == null)
                position = cursor.advance();
            assertEquals("a", cursor.content());

            Field streamBuffer = ReflectionUtils.getField(RebufferingInputStream.class, "buffer");
            streamBuffer.setAccessible(true);
            assertEquals(buffer.capacity(), ((ByteBuffer) streamBuffer.get(cursor.rdr)).capacity());
            cursor.close();
            assertEquals(0, ((ByteBuffer) streamBuffer.get(cursor.rdr)).capacity());
        }
    }

    /// A cursor that has run out of positions must keep reporting the exhausted position from
    /// [Cursor#encodedPosition], not the last live one it was on.
    ///
    /// The round-trip tests all drive a cursor by the position `advance` returns and stop as soon as that is
    /// exhausted, so none of them asks an exhausted cursor where it is. The merge cursors do: they hold two or more
    /// sources, advance the one that is behind, and then read the position of each to decide which to take next. A
    /// source that answers with a live position after it has ended is advanced again and again, and the merge
    /// republishes the content it ended on -- so a trie read back off disk silently gains rows when it is merged with
    /// anything, and the merge only terminates because the other source does.
    @Test
    public void testExhaustedCursorReportsTheExhaustedPosition() throws IOException, TrieSpaceExhaustedException
    {
        InMemoryTrie<String> plain = InMemoryTrie.shortLived(VERSION);
        plain.putRecursive(TrieUtil.directComparable("abc"), "one", (x, y) -> y);
        plain.putRecursive(TrieUtil.directComparable("abd"), "two", (x, y) -> y);

        try (FileTries.Plain<String> read = TrieUtil.onDiskRoundtripStrings(plain, false))
        {
            for (Direction direction : Direction.values())
            {
                assertExhaustedPositionReported("advanced to the end", read.cursor(direction), Cursor::advance);
                assertExhaustedPositionReported("advanceMultiple to the end",
                                                read.cursor(direction),
                                                c -> c.advanceMultiple(null));
                // Skipping past everything at the top level leaves the cursor exhausted through the ascent path
                // rather than through a node running out of children.
                assertExhaustedPositionReported("skipped past the end",
                                                read.cursor(direction),
                                                c -> c.skipTo(Cursor.encode(1, direction.select(0xFF, 0x00), direction)));
                // A tail cursor ends the same way and is what tailTrie hands to a merge.
                try (Cursor<String> parent = read.cursor(direction))
                {
                    assertExhaustedPositionReported("tail advanced to the end",
                                                    parent.tailCursor(direction),
                                                    Cursor::advance);
                }
            }
        }
    }

    /// Closes the cursor.
    private static void assertExhaustedPositionReported(String message,
                                                       Cursor<String> cursor,
                                                       ToLongFunction<Cursor<String>> step)
    {
        try (cursor)
        {
            long position = cursor.encodedPosition();
            while (!Cursor.isExhausted(position))
                position = step.applyAsLong(cursor);

            assertEquals(message,
                         Cursor.toString(position),
                         Cursor.toString(cursor.encodedPosition()));
        }
    }

    /// A cursor over the given bytes. A leaf node with no content is appended as the root so that the
    /// cursor can be constructed; the tests read positions within the bytes before it directly.
    private static OnDiskCursor<Void> cursorOver(DataOutputBuffer out) throws IOException
    {
        return cursorOver(out, (rdr, length) -> null);
    }

    private static OnDiskCursor<Void> cursorOver(DataOutputBuffer out, OnDiskCursor.DataDeserializer<Void> deserializer) throws IOException
    {
        out.writeByte(OnDiskWriteNodeType.LEAF.bits);
        ByteBuffer buffer = out.asNewBuffer();
        return new OnDiskCursor<>(deserializer,
                                  new ByteBufferRebufferer(buffer),
                                  VERSION,
                                  Direction.FORWARD,
                                  true,
                                  false,
                                  buffer.limit());
    }
}
