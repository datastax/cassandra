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
import java.util.Arrays;
import java.util.function.ToLongFunction;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.ReflectionUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/// Covers the back-to-front integer decoding of [OnDiskCursor], which the round-trip tests only reach
/// for the small content sizes they produce, and its behaviour on lengths that a corrupt commit-log
/// record or message payload can present.
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
                OnDiskTrieWriter.writeReversedVint(out, VALUES[i]);
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
            OnDiskTrieWriter.writeReversedVint(out, 1 << 20);
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

    /// Closing a range cursor descends to the nearest content to work out the deletions at its position, along the same
    /// pointers. The close must still give the buffer back, and a tail asked for afterwards must report the corruption.
    @Test(timeout = 60_000)
    public void testSelfPointingChildIsRejectedOnRangeClose()
    {
        ByteBuffer buffer = selfPointingDenseNode();
        TrieUtil.CountingRebuffererFactory factory = new TrieUtil.CountingRebuffererFactory(new ByteBufferRebufferer(buffer));
        OnDiskCursor.Range<TestRangeState> cursor = new OnDiskCursor.Range<>(TrieUtil.RANGE_SERDE, factory,
                                                                             VERSION, Direction.FORWARD, buffer.limit());
        cursor.close();
        assertEquals(0, factory.outstanding);
        assertThrows(UncheckedIOException.class, () -> cursor.tailCursor(Direction.FORWARD));
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
            OnDiskCursor.Range<TestRangeState> cursor = new OnDiskCursor.Range<>(deserializer, factory,
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

    private static void testRangeCursorStateAfterAFailedMove(ToLongFunction<OnDiskCursor.Range<TestRangeState>> move) throws IOException
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
            OnDiskCursor.Range<TestRangeState> cursor = new OnDiskCursor.Range<>(deserializer, factory,
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
    @Test
    public void testRangeTailReadAtStartOfDataIsRejected()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 0,                                          // descent content
                                                        1,                                          // its length
                                                        (byte) (OnDiskWriteNodeType.PREFIX.bits
                                                                | OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT
                                                                | OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT),
                                                        'a',
                                                        (byte) OnDiskWriteNodeType.CHAIN.bits });   // root, child "a"
        TestRangeState content = new TestRangeState(TrieUtil.directComparable("a"), -1, 1);
        TestRangeState rootAscentContent = new TestRangeState(ByteComparable.EMPTY, 1, -1);
        OnDiskCursor.Range<TestRangeState> cursor = new OnDiskCursor.Range<>((rdr, length) -> content,
                                                                             new ByteBufferRebufferer(buffer), VERSION,
                                                                             Direction.FORWARD, buffer.limit() - 1,
                                                                             OnDiskWriteNodeType.CHAIN.bits,
                                                                             null, rootAscentContent);
        cursor.advance();
        assertEquals(content, cursor.content());
        assertThrows(UncheckedIOException.class, cursor::advance);
    }

    /// The deletion branch pointer is a delta back from its slot. Eight bytes of it can read back negative, which would
    /// put the branch after the node that refers to it.
    @Test
    public void testNegativeAlternateBranchPointerIsRejected() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeByte(OnDiskWriteNodeType.LEAF.bits);       // a branch root, at position 1
            OnDiskTrieWriter.writeReversedSized(out, -1L, 8);   // the slot: delta -1
            OnDiskTrieWriter.writeReversedVint(out, 8);
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
            OnDiskTrie<String> read = new OnDiskTrie<>(rebufferer, TrieUtil.STRING_SERDE, VERSION, false, buffer.limit(), null);
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
