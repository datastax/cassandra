/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/// Tests of the encoders in [OnDiskWriteNodeType] taken by themselves: that the type selected for a set of children
/// writes them so that they are read back, that the sizes the types announce are the sizes of what they write, and
/// that a selection holds for the positions it is said to hold for. [OnDiskNodeWriterPageAware] relies on the last two
/// to know the size of a branch before writing it, and to know when a size it has calculated has to be calculated
/// again.
public class OnDiskWriteNodeTypeTest
{
    private static final long SEED = 0x6e6f646574797065L;

    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    /// The content of the leaves that stand for the children of a node: the transition to the child, in as many bytes
    /// as the test gives it.
    private static final OnDiskCursor.DataDeserializer<Integer> CONTENT = (in, length) -> {
        int value = 0;
        for (int i = 0; i < length; ++i)
            value = (value << 8) | in.readUnsignedByte();
        return value;
    };

    private static void fill(DataOutputPlus out, long count) throws IOException
    {
        byte[] filler = new byte[(int) Math.min(count, 1 << 16)];
        Arrays.fill(filler, (byte) 0xFF);
        while (count > 0)
        {
            int length = (int) Math.min(count, filler.length);
            out.write(filler, 0, length);
            count -= length;
        }
    }

    /// Where the children of a node are in relation to it.
    private enum Placement
    {
        /// All children are written one after the other, the node directly after the last of them.
        ADJACENT,
        /// The same with a gap after the first child written, which makes it and the node far apart.
        FAR_FROM_OTHERS,
        /// A gap between the children and the node, which also has the first child of the node far from it.
        FAR_FROM_ALL,
        /// A gap after the first child written, and a smaller one between the children and the node.
        FAR_FROM_OTHERS_NEAR_FIRST
    }

    /// Children at various distances from each other and from the node that lists them, written with the type that
    /// is selected for them: the node takes the size its type gives, and is read as a node with these children.
    ///
    /// With the pointers one buffer can hold, i.e. of up to four bytes. A leaf with the transition as content stands
    /// for each child.
    @Test
    public void testChildrenAreReadBackAndSizesMatch() throws IOException
    {
        Random rand = new Random(SEED);
        Set<OnDiskWriteNodeType> typesSeen = new HashSet<>();
        int[] counts = { 1, 2, 3, 24, 25, 26, 100, 223, 224, 225, 255, 256 };
        // the gaps that need pointers of one more byte each
        long[] gaps = { 0, 300, 1 << 16, 1 << 24 };
        for (int count : counts)
            for (long gap : gaps)
                for (Placement placement : Placement.values())
                    // the smaller gaps that need pointers of each size
                    for (long nearGap : new long[]{ 1 + rand.nextInt(200), 300, 1 << 16 })
                    {
                        if (nearGap >= gap && nearGap > 200 || nearGap > 200 && placement != Placement.FAR_FROM_OTHERS_NEAR_FIRST)
                            continue;
                        // leaves of one byte to have many children within the reach of one-byte pointers
                        for (int contentBytes : gap == 0 ? new int[]{ 0, 2 } : new int[]{ 2 })
                            for (boolean folded : count == 1 ? new boolean[]{ false, true } : new boolean[]{ false })
                                typesSeen.add(writeAndReadChildren(rand, count, gap, nearGap, placement, contentBytes, folded));
                    }
        for (int i = 0; i < 300; ++i)
            typesSeen.add(writeAndReadChildren(rand,
                                               1 + rand.nextInt(256),
                                               gaps[rand.nextInt(3)],
                                               1 + rand.nextInt(200),
                                               Placement.values()[rand.nextInt(Placement.values().length)],
                                               rand.nextInt(3),
                                               false));

        // every type that pointers of up to four bytes can make
        assertTrue(typesSeen.contains(OnDiskWriteNodeType.CHAIN[0]));
        assertTrue(typesSeen.contains(OnDiskWriteNodeType.FOLDED_CHAIN[0]));
        for (int relay = 1; relay <= 4; ++relay)
        {
            String message = "Relay with pointers of " + relay + " bytes";
            assertTrue(message, typesSeen.contains(OnDiskWriteNodeType.CHAIN[relay]));
            assertTrue(message, typesSeen.contains(OnDiskWriteNodeType.FOLDED_CHAIN[relay]));
            // the other children are at least as far as the first here
            for (int pointer = relay; pointer <= 4; ++pointer)
            {
                String both = message + " and pointers of " + pointer + " bytes";
                assertTrue(both, typesSeen.contains(OnDiskWriteNodeType.SPARSE[relay * OnDiskWriteNodeType.SPARSES + pointer - 1]));
                assertTrue(both, typesSeen.contains(OnDiskWriteNodeType.BITMAP[relay * OnDiskWriteNodeType.BITMAPS + pointer - 1]));
            }
        }
        for (int pointer = 1; pointer <= 4; ++pointer)
        {
            String message = "Pointers of " + pointer + " bytes";
            assertTrue(message, typesSeen.contains(OnDiskWriteNodeType.SPARSE[pointer - 1]));
            assertTrue(message, typesSeen.contains(OnDiskWriteNodeType.BITMAP[pointer - 1]));
            assertTrue(message, typesSeen.contains(OnDiskWriteNodeType.DENSE[pointer - 1]));
        }
    }

    private static OnDiskWriteNodeType writeAndReadChildren(Random rand, int count, long gap, long nearGap, Placement placement, int contentBytes, boolean folded) throws IOException
    {
        // distinct transitions in descending order, which is the order in which the children are written
        List<Integer> all = new ArrayList<>();
        for (int t = 0; t < 256; ++t)
            all.add(t);
        Collections.shuffle(all, rand);
        int[] chosen = all.subList(0, count).stream().mapToInt(Integer::intValue).sorted().toArray();
        byte[] transitions = new byte[count];
        long[] positions = new long[count];
        OnDiskTrieWriter.DataSerializer<Integer> serializer = (out, value) -> {
            for (int i = contentBytes - 1; i >= 0; --i)
                out.writeByte(value >> (i * 8));
        };
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            fill(out, rand.nextInt(3) == 0 ? 0 : rand.nextInt(500));
            for (int c = 0; c < count; ++c)
            {
                transitions[c] = (byte) chosen[count - 1 - c];
                OnDiskWriteNodeType.writePayload(out, serializer, transitions[c] & 0xFF, null, 0, false);
                positions[c] = out.position();
                if (c == 0 && count > 1 && (placement == Placement.FAR_FROM_OTHERS || placement == Placement.FAR_FROM_OTHERS_NEAR_FIRST))
                    fill(out, gap);
            }
            // The last child written is the first of the node. What follows it separates it from the node.
            if (placement == Placement.FAR_FROM_ALL)
                fill(out, gap);
            else if (placement == Placement.FAR_FROM_OTHERS_NEAR_FIRST)
                fill(out, nearGap);

            long base = out.position();
            long furthest = Long.MAX_VALUE;
            for (int c = 0; c < count - 1; ++c)
                furthest = Math.min(furthest, positions[c]);
            long furthestDelta = base - furthest;
            long firstDelta = base - positions[count - 1];
            OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(furthestDelta, firstDelta, count, folded);
            String message = String.format("%d children %s with gaps %d and %d, content of %d bytes, first at %d, furthest at %d, folded %s, type %x",
                                           count, placement, gap, nearGap, contentBytes, firstDelta, furthestDelta, folded, type.bits);
            type.writeChildren(out, positions, transitions, count, base);
            long root = out.position();
            assertEquals(message, type.sizeChildren(count), root - base);

            // A dense node lists the first child like the others. All other types need it, or a relay to it, to end
            // where they start.
            boolean isDense = (type.bits & 0xF8) == OnDiskWriteNodeType.DENSE[0].bits;
            boolean hasRelay = (type.bits & 0xF8) == 0xF8;
            assertEquals(message, firstDelta > 0 && !isDense, hasRelay);
            // The smallest pointers that reach all children.
            if (!hasRelay && count > 1)
                assertEquals(message,
                             OnDiskBaseTrieWriter.bytesFor(isDense ? Math.max(firstDelta, furthestDelta) + 1 : furthestDelta),
                             (type.bits & (isDense || (type.bits & 0xF8) == OnDiskWriteNodeType.BITMAP[0].bits ? 7 : 3)) + 1);

            OnDiskTrie<Integer> read = new OnDiskTrie<>(new ByteBufferRebufferer(out.buffer()), CONTENT, VERSION, false, root);
            int contentMask = contentBytes == 0 ? 0 : 0xFF;
            List<String> expected = new ArrayList<>();
            for (int c = count - 1; c >= 0; --c)
                // a folded transition is not written with the node, which is thus read as its child
                expected.add((folded ? "" : String.format("%02x", transitions[c] & 0xFF)) + "=" + (transitions[c] & contentMask));
            try
            {
                assertEquals(message, expected, entries(read, Direction.FORWARD));
                Collections.reverse(expected);
                assertEquals(message, expected, entries(read, Direction.REVERSE));
            }
            catch (RuntimeException e)
            {
                throw new AssertionError(message, e);
            }
            return type;
        }
    }

    private static List<String> entries(Trie<Integer> trie, Direction direction)
    {
        List<String> entries = new ArrayList<>();
        trie.forEachEntry(direction, (key, value) -> {
            StringBuilder entry = new StringBuilder();
            for (byte b : key.asByteComparableArray(VERSION))
                entry.append(String.format("%02x", b));
            entries.add(entry.append('=').append(value).toString());
        });
        return entries;
    }

    private static final long[] DELTAS;

    static
    {
        // around every value at which a pointer needs another byte, where the selection changes
        List<Long> deltas = new ArrayList<>(List.of(0L, 1L, 2L, 3L, 5L, 17L, 100L));
        for (int bytes = 1; bytes < 8; ++bytes)
            for (long offset = -12; offset <= 3; ++offset)
                deltas.add((1L << (bytes * 8)) + offset);
        deltas.add(1L << 60);
        DELTAS = deltas.stream().mapToLong(Long::longValue).toArray();
    }

    private static long randomDelta(Random rand)
    {
        switch (rand.nextInt(3))
        {
            case 0:
                return DELTAS[rand.nextInt(DELTAS.length)];
            case 1:
                return rand.nextInt(70000);
            default:
                return rand.nextLong() >>> (4 + rand.nextInt(56));
        }
    }

    /// The node type for the given children when the position of the node is `shift` bytes later, so that the
    /// children that are already written are that much further away.
    private static OnDiskWriteNodeType selectShifted(int count, boolean folded,
                                                     long firstDelta, boolean firstIsWritten,
                                                     long furthestDelta, boolean furthestIsWritten,
                                                     long shift)
    {
        return OnDiskWriteNodeType.selectChildrenType(furthestIsWritten ? furthestDelta + shift : furthestDelta,
                                                      firstIsWritten ? firstDelta + shift : firstDelta,
                                                      count,
                                                      folded);
    }

    /// The selection of a node type is said to hold for a number of positions of the node, which the page-aware
    /// writer uses to tell when the size it has calculated for a branch is no longer valid. Within that many positions
    /// the selection must remain what it was, however the children that are yet to be written (whose distance from the
    /// node does not change) and the ones that are already written (which get further away) are divided; and it must
    /// not be shorter than necessary, i.e. it must end where the selection changes when all of them are written.
    @Test
    public void testSizeValidFor()
    {
        Random rand = new Random(SEED + 1);
        int[] counts = { 1, 2, 3, 24, 25, 26, 100, 223, 224, 225, 255, 256 };
        Set<OnDiskWriteNodeType> typesSeen = new HashSet<>();
        int limited = 0;
        for (int i = 0; i < 200000; ++i)
        {
            int count = counts[rand.nextInt(counts.length)];
            boolean folded = count == 1 && rand.nextBoolean();
            // A first child that is not written yet is written with the node, and thus adjacent to it.
            boolean firstIsWritten = rand.nextInt(3) != 0;
            long firstDelta = firstIsWritten ? rand.nextInt(4) == 0 ? 0 : randomDelta(rand) : 0;
            long furthestDelta = count == 1 ? 0 : randomDelta(rand);
            OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(furthestDelta, firstDelta, count, folded);
            typesSeen.add(type);
            long validFor = type.sizeValidFor(furthestDelta, firstDelta, firstIsWritten);
            String message = String.format("Round %d: %d children, first at %d %s, furthest at %d, folded %s, type %x valid for %d",
                                           i, count, firstDelta, firstIsWritten ? "written" : "not written", furthestDelta, folded, type.bits, validFor);
            assertTrue(message, validFor >= 1);

            long[] shifts = { 0, 1, 2, validFor / 2, validFor - 2, validFor - 1, (rand.nextLong() >>> 1) % validFor };
            for (long shift : shifts)
            {
                if (shift < 0 || shift >= validFor || shift > 1L << 61)
                    continue;
                for (boolean furthestIsWritten : new boolean[]{ false, true })
                {
                    OnDiskWriteNodeType shifted = selectShifted(count, folded, firstDelta, firstIsWritten, furthestDelta, furthestIsWritten, shift);
                    assertSame(message + " at shift " + shift + (furthestIsWritten ? " with all written" : " with only the first written"),
                               type, shifted);
                    assertEquals(message, type.sizeChildren(count), shifted.sizeChildren(count));
                }
            }

            // A type with a single child has no furthest child to speak of.
            boolean anyWritten = firstIsWritten || count > 1;
            if (validFor != Long.MAX_VALUE && anyWritten)
            {
                ++limited;
                OnDiskWriteNodeType after = selectShifted(count, folded, firstDelta, firstIsWritten, furthestDelta, true, validFor);
                assertNotSame(message + ": no change after " + validFor, type, after);
            }
        }
        assertTrue(limited > 50000);
        // all types, with relays of any size
        for (OnDiskWriteNodeType[] types : new OnDiskWriteNodeType[][]{ OnDiskWriteNodeType.CHAIN,
                                                                         OnDiskWriteNodeType.FOLDED_CHAIN,
                                                                         OnDiskWriteNodeType.SPARSE,
                                                                         OnDiskWriteNodeType.BITMAP,
                                                                         OnDiskWriteNodeType.DENSE })
            for (OnDiskWriteNodeType type : types)
                assertTrue(String.format("Type %x %s", type.bits, type.getClass().getSimpleName()), typesSeen.contains(type));
    }

    /// The same for the validity of a type whose first child is written with it, as asked for by a node that has
    /// other children written already: the first child stays where the type expects it.
    @Test
    public void testSizeValidForWithFirstChildNotWritten()
    {
        for (int count : new int[]{ 2, 25, 26, 224, 225, 256 })
            for (long furthestDelta : DELTAS)
            {
                OnDiskWriteNodeType type = OnDiskWriteNodeType.selectChildrenType(furthestDelta, 0, count, false);
                long validFor = type.sizeValidFor(furthestDelta, 0, false);
                String message = String.format("%d children, furthest at %d, type %x valid for %d", count, furthestDelta, type.bits, validFor);
                // a type without a relay
                assertTrue(message, (type.bits & 0xF8) != 0xF8);
                if (validFor != Long.MAX_VALUE)
                {
                    assertSame(message, type, OnDiskWriteNodeType.selectChildrenType(furthestDelta + validFor - 1, 0, count, false));
                    assertNotSame(message, type, OnDiskWriteNodeType.selectChildrenType(furthestDelta + validFor, 0, count, false));
                }
                else
                    // nothing wider to change to
                    assertEquals(message, 8, OnDiskBaseTrieWriter.bytesFor(furthestDelta + 1));
            }
    }

    /// Make sure the immediate version of `selectChildrenType` matches the four-argument one.
    @Test
    public void testSelectChildrenTypeVersionsMatch()
    {
        for (int count : new int[]{ 2, 25, 26, 224, 225, 256 })
            for (long furthestDelta : DELTAS)
            {
                OnDiskWriteNodeType typeExpected = OnDiskWriteNodeType.selectChildrenType(furthestDelta, 0, count, false);
                OnDiskWriteNodeType typeActual = OnDiskWriteNodeType.selectChildrenType(furthestDelta, count);
                assertSame(typeExpected, typeActual);
            }
    }

    /// A chain with a folded transition is the chain that begins with that transition.
    @Test
    public void testFoldedChainIsLongerChain() throws IOException
    {
        assertFalse(OnDiskWriteNodeType.isExpandableChain(null));
        for (int length = 1; length <= 200; ++length)
        {
            byte[] chain = new byte[length];
            for (int i = 0; i < length; ++i)
                chain[i] = (byte) (i + 1);
            byte[] longer = new byte[length + 1];
            longer[0] = (byte) 0xAA;
            System.arraycopy(chain, 0, longer, 1, length);

            try (DataOutputBuffer plain = new DataOutputBuffer();
                 DataOutputBuffer expected = new DataOutputBuffer();
                 DataOutputBuffer folded = new DataOutputBuffer())
            {
                OnDiskWriteNodeType.writeChain(plain, chain, 0, length);
                assertEquals("Chain of " + length, OnDiskWriteNodeType.sizeChain(length), plain.position());
                OnDiskWriteNodeType.writeChain(expected, longer, 0, length + 1);
                assertEquals("Chain of " + (length + 1), OnDiskWriteNodeType.sizeChain(length + 1), expected.position());

                // A chain that fills its last node cannot take the transition without another node, which is the
                // node that a parent that does not fold writes.
                boolean expandable = length % 64 != 0;
                assertEquals("Chain of " + length, expandable, OnDiskWriteNodeType.isExpandableChain(chain));
                if (!expandable)
                {
                    assertEquals(plain.position() + OnDiskWriteNodeType.CHAIN[0].sizeChildren(1), expected.position());
                    continue;
                }

                OnDiskWriteNodeType.writeChainFolded(folded, chain, 0, length, 0xAA);
                assertArrayEquals("Chain of " + length, expected.toByteArray(), folded.toByteArray());
                // which is what the page-aware writer takes the size of a folded chain to be
                assertEquals(OnDiskWriteNodeType.sizeChain(length) + 1, folded.position());
            }
        }
    }

    /// The size given for the payload of a node is the size of what is written for it, for all combinations of
    /// content, alternate branch and children.
    @Test
    public void testPayloadSizeMatchesWhatIsWritten() throws IOException
    {
        OnDiskTrieWriter.DataSerializer<byte[]> bytes = (out, value) -> out.write(value);
        int[] sizes = { -1, 0, 1, 62, 63, 64, 127, 128, 16383, 16384, 70000 };
        long[] branchDeltas = { -1, 0, 1, 255, 256, 65535, 65536 };
        int checked = 0;
        for (int descent : sizes)
            for (int ascent : sizes)
                for (long branchDelta : branchDeltas)
                    for (boolean hasChild : new boolean[]{ false, true })
                    {
                        boolean hasBranch = branchDelta >= 0;
                        // the pointer to the branch takes the place of ascent-side content
                        if (hasBranch && ascent >= 0)
                            continue;
                        // a node's children are written between its branch and the node
                        if (branchDelta == 0 && hasChild)
                            continue;

                        String message = String.format("Descent %d ascent %d branch at %d child %s", descent, ascent, branchDelta, hasChild);
                        try (DataOutputBuffer out = new DataOutputBuffer())
                        {
                            // the root of the branch is the end of a node, and thus not at the very start
                            fill(out, Math.max(branchDelta, 0) + 1);
                            long start = out.position();
                            OnDiskWriteNodeType.writePayload(out, bytes,
                                                             descent >= 0 ? new byte[descent] : null,
                                                             ascent >= 0 ? new byte[ascent] : null,
                                                             hasBranch ? start - branchDelta : 0,
                                                             hasChild);
                            long written = out.position() - start;
                            long expected = OnDiskWriteNodeType.sizePayload(descent, ascent, hasBranch, hasChild)
                                            + (hasBranch ? OnDiskWriteNodeType.sizeAlternateBranchPointer(branchDelta) : 0);
                            assertEquals(message, expected, written);

                            if (descent < 0 && ascent < 0 && !hasBranch)
                            {
                                assertEquals(message, 0, written);
                                continue;
                            }
                            int code = out.buffer().get((int) out.position() - 1) & 0xFF;
                            if (descent >= 0 && descent <= OnDiskWriteNodeType.MAX_LEAF_LENGTH_INCLUSIVE && ascent < 0 && !hasBranch && !hasChild)
                                assertEquals(message, OnDiskWriteNodeType.LEAF_BITS | descent, code);
                            else
                                assertEquals(message,
                                             OnDiskWriteNodeType.PREFIX_BITS
                                             | (descent >= 0 ? OnDiskWriteNodeType.PREFIX_HAS_DESCENT_CONTENT : 0)
                                             | (ascent >= 0 || hasBranch ? OnDiskWriteNodeType.PREFIX_HAS_ASCENT_CONTENT : 0)
                                             | (hasChild ? OnDiskWriteNodeType.PREFIX_HAS_CHILD : 0),
                                             code);
                            ++checked;
                        }
                    }
        assertTrue(checked > 200);
    }
}
