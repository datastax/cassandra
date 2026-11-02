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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/// Pins the bytes [OnDiskTrieWriter] produces against the worked examples in `OnDiskTrie.md`. The round-trip suites
/// only check that the reader agrees with the writer; this is what catches a change to the layout itself. The dense
/// node example would take a trie of over 16 MiB to write, so it is read back instead.
public class OnDiskTrieWriterTest
{
    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        DatabaseDescriptor.toolInitialization();
    }

    /// One byte per value, so the expected layouts can be written out by hand.
    static class ByteSerializer implements OnDiskTrieWriter.DataSerializer<Integer>
    {
        @Override
        public void serialize(DataOutputPlus out, Integer value) throws IOException
        {
            out.writeByte(value);
        }
    }

    static final ByteSerializer BYTE_SERIALIZER = new ByteSerializer();

    static ByteComparable.Preencoded key(String s)
    {
        // Raw bytes: TrieUtil.comparable appends a terminator, which would change every example.
        return ByteComparable.preencoded(VERSION, s.getBytes(StandardCharsets.US_ASCII));
    }

    static InMemoryTrie<Integer> trie(boolean isOrdered, Object... keysAndValues) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<Integer> trie = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        for (int i = 0; i < keysAndValues.length; i += 2)
            trie.putRecursive(key((String) keysAndValues[i]), (Integer) keysAndValues[i + 1], false, (x, y) -> y);
        return trie;
    }

    static byte[] write(BaseTrie<Integer, ?, ?> trie, boolean isOrdered) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, isOrdered, BYTE_SERIALIZER, out);
            return out.toByteArray();
        }
    }

    static void assertBytes(String expectedHex, byte[] actual)
    {
        assertEquals(expectedHex.replace(" ", "").toLowerCase(), Hex.bytesToHex(actual));
    }

    /// The `tractor / tree / trie` example from `OnDiskTrie.md`, byte for byte: three leaves each under a one-byte
    /// chain, a sparse node with one implicit and two explicit one-byte pointers, and the `tr` chain as the root.
    @Test
    public void testDocumentedExample() throws Exception
    {
        assertBytes("03 01 65 40  02 01 65 40  01 01 72 6f 74 63 43  0b 07 69 65 61 84  72 74 41",
                    write(trie(false, "tractor", 1, "tree", 2, "trie", 3), false));
    }

    /// The leaf node example from `OnDiskTrie.md`, `00 00 00 01 04`: a 32-bit integer payload with value 1.
    @Test
    public void testDocumentedLeafExample() throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie(false, "", 1), false, (o, value) -> o.writeInt(value), out);
            assertBytes("00 00 00 01 04", out.toByteArray());
        }
    }

    /// The chain node example from `OnDiskTrie.md`, `43 42 41 42`: the transition `ABC ->`, here to a leaf, `01 01`.
    @Test
    public void testDocumentedChainExample() throws Exception
    {
        assertBytes("01 01  43 42 41 42", write(trie(false, "ABC", 1), false));
    }

    /// The sparse node example from `OnDiskTrie.md`, `04 01 82 00 43 42 41 85` ending at 0x20E, with the children it
    /// points to at 0x206, 0x184 and 0x102. A value longer than 63 bytes is written as a prefix node: the value, its vint
    /// length and the code. "C" is 255 bytes, 0x102 with a two-byte length; "Ba" and "Aa" are 126 bytes, 0x82 with a
    /// one-byte length and the one-transition chain to them.
    @Test
    public void testDocumentedSparseExample() throws Exception
    {
        byte[] bytes = writeSized(trie(false, "Aa", 126, "Ba", 126, "C", 255));
        assertEquals(0x20E, bytes.length);
        assertBytes("04 01 82 00 43 42 41 85", Arrays.copyOfRange(bytes, 0x206, 0x20E));
    }

    /// The bitmap node example from `OnDiskTrie.md`: a child for each capital letter, with three-byte pointers. "A" is
    /// written last, as a prefix node of 0x2289 bytes (8838 + 2 + 1), and "B" before it, of 0x010380 - 0x2289 bytes
    /// (57587 + 3 + 1); "C" to "Z" are empty leaves of one byte each.
    @Test
    public void testDocumentedBitmapExample() throws Exception
    {
        Object[] keysAndValues = new Object[52];
        for (int i = 0; i < 26; ++i)
        {
            keysAndValues[2 * i] = String.valueOf((char) ('A' + i));
            keysAndValues[2 * i + 1] = i == 0 ? 8838 : i == 1 ? 57587 : 0;
        }
        // "Z" to "C" end at positions 1 to 24, and "B" and "A" take the 0x010380 bytes up to the start of the node.
        long nodeStart = 24 + 0x010380;
        // The pointers the example leaves out, those of "Z" to "D".
        StringBuilder omittedPointers = new StringBuilder();
        for (long leafEnd = 1; leafEnd <= 23; ++leafEnd)
        {
            long delta = nodeStart - leafEnd;
            omittedPointers.append(String.format("%02x %02x %02x ", delta & 0xFF, (delta >> 8) & 0xFF, delta >> 16));
        }

        byte[] bytes = writeSized(trie(false, keysAndValues));
        assertEquals(nodeStart + 25 * 3 + 32 + 1, bytes.length);
        assertBytes(omittedPointers + "80 03 01 89 22 00 " +
                    "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 07 FF FF FE 00 00 00 00 00 00 00 00 " +
                    "E2",
                    Arrays.copyOfRange(bytes, (int) nodeStart, bytes.length));
    }

    /// The dense node example from `OnDiskTrie.md`, `... 00 00 00 00 FF FF FF FF EB`, read back: four-byte pointers,
    /// no child for 00, and the 01 child immediately before the node. The node is built by hand, with no child for any
    /// of the transitions the example leaves out.
    @Test
    public void testDocumentedDenseExample() throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(Hex.hexToBytes("0101"));      // the 01 child, a leaf with the content 01
            for (int i = 255; i >= 2; --i)
                out.writeInt(-1);                   // FF FF FF FF, no child
            out.write(Hex.hexToBytes("00 00 00 00 FF FF FF FF EB".replace(" ", "")));
            OnDiskCursor.DataDeserializer<Integer> oneByte = (rdr, length) -> (int) rdr.readByte();
            TrieUtil.assertTriesEqual(trie(false, "\u0001", 1),
                                      OnDiskTrie.open(out.asNewBuffer(), oneByte, VERSION, false, out.getLength()));
        }
    }

    /// A dense node as the writer lays it out: an empty leaf for every transition but 00, and one-byte pointers in
    /// transition order reading back from the code, with `FF` for the absent 00 child.
    @Test
    public void testDenseNodeLayout() throws Exception
    {
        InMemoryTrie<Integer> trie = InMemoryTrie.shortLived(VERSION);
        StringBuilder expected = new StringBuilder();
        for (int i = 1; i < 256; ++i)
        {
            trie.putRecursive(ByteComparable.preencoded(VERSION, new byte[]{ (byte) i }), 0, false, (x, y) -> y);
            expected.append("00 ");     // the leaves, each an empty leaf code
        }
        for (int i = 254; i >= 0; --i)
            expected.append(String.format("%02x ", i));     // pointers for FF down to 01
        expected.append("FF E8");
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, SIZED_SERIALIZER, out);
            assertBytes(expected.toString(), out.toByteArray());
        }
    }

    /// A range trie marker of one position only, as [RangeTrie#point] places one, with no range open on either side of
    /// it, e.g. a row-level marker. It holds a single byte, which is how [#POINT_SERDE] writes it.
    static final class PointState implements RangeState<PointState>
    {
        final int value;

        PointState(int value)
        {
            this.value = value;
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public PointState precedingState(Direction direction)
        {
            return null;
        }

        @Override
        public PointState succedingState(Direction direction)
        {
            return null;
        }

        @Override
        public PointState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return this;    // no sides to restrict
        }

        @Override
        public PointState asBoundary(Direction direction)
        {
            throw new AssertionError("A point is never a covering state");
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof PointState && ((PointState) o).value == value;
        }

        @Override
        public int hashCode()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return "P" + value;
        }
    }

    static class PointSerDe implements OnDiskTrieWriter.DataSerializer<PointState>,
                                       OnDiskCursor.DataDeserializer<PointState>
    {
        @Override
        public void serialize(DataOutputPlus out, PointState value) throws IOException
        {
            out.writeByte(value.value);
        }

        @Override
        public PointState deserialize(DataInputPlus rdr, int length) throws IOException
        {
            return new PointState(rdr.readUnsignedByte());
        }
    }

    static final PointSerDe POINT_SERDE = new PointSerDe();

    /// Writes the range trie `trie` with [#POINT_SERDE], as range tries are written, ordered.
    private static byte[] writePoints(RangeTrie<PointState> trie) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, true, POINT_SERDE, out);
            return out.toByteArray();
        }
    }

    /// The generic content node example from `OnDiskTrie.md`, `40 40 02 01 02 01 F7`: a range trie node with the
    /// content `02` on both sides and a child, reached with the one-transition chain for 0x40. The child, `01 01`, a
    /// leaf with the content `01`, precedes the chain; the example leaves it out.
    @Test
    public void testDocumentedGenericContentExample() throws Exception
    {
        RangeTrie<PointState> trie = RangeTrie.merge(Arrays.asList(
            RangeTrie.point(ByteComparable.EMPTY, VERSION, true, new PointState(2)),    // before the branch
            RangeTrie.point(key("@"), VERSION, true, new PointState(1)),
            RangeTrie.point(ByteComparable.EMPTY, VERSION, false, new PointState(2))),  // after it
            Trie.throwingResolver());
        byte[] bytes = writePoints(trie);
        assertBytes("01 01  40 40 02 01 02 01 F7", bytes);
        TrieUtil.assertTriesEqual(trie,
                                  OnDiskRangeTrie.open(ByteBuffer.wrap(bytes), POINT_SERDE, VERSION, bytes.length));
    }

    /// Markers as their two sides, one byte each.
    static final OnDiskTrieWriter.DataSerializer<DeletionMarker> MARKER_SERIALIZER = (out, marker) -> {
        out.writeByte(marker.leftSide);
        out.writeByte(marker.rightSide);
    };

    /// A deletion-aware trie with the given live content and a deletion of `[ab, ac)`, written as a deletion branch at
    /// "a", which [#MARKER_SERIALIZER] writes as `05 FF 02  FF 05 02  03 63 62 80`.
    @SuppressWarnings("unchecked")
    static <T> InMemoryDeletionAwareTrie<T, DeletionMarker> deletionAwareTrie(Object... keysAndValues) throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<T, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        InMemoryDeletionAwareTrie<T, DeletionMarker>.Mutator<T, DeletionMarker> mutator =
            trie.mutator((T x, T y) -> y,
                         (DeletionMarker x, DeletionMarker y) -> y,
                         (T x, DeletionMarker y) -> x,
                         (DeletionMarker x, T y) -> y,
                         false,
                         v -> false);
        mutator.apply(DeletionAwareTrie.deletedRange(key("a"), key("b"), key("c"), VERSION, new DeletionMarker(key("x"), 5, 5)));
        for (int i = 0; i < keysAndValues.length; i += 2)
            mutator.apply(DeletionAwareTrie.singleton(key((String) keysAndValues[i]), VERSION, (T) keysAndValues[i + 1]));
        return trie;
    }

    /// The deletion-aware node example from `OnDiskTrie.md`, `34 12 02 07 01 F7`: live content `07`, a child, and a
    /// deletion branch whose root is 0x1234 bytes before the start of the node. Between the two are the child "ad", a
    /// 4655-byte value with a two-byte length and the code, and the chain to it, `64 40`; the root's chain to "a",
    /// `61 40`, follows.
    @Test
    public void testDocumentedDeletionBranchExample() throws Exception
    {
        InMemoryDeletionAwareTrie<byte[], DeletionMarker> trie = deletionAwareTrie("a", new byte[]{ 7 }, "ad", new byte[4655]);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(trie, BYTES_SERDE, MARKER_SERIALIZER, out);
            byte[] bytes = out.toByteArray();
            assertBytes("64 40  34 12 02 07 01 F7  61 40", Arrays.copyOfRange(bytes, bytes.length - 10, bytes.length));
        }
    }

    /// The deletion-aware node example from `OnDiskTrie.md` without children, `00 07 01 F6`: the deletion branch ends
    /// where the node starts, and the delta 0 is an empty slot.
    @Test
    public void testDocumentedDeletionBranchSlotExample() throws Exception
    {
        InMemoryDeletionAwareTrie<Integer, DeletionMarker> trie = deletionAwareTrie("a", 7);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(trie, BYTE_SERIALIZER, MARKER_SERIALIZER, out);
            assertBytes("05 FF 02  FF 05 02  03 63 62 80  00 07 01 F6  61 40", out.toByteArray());
        }
    }

    /// A node with content and a single child is a one-transition chain node followed by a prefix node, with the
    /// chain above it written after both -- the construction `OnDiskTrie.md` describes, with no folding of the
    /// parent's transition into the child's chain.
    @Test
    public void testSingleChildPrefix() throws Exception
    {
        assertBytes("02 01  66 65 41  64 40  01 01 f5  63 62 61 42",
                    write(trie(false, "abc", 1, "abcdef", 2), false));
    }

    /// The reverse walk reaches "abx" before "abcdef"; the writer must leave no trace of it, so the bytes are those
    /// of the trie without it.
    @Test
    public void testUnproductiveBranchVisitedBeforeItsSibling() throws Exception
    {
        Trie<Integer> withBranch = trie(false, "abcdef", 1, "abx", 2).mapValues(v -> v == 2 ? null : v);
        assertBytes(Hex.bytesToHex(write(trie(false, "abcdef", 1), false)), write(withBranch, false));
    }

    /// The same with an ordered trie, whose cursor also stops on return-path positions.
    @Test
    public void testUnproductiveBranchVisitedBeforeItsSiblingOrdered() throws Exception
    {
        Trie<Integer> withBranch = trie(true, "abcdef", 1, "abx", 2).mapValues(v -> v == 2 ? null : v);
        assertBytes(Hex.bytesToHex(write(trie(true, "abcdef", 1), true)), write(withBranch, true));
    }

    /// Walked in reverse, an ordered trie presents its root's content on the root's return path, after the walk has
    /// been through "abc", which leaves no node behind. That content belongs to the root, not to "abc".
    @Test
    public void testUnproductiveBranchBeforeRootReturnPathContent() throws Exception
    {
        Trie<Integer> withBranch = trie(true, "", 1, "abc", 2).mapValues(v -> v == 2 ? null : v);
        assertBytes(Hex.bytesToHex(write(trie(true, "", 1), true)), write(withBranch, true));
    }

    /// The same in a range trie, which is always written ordered, the way a deletion branch is: a marker placed before
    /// the root's branch is presented on the root's return path in reverse. An intersection with a set that ends at
    /// "abb" walks into "ab" with the set and drops "abc", which leaves the branch empty.
    @Test
    public void testUnproductiveBranchBeforeRootReturnPathContentInRangeTrie() throws Exception
    {
        RangeTrie<PointState> root = RangeTrie.point(ByteComparable.EMPTY, VERSION, true, new PointState(1));
        RangeTrie<PointState> withBranch = root.mergeWith(RangeTrie.point(key("abc"), VERSION, true, new PointState(2)),
                                                          Trie.throwingResolver())
                                               .intersect(TrieSet.range(VERSION, null, true, key("abb"), false));
        assertBytes(Hex.bytesToHex(writePoints(root)), writePoints(withBranch));
    }

    /// "abca" is reached after "abcdef", so when the walk moves to it the node at "abc" already exists with its "d"
    /// child. The branch itself writes nothing, but that node is left with one child and no content and is written as
    /// a one-transition chain node, splitting the chain: two bytes more than "abcdef" alone
    /// (`01 01 66 65 64 63 62 61 45`).
    @Test
    public void testUnproductiveBranchVisitedAfterItsSibling() throws Exception
    {
        Trie<Integer> withBranch = trie(false, "abcdef", 1, "abca", 2).mapValues(v -> v == 2 ? null : v);
        assertBytes("01 01  66 65 41  64 40  63 62 61 42", write(withBranch, false));
    }

    /// Raw bytes as content, to control node sizes exactly.
    static class BytesSerDe implements OnDiskTrieWriter.DataSerializer<byte[]>, OnDiskCursor.DataDeserializer<byte[]>
    {
        @Override
        public void serialize(DataOutputPlus out, byte[] value) throws IOException
        {
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInputPlus rdr, int length) throws IOException
        {
            byte[] bytes = new byte[length];
            rdr.readFully(bytes);
            return bytes;
        }
    }

    static final BytesSerDe BYTES_SERDE = new BytesSerDe();

    /// A dense node marks an absent child with all-ones of its pointer width, so the furthest child's delta must
    /// never be exactly that, and a dense node would need three-byte pointers here, where the delta to the first-written
    /// child (key 0xff) is exactly 0xffff. A bitmap with two-byte pointers is smaller than that, so it is the one
    /// written; the delta is still read back as a child, not as an absent one.
    @Test
    public void testDenseFurthestDeltaAtPointerLimit() throws Exception
    {
        // A value longer than 63 bytes is written as a prefix node: value, two-byte vint length, code = length + 3.
        // The 239 children written after key 0xff are 238 * 273 + 561 = 65535 bytes; key 0x10 is written last.
        InMemoryTrie<byte[]> trie = InMemoryTrie.shortLived(VERSION);
        for (int b = 0x10; b <= 0xff; ++b)
            trie.putRecursive(ByteComparable.preencoded(VERSION, new byte[]{ (byte) b }),
                              new byte[b == 0x10 ? 558 : 270], false, (x, y) -> y);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, BYTES_SERDE, out);
            // 240 children (239 * 273 + 561) + bitmap node with two-byte pointers (32 + 239 * 2 + 1)
            assertEquals(65808 + 511, out.getLength());
            assertEquals(OnDiskWriteNodeType.BITMAP.bits | 1, out.toByteArray()[out.getLength() - 1] & 0xFF);
        }

        try (FileTries.Plain<byte[]> read = TrieUtil.onDiskRoundtrip(trie, false, BYTES_SERDE, BYTES_SERDE))
        {
            assertNotNull("child 0xff must not read as absent", read.get(ByteComparable.preencoded(VERSION, new byte[]{ (byte) 0xff })));
            // Values are byte[], which assertTriesEqual would compare by identity.
            for (int b = 0x10; b <= 0xff; ++b)
            {
                ByteComparable.Preencoded key = ByteComparable.preencoded(VERSION, new byte[]{ (byte) b });
                assertArrayEquals("key " + b, trie.get(key), read.get(key));
            }
        }
    }

    /// Just below the limit above, with every transition present so that a dense node is the smaller: the furthest
    /// delta is 0xfffe, one below the absent marker of two-byte pointers, which the dense node then uses.
    @Test
    public void testDenseFurthestDeltaBelowPointerLimit() throws Exception
    {
        // Values of 253 and 254 bytes are written as prefix nodes of 256 and 257 bytes (two-byte vint length, code).
        // The 255 children written after key 0xff are 254 * 257 + 256 = 65534 bytes; key 0x00 is written last.
        InMemoryTrie<byte[]> trie = InMemoryTrie.shortLived(VERSION);
        for (int b = 0x00; b <= 0xff; ++b)
            trie.putRecursive(ByteComparable.preencoded(VERSION, new byte[]{ (byte) b }),
                              new byte[b == 0x00 ? 253 : 254], false, (x, y) -> y);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, BYTES_SERDE, out);
            // 256 children (255 * 257 + 256) + dense node with two-byte pointers (256 * 2 + 1)
            assertEquals(65791 + 513, out.getLength());
            assertEquals(OnDiskWriteNodeType.DENSE.bits | 1, out.toByteArray()[out.getLength() - 1] & 0xFF);
        }

        try (FileTries.Plain<byte[]> read = TrieUtil.onDiskRoundtrip(trie, false, BYTES_SERDE, BYTES_SERDE))
        {
            for (int b = 0x00; b <= 0xff; ++b)
            {
                ByteComparable.Preencoded key = ByteComparable.preencoded(VERSION, new byte[]{ (byte) b });
                assertArrayEquals("key " + b, trie.get(key), read.get(key));
            }
        }
    }

    /// Writes `value` zero bytes, so that a child's size in the file can be chosen.
    static final OnDiskTrieWriter.DataSerializer<Integer> SIZED_SERIALIZER = (out, value) -> out.write(new byte[value]);

    /// Reads back the content [#SIZED_SERIALIZER] writes as its size.
    static final OnDiskCursor.DataDeserializer<Integer> SIZE_DESERIALIZER = (rdr, length) -> length;

    /// Writes `trie` with [#SIZED_SERIALIZER], checks that it reads back, and returns the bytes.
    private static byte[] writeSized(InMemoryTrie<Integer> trie) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, SIZED_SERIALIZER, out);
            TrieUtil.assertTriesEqual(trie, OnDiskTrie.open(out.asNewBuffer(), SIZE_DESERIALIZER, VERSION, false, out.getLength()));
            return out.toByteArray();
        }
    }

    /// A bitmap node is written when it is smaller than a dense one. With 224 one-byte children and one-byte pointers a
    /// bitmap takes 32 + 223 = 255 bytes against a dense node's 256.
    @Test
    public void testBitmapWhenOneByteSmaller() throws Exception
    {
        int[] sizes = new int[224];
        assertEquals(OnDiskWriteNodeType.BITMAP.bits, rootCode(sizes));
        assertEquals(OnDiskWriteNodeType.DENSE.bits, rootCode(new int[225]));    // 32 + 224 = 256, no saving
    }

    /// A dense node's pointers can be one byte wider than a bitmap's, when the furthest delta is all-ones in its width.
    /// Here 240 children have a furthest delta of exactly 255: one-byte pointers make a bitmap of 32 + 239 = 271 bytes,
    /// while the dense node needs two-byte pointers, 512 bytes.
    @Test
    public void testBitmapWhenDensePointersAreWider() throws Exception
    {
        int[] sizes = new int[240];
        sizes[0] = 16;      // a 17-byte leaf; the other 238 after the furthest child take a byte each, 255 in all
        assertEquals(OnDiskWriteNodeType.BITMAP.bits, rootCode(sizes));
    }

    /// The code of the root of a trie with a child for each transition from 0, whose leaf content is `sizes[i]` bytes,
    /// with the bytes-per-pointer bits masked off.
    private static int rootCode(int[] sizes) throws Exception
    {
        InMemoryTrie<Integer> trie = InMemoryTrie.shortLived(VERSION);
        for (int i = 0; i < sizes.length; ++i)
            trie.putRecursive(ByteComparable.preencoded(VERSION, new byte[]{ (byte) i }), sizes[i], false, (x, y) -> y);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(trie, false, SIZED_SERIALIZER, out);
            byte[] bytes = out.toByteArray();
            return bytes[bytes.length - 1] & ~0x7 & 0xFF;
        }
    }

    /// One writer writing several tries, as the deletion-aware writer does with its deletion branches, writes each as a
    /// new writer would. Walked in reverse, the first range trie ends on its root's return path, presenting the open
    /// marker there, and the second starts with content at its root, the close marker of a point deletion.
    @Test
    public void testWriterWritesSeveralTries() throws Exception
    {
        RangeTrie<TestRangeState> first = TestRangeState.fromList(TestRangeState.verify(Arrays.asList(
            new TestRangeState(ByteComparable.EMPTY, -1, 1),
            new TestRangeState(key("a"), 1, -1))));
        RangeTrie<TestRangeState> second = TestRangeState.fromList(TestRangeState.verify(Arrays.asList(
            new TestRangeState(ByteComparable.EMPTY, false, -1, 1),
            new TestRangeState(ByteComparable.EMPTY, true, 1, -1))));
        try (DataOutputBuffer expected = new DataOutputBuffer();
             DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskTrieWriter.write(first, true, TrieUtil.RANGE_SERDE, expected);
            OnDiskTrieWriter.write(second, true, TrieUtil.RANGE_SERDE, expected);

            OnDiskTrieWriter<TestRangeState> writer = new OnDiskTrieWriter<>(out, TrieUtil.RANGE_SERDE, true);
            try (Cursor<TestRangeState> c = first.cursor(Direction.REVERSE))
            {
                writer.writeFrom(c);
            }
            try (Cursor<TestRangeState> c = second.cursor(Direction.REVERSE))
            {
                writer.writeFrom(c);
            }
            assertEquals(Hex.bytesToHex(expected.toByteArray()), Hex.bytesToHex(out.toByteArray()));
        }
    }

    /// Writing into a stream that already holds data: an empty trie writes nothing and reports root 0, which the readers
    /// take as the empty trie, and a non-empty one reports the position `out` is left at. Both roots open as written.
    @Test
    public void testWriteAtNonZeroOffset() throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeLong(0);
            long emptyRoot = OnDiskTrieWriter.write(trie(false), false, BYTE_SERIALIZER, out);
            assertEquals(0, emptyRoot);
            assertEquals(8, out.getLength());

            long root = OnDiskTrieWriter.write(trie(false, "a", 1), false, BYTE_SERIALIZER, out);
            assertEquals(out.getLength(), root);

            ByteBuffer buffer = out.asNewBuffer();
            OnDiskCursor.DataDeserializer<Integer> oneByte = (rdr, length) -> (int) rdr.readByte();
            TrieUtil.assertTriesEqual(trie(false), OnDiskTrie.open(buffer, oneByte, VERSION, false, emptyRoot));
            TrieUtil.assertTriesEqual(trie(false, "a", 1), OnDiskTrie.open(buffer, oneByte, VERSION, false, root));
        }
    }
}
