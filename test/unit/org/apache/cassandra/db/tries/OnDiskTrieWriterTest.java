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
/// only check that the reader agrees with the writer; this is what catches a change to the layout itself.
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

    /// The sparse node example from `OnDiskTrie.md`.
    @Test
    public void testDocumentedSparseExample() throws Exception
    {
        assertBytes("03 01  02 01  01 01  04 02 43 42 41 84",
                    write(trie(false, "A", 1, "B", 2, "C", 3), false));
    }

    /// The bitmap node example from `OnDiskTrie.md`: a child for each capital letter.
    @Test
    public void testDocumentedBitmapExample() throws Exception
    {
        Object[] keysAndValues = new Object[52];
        StringBuilder leaves = new StringBuilder();
        for (int i = 0; i < 26; ++i)
        {
            keysAndValues[2 * i] = String.valueOf((char) ('A' + i));
            keysAndValues[2 * i + 1] = i + 1;
            leaves.insert(0, String.format("%02x 01 ", i + 1));
        }
        assertBytes(leaves + "32 30 2E 2C 2A 28 26 24 22 20 1E 1C 1A 18 16 14 12 10 0E 0C 0A 08 06 04 02 " +
                    "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 07 FF FF FE 00 00 00 00 00 00 00 00 " +
                    "E0",
                    write(trie(false, keysAndValues), false));
    }

    /// The dense node example from `OnDiskTrie.md`: an empty leaf for every transition but 00.
    @Test
    public void testDocumentedDenseExample() throws Exception
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

    /// The prefix node example from `OnDiskTrie.md`: content and a child.
    @Test
    public void testDocumentedPrefixExample() throws Exception
    {
        assertBytes("02 01  62 40  01 01 F5  61 40",
                    write(trie(false, "a", 1, "ab", 2), false));
    }

    /// The deletion-aware example from `OnDiskTrie.md`: live content and a deletion branch at the same node, with the
    /// branch pointer in the node's ascent-side slot as a delta from the start of the slot.
    @Test
    public void testDocumentedDeletionBranchSlotExample() throws Exception
    {
        InMemoryDeletionAwareTrie<Integer, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        InMemoryDeletionAwareTrie<Integer, DeletionMarker>.Mutator<Integer, DeletionMarker> mutator =
            trie.mutator((Integer x, Integer y) -> y,
                         (DeletionMarker x, DeletionMarker y) -> y,
                         (Integer x, DeletionMarker y) -> x,
                         (DeletionMarker x, Integer y) -> y,
                         false,
                         v -> false);
        mutator.apply(DeletionAwareTrie.deletedRange(key("a"), key("b"), key("c"), VERSION, new DeletionMarker(key("x"), 5, 5)));
        mutator.apply(DeletionAwareTrie.singleton(key("a"), VERSION, 7));

        // Markers as their two sides, one byte each.
        OnDiskTrieWriter.DataSerializer<DeletionMarker> markerSerializer = (out, marker) -> {
            out.writeByte(marker.leftSide);
            out.writeByte(marker.rightSide);
        };
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            OnDiskDeletionAwareTrieWriter.write(trie, BYTE_SERIALIZER, markerSerializer, out);
            assertBytes("05 FF 02  FF 05 02  03 63 62 80  00 01 07 01 F6  61 40", out.toByteArray());
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

        try (OnDiskTrie<byte[]> read = TrieUtil.onDiskRoundtrip(trie, false, BYTES_SERDE, BYTES_SERDE))
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

        try (OnDiskTrie<byte[]> read = TrieUtil.onDiskRoundtrip(trie, false, BYTES_SERDE, BYTES_SERDE))
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
