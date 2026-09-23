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
import java.nio.charset.StandardCharsets;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

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
        public int serialize(DataOutputPlus out, Integer value) throws IOException
        {
            out.writeByte(value);
            return 1;
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

    /// Writing into a stream that already holds data: an empty trie writes nothing, so there is no root to report, and
    /// a non-empty one reports the position `out` is left at.
    @Test
    public void testWriteAtNonZeroOffset() throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeLong(0);
            assertEquals(-1, OnDiskTrieWriter.write(trie(false), false, BYTE_SERIALIZER, out));
            assertEquals(8, out.getLength());

            long root = OnDiskTrieWriter.write(trie(false, "a", 1), false, BYTE_SERIALIZER, out);
            assertEquals(out.getLength(), root);
        }
    }
}
