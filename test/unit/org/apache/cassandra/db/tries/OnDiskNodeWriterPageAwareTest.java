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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.tries.OnDiskCursor.ContentStyle;
import org.apache.cassandra.db.tries.OnDiskTrieVerifier.Result;
import org.apache.cassandra.db.tries.OnDiskTrieVerifier.Type;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.OnDiskCursor.ContentStyle.RANGE;
import static org.apache.cassandra.db.tries.OnDiskCursor.ContentStyle.UNORDERED;
import static org.apache.cassandra.db.tries.OnDiskTrieWriter.plainStyle;
import static org.apache.cassandra.db.tries.TrieUtil.RANGE_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.STRING_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// Tests of the layout [OnDiskNodeWriterPageAware] produces.
///
/// Every trie written here is read back with the production reader in both directions, and its serialized form is
/// examined by [OnDiskTrieVerifier], a decoder that is independent of that reader: this is what checks the pointers,
/// the padding and the placement of the nodes in pages, and what the tests take the numbers of relays, padding bytes
/// etc. from to make sure they exercise what they claim to. The verifier itself is validated at the start of this
/// class, against layouts written out by hand from `OnDiskTrie.md` and against the output of
/// [OnDiskNodeWriterImmediate].
///
/// The exception to reading back are the tries written to a [SkippingOutput], which have distances in them that no
/// buffer can hold; these are only examined by the verifier.
public class OnDiskNodeWriterPageAwareTest
{
    private static final long SEED = 0x6c61796f7574L;
    private static final int[] PAGE_SIZES = { 32, 64, 100, 256, 4096 };
    private static final byte PADDING = TrieUtil.PagedDataOutputBuffer.PADDING;
    /// What the output is filled with before a trie that does not start at offset 0.
    private static final byte FILLER = 0x5A;
    /// Set to print the statistics the tests gather about the layouts they make.
    private static final boolean VERBOSE = false;

    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        DatabaseDescriptor.toolInitialization();
    }

    /// A way of making the node writer for a trie: one of the [OnDiskNodeWriter.Type]s, or a variation that only the
    /// tests can make.
    interface NodeWriters
    {
        <T> OnDiskNodeWriter<T> create(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer);
    }

    /// The page-aware node writer without chain folding, which writes the same nodes as the immediate one.
    static final NodeWriters PAGE_AWARE_WITHOUT_FOLDING = new NodeWriters()
    {
        @Override
        public <T> OnDiskNodeWriter<T> create(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer)
        {
            return new OnDiskNodeWriterPageAware<>(out, serializer, false);
        }

        @Override
        public String toString()
        {
            return "PAGE_AWARE_WITHOUT_FOLDING";
        }
    };

    // Helpers: building tries

    static ByteComparable.Preencoded key(byte[] bytes)
    {
        return ByteComparable.preencoded(VERSION, bytes);
    }

    static byte[] bytes(String s)
    {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    static byte[] concat(byte[] prefix, int... more)
    {
        byte[] result = Arrays.copyOf(prefix, prefix.length + more.length);
        for (int i = 0; i < more.length; ++i)
            result[prefix.length + i] = (byte) more[i];
        return result;
    }

    static byte[] repeat(int value, int count)
    {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    static InMemoryTrie<String> newTrie(boolean ordered)
    {
        return ordered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
    }

    static void put(InMemoryTrie<String> trie, byte[] key, String value)
    {
        try
        {
            trie.putRecursive(key(key), value, (x, y) -> y);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    static void put(InMemoryTrie<String> trie, String key, String value)
    {
        put(trie, bytes(key), value);
    }

    /// Like [#put] but without recursion, for keys whose length could exhaust the stack.
    static void putNonRecursive(InMemoryTrie<String> trie, byte[] key, String value)
    {
        try
        {
            trie.putSingleton(key(key), value, (x, y) -> y);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    static InMemoryTrie<String> trie(boolean ordered, String... keysAndValues)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        for (int i = 0; i < keysAndValues.length; i += 2)
            put(trie, keysAndValues[i], keysAndValues[i + 1]);
        return trie;
    }

    static String randomValue(Random rand, int index)
    {
        switch (rand.nextInt(16))
        {
            case 0:
                return "";
            case 1:
                return "m".repeat(56 + rand.nextInt(16));    // around the limit of the leaf node type
            case 2:
                return "L".repeat(100 + rand.nextInt(400));
            default:
                return "v" + index;
        }
    }

    /// Keys of up to 12 bytes from small alphabets, some of them prefixes of others (possibly the empty key), some with
    /// a long tail of arbitrary bytes.
    static InMemoryTrie<String> randomTrie(Random rand, boolean ordered, int count)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        for (int i = 0; i < count; ++i)
        {
            int length = rand.nextInt(13);
            int tail = rand.nextInt(20) == 0 ? rand.nextInt(100) : 0;
            byte[] key = new byte[length + tail];
            for (int j = 0; j < length; ++j)
                key[j] = (byte) ('a' + rand.nextInt(rand.nextInt(4) == 0 ? 26 : 3));
            for (int j = length; j < key.length; ++j)
                key[j] = (byte) rand.nextInt(256);
            put(trie, key, randomValue(rand, i));
        }
        return trie;
    }

    /// Prefix-free keys of 10 to 50 arbitrary bytes with shared prefixes, as used by the other trie tests.
    static InMemoryTrie<String> generatedKeysTrie(Random rand, boolean ordered, int count)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        ByteComparable.Preencoded[] keys = TrieUtil.generateKeys(rand, count);
        for (int i = 0; i < count; ++i)
            put(trie, keys[i].asByteComparableArray(VERSION), randomValue(rand, i));
        return trie;
    }

    /// A node with the given number of children, placed below the given path.
    ///
    /// @param topAligned true to use the highest transitions, false to spread them over all values starting with 0
    /// @param below what to put below each child: 0 for nothing (the child is a leaf), 1 for a chain to a leaf, 2 for
    ///              three further children
    static InMemoryTrie<String> wideTrie(boolean ordered, byte[] path, int fanout, boolean topAligned, int below, boolean contentOnNode, int valueLength)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        if (contentOnNode)
            put(trie, path, "node");
        for (int i = 0; i < fanout; ++i)
        {
            int transition = topAligned ? 256 - fanout + i : i * 256 / fanout;
            String value = "c".repeat(valueLength) + i;
            switch (below)
            {
                case 0:
                    put(trie, concat(path, transition), value);
                    break;
                case 1:
                    put(trie, concat(path, transition, 'x', 'y'), value);
                    break;
                default:
                    put(trie, concat(path, transition, 0), value);
                    put(trie, concat(path, transition, 7, 7), value + "b");
                    put(trie, concat(path, transition, 255), value + "c");
                    break;
            }
        }
        return trie;
    }

    private static class RangeEvent
    {
        final byte[] key;
        final boolean after;

        RangeEvent(byte[] key, boolean after)
        {
            this.key = key;
            this.after = after;
        }

        /// The order in which a forward walk meets the two sides of the nodes of a trie.
        static int compare(RangeEvent x, RangeEvent y)
        {
            int common = Math.min(x.key.length, y.key.length);
            int mismatch = Arrays.mismatch(x.key, 0, common, y.key, 0, common);
            if (mismatch >= 0)
                return Integer.compare(x.key[mismatch] & 0xFF, y.key[mismatch] & 0xFF);
            if (x.key.length == y.key.length)
                return Boolean.compare(x.after, y.after);
            if (x.key.length < y.key.length)
                return x.after ? 1 : -1;
            return y.after ? -1 : 1;
        }
    }

    /// A range trie with boundaries on either or both sides of random nodes, which may be below one another.
    static InMemoryRangeTrie<TestRangeState> randomRangeTrie(Random rand, int keyCount)
    {
        return randomRangeTrie(rand, keyCount, 0, true);
    }

    /// @param minLength 1 to have no boundary at the root, 0 to permit it
    /// @param bothSides false to have boundaries only on the descent side of nodes
    static InMemoryRangeTrie<TestRangeState> randomRangeTrie(Random rand, int keyCount, int minLength, boolean bothSides)
    {
        int alphabet = 2 + rand.nextInt(rand.nextBoolean() ? 3 : 60);
        int maxLength = 1 + rand.nextInt(8);
        if (keyCount > 200)
        {
            // make room for that many keys
            alphabet = Math.max(alphabet, 16);
            maxLength = Math.max(maxLength, 4);
        }
        TreeSet<byte[]> keys = new TreeSet<>(Arrays::compareUnsigned);
        for (int i = 0; i < keyCount; ++i)
        {
            byte[] key = new byte[minLength + rand.nextInt(maxLength + 1 - minLength)];
            for (int j = 0; j < key.length; ++j)
                key[j] = (byte) (rand.nextInt(alphabet) * 255 / (alphabet - 1));
            keys.add(key);
        }

        List<RangeEvent> events = new ArrayList<>();
        for (byte[] key : keys)
        {
            // a single boundary cannot close what it opens
            int sides = !bothSides ? 1 : keys.size() == 1 ? 2 : rand.nextInt(4);
            if (sides != 0)
                events.add(new RangeEvent(key, false));
            if (sides != 1)
                events.add(new RangeEvent(key, true));
        }
        events.sort(RangeEvent::compare);

        List<TestRangeState> markers = new ArrayList<>();
        int active = -1;
        int next = 1;
        for (int i = 0; i < events.size(); ++i)
        {
            RangeEvent event = events.get(i);
            int right;
            if (i == events.size() - 1)
                right = -1;
            else if (active < 0 || rand.nextInt(3) != 0)
                right = next++;
            else
                right = -1;
            if (right == active)
                continue;
            markers.add(new TestRangeState(key(event.key), event.after, active, right));
            active = right;
        }
        return TestRangeState.fromList(markers);
    }

    // Helpers: writing and checking

    /// A serialized trie.
    static class Written
    {
        final ByteBuffer data;
        final long start;
        final long root;
        final int pageSize;

        Written(ByteBuffer data, long start, long root, int pageSize)
        {
            this.data = data;
            this.start = start;
            this.root = root;
            this.pageSize = pageSize;
        }

        long size()
        {
            return root - start;
        }

        byte[] bytes()
        {
            byte[] bytes = new byte[(int) size()];
            data.duplicate().position((int) start).get(bytes);
            return bytes;
        }

        Result verify()
        {
            OnDiskTrieVerifier verifier = new OnDiskTrieVerifier(data).from(start);
            if (pageSize > 0)
                verifier.pages(pageSize, PADDING);
            return verifier.verify(root);
        }
    }

    static DataOutputBuffer newOutput(int pageSize, int startOffset) throws IOException
    {
        DataOutputBuffer out = pageSize > 0 ? new TrieUtil.PagedDataOutputBuffer(pageSize) : new DataOutputBuffer();
        for (int i = 0; i < startOffset; ++i)
            out.writeByte(FILLER);
        return out;
    }

    /// Serializes the trie into an output with pages of the given size (0 for one without pages), in which
    /// `startOffset` bytes have already been written.
    static <T> Written write(BaseTrie<T, ?, ?> trie, ContentStyle contentStyle, OnDiskTrieWriter.DataSerializer<T> serializer,
                             OnDiskNodeWriter.Type type, int pageSize, int startOffset) throws IOException
    {
        return write(trie, contentStyle, serializer, type::create, pageSize, startOffset);
    }

    static <T> Written write(BaseTrie<T, ?, ?> trie, ContentStyle contentStyle, OnDiskTrieWriter.DataSerializer<T> serializer,
                             NodeWriters writers, int pageSize, int startOffset) throws IOException
    {
        DataOutputBuffer out = newOutput(pageSize, startOffset);
        long root;
        try (Cursor<T> cursor = trie.cursor(Direction.REVERSE))
        {
            root = new OnDiskTrieWriter<>(writers.create(out, serializer), contentStyle).writeFrom(cursor);
        }
        if (root > 0)
            assertEquals("The root is where the output is left", out.position(), root);
        else
            assertEquals("An empty trie writes nothing", startOffset, out.position());
        return new Written(out.buffer(), startOffset, root, pageSize);
    }

    static Written writePageAware(Trie<String> trie, boolean isOrdered, int pageSize) throws IOException
    {
        return write(trie, plainStyle(isOrdered), STRING_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, 0);
    }

    static Written writeImmediate(Trie<String> trie, boolean isOrdered) throws IOException
    {
        return write(trie, plainStyle(isOrdered), STRING_SERDE, OnDiskNodeWriter.Type.IMMEDIATE, 0, 0);
    }

    /// What the verifier must find in the serialized form of a trie: its content with the paths that lead to it.
    static class Expected
    {
        final List<byte[]> keys = new ArrayList<>();
        final List<Boolean> ascent = new ArrayList<>();
        final List<byte[]> contents = new ArrayList<>();

        void add(byte[] key, boolean onAscent, byte[] content)
        {
            keys.add(key);
            ascent.add(onAscent);
            contents.add(content);
        }

        static Expected of(Trie<String> trie)
        {
            Expected expected = new Expected();
            trie.forEachEntry((k, v) -> expected.add(k.asByteComparableArray(VERSION), false, v.getBytes(StandardCharsets.UTF_8)));
            return expected;
        }

        static Expected of(RangeTrie<TestRangeState> trie)
        {
            Expected expected = new Expected();
            for (TestRangeState state : TestRangeState.toList(trie, Direction.FORWARD))
            {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    RANGE_SERDE.serialize(out, state);
                    expected.add(state.position.asByteComparableArray(VERSION), state.appliesAfter, out.toByteArray());
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            }
            return expected;
        }

        void assertFoundBy(Result result)
        {
            assertEquals("Number of content items", keys.size(), result.entries.size());
            for (int i = 0; i < keys.size(); ++i)
            {
                OnDiskTrieVerifier.Entry entry = result.entries.get(i);
                String at = "Entry " + i + " at " + Hex.bytesToHex(keys.get(i));
                assertArrayEquals(at + " key", keys.get(i), entry.key);
                assertEquals(at + " side", ascent.get(i), entry.ascent);
                assertArrayEquals(at + " content", contents.get(i), entry.content);
            }
        }
    }

    /// Checks a serialized plain trie in every way we have: by reading it with the production reader in both
    /// directions, by verifying its structure and layout, and by comparing the content the verifier finds in it.
    static Result check(String message, Trie<String> trie, boolean isOrdered, Written written)
    {
        return check(message, trie, isOrdered, written, false);
    }

    /// Fails if the verifier found any problem, with one exception if `acceptMisplacedRoot` is true: a node in the
    /// last page of the trie, i.e. the root or one of the nodes written together with it, that crosses the boundary
    /// at which that page begins. This is the subject of [#testRootBranchGrowingWhenMovedToNewPage].
    static void assertValid(String message, Result result, boolean acceptMisplacedRoot)
    {
        if (!acceptMisplacedRoot)
            result.assertValid(message);
        result.assertStructurallyValid(message);
        if (result.layoutErrors.size() > 1 || result.layoutErrors.size() == 1 && result.root - result.misplacedLogicalNodes.get(0) >= result.pageSize)
            throw new AssertionError(message + ": " + result.describeProblems());
    }

    static Result check(String message, Trie<String> trie, boolean isOrdered, Written written, boolean acceptMisplacedRoot)
    {
        try
        {
            assertTrue("Non-empty trie", written.root > written.start);
            OnDiskTrie<String> read = new OnDiskTrie<>(new ByteBufferRebufferer(written.data), STRING_SERDE, VERSION, isOrdered, written.root);
            TrieUtil.assertTriesEqual(trie, read);
            Result result = written.verify();
            assertValid("Verification", result, acceptMisplacedRoot);
            Expected.of(trie).assertFoundBy(result);
            assertEquals("Plain tries have no ascent-side content", 0, result.ascentContents);
            assertEquals("Pointers wider than necessary", 0, result.nodesWithOversizedPointers);
            return result;
        }
        catch (Throwable t)
        {
            throw new AssertionError(message + ": " + t.getMessage(), t);
        }
    }

    static Result checkRange(String message, RangeTrie<TestRangeState> trie, Written written)
    {
        try
        {
            assertTrue("Non-empty trie", written.root > written.start);
            OnDiskRangeTrie<TestRangeState> read = new OnDiskRangeTrie<>(new ByteBufferRebufferer(written.data), RANGE_SERDE, VERSION, written.root);
            TrieUtil.assertTriesEqual(trie, read);
            Result result = written.verify();
            result.assertValid("Verification");
            Expected.of(trie).assertFoundBy(result);
            assertEquals("Pointers wider than necessary", 0, result.nodesWithOversizedPointers);
            return result;
        }
        catch (Throwable t)
        {
            throw new AssertionError(message + ": " + t.getMessage(), t);
        }
    }

    /// Writes the trie with the page-aware writer for the given page size and checks the result.
    static Result writeAndCheck(String message, Trie<String> trie, boolean isOrdered, int pageSize) throws IOException
    {
        return check(message + " page size " + pageSize + (isOrdered ? " ordered" : " unordered"), trie, isOrdered, writePageAware(trie, isOrdered, pageSize));
    }

    /// Sums of what the verifier found over a series of tries, to assert that the series covered what it was meant to.
    static class Totals
    {
        int tries;
        int multiPage;
        long relays;
        long paddingBytes;
        long oversized;
        long smallNodesCrossingPages;
        long needlessRelays;
        int maxPointerBytes;
        final long[] byType = new long[Type.values().length];

        void add(Result result)
        {
            ++tries;
            if (result.pages > 1)
                ++multiPage;
            relays += result.relays;
            paddingBytes += result.paddingBytes;
            oversized += result.oversizedLogicalNodes;
            smallNodesCrossingPages += result.smallNodesCrossingPages;
            needlessRelays += result.needlessRelays;
            maxPointerBytes = Math.max(maxPointerBytes, result.maxPointerBytes);
            for (Type type : Type.values())
                byType[type.ordinal()] += result.count(type);
        }

        long count(Type type)
        {
            return byType[type.ordinal()];
        }

        @Override
        public String toString()
        {
            return String.format("%d tries, %d over a page, %d relays (%d needless), %d padding bytes, %d logical nodes over a page, " +
                                 "%d small nodes crossing pages, nodes by type %s",
                                 tries, multiPage, relays, needlessRelays, paddingBytes, oversized, smallNodesCrossingPages, Arrays.toString(byType));
        }
    }

    // Validation of the verifier

    static byte[] hex(String spaced)
    {
        return Hex.hexToBytes(spaced.replace(" ", ""));
    }

    /// A node with no children holding `length` bytes of the given value: a leaf node if that fits one, otherwise a
    /// generic content node with descent-side content, whose length is a reversed variable-length integer.
    static byte[] content(int value, int length)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < length; ++i)
            out.write(value);
        if (length < 64)
            out.write(length);
        else
        {
            if (length < 128)
                out.write(length);
            else
            {
                assertTrue(length < 1 << 14);
                out.write(length & 0xFF);
                out.write(0x80 | (length >> 8));
            }
            out.write(0xF4);
        }
        return out.toByteArray();
    }

    static byte[] join(byte[]... parts)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts)
            out.write(part, 0, part.length);
        return out.toByteArray();
    }

    static Result verify(byte[] data, int pageSize)
    {
        OnDiskTrieVerifier verifier = new OnDiskTrieVerifier(data);
        if (pageSize > 0)
            verifier.pages(pageSize, PADDING);
        return verifier.verify(data.length);
    }

    static void assertEntry(Result result, int index, String key, boolean ascent, byte[] content)
    {
        OnDiskTrieVerifier.Entry entry = result.entries.get(index);
        assertEquals("Key of entry " + index, key, new String(entry.key, StandardCharsets.ISO_8859_1));
        assertEquals("Side of entry " + index, ascent, entry.ascent);
        assertArrayEquals("Content of entry " + index, content, entry.content);
    }

    static OnDiskTrieVerifier.Node only(Result result, Type type)
    {
        OnDiskTrieVerifier.Node found = null;
        for (OnDiskTrieVerifier.Node node : result.allNodes)
            if (node.type == type)
            {
                assertEquals("More than one " + type + " node", null, found);
                found = node;
            }
        assertNotNull("No " + type + " node", found);
        return found;
    }

    /// The examples of `OnDiskTrie.md`, completed where necessary with the nodes they point to.
    @Test
    public void testVerifierOnDocumentedExamples()
    {
        // tractor -> 01, tree -> 02, trie -> 03
        Result example = verify(hex("03 01 65 40  02 01 65 40  01 01 72 6F 74 63 43  0B 07 69 65 61 84  72 74 41"), 0);
        example.assertValid("Documented example");
        assertEquals(8, example.nodes);
        assertEquals(3, example.count(Type.LEAF));
        assertEquals(4, example.count(Type.CHAIN));
        assertEquals(1, example.count(Type.SPARSE));
        assertEquals(3, example.entries.size());
        assertEntry(example, 0, "tractor", false, new byte[]{ 1 });
        assertEntry(example, 1, "tree", false, new byte[]{ 2 });
        assertEntry(example, 2, "trie", false, new byte[]{ 3 });
        assertEquals(7, example.maxKeyLength);
        assertEquals(0, example.relays);

        // a prefix node with content on both sides over a single-transition chain, completed with a leaf
        Result prefix = verify(hex("05 01  40 40  02 01 02 01 F7"), 0);
        prefix.assertValid("Prefix example");
        assertEquals(3, prefix.nodes);
        assertEquals(3, prefix.entries.size());
        assertEntry(prefix, 0, "", false, new byte[]{ 2 });
        assertEntry(prefix, 1, "@", false, new byte[]{ 5 });
        assertEntry(prefix, 2, "", true, new byte[]{ 2 });
        assertEquals(1, prefix.ascentContents);

        // The sparse node "04 01 82 00 33 32 31 85" at 0x20E: 1 -> 0x206, 2 -> 0x184, 3 -> 0x102.
        byte[] sparseExample = join(content(0xC3, 255),                         // [0, 0x102)
                                    content(0xB2, 126), hex("78 40"),           // [0x102, 0x184)
                                    content(0xA1, 126), hex("79 40"),           // [0x184, 0x206)
                                    hex("04 01 82 00 33 32 31 85"));
        assertEquals(0x20E, sparseExample.length);
        Result sparse = verify(sparseExample, 0);
        sparse.assertValid("Sparse example");
        OnDiskTrieVerifier.Node sparseNode = only(sparse, Type.SPARSE);
        assertEquals(2, sparseNode.pointerBytes);
        assertArrayEquals(new long[]{ 0x206, 0x184, 0x102 }, sparseNode.targets);
        assertArrayEquals(new byte[]{ 0x31, 0x32, 0x33 }, sparseNode.transitions);
        assertEntry(sparse, 0, "1y", false, repeat(0xA1, 126));
        assertEntry(sparse, 1, "2x", false, repeat(0xB2, 126));
        assertEntry(sparse, 2, "3", false, repeat(0xC3, 255));
        assertEquals(2, sparse.maxPointerBytes);

        // The relay example "11 01 F9 03 62 61 80" at 0x1235: a -> 0x111D through the relay, b -> 0x122E.
        byte[] relayExample = join(content(0xAA, 4378),                         // [0, 0x111D)
                                   content(0xBB, 270),                          // [0x111D, 0x122E)
                                   hex("11 01 F9  03 62 61 80"));
        assertEquals(0x1235, relayExample.length);
        for (int pageSize : new int[]{ 0, 4096 })
        {
            Result relay = verify(relayExample, pageSize);
            relay.assertValid("Relay example");
            assertEquals(1, relay.relays);
            assertArrayEquals(new long[]{ 0x111D }, only(relay, Type.RELAY).targets);
            assertArrayEquals(new long[]{ 0x1231, 0x122E }, only(relay, Type.SPARSE).targets);
            assertEntry(relay, 0, "a", false, repeat(0xAA, 4378));
            assertEntry(relay, 1, "b", false, repeat(0xBB, 270));
            assertEquals(0, relay.paddingBytes);
            // the content of "a" is larger than a page
            assertEquals(pageSize == 0 ? 0 : 1, relay.oversizedLogicalNodes);
            assertEquals(0, relay.smallNodesCrossingPages);
        }
    }

    /// The bitmap node of `OnDiskTrie.md` for the transitions A to Z, with a leaf for each, and a dense node with
    /// two-byte pointers that has no child for the first 16 transitions.
    @Test
    public void testVerifierOnWideNodes()
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int t = 'Z'; t >= 'A'; --t)
        {
            out.write(t);
            out.write(0x01);
        }
        int base = out.size();
        for (int t = 'Z'; t > 'A'; --t)
            out.write(base - 2 * ('Z' - t + 1));
        byte[] bitmap = hex("00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 07 FF FF FE 00 00 00 00 00 00 00 00 E0");
        out.write(bitmap, 0, bitmap.length);
        Result result = verify(out.toByteArray(), 0);
        result.assertValid("Bitmap");
        assertEquals(1, result.count(Type.BITMAP));
        assertEquals(26, result.count(Type.LEAF));
        assertEquals(26, result.entries.size());
        for (int i = 0; i < 26; ++i)
            assertEntry(result, i, String.valueOf((char) ('A' + i)), false, new byte[]{ (byte) ('A' + i) });

        out = new ByteArrayOutputStream();
        for (int t = 0xFF; t >= 0x10; --t)
        {
            out.write(t);
            out.write(0x01);
        }
        base = out.size();
        assertEquals(480, base);
        for (int t = 0xFF; t >= 0; --t)
        {
            int delta = t >= 0x10 ? base - 2 * (0xFF - t + 1) : 0xFFFF;
            out.write(delta & 0xFF);
            out.write(delta >> 8);
        }
        out.write(0xE9);
        result = verify(out.toByteArray(), 0);
        result.assertValid("Dense");
        assertEquals(1, result.count(Type.DENSE));
        assertEquals(240, result.entries.size());
        for (int i = 0; i < 240; ++i)
        {
            assertArrayEquals(new byte[]{ (byte) (0x10 + i) }, result.entries.get(i).key);
            assertArrayEquals(new byte[]{ (byte) (0x10 + i) }, result.entries.get(i).content);
        }
        assertEquals(2, result.maxPointerBytes);
        assertEquals(0, result.nodesWithOversizedPointers);
    }

    /// Pages of 8 bytes: the leaf of "a" alone in the first, padded; the leaf of "b", a relay to "a" and the sparse
    /// node that lists the two in the second.
    private static final String PAGED_EXAMPLE = "AA 01 FF FF FF FF FF FF  BB 01  08 F8  02 62 61 80";

    @Test
    public void testVerifierOnPaddedPages()
    {
        Result result = verify(hex(PAGED_EXAMPLE), 8);
        result.assertValid("Paged example");
        assertEquals(1, result.relays);
        assertEquals(0, result.explicitlyReferencedRelays);
        assertEquals(6, result.paddingBytes);
        assertEquals(1, result.paddingRuns);
        assertEquals(2, result.pages);
        assertEquals(3, result.logicalNodes);
        assertEquals(0, result.nodesCrossingPages);
        assertEntry(result, 0, "a", false, new byte[]{ (byte) 0xAA });
        assertEntry(result, 1, "b", false, new byte[]{ (byte) 0xBB });
        // only the path to "a" leaves the page of the root
        assertEquals(2, result.leafPaths);
        assertEquals(1, result.pathPageCrossings);
        assertEquals(1, result.maxPathPageCrossings);
        // the relay is necessary: the two leaves and the node do not fit a page
        assertEquals(0, result.needlessRelays);

        // the same at an offset of two pages, which must not be looked at
        byte[] shifted = join(repeat(0x00, 16), hex(PAGED_EXAMPLE));
        result = new OnDiskTrieVerifier(shifted).from(16).pages(8, PADDING).verify(shifted.length);
        result.assertValid("Shifted paged example");
        assertEquals(6, result.paddingBytes);
        assertEquals(2, result.pages);

        // The same with 0x100 bytes of something else between the two pages, which are left out of what the verifier
        // is given: the leaf of "b" is now at 0x10A and followed by padding, the relay begins at 0x110 and needs two
        // bytes for the distance from there to 2, and the sparse node begins at 0x113.
        byte[] withForeign = hex("AA 01 FF FF FF FF FF FF  BB 01 FF FF FF FF FF FF  0E 01 F9  09 62 61 80");
        result = new OnDiskTrieVerifier(withForeign).pages(8, PADDING).withForeignData(8, 0x108).verify(0x117);
        result.assertValid("Paged example with foreign data");
        assertEquals(1, result.relays);
        assertEquals(12, result.paddingBytes);
        assertEquals(2, result.paddingRuns);
        assertArrayEquals(new long[]{ 2 }, only(result, Type.RELAY).targets);
        assertArrayEquals(new long[]{ 0x113, 0x10A }, only(result, Type.SPARSE).targets);
        assertEntry(result, 0, "a", false, new byte[]{ (byte) 0xAA });
        assertEntry(result, 1, "b", false, new byte[]{ (byte) 0xBB });

        // a relay that leads into the foreign data, and a node that begins in it
        result = new OnDiskTrieVerifier(hex("AA 01 FF FF FF FF FF FF  BB 01 FF FF FF FF FF FF  C0 00 F9  09 62 61 80"))
                 .pages(8, PADDING).withForeignData(8, 0x108).verify(0x117);
        assertFalse(result.isValid());
        assertTrue(result.describeProblems(), result.describeProblems().contains("leads into foreign data"));
        result = new OnDiskTrieVerifier(hex("AA 01 FF FF FF FF FF FF  BB 05 FF FF FF FF FF FF  0E 01 F9  09 62 61 80"))
                 .pages(8, PADDING).withForeignData(8, 0x108).verify(0x117);
        assertFalse(result.isValid());
        assertTrue(result.describeProblems(), result.describeProblems().contains("extends over foreign data"));
        // without the padding before the foreign data, which must end at a page boundary like any other
        result = new OnDiskTrieVerifier(hex("AA 01 FF FF FF  BB 01  05 01 F9  03 62 61 80")).pages(8, PADDING).withForeignData(5, 0x105).verify(0x10E);
        assertFalse(result.isValid());
        assertTrue(result.describeProblems(), result.describeProblems().contains("does not end at a page boundary"));
    }

    private static void assertRejected(String what, String data, int pageSize, String expectedProblem)
    {
        Result result = verify(hex(data), pageSize);
        assertFalse(what + " must be rejected", result.isValid());
        assertTrue(what + " must be rejected because of '" + expectedProblem + "' but got: " + result.describeProblems(),
                   result.describeProblems().contains(expectedProblem));
    }

    @Test
    public void testVerifierRejectsBrokenFiles()
    {
        verify(hex(PAGED_EXAMPLE), 8).assertValid("Unmodified");

        assertRejected("Padding in data without pages",
                       PAGED_EXAMPLE, 0, "not part of any reachable node");
        assertRejected("Padding of other bytes",
                       "AA 01 FF FF FF 00 FF FF  BB 01  08 F8  02 62 61 80", 8, "are not padding");
        assertRejected("Padding that stops short of the page boundary",
                       "AA 01 FF FF FF FF  BB 01  06 F8  02 62 61 80", 8, "does not end at a page boundary");
        assertRejected("Pointer into the middle of a node",
                       "AA 01 FF FF FF FF FF FF  BB 01  08 F8  01 62 61 80", 8, "overlaps");
        assertRejected("Relay into the middle of a node",
                       "AA 01 FF FF FF FF FF FF  BB 01  09 F8  02 62 61 80", 8, "before the start of the data");
        assertRejected("Relay to where it is",
                       "AA 01 FF FF FF FF FF FF  BB 01  00 F8  02 62 61 80", 8, "has the distance 0");
        assertRejected("Pointer before the start of the data",
                       "AA 01 FF FF FF FF FF FF  BB 01  08 F8  0C 62 61 80", 8, "leads outside the data");
        assertRejected("Transitions in the wrong order",
                       "AA 01 FF FF FF FF FF FF  BB 01  08 F8  02 61 62 80", 8, "not in strictly descending order");
        assertRejected("Repeated transition",
                       "AA 01 FF FF FF FF FF FF  BB 01  08 F8  02 61 61 80", 8, "not in strictly descending order");
        assertRejected("Node referenced twice",
                       "AA 01 FF FF FF FF FF FF  BB 01  08 F8  0A 62 61 80", 8, "already referenced");
        assertRejected("Unreferenced node",
                       "AA 01 CC 01 FF FF FF FF  BB 01  08 F8  02 62 61 80", 8, "are not padding");
        assertRejected("Prefix node without content",
                       "AA 01 FF FF FF FF FF FF  BB 01  08 F8  02 62 61 80 F1", 8, "has no content");
        assertRejected("Prefix node over a leaf",
                       "AA 01 01 01 F5", 0, "cannot list the children");
        assertRejected("Prefix node over a prefix node",
                       "AA 01 F4 01 01 F5", 0, "cannot list the children");
        assertRejected("Prefix node over a relay to a leaf",
                       "AA 01 FF FF FF FF FF FF  06 F8  01 01 F5", 8, "cannot list the children");
        assertRejected("Prefix node over a relay to a prefix node",
                       "AA 01 F4 FF FF FF FF FF  05 F8  01 01 F5", 8, "cannot list the children");
        // "a" is a relay to 6, where there is a relay to the leaf at 2
        assertRejected("Relay to a relay",
                       "AA 01  BB 01  02 F8  CC 01  02 F8  02 06 63 62 61 84", 0, "leads to another relay");
        assertRejected("Content longer than the data",
                       "AA 09 F4", 0, "does not fit");
        assertRejected("Chain longer than the data",
                       "AA 01 61 45", 0, "before the start of the data");

        // A leaf of two bytes that straddles the boundary between pages of 8, which is only a problem of layout.
        Result crossing = verify(hex("11 11 11 11 11 11 06  BB 01  02 62 61 80"), 8);
        crossing.assertStructurallyValid("Crossing leaf");
        assertFalse(crossing.isValid());
        assertEquals(1, crossing.layoutErrors.size());
        assertEquals(1, crossing.smallNodesCrossingPages);
        verify(hex("11 11 11 11 11 11 06  BB 01  02 62 61 80"), 16).assertValid("The same in bigger pages");

        // A child of a dense node at a distance equal to the marker for an absent child: the node at [0, 2) is at
        // distance 0xFF from the dense node that starts at 0x101.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] children = join(hex("59 01"), content(0x58, 250), hex("5A 01"));
        assertEquals(0x101, children.length);
        out.write(children, 0, children.length);
        for (int t = 0xFF; t >= 0; --t)
            out.write(t == 0 ? 0xFF : t == 1 ? 0x02 : t == 2 ? 0x00 : 0xFF);
        out.write(0xE8);
        Result dense = verify(out.toByteArray(), 0);
        assertFalse(dense.isValid());
        assertTrue(dense.describeProblems(), dense.describeProblems().contains("the bytes [0,2) are not part of any reachable node"));
    }

    /// The output of the immediate writer has no relays, no padding and no byte that is not part of a reachable node,
    /// and the verifier finds in it exactly the content of the trie.
    @Test
    public void testVerifierOnImmediateOutput() throws IOException
    {
        Random rand = new Random(SEED);
        Totals totals = new Totals();
        for (int i = 0; i < 60; ++i)
        {
            boolean ordered = i % 2 == 0;
            Trie<String> trie = i % 3 == 0 ? generatedKeysTrie(rand, ordered, 1 + rand.nextInt(300))
                                           : randomTrie(rand, ordered, 1 + rand.nextInt(i < 30 ? 20 : 1000));
            Result result = check("Immediate, seed " + SEED + " trie " + i, trie, ordered, writeImmediate(trie, ordered));
            assertEquals(0, result.relays);
            assertEquals(0, result.paddingBytes);
            assertEquals(0, result.nodesWithOversizedPointers);
            totals.add(result);
        }
        for (int fanout : new int[]{ 2, 25, 26, 100, 239, 240, 256 })
        {
            Trie<String> trie = wideTrie(false, bytes("w"), fanout, fanout % 2 == 0, fanout % 3, true, 3);
            totals.add(check("Immediate, fanout " + fanout, trie, false, writeImmediate(trie, false)));
        }
        for (int i = 0; i < 30; ++i)
        {
            RangeTrie<TestRangeState> trie = randomRangeTrie(rand, 1 + rand.nextInt(i < 10 ? 5 : 200));
            Written written = write(trie, RANGE, RANGE_SERDE, OnDiskNodeWriter.Type.IMMEDIATE, 0, 0);
            Result result = checkRange("Immediate, seed " + SEED + " range trie " + i, trie, written);
            assertEquals(0, result.relays);
            totals.add(result);
        }
        for (Type type : Type.values())
            assertEquals(totals.toString(), type != Type.RELAY, totals.count(type) > 0);
        assertEquals(0, totals.relays);
        assertEquals(0, totals.paddingBytes);
    }

    // Byte identity with the immediate writer

    /// A page large enough for no layout to be necessary.
    private static final int HUGE_PAGE = 1 << 22;

    /// When nothing needs to be laid out and chains are not folded, the page-aware writer must write exactly what the
    /// immediate writer does, wherever in the page the trie starts.
    private static <T> void assertSameAsImmediate(String message, BaseTrie<T, ?, ?> trie, ContentStyle contentStyle, OnDiskTrieWriter.DataSerializer<T> serializer) throws IOException
    {
        for (int startOffset : new int[]{ 0, 13 })
        {
            String at = message + " at offset " + startOffset;
            Written immediate = write(trie, contentStyle, serializer, OnDiskNodeWriter.Type.IMMEDIATE, 0, startOffset);
            assertTrue(at, immediate.root < HUGE_PAGE);
            Written unfolded = write(trie, contentStyle, serializer, PAGE_AWARE_WITHOUT_FOLDING, HUGE_PAGE, startOffset);
            assertEquals(at, immediate.root, unfolded.root);
            if (immediate.root == 0)
                continue;
            assertArrayEquals(at, immediate.bytes(), unfolded.bytes());

            // Folding chains can only remove single-transition chain nodes.
            Written folded = write(trie, contentStyle, serializer, OnDiskNodeWriter.Type.PAGE_AWARE, HUGE_PAGE, startOffset);
            assertTrue(at, folded.size() <= immediate.size());
            Result foldedResult = folded.verify();
            foldedResult.assertValid(at);
            assertEquals(at, 0, foldedResult.relays);
            assertEquals(at, 0, foldedResult.paddingBytes);
        }
    }

    @Test
    public void testByteIdenticalToImmediateWithoutFolding() throws IOException
    {
        Random rand = new Random(SEED + 1);
        for (int i = 0; i < 150; ++i)
        {
            boolean ordered = i % 2 == 0;
            Trie<String> trie = i % 3 == 0 ? generatedKeysTrie(rand, ordered, 1 + rand.nextInt(300))
                                           : randomTrie(rand, ordered, rand.nextInt(i < 50 ? 20 : 1000));
            assertSameAsImmediate("Seed " + SEED + " trie " + i, trie, plainStyle(ordered), STRING_SERDE);
        }
        for (int i = 0; i < 80; ++i)
            assertSameAsImmediate("Seed " + SEED + " range trie " + i, randomRangeTrie(rand, 1 + rand.nextInt(i < 20 ? 5 : 300)), RANGE, RANGE_SERDE);

        int variant = 0;
        for (int fanout : FANOUTS)
            for (int below = 0; below < 3; ++below)
            {
                ++variant;
                Trie<String> trie = wideTrie(variant % 2 == 0, variant % 3 == 0 ? new byte[0] : bytes("pq"), fanout, variant % 4 < 2, below, variant % 5 < 3, variant % 7 * 12);
                assertSameAsImmediate("Fanout " + fanout + " variant " + variant, trie, plainStyle(variant % 2 == 0), STRING_SERDE);
            }
        for (int length : CHAIN_LENGTHS)
            for (int shape = 0; shape < CHAIN_SHAPES; ++shape)
                for (boolean ordered : new boolean[]{ false, true })
                    assertSameAsImmediate("Chain " + length + " shape " + shape, chainTrie(ordered, length, shape), plainStyle(ordered), STRING_SERDE);
        for (boolean ordered : new boolean[]{ false, true })
        {
            assertSameAsImmediate("Empty", newTrie(ordered), plainStyle(ordered), STRING_SERDE);
            assertSameAsImmediate("Root only", trie(ordered, "", "root"), plainStyle(ordered), STRING_SERDE);
            assertSameAsImmediate("Path", pathTrie(ordered, 100), plainStyle(ordered), STRING_SERDE);
            assertSameAsImmediate("Comb", combTrie(ordered, 100, 'c'), plainStyle(ordered), STRING_SERDE);
            assertSameAsImmediate("Payloads", payloadTrie(ordered, 4096), plainStyle(ordered), STRING_SERDE);
        }
    }

    // Round trips and layout invariants for various shapes

    private static void assertExercisesPaging(String message, Totals totals)
    {
        if (VERBOSE)
            System.out.println(message + ": " + totals);
        assertTrue(message + " must have tries of more than one page: " + totals, totals.multiPage > 0);
        assertTrue(message + " must have relays: " + totals, totals.relays > 0);
        assertTrue(message + " must have padding: " + totals, totals.paddingBytes > 0);
    }

    @Test
    public void testRandomTries() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Random rand = new Random(SEED + pageSize);
            Totals totals = new Totals();
            for (int i = 0; i < 60; ++i)
            {
                boolean ordered = i % 2 == 0;
                int count = 1 + rand.nextInt(i < 20 ? 20 : i < 50 ? 300 : 2000);
                Trie<String> trie = i % 3 == 0 ? generatedKeysTrie(rand, ordered, Math.min(count, 500))
                                               : randomTrie(rand, ordered, count);
                totals.add(writeAndCheck("Seed " + SEED + " trie " + i, trie, ordered, pageSize));
            }
            assertExercisesPaging("Random tries with page size " + pageSize, totals);
            assertTrue(totals.toString(), totals.count(Type.SPARSE) > 0 && totals.count(Type.BITMAP) > 0 && totals.count(Type.PREFIX) > 0);
        }
    }

    @Test
    public void testRandomRangeTries() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Random rand = new Random(SEED + pageSize);
            Totals totals = new Totals();
            for (int i = 0; i < 60; ++i)
            {
                RangeTrie<TestRangeState> trie = randomRangeTrie(rand, 1 + rand.nextInt(i < 20 ? 6 : i < 50 ? 100 : 1500));
                Written written = write(trie, RANGE, RANGE_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, 0);
                totals.add(checkRange("Seed " + SEED + " range trie " + i + " page size " + pageSize, trie, written));
            }
            assertExercisesPaging("Random range tries with page size " + pageSize, totals);
        }
    }

    private static final int[] FANOUTS = { 2, 3, 24, 25, 26, 27, 64, 128, 200, 223, 224, 225, 230, 239, 240, 241, 255, 256 };

    /// Nodes with every number of children at which the node type changes, as the root and below it, with content
    /// on them or not, over leaves, chains and further nodes, with content that fits a leaf node or does not.
    @Test
    public void testWideNodes() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Totals totals = new Totals();
            int variant = 0;
            for (int fanout : FANOUTS)
                for (int below = 0; below < 3; ++below)
                    for (int valueLength : new int[]{ 0, 5, 70 })
                    {
                        ++variant;
                        boolean ordered = variant % 2 == 0;
                        Trie<String> trie = wideTrie(ordered, variant % 3 == 0 ? new byte[0] : bytes("pq"), fanout, variant % 4 < 2, below, variant % 5 < 3, valueLength);
                        totals.add(writeAndCheck("Fanout " + fanout + " variant " + variant, trie, ordered, pageSize));
                    }
            assertExercisesPaging("Wide nodes with page size " + pageSize, totals);
            assertTrue(totals.toString(), totals.count(Type.SPARSE) > 0 && totals.count(Type.BITMAP) > 0 && totals.count(Type.DENSE) > 0);
            // Bitmap and dense nodes do not fit the smaller pages.
            assertEquals(totals.toString(), pageSize <= 256, totals.oversized > 0);
        }
    }

    private static final int[] CHAIN_LENGTHS = { 1, 2, 31, 32, 33, 62, 63, 64, 65, 66, 100, 127, 128, 129, 200 };
    private static final int CHAIN_SHAPES = 6;

    /// Tries with a chain of the given number of transitions:
    /// 0. as the whole trie
    /// 1. below a node with other children
    /// 2. above a node with several children
    /// 3. with content in the middle
    /// 4. below a node with content, which is where the page-aware writer folds chains
    /// 5. between two nodes with content and other children
    static InMemoryTrie<String> chainTrie(boolean ordered, int length, int shape)
    {
        byte[] chain = new byte[length];
        for (int i = 0; i < length; ++i)
            chain[i] = (byte) (i * 37 + 11);
        InMemoryTrie<String> trie = newTrie(ordered);
        switch (shape)
        {
            case 0:
                put(trie, chain, "end");
                break;
            case 1:
                put(trie, join(bytes("a"), chain), "end");
                put(trie, bytes("b"), "sibling");
                put(trie, bytes("c"), "other");
                break;
            case 2:
                put(trie, join(chain, bytes("a")), "one");
                put(trie, join(chain, bytes("b")), "two");
                put(trie, join(chain, bytes("c")), "three");
                break;
            case 3:
                put(trie, Arrays.copyOf(chain, length / 2), "middle");
                put(trie, chain, "end");
                break;
            case 4:
                put(trie, bytes("p"), "parent");
                put(trie, join(bytes("p"), chain), "end");
                break;
            default:
                put(trie, bytes("p"), "parent");
                put(trie, bytes("q"), "sibling");
                put(trie, join(bytes("p"), chain), "end");
                put(trie, join(bytes("p"), chain, bytes("x")), "below");
                put(trie, join(bytes("p"), chain, bytes("y")), "below too");
                break;
        }
        return trie;
    }

    @Test
    public void testLongChains() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Totals totals = new Totals();
            for (int length : CHAIN_LENGTHS)
                for (int shape = 0; shape < CHAIN_SHAPES; ++shape)
                    for (boolean ordered : new boolean[]{ false, true })
                    {
                        Trie<String> trie = chainTrie(ordered, length, shape);
                        Result result = writeAndCheck("Chain " + length + " shape " + shape, trie, ordered, pageSize);
                        if (shape == 0)
                        {
                            // only the chain, in pieces of at most 64 transitions, and the leaf
                            assertEquals((length + 63) / 64, result.count(Type.CHAIN));
                            assertEquals(length / 64, result.chainNodesOfFullLength);
                            assertEquals(result.count(Type.CHAIN) + 1, result.nodes);
                        }
                        totals.add(result);
                    }
            assertTrue(totals.toString(), totals.count(Type.CHAIN) > 0);
            if (pageSize <= 64)
            {
                // chain nodes of the maximum length do not fit a page
                assertExercisesPaging("Chains with page size " + pageSize, totals);
                assertTrue(totals.toString(), totals.oversized > 0);
            }
        }
    }

    /// A trie which is a single path with content at each of its `depth + 1` nodes.
    static InMemoryTrie<String> pathTrie(boolean ordered, int depth)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        for (int i = 0; i <= depth; ++i)
            putNonRecursive(trie, repeat('b', i), "" + (char) ('0' + i % 10));
        return trie;
    }

    /// A trie which is a single path with content at each of its nodes, and from each of them a leaf reached with the
    /// given transition.
    static InMemoryTrie<String> combTrie(boolean ordered, int depth, int leafTransition)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        for (int i = 0; i <= depth; ++i)
        {
            putNonRecursive(trie, repeat('b', i), "" + (char) ('0' + i % 10));
            if (i < depth)
                putNonRecursive(trie, concat(repeat('b', i), leafTransition), "" + (char) ('A' + i % 26));
        }
        return trie;
    }

    /// Long paths of nodes that all have content, with and without further children.
    @Test
    public void testDeepTries() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Totals totals = new Totals();
            for (int depth : new int[]{ 20, 127, 128, 129, 130, 300 })
                for (boolean ordered : new boolean[]{ false, true })
                {
                    Result path = writeAndCheck("Path of depth " + depth, pathTrie(ordered, depth), ordered, pageSize);
                    assertEquals(depth + 1, path.maxLogicalDepth);
                    totals.add(path);
                    totals.add(writeAndCheck("Comb of depth " + depth + " with leaves after", combTrie(ordered, depth, 'c'), ordered, pageSize));
                    totals.add(writeAndCheck("Comb of depth " + depth + " with leaves before", combTrie(ordered, depth, 'a'), ordered, pageSize));
                }
            if (pageSize <= 256)
                assertExercisesPaging("Deep tries with page size " + pageSize, totals);
        }
    }

    /// Content sizes on either side of the given page size, and of several pages.
    static int[] payloadSizes(int pageSize)
    {
        return new int[]{ pageSize - 4, pageSize - 3, pageSize - 2, pageSize - 1, pageSize, pageSize + 1, pageSize + 2,
                          2 * pageSize - 2, 2 * pageSize, 2 * pageSize + 1, 3 * pageSize + 5, 7 * pageSize - 3 };
    }

    /// A trie with content of all of the sizes above, on leaves and on nodes with children, mixed with small content.
    static InMemoryTrie<String> payloadTrie(boolean ordered, int pageSize)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        int[] sizes = payloadSizes(pageSize);
        for (int i = 0; i < sizes.length; ++i)
        {
            byte[] key = concat(bytes("k"), 'a' + i);
            put(trie, key, "P".repeat(sizes[i]));
            put(trie, concat(key, 'x'), "small");
            if (i % 2 == 0)
                put(trie, concat(key, 'y', 'z'), "Q".repeat(sizes[sizes.length - 1 - i]));
            put(trie, concat(bytes("l"), 'a' + i), "tiny" + i);
        }
        return trie;
    }

    @Test
    public void testBigPayloads() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Totals totals = new Totals();
            for (boolean ordered : new boolean[]{ false, true })
            {
                for (int size : payloadSizes(pageSize))
                {
                    // as the whole trie, at the root and below a chain, and next to a sibling
                    Result result = writeAndCheck("Root with " + size + " bytes", trie(ordered, "", "R".repeat(size)), ordered, pageSize);
                    assertEquals(1, result.nodes);
                    totals.add(result);
                    totals.add(writeAndCheck("Leaf with " + size + " bytes", trie(ordered, "key", "L".repeat(size)), ordered, pageSize));
                    totals.add(writeAndCheck("Leaf with " + size + " bytes and sibling", trie(ordered, "ka", "L".repeat(size), "kb", "s"), ordered, pageSize));
                    totals.add(writeAndCheck("Node with " + size + " bytes", trie(ordered, "k", "N".repeat(size), "ka", "a", "kb", "b"), ordered, pageSize));
                }
                totals.add(writeAndCheck("All sizes", payloadTrie(ordered, pageSize), ordered, pageSize));
            }
            assertExercisesPaging("Payloads with page size " + pageSize, totals);
            assertTrue(totals.toString(), totals.oversized > 0);
        }
    }

    // Chain folding

    /// One byte per value, to write expected layouts out by hand.
    static final OnDiskTrieWriter.DataSerializer<String> ONE_BYTE_SERIALIZER = new OnDiskTrieWriter.DataSerializer<String>()
    {
        @Override
        public void serialize(DataOutputPlus out, String value) throws IOException
        {
            out.writeByte(Integer.parseInt(value));
        }
    };

    /// A node with content and a single child gives the transition to its child, if the child is reached by a chain.
    /// Compare with `OnDiskTrieWriterTest.testSingleChildPrefix`, which pins what the immediate writer does for the
    /// same trie.
    @Test
    public void testChainFolding() throws IOException
    {
        Trie<String> trie = trie(false, "abc", "1", "abcdef", "2");
        Written immediate = write(trie, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.IMMEDIATE, 0, 0);
        Written unfolded = write(trie, UNORDERED, ONE_BYTE_SERIALIZER, PAGE_AWARE_WITHOUT_FOLDING, 4096, 0);
        Written folded = write(trie, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.PAGE_AWARE, 4096, 0);
        assertEquals("02 01  66 65 41  64 40  01 01 f5  63 62 61 42".replace(" ", ""), Hex.bytesToHex(immediate.bytes()));
        assertEquals("02 01  66 65 41  64 40  01 01 f5  63 62 61 42".replace(" ", ""), Hex.bytesToHex(unfolded.bytes()));
        // the chain "ef" becomes "def" and the chain node for "d" disappears
        assertEquals("02 01  66 65 64 42  01 01 f5  63 62 61 42".replace(" ", ""), Hex.bytesToHex(folded.bytes()));
        assertEquals(immediate.size() - 1, folded.size());

        Result result = folded.verify();
        result.assertValid("Folded");
        assertEquals(2, result.count(Type.CHAIN));
        assertEquals(4, result.nodes);
        assertEntry(result, 0, "abc", false, new byte[]{ 1 });
        assertEntry(result, 1, "abcdef", false, new byte[]{ 2 });

        // the same with the content read back
        for (boolean ordered : new boolean[]{ false, true })
            for (int pageSize : PAGE_SIZES)
            {
                Trie<String> strings = trie(ordered, "abc", "one", "abcdef", "two");
                Result read = writeAndCheck("Folded", strings, ordered, pageSize);
                assertEquals(2, read.count(Type.CHAIN));
                assertEquals(writeImmediate(strings, ordered).size() - 1, read.root);
            }

        // A child that is directly below its parent has no chain to add the transition to.
        Trie<String> direct = trie(false, "abc", "1", "abcd", "2");
        assertArrayEquals(write(direct, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.IMMEDIATE, 0, 0).bytes(),
                          write(direct, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.PAGE_AWARE, 4096, 0).bytes());
        // Neither is anything folded into one of several children.
        Trie<String> several = trie(false, "abc", "1", "abcdef", "2", "abcxyz", "3");
        assertArrayEquals(write(several, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.IMMEDIATE, 0, 0).bytes(),
                          write(several, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.PAGE_AWARE, 4096, 0).bytes());
    }

    /// Folding repeats at every level: each node of a path of nodes with content gives its transition to the next.
    @Test
    public void testChainFoldingAtEveryLevel() throws IOException
    {
        for (boolean ordered : new boolean[]{ false, true })
        {
            InMemoryTrie<String> trie = newTrie(ordered);
            int levels = 40;
            // a single path with content after every third transition
            byte[] path = new byte[3 * levels];
            for (int i = 0; i < path.length; ++i)
                path[i] = (byte) ('a' + i / 3 % 3);
            for (int i = 1; i <= levels; ++i)
                put(trie, Arrays.copyOf(path, 3 * i), "level" + i);
            long immediateSize = writeImmediate(trie, ordered).size();
            for (int pageSize : PAGE_SIZES)
            {
                Result result = writeAndCheck("Folding at every level", trie, ordered, pageSize);
                if (pageSize == 4096)
                {
                    // all in one page: every node but the deepest saves the chain node of its single transition
                    assertEquals(0, result.relays);
                    assertEquals(immediateSize - (levels - 1), result.root);
                }
            }
        }
    }

    /// The length of the child's chain with the folded transition added to it goes from 64, which is one chain node,
    /// to 65, which is two.
    @Test
    public void testChainFoldingAtMaximumChainLength() throws IOException
    {
        for (int childChain : new int[]{ 1, 2, 62, 63, 64, 65, 126, 127, 128, 129 })
            for (boolean ordered : new boolean[]{ false, true })
            {
                // "p" has content and a single child, reached by one transition and then a chain of the given length
                InMemoryTrie<String> trie = newTrie(ordered);
                put(trie, bytes("p"), "parent");
                put(trie, join(bytes("pq"), repeat('c', childChain)), "child");

                String message = "Child chain of " + childChain;
                Written immediate = writeImmediate(trie, ordered);
                Result immediateResult = check(message, trie, ordered, immediate);
                Written folded = write(trie, plainStyle(ordered), STRING_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, 4096, 0);
                Result foldedResult = check(message, trie, ordered, folded);

                // The immediate writer has the chain "p", the chain "q" and the child's chain in pieces of up to 64.
                assertEquals(message, 2 + (childChain + 63) / 64, immediateResult.count(Type.CHAIN));
                // The folded transition makes the child's chain one longer, and there is no chain "q".
                assertEquals(message, 1 + (childChain + 1 + 63) / 64, foldedResult.count(Type.CHAIN));
                // This saves a byte, unless the longer chain needs one more node.
                boolean extraNode = childChain % 64 == 0;
                assertEquals(message, immediate.size() - (extraNode ? 0 : 1), folded.size());
                if (extraNode)
                    assertArrayEquals(message, immediate.bytes(), folded.bytes());

                for (int pageSize : PAGE_SIZES)
                    writeAndCheck(message, trie, ordered, pageSize);
            }
    }

    /// A child that has taken over the transition from its parent and is then written in an earlier page than the
    /// parent: all that the parent has in place of its child is a relay.
    @Test
    public void testFoldedChildWrittenElsewhere() throws IOException
    {
        for (boolean ordered : new boolean[]{ false, true })
        {
            // With pages of 64 bytes: the child (41 bytes of leaf and the chain "wxyz", 5 bytes) and its parent "a"
            // (32 bytes of content) are over a page together, so the child is written when the parent completes. The
            // parent remains in memory for the root, whose branch (11 bytes for "b", 2 for the relay, 32 for "a" and 4
            // for the root) no longer fits what is left of the first page.
            Trie<String> trie = trie(ordered,
                                     "a", "A".repeat(30),
                                     "awxyz", "C".repeat(40),
                                     "b", "B".repeat(10));
            Result result = writeAndCheck("Folded child in an earlier page", trie, ordered, 64);
            assertEquals(113, result.root);
            assertEquals(18, result.paddingBytes);
            assertEquals(1, result.relays);
            assertEquals(1, result.relaysBelowPrefix);
            OnDiskTrieVerifier.Node relay = only(result, Type.RELAY);
            assertEquals(OnDiskTrieVerifier.Ref.PREFIX_CHILD, relay.referencedAs);
            assertArrayEquals(new long[]{ 46 }, relay.targets);
            // the child's chain, with the folded transition
            OnDiskTrieVerifier.Node chain = only(result, Type.CHAIN);
            assertEquals(46, chain.end);
            assertArrayEquals(bytes("wxyz"), chain.transitions);

            // Without folding the relay stands in for the child of the chain node of the transition.
            Written unfolded = write(trie, plainStyle(ordered), STRING_SERDE, PAGE_AWARE_WITHOUT_FOLDING, 64, 0);
            Result unfoldedResult = check("Unfolded child in an earlier page", trie, ordered, unfolded);
            assertEquals(1, unfoldedResult.relays);
            assertEquals(0, unfoldedResult.relaysBelowPrefix);
            assertEquals(OnDiskTrieVerifier.Ref.CHAIN_CHILD, only(unfoldedResult, Type.RELAY).referencedAs);
            assertEquals(2, unfoldedResult.count(Type.CHAIN));
        }
    }

    /// A node with a child for every transition whose furthest child is at a distance that is all-ones in the width
    /// of its pointers. A dense node would need wider pointers to tell that child from an absent one, so this is
    /// written as a bitmap node, with 256 children.
    @Test
    public void testBitmapNodeWithEveryTransition() throws IOException
    {
        for (boolean ordered : new boolean[]{ false, true })
        {
            InMemoryTrie<String> trie = newTrie(ordered);
            for (int t = 0; t < 256; ++t)
                put(trie, new byte[]{ (byte) t }, "");
            // In one page: 256 leaves of one byte, of which the furthest is 255 bytes from the node.
            Result result = writeAndCheck("Every transition", trie, ordered, 4096);
            assertEquals(1, result.count(Type.BITMAP));
            assertEquals(0, result.count(Type.DENSE));
            assertEquals(256 + 255 + 32 + 1, result.root);
            assertArrayEquals(writeImmediate(trie, ordered).bytes(), writePageAware(trie, ordered, 4096).bytes());
            // With smaller pages the node does not fit one, and its children are not all next to it.
            for (int pageSize : PAGE_SIZES)
                writeAndCheck("Every transition", trie, ordered, pageSize);
        }
    }

    // Branches that lead to nothing

    /// The value of the keys whose content [#hideDeadEnds] hides.
    static final String DEAD_END = "dead end";

    /// A view of the trie in which the keys with the value [#DEAD_END] have no content. The paths to them remain and
    /// are walked, but lead to nothing, as the paths an intersection follows along the boundary of its set do.
    static Trie<String> hideDeadEnds(Trie<String> trie)
    {
        return trie.mapValues(v -> v.equals(DEAD_END) ? null : v);
    }

    /// The trie without the keys with the value [#DEAD_END], i.e. with the same content as [#hideDeadEnds] and no
    /// path that leads to nothing. The content is taken from a walk in the direction in which tries are written.
    static InMemoryTrie<String> withoutDeadEnds(Trie<String> trie, boolean ordered)
    {
        InMemoryTrie<String> result = newTrie(ordered);
        trie.forEachEntry(Direction.REVERSE, (k, v) -> {
            if (!v.equals(DEAD_END))
                put(result, k.asByteComparableArray(VERSION), v);
        });
        return result;
    }

    static boolean hasContent(Trie<String> trie)
    {
        return trie.valuesUnordered().iterator().hasNext();
    }

    /// A branch that leads to no content, when it is walked after a sibling that has some, leaves the trie writer
    /// with a node that has that sibling as its only child, and nothing else. (When it is walked before, no node is
    /// made, see `OnDiskTrieWriterTest.testUnproductiveBranchVisitedAfterItsSibling` for both.) The immediate writer
    /// writes it as a chain node of its own; the page-aware writer makes its transitions part of the child's chain,
    /// which gives the trie exactly the form it has without the branch.
    @Test
    public void testNodeWithOnlyAChild() throws IOException
    {
        // "ab" has the child "q", which leads to content, and the child "c", which does not and is walked second
        Trie<String> trie = trie(false, "abqrs", "1", "abc", "0").mapValues(v -> v.equals("0") ? null : v);
        Trie<String> plain = trie(false, "abqrs", "1");
        Written immediate = write(trie, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.IMMEDIATE, 0, 0);
        Written unfolded = write(trie, UNORDERED, ONE_BYTE_SERIALIZER, PAGE_AWARE_WITHOUT_FOLDING, 4096, 0);
        Written pageAware = write(trie, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.PAGE_AWARE, 4096, 0);
        // the leaf with its chain "rs", the node "ab" as the chain "q", and the chain "ab" that leads to it
        assertEquals("01 01  73 72 41  71 40  62 61 41".replace(" ", ""), Hex.bytesToHex(immediate.bytes()));
        assertEquals("01 01  73 72 41  71 40  62 61 41".replace(" ", ""), Hex.bytesToHex(unfolded.bytes()));
        // the leaf with the chain "abqrs"
        assertEquals("01 01  73 72 71 62 61 44".replace(" ", ""), Hex.bytesToHex(pageAware.bytes()));
        assertArrayEquals(write(plain, UNORDERED, ONE_BYTE_SERIALIZER, OnDiskNodeWriter.Type.PAGE_AWARE, 4096, 0).bytes(), pageAware.bytes());
        pageAware.verify().assertValid("Node with only a child");

        String[][] shapes = {
            // the node is the root, and the child directly below it
            { "q", "one", "c", DEAD_END },
            // the node is below a node with other children, and reached by a chain or not
            { "aqrs", "one", "ac", DEAD_END, "b", "two" },
            { "axyqrs", "one", "axyc", DEAD_END, "b", "two" },
            // the child is itself a node with children, and one with content and a single child
            { "abqr", "one", "abqs", "two", "abc", DEAD_END },
            { "abq", "one", "abqrs", "two", "abc", DEAD_END },
            // one such node below another: both "a" and "ab" have nothing but a child
            { "abqrs", "one", "abc", DEAD_END, "a0", DEAD_END },
            // with something else below it, the node is like any other
            { "abqrs", "one", "abc", DEAD_END, "abcd", "two" },
            // a node with content is not such a node; its transition is folded into the chain of its child
            { "ab", "one", "abqrs", "two", "abc", DEAD_END },
            // the content of the root of an ordered trie is presented after its branch
            { "", "one", "qrs", "two", "c", DEAD_END },
        };
        for (String[] shape : shapes)
            for (boolean ordered : new boolean[]{ false, true })
            {
                String message = "Shape " + Arrays.toString(shape);
                InMemoryTrie<String> full = trie(ordered, shape);
                Trie<String> withDeadEnds = hideDeadEnds(full);
                Trie<String> withoutDeadEnds = withoutDeadEnds(full, ordered);
                Written written = writePageAware(withDeadEnds, ordered, 4096);
                check(message, withoutDeadEnds, ordered, written);
                assertArrayEquals(message, writePageAware(withoutDeadEnds, ordered, 4096).bytes(), written.bytes());
                // and with layouts
                for (int pageSize : PAGE_SIZES)
                    check(message, withoutDeadEnds, ordered, writePageAware(withDeadEnds, ordered, pageSize));
                check(message, withoutDeadEnds, ordered, writeImmediate(withDeadEnds, ordered));
            }

        // The chains that are joined can be of any length.
        for (int above : new int[]{ 0, 1, 62, 63, 64, 65, 127 })
            for (int below : new int[]{ 0, 1, 62, 63, 64, 65, 127 })
                for (boolean ordered : new boolean[]{ false, true })
                {
                    String message = "Chains of " + above + " and " + below;
                    byte[] node = join(bytes("p"), repeat('u', above));
                    InMemoryTrie<String> full = newTrie(ordered);
                    put(full, join(node, bytes("q"), repeat('d', below)), "one");
                    put(full, join(node, bytes("c")), DEAD_END);
                    put(full, bytes("z"), "two");
                    Trie<String> withoutDeadEnds = withoutDeadEnds(full, ordered);
                    Written written = writePageAware(hideDeadEnds(full), ordered, 4096);
                    Result result = check(message, withoutDeadEnds, ordered, written);
                    assertArrayEquals(message, writePageAware(withoutDeadEnds, ordered, 4096).bytes(), written.bytes());
                    // one chain of all the transitions between "p" and the leaf, in pieces of 64
                    assertEquals(message, (above + 1 + below + 63) / 64, result.count(Type.CHAIN));
                    for (int pageSize : PAGE_SIZES)
                        check(message, withoutDeadEnds, ordered, writePageAware(hideDeadEnds(full), ordered, pageSize));
                }
    }

    /// A node with only a child whose child is written while the node stays in memory: here because the node is
    /// what makes the two exceed a page. If such a node took over nothing but its child's place, as a node whose
    /// transition was folded into the chain of its child does, a relay to the child would be all there is to it when
    /// it is written elsewhere, and the relay in front of a parent that expects to find the node before it would lead
    /// to that relay. Readers do not follow a relay from a relay.
    @Test
    public void testNodeWithOnlyAChildThatIsWrittenFirst() throws IOException
    {
        for (boolean ordered : new boolean[]{ false, true })
        {
            // With pages of 64 bytes: the leaf "azwxyz" (59 bytes) with the chain "wxyz" (5 bytes) is exactly a page.
            // "a" has that as its only child because "am" leads to nothing; with the transition "z" the two are over
            // a page, and the leaf is written, across a page boundary. "b" (60 bytes) follows, leaving 3 bytes of the
            // second page. The root does not fit in them and is written in the third page, with a relay to its first
            // child, "a".
            InMemoryTrie<String> full = trie(ordered,
                                             "b", "B".repeat(59),
                                             "azwxyz", "C".repeat(58),
                                             "am", DEAD_END);
            Trie<String> withoutDeadEnds = withoutDeadEnds(full, ordered);
            Written written = writePageAware(hideDeadEnds(full), ordered, 64);
            Result result = check("Node with only a child, written after it", withoutDeadEnds, ordered, written);
            assertArrayEquals(writePageAware(withoutDeadEnds, ordered, 64).bytes(), written.bytes());
            assertEquals(134, result.root);
            assertEquals(1, result.relays);
            OnDiskTrieVerifier.Node relay = only(result, Type.RELAY);
            assertEquals(OnDiskTrieVerifier.Ref.FIRST_CHILD, relay.referencedAs);
            // the relay leads to the leaf's chain, which has all the transitions from "a" down
            assertArrayEquals(new long[]{ 65 }, relay.targets);
            OnDiskTrieVerifier.Node chain = only(result, Type.CHAIN);
            assertEquals(65, chain.end);
            assertArrayEquals(bytes("zwxyz"), chain.transitions);

            // Without folding the node is a chain node, which is what the relay in front of the root leads to.
            Written unfolded = write(hideDeadEnds(full), plainStyle(ordered), STRING_SERDE, PAGE_AWARE_WITHOUT_FOLDING, 64, 0);
            check("Unfolded node with only a child", withoutDeadEnds, ordered, unfolded);
        }
    }

    /// Random tries where the content of a part of the keys is hidden, written with small pages so that nodes are
    /// separated from their children in every possible way.
    @Test
    public void testRandomTriesWithDeadEnds() throws IOException
    {
        Random rand = new Random(SEED + 11);
        int triesWithNodesWithOnlyAChild = 0;
        Totals totals = new Totals();
        for (int i = 0; i < 80; ++i)
        {
            boolean ordered = rand.nextBoolean();
            int count = 1 + rand.nextInt(i % 10 == 0 ? 1000 : 200);
            InMemoryTrie<String> full = i % 3 == 0 ? generatedKeysTrie(rand, ordered, count) : randomTrie(rand, ordered, count);
            // hide a share of the keys, both leaves and nodes with others below them
            int hiddenOneIn = 2 + rand.nextInt(4);
            List<byte[]> keys = new ArrayList<>();
            full.forEachEntry((k, v) -> keys.add(k.asByteComparableArray(VERSION)));
            for (byte[] key : keys)
                if (rand.nextInt(hiddenOneIn) == 0)
                    put(full, key, DEAD_END);
            // and add paths that lead to nothing next to the ones that remain
            for (byte[] key : keys)
                if (key.length > 0 && rand.nextInt(4) == 0)
                    put(full, concat(Arrays.copyOf(key, rand.nextInt(key.length)), rand.nextInt(256)), DEAD_END);

            Trie<String> withDeadEnds = hideDeadEnds(full);
            InMemoryTrie<String> withoutDeadEnds = withoutDeadEnds(full, ordered);
            if (!hasContent(withoutDeadEnds))
                continue;

            String message = "Seed " + (SEED + 11) + " trie " + i + (ordered ? " ordered" : " unordered");
            // The immediate writer writes the nodes with only a child that the paths to nothing leave behind.
            Written immediate = writeImmediate(withDeadEnds, ordered);
            check(message, withoutDeadEnds, ordered, immediate);
            if (immediate.size() != writeImmediate(withoutDeadEnds, ordered).size())
                ++triesWithNodesWithOnlyAChild;

            for (int pageSize : PAGE_SIZES)
                totals.add(check(message, withoutDeadEnds, ordered, writePageAware(withDeadEnds, ordered, pageSize)));
            // Without folding these nodes are written as the immediate writer writes them.
            int pageSize = PAGE_SIZES[rand.nextInt(PAGE_SIZES.length)];
            check(message, withoutDeadEnds, ordered, write(withDeadEnds, plainStyle(ordered), STRING_SERDE, PAGE_AWARE_WITHOUT_FOLDING, pageSize, 0));
        }
        if (VERBOSE)
            System.out.println("Random tries with dead ends: " + triesWithNodesWithOnlyAChild + " with nodes with only a child, " + totals);
        assertTrue(totals.toString(), triesWithNodesWithOnlyAChild > 50);
        assertTrue(totals.toString(), totals.multiPage > 250);
        assertTrue(totals.toString(), totals.relays > 1000);
    }

    /// The paths to nothing that the intersection of a trie with a set of ranges has along the boundaries of the
    /// ranges, which is what a trie restricted to a part of the key space is written from.
    @Test
    public void testRandomSlices() throws IOException
    {
        Random rand = new Random(SEED + 12);
        Totals totals = new Totals();
        int slices = 0;
        for (int i = 0; i < 30; ++i)
        {
            boolean ordered = rand.nextBoolean();
            InMemoryTrie<String> full = i % 2 == 0 ? generatedKeysTrie(rand, ordered, 250) : randomTrie(rand, ordered, 250);
            List<byte[]> keys = new ArrayList<>();
            full.forEachEntry((k, v) -> keys.add(k.asByteComparableArray(VERSION)));
            for (int j = 0; j < 4; ++j)
            {
                // boundaries that share a prefix with keys of the trie and then go their own way
                byte[] left = boundary(rand, keys);
                byte[] right = boundary(rand, keys);
                // The order of two boundaries of which one is a prefix of the other depends on their inclusivity.
                int mismatch = Arrays.mismatch(left, right);
                if (mismatch < 0 || mismatch == Math.min(left.length, right.length))
                    continue;
                if (Arrays.compareUnsigned(left, right) > 0)
                {
                    byte[] swap = left;
                    left = right;
                    right = swap;
                }
                Trie<String> slice = ordered && rand.nextBoolean()
                                     ? full.slice(key(left), rand.nextBoolean(), key(right), rand.nextBoolean())
                                     : full.intersect(TrieSet.range(VERSION, key(left), rand.nextBoolean(), key(right), rand.nextBoolean()));
                // What must be found in the file is what the walk that writes it presents. (Whether the content of a
                // boundary that is excluded is part of the intersection depends on the direction of the walk when the
                // trie is not ordered.)
                InMemoryTrie<String> expected = withoutDeadEnds(slice, ordered);
                if (!hasContent(expected))
                    continue;
                ++slices;
                String message = "Seed " + (SEED + 12) + " trie " + i + " slice " + Hex.bytesToHex(left) + "-" + Hex.bytesToHex(right)
                                 + (ordered ? " ordered" : " unordered");
                check(message, expected, ordered, writeImmediate(slice, ordered));
                for (int pageSize : PAGE_SIZES)
                    totals.add(check(message, expected, ordered, writePageAware(slice, ordered, pageSize)));
            }
        }
        if (VERBOSE)
            System.out.println("Random slices: " + slices + " slices, " + totals);
        assertTrue(totals.toString(), slices > 80);
        assertTrue(totals.toString(), totals.relays > 100);
    }

    private static byte[] boundary(Random rand, List<byte[]> keys)
    {
        byte[] key = keys.get(rand.nextInt(keys.size()));
        switch (rand.nextInt(3))
        {
            case 0:
                return key;
            case 1:
                return Arrays.copyOf(key, rand.nextInt(key.length + 1));
            default:
                return concat(Arrays.copyOf(key, rand.nextInt(key.length + 1)), rand.nextInt(256));
        }
    }

    // Degenerate inputs

    @Test
    public void testEmptyTrie() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
            for (int startOffset : new int[]{ 0, 1, 13, pageSize - 1, pageSize, pageSize + 7 })
                for (boolean ordered : new boolean[]{ false, true })
                {
                    // write() checks that nothing is written
                    assertEquals(0, write(newTrie(ordered), plainStyle(ordered), STRING_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, startOffset).root);
                    assertEquals(0, write(InMemoryRangeTrie.<TestRangeState>shortLived(VERSION), RANGE, RANGE_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, startOffset).root);
                    // a trie with nodes but no content
                    Trie<String> unproductive = trie(ordered, "abc", "x", "abd", "x").mapValues(v -> null);
                    assertEquals(0, write(unproductive, plainStyle(ordered), STRING_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, startOffset).root);
                }
    }

    /// Content on the root only. An ordered trie presents it on the return path of the reverse walk the writer makes,
    /// and a range trie can have some on both sides.
    @Test
    public void testRootOnly() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
            for (int startOffset : new int[]{ 0, 13, pageSize - 1 })
            {
                for (boolean ordered : new boolean[]{ false, true })
                {
                    Trie<String> trie = trie(ordered, "", "root");
                    Written written = write(trie, plainStyle(ordered), STRING_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, startOffset);
                    Result result = check("Root only at " + startOffset + " page size " + pageSize, trie, ordered, written);
                    assertEquals(1, result.nodes);
                    assertEquals(1, result.count(Type.LEAF));
                    // it is moved to the next page if it does not fit this one
                    long expectedStart = startOffset % pageSize + 5 <= pageSize ? startOffset : startOffset + pageSize - startOffset % pageSize;
                    assertEquals(expectedStart + 5, written.root);
                    assertEquals(expectedStart - startOffset, result.paddingBytes);
                }

                // a range that covers everything: a boundary on each side of the root
                RangeTrie<TestRangeState> trie = TestRangeState.fromList(Arrays.asList(new TestRangeState(key(new byte[0]), false, -1, 1),
                                                                                       new TestRangeState(key(new byte[0]), true, 1, -1)));
                Written written = write(trie, RANGE, RANGE_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, startOffset);
                Result result = checkRange("Range root only at " + startOffset + " page size " + pageSize, trie, written);
                assertEquals(1, result.nodes);
                assertEquals(2, result.contents);
                assertEquals(1, result.ascentContents);
            }
    }

    /// Nodes that take more than a page by themselves: 200 children make a bitmap node of at least 232 bytes and 256
    /// a dense node of at least 257.
    @Test
    public void testNodesWiderThanAPage() throws IOException
    {
        for (int fanout : new int[]{ 200, 256 })
            for (boolean ordered : new boolean[]{ false, true })
                for (int variant = 0; variant < 4; ++variant)
                {
                    byte[] path = variant < 2 ? new byte[0] : bytes("path");
                    boolean content = variant % 2 == 1;
                    Trie<String> trie = wideTrie(ordered, path, fanout, true, 0, content, 0);
                    String message = fanout + " children, variant " + variant;
                    Result result = writeAndCheck(message, trie, ordered, 32);
                    assertEquals(message, 1, result.count(fanout == 256 ? Type.DENSE : Type.BITMAP));
                    assertEquals(message, 1, result.oversizedLogicalNodes);
                    assertEquals(message, fanout + 1, result.logicalNodes);
                    assertTrue(message, result.largestNode >= (fanout == 256 ? 257 : 232));
                    // The wide node is started where it crosses as few page boundaries as its size permits.
                    assertEquals(message, 0, result.oversizedLogicalNodesWithExtraCrossings);
                    // The leaves are 2 to 4 bytes, and all but the last page of them are full or nearly so.
                    assertTrue(message + ": " + result, result.paddingBytes <= 3 * result.pages + 32);
                }
    }

    /// With no limit to it, the depth of the recursion that writes a branch would be the depth of the branch, which
    /// only has to fit a page: here each level takes 5 bytes (9 with the leaves of the comb).
    @Test
    public void testDeeperThanBufferedDepthLimit() throws IOException
    {
        int depth = 3 * OnDiskNodeWriterPageAware.MAX_BUFFERED_DEPTH + 16;
        for (boolean ordered : new boolean[]{ false, true })
        {
            Trie<String> path = pathTrie(ordered, depth);
            Written immediate = writeImmediate(path, ordered);
            assertTrue(immediate.size() < 4096);
            Written written = writePageAware(path, ordered, 4096);
            Result result = check("Path", path, ordered, written);
            assertEquals(depth + 1, result.maxLogicalDepth);
            assertEquals(depth + 1, result.contents);
            // When a node has only one child, writing the child early changes nothing.
            assertArrayEquals(immediate.bytes(), written.bytes());

            for (int leafTransition : new int[]{ 'a', 'c' })
            {
                Trie<String> comb = combTrie(ordered, depth, leafTransition);
                immediate = writeImmediate(comb, ordered);
                assertTrue(immediate.size() < 4096);
                written = writePageAware(comb, ordered, 4096);
                result = check("Comb " + leafTransition, comb, ordered, written);
                assertEquals(depth + 1, result.maxLogicalDepth);
                assertEquals(2 * depth + 1, result.contents);
                assertEquals(1, result.pages);
                assertEquals(0, result.paddingBytes);
                // The children of the nodes at the depth limit were laid out, biggest first, although everything fits
                // one page. When the leaf is the first child of such a node, it is written after the rest of the path
                // as usual and the node follows it directly. When the leaf is the second child, the node's first
                // child, the rest of the path, is written before the leaf and the node needs a relay to it.
                int layouts = depth / OnDiskNodeWriterPageAware.MAX_BUFFERED_DEPTH;
                assertEquals("Relays " + result, leafTransition == 'a' ? 0 : layouts, result.relays);
                assertEquals(0, result.needlessRelays);
                assertEquals(leafTransition == 'a', Arrays.equals(immediate.bytes(), written.bytes()));
            }
        }
    }

    @Test
    public void testVeryDeepTrieWithSmallStack() throws Exception
    {
        int depth = 5000;
        InMemoryTrie<String> trie = pathTrie(false, depth);
        for (int pageSize : new int[]{ 64, 4096, 1 << 20 })
        {
            AtomicReference<Object> outcome = new AtomicReference<>();
            Thread thread = new Thread(null, () ->
            {
                try
                {
                    outcome.set(writePageAware(trie, false, pageSize));
                }
                catch (Throwable t)
                {
                    outcome.set(t);
                }
            }, "small-stack-writer", 128 * 1024);
            thread.start();
            thread.join();
            if (outcome.get() instanceof Throwable)
                throw new AssertionError("Writing a trie of depth " + depth + " with a small stack, page size " + pageSize, (Throwable) outcome.get());

            Result result = check("Depth " + depth + " page size " + pageSize, trie, false, (Written) outcome.get());
            assertEquals(depth + 1, result.maxLogicalDepth);
            assertEquals(depth + 1, result.contents);
            assertEquals(depth, result.maxKeyLength);
        }
    }

    // Layout quality

    /// A trie of many small branches: 60 nodes below the root, each with a few dozen children that have a handful of
    /// short keys with small values below them.
    static InMemoryTrie<String> manySmallBranchesTrie(Random rand, boolean ordered)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        int index = 0;
        for (int a = 0; a < 60; ++a)
            for (int b = 10 + rand.nextInt(30); b > 0; --b)
            {
                byte[] prefix = { (byte) (a * 4 + 3), (byte) rand.nextInt(256) };
                for (int c = 1 + rand.nextInt(6); c > 0; --c)
                {
                    byte[] tail = new byte[1 + rand.nextInt(8)];
                    for (int i = 0; i < tail.length; ++i)
                        tail[i] = (byte) ('a' + rand.nextInt(4));
                    put(trie, join(prefix, tail), "value" + index++);
                }
            }
        return trie;
    }

    /// When all branches are much smaller than a page, pages are filled with whole branches: nothing crosses a page
    /// boundary, little space is lost to padding, relays are only placed where a node could not be written together
    /// with its children, and a lookup touches fewer pages than in the layout of the immediate writer.
    @Test
    public void testLayoutQuality() throws IOException
    {
        int pageSize = 4096;
        Random rand = new Random(SEED + 5);
        for (boolean ordered : new boolean[]{ false, true })
        {
            Trie<String> trie = manySmallBranchesTrie(rand, ordered);
            String message = "Seed " + SEED + (ordered ? " ordered" : " unordered");
            Written written = writePageAware(trie, ordered, pageSize);
            Result result = check(message, trie, ordered, written);
            Written immediate = writeImmediate(trie, ordered);
            Result unpacked = new OnDiskTrieVerifier(immediate.data).pagesForStatisticsOnly(pageSize).verify(immediate.root);
            unpacked.assertStructurallyValid(message);
            message += ": " + result + " compared to immediate " + unpacked;
            if (VERBOSE)
                System.out.println("Layout quality " + message);

            assertTrue(message, result.pages >= 10);
            // No node is larger than a page here, so none may cross a page boundary.
            assertEquals(message, 0, result.oversizedLogicalNodes);
            assertEquals(message, 0, result.nodesCrossingPages);
            assertTrue(message, unpacked.nodesCrossingPages > 0);

            assertTrue(message, result.paddingBytes > 0);
            assertTrue(message, result.paddingBytes * 10 < written.size());
            assertTrue(message, written.size() * 100 < immediate.size() * 115);

            // A relay is only needed by a node whose children could not all be written together with it.
            assertTrue(message, result.relays > 0);
            assertTrue(message, result.relays <= result.logicalNodesWithBranchOverAPage);
            assertEquals(message, 0, result.needlessRelays);
            assertEquals(message, 0, result.explicitlyReferencedRelays);

            // The root, a node below it and everything below that: at most two steps to another page per lookup.
            assertEquals(message, unpacked.leafPaths, result.leafPaths);
            assertTrue(message, result.maxPathPageCrossings <= 2);
            assertTrue(message, result.pathPageCrossings <= unpacked.pathPageCrossings);
        }
    }

    /// The children of a node that is laid out are written largest first, each in what remains of the current page if
    /// it fits there, and a child that needs exactly what remains fits. Here the root has the leaves "a" and "b", which
    /// hold `n` bytes of content each and thus take `n + 1` bytes: "a" exactly the rest of the 64-byte page the trie
    /// starts in, at offset 0 or 16, and "b" 20. Together with the root they do not fit a page, so the root lays them
    /// out: "a" fills its page to the end and "b" starts the next one, and nothing is padded. Passing over "a" would
    /// write "b" first and pad the rest of the page, which "a" no longer fits.
    @Test
    public void testChildExactlyFillingThePage() throws IOException
    {
        int pageSize = 64;
        for (int startOffset : new int[]{ 0, 16 })
            for (boolean ordered : new boolean[]{ false, true })
            {
                int rest = pageSize - startOffset;
                Trie<String> trie = trie(ordered, "a", "A".repeat(rest - 1), "b", "B".repeat(19));
                Written written = write(trie, plainStyle(ordered), STRING_SERDE, OnDiskNodeWriter.Type.PAGE_AWARE, pageSize, startOffset);
                String message = String.format("Start offset %d %s", startOffset, ordered ? "ordered" : "unordered");
                Result result = check(message, trie, ordered, written);
                message += ": " + result;

                assertEquals(message, 0, result.paddingBytes);
                OnDiskTrieVerifier.Node a = leafWithContentLength(result, rest - 1);
                assertEquals(message, startOffset, a.start);
                assertEquals(message, pageSize, a.end);
                OnDiskTrieVerifier.Node b = leafWithContentLength(result, 19);
                assertEquals(message, pageSize, b.start);
                assertEquals(message, pageSize + 20, b.end);
            }
    }

    private static OnDiskTrieVerifier.Node leafWithContentLength(Result result, int length)
    {
        OnDiskTrieVerifier.Node found = null;
        for (OnDiskTrieVerifier.Node node : result.allNodes)
            if (node.type == Type.LEAF && node.descentLength == length)
            {
                assertNull("More than one leaf with " + length + " bytes of content: " + result, found);
                found = node;
            }
        assertNotNull("No leaf with " + length + " bytes of content: " + result, found);
        return found;
    }

    // Sizes that depend on the position

    /// An output in which positions can be skipped. The skipped regions stand for data that something else has
    /// written to the same output while a trie is being written to it, which is what the deletion branches of a
    /// deletion-aware trie are to its data trie. They take no memory, which makes it possible to have distances in
    /// the trie that need pointers of any size.
    static class SkippingOutput extends TrieUtil.PagedDataOutputBuffer
    {
        private long skipped = 0;
        private final List<long[]> skips = new ArrayList<>();

        SkippingOutput(int pageSize)
        {
            super(pageSize);
        }

        @Override
        public long position()
        {
            return super.position() + skipped;
        }

        void skip(long bytes)
        {
            long from = position();
            skipped += bytes;
            skips.add(new long[]{ from, from + bytes });
        }

        /// A verifier of what was written, in which the skipped regions are foreign data.
        OnDiskTrieVerifier verifier()
        {
            OnDiskTrieVerifier verifier = new OnDiskTrieVerifier(buffer()).pages(pageSize, PADDING);
            for (long[] skip : skips)
                if (skip[0] < skip[1])
                    verifier.withForeignData(skip[0], skip[1]);
            return verifier;
        }
    }

    private static final int WRITTEN_CHILDREN_SHAPES = 9;

    /// A trie in which the node "m" is completed when some or all of its children are already written, because their
    /// content is larger than a page. Unless it is itself larger than a page, the node is then in memory, with the
    /// positions of these children, while the walk goes on to "k". Whatever is written for "k" gets between "m" and
    /// its children and makes the pointers of "m" longer than they were when its size was first worked out.
    ///
    /// @param largeRoot true to give the root enough other children for it to be laid out rather than written whole
    static InMemoryTrie<String> writtenChildrenTrie(boolean ordered, int shape, int pageSize, String valueOfK, boolean largeRoot)
    {
        InMemoryTrie<String> trie = newTrie(ordered);
        String big = "B".repeat(pageSize + 3);
        byte[] m = bytes("m");
        switch (shape)
        {
            case 0:
                // two written children: a sparse node and a relay
                put(trie, "mx", big);
                put(trie, "my", big);
                break;
            case 1:
                // 30 written children: a bitmap node and a relay
                for (int i = 0; i < 30; ++i)
                    put(trie, concat(m, 40 + 5 * i), big);
                break;
            case 2:
                // 256 children of which 16 are written: a dense node, which never has a relay
                for (int i = 0; i < 256; ++i)
                    put(trie, concat(m, i), i % 16 == 3 ? big : "");
                break;
            case 3:
                // content and a single written child: a chain node and a relay
                put(trie, "m", "content");
                put(trie, "mxyz", big);
                break;
            case 4:
                // the first child written and the other in memory
                put(trie, "mx", big);
                put(trie, "my", "small");
                break;
            case 5:
                // the first child in memory and the other written
                put(trie, "mx", "small");
                put(trie, "my", big);
                break;
            case 6:
                // a node with written children that is part of a branch in memory
                put(trie, "m", "content");
                put(trie, "mqx", big);
                put(trie, "mqy", big);
                put(trie, "mz", "small");
                break;
            case 7:
                // 20 children, the first and the last written: a sparse node as long as the pointers fit 4 bytes
                for (int i = 0; i < 20; ++i)
                    put(trie, concat(m, 10 * i), i == 0 || i == 19 ? big : "");
                break;
            default:
                // 244 children, two of them written: a dense node with pointers of up to two bytes, a bitmap node
                // with longer ones
                for (int i = 0; i < 244; ++i)
                    put(trie, concat(m, i), i == 0 || i == 200 ? big : "");
                break;
        }
        put(trie, "k", valueOfK);
        put(trie, "a", "first");
        if (largeRoot)
            for (int i = 0; i < 8; ++i)
                put(trie, concat(new byte[0], (i < 4 ? 'b' : 'p') + i), "S".repeat(pageSize / 4));
        return trie;
    }

    private static final String SKIP = "skip";

    /// A trie written with a range of positions skipped, as checked by the verifier.
    static class WrittenWithSkip
    {
        final Result result;
        final long skipStart;
        final long skipEnd;
        /// The distances that the pointers leading from one side of the skipped range to the other cover.
        final TreeSet<Long> distancesOverSkip = new TreeSet<>();
        boolean hasRelayOverSkip;
        /// The same for the pointers to deletion branches, which are not among the above.
        final TreeSet<Long> branchDistancesOverSkip = new TreeSet<>();
        /// The most bytes taken by any pointer to a deletion branch.
        int maxBranchPointerBytes;

        WrittenWithSkip(Result result, long skipStart, long skipEnd)
        {
            this.result = result;
            this.skipStart = skipStart;
            this.skipEnd = skipEnd;
            for (OnDiskTrieVerifier.Node node : result.allNodes)
            {
                if (node.deletionBranch >= 0)
                {
                    maxBranchPointerBytes = Math.max(maxBranchPointerBytes, node.ascentLength);
                    if (node.start >= skipEnd && node.deletionBranch <= skipStart)
                        branchDistancesOverSkip.add(node.start - node.deletionBranch);
                }
                if (node.pointerBytes == 0 || node.start < skipEnd)
                    continue;
                // the first child of a sparse or bitmap node has no pointer
                boolean firstIsImplicit = node.type == Type.SPARSE || node.type == Type.BITMAP;
                for (int i = firstIsImplicit ? 1 : 0; i < node.targets.length; ++i)
                    if (node.targets[i] <= skipStart)
                    {
                        distancesOverSkip.add(node.start - node.targets[i]);
                        hasRelayOverSkip |= node.type == Type.RELAY;
                    }
            }
        }
    }

    /// Writes [#writtenChildrenTrie] with the given number of positions skipped when the walk reaches "k", and checks
    /// the result with the verifier.
    private static WrittenWithSkip writeWithSkip(String message, boolean ordered, int shape, int pageSize, long gap, boolean largeRoot) throws IOException
    {
        InMemoryTrie<String> trie = writtenChildrenTrie(ordered, shape, pageSize, SKIP, largeRoot);
        SkippingOutput out = new SkippingOutput(pageSize);
        Trie<String> skipping = trie.mapValues(v ->
        {
            if (SKIP.equals(v) && out.skips.isEmpty())
                out.skip(gap);
            return v;
        });
        try
        {
            long root = OnDiskTrieWriter.write(skipping, plainStyle(ordered), STRING_SERDE, out, OnDiskNodeWriter.Type.PAGE_AWARE);
            assertEquals(out.position(), root);
            assertEquals(1, out.skips.size());
            Result result = out.verifier().verify(root);
            assertValid("Verification", result, true);
            assertEquals("Pointers wider than necessary: " + result, 0, result.nodesWithOversizedPointers);
            Expected.of(trie).assertFoundBy(result);
            return new WrittenWithSkip(result, out.skips.get(0)[0], out.skips.get(0)[1]);
        }
        catch (Throwable t)
        {
            throw new AssertionError(message + ": " + t, t);
        }
    }

    private static boolean hasRelayWithPointerBytes(Result result, int bytes)
    {
        for (OnDiskTrieVerifier.Node node : result.allNodes)
            if (node.type == Type.RELAY && node.pointerBytes == bytes)
                return true;
        return false;
    }

    /// The size of a node that is in memory and has children on disk changes with the position it is written at.
    /// This moves the position, between the time such a node is first sized and the time it is written, by the
    /// distances that put each of the pointers involved on either side of the values where it needs one more byte,
    /// up to the full 8 bytes. The sizes the writer works with must remain exact (it asserts they are), and what it
    /// writes must remain valid.
    @Test
    public void testPointersGrowingWhileNodeIsInMemory() throws IOException
    {
        for (int bytes = 1; bytes < 8; ++bytes)
        {
            long boundary = 1L << (8 * bytes);
            for (int pageSize : new int[]{ 64, 256, 4096 })
            {
                int tries = 0;
                int withLongerRelays = 0;
                int withLargestBelowBoundary = 0;
                int withSmallestAtBoundary = 0;
                int withMisplacedRoot = 0;
                for (int shape = 0; shape < WRITTEN_CHILDREN_SHAPES; ++shape)
                    for (int variant = 0; variant < 4; ++variant)
                    {
                        // the shapes with many children are only kept in memory when pages are large
                        if (shape == 1 && pageSize < 256 || (shape == 2 || shape == 8) && pageSize < 4096)
                            continue;
                        boolean ordered = variant % 2 == 0;
                        boolean largeRoot = variant / 2 == 0;
                        String message = String.format("Shape %d page size %d %s %s gap ", shape, pageSize,
                                                       ordered ? "ordered" : "unordered", largeRoot ? "large root" : "small root");

                        // Find what the pointers add to the skipped distance, to try the distances that place each
                        // of them around the boundary.
                        long base = Math.max(0, boundary - 3L * pageSize);
                        WrittenWithSkip first = writeWithSkip(message + base, ordered, shape, pageSize, base, largeRoot);
                        TreeSet<Long> gaps = new TreeSet<>();
                        gaps.add(boundary + 2);
                        for (long distance : first.distancesOverSkip)
                            for (long gap = boundary - (distance - base) - 2; gap <= boundary - (distance - base) + 1; ++gap)
                                if (gap >= 0)
                                    gaps.add(gap);

                        for (long gap : gaps)
                        {
                            WrittenWithSkip written = writeWithSkip(message + Long.toHexString(gap), ordered, shape, pageSize, gap, largeRoot);
                            Result result = written.result;
                            String details = message + Long.toHexString(gap) + ": " + result;
                            ++tries;
                            if (!result.isValid())
                                ++withMisplacedRoot;
                            if (written.distancesOverSkip.contains(boundary - 1))
                                ++withLargestBelowBoundary;
                            if (written.distancesOverSkip.contains(boundary))
                                ++withSmallestAtBoundary;
                            if (gap < boundary)
                                continue;

                            // something is on either side of the skipped range whatever is done
                            assertTrue(details, result.maxPointerBytes >= bytes + 1);
                            // the node "m" and its first child are, except where it is a dense node
                            if (shape != 5 && shape != 2 && (shape != 8 || bytes + 1 > 2))
                            {
                                assertTrue(details, written.hasRelayOverSkip && hasRelayWithPointerBytes(result, bytes + 1));
                                ++withLongerRelays;
                            }
                            if (shape == 7)
                                assertEquals(details, bytes + 1 > 4 ? 1 : 0, result.count(Type.BITMAP));
                            if (shape == 8)
                                assertEquals(details, bytes + 1 > 2 ? 0 : 1, result.count(Type.DENSE));
                        }
                    }
                String summary = String.format("%d-byte pointers, page size %d: %d tries, %d with a distance one below the boundary, %d at the boundary, " +
                                               "%d with longer relays, %d with a misplaced node in the last page",
                                               bytes + 1, pageSize, tries, withLargestBelowBoundary, withSmallestAtBoundary, withLongerRelays, withMisplacedRoot);
                if (VERBOSE)
                    System.out.println("Skipped positions: " + summary);
                assertTrue(summary, withLargestBelowBoundary > 0 && withSmallestAtBoundary > 0 && withLongerRelays > 0);
            }
        }
    }

    /// The same with distances made of real content, so that the result can also be read back.
    @Test
    public void testPointersGrowingWhileNodeIsInMemoryReadBack() throws IOException
    {
        for (int bytes = 1; bytes <= 3; ++bytes)
        {
            int boundary = 1 << (8 * bytes);
            for (int pageSize : new int[]{ 64, 256, 4096 })
            {
                // The content of "k" must be larger than a page to be written before "m" is. With 16MB of it, try
                // only some of the combinations.
                if (boundary - 90 <= pageSize || bytes == 3 && pageSize != 256)
                    continue;
                int withLongerRelays = 0;
                int withShorterPointers = 0;
                int step = bytes == 3 ? 46 : bytes == 2 ? 3 : 1;
                for (int gap = boundary + 2; gap >= boundary - 90; gap -= step)
                    for (int shape = 0; shape < WRITTEN_CHILDREN_SHAPES; ++shape)
                    {
                        if (shape == 1 && pageSize < 256 || (shape == 2 || shape == 8) && pageSize < 4096)
                            continue;
                        if (bytes == 3 && shape % 3 != 0)
                            continue;
                        boolean ordered = (gap & 1) == 0;
                        boolean largeRoot = (gap & 2) == 0;
                        String message = String.format("Shape %d page size %d gap %x %s %s", shape, pageSize, gap,
                                                       ordered ? "ordered" : "unordered", largeRoot ? "large root" : "small root");
                        Trie<String> trie = writtenChildrenTrie(ordered, shape, pageSize, "G".repeat(gap), largeRoot);
                        Result result = check(message, trie, ordered, writePageAware(trie, ordered, pageSize), true);
                        if (result.maxPointerBytes <= bytes)
                            ++withShorterPointers;
                        if (gap >= boundary)
                        {
                            assertTrue(message + ": " + result, result.maxPointerBytes >= bytes + 1);
                            if (hasRelayWithPointerBytes(result, bytes + 1))
                                ++withLongerRelays;
                        }
                    }
                String summary = String.format("%d-byte pointers, page size %d: %d with shorter pointers, %d with longer relays",
                                               bytes + 1, pageSize, withShorterPointers, withLongerRelays);
                assertTrue(summary, withLongerRelays > 0);
                // with the larger steps the lower distances are all below the boundary only when pages are small
                assertTrue(summary, withShorterPointers > 0 || pageSize > 64);
            }
        }
    }

    private static final int WRITTEN_BRANCH_SHAPES = 4;

    /// A deletion-aware trie in which the deletion branch of the node "m" is written before "m" is completed, because
    /// the marker at the root of the branch is larger than a page. The node is then in memory with the position of its
    /// branch while the walk goes on to "k", and whatever is written for "k" makes the pointer to the branch longer
    /// than it was when the size of "m" was first worked out. This is [#writtenChildrenTrie] for the pointer in the
    /// ascent-side slot of a prefix node rather than the ones to its children.
    ///
    /// @param largeRoot true to give the root enough other children for it to be laid out rather than written whole
    static DeletionAwareTrie<String, TestRangeState> writtenBranchTrie(int shape, int pageSize, String valueOfK, boolean largeRoot)
    {
        InMemoryTrie<String> live = newTrie(false);
        switch (shape)
        {
            case 0:
                // content and the branch: a prefix node without a child
                put(live, "m", "content");
                break;
            case 1:
                // the branch and a child in memory: a prefix node without descent-side content
                put(live, "mx", "small");
                break;
            case 2:
                // content, the branch and a child that is also written, after the branch and thus closer to "m"
                put(live, "m", "content");
                put(live, "mx", "B".repeat(pageSize + 3));
                break;
            default:
                // content, the branch and two children in memory
                put(live, "m", "content");
                put(live, "mx", "small");
                put(live, "my", "small");
                break;
        }
        put(live, "k", valueOfK);
        put(live, "a", "first");
        if (largeRoot)
            for (int i = 0; i < 8; ++i)
                put(live, concat(new byte[0], (i < 4 ? 'b' : 'p') + i), "S".repeat(pageSize / 4));

        InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState> mutator =
            trie.mutator((existing, update) -> update,
                         (existing, update) -> update,
                         (existing, deletion) -> existing,
                         (deletion, inserted) -> inserted,
                         true,
                         features -> false,
                         features -> false);
        try
        {
            mutator.apply(DeletionAwareTrie.wrap(live));
            mutator.apply(DeletionAwareTrie.deletionBranch(key(bytes("m")), VERSION, wholeBranchDeletion(pageSize)));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /// A range trie that deletes everything in the branch it is attached to. The position of a [TestRangeState] is
    /// only a label that is serialized with it, and the one of the opening marker makes it larger than a page.
    static RangeTrie<TestRangeState> wholeBranchDeletion(int pageSize)
    {
        InMemoryRangeTrie<TestRangeState> branch = InMemoryRangeTrie.shortLived(VERSION);
        try
        {
            branch.putRecursive(ByteComparable.EMPTY, new TestRangeState(key(repeat('D', pageSize + 3)), false, -1, 1), false, (existing, update) -> update);
            branch.putRecursive(ByteComparable.EMPTY, new TestRangeState(ByteComparable.EMPTY, true, 1, -1), true, (existing, update) -> update);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return branch;
    }

    /// Writes [#writtenBranchTrie] with the given number of positions skipped when the walk reaches "k", and checks
    /// the result with the verifier.
    private static WrittenWithSkip writeBranchWithSkip(String message, int shape, int pageSize, long gap, boolean largeRoot) throws IOException
    {
        DeletionAwareTrie<String, TestRangeState> trie = writtenBranchTrie(shape, pageSize, SKIP, largeRoot);
        SkippingOutput out = new SkippingOutput(pageSize);
        DeletionAwareTrie<String, TestRangeState> skipping = trie.mapValues(v ->
        {
            if (SKIP.equals(v) && out.skips.isEmpty())
                out.skip(gap);
            return v;
        });
        try
        {
            long root = OnDiskDeletionAwareTrieWriter.write(skipping, STRING_SERDE, RANGE_SERDE, out, OnDiskNodeWriter.Type.PAGE_AWARE);
            assertEquals(out.position(), root);
            assertEquals(1, out.skips.size());
            Result result = out.verifier().deletionAware().verify(root);
            assertValid("Verification", result, true);
            assertEquals("Deletion branches: " + result, 1, result.deletionBranches);
            assertEquals("Pointers wider than necessary: " + result, 0, result.nodesWithOversizedPointers);
            return new WrittenWithSkip(result, out.skips.get(0)[0], out.skips.get(0)[1]);
        }
        catch (Throwable t)
        {
            throw new AssertionError(message + ": " + t, t);
        }
    }

    private static int maxBranchPointerBytes(Result result)
    {
        int bytes = 0;
        for (OnDiskTrieVerifier.Node node : result.allNodes)
            if (node.deletionBranch >= 0)
                bytes = Math.max(bytes, node.ascentLength);
        return bytes;
    }

    /// The same as [#testPointersGrowingWhileNodeIsInMemory] for the pointer to a deletion branch that is written
    /// before its node is. The skipped range moves the node, and with it the distance to its branch, to either side of
    /// each value where the pointer needs one more byte, up to the full 8 bytes.
    @Test
    public void testBranchPointerGrowingWhileNodeIsInMemory() throws IOException
    {
        for (int bytes = 1; bytes < 8; ++bytes)
        {
            long boundary = 1L << (8 * bytes);
            for (int pageSize : new int[]{ 64, 256, 4096 })
            {
                int tries = 0;
                int withLargestBelowBoundary = 0;
                int withSmallestAtBoundary = 0;
                for (int shape = 0; shape < WRITTEN_BRANCH_SHAPES; ++shape)
                    for (boolean largeRoot : new boolean[]{ true, false })
                    {
                        String message = String.format("Shape %d page size %d %s gap ", shape, pageSize, largeRoot ? "large root" : "small root");

                        // Find what the rest of the trie adds to the skipped distance, to try the distances that place
                        // the pointer around the boundary.
                        long base = Math.max(0, boundary - 3L * pageSize);
                        WrittenWithSkip first = writeBranchWithSkip(message + base, shape, pageSize, base, largeRoot);
                        assertEquals(message + base + ": the branch must be on one side of the skipped range and its node on the other, " + first.result,
                                     1, first.branchDistancesOverSkip.size());
                        long added = first.branchDistancesOverSkip.first() - base;
                        TreeSet<Long> gaps = new TreeSet<>();
                        gaps.add(boundary + 2);
                        for (long gap = boundary - added - 2; gap <= boundary - added + 1; ++gap)
                            if (gap >= 0)
                                gaps.add(gap);

                        for (long gap : gaps)
                        {
                            WrittenWithSkip written = writeBranchWithSkip(message + Long.toHexString(gap), shape, pageSize, gap, largeRoot);
                            String details = message + Long.toHexString(gap) + ": " + written.result;
                            ++tries;
                            assertEquals(details, 1, written.branchDistancesOverSkip.size());
                            long distance = written.branchDistancesOverSkip.first();
                            if (distance == boundary - 1)
                                ++withLargestBelowBoundary;
                            if (distance == boundary)
                                ++withSmallestAtBoundary;
                            assertEquals(details, OnDiskBaseTrieWriter.bytesFor(distance), written.maxBranchPointerBytes);
                            if (gap >= boundary)
                                assertEquals(details, bytes + 1, written.maxBranchPointerBytes);
                        }
                    }
                String summary = String.format("%d-byte branch pointers, page size %d: %d tries, %d with a distance one below the boundary, %d at the boundary",
                                               bytes + 1, pageSize, tries, withLargestBelowBoundary, withSmallestAtBoundary);
                if (VERBOSE)
                    System.out.println("Skipped positions before a deletion branch: " + summary);
                assertTrue(summary, withLargestBelowBoundary > 0 && withSmallestAtBoundary > 0);
            }
        }
    }

    /// The same with distances made of real content, so that the result can also be read back.
    @Test
    public void testBranchPointerGrowingWhileNodeIsInMemoryReadBack() throws IOException
    {
        for (int bytes = 1; bytes <= 3; ++bytes)
        {
            int boundary = 1 << (8 * bytes);
            for (int pageSize : new int[]{ 64, 256, 4096 })
            {
                // The content of "k" must be larger than a page to be written before "m" is. With 16MB of it, try
                // only some of the combinations.
                if (boundary - 90 <= pageSize || bytes == 3 && pageSize != 256)
                    continue;
                int withLongerPointers = 0;
                int withShorterPointers = 0;
                int step = bytes == 3 ? 46 : bytes == 2 ? 3 : 1;
                for (int gap = boundary + 2; gap >= boundary - 90; gap -= step)
                    for (int shape = 0; shape < WRITTEN_BRANCH_SHAPES; ++shape)
                    {
                        if (bytes == 3 && shape % 2 != 0)
                            continue;
                        boolean largeRoot = (gap & 1) == 0;
                        String message = String.format("Shape %d page size %d gap %x %s", shape, pageSize, gap, largeRoot ? "large root" : "small root");
                        DeletionAwareTrie<String, TestRangeState> trie = writtenBranchTrie(shape, pageSize, "G".repeat(gap), largeRoot);
                        try
                        {
                            DataOutputBuffer out = newOutput(pageSize, 0);
                            long root = OnDiskDeletionAwareTrieWriter.write(trie, STRING_SERDE, RANGE_SERDE, out, OnDiskNodeWriter.Type.PAGE_AWARE);
                            ByteBuffer data = out.buffer();
                            TrieUtil.assertTriesEqual(trie, OnDiskDeletionAwareTrie.open(data.duplicate(), STRING_SERDE, RANGE_SERDE, VERSION, root));
                            Result result = new OnDiskTrieVerifier(data).deletionAware().pages(pageSize, PADDING).verify(root);
                            assertValid("Verification", result, true);
                            assertEquals("Deletion branches: " + result, 1, result.deletionBranches);

                            int branchPointerBytes = maxBranchPointerBytes(result);
                            if (branchPointerBytes <= bytes)
                                ++withShorterPointers;
                            if (gap >= boundary)
                            {
                                assertEquals(result.toString(), bytes + 1, branchPointerBytes);
                                ++withLongerPointers;
                            }
                        }
                        catch (Throwable t)
                        {
                            throw new AssertionError(message + ": " + t.getMessage(), t);
                        }
                    }
                String summary = String.format("%d-byte branch pointers, page size %d: %d with shorter pointers, %d with longer ones",
                                               bytes + 1, pageSize, withShorterPointers, withLongerPointers);
                assertTrue(summary, withLongerPointers > 0);
                // with the larger steps the lower distances are all below the boundary only when pages are small
                assertTrue(summary, withShorterPointers > 0 || pageSize > 64);
            }
        }
    }

    /// The part of the trie that is still in memory when the walk completes is written in a page of its own if it does
    /// not fit what is left of the current one. Here that part is exactly one page long (32 bytes: the nodes "m" with
    /// a relay, "a" and the root), and one of the pointers of "m" to its children, which are already on disk, needs a
    /// second byte when the distance is taken from the start of the next page. The 33 bytes this makes cannot be
    /// placed in a page any more. What is written is nevertheless correct, but the root node, which is 6 bytes long,
    /// must not end up across a page boundary.
    @Test
    public void testRootBranchGrowingWhenMovedToNewPage() throws IOException
    {
        int pageSize = 32;
        for (boolean ordered : new boolean[]{ false, true })
        {
            Trie<String> trie = trie(ordered,
                                     "mx", "B".repeat(33),
                                     "my", "B".repeat(33),
                                     "k", "G".repeat(190),
                                     "a", "A".repeat(19));
            Written written = writePageAware(trie, ordered, pageSize);
            // It reads back correctly whether or not the layout is the best one,
            Result result = check("Root branch of a page that grows", trie, ordered, written, true);
            assertEquals(2, result.maxPointerBytes);
            // but no node that fits in a page may cross a page boundary.
            result.assertValid("Root branch of a page that grows");
        }
    }

    private static String randomLargeValue(Random rand, int pageSize)
    {
        switch (rand.nextInt(24))
        {
            case 0:
                return "H".repeat(65536 - 300 + rand.nextInt(400));
            case 1:
            case 2:
            case 3:
                return "M".repeat(256 - 60 + rand.nextInt(120));
            default:
                return "L".repeat(pageSize + 1 + rand.nextInt(pageSize));
        }
    }

    private static void addRandomBranch(Random rand, InMemoryTrie<String> trie, byte[] prefix, int levels, int pageSize)
    {
        int children = 1 + rand.nextInt(levels > 0 ? 6 : 3);
        int[] transitions = rand.ints(0, 256).distinct().limit(children).toArray();
        for (int transition : transitions)
        {
            byte[] key = concat(prefix, transition);
            if (rand.nextInt(4) == 0)
                key = concat(key, 'c', 'h');
            switch (rand.nextInt(levels > 0 ? 6 : 3))
            {
                case 0:
                    put(trie, key, randomLargeValue(rand, pageSize));
                    break;
                case 1:
                case 2:
                    put(trie, key, "v" + rand.nextInt(100));
                    break;
                case 3:
                    put(trie, key, rand.nextBoolean() ? "node" : randomLargeValue(rand, pageSize));
                    addRandomBranch(rand, trie, key, levels - 1, pageSize);
                    break;
                default:
                    addRandomBranch(rand, trie, key, levels - 1, pageSize);
                    break;
            }
        }
    }

    /// Random tries in which content larger than a page, which is written as soon as its node is completed, is mixed
    /// with small branches. This leaves many nodes in memory with children on disk at distances that keep changing.
    @Test
    public void testRandomTriesWithWrittenChildren() throws IOException
    {
        for (int pageSize : PAGE_SIZES)
        {
            Random rand = new Random(SEED + 7 * pageSize);
            Totals totals = new Totals();
            int withThreeByteRelays = 0;
            for (int i = 0; i < 50; ++i)
            {
                boolean ordered = i % 2 == 0;
                InMemoryTrie<String> trie = newTrie(ordered);
                addRandomBranch(rand, trie, new byte[0], 1 + i % 3, pageSize);
                Result result = writeAndCheck("Seed " + SEED + " trie " + i, trie, ordered, pageSize);
                totals.add(result);
                if (hasRelayWithPointerBytes(result, 3))
                    ++withThreeByteRelays;
            }
            assertExercisesPaging("Random tries with written children with page size " + pageSize, totals);
            assertTrue(totals.toString(), totals.oversized > 0);
            assertEquals(totals.toString(), 3, totals.maxPointerBytes);
            assertTrue(totals.toString(), withThreeByteRelays > 0);
        }
    }

    // Deletion-aware tries

    /// The data trie of a deletion-aware trie stores the position of a node's deletion branch in the ascent-side slot
    /// of the node. Here the root has content `CC`, the child "a" with content `AA`, and a deletion branch at 4 that
    /// has the content `D1` at "x".
    @Test
    public void testVerifierOnDeletionBranchPointers()
    {
        byte[] data = hex("D1 01 78 40  AA 01 61 40  04 01 CC 01 F7");
        Result result = new OnDiskTrieVerifier(data).deletionAware().verify(data.length);
        result.assertValid("Deletion branch");
        assertEquals(5, result.nodes);
        assertEquals(1, result.deletionBranches);
        assertEquals(3, result.entries.size());
        assertEntry(result, 0, "", false, new byte[]{ (byte) 0xCC });
        assertEquals(null, result.entries.get(0).branchOf);
        assertEntry(result, 1, "x", false, new byte[]{ (byte) 0xD1 });
        assertArrayEquals(new byte[0], result.entries.get(1).branchOf);
        assertEntry(result, 2, "a", false, new byte[]{ (byte) 0xAA });
        assertEquals(null, result.entries.get(2).branchOf);
        assertEquals(0, result.ascentContents);

        // read as a plain trie the pointer is content, and the branch is not part of the trie
        result = new OnDiskTrieVerifier(data).verify(data.length);
        assertFalse(result.isValid());
        assertTrue(result.describeProblems(), result.describeProblems().contains("the bytes [0,4) are not part of any reachable node"));

        // a branch pointer that leads into a node of the data trie
        result = new OnDiskTrieVerifier(hex("D1 01 78 40  AA 01 61 40  02 01 CC 01 F7")).deletionAware().verify(data.length);
        assertFalse(result.isValid());
        // a branch pointer of more bytes than it needs
        result = new OnDiskTrieVerifier(hex("D1 01 78 40  AA 01 61 40  04 00 02 CC 01 F7")).deletionAware().verify(data.length + 1);
        assertFalse(result.isValid());
        assertTrue(result.describeProblems(), result.describeProblems().contains("is stored in 2 bytes"));
        // a branch pointer that leads before the start of the data
        result = new OnDiskTrieVerifier(hex("D1 01 78 40  AA 01 61 40  09 01 CC 01 F7")).deletionAware().verify(data.length);
        assertFalse(result.isValid());
        // content on the ascent side inside a deletion branch is content
        data = hex("E2 01 D1 01 F6  78 40  AA 01 61 40  04 01 CC 01 F7");
        result = new OnDiskTrieVerifier(data).deletionAware().verify(data.length);
        result.assertValid("Deletion branch with content on both sides");
        assertEquals(4, result.entries.size());
        assertEntry(result, 1, "x", false, new byte[]{ (byte) 0xD1 });
        assertEntry(result, 2, "x", true, new byte[]{ (byte) 0xE2 });
        assertEquals(1, result.ascentContents);
    }

    /// Live content from a random trie, with range tries as the deletion branches of some of the nodes at depth 2.
    static DeletionAwareTrie<String, TestRangeState> randomDeletionAwareTrie(Random rand, int liveCount, int branchCount)
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState> mutator =
            trie.mutator((existing, update) -> update,
                         (existing, update) -> update,
                         (existing, deletion) -> existing,
                         (deletion, inserted) -> inserted,
                         true,
                         features -> false,
                         features -> false);
        try
        {
            mutator.apply(DeletionAwareTrie.wrap(randomTrie(rand, false, liveCount)));
            // distinct nodes, none of them below another
            for (int owner : rand.ints(0, 12).distinct().limit(branchCount).toArray())
            {
                byte[] ownerKey = { (byte) ('a' + owner / 4), (byte) ('a' + owner % 4) };
                mutator.apply(DeletionAwareTrie.deletionBranch(key(ownerKey), VERSION, randomRangeTrie(rand, 2 + rand.nextInt(30), 1, false)));
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /// What the two writers produce for deletion-aware tries: every deletion branch must be reachable from the node it
    /// belongs to, and be a well-formed range trie, placed like any other.
    @Test
    public void testVerifierOnDeletionAwareTries() throws IOException
    {
        int[] pageSizes = { 0, 32, 64, 100, 256, 4096 };
        for (int pageSize : pageSizes)
        {
            Random rand = new Random(SEED + 11);
            int branches = 0;
            long relays = 0;
            long padding = 0;
            for (int i = 0; i < 30; ++i)
            {
                DeletionAwareTrie<String, TestRangeState> trie = randomDeletionAwareTrie(rand, rand.nextInt(i < 10 ? 10 : 200), 1 + rand.nextInt(6));
                String message = "Seed " + SEED + " deletion-aware trie " + i + " page size " + pageSize;
                try
                {
                    DataOutputBuffer out = newOutput(pageSize, i % 4 == 0 ? 13 : 0);
                    long start = out.position();
                    long root = OnDiskDeletionAwareTrieWriter.write(trie, STRING_SERDE, RANGE_SERDE, out,
                                                                    pageSize == 0 ? OnDiskNodeWriter.Type.IMMEDIATE : OnDiskNodeWriter.Type.PAGE_AWARE);
                    assertEquals(out.position(), root);
                    ByteBuffer data = out.buffer();
                    TrieUtil.assertTriesEqual(trie, OnDiskDeletionAwareTrie.open(data.duplicate(), STRING_SERDE, RANGE_SERDE, VERSION, root));

                    OnDiskTrieVerifier verifier = new OnDiskTrieVerifier(data).from(start).deletionAware();
                    if (pageSize > 0)
                        verifier.pages(pageSize, PADDING);
                    Result result = verifier.verify(root);
                    result.assertValid("Verification");
                    assertTrue(result.toString(), result.deletionBranches > 0);
                    branches += result.deletionBranches;
                    relays += result.relays;
                    padding += result.paddingBytes;
                }
                catch (Throwable t)
                {
                    throw new AssertionError(message + ": " + t.getMessage(), t);
                }
            }
            String summary = "Page size " + pageSize + ": " + branches + " deletion branches, " + relays + " relays, " + padding + " padding bytes";
            if (VERBOSE)
                System.out.println("Deletion-aware tries: " + summary);
            assertTrue(summary, branches > 30);
            assertEquals(summary, pageSize > 0, relays > 0);
            assertEquals(summary, pageSize > 0, padding > 0);
        }
    }

    /// A deletion-aware trie of the given number of small "partitions": nodes at depth 3 that each have a deletion
    /// branch and a couple of live entries below them.
    static DeletionAwareTrie<String, TestRangeState> partitionsWithDeletionBranches(Random rand, int partitions)
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState> mutator =
            trie.mutator((existing, update) -> update,
                         (existing, update) -> update,
                         (existing, deletion) -> existing,
                         (deletion, inserted) -> inserted,
                         true,
                         features -> false,
                         features -> false);
        try
        {
            InMemoryTrie<String> live = newTrie(false);
            for (int i = 0; i < partitions; ++i)
            {
                byte[] owner = { 'p', (byte) ('a' + i / 26), (byte) ('a' + i % 26) };
                for (int row = 0; row < 2; ++row)
                {
                    byte[] rowKey = Arrays.copyOf(owner, owner.length + 2);
                    rowKey[owner.length] = 'r';
                    rowKey[owner.length + 1] = (byte) ('0' + row);
                    put(live, rowKey, "v" + rand.nextInt(100));
                }
            }
            mutator.apply(DeletionAwareTrie.wrap(live));
            for (int i = 0; i < partitions; ++i)
            {
                byte[] owner = { 'p', (byte) ('a' + i / 26), (byte) ('a' + i % 26) };
                mutator.apply(DeletionAwareTrie.deletionBranch(key(owner), VERSION, randomRangeTrie(rand, 2 + rand.nextInt(3), 1, false)));
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /// A deletion branch is laid out together with the node it belongs to: when the node, the branch and everything
    /// below the node fit a page, they are placed in the same one, however many pages the trie spans.
    @Test
    public void testDeletionBranchesArePackedWithTheirNodes() throws IOException
    {
        for (int pageSize : new int[]{ 256, 4096 })
        {
            Random rand = new Random(SEED + 12);
            int partitions = pageSize == 256 ? 150 : 600;
            DeletionAwareTrie<String, TestRangeState> trie = partitionsWithDeletionBranches(rand, partitions);
            String message = "Seed " + SEED + " page size " + pageSize;

            DataOutputBuffer out = newOutput(pageSize, 0);
            long root = OnDiskDeletionAwareTrieWriter.write(trie, STRING_SERDE, RANGE_SERDE, out, OnDiskNodeWriter.Type.PAGE_AWARE);
            ByteBuffer data = out.buffer();
            TrieUtil.assertTriesEqual(trie, OnDiskDeletionAwareTrie.open(data.duplicate(), STRING_SERDE, RANGE_SERDE, VERSION, root));

            Result result = new OnDiskTrieVerifier(data).deletionAware().pages(pageSize, PADDING).verify(root);
            result.assertValid(message);
            // a random range trie can come out empty, in which case the node gets no branch
            assertTrue(message + ": " + result, result.deletionBranches >= partitions * 9 / 10);
            assertTrue(message + ": the trie must span several pages, " + result, result.pages >= 4);

            int owners = 0;
            for (OnDiskTrieVerifier.Node node : result.allNodes)
            {
                if (node.deletionBranch < 0)
                    continue;
                ++owners;
                assertEquals(String.format("%s: the node at %x and the root of its deletion branch at %x are in different pages",
                                           message, node.end, node.deletionBranch),
                             (node.end - 1) / pageSize,
                             (node.deletionBranch - 1) / pageSize);
            }
            assertEquals(message, result.deletionBranches, owners);
        }
    }

    /// Without chain folding and with room for the whole trie in a page, the page-aware writer gives a
    /// deletion-aware trie the same bytes as the immediate one: the deletion branch of a node is written first,
    /// followed by the node's children and the node itself.
    @Test
    public void testDeletionAwareWithoutLayoutMatchesImmediate() throws IOException
    {
        Random rand = new Random(SEED + 13);
        for (int i = 0; i < 40; ++i)
        {
            DeletionAwareTrie<String, TestRangeState> trie = i % 4 == 0 ? partitionsWithDeletionBranches(rand, 1 + rand.nextInt(60))
                                                                        : randomDeletionAwareTrie(rand, rand.nextInt(200), 1 + rand.nextInt(6));
            int startOffset = i % 3 == 0 ? 13 : 0;
            String message = "Seed " + SEED + " deletion-aware trie " + i + " at offset " + startOffset;

            DataOutputBuffer immediate = newOutput(0, startOffset);
            long immediateRoot = OnDiskDeletionAwareTrieWriter.write(trie, STRING_SERDE, RANGE_SERDE, immediate);

            DataOutputBuffer unfolded = newOutput(HUGE_PAGE, startOffset);
            long unfoldedRoot = new OnDiskDeletionAwareTrieWriter<>(new OnDiskNodeWriterPageAware<>(unfolded, STRING_SERDE, false), RANGE_SERDE).writeFrom(trie);

            assertEquals(message, immediateRoot, unfoldedRoot);
            assertArrayEquals(message, immediate.toByteArray(), unfolded.toByteArray());
        }
    }
}
