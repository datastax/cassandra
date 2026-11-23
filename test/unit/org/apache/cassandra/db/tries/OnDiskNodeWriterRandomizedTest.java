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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;

import org.agrona.DirectBuffer;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ByteBufferRebufferer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.apache.cassandra.db.tries.TrieUtil.RANGE_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.STRING_SERDE;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Seeded, randomized differential test of the two [OnDiskNodeWriter] implementations.
///
/// Every generated trie is written twice: as `A` by [OnDiskNodeWriterImmediate], whose layout has no relays, no
/// padding and no folded chains, and as `P` by the writer under test, which for the page-aware writer is run with
/// pages from 4096 bytes down to 17, the latter smaller than most nodes. The following must then hold:
/// 1. `read(P)` walks exactly like the source trie and like `read(A)`, in both directions.
/// 2. Writing `read(P)` with the immediate writer gives exactly the bytes of `A`. This is the canonical form of a
///    trie, and getting it also takes the walker's [Cursor#advanceMultiple] through every relay and folded chain.
///    That `immediate(read(A)) == A` is asserted for every trie first, and holds for all shapes generated here.
/// 3. Cursors over `read(A)`, `read(P)` and the source trie that are given the same random sequence of
///    [Cursor#advance], [Cursor#advanceMultiple], [Cursor#skipTo] and [Cursor#tailCursor] calls stay at the same
///    position with the same content (and, for range tries, the same [RangeCursor#state]) after every step, see
///    [Walk]. So do the tries [BaseTrie#tailTrie] gives and the tails of those, the walks that
///    [BaseTrie#forEachEntry] and [BaseTrie#dump] make, [BaseTrie#get] and [RangeTrie#applicableRange].
///
/// `read(A)` and `read(P)` are compared exactly, including the flags of the positions and the parts of a range state
/// that its `equals` ignores. The source trie is compared with [Object#equals], because an in-memory range cursor
/// may derive a covering state from a different marker than the on-disk one does.
///
/// Every failure names the seed, the shape with its parameters and the writer; [#runPlain] and [#runRange] run a
/// single such combination.
public class OnDiskNodeWriterRandomizedTest
{
    private static final long[] SEEDS = { 20261113L, 77001L, 4096L, 18906L, 5L, 990017L };

    /// The seeds of the file-backed test.
    private static final long FILE_SEED = 18906L;

    /// The page size that sizes the payloads of the tries written by the immediate writer alone.
    private static final int NOMINAL_PAGE = 256;

    @BeforeClass
    public static void setUp()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    private static List<TrieUtil.OnDiskWriterKind> kinds()
    {
        List<TrieUtil.OnDiskWriterKind> kinds = new ArrayList<>(TrieUtil.ON_DISK_WRITER_KINDS);
        kinds.add(TrieUtil.pageAware(100));     // not a power of two
        kinds.add(TrieUtil.pageAware(17));      // smaller than most nodes
        return kinds;
    }

    @Test
    public void testUnorderedTries()
    {
        testPlainTries(false);
    }

    @Test
    public void testOrderedTries()
    {
        testPlainTries(true);
    }

    private void testPlainTries(boolean ordered)
    {
        List<TrieUtil.OnDiskWriterKind> kinds = kinds();
        for (int seedIndex = 0; seedIndex < SEEDS.length; ++seedIndex)
            for (Shape shape : Shape.values())
                for (int repetition = 0; repetition < shape.repetitions; ++repetition)
                    for (int kindIndex = 0; kindIndex < kinds.size(); ++kindIndex)
                        if ((kindIndex + seedIndex + repetition) % shape.kindStride == 0)
                            runPlain(SEEDS[seedIndex], shape, repetition, ordered, kinds.get(kindIndex));
    }

    @Test
    public void testRangeTries()
    {
        List<TrieUtil.OnDiskWriterKind> kinds = kinds();
        for (int seedIndex = 0; seedIndex < SEEDS.length; ++seedIndex)
            for (Shape shape : Shape.values())
                for (int repetition = 0; repetition < shape.repetitions; ++repetition)
                    for (int kindIndex = 0; kindIndex < kinds.size(); ++kindIndex)
                        if ((kindIndex + seedIndex + repetition) % shape.rangeKindStride == 0)
                            runRange(SEEDS[seedIndex], shape, repetition, kinds.get(kindIndex));
    }

    /// Runs all checks for one generated plain trie and one writer.
    private static void runPlain(long seed, Shape shape, int repetition, boolean ordered, TrieUtil.OnDiskWriterKind kind)
    {
        run(plainCase(seed, shape, repetition, ordered, pageFor(kind)), kind, seed);
    }

    /// Runs all checks for one generated range trie and one writer.
    private static void runRange(long seed, Shape shape, int repetition, TrieUtil.OnDiskWriterKind kind)
    {
        run(rangeCase(seed, shape, repetition, pageFor(kind)), kind, seed);
    }

    private static int pageFor(TrieUtil.OnDiskWriterKind kind)
    {
        return kind.isPageAware() ? kind.pageSize : NOMINAL_PAGE;
    }

    private static <T> void run(Case<T> testCase, TrieUtil.OnDiskWriterKind kind, long seed)
    {
        Random rand = new Random(seed * 31 + kind.pageSize);
        // A trie is not always written at the start of the output, or of a page.
        int offset = rand.nextBoolean() ? 0 : 1 + rand.nextInt(2 * pageFor(kind));
        try
        {
            check(testCase, kind, offset, rand);
        }
        catch (Throwable t)
        {
            throw new AssertionError(testCase.description + ", written by " + kind + " at offset " + offset + ": " + t, t);
        }
    }

    // Generation

    private static final int[] EDGE_BYTES = { 0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff };

    /// Numbers of children on both sides of the limits of the node types: a sparse node holds up to 25, and a node
    /// with one- or two-byte pointers becomes dense at 224 and 240 children respectively.
    private static final int[] WIDE_COUNTS = { 26, 33, 100, 223, 224, 225, 239, 240, 241, 250, 256 };

    /// Lengths of single-child runs on both sides of multiples of the longest chain node, 64.
    private static final int[] RUN_LENGTHS = { 1, 2, 63, 64, 65, 66, 127, 128, 129, 130, 193, 257 };

    private static final int[] SMALL_SIZES = { 0, 1, 1, 2, 3, 5, 8, 13 };

    /// The keys of a trie to generate, with the size of the payload of each.
    private static final class Keys
    {
        final TreeMap<byte[], Integer> payloadSizes = new TreeMap<>(Arrays::compareUnsigned);

        void add(byte[] key, int payloadSize)
        {
            payloadSizes.put(key, payloadSize);
        }
    }

    /// The shapes of the generated tries. Each gives the keys and payload sizes for a seed; `page` is the size of the
    /// pages the trie is going to be written to, for payloads that are about as big as a page or bigger.
    private enum Shape
    {
        /// A handful of short keys over a few bytes, with payloads of all the interesting sizes. Often small enough
        /// to stay in one page, and sometimes empty.
        TINY(4, 1, 1)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] alphabet = alphabet(rand, 1 + rand.nextInt(4));
                int count = rand.nextInt(13);
                for (int i = 0; i < count; ++i)
                    keys.add(randomKey(rand, alphabet, rand.nextInt(7)), boundarySize(rand, page));
            }
        },

        /// Many keys over two or three bytes: most keys are prefixes of others and all nodes are sparse.
        PREFIXES(2, 1, 1)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] alphabet = alphabet(rand, 2 + rand.nextInt(2));
                int count = 20 + rand.nextInt(130);
                for (int i = 0; i < count; ++i)
                    keys.add(randomKey(rand, alphabet, rand.nextInt(11)), mixedSize(rand, page, 10));
            }
        },

        /// One to three nodes with more children than a sparse node can hold, each below a child of the previous
        /// one, giving bitmap and dense nodes with pointers of various sizes.
        WIDE(2, 1, 2)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] alphabet = alphabet(rand, 2);
                byte[] prefix = randomKey(rand, alphabet, rand.nextInt(4));
                int nodes = 1 + rand.nextInt(3);
                for (int node = 0; node < nodes; ++node)
                {
                    int count = WIDE_COUNTS[rand.nextInt(WIDE_COUNTS.length)];
                    // Children of a byte or two each put all of them within reach of single-byte pointers, or just
                    // out of it: the limit is a distance of 255, which a dense node cannot use.
                    boolean compact = rand.nextInt(3) == 0;
                    byte[] next = null;
                    for (int transition : alphabet(rand, count))
                    {
                        byte[] child = append(prefix, transition);
                        int below = compact ? -1 : rand.nextInt(10);
                        if (below < 0)
                            keys.add(child, rand.nextInt(count) == 0 ? 1 : 0);
                        else if (below < 6)
                            keys.add(child, mixedSize(rand, page, 30));
                        else if (below < 9)
                            for (int i = rand.nextInt(3); i >= 0; --i)
                                keys.add(concat(child, randomKey(rand, alphabet, 1 + rand.nextInt(3))), mixedSize(rand, page, 30));
                        else
                            keys.add(concat(child, randomBytes(rand, 60 + rand.nextInt(10))), smallSize(rand));

                        if (next == null || rand.nextInt(count) == 0)
                            next = child;
                    }
                    if (rand.nextBoolean())
                        keys.add(prefix, smallSize(rand));
                    prefix = next;
                }
            }
        },

        /// A node with enough children to be dense when its pointers are short, which they are when the children are
        /// small: most of them here are leaves of a single byte. The first child and a few others are bigger, so that
        /// a writer that places the children by size puts them away from the node, around the distance of 255 where
        /// a dense node has to change the size of its pointers.
        DENSE(3, 1, 2)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                byte[] prefix = randomBytes(rand, rand.nextInt(3));
                int[] transitions = alphabet(rand, 224 + rand.nextInt(33));
                int first = Arrays.stream(transitions).min().getAsInt();
                for (int transition : transitions)
                    keys.add(append(prefix, transition),
                             transition == first ? 2 + rand.nextInt(60)
                                                 : rand.nextInt(60) == 0 ? 1 + rand.nextInt(30) : 0);
                if (rand.nextBoolean())
                    keys.add(prefix, smallSize(rand));
            }
        },

        /// The same around the next limit: the children are leaves of a few hundred bytes that together take about
        /// 64k, the reach of two-byte pointers.
        DENSE_64K(1, 3, 7)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] transitions = alphabet(rand, 240 + rand.nextInt(17));
                int first = Arrays.stream(transitions).min().getAsInt();
                // a leaf with a payload of this size takes three bytes more
                int average = (0xFFFF - 700 + rand.nextInt(1400)) / (transitions.length - 1) - 3;
                for (int transition : transitions)
                    keys.add(new byte[]{ (byte) transition },
                             transition == first ? average + 100 + rand.nextInt(200)
                                                 : average - 20 + rand.nextInt(41));
            }
        },

        /// Long runs of single transitions, which branch off each other and have content in the middle.
        CHAINS(2, 1, 1)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int runs = 1 + rand.nextInt(6);
                byte[] previous = new byte[0];
                for (int run = 0; run < runs; ++run)
                {
                    int shared = rand.nextInt(Math.min(previous.length, 300) + 1);
                    byte[] key = concat(Arrays.copyOf(previous, shared),
                                        randomBytes(rand, RUN_LENGTHS[rand.nextInt(RUN_LENGTHS.length)]));
                    keys.add(key, mixedSize(rand, page, 4));
                    for (int i = rand.nextInt(4); i > 0; --i)
                    {
                        int length;
                        switch (rand.nextInt(6))
                        {
                            case 0: length = key.length - 1; break;
                            case 1: length = key.length - 64; break;
                            case 2: length = key.length - 65; break;
                            case 3: length = shared + 1; break;
                            case 4: length = shared; break;
                            default: length = rand.nextInt(key.length); break;
                        }
                        keys.add(Arrays.copyOf(key, Math.max(0, length)), mixedSize(rand, page, 4));
                    }
                    previous = key;
                }
            }
        },

        /// Keys nested in each other to more levels than the page-aware writer keeps buffered
        /// ([OnDiskNodeWriterPageAware#MAX_BUFFERED_DEPTH]), with payloads small enough for all of it to fit a page.
        DEEP(1, 1, 2)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] alphabet = alphabet(rand, 3);
                int levels = OnDiskNodeWriterPageAware.MAX_BUFFERED_DEPTH + 1 + rand.nextInt(130);
                boolean minimal = rand.nextBoolean();
                byte[] key = new byte[0];
                for (int level = 0; level < levels; ++level)
                {
                    key = concat(key, randomKey(rand, alphabet, 1 + rand.nextInt(2)));
                    keys.add(key, minimal ? rand.nextInt(2) : mixedSize(rand, page, 20));
                    if (rand.nextInt(8) == 0)
                        keys.add(append(Arrays.copyOf(key, key.length - 1), rand.nextInt(256)), smallSize(rand));
                }
            }
        },

        /// A few hundred keys with a mixture of everything.
        RANDOM(1, 2, 3)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] common = alphabet(rand, 3);
                int count = 200 + rand.nextInt(800);
                for (int i = 0; i < count; ++i)
                {
                    int length = 1 + rand.nextInt(12);
                    byte[] key = new byte[length + (rand.nextInt(20) == 0 ? 100 : 0)];
                    for (int j = 0; j < key.length; ++j)
                        key[j] = (byte) (j < length && rand.nextInt(4) != 0 ? common[rand.nextInt(common.length)]
                                                                           : rand.nextInt(256));
                    keys.add(key, mixedSize(rand, page, 25));
                }
            }
        },

        /// A wide node whose children are all wide too, for bitmap and dense nodes over branches of many sizes.
        TWO_LEVEL(1, 3, 7)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                for (int first : alphabet(rand, 26 + rand.nextInt(231)))
                    for (int second : alphabet(rand, 2 + rand.nextInt(40)))
                        keys.add(new byte[]{ (byte) first, (byte) second }, mixedSize(rand, page, 200));
            }
        },

        /// A few keys whose payloads are mostly about as big as a page or bigger.
        BIG_PAYLOADS(2, 1, 1)
        {
            @Override
            void generate(Random rand, int page, Keys keys)
            {
                int[] alphabet = alphabet(rand, 2 + rand.nextInt(3));
                int count = 1 + rand.nextInt(25);
                for (int i = 0; i < count; ++i)
                    keys.add(randomKey(rand, alphabet, rand.nextInt(5)),
                             rand.nextInt(4) == 0 ? smallSize(rand)
                                                  : rand.nextBoolean() ? aboutPageSize(rand, page)
                                                                       : overPageSize(rand, page));
            }
        };

        /// The number of tries of this shape to generate per seed.
        final int repetitions;
        /// The heavier shapes are not run with all writers for every seed: plain tries of the shape are run with one
        /// in `kindStride` writers, range tries with one in `rangeKindStride`, a different selection for each seed.
        final int kindStride;
        final int rangeKindStride;

        Shape(int repetitions, int kindStride, int rangeKindStride)
        {
            this.repetitions = repetitions;
            this.kindStride = kindStride;
            this.rangeKindStride = rangeKindStride;
        }

        abstract void generate(Random rand, int page, Keys keys);

        Keys keys(Random rand, int page)
        {
            Keys keys = new Keys();
            generate(rand, page, keys);
            return keys;
        }
    }

    /// The given number of distinct byte values, biased towards the ends and the middle of the range.
    private static int[] alphabet(Random rand, int size)
    {
        BitSet used = new BitSet(256);
        int[] alphabet = new int[size];
        for (int i = 0; i < size; )
        {
            int b = rand.nextInt(3) == 0 ? EDGE_BYTES[rand.nextInt(EDGE_BYTES.length)] : rand.nextInt(256);
            if (used.get(b))
                continue;
            used.set(b);
            alphabet[i++] = b;
        }
        return alphabet;
    }

    private static byte[] randomKey(Random rand, int[] alphabet, int length)
    {
        byte[] key = new byte[length];
        for (int i = 0; i < length; ++i)
            key[i] = (byte) alphabet[rand.nextInt(alphabet.length)];
        return key;
    }

    private static byte[] randomBytes(Random rand, int length)
    {
        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        return bytes;
    }

    private static byte[] append(byte[] key, int transition)
    {
        byte[] result = Arrays.copyOf(key, key.length + 1);
        result[key.length] = (byte) transition;
        return result;
    }

    private static byte[] concat(byte[] left, byte[] right)
    {
        byte[] result = Arrays.copyOf(left, left.length + right.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static int smallSize(Random rand)
    {
        return SMALL_SIZES[rand.nextInt(SMALL_SIZES.length)];
    }

    /// A payload size such that the node that holds the payload is within a few bytes of filling a page.
    private static int aboutPageSize(Random rand, int page)
    {
        return Math.max(0, page - 5 + rand.nextInt(7));
    }

    private static int overPageSize(Random rand, int page)
    {
        switch (rand.nextInt(4))
        {
            case 0: return page + 2;
            case 1: return page + 2 + rand.nextInt(page);
            case 2: return 2 * page + 3;
            default: return 4 * page + 1;
        }
    }

    /// A payload size on one of the boundaries of the format (a leaf holds up to 63 bytes, a one-byte length up to
    /// 127) or of the page.
    private static int boundarySize(Random rand, int page)
    {
        switch (rand.nextInt(8))
        {
            case 0: return 0;
            case 1: return 1;
            case 2: return 63;
            case 3: return 64;
            case 4: return 127;
            case 5: return 128;
            case 6: return aboutPageSize(rand, page);
            default: return overPageSize(rand, page);
        }
    }

    private static int mixedSize(Random rand, int page, int boundaryOneIn)
    {
        return rand.nextInt(boundaryOneIn) == 0 ? boundarySize(rand, page) : smallSize(rand);
    }

    /// A payload of the given size in bytes that is different for different indexes if the size permits.
    private static byte[] payload(int index, int size)
    {
        byte[] token = (Integer.toString(index, 36) + ':').getBytes(StandardCharsets.US_ASCII);
        byte[] payload = new byte[size];
        for (int i = 0; i < size; ++i)
            payload[i] = token[i % token.length];
        return payload;
    }

    private static ByteComparable.Preencoded comparable(byte[] key)
    {
        return ByteComparable.preencoded(VERSION, key);
    }

    /// A trie to test with.
    private static final class Case<T>
    {
        final String description;
        final BaseTrie<T, ?, ?> source;
        final boolean isOrdered;
        final OnDiskTrieWriter.DataSerializer<T> serializer;
        final Reader<T> reader;
        final List<byte[]> keys;

        Case(String description, BaseTrie<T, ?, ?> source, boolean isOrdered, OnDiskTrieWriter.DataSerializer<T> serializer, Reader<T> reader, List<byte[]> keys)
        {
            this.description = description;
            this.source = source;
            this.isOrdered = isOrdered;
            this.serializer = serializer;
            this.reader = reader;
            this.keys = keys;
        }
    }

    private interface Reader<T>
    {
        /// Opens the trie in the given buffer, whose root is at the given position, or which is empty if that is 0.
        BaseTrie<T, ?, ?> read(ByteBuffer buffer, long root);
    }

    private static Random generator(long seed, Shape shape, int repetition)
    {
        return new Random((seed * 1000003 + shape.ordinal()) * 1009 + repetition);
    }

    private static Case<String> plainCase(long seed, Shape shape, int repetition, boolean ordered, int page)
    {
        Random rand = generator(seed, shape, repetition);
        Keys keys = shape.keys(rand, page);
        return plainCase(String.format("seed %d shape %s#%d %s page %d", seed, shape, repetition, ordered ? "ordered" : "unordered", page),
                         keys, ordered, rand);
    }

    private static Case<String> plainCase(String description, Keys keys, boolean ordered, Random rand)
    {
        InMemoryTrie<String> trie = ordered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        int index = 0;
        try
        {
            for (Map.Entry<byte[], Integer> entry : keys.payloadSizes.entrySet())
            {
                // In an ordered trie content can also be placed after the branch below its key, where a forward walk
                // presents it on the return path and the on-disk node holds it in its other content slot. (Not on both
                // sides of the same key: the cursor of a plain in-memory trie only looks at one content per node.)
                boolean afterBranch = ordered && rand.nextInt(3) == 0;
                trie.putRecursive(comparable(entry.getKey()),
                                  new String(payload(index++, entry.getValue()), StandardCharsets.US_ASCII),
                                  afterBranch,
                                  (existing, update) -> update);
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        return new Case<>(description + ": " + keys.payloadSizes.size() + " keys",
                          trie,
                          ordered,
                          STRING_SERDE,
                          (buffer, root) -> new OnDiskTrie<>(new ByteBufferRebufferer(buffer), STRING_SERDE, VERSION, ordered, root),
                          new ArrayList<>(keys.payloadSizes.keySet()));
    }

    /// One side of a key of a range trie, where a marker can be.
    private static final class Point
    {
        final byte[] key;
        final boolean afterBranch;
        final int payloadSize;

        Point(byte[] key, boolean afterBranch, int payloadSize)
        {
            this.key = key;
            this.afterBranch = afterBranch;
            this.payloadSize = payloadSize;
        }
    }

    /// The order in which a forward walk meets the points: a key is met before the branch below it, which is before
    /// the return to the key.
    private static final Comparator<Point> WALK_ORDER = (l, r) ->
    {
        int mismatch = Arrays.mismatch(l.key, r.key);
        if (mismatch < 0)
            return Boolean.compare(l.afterBranch, r.afterBranch);
        if (mismatch == l.key.length)
            return l.afterBranch ? 1 : -1;
        if (mismatch == r.key.length)
            return r.afterBranch ? -1 : 1;
        return Integer.compare(l.key[mismatch] & 0xFF, r.key[mismatch] & 0xFF);
    };

    private static Case<TestRangeState> rangeCase(long seed, Shape shape, int repetition, int page)
    {
        Random rand = generator(seed, shape, repetition);
        Keys keys = shape.keys(rand, page);

        List<Point> points = new ArrayList<>();
        for (Map.Entry<byte[], Integer> entry : keys.payloadSizes.entrySet())
        {
            int placement = rand.nextInt(5);
            if (placement != 3)
                points.add(new Point(entry.getKey(), false, entry.getValue()));
            if (placement >= 3)
                points.add(new Point(entry.getKey(), true, placement == 3 ? entry.getValue() : smallSize(rand)));
        }
        points.sort(WALK_ORDER);

        // Put a boundary on every point, each continuing with what the one before it left active.
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        int markers = 0;
        int active = -1;
        try
        {
            for (int i = 0; i < points.size(); ++i)
            {
                Point point = points.get(i);
                boolean last = i == points.size() - 1;
                // Values of various sizes when serialized.
                int next = last || rand.nextInt(4) == 0 ? -1 : 1 + rand.nextInt(rand.nextBoolean() ? 100 : 200000);
                if (next == active)
                {
                    if (last)
                        break;      // nothing to close
                    next = active < 0 ? 1 + rand.nextInt(100) : active + 1;
                }
                // The state's position is not used by the trie; it is serialized with the state, which makes it a
                // way to control the size of the payload. The rest of the state takes at least three bytes.
                ByteComparable padding = comparable(payload(i, Math.max(0, point.payloadSize - 3)));
                TestRangeState state = new TestRangeState(padding, point.afterBranch, active, next);
                trie.putRecursive(comparable(point.key), state, point.afterBranch, (existing, update) -> update);
                active = next;
                ++markers;
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        return new Case<>(String.format("seed %d range shape %s#%d page %d: %d keys, %d markers", seed, shape, repetition, page, keys.payloadSizes.size(), markers),
                          trie,
                          true,
                          RANGE_SERDE,
                          (buffer, root) -> new OnDiskRangeTrie<>(new ByteBufferRebufferer(buffer), RANGE_SERDE, VERSION, root),
                          new ArrayList<>(keys.payloadSizes.keySet()));
    }

    // Checks

    private static final class Written
    {
        final byte[] bytes;
        final long root;

        Written(byte[] bytes, long root)
        {
            this.bytes = bytes;
            this.root = root;
        }
    }

    private static <T> Written write(BaseTrie<T, ?, ?> trie, Case<T> testCase, TrieUtil.OnDiskWriterKind kind) throws IOException
    {
        return write(trie, testCase, kind, 0);
    }

    /// Writes the trie with the given kind of writer to an output that already has `offset` bytes in it.
    private static <T> Written write(BaseTrie<T, ?, ?> trie, Case<T> testCase, TrieUtil.OnDiskWriterKind kind, int offset) throws IOException
    {
        try (DataOutputBuffer out = kind.newOutput())
        {
            for (int i = 0; i < offset; ++i)
                out.writeByte(TrieUtil.PagedDataOutputBuffer.PADDING);
            long root = OnDiskTrieWriter.write(trie, testCase.isOrdered, testCase.serializer, out, kind.nodeWriterType);
            byte[] bytes = out.toByteArray();
            assertEquals("Root position reported by " + kind, bytes.length == offset ? 0 : bytes.length, root);
            return new Written(bytes, root);
        }
    }

    private static <T> BaseTrie<T, ?, ?> read(Case<T> testCase, Written written)
    {
        // an empty trie has no root, which the readers expect to be given as 0
        return testCase.reader.read(ByteBuffer.wrap(written.bytes), Math.max(written.root, 0));
    }

    private static <T> void check(Case<T> testCase, TrieUtil.OnDiskWriterKind kind, int offset, Random rand) throws IOException
    {
        Written immediate = write(testCase.source, testCase, TrieUtil.IMMEDIATE);
        BaseTrie<T, ?, ?> readImmediate = read(testCase, immediate);
        // What the immediate writer makes is the canonical form of a trie, which only depends on what a walk of the
        // trie presents and thus must come out the same when it is written again from what was read.
        assertBytesEqual("immediate(read(IMMEDIATE))", immediate.bytes, write(readImmediate, testCase, TrieUtil.IMMEDIATE).bytes);

        Written tested = write(testCase.source, testCase, kind, offset);
        BaseTrie<T, ?, ?> readTested = read(testCase, tested);
        // All pointers are relative, so what the immediate writer makes does not depend on where it is written.
        if (!kind.isPageAware())
            assertBytesEqual("the immediate writer at offset " + offset, immediate.bytes, Arrays.copyOfRange(tested.bytes, offset, tested.bytes.length));

        TrieUtil.assertTriesEqual(testCase.source, readTested);
        TrieUtil.assertTriesEqual(readImmediate, readTested);
        assertBytesEqual("immediate(read(" + kind + "))", immediate.bytes, write(readTested, testCase, TrieUtil.IMMEDIATE).bytes);

        compareTries(testCase.source, readImmediate, readTested, testCase.keys, rand);
    }

    private static void assertBytesEqual(String what, byte[] expected, byte[] actual)
    {
        int mismatch = Arrays.mismatch(expected, actual);
        if (mismatch < 0)
            return;
        int from = Math.max(0, mismatch - 16);
        fail(String.format("%s differs from what the immediate writer wrote: %d bytes instead of %d, first difference at %d; expected %s, got %s from %d",
                           what, actual.length, expected.length, mismatch, hex(expected, from, mismatch + 16), hex(actual, from, mismatch + 16), from));
    }

    private static String hex(byte[] bytes, int from, int to)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = from; i < Math.min(to, bytes.length); ++i)
            builder.append(String.format("%02x", bytes[i]));
        return builder.toString();
    }

    /// The differential probes: everything that can be asked of the three tries must give the same answers.
    ///
    /// @param source the trie that was written, or null if the tries are not to be compared with it
    /// @param readImmediate what the immediate writer wrote
    /// @param readTested what the writer under test wrote
    /// @param keys the keys of the tries, to steer the probes to where the content is
    private static <T> void compareTries(BaseTrie<T, ?, ?> source, BaseTrie<T, ?, ?> readImmediate, BaseTrie<T, ?, ?> readTested, List<byte[]> keys, Random rand)
    {
        compareContentWalks(source, readImmediate, readTested);

        boolean small = keys.size() <= 40;
        for (Direction direction : Direction.values())
        {
            // A walk with as few stops as the tries can make.
            try (Walk<T> walk = Walk.over(readImmediate, readTested, source, direction))
            {
                walk.start();
                while (!Cursor.isExhausted(walk.position))
                    walk.advanceMultiple();
            }

            // Tails at every position of a small trie.
            if (small)
            {
                try (Walk<T> walk = Walk.over(readImmediate, readTested, source, direction))
                {
                    walk.start();
                    while (!Cursor.isExhausted(walk.position))
                    {
                        compareTails(walk, rand, null, 0, 6);
                        walk.advance();
                    }
                }
            }

            for (int i = small ? 12 : 6; i > 0; --i)
            {
                try (Walk<T> walk = Walk.over(readImmediate, readTested, source, direction))
                {
                    walk.start();
                    randomWalk(walk, rand, keys.isEmpty() ? null : probeKey(rand, keys), small ? 60 : 150, 2);
                }
            }

            for (int i = Math.min(30, 6 + keys.size() / 8); i > 0; --i)
            {
                try (Walk<T> walk = Walk.over(readImmediate, readTested, source, direction))
                {
                    walk.start();
                    byte[] key = probeKey(rand, keys);
                    walk.descendAlong(key);
                    if (rand.nextBoolean())
                        compareTails(walk, rand, null, 1, 12);
                    randomWalk(walk, rand, key, 12, 1);
                }
            }
        }

        compareTailTries(source, readImmediate, readTested, keys, rand, Math.min(12, 4 + keys.size() / 20), 2);
        compareLookups(source, readImmediate, readTested, keys, rand, Math.min(60, 10 + keys.size() / 4));
    }

    /// Compares the walks made by [BaseTrie#forEachEntry] and [BaseTrie#dump], which go from content to content with
    /// [Cursor#advanceMultiple] and reconstruct the keys from the transitions it reports.
    private static <T> void compareContentWalks(BaseTrie<T, ?, ?> source, BaseTrie<T, ?, ?> readImmediate, BaseTrie<T, ?, ?> readTested)
    {
        for (Direction direction : Direction.values())
        {
            List<String> expected = entries(readImmediate, direction);
            assertListsEqual("forEachEntry " + direction + " of the trie under test vs the immediate one", expected, entries(readTested, direction));
            if (source != null)
                assertListsEqual("forEachEntry " + direction + " of the immediate trie vs the source", entries(source, direction), expected);
        }
        assertEquals("dump of the trie under test vs the immediate one", readImmediate.dump(), readTested.dump());
    }

    private static <T> List<String> entries(BaseTrie<T, ?, ?> trie, Direction direction)
    {
        List<String> entries = new ArrayList<>();
        trie.forEachEntry(direction, (key, content) -> entries.add(key.byteComparableAsString(VERSION) + " -> " + content));
        return entries;
    }

    private static void assertListsEqual(String what, List<String> expected, List<String> actual)
    {
        for (int i = 0; i < Math.min(expected.size(), actual.size()); ++i)
            if (!expected.get(i).equals(actual.get(i)))
                fail(String.format("%s: entry %d is %s instead of %s", what, i, abbreviate(actual.get(i)), abbreviate(expected.get(i))));
        assertEquals(what + ": number of entries", expected.size(), actual.size());
    }

    private static String abbreviate(Object value)
    {
        String string = String.valueOf(value);
        return string.length() <= 80 ? string : string.substring(0, 80) + "...(" + string.length() + " chars)";
    }

    /// A key to probe the tries with: one of their keys, or something that is close to one.
    private static byte[] probeKey(Random rand, List<byte[]> keys)
    {
        if (keys.isEmpty() || rand.nextInt(10) == 0)
            return randomBytes(rand, rand.nextInt(5));

        byte[] key = keys.get(rand.nextInt(keys.size()));
        switch (rand.nextInt(8))
        {
            case 0:
            case 1:
                // a prefix, which is usually not a key itself
                return Arrays.copyOf(key, rand.nextInt(key.length + 1));
            case 2:
                // one or more bytes past the key
                return concat(key, randomBytes(rand, 1 + rand.nextInt(3)));
            case 3:
                // next to the key or to one of its prefixes
                if (key.length == 0)
                    return key;
                byte[] near = Arrays.copyOf(key, 1 + rand.nextInt(key.length));
                near[near.length - 1] += rand.nextBoolean() ? 1 : -1;
                return near;
            default:
                return key;
        }
    }

    /// Follows the given key in the cursors, the way [Cursor#descendAlong] does, taking tails on the way, and makes a
    /// random walk from where that ends.
    private static <T> void randomWalk(Walk<T> walk, Random rand, byte[] guide, int steps, int tailNesting)
    {
        while (steps-- > 0 && !Cursor.isExhausted(walk.position))
        {
            int choice = rand.nextInt(100);
            if (choice < 25)
                walk.advance();
            else if (choice < 40)
                walk.advanceMultiple();
            else if (choice < 90 || tailNesting == 0)
            {
                long target = walk.randomSkipTarget(rand, guide);
                if (target == Walk.NO_TARGET)
                    walk.advance();
                else
                    walk.skipTo(target);
            }
            else
                compareTails(walk, rand, guide, tailNesting - 1, Math.min(steps, 25));
        }
    }

    /// Takes the tails in both directions at the current position of the walk, and makes a random walk in each. With
    /// `nesting`, does the same with the tails first.
    private static <T> void compareTails(Walk<T> walk, Random rand, byte[] guide, int nesting, int steps)
    {
        if (Cursor.isExhausted(walk.position) || Cursor.isOnReturnPath(walk.position))
            return;     // no tails can be taken there

        // If the walk has been following the guide, the tails can continue with what remains of it.
        int depth = Cursor.depth(walk.position);
        byte[] tailGuide = guide != null && guide.length > depth ? Arrays.copyOfRange(guide, depth, guide.length) : null;
        for (Direction direction : Direction.values())
        {
            try (Walk<T> tail = walk.tail(direction))
            {
                tail.start();
                if (nesting > 0)
                    compareTails(tail, rand, tailGuide, nesting - 1, steps);
                randomWalk(tail, rand, tailGuide, steps, nesting);
            }
        }
    }

    /// Compares the tries that [BaseTrie#tailTrie] gives for some keys, and the tails of those.
    private static <T> void compareTailTries(BaseTrie<T, ?, ?> source, BaseTrie<T, ?, ?> readImmediate, BaseTrie<T, ?, ?> readTested, List<byte[]> keys, Random rand, int count, int nesting)
    {
        for (int i = 0; i < count; ++i)
        {
            byte[] key = probeKey(rand, keys);
            String what = "tailTrie(" + hex(key, 0, key.length) + ")";
            BaseTrie<T, ?, ?> tailImmediate = readImmediate.tailTrie(comparable(key));
            BaseTrie<T, ?, ?> tailTested = readTested.tailTrie(comparable(key));
            BaseTrie<T, ?, ?> tailSource = source != null ? source.tailTrie(comparable(key)) : null;
            assertEquals(what + " exists in the trie under test", tailImmediate != null, tailTested != null);
            if (source != null)
                assertEquals(what + " exists in the source trie", tailImmediate != null, tailSource != null);
            if (tailImmediate == null)
                continue;

            // The keys of the tail are what follows the probe in the keys of the trie.
            List<byte[]> tailKeys = new ArrayList<>();
            for (byte[] candidate : keys)
                if (candidate.length >= key.length && Arrays.equals(candidate, 0, key.length, key, 0, key.length))
                    tailKeys.add(Arrays.copyOfRange(candidate, key.length, candidate.length));

            try
            {
                for (Direction direction : Direction.values())
                {
                    try (Walk<T> walk = Walk.over(tailImmediate, tailTested, tailSource, direction))
                    {
                        walk.start();
                        // all of a small tail, the beginning of a big one
                        for (int steps = 200; steps > 0 && !Cursor.isExhausted(walk.position); --steps)
                            walk.advance();
                    }
                    try (Walk<T> walk = Walk.over(tailImmediate, tailTested, tailSource, direction))
                    {
                        walk.start();
                        randomWalk(walk, rand, tailKeys.isEmpty() ? null : probeKey(rand, tailKeys), 40, 1);
                    }
                }
                if (nesting > 0)
                    compareTailTries(tailSource, tailImmediate, tailTested, tailKeys, rand, 2, nesting - 1);
            }
            catch (Throwable t)
            {
                throw new AssertionError("In " + what + ": " + t, t);
            }
        }
    }

    /// Compares the results of [BaseTrie#get] and, for range tries, [RangeTrie#applicableRange].
    ///
    /// The source trie is not asked what it gets: [InMemoryTrie] looks its nodes up directly and, unlike the walk
    /// along the key that the other tries make, also returns the content placed after the branch of a key that has
    /// no children.
    private static <T> void compareLookups(BaseTrie<T, ?, ?> source, BaseTrie<T, ?, ?> readImmediate, BaseTrie<T, ?, ?> readTested, List<byte[]> keys, Random rand, int count)
    {
        for (int i = 0; i < count; ++i)
        {
            byte[] key = probeKey(rand, keys);
            String at = " at " + hex(key, 0, key.length);
            T expected = readImmediate.get(comparable(key));
            T actual = readTested.get(comparable(key));
            assertTrue("get" + at + " in the trie under test: " + abbreviate(actual) + " instead of " + abbreviate(expected), identical(expected, actual));

            if (readImmediate instanceof RangeTrie)
            {
                Object applicable = ((RangeTrie<?>) readImmediate).applicableRange(comparable(key));
                Object applicableTested = ((RangeTrie<?>) readTested).applicableRange(comparable(key));
                assertTrue("applicableRange" + at + " in the trie under test: " + abbreviate(applicableTested) + " instead of " + abbreviate(applicable),
                           identical(applicable, applicableTested));
                if (source != null)
                    assertEquals("applicableRange" + at + " in the immediate trie",
                                 ((RangeTrie<?>) source).applicableRange(comparable(key)),
                                 applicable);
            }
        }
    }

    /// Whether the two values are equal in everything, including what [TestRangeState#equals] does not look at.
    private static boolean identical(Object left, Object right)
    {
        if (left == right)
            return true;
        if (left == null || right == null)
            return false;
        if (!(left instanceof TestRangeState) || !(right instanceof TestRangeState))
            return left.equals(right);

        TestRangeState l = (TestRangeState) left;
        TestRangeState r = (TestRangeState) right;
        return l.leftSide == r.leftSide
               && l.rightSide == r.rightSide
               && l.appliesAfter == r.appliesAfter
               && ByteComparable.compare(l.position, r.position, VERSION) == 0;
    }

    /// The path to the position of a cursor, collected from the transitions [Cursor#advanceMultiple] reports and the
    /// positions the cursor returns.
    private static final class Path implements Cursor.TransitionsReceiver
    {
        byte[] bytes = new byte[64];
        int length;

        @Override
        public void addPathByte(int nextByte)
        {
            if (length == bytes.length)
                bytes = Arrays.copyOf(bytes, length * 2);
            bytes[length++] = (byte) nextByte;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addPathByte(buffer.getByte(pos + i) & 0xFF);
        }

        /// To be called with the position a cursor returned when it was advanced in any way.
        long arrive(long position)
        {
            int depth = Cursor.depth(position);
            length = Math.max(depth - 1, 0);
            if (depth > 0)
                addPathByte(Cursor.incomingTransition(position));
            return position;
        }

        boolean sameAs(Path other)
        {
            return length == other.length && Arrays.equals(bytes, 0, length, other.bytes, 0, length);
        }

        @Override
        public String toString()
        {
            return hex(bytes, 0, length);
        }
    }

    /// Cursors over tries that must be the same, advanced together: one over what the immediate writer wrote, which
    /// is the reference, one over what the writer under test wrote, and, optionally, one over the trie both were
    /// written from. Every method that advances them checks that they end up at the same place with the same content.
    ///
    /// Closes its cursors, so that walks over file-backed tries return their buffers.
    private static final class Walk<T> implements AutoCloseable
    {
        static final long NO_TARGET = Long.MAX_VALUE;
        private static final int HISTORY = 40;

        private final Walk<T> tailOf;
        private final Cursor<T> immediate;
        private final Cursor<T> tested;
        private Cursor<T> source;
        private final Path immediatePath = new Path();
        private final Path testedPath = new Path();
        private final Path sourcePath = new Path();
        private final Direction direction;

        // The last calls made, for the messages.
        private final char[] calls = new char[HISTORY];
        private final long[] arguments = new long[HISTORY];
        private int callCount;

        // The positions the cursors returned when they were last advanced.
        private long immediatePosition;
        private long testedPosition;
        private long sourcePosition;

        /// The position all cursors are at.
        long position;

        private Walk(Walk<T> tailOf, Cursor<T> immediate, Cursor<T> tested, Cursor<T> source)
        {
            this.tailOf = tailOf;
            this.immediate = immediate;
            this.tested = tested;
            this.source = source;
            this.direction = immediate.direction();
            this.position = this.immediatePosition = immediate.encodedPosition();
            this.testedPosition = tested.encodedPosition();
            this.sourcePosition = source != null ? source.encodedPosition() : 0;
        }

        static <T> Walk<T> over(BaseTrie<T, ?, ?> readImmediate, BaseTrie<T, ?, ?> readTested, BaseTrie<T, ?, ?> source, Direction direction)
        {
            Cursor<T> immediate = null;
            Cursor<T> tested = null;
            Cursor<T> original = null;
            try
            {
                immediate = readImmediate.cursor(direction);
                tested = readTested.cursor(direction);
                original = source != null ? source.cursor(direction) : null;
                return new Walk<>(null, immediate, tested, original);
            }
            catch (Throwable t)
            {
                closeAll(immediate, tested, original);
                throw t;
            }
        }

        private static void closeAll(Cursor<?>... cursors)
        {
            for (Cursor<?> cursor : cursors)
                if (cursor != null)
                    cursor.close();
        }

        @Override
        public void close()
        {
            closeAll(immediate, tested, source);
        }

        /// Checks that the cursors start in the same state.
        void start()
        {
            verify();
        }

        long advance()
        {
            record('a', 0);
            immediatePosition = immediatePath.arrive(immediate.advance());
            testedPosition = testedPath.arrive(tested.advance());
            if (source != null)
                sourcePosition = sourcePath.arrive(source.advance());
            return verify();
        }

        long skipTo(long target)
        {
            record('s', target);
            immediatePosition = immediatePath.arrive(immediate.skipTo(target));
            testedPosition = testedPath.arrive(tested.skipTo(target));
            if (source != null)
                sourcePosition = sourcePath.arrive(source.skipTo(target));
            return verify();
        }

        /// Calls [Cursor#advanceMultiple] on all cursors. They are not required to take the same number of steps,
        /// only not to step over any content; the ones that stopped earlier are advanced further until all meet.
        /// The tries do differ in this: the page-aware writer merges runs of transitions that the immediate one
        /// writes as separate chain nodes, and the in-memory trie has its own limits on the runs it can step over.
        long advanceMultiple()
        {
            record('m', 0);
            long previous = position;
            immediatePosition = immediatePath.arrive(immediate.advanceMultiple(immediatePath));
            testedPosition = testedPath.arrive(tested.advanceMultiple(testedPath));
            if (source != null)
                sourcePosition = sourcePath.arrive(source.advanceMultiple(sourcePath));
            if (!Cursor.ascended(immediatePosition, previous))
            {
                int depth;
                do
                {
                    depth = Math.max(Cursor.depth(immediatePosition), Cursor.depth(testedPosition));
                    if (source != null)
                        depth = Math.max(depth, Cursor.depth(sourcePosition));
                    immediatePosition = catchUp("immediate trie", immediate, immediatePath, immediatePosition, previous, depth);
                    testedPosition = catchUp("trie under test", tested, testedPath, testedPosition, previous, depth);
                    if (source != null)
                        sourcePosition = catchUp("source trie", source, sourcePath, sourcePosition, previous, depth);
                }
                while (Cursor.depth(immediatePosition) > depth || Cursor.depth(testedPosition) > depth || source != null && Cursor.depth(sourcePosition) > depth);
            }
            return verify();
        }

        private long catchUp(String name, Cursor<T> cursor, Path path, long cursorPosition, long previous, int depth)
        {
            if (Cursor.ascended(cursorPosition, previous))
                return cursorPosition;      // this cursor did not descend like the others, which verify() will report

            while (Cursor.depth(cursorPosition) < depth)
            {
                if (cursor.content() != null)
                    throw failure("advanceMultiple stopped the cursor over the " + name + " at " + Cursor.toString(cursorPosition)
                                  + " with content " + abbreviate(cursor.content()) + ", which another cursor stepped over");
                long next = cursor.advanceMultiple(path);
                if (Cursor.ascended(next, cursorPosition))
                    throw failure("advanceMultiple stopped the cursor over the " + name + " at " + Cursor.toString(cursorPosition)
                                  + " and then ascended to " + Cursor.toString(next) + ", while another cursor went deeper than the former");
                cursorPosition = path.arrive(next);
            }
            return cursorPosition;
        }

        /// Makes the same skips as [Cursor#descendAlong] does for the given key. Returns true if all cursors could
        /// follow all of the key.
        boolean descendAlong(byte[] key)
        {
            for (byte next : key)
            {
                if (Cursor.isOnReturnPath(position))
                    return false;
                long target = Cursor.positionForDescentWithByte(position, next & 0xFF);
                if (Cursor.compare(skipTo(target), target) != 0)
                    return false;
            }
            return true;
        }

        /// A position that the cursors can be asked to skip to from where they are, or [#NO_TARGET] if there is none.
        long randomSkipTarget(Random rand, byte[] guide)
        {
            int depth = Cursor.depth(position);
            boolean onReturnPath = Cursor.isOnReturnPath(position);
            int choice = rand.nextInt(10);
            if (!onReturnPath && (choice < 5 || depth == 0))
            {
                // To a child: the one the guide leads to, if we are given one and are not past its end, or any.
                int transition = guide != null && depth < guide.length && rand.nextInt(4) != 0 ? guide[depth] & 0xFF
                                                                                              : rand.nextInt(256);
                return Cursor.positionForDescentWithByte(position, transition);
            }
            if (depth == 0)
                return NO_TARGET;   // on the return path of the root, where nothing follows

            // To a later position at the current depth or above it.
            int targetDepth = rand.nextBoolean() ? depth : 1 + rand.nextInt(depth);
            int visited = immediatePath.bytes[targetDepth - 1] & 0xFF;
            long visitedPosition = Cursor.encode(targetDepth, visited, direction);
            if (choice < 7)
            {
                // a later transition, if there is any
                int later = direction.isForward() ? 255 - visited : visited;
                if (later > 0)
                    return Cursor.encode(targetDepth, visited + direction.increase * (1 + rand.nextInt(later)), direction);
            }
            else if (choice < 9 && !(onReturnPath && targetDepth == depth))
            {
                // the return path of the position we are at or below, where its content after the branch is
                return visitedPosition | Cursor.ON_RETURN_PATH_BIT;
            }
            // past the branch, which also works where there are no later transitions
            return Cursor.positionForSkippingBranch(visitedPosition);
        }

        /// Returns the tails of the cursors at the current position.
        Walk<T> tail(Direction tailDirection)
        {
            record('t', tailDirection.ordinal());
            Cursor<T> immediateTail = null;
            Cursor<T> testedTail = null;
            Cursor<T> sourceTail = null;
            try
            {
                immediateTail = tailCursor(immediate, tailDirection);
                testedTail = tailCursor(tested, tailDirection);
                sourceTail = source != null && true ? source.tailCursor(tailDirection) : null;
                return new Walk<>(this, immediateTail, testedTail, sourceTail);
            }
            catch (Throwable t)
            {
                closeAll(immediateTail, testedTail, sourceTail);
                throw t;
            }
        }

        private static <T> Cursor<T> tailCursor(Cursor<T> cursor, Direction tailDirection)
        {
            return cursor.tailCursor(tailDirection);
        }

        private void record(char call, long argument)
        {
            calls[callCount % HISTORY] = call;
            arguments[callCount % HISTORY] = argument;
            ++callCount;
        }

        private String history()
        {
            StringBuilder builder = new StringBuilder();
            if (tailOf != null)
                builder.append(tailOf.history()).append(" > ");
            builder.append(direction).append(" cursor, ").append(callCount).append(" calls");
            if (callCount > 0)
                builder.append(callCount > HISTORY ? ", the last of them" : ":");
            for (int i = Math.max(0, callCount - HISTORY); i < callCount; ++i)
            {
                long argument = arguments[i % HISTORY];
                switch (calls[i % HISTORY])
                {
                    case 'a': builder.append(" advance"); break;
                    case 'm': builder.append(" advanceMultiple"); break;
                    case 's': builder.append(" skipTo(").append(Cursor.toString(argument).trim()).append(')'); break;
                    default: builder.append(" tailCursor(").append(Direction.values()[(int) argument]).append(')'); break;
                }
            }
            return builder.toString();
        }

        private AssertionError failure(String message)
        {
            return new AssertionError(message + " [" + history() + "]");
        }

        /// Checks that all cursors are in the same state, and returns the position they are at.
        private long verify()
        {
            if (immediatePosition != testedPosition)
                throw failure("Cursor over the trie under test is at " + Cursor.toString(testedPosition) + " after " + testedPath
                              + ", the one over the immediate trie at " + Cursor.toString(immediatePosition) + " after " + immediatePath);
            if (!testedPath.sameAs(immediatePath))
                throw failure("Cursor over the trie under test took the path " + testedPath + ", the one over the immediate trie " + immediatePath);
            position = immediatePosition;

            boolean exhausted = Cursor.isExhausted(position);
            if (immediate.encodedPosition() != position)
                throw failure("Cursor over the immediate trie returned the position " + Cursor.toString(position) + " and reports " + Cursor.toString(immediate.encodedPosition()));
            if (tested.encodedPosition() != position)
                throw failure("Cursor over the trie under test returned the position " + Cursor.toString(position) + " and reports " + Cursor.toString(tested.encodedPosition()));

            T content = exhausted ? null : immediate.content();
            if (!exhausted && !identical(content, tested.content()))
                throw failure("Content at " + Cursor.toString(position) + " after " + immediatePath + " is " + abbreviate(tested.content())
                              + " in the trie under test, " + abbreviate(content) + " in the immediate trie");

            Object state = null;
            Object precedingState = null;
            if (immediate instanceof RangeCursor)
            {
                state = ((RangeCursor<?>) immediate).state();
                Object testedState = ((RangeCursor<?>) tested).state();
                if (!identical(state, testedState))
                    throw failure("State at " + Cursor.toString(position) + " after " + immediatePath + " is " + abbreviate(testedState)
                                  + " in the trie under test, " + abbreviate(state) + " in the immediate trie");
                precedingState = ((RangeCursor<?>) immediate).precedingState();
                Object testedPrecedingState = ((RangeCursor<?>) tested).precedingState();
                if (!identical(precedingState, testedPrecedingState))
                    throw failure("Preceding state at " + Cursor.toString(position) + " after " + immediatePath + " is " + abbreviate(testedPrecedingState)
                                  + " in the trie under test, " + abbreviate(precedingState) + " in the immediate trie");
            }

            if (source == null)
                return position;

            // The on-disk reader as such, against the trie in memory.
            if (sourcePosition != position)
                throw failure("Cursor over the immediate trie is at " + Cursor.toString(position) + " after " + immediatePath
                              + ", the one over the source trie at " + Cursor.toString(sourcePosition) + " after " + sourcePath);
            if (!sourcePath.sameAs(immediatePath))
                throw failure("Cursor over the immediate trie took the path " + immediatePath + ", the one over the source trie " + sourcePath);
            if (!exhausted && !Objects.equals(source.content(), content))
                throw failure("Content at " + Cursor.toString(position) + " after " + immediatePath + " is " + abbreviate(content)
                              + " in the immediate trie, " + abbreviate(source.content()) + " in the source trie");
            if (source instanceof RangeCursor)
            {
                Object sourceState = ((RangeCursor<?>) source).state();
                if (!Objects.equals(sourceState, state))
                    throw failure("State at " + Cursor.toString(position) + " after " + immediatePath + " is " + abbreviate(state)
                                  + " in the immediate trie, " + abbreviate(sourceState) + " in the source trie");
                Object sourcePrecedingState = ((RangeCursor<?>) source).precedingState();
                if (!Objects.equals(sourcePrecedingState, precedingState))
                    throw failure("Preceding state at " + Cursor.toString(position) + " after " + immediatePath + " is " + abbreviate(precedingState)
                                  + " in the immediate trie, " + abbreviate(sourcePrecedingState) + " in the source trie");
            }
            return position;
        }
    }

    // File-backed tries

    @Test
    public void testFileBackedUnordered() throws IOException
    {
        testFileBacked(false);
    }

    @Test
    public void testFileBackedOrdered() throws IOException
    {
        testFileBacked(true);
    }

    /// The same comparison through real files: the trie is written by the page-aware writer to the 4096-byte pages
    /// of a [org.apache.cassandra.io.util.SequentialWriter] and read through the chunks of a file, where every
    /// cursor holds a buffer that it must give back.
    private void testFileBacked(boolean ordered) throws IOException
    {
        Random rand = new Random(FILE_SEED);
        Keys keys = new Keys();
        // Everything the shapes have, together, under different first bytes.
        int[] firsts = alphabet(rand, 4 * Shape.values().length);
        int next = 0;
        for (int round = 0; round < 4; ++round)
            for (Shape shape : Shape.values())
            {
                byte[] prefix = { (byte) firsts[next++] };
                for (Map.Entry<byte[], Integer> entry : shape.keys(rand, 4096).payloadSizes.entrySet())
                    keys.add(concat(prefix, entry.getKey()), entry.getValue());
            }
        Case<String> testCase = plainCase(String.format("seed %d file-backed %s", FILE_SEED, ordered ? "ordered" : "unordered"), keys, ordered, rand);

        File immediateFile = FileUtils.createTempFile("immediate", ".trie");
        File pageAwareFile = FileUtils.createTempFile("pageaware", ".trie");
        try
        {
            OnDiskTrieWriter.write(testCase.source, ordered, STRING_SERDE, immediateFile);
            OnDiskTrieWriter.write(testCase.source, ordered, STRING_SERDE, pageAwareFile, OnDiskNodeWriter.Type.PAGE_AWARE);
            byte[] immediateBytes = Files.readAllBytes(immediateFile.toPath());
            assertTrue(testCase.description + ": the trie is expected to take a few hundred kilobytes, took " + immediateBytes.length,
                       immediateBytes.length > 200_000 && pageAwareFile.length() > 200_000);

            long buffersInUse = BufferPools.forNetworking().usedSizeInBytes();
            try (FileTries.Plain<String> readImmediate = FileTries.open(immediateFile, STRING_SERDE, ordered);
                 FileTries.Plain<String> readPageAware = FileTries.open(pageAwareFile, STRING_SERDE, ordered))
            {
                // The leak check below relies on an open cursor showing in the pool's accounting.
                try (Cursor<String> cursor = readPageAware.cursor(Direction.FORWARD))
                {
                    assertTrue("A cursor over a file is expected to hold a buffer", BufferPools.forNetworking().usedSizeInBytes() > buffersInUse);
                }

                TrieUtil.assertTriesEqual(testCase.source, readPageAware);
                TrieUtil.assertTriesEqual(readImmediate, readPageAware);

                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    OnDiskTrieWriter.write(readPageAware, ordered, STRING_SERDE, out);
                    assertBytesEqual("immediate(read(PAGE_AWARE file))", immediateBytes, out.toByteArray());
                }

                compareTries(testCase.source, readImmediate, readPageAware, testCase.keys, rand);
            }
            assertEquals("Buffers still in use after all cursors and tries were closed", buffersInUse, BufferPools.forNetworking().usedSizeInBytes());
        }
        catch (Throwable t)
        {
            throw new AssertionError(testCase.description + ": " + t, t);
        }
        finally
        {
            immediateFile.tryDelete();
            pageAwareFile.tryDelete();
        }
    }

    // What the probes found

    /// A reader bug that the probes above ran into, which has nothing to do with how the nodes are laid out.
    ///
    /// A cursor can be asked to skip to a return-path position, which is where an ordered trie walked in reverse
    /// presents the content of a key that has children, and a range trie the markers placed after a branch. The
    /// in-memory cursors stop there. [OnDiskCursor#skipTo] does not: asked from above the position, the parent node
    /// steps over the whole child as being before the target; asked from inside the branch, the cursor drops the
    /// backtrack entry that holds the return-path content together with the ones for the children below it.
    ///
    /// Through the public interface this shows as an entry missing from a reverse walk of a slice.
    @Test
    public void testSkipToReturnPathPosition() throws Exception
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLivedOrdered(VERSION);
        trie.putRecursive(comparable(new byte[]{ 'a' }), "a", (existing, update) -> update);
        trie.putRecursive(comparable(new byte[]{ 'a', 'b' }), "ab", (existing, update) -> update);

        for (TrieUtil.OnDiskWriterKind kind : kinds())
        {
            OnDiskTrie<String> read = TrieUtil.onDiskRoundtrip(trie, true, STRING_SERDE, STRING_SERDE, kind);
            {
                // In reverse the content of "a" is presented when the walk returns to it from "ab".
                long target = Cursor.positionForDescentWithByte(Cursor.rootPosition(Direction.REVERSE), 'a') | Cursor.ON_RETURN_PATH_BIT;
                String[] from = { "the root", "a", "ab" };
                for (int steps = 0; steps < from.length; ++steps)
                {
                    try (Cursor<String> expected = trie.cursor(Direction.REVERSE);
                         Cursor<String> actual = read.cursor(Direction.REVERSE))
                    {
                        for (int i = 0; i < steps; ++i)
                            assertEquals(Cursor.toString(expected.advance()), Cursor.toString(actual.advance()));

                        String what = kind + ": skipTo(" + Cursor.toString(target).trim() + ") from " + from[steps];
                        long expectedPosition = expected.skipTo(target);
                        long actualPosition = actual.skipTo(target);
                        assertEquals(what, Cursor.toString(expectedPosition), Cursor.toString(actualPosition));
                        assertEquals(what, expected.content(), actual.content());
                    }
                }

                // The slice up to "a" exclusive of the branch below it still has "a" itself, in either direction.
                TrieSet set = TrieSet.range(VERSION, comparable(new byte[]{ '0' }), false, comparable(new byte[]{ 'a' }), false);
                for (Direction direction : Direction.values())
                    assertEquals(kind + ": slice walked " + direction, entries(trie.intersect(set), direction), entries(read.intersect(set), direction));
            }
        }
    }

    /// Another one: a cursor that has returned the exhausted position must also report it afterwards.
    /// [OnDiskCursor#advance] and [OnDiskCursor#advanceMultiple] return it without recording it, and so does
    /// [OnDiskCursor#skipTo] unless it runs out of backtracking entries before it reaches the depth it was given.
    /// [Cursor#skipToWhenAhead], which the merges call on the cursors they are not positioned on, starts by asking
    /// the cursor where it is.
    @Test
    public void testPositionAfterExhaustion() throws Exception
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLived(VERSION);
        trie.putRecursive(comparable(new byte[]{ 'a' }), "a", (existing, update) -> update);
        trie.putRecursive(comparable(new byte[]{ 'b' }), "b", (existing, update) -> update);

        OnDiskTrie<String> read = TrieUtil.onDiskRoundtrip(trie, false, STRING_SERDE, STRING_SERDE, TrieUtil.IMMEDIATE);
        {
            for (Direction direction : Direction.values())
            {
                // makeCursor, because the verification that cursor() adds fails an assertion of its own on this
                try (Cursor<String> cursor = read.makeCursor(direction))
                {
                    cursor.advance();
                    cursor.advance();
                    long exhausted = cursor.advance();
                    assertTrue(Cursor.isExhausted(exhausted));
                    assertEquals("Position reported after advance returned " + Cursor.toString(exhausted).trim(),
                                 Cursor.toString(exhausted), Cursor.toString(cursor.encodedPosition()));
                }
                try (Cursor<String> cursor = read.makeCursor(direction))
                {
                    cursor.advance();
                    // past the first child of the root in this direction, and all of the alphabet
                    long exhausted = cursor.skipTo(Cursor.positionForDescentWithByte(Cursor.rootPosition(direction), direction.select(0xFF, 0x00)));
                    assertTrue(Cursor.isExhausted(exhausted));
                    assertEquals("Position reported after skipTo returned " + Cursor.toString(exhausted).trim(),
                                 Cursor.toString(exhausted), Cursor.toString(cursor.encodedPosition()));
                }
            }
        }
    }

    /// And one in what the readers give for a trie that has nothing in it: [Cursor.Empty#tailCursor] compares its
    /// position with the root position of the direction it is asked for instead of its own, and fails its assertion
    /// when the two are not the same. This is what the reverse walk of the [BaseTrie#tailTrie] of an empty on-disk
    /// trie does.
    @Test
    public void testTailOfEmptyTrie()
    {
        OnDiskTrie<String> read = TrieUtil.onDiskRoundtrip(InMemoryTrie.<String>shortLived(VERSION), false, STRING_SERDE, STRING_SERDE, TrieUtil.IMMEDIATE);
        {
            for (Direction direction : Direction.values())
                for (Direction tailDirection : Direction.values())
                    try (Cursor<String> cursor = read.makeCursor(direction);
                         Cursor<String> tail = cursor.tailCursor(tailDirection))
                    {
                        assertEquals(Cursor.toString(Cursor.rootPosition(tailDirection)), Cursor.toString(tail.encodedPosition()));
                        assertTrue(Cursor.isExhausted(tail.advance()));
                    }

            assertEquals(List.of(), entries(read.tailTrie(ByteComparable.EMPTY), Direction.REVERSE));
        }
    }

    /// One in the tails of on-disk range tries. A tail starts with the ranges that are active where it was taken given
    /// as content of its root; a marker at that position that only closes a range leaves nothing to give. The cursor
    /// of the tail ([OnDiskCursor.RangeBranch]) is made by starting a cursor at the node, which takes the marker as
    /// its state, and then replacing the content with what the root is to present. When that is nothing, the state
    /// stays: the tail reports the closing marker as its state, without it being its content, and the range that
    /// was closed as the one that precedes it, until it comes to its first marker.
    @Test
    public void testRangeTailWhereRangeCloses() throws Exception
    {
        ByteComparable b = comparable(new byte[]{ 'b' });
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        trie.putRecursive(comparable(new byte[]{ 'a' }), new TestRangeState(ByteComparable.EMPTY, false, -1, 1), false, (existing, update) -> update);
        trie.putRecursive(b, new TestRangeState(ByteComparable.EMPTY, false, 1, -1), false, (existing, update) -> update);
        trie.putRecursive(comparable(new byte[]{ 'b', 'c' }), new TestRangeState(ByteComparable.EMPTY, false, -1, 2), false, (existing, update) -> update);
        trie.putRecursive(comparable(new byte[]{ 'b', 'd' }), new TestRangeState(ByteComparable.EMPTY, false, 2, -1), false, (existing, update) -> update);

        OnDiskRangeTrie<TestRangeState> read = TrieUtil.onDiskRoundtrip(trie, RANGE_SERDE, RANGE_SERDE, TrieUtil.IMMEDIATE);
        {
            // makeCursor, because the verification that cursor() adds refuses to make the tail
            for (RangeTrie<TestRangeState> tested : List.<RangeTrie<TestRangeState>>of(trie, read))
                try (RangeCursor<TestRangeState> cursor = tested.makeCursor(Direction.FORWARD))
                {
                    cursor.advance();
                    cursor.advance();
                    assertEquals(new TestRangeState(ByteComparable.EMPTY, false, 1, -1), cursor.content());
                    try (RangeCursor<TestRangeState> tail = cursor.tailCursor(Direction.FORWARD))
                    {
                        String what = tested.getClass().getSimpleName() + ": tail at b, ";
                        assertEquals(what + "content of the root", null, tail.content());
                        assertEquals(what + "state at the root", null, tail.state());
                        tail.advance();
                        assertEquals(what + "state at bc", new TestRangeState(ByteComparable.EMPTY, false, -1, 2), tail.state());
                    }
                }

            // The same with the verification, which is what a user of the trie gets when it is enabled.
            TrieUtil.assertTriesEqual(trie.tailTrie(b), read.tailTrie(b));
        }
    }

    /// The last one is not in the on-disk tries at all, but in the trie they are compared with. The cursor of an
    /// [InMemoryRangeTrie] that has skipped works out the range it is in from the nearest marker ahead of it, which
    /// it looks for in the before-branch slot of each prefix node on the way down. A node that only has a marker
    /// after its branch keeps it in that same slot, with a flag that the walk obeys and this search does not. The
    /// marker that closes a range after the branch of "a" is thus found before the one that opens it at "ab".
    @Test
    public void testInMemoryRangeStateAfterSkip() throws Exception
    {
        ByteComparable a = comparable(new byte[]{ 'a' });
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        trie.putRecursive(comparable(new byte[]{ 'a', 'b' }), new TestRangeState(ByteComparable.EMPTY, false, -1, 1), false, (existing, update) -> update);
        trie.putRecursive(a, new TestRangeState(ByteComparable.EMPTY, true, 1, -1), true, (existing, update) -> update);

        // What a walk gives is right.
        try (RangeCursor<TestRangeState> cursor = trie.cursor(Direction.FORWARD))
        {
            cursor.advance();
            assertEquals("State at a when walked to", null, cursor.state());
        }
        // And this is what the trie gives on disk.
        OnDiskRangeTrie<TestRangeState> read = TrieUtil.onDiskRoundtrip(trie, RANGE_SERDE, RANGE_SERDE, TrieUtil.IMMEDIATE);
        {
            assertEquals("Range applicable to a on disk", null, read.applicableRange(a));
        }

        try (RangeCursor<TestRangeState> cursor = trie.cursor(Direction.FORWARD))
        {
            cursor.skipTo(Cursor.positionForDescentWithByte(cursor.encodedPosition(), 'a'));
            assertEquals("State at a when skipped to", null, cursor.state());
        }
        assertEquals("Range applicable to a", null, trie.applicableRange(a));
    }
}
