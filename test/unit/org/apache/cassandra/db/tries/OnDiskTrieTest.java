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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class OnDiskTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        DatabaseDescriptor.toolInitialization();
    }

    @Parameterized.Parameter(0)
    public boolean isOrdered = true;

    @Parameterized.Parameters(name = "ordered {0}")
    public static List<Object[]> data()
    {
        List<Object[]> list = new ArrayList<>();
        for (Boolean ordered : List.of(Boolean.FALSE, Boolean.TRUE))
            list.add(new Object[]{ ordered });

        return list;
    }

    Random rand = new Random(1);

    @Test
    public void testSimple() throws TrieSpaceExhaustedException
    {
        testEntries("abcdef", "abfghijklmn");
    }

    @Test
    public void testSimpleA() throws TrieSpaceExhaustedException
    {
        testEntries("tractor", "tree", "trie");
    }

    @Test
    public void testEmpty() throws TrieSpaceExhaustedException
    {
        testEntries();
    }

    @Test
    public void testRootOnly() throws TrieSpaceExhaustedException
    {
        testEntries("");
    }

    @Test
    public void testRootReturnOnly() throws TrieSpaceExhaustedException
    {
        Assume.assumeTrue(isOrdered);
        testEntries("^");
    }

    @Test
    public void testWithRootContent() throws TrieSpaceExhaustedException
    {
        testEntries("", "abc", "abd");
    }

    @Test
    public void testChains() throws TrieSpaceExhaustedException
    {
        List<String> entries = new ArrayList<>();
        entries.add("abcdef");

        for (int i = 0; i <= 9; ++i)
        {
            String key = "abc" + i;
            int chainLen = i*10 + 1;
            for (int j = 0; j <= 5; ++j)
            {
                entries.add(key);
                key += randomString(rand, chainLen);
            }
        }

        testEntries(entries.toArray(String[]::new));
    }

    @Test
    public void testPrefixes() throws TrieSpaceExhaustedException
    {
        // InMemoryTrie does not support alternate content (i.e. content can be on ascent or descent path but not both)
        // TODO: check full prefix functionality with range trie.
        Assume.assumeTrue(isOrdered);
        testEntries("abcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", // descend-side only (not leaf because of long content
                    "abbbb^", // ascend-side only
//                    "abaaa", "abaaa^", // ascend- and descend-side, no child
                    "abdddd", "abddddeeeeee",   // descend-side prefix
                    "abeeee^", "abeeeeffffff", // ascend-side prefix
//                    "abfff", "abfff^", "abfffhijklmn" // ascend- and descend-side with substructure
                    "zzz"
        );
    }

    @Test
    public void testDense() throws TrieSpaceExhaustedException
    {
        testNodesWithChildren(240, 245, 250, 253, 255, 256);
    }

    @Test
    public void testBitmap() throws TrieSpaceExhaustedException
    {
        testNodesWithChildren(26);//, 50, 75, 100, 125, 150, 200, 220, 235);
    }

    @Test
    public void testSparse() throws TrieSpaceExhaustedException
    {
        testNodesWithChildren(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 21, 22, 23, 24, 25);
    }

    private void testNodesWithChildren(int... childCounts) throws TrieSpaceExhaustedException
    {
        for (String postfix : Arrays.asList("ef", ""))
        {
            List<String> tests = new ArrayList<>();
            for (int count : childCounts) // 10*25 switches to Dense
            {
                String prefix = "ab00" + (count == 256 ? "0100" : byteToHex(count)) + "00cd";
                addChildren(count, tests, prefix, postfix);
            }
            testEntriesHex(tests.toArray(String[]::new));
        }
    }

    private void addChildren(int count, List<String> tests, String prefix, String postfix)
    {
        BitSet children = new BitSet(256);
        for (int j = 0; j < count; ++j)
            children.set(rand.nextInt(256));
        while (children.cardinality() < count)
            children.set(rand.nextInt(256));

        for (int j = 0; j < 256; ++j)
        {
            if (children.get(j))
                tests.add(prefix + byteToHex(j) + postfix);
        }
    }

    private String byteToHex(int i)
    {
        return String.format("%02x", i);
    }

    private static String randomString(Random rand, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            sb.append((char) ('a' + rand.nextInt('z' - 'a' + 1)));
        return sb.toString();
    }

    @Test
    public void testSkipToPositionForSkippingBranchesOnMax() throws TrieSpaceExhaustedException
    {
        testEntriesHex(
            // chain parent ending in 00
            "aaaaaa00", "aaaaaa0000", "aaaaaa00ab",
            // chain parent ending in FF
            "bbbbbbff", "bbbbbbffff", "bbbbbbffab",
            // sparse parent
            "cc00", "cc80", "ccff", "cc00ab", "cc80ab", "ccffab",
            // split parent
            "dd00", "dd80", "ddf0", "dd00ab", "dd80ab", "ddf0ab",
            "dd04", "dd84", "ddf4", "dd04ab", "dd84ab", "ddf4ab",
            "dd08", "dd88", "ddf8", "dd08ab", "dd88ab", "ddf8ab",
            "dd0f", "dd8f", "ddff", "dd0fab", "dd8fab", "ddffab"
        );
    }


    /// Slices with bounds before, at, between and below the keys of a trie whose nodes carry content above other
    /// content. Walked in reverse an ordered trie presents that content on the return path, and a slice ending at it
    /// skips to the return-path position; the skip must still stop there.
    @Test
    public void testSlicesMatchInMemory() throws TrieSpaceExhaustedException
    {
        String[] keys = { "a", "ab", "abc", "abd", "b", "bb", "bbc" };
        InMemoryTrie<String> inMemTrie = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        for (String key : keys)
            inMemTrie.putRecursive(TrieUtil.directComparable(key), key, (x, y) -> y);

        try (OnDiskTrie<String> trie = TrieUtil.onDiskRoundtripStrings(inMemTrie, isOrdered))
        {
            String[] bounds = { null, "", "a", "aa", "ab", "abb", "abc", "abcd", "abd", "b", "ba", "bb", "bbc", "c" };
            for (String left : bounds)
                for (String right : bounds)
                {
                    for (boolean leftInclusive : new boolean[]{ true, false })
                        for (boolean rightInclusive : new boolean[]{ true, false })
                        {
                            if (!boundsInOrder(left, !leftInclusive, right, rightInclusive))
                                continue;
                            TrieSet set = TrieSet.range(VERSION,
                                                        left == null ? null : TrieUtil.directComparable(left),
                                                        leftInclusive,
                                                        right == null ? null : TrieUtil.directComparable(right),
                                                        rightInclusive);
                            for (Direction direction : Direction.values())
                                assertEquals("Slice " + (leftInclusive ? "[" : "(") + left + ", " + right + (rightInclusive ? "]" : ")") + " " + direction,
                                             valuesOf(inMemTrie.intersect(set), direction),
                                             valuesOf(trie.intersect(set), direction));
                        }
                }

            if (isOrdered)
                assertEquals(Arrays.asList("ab", "a"),
                             valuesOf(trie.intersect(TrieSet.range(VERSION, null, true, TrieUtil.directComparable("ab"), false)),
                                      Direction.REVERSE));
        }
    }

    /// Whether a left bound comes before a right one. A bound sits either before its key or, when it excludes the key
    /// on the left or includes it on the right, after the key's whole branch.
    private static boolean boundsInOrder(String left, boolean leftAfterBranch, String right, boolean rightAfterBranch)
    {
        if (left == null || right == null)
            return true;
        if (left.equals(right))
            return !leftAfterBranch || rightAfterBranch;
        if (right.startsWith(left))
            return !leftAfterBranch;
        if (left.startsWith(right))
            return rightAfterBranch;
        return left.compareTo(right) < 0;
    }

    private static List<String> valuesOf(Trie<String> trie, Direction direction)
    {
        List<String> values = new ArrayList<>();
        trie.values(direction).forEach(values::add);
        return values;
    }

    /// Random sequences of `advance` and `skipTo` calls on the on-disk cursor and the in-memory one it was written
    /// from must stop at the same positions. The skip targets are every position the walk can reach, the positions
    /// past each branch, and the return-path position of every node, including the ones that present nothing there.
    @Test
    public void testRandomSkipsMatchInMemory() throws TrieSpaceExhaustedException
    {
        Random rand = new Random(1);
        for (int iteration = 0; iteration < 200; ++iteration)
        {
            InMemoryTrie<String> inMemTrie = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
            int count = 1 + rand.nextInt(8);
            for (int i = 0; i < count; ++i)
            {
                String key = randomKey(rand, 1 + rand.nextInt(4));
                inMemTrie.putRecursive(TrieUtil.directComparable(key), key, (x, y) -> y);
            }

            try (OnDiskTrie<String> trie = TrieUtil.onDiskRoundtripStrings(inMemTrie, isOrdered))
            {
                for (Direction direction : Direction.values())
                {
                    List<Long> targets = new ArrayList<>();
                    Cursor<String> walk = inMemTrie.cursor(direction);
                    for (long p = walk.encodedPosition(); !Cursor.isExhausted(p); p = walk.advance())
                    {
                        targets.add(p & ~Cursor.FLAGS_MASK);
                        targets.add(Cursor.positionForSkippingBranch(p));
                        if (!Cursor.isOnReturnPath(p))
                            targets.add((p & ~Cursor.FLAGS_MASK) | Cursor.ON_RETURN_PATH_BIT);
                    }

                    for (int repeat = 0; repeat < 10; ++repeat)
                        assertRandomSkipsMatch(rand, targets, inMemTrie.cursor(direction), trie.cursor(direction));
                }
            }
        }
    }

    private static void assertRandomSkipsMatch(Random rand, List<Long> targets, Cursor<String> expected, Cursor<String> actual)
    {
        Direction direction = expected.direction();
        StringBuilder steps = new StringBuilder();
        int[] path = new int[16];
        long position = expected.encodedPosition();
        while (!Cursor.isExhausted(position))
        {
            int depth = Cursor.depth(position);
            if (depth > 0)
                path[depth - 1] = Cursor.incomingTransition(position);

            List<Long> candidates = new ArrayList<>();
            for (long target : targets)
            {
                // A valid skip target is after the current position, at most one level deeper, and, at a depth the
                // walk has already passed, after the byte it took there or on that byte's return path.
                int targetDepth = Cursor.depth(target);
                if (Cursor.compare(target, position) <= 0 || targetDepth > depth + 1)
                    continue;
                if (targetDepth > 0 && targetDepth <= depth)
                {
                    int visited = path[targetDepth - 1];
                    int transition = Cursor.incomingTransition(target);
                    if (!direction.gt(transition, visited) && !(transition == visited && Cursor.isOnReturnPath(target)))
                        continue;
                }
                candidates.add(target);
            }

            long actualPosition;
            if (candidates.isEmpty() || rand.nextInt(3) == 0)
            {
                steps.append(" advance");
                position = expected.advance();
                actualPosition = actual.advance();
            }
            else
            {
                long target = candidates.get(rand.nextInt(candidates.size()));
                steps.append(" skipTo(").append(Cursor.toString(target)).append(')');
                position = expected.skipTo(target);
                actualPosition = actual.skipTo(target);
            }
            assertEquals("Position after" + steps, Cursor.toString(position), Cursor.toString(actualPosition));
            if (!Cursor.isExhausted(position))
                assertEquals("Content after" + steps, expected.content(), actual.content());
        }
        actual.close();
    }

    /// Corrupt bytes, as a damaged commit-log record or message payload would present them, must be reported as
    /// corruption: a walk over them must neither hang nor fail with any other exception. Each trie gets one to three
    /// random bytes overwritten and is walked to the end with its content read; the walk is bounded, as a reader that
    /// follows a pointer back into a node it has already left would never finish.
    @Test
    public void testCorruptTriesFailAsCorruption() throws IOException, TrieSpaceExhaustedException
    {
        final int maxSteps = 1_000_000;
        Random rand = new Random(7);
        int rejected = 0;
        for (int iteration = 0; iteration < 500; ++iteration)
        {
            InMemoryTrie<String> source = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
            int count = 1 + rand.nextInt(30);
            for (int i = 0; i < count; ++i)
            {
                StringBuilder key = new StringBuilder();
                int length = 1 + rand.nextInt(6);
                for (int j = 0; j < length; ++j)
                    key.append((char) (rand.nextInt(4) == 0 ? rand.nextInt(256) : 'a' + rand.nextInt(4)));
                source.putRecursive(TrieUtil.directComparable(key.toString()), key.toString(), (x, y) -> y);
            }

            byte[] bytes;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                OnDiskTrieWriter.write(source, isOrdered, TrieUtil.STRING_SERDE, out);
                bytes = out.toByteArray();
            }
            int flips = 1 + rand.nextInt(3);
            for (int i = 0; i < flips; ++i)
                bytes[rand.nextInt(bytes.length)] = (byte) rand.nextInt(256);

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            OnDiskTrie<String> trie = OnDiskTrie.open(buffer, TrieUtil.STRING_SERDE, VERSION, isOrdered, buffer.limit());
            Direction direction = rand.nextBoolean() ? Direction.FORWARD : Direction.REVERSE;
            // Not verified: corrupt bytes can list transitions out of order, which the reader does not check.
            try (Cursor<String> cursor = trie.makeCursor(direction))
            {
                long position = cursor.encodedPosition();
                int steps = 0;
                while (!Cursor.isExhausted(position))
                {
                    Cursor.content(cursor, position);
                    position = cursor.advance();
                    if (++steps > maxSteps)
                        fail("Walk over a corrupt trie did not finish in " + maxSteps + " steps: " + ByteBufferUtil.bytesToHex(buffer));
                }
            }
            catch (UncheckedIOException e)
            {
                assertTrue(e.getMessage(), e.getCause() instanceof IOException);
                ++rejected;
            }
        }
        // Most single-byte changes are detected; make sure the test is not passing because none are.
        assertTrue("Only " + rejected + " corrupt tries rejected", rejected > 100);
    }

    /// Every way of reading a trie that does not hand a cursor to its caller must give back the buffers of the cursors
    /// it takes, once the walk has run to its end. Over a file each cursor holds a buffer until it is closed.
    @Test
    public void testReadsReleaseTheirCursors() throws IOException, TrieSpaceExhaustedException
    {
        InMemoryTrie<String> source = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        for (String key : new String[]{ "a", "ab", "abc", "abd", "b", "bb", "bbc", "c" })
            source.putRecursive(TrieUtil.directComparable(key), key, (x, y) -> y);
        File file = OnDiskTrieWriter.write(source, isOrdered, TrieUtil.STRING_SERDE, new File(java.io.File.createTempFile("release", ".trie")));

        TrieUtil.CountingRebuffererFactory factory =
            new TrieUtil.CountingRebuffererFactory(OnDiskBaseTrie.openChunkReader(new ChannelProxy(file)));
        try (OnDiskTrie<String> trie = new OnDiskTrie<>(factory, TrieUtil.STRING_SERDE, VERSION, isOrdered, factory.fileLength(), factory.channel()))
        {
            ByteComparable.Preencoded a = TrieUtil.directComparable("a");
            ByteComparable.Preencoded b = TrieUtil.directComparable("b");
            Map<String, Runnable> reads = new LinkedHashMap<>();
            for (Direction direction : Direction.values())
            {
                reads.put("values " + direction, () -> trie.values(direction).forEach(v -> {}));
                reads.put("entrySet " + direction, () -> trie.entrySet(direction).forEach(e -> {}));
                reads.put("filteredValues " + direction, () -> trie.filteredValues(direction, String.class).forEach(v -> {}));
                reads.put("filteredEntrySet " + direction, () -> trie.filteredEntrySet(direction, String.class).forEach(e -> {}));
                reads.put("forEachValue " + direction, () -> trie.forEachValue(direction, v -> {}));
                reads.put("forEachEntry " + direction, () -> trie.forEachEntry(direction, (k, v) -> {}));
                reads.put("forEachEntrySkippingBranches " + direction, () -> trie.forEachEntrySkippingBranches(direction, (k, v) -> {}));
                reads.put("tailTries " + direction, () -> trie.tailTries(direction, v -> v.length() == 2)
                                                              .forEach(e -> e.getValue().values(direction).forEach(v -> {})));
                reads.put("subtrie values " + direction, () -> trie.subtrie(a, b).values(direction).forEach(v -> {}));
                reads.put("tailTrie values " + direction, () -> trie.tailTrie(a).values(direction).forEach(v -> {}));
                reads.put("mergeWith values " + direction, () -> trie.mergeWith(source, (x, y) -> x).values(direction).forEach(v -> {}));
            }
            reads.put("valueIterator", () -> trie.valueIterator().forEachRemaining(v -> {}));
            reads.put("entryIterator", () -> trie.entryIterator().forEachRemaining(e -> {}));
            reads.put("get", () -> trie.get(a));
            reads.put("dump", trie::dump);
            reads.put("process", () -> trie.process(Direction.FORWARD, new TrieDumper.Plain<>(Object::toString)));
            reads.put("write", () -> {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    OnDiskTrieWriter.write(trie, isOrdered, TrieUtil.STRING_SERDE, out);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });

            List<String> leaking = new ArrayList<>();
            for (Map.Entry<String, Runnable> read : reads.entrySet())
            {
                int before = factory.outstanding;
                read.getValue().run();
                if (factory.outstanding != before)
                    leaking.add(read.getKey() + " (" + (factory.outstanding - before) + ")");
            }
            assertEquals("Reads leaving rebufferers outstanding", new ArrayList<>(), leaking);
        }
    }

    /// A key over a three-letter alphabet, so that random keys share prefixes and land on each other's paths.
    private static String randomKey(Random rand, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            sb.append((char) ('a' + rand.nextInt(3)));
        return sb.toString();
    }

    private void testEntries(String... tests) throws TrieSpaceExhaustedException
    {
        for (Function<String, ByteComparable.Preencoded> mapping :
            ImmutableList.<Function<String, ByteComparable.Preencoded>>of(TrieUtil::directComparable, TrieUtil::comparable))
        {
            testEntries(tests, mapping);
        }
    }

    private void testEntriesHex(String... tests) throws TrieSpaceExhaustedException
    {
        testEntries(tests, s -> ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s)));
        // Run the other translations just in case.
        testEntries(tests);
    }

    private void testEntries(String[] tests, Function<String, ByteComparable.Preencoded> mapping) throws TrieSpaceExhaustedException

    {
        InMemoryTrie<String> inMemTrie = isOrdered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
        for (String test : tests)
        {
            String testKey = test;
            boolean afterBranch = false;
            if (test.endsWith("~")) // ascend-side content
            {
                testKey = test.substring(0, test.length() - 1);
                afterBranch = true;
            }
            ByteComparable.Preencoded e = mapping.apply(testKey);
            System.out.println("Adding " + asString(e) + ": " + test);
            inMemTrie.putRecursive(e, test, afterBranch, (x, y) -> y);
        }

        try (OnDiskTrie<String> trie = TrieUtil.onDiskRoundtripStrings(inMemTrie, isOrdered))
        {
            TrieUtil.assertTriesEqual(inMemTrie, trie);

            if (isOrdered)
                testGetOrdered(tests, mapping, trie);
            else
                testGetUnordered(tests, mapping, trie);

            testSkipOverBranch(inMemTrie, trie);

            testIntersections(inMemTrie, trie, tests, mapping);
        }
    }

    private void testIntersections(InMemoryTrie<String> inMemTrie, OnDiskTrie<String> trie, String[] tests, Function<String, ByteComparable.Preencoded> mapping)
    {
        if (tests.length == 0)
            return;

        tests = Arrays.copyOf(tests, tests.length);
        Arrays.sort(tests, (x, y) -> mapping.apply(x).compareTo(mapping.apply(y)));
        for (int i = 0; i < 20; ++i)
        {
            int leftIndex = rand.nextInt(tests.length + 1) - 1;
            ByteComparable.Preencoded leftKey = leftIndex < 0 ? null : mapping.apply(tests[leftIndex]);
            int leftOffset = Math.max(leftIndex, 0);
            int limit = tests.length - leftOffset + 1;
            for (int j = 0; j < 5; ++j)
            {
                int rightIndex = leftOffset + rand.nextInt(limit);
                ByteComparable.Preencoded rightKey = rightIndex < tests.length ? mapping.apply(tests[rightIndex]) : null;

                TrieUtil.assertTriesEqual(inMemTrie.subtrie(leftKey, rightKey), trie.subtrie(leftKey, rightKey));
            }
        }
    }

    private static void testGetOrdered(String[] tests, Function<String, ByteComparable.Preencoded> mapping, OnDiskTrie<String> trie)
    {
        for (String test : tests)
        {
            if (!test.endsWith("~"))
                assertEquals(test, trie.get(mapping.apply(test)));
            else
                assertEquals(test, getReversed(trie, mapping.apply(test.substring(0, test.length() - 1))));
        }
    }

    private static void testGetUnordered(String[] tests, Function<String, ByteComparable.Preencoded> mapping, OnDiskTrie<String> trie)
    {
        for (String test : tests)
        {
            ByteComparable.Preencoded key = mapping.apply(test);
            assertEquals(test, trie.get(key));
            assertEquals(test, getReversed(trie, key));
        }
    }

    static void testSkipOverBranch(Trie<String> expected, Trie<String> actual)
    {
        expected.forEachEntry((key, str) ->
                              {
                                  for (Direction d : Direction.values())
                                  {
                                      String context = "Key " + key.byteComparableAsString(VERSION) + "(" + str + ") " + d;
                                      Cursor<String> ec = expected.cursor(d);
                                      Cursor<String> ac = actual.cursor(d);
                                      assertEquals("descendAlong result for " + context,
                                                   ec.descendAlong(key.getPreencodedBytes()),
                                                   ac.descendAlong(key.getPreencodedBytes()));
                                      assertEquals("position after descendAlong expected " + Cursor.toString(ec.encodedPosition()) + " was " + Cursor.toString(ac.encodedPosition()) + " for " + context,
                                                   ec.encodedPosition(),
                                                   ac.encodedPosition());

                                      long skipBranch = Cursor.positionForSkippingBranch(ec.encodedPosition());
                                      long eSkipTo = ec.skipTo(skipBranch);
                                      long aSkipTo = ac.skipTo(skipBranch);
                                      assertEquals("skipTo result expected " + Cursor.toString(eSkipTo) + " was " + Cursor.toString(aSkipTo) + " for " + context,
                                                   eSkipTo,
                                                   aSkipTo);
                                      assertEquals("position after skipTo expected " + Cursor.toString(ec.encodedPosition()) + " was " + Cursor.toString(ac.encodedPosition()) + " for " + context,
                                                   ec.encodedPosition(),
                                                   ac.encodedPosition());
                                  }
                              });
    }

    static <T> T getReversed(Trie<T> trie, ByteComparable key)
    {
        Cursor<T> c = trie.cursor(Direction.REVERSE);
        if (!c.descendAlong(key.asComparableBytes(c.byteComparableVersion())))
            return null;
        else
            return c.content();
    }

}
