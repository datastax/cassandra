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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.junit.Assert.assertEquals;

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

    // TODO: Test unproductive path (via custom cursor)

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
//            "aaaaaa00", "aaaaaa0000", "aaaaaa00ab",
            // chain parent ending in FF
            "bbbbbbff", "bbbbbbffff", "bbbbbbffab"//,
            // sparse parent
//            "cc00", "cc80", "ccff", "cc00ab", "cc80ab", "ccffab",
//            // split parent
//            "dd00", "dd80", "ddf0", "dd00ab", "dd80ab", "ddf0ab",
//            "dd04", "dd84", "ddf4", "dd04ab", "dd84ab", "ddf4ab",
//            "dd08", "dd88", "ddf8", "dd08ab", "dd88ab", "ddf8ab",
//            "dd0f", "dd8f", "ddff", "dd0fab", "dd8fab", "ddffab"
        );
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
