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

package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.PatternMatcher;
import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link PatternValueIterator} on on-disk tries of all the writer types, comparing its results in both directions
 * with {@link org.apache.cassandra.db.tries.Trie#patternMatched} on an in-memory trie with the same keys.
 */
public class PatternValueIteratorTest extends AbstractTrieTestBase
{
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());

    /** Characters whose UTF-8 encodings have 1, 2, 3 and 4 bytes, including the glob wildcard characters. */
    private static final String[] ALPHABET = { "a", "b", "*", "?", "é", "€", "😀" };

    private static final String[] WORDS = { "alpha", "beta", "data", "home", "logs", "node", "report", "src", "tmp",
                                            "café", "東京", "日本" };
    private static final String[] EXTENSIONS = { ".txt", ".log", ".json", ".db", "" };

    /** Iterates the keys of the matching nodes. */
    static class KeyIterator extends PatternValueIterator<KeyIterator>
    {
        KeyIterator(Rebufferer source, long root, TriePattern pattern, Direction direction)
        {
            super(source, root, PatternMatcher.create(pattern), direction, true, version);
        }

        List<String> keys()
        {
            List<String> keys = new ArrayList<>();
            while (hasNext())
                keys.add(nextValue(() -> new String(ByteSourceInverse.readBytes(collectedKey().asComparableBytes(byteComparableVersion)), UTF_8)));
            return keys;
        }
    }

    @Test
    public void testRandomPatterns() throws IOException, TrieSpaceExhaustedException
    {
        Random rand = new Random(SEED);
        for (int i = 0; i < 100; ++i)
        {
            List<Object> tokens = randomTokens(rand);
            TriePattern pattern = build(tokens);
            List<String> keys = randomKeys(rand, tokens);
            check("seed " + SEED + " case " + i, keys, pattern);
        }
    }

    @Test
    public void testPathsTrie() throws IOException, TrieSpaceExhaustedException
    {
        // A bigger trie, laid out on several pages, with dense nodes (e.g. for the digits).
        Random rand = new Random(SEED + 1);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 3000; ++i)
            keys.add(randomPath(rand));

        // '?' takes whole UTF-8 characters, e.g. the two of 東京
        for (String glob : new String[]{ "*", "alpha/*", "*.json", "*/data/*.log", "alpha/*/report????.txt",
                                         "*a*e*i*o*u*", "????/data/*", "alpha/????/node/*", "????/????/????/data????.log",
                                         "*/??/*", "*1", "*1?3*", "alpha/beta/report", "nomatch*", "", "caf?/*", "?????/*" })
            check("glob " + glob, keys, TriePattern.glob(glob));
    }

    @Test
    public void testDenseNodesWithMissingTransitions() throws IOException, TrieSpaceExhaustedException
    {
        List<String> keys = Arrays.asList("a0", "a2", "a5", "a5x", "a7", "a9", "b1", "b3", "b30", "b35", "c");
        for (String glob : new String[]{ "a?", "a3", "a5", "a5*", "?3*", "?5?", "b3?", "a*", "*", "*0", "c", "d" })
            check("glob " + glob, keys, TriePattern.glob(glob));
    }

    private void check(String msg, List<String> keyList, TriePattern pattern) throws IOException, TrieSpaceExhaustedException
    {
        // The writers need unique keys in order.
        TreeSet<byte[]> sorted = new TreeSet<>(Arrays::compareUnsigned);
        for (String key : keyList)
            sorted.add(key.getBytes(UTF_8));

        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = newTrieWriter(serializer, buf);
        InMemoryTrie<String> reference = InMemoryTrie.shortLivedOrdered(ByteComparable.Version.OSS50);
        for (byte[] key : sorted)
        {
            builder.add(ByteComparable.preencoded(version, key), 1);
            reference.putRecursive(ByteComparable.preencoded(ByteComparable.Version.OSS50, key), new String(key, UTF_8), (x, y) -> y);
        }
        long root = builder.complete();
        ByteBuffer data = buf.asNewBuffer();

        List<String> expected = Iterables.toList(reference.patternMatched(pattern).values(Direction.FORWARD));
        msg += " pattern " + pattern + " ";
        try (KeyIterator iterator = new KeyIterator(new ByteBufRebufferer(data), root, pattern, Direction.FORWARD))
        {
            assertEquals(msg + "forward", expected, iterator.keys());
        }
        try (KeyIterator iterator = new KeyIterator(new ByteBufRebufferer(data), root, pattern, Direction.REVERSE))
        {
            assertEquals(msg + "reverse", Lists.reverse(expected), iterator.keys());
        }
    }

    /** A random pattern: a list of literal strings, and the builder methods' names for wildcards. */
    private static List<Object> randomTokens(Random rand)
    {
        int length = rand.nextInt(4) == 0 ? 20 + rand.nextInt(20) : rand.nextInt(8);
        List<Object> tokens = new ArrayList<>();
        for (int i = 0; i < length; ++i)
        {
            int r = rand.nextInt(20);
            if (r < 3)
                tokens.add("anyString");
            else if (r < 7)
                tokens.add("anyChar");
            else
                tokens.add(new StringBuilder(ALPHABET[rand.nextInt(ALPHABET.length)])); // a literal
        }
        return tokens;
    }

    private static TriePattern build(List<Object> tokens)
    {
        TriePattern.Builder builder = TriePattern.builder();
        for (Object token : tokens)
        {
            if (token instanceof StringBuilder)
                builder.literal(token.toString());
            else if (token.equals("anyString"))
                builder.anyString();
            else
                builder.anyChar();
        }
        return builder.build();
    }

    /** Keys that are instances of the pattern with a random edit, and some random strings. */
    private static List<String> randomKeys(Random rand, List<Object> tokens)
    {
        List<String> keys = new ArrayList<>();
        int count = 10 + rand.nextInt(30);
        for (int i = 0; i < count; ++i)
        {
            List<String> chars = new ArrayList<>();
            for (Object token : tokens)
            {
                if (token instanceof StringBuilder)
                    chars.add(token.toString());
                else if (token.equals("anyString"))
                    for (int j = rand.nextInt(4); j > 0; --j)
                        chars.add(ALPHABET[rand.nextInt(ALPHABET.length)]);
                else
                    chars.add(ALPHABET[rand.nextInt(ALPHABET.length)]);
            }

            int pos = rand.nextInt(chars.size() + 1);
            String c = ALPHABET[rand.nextInt(ALPHABET.length)];
            switch (rand.nextInt(5))
            {
                case 0:
                    if (pos < chars.size())
                        chars.set(pos, c);
                    break;
                case 1:
                    chars.add(pos, c);
                    break;
                case 2:
                    if (pos < chars.size())
                        chars.remove(pos);
                    break;
                case 3:
                    chars.subList(pos, chars.size()).clear();
                    break;
                default:
                    break;
            }
            keys.add(String.join("", chars));
        }
        for (int i = 0; i < 5; ++i)
        {
            StringBuilder key = new StringBuilder();
            for (int j = rand.nextInt(tokens.size() + 3); j > 0; --j)
                key.append(ALPHABET[rand.nextInt(ALPHABET.length)]);
            keys.add(key.toString());
        }
        return keys;
    }

    private static String randomPath(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        int depth = 1 + rand.nextInt(5);
        for (int i = 0; i < depth; ++i)
            sb.append(WORDS[rand.nextInt(WORDS.length)]).append('/');
        sb.append(WORDS[rand.nextInt(WORDS.length)])
          .append(rand.nextInt(10000))
          .append(EXTENSIONS[rand.nextInt(EXTENSIONS.length)]);
        return sb.toString();
    }
}
