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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;
import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.VERSION;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.addNthToInMemoryTrie;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.addToInMemoryTrie;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.assertIterablesEqual;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.assertMapEquals;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.checkGet;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.generateKey;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.generateKeys;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PrefixTailTrieTest
{
    private static final int COUNT_TAIL = 5000;
    private static final int COUNT_HEAD = 40;
    public static final Comparator<ByteComparable> BYTE_COMPARABLE_COMPARATOR = (a, b) -> ByteComparable.compare(a, b, VERSION);
    Random rand = new Random();

    static
    {
        // Use prefix-free keys to avoid putting partitions within partitions
        InMemoryTrieTestBase.prefixFree = true;
    }

    static final InMemoryTrie.UpsertTransformer<Object, Object> THROWING_UPSERT = (e, u) -> {
        if (e != null) throw new AssertionError();
        return u;
    };

    static final Function<Object, String> CONTENT_TO_STRING = x -> x instanceof ByteBuffer
                                                                   ? ByteBufferUtil.bytesToHex((ByteBuffer) x)
                                                                   : x.toString();

    static class Tail
    {
        byte[] prefix;
        NavigableMap<ByteComparable, ByteBuffer> data;

        public Tail(byte[] prefix, NavigableMap<ByteComparable, ByteBuffer> map)
        {
            this.prefix = prefix;
            this.data = map;
        }

        public String toString()
        {
            return "Tail{" + ByteBufferUtil.bytesToHex(ByteBuffer.wrap(prefix)) + '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tail tail = (Tail) o;
            return Arrays.equals(prefix, tail.prefix) && Objects.equals(data, tail.data);
        }
    }

    static <T> T getRootContent(Trie<T> trie)
    {
        return trie.get(ByteComparable.EMPTY);
    }

    @Test
    public void testPrefixTail() throws Exception
    {
        testPrefixTail(1, false);
    }

    @Test
    public void testPrefixTailMerge2InHead() throws Exception
    {
        testPrefixTail(2, false);
    }

    @Test
    public void testPrefixTailMerge2InTail() throws Exception
    {
        testPrefixTail(2, true);
    }

    @Test
    public void testPrefixTailMerge5InHead() throws Exception
    {
        testPrefixTail(5, false);
    }

    @Test
    public void testPrefixTailMerge5InTail() throws Exception
    {
        testPrefixTail(5, true);
    }

    static Tail combineTails(Object x, Object y)
    {
        // Cast failure is a test problem
        Tail tx = (Tail) x;
        Tail ty = (Tail) y;
        var map = new TreeMap<ByteComparable, ByteBuffer>(BYTE_COMPARABLE_COMPARATOR);
        map.putAll(tx.data);
        map.putAll(ty.data);
        return new Tail(tx.prefix, map);
    }

    public void testPrefixTail(int splits, boolean splitInTail) throws Exception
    {
        ByteComparable[] prefixes = generateKeys(rand, COUNT_HEAD);

        NavigableMap<ByteComparable, Tail> data = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
        final Trie<Object> trie = splitInTail ? prepareSplitInTailTrie(splits, prefixes, data)
                                              : prepareSplitInHeadTrie(splits, prefixes, data);
//        System.out.println(trie.dump(CONTENT_TO_STRING));

        // Test tailTrie for known prefix
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            Tail t = data.get(prefixes[i]);
            Trie<Object> tail = trie.tailTrie(prefixes[i]);
            assertEquals(t, getRootContent(tail));
            checkContent(tail, t.data);
        }

        // Test tail iteration for metadata
        for (Direction td : Direction.values())
        {
            long count = 0;
            for (var en : trie.tailTries(td, Tail.class))
            {
                System.out.println(en.getKey().byteComparableAsString(VERSION));
                Trie<Object> tail = en.getValue();
                Tail t = data.get(en.getKey());
                assertNotNull(t);
                assertEquals(t, getRootContent(tail));
                checkContent(tail, t.data);
                ++count;
            }
            assertEquals(COUNT_HEAD, count);
        }

        // test a sample of tail slices
        for (int i = rand.nextInt(7); i < COUNT_HEAD; i += 1 + rand.nextInt(7))
        {
            Tail t = data.get(prefixes[i]);
            int keyCount = t.data.keySet().size();
            int firstIndex = rand.nextInt(keyCount - 1);
            int lastIndex = firstIndex + rand.nextInt(keyCount - firstIndex);
            ByteComparable first = rand.nextInt(5) > 0 ? Iterables.get(t.data.keySet(), firstIndex) : null;
            ByteComparable last = rand.nextInt(5) > 0 ? Iterables.get(t.data.keySet(), lastIndex) : null;
            ByteComparable prefix = prefixes[i];
            final ByteComparable leftWithPrefix = concat(prefix, first, rand.nextBoolean() ? prefix
                                                                                           : rand.nextBoolean()
                                                                                             ? data.lowerKey(prefix)
                                                                                             : null);
            final ByteComparable rightWithPrefix = concat(prefix, last, rand.nextBoolean() ? data.higherKey(prefix)
                                                                                           : null);
            Trie<Object> tail = trie.subtrie(leftWithPrefix,
                                             rightWithPrefix)
                                    .tailTrie(prefixes[i]);
            System.out.println("Between " + (leftWithPrefix == null ? "null" : leftWithPrefix.byteComparableAsString(VERSION)) + " and " + (rightWithPrefix == null ? "null" : rightWithPrefix.byteComparableAsString(VERSION)));
            assertEquals(first == null ? t : null, getRootContent(tail));   // this behavior will change soon to report all prefixes
            checkContent(tail, subMap(t.data, first, last));
        }
    }

    private static void checkContent(Trie<Object> tail, NavigableMap<ByteComparable, ByteBuffer> data)
    {

        assertMapEquals(tail.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                        data.entrySet().iterator());
        assertIterablesEqual(tail.filteredValues(Direction.FORWARD, ByteBuffer.class),
                             data.values());
        // As the keys are prefix-free, reverse iteration is the inverse of forward.
        assertMapEquals(tail.filteredEntryIterator(Direction.REVERSE, ByteBuffer.class),
                        data.descendingMap().entrySet().iterator());
        assertIterablesEqual(tail.filteredValues(Direction.REVERSE, ByteBuffer.class),
                             data.descendingMap().values());
        checkGet(tail, data);
    }

    private static <K, V> NavigableMap<K, V> subMap(NavigableMap<K, V> data, K left, K right)
    {
        if (left == null)
            return right == null ? data : data.headMap(right, false);
        else
            return right == null
                   ? data.tailMap(left, true)
                   : data.subMap(left, true, right, false);
    }

    private static ByteComparable concat(ByteComparable a, ByteComparable b, ByteComparable ifBNull)
    {
        if (b == null)
            return ifBNull;
        return ByteComparable.fixedLength(Bytes.concat(a.asByteComparableArray(VERSION),
                                                       b.asByteComparableArray(VERSION)));
    }

    private Trie<Object> prepareSplitInTailTrie(int splits, ByteComparable[] prefixes, Map<ByteComparable, Tail> data) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<Object>[] tries = new InMemoryTrie[splits];
        for (int i = 0; i < splits; ++i)
            tries[i] = InMemoryTrie.shortLived();
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            ByteComparable[] src = generateKeys(rand, COUNT_TAIL);
            NavigableMap<ByteComparable, ByteBuffer> allContent = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
            for (int k = 0; k < splits; ++k)
            {
                NavigableMap<ByteComparable, ByteBuffer> content = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
                InMemoryTrie<Object> tail = InMemoryTrie.shortLived();
                addNthToInMemoryTrie(src, content, tail, true, splits, k);

                Tail t = new Tail(prefixes[i].asByteComparableArray(VERSION), content);
                allContent.putAll(content);
                tail.putRecursive(ByteComparable.EMPTY, t, THROWING_UPSERT);
//            System.out.println(tail.dump(CONTENT_TO_STRING));
                tries[k].apply(tail.prefix(prefixes[i]), THROWING_UPSERT, Predicates.alwaysFalse());
            }
            Tail t = new Tail(prefixes[i].asByteComparableArray(VERSION), allContent);
            data.put(ByteComparable.fixedLength(t.prefix), t);
        }

        return Trie.merge(Arrays.asList(tries), c -> c.stream().reduce(PrefixTailTrieTest::combineTails).get());
    }


    private Trie<Object> prepareSplitInHeadTrie(int splits, ByteComparable[] prefixes, Map<ByteComparable, Tail> data) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<Object>[] tries = new InMemoryTrie[splits];
        for (int i = 0; i < splits; ++i)
            tries[i] = InMemoryTrie.shortLived();
        int trieIndex = 0;
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            ByteComparable[] src = generateKeys(rand, COUNT_TAIL);

            NavigableMap<ByteComparable, ByteBuffer> content = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
            InMemoryTrie<Object> tail = InMemoryTrie.shortLived();
            addToInMemoryTrie(src, content, tail, true);

            Tail t = new Tail(prefixes[i].asByteComparableArray(VERSION), content);
            tail.putRecursive(ByteComparable.EMPTY, t, THROWING_UPSERT);
//            System.out.println(tail.dump(CONTENT_TO_STRING));
            tries[trieIndex].apply(tail.prefix(prefixes[i]), THROWING_UPSERT, Predicates.alwaysFalse());

            data.put(ByteComparable.fixedLength(t.prefix), t);
            trieIndex = (trieIndex + 1) % splits;
        }

        return Trie.mergeDistinct(Arrays.asList(tries));
    }

    // also do same prefix updates

    @Test
    public void testTailMerge() throws Exception
    {
        ByteComparable prefix = generateKey(rand);
        InMemoryTrie<Object> trie = InMemoryTrie.shortLived();
        NavigableMap<ByteComparable, ByteBuffer> content = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);

        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            ByteComparable[] src = generateKeys(rand, COUNT_TAIL);
            InMemoryTrie<Object> tail = InMemoryTrie.shortLived();
            addToInMemoryTrie(src, content, tail, true);
//                        System.out.println(tail.dump(CONTENT_TO_STRING));
            tail.putRecursive(ByteComparable.EMPTY, 1, THROWING_UPSERT);
            trie.apply(tail.prefix(prefix),
                       (x, y) -> x instanceof Integer ? (Integer) x + (Integer) y : y,
                       Predicates.alwaysFalse());
        }

//                System.out.println(trie.dump(CONTENT_TO_STRING));

        Trie<Object> tail = trie.tailTrie(prefix);
        assertEquals(COUNT_HEAD, ((Integer) getRootContent(tail)).intValue());
        assertMapEquals(tail.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                        content.entrySet().iterator());
        assertIterablesEqual(tail.filteredValues(Direction.FORWARD, ByteBuffer.class),
                             content.values());


        // Test tail iteration for metadata
        long count = 0;
        for (var en : trie.tailTries(Direction.FORWARD, Integer.class))
        {
            System.out.println(en.getKey().byteComparableAsString(VERSION));
            Trie<Object> tt = en.getValue();
            assertNotNull(tt);
            assertEquals(COUNT_HEAD, ((Integer) getRootContent(tail)).intValue());
            assertMapEquals(tt.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                            content.entrySet().iterator());
            assertIterablesEqual(tt.filteredValues(Direction.FORWARD, ByteBuffer.class),
                                 content.values());
            ++count;
        }
        assertEquals(1, count);
    }
}
