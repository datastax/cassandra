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
package org.apache.cassandra.index.sai.disk.v1.trie;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.NOT_FOUND;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Version.OSS41;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.compare;

public class TrieTermsDictionaryTest extends SaiRandomizedTest
{
    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        indexContext = SAITester.createIndexContext(index, UTF8Type.instance);
    }

    @Test
    public void testExactMatch() throws Exception
    {
        doTestExactMatch();
    }

    private void doTestExactMatch() throws Exception
    {
        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            writer.add(asByteComparable("ab"), 0);
            writer.add(asByteComparable("abb"), 1);
            writer.add(asByteComparable("abc"), 2);
            writer.add(asByteComparable("abcd"), 3);
            writer.add(asByteComparable("abd"), 4);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            assertEquals(NOT_FOUND, reader.exactMatch(asByteComparable("a")));
            assertEquals(0, reader.exactMatch(asByteComparable("ab")));
            assertEquals(2, reader.exactMatch(asByteComparable("abc")));
            assertEquals(NOT_FOUND, reader.exactMatch(asByteComparable("abca")));
            assertEquals(1, reader.exactMatch(asByteComparable("abb")));
            assertEquals(NOT_FOUND, reader.exactMatch(asByteComparable("abba")));
        }
    }

    @Test
    public void testCeiling() throws Exception
    {
        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            writer.add(asByteComparable("ab"), 0);
            writer.add(asByteComparable("abb"), 1);
            writer.add(asByteComparable("abc"), 2);
            writer.add(asByteComparable("abcd"), 3);
            writer.add(asByteComparable("abd"), 4);
            writer.add(asByteComparable("cbbb"), 5);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            assertEquals(0, reader.ceiling(asByteComparable("A")));
            assertEquals(0, reader.ceiling(asByteComparable("a")));
            assertEquals(NOT_FOUND, reader.ceiling(asByteComparable("z")));
            assertEquals(0, reader.ceiling(asByteComparable("ab")));
            assertEquals(2, reader.ceiling(asByteComparable("abbb")));
            assertEquals(2, reader.ceiling(asByteComparable("abc")));
            assertEquals(3, reader.ceiling(asByteComparable("abca")));
            assertEquals(1, reader.ceiling(asByteComparable("abb")));
            assertEquals(2, reader.ceiling(asByteComparable("abba")));
            assertEquals(5, reader.ceiling(asByteComparable("cb")));
            assertEquals(5, reader.ceiling(asByteComparable("c")));
            assertEquals(NOT_FOUND, reader.ceiling(asByteComparable("cbbbb")));
        }
    }

    @Test
    public void testCeilingWithEmulatedPrimaryKey() throws Exception
    {
        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            writer.add(primaryKey("ab", "cd", "def"), 0);
            writer.add(primaryKey("ab", "cde", "def"), 1);
            writer.add(primaryKey("ab", "ce", "def"), 2);
            writer.add(primaryKey("ab", "ce", "defg"), 3);
            writer.add(primaryKey("ab", "cf", "def"), 4);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            // Validate token only searches
            assertEquals(0, reader.ceiling(primaryKey("a", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(0, reader.ceiling(primaryKey("ab", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(0, reader.ceiling(primaryKey("aa", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.ceiling(primaryKey("abc", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.ceiling(primaryKey("ba", ByteSource.LT_NEXT_COMPONENT)));

            // Validate token and partition key only searches
            assertEquals(0, reader.ceiling(primaryKey("a", "b", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(0, reader.ceiling(primaryKey("ab", "b", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(2, reader.ceiling(primaryKey("ab", "ce", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(4, reader.ceiling(primaryKey("ab", "cee", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.ceiling(primaryKey("ab", "d", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.ceiling(primaryKey("abb", "a", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(0, reader.ceiling(primaryKey("aa", "d", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.ceiling(primaryKey("abc", "a", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.ceiling(primaryKey("ba", "a", ByteSource.LT_NEXT_COMPONENT)));


            // Validate token, partition key, and clustring column searches
            assertEquals(0, reader.ceiling(primaryKey("a", "b", "c", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(1, reader.ceiling(primaryKey("ab", "cdd", "a", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(1, reader.ceiling(primaryKey("ab", "cde", "a", ByteSource.LT_NEXT_COMPONENT)));
            assertEquals(2, reader.ceiling(primaryKey("ab", "cde", "z", ByteSource.LT_NEXT_COMPONENT)));
        }
    }

    @Test
    public void testFloor() throws Exception
    {
        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            writer.add(asByteComparable("ab"), 0);
            writer.add(asByteComparable("abb"), 1);
            writer.add(asByteComparable("abc"), 2);
            writer.add(asByteComparable("abcd"), 3);
            writer.add(asByteComparable("abd"), 4);
            writer.add(asByteComparable("ca"), 5);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            assertEquals(NOT_FOUND, reader.floor(asByteComparable("a")));
            assertEquals(5, reader.floor(asByteComparable("z")));
            assertEquals(0, reader.floor(asByteComparable("ab")));
            assertEquals(2, reader.floor(asByteComparable("abc")));
            assertEquals(1, reader.floor(asByteComparable("abca")));
            assertEquals(1, reader.floor(asByteComparable("abb")));
            assertEquals(NOT_FOUND, reader.floor(asByteComparable("abba")));
            assertEquals(3, reader.floor(asByteComparable("abda")));
            assertEquals(4, reader.floor(asByteComparable("c")));
        }
    }

    @Test
    public void testFloorWithEmulatedPrimaryKey() throws Exception
    {
        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            writer.add(primaryKey("ab", "cd", "def"), 0);
            writer.add(primaryKey("ab", "cde", "def"), 1);
            writer.add(primaryKey("ab", "ce", "def"), 2);
            writer.add(primaryKey("ab", "ce", "defg"), 3);
            writer.add(primaryKey("ab", "cf", "def"), 4);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            // Validate token only searches
            assertEquals(NOT_FOUND, reader.floor(primaryKey("a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("ab", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.floor(primaryKey("aa", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("abc", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("ba", ByteSource.GT_NEXT_COMPONENT)));

            // Validate token and partition key only searches
            assertEquals(NOT_FOUND, reader.floor(primaryKey("a", "b", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.floor(primaryKey("ab", "b", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(3, reader.floor(primaryKey("ab", "ce", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(3, reader.floor(primaryKey("ab", "cee", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("ab", "d", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("abb", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.floor(primaryKey("aa", "d", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("abc", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey("ba", "a", ByteSource.GT_NEXT_COMPONENT)));


            // Validate token, partition key, and clustring column searches
            assertEquals(NOT_FOUND, reader.floor(primaryKey("a", "b", "c", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(0, reader.floor(primaryKey("ab", "cdd", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(0, reader.floor(primaryKey("ab", "cde", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(1, reader.floor(primaryKey("ab", "cde", "z", ByteSource.GT_NEXT_COMPONENT)));
        }
    }

    @Test
    public void testTermEnum() throws IOException
    {
        final List<ByteComparable> byteComparables = generateSortedByteComparables();

        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            for (int i = 0; i < byteComparables.size(); ++i)
            {
                writer.add(byteComparables.get(i), i);
            }
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            final Iterator<Pair<ByteComparable, Long>> iterator = reader.iterator();
            final Iterator<ByteComparable> expected = byteComparables.iterator();
            int offset = 0;
            while (iterator.hasNext())
            {
                assertTrue(expected.hasNext());
                final Pair<ByteComparable, Long> actual = iterator.next();

                assertEquals(0, compare(expected.next(), actual.left, OSS41));
                assertEquals(offset++, actual.right.longValue());
            }
            assertFalse(expected.hasNext());
        }
    }

    @Test
    public void testMinMaxTerm() throws IOException
    {
        final List<ByteComparable> byteComparables = generateSortedByteComparables();

        long fp;
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            for (int i = 0; i < byteComparables.size(); ++i)
            {
                writer.add(byteComparables.get(i), i);
            }
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            final ByteComparable expectedMaxTerm = byteComparables.get(byteComparables.size() - 1);
            final ByteComparable actualMaxTerm = reader.getMaxTerm();
            assertEquals(0, compare(expectedMaxTerm, actualMaxTerm, OSS41));

            final ByteComparable expectedMinTerm = byteComparables.get(0);
            final ByteComparable actualMinTerm = reader.getMinTerm();
            assertEquals(0, compare(expectedMinTerm, actualMinTerm, OSS41));
        }
    }

    private List<ByteComparable> generateSortedByteComparables()
    {
        final int numKeys = randomIntBetween(16, 512);
        final List<String> randomStrings = Stream.generate(() -> randomSimpleString(4, 48))
                                                 .limit(numKeys)
                                                 .sorted()
                                                 .collect(Collectors.toList());

        // Get rid of any duplicates otherwise the tests will fail.
        return randomStrings.stream()
                            .filter(string -> Collections.frequency(randomStrings, string) == 1)
                            .map(this::asByteComparable)
                            .collect(Collectors.toList());
    }

    /**
     * Used to generate ByteComparable objects that are used as keys in the TrieTermsDictionary.
     * @param token
     * @param partitionKey
     * @param clustringColumn
     * @return
     */
    private ByteComparable primaryKey(String token, String partitionKey, String clustringColumn)
    {
        assert token != null && partitionKey != null && clustringColumn != null;
        return primaryKey(token, partitionKey, clustringColumn, ByteSource.TERMINATOR);
    }

    private ByteComparable primaryKey(String token, int terminator)
    {
        assert token != null;
        return primaryKey(token, null, null, terminator);
    }

    private ByteComparable primaryKey(String token, String partitionKey, int terminator)
    {
        assert token != null && partitionKey != null;
        return primaryKey(token, partitionKey, null, terminator);
    }

    private ByteComparable primaryKey(String token, String partitionKey, String clustringColumn, int terminator)
    {
        ByteComparable tokenByteComparable = asByteComparable(token);
        if (partitionKey == null)
            return (v) -> ByteSource.withTerminator(terminator, tokenByteComparable.asComparableBytes(v));
        ByteComparable partitionKeyByteComparable = asByteComparable(partitionKey);
        if (clustringColumn == null)
            return (v) -> ByteSource.withTerminator(terminator,
                                                    tokenByteComparable.asComparableBytes(v),
                                                    partitionKeyByteComparable.asComparableBytes(v));
        ByteComparable clusteringColumnByteComparable = asByteComparable(clustringColumn);
        return (v) -> ByteSource.withTerminator(terminator,
                                                tokenByteComparable.asComparableBytes(v),
                                                partitionKeyByteComparable.asComparableBytes(v),
                                                clusteringColumnByteComparable.asComparableBytes(v));

    }

    private ByteComparable asByteComparable(String s)
    {
        return ByteComparable.fixedLength(ByteBufferUtil.bytes(s));
    }
}
