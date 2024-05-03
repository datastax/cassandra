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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
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
    public final static ByteComparable.Version VERSION = OSS41;

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
        testForDifferentByteComparableEncodings(this::doTestExactMatch);
    }

    private void doTestExactMatch(Function<String, ByteComparable> asByteComparable) throws Exception
    {
        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            writer.add(asByteComparable.apply("ab"), 0);
            writer.add(asByteComparable.apply("abb"), 1);
            writer.add(asByteComparable.apply("abc"), 2);
            writer.add(asByteComparable.apply("abcd"), 3);
            writer.add(asByteComparable.apply("abd"), 4);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp, VERSION))
        {
            assertEquals(NOT_FOUND, reader.exactMatch(asByteComparable.apply("a")));
            assertEquals(0, reader.exactMatch(asByteComparable.apply("ab")));
            assertEquals(2, reader.exactMatch(asByteComparable.apply("abc")));
            assertEquals(NOT_FOUND, reader.exactMatch(asByteComparable.apply("abca")));
            assertEquals(1, reader.exactMatch(asByteComparable.apply("abb")));
            assertEquals(NOT_FOUND, reader.exactMatch(asByteComparable.apply("abba")));
        }
    }

    @Test
    public void testCeilingWithoutTrackingState() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestCeiling);
    }

    private void doTestCeiling(Function<String, ByteComparable> asByteComparable) throws Exception
    {
        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            writer.add(asByteComparable.apply("ab"), 0);
            writer.add(asByteComparable.apply("abb"), 1);
            writer.add(asByteComparable.apply("abc"), 2);
            writer.add(asByteComparable.apply("abcd"), 3);
            writer.add(asByteComparable.apply("abd"), 4);
            writer.add(asByteComparable.apply("cbb"), 5);
            writer.add(asByteComparable.apply("cbbbb"), 6);
            fp = writer.complete(new MutableLong());
        }

        IndexComponent.ForRead termsData = components.get(IndexComponentType.TERMS_DATA);
        ByteComparable key13 = asByteComparable.apply("A");
        readAndAssertCeiling(fp, 0, key13, termsData);
        ByteComparable key12 = asByteComparable.apply("a");
        readAndAssertCeiling(fp, 0, key12, termsData);
        ByteComparable key11 = asByteComparable.apply("z");
        readAndAssertCeiling(fp, NOT_FOUND, key11, termsData);
        ByteComparable key10 = asByteComparable.apply("ab");
        readAndAssertCeiling(fp, 0, key10, termsData);
        ByteComparable key9 = asByteComparable.apply("abbb");
        readAndAssertCeiling(fp, 2, key9, termsData);
        ByteComparable key8 = asByteComparable.apply("abc");
        readAndAssertCeiling(fp, 2, key8, termsData);
        ByteComparable key7 = asByteComparable.apply("abca");
        readAndAssertCeiling(fp, 3, key7, termsData);
        ByteComparable key6 = asByteComparable.apply("abb");
        readAndAssertCeiling(fp, 1, key6, termsData);
        ByteComparable key5 = asByteComparable.apply("abba");
        readAndAssertCeiling(fp, 2, key5, termsData);
        ByteComparable key4 = asByteComparable.apply("cb");
        readAndAssertCeiling(fp, 5, key4, termsData);
        ByteComparable key3 = asByteComparable.apply("c");
        readAndAssertCeiling(fp, 5, key3, termsData);
        ByteComparable key2 = asByteComparable.apply("cbb");
        readAndAssertCeiling(fp, 5, key2, termsData);
        ByteComparable key1 = asByteComparable.apply("cbbb");
        readAndAssertCeiling(fp, 6, key1, termsData);
        ByteComparable key = asByteComparable.apply("cbbbbb");
        readAndAssertCeiling(fp, NOT_FOUND, key, termsData);
    }

    @Test
    public void testCeilingTrackingState() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestCeilingStateful);
    }

    private void doTestCeilingStateful(Function<String, ByteComparable> asByteComparable) throws Exception
    {
        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            writer.add(asByteComparable.apply("ab"), 0);
            writer.add(asByteComparable.apply("abb"), 1);
            writer.add(asByteComparable.apply("abc"), 2);
            writer.add(asByteComparable.apply("abcd"), 3);
            writer.add(asByteComparable.apply("abd"), 4);
            writer.add(asByteComparable.apply("cbb"), 5);
            writer.add(asByteComparable.apply("cbbbb"), 6);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp, VERSION))
        {
            assertEquals(0, reader.ceiling(asByteComparable.apply("a")));
            assertEquals(2, reader.ceiling(asByteComparable.apply("abc")));
            assertEquals(3, reader.ceiling(asByteComparable.apply("abcc")));

            // The current behavior is to advance past the node that the ceiling returns.
            // As such, even though abccc is before abcd, the ceiling will return 4 for abd.
            assertEquals(4, reader.ceiling(asByteComparable.apply("abccc")));
        }
    }

    @Test
    public void testCeilingWihtoutTrackingStateWithEmulatedPrimaryKey() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestCeilingWithEmulatedPrimaryKey);
    }

    private void doTestCeilingWithEmulatedPrimaryKey(Function<String, ByteComparable> asByteComparable) throws Exception
    {
        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            writer.add(primaryKey(asByteComparable, "ab", "cd", "def"), 0);
            writer.add(primaryKey(asByteComparable, "ab", "cde", "def"), 1);
            writer.add(primaryKey(asByteComparable, "ab", "ce", "def"), 2);
            writer.add(primaryKey(asByteComparable, "ab", "ce", "defg"), 3);
            writer.add(primaryKey(asByteComparable, "ab", "cf", "def"), 4);
            fp = writer.complete(new MutableLong());
        }

        IndexComponent.ForRead termsData = components.get(IndexComponentType.TERMS_DATA);

        // Validate token only searches
        ByteComparable key17 = primaryKey(asByteComparable, "a", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key17, termsData);
        ByteComparable key16 = primaryKey(asByteComparable, "ab", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key16, termsData);
        ByteComparable key15 = primaryKey(asByteComparable, "aa", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key15, termsData);
        ByteComparable key14 = primaryKey(asByteComparable, "abc", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, NOT_FOUND, key14, termsData);
        ByteComparable key13 = primaryKey(asByteComparable, "ba", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, NOT_FOUND, key13, termsData);

        // Validate token and partition key only searches
        ByteComparable key12 = primaryKey(asByteComparable, "a", "b", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key12, termsData);
        ByteComparable key11 = primaryKey(asByteComparable, "ab", "b", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key11, termsData);
        ByteComparable key10 = primaryKey(asByteComparable, "ab", "ce", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 2, key10, termsData);
        ByteComparable key9 = primaryKey(asByteComparable, "ab", "cee", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 4, key9, termsData);
        ByteComparable key8 = primaryKey(asByteComparable, "ab", "d", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, NOT_FOUND, key8, termsData);
        ByteComparable key7 = primaryKey(asByteComparable, "abb", "a", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, NOT_FOUND, key7, termsData);
        ByteComparable key6 = primaryKey(asByteComparable, "aa", "d", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key6, termsData);
        ByteComparable key5 = primaryKey(asByteComparable, "abc", "a", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, NOT_FOUND, key5, termsData);
        ByteComparable key4 = primaryKey(asByteComparable, "ba", "a", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, NOT_FOUND, key4, termsData);


        // Validate token, partition key, and clustring column searches
        ByteComparable key3 = primaryKey(asByteComparable, "a", "b", "c", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 0, key3, termsData);
        ByteComparable key2 = primaryKey(asByteComparable, "ab", "cdd", "a", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 1, key2, termsData);
        ByteComparable key1 = primaryKey(asByteComparable, "ab", "cde", "a", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 1, key1, termsData);
        ByteComparable key = primaryKey(asByteComparable, "ab", "cde", "z", ByteSource.LT_NEXT_COMPONENT);
        readAndAssertCeiling(fp, 2, key, termsData);
    }

    // Tests using this method are verifying the correctness of individual calls to ceiling. Because the reader is
    // stateful across calls to ceiling, a new one must be opened for each call.
    private void readAndAssertCeiling(long root, long expected, ByteComparable key, IndexComponent.ForRead termsData)
    {
        try (FileHandle input = termsData.createFileHandle();
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), root, VERSION))
        {
            assertEquals(expected, reader.ceiling(key));
        }
    }

    @Test
    public void testFloor() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestFloor);
    }

    private void doTestFloor(Function<String, ByteComparable> asByteComparable) throws Exception
    {
        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            writer.add(asByteComparable.apply("ab"), 0);
            writer.add(asByteComparable.apply("abb"), 1);
            writer.add(asByteComparable.apply("abc"), 2);
            writer.add(asByteComparable.apply("abcd"), 3);
            writer.add(asByteComparable.apply("abd"), 4);
            writer.add(asByteComparable.apply("ca"), 5);
            writer.add(asByteComparable.apply("caaaaa"), 6);
            writer.add(asByteComparable.apply("cab"), 7);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp, VERSION))
        {
            assertEquals(NOT_FOUND, reader.floor(asByteComparable.apply("a")));
            assertEquals(7, reader.floor(asByteComparable.apply("z")));
            assertEquals(0, reader.floor(asByteComparable.apply("ab")));
            assertEquals(2, reader.floor(asByteComparable.apply("abc")));
            assertEquals(2, reader.floor(asByteComparable.apply("abca")));
            assertEquals(1, reader.floor(asByteComparable.apply("abb")));
            assertEquals(1, reader.floor(asByteComparable.apply("abba")));
            assertEquals(4, reader.floor(asByteComparable.apply("abda")));
            assertEquals(4, reader.floor(asByteComparable.apply("c")));
            assertEquals(5, reader.floor(asByteComparable.apply("caaaa")));
            assertEquals(7, reader.floor(asByteComparable.apply("cac")));
        }
    }



    @Test
    public void testFloorWithEmulatedPrimaryKey() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestFloorWithEmulatedPrimaryKey);
    }

    private void doTestFloorWithEmulatedPrimaryKey(Function<String, ByteComparable> asByteComparable) throws Exception
    {
        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            writer.add(primaryKey(asByteComparable, "ab", "cd", "def"), 0);
            writer.add(primaryKey(asByteComparable, "ab", "cde", "def"), 1);
            writer.add(primaryKey(asByteComparable, "ab", "ce", "def"), 2);
            writer.add(primaryKey(asByteComparable, "ab", "ce", "defg"), 3);
            writer.add(primaryKey(asByteComparable, "ab", "cf", "def"), 4);
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp, VERSION))
        {
            // Validate token only searches
            assertEquals(NOT_FOUND, reader.floor(primaryKey(asByteComparable, "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "ab", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.floor(primaryKey(asByteComparable, "aa", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "abc", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "ba", ByteSource.GT_NEXT_COMPONENT)));

            // Validate token and partition key only searches
            assertEquals(NOT_FOUND, reader.floor(primaryKey(asByteComparable, "a", "b", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.floor(primaryKey(asByteComparable, "ab", "b", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(3, reader.floor(primaryKey(asByteComparable, "ab", "ce", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(3, reader.floor(primaryKey(asByteComparable, "ab", "cee", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "ab", "d", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "abb", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(NOT_FOUND, reader.floor(primaryKey(asByteComparable, "aa", "d", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "abc", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(4, reader.floor(primaryKey(asByteComparable, "ba", "a", ByteSource.GT_NEXT_COMPONENT)));


            // Validate token, partition key, and clustring column searches
            assertEquals(NOT_FOUND, reader.floor(primaryKey(asByteComparable, "a", "b", "c", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(0, reader.floor(primaryKey(asByteComparable, "ab", "cdd", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(0, reader.floor(primaryKey(asByteComparable, "ab", "cde", "a", ByteSource.GT_NEXT_COMPONENT)));
            assertEquals(1, reader.floor(primaryKey(asByteComparable, "ab", "cde", "z", ByteSource.GT_NEXT_COMPONENT)));
        }
    }

    @Test
    public void testTermEnum() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestTermEnum);
    }


    private void doTestTermEnum(Function<String, ByteComparable> asByteComparable) throws IOException
    {
        final List<ByteComparable> byteComparables = generateSortedByteComparables(asByteComparable);

        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            for (int i = 0; i < byteComparables.size(); ++i)
            {
                writer.add(byteComparables.get(i), i);
            }
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
             TrieTermsDictionaryReader iterator = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp, VERSION);
             ReverseTrieTermsDictionaryReader reverseIterator = new ReverseTrieTermsDictionaryReader(input.instantiateRebufferer(), fp))
        {
            verifyOrder(iterator, byteComparables, true);
            Collections.reverse(byteComparables);
            verifyOrder(reverseIterator, byteComparables, false);
        }
    }

    private void verifyOrder(Iterator<Pair<ByteComparable, Long>> iterator, List<ByteComparable> byteComparables, boolean ascending)
    {
        var expected = byteComparables.iterator();
        int offset = ascending ? 0 : byteComparables.size() - 1;
        while (iterator.hasNext())
        {
            assertTrue(expected.hasNext()); // verify that hasNext is idempotent
            final Pair<ByteComparable, Long> actual = iterator.next();
            assertEquals(0, compare(expected.next(), actual.left, OSS41));
            assertEquals(offset, actual.right.longValue());
            offset += ascending ? 1 : -1;
        }
        assertFalse(expected.hasNext());
    }

    @Test
    public void testTermEnumWithEmulatedPrimaryKey() throws Exception
    {
        testForDifferentByteComparableEncodings(this::doTestMinMaxTerm);
    }

    private void doTestMinMaxTerm(Function<String, ByteComparable> asByteComparable) throws IOException
    {
        final List<ByteComparable> byteComparables = generateSortedByteComparables(asByteComparable);

        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(components))
        {
            for (int i = 0; i < byteComparables.size(); ++i)
            {
                writer.add(byteComparables.get(i), i);
            }
            fp = writer.complete(new MutableLong());
        }

        try (FileHandle input = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
             TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(input.instantiateRebufferer(), fp, VERSION))
        {
            final ByteComparable expectedMaxTerm = byteComparables.get(byteComparables.size() - 1);
            final ByteComparable actualMaxTerm = reader.getMaxTerm();
            assertEquals(0, compare(expectedMaxTerm, actualMaxTerm, OSS41));

            final ByteComparable expectedMinTerm = byteComparables.get(0);
            final ByteComparable actualMinTerm = reader.getMinTerm();
            assertEquals(0, compare(expectedMinTerm, actualMinTerm, OSS41));
        }
    }

    private List<ByteComparable> generateSortedByteComparables(Function<String, ByteComparable> asByteComparable)
    {
        final int numKeys = randomIntBetween(16, 512);
        final List<String> randomStrings = Stream.generate(() -> randomSimpleString(4, 48))
                                                 .limit(numKeys)
                                                 .sorted()
                                                 .collect(Collectors.toList());

        // Get rid of any duplicates otherwise the tests will fail.
        return randomStrings.stream()
                            .filter(string -> Collections.frequency(randomStrings, string) == 1)
                            .map(asByteComparable)
                            .collect(Collectors.toList());
    }

    /**
     * Used to generate ByteComparable objects that are used as keys in the TrieTermsDictionary.
     * @param token
     * @param partitionKey
     * @param clustringColumn
     * @return
     */
    private ByteComparable primaryKey(Function<String, ByteComparable> asByteComparable,
                                      String token, String partitionKey, String clustringColumn)
    {
        assert token != null && partitionKey != null && clustringColumn != null;
        return primaryKey(asByteComparable, token, partitionKey, clustringColumn, ByteSource.TERMINATOR);
    }

    private ByteComparable primaryKey(Function<String, ByteComparable> asByteComparable, String token, int terminator)
    {
        assert token != null;
        return primaryKey(asByteComparable, token, null, null, terminator);
    }

    private ByteComparable primaryKey(Function<String, ByteComparable> asByteComparable,
                                      String token, String partitionKey, int terminator)
    {
        assert token != null && partitionKey != null;
        return primaryKey(asByteComparable, token, partitionKey, null, terminator);
    }

    private ByteComparable primaryKey(Function<String, ByteComparable> asByteComparable,
                                      String token, String partitionKey, String clustringColumn, int terminator)
    {
        ByteComparable tokenByteComparable = asByteComparable.apply(token);
        if (partitionKey == null)
            return (v) -> ByteSource.withTerminator(terminator, tokenByteComparable.asComparableBytes(v));
        ByteComparable partitionKeyByteComparable = asByteComparable.apply(partitionKey);
        if (clustringColumn == null)
            return (v) -> ByteSource.withTerminator(terminator,
                                                    tokenByteComparable.asComparableBytes(v),
                                                    partitionKeyByteComparable.asComparableBytes(v));
        ByteComparable clusteringColumnByteComparable = asByteComparable.apply(clustringColumn);
        return (v) -> ByteSource.withTerminator(terminator,
                                                tokenByteComparable.asComparableBytes(v),
                                                partitionKeyByteComparable.asComparableBytes(v),
                                                clusteringColumnByteComparable.asComparableBytes(v));

    }

    /**
     * There are multiple ways of encoding a ByteComparable object. This method tests two of those ways.
     * Fixed length results in a ByteStream without a terminating 0 while ByteComparable.of adds the terminating
     * 0. The primary nuance is whether a ByteComparable object ends up strictly as a prefix or as a lower/greater
     * branch. In both cases, the result for floor and ceiling ought to provide the same results, though the code
     * path will be slightly different.
     */
    private void testForDifferentByteComparableEncodings(ThrowingConsumer<Function<String, ByteComparable>> test) throws Exception
    {
        test.accept(s -> ByteComparable.fixedLength(ByteBufferUtil.bytes(s)));
        test.accept(ByteComparable::of);
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }
}
