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

package org.apache.cassandra.index.sai.disk.v2.keystore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyLookupTest extends SaiRandomizedTest
{
    public static final ByteComparable.Version VERSION = TypeUtil.BYTE_COMPARABLE_VERSION;
    protected IndexDescriptor indexDescriptor;

    @Before
    public void setup() throws Exception
    {
        indexDescriptor = newIndexDescriptor();
    }

//    @Test
//    public void testLexicographicException() throws Exception
//    {
//        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
//        try (MetadataWriter metadataWriter = new MetadataWriter(components))
//        {
//            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS),
//                                                                        metadataWriter, true);
//            try (KeyStoreWriter writer = new KeyStoreWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
//                                                                  metadataWriter,
//                                                                  blockFPWriter,
//                                                            4, false))
//            {
//                ByteBuffer buffer = Int32Type.instance.decompose(99999);
//                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
//                byte[] bytes1 = ByteSourceInverse.readBytes(byteSource);
//
//                writer.add(ByteComparable.preencoded(VERSION, bytes1));
//
//                buffer = Int32Type.instance.decompose(444);
//                byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
//                byte[] bytes2 = ByteSourceInverse.readBytes(byteSource);
//
//                assertThrows(IllegalArgumentException.class, () -> writer.add(ByteComparable.preencoded(VERSION, bytes2)));
//            }
//        }
//    }

    @Test
    public void testFileValidation() throws Exception
    {
        List<PrimaryKey> primaryKeys = new ArrayList<>();

        for (int x = 0; x < 11; x++)
        {
            ByteBuffer buffer = UTF8Type.instance.decompose(Integer.toString(x));
            DecoratedKey partitionKey = Murmur3Partitioner.instance.decorateKey(buffer);
            PrimaryKey primaryKey = SAITester.TEST_FACTORY.create(partitionKey, Clustering.EMPTY);
            primaryKeys.add(primaryKey);
        }

        primaryKeys.sort(PrimaryKey::compareTo);
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();

        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
//            IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponentType.PARTITION_KEY_BLOCKS);
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS), metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
//                                                            bytesWriter,
                                                            blockFPWriter,
                                                            4,
                                                            false))
            {
                primaryKeys.forEach(primaryKey -> {
                    try
                    {
                        writer.add(primaryKey);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                });
            }
        }
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCKS, true));
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCKS, false));
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS, true));
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS, false));
    }

    @Test
    public void testSeekToTerm() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeTerms(keys);

        // iterate on keys ascending
        withKeyLookup(reader ->
                      {
                          for (int x = 0; x < keys.size(); x++)
                          {
                              try (KeyLookup.Cursor cursor = reader.openCursor())
                              {
                                  ByteComparable key = cursor.seekToPointId(x);

                                  byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));

                                  assertArrayEquals(keys.get(x), bytes);
//                    long pointId = cursor.ceiling(ByteComparable.preencoded(VERSION, keys.get(x)));
//                    assertEquals(x, pointId);
                              }
                          }
                      });

        // iterate on keys descending
        withKeyLookup(reader ->
                      {
                          for (int x = keys.size() - 1; x >= 0; x--)
                          {
                              try (KeyLookup.Cursor cursor = reader.openCursor())
                              {
                                  ByteComparable key = cursor.seekToPointId(x);

                                  byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));

                                  assertArrayEquals(keys.get(x), bytes);
//                    long pointId = cursor.ceiling(ByteComparable.preencoded(VERSION, keys.get(x)));
//                    assertEquals(x, pointId);
                              }
                          }
                      });

        // iterate randomly
        withKeyLookup(reader ->
                      {
                          for (int x = 0; x < keys.size(); x++)
                          {
                              int target = nextInt(0, keys.size());

                              try (KeyLookup.Cursor cursor = reader.openCursor())
                              {
                                  ByteComparable key = cursor.seekToPointId(target);

                                  byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));

                                  assertArrayEquals(keys.get(target), bytes);
//                    long pointId = cursor.ceiling(ByteComparable.preencoded(VERSION, keys.get(target)));
//                    assertEquals(target, pointId);
                              }
                          }
                      });
    }

//    @Test
//    public void testSeekToTermMinMaxPrefixNoMatch() throws Exception
//    {
//        IndexDescriptor descriptor = newIndexDescriptor();
//
//        List<ByteSource> termsMinPrefixNoMatch = new ArrayList<>();
//        List<ByteSource> termsMaxPrefixNoMatch = new ArrayList<>();
//        int valuesPerPrefix = 10;
//        writeTerms(descriptor, termsMinPrefixNoMatch, termsMaxPrefixNoMatch, valuesPerPrefix, false);
//
//        var countEndOfData = new AtomicInteger();
//        // iterate on terms ascending
//        withKeyLookup(reader ->
//        {
//            for (int x = 0; x < termsMaxPrefixNoMatch.size(); x++)
//            {
//                try (KeyLookup.Cursor cursor = reader.openCursor())
//                {
//                    int index = x;
//                    long pointIdEnd = cursor.ceiling(v -> termsMinPrefixNoMatch.get(index));
//                    long pointIdStart = cursor.floor(v -> termsMaxPrefixNoMatch.get(index));
//                    if (pointIdStart >= 0 && pointIdEnd >= 0)
//                        assertTrue(pointIdEnd > pointIdStart);
//                    else
//                        countEndOfData.incrementAndGet();
//                }
//            }
//        });
//        // ceiling reaches the end of the data because we call writeTerms with matchesData false, which means that
//        // the last set of terms we are calling ceiling on are greater than anything in the trie, so ceiling returns
//        // a negative value.
//        assertEquals(valuesPerPrefix, countEndOfData.get());
//    }

//    @Test
//    public void testSeekToTermMinMaxPrefix() throws Exception
//    {
//        IndexDescriptor descriptor = newIndexDescriptor();
//
//        List<ByteSource> termsMinPrefix = new ArrayList<>();
//        List<ByteSource> termsMaxPrefix = new ArrayList<>();
//        int valuesPerPrefix = 10;
//        writeTerms(descriptor, termsMinPrefix, termsMaxPrefix, valuesPerPrefix, true);
//
//        // iterate on terms ascending
//        withKeyLookup(reader ->
//        {
//            for (int x = 0; x < termsMaxPrefix.size(); x++)
//            {
//                try (KeyLookup.Cursor cursor = reader.openCursor())
//                {
//                    int index = x;
//                    long pointIdEnd = cursor.ceiling(v -> termsMinPrefix.get(index));
//                    long pointIdStart = cursor.floor(v -> termsMaxPrefix.get(index));
//                    assertEquals(pointIdEnd, x / valuesPerPrefix * valuesPerPrefix);
//                    assertEquals(pointIdEnd + valuesPerPrefix - 1, pointIdStart);
//                }
//            }
//        });
//    }

//    @Test
//    public void testAdvance() throws Exception
//    {
//        IndexDescriptor descriptor = newIndexDescriptor();
//
//        List<byte[]> terms = new ArrayList<>();
//        writeTerms(descriptor, terms);
//
//        withKeyLookupCursor(cursor ->
//        {
//            int x = 0;
//            while (cursor.advance())
//            {
//                ByteComparable term = cursor.term();
//
//                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
//                assertArrayEquals(terms.get(x), bytes);
//
//                x++;
//            }
//
//            // assert we don't increase the point id beyond one point after the last item
//            assertEquals(cursor.pointId(), terms.size());
//            assertFalse(cursor.advance());
//            assertEquals(cursor.pointId(), terms.size());
//        });
//    }

//    @Test
//    public void testReset() throws Exception
//    {
//        IndexDescriptor descriptor = newIndexDescriptor();
//
//        List<byte[]> terms = new ArrayList<>();
//        writeTerms(descriptor, terms);
//
//        withKeyLookupCursor(cursor ->
//        {
//            assertTrue(cursor.advance());
//            assertTrue(cursor.advance());
//            String term1 = cursor.term().byteComparableAsString(VERSION);
//            cursor.reset();
//            assertTrue(cursor.advance());
//            assertTrue(cursor.advance());
//            String term2 = cursor.term().byteComparableAsString(VERSION);
//            assertEquals(term1, term2);
//            assertEquals(1, cursor.pointId());
//        });
//    }

    @Test
    public void testLongPrefixesAndSuffixes() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            // The following writes a set of keys that cover the following conditions:

            // Start value 0
            byte[] bytes = new byte[20];
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix > 15
            bytes = new byte[20];
            Arrays.fill(bytes, 16, 20, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix == 15
            bytes = new byte[20];
            Arrays.fill(bytes, 15, 20, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix < 15
            bytes = new byte[20];
            Arrays.fill(bytes, 14, 20, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // suffix > 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 4, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // suffix == 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 5, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // suffix < 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 6, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));

            bytes = new byte[32];
            Arrays.fill(bytes, 0, 16, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix >= 15 && suffix >= 16
            bytes = new byte[32];
            Arrays.fill(bytes, 0, 32, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testNonUniqueKeys() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();

        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(5000);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, TypeUtil.BYTE_COMPARABLE_VERSION);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                keys.add(bytes);

                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();

        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                keys.add(bytes);

                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testSeekToPointIdCC() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();
        writeTerms(terms);

        // iterate ascending
        withKeyLookupCursor(cursor ->
                            {
                                for (int x = 0; x < terms.size(); x++)
                                {
                                    ByteComparable term = cursor.seekToPointId(x);

                                    byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                                    assertArrayEquals(terms.get(x), bytes);
                                }
                            });

        // iterate descending
        withKeyLookupCursor(cursor ->
                            {
                                for (int x = terms.size() - 1; x >= 0; x--)
                                {
                                    ByteComparable term = cursor.seekToPointId(x);

                                    byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                                    assertArrayEquals(terms.get(x), bytes);
                                }
                            });

        // iterate randomly
        withKeyLookupCursor(cursor ->
                            {
                                for (int x = 0; x < terms.size(); x++)
                                {
                                    int target = nextInt(0, terms.size());
                                    ByteComparable term = cursor.seekToPointId(target);

                                    byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                                    assertArrayEquals(terms.get(target), bytes);
                                }
                            });
    }

    @Test
    public void testSeekToPointIdOutOfRange() throws Exception
    {
        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);

                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        withKeyLookupCursor(cursor -> {
            assertThatThrownBy(() -> cursor.seekToPointId(-2)).isInstanceOf(IndexOutOfBoundsException.class)
                                                              .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, -2, 4000));
            assertThatThrownBy(() -> cursor.seekToPointId(Long.MAX_VALUE)).isInstanceOf(IndexOutOfBoundsException.class)
                                                                          .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, Long.MAX_VALUE, 4000));
            assertThatThrownBy(() -> cursor.seekToPointId(4000)).isInstanceOf(IndexOutOfBoundsException.class)
                                                                .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, 4000, 4000));
        });
    }

    @Test
    public void testSeekToKey() throws Exception
    {
        Map<Long, byte[]> keys = new HashMap<>();

        writeKeys(writer -> {
            long pointId = 0;
            for (int x = 0; x < 4000; x += 4)
            {
                byte[] key = makeKey(x);
                keys.put(pointId++, key);

                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            assertEquals(0L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(0L)), 0L, 10L));
            cursor.reset();
            assertEquals(160L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(160L)), 160L, 170L));
            cursor.reset();
            assertEquals(165L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(165L)), 160L, 170L));
            cursor.reset();
            assertEquals(175L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(175L)), 160L, 176L));
            cursor.reset();
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(176L)), 160L, 177L));
            cursor.reset();
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(176L)), 175L, 177L));
            cursor.reset();
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(701)), 160L, 177L));
            cursor.reset();
            assertEquals(504L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(504L)), 200L, 600L));
            cursor.reset();
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(4000)), 0L, 1000L));
            cursor.reset();
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(4000)), 999L, 1000L));
            cursor.reset();
            assertEquals(999L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(999L)), 0L, 1000L));
        });
    }

    @Test
    public void seekToKeyOnNonPartitionedTest() throws Throwable
    {
        Map<Long, byte[]> keys = new HashMap<>();

        writeKeys(writer -> {
            long pointId = 0;
            for (int x = 0; x < 16; x += 4)
            {
                byte[] key = makeKey(x);
                keys.put(pointId++, key);

                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, false);

        withKeyLookupCursor(cursor -> assertThatThrownBy(() -> cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(0L)), 0L, 10L))
                                      .isInstanceOf(AssertionError.class));
    }

    @Test
    public void partitionedKeysMustBeInOrderInPartitions() throws Throwable
    {
        writeKeys(writer -> {
            writer.startPartition();
            writer.add(ByteComparable.preencoded(VERSION, makeKey(0)));
            writer.add(ByteComparable.preencoded(VERSION, makeKey(10)));
            assertThatThrownBy(() -> writer.add(ByteComparable.preencoded(VERSION, makeKey(9)))).isInstanceOf(IllegalArgumentException.class);
            writer.startPartition();
            writer.add(ByteComparable.preencoded(VERSION, makeKey(9)));
        }, true);
    }

    private byte[] makeKey(int value)
    {
        ByteBuffer buffer = Int32Type.instance.decompose(value);
        ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, TypeUtil.BYTE_COMPARABLE_VERSION);
        return ByteSourceInverse.readBytes(byteSource);
    }

    private void doTestKeyLookup(List<byte[]> keys) throws Exception
    {
        // iterate ascending
        withKeyLookupCursor(cursor -> {
            for (int x = 0; x < keys.size(); x++)
            {
                ByteComparable key = cursor.seekToPointId(x);

                byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

                assertArrayEquals(keys.get(x), bytes);
            }
        });

        // iterate ascending skipping blocks
        withKeyLookupCursor(cursor -> {
            for (int x = 0; x < keys.size(); x += 17)
            {
                ByteComparable key = cursor.seekToPointId(x);

                byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

                assertArrayEquals(keys.get(x), bytes);
            }
        });

        withKeyLookupCursor(cursor -> {
            ByteComparable key = cursor.seekToPointId(7);
            byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(keys.get(7), bytes);

            key = cursor.seekToPointId(7);
            bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(keys.get(7), bytes);
        });
    }

    private void writeTerms(List<byte[]> terms) throws Exception
    {
//        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
//        try (MetadataWriter metadataWriter = new MetadataWriter(components))
//        {
//            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
//                                                                        metadataWriter, true);
//            try (SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
//                                                                  metadataWriter,
//                                                                  blockFPWriter,
//                                                                  components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
//            {
        writeKeys(writer -> {

            for (int x = 0; x < 1000 * 4; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);
//        }
//        components.markComplete();
    }

    private void writeTerms(List<ByteSource> termsMinPrefix, List<ByteSource> termsMaxPrefix, int numPerPrefix, boolean matchesData) throws Exception
    {
//        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
//        try (MetadataWriter metadataWriter = new MetadataWriter(components))
//        {
//            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
//                                                                        metadataWriter, true);
//            try (SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
//                                                                  metadataWriter,
//                                                                  blockFPWriter,
//                                                                  components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
//            {
        writeKeys(writer -> {
            for (int x = 0; x < 1000; x++)
            {
                int component1 = x * 2;
                for (int i = 0; i < numPerPrefix; i++)
                {
                    String component2 = "v" + i;
                    termsMinPrefix.add(ByteSource.withTerminator(ByteSource.LT_NEXT_COMPONENT, intByteSource(component1 + (matchesData ? 0 : 1))));
                    termsMaxPrefix.add(ByteSource.withTerminator(ByteSource.GT_NEXT_COMPONENT, intByteSource(component1 + (matchesData ? 0 : 1))));
                    writer.add(v -> ByteSource.withTerminator(ByteSource.TERMINATOR, intByteSource(component1), utfByteSource(component2)));
                }
            }
        }, false);
//        }
//        components.markComplete();
    }

    private ByteSource intByteSource(int value)
    {
        ByteBuffer buffer = Int32Type.instance.decompose(value);
        return Int32Type.instance.asComparableBytes(buffer, VERSION);
    }

    private ByteSource utfByteSource(String value)
    {
        ByteBuffer buffer = UTF8Type.instance.decompose(value);
        return UTF8Type.instance.asComparableBytes(buffer, VERSION);
    }

    protected void writeKeys(ThrowingConsumer<KeyStoreWriter> testCode, boolean clustering) throws Exception
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
//            IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PARTITION_KEY_BLOCKS);
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS), metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
//                                                            bytesWriter,
                                                            blockFPWriter,
                                                            4,
                                                            clustering))
            {
                testCode.accept(writer);
            }
        }
        components.markComplete();
    }

    private void withKeyLookup(ThrowingConsumer<KeyLookup> testCode) throws Exception
    {
        IndexComponents.ForRead components = indexDescriptor.perSSTableComponents();
        MetadataSource metadataSource = MetadataSource.loadMetadata(components);
        NumericValuesMeta blockPointersMeta = new NumericValuesMeta(metadataSource.get(components.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS)));
        KeyLookupMeta keyLookupMeta = new KeyLookupMeta(metadataSource.get(components.get(IndexComponentType.PARTITION_KEY_BLOCKS)));
        try (FileHandle keysData = components.get(IndexComponentType.PARTITION_KEY_BLOCKS).createFileHandle(null);
             FileHandle blockOffsets = components.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS).createFileHandle(null))
        {
            KeyLookup reader = new KeyLookup(keysData, blockOffsets, keyLookupMeta, blockPointersMeta);
            testCode.accept(reader);
        }
    }

    private void withKeyLookupCursor(ThrowingConsumer<KeyLookup.Cursor> testCode) throws Exception
    {
        withKeyLookup(reader -> {
            try (KeyLookup.Cursor cursor = reader.openCursor())
            {
                testCode.accept(cursor);
            }
        });
    }

    private boolean validateComponent(IndexComponents.ForRead components, IndexComponentType indexComponentType, boolean checksum)
    {
        try (IndexInput input = components.get(indexComponentType).openInput())
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input);
            return true;
        }
        catch (Throwable ignored)
        {
        }
        return false;
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T>
    {
        void accept(T t) throws Exception;
    }
}
