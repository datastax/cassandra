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

package org.apache.cassandra.index.sai.disk.v2.sortedterms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
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

public class SortedTermsTest extends SaiRandomizedTest
{

    public static final ByteComparable.Version VERSION = TypeUtil.BYTE_COMPARABLE_VERSION;

    @Test
    public void testLexicographicException() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
                                                                  metadataWriter,
                                                                  blockFPWriter,
                                                                  components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
            {
                ByteBuffer buffer = Int32Type.instance.decompose(99999);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes1 = ByteSourceInverse.readBytes(byteSource);

                writer.add(ByteComparable.preencoded(VERSION, bytes1));

                buffer = Int32Type.instance.decompose(444);
                byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes2 = ByteSourceInverse.readBytes(byteSource);

                assertThrows(IllegalArgumentException.class, () -> writer.add(ByteComparable.preencoded(VERSION, bytes2)));
            }
        }
    }

    @Test
    public void testFileValidation() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        List<PrimaryKey> primaryKeys = new ArrayList<>();

        for (int x = 0; x < 11; x++)
        {
            ByteBuffer buffer = UTF8Type.instance.decompose(Integer.toString(x));
            DecoratedKey partitionKey = Murmur3Partitioner.instance.decorateKey(buffer);
            PrimaryKey primaryKey = SAITester.ROW_AWARE_TEST_FACTORY.create(partitionKey, Clustering.EMPTY);
            primaryKeys.add(primaryKey);
        }

        primaryKeys.sort(PrimaryKey::compareTo);

        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
                                                                  metadataWriter,
                                                                  blockFPWriter,
                                                                  components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
            {
                primaryKeys.forEach(primaryKey -> {
                    try
                    {
                        writer.add(v -> primaryKey.asComparableBytes(v));
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                });
            }
        }
        assertTrue(validateComponent(components, IndexComponentType.PRIMARY_KEY_TRIE, true));
        assertTrue(validateComponent(components, IndexComponentType.PRIMARY_KEY_TRIE, false));
        assertTrue(validateComponent(components, IndexComponentType.PRIMARY_KEY_BLOCKS, true));
        assertTrue(validateComponent(components, IndexComponentType.PRIMARY_KEY_BLOCKS, false));
        assertTrue(validateComponent(components, IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS, true));
        assertTrue(validateComponent(components, IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS, false));
    }

    @Test
    public void testSeekToTerm() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        writeTerms(descriptor, terms);

        // iterate on terms ascending
        withSortedTermsReader(descriptor, reader ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                try (SortedTermsReader.Cursor cursor = reader.openCursor())
                {
                    long pointId = cursor.ceiling(ByteComparable.preencoded(VERSION, terms.get(x)));
                    assertEquals(x, pointId);
                }
            }
        });

        // iterate on terms descending
        withSortedTermsReader(descriptor, reader ->
        {
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                try (SortedTermsReader.Cursor cursor = reader.openCursor())
                {
                    long pointId = cursor.ceiling(ByteComparable.preencoded(VERSION, terms.get(x)));
                    assertEquals(x, pointId);
                }
            }
        });

        // iterate randomly
        withSortedTermsReader(descriptor, reader ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());

                try (SortedTermsReader.Cursor cursor = reader.openCursor())
                {
                    long pointId = cursor.ceiling(ByteComparable.preencoded(VERSION, terms.get(target)));
                    assertEquals(target, pointId);
                }
            }
        });
    }

    /**
     * {@code ceiling()} on a single cursor is stateful: the trie walk restarts from the current cursor position. This
     * pins that increasing keys (the documented contract), exact or between terms, return the least term greater
     * than or equal to the key whether the bytes on the cursor path are below or above 0x80 (e.g. the sign-flipped
     * first byte of every non-negative int/long/token encoding).
     */
    @Test
    public void testCeilingOnSingleCursorWithBytesAbove0x7F() throws Exception
    {
        // in unsigned byte order; point ids 0..9
        byte[] firstBytes = { 0x10, (byte) 0x90 };
        byte[] secondBytes = { 0x10, 0x30, 0x50, (byte) 0x90, (byte) 0xB0 };
        List<byte[]> terms = new ArrayList<>();
        for (byte first : firstBytes)
            for (byte second : secondBytes)
                terms.add(new byte[]{ first, second });

        IndexDescriptor descriptor = newIndexDescriptor();
        writeSortedTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, cursor -> {
            assertEquals(1, cursor.ceiling(key(0x10, 0x20)));
            assertEquals(3, cursor.ceiling(key(0x10, 0x90)));
            assertEquals(4, cursor.ceiling(key(0x10, 0xA0)));
            assertEquals(5, cursor.ceiling(key(0x90, 0x00)));
            assertEquals(6, cursor.ceiling(key(0x90, 0x30)));
            assertEquals(8, cursor.ceiling(key(0x90, 0x60)));
            assertEquals(9, cursor.ceiling(key(0x90, 0xB0)));
            assertTrue(cursor.ceiling(key(0x90, 0xB1)) < 0);
        });
    }

    private static ByteComparable key(int first, int second)
    {
        return ByteComparable.preencoded(VERSION, new byte[]{ (byte) first, (byte) second });
    }

    @Test
    public void testSeekToTermMinMaxPrefixNoMatch() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<ByteSource> termsMinPrefixNoMatch = new ArrayList<>();
        List<ByteSource> termsMaxPrefixNoMatch = new ArrayList<>();
        int valuesPerPrefix = 10;
        writeTerms(descriptor, termsMinPrefixNoMatch, termsMaxPrefixNoMatch, valuesPerPrefix, false);

        var countEndOfData = new AtomicInteger();
        // iterate on terms ascending
        withSortedTermsReader(descriptor, reader ->
        {
            for (int x = 0; x < termsMaxPrefixNoMatch.size(); x++)
            {
                try (SortedTermsReader.Cursor cursor = reader.openCursor())
                {
                    int index = x;
                    long pointIdEnd = cursor.ceiling(v -> termsMinPrefixNoMatch.get(index));
                    long pointIdStart = cursor.floor(v -> termsMaxPrefixNoMatch.get(index));
                    if (pointIdStart >= 0 && pointIdEnd >= 0)
                        assertTrue(pointIdEnd > pointIdStart);
                    else
                        countEndOfData.incrementAndGet();
                }
            }
        });
        // ceiling reaches the end of the data because we call writeTerms with matchesData false, which means that
        // the last set of terms we are calling ceiling on are greater than anything in the trie, so ceiling returns
        // a negative value.
        assertEquals(valuesPerPrefix, countEndOfData.get());
    }

    @Test
    public void testSeekToTermMinMaxPrefix() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<ByteSource> termsMinPrefix = new ArrayList<>();
        List<ByteSource> termsMaxPrefix = new ArrayList<>();
        int valuesPerPrefix = 10;
        writeTerms(descriptor, termsMinPrefix, termsMaxPrefix, valuesPerPrefix, true);

        // iterate on terms ascending
        withSortedTermsReader(descriptor, reader ->
        {
            for (int x = 0; x < termsMaxPrefix.size(); x++)
            {
                try (SortedTermsReader.Cursor cursor = reader.openCursor())
                {
                    int index = x;
                    long pointIdEnd = cursor.ceiling(v -> termsMinPrefix.get(index));
                    long pointIdStart = cursor.floor(v -> termsMaxPrefix.get(index));
                    assertEquals(pointIdEnd, x / valuesPerPrefix * valuesPerPrefix);
                    assertEquals(pointIdEnd + valuesPerPrefix - 1, pointIdStart);
                }
            }
        });
    }

    @Test
    public void testAdvance() throws IOException
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        writeTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, cursor ->
        {
            int x = 0;
            while (cursor.advance())
            {
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                assertArrayEquals(terms.get(x), bytes);

                x++;
            }

            // assert we don't increase the point id beyond one point after the last item
            assertEquals(cursor.pointId(), terms.size());
            assertFalse(cursor.advance());
            assertEquals(cursor.pointId(), terms.size());
        });
    }

    @Test
    public void testReset() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        writeTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, cursor ->
        {
            assertTrue(cursor.advance());
            assertTrue(cursor.advance());
            String term1 = cursor.term().byteComparableAsString(VERSION);
            cursor.reset();
            assertTrue(cursor.advance());
            assertTrue(cursor.advance());
            String term2 = cursor.term().byteComparableAsString(VERSION);
            assertEquals(term1, term2);
            assertEquals(1, cursor.pointId());
        });
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        writeTerms(descriptor, terms);

        // iterate ascending
        withSortedTermsCursor(descriptor, cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                cursor.seekToPointId(x);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                assertArrayEquals(terms.get(x), bytes);
            }
        });

        // iterate descending
        withSortedTermsCursor(descriptor, cursor ->
        {
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                cursor.seekToPointId(x);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                assertArrayEquals(terms.get(x), bytes);
            }
        });

        // iterate randomly
        withSortedTermsCursor(descriptor, cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());
                cursor.seekToPointId(target);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                assertArrayEquals(terms.get(target), bytes);
            }
        });
    }

    @Test
    public void testSeekToPointIdOutOfRange() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        writeTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, cursor ->
        {
            assertThrows(IndexOutOfBoundsException.class, () -> cursor.seekToPointId(-2));
            assertThrows(IndexOutOfBoundsException.class, () -> cursor.seekToPointId(Long.MAX_VALUE));
        });
    }

    private void writeTerms(IndexDescriptor indexDescriptor, List<byte[]> terms) throws IOException
    {
        for (int x = 0; x < 1000 * 4; x++)
        {
            ByteBuffer buffer = Int32Type.instance.decompose(x);
            ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
            terms.add(ByteSourceInverse.readBytes(byteSource));
        }
        writeSortedTerms(indexDescriptor, terms);
    }

    private void writeSortedTerms(IndexDescriptor indexDescriptor, List<byte[]> terms) throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
                                                                  metadataWriter,
                                                                  blockFPWriter,
                                                                  components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
            {
                for (byte[] bytes : terms)
                    writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }
        components.markComplete();
    }

    private void writeTerms(IndexDescriptor indexDescriptor, List<ByteSource> termsMinPrefix, List<ByteSource> termsMaxPrefix, int numPerPrefix, boolean matchesData) throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (SortedTermsWriter writer = new SortedTermsWriter(components.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
                                                                  metadataWriter,
                                                                  blockFPWriter,
                                                                  components.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE)))
            {
                for (int x = 0; x < 1000 ; x++)
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
            }
        }
        components.markComplete();
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

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws IOException;
    }

    private void withSortedTermsCursor(IndexDescriptor indexDescriptor,
                                       ThrowingConsumer<SortedTermsReader.Cursor> testCode) throws IOException
    {
        IndexComponents.ForRead components = indexDescriptor.perSSTableComponents();
        MetadataSource metadataSource = MetadataSource.loadMetadata(components);
        IndexComponent.ForRead blocksComponent = components.get(IndexComponentType.PRIMARY_KEY_BLOCKS);
        IndexComponent.ForRead blockOffsetsComponent = components.get(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS);
        NumericValuesMeta blockPointersMeta = new NumericValuesMeta(metadataSource.get(blockOffsetsComponent.fileNamePart()));
        SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(blocksComponent.fileNamePart()));
        try (FileHandle trieHandle = components.get(IndexComponentType.PRIMARY_KEY_TRIE).createFileHandle();
             FileHandle termsData = blocksComponent.createFileHandle();
             FileHandle blockOffsets = blockOffsetsComponent.createFileHandle())
        {
            SortedTermsReader reader = new SortedTermsReader(termsData, blockOffsets, trieHandle, sortedTermsMeta, blockPointersMeta);
            try (SortedTermsReader.Cursor cursor = reader.openCursor())
            {
                testCode.accept(cursor);
            }
        }
    }

    private void withSortedTermsReader(IndexDescriptor indexDescriptor,
                                       ThrowingConsumer<SortedTermsReader> testCode) throws IOException
    {
        IndexComponents.ForRead components = indexDescriptor.perSSTableComponents();
        MetadataSource metadataSource = MetadataSource.loadMetadata(components);
        IndexComponent.ForRead blocksComponent = components.get(IndexComponentType.PRIMARY_KEY_BLOCKS);
        IndexComponent.ForRead blockOffsetsComponent = components.get(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS);
        NumericValuesMeta blockPointersMeta = new NumericValuesMeta(metadataSource.get(blockOffsetsComponent.fileNamePart()));
        SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(blocksComponent.fileNamePart()));
        try (FileHandle trieHandle = components.get(IndexComponentType.PRIMARY_KEY_TRIE).createFileHandle();
             FileHandle termsData = blocksComponent.createFileHandle();
             FileHandle blockOffsets = blockOffsetsComponent.createFileHandle())
        {
            SortedTermsReader reader = new SortedTermsReader(termsData, blockOffsets, trieHandle, sortedTermsMeta, blockPointersMeta);
            testCode.accept(reader);
        }
    }

    private boolean validateComponent(IndexComponents.ForRead components, IndexComponentType indexComponentType, boolean checksum)
    {
        try (IndexInput input = components.get(indexComponentType).openInput())
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input, components.version());
            else
                SAICodecUtils.validate(input);
            return true;
        }
        catch (Throwable e)
        {
        }
        return false;
    }
}
