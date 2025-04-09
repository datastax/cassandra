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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.index.CorruptIndexException;

import static org.apache.cassandra.Util.dk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

//TODO This test needs rethinking because we always now end up with a single segment after a flush
// and we are not restricted to Integer.MAX_VALUE in the segments
@RunWith(Parameterized.class)
public class SegmentFlushTest
{
    private static long segmentRowIdOffset;
    private static int minSegmentRowId;
    private static int maxSegmentRowId;
    private static PrimaryKey minKey;
    private static PrimaryKey maxKey;
    private static ByteBuffer minTerm;
    private static ByteBuffer maxTerm;
    private static int numRowsPerSegment;

    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        // Required because it configures SEGMENT_BUILD_MEMORY_LIMIT, which is needed for Version.AA
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(DatabaseDescriptor.loadConfig());
        return Version.ALL.stream().map(v -> new Object[]{v}).collect(Collectors.toList());
    }

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    @After
    public void reset()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1); // reset
    }

    @Test
    public void multiSegmentBalancedTreePassesChecksumValidation() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.create(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)), Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "ts", TimestampType.instance);
        StorageAttachedIndex index = SAITester.createMockIndex(column);

        SSTableIndexWriter writer = new SSTableIndexWriter(indexDescriptor, index, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(0)), createRow(column, TimestampType.instance.decompose(new Date(1_000L))), 0L);
        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(1)), createRow(column, TimestampType.instance.decompose(new Date(2_000L))), SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1);
        writer.complete(Stopwatch.createStarted());

        // Will throw if checksum validation fails:
        indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.CHECKSUM, true, true);
    }

    @Test
    public void multiSegmentTermsDataPassesChecksumValidation() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.create(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)), Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "name", UTF8Type.instance);
        StorageAttachedIndex index = SAITester.createMockIndex(column);

        SSTableIndexWriter writer = new SSTableIndexWriter(indexDescriptor, index, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(0)), createRow(column, UTF8Type.instance.decompose("a")), 0L);
        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(1)), createRow(column, UTF8Type.instance.decompose("b")), SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1);
        writer.complete(Stopwatch.createStarted());

        // Will throw if checksum validation fails:
        indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.CHECKSUM, true, true);
    }

    @Test
    public void multiSegmentBalancedTreePassesHeaderFooterValidation() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.create(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)), Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "ts", TimestampType.instance);
        StorageAttachedIndex index = SAITester.createMockIndex(column);

        SSTableIndexWriter writer = new SSTableIndexWriter(indexDescriptor, index, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(0)), createRow(column, TimestampType.instance.decompose(new Date(1_000L))), 0L);
        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(1)), createRow(column, TimestampType.instance.decompose(new Date(2_000L))), SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1);
        writer.complete(Stopwatch.createStarted());

        indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.HEADER_FOOTER, false, true);
    }

    @Test
    public void multiSegmentBalancedTreeFailsChecksumOnFirstSegmentByteFlip() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.create(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)), Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "ts", TimestampType.instance);
        StorageAttachedIndex index = SAITester.createMockIndex(column);

        SSTableIndexWriter writer = new SSTableIndexWriter(indexDescriptor, index, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(0)), createRow(column, TimestampType.instance.decompose(new Date(1_000L))), 0L);
        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(1)), createRow(column, TimestampType.instance.decompose(new Date(2_000L))), SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1);
        writer.complete(Stopwatch.createStarted());

        // Locate segment 0's payload extent so the flip lands inside it. Corrupting the FIRST
        // (not last) segment specifically proves the validator inspects every segment -- a
        // validator that only checked the trailing footer would miss this and silently pass.
        MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, index.identifier());
        List<SegmentMetadata> segments = SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);
        assertEquals(2, segments.size());
        SegmentMetadata.ComponentMetadata cm = segments.get(0).componentMetadatas.get(IndexComponent.BALANCED_TREE);
        long flipPosition = cm.offset + cm.length / 2;

        File balancedTree = indexDescriptor.fileFor(IndexComponent.BALANCED_TREE, index.identifier());
        try (RandomAccessFile raf = new RandomAccessFile(balancedTree.toJavaIOFile(), "rw"))
        {
            raf.seek(flipPosition);
            int original = raf.readByte();
            raf.seek(flipPosition);
            raf.writeByte(original ^ 0xFF);
        }

        try
        {
            indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.CHECKSUM, true, true);
            fail("Expected corrupted first segment to fail checksum validation");
        }
        catch (UncheckedIOException expected)
        {
            assertTrue("Expected CorruptIndexException cause; got " + expected.getCause(), expected.getCause() instanceof CorruptIndexException);
        }
    }

    @Test
    public void multiSegmentBalancedTreeFailsChecksumOnAppendedGarbage() throws IOException
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.create(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)), Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "ts", TimestampType.instance);
        StorageAttachedIndex index = SAITester.createMockIndex(column);

        SSTableIndexWriter writer = new SSTableIndexWriter(indexDescriptor, index, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(0)), createRow(column, TimestampType.instance.decompose(new Date(1_000L))), 0L);
        writer.addRow(SAITester.TEST_FACTORY.create(keys.get(1)), createRow(column, TimestampType.instance.decompose(new Date(2_000L))), SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1);
        writer.complete(Stopwatch.createStarted());

        // Append 100 random bytes past the last segment's footer. The per-segment slice loop
        // walks each segment's declared frame, then asserts that frameStart == input.length()
        // once done. This corruption trips that post-loop invariant, not a per-segment CRC.
        SAITester.CorruptionType.APPENDED_DATA.corrupt(indexDescriptor.fileFor(IndexComponent.BALANCED_TREE, index.identifier()));

        try
        {
            indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.CHECKSUM, true, true);
            fail("Expected trailing garbage to fail checksum validation");
        }
        catch (UncheckedIOException expected)
        {
            assertTrue("Expected CorruptIndexException cause; got " + expected.getCause(), expected.getCause() instanceof CorruptIndexException);
        }
    }

    @Test
    public void testFlushBetweenRowIds() throws Exception
    {
        // exceeds max rowId per segment
        testFlushBetweenRowIds(0, Integer.MAX_VALUE, 2);
        testFlushBetweenRowIds(0, Long.MAX_VALUE - 1, 2);
        testFlushBetweenRowIds(0, SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID + 1, 2);
        testFlushBetweenRowIds(Integer.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID - 1, Integer.MAX_VALUE - 1, 1);
        testFlushBetweenRowIds(Long.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID - 1, Long.MAX_VALUE - 1, 1);
    }

    @Test
    public void testNoFlushBetweenRowIds() throws Exception
    {
        // not exceeds max rowId per segment
        testFlushBetweenRowIds(0, SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID, 1);
        testFlushBetweenRowIds(Long.MAX_VALUE - SegmentBuilder.LAST_VALID_SEGMENT_ROW_ID, Long.MAX_VALUE - 1, 1);
    }

    private void testFlushBetweenRowIds(long sstableRowId1, long sstableRowId2, int segments) throws Exception
    {
        Path tmpDir = Files.createTempDirectory("SegmentFlushTest");
        IndexDescriptor indexDescriptor = IndexDescriptor.empty(new Descriptor(new File(tmpDir.toFile()), "ks", "cf", new SequenceBasedSSTableId(1)));

        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "column", UTF8Type.instance);

        IndexContext indexContext = new IndexContext("ks",
                                                     "cf",
                                                     TableId.generate(),
                                                     UTF8Type.instance,
                                                     new ClusteringComparator(),
                                                     column,
                                                     IndexTarget.Type.SIMPLE,
                                                     config,
                                                     MockSchema.newCFS("ks"));

        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        SSTableIndexWriter writer = new SSTableIndexWriter(components, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER, () -> true, 2);

        List<DecoratedKey> keys = Arrays.asList(dk("1"), dk("2"));
        Collections.sort(keys);

        DecoratedKey key1 = keys.get(0);
        ByteBuffer term1 = UTF8Type.instance.decompose("a");
        Row row1 = createRow(column, term1);
        writer.addRow(SAITester.TEST_FACTORY.create(key1, Clustering.EMPTY), row1, sstableRowId1);

        // expect a flush if exceed max rowId per segment
        DecoratedKey key2 = keys.get(1);
        ByteBuffer term2 = UTF8Type.instance.decompose("b");
        Row row2 = createRow(column, term2);
        writer.addRow(SAITester.TEST_FACTORY.create(key2, Clustering.EMPTY), row2, sstableRowId2);

        writer.complete(Stopwatch.createStarted());

        MetadataSource source = MetadataSource.loadMetadata(components);

        // verify segment count
        List<SegmentMetadata> segmentMetadatas = SegmentMetadata.load(source, indexContext);
        assertEquals(segments, segmentMetadatas.size());

        // verify segment metadata
        SegmentMetadata segmentMetadata = segmentMetadatas.get(0);
        segmentRowIdOffset = sstableRowId1; // segmentRowIdOffset is the first sstable row id
        minSegmentRowId = 0;
        maxSegmentRowId = segments == 1 ? (int) (sstableRowId2 - segmentRowIdOffset) : 0;
        minKey = SAITester.TEST_FACTORY.createTokenOnly(key1.getToken());
        maxKey = SAITester.TEST_FACTORY.createTokenOnly(segments == 1 ? key2.getToken() : key1.getToken());
        minTerm = term1;
        maxTerm = segments == 1 ? term2 : term1;
        numRowsPerSegment = segments == 1 ? 2 : 1;
        verifySegmentMetadata(segmentMetadata);
        verifyStringIndex(components, segmentMetadata);

        // verify 2nd segment
        if (segments > 1)
        {
            Preconditions.checkState(segments == 2);
            segmentRowIdOffset = sstableRowId2;
            minSegmentRowId = 0;
            maxSegmentRowId = 0;
            minKey = SAITester.TEST_FACTORY.createTokenOnly(key2.getToken());
            maxKey = minKey;
            minTerm = term2;
            maxTerm = term2;
            numRowsPerSegment = 1;

            segmentMetadata = segmentMetadatas.get(1);
            verifySegmentMetadata(segmentMetadata);
            verifyStringIndex(components, segmentMetadata);
        }
    }

    private void verifyStringIndex(IndexComponents.ForRead components, SegmentMetadata segmentMetadata) throws IOException
    {
        FileHandle termsData = components.get(IndexComponentType.TERMS_DATA).createFileHandle();
        FileHandle postingLists = components.get(IndexComponentType.POSTING_LISTS).createFileHandle();

        long termsFooterPointer = Long.parseLong(segmentMetadata.componentMetadatas.get(IndexComponentType.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(components.context(),
                                                  termsData,
                                                  components.byteComparableVersionFor(IndexComponentType.TERMS_DATA),
                                                  postingLists,
                                                  segmentMetadata.componentMetadatas.get(IndexComponentType.TERMS_DATA).root,
                                                  termsFooterPointer,
                                                  version))
        {
            TermsIterator iterator = reader.allTerms();
            assertEquals(minTerm, iterator.getMinTerm());
            assertEquals(maxTerm, iterator.getMaxTerm());

            verifyTermPostings(iterator, minTerm, minSegmentRowId, minSegmentRowId);

            if (numRowsPerSegment > 1)
            {
                verifyTermPostings(iterator, maxTerm, maxSegmentRowId, maxSegmentRowId);
            }

            assertFalse(iterator.hasNext());
        }
    }

    private void verifyTermPostings(TermsIterator iterator, ByteBuffer expectedTerm, int minSegmentRowId, int maxSegmentRowId) throws IOException
    {
        ByteComparable term = iterator.next();
        PostingList postings = iterator.postings();

        assertEquals(0, ByteComparable.compare(term,
                                               ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, expectedTerm),
                                               TypeUtil.BYTE_COMPARABLE_VERSION));
        assertEquals(minSegmentRowId == maxSegmentRowId ? 1 : 2, postings.size());
    }

    private void verifySegmentMetadata(SegmentMetadata segmentMetadata)
    {
        assertEquals(segmentRowIdOffset, segmentMetadata.minSSTableRowId);
        assertEquals(minKey, segmentMetadata.minKey);
        assertEquals(maxKey, segmentMetadata.maxKey);
        assertEquals(minTerm, segmentMetadata.minTerm);
        assertEquals(maxTerm, segmentMetadata.maxTerm);
        assertEquals(numRowsPerSegment, segmentMetadata.numRows);
    }

    private Row createRow(ColumnMetadata column, ByteBuffer value)
    {
        Row.Builder builder1 = BTreeRow.sortedBuilder();
        builder1.newRow(Clustering.EMPTY);
        builder1.addCell(BufferCell.live(column, 0, value));
        return builder1.build();
    }
}
