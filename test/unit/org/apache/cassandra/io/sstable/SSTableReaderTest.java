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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner.LocalToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedRegions;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.BloomCalculations;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.io.sstable.format.SSTableReader.selectOnlyBigTableReaders;
import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SSTableReaderTest
{
    public static final String KEYSPACE1 = "SSTableReaderTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_STANDARD3 = "Standard3";
    public static final String CF_MOVE_AND_OPEN = "MoveAndOpen";
    public static final String CF_COMPRESSED = "Compressed";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARD_LOW_INDEX_INTERVAL = "StandardLowIndexInterval";
    public static final String CF_STANDARD_SMALL_BLOOM_FILTER = "StandardSmallBloomFilter";
    public static final String CF_STANDARD_NO_BLOOM_FILTER = "StandardNoBloomFilter";

    private final List<Ref<?>> refsToRelease = new ArrayList<>();
    private IPartitioner partitioner;

    Token t(int i)
    {
        return partitioner.getToken(ByteBufferUtil.bytes(String.valueOf(i)));
    }

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(8),  // ensure close key count estimation
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD3),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_MOVE_AND_OPEN),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_COMPRESSED).compression(CompressionParams.DEFAULT),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, CF_INDEXED, true),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_LOW_INDEX_INTERVAL)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_SMALL_BLOOM_FILTER)
                                                .minIndexInterval(4)
                                                .maxIndexInterval(4)
                                                .bloomFilterFpChance(0.99),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD_NO_BLOOM_FILTER)
                                                .bloomFilterFpChance(1));

        // All tests in this class assume auto-compaction is disabled.
        CompactionManager.instance.disableAutoCompaction();
    }

    @After
    public void Cleanup() {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).truncateBlocking();
        BloomFilter.recreateOnFPChanceChange = false;

        Throwable exceptions = null;
        for (Ref<?> ref : refsToRelease)
        {
            try
            {
                ref.release();
            }
            catch (Throwable exc)
            {
                exceptions = Throwables.merge(exceptions, exc);
            }
        }

        if (exceptions != null)
            fail("Unable to release all tracked references " + exceptions);

        refsToRelease.clear();
    }

    @Test
    public void testGetPositionsForRanges()
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD2);
        partitioner = store.getPartitioner();

        // insert data and compact to a single sstable
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        List<Range<Token>> ranges = new ArrayList<>();
        // 1 key
        ranges.add(new Range<>(t(0), t(1)));
        // 2 keys
        ranges.add(new Range<>(t(2), t(4)));
        // wrapping range from key to end
        ranges.add(new Range<>(t(6), partitioner.getMinimumToken()));
        // empty range (should be ignored)
        ranges.add(new Range<>(t(9), t(91)));

        // confirm that positions increase continuously
        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        long previous = -1;
        for (PartitionPositionBounds section : sstable.getPositionsForRanges(ranges))
        {
            assert previous <= section.lowerPosition : previous + " ! < " + section.lowerPosition;
            assert section.lowerPosition < section.upperPosition : section.lowerPosition + " ! < " + section.upperPosition;
            previous = section.upperPosition;
        }
    }

    @Test
    public void testEstimatedKeysForRangesAndKeySamples()
    {
        // prepare data
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        partitioner = store.getPartitioner();

        Random random = new Random();
        List<Token> tokens = new ArrayList<>();
        tokens.add(partitioner.getMinimumToken());
        if (partitioner.splitter().isPresent())
            tokens.add(partitioner.getMaximumToken());

        for (int j = 0; j < 100; j++)
        {
            Mutation mutation = new RowUpdateBuilder(store.metadata(), j, String.valueOf(random.nextInt())).clustering("0")
                                                                                                           .add("val",
                                                                                                                ByteBufferUtil.EMPTY_BYTE_BUFFER)
                                                                                                           .build();
            if (j % 4 != 0) // skip some keys
                mutation.applyUnsafe();
            tokens.add(mutation.key().getToken());
        }

        store.forceBlockingFlush(UNIT_TESTS);
        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // verify any combination of start and end point among the keys we have, which includes empty, full and
        // wrap-around ranges
        for (int i = 0; i < tokens.size(); i++)
            for (int j = 0; j < tokens.size(); j++)
            {
                verifyEstimatedKeysAndKeySamples(sstable, new Range<Token>(tokens.get(i), tokens.get(j)));
            }
    }

    private void verifyEstimatedKeysAndKeySamples(SSTableReader sstable, Range<Token> range)
    {
        List<DecoratedKey> expectedKeys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator rowIterator = scanner.next())
                {
                    if (range.contains(rowIterator.partitionKey().getToken()))
                        expectedKeys.add(rowIterator.partitionKey());
                }
            }
        }

        // check estimated key
        long estimated = sstable.estimatedKeysForRanges(Collections.singleton(range));
        assertTrue("Range: " + range + " having " + expectedKeys.size() + " partitions, but estimated "
                   + estimated, closeEstimation(expectedKeys.size(), estimated));

        // check key samples
        List<DecoratedKey> sampledKeys = new ArrayList<>();
        sstable.getKeySamples(range).forEach(sampledKeys::add);

        assertTrue("Range: " + range + " having " + expectedKeys + " keys, but keys sampled: "
                   + sampledKeys, expectedKeys.containsAll(sampledKeys));
        // no duplicate
        assertEquals(expectedKeys.size(), expectedKeys.stream().distinct().count());
        assertEquals(sampledKeys.size(), sampledKeys.stream().distinct().count());
    }

    private boolean closeEstimation(long expected, long estimated)
    {
        return expected <= estimated + 16 && expected >= estimated - 16;
    }

    @Test
    public void testOnDiskSizeForRanges()
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD2);
        partitioner = store.getPartitioner();
        int count = 1000;

        // insert data and compact to a single sstable
        for (int j = 0; j < count; j++)
        {
            new RowUpdateBuilder(store.metadata(), 15000, k0(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);
        store.forceMajorCompaction();

        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // Non-compression-dependent checks
        // Check several ways of going through the whole file
        assertEquals(sstable.onDiskLength(),
                     onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(0), 1)), t0(count - 1)))));
        assertEquals(sstable.onDiskLength(),
                     onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(sstable.getPartitioner().getMinimumToken(), sstable.getPartitioner().getMinimumToken()))));

        // Split at exact match
        assertEquals(sstable.onDiskLength(),
                     onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t0(347)),
                                                                   new Range<>(t0(347), t0(count - 1)))));

        // Split at different prefixes pointing to the same position
        assertEquals(sstable.onDiskLength(),
                     onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t(cut(k0(600), 2))),
                                                                   new Range<>(t(cut(k0(600), 1)), t0(count - 1)))));

        if (!sstable.compression)
        {
            double delta = 0.9;
            // Size one row
            double oneRowSize = sstable.onDiskLength() * 1.0 / count;
            System.out.println("One row size: " + oneRowSize);

            // Ranges are end-inclusive, indexes are adjusted by one here to account for that.
            assertEquals((52 - 38),
                         onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t0(37), t0(51)))) / oneRowSize,
                         delta);

            // Try non-matching positions (inexact indexes are not adjusted for the count).
            assertEquals((34 - 30),
                         onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(30), 1)),
                                                                                        t0(33)))) / oneRowSize,
                         delta);

            assertEquals((700 - 554),
                         onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t0(553),
                                                                                       t(cut(k0(700), 2))))) / oneRowSize,
                         delta);

            assertEquals((500 - 30),
                         onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(30), 1)),
                                                                                       t(cut(k0(500), 2))))) / oneRowSize,
                         delta);

            // Try a list
            List<Range<Token>> ranges = ImmutableList.of(new Range<>(t0(37), t0(51)),
                                                         new Range<>(t0(71), t(cut(k0(100), 2))),
                                                         new Range<>(t(cut(k0(230), 1)), t0(243)),
                                                         new Range<>(t(cut(k0(260), 1)), t(cut(k0(300), 2))),
                                                         new Range<>(t0(373), t0(382)),
                                                         new Range<>(t0(382), t0(385)),
                                                         new Range<>(t(cut(k0(400), 2)), t(cut(k0(400), 1))),  // empty range
                                                         new Range<>(t0(563), t(cut(k0(600), 2))), // touching ranges
                                                         new Range<>(t(cut(k0(600), 1)), t0(621))
            );
            assertEquals((52 - 38 + 100 - 72 + 244 - 230 + 300 - 260 + 383 - 374 + 386 - 383 + 400 - 400 + 622 - 564),
                         onDiskSizeForRanges(sstable, ranges) / oneRowSize,
                         delta);

            // Check going through the whole file
            assertEquals(sstable.onDiskLength(),
                         onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(0), 1)), t0(count - 1)))));

            assertEquals(sstable.onDiskLength(),
                         onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t0(347)),
                                                                      new Range<>(t0(347), t0(count - 1)))));

            assertEquals(sstable.onDiskLength(),
                         onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t(cut(k0(600), 2))),
                                                                      new Range<>(t(cut(k0(600), 1)), t0(count - 1)))));
        }
        else
        {
            // It's much harder to test with compression.

            // Check first three rows have the same size (they must be in the same chunk)
            final long row0size = onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(0), 1)), t0(0))));
            assertEquals(row0size, onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t0(0), t0(1)))));
            assertEquals(row0size, onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t0(1), t0(2)))));

            // As well as the first three rows together
            assertEquals(row0size, onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(0), 1)), t0(2)))));

            // And also when we query for them in separate ranges
            assertEquals(row0size, onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t0(0)),
                                                                                new Range<>(t0(0), t0(1)))));
            assertEquals(row0size, onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t0(0)),
                                                                                new Range<>(t0(1), t0(2)))));
            assertEquals(row0size, onDiskSizeForRanges(sstable, ImmutableList.of(new Range<>(t(cut(k0(0), 1)), t0(0)),
                                                                                new Range<>(t0(0), t0(1)),
                                                                                new Range<>(t0(1), t0(2)))));

            // Finally, check that if we query for every second row we get the total size of the file.
            assertEquals(sstable.onDiskLength(),
                         onDiskSizeForRanges(sstable, IntStream.range(0, count)
                                                              .filter(i -> i % 2 != 0)
                                                              .mapToObj(i -> new Range<>(t0(i), t0(i + 1)))
                                                              .collect(Collectors.toList())));
        }
    }

    @Test
    public void testAssertionsOnDiskSizeForPartitionPositions()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_COMPRESSED);
        SSTableReader sstable = getNewSSTable(cfs);
        partitioner = sstable.getPartitioner();

        assertThatThrownBy(() -> sstable.onDiskSizeForPartitionPositions(Collections.singleton(new PartitionPositionBounds(-1, 0))))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> sstable.onDiskSizeForPartitionPositions(Collections.singleton(new PartitionPositionBounds(2, 1))))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    public void testDiskSizeForEmptyPosition()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_COMPRESSED);
        SSTableReader sstable = getNewSSTable(cfs);
        partitioner = sstable.getPartitioner();

        long size = sstable.onDiskSizeForPartitionPositions(Collections.singleton(new PartitionPositionBounds(0, 0)));
        assertEquals(0, size);
    }

    @Test
    public void testDoNotFailOnChunkEndingPosition()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_COMPRESSED);

        // we want the last row to align to the end of chunk
        int rowCount = DEFAULT_CHUNK_LENGTH;

        // insert data and compact to a single sstable
        for (int j = 0; j < rowCount; j++)
        {
            new RowUpdateBuilder(cfs.metadata(), 15000, k0(j))
                    .clustering("0")
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
        }
        cfs.forceBlockingFlush(UNIT_TESTS);
        cfs.forceMajorCompaction();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        partitioner = sstable.getPartitioner();

        long totalDiskSizeForTheWholeRange = onDiskSizeForRanges(sstable, Collections.singleton(new Range<>(t(cut(k0(0), 1)), t0(rowCount))));
        assertEquals(sstable.onDiskLength(), totalDiskSizeForTheWholeRange);
    }

    long onDiskSizeForRanges(SSTableReader sstable, Collection<Range<Token>> ranges)
    {
        return sstable.onDiskSizeForPartitionPositions(sstable.getPositionsForRanges(ranges));
    }

    private Token t(String key)
    {
        return partitioner.getToken(ByteBufferUtil.bytes(key));
    }

    private String k0(int k)
    {
        return String.format("%08d", k);
    }

    private Token t0(int k)
    {
        return t(k0(k));
    }

    private String cut(String s, int n)
    {
        return s.substring(0, s.length() - n);
    }


    @Test
    public void testSpannedIndexPositions() throws IOException
    {
        doTestSpannedIndexPositions(PageAware.PAGE_SIZE);
    }

    @Test
    public void testSpannedIndexPositionsWithMaxSegmentSizeSmallerThanPageSize() throws IOException
    {
        doTestSpannedIndexPositions(PageAware.PAGE_SIZE - 1);
    }

    public void doTestSpannedIndexPositions(int maxSegmentSize) throws IOException
    {
        // expect to create many regions - that is, the size of index must exceed the page size multiple times
        int originalMaxSegmentSize = MmappedRegions.MAX_SEGMENT_SIZE;
        MmappedRegions.MAX_SEGMENT_SIZE = maxSegmentSize;

        try
        {
            ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD);
            partitioner = store.getPartitioner();

            // insert a bunch of data and compact to a single sstable
            for (int j = 0; j < 10000; j += 2)
            {
                new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
            }
            store.forceBlockingFlush(UNIT_TESTS);
            CompactionManager.instance.performMaximal(store, false);

            // check that all our keys are found correctly
            SSTableReader sstable = store.getLiveSSTables().iterator().next();
            for (int j = 0; j < 10000; j += 2)
            {
                DecoratedKey dk = Util.dk(String.valueOf(j));
                FileDataInput file = sstable.getFileDataInput(sstable.getPosition(dk, SSTableReader.Operator.EQ).position);
                DecoratedKey keyInDisk = sstable.decorateKey(ByteBufferUtil.readWithShortLength(file));
                assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getFile());
            }

            // check no false positives
            for (int j = 1; j < 11000; j += 2)
            {
                DecoratedKey dk = Util.dk(String.valueOf(j));
                assert sstable.getPosition(dk, SSTableReader.Operator.EQ) == null;
            }
        }
        finally
        {
            MmappedRegions.MAX_SEGMENT_SIZE = originalMaxSegmentSize;
        }
    }

    @Test
    public void testPersistentStatistics()
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD3);
        partitioner = store.getPartitioner();

        for (int j = 0; j < 100; j += 2)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);

        clearAndLoad(store);
        assert store.metric.maxPartitionSize.getValue() != 0;
    }

    private void clearAndLoad(ColumnFamilyStore cfs)
    {
        cfs.clearUnsafe();
        cfs.loadNewSSTables();
    }

    @Test
    public void testReadRateTracking()
    {
        // try to make sure CASSANDRA-8239 never happens again
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD);
        partitioner = store.getPartitioner();

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        assertEquals(0, sstable.getReadMeter().count());

        DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("4"));
        Util.getAll(Util.cmd(store, key).build());
        assertEquals(1, sstable.getReadMeter().count());

        Util.getAll(Util.cmd(store, key).includeRow("0").build());
        assertEquals(2, sstable.getReadMeter().count());
    }

    @Test
    public void testGetPositionsForRangesWithKeyCache()
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD2);
        partitioner = store.getPartitioner();
        CacheService.instance.keyCache.setCapacity(100);

        // insert data and compact to a single sstable
        for (int j = 0; j < 10; j++)
        {

            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        long p2 = sstable.getPosition(k(2), SSTableReader.Operator.EQ).position;
        long p3 = sstable.getPosition(k(3), SSTableReader.Operator.EQ).position;
        long p6 = sstable.getPosition(k(6), SSTableReader.Operator.EQ).position;
        long p7 = sstable.getPosition(k(7), SSTableReader.Operator.EQ).position;

        PartitionPositionBounds p = sstable.getPositionsForRanges(makeRanges(t(2), t(6))).get(0);

        // range are start exclusive so we should start at 3
        assert p.lowerPosition == p3;

        // to capture 6 we have to stop at the start of 7
        assert p.upperPosition == p7;
    }

    @Test
    public void testPersistentStatisticsWithSecondaryIndex()
    {
        // Create secondary index and flush to disk
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_INDEXED);
        partitioner = store.getPartitioner();

        new RowUpdateBuilder(store.metadata(), System.currentTimeMillis(), "k1")
            .clustering("0")
            .add("birthdate", 1L)
            .build()
            .applyUnsafe();

        store.forceBlockingFlush(UNIT_TESTS);

        // check if opening and querying works
        assertIndexQueryWorks(store);
    }

    @Test
    public void testGetPositionsKeyCacheStats()
    {
        Assume.assumeThat(SSTableFormat.Type.current(), is(SSTableFormat.Type.BIG));
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD2);
        partitioner = store.getPartitioner();
        CacheService.instance.keyCache.setCapacity(1000);

        // insert data and compact to a single sstable
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        // existing, non-cached key
        sstable.getPosition(k(2), SSTableReader.Operator.EQ);
        assertEquals(1, sstable.getKeyCacheRequest());
        assertEquals(0, sstable.getKeyCacheHit());
        // existing, cached key
        assertEquals(1, store.getBloomFilterTracker().getTruePositiveCount());
        sstable.getPosition(k(2), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getKeyCacheRequest());
        assertEquals(1, sstable.getKeyCacheHit());
        // non-existing key (it is specifically chosen to not be rejected by Bloom Filter check)
        sstable.getPosition(k(14), SSTableReader.Operator.EQ);
        assertEquals(3, sstable.getKeyCacheRequest());
        assertEquals(2, store.getBloomFilterTracker().getTruePositiveCount());
        sstable.getPosition(k(15), SSTableReader.Operator.EQ);
        assertEquals(1, sstable.getKeyCacheHit());
    }

    @Test
    public void testGetPositionsBloomFilterStats()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD_SMALL_BLOOM_FILTER);
        partitioner = store.getPartitioner();
        CacheService.instance.keyCache.setCapacity(1000);

        // insert data and compact to a single sstable
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                    .clustering("0")
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = trackReleaseableRef(() -> store.getLiveSSTables().iterator().next());
        // the keys are specifically chosen to cover certain use cases
        // existing key is read from index
        sstable.getPosition(k(2), SSTableReader.Operator.EQ);
        assertEquals(1, sstable.getBloomFilterTracker().getTruePositiveCount());
        assertEquals(0, sstable.getBloomFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getBloomFilterTracker().getFalsePositiveCount());
        // existing key is read from Cache Key
        sstable.getPosition(k(2), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getBloomFilterTracker().getTruePositiveCount());
        assertEquals(0, sstable.getBloomFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getBloomFilterTracker().getFalsePositiveCount());
        // non-existing key is rejected by Bloom Filter check
        sstable.getPosition(k(10), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getBloomFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getBloomFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getBloomFilterTracker().getFalsePositiveCount());
        // non-existing key is rejected by sstable keys range check
        sstable.getPosition(k(99), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getBloomFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getBloomFilterTracker().getTrueNegativeCount());
        assertEquals(1, sstable.getBloomFilterTracker().getFalsePositiveCount());
        // non-existing key is rejected by index interval check
        sstable.getPosition(k(14), SSTableReader.Operator.EQ);
        assertEquals(2, store.getBloomFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getBloomFilterTracker().getTrueNegativeCount());
        assertEquals(2, sstable.getBloomFilterTracker().getFalsePositiveCount());
        // non-existing key is rejected by index lookup check
        sstable.getPosition(k(807), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getBloomFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getBloomFilterTracker().getTrueNegativeCount());
        assertEquals(3, sstable.getBloomFilterTracker().getFalsePositiveCount());
    }

    @Test
    public void testOpeningSSTable() throws Exception
    {
        String ks = KEYSPACE1;
        String cf = CF_STANDARD;

        // clear and create just one sstable for this test
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cf);
        store.clearUnsafe();

        DecoratedKey firstKey = null, lastKey = null;
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < store.metadata().params.minIndexInterval; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            if (firstKey == null)
                firstKey = key;
            if (lastKey == null)
                lastKey = key;
            if (store.metadata().partitionKeyType.compare(lastKey.getKey(), key.getKey()) < 0)
                lastKey = key;


            new RowUpdateBuilder(store.metadata(), timestamp, key.getKey())
                .clustering("col")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
        }
        store.forceBlockingFlush(UNIT_TESTS);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        assertTrue(SSTableReader.hasGlobalReference(sstable.descriptor));
        Descriptor desc = sstable.descriptor;
        boolean hasSummary = desc.getFormat().supportedComponents().contains(Component.SUMMARY);

        // test to see if sstable can be opened as expected
        SSTableReader target = desc.getFormat().getReaderFactory().open(desc);
        assert target.first.equals(firstKey);
        assert target.last.equals(lastKey);

        executeInternal(String.format("ALTER TABLE \"%s\".\"%s\" WITH bloom_filter_fp_chance = 0.3", ks, cf));

        File summaryFile = desc.fileFor(Component.SUMMARY);
        Path bloomPath = desc.fileFor(Component.FILTER).toPath();
        Path summaryPath = summaryFile.toPath();

        long bloomModified = Files.getLastModifiedTime(bloomPath).toMillis();
        long summaryModified = hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0;

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different

        // Offline tests
        // check that bloomfilter/summary ARE NOT regenerated
        target = desc.getFormat().getReaderFactory().openNoValidation(desc, store.metadata);

        assertEquals(bloomModified, Files.getLastModifiedTime(bloomPath).toMillis());
        assertEquals(summaryModified, hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0);

        target.selfRef().release();

        // check that bloomfilter/summary ARE NOT regenerated and BF=AlwaysPresent when filter component is missing
        Set<Component> components = SSTable.discoverComponentsFor(desc);
        components.remove(Component.FILTER);
        target = desc.getFormat().getReaderFactory().openNoValidation(desc, components, store);

        assertEquals(bloomModified, Files.getLastModifiedTime(bloomPath).toMillis());
        assertEquals(summaryModified, hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0);
        assertEquals(FilterFactory.AlwaysPresent, target.getBloomFilter());

        target.selfRef().release();

        // #### online tests ####
        // check that summary & bloomfilter are not regenerated when SSTable is opened and BFFP has been changed
        target = desc.getFormat().getReaderFactory().open(desc, store.metadata);

        assertEquals(bloomModified, Files.getLastModifiedTime(bloomPath).toMillis());
        assertEquals(summaryModified, hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0);

        target.selfRef().release();

        // check that bloomfilter is recreated when it doesn't exist and this causes the summary to be recreated
        components = SSTable.discoverComponentsFor(desc);
        components.remove(Component.FILTER);

        target = desc.getFormat().getReaderFactory().open(desc, components, store.metadata);

        assertTrue("Bloomfilter was not recreated", bloomModified < Files.getLastModifiedTime(bloomPath).toMillis());
        assertTrue("Summary was not recreated", !hasSummary || summaryModified < Files.getLastModifiedTime(summaryPath).toMillis());

        target.selfRef().release();

        // check that only the summary is regenerated when it is deleted
        components.add(Component.FILTER);
        summaryModified = hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0;
        if (hasSummary)
            summaryFile.tryDelete();

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
        bloomModified = Files.getLastModifiedTime(bloomPath).toMillis();

        target = desc.getFormat().getReaderFactory().open(desc, components, store.metadata);

        assertEquals(bloomModified, Files.getLastModifiedTime(bloomPath).toMillis());
        assertTrue("Summary was not recreated", !hasSummary || summaryModified < Files.getLastModifiedTime(summaryPath).toMillis());

        target.selfRef().release();

        // check that summary and bloomfilter is not recreated when the INDEX is missing
        components.add(Component.SUMMARY);
        components.remove(Component.PRIMARY_INDEX);

        summaryModified = hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0;
        target = desc.getFormat().getReaderFactory().open(desc, components, store.metadata, false, false);

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
        assertEquals(bloomModified, Files.getLastModifiedTime(bloomPath).toMillis());
        assertEquals(summaryModified, hasSummary ? Files.getLastModifiedTime(summaryPath).toMillis() : 0);

        target.selfRef().release();
    }

    @Test
    public void testLoadingSummaryUsesCorrectPartitioner()
    {
        Assume.assumeThat(SSTableFormat.Type.current(), is(SSTableFormat.Type.BIG));
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_INDEXED);

        new RowUpdateBuilder(store.metadata(), System.currentTimeMillis(), "k1")
        .clustering("0")
        .add("birthdate", 1L)
        .build()
        .applyUnsafe();

        store.forceBlockingFlush(UNIT_TESTS);

        for(ColumnFamilyStore indexCfs : store.indexManager.getAllIndexColumnFamilyStores())
        {
            assert indexCfs.isIndex();
            SSTableReader sstable = (SSTableReader) indexCfs.getLiveSSTables().iterator().next();
            assert sstable.first.getToken() instanceof LocalToken;

            SSTableReader.saveSummary(sstable.descriptor, sstable.first, sstable.last, sstable.indexSummary);
            SSTableReader reopened = trackReleaseableRef(() -> sstable.descriptor.getFormat().getReaderFactory().open(sstable.descriptor));
            assert reopened.first.getToken() instanceof LocalToken;
        }
    }

    /** see CASSANDRA-5407 */
    @Test
    public void testGetScannerForNoIntersectingRanges()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD3);
        partitioner = store.getPartitioner();

        new RowUpdateBuilder(store.metadata(), 0, "k1")
            .clustering("xyz")
            .add("val", "abc")
            .build()
            .applyUnsafe();

        store.forceBlockingFlush(UNIT_TESTS);

        Set<SSTableReader> liveSSTables = store.getLiveSSTables();
        assertEquals("The table should have only one sstable", 1, liveSSTables.size());

        ISSTableScanner scanner = liveSSTables.iterator().next().getScanner(new Range<>(t(0), t(1)));
        assertEquals(0, scanner.getLengthInBytes());
    }

    @Test
    public void testGetPositionsForRangesFromTableOpenedForBulkLoading()
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD2);
        partitioner = store.getPartitioner();

        // insert data and compact to a single sstable. The
        // number of keys inserted is greater than index_interval
        // to ensure multiple segments in the index file
        for (int j = 0; j < 130; j++)
        {

            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        // construct a range which is present in the sstable, but whose
        // keys are not found in the first segment of the index.
        List<Range<Token>> ranges = new ArrayList<Range<Token>>();
        ranges.add(new Range<Token>(t(98), t(99)));

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(ranges);
        assert sections.size() == 1 : "Expected to find range in sstable" ;

        // re-open the same sstable as it would be during bulk loading
        Set<Component> components = Sets.newHashSet(sstable.descriptor.getFormat().requiredComponents());
        if (sstable.components().contains(Component.COMPRESSION_INFO))
            components.add(Component.COMPRESSION_INFO);
        SSTableReader bulkLoaded = sstable.descriptor.getFormat().getReaderFactory().openForBatch(sstable.descriptor, components, store.metadata);
        sections = bulkLoaded.getPositionsForRanges(ranges);
        assert sections.size() == 1 : "Expected to find range in sstable opened for bulk loading";
        bulkLoaded.selfRef().release();
    }

    @Test
    public void testIndexSummaryReplacement() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD_LOW_INDEX_INTERVAL); // index interval of 8, no key caching

        final int NUM_PARTITIONS = 512;
        for (int j = 0; j < NUM_PARTITIONS; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.format("%3d", j))
            .clustering("0")
            .add("val", String.format("%3d", j))
            .build()
            .applyUnsafe();

        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        List<SSTableReader> sstables = ImmutableList.copyOf(store.getLiveSSTables());
        assert sstables.size() == 1;
        final SSTableReader sstable = sstables.get(0);

        ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);
        List<Future<?>> futures = new ArrayList<>(NUM_PARTITIONS * 2);
        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            final ByteBuffer key = ByteBufferUtil.bytes(String.format("%3d", i));
            final int index = i;

            futures.add(executor.submit(new Runnable()
            {
                public void run()
                {
                    Row row = Util.getOnlyRowUnfiltered(Util.cmd(store, key).build());
                    assertEquals(0, ByteBufferUtil.compare(String.format("%3d", index).getBytes(), row.cells().iterator().next().buffer()));
                }
            }));

            futures.add(executor.submit(new Runnable()
            {
                public void run()
                {
                    Iterable<DecoratedKey> results = store.keySamples(
                            new Range<>(sstable.getPartitioner().getMinimumToken(), sstable.getPartitioner().getToken(key)));
                    assertTrue(results.iterator().hasNext());
                }
            }));
        }

        SSTableReader replacement;
        try (LifecycleTransaction txn = store.getTracker().tryModify(Collections.singletonList(sstable), OperationType.UNKNOWN))
        {
            replacement = sstable.cloneWithNewSummarySamplingLevel(store, 1);
            txn.update(replacement, true);
            txn.finish();
        }
        for (Future<?> future : futures)
            future.get();

        assertEquals(sstable.estimatedKeys(), replacement.estimatedKeys(), 1);
    }

    @Test
    public void testIndexSummaryUpsampleAndReload() throws Exception
    {
        Assume.assumeThat(SSTableFormat.Type.current(), is(SSTableFormat.Type.BIG));

        int originalMaxSegmentSize = MmappedRegions.MAX_SEGMENT_SIZE;
        MmappedRegions.MAX_SEGMENT_SIZE = 40; // each index entry is ~11 bytes, so this will generate lots of segments

        try
        {
            testIndexSummaryUpsampleAndReload0();
        }
        finally
        {
            MmappedRegions.MAX_SEGMENT_SIZE = originalMaxSegmentSize;
        }
    }

    private void testIndexSummaryUpsampleAndReload0() throws Exception
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD_LOW_INDEX_INTERVAL); // index interval of 8, no key caching

        final int NUM_PARTITIONS = 512;
        for (int j = 0; j < NUM_PARTITIONS; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.format("%3d", j))
            .clustering("0")
            .add("val", String.format("%3d", j))
            .build()
            .applyUnsafe();

        }
        store.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performMaximal(store, false);

        Collection<SSTableReader> sstables = selectOnlyBigTableReaders(store.getLiveSSTables(), Collectors.toList());
        assert sstables.size() == 1;
        final SSTableReader sstable = sstables.iterator().next();

        try (LifecycleTransaction txn = store.getTracker().tryModify(Collections.singletonList(sstable), OperationType.UNKNOWN))
        {
            SSTableReader replacement = sstable.cloneWithNewSummarySamplingLevel(store, sstable.getIndexSummarySamplingLevel() + 1);
            txn.update(replacement, true);
            txn.finish();
        }
        SSTableReader reopen = trackReleaseableRef(() -> sstable.descriptor.formatType.info.getReaderFactory().open(sstable.descriptor));
        assert reopen.getIndexSummarySamplingLevel() == sstable.getIndexSummarySamplingLevel() + 1;
    }

    private void assertIndexQueryWorks(ColumnFamilyStore indexedCFS)
    {
        assert CF_INDEXED.equals(indexedCFS.name);

        // make sure all sstables including 2ary indexes load from disk
        for (ColumnFamilyStore cfs : indexedCFS.concatWithIndexes())
            clearAndLoad(cfs);


        // query using index to see if sstable for secondary index opens
        ReadCommand rc = Util.cmd(indexedCFS).fromKeyIncl("k1").toKeyIncl("k3")
                                             .columns("birthdate")
                                             .filterOn("birthdate", Operator.EQ, 1L)
                                             .build();
        Index.Searcher searcher = rc.getIndex(indexedCFS).searcherFor(rc);
        assertNotNull(searcher);
        try (ReadExecutionController executionController = rc.executionController())
        {
            assertEquals(1, Util.size(UnfilteredPartitionIterators.filter(searcher.search(executionController), rc.nowInSec())));
        }
    }

    private List<Range<Token>> makeRanges(Token left, Token right)
    {
        return Collections.singletonList(new Range<>(left, right));
    }

    private DecoratedKey k(int i)
    {
        return new BufferDecoratedKey(t(i), ByteBufferUtil.bytes(String.valueOf(i)));
    }

    @Test(expected = RuntimeException.class)
    public void testMoveAndOpenLiveSSTable()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);
        SSTableReader sstable = getNewSSTable(cfs);
        Descriptor notLiveDesc = new Descriptor(new File("/tmp"), "", "", SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        notLiveDesc.getFormat().getReaderFactory().moveAndOpenSSTable(cfs, sstable.descriptor, notLiveDesc, sstable.components(), false);
    }

    @Test(expected = RuntimeException.class)
    public void testMoveAndOpenLiveSSTable2()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);
        SSTableReader sstable = getNewSSTable(cfs);
        Descriptor notLiveDesc = new Descriptor(new File("/tmp"), "", "", SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        sstable.descriptor.getFormat().getReaderFactory().moveAndOpenSSTable(cfs, notLiveDesc, sstable.descriptor, sstable.components(), false);
    }

    @Test
    public void testMoveAndOpenSSTable() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_MOVE_AND_OPEN);
        SSTableReader sstable = trackReleaseableRef(() -> getNewSSTable(cfs));
        cfs.clearUnsafe();
        File tmpdir = new File(Files.createTempDirectory("testMoveAndOpen"));
        tmpdir.deleteOnExit();
        SSTableId id = SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get();
        Descriptor notLiveDesc = new Descriptor(tmpdir, sstable.descriptor.ksname, sstable.descriptor.cfname, id);
        // make sure the new directory is empty and that the old files exist:
        for (Component c : sstable.components())
        {
            File f = notLiveDesc.fileFor(c);
            assertFalse(f.exists());
            assertTrue(sstable.descriptor.fileFor(c).exists());
        }
        trackReleaseableRef(() -> notLiveDesc.getFormat().getReaderFactory().moveAndOpenSSTable(cfs, sstable.descriptor, notLiveDesc, sstable.components(), false));
        // make sure the files were moved:
        for (Component c : sstable.components())
        {
            File f = notLiveDesc.fileFor(c);
            assertTrue(f.exists());
            assertTrue(f.toString().contains(String.format("-%s-", id)));
            f.deleteOnExit();
            assertFalse(sstable.descriptor.fileFor(c).exists());
        }
    }

    private SSTableReader getNewSSTable(ColumnFamilyStore cfs)
    {
        return getNewSSTable(cfs, 100, 2);
    }

    private SSTableReader getNewSSTable(ColumnFamilyStore cfs, int numKeys, int step)
    {
        Set<SSTableReader> before = cfs.getLiveSSTables();
        for (int j = 0; j < numKeys; j += step)
        {
            new RowUpdateBuilder(cfs.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        cfs.forceBlockingFlush(UNIT_TESTS);
        return Sets.difference(cfs.getLiveSSTables(), before).iterator().next();
    }

    @Test
    public void testGetApproximateKeyCount() throws InterruptedException
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD);
        getNewSSTable(store);

        try (ColumnFamilyStore.RefViewFragment viewFragment1 = store.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            store.discardSSTables(System.currentTimeMillis());

            TimeUnit.MILLISECONDS.sleep(1000); //Giving enough time to clear files.
            List<SSTableReader> sstables = new ArrayList<>(viewFragment1.sstables);
            assertEquals(50, SSTableReader.getApproximateKeyCount(sstables));
        }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testVerifyCompressionInfoExistenceThrows()
    {
        Descriptor desc = setUpForTestVerfiyCompressionInfoExistence();

        // delete the compression info, so it is corrupted.
        File compressionInfoFile = desc.fileFor(Component.COMPRESSION_INFO);
        compressionInfoFile.tryDelete();
        assertFalse("CompressionInfo file should not exist", compressionInfoFile.exists());

        // discovert the components on disk after deletion
        Set<Component> components = SSTable.discoverComponentsFor(desc);

        expectedException.expect(CorruptSSTableException.class);
        expectedException.expectMessage("CompressionInfo.db");
        SSTableReader.verifyCompressionInfoExistenceIfApplicable(desc, components);
    }

    @Test
    public void testVerifyCompressionInfoExistenceWhenTOCUnableToOpen()
    {
        Descriptor desc = setUpForTestVerfiyCompressionInfoExistence();
        Set<Component> components = SSTable.discoverComponentsFor(desc);
        SSTableReader.verifyCompressionInfoExistenceIfApplicable(desc, components);

        // mark the toc file not readable in order to trigger the FSReadError
        File tocFile = desc.fileFor(Component.TOC);
        tocFile.trySetReadable(false);

        expectedException.expect(FSReadError.class);
        expectedException.expectMessage("TOC.txt");
        SSTableReader.verifyCompressionInfoExistenceIfApplicable(desc, components);
    }

    @Test
    public void testVerifyCompressionInfoExistencePasses()
    {
        Descriptor desc = setUpForTestVerfiyCompressionInfoExistence();
        Set<Component> components = SSTable.discoverComponentsFor(desc);
        SSTableReader.verifyCompressionInfoExistenceIfApplicable(desc, components);
    }

    @Test
    public void testBloomFilterIsCreatedOnLoad() throws IOException
    {
        BloomFilter.recreateOnFPChanceChange = true;

        final int numKeys = 100;
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD_NO_BLOOM_FILTER);

        SSTableReader sstable = getNewSSTable(cfs, numKeys, 1);
        Assert.assertTrue(sstable.getBloomFilterSerializedSize() == 0);
        Assert.assertSame(FilterFactory.AlwaysPresent, sstable.getBloomFilter());

        // should do nothing
        checkSSTableOpenedWithGivenFPChance(sstable, 1, false, numKeys, false);

        // should create BF because the FP has changed
        checkSSTableOpenedWithGivenFPChance(sstable, BloomCalculations.minSupportedBloomFilterFpChance(), true, numKeys, true);
        checkSSTableOpenedWithGivenFPChance(sstable, 0.05, true, numKeys, true);
        checkSSTableOpenedWithGivenFPChance(sstable, 0.1, true, numKeys, true);

        // should deserialize the existing BF
        checkSSTableOpenedWithGivenFPChance(sstable, 0.1, true, numKeys, false);
        // should create BF because the FP has changed
        checkSSTableOpenedWithGivenFPChance(sstable, 1 - BloomFilter.fpChanceTolerance, true, numKeys, true);
        // should install empty filter without changing file or metadata
        checkSSTableOpenedWithGivenFPChance(sstable, 1, false, numKeys, false);

        // corrupted bf file should fail to deserialize and we should fall back to recreating it
        Files.write(sstable.descriptor.fileFor(Component.FILTER).toPath(), new byte[] { 0, 0, 0, 0});
        checkSSTableOpenedWithGivenFPChance(sstable, 1 - BloomFilter.fpChanceTolerance, true, numKeys, true);

        // missing primary index file should make BF fail to load and we should install the empty one
        if (sstable.descriptor.getFormat().getType() == SSTableFormat.Type.BIG)
            sstable.descriptor.fileFor(Component.PRIMARY_INDEX).delete();
        else
            sstable.descriptor.fileFor(Component.PARTITION_INDEX).delete();

        checkSSTableOpenedWithGivenFPChance(sstable, 0.05, false, numKeys, false);
    }

    @Test
    public void testSSTableFlushBloomFilterReachedLimit() throws Exception
    {
        final int numKeys = 100; // will use about 128 bytes
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);

        SSTableReader sstable;
        long bfSpace = BloomFilter.memoryLimiter.maxMemory -  BloomFilter.memoryLimiter.memoryAllocated() - 100;
        try
        {
            BloomFilter.memoryLimiter.increment(bfSpace);
            sstable = getNewSSTable(cfs, numKeys, 1);
            Assert.assertFalse(PathUtils.exists(sstable.descriptor.pathFor(Component.FILTER)));
            Assert.assertSame(FilterFactory.AlwaysPresent, sstable.getBloomFilter());
        }
        finally
        {
            // reset
            BloomFilter.memoryLimiter.decrement(bfSpace);
        }
    }

    private void checkSSTableOpenedWithGivenFPChance(SSTableReader sstable, double fpChance, boolean bfShouldExist, int numKeys, boolean expectRecreated) throws IOException
    {
        Descriptor desc = sstable.descriptor;
        TableMetadata metadata = sstable.metadata.get().unbuild().bloomFilterFpChance(fpChance).build();
        ValidationMetadata prevValidationMetadata = getValidationMetadata(desc);
        Assert.assertNotNull(prevValidationMetadata);
        File bfFile = desc.fileFor(Component.FILTER);

        SSTableReader target = null;
        try
        {
            FileTime bf0Time = bfFile.exists() ? Files.getLastModifiedTime(bfFile.toPath()) : FileTime.from(Instant.MIN);

            // make sure we wait enough - some JDK implementations use seconds granularity and we need to wait a bit to actually see the change
            Uninterruptibles.sleepUninterruptibly(1, Util.supportedMTimeGranularity);

            target = desc.getFormat().getReaderFactory().open(desc,
                                                              SSTableReader.discoverComponentsFor(desc),
                                                              TableMetadataRef.forOfflineTools(metadata),
                                                              false,
                                                              false);
            IFilter bloomFilter = target.getBloomFilter();
            ValidationMetadata validationMetadata = getValidationMetadata(desc);
            Assert.assertNotNull(validationMetadata);
            FileTime bf1Time = bfFile.exists() ? Files.getLastModifiedTime(bfFile.toPath()) : FileTime.from(Instant.MIN);

            if (expectRecreated)
            {
                Assert.assertTrue(bf0Time.compareTo(bf1Time) < 0);
            }
            else
            {
                assertEquals(bf0Time, bf1Time);
            }

            if (bfShouldExist)
            {
                Assert.assertNotEquals(FilterFactory.AlwaysPresent, bloomFilter);
                Assert.assertTrue(bloomFilter.serializedSize() > 0);
                Assert.assertEquals(fpChance, validationMetadata.bloomFilterFPChance, BloomFilter.fpChanceTolerance);
                Assert.assertTrue(bfFile.exists());
                Assert.assertEquals(bloomFilter.serializedSize(), bfFile.length());
            }
            else
            {
                Assert.assertEquals(FilterFactory.AlwaysPresent, sstable.getBloomFilter());
                Assert.assertTrue(sstable.getBloomFilterSerializedSize() == 0);
                Assert.assertEquals(prevValidationMetadata.bloomFilterFPChance, validationMetadata.bloomFilterFPChance, BloomFilter.fpChanceTolerance);
                Assert.assertEquals(bfFile.exists(), bfFile.exists());
            }

            // verify all keys are present according to the BF
            Token token = new Murmur3Partitioner.LongToken(0L);
            for (int i = 0; i < numKeys; i++)
            {
                DecoratedKey key = new BufferDecoratedKey(token, ByteBufferUtil.bytes(String.valueOf(i)));
                Assert.assertTrue("Expected key to be in BF: " + i, bloomFilter.isPresent(key));
            }
        }
        finally
        {
            if (target != null)
                target.selfRef().release();
        }
    }

    @Test
    public void testOnDiskComponentsSize()
    {
        final int numKeys = 1000;
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);

        SSTableReader sstable = getNewSSTable(cfs, numKeys, 1);
        assertEquals(sstable.onDiskLength(), FileUtils.size(sstable.descriptor.pathFor(Component.DATA)));

        assertTrue(sstable.components().contains(Component.DATA));
        assertTrue(sstable.components().size() > 1);
        assertTrue(sstable.onDiskComponentsSize() > sstable.onDiskLength());
    }

    private static ValidationMetadata getValidationMetadata(Descriptor descriptor)
    {
        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION);

        Map<MetadataType, MetadataComponent> sstableMetadata;
        try
        {
            sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
        }
        catch (Throwable t)
        {
            throw new CorruptSSTableException(t, descriptor.fileFor(Component.STATS));
        }

        return (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
    }

    private Descriptor setUpForTestVerfiyCompressionInfoExistence()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_COMPRESSED);
        SSTableReader sstable = getNewSSTable(cfs);
        cfs.clearUnsafe();
        Descriptor desc = sstable.descriptor;

        File compressionInfoFile = desc.fileFor(Component.COMPRESSION_INFO);
        File tocFile = desc.fileFor(Component.TOC);
        assertTrue("CompressionInfo file should exist", compressionInfoFile.exists());
        assertTrue("TOC file should exist", tocFile.exists());
        return desc;
    }

    private ColumnFamilyStore discardSSTables(String ks, String cf)
    {
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
        cfs.discardSSTables(System.currentTimeMillis());
        return cfs;
    }

    private <T extends SelfRefCounted<T>> T trackReleaseableRef(Supplier<T> refSupplier)
    {
        T ref = refSupplier.get();
        refsToRelease.add(ref.selfRef());
        return ref;
    }
}
