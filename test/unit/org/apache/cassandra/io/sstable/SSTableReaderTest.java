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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.utils.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
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
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.IndexSummaryComponent;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport;
import org.apache.cassandra.io.sstable.keycache.KeyCache;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedRegions;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;
import org.mockito.Mockito;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.BF_FP_CHANCE_TOLERANCE;
import static org.apache.cassandra.config.CassandraRelevantProperties.BF_RECREATE_ON_FP_CHANCE_CHANGE;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

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
        return partitioner.getToken(keyFor(i));
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
        BF_RECREATE_ON_FP_CHANCE_CHANGE.setBoolean(false);

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
        Util.flush(store);
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
            Util.flush(store);
            CompactionManager.instance.performMaximal(store, false);

            // check that all our keys are found correctly
            SSTableReader sstable = store.getLiveSSTables().iterator().next();
            for (int j = 0; j < 10000; j += 2)
            {
                DecoratedKey dk = Util.dk(String.valueOf(j));
                FileDataInput file = sstable.getFileDataInput(sstable.getPosition(dk, SSTableReader.Operator.EQ));
                DecoratedKey keyInDisk = sstable.decorateKey(ByteBufferUtil.readWithShortLength(file));
                assert keyInDisk.equals(dk) : format("%s != %s in %s", keyInDisk, dk, file.getFile());
            }

            // check no false positives
            for (int j = 1; j < 11000; j += 2)
            {
                DecoratedKey dk = Util.dk(String.valueOf(j));
                assert sstable.getPosition(dk, SSTableReader.Operator.EQ) < 0;
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
        Util.flush(store);

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

        Util.flush(store);

        boolean startState = DatabaseDescriptor.getSStableReadRatePersistenceEnabled();
        try
        {
            DatabaseDescriptor.setSStableReadRatePersistenceEnabled(true);

            SSTableReader sstable = store.getLiveSSTables().iterator().next();
            assertEquals(0, sstable.getReadMeter().count());

            DecoratedKey key = sstable.decorateKey(ByteBufferUtil.bytes("4"));
            Util.getAll(Util.cmd(store, key).build());
            assertEquals(1, sstable.getReadMeter().count());

            Util.getAll(Util.cmd(store, key).includeRow("0").build());
            assertEquals(2, sstable.getReadMeter().count());

            // With persistence enabled, we should be able to retrieve the state of the meter.
            sstable.maybePersistSSTableReadMeter();

            UntypedResultSet meter = SystemKeyspace.readSSTableActivity(store.getKeyspaceName(), store.name, sstable.descriptor.id);
            assertFalse(meter.isEmpty());

            Util.getAll(Util.cmd(store, key).includeRow("0").build());
            assertEquals(3, sstable.getReadMeter().count());

            // After cleaning existing state and disabling persistence, there should be no meter state to read.
            SystemKeyspace.clearSSTableReadMeter(store.getKeyspaceName(), store.name, sstable.descriptor.id);
            DatabaseDescriptor.setSStableReadRatePersistenceEnabled(false);
            sstable.maybePersistSSTableReadMeter();
            meter = SystemKeyspace.readSSTableActivity(store.getKeyspaceName(), store.name, sstable.descriptor.id);
            assertTrue(meter.isEmpty());
        }
        finally
        {
            DatabaseDescriptor.setSStableReadRatePersistenceEnabled(startState);
        }
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
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        long p2 = sstable.getPosition(dk(2), SSTableReader.Operator.EQ);
        long p3 = sstable.getPosition(dk(3), SSTableReader.Operator.EQ);
        long p6 = sstable.getPosition(dk(6), SSTableReader.Operator.EQ);
        long p7 = sstable.getPosition(dk(7), SSTableReader.Operator.EQ);

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

        Util.flush(store);

        // check if opening and querying works
        assertIndexQueryWorks(store);
    }

    @Test
    public void testGetPositionsKeyCacheStats()
    {
        Assume.assumeTrue(KeyCacheSupport.isSupportedBy(DatabaseDescriptor.getSelectedSSTableFormat()));
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
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        KeyCache keyCache = ((KeyCacheSupport<?>) sstable).getKeyCache();
        assumeTrue(keyCache.isEnabled());
        // existing, non-cached key
        sstable.getPosition(dk(2), SSTableReader.Operator.EQ);
        assertEquals(1, keyCache.getRequests());
        assertEquals(0, keyCache.getHits());
        // existing, cached key
        assertEquals(1, store.getBloomFilterTracker().getTruePositiveCount());
        sstable.getPosition(dk(2), SSTableReader.Operator.EQ);
        assertEquals(2, keyCache.getRequests());
        assertEquals(1, keyCache.getHits());
        // non-existing key (it is specifically chosen to not be rejected by Bloom Filter check)
        sstable.getPosition(dk(14), SSTableReader.Operator.EQ);
        assertEquals(3, keyCache.getRequests());
        assertEquals(2, store.getBloomFilterTracker().getTruePositiveCount());
        sstable.getPosition(dk(15), SSTableReader.Operator.EQ);
        assertEquals(1, keyCache.getHits());
    }

    @Test
    public void testGetPositionsBloomFilterStats()
    {
        SSTableReaderWithFilter sstable = prepareGetPositions();

        // the keys are specifically chosen to cover certain use cases
        // existing key is read from index
        sstable.getPosition(dk(2), SSTableReader.Operator.EQ);
        assertEquals(1, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(0, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getFilterTracker().getFalsePositiveCount());

        // existing key is read from Cache Key (if used)
        sstable.getPosition(dk(7), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(0, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getFilterTracker().getFalsePositiveCount());

        // non-existing key is rejected by Bloom Filter check
        sstable.getPosition(dk(45), SSTableReader.Operator.EQ);    // note: 45 falls between 4 and 5
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getFilterTracker().getFalsePositiveCount());

        // GT should not affect bloom filter counts
        sstable.getPosition(dk(56), SSTableReader.Operator.GE);    // note: 56 falls between 5 and 6
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getFilterTracker().getFalsePositiveCount());
        sstable.getPosition(dk(57), SSTableReader.Operator.GT);    // note: 57 falls between 5 and 6
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(0, sstable.getFilterTracker().getFalsePositiveCount());

        // non-existing key is rejected by sstable keys range check, if performed, otherwise it's a false positive
        sstable.getPosition(collisionFor(9), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        long fpCount = sstable.getFilterTracker().getFalsePositiveCount();

        // existing key filtered out by sstable keys range check, performed because of moved start
        sstable.getPosition(dk(1), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(fpCount, sstable.getFilterTracker().getFalsePositiveCount());
        fpCount = sstable.getFilterTracker().getFalsePositiveCount();

        // non-existing key is rejected by index interval check
        sstable.getPosition(collisionFor(5), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(fpCount + 1, sstable.getFilterTracker().getFalsePositiveCount());

        // non-existing key is rejected by index lookup check
        sstable.getPosition(dk(807), SSTableReader.Operator.EQ);
        assertEquals(2, sstable.getFilterTracker().getTruePositiveCount());
        assertEquals(1, sstable.getFilterTracker().getTrueNegativeCount());
        assertEquals(fpCount + 2, sstable.getFilterTracker().getFalsePositiveCount());
    }


    @Test
    public void testGetPositionsListenerCalls()
    {
        SSTableReaderWithFilter sstable = prepareGetPositions();

        SSTableReadsListener listener = Mockito.mock(SSTableReadsListener.class);
        // the keys are specifically chosen to cover certain use cases
        // existing key is read from index
        sstable.getPosition(dk(7), SSTableReader.Operator.EQ, listener);
        Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        Mockito.reset(listener);

        // existing key is read from Cache Key (if used)
        // Note: key cache may fail to cache the partition if it is wide.
        sstable.getPosition(dk(7), SSTableReader.Operator.EQ, listener);
        if (sstable instanceof BigTableReader)
            Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.KEY_CACHE_HIT);
        else
            Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        Mockito.reset(listener);

        // As above with other ops
        sstable.getPosition(dk(7), SSTableReader.Operator.GT, listener);    // GT does not engage key cache
        Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        Mockito.reset(listener);

        sstable.getPosition(dk(7), SSTableReader.Operator.GE, listener);    // GE does
        if (sstable instanceof BigTableReader)
            Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.KEY_CACHE_HIT);
        else
            Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        Mockito.reset(listener);

        // non-existing key is rejected by Bloom Filter check
        sstable.getPosition(dk(45), SSTableReader.Operator.EQ, listener);    // note: 45 falls between 4 and 5
        Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.BLOOM_FILTER);
        Mockito.reset(listener);

        // non-existing key is rejected by sstable keys range check, if performed, otherwise it's a false positive
        sstable.getPosition(collisionFor(9), SSTableReader.Operator.EQ, listener);
        if (sstable instanceof BigTableReader)
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.MIN_MAX_KEYS);
        else
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.INDEX_ENTRY_NOT_FOUND);
        Mockito.reset(listener);

        sstable.getPosition(collisionFor(9), SSTableReader.Operator.GE, listener);
        if (sstable instanceof BigTableReader)
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.MIN_MAX_KEYS);
        else
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.INDEX_ENTRY_NOT_FOUND);
        Mockito.reset(listener);

        sstable.getPosition(dk(9), SSTableReader.Operator.GT, listener);
        if (sstable instanceof BigTableReader)
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.MIN_MAX_KEYS);
        else
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.INDEX_ENTRY_NOT_FOUND);
        Mockito.reset(listener);

        // existing key filtered out by sstable keys range check, performed because of moved start
        sstable.getPosition(dk(1), SSTableReader.Operator.EQ, listener);
        Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.MIN_MAX_KEYS);
        Mockito.reset(listener);
        long pos = sstable.getPosition(dk(1), SSTableReader.Operator.GT, listener);
        Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        assertEquals(sstable.getPosition(dk(3), SSTableReader.Operator.EQ), pos);
        Mockito.reset(listener);

        // non-existing key is rejected by index interval check
        sstable.getPosition(collisionFor(5), SSTableReader.Operator.EQ, listener);
        if (sstable instanceof BigTableReader)
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.PARTITION_INDEX_LOOKUP);
        else
            Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.INDEX_ENTRY_NOT_FOUND);
        Mockito.reset(listener);

        // non-existing key is rejected by index lookup
        sstable.getPosition(dk(807), SSTableReader.Operator.EQ, listener);
        Mockito.verify(listener).onSSTableSkipped(sstable, SSTableReadsListener.SkippingReason.PARTITION_INDEX_LOOKUP);
        Mockito.reset(listener);

        // Variations of non-equal match
        sstable.getPosition(dk(31), SSTableReader.Operator.GE, listener);
        Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        Mockito.reset(listener);

        sstable.getPosition(dk(81), SSTableReader.Operator.GE, listener);
        Mockito.verify(listener).onSSTableSelected(sstable, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
        Mockito.reset(listener);
    }

    private SSTableReaderWithFilter prepareGetPositions()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD_SMALL_BLOOM_FILTER);
        store.truncateBlocking();
        partitioner = store.getPartitioner();
        CacheService.instance.keyCache.setCapacity(1000);

        // insert data and compact to a single sstable
        for (int j = 0; j < 10; j++)
        {
            if (j == 8) // leave a missing prefix
                continue;

            int rowCount = j < 5 ? 2000 : 1;    // make some of the partitions wide
            for (int r = 0; r < rowCount; ++r)
            {
                new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                .clustering(Integer.toString(r))
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
            }
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        return (SSTableReaderWithFilter) trackReleaseableRef(() -> {
            SSTableReader reader = store.getLiveSSTables().iterator().next();
            return reader.cloneWithNewStart(dk(3));
        });
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
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        assertTrue(SSTableReader.hasGlobalReference(sstable.descriptor));
        Descriptor desc = sstable.descriptor;

        // test to see if sstable can be opened as expected
        SSTableReader target = SSTableReader.open(store, desc);
        try
        {
            assert target.getFirst().equals(firstKey);
            assert target.getLast().equals(lastKey);
        }
        finally
        {
            target.selfRef().close();
        }

        if (BigFormat.isSelected())
            checkOpenedBigTable(ks, cf, store, desc);
        else if (BtiFormat.isSelected())
            checkOpenedBtiTable(ks, cf, store, desc);
        else
            throw Util.testMustBeImplementedForSSTableFormat();
    }

    private static void checkOpenedBigTable(String ks, String cf, ColumnFamilyStore store, Descriptor desc) throws Exception
    {
        executeInternal(format("ALTER TABLE \"%s\".\"%s\" WITH bloom_filter_fp_chance = 0.3", ks, cf));

        File bloomFile = desc.fileFor(Components.FILTER);
        long bloomModified = bloomFile.lastModified();

        File summaryFile = desc.fileFor(Components.SUMMARY);
        long summaryModified = summaryFile.lastModified();

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different

        // Offline tests
        // check that bloomfilter/summary ARE NOT regenerated
        SSTableReader target = SSTableReader.openNoValidation(store, desc, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that bloomfilter/summary ARE NOT regenerated and BF=AlwaysPresent when filter component is missing
        Set<Component> components = desc.discoverComponents();
        components.remove(Components.FILTER);
        target = SSTableReader.openNoValidation(desc, components, store);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
            assertEquals(0, ((SSTableReaderWithFilter) target).getFilterOffHeapSize());
        }
        finally
        {
            target.selfRef().close();
        }

        // #### online tests ####
        // check that summary & bloomfilter are not regenerated when SSTable is opened and BFFP has been changed
        target = SSTableReader.open(store, desc, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that bloomfilter is recreated when it doesn't exist and this causes the summary to be recreated
        components = desc.discoverComponents();
        components.remove(Components.FILTER);
        components.remove(Components.SUMMARY);

        target = SSTableReader.open(store, desc, components, store.metadata);
        try {
            assertTrue("Bloomfilter was not recreated", bloomModified < bloomFile.lastModified());
            assertTrue("Summary was not recreated", summaryModified < summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that only the summary is regenerated when it is deleted
        components.add(Components.FILTER);
        summaryModified = summaryFile.lastModified();
        summaryFile.tryDelete();

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
        bloomModified = bloomFile.lastModified();

        target = SSTableReader.open(store, desc, components, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertTrue("Summary was not recreated", summaryModified < summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that summary and bloomfilter is not recreated when the INDEX is missing
        components.add(Components.SUMMARY);
        components.remove(Components.PRIMARY_INDEX);

        summaryModified = summaryFile.lastModified();
        target = SSTableReader.open(store, desc, components, store.metadata, false, false);
        try
        {
            TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(summaryModified, summaryFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }
    }

    private static void checkOpenedBtiTable(String ks, String cf, ColumnFamilyStore store, Descriptor desc) throws Exception
    {
        executeInternal(format("ALTER TABLE \"%s\".\"%s\" WITH bloom_filter_fp_chance = 0.3", ks, cf));

        File bloomFile = desc.fileFor(Components.FILTER);
        long bloomModified = bloomFile.lastModified();

        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different

        // Offline tests
        // check that bloomfilter is not regenerated
        SSTableReader target = SSTableReader.openNoValidation(store, desc, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that bloomfilter is not regenerated and BF=AlwaysPresent when filter component is missing
        Set<Component> components = desc.discoverComponents();
        components.remove(Components.FILTER);
        target = SSTableReader.openNoValidation(desc, components, store);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
            assertEquals(0, ((SSTableReaderWithFilter) target).getFilterOffHeapSize());
        }
        finally
        {
            target.selfRef().close();
        }

        // #### online tests ####
        // check that bloomfilter is not regenerated when SSTable is opened and BFFP has been changed
        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
        target = SSTableReader.open(store, desc, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that bloomfilter is recreated when it doesn't exist
        components = desc.discoverComponents();
        components.remove(Components.FILTER);

        target = SSTableReader.open(store, desc, components, store.metadata);
        try
        {
            assertTrue("Bloomfilter was not recreated", bloomModified < bloomFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        bloomModified = bloomFile.lastModified();
        TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different

        components.add(Components.FILTER);
        target = SSTableReader.open(store, desc, components, store.metadata);
        try
        {
            assertEquals(bloomModified, bloomFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }

        // check that bloomfilter is not recreated when the INDEX is missing
        components.remove(BtiFormat.Components.PARTITION_INDEX);

        target = SSTableReader.open(store, desc, components, store.metadata, false, false);
        try
        {
            TimeUnit.MILLISECONDS.sleep(1000); // sleep to ensure modified time will be different
            assertEquals(bloomModified, bloomFile.lastModified());
        }
        finally
        {
            target.selfRef().close();
        }
    }

    @Test
    public void testLoadingSummaryUsesCorrectPartitioner() throws Exception
    {
        Assume.assumeTrue(BigFormat.isSelected());
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_INDEXED);

        new RowUpdateBuilder(store.metadata(), System.currentTimeMillis(), "k1")
        .clustering("0")
        .add("birthdate", 1L)
        .build()
        .applyUnsafe();

        Util.flush(store);

        for (ColumnFamilyStore indexCfs : store.indexManager.getAllIndexColumnFamilyStores())
        {
            assert indexCfs.isIndex();
            SSTableReader sstable = indexCfs.getLiveSSTables().iterator().next();
            assert sstable.getFirst().getToken() instanceof LocalToken;

            if (sstable instanceof IndexSummarySupport<?>)
            {
                new IndexSummaryComponent(((IndexSummarySupport<?>) sstable).getIndexSummary(), sstable.getFirst(), sstable.getLast()).save(sstable.descriptor.fileFor(Components.SUMMARY), true);
                SSTableReader reopened = trackReleaseableRef(() -> SSTableReader.open(store, sstable.descriptor));
                assert reopened.getFirst().getToken() instanceof LocalToken;
            }
        }
    }

    /**
     * see CASSANDRA-5407
     */
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

        Util.flush(store);

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
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        // construct a range which is present in the sstable, but whose
        // keys are not found in the first segment of the index.
        List<Range<Token>> ranges = new ArrayList<Range<Token>>();
        ranges.add(new Range<Token>(t(98), t(99)));

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(ranges);
        assert sections.size() == 1 : "Expected to find range in sstable" ;

        // re-open the same sstable as it would be during bulk loading
        Set<Component> components = Sets.newHashSet(sstable.descriptor.getFormat().primaryComponents());
        if (sstable.components().contains(Components.COMPRESSION_INFO))
            components.add(Components.COMPRESSION_INFO);
        SSTableReader bulkLoaded = SSTableReader.openForBatch(store, sstable.descriptor, components, store.metadata);
        sections = bulkLoaded.getPositionsForRanges(ranges);
        assert sections.size() == 1 : "Expected to find range in sstable opened for bulk loading";
        bulkLoaded.selfRef().release();
    }

    @Test
    public void testIndexSummaryReplacement() throws IOException, ExecutionException, InterruptedException
    {
        assumeTrue(IndexSummarySupport.isSupportedBy(DatabaseDescriptor.getSelectedSSTableFormat()));
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD_LOW_INDEX_INTERVAL); // index interval of 8, no key caching

        final int NUM_PARTITIONS = 512;
        for (int j = 0; j < NUM_PARTITIONS; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, format("%3d", j))
            .clustering("0")
            .add("val", format("%3d", j))
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        Collection<SSTableReader> sstables = store.getLiveSSTables();
        assert sstables.size() == 1;
        final SSTableReader sstable = sstables.iterator().next();

        ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);
        List<Future<?>> futures = new ArrayList<>(NUM_PARTITIONS * 2);
        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            final ByteBuffer key = ByteBufferUtil.bytes(format("%3d", i));
            final int index = i;

            futures.add(executor.submit(new Runnable()
            {
                public void run()
                {
                    Row row = Util.getOnlyRowUnfiltered(Util.cmd(store, key).build());
                    assertEquals(0, ByteBufferUtil.compare(format("%3d", index).getBytes(), row.cells().iterator().next().buffer()));
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
            replacement = ((IndexSummarySupport<?>) sstable).cloneWithNewSummarySamplingLevel(store, 1);
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
        assumeTrue(IndexSummarySupport.isSupportedBy(DatabaseDescriptor.getSelectedSSTableFormat()));
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

    private <R extends SSTableReader & IndexSummarySupport<R>> void testIndexSummaryUpsampleAndReload0() throws Exception
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD_LOW_INDEX_INTERVAL); // index interval of 8, no key caching

        final int NUM_PARTITIONS = 512;
        for (int j = 0; j < NUM_PARTITIONS; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, format("%3d", j))
            .clustering("0")
            .add("val", format("%3d", j))
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        Collection<R> sstables = ServerTestUtils.<R>getLiveIndexSummarySupportingReaders(store);
        assert sstables.size() == 1;
        final R sstable = sstables.iterator().next();

        try (LifecycleTransaction txn = store.getTracker().tryModify(Collections.singletonList(sstable), OperationType.UNKNOWN))
        {
            SSTableReader replacement = sstable.cloneWithNewSummarySamplingLevel(store, sstable.getIndexSummary().getSamplingLevel() + 1);
            txn.update(replacement, true);
            txn.finish();
        }
        R reopen = (R) trackReleaseableRef(() -> SSTableReader.open(store, sstable.descriptor));
        assert reopen.getIndexSummary().getSamplingLevel() == sstable.getIndexSummary().getSamplingLevel() + 1;
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
        Index.Searcher searcher = rc.indexSearcher();
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

    private DecoratedKey dk(int i)
    {
        return partitioner.decorateKey(keyFor(i));
    }

    private static ByteBuffer keyFor(int i)
    {
        return ByteBufferUtil.bytes(String.valueOf(i));
    }

    private DecoratedKey collisionFor(int i)
    {
        return partitioner.decorateKey(Util.generateMurmurCollision(ByteBufferUtil.bytes(String.valueOf(i))));
    }

    @Test(expected = RuntimeException.class)
    public void testMoveAndOpenLiveSSTable()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);
        SSTableReader sstable = getNewSSTable(cfs);
        Descriptor notLiveDesc = new Descriptor(new File("/testdir"), "", "", SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        SSTableReader.moveAndOpenSSTable(cfs, sstable.descriptor, notLiveDesc, sstable.components(), false);
    }

    @Test(expected = RuntimeException.class)
    public void testMoveAndOpenLiveSSTable2()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);
        SSTableReader sstable = getNewSSTable(cfs);
        Descriptor notLiveDesc = new Descriptor(new File("/testdir"), "", "", SSTableIdFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        SSTableReader.moveAndOpenSSTable(cfs, notLiveDesc, sstable.descriptor, sstable.components(), false);
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
        trackReleaseableRef(() -> SSTableReader.moveAndOpenSSTable(cfs, sstable.descriptor, notLiveDesc, sstable.components(), false));
        // make sure the files were moved:
        for (Component c : sstable.components())
        {
            File f = notLiveDesc.fileFor(c);
            assertTrue(f.exists());
            assertTrue(f.toString().contains(format("-%s-", id)));
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
        Util.flush(cfs);
        return Sets.difference(cfs.getLiveSSTables(), before).iterator().next();
    }

    @Test
    public void testGetApproximateKeyCount() throws Exception
    {
        ColumnFamilyStore store = discardSSTables(KEYSPACE1, CF_STANDARD);
        getNewSSTable(store);

        try (ColumnFamilyStore.RefViewFragment viewFragment1 = store.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            store.discardSSTables(System.currentTimeMillis());

            TimeUnit.MILLISECONDS.sleep(1000); //Giving enough time to clear files.
            List<SSTableReader> sstables = new ArrayList<>(viewFragment1.sstables);
            assertEquals(50, SSTableReader.getApproximateKeyCount(sstables));

            Field compactionMetadataField = ReflectionUtils.getField(SSTable.class, "compactionMetadata");
            for (SSTableReader sstable : sstables)
                compactionMetadataField.set(sstable, null);

            // simulate the case in which CompactionMetadata is not loaded yet
            for (SSTableReader sstable : sstables)
                assertNull(compactionMetadataField.get(sstable));

            assertEquals(50, SSTableReader.getApproximateKeyCount(sstables));

            // verify that CompactionMetadata has been loaded from storage
            for (SSTableReader sstable : sstables)
            {
                Optional<CompactionMetadata> compactionMetadata = (Optional<CompactionMetadata>) compactionMetadataField.get(sstable);
                assertNotNull(compactionMetadata);
                assertTrue(compactionMetadata.isPresent());
            }

            // simulate the case in which CompactionMetadata is not present
            for (SSTableReader sstable : sstables)
                compactionMetadataField.set(sstable, Optional.empty());

            // in this case the cardinality is not available, so we fallback to the index summary
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
        File compressionInfoFile = desc.fileFor(Components.COMPRESSION_INFO);
        compressionInfoFile.tryDelete();
        assertFalse("CompressionInfo file should not exist", compressionInfoFile.exists());

        // discovert the components on disk after deletion
        Set<Component> components = desc.discoverComponents();

        expectedException.expect(CorruptSSTableException.class);
        expectedException.expectMessage("CompressionInfo.db");
        CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(desc, components);
    }

    @Test
    public void testVerifyCompressionInfoExistenceWhenTOCUnableToOpen()
    {
        Descriptor desc = setUpForTestVerfiyCompressionInfoExistence();
        Set<Component> components = desc.discoverComponents();
        CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(desc, components);

        // mark the toc file not readable in order to trigger the FSReadError
        File tocFile = desc.fileFor(Components.TOC);
        tocFile.trySetReadable(false);

        expectedException.expect(FSReadError.class);
        expectedException.expectMessage("TOC.txt");
        CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(desc, components);
    }

    @Test
    public void testVerifyCompressionInfoExistencePasses()
    {
        Descriptor desc = setUpForTestVerfiyCompressionInfoExistence();
        Set<Component> components = desc.discoverComponents();
        CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(desc, components);
    }

    @Test
    public void testBloomFilterIsCreatedOnLoad() throws IOException
    {
        BF_RECREATE_ON_FP_CHANCE_CHANGE.setBoolean(true);

        final int numKeys = 100;
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD_NO_BLOOM_FILTER);

        SSTableReader sstable = getNewSSTable(cfs, numKeys, 1);
        Assert.assertTrue(getFilterSize(sstable) == 0);
        Assert.assertSame(FilterFactory.AlwaysPresent, getFilter(sstable));

        // should do nothing
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 1, false, numKeys, false);

        // should create BF because the FP has changed
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, BloomCalculations.minSupportedBloomFilterFpChance(), true, numKeys, true);
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 0.05, true, numKeys, true);
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 0.1, true, numKeys, true);

        // should deserialize the existing BF
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 0.1, true, numKeys, false);
        // should create BF because the FP has changed
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 1 - BF_FP_CHANCE_TOLERANCE.getDouble(), true, numKeys, true);
        // should install empty filter without changing file or metadata
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 1, false, numKeys, false);

        // corrupted bf file should fail to deserialize and we should fall back to recreating it
        Files.write(sstable.descriptor.fileFor(Components.FILTER).toPath(), new byte[] { 0, 0, 0, 0});
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 1 - BF_FP_CHANCE_TOLERANCE.getDouble(), true, numKeys, true);

        // missing primary index file should make BF fail to load and we should install the empty one
        HashSet<Component> nonDataPrimaryComponents = new HashSet<>(sstable.descriptor.getFormat().primaryComponents());
        nonDataPrimaryComponents.remove(Components.DATA);
        nonDataPrimaryComponents.remove(Components.COMPRESSION_INFO);
        nonDataPrimaryComponents.remove(Components.STATS);
        for (Component component : nonDataPrimaryComponents)
            sstable.descriptor.fileFor(component).delete();
        checkSSTableOpenedWithGivenFPChance(cfs, sstable, 0.05, false, numKeys, false);
    }

    @Test
    public void testOnDiskComponentsSize()
    {
        final int numKeys = 1000;
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD);

        SSTableReader sstable = getNewSSTable(cfs, numKeys, 1);
        assertEquals(sstable.onDiskLength(), FileUtils.size(sstable.descriptor.pathFor(Components.DATA)));

        assertTrue(sstable.components().contains(Components.DATA));
        assertTrue(sstable.components().size() > 1);
        assertTrue(sstable.onDiskComponentsSize() > sstable.onDiskLength());
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
            Assert.assertFalse(PathUtils.exists(sstable.descriptor.pathFor(Components.FILTER)));
            Assert.assertSame(FilterFactory.AlwaysPresent, getFilter(sstable));
        }
        finally
        {
            // reset
            BloomFilter.memoryLimiter.decrement(bfSpace);
        }
    }

    private void checkSSTableOpenedWithGivenFPChance(ColumnFamilyStore cfs, SSTableReader sstable, double fpChance, boolean bfShouldExist, int numKeys, boolean expectRecreated) throws IOException
    {
        Descriptor desc = sstable.descriptor;
        TableMetadata metadata = sstable.metadata.get().unbuild().bloomFilterFpChance(fpChance).build();
        ValidationMetadata prevValidationMetadata = getValidationMetadata(desc);
        Assert.assertNotNull(prevValidationMetadata);
        File bfFile = desc.fileFor(Components.FILTER);

        SSTableReader target = null;
        try
        {
            FileTime bf0Time = bfFile.exists() ? Files.getLastModifiedTime(bfFile.toPath()) : FileTime.from(Instant.MIN);

            // make sure we wait enough - some JDK implementations use seconds granularity and we need to wait a bit to actually see the change
            Uninterruptibles.sleepUninterruptibly(1, Util.supportedMTimeGranularity);

            target = SSTableReader.open(cfs,
                                        desc,
                                        TOCComponent.loadTOC(desc),
                                        TableMetadataRef.forOfflineTools(metadata),
                                        false,
                                        false);
            IFilter bloomFilter = getFilter(target);
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
                Assert.assertTrue(bloomFilter.serializedSize(false) > 0);
                Assert.assertEquals(fpChance, validationMetadata.bloomFilterFPChance, BF_FP_CHANCE_TOLERANCE.getDouble());
                Assert.assertTrue(bfFile.exists());
                Assert.assertEquals(bloomFilter.serializedSize(false), bfFile.length());
            }
            else
            {
                Assert.assertEquals(FilterFactory.AlwaysPresent, getFilter(sstable));
                Assert.assertTrue(getFilterSize(sstable) == 0);
                Assert.assertEquals(prevValidationMetadata.bloomFilterFPChance, validationMetadata.bloomFilterFPChance, BF_FP_CHANCE_TOLERANCE.getDouble());
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

    static long getFilterSize(SSTableReader rdr)
    {
        return ((SSTableReaderWithFilter) rdr).getFilterSerializedSize();
    }

    static IFilter getFilter(SSTableReader rdr)
    {
        return ((SSTableReaderWithFilter) rdr).getFilter();
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
            throw new CorruptSSTableException(t, descriptor.fileFor(Components.STATS));
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

        File compressionInfoFile = desc.fileFor(Components.COMPRESSION_INFO);
        File tocFile = desc.fileFor(Components.TOC);
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
