/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.openhft.chronicle.core.util.ThrowingBiConsumer;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.SchemaLoader.compressionParams;
import static org.apache.cassandra.SchemaLoader.counterCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.getCompressionParameters;
import static org.apache.cassandra.SchemaLoader.loadSchema;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class ScrubTest
{
    public static final String INVALID_LEGACY_SSTABLE_ROOT_PROP = "invalid-legacy-sstable-root";

    public static final String CF = "Standard1";
    public static final String COMPRESSED_CF = "compressed_table";
    public static final String COUNTER_CF = "Counter1";
    public static final String CF_UUID = "UUIDKeys";
    public static final String CF_INDEX1 = "Indexed1";
    public static final String CF_INDEX2 = "Indexed2";
    public static final String CF_INDEX1_BYTEORDERED = "Indexed1_ordered";
    public static final String CF_INDEX2_BYTEORDERED = "Indexed2_ordered";
    public static final String COL_INDEX = "birthdate";
    public static final String COL_NON_INDEX = "notbirthdate";

    public static final Integer COMPRESSION_CHUNK_LENGTH = 4096;

    private static final AtomicInteger seq = new AtomicInteger();

    String ksName;
    Keyspace keyspace;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        loadSchema();
        if (ChunkCache.instance != null)
            ChunkCache.instance.enable(false);
    }

    @Before
    public void setup()
    {
        ksName = "scrub_test_" + seq.incrementAndGet();
        createKeyspace(ksName,
                       KeyspaceParams.simple(1),
                       standardCFMD(ksName, CF),
                       // force this table to use compression
                       standardCFMD(ksName, COMPRESSED_CF).compression(compressionParams(COMPRESSION_CHUNK_LENGTH)),
                       counterCFMD(ksName, COUNTER_CF).compression(getCompressionParameters(COMPRESSION_CHUNK_LENGTH)),
                       standardCFMD(ksName, CF_UUID, 0, UUIDType.instance),
                       SchemaLoader.keysIndexCFMD(ksName, CF_INDEX1, true),
                       SchemaLoader.compositeIndexCFMD(ksName, CF_INDEX2, true),
                       SchemaLoader.keysIndexCFMD(ksName, CF_INDEX1_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance),
                       SchemaLoader.compositeIndexCFMD(ksName, CF_INDEX2_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance));
        keyspace = Keyspace.open(ksName);

        CompactionManager.instance.disableAutoCompaction();
        System.setProperty(org.apache.cassandra.tools.Util.ALLOW_TOOL_REINIT_FOR_TEST, "true"); // Necessary for testing
    }

    @AfterClass
    public static void clearClassEnv()
    {
        System.clearProperty(org.apache.cassandra.tools.Util.ALLOW_TOOL_REINIT_FOR_TEST);
    }

    @Test
    public void testScrubOnePartition() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        CompactionManager.instance.performScrub(cfs, false, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 1);
    }

    @Test
    public void testScrubLastBrokenPartition() throws ExecutionException, InterruptedException, IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ksName, CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
        assertThat(liveSSTables).hasSize(1);
        File file = liveSSTables.iterator().next().getDataFile();
        Files.write(Paths.get(file.path()), new byte[10], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        ChunkCache.instance.invalidateFile(file);

        CompactionManager.instance.performScrub(cfs, true, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 0);
    }

    @Test
    public void testScrubOneBrokenPartition() throws ExecutionException, InterruptedException, IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
        assertThat(liveSSTables).hasSize(1);
        Files.write(liveSSTables.iterator().next().getDataFile().toPath(), new byte[10], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        CompactionManager.instance.performScrub(cfs, true, true, false, 2);

        // check data is still there
        assertOrderedAll(cfs, 0);
    }

    @Test
    public void testScrubOneBrokenPartitionInTheMiddleOfCompressedFile() throws ExecutionException, InterruptedException, IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COMPRESSED_CF);

        // insert data and verify we get it back w/ range query
        int partitions = 5;
        int rowsPerPartition = 2000;
        int total = partitions * rowsPerPartition;
        fillCF(cfs, partitions, rowsPerPartition);
        int outputRows = assertRowsOrdered(Util.cmd(cfs).build());
        assertThat(outputRows).isEqualTo(total);

        Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
        assertThat(liveSSTables).hasSize(1);

        try (FileChannel channel = FileChannel.open(liveSSTables.iterator().next().getDataFile().toPath(), StandardOpenOption.WRITE))
        {
            // corrupt data in the middle
            long middle = channel.size() / 2;
            channel.position(middle);

            byte[] buffer = new byte[50];
            ThreadLocalRandom.current().nextBytes(buffer);
            channel.write(ByteBuffer.wrap(buffer));
        }

        ChunkCache.instance.clear();
        CompactionManager.instance.performScrub(cfs, true, true, false, 1);

        // check data is still there, some corrupted partitions are discarded
        outputRows = assertRowsOrdered(Util.cmd(cfs).build());
        assertThat(outputRows).isGreaterThan(0).isLessThan(total);

        // check digest file do not exist because scruber can't reset the CRC value after resetting file position when corruption is found.
        SSTableReader outputSSTable = cfs.getLiveSSTables().iterator().next();
        assertThat(outputSSTable.descriptor.fileFor(Component.DIGEST).exists()).isTrue();

        try (DataIntegrityMetadata.FileDigestValidator validator = DataIntegrityMetadata.fileDigestValidator(outputSSTable.descriptor))
        {
            validator.validate();
        }
    }

    @Test
    public void testScrubCorruptedCounterPartition() throws IOException, WriteTimeoutException
    {
        // When compression is enabled, for testing corrupted chunks we need enough partitions to cover
        // at least 3 chunks of size COMPRESSION_CHUNK_LENGTH
        int numPartitions = 1000;

        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.truncateBlocking();
        fillCounterCF(cfs, numPartitions);

        assertOrderedAll(cfs, numPartitions);

        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        //make sure to override at most 1 chunk when compression is enabled
        //use 0x00 instead of the usual 0x7A because if by any chance it's able to iterate over the corrupt
        //section, then we get many out-of-order errors, which we don't want
        overrideWithGarbage(sstable, ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("1"), (byte)0x00);

        // with skipCorrupted == false, the scrub is expected to fail
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             Scrubber scrubber = new Scrubber(cfs, txn, false, true))
        {
            scrubber.scrub();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err)
        {
            assertTrue(Throwables.isCausedBy(err, CorruptSSTableException.class));
        }

        // with skipCorrupted == true, the corrupt rows will be skipped
        Scrubber.ScrubResult scrubResult;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             Scrubber scrubber = new Scrubber(cfs, txn, true, true))
        {
            scrubResult = scrubber.scrubWithResult();
        }

        assertNotNull(scrubResult);

        boolean compression = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"));
        assertEquals(0, scrubResult.emptyPartitions);
        if (compression)
        {
            assertEquals(numPartitions, scrubResult.badPartitions + scrubResult.goodPartitions);
            //because we only corrupted 1 chunk and we chose enough partitions to cover at least 3 chunks
            assertTrue(scrubResult.goodPartitions >= scrubResult.badPartitions * 2);
        }
        else
        {
            assertEquals(1, scrubResult.badPartitions);
            assertEquals(numPartitions - 1, scrubResult.goodPartitions);
        }
        assertEquals(1, cfs.getLiveSSTables().size());

        assertOrderedAll(cfs, scrubResult.goodPartitions);
    }

    private File primaryIndexPath(SSTableReader reader)
    {
        if (reader.descriptor.getFormat().getType() == SSTableFormat.Type.BIG)
            return reader.descriptor.fileFor(Component.PRIMARY_INDEX);
        if (reader.descriptor.getFormat().getType() == SSTableFormat.Type.BTI)
            return reader.descriptor.fileFor(Component.PARTITION_INDEX);
        else throw new IllegalArgumentException();
    }

    @Test
    public void testScrubCorruptedRowInSmallFile() throws Throwable
    {
        // overwrite one row with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  overrideWithGarbage(sstable,
                                                      ByteBufferUtil.bytes(keys[0]),
                                                      ByteBufferUtil.bytes(keys[1]),
                                                      (byte) 0x7A),
                                  false,
                                  4);
    }


    @Test
    public void testScrubCorruptedIndex() throws Throwable
    {
        // overwrite a part of the index with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  overrideWithGarbage(primaryIndexPath(sstable),
                                                      5,
                                                      6,
                                                      (byte) 0x7A),
                                  true,
                                  5);
    }

    @Test
    public void testScrubCorruptedIndexOnOpen() throws Throwable
    {
        // overwrite the whole index with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  overrideWithGarbage(primaryIndexPath(sstable),
                                                      0,
                                                      60,
                                                      (byte) 0x7A),
                                  true,
                                  5);
    }

    @Test
    public void testScrubCorruptedRowCorruptedIndex() throws Throwable
    {
        // overwrite one row, and the index with garbage
        testCorruptionInSmallFile((sstable, keys) ->
                                  {
                                      overrideWithGarbage(sstable,
                                                          ByteBufferUtil.bytes(keys[2]),
                                                          ByteBufferUtil.bytes(keys[3]),
                                                          (byte) 0x7A);
                                      overrideWithGarbage(primaryIndexPath(sstable),
                                                          5,
                                                          6,
                                                          (byte) 0x7A);
                                  },
                                  false,
                                  2);   // corrupt after the second partition, no way to resync
    }

    public void testCorruptionInSmallFile(ThrowingBiConsumer<SSTableReader, String[], IOException> corrupt, boolean isFullyRecoverable, int expectedPartitions) throws Throwable
    {
        // cannot test this with compression
        assumeTrue(!Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")));

        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        cfs.clearUnsafe();

        String[] keys = fillCounterCF(cfs, 5);

        assertOrderedAll(cfs, 5);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // overwrite one row with garbage
        corrupt.accept(sstable, keys);

        // with skipCorrupted == false, the scrub is expected to fail
        if (!isFullyRecoverable)
        {
            try (LifecycleTransaction txn = cfs.getTracker().tryModify(Arrays.asList(sstable), OperationType.SCRUB);
                 Scrubber scrubber = new Scrubber(cfs, txn, false, true))
            {
                // with skipCorrupted == true, the corrupt row will be skipped
                scrubber.scrub();
                fail("Expected a CorruptSSTableException to be thrown");
            }
            catch (IOError err)
            {
            }
        }

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(ImmutableList.of(sstable), OperationType.SCRUB);
             Scrubber scrubber = new Scrubber(cfs, txn, true, true))
        {
            // with skipCorrupted == true, the corrupt row will be skipped
            scrubber.scrub();
        }

        assertEquals(1, cfs.getLiveSSTables().size());
        // verify that we can read all of the rows, and there is now the expected number of rows
        assertOrderedAll(cfs, expectedPartitions);
    }

    @Test
    public void testScrubOneRowWithCorruptedKey() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        // cannot test this with compression
        assumeTrue(!Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")));

        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 4);
        assertOrderedAll(cfs, 4);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        overrideWithGarbage(sstable, 0, 2, (byte)0x7A);

        CompactionManager.instance.performScrub(cfs, false, true, 2);

        // check data is still there
        // For Trie format we won't be able to recover the damaged partition key (partion index doesn't store the
        // whole key)
        assertOrderedAll(cfs, SSTableFormat.Type.current() == SSTableFormat.Type.BTI ? 3 : 4);
    }

    @Test
    public void testScrubCorruptedCounterRowNoEarlyOpen() throws IOException, WriteTimeoutException
    {
        boolean oldDisabledVal = SSTableRewriter.disableEarlyOpeningForTests;
        try
        {
            SSTableRewriter.disableEarlyOpeningForTests = true;
            testScrubCorruptedCounterPartition();
        }
        finally
        {
            SSTableRewriter.disableEarlyOpeningForTests = oldDisabledVal;
        }
    }

    @Test
    public void testScrubMultiRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        assertOrderedAll(cfs, 10);

        CompactionManager.instance.performScrub(cfs, false, true, 2);

        // check data is still there
        assertOrderedAll(cfs, 10);
    }

    @Test
    public void testScrubNoIndex() throws ExecutionException, InterruptedException, ConfigurationException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        assertOrderedAll(cfs, 10);

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            switch (sstable.descriptor.getFormat().getType()) {
                case BIG:
                    assertTrue(sstable.descriptor.fileFor(Component.PRIMARY_INDEX).tryDelete());
                    break;
                case BTI:
                    assertTrue(sstable.descriptor.fileFor(Component.PARTITION_INDEX).tryDelete());
                    sstable.descriptor.fileFor(Component.ROW_INDEX).delete(); // row index is optional
                    break;
                default:
                    fail("Unknonw SSTable format");
            }
        }

        CompactionManager.instance.performScrub(cfs, false, true, 2);

        // check data is still there
        assertOrderedAll(cfs, 10);
    }

    @Test
    public void testScrubOutOfOrder() throws IOException
    {
        // Run only for Big Table format because Big Table Format does not complain if partitions are given in invalid
        // order. Legacy SSTables with out-of-order partitions exist in production systems and must be corrected
        // by scrubbing. The trie index format does not permit such partitions.
        Assume.assumeThat(SSTableFormat.Type.current(), is(SSTableFormat.Type.BIG));

        // This test assumes ByteOrderPartitioner to create out-of-order SSTable
        IPartitioner oldPartitioner = DatabaseDescriptor.getPartitioner();
        DatabaseDescriptor.setPartitionerUnsafe(new ByteOrderedPartitioner());

        // Create out-of-order SSTable
        File tempDir = FileUtils.createTempFile("ScrubTest.testScrubOutOfOrder", "").parent();
        // create ks/cf directory
        File tempDataDir = new File(tempDir, String.join(File.pathSeparator(), ksName, CF));
        assertTrue(tempDataDir.tryCreateDirectories());
        try
        {
            CompactionManager.instance.disableAutoCompaction();
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

            List<String> keys = Arrays.asList("t", "a", "b", "z", "c", "y", "d");
            Descriptor desc = cfs.newSSTableDescriptor(tempDataDir);

            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata);
                 SSTableTxnWriter writer = new SSTableTxnWriter(txn, createTestWriter(desc, keys.size(), cfs.metadata, txn)))
            {
                for (String k : keys)
                {
                    PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), Util.dk(k))
                                                          .newRow("someName").add("val", "someValue")
                                                          .build();

                    writer.append(update.unfilteredIterator());
                }
                writer.finish(false, cfs.getStorageHandler());
            }

            try
            {
                desc.getFormat().getReaderFactory().open(desc, cfs.metadata);
                fail("SSTR validation should have caught the out-of-order rows");
            }
            catch (CorruptSSTableException ise)
            { /* this is expected */ }

            // open without validation for scrubbing
            Set<Component> components = new HashSet<>();
            if (desc.fileFor(Component.COMPRESSION_INFO).exists())
                components.add(Component.COMPRESSION_INFO);
            components.add(Component.DATA);
            components.addAll(desc.getFormat().primaryIndexComponents());
            components.add(Component.FILTER);
            components.add(Component.STATS);
            components.add(Component.SUMMARY);
            components.add(Component.TOC);

            SSTableReader sstable = desc.getFormat().getReaderFactory().openNoValidation(desc, components, cfs);
            if (sstable.last.compareTo(sstable.first) < 0)
                sstable.last = sstable.first;

            try (LifecycleTransaction scrubTxn = LifecycleTransaction.offline(OperationType.SCRUB, sstable);
                 Scrubber scrubber = new Scrubber(cfs, scrubTxn, false, true))
            {
                scrubber.scrub();
            }
            LifecycleTransaction.waitForDeletions();
            cfs.loadNewSSTables();
            assertOrderedAll(cfs, 7);
        }
        finally
        {
            FileUtils.deleteRecursive(tempDataDir);
            // reset partitioner
            DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
        }
    }

    static void overrideWithGarbage(SSTableReader sstable, ByteBuffer key1, ByteBuffer key2) throws IOException
    {
        overrideWithGarbage(sstable, key1, key2, (byte) 'z');
    }

    static void overrideWithGarbage(SSTableReader sstable, ByteBuffer key1, ByteBuffer key2, byte junk) throws IOException
    {
        boolean compression = Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false"));
        long startPosition, endPosition;

        if (compression)
        { // overwrite with garbage the compression chunks from key1 to key2
            CompressionMetadata compData = CompressionMetadata.read(sstable.getDataFile(), false);

            CompressionMetadata.Chunk chunk1 = compData.chunkFor(
            sstable.getPosition(PartitionPosition.ForKey.get(key1, sstable.getPartitioner()), SSTableReader.Operator.EQ).position);
            CompressionMetadata.Chunk chunk2 = compData.chunkFor(
            sstable.getPosition(PartitionPosition.ForKey.get(key2, sstable.getPartitioner()), SSTableReader.Operator.EQ).position);

            startPosition = Math.min(chunk1.offset, chunk2.offset);
            endPosition = Math.max(chunk1.offset + chunk1.length, chunk2.offset + chunk2.length);

            compData.close();
        }
        else
        { // overwrite with garbage from key1 to key2
            long row0Start = sstable.getPosition(PartitionPosition.ForKey.get(key1, sstable.getPartitioner()), SSTableReader.Operator.EQ).position;
            long row1Start = sstable.getPosition(PartitionPosition.ForKey.get(key2, sstable.getPartitioner()), SSTableReader.Operator.EQ).position;
            startPosition = Math.min(row0Start, row1Start);
            endPosition = Math.max(row0Start, row1Start);
        }

        overrideWithGarbage(sstable, startPosition, endPosition, junk);
    }

    private static void overrideWithGarbage(SSTableReader sstable, long startPosition, long endPosition) throws IOException
    {
        overrideWithGarbage(sstable, startPosition, endPosition, (byte) 'z');
    }

    private static void overrideWithGarbage(SSTableReader sstable, long startPosition, long endPosition, byte junk) throws IOException
    {
        overrideWithGarbage(sstable.getDataFile(), startPosition, endPosition, junk);
    }

    private static void overrideWithGarbage(File path, long startPosition, long endPosition, byte junk) throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(path.toJavaIOFile(), "rw"))
        {
            file.seek(startPosition);
            int length = (int)(endPosition - startPosition);
            byte[] buff = new byte[length];
            Arrays.fill(buff, junk);
            file.write(buff, 0, length);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(path);
    }

    static void assertOrderedAll(ColumnFamilyStore cfs, int expectedSize)
    {
        assertOrdered(Util.cmd(cfs).build(), expectedSize);
    }

    private static void assertOrdered(ReadCommand cmd, int expectedSize)
    {
        int size = 0;
        DecoratedKey prev = null;
        for (Partition partition : Util.getAllUnfiltered(cmd))
        {
            DecoratedKey current = partition.partitionKey();
            assertTrue("key " + current + " does not sort after previous key " + prev, prev == null || prev.compareTo(current) < 0);
            prev = current;
            ++size;
        }
        assertEquals(expectedSize, size);
    }

    private static int assertRowsOrdered(ReadCommand cmd)
    {
        int size = 0;
        DecoratedKey prev = null;
        for (Partition partition : Util.getAllUnfiltered(cmd))
        {
            DecoratedKey current = partition.partitionKey();
            assertTrue("key " + current + " does not sort after previous key " + prev, prev == null || prev.compareTo(current) < 0);
            prev = current;

            ClusteringPrefix<?> prevClustering = null;
            UnfilteredRowIterator rows = partition.unfilteredIterator();
            while (rows.hasNext())
            {
                Unfiltered unfiltered = rows.next();
                ClusteringPrefix<?> currentClustering = unfiltered.clustering();

                assertTrue("Clustering " + currentClustering + " does not sort after previous clustering " + prevClustering,
                           prevClustering == null || cmd.metadata().comparator.compare(prevClustering, currentClustering) < 0);

                prevClustering = currentClustering;
                size++;
            }
        }
        return size;
    }

    protected static void fillCF(ColumnFamilyStore cfs, int partitionsPerSSTable)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                                                  .newRow("r1").add("val", "1")
                                                  .newRow("r1").add("val", "1")
                                                  .build();

            new Mutation(update).applyUnsafe();
        }

        cfs.forceBlockingFlush(UNIT_TESTS);
    }

    protected static void fillCF(ColumnFamilyStore cfs, int partitionsPerSSTable, int rowsPerPartition)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            for (int r = 0; r < rowsPerPartition; r++)
            {
                PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                                                      .newRow(String.valueOf(r)).add("val", "1")
                                                      .build();

                new Mutation(update).applyUnsafe();
            }
        }

        cfs.forceBlockingFlush(UNIT_TESTS);
    }

    public static void fillIndexCF(ColumnFamilyStore cfs, boolean composite, long... values)
    {
        assertEquals(0, values.length % 2);
        for (int i = 0; i < values.length; i += 2)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), String.valueOf(i));
            if (composite)
            {
                builder.newRow("c" + i)
                       .add(COL_INDEX, values[i])
                       .add(COL_NON_INDEX, values[i + 1]);
            }
            else
            {
                builder.newRow()
                       .add(COL_INDEX, values[i])
                       .add(COL_NON_INDEX, values[i + 1]);
            }
            new Mutation(builder.build()).applyUnsafe();
        }

        cfs.forceBlockingFlush(UNIT_TESTS);
    }

    protected static String[] fillCounterCF(ColumnFamilyStore cfs, int partitionsPerSSTable) throws WriteTimeoutException
    {
        SortedSet<String> tokenSorted = Sets.newTreeSet(Comparator.comparing(a -> cfs.getPartitioner()
                                                                                     .decorateKey(ByteBufferUtil.bytes(a))));

        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            PartitionUpdate update = UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                                                  .newRow("r1").add("val", 100L)
                                                  .build();
            tokenSorted.add(String.valueOf(i));
            new CounterMutation(new Mutation(update), ConsistencyLevel.ONE).apply();
        }

        cfs.forceBlockingFlush(UNIT_TESTS);

        return tokenSorted.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    @Test
    public void testScrubColumnValidation() throws InterruptedException, RequestExecutionException, ExecutionException
    {
        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_compact_static_columns (a bigint, b timeuuid, c boolean static, d text, PRIMARY KEY (a, b))", ksName), ConsistencyLevel.ONE);

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_static_columns");

        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_static_columns (a, b, c, d) VALUES (123, c3db07e8-b602-11e3-bc6b-e0b9a54a6d93, true, 'foobar')", ksName));
        cfs.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performScrub(cfs, false, true, 2);

        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_scrub_validation (a text primary key, b int)", ksName), ConsistencyLevel.ONE);
        ColumnFamilyStore cfs2 = keyspace.getColumnFamilyStore("test_scrub_validation");

        new Mutation(UpdateBuilder.create(cfs2.metadata(), "key").newRow().add("b", Int32Type.instance.decompose(1)).build()).apply();
        cfs2.forceBlockingFlush(UNIT_TESTS);

        CompactionManager.instance.performScrub(cfs2, false, false, 2);
    }

    /**
     * For CASSANDRA-6892 too, check that for a compact table with one cluster column, we can insert whatever
     * we want as value for the clustering column, including something that would conflict with a CQL column definition.
     */
    @Test
    public void testValidationCompactStorage() throws Exception
    {
        QueryProcessor.process(String.format("CREATE TABLE \"%s\".test_compact_dynamic_columns (a int, b text, c text, PRIMARY KEY (a, b)) WITH COMPACT STORAGE", ksName), ConsistencyLevel.ONE);

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("test_compact_dynamic_columns");

        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'a', 'foo')", ksName));
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'b', 'bar')", ksName));
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".test_compact_dynamic_columns (a, b, c) VALUES (0, 'c', 'boo')", ksName));
        cfs.forceBlockingFlush(UNIT_TESTS);
        CompactionManager.instance.performScrub(cfs, true, true, 2);

        // Scrub is silent, but it will remove broken records. So reading everything back to make sure nothing to "scrubbed away"
        UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".test_compact_dynamic_columns", ksName));
        assertNotNull(rs);
        assertEquals(3, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        assertEquals("foo", iter.next().getString("c"));
        assertEquals("bar", iter.next().getString("c"));
        assertEquals("boo", iter.next().getString("c"));
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubKeysIndex_preserveOrder() throws IOException, ExecutionException, InterruptedException
    {
        //If the partitioner preserves the order then SecondaryIndex uses BytesType comparator,
        // otherwise it uses LocalByPartitionerType
        testScrubIndex(CF_INDEX1_BYTEORDERED, COL_INDEX, false, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubCompositeIndex_preserveOrder() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2_BYTEORDERED, COL_INDEX, true, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubKeysIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX1, COL_INDEX, false, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubCompositeIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_INDEX, true, true);
    }

    @Test /* CASSANDRA-5174 */
    public void testFailScrubKeysIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX1, COL_INDEX, false, false);
    }

    @Test /* CASSANDRA-5174 */
    public void testFailScrubCompositeIndex() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_INDEX, true, false);
    }

    @Test /* CASSANDRA-5174 */
    public void testScrubTwice() throws IOException, ExecutionException, InterruptedException
    {
        testScrubIndex(CF_INDEX2, COL_INDEX, true, true, true);
    }

    @SuppressWarnings("SameParameterValue")
    private void testScrubIndex(String cfName, String colName, boolean composite, boolean... scrubs)
    throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        int numRows = 1000;
        long[] colValues = new long[numRows * 2]; // each row has two columns
        for (int i = 0; i < colValues.length; i += 2)
        {
            colValues[i] = (i % 4 == 0 ? 1L : 2L); // index column
            colValues[i + 1] = 3L; //other column
        }
        fillIndexCF(cfs, composite, colValues);

        // check index

        assertOrdered(Util.cmd(cfs).filterOn(colName, Operator.EQ, 1L).build(), numRows / 2);

        // scrub index
        Set<ColumnFamilyStore> indexCfss = cfs.indexManager.getAllIndexColumnFamilyStores();
        assertEquals(1, indexCfss.size());
        for (ColumnFamilyStore indexCfs : indexCfss)
        {
            for (int i = 0; i < scrubs.length; i++)
            {
                boolean failure = !scrubs[i];
                if (failure)
                { //make sure the next scrub fails
                    overrideWithGarbage(indexCfs.getLiveSSTables().iterator().next(), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes(2L), (byte)0x7A);
                }
                CompactionManager.AllSSTableOpStatus result = indexCfs.scrub(false, false, false, true, false, 0);
                assertEquals(failure ?
                             CompactionManager.AllSSTableOpStatus.ABORTED :
                             CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                             result);
            }
        }


        // check index is still working
        assertOrdered(Util.cmd(cfs).filterOn(colName, Operator.EQ, 1L).build(), numRows / 2);
    }

    private static SSTableMultiWriter createTestWriter(Descriptor descriptor, long keyCount, TableMetadataRef metadata, LifecycleTransaction txn)
    {
        SerializationHeader header = new SerializationHeader(true, metadata.get(), metadata.get().regularAndStaticColumns(), EncodingStats.NO_STATS);
        MetadataCollector collector = new MetadataCollector(metadata.get().comparator).sstableLevel(0);
        return new TestMultiWriter(new TestWriter(descriptor, keyCount, 0, null, false, metadata, collector, header, txn), txn);
    }

    private static class TestMultiWriter extends SimpleSSTableMultiWriter
    {
        TestMultiWriter(SSTableWriter writer, LifecycleNewTracker lifecycleNewTracker)
        {
            super(writer, lifecycleNewTracker);
        }
    }

    /**
     * Test writer that allows to write out of order SSTable.
     */
    private static class TestWriter extends BigTableWriter
    {
        TestWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, boolean isTransient, TableMetadataRef metadata,
                   MetadataCollector collector, SerializationHeader header, LifecycleTransaction txn)
        {
            super(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, collector, header, Collections.emptySet(), txn, Collections.emptySet());
        }

        @Override
        protected void checkKeyOrder(DecoratedKey decoratedKey)
        {}
    }

    /**
     * Tests with invalid sstables (containing duplicate entries in 2.0 and 3.0 storage format),
     * that were caused by upgrading from 2.x with duplicate range tombstones.
     *
     * See CASSANDRA-12144 for details.
     */
    @Test
    public void testFilterOutDuplicates() throws Exception
    {
        IPartitioner oldPart = DatabaseDescriptor.getPartitioner();
        try
        {
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            QueryProcessor.process(String.format("CREATE TABLE \"%s\".cf_with_duplicates_3_0 (a int, b int, c int, PRIMARY KEY (a, b))", ksName), ConsistencyLevel.ONE);

            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("cf_with_duplicates_3_0");

            Path legacySSTableRoot = Paths.get(System.getProperty(INVALID_LEGACY_SSTABLE_ROOT_PROP),
                                               "Keyspace1",
                                               "cf_with_duplicates_3_0");

            for (String filename : new String[]{ "mb-3-big-CompressionInfo.db",
                                                 "mb-3-big-Digest.crc32",
                                                 "mb-3-big-Index.db",
                                                 "mb-3-big-Summary.db",
                                                 "mb-3-big-Data.db",
                                                 "mb-3-big-Filter.db",
                                                 "mb-3-big-Statistics.db",
                                                 "mb-3-big-TOC.txt" })
            {
                Files.copy(Paths.get(legacySSTableRoot.toString(), filename), cfs.getDirectories().getDirectoryForNewSSTables().toPath().resolve(filename));
            }

            cfs.loadNewSSTables();

            cfs.scrub(true, true, false, false, false, 1);

            UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".cf_with_duplicates_3_0", ksName));
            assertNotNull(rs);
            assertEquals(1, rs.size());

            QueryProcessor.executeInternal(String.format("DELETE FROM \"%s\".cf_with_duplicates_3_0 WHERE a=1 AND b =2", ksName));
            rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".cf_with_duplicates_3_0", ksName));
            assertNotNull(rs);
            assertEquals(0, rs.size());
        }
        finally
        {
            DatabaseDescriptor.setPartitionerUnsafe(oldPart);
        }
    }
}
