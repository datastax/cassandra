/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Iterators;
import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.AbstractMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

import static junit.framework.Assert.assertNotNull;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.db.ColumnFamilyStore.STATUS.INVALID_DROPPED;
import static org.apache.cassandra.db.ColumnFamilyStore.STATUS.INVALID_UNLOADED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class ColumnFamilyStoreTest
{
    public static final String KEYSPACE1 = "ColumnFamilyStoreTest1";
    public static final String KEYSPACE2 = "ColumnFamilyStoreTest2";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_INDEX1 = "Indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.keysIndexCFMD(KEYSPACE1, CF_INDEX1, true));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
    }

    @Test
    public void testRWCDLocking() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        CountDownLatch task1StaredtLatch = new CountDownLatch(1);
        CountDownLatch task1FinishLatch = new CountDownLatch(1);
        CountDownLatch task2StaredtLatch = new CountDownLatch(1);

        Thread task1 = new Thread(() -> {
            cfs.runWithCompactionsDisabled(() -> {
                                               task1StaredtLatch.countDown();
                                               task1FinishLatch.await();
                                               return null;
                                           },
                                           true,
                                           true,
                                           TableOperation.StopTrigger.UNIT_TESTS);
        });
        task1.start();

        Thread task2 = new Thread(() -> {
            cfs.runWithCompactionsDisabled(() -> {
                                               task2StaredtLatch.countDown();
                                               return null;
                                           },
                                           true,
                                           true,
                                           TableOperation.StopTrigger.UNIT_TESTS);
        });
        task2.start();

        // Check that task1 started but task2 is waiting
        assertTrue(task1StaredtLatch.await(30, TimeUnit.SECONDS));
        assertEquals(1, task2StaredtLatch.getCount());

        // Allow task1 to complete and check task2 completed next
        task1FinishLatch.countDown();
        assertTrue(task2StaredtLatch.await(30, TimeUnit.SECONDS));

        task1.join();
        task2.join();
    }

    @Test
    public void testMemtableTimestamp() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        assertEquals(AbstractAllocatorMemtable.NO_MIN_TIMESTAMP, fakeMemTableWithMinTS(cfs, EncodingStats.NO_STATS.minTimestamp).getMinTimestamp());
    }

    @Test
    // create two sstables, and verify that we only deserialize data from the most recent one
    public void testTimeSortedQuery()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        new RowUpdateBuilder(cfs.metadata(), 0, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);

        new RowUpdateBuilder(cfs.metadata(), 1, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);

        ((ClearableHistogram)cfs.metric.sstablesPerReadHistogram.tableOrKeyspaceHistogram()).clear(); // resets counts
        Util.getAll(Util.cmd(cfs, "key1").includeRow("c1").build());
        assertEquals(1, cfs.metric.sstablesPerReadHistogram.tableOrKeyspaceHistogram().getCount());
    }

    @Test
    public void testGetColumnWithWrongBF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        keyspace.getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);

        List<Mutation> rms = new LinkedList<>();
        rms.add(new RowUpdateBuilder(cfs.metadata(), 0, "key1")
                .clustering("Column1")
                .add("val", "asdf")
                .build());

        Util.writeColumnFamily(rms);

        List<SSTableReader> ssTables = keyspace.getAllSSTables(SSTableSet.LIVE);
        assertEquals(1, ssTables.size());
        Util.disableBloomFilter(cfs);
        Util.assertEmpty(Util.cmd(cfs, "key2").build());
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD2);

        RowUpdateBuilder.deleteRow(cfs.metadata(), FBUtilities.timestampMicros(), "key1", "Column1").applyUnsafe();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                Row toCheck = Util.getOnlyRowUnfiltered(Util.cmd(cfs, "key1").build());
                Iterator<Cell<?>> iter = toCheck.cells().iterator();
                assert(Iterators.size(iter) == 0);
            }
        };

        reTest(cfs, r);
    }

    @Test
    public void testDiscardSSTables() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);

        new RowUpdateBuilder(cfs.metadata(), 0, "key1").clustering("Column1").build().applyUnsafe();
        cfs.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS).get();

        new RowUpdateBuilder(cfs.metadata(), 0, "key1").clustering("Column1").build().applyUnsafe();
        cfs.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS).get();

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertEquals(2, sstables.size());

        SSTableReader discarded = sstables.iterator().next();
        cfs.discardSSTables(sstables, s -> s == discarded, OperationType.SSTABLE_DISCARD);

        assertEquals(1, cfs.getLiveSSTables().size());
        assertFalse(cfs.getLiveSSTables().contains(discarded));
    }

    @Test
    public void testDeleteStandardRowSticksAfterFlush() throws Throwable
    {
        // test to make sure flushing after a delete doesn't resurrect delted cols.
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer col = ByteBufferUtil.bytes("val");
        ByteBuffer val = ByteBufferUtil.bytes("val1");

        // insert
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(keyspaceName, cfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes("val2")));
        builder.update(cfName).row("Column1").add("val", "val1").build();

        new RowUpdateBuilder(cfs.metadata(), 0, "key1").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 0, "key2").clustering("Column1").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 2);

        // flush.
        cfs.forceBlockingFlush(UNIT_TESTS);

        // insert, don't flush
        new RowUpdateBuilder(cfs.metadata(), 1, "key3").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 1, "key4").clustering("Column1").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 4);

        // delete (from sstable and memtable)
        RowUpdateBuilder.deleteRow(cfs.metadata(), 5, "key1", "Column1").applyUnsafe();
        RowUpdateBuilder.deleteRow(cfs.metadata(), 5, "key3", "Column1").applyUnsafe();

        // verify delete
        assertRangeCount(cfs, col, val, 2);

        // flush
        cfs.forceBlockingFlush(UNIT_TESTS);

        // re-verify delete. // first breakage is right here because of CASSANDRA-1837.
        assertRangeCount(cfs, col, val, 2);

        // simulate a 'late' insertion that gets put in after the deletion. should get inserted, but fail on read.
        new RowUpdateBuilder(cfs.metadata(), 2, "key1").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 2, "key3").clustering("Column1").add("val", "val1").build().applyUnsafe();

        // should still be nothing there because we deleted this row. 2nd breakage, but was undetected because of 1837.
        assertRangeCount(cfs, col, val, 2);

        // make sure that new writes are recognized.
        new RowUpdateBuilder(cfs.metadata(), 10, "key5").clustering("Column1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), 10, "key6").clustering("Column1").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 4);

        // and it remains so after flush. (this wasn't failing before, but it's good to check.)
        cfs.forceBlockingFlush(UNIT_TESTS);
        assertRangeCount(cfs, col, val, 4);
    }

    @Test
    public void testClearEphemeralSnapshots() throws Throwable
    {
        // We don't do snapshot-based repair on Windows so we don't have ephemeral snapshots from repair that need clearing.
        // This test will fail as we'll revert to the WindowsFailedSnapshotTracker and counts will be off, but since we
        // don't do snapshot-based repair on Windows, we just skip this test.
        Assume.assumeTrue(!FBUtilities.isWindows);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1);

        //cleanup any previous test gargbage
        cfs.clearSnapshot("");

        int numRows = 1000;
        long[] colValues = new long [numRows * 2]; // each row has two columns
        for (int i = 0; i < colValues.length; i+=2)
        {
            colValues[i] = (i % 4 == 0 ? 1L : 2L); // index column
            colValues[i+1] = 3L; //other column
        }
        ScrubTest.fillIndexCF(cfs, false, colValues);

        cfs.snapshot("nonEphemeralSnapshot", null, false, false);
        cfs.snapshot("ephemeralSnapshot", null, true, false);

        Map<String, Directories.SnapshotSizeDetails> snapshotDetails = cfs.getSnapshotDetails();
        assertEquals(2, snapshotDetails.size());
        assertTrue(snapshotDetails.containsKey("ephemeralSnapshot"));
        assertTrue(snapshotDetails.containsKey("nonEphemeralSnapshot"));

        ColumnFamilyStore.clearEphemeralSnapshots(cfs.getDirectories());

        snapshotDetails = cfs.getSnapshotDetails();
        assertEquals(1, snapshotDetails.size());
        assertTrue(snapshotDetails.containsKey("nonEphemeralSnapshot"));

        //test cleanup
        cfs.clearSnapshot("");
    }

    @Test
    public void testSnapshotSize()
    {
        // cleanup any previous test gargbage
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs.clearSnapshot("");

        // Add row
        new RowUpdateBuilder(cfs.metadata(), 0, "key1")
        .clustering("Column1")
        .add("val", "asdf")
        .build()
        .applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);

        // snapshot
        cfs.snapshot("basic", null, false, false);

        // check snapshot was created
        Map<String, Directories.SnapshotSizeDetails> snapshotDetails = cfs.getSnapshotDetails();
        assertThat(snapshotDetails).hasSize(1);
        assertThat(snapshotDetails).containsKey("basic");

        // check that sizeOnDisk > trueSize = 0
        Directories.SnapshotSizeDetails details = snapshotDetails.get("basic");
        assertThat(details.sizeOnDiskBytes).isGreaterThan(details.dataSizeBytes);
        assertThat(details.dataSizeBytes).isZero();

        // compact base table to make trueSize > 0
        cfs.forceMajorCompaction();
        LifecycleTransaction.waitForDeletions();

        // sizeOnDisk > trueSize because trueSize does not include manifest.json
        // Check that truesize now is > 0
        snapshotDetails = cfs.getSnapshotDetails();
        details = snapshotDetails.get("basic");
        assertThat(details.sizeOnDiskBytes).isGreaterThan(details.dataSizeBytes);
        assertThat(details.dataSizeBytes).isPositive();
    }

    @Test
    public void testBackupAfterFlush() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1);
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key1")).clustering("Column1").add("val", "asdf").build().applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);
        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key2")).clustering("Column1").add("val", "asdf").build().applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);

        for (SSTableReader liveSSTable : cfs.getLiveSSTables())
        {
            Descriptor existing = liveSSTable.descriptor;
            Descriptor desc = new Descriptor(Directories.getBackupsDirectory(existing), KEYSPACE2, CF_STANDARD1, liveSSTable.descriptor.id, liveSSTable.descriptor.formatType);
            for (Component c : liveSSTable.components())
                assertTrue("Cannot find backed-up file:" + desc.fileFor(c), desc.fileFor(c).exists());
        }
    }

    // TODO: Fix once we have working supercolumns in 8099
//    // CASSANDRA-3467.  the key here is that supercolumn and subcolumn comparators are different
//    @Test
//    public void testSliceByNamesCommandOnUUIDTypeSCF() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_SUPER6;
//        ByteBuffer superColName = LexicalUUIDType.instance.fromString("a4ed3562-0e8e-4b41-bdfd-c45a2774682d");
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        DecoratedKey key = Util.dk("slice-get-uuid-type");
//
//        // Insert a row with one supercolumn and multiple subcolumns
//        putColsSuper(cfs, key, superColName, new BufferCell(cellname("a"), ByteBufferUtil.bytes("A"), 1),
//                                             new BufferCell(cellname("b"), ByteBufferUtil.bytes("B"), 1));
//
//        // Get the entire supercolumn like normal
//        ColumnFamily cfGet = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
//        assertEquals(ByteBufferUtil.bytes("A"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
//        assertEquals(ByteBufferUtil.bytes("B"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
//
//        // Now do the SliceByNamesCommand on the supercolumn, passing both subcolumns in as columns to get
//        SortedSet<CellName> sliceColNames = new TreeSet<CellName>(cfs.metadata.comparator);
//        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a")));
//        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b")));
//        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(keyspaceName, key.getKey(), cfName, System.currentTimeMillis(), new NamesQueryFilter(sliceColNames));
//        ColumnFamily cfSliced = cmd.getRow(keyspace).cf;
//
//        // Make sure the slice returns the same as the straight get
//        assertEquals(ByteBufferUtil.bytes("A"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
//        assertEquals(ByteBufferUtil.bytes("B"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
//    }


    // TODO: Fix once SSTableSimpleWriter's back in
    // @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-6086">CASSANDRA-6086</a>


    // TODO: Fix once SSTableSimpleWriter's back in
//    @Test
//    public void testLoadNewSSTablesAvoidsOverwrites() throws Throwable
//    {
//        String ks = KEYSPACE1;
//        String cf = CF_STANDARD1;
//        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cf);
//        SSTableDeletingTask.waitForDeletions();
//
//        final CFMetaData cfmeta = Schema.instance.getTableMetadataRef(ks, cf);
//        Directories dir = new Directories(cfs.metadata);
//
//        // clear old SSTables (probably left by CFS.clearUnsafe() calls in other tests)
//        for (Map.Entry<Descriptor, Set<Component>> entry : dir.sstableLister().list().entrySet())
//        {
//            for (Component component : entry.getValue())
//            {
//                FileUtils.delete(entry.getKey().filenameFor(component));
//            }
//        }
//
//        // sanity check
//        int existingSSTables = dir.sstableLister().list().keySet().size();
//        assert existingSSTables == 0 : String.format("%d SSTables unexpectedly exist", existingSSTables);
//
//        ByteBuffer key = bytes("key");
//
//        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                                             cfmeta, StorageService.getPartitioner())
//        {
//            @Override
//            protected SSTableWriter getWriter()
//            {
//                // hack for reset generation
//                generation.set(0);
//                return super.getWriter();
//            }
//        };
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                         cfmeta, StorageService.getPartitioner());
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        Set<Integer> generations = new HashSet<>();
//        for (Descriptor descriptor : dir.sstableLister().list().keySet())
//            generations.add(descriptor.generation);
//
//        // we should have two generations: [1, 2]
//        assertEquals(2, generations.size());
//        assertTrue(generations.contains(1));
//        assertTrue(generations.contains(2));
//
//        assertEquals(0, cfs.getLiveSSTables().size());
//
//        // start the generation counter at 1 again (other tests have incremented it already)
//        cfs.resetFileIndexGenerator();
//
//        boolean incrementalBackupsEnabled = DatabaseDescriptor.isIncrementalBackupsEnabled();
//        try
//        {
//            // avoid duplicate hardlinks to incremental backups
//            DatabaseDescriptor.setIncrementalBackupsEnabled(false);
//            cfs.loadNewSSTables();
//        }
//        finally
//        {
//            DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackupsEnabled);
//        }
//
//        assertEquals(2, cfs.getLiveSSTables().size());
//        generations = new HashSet<>();
//        for (Descriptor descriptor : dir.sstableLister().list().keySet())
//            generations.add(descriptor.generation);
//
//        // normally they would get renamed to generations 1 and 2, but since those filenames already exist,
//        // they get skipped and we end up with generations 3 and 4
//        assertEquals(2, generations.size());
//        assertTrue(generations.contains(3));
//        assertTrue(generations.contains(4));
//    }

    public void reTest(ColumnFamilyStore cfs, Runnable verify) throws Exception
    {
        verify.run();
        cfs.forceBlockingFlush(UNIT_TESTS);
        verify.run();
    }

    private void assertRangeCount(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val, int count)
    {
        assertRangeCount(cfs, cfs.metadata().getColumn(col), val, count);
    }

    private void assertRangeCount(ColumnFamilyStore cfs, ColumnMetadata col, ByteBuffer val, int count)
    {

        int found = 0;
        if (count != 0)
        {
            for (FilteredPartition partition : Util.getAll(Util.cmd(cfs).filterOn(col.name.toString(), Operator.EQ, val).build()))
            {
                for (Row r : partition.rows())
                {
                    if (r.getCell(col).buffer().equals(val))
                        ++found;
                }
            }
        }
        assertEquals(count, found);
    }

    @Test
    public void testSnapshotWithoutFlushWithSecondaryIndexes() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX1);
        cfs.truncateBlocking();

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata.get(), "key")
                                             .newRow()
                                             .add("birthdate", 1L)
                                             .add("notbirthdate", 2L);
        new Mutation(builder.build()).applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);

        String snapshotName = "newSnapshot";
        cfs.snapshotWithoutMemtable(snapshotName);

        File snapshotManifestFile = cfs.getDirectories().getSnapshotManifestFile(snapshotName);
        JSONParser parser = new JSONParser();
        JSONObject manifest = (JSONObject) parser.parse(new FileReader(snapshotManifestFile));
        JSONArray files = (JSONArray) manifest.get("files");

        // Keyspace1-Indexed1 and the corresponding index
        assert files.size() == 2;

        // Snapshot of the secondary index is stored in the subfolder with the same file name
        String baseTableFile = (String) files.get(0);
        String indexTableFile = (String) files.get(1);
        assertThat(baseTableFile).isNotEqualTo(indexTableFile);
        assertThat(Directories.isSecondaryIndexFolder(new File(indexTableFile).parent())).isTrue();

        Set<File> originalFiles = new HashSet<>();
        Iterables.toList(cfs.concatWithIndexes()).stream()
                 .flatMap(c -> c.getLiveSSTables().stream().map(t -> t.descriptor.fileFor(Component.DATA)))
                 .forEach(originalFiles::add);
        assertThat(originalFiles.stream().anyMatch(f -> f.toString().endsWith(indexTableFile))).isTrue();
        assertThat(originalFiles.stream().anyMatch(f -> f.toString().endsWith(baseTableFile))).isTrue();
    }

    @Test
    public void testDataDirectoriesOfColumnFamily() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        List<String> dataPaths = cfs.getDataPaths();
        assertFalse(dataPaths.isEmpty());

        Path path = Paths.get(dataPaths.get(0));

        String keyspace = path.getParent().getFileName().toString();
        String table = path.getFileName().toString().split("-")[0];

        Assert.assertEquals(cfs.getTableName(), table);
        Assert.assertEquals(KEYSPACE1, keyspace);
    }

    @Test
    public void testScrubDataDirectories() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);

        ColumnFamilyStore.scrubDataDirectories(cfs.metadata());

        new RowUpdateBuilder(cfs.metadata(), 2, "key").clustering("name").add("val", "2").build().applyUnsafe();
        cfs.forceBlockingFlush(UNIT_TESTS);

        // Nuke the metadata and reload that sstable
        Collection<SSTableReader> ssTables = cfs.getLiveSSTables();
        assertEquals(1, ssTables.size());
        SSTableReader ssTable = ssTables.iterator().next();

        File dataFileName = ssTable.descriptor.fileFor(Component.DATA);
        File tmpDataFile = ssTable.descriptor.tmpFileFor(Component.DATA);
        dataFileName.tryMove(tmpDataFile);

        ssTable.selfRef().release();

        ColumnFamilyStore.scrubDataDirectories(cfs.metadata());

        List<File> ssTableFiles = new Directories(cfs.metadata()).sstableLister(Directories.OnTxnErr.THROW).listFiles();
        assertNotNull(ssTableFiles);
        assertEquals(0, ssTableFiles.size());
        cfs.clearUnsafe();
    }

    @Test
    public void testMutateRepaired() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);

        new RowUpdateBuilder(cfs.metadata(), 0, "key1").clustering("Column1").add("val", "val1").build().applyUnsafe();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertEquals(1, sstables.size());

        SSTableReader sstable = sstables.iterator().next();
        assertFalse(sstable.isRepaired());

        int repaired = cfs.mutateRepaired(sstables, 1, null, false);
        assertEquals(1, repaired);

        sstables = cfs.getLiveSSTables();
        sstable = sstables.iterator().next();
        assertTrue(sstable.isRepaired());
    }

    @Test
    public void testInvalidateWithDropping()
    {
        testInvalidateCFS(true);
    }

    @Test
    public void testInvalidateWithoutDropping()
    {
        testInvalidateCFS(false);
    }

    private void testInvalidateCFS(boolean dropData)
    {
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        ColumnFamilyStore cfs = MockSchema.newCFS();
        assertTrue(cfs.isValid());
        Tracker tracker = cfs.getTracker();

        Collection<SSTableReader> readers = IntStream.range(0, 10)
                                                     .mapToObj(i -> MockSchema.sstable(i, 10, true, cfs))
                                                     .collect(Collectors.toList());
        tracker.addInitialSSTables(readers);
        readers.forEach(reader -> assertEquals(1, reader.selfRef().globalCount()));

        cfs.invalidate(false, dropData);
        assertFalse(cfs.isValid());
        assertThat(cfs.status()).isEqualTo(dropData ? INVALID_DROPPED : INVALID_UNLOADED);
        System.gc();
        System.gc();

        readers.forEach(reader -> {
            assertEquals(0, reader.selfRef().globalCount());
            assertEquals(!dropData, Files.exists(reader.descriptor.pathFor(Component.DATA)));
        });
    }

    private Memtable fakeMemTableWithMinTS(ColumnFamilyStore cfs, long minTS)
    {
        return new AbstractMemtable(cfs.metadata, minTS)
        {
            @Override
            public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
            {
                return 0;
            }

            @Override
            public Partition getPartition(DecoratedKey key)
            {
                return null;
            }

            @Override
            public MemtableUnfilteredPartitionIterator makePartitionIterator(ColumnFilter columnFilter, DataRange dataRange)
            {
                return null;
            }

            @Override
            public long partitionCount()
            {
                return 0;
            }

            @Override
            public long getLiveDataSize()
            {
                return 0;
            }

            @Override
            public long getEstimatedAverageRowSize()
            {
                return 0;
            }

            @Override
            public DecoratedKey minPartitionKey()
            {
                return null;
            }

            @Override
            public DecoratedKey maxPartitionKey()
            {
                return null;
            }

            @Override
            public void addMemoryUsageTo(Memtable.MemoryUsage usage)
            {
            }

            @Override
            public void signalFlushRequired(ColumnFamilyStore.FlushReason flushReason, boolean skipIfSignaled)
            {
            }

            @Override
            public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
            {
            }

            @Override
            public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
            {
            }

            @Override
            public Memtable.FlushCollection<?> getFlushSet(PartitionPosition from, PartitionPosition to)
            {
                return null;
            }

            @Override
            public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
            {
            }

            @Override
            public void discard()
            {
            }

            @Override
            public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
            {
                return false;
            }

            @Override
            public CommitLogPosition getApproximateCommitLogLowerBound()
            {
                return null;
            }

            @Override
            public CommitLogPosition getCommitLogLowerBound()
            {
                return null;
            }

            @Override
            public LastCommitLogPosition getFinalCommitLogUpperBound()
            {
                return null;
            }

            @Override
            public boolean mayContainDataBefore(CommitLogPosition position)
            {
                return false;
            }

            @Override
            public boolean isClean()
            {
                return false;
            }

            @Override
            public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
            {
                return false;
            }

            @Override
            public void metadataUpdated()
            {
            }

            @Override
            public void performSnapshot(String snapshotName)
            {
            }
        };
    }
}
