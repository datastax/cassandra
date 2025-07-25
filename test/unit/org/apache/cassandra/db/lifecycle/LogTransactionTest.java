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
package org.apache.cassandra.db.lifecycle;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Runnables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LogTransactionTest extends AbstractTransactionalTest
{
    private static final String KEYSPACE = "TransactionLogsTest";

    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();
    }

    protected AbstractTransactionalTest.TestableTransaction newTest() throws Exception
    {
        LifecycleTransaction.waitForDeletions();
        SSTableReader.resetTidying();
        return new TxnTest();
    }

    private static final class TxnTest extends TestableTransaction
    {
        private final static class Transaction extends Transactional.AbstractTransactional implements Transactional
        {
            final ColumnFamilyStore cfs;
            final LogTransaction txnLogs;
            final File dataFolder;
            final SSTableReader sstableOld;
            final SSTableReader sstableNew;
            final AbstractLogTransaction.ReaderTidier tidier;

            Transaction(ColumnFamilyStore cfs, LogTransaction txnLogs) throws IOException
            {
                this.cfs = cfs;
                this.txnLogs = txnLogs;
                this.dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
                this.sstableOld = sstable(dataFolder, cfs, 0, 128);
                this.sstableNew = sstable(dataFolder, cfs, 1, 128);

                assertNotNull(txnLogs);
                assertNotNull(txnLogs.id());
                assertEquals(OperationType.COMPACTION, txnLogs.opType());

                txnLogs.trackNew(sstableNew);
                tidier = txnLogs.obsoleted(sstableOld);
                assertNotNull(tidier);
            }

            protected Throwable doCommit(Throwable accumulate)
            {
                sstableOld.markObsolete(tidier);
                sstableOld.selfRef().release();
                LifecycleTransaction.waitForDeletions();

                Throwable ret = txnLogs.commit(accumulate);

                sstableNew.selfRef().release();
                return ret;
            }

            protected Throwable doAbort(Throwable accumulate)
            {
                accumulate = tidier.abort(accumulate);
                LifecycleTransaction.waitForDeletions();

                Throwable ret = txnLogs.abort(accumulate);

                sstableNew.selfRef().release();
                sstableOld.selfRef().release();
                return ret;
            }

            protected void doPrepare()
            {
                txnLogs.prepareToCommit();
            }

            void assertInProgress() throws Exception
            {
                assertFiles(dataFolder.path(), Sets.newHashSet(Iterables.concat(getAllFilePaths(sstableNew),
                                                                                getAllFilePaths(sstableOld),
                                                                                txnLogs.logFilePaths())));
            }

            void assertPrepared() throws Exception
            {
            }

            void assertAborted() throws Exception
            {
                assertFiles(dataFolder.path(), getAllFilePaths(sstableOld));
            }

            void assertCommitted() throws Exception
            {
                assertFiles(dataFolder.path(), getAllFilePaths(sstableNew));
            }
        }

        final Transaction txn;

        private TxnTest() throws IOException
        {
            this(MockSchema.newCFS(KEYSPACE));
        }

        private TxnTest(ColumnFamilyStore cfs) throws IOException
        {
            this(cfs, createLogTransaction(OperationType.COMPACTION, cfs.metadata));
        }

        private TxnTest(ColumnFamilyStore cfs, LogTransaction txnLogs) throws IOException
        {
            this(new Transaction(cfs, txnLogs));
        }

        private TxnTest(Transaction txn)
        {
            super(txn);
            this.txn = txn;
        }

        protected void assertInProgress() throws Exception
        {
            txn.assertInProgress();
        }

        protected void assertPrepared() throws Exception
        {
            txn.assertPrepared();
        }

        protected void assertAborted() throws Exception
        {
            txn.assertAborted();
        }

        protected void assertCommitted() throws Exception
        {
            txn.assertCommitted();
        }
    }

    @Test
    public void testUntrack() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        // complete a transaction without keep the new files since they were untracked
        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        log.untrackNew(sstableNew);

        log.finish();

        sstableNew.selfRef().release();
        Thread.sleep(1);
        LifecycleTransaction.waitForDeletions();

        assertFiles(dataFolder.path(), Collections.<File>emptySet());
    }

    @Test
    public void testUntrackIdenticalLogFilesOnDisk() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File datadir1 = new File(Files.createTempDirectory("datadir1"));
        File datadir2 = new File(Files.createTempDirectory("datadir2"));
        SSTableReader sstable1 = sstable(datadir1, cfs, 1, 128);
        SSTableReader sstable2 = sstable(datadir2, cfs, 1, 128);


        for (Consumer<LogTransaction> c : Arrays.<Consumer<LogTransaction>>asList((log) -> log.trackNew(sstable2),
                                                                                  (log) -> log.obsoleted(sstable2),
                                                                                  (log) -> log.txnFile().addAll(LogRecord.Type.ADD, Collections.singleton(sstable2))))
        {
            try (LogTransaction log = new LogTransaction(OperationType.COMPACTION, LifecycleTransaction.newId()))
            {
                log.trackNew(sstable1); // creates a log file in datadir1
                log.untrackNew(sstable1); // removes sstable1 from `records`, but still on disk & in `onDiskRecords`

                c.accept(log);  // creates a log file in datadir2, based on contents in onDiskRecords
                byte[] log1 = Files.readAllBytes(log.logFiles().get(0).toPath());
                byte[] log2 = Files.readAllBytes(log.logFiles().get(1).toPath());
                assertArrayEquals(log1, log2);
            }
        }
        sstable1.selfRef().release();
        sstable2.selfRef().release();
        Thread.sleep(1);
        FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(Runnables.doNothing(), 0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCommitSameDesc() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableOld1 = sstable(dataFolder, cfs, 0, 128);
        SSTableReader sstableOld2 = sstable(dataFolder, cfs, 0, 256);
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);

        sstableOld1.setReplaced();

        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstableOld2);
        assertNotNull(tidier);

        log.finish();

        sstableOld2.markObsolete(tidier);

        sstableOld1.selfRef().release();
        sstableOld2.selfRef().release();

        assertFiles(dataFolder.path(), new HashSet<>(getAllFilePaths(sstableNew)));

        sstableNew.selfRef().release();
    }

    @Test
    public void testCommitOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable = sstable(dataFolder, cfs, 0, 128);

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstable);
        log.finish();

        assertFiles(dataFolder.path(), getAllFilePaths(sstable));

        sstable.selfRef().release();
    }

    @Test
    public void testCommitOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable = sstable(dataFolder, cfs, 0, 128);

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstable);
        assertNotNull(tidier);

        log.finish();
        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        assertFiles(dataFolder.path(), new HashSet<>());
    }

    @Test
    public void testCommitMultipleFolders() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);

        File origiFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        File dataFolder1 = new File(origiFolder, "1");
        File dataFolder2 = new File(origiFolder, "2");
        Files.createDirectories(dataFolder1.toPath());
        Files.createDirectories(dataFolder2.toPath());

        SSTableReader[] sstables = { sstable(dataFolder1, cfs, 0, 128),
                                     sstable(dataFolder1, cfs, 1, 128),
                                     sstable(dataFolder2, cfs, 2, 128),
                                     sstable(dataFolder2, cfs, 3, 128)
        };

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier[] tidiers = { log.obsoleted(sstables[0]), log.obsoleted(sstables[2]) };

        log.trackNew(sstables[1]);
        log.trackNew(sstables[3]);

        log.finish();

        sstables[0].markObsolete(tidiers[0]);
        sstables[2].markObsolete(tidiers[1]);

        Arrays.stream(sstables).forEach(s -> s.selfRef().release());
        LifecycleTransaction.waitForDeletions();

        assertFiles(dataFolder1.path(), getAllFilePaths(sstables[1]));
        assertFiles(dataFolder2.path(), getAllFilePaths(sstables[3]));
    }

    @Test
    public void testAbortOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable = sstable(dataFolder, cfs, 0, 128);

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstable);
        log.abort();

        sstable.selfRef().release();

        assertFiles(dataFolder.path(), new HashSet<>());
    }

    @Test
    public void testAbortOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable = sstable(dataFolder, cfs, 0, 128);

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstable);
        assertNotNull(tidier);

        tidier.abort(null);
        log.abort();

        sstable.selfRef().release();

        assertFiles(dataFolder.path(), getAllFilePaths(sstable));
    }

    @Test
    public void testAbortMultipleFolders() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);

        File origiFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        File dataFolder1 = new File(origiFolder, "1");
        File dataFolder2 = new File(origiFolder, "2");
        Files.createDirectories(dataFolder1.toPath());
        Files.createDirectories(dataFolder2.toPath());

        SSTableReader[] sstables = { sstable(dataFolder1, cfs, 0, 128),
                                     sstable(dataFolder1, cfs, 1, 128),
                                     sstable(dataFolder2, cfs, 2, 128),
                                     sstable(dataFolder2, cfs, 3, 128)
        };

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier[] tidiers = { log.obsoleted(sstables[0]), log.obsoleted(sstables[2]) };

        log.trackNew(sstables[1]);
        log.trackNew(sstables[3]);

        Arrays.stream(tidiers).forEach(tider -> tider.abort(null));
        log.abort();

        Arrays.stream(sstables).forEach(s -> s.selfRef().release());
        LifecycleTransaction.waitForDeletions();

        assertFiles(dataFolder1.path(), getAllFilePaths(sstables[0]));
        assertFiles(dataFolder2.path(), getAllFilePaths(sstables[2]));
    }


    @Test
    public void testRemoveUnfinishedLeftovers_abort() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableOld = sstable(dataFolder, cfs, 0, 128);
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstableOld);

        Set<File> tmpFiles = getAllFilePaths(sstableNew);

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        assertEquals(tmpFiles, getTemporaryFiles(sstableNew.descriptor.directory));

        // normally called at startup
        LifecycleTransaction.removeUnfinishedLeftovers(cfs.metadata());

        // sstableOld should be only table left
        Directories directories = new Directories(cfs.metadata());
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(1, sstables.size());

        assertFiles(dataFolder.path(), getAllFilePaths(sstableOld));

        // complete the transaction before releasing files
        tidier.commit();
        log.close();
    }

    @Test
    public void testRemoveUnfinishedLeftovers_commit() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableOld = sstable(dataFolder, cfs, 0, 128);
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        // simulate tracking sstables with a committed transaction (new log file deleted)
        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstableOld);

        //Fake a commit
        log.txnFile().commit();

        Set<File> tmpFiles = getAllFilePaths(sstableOld);

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        assertEquals(tmpFiles, getTemporaryFiles(sstableOld.descriptor.directory));

        // normally called at startup
        LifecycleTransaction.removeUnfinishedLeftovers(cfs.metadata());

        // sstableNew should be only table left
        Directories directories = new Directories(cfs.metadata());
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(1, sstables.size());

        assertFiles(dataFolder.path(), getAllFilePaths(sstableNew));

        // complete the transaction to avoid LEAK errors
        tidier.commit();
        assertNull(log.complete(null));
    }

    @Test
    public void testRemoveUnfinishedLeftovers_commit_multipleFolders() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);

        File origiFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        File dataFolder1 = new File(origiFolder, "1");
        File dataFolder2 = new File(origiFolder, "2");
        Files.createDirectories(dataFolder1.toPath());
        Files.createDirectories(dataFolder2.toPath());

        SSTableReader[] sstables = { sstable(dataFolder1, cfs, 0, 128),
                                     sstable(dataFolder1, cfs, 1, 128),
                                     sstable(dataFolder2, cfs, 2, 128),
                                     sstable(dataFolder2, cfs, 3, 128)
        };

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier[] tidiers = { log.obsoleted(sstables[0]), log.obsoleted(sstables[2]) };

        log.trackNew(sstables[1]);
        log.trackNew(sstables[3]);

        Collection<File> logFiles = log.logFiles();
        assertEquals(2, logFiles.size());

        // fake a commit
        log.txnFile().commit();

        Arrays.stream(sstables).forEach(s -> s.selfRef().release());

        // test listing
        assertEquals(getAllFilePaths(sstables[0]), getTemporaryFiles(dataFolder1));
        assertEquals(getAllFilePaths(sstables[2]), getTemporaryFiles(dataFolder2));

        // normally called at startup
        assertTrue(LifecycleTransaction.removeUnfinishedLeftovers(Arrays.asList(dataFolder1, dataFolder2)));

        // new tables should be only table left
        assertFiles(dataFolder1.path(), getAllFilePaths(sstables[1]));
        assertFiles(dataFolder2.path(), getAllFilePaths(sstables[3]));

        // complete the transaction to avoid LEAK errors
        Arrays.stream(tidiers).forEach(AbstractLogTransaction.ReaderTidier::commit);
        assertNull(log.complete(null));
    }

    @Test
    public void testRemoveUnfinishedLeftovers_abort_multipleFolders() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);

        File origiFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        File dataFolder1 = new File(origiFolder, "1");
        File dataFolder2 = new File(origiFolder, "2");
        Files.createDirectories(dataFolder1.toPath());
        Files.createDirectories(dataFolder2.toPath());

        SSTableReader[] sstables = { sstable(dataFolder1, cfs, 0, 128),
                                     sstable(dataFolder1, cfs, 1, 128),
                                     sstable(dataFolder2, cfs, 2, 128),
                                     sstable(dataFolder2, cfs, 3, 128)
        };

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier[] tidiers = { log.obsoleted(sstables[0]), log.obsoleted(sstables[2]) };

        log.trackNew(sstables[1]);
        log.trackNew(sstables[3]);

        Collection<File> logFiles = log.logFiles();
        assertEquals(2, logFiles.size());

        // fake an abort
        log.txnFile().abort();

        Arrays.stream(sstables).forEach(s -> s.selfRef().release());

        // test listing
        assertEquals(getAllFilePaths(sstables[1]), getTemporaryFiles(dataFolder1));
        assertEquals(getAllFilePaths(sstables[3]), getTemporaryFiles(dataFolder2));

        // normally called at startup
        assertTrue(LifecycleTransaction.removeUnfinishedLeftovers(Arrays.asList(dataFolder1, dataFolder2)));

        // old tables should be only table left
        assertFiles(dataFolder1.path(), getAllFilePaths(sstables[0]));
        assertFiles(dataFolder2.path(), getAllFilePaths(sstables[2]));

        // complete the transaction to avoid LEAK errors
        Arrays.stream(tidiers).forEach(AbstractLogTransaction.ReaderTidier::commit);
        assertNull(log.complete(null));
    }

    private static LogTransaction createLogTransaction(OperationType type, TableMetadataRef metadata)
    {
        LogTransaction txn = (LogTransaction) ILogTransactionsFactory.instance.createLogTransaction(type,
                                                                                                    LifecycleTransaction.newId(),
                                                                                                    metadata);
        assertEquals(type, txn.opType());
        return txn;
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_mismatchedFinalRecords() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert mismatched records
            FileUtils.append(logFiles.get(0), LogRecord.makeCommit(System.currentTimeMillis()).raw);
            FileUtils.append(logFiles.get(1), LogRecord.makeAbort(System.currentTimeMillis()).raw);

        }, false);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_partialFinalRecords_first() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert a full record and a partial one
            String finalRecord = LogRecord.makeCommit(System.currentTimeMillis()).raw;
            int toChop = finalRecord.length() / 2;
            FileUtils.append(logFiles.get(0), finalRecord.substring(0, finalRecord.length() - toChop));
            FileUtils.append(logFiles.get(1), finalRecord);

        }, true);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_partialFinalRecords_second() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert a full record and a partial one
            String finalRecord = LogRecord.makeCommit(System.currentTimeMillis()).raw;
            int toChop = finalRecord.length() / 2;
            FileUtils.append(logFiles.get(0), finalRecord);
            FileUtils.append(logFiles.get(1), finalRecord.substring(0, finalRecord.length() - toChop));

        }, true);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_partialNonFinalRecord_first() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert a partial sstable record and a full commit record
            String sstableRecord = LogRecord.make(LogRecord.Type.ADD, Collections.emptyList(), 0, "abc").raw;
            int toChop = sstableRecord.length() / 2;
            FileUtils.append(logFiles.get(0), sstableRecord.substring(0, sstableRecord.length() - toChop));
            FileUtils.append(logFiles.get(1), sstableRecord);
            String finalRecord = LogRecord.makeCommit(System.currentTimeMillis()).raw;
            FileUtils.append(logFiles.get(0), finalRecord);
            FileUtils.append(logFiles.get(1), finalRecord);

        }, false);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_partialNonFinalRecord_second() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert a partial sstable record and a full commit record
            String sstableRecord = LogRecord.make(LogRecord.Type.ADD, Collections.emptyList(), 0, "abc").raw;
            int toChop = sstableRecord.length() / 2;
            FileUtils.append(logFiles.get(0), sstableRecord);
            FileUtils.append(logFiles.get(1), sstableRecord.substring(0, sstableRecord.length() - toChop));
            String finalRecord = LogRecord.makeCommit(System.currentTimeMillis()).raw;
            FileUtils.append(logFiles.get(0), finalRecord);
            FileUtils.append(logFiles.get(1), finalRecord);

        }, false);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_missingFinalRecords_first() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert only one commit record
            FileUtils.append(logFiles.get(0), LogRecord.makeCommit(System.currentTimeMillis()).raw);

        }, true);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_missingFinalRecords_second() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert only one commit record
            FileUtils.append(logFiles.get(1), LogRecord.makeCommit(System.currentTimeMillis()).raw);

        }, true);
    }

    @Test
    public void testRemoveUnfinishedLeftovers_multipleFolders_tooManyFinalRecords() throws Throwable
    {
        testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(txn -> {
            List<File> logFiles = txn.logFiles();
            assertEquals(2, logFiles.size());

            // insert mismatched records
            FileUtils.append(logFiles.get(0), LogRecord.makeCommit(System.currentTimeMillis()).raw);
            FileUtils.append(logFiles.get(1), LogRecord.makeCommit(System.currentTimeMillis()).raw);
            FileUtils.append(logFiles.get(1), LogRecord.makeCommit(System.currentTimeMillis()).raw);

        }, false);
    }

    private static void testRemoveUnfinishedLeftovers_multipleFolders_errorConditions(Consumer<LogTransaction> modifier, boolean shouldCommit) throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);

        File origiFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        File dataFolder1 = new File(origiFolder, "1");
        File dataFolder2 = new File(origiFolder, "2");
        Files.createDirectories(dataFolder1.toPath());
        Files.createDirectories(dataFolder2.toPath());

        SSTableReader[] sstables = { sstable(dataFolder1, cfs, 0, 128),
                                     sstable(dataFolder1, cfs, 1, 128),
                                     sstable(dataFolder2, cfs, 2, 128),
                                     sstable(dataFolder2, cfs, 3, 128)
        };

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        AbstractLogTransaction.ReaderTidier[] tidiers = { log.obsoleted(sstables[0]), log.obsoleted(sstables[2]) };

        log.trackNew(sstables[1]);
        log.trackNew(sstables[3]);

        // fake some error condition on the txn logs
        modifier.accept(log);

        Arrays.stream(sstables).forEach(s -> s.selfRef().release());

        // if shouldCommit is true then it should remove the leftovers and return true, false otherwise
        assertEquals(shouldCommit, LifecycleTransaction.removeUnfinishedLeftovers(Arrays.asList(dataFolder1, dataFolder2)));
        LifecycleTransaction.waitForDeletions();

        if (shouldCommit)
        {
            // only new sstables should still be there
            assertFiles(dataFolder1.path(), getAllFilePaths(sstables[1]));
            assertFiles(dataFolder2.path(), getAllFilePaths(sstables[3]));
        }
        else
        {
            // all files should still be there
            assertFiles(dataFolder1.path(), Sets.newHashSet(Iterables.concat(getAllFilePaths(sstables[0]),
                                                                             getAllFilePaths(sstables[1]),
                                                                             Collections.singleton(log.logFilePaths().get(0)))));
            assertFiles(dataFolder2.path(), Sets.newHashSet(Iterables.concat(getAllFilePaths(sstables[2]),
                                                                             getAllFilePaths(sstables[3]),
                                                                             Collections.singleton(log.logFilePaths().get(1)))));
        }


        // complete the transaction to avoid LEAK errors
        Arrays.stream(tidiers).forEach(AbstractLogTransaction.ReaderTidier::commit);
        log.txnFile().commit(); // just anything to make sure transaction tidier will finish
        assertNull(log.complete(null));
    }

    @Test
    public void testGetTemporaryFiles() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable1 = sstable(dataFolder, cfs, 0, 128);

        Set<File> tmpFiles = getTemporaryFiles(dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        try(LogTransaction log = createLogTransaction(OperationType.WRITE, cfs.metadata))
        {
            Directories directories = new Directories(cfs.metadata());

            File[] beforeSecondSSTable = dataFolder.tryList(pathname -> !pathname.isDirectory());

            SSTableReader sstable2 = sstable(dataFolder, cfs, 1, 128);
            log.trackNew(sstable2);

            Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
            assertEquals(2, sstables.size());

            // this should contain sstable1, sstable2 and the transaction log file
            File[] afterSecondSSTable = dataFolder.tryList(pathname -> !pathname.isDirectory());

            int numNewFiles = afterSecondSSTable.length - beforeSecondSSTable.length;
            assertEquals(numNewFiles - 1, getAllFilePaths(sstable2).size()); // new files except for transaction log file

            tmpFiles = getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(numNewFiles - 1, tmpFiles.size());

            File ssTable2DataFile = sstable2.descriptor.fileFor(Component.DATA);
            File ssTable2IndexFile = sstable2.descriptor.fileFor(Component.PRIMARY_INDEX);

            assertTrue(tmpFiles.contains(ssTable2DataFile));
            assertTrue(tmpFiles.contains(ssTable2IndexFile));

            List<File> files = directories.sstableLister(Directories.OnTxnErr.THROW).listFiles();
            List<File> filesNoTmp = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true).listFiles();
            assertNotNull(files);
            assertNotNull(filesNoTmp);

            assertTrue(files.contains(ssTable2DataFile));
            assertTrue(files.contains(ssTable2IndexFile));

            assertFalse(filesNoTmp.contains(ssTable2DataFile));
            assertFalse(filesNoTmp.contains(ssTable2IndexFile));

            log.finish();

            //Now it should be empty since the transaction has finished
            tmpFiles = getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(0, tmpFiles.size());

            filesNoTmp = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true).listFiles();
            assertNotNull(filesNoTmp);
            assertTrue(filesNoTmp.contains(ssTable2DataFile));
            assertTrue(filesNoTmp.contains(ssTable2IndexFile));

            sstable1.selfRef().release();
            sstable2.selfRef().release();
        }
    }

    @Test
    public void testGetTemporaryFilesMultipleFolders() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);

        File origiFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        File dataFolder1 = new File(origiFolder, "1");
        File dataFolder2 = new File(origiFolder, "2");
        Files.createDirectories(dataFolder1.toPath());
        Files.createDirectories(dataFolder2.toPath());

        SSTableReader[] sstables = { sstable(dataFolder1, cfs, 0, 128),
                                     sstable(dataFolder1, cfs, 1, 128),
                                     sstable(dataFolder2, cfs, 2, 128),
                                     sstable(dataFolder2, cfs, 3, 128)
        };

        // they should all have the same number of files since they are created in the same way
        int numSStableFiles = getAllFilePaths(sstables[0]).size();

        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        for (File dataFolder : new File[] {dataFolder1, dataFolder2})
        {
            Set<File> tmpFiles = getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(0, tmpFiles.size());
        }

        AbstractLogTransaction.ReaderTidier[] tidiers = { log.obsoleted(sstables[0]), log.obsoleted(sstables[2]) };

        log.trackNew(sstables[1]);
        log.trackNew(sstables[3]);

        for (File dataFolder : new File[] {dataFolder1, dataFolder2})
        {
            Set<File> tmpFiles = getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(numSStableFiles, tmpFiles.size());
        }

        log.finish();

        for (File dataFolder : new File[] {dataFolder1, dataFolder2})
        {
            Set<File> tmpFiles = getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(numSStableFiles, tmpFiles.size());
        }

        sstables[0].markObsolete(tidiers[0]);
        sstables[2].markObsolete(tidiers[1]);

        Arrays.stream(sstables).forEach(s -> s.selfRef().release());
        LifecycleTransaction.waitForDeletions();

        for (File dataFolder : new File[] {dataFolder1, dataFolder2})
        {
            Set<File> tmpFiles = getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(0, tmpFiles.size());
        }

    }

    @Test
    public void testWrongChecksumLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum
                              long now = System.currentTimeMillis();
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("commit:[%d,0,0][%d]", now, 12345678L)));
                          },
                          true);
    }

    @Test
    public void testWrongChecksumSecondFromLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake two lines with invalid checksum
                              long now = System.currentTimeMillis();
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("add:[ma-3-big,%d,4][%d]", now, 12345678L)));
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("commit:[%d,0,0][%d]", now, 12345678L)));
                          },
                          false);
    }

    @Test
    public void testWrongChecksumLastLineMissingFile() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum and also delete one of the old files
                              for (File filePath : getAllFilePaths(s))
                              {
                                  if (filePath.name().endsWith("Data.db"))
                                  {
                                      assertTrue(filePath.tryDelete());
                                      assertNull(t.txnFile().syncDirectory(null));
                                      break;
                                  }
                              }

                              long now = System.currentTimeMillis();
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("commit:[%d,0,0][%d]", now, 12345678L)));
                          },
                          false);
    }

    @Test
    public void testWrongChecksumLastLineWrongRecordFormat() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum and a wrong record format (extra spaces)
                              long now = System.currentTimeMillis();
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("commit:[%d ,0 ,0 ][%d]", now, 12345678L)));
                          },
                          true);
    }

    @Test
    public void testMissingChecksumLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          {
                              // Fake a commit without a checksum
                              long now = System.currentTimeMillis();
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("commit:[%d,0,0]", now)));
                          },
                          true);
    }

    @Test
    public void testMissingChecksumSecondFromLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake two lines without a checksum
                              long now = System.currentTimeMillis();
                              t.logFiles().forEach( f -> FileUtils.append(f, String.format("add:[ma-3-big,%d,4]", now)));
                              t.logFiles().forEach(f -> FileUtils.append(f, String.format("commit:[%d,0,0]", now)));
                          },
                          false);
    }

    @Test
    public void testUnparsableLastRecord() throws IOException
    {
        testCorruptRecord((t, s) -> t.logFiles().forEach(f -> FileUtils.append(f, "commit:[a,b,c][12345678]")), true);
    }

    @Test
    public void testUnparsableFirstRecord() throws IOException
    {
        testCorruptRecord((t, s) -> t.logFiles().forEach(f -> {
            List<String> lines = FileUtils.readLines(f);
            lines.add(0, "add:[a,b,c][12345678]");
            FileUtils.replace(f, lines.toArray(new String[lines.size()]));
        }), false);
    }

    @Test
    public void testUnparsableFirstRecordThrows()
    {
        assertThatThrownBy(() -> {
            testCorruptRecord((t, s) -> t.logFiles().forEach(f -> {
                List<String> lines = FileUtils.readLines(f);
                lines.add(0, "add:[a,b,c][12345678]");
                FileUtils.replace(f, lines.toArray(new String[lines.size()]));
            }), false, Directories.OnTxnErr.THROW);
        }).hasCauseInstanceOf(LogTransaction.CorruptTransactionLogException.class);
    }

    private static void testCorruptRecord(BiConsumer<LogTransaction, SSTableReader> modifier, boolean isRecoverable) throws IOException
    {
        testCorruptRecord(modifier, isRecoverable, Directories.OnTxnErr.IGNORE);
    }

    private static void testCorruptRecord(BiConsumer<LogTransaction, SSTableReader> modifier, boolean isRecoverable, Directories.OnTxnErr onTxnErr) throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableOld = sstable(dataFolder, cfs, 0, 128);
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        // simulate tracking sstables with a committed transaction except the checksum will be wrong
        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstableOld);

        // Modify the transaction log or disk state for sstableOld
        modifier.accept(log, sstableOld);

        // Sync the folder to make sure that later on removeUnfinishedLeftovers picks up
        // any changes to the txn files done by the modifier
        assertNull(log.txnFile().syncDirectory(null));

        assertNull(log.complete(null));

        sstableOld.selfRef().release();
        sstableNew.selfRef().release();

        // The files on disk, for old files make sure to exclude the files that were deleted by the modifier
        Set<File> newFiles = getAllFilePaths(sstableNew);
        Set<File> oldFiles = getAllFilePaths(sstableOld, true);

        //This should filter as in progress since the last record is corrupt
        assertFiles(newFiles, getTemporaryFiles(dataFolder, onTxnErr));
        assertFiles(oldFiles, getFinalFiles(dataFolder));

        if (isRecoverable)
        { // the corruption is recoverable but the commit record is unreadable so the transaction is still in progress

            //This should remove new files
            LifecycleTransaction.removeUnfinishedLeftovers(cfs.metadata());

            // make sure to exclude the old files that were deleted by the modifier
            assertFiles(dataFolder.path(), oldFiles);
        }
        else
        { // if an intermediate line was also modified, it should ignore the tx log file

            //This should not remove any files
            LifecycleTransaction.removeUnfinishedLeftovers(cfs.metadata());

            assertFiles(dataFolder.path(), Sets.newHashSet(Iterables.concat(newFiles,
                                                                            oldFiles,
                                                                            log.logFilePaths())));
        }

        // make sure to run the tidier to avoid any leaks in the logs
        tidier.commit();
    }

    @Test
    public void testDeleteNonExistingFile()
    {
        File nonExisting = new File("a/b/c.txt");
        Assert.assertFalse(nonExisting.exists());
        LogTransaction.delete(nonExisting);
    }

    @Test
    public void testDeleteWithIOException() throws IOException
    {
        File file = Mockito.mock(File.class);
        Path path = Mockito.mock(Path.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        FileSystemProvider fsp = Mockito.mock(FileSystemProvider.class);

        Mockito.when(file.toPath()).thenReturn(path);
        Mockito.when(path.getFileSystem()).thenReturn(fs);
        Mockito.when(fs.provider()).thenReturn(fsp);
        Mockito.doThrow(new IOException("mock exception")).when(fsp).delete(path);

        Assert.assertThrows(FSWriteError.class, () -> LogTransaction.delete(file));
    }

    @Test
    public void testObsoletedDataFileUpdateTimeChanged() throws IOException
    {
        testObsoletedFilesChanged(sstable ->
                                  {
                                      // increase the modification time of the Data file
                                      for (File filePath : getAllFilePaths(sstable))
                                      {
                                          if (filePath.name().endsWith("Data.db"))
                                              assertTrue(filePath.trySetLastModified(System.currentTimeMillis() + 60000)); //one minute later
                                      }
                                  });
    }

    private static void testObsoletedFilesChanged(Consumer<SSTableReader> modifier) throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableOld = sstable(dataFolder, cfs, 0, 128);
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        // simulate tracking sstables with a committed transaction except the checksum will be wrong
        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstableOld);

        //modify the old sstable files
        modifier.accept(sstableOld);

        //Fake a commit
        log.txnFile().commit();

        //This should not remove the old files
        LifecycleTransaction.removeUnfinishedLeftovers(cfs.metadata());

        assertFiles(dataFolder.path(), Sets.newHashSet(Iterables.concat(getAllFilePaths(sstableNew),
                                                                        getAllFilePaths(sstableOld),
                                                                        log.logFilePaths())));

        sstableOld.selfRef().release();
        sstableNew.selfRef().release();

        // complete the transaction to avoid LEAK errors
        assertNull(log.complete(null));

        assertFiles(dataFolder.path(), Sets.newHashSet(Iterables.concat(getAllFilePaths(sstableNew),
                                                                        getAllFilePaths(sstableOld),
                                                                        log.logFilePaths())));

        // make sure to run the tidier to avoid any leaks in the logs
        tidier.commit();
    }

    @Test
    public void testTruncateFileUpdateTime() throws IOException
    {
        // Idea is that we truncate the actual modification time on disk after creating the log file.
        // On java11 this would fail since we would have millisecond resolution in the log file, but
        // then the file gives second resolution.
        testTruncatedModificationTimesHelper(sstable ->
                                  {
                                      // increase the modification time of the Data file
                                      for (File filePath : getAllFilePaths(sstable))
                                      {
                                          long lastModified = filePath.lastModified();
                                          filePath.trySetLastModified(lastModified - (lastModified % 1000));
                                      }
                                  });
    }

    private static void testTruncatedModificationTimesHelper(Consumer<SSTableReader> modifier) throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstableOld = sstable(dataFolder, cfs, 0, 128);
        SSTableReader sstableNew = sstable(dataFolder, cfs, 1, 128);

        // simulate tracking sstables with a committed transaction except the checksum will be wrong
        LogTransaction log = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        AbstractLogTransaction.ReaderTidier tidier = log.obsoleted(sstableOld);

        //modify the old sstable files
        modifier.accept(sstableOld);

        //Fake a commit
        log.txnFile().commit();

        LifecycleTransaction.removeUnfinishedLeftovers(cfs.metadata());

        // only the new files should be there
        assertFiles(dataFolder.path(), Sets.newHashSet(getAllFilePaths(sstableNew)));
        sstableNew.selfRef().release();

        // complete the transaction to avoid LEAK errors
        assertNull(log.complete(null));

        assertFiles(dataFolder.path(), Sets.newHashSet(getAllFilePaths(sstableNew)));

        // make sure to run the tidier to avoid any leaks in the logs
        tidier.commit();
    }

    @Test
    public void testGetTemporaryFilesSafeAfterObsoletion() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable = sstable(dataFolder, cfs, 0, 128);

        LogTransaction logs = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(logs);

        AbstractLogTransaction.ReaderTidier tidier = logs.obsoleted(sstable);

        logs.finish();

        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        // This should race with the asynchronous deletion of txn log files
        // It doesn't matter what it returns but it should not throw because the txn
        // was completed before deleting files (i.e. releasing sstables)
        for (int i = 0; i < 200; i++)
            getTemporaryFiles(dataFolder);
    }

    @Test
    public void testGetTemporaryFilesThrowsIfCompletingAfterObsoletion() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        File dataFolder = new Directories(cfs.metadata()).getDirectoryForNewSSTables();
        SSTableReader sstable = sstable(dataFolder, cfs, 0, 128);

        LogTransaction logs = createLogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(logs);

        AbstractLogTransaction.ReaderTidier tidier = logs.obsoleted(sstable);

        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        LifecycleTransaction.waitForDeletions();

        try
        {
            // This should race with the asynchronous deletion of txn log files
            // it should throw because we are violating the requirement that a transaction must
            // finish before deleting files (i.e. releasing sstables)
            getTemporaryFiles(dataFolder);
            fail("Expected runtime exception");
        }
        catch(RuntimeException e)
        {
            //pass as long as the cause is not an assertion
            assertFalse(e.getCause() instanceof AssertionError);
        }

        logs.finish();
    }

    private static SSTableReader sstable(File dataFolder, ColumnFamilyStore cfs, int generation, int size) throws IOException
    {
        Descriptor descriptor = new Descriptor(dataFolder, cfs.keyspace.getName(), cfs.getTableName(), new SequenceBasedSSTableId(generation), SSTableFormat.Type.BIG);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = descriptor.fileFor(component);
            if (!file.exists())
                assertTrue(file.createFileIfNotExists());

            try (RandomAccessFile raf = new RandomAccessFile(file.toJavaIOFile(), "rw"))
            {
                raf.setLength(size);
            }
        }

        FileHandle dFile = new FileHandle.Builder(descriptor.fileFor(Component.DATA)).complete();
        FileHandle iFile = new FileHandle.Builder(descriptor.fileFor(Component.PRIMARY_INDEX)).complete();

        SerializationHeader header = SerializationHeader.make(cfs.metadata(), Collections.emptyList());
        StatsMetadata metadata = (StatsMetadata) new MetadataCollector(cfs.metadata().comparator)
                                                 .finalizeMetadata(cfs.metadata().partitioner.getClass().getCanonicalName(), 0.01f, -1, null, false, header)
                                                 .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor,
                                                          components,
                                                          cfs.metadata,
                                                          iFile,
                                                          dFile,
                                                          MockSchema.indexSummary.sharedCopy(),
                                                          new AlwaysPresentFilter(),
                                                          1L,
                                                          metadata,
                                                          SSTableReader.OpenReason.NORMAL,
                                                          header);
        reader.first = reader.last = MockSchema.readerBounds(generation);
        return reader;
    }

    static Set<File> getAllFilePaths(SSTableReader sstable)
    {
        return getAllFilePaths(sstable, false);
    }

    /**
     * @param sstable the sstable for which we want to check the files
     * @param existingOnly  if true then only return files that do exist on disk
     *
     * @return the files expected to exist according to the sstable components
     */
    static Set<File> getAllFilePaths(SSTableReader sstable, boolean existingOnly)
    {
        Set<File> ret = new HashSet<>(sstable.components().size());
        for (Component component : sstable.components())
        {
            File path = sstable.descriptor.fileFor(component);

            // other components are expected to exist unless the test is explicitly checking
            // a case where a component was not created, in which case existingOnly will be true
            // and the file will only be added if it exists
            if (!existingOnly || path.exists())
                ret.add(path);
        }

        return ret;
    }

    private static void assertFiles(String dirPath, Set<File> expectedFiles) throws IOException
    {
        assertFiles(dirPath, expectedFiles, false);
    }

    private static void assertFiles(String dirPath, Set<File> expectedFiles, boolean excludeNonExistingFiles) throws IOException
    {
        LifecycleTransaction.waitForDeletions();

        File dir = new File(dirPath).toCanonical();
        File[] files = dir.tryList();
        if (files != null)
        {
            for (File file : files)
            {
                if (file.isDirectory())
                    continue;

                assertTrue(String.format("%s not in [%s]", file, expectedFiles), expectedFiles.contains(file));
                expectedFiles.remove(file);
            }
        }

        if (excludeNonExistingFiles)
        {
            for (File file : expectedFiles)
            {
                if (!file.exists())
                    expectedFiles.remove(file);
            }
        }

        assertTrue(expectedFiles.toString(), expectedFiles.isEmpty());
    }

    // Check either that a temporary file is expected to exist (in the existingFiles) or that
    // it does not exist any longer (on Windows we need to check File.exists() because a list
    // might return a file as existing even if it does not)
    private static void assertFiles(Iterable<File> existingFiles, Set<File> temporaryFiles)
    {
        for (File filePath : existingFiles)
        {
            assertTrue(filePath.toString(), temporaryFiles.contains(filePath));
            temporaryFiles.remove(filePath);
        }

        for (File file : temporaryFiles)
        {
            if (!file.exists())
                temporaryFiles.remove(file);
        }

        assertTrue(temporaryFiles.toString(), temporaryFiles.isEmpty());
    }

    static Set<File> getTemporaryFiles(File folder)
    {
        return listFiles(folder, Directories.FileType.TEMPORARY);
    }

    static Set<File> getTemporaryFiles(File folder, Directories.OnTxnErr onTxnErr)
    {
        return listFiles(folder, onTxnErr, Directories.FileType.TEMPORARY);
    }

    static Set<File> getFinalFiles(File folder)
    {
        return listFiles(folder, Directories.FileType.FINAL);
    }

    // Used by listFiles - this test is deliberately racing with files being
    // removed and cleaned up, so it is possible that files are removed between listing and getting their
    // canonical names, in which case they can be dropped from the stream.
    private static Stream<File> toCanonicalIgnoringNotFound(File file)
    {
        try
        {
            return Stream.of(file.toCanonical());
        }
        catch (UncheckedIOException io)
        {
            if (Throwables.isCausedBy(io, t -> t instanceof NoSuchFileException))
            {
                return Stream.empty();
            }
            else
            {
                throw io;
            }
        }
    }

    static Set<File> listFiles(File folder, Directories.FileType... types)
    {
        return listFiles(folder, Directories.OnTxnErr.IGNORE, types);
    }

    static Set<File> listFiles(File folder, Directories.OnTxnErr onTxnErr, Directories.FileType... types)
    {
        Collection<Directories.FileType> match = Arrays.asList(types);
        return ILogTransactionsFactory.instance.createLogAwareFileLister()
                                               .list(folder.toPath(), (file, type) -> match.contains(type), onTxnErr)
                                               .stream()
                                               .flatMap(LogTransactionTest::toCanonicalIgnoringNotFound)
                                               .collect(Collectors.toSet());
    }
}
