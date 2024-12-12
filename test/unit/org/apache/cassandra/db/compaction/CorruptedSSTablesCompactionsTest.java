package org.apache.cassandra.db.compaction;
/*
 * 
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
 * 
 */


import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.PartialLifecycleTransaction;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.compaction.SSTableCursor;
import org.apache.cassandra.io.sstable.compaction.SortedStringTableCursor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.utils.ApproximateTime.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CorruptedSSTablesCompactionsTest
{
    private static final Logger logger = LoggerFactory.getLogger(CorruptedSSTablesCompactionsTest.class);

    private static Random random;

    private static final String KEYSPACE1 = "CorruptedSSTablesCompactionsTest";
    private static final String STANDARD_STCS = "Standard_STCS";
    private static final String STANDARD_LCS = "Standard_LCS";
    private static final String STANDARD_UCS = "Standard_UCS";
    private static final String STANDARD_UCS_PARALLEL = "Standard_UCS_Parallel";
    private static int maxValueSize;

    @After
    public void leakDetect() throws InterruptedException
    {
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(10);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        long seed = nanoTime();

        //long seed = 754271160974509L; // CASSANDRA-9530: use this seed to reproduce compaction failures if reading empty rows
        //long seed = 2080431860597L; // CASSANDRA-12359: use this seed to reproduce undetected corruptions
        //long seed = 9823169134884L; // CASSANDRA-15879: use this seed to reproduce duplicate clusterings

        logger.info("Seed {}", seed);
        random = new Random(seed);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    makeTable(STANDARD_STCS).compaction(CompactionParams.stcs(Collections.emptyMap())),
                                    makeTable(STANDARD_LCS).compaction(CompactionParams.lcs(Collections.emptyMap())),
                                    makeTable(STANDARD_UCS).compaction(CompactionParams.ucs(Collections.emptyMap())),
                                    makeTable(STANDARD_UCS_PARALLEL).compaction(CompactionParams.ucs(new HashMap<>(ImmutableMap.of("min_sstable_size", "1KiB")))));

        maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024);
        closeStdErr();
    }

    /**
     * Return a table metadata, we use types with fixed size to increase the chance of detecting corrupt data
     */
    private static TableMetadata.Builder makeTable(String tableName)
    {
        return SchemaLoader.standardCFMD(KEYSPACE1, tableName, 1, LongType.instance, LongType.instance, LongType.instance);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setMaxValueSize(maxValueSize);
    }

    public static void closeStdErr()
    {
        // These tests generate an error message per CorruptSSTableException since it goes through
        // DebuggableThreadPoolExecutor, which will log it in afterExecute.  We could stop that by
        // creating custom CompactionStrategy and CompactionTask classes, but that's kind of a
        // ridiculous amount of effort, especially since those aren't really intended to be wrapped
        // like that.
        System.err.close();
    }

    @Test
    public void testCorruptedSSTablesWithSizeTieredCompactionStrategy() throws Throwable
    {
        testCorruptedSSTables(STANDARD_STCS);
    }

    @Test
    public void testCorruptedSSTablesWithLeveledCompactionStrategy() throws Throwable
    {
        testCorruptedSSTables(STANDARD_LCS);
    }

    @Test
    public void testCorruptedSSTablesWithUnifiedCompactionStrategy() throws Throwable
    {
        testCorruptedSSTables(STANDARD_UCS);
    }

    @Test
    public void testCorruptedSSTablesWithUnifiedCompactionStrategyParallelized() throws Throwable
    {
        testCorruptedSSTables(STANDARD_UCS_PARALLEL);
    }

    static final int COMPACTION_FAIL = -1;

    public void testCorruptedSSTables(String tableName) throws Throwable
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        final int ROWS_PER_SSTABLE = 1000; // enough data so that compression does not try to open the same chunk for multiple partial compaction tasks
        final int SSTABLES = 25;
        final int SSTABLES_TO_CORRUPT = 8;

        assertTrue(String.format("Not enough sstables (%d), expected at least %d sstables to corrupt", SSTABLES, SSTABLES_TO_CORRUPT),
                   SSTABLES > SSTABLES_TO_CORRUPT);

        // disable compaction while flushing
        cfs.disableAutoCompaction();
        //test index corruption
        //now create a few new SSTables
        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<>();

        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                DecoratedKey key = Util.dk(String.valueOf(i), LongType.instance);
                long timestamp = j * ROWS_PER_SSTABLE + i;
                new RowUpdateBuilder(cfs.metadata(), timestamp, key.getKey())
                        .clustering(Long.valueOf(i))
                        .add("val", Long.valueOf(i))
                        .build()
                        .applyUnsafe();
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                inserted.add(key);
            }
            cfs.forceBlockingFlush(UNIT_TESTS);
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());
        }

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        int currentSSTable = 0;

        // corrupt first 'sstablesToCorrupt' SSTables
        for (SSTableReader sstable : sstables)
        {
            if (currentSSTable + 1 > SSTABLES_TO_CORRUPT)
                break;

            do
            {
                RandomAccessFile raf = null;

                try
                {
                    int corruptionSize = 25;
                    raf = new RandomAccessFile(sstable.getFilename(), "rw");
                    assertNotNull(raf);
                    assertTrue(raf.length() > corruptionSize);
                    long pos = random.nextInt((int) (raf.length() - corruptionSize));
                    logger.info("Corrupting sstable {} [{}] at pos {} / {}", currentSSTable, sstable.getFilename(), pos, raf.length());
                    raf.seek(pos);
                    // We want to write something large enough that the corruption cannot get undetected
                    // (even without compression)
                    byte[] corruption = new byte[corruptionSize];
                    random.nextBytes(corruption);
                    raf.write(corruption);
                }
                finally
                {
                FileUtils.closeQuietly(raf);
                }
                if (ChunkCache.instance != null)
                    ChunkCache.instance.invalidateFile(sstable.getFilename());
            }
            while (readsWithoutError(sstable));

            currentSSTable++;
        }

        int failures = 0;

        // in case something will go wrong we don't want to loop forever using for (;;)
        for (int i = 0; i < sstables.size(); i++)
        {
            try
            {
                cfs.forceMajorCompaction();
                break; // After all corrupted sstables are marked as such, compaction of the rest should succeed.
            }
            catch (Throwable e)
            {
                System.out.println(e);
                // This is the expected path.
                int fails = processException(e);
                if (fails == COMPACTION_FAIL)
                {
                    logger.info("Completing test after {} failures because of non-sstable-specific AssertionError\n{}", failures, e);
                    failures = SSTABLES_TO_CORRUPT;
                    break;
                }
                else
                {
                    failures += fails;
                }
            }
        }

        cfs.truncateBlocking();
        assertEquals(SSTABLES_TO_CORRUPT, failures);
    }

    private int processException(Throwable e) throws Throwable
    {
        Throwable cause = e;
        int failures = 0;
        boolean foundCause = false;
        while (cause != null)
        {
            // The SSTable should be marked corrupted, and retrying the compaction
            // should move on to the next corruption.
            if (cause instanceof CorruptSSTableException)
            {
                ++failures;
                foundCause = true;
                break;
            }

            // If we are compacting with cursors, we may be unable to identify the sstable at the source of the
            // corruption, sometimes failing with an AssertionError in the compaction class. If so, complete the
            // test.
            if (CassandraRelevantProperties.CURSORS_ENABLED.getBoolean() &&
                cause instanceof AssertionError &&
                cause.getMessage().contains("nodetool scrub"))
            {
                return COMPACTION_FAIL;
            }

            // If the compactions are parallelized, the error message should contain all failures of the current path.
            for (var t : cause.getSuppressed())
            {
                final int childFailures = processException(t);
                if (childFailures == COMPACTION_FAIL)
                    return COMPACTION_FAIL;
                failures += childFailures;
            }
            if (cause instanceof PartialLifecycleTransaction.AbortedException)
            {
                foundCause = true;
                break;
            }
            cause = cause.getCause();
        }
        if (!foundCause)
            throw e;
        return failures;
    }

    private boolean readsWithoutError(SSTableReader sstable)
    {
        if (CassandraRelevantProperties.CURSORS_ENABLED.getBoolean())
            return readsWithoutErrorCursor(sstable);
        else
            return readsWithoutErrorIterator(sstable);
    }

    private boolean readsWithoutErrorIterator(SSTableReader sstable)
    {
        try
        {
            ISSTableScanner scanner = sstable.getScanner();
            while (scanner.hasNext())
            {
                UnfilteredRowIterator iter = scanner.next();
                while (iter.hasNext())
                    iter.next();
            }
            return true;
        }
        catch (Throwable t)
        {
            sstable.unmarkSuspect();
            return false;
        }
    }

    private boolean readsWithoutErrorCursor(SSTableReader sstable)
    {
        try
        {
            SSTableCursor cursor = new SortedStringTableCursor(sstable);
            while (cursor.advance() != SSTableCursor.Type.EXHAUSTED) {}
            return true;
        }
        catch (Throwable t)
        {
            sstable.unmarkSuspect();
            return false;
        }
    }
}
