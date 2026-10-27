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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.concurrent.Ref;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.loadSchema;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Failed periodic early opens (DSP-25176) must not abort the compaction, lose data or leak readers.
 */
@RunWith(BMUnitRunner.class)
public class EarlyOpenWithBytemanTest
{
    public static AtomicInteger earlyOpenAttempts = new AtomicInteger(0);
    public static AtomicInteger firstKeyBeyondCalls = new AtomicInteger(0);
    public static AtomicInteger successesAfterFailure = new AtomicInteger(0);
    public static AtomicInteger failedEarlyOpenCounter = new AtomicInteger(0);
    public static AtomicBoolean canTriggerException = new AtomicBoolean(false);

    private static final String KEYSPACE = "early_open_byteman_test";
    private static final String CF = "Standard1";
    private static final int EARLY_OPEN_INTERVAL_MIB = 1;
    private static final int PARTITIONS = 100_000;

    private final List<Object> leaks = new CopyOnWriteArrayList<>();
    private boolean savedDisableEarlyOpening;
    private int savedInterval;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        loadSchema();
        createKeyspace(KEYSPACE, KeyspaceParams.simple(1), standardCFMD(KEYSPACE, CF));
    }

    @Before
    public void setUp()
    {
        savedDisableEarlyOpening = SSTableRewriter.disableEarlyOpeningForTests;
        savedInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        // ensure early opening is enabled, with a small interval to trigger it quickly
        SSTableRewriter.disableEarlyOpeningForTests = false;
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(EARLY_OPEN_INTERVAL_MIB);
        CompactionManager.instance.disableAutoCompaction();
        earlyOpenAttempts.set(0);
        firstKeyBeyondCalls.set(0);
        successesAfterFailure.set(0);
        failedEarlyOpenCounter.set(0);
        Ref.setOnLeak(leaks::add);
    }

    @After
    public void tearDown()
    {
        Ref.setOnLeak(null);
        canTriggerException.set(false);
        SSTableRewriter.disableEarlyOpeningForTests = savedDisableEarlyOpening;
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(savedInterval);
    }

    // Make every odd periodic early open fail in moveStarts, after the early reader and the clone of the first
    // original with a moved start are staged (both originals span the same range, so each attempt clones both), so
    // that the staged readers must be released. The even ones succeed after a rolled back attempt, cloning the same
    // originals again, and their readers must be released normally when replaced.
    @Test
    @BMRules(rules = { @BMRule(name = "count_early_open_attempts",
                               targetClass = "org.apache.cassandra.io.sstable.SSTableRewriter",
                               targetMethod = "moveStarts",
                               targetLocation = "AT ENTRY",
                               condition = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.canTriggerException.get() && " +
                                           "callerMatches(\".*maybeReopenEarly.*\")",
                               action = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.earlyOpenAttempts.incrementAndGet();" +
                                        "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.firstKeyBeyondCalls.set(0);"),
                       @BMRule(name = "fail_second_move_of_every_odd_attempt",
                               targetClass = "org.apache.cassandra.io.sstable.SSTableRewriter",
                               targetMethod = "moveStarts",
                               targetLocation = "AT INVOKE org.apache.cassandra.io.sstable.format.SSTableReader.firstKeyBeyond",
                               condition = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.canTriggerException.get() && " +
                                           "callerMatches(\".*maybeReopenEarly.*\") && " +
                                           "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.earlyOpenAttempts.get() % 2 == 1 && " +
                                           "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.firstKeyBeyondCalls.incrementAndGet() == 2",
                               action = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.failedEarlyOpenCounter.incrementAndGet();" +
                                        "throw new java.lang.RuntimeException(\"!!!Test simulated corruption!!!\");"),
                       @BMRule(name = "count_successes_after_a_failure",
                               targetClass = "org.apache.cassandra.db.lifecycle.LifecycleTransaction",
                               targetMethod = "checkpoint()",
                               targetLocation = "AT ENTRY",
                               condition = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.canTriggerException.get() && " +
                                           "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.failedEarlyOpenCounter.get() > 0 && " +
                                           "callerMatches(\".*maybeReopenEarly.*\", false, 1, 3)",
                               action = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.successesAfterFailure.incrementAndGet();") })
    public void testExceptionsOnLifecycleBoundaries() throws Exception
    {
        compactWithFailures();
        assertTrue("No periodic early open succeeded after a failed one: " + earlyOpenAttempts.get() + " attempts, " +
                   failedEarlyOpenCounter.get() + " failures",
                   successesAfterFailure.get() > 0);
    }

    // Make every periodic early open fail in LifecycleTransaction.update, before the early reader is staged: the
    // reader must be released by the rewriter, and the failed early open must not be retried at every partition.
    @Test
    @BMRule(name = "fail_update_of_early_reader",
            targetClass = "org.apache.cassandra.db.lifecycle.LifecycleTransaction",
            targetMethod = "update(org.apache.cassandra.io.sstable.format.SSTableReader, boolean)",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.canTriggerException.get() && " +
                        "!$2 && callerMatches(\".*maybeReopenEarly.*\", false, 1, 3)",
            action = "org.apache.cassandra.io.sstable.EarlyOpenWithBytemanTest.failedEarlyOpenCounter.incrementAndGet();" +
                     "throw new java.lang.RuntimeException(\"!!!Test simulated update failure!!!\");")
    public void testUpdateFailureOnEarlyOpen() throws Exception
    {
        ColumnFamilyStore cfs = compactWithFailures();

        // Each failure is an early reader that was opened (a partial index snapshot, a data file handle, a bloom
        // filter copy) and discarded. Early readers are opened once per interval of written data, not again at every
        // flush after a failure.
        long written = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            written += sstable.uncompressedLength();
        long maxEarlyOpens = written / (EARLY_OPEN_INTERVAL_MIB << 20);
        assertTrue("Early open failed " + failedEarlyOpenCounter.get() + " times for " + written +
                   " bytes written, expected at most " + maxEarlyOpens,
                   failedEarlyOpenCounter.get() <= maxEarlyOpens);
    }

    private ColumnFamilyStore compactWithFailures() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);
        // unlike clearUnsafe, this releases the readers left by a previous test, which would otherwise be reported
        // as leaks here
        cfs.truncateBlocking();

        // insert enough data to trigger early opening, twice to have multiple source sstables
        ScrubTest.fillCF(cfs, PARTITIONS);
        ScrubTest.fillCF(cfs, PARTITIONS);
        ScrubTest.assertOrderedAll(cfs, PARTITIONS);

        canTriggerException.set(true);
        cfs.forceMajorCompaction();
        canTriggerException.set(false);
        assertTrue("No early open failure was injected", failedEarlyOpenCounter.get() > 0);

        // the data is all there
        ScrubTest.assertOrderedAll(cfs, PARTITIONS);

        // and no reader was leaked: those that were not released are reported once collected
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (leaks.isEmpty() && System.nanoTime() < deadline)
        {
            System.gc();
            Thread.sleep(100);
        }
        assertEquals("Leaked references: " + leaks, 0, leaks.size());
        return cfs;
    }
}
