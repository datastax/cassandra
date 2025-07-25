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
package org.apache.cassandra.index.sai.functional;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.metrics.AbstractMetricsTest;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionTest extends AbstractMetricsTest
{
    @Test
    public void testAntiCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        IndexContext numericIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        verifyNoIndexFiles();

        // create 100 rows in 1 sstable
        int num = 100;
        for (int i = 0; i < num; i++)
            execute( "INSERT INTO %s (id1, v1) VALUES (?, 0)", Integer.toString(i));
        flush();

        // verify 1 sstable index
        assertNumRows(num, "SELECT * FROM %%s WHERE v1 >= 0");
        verifyIndexFiles(numericIndexContext, null, 1, 0);
        verifySSTableIndexes(numericIndexContext.getIndexName(), 1);

        // split sstable into repaired and unrepaired
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Range<Token> range = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                         DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes("30")));
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("10.0.0.1");
            UUID parentRepairSession = UUID.randomUUID();
            ActiveRepairService.instance.registerParentRepairSession(parentRepairSession,
                                                                     endpoint,
                                                                     Lists.newArrayList(cfs),
                                                                     Collections.singleton(range),
                                                                     true,
                                                                     1000,
                                                                     false,
                                                                     PreviewKind.NONE);
            RangesAtEndpoint replicas = RangesAtEndpoint.builder(endpoint).add(Replica.fullReplica(endpoint, range)).build();
            CompactionManager.instance.performAnticompaction(cfs, replicas, refs, txn, parentRepairSession, () -> false);
        }

        // verify 2 sstable indexes
        assertNumRows(num, "SELECT * FROM %%s WHERE v1 >= 0");
        waitForAssert(() -> verifyIndexFiles(numericIndexContext, null, 2, 0));
        verifySSTableIndexes(numericIndexContext.getIndexName(), 2);

        // index components are included after anti-compaction
        verifyIndexComponentsIncludedInSSTable();
    }

    @Test
    public void testConcurrentQueryWithCompaction()
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        int num = 10;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
            flush();
        }

        TestWithConcurrentVerification compactionTest = new TestWithConcurrentVerification(() -> {
            for (int i = 0; i < 30; i++)
            {
                try
                {
                    assertNumRows(num, "SELECT id1 FROM %%s WHERE v1>=0");
                    assertNumRows(num, "SELECT id1 FROM %%s WHERE v2='0'");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }
        }, this::upgradeSSTables);

        compactionTest.start();

        verifySSTableIndexes(v1IndexName, num);
        verifySSTableIndexes(v2IndexName, num);
    }

    @Test
    public void testCompactionWithDisabledIndexReads() throws Throwable
    {
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(true);
        try
        {
            createTable(CREATE_TABLE_TEMPLATE);
            String v1IndexName = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));
            String v2IndexName = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v2"));

            int num = 10;
            for (int i = 0; i < num; i++)
            {
                execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
                flush();
            }

            startCompaction();
            waitForCompactionsFinished();

            // Compactions should run fine and create the expected number of index files for each index (1 since we
            // compacted everything into a single sstable).
            verifySSTableIndexes(v1IndexName, 1);
            verifySSTableIndexes(v2IndexName, 1);

            // But index queries should not be allowed.
            assertThrows(IndexNotAvailableException.class, () -> executeInternal("SELECT id1 FROM %s WHERE v1>=0"));
        }
        finally
        {
            CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(false);
        }
    }

    @Test
    public void testAbortCompactionWithEarlyOpenSSTables() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        int sstables = 2;
        int num = 10;
        for (int i = 0; i < num; i++)
        {
            if (i == num / sstables)
                flush();

            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        // make sure early open is triggered
        Injections.Counter earlyOpenCounter = Injections.newCounter("early_open_counter")
                                                        .add(InvokePointBuilder.newInvokePoint().onClass(LifecycleTransaction.class).onMethod("checkpoint"))
                                                        .build();

        // abort compaction
        String errMessage = "Injected failure!";
        Injection failSSTableCompaction = Injections.newCustom("fail_sstable_compaction")
                                                    .add(InvokePointBuilder.newInvokePoint().onClass(SSTableWriter.class).onMethod("prepareToCommit"))
                                                    .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote(errMessage)))
                                                    .build();

        try
        {
            Injections.inject(failSSTableCompaction, earlyOpenCounter);

            compact();
            fail("Expected compaction being interrupted");
        }
        catch (Throwable e)
        {
            while (e.getCause() != null)
                e = e.getCause();

            assertTrue(String.format("Expected %s, but got %s", errMessage, e.getMessage()), e.getMessage().contains(errMessage));
        }
        finally
        {
            earlyOpenCounter.disable();
            failSSTableCompaction.disable();
        }
        assertNotEquals(0, earlyOpenCounter.get());

        // verify indexes are working
        assertNumRows(num, "SELECT id1 FROM %%s WHERE v1=0");
        assertNumRows(num, "SELECT id1 FROM %%s WHERE v2='0'");
        verifySSTableIndexes(v1IndexName, sstables);
        verifySSTableIndexes(v2IndexName, sstables);
    }

    @Test
    // This test probably never worked, but initially `TestWithConcurrentVerification` was also not working properly
    // and was ignoring errors throw during the `verificationTask`, hidding the problem with this test.
    // `TestWithConcurrentVerification` was fixed, leading to this failing, and it's not immediately clear what the
    // right fix should be, so for now it is ignored, but it should eventually be fixed.
    @Ignore
    public void testConcurrentIndexBuildWithCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        // prepare data into one sstable
        int sstables = 1;
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        Injections.Barrier compactionLatch =
        Injections.newBarrier("pause_compaction", 2, false)
                  .add(InvokePointBuilder.newInvokePoint().onClass(CompactionManager.class).onMethod("performSSTableRewrite"))
                  .build();

        try
        {
            // stop in-progress compaction
            Injections.inject(compactionLatch);

            TestWithConcurrentVerification compactionTask = new TestWithConcurrentVerification(
                    () -> {
                        try
                        {
                            upgradeSSTables();
                            fail("Expected CompactionInterruptedException");
                        }
                        catch (Exception e)
                        {
                            assertTrue("Expected CompactionInterruptedException, but got " + e,
                                       Throwables.isCausedBy(e, CompactionInterruptedException.class));
                        }
                    },
                    () -> {
                        try
                        {
                            waitForAssert(() -> Assert.assertEquals(1, compactionLatch.getCount()));

                            // build indexes on SSTables that will be compacted soon
                            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
                            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

                            // continue in-progress compaction
                            compactionLatch.countDown();
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException(e);
                        }
                    }, -1 // run verification task once
            );

            compactionTask.start();
        }
        finally
        {
            compactionLatch.disable();
        }

        assertNumRows(num, "SELECT id1 FROM %%s WHERE v1>=0");
        assertNumRows(num, "SELECT id1 FROM %%s WHERE v2='0'");
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), sstables);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), sstables);
    }

    @Test
    public void testConcurrentIndexDropWithCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));


        // Load data into a single SSTable...
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        assertNotEquals(0, getOpenIndexFiles());
        assertNotEquals(0, getDiskUsage());

        Injections.Barrier compactionLatch =
                Injections.newBarrier("pause_compaction_for_drop", 2, false)
                          .add(InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("addRow"))
                          .build();
        try
        {
            // pause in-progress compaction
            Injections.inject(compactionLatch);

            TestWithConcurrentVerification compactionTask = new TestWithConcurrentVerification(
                    () -> upgradeSSTables(),
                    () -> {
                        try
                        {
                            waitForAssert(() -> Assert.assertEquals(1, compactionLatch.getCount()));

                            // drop all indexes
                            dropIndex("DROP INDEX %s." + v1IndexName);
                            dropIndex("DROP INDEX %s." + v2IndexName);

                            // continue in-progress compaction
                            compactionLatch.countDown();
                        }
                        catch (Throwable e)
                        {
                            throw new RuntimeException(e);
                        }
                    }, -1 // run verification task once
            );

            compactionTask.start();
            waitForCompactionsFinished();
        }
        finally
        {
            compactionLatch.disable();
        }

        // verify index group metrics are cleared.
        assertThatThrownBy(this::getOpenIndexFiles).hasRootCauseInstanceOf(InstanceNotFoundException.class);
        assertThatThrownBy(this::getDiskUsage).hasRootCauseInstanceOf(InstanceNotFoundException.class);

        // verify indexes are dropped
        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v1>=0"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
        assertThatThrownBy(() -> executeNet("SELECT id1 FROM %s WHERE v2='0'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    @Test
    public void testSegmentBuilderFlushWithShardedCompaction() throws Throwable
    {
        int shards = 64;
        String createTable = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                             "{'class' : 'UnifiedCompactionStrategy', 'enabled' : false, 'base_shard_count': " + shards + ", 'min_sstable_size': '1KiB' }";
        createTable(createTable);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        disableCompaction(keyspace(), currentTable());

        int rowsPerSSTable = 2000;
        int numSSTables = 4;
        int key = 0;
        for (int s = 0; s < numSSTables; s++)
        {
            for (int i = 0; i < rowsPerSSTable; i++)
            {
                execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '01e2wefnewirui32e21e21wre')", Integer.toString(key++));
            }
            flush();
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> future = executor.submit(() -> {
                getCurrentColumnFamilyStore().forceMajorCompaction(false, 1);
                waitForCompactions();
            });

            // verify that it's not accumulating segment builders
            while (!future.isDone())
            {
                // ACTIVE_BUILDER_COUNT starts from 1. There are 2 segments for 2 indexes
                assertThat(SegmentBuilder.ACTIVE_BUILDER_COUNT.get()).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(3);
            }
            future.get(30, TimeUnit.SECONDS);

            // verify results are sharded
            assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(shards);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    protected int getOpenIndexFiles()
    {
        return (int) getMetricValue(objectNameNoIndex("OpenIndexFiles", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }

    protected long getDiskUsage()
    {
        return (long) getMetricValue(objectNameNoIndex("DiskUsedBytes", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }
}
