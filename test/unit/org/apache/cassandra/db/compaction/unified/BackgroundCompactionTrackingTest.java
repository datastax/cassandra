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

package org.apache.cassandra.db.compaction.unified;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionStrategyStatistics;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.compaction.UnifiedCompactionStatistics;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(BMUnitRunner.class)
@BMUnitConfig(debug=true)
@BMRule(
name = "Get stats before task completion",
targetClass = "org.apache.cassandra.db.compaction.ActiveOperations",
targetMethod = "completeOperation",
targetLocation = "AT ENTRY",
action = "org.apache.cassandra.db.compaction.unified.BackgroundCompactionTrackingTest.getStats()"
)
public class BackgroundCompactionTrackingTest extends CQLTester
{
    // Get rid of commitlog noise
    @Before
    public void disableCommitlog()
    {
        schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH durable_writes = false");
    }
    @After
    public void enableCommitlog()
    {
        schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH durable_writes = true");
    }

    @Test
    public void testBackgroundCompactionTrackingIterators() throws InterruptedException
    {
        testBackgroundCompactionTracking(false, false, 5);
    }

    @Test
    public void testBackgroundCompactionTrackingIteratorsParallelized() throws InterruptedException
    {
        testBackgroundCompactionTracking(true, false, 5);
    }

    @Test
    public void testBackgroundCompactionTrackingCursors() throws InterruptedException
    {
        testBackgroundCompactionTracking(false, true,5);
    }

    @Test
    public void testBackgroundCompactionTrackingCursorsParallelized() throws InterruptedException
    {
        testBackgroundCompactionTracking(true, true,5);
    }

    public void testBackgroundCompactionTracking(boolean parallelize, boolean useCursors, int shards) throws InterruptedException
    {
        CompactionManager.instance.setMaximumCompactorThreads(50);
        CompactionManager.instance.setCoreCompactorThreads(50);
        CassandraRelevantProperties.ALLOW_CURSOR_COMPACTION.setBoolean(useCursors);
        String table = createTable(String.format("CREATE TABLE %%s (k int, t int, v blob, PRIMARY KEY (k, t))" +
                                                 " with compaction = {" +
                                                 "'class': 'UnifiedCompactionStrategy', " +
                                                 "'parallelize_output_shards': '%s', " +
                                                 "'num_shards': %d, " +
                                                 "'min_sstable_size': '1KiB', " +
                                                 "'log': 'all', " +
                                                 "'scaling_parameters': 'T4, T7'" +
                                   "}",
                                   parallelize, shards));
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        cfs.disableAutoCompaction();
        strategy = cfs.getCompactionStrategy();
        int partitions = 5000;
        int rows_per_partition = 10;

        for (int iter = 1; iter <= 5; ++iter)
        {
            byte [] payload = new byte[5000];
            new Random(42).nextBytes(payload);
            ByteBuffer b = ByteBuffer.wrap(payload);
            Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());

            for (int i = 0; i < partitions; i++)
            {
                for (int j = 0; j < rows_per_partition; j++)
                    execute(String.format("INSERT INTO %s.%s(k, t, v) VALUES (?, ?, ?)", KEYSPACE, table), i, j, b);

                if ((i + 1) % ((partitions  + 3) / 4) == 0)
                    cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            operations = new ArrayList<>();
            statistics = new ArrayList<>();
            Set<SSTableReader> newSSTables = new HashSet<>(cfs.getLiveSSTables());
            newSSTables.removeAll(before);
            long totalSize = newSSTables.stream().mapToLong(SSTableReader::onDiskLength).sum();
            long uncompressedSize = newSSTables.stream().mapToLong(SSTableReader::uncompressedLength).sum();

            cfs.enableAutoCompaction(true); // since the trigger is hit, this initiates an L0 compaction
            CompactionManager.instance.submitBackground(cfs).await();    // no more compactions to run, refresh stats
            cfs.disableAutoCompaction();

            // Check that the background compactions state is correct during the compaction
            Assert.assertTrue("Byteman rule did not fire", !operations.isEmpty());
            printStats();
            assertEquals(1, operations.size());
            var ops = operations.get(0)
                                .stream()
                                .filter(op -> op.metadata() == cfs.metadata())
                                .collect(Collectors.toList());
            assertEquals(1, ops.size());
            var op = ops.get(0);
            {
                assertSame(cfs.metadata(), op.metadata());

                assertEquals(uncompressedSize, op.total());
                assertEquals(uncompressedSize, op.completed());
            }

            var stats = statistics.get(0).get(0); // unrepaired
            if (stats.aggregates().size() > 1)
            {
                var L1 = (UnifiedCompactionStatistics) stats.aggregates().get(1);
                assertEquals(1, L1.bucket());
                assertEquals(shards * (iter - 1), L1.numSSTables());  // pre-compaction state
                assertEquals(totalSize * 1.0 * (iter - 1), L1.sizeInBytes(), totalSize * 0.03);
                assertEquals(iter - 1, L1.maxOverlap());
            }
            var L0 = (UnifiedCompactionStatistics) stats.aggregates().get(0);
            assertEquals(0, L0.bucket());
            assertEquals(totalSize * 1.0, L0.sizeInBytes(), totalSize * 0.03);
            assertEquals(uncompressedSize * 1.0, L0.tot(), uncompressedSize * 0.03);
            assertEquals(uncompressedSize * 1.0, L0.written(), uncompressedSize * 0.03);
            assertEquals(uncompressedSize * 1.0, L0.read(), uncompressedSize * 0.03);
            assertEquals(1, L0.numCompactionsInProgress());
            assertEquals(4, L0.numCompactingSSTables());
            assertEquals(4, L0.numSSTables());
            assertEquals(4, L0.maxOverlap());

            assertEquals(iter * shards, cfs.getLiveSSTables().size());

            // Check that the background compactions state is correct after the compaction
            operations.clear();
            statistics.clear();
            getStats();
            printStats();
            assertEquals(0, operations.get(0).size());
            stats = statistics.get(statistics.size() - 1).get(0); // unrepaired
            var L1 = (UnifiedCompactionStatistics) stats.aggregates().get(0);
            assertEquals(1, L1.bucket());
            assertEquals(shards * iter, L1.numSSTables());  // pre-compaction state
            assertEquals(totalSize * 1.0 * iter, L1.sizeInBytes(), totalSize * 0.03);
            assertEquals(iter, L1.maxOverlap());
        }
    }

    private void printStats()
    {
        for (int i = 0; i < operations.size(); ++i)
        {
            System.out.println(operations.get(i).stream().map(Object::toString).collect(Collectors.joining("\n")));
            System.out.println(statistics.get(i));
        }
    }

    public static synchronized void getStats()
    {
        operations.add(CompactionManager.instance.getSSTableTasks()
                                                 .stream()
                                                 .map(BackgroundCompactionTrackingTest::snapshot)
                                                 .collect(Collectors.toList()));
        statistics.add(strategy.getStatistics());
    }

    private static TableOperation.Progress snapshot(TableOperation.Progress progress)
    {
        // Take a snapshot to make sure we are capturing the values at the time ActiveOperations is called.
        // This is to make sure we report the completed state then, and not end up okay because they were corrected
        // when some component closed at a later time.
        return new AbstractTableOperation.OperationProgress(progress.metadata(),
                                                            progress.operationType(),
                                                            progress.completed(),
                                                            progress.total(),
                                                            progress.unit(),
                                                            progress.operationId(),
                                                            progress.sstables(),
                                                            null);
    }

    static CompactionStrategy strategy;
    static List<List<CompactionStrategyStatistics>> statistics;
    static List<List<TableOperation.Progress>> operations;
}
