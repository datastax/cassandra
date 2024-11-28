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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyFactory;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.ShardManagerNoDisks;
import org.apache.cassandra.db.compaction.SharedCompactionObserver;
import org.apache.cassandra.db.compaction.SharedCompactionProgress;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.lifecycle.CompositeLifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.PartialLifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParallelizedTasksTest extends ShardingTestBase
{
    @Test
    public void testOneSSTablePerShardIterators() throws Throwable
    {
        int numShards = 5;
        testParallelized(numShards, PARTITIONS, numShards, true, false);
    }

    @Test
    public void testMultipleInputSSTablesIterators() throws Throwable
    {
        int numShards = 3;
        testParallelized(numShards, PARTITIONS, numShards, false, false);
    }

    @Test
    public void testOneSSTablePerShardCursors() throws Throwable
    {
        int numShards = 5;
        testParallelized(numShards, PARTITIONS, numShards, true, true);
    }

    @Test
    public void testMultipleInputSSTablesCursors() throws Throwable
    {
        int numShards = 3;
        testParallelized(numShards, PARTITIONS, numShards, false, true);
    }

    private void testParallelized(int numShards, int rowCount, int numOutputSSTables, boolean compact, boolean useCursors) throws Throwable
    {
        CassandraRelevantProperties.CURSORS_ENABLED.setBoolean(useCursors);
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(rowCount, compact);

        LifecycleTransaction transaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION, UnifiedCompactionStrategy.nextTimeUUID());

        ShardManager shardManager = new ShardManagerNoDisks(SortedLocalRanges.forTestingFull(cfs));

        Controller mockController = Mockito.mock(Controller.class);
        Mockito.when(mockController.getNumShards(Mockito.anyDouble())).thenReturn(numShards);

        Collection<SSTableReader> sstables = transaction.originals();
        CompositeLifecycleTransaction compositeTransaction = new CompositeLifecycleTransaction(transaction);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(new CompactionStrategyFactory(cfs), mockController);
        UnifiedCompactionStrategy mockStrategy = strategy;
        strategy.getCompactionLogger().enable();
        SharedCompactionProgress sharedProgress = new SharedCompactionProgress(transaction.opId(), transaction.opType(), TableOperation.Unit.BYTES);
        SharedCompactionObserver sharedObserver = new SharedCompactionObserver(strategy);

        List<AbstractCompactionTask> tasks = shardManager.splitSSTablesInShards(
            sstables,
            numShards,
            (rangeSSTables, range) ->
            new UnifiedCompactionTask(cfs,
                                      mockStrategy,
                                      new PartialLifecycleTransaction(compositeTransaction),
                                      0,
                                      false,
                                      shardManager,
                                      new UnifiedCompactionStrategy.ShardingStats(rangeSSTables, shardManager, mockController),
                                      range,
                                      rangeSSTables,
                                      sharedProgress,
                                      sharedObserver)
        );
        compositeTransaction.completeInitialization();
        assertEquals(numOutputSSTables, tasks.size());

        List<Future<?>> futures = tasks.stream()
                                       .map(t -> ForkJoinPool.commonPool()
                                                             .submit(() -> {
                                                                 t.execute(CompactionManager.instance.active);
                                                             }))
                                       .collect(Collectors.toList());

        FBUtilities.waitOnFutures(futures);
        assertTrue(transaction.state() == Transactional.AbstractTransactional.State.COMMITTED);

        verifySharding(numShards, rowCount, numOutputSSTables, cfs);
        cfs.truncateBlocking();
    }
}