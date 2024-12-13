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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionAggregate;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyFactory;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

/// This tests UnifiedWithRange aggregates that pre-split compactions into shards and is used by CNDB.
public class RangedAggregatesTest extends ShardingTestBase
{
    @Test
    public void testOneSSTablePerShard() throws Throwable
    {
        testRangedAggregates(6, 6, PARTITIONS, 6, true, true);
    }

    @Test
    public void testOneSSTablePerShardFurtherSplit() throws Throwable
    {
        testRangedAggregates(6, 3, PARTITIONS, 6, true, true);
    }

    @Test
    public void testOneSSTablePerShardOnlyFurtherSplit() throws Throwable
    {
        testRangedAggregates(6, 1, PARTITIONS, 6, true, true);
    }

    @Test
    public void testMultipleInputSSTables() throws Throwable
    {
        testRangedAggregates(6, 6, PARTITIONS, 6, false, true);
    }

    @Test
    public void testMultipleInputSSTablesFurtherSplit() throws Throwable
    {
        testRangedAggregates(6, 3, PARTITIONS, 6, false, true);
    }

    @Test
    public void testMultipleInputSSTablesOnlyFurtherSplit() throws Throwable
    {
        testRangedAggregates(6, 1, PARTITIONS, 6, false, true);
    }

    private void testRangedAggregates(int numShards, int aggregateParallelism, int rowCount, int numOutputSSTables, boolean compact, boolean useCursors) throws Throwable
    {
        CassandraRelevantProperties.CURSORS_ENABLED.setBoolean(useCursors);
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(rowCount, compact);
        var originals = new HashSet<>(cfs.getLiveSSTables());

        Controller mockController = Mockito.mock(Controller.class);
        Mockito.when(mockController.getNumShards(Mockito.anyDouble())).thenReturn(numShards);
        Mockito.when(mockController.parallelizeOutputShards()).thenReturn(true);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(new CompactionStrategyFactory(cfs), mockController);
        ShardManager shardManager = strategy.getShardManager();
        Collection<CompactionAggregate.UnifiedAggregate> maximals = strategy.getMaximalAggregates();
        maximals = maximals.stream()
                           .flatMap(agg ->
                               shardManager.splitSSTablesInShardsLimited
                               (
                                   agg.getSelected().sstables(),
                                   null,
                                   numShards,
                                   numShards,
                                   aggregateParallelism,
                                   (rangeSSTables, range) ->
                                       CompactionAggregate.createUnifiedWithRange
                                       (
                                           agg,
                                           rangeSSTables,
                                           range,
                                           numShards    // this should further split
                                       )
                               ).stream())
                           .collect(Collectors.toList());

        int totalTaskCount = 0;
        for (CompactionAggregate.UnifiedAggregate maximal : maximals)
        {
            // execute each partial aggregate separately because we can only mark the inputs compacting once
            List<AbstractCompactionTask> tasks = new ArrayList<>();
            strategy.createAndAddTasks(0, maximal, tasks);
            totalTaskCount += tasks.size();
            List<Future<?>> futures = tasks.stream()
                                           .map(t -> ForkJoinPool.commonPool()
                                                                 .submit(() -> {
                                                                     t.execute(CompactionManager.instance.active);
                                                                 }))
                                           .collect(Collectors.toList());
            FBUtilities.waitOnFutures(futures);
        }
        assertEquals(numOutputSSTables, totalTaskCount);


        // make sure the partial aggregates are not deleting sstables
        Assert.assertTrue(cfs.getLiveSSTables().containsAll(originals));
        cfs.getTracker().removeUnsafe(originals);

        verifySharding(numShards, rowCount, numOutputSSTables, cfs);
        cfs.truncateBlocking();
    }
}