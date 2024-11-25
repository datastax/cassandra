/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class UnifiedCompactionTask extends CompactionTask
{
    private final ShardManager shardManager;
    private final Range<Token> operationRange;
    private final Set<SSTableReader> actuallyCompact;
    private final UnifiedCompactionStrategy.ShardingStats shardingStats;

    public UnifiedCompactionTask(CompactionRealm cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 int gcBefore,
                                 ShardManager shardManager,
                                 UnifiedCompactionStrategy.ShardingStats shardingStats)
    {
        this(cfs, strategy, txn, gcBefore, shardManager, shardingStats, null, null);
    }


    public UnifiedCompactionTask(CompactionRealm cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 int gcBefore,
                                 ShardManager shardManager,
                                 UnifiedCompactionStrategy.ShardingStats shardingStats,
                                 Range<Token> operationRange,
                                 Collection<SSTableReader> actuallyCompact)
    {
        super(cfs, txn, gcBefore, strategy.getController().getIgnoreOverlapsInExpirationCheck(), strategy);
        this.shardManager = shardManager;
        this.shardingStats = shardingStats;

        if (operationRange != null)
            assert actuallyCompact != null : "Ranged tasks should use a set of sstables to compact";

        this.operationRange = operationRange;
        // To make sure actuallyCompact tracks any removals from txn.originals(), we intersect the given set with it.
        // This should not be entirely necessary (as shouldReduceScopeForSpace() is false for ranged tasks), but it
        // is cleaner to enforce inputSSTables()'s requirements.
        this.actuallyCompact = actuallyCompact != null ? Sets.intersection(ImmutableSet.copyOf(actuallyCompact),
                                                                           txn.originals())
                                                       : txn.originals();
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                          Directories directories,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        // In multi-task operations we need to expire many ranges in a source sstable for early open. Not doable yet.
        final boolean earlyOpenAllowed = operationRange == null;
        return new ShardedCompactionWriter(realm,
                                           directories,
                                           transaction,
                                           nonExpiredSSTables,
                                           shardingStats.uniqueKeyRatio,
                                           keepOriginals,
                                           earlyOpenAllowed,
                                           shardManager.boundaries(shardingStats.shardCountForDensity));
    }

    @Override
    protected Range<Token> tokenRange()
    {
        return operationRange;
    }

    @Override
    protected boolean shouldReduceScopeForSpace()
    {
        return tokenRange() == null;
    }

    @Override
    protected Set<SSTableReader> inputSSTables()
    {
        return actuallyCompact;
    }
}