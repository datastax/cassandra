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

import java.util.Set;

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

/**
 * The sole purpose of this class is to currently create a {@link ShardedCompactionWriter}.
 */
public class UnifiedCompactionTask extends CompactionTask
{
    private final ShardManager shardManager;
    private final Controller controller;
    private final Range<Token> operationRange;

    public UnifiedCompactionTask(CompactionRealm cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 int gcBefore,
                                 ShardManager shardManager)
    {
        this(cfs, strategy, txn, gcBefore, shardManager, null);
    }


    public UnifiedCompactionTask(CompactionRealm cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 int gcBefore,
                                 ShardManager shardManager,
                                 Range<Token> operationRange)
    {
        super(cfs, txn, gcBefore, strategy.getController().getIgnoreOverlapsInExpirationCheck(), strategy);
        this.controller = strategy.getController();
        this.shardManager = shardManager;
        this.operationRange = operationRange;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                          Directories directories,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        double density = shardManager.calculateCombinedDensity(nonExpiredSSTables);
        int numShards = controller.getNumShards(density * shardManager.shardSetCoverage());
        // In multi-task operations we need to expire many ranges in a source sstable for early open. Not doable yet.
        final boolean earlyOpenAllowed = operationRange == null;
        return new ShardedCompactionWriter(realm,
                                           directories,
                                           transaction,
                                           nonExpiredSSTables,
                                           keepOriginals,
                                           earlyOpenAllowed,
                                           shardManager.boundaries(numShards));
    }

    protected Range<Token> tokenRange()
    {
        return operationRange;
    }
}