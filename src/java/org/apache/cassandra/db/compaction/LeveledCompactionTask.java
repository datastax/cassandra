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
package org.apache.cassandra.db.compaction;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;

public class LeveledCompactionTask extends CompactionTask
{
    private final int level;
    private final long maxSSTableBytes;
    private final boolean majorCompaction;

    public LeveledCompactionTask(LeveledCompactionStrategy strategy, ILifecycleTransaction txn, int level, int gcBefore, long maxSSTableBytes, boolean majorCompaction)
    {
        super(strategy.realm, txn, gcBefore, false, strategy);
        this.level = level;
        this.maxSSTableBytes = maxSSTableBytes;
        this.majorCompaction = majorCompaction;
    }

    public LeveledCompactionTask(ColumnFamilyStore cfs, ILifecycleTransaction txn, int level, int gcBefore, long maxSSTableBytes, boolean majorCompaction, @Nullable CompactionStrategy strategy) {
        super(cfs, txn, gcBefore, false, strategy);
        this.level = level;
        this.maxSSTableBytes = maxSSTableBytes;
        this.majorCompaction = majorCompaction;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                          Directories directories,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        if (majorCompaction)
            return new MajorLeveledCompactionWriter(realm, directories, transaction, nonExpiredSSTables, maxSSTableBytes, false);
        return new MaxSSTableSizeWriter(realm, directories, transaction, nonExpiredSSTables, maxSSTableBytes, getLevel(), false);
    }

    @Override
    protected boolean partialCompactionsAcceptable()
    {
        // LCS allows removing L0 sstable from L0/L1 compaction task for limited disk space. It's handled in #reduceScopeForLimitedSpace
        return level <= 1;
    }

    protected int getLevel()
    {
        return level;
    }

    @Override
    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (nonExpiredSSTables.size() > 1 && level <= 1)
        {
            // Try again w/o the largest one.
            logger.warn("insufficient space to do L0 -> L{} compaction. {}MiB required, {} for compaction {}",
                        level,
                        (float) expectedSize / 1024 / 1024,
                        transaction.originals()
                                   .stream()
                                   .map(sstable -> String.format("%s (level=%s, size=%s)", sstable, sstable.getSSTableLevel(), sstable.onDiskLength()))
                                   .collect(Collectors.joining(",")),
                        transaction.opIdString());
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            int l0SSTableCount = 0;
            SSTableReader largestL0SSTable = null;
            for (SSTableReader sstable : nonExpiredSSTables)
            {
                if (sstable.getSSTableLevel() == 0)
                {
                    l0SSTableCount++;
                    if (largestL0SSTable == null || sstable.onDiskLength() > largestL0SSTable.onDiskLength())
                        largestL0SSTable = sstable;
                }
            }
            // no point doing a L0 -> L{0,1} compaction if we have cancelled all L0 sstables
            if (largestL0SSTable != null && l0SSTableCount > 1)
            {
                logger.info("Removing {} (level={}, size={}) from compaction {}",
                            largestL0SSTable,
                            largestL0SSTable.getSSTableLevel(),
                            largestL0SSTable.onDiskLength(),
                            transaction.opIdString());
                transaction.cancel(largestL0SSTable);
                nonExpiredSSTables.remove(largestL0SSTable);
                return true;
            }
        }
        return false;
    }
}
