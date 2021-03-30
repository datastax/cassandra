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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.schema.TableMetadata;

public class TableCompactions
{
    private final String keyspaceName;
    private final String tableName;
    private final long numCompleted;
    private final Set<CompactionInfo.Holder> inProgress;

    public TableCompactions(TableMetadata metadata)
    {
        this(metadata.keyspace, metadata.name, Collections.emptySet(), 0);
    }

    TableCompactions(String keyspaceName, String tableName, Set<CompactionInfo.Holder> inProgress, long numCompleted)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.inProgress = inProgress;
        this.numCompleted = numCompleted;
    }

    public TableCompactions compactionStarted(CompactionInfo.Holder ci)
    {
        return new TableCompactions(keyspaceName,
                                    tableName,
                                    Sets.newCopyOnWriteArraySet(Iterables.concat(inProgress, Collections.singleton(ci))),
                                    numCompleted);
    }

    public TableCompactions compactionCompleted(CompactionInfo.Holder ci, CompactionInfo ciInfo, CompactionMetrics metrics)
    {
        long numCompleted = this.numCompleted;
        Set<CompactionInfo.Holder> inProgress = this.inProgress;
        if (inProgress.contains(ci))
        {
            metrics.bytesCompacted.inc(ciInfo.getTotal());
            metrics.totalCompactionsCompleted.mark();
            inProgress = Sets.difference(this.inProgress, Collections.singleton(ci));
            numCompleted++;
        }
        return new TableCompactions(keyspaceName, tableName, inProgress, numCompleted);
    }

    public Set<CompactionInfo.Holder> getInProgress()
    {
        return inProgress;
    }

    public final static class Snapshot
    {
        public final String keyspaceName;
        public final String tableName;
        public final long numPending;
        public final long numCompleted;
        public final long numLiveSstables;
        public final long numCompactingSstables;
        public final long liveSizeOnDiskBytes;
        public final List<CompactionInfo> inProgress;

        public Snapshot(TableCompactions compactions)
        {
            ColumnFamilyStore cfs = Keyspace.open(compactions.keyspaceName).getColumnFamilyStore(compactions.tableName);

            this.keyspaceName = cfs.keyspace.getName();
            this.tableName = cfs.name;
            this.numCompleted = compactions.numCompleted;
            this.numPending = cfs.getCompactionStrategyManager().getEstimatedRemainingTasks();
            this.numLiveSstables = cfs.getTracker().getView().liveSSTables().size();
            this.numCompactingSstables = cfs.getTracker().getCompacting().size();
            this.liveSizeOnDiskBytes = cfs.getTracker().getView().liveSSTables().stream().map(sstable -> sstable.onDiskLength()).reduce(Long::sum).orElse(0L);
            this.inProgress = compactions.inProgress.stream().map(ci -> ci.getCompactionInfo()).collect(Collectors.toList());
        }
    }
}
