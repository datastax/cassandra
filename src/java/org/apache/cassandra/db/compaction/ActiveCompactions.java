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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public class ActiveCompactions implements ActiveCompactionsTracker
{
    // The compaction statistics ordered by keyspace.table
    private static final ConcurrentMap<String, TableCompactions> compactionsByTable = new ConcurrentHashMap<>();

    public List<CompactionInfo.Holder> getCompactions()
    {
        return compactionsByTable.values()
                                 .stream()
                                 .flatMap(compactions -> compactions.getInProgress().stream())
                                 .collect(Collectors.toList());
    }

    public void beginCompaction(CompactionInfo.Holder ci)
    {
        CompactionInfo compactionInfo = ci.getCompactionInfo();
        String key = compactionsByTableKey(compactionInfo);

        compactionsByTable.computeIfAbsent(key, k -> new TableCompactions(compactionInfo.getTableMetadata()));
        compactionsByTable.computeIfPresent(key, (k, tableCompactions) -> tableCompactions.compactionStarted(ci));
    }

    public void finishCompaction(CompactionInfo.Holder ci)
    {
        CompactionInfo compactionInfo = ci.getCompactionInfo();
        compactionsByTable.computeIfPresent(compactionsByTableKey(compactionInfo),
                                            (key, tableCompactions) -> {
            return tableCompactions.compactionCompleted(ci,
                                                        compactionInfo,
                                                        CompactionManager.instance.getMetrics());
        });
    }

    public TableCompactions compactionsByMetadata(TableMetadata metadata)
    {
        return compactionsByTable.get(compactionsByTableKey(metadata));
    }

    private String compactionsByTableKey(CompactionInfo compactionInfo)
    {
        return compactionsByTableKey(compactionInfo.getTableMetadata());
    }

    private String compactionsByTableKey(TableMetadata metadata)
    {
        return metadata.keyspace + "." + metadata.name;
    }

    /**
     * Iterates over the active compactions and tries to find CompactionInfos with the given compactionType for the given sstable
     *
     * Number of entries in compactions should be small (< 10) but avoid calling in any time-sensitive context
     */
    public Collection<CompactionInfo> getCompactionsForSSTable(SSTableReader sstable, OperationType compactionType)
    {
        List<CompactionInfo> toReturn = null;
        synchronized (compactionsByTable)
        {
            for (TableCompactions tableCompactions : compactionsByTable.values())
            {
                for (CompactionInfo.Holder holder : tableCompactions.getInProgress())
                {
                    CompactionInfo compactionInfo = holder.getCompactionInfo();
                    if (compactionInfo.getSSTables().contains(sstable) && compactionInfo.getTaskType() == compactionType)
                    {
                        if (toReturn == null)
                            toReturn = new ArrayList<>();
                        toReturn.add(compactionInfo);
                    }
                }
            }
        }
        return toReturn;
    }

    /**
     * @return true if given compaction is still active
     */
    public boolean isActive(CompactionInfo.Holder ci)
    {
        return getCompactions().contains(ci);
    }
}
