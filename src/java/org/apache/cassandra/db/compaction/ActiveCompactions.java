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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public class ActiveCompactions implements TableOperationsTracker
{
    // The statistics ordered by keyspace.table for all the operations that are currently in progress.
    private static final ConcurrentMap<String, TableOperations> operationsByTable = new ConcurrentHashMap<>();

    public List<AbstractTableOperation> getCompactions()
    {
        return operationsByTable.values()
                                .stream()
                                .flatMap(compactions -> compactions.getInProgress().stream())
                                .collect(Collectors.toList());
    }

    public void begin(AbstractTableOperation op)
    {
        AbstractTableOperation.Progress progress = op.getProgress();
        String key = operationsByTableKey(progress);

        operationsByTable.computeIfAbsent(key, k -> new TableOperations(progress.getTableMetadata()));
        operationsByTable.computeIfPresent(key, (k, tableOperations) -> tableOperations.operationsStarted(op));
    }

    public void finish(AbstractTableOperation op)
    {
        AbstractTableOperation.Progress progress = op.getProgress();
        operationsByTable.computeIfPresent(operationsByTableKey(progress),
                                           (key, tableOperations) -> {
            return tableOperations.operationsCompleted(op,
                                                       progress,
                                                       CompactionManager.instance.getMetrics());
        });
    }

    public TableOperations operationsByMetadata(TableMetadata metadata)
    {
        return operationsByTable.get(operationsByTableKey(metadata));
    }

    private String operationsByTableKey(AbstractTableOperation.Progress progress)
    {
        return operationsByTableKey(progress.getTableMetadata());
    }

    private String operationsByTableKey(TableMetadata metadata)
    {
        return metadata.keyspace + "." + metadata.name;
    }

    /**
     * Iterates over the active compactions and tries to find CompactionInfos with the given compactionType for the given sstable
     *
     * Number of entries in compactions should be small (< 10) but avoid calling in any time-sensitive context
     */
    public Collection<AbstractTableOperation.Progress> getOperationsForSSTable(SSTableReader sstable, OperationType compactionType)
    {
        List<AbstractTableOperation.Progress> toReturn = null;
        synchronized (operationsByTable)
        {
            for (TableOperations tableOperations : operationsByTable.values())
            {
                for (AbstractTableOperation op : tableOperations.getInProgress())
                {
                    AbstractTableOperation.Progress progress = op.getProgress();
                    if (progress.getSSTables().contains(sstable) && progress.getOperationType() == compactionType)
                    {
                        if (toReturn == null)
                            toReturn = new ArrayList<>();
                        toReturn.add(progress);
                    }
                }
            }
        }
        return toReturn;
    }

    /**
     * @return true if given compaction is still active
     */
    public boolean isActive(AbstractTableOperation ci)
    {
        return getCompactions().contains(ci);
    }
}
