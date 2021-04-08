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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.NonThrowingCloseable;

public class ActiveOperations extends SchemaChangeListener implements TableOperationObserver
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveOperations.class);

    // The operations ordered by keyspace.table for all the operations that are currently in progress.
    private static final ConcurrentMap<String, TableOperations> operationsByTable = new ConcurrentHashMap<>();

    public ActiveOperations()
    {
        Schema.instance.registerListener(this);
    }

    /**
     * @return all the table operations currently in progress. This is mostly compactions but it can include other
     *         operations too, basically any operation that calls {@link this#onOperationStart(TableOperation).}
     */
    public List<TableOperation> getTableOperations()
    {
        return operationsByTable.values()
                                .stream()
                                .flatMap(ops -> ops.getInProgress().stream())
                                .collect(Collectors.toList());
    }

    @Override
    public NonThrowingCloseable onOperationStart(TableOperation op)
    {
        TableOperation.Progress progress = op.getProgress();;
        String key = operationsByTableKey(progress);
        operationsByTable.computeIfAbsent(key, k -> new TableOperations(progress.metadata()));
        operationsByTable.computeIfPresent(key, (k, ops) -> ops.operationStarted(op));
        return () -> operationsByTable.computeIfPresent(key,
                                                        (k, ops) -> ops.operationCompleted(op,
                                                                                           op.getProgress(),
                                                                                           CompactionManager.instance.getMetrics()));
    }

    public TableOperations operationsByMetadata(TableMetadata metadata)
    {
        return operationsByTable.get(operationsByTableKey(metadata));
    }

    private String operationsByTableKey(TableOperation.Progress progress)
    {
        return operationsByTableKey(progress.metadata());
    }

    private String operationsByTableKey(TableMetadata metadata)
    {
        return operationsByTableKey(metadata.keyspace, metadata.name);
    }

    private String operationsByTableKey(String keyspace, String table)
    {
        return keyspace + "." + table;
    }

    public TableOperations.Snapshot operationsInProgress(TableMetadata metadata)
    {
        TableOperations tableOperations = operationsByTable.get(operationsByTableKey(metadata));
        if (tableOperations == null)
            return null;

        try
        {
            ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);
            return new TableOperations.Snapshot(cfs, tableOperations);
        }
        catch (IllegalArgumentException ex)
        {
            // this happens when the table is dropped
            logger.debug("Could not return operations in progress for {}: {}", metadata, ex.getMessage());
        }

        return null;
    }

    /**
     * Iterates over the active operations and tries to find OperationProgresses with the given operation type for the given sstable
     *
     * Number of entries in operations should be small (< 10) but avoid calling in any time-sensitive context
     */
    public Collection<AbstractTableOperation.OperationProgress> getOperationsForSSTable(SSTableReader sstable, OperationType operationType)
    {
        List<AbstractTableOperation.OperationProgress> toReturn = null;
        synchronized (operationsByTable)
        {
            for (TableOperations tableOperations : operationsByTable.values())
            {
                for (TableOperation op : tableOperations.getInProgress())
                {
                    AbstractTableOperation.OperationProgress progress = op.getProgress();
                    if (progress.sstables().contains(sstable) && progress.operationType() == operationType)
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
     * @return true if given table operation is still active
     */
    public boolean isActive(TableOperation op)
    {
        return getTableOperations().contains(op);
    }

    //
    // SchemaChangeListener
    //

    @Override
    public void onDropTable(String keyspace, String table)
    {
        operationsByTable.remove(operationsByTableKey(keyspace, table));
    }
}
