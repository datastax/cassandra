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
package org.apache.cassandra.metrics;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyStats;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.compaction.TableOperations;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for the compaction executor. Note that several different operations execute on the compaction
 * executor, for example index or view building. These operations are abstracted by {@link AbstractTableOperation}
 * but previously we would refer to these operations as "compactions", so this incorrect name may still be
 * found in the metrics that are exported to the users.
 */
public class CompactionMetrics
{
    public static final MetricNameFactory factory = new DefaultNameFactory("Compaction");

    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Estimated number of compactions remaining to perform, group by keyspace and then table name */
    public final Gauge<Map<String, Map<String, Integer>>> pendingTasksByTableName;

    /** Number of completed operations since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of operations since server [re]start */
    public final Meter totalCompactionsCompleted;
    /** Total number of bytes processed by operations since server [re]start */
    public final Counter bytesCompacted;

    /** Total number of compactions that have had sstables drop out of them */
    public final Counter compactionsReduced;

    /** Total number of sstables that have been dropped out */
    public final Counter sstablesDropppedFromCompactions;

    /** Total number of compactions which have outright failed due to lack of disk space */
    public final Counter compactionsAborted;

    /** The compaction strategy information for each table. */
    public final Gauge<List<CompactionStrategyStats>> strategyStats;

    public CompactionMetrics(final ThreadPoolExecutor... collectors)
    {
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                int n = 0;
                // add estimate number of compactions need to be done
                for (String keyspaceName : Schema.instance.getKeyspaces())
                {
                    for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                        n += cfs.getCompactionStrategyManager().getEstimatedRemainingTasks();
                }
                // add number of currently running compactions
                return n + CompactionManager.instance.active.getCompactions().size();
            }
        });

        pendingTasksByTableName = Metrics.register(factory.createMetricName("PendingTasksByTableName"),
            new Gauge<Map<String, Map<String, Integer>>>()
        {
            @Override
            public Map<String, Map<String, Integer>> getValue() 
            {
                Map<String, Map<String, Integer>> resultMap = new HashMap<>();
                // estimation of compactions need to be done
                for (String keyspaceName : Schema.instance.getKeyspaces())
                {
                    for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                    {
                        TableMetadata metadata = cfs.metadata.get();
                        TableOperations tableOperations = CompactionManager.instance
                                                                             .active
                                                                             .operationsByMetadata(metadata);
                        int taskNumber = cfs.getCompactionStrategyManager().getEstimatedRemainingTasks() +
                                         Math.toIntExact(tableOperations != null ? tableOperations.getInProgress().size() : 0);
                        if (taskNumber > 0)
                        {
                            if (!resultMap.containsKey(keyspaceName))
                            {
                                resultMap.put(keyspaceName, new HashMap<>());
                            }
                            resultMap.get(keyspaceName).put(cfs.getTableName(), taskNumber);
                        }
                    }
                }

                // currently running compactions
                for (TableOperation op : CompactionManager.instance.active.getCompactions())
                {
                    TableMetadata metaData = op.getProgress().metadata();
                    if (metaData == null)
                    {
                        continue;
                    }
                    if (!resultMap.containsKey(metaData.keyspace))
                    {
                        resultMap.put(metaData.keyspace, new HashMap<>());
                    }

                    Map<String, Integer> tableNameToCountMap = resultMap.get(metaData.keyspace);
                    if (tableNameToCountMap.containsKey(metaData.name))
                    {
                        tableNameToCountMap.put(metaData.name,
                                                tableNameToCountMap.get(metaData.name) + 1);
                    }
                    else
                    {
                        tableNameToCountMap.put(metaData.name, 1);
                    }
                }
                return resultMap;
            }
        });

        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long completedTasks = 0;
                for (ThreadPoolExecutor collector : collectors)
                    completedTasks += collector.getCompletedTaskCount();
                return completedTasks;
            }
        });
        totalCompactionsCompleted = Metrics.meter(factory.createMetricName("TotalCompactionsCompleted"));
        bytesCompacted = Metrics.counter(factory.createMetricName("BytesCompacted"));

        // compaction failure metrics
        compactionsReduced = Metrics.counter(factory.createMetricName("CompactionsReduced"));
        sstablesDropppedFromCompactions = Metrics.counter(factory.createMetricName("SSTablesDroppedFromCompaction"));
        compactionsAborted = Metrics.counter(factory.createMetricName("CompactionsAborted"));

        strategyStats = Metrics.register(factory.createMetricName("StrategyStats"), () -> {
            List<CompactionStrategyStats> ret = new ArrayList<>();
            // estimation of compactions need to be done
            for (String keyspaceName : Schema.instance.getKeyspaces())
            {
                for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                {
                    TableMetadata metadata = cfs.metadata.get();
                    TableOperations tableOperations = CompactionManager.instance
                                                      .active
                                                      .operationsByMetadata(metadata);

                    int taskNumber = cfs.getCompactionStrategyManager().getEstimatedRemainingTasks() +
                                     Math.toIntExact(tableOperations != null ? tableOperations.getInProgress().size() : 0);

                    if (taskNumber > 0)
                        ret.addAll(cfs.getCompactionStrategyManager()
                                      .getStrategies()
                                      .stream()
                                      .flatMap(list -> list.stream())
                                      .map(AbstractCompactionStrategy::getStats)
                                      .collect(Collectors.toList()));
                }
            }

            return ret;
        });
    }
}
