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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TrieMemtableMetricsView
{
    private static final String UNCONTENDED_PUTS = "Uncontended memtable puts";
    private static final String CONTENDED_PUTS = "Contended memtable puts";
    private static final String CONTENTION_TIME = "Contention time";
    private static final String LAST_FLUSH_SHARD_SIZES = "Shard sizes during last flush";

    private static final Map<String, TrieMemtableMetricsView> perTableMetrics = new ConcurrentHashMap<>();

    // the number of memtable puts that did not need to wait on write lock
    public final Counter uncontendedPuts;

    // the number of memtable puts that needed to wait on write lock
    public final Counter contendedPuts;

    // shard put contention measurements
    public final LatencyMetrics contentionTime;

    // shard sizes distribution
    public final MinMaxAvgMetric lastFlushShardDataSizes;

    private final TrieMemtableMetricNameFactory factory;
    private final String keyspace;
    private final String table;
    private final CassandraMetricsRegistry metricsRegistry;

    public static TrieMemtableMetricsView getOrCreate(String keyspace, String table)
    {
        return perTableMetrics.computeIfAbsent(getKey(keyspace, table), k -> MetricsFactory.instance.newMemtableMetrics(keyspace, table));
    }

    public TrieMemtableMetricsView(String keyspace, String table, CassandraMetricsRegistry metricsRegistry)
    {
        this.keyspace = keyspace;
        this.table = table;
        factory = new TrieMemtableMetricNameFactory(keyspace, table);
        this.metricsRegistry = metricsRegistry;
        
        uncontendedPuts = metricsRegistry.counter(factory.createMetricName(UNCONTENDED_PUTS));
        contendedPuts = metricsRegistry.counter(factory.createMetricName(CONTENDED_PUTS));
        contentionTime = new LatencyMetrics(factory, CONTENTION_TIME, metricsRegistry);
        lastFlushShardDataSizes = new MinMaxAvgMetric(factory, LAST_FLUSH_SHARD_SIZES, metricsRegistry);
    }

    public void release()
    {
        perTableMetrics.remove(getKey(keyspace, table));

        metricsRegistry.remove(factory.createMetricName(UNCONTENDED_PUTS));
        metricsRegistry.remove(factory.createMetricName(CONTENDED_PUTS));
        contentionTime.release();
        lastFlushShardDataSizes.release();
    }

    static class TrieMemtableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspace;
        private final String table;

        TrieMemtableMetricNameFactory(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            String type = "TrieMemtable";

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspace);
            mbeanName.append(",scope=").append(table);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, keyspace + "." + table, mbeanName.toString());
        }
    }

    private static String getKey(String keyspace, String table)
    {
        return keyspace + "." + table;
    }
}
