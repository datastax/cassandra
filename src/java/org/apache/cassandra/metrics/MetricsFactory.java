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

import javax.annotation.Nonnull;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

public interface MetricsFactory
{
    MetricsFactory instance = loadFactory();

    @Nonnull
    KeyspaceMetrics newKeyspaceMetrics(@Nonnull final Keyspace ks);

    @Nonnull
    TableMetrics newTableMetrics(@Nonnull final ColumnFamilyStore cfs, TableMetrics.ReleasableMetric memtableMetrics);

    @Nonnull
    TrieMemtableMetricsView newMemtableMetrics(@Nonnull String keyspace, @Nonnull String table);

    static MetricsFactory loadFactory()
    {
        MetricsFactory factory = null;
        String customMetricsFactoryClass = System.getProperty("cassandra.custom_metrics_factory_class");
        if (null != customMetricsFactoryClass)
        {
            try
            {
                factory = FBUtilities.construct(customMetricsFactoryClass, "Metrics factory");
                LoggerFactory.getLogger(MetricsFactory.class)
                             .info("using {} to generate metrics", customMetricsFactoryClass);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                LoggerFactory.getLogger(MetricsFactory.class)
                             .error(String.format("cannot use class %s for metrics, defaulting to normal metrics factory", customMetricsFactoryClass), e);
            }
        }
        return factory == null ? DefaultMetricsFactory.instance() : factory;
    }
}
