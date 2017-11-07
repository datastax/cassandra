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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to Storage.
 */
public class StorageMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Storage");

    public static final Counter load = Metrics.counter(factory.createMetricName("Load"));
    public static final Counter exceptions = Metrics.counter(factory.createMetricName("Exceptions"));
    public static final Counter totalHintsInProgress  = Metrics.counter(factory.createMetricName("TotalHintsInProgress"));
    /** Total hints replayed in the target node **/
    public static final Counter totalHintsReplayed = Metrics.counter(factory.createMetricName("TotalHintsReplayed"));
    public static final Counter totalHints = Metrics.counter(factory.createMetricName("TotalHints"));
    public static final Meter batchlogReplays = Metrics.meter(factory.createMetricName("BatchlogReplays"));
    public static final Meter hintedBatchlogReplays = Metrics.meter(factory.createMetricName("HintedBatchlogReplays"));
    public static final Gauge netHintsSinceStartup = Metrics.register(factory.createMetricName("NetHintsSinceStartup"),
                                                                      () -> totalHints.getCount() - HintedHandoffMetrics.hintsSinceStartupDispatchedAndDiscarded.getCount());
}
