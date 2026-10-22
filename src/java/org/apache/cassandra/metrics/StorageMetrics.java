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

import java.util.function.ToLongFunction;
import java.util.stream.StreamSupport;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.db.Keyspace;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to Storage.
 */
public class StorageMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Storage");

    public static final Counter load = Metrics.counter(factory.createMetricName("Load"));
    public static final Counter uncompressedLoad = Metrics.counter(factory.createMetricName("UncompressedLoad"));

    public static final Gauge<Long> unreplicatedLoad =
        createSummingGauge("UnreplicatedLoad", metric -> metric.unreplicatedLiveDiskSpaceUsed.getValue());
    public static final Gauge<Long> unreplicatedUncompressedLoad =
        createSummingGauge("UnreplicatedUncompressedLoad", metric -> metric.unreplicatedUncompressedLiveDiskSpaceUsed.getValue());

    public static final Counter uncaughtExceptions = Metrics.counter(factory.createMetricName("Exceptions"));
    public static final Counter totalHintsInProgress  = Metrics.counter(factory.createMetricName("TotalHintsInProgress"));
    public static final Counter totalHints = Metrics.counter(factory.createMetricName("TotalHints"));
    public static final Counter repairExceptions = Metrics.counter(factory.createMetricName("RepairExceptions"));

    /**
     * Batches replayed from the batchlog for which no hint was written, normally because every replica acknowledged
     * the replayed mutations or the local node was their only live replica. Each replay of a batch is counted in
     * exactly one of this meter and {@link #hintedBatchlogReplays}, once its mutations complete, even if the batch
     * then stays in the batchlog awaiting its interceptor callback; like {@code TotalBatchesReplayed}, both meters
     * may count a batch again if the replay cycle aborts and the batch is later replayed from scratch. Batches that
     * are expired, whose tables were all truncated, or that cannot be deserialized are not replayed and count in
     * neither meter.
     */
    public static final Meter batchlogReplays = Metrics.meter(factory.createMetricName("BatchlogReplays"));

    /**
     * Batches replayed from the batchlog for which at least one hint was written, because a replica was down when
     * the mutations were sent, failed them, or did not acknowledge them in time. Counted once per replay of the
     * batch, as described on {@link #batchlogReplays}. The DSE meter of the same name was also marked for expired,
     * truncated and locally-applied-only batches that wrote no hint; this one is not.
     */
    public static final Meter hintedBatchlogReplays = Metrics.meter(factory.createMetricName("HintedBatchlogReplays"));

    public static final Counter totalOpsForInvalidToken = Metrics.counter(factory.createMetricName("TotalOpsForInvalidToken"));
    public static final Counter startupOpsForInvalidToken = Metrics.counter(factory.createMetricName("StartupOpsForInvalidToken"));

    private static Gauge<Long> createSummingGauge(String name, ToLongFunction<KeyspaceMetrics> extractor)
    {
        return Metrics.register(factory.createMetricName(name),
                                () -> StreamSupport.stream(Keyspace.all().spliterator(), false)
                                                   .mapToLong(keyspace -> extractor.applyAsLong(keyspace.metric))
                                                   .sum());
    }
}
