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

import com.codahale.metrics.Gauge;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.InboundMessageHandlers;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for internode connections.
 */
public class InternodeInboundMetrics
{
    public final Gauge<Long> corruptFramesRecovered;
    public final Gauge<Long> corruptFramesUnrecovered;
    public final Gauge<Long> errorBytes;
    public final Gauge<Long> errorCount;
    public final Gauge<Long> expiredBytes;
    public final Gauge<Long> expiredCount;
    public final Gauge<Long> pendingBytes;
    public final Gauge<Long> pendingCount;
    public final Gauge<Long> processedBytes;
    public final Gauge<Long> processedCount;
    public final Gauge<Long> receivedBytes;
    public final Gauge<Long> receivedCount;
    public final Gauge<Long> throttledCount;
    public final Gauge<Long> throttledNanos;

    private final MetricNameFactory factory;

    /**
     * Create metrics for given inbound message handlers.
     *
     * @param peer IP address and port to use for metrics label
     */
    public InternodeInboundMetrics(InetAddressAndPort peer, InboundMessageHandlers handlers)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        factory = new DefaultNameFactory("InboundConnection", peer.getHostAddressAndPortForJMX());

        corruptFramesRecovered = Metrics.register(factory.createMetricName("CorruptFramesRecovered"), handlers::corruptFramesRecovered);
        corruptFramesUnrecovered = Metrics.register(factory.createMetricName("CorruptFramesUnrecovered"), handlers::corruptFramesUnrecovered);
        errorBytes = Metrics.register(factory.createMetricName("ErrorBytes"), handlers::errorBytes);
        errorCount = Metrics.register(factory.createMetricName("ErrorCount"), handlers::errorCount);
        expiredBytes = Metrics.register(factory.createMetricName("ExpiredBytes"), handlers::expiredBytes);
        expiredCount = Metrics.register(factory.createMetricName("ExpiredCount"), handlers::expiredCount);
        pendingBytes = Metrics.register(factory.createMetricName("ScheduledBytes"), handlers::scheduledBytes);
        pendingCount = Metrics.register(factory.createMetricName("ScheduledCount"), handlers::scheduledCount);
        processedBytes = Metrics.register(factory.createMetricName("ProcessedBytes"), handlers::processedBytes);
        processedCount = Metrics.register(factory.createMetricName("ProcessedCount"), handlers::processedCount);
        receivedBytes = Metrics.register(factory.createMetricName("ReceivedBytes"), handlers::receivedBytes);
        receivedCount = Metrics.register(factory.createMetricName("ReceivedCount"), handlers::receivedCount);
        throttledCount = Metrics.register(factory.createMetricName("ThrottledCount"), handlers::throttledCount);
        throttledNanos = Metrics.register(factory.createMetricName("ThrottledNanos"), handlers::throttledNanos);
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("CorruptFramesRecovered"));
        Metrics.remove(factory.createMetricName("CorruptFramesUnrecovered"));
        Metrics.remove(factory.createMetricName("ErrorBytes"));
        Metrics.remove(factory.createMetricName("ErrorCount"));
        Metrics.remove(factory.createMetricName("ExpiredBytes"));
        Metrics.remove(factory.createMetricName("ExpiredCount"));
        Metrics.remove(factory.createMetricName("PendingBytes"));
        Metrics.remove(factory.createMetricName("PendingCount"));
        Metrics.remove(factory.createMetricName("ProcessedBytes"));
        Metrics.remove(factory.createMetricName("ProcessedCount"));
        Metrics.remove(factory.createMetricName("ReceivedBytes"));
        Metrics.remove(factory.createMetricName("ReceivedCount"));
        Metrics.remove(factory.createMetricName("ThrottledCount"));
        Metrics.remove(factory.createMetricName("ThrottledNanos"));
    }

}
