/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for read coordination behaviors.
 */
public final class ReadCoordinationMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("ReadCoordination");

    public static final Counter nonreplicaRequests = Metrics.counter(factory.createMetricName("LocalNodeNonreplicaRequests"));
    public static final Counter preferredOtherReplicas = Metrics.counter(factory.createMetricName("PreferredOtherReplicas"));

    private static final ConcurrentMap<InetAddress, Histogram> replicaLatencies = new ConcurrentHashMap<>();

    public static void updateReplicaLatency(InetAddress address, long latency)
    {
        if (latency == DatabaseDescriptor.getReadRpcTimeout())
            return; // don't track timeouts

        Histogram histogram = replicaLatencies.get(address);

        // avoid computeIfAbsent() call on the common path
        if (null == histogram)
            histogram = replicaLatencies.computeIfAbsent(address, ReadCoordinationMetrics::createHistogram);

        histogram.update(latency);
    }

    private static Histogram createHistogram(InetAddress a)
    {
        return Metrics.histogram(DefaultNameFactory.createMetricName("ReadCoordination", "ReplicaLatency", a.getHostAddress().replace(':', '.')), false);
    }
}
