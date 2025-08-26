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
import com.codahale.metrics.Histogram;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for tracking result sizes coming from replicas/writers to coordinators.
 */
public class ReplicaResponseSizeMetrics
{
    private static final String TYPE = "ReplicaResponseSize";
    
    /**
     * Controls whether replica response size metrics collection is enabled.
     */
    private static final boolean METRICS_ENABLED = CassandraRelevantProperties.REPLICA_RESPONSE_SIZE_METRICS_ENABLED.getBoolean();
    
    /** Total bytes received from replicas in response messages */
    public static final Counter totalBytesReceived = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "TotalBytesReceived", null));
    
    /** Histogram of response sizes from replicas */
    public static final Histogram bytesReceivedPerResponse = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE, "BytesReceivedPerResponse", null), true);
    
    /** Total bytes received from replicas in read responses */
    public static final Counter readResponseBytesReceived = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "ReadResponseBytesReceived", null));
    
    /** Histogram of read response sizes from replicas */
    public static final Histogram readResponseBytesPerResponse = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE, "ReadResponseBytesPerResponse", null), true);
    
    /** Total bytes received from replicas in write responses */
    public static final Counter writeResponseBytesReceived = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "WriteResponseBytesReceived", null));
    
    /** Histogram of write response sizes from replicas */
    public static final Histogram writeResponseBytesPerResponse = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE, "WriteResponseBytesPerResponse", null), true);
    
    /**
     * Check if metrics collection is enabled
     * @return true if metrics are enabled, false otherwise
     */
    public static boolean isMetricsEnabled()
    {
        return METRICS_ENABLED;
    }
    
    /**
     * Record the size of a read response received from a replica
     * @param responseSize the size of the response in bytes
     */
    public static void recordReadResponseSize(int responseSize)
    {
        if (!METRICS_ENABLED)
            return;
            
        totalBytesReceived.inc(responseSize);
        bytesReceivedPerResponse.update(responseSize);
        readResponseBytesReceived.inc(responseSize);
        readResponseBytesPerResponse.update(responseSize);
    }
    
    /**
     * Record the size of a write response received from a replica
     * @param responseSize the size of the response in bytes
     */
    public static void recordWriteResponseSize(int responseSize)
    {
        if (!METRICS_ENABLED)
            return;
            
        totalBytesReceived.inc(responseSize);
        bytesReceivedPerResponse.update(responseSize);
        writeResponseBytesReceived.inc(responseSize);
        writeResponseBytesPerResponse.update(responseSize);
    }
}
