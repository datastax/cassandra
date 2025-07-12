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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for tracking result sizes coming from replicas/writers to coordinators.
 */
public class ReplicaResponseSizeMetrics
{
    private static final String TYPE = "ReplicaResponseSize";
    
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
     * Record the size of a response received from a replica
     * @param responseSize the size of the response in bytes
     * @param isReadResponse true if this is a read response, false for write response
     */
    public static void recordReplicaResponseSize(int responseSize, boolean isReadResponse)
    {
        totalBytesReceived.inc(responseSize);
        bytesReceivedPerResponse.update(responseSize);
        
        if (isReadResponse)
        {
            readResponseBytesReceived.inc(responseSize);
            readResponseBytesPerResponse.update(responseSize);
        }
        else
        {
            writeResponseBytesReceived.inc(responseSize);
            writeResponseBytesPerResponse.update(responseSize);
        }
    }
}