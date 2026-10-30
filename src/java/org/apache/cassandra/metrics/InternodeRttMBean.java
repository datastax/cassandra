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

/**
 * JMX MBean interface for the per-peer internode round-trip time metric.
 *
 * <p>Allows the metric to be enabled or disabled at runtime without a restart.
 * Because the metric has high cardinality (one histogram per peer × verb combination),
 * it is disabled by default and should only be turned on temporarily for diagnosis.</p>
 *
 * <p>MBean object name:
 * {@code org.apache.cassandra.metrics:type=Messaging,name=InternodeRtt}</p>
 *
 * <p>The metric can also be enabled at startup with the system property
 * {@code cassandra.internode_rtt_metrics_enabled=true}.</p>
 */
public interface InternodeRttMBean
{
    /**
     * Returns {@code true} if per-peer internode RTT recording is currently active.
     */
    boolean isEnabled();

    /**
     * Enables or disables per-peer internode RTT recording at runtime.
     *
     * @param enabled {@code true} to start recording, {@code false} to stop
     */
    void setEnabled(boolean enabled);
}
