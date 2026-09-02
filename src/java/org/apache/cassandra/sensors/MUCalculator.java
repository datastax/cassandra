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

package org.apache.cassandra.sensors;

/**
 * Abstraction for computing coordinator-level Read Measure Units (RMU) and Write Measure Units (WMU).
 *
 * <p>Both values incorporate the coordinator-observed latency for the request alongside the raw byte
 * counts gathered from replicas via the {@link Type#READ_BYTES}, {@link Type#WRITE_BYTES}, and
 * {@link Type#INDEX_WRITE_BYTES} sensors, normalized against per-table baseline throughput values (see
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#BASELINE_READ_BYTES} and
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#BASELINE_WRITE_BYTES}).
 * WMU uses the combined {@code write_bytes + index_write_bytes} as the total write byte count.
 *
 * <p>If either baseline is not configured (i.e., &lt;= 0), the corresponding latency term is not used and the
 * byte-based term degenerates to the raw byte count.
 */
public interface MUCalculator
{
    /**
     * Computes the coordinator-level Read Measure Units (RMU) for the given context.
     *
     * @param sensors        the request sensors holding accumulated byte counts for this request
     * @param context        the keyspace/table context
     * @param readLatencyNanos the end-to-end read latency observed by the coordinator, in nanoseconds
     * @return the RMU value for this request
     */
    double computeRMU(RequestSensors sensors, Context context, long readLatencyNanos);

    /**
     * Computes the coordinator-level Write Measure Units (WMU) for the given context.
     *
     * @param sensors         the request sensors holding accumulated byte counts for this request
     * @param context         the keyspace/table context
     * @param writeLatencyNanos the end-to-end write latency observed by the coordinator, in nanoseconds
     * @return the WMU value for this request
     */
    double computeWMU(RequestSensors sensors, Context context, long writeLatencyNanos);
}
