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
 * The type of the measurement a {@link Sensor} refers to.
 */
public enum Type
{
    /** Inbound and outbound internode message bytes for a request/response cycle. */
    INTERNODE_BYTES,

    /** Bytes read from storage (memtable or SSTable) on a replica. */
    READ_BYTES,

    /** Bytes written to the primary table memtable on a replica. */
    WRITE_BYTES,

    /** Bytes written to secondary indexes on a replica. */
    INDEX_WRITE_BYTES,

    /**
     * Wall-clock execution time in nanoseconds for a read operation, taken on each replica and the
     * coordinator: please note execution times are not summed up but rather recorded separately, with the coordinator
     * one recording the whole span of a request.
     */
    READ_EXECUTION_TIME,

    /**
     * Wall-clock execution time in nanoseconds for a write operation, taken on each replica and the
     * coordinator: please note execution times are not summed up but rather recorded separately, with the coordinator
     * one recording the whole span of a request.
     */
    WRITE_EXECUTION_TIME,
    /**
     * Read Measure Units: computed in {@link org.apache.cassandra.sensors.SensorsCustomParams#computeRMU(RequestSensors, long)}
     */
    RMU,

    /**
     * Write Measure Units: computed in {@link org.apache.cassandra.sensors.SensorsCustomParams#computeWMU(RequestSensors, long)}
     */
    WMU,
    /**
     * Total Measure Units: computed in {@link org.apache.cassandra.sensors.SensorsCustomParams#computeTMU(RequestSensors)}
     */
    TMU;
}