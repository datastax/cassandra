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
    INTERNODE_BYTES,

    READ_BYTES,

    WRITE_BYTES,
    INDEX_WRITE_BYTES,

    /**
     * Memory types, alternatively, we could use MEMORY_BYTES only type, and have a specialized {@link Context} for
     * memory with MemoryType (ON_HEAP_BYTES, OFF_HEAP_BYTES, UNSAFE_BYTES) and ExpectedLifetime (SHORT, LONG) etc.
     */
    ON_HEAP_BYTES, // for bytes capped at -Xmx
    OFF_HEAP_BYTES, // for bytes capped at -XX:MaxDirectMemorySize
    UNSAFE_BYTES, // not configured, only capped by the amount of physical memory
    OOM_PREDICTION_SECONDS, // time in seconds until OOM is predicted
    CPU_UTILIZATION, // CPU utilization percentage, reflective of load
}