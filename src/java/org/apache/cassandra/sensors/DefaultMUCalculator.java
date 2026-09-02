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

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * Default implementation of {@link MUCalculator}.
 *
 * <p>RMU and WMU are each computed as:
 * <pre>
 *   RMU = max(read_latency_nanos / 1_000_000_000, read_bytes / baseline_read_bytes) * 4000
 *   WMU = max(write_latency_nanos / 1_000_000_000, (write_bytes + index_write_bytes) / baseline_write_bytes) * 4000
 * </pre>
 *
 * <p>The baseline values are read from the
 * {@link CassandraRelevantProperties#BASELINE_READ_BYTES} and
 * {@link CassandraRelevantProperties#BASELINE_WRITE_BYTES} system properties at construction time.
 * If a baseline value is &lt;= 0 the latency term is dropped and the formula simplifies to
 * {@code bytes * 4000}.
 */
public class DefaultMUCalculator implements MUCalculator
{
    private static final double MU_SCALE = 4000.0;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    public static final DefaultMUCalculator instance = new DefaultMUCalculator();

    private final double baselineReadBytes;
    private final double baselineWriteBytes;

    public DefaultMUCalculator()
    {
        this(CassandraRelevantProperties.BASELINE_READ_BYTES.getLong(),
             CassandraRelevantProperties.BASELINE_WRITE_BYTES.getLong());
    }

    public DefaultMUCalculator(double baselineReadBytes, double baselineWriteBytes)
    {
        this.baselineReadBytes = baselineReadBytes;
        this.baselineWriteBytes = baselineWriteBytes;
    }

    @Override
    public double computeRMU(RequestSensors sensors, Context context, long readLatencyNanos)
    {
        if (sensors == null || context == null)
            return 0.0;

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).map(Sensor::getValue).orElse(0.0);

        double normalizedBytes = baselineReadBytes > 0 ? readBytes / baselineReadBytes : readBytes;
        double normalizedLatency = baselineReadBytes > 0 ? readLatencyNanos / NANOS_PER_SECOND : 0.0;

        return Math.max(normalizedLatency, normalizedBytes) * MU_SCALE;
    }

    @Override
    public double computeWMU(RequestSensors sensors, Context context, long writeLatencyNanos)
    {
        if (sensors == null || context == null)
            return 0.0;

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).map(Sensor::getValue).orElse(0.0)
                            + sensors.getSensor(context, Type.INDEX_WRITE_BYTES).map(Sensor::getValue).orElse(0.0);

        double normalizedBytes = baselineWriteBytes > 0 ? writeBytes / baselineWriteBytes : writeBytes;
        double normalizedLatency = baselineWriteBytes > 0 ? writeLatencyNanos / NANOS_PER_SECOND : 0.0;

        return Math.max(normalizedLatency, normalizedBytes) * MU_SCALE;
    }
}
