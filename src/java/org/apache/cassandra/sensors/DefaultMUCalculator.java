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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Default implementation of {@link MUCalculator}.
 *
 * <p>RMU and WMU are each computed as:
 * <pre>
 *   RMU = max(read_latency_ns / 1_000_000_000, read_bytes / (baseline_read_bytes_sec / num_cores)) * 4000
 *   WMU = max(write_latency_ns / 1_000_000_000, (write_bytes + index_write_bytes) / (baseline_write_bytes_sec / num_cores)) * 4000
 * </pre>
 *
 * <p>The baseline values are read from the
 * {@link CassandraRelevantProperties#BASELINE_READ_BYTES} and
 * {@link CassandraRelevantProperties#BASELINE_WRITE_BYTES} system properties at construction time
 * and divided by the number of available CPU cores (via {@link FBUtilities#getAvailableProcessors()})
 * to obtain a per-core baseline.
 * If a baseline value is &lt;= 0 the latency term is dropped and the formula simplifies to
 * {@code bytes * 4000}.
 */
public class DefaultMUCalculator implements MUCalculator
{
    private static final double MU_SCALE = 4000.0;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    public static final DefaultMUCalculator instance = new DefaultMUCalculator();

    private final double baselineReadBytesPerCore;
    private final double baselineWriteBytesPerCore;

    private DefaultMUCalculator()
    {
        this(CassandraRelevantProperties.BASELINE_READ_BYTES.getLong(),
             CassandraRelevantProperties.BASELINE_WRITE_BYTES.getLong(),
             FBUtilities.getAvailableProcessors());
    }

    @VisibleForTesting
    public DefaultMUCalculator(double baselineReadBytes, double baselineWriteBytes, int numCores)
    {
        int cores = numCores > 0 ? numCores : 1;
        this.baselineReadBytesPerCore = baselineReadBytes > 0 ? baselineReadBytes / cores : baselineReadBytes;
        this.baselineWriteBytesPerCore = baselineWriteBytes > 0 ? baselineWriteBytes / cores : baselineWriteBytes;
    }

    @Override
    public double computeRMU(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).map(Sensor::getValue).orElse(0.0);
        double readExecutionTimeNanos = sensors.getSensor(context, Type.READ_EXECUTION_TIME).map(Sensor::getValue).orElse(0.0);

        double normalizedBytes = baselineReadBytesPerCore > 0 ? readBytes / baselineReadBytesPerCore : readBytes;
        double normalizedExecutionTime = baselineReadBytesPerCore > 0 ? readExecutionTimeNanos / NANOS_PER_SECOND : 0.0;

        return Math.max(normalizedExecutionTime, normalizedBytes) * MU_SCALE;
    }

    @Override
    public double computeWMU(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).map(Sensor::getValue).orElse(0.0)
                            + sensors.getSensor(context, Type.INDEX_WRITE_BYTES).map(Sensor::getValue).orElse(0.0);
        double writeExecutionTimeNanos = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).map(Sensor::getValue).orElse(0.0);

        double normalizedBytes = baselineWriteBytesPerCore > 0 ? writeBytes / baselineWriteBytesPerCore : writeBytes;
        double normalizedExecutionTime = baselineWriteBytesPerCore > 0 ? writeExecutionTimeNanos / NANOS_PER_SECOND : 0.0;

        return Math.max(normalizedExecutionTime, normalizedBytes) * MU_SCALE;
    }
}
