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

import org.apache.cassandra.utils.FBUtilities;

/**
 * Test-only {@link CostCalculator} implementing a byte+execution-time cost formula.
 * Kept in test sources so that the production tree ships only the {@link NoopCostCalculator},
 * while unit tests can still assert non-zero cost values.
 *
 * <pre>
 *   readCost  = max(read_latency_ns / 1e9, read_bytes / (baseline_read / cores)) * 4000
 *   writeCost = max(write_latency_ns / 1e9, (write_bytes + index_write_bytes) / (baseline_write / cores)) * 4000
 * </pre>
 *
 * <p>When constructed with the no-arg constructor (used by {@link TestSensorsFactory}), baseline
 * values default to {@code -1} (not configured) and cost reduces to {@code bytes * 4000}.
 * Tests that need exact normalized values should use the explicit 2- or 3-arg constructor.
 */
public class TestCostCalculator implements CostCalculator
{
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final double DEFAULT_COST_SCALE = 4000.0;
    private static final long DEFAULT_BASELINE = -1L;

    /** Singleton used by {@link TestSensorsFactory} so that {@link CostCalculator#INSTANCE} is non-noop in tests. */
    public static final TestCostCalculator instance = new TestCostCalculator();

    private final double costScale;
    private final double baselineReadBytesPerCore;
    private final double baselineWriteBytesPerCore;

    /**
     * Default constructor uses baseline=-1 (not configured) and cost_scale=4000.
     * Cost reduces to {@code bytes * 4000} — suitable for tests that only assert positivity.
     */
    public TestCostCalculator()
    {
        this(DEFAULT_BASELINE, DEFAULT_BASELINE, FBUtilities.getAvailableProcessors());
    }

    public TestCostCalculator(double baselineReadBytes, double baselineWriteBytes)
    {
        this(baselineReadBytes, baselineWriteBytes, FBUtilities.getAvailableProcessors());
    }

    public TestCostCalculator(double baselineReadBytes, double baselineWriteBytes, int numCores)
    {
        this.costScale = DEFAULT_COST_SCALE;
        int cores = numCores > 0 ? numCores : 1;
        this.baselineReadBytesPerCore = baselineReadBytes > 0 ? baselineReadBytes / cores : baselineReadBytes;
        this.baselineWriteBytesPerCore = baselineWriteBytes > 0 ? baselineWriteBytes / cores : baselineWriteBytes;
    }

    @Override
    public double computeReadCost(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).map(Sensor::getValue).orElse(0.0);
        double readExecutionTimeNanos = sensors.getSensor(context, Type.READ_EXECUTION_TIME).map(Sensor::getValue).orElse(0.0);

        double normalizedBytes = baselineReadBytesPerCore > 0 ? readBytes / baselineReadBytesPerCore : readBytes;
        double normalizedExecutionTime = baselineReadBytesPerCore > 0 ? readExecutionTimeNanos / NANOS_PER_SECOND : 0.0;

        return Math.max(normalizedExecutionTime, normalizedBytes) * costScale;
    }

    @Override
    public double computeWriteCost(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).map(Sensor::getValue).orElse(0.0)
                            + sensors.getSensor(context, Type.INDEX_WRITE_BYTES).map(Sensor::getValue).orElse(0.0);
        double writeExecutionTimeNanos = sensors.getSensor(context, Type.WRITE_EXECUTION_TIME).map(Sensor::getValue).orElse(0.0);

        double normalizedBytes = baselineWriteBytesPerCore > 0 ? writeBytes / baselineWriteBytesPerCore : writeBytes;
        double normalizedExecutionTime = baselineWriteBytesPerCore > 0 ? writeExecutionTimeNanos / NANOS_PER_SECOND : 0.0;

        return Math.max(normalizedExecutionTime, normalizedBytes) * costScale;
    }

    @Override
    public double computeTotalCost(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        double rmu = sensors.getSensor(context, Type.RMU).map(Sensor::getValue).orElse(0.0);
        double wmu = sensors.getSensor(context, Type.WMU).map(Sensor::getValue).orElse(0.0);
        return rmu + wmu;
    }
}
