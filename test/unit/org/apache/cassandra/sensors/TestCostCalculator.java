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
 * Test-only {@link CostCalculator} implementing a simple additive cost formula:
 * <pre>
 *   readCost  = read_bytes
 *   writeCost = write_bytes + index_write_bytes
 *   totalCost = sum(READ_COST) + sum(WRITE_COST)
 * </pre>
 *
 * Kept in test sources so that the production tree ships only the {@link NoOpCostCalculator},
 * while unit tests can still assert non-zero cost values.
 */
public class TestCostCalculator implements CostCalculator
{
    @Override
    public double computeReadCost(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        return sensors.getSensor(context, Type.READ_BYTES).map(Sensor::getValue).orElse(0.0);
    }

    @Override
    public double computeWriteCost(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        return sensors.getSensor(context, Type.WRITE_BYTES).map(Sensor::getValue).orElse(0.0)
               + sensors.getSensor(context, Type.INDEX_WRITE_BYTES).map(Sensor::getValue).orElse(0.0);
    }

    @Override
    public double computeTotalCost(RequestSensors sensors)
    {
        if (sensors == null)
            return 0.0;

        double readCost = sensors.getSensors(s -> s.getType() == Type.READ_COST)
                                 .stream()
                                 .mapToDouble(Sensor::getValue)
                                 .sum();
        double writeCost = sensors.getSensors(s -> s.getType() == Type.WRITE_COST)
                                  .stream()
                                  .mapToDouble(Sensor::getValue)
                                  .sum();
        return readCost + writeCost;
    }
}
