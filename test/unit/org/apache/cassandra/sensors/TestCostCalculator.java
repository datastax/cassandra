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
 *   readCost  = sum of READ_BYTES across all table contexts
 *   writeCost = sum of (WRITE_BYTES + INDEX_WRITE_BYTES) across all table contexts
 *   totalCost = readCost + writeCost
 * </pre>
 * <p>
 * Kept in test sources so that the production tree ships only the {@link NoOpCostCalculator},
 * while unit tests can still assert non-zero cost values.
 */
public class TestCostCalculator implements CostCalculator
{
    @Override
    public double computeReadCost(RequestSensors sensors)
    {
        if (sensors == null)
            return 0.0;

        return sensors.getSensors(s -> s.getType() == Type.READ_BYTES && !s.getContext().isRequestContext())
                      .stream()
                      .mapToDouble(Sensor::getValue)
                      .sum();
    }

    @Override
    public double computeWriteCost(RequestSensors sensors)
    {
        if (sensors == null)
            return 0.0;

        return sensors.getSensors(s -> (s.getType() == Type.WRITE_BYTES || s.getType() == Type.INDEX_WRITE_BYTES) && !s.getContext().isRequestContext())
                      .stream()
                      .mapToDouble(Sensor::getValue)
                      .sum();
    }

    @Override
    public double computeTotalCost(RequestSensors sensors)
    {
        if (sensors == null)
            return 0.0;

        return sensors.getSensor(Context.from(sensors), Type.READ_COST).map(Sensor::getValue).orElse(0.0)
               + sensors.getSensor(Context.from(sensors), Type.WRITE_COST).map(Sensor::getValue).orElse(0.0);
    }
}
