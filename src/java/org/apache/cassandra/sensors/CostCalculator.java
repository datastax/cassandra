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

import com.google.common.base.Preconditions;

/**
 * Abstraction for computing a coordinator-level cost for read and write operations.
 *
 * <p>The C* layer ships with a {@link NoopCostCalculator} that always returns {@code 0}.
 * Concrete implementations that derive cost from byte counts and execution time should live
 * outside this module and be registered via {@link SensorsFactory#createCostCalculator()}.
 */
public interface CostCalculator
{
    CostCalculator INSTANCE = SensorsFactory.instance.createCostCalculator();

    /**
     * Computes the read cost for the given context, based on the given request sensors.
     *
     * @param sensors accumulated sensors for this request
     * @param context the keyspace/table context
     * @return the read cost
     */
    double computeReadCost(RequestSensors sensors, Context context);

    /**
     * Computes the write cost for the given context, based on the given request sensors.
     *
     * @param sensors accumulated sensors for this request
     * @param context the keyspace/table context
     * @return the write cost
     */
    double computeWriteCost(RequestSensors sensors, Context context);

    /**
     * Computes the total cost for the given context, based on the given request sensors.
     * <br/>
     * The total cost represents the combined cost of all read and write operations within the request.
     *
     * @param sensors accumulated sensors for this request
     * @param context the keyspace/table context
     * @return the total cost
     */
    double computeTotalCost(RequestSensors sensors, Context context);

    /**
     * Computes all costs for the registered cost sensors in {@code sensors}: for each context that has a
     * {@link Type#READ_COST}, {@link Type#WRITE_COST}, or {@link Type#TOTAL_COST} sensor registered,
     * invokes the corresponding instance method and increments that sensor by the result.
     *
     * <p>The computation follows a fixed order — READ_COST, then WRITE_COST, then TOTAL_COST — so that
     * TOTAL_COST (which aggregates the other two) always sees fully populated values.
     * Cost sensors for types not registered in {@code sensors} are silently skipped.
     *
     * @param sensors the request sensors for the current request
     */
    static void computeCost(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        boolean hasCost = false;
        for (Sensor sensor : sensors.getSensors(s -> s.getType() == Type.READ_COST))
        {
            Context context = sensor.getContext();
            sensors.incrementSensor(context, Type.READ_COST, INSTANCE.computeReadCost(sensors, context));
            hasCost = true;
        }
        for (Sensor sensor : sensors.getSensors(s -> s.getType() == Type.WRITE_COST))
        {
            Context context = sensor.getContext();
            sensors.incrementSensor(context, Type.WRITE_COST, INSTANCE.computeWriteCost(sensors, context));
            hasCost = true;
        }
        for (Sensor sensor : sensors.getSensors(s -> s.getType() == Type.TOTAL_COST))
        {
            Context context = sensor.getContext();
            sensors.incrementSensor(context, Type.TOTAL_COST, INSTANCE.computeTotalCost(sensors, context));
            hasCost = true;
        }
        if (hasCost)
            sensors.syncAllSensors();
    }
}
