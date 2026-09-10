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
     * Computes the read cost for every READ_COST sensor registered in {@code sensors} and increments each sensor by the
     * computed value. Must be called <em>after</em> all other sensor increments for the request are complete and
     * <em>before</em> the final {@link RequestSensors#syncAllSensors()} call. Because intermediate
     * {@code syncAllSensors()} calls earlier in the request path skip READ_COST (its value is 0 until this method runs,
     * so the delta is 0 and the registry is not touched), the final sync after this call is the one that delivers
     * the correct READ_COST value to the global {@link SensorsRegistry}.
     * Must also be called before {@link SensorsCustomParams#addSensorsToInternodeResponse} so the response message
     * carries the correct READ_COST value.
     *
     * @param sensors the request sensors for the current request
     */
    static void computeReadCost(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        for (Sensor readCostSensor : sensors.getSensors(s -> s.getType() == Type.READ_COST))
        {
            Context context = readCostSensor.getContext();
            sensors.incrementSensor(context, Type.READ_COST, INSTANCE.computeReadCost(sensors, context));
        }
    }

    /**
     * Computes the write cost for every WRITE_COST sensor registered in {@code sensors} and increments each sensor by the
     * computed value. Must be called <em>after</em> all other sensor increments for the request are complete and
     * <em>before</em> the final {@link RequestSensors#syncAllSensors()} call. Because intermediate
     * {@code syncAllSensors()} calls earlier in the request path skip WRITE_COST (its value is 0 until this method runs,
     * so the delta is 0 and the registry is not touched), the final sync after this call is the one that delivers
     * the correct WRITE_COST value to the global {@link SensorsRegistry}.
     * Must also be called before {@link SensorsCustomParams#addSensorsToInternodeResponse} so the response message
     * carries the correct WRITE_COST value.
     *
     * @param sensors the request sensors for the current request
     */
    static void computeWriteCost(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        for (Sensor writeCostSensor : sensors.getSensors(s -> s.getType() == Type.WRITE_COST))
        {
            Context context = writeCostSensor.getContext();
            sensors.incrementSensor(context, Type.WRITE_COST, INSTANCE.computeWriteCost(sensors, context));
        }
    }

    /**
     * Computes the total cost for every {@link Type#TOTAL_COST} sensor registered in {@code sensors}
     * and increments each sensor by the computed value. Must be called <em>after</em> both
     * {@link #computeReadCost(RequestSensors)} and {@link #computeWriteCost(RequestSensors)} so that
     * READ_COST and WRITE_COST values are already populated.
     * {@link Type#TOTAL_COST} is synced to the global {@link SensorsRegistry} via the normal
     * {@link RequestSensors#syncAllSensors()} call but is <em>never</em> included in CQL responses.
     *
     * @param sensors the request sensors for the current request
     */
    static void computeTotalCost(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        for (Sensor totalCostSensor : sensors.getSensors(s -> s.getType() == Type.TOTAL_COST))
        {
            Context context = totalCostSensor.getContext();
            sensors.incrementSensor(context, Type.TOTAL_COST, INSTANCE.computeTotalCost(sensors, context));
        }
    }
}
