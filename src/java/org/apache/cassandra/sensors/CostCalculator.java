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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Abstraction for computing a coordinator-level cost for read and write operations using {@link Sensor}s.
 *
 * <p>All three cost sensors — {@link Type#READ_COST}, {@link Type#WRITE_COST}, and
 * {@link Type#TOTAL_COST} — are request-scoped and keyed on {@link Context#from(RequestSensors)}, so there is
 * exactly one of each per request regardless of how many tables were touched.
 *
 * <p>The default implementation is {@link NoOpCostCalculator}, which always returns {@code 0}.
 * A custom implementation can be plugged in by setting the
 * {@link CassandraRelevantProperties#COST_CALCULATOR} system property to a fully-qualified class
 * name; the class must have a no-arg constructor and implement this interface.
 */
public interface CostCalculator
{
    CostCalculator INSTANCE = CassandraRelevantProperties.COST_CALCULATOR.isPresent()
                              ? FBUtilities.construct(CassandraRelevantProperties.COST_CALCULATOR.getString(), "cost calculator")
                              : NoOpCostCalculator.instance;

    /**
     * Computes the read cost for the entire request.
     *
     * @param sensors accumulated sensors for this request (all contexts)
     * @return the read cost for the request
     */
    double computeReadCost(RequestSensors sensors);

    /**
     * Computes the write cost for the entire request.
     *
     * @param sensors accumulated sensors for this request (all contexts)
     * @return the write cost for the request
     */
    double computeWriteCost(RequestSensors sensors);

    /**
     * Computes the total cost for the entire request.
     *
     * @param sensors accumulated sensors for this request (all contexts)
     * @return the total cost for the request
     */
    double computeTotalCost(RequestSensors sensors);

    /**
     * Populates all cost sensors that are registered in {@code sensors}, in the given order:
     * <ol>
     *   <li>If a {@link Type#READ_COST} sensor is registered on {@link Context#from(RequestSensors)}, invokes
     *       {@link #computeReadCost(RequestSensors)} once and stores the result.</li>
     *   <li>If a {@link Type#WRITE_COST} sensor is registered on {@link Context#from(RequestSensors)}, invokes
     *       {@link #computeWriteCost(RequestSensors)} once and stores the result.</li>
     *   <li>If a {@link Type#TOTAL_COST} sensor is registered on {@link Context#from(RequestSensors)}, invokes
     *       {@link #computeTotalCost(RequestSensors)} once and stores the result.</li>
     * </ol>
     * Sensors for cost types not registered in {@code sensors} are silently skipped.
     * All populated sensors are synced to the global {@link SensorsRegistry} before returning.
     *
     * @param sensors the request sensors for the current request
     */
    static void populateCostSensors(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        boolean hasCost = false;
        Context requestContext = Context.from(sensors);
        if (sensors.getSensor(requestContext, Type.READ_COST).isPresent())
        {
            sensors.incrementSensor(requestContext, Type.READ_COST, INSTANCE.computeReadCost(sensors));
            hasCost = true;
        }
        if (sensors.getSensor(requestContext, Type.WRITE_COST).isPresent())
        {
            sensors.incrementSensor(requestContext, Type.WRITE_COST, INSTANCE.computeWriteCost(sensors));
            hasCost = true;
        }
        if (sensors.getSensor(requestContext, Type.TOTAL_COST).isPresent())
        {
            sensors.incrementSensor(requestContext, Type.TOTAL_COST, INSTANCE.computeTotalCost(sensors));
            hasCost = true;
        }
        if (hasCost)
            sensors.syncAllSensors();
    }
}
