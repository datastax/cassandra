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
 * <p>Three cost types are supported, computed in order by {@link #computeCost(RequestSensors)}:
 * <ol>
 *   <li>{@link Type#READ_COST} — per keyspace/table, derived from read bytes and execution time.</li>
 *   <li>{@link Type#WRITE_COST} — per keyspace/table, derived from write bytes (including index writes) and execution time.</li>
 *   <li>{@link Type#TOTAL_COST} — a single request-level aggregate across all contexts.</li>
 * </ol>
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
     * Computes the total cost for the entire request, based on all sensors accumulated across all
     * contexts. The returned value is stored in a single {@link Type#TOTAL_COST} sensor keyed on
     * {@link Context#request()}, so there is exactly one total-cost sensor per request regardless
     * of how many tables were touched.
     *
     * <p>Implementations could sum the per-context {@link Type#READ_COST} and
     * {@link Type#WRITE_COST} values already computed by {@link #computeReadCost} /
     * {@link #computeWriteCost}, but are free to combine them in other ways.
     *
     * @param sensors accumulated sensors for this request (all contexts)
     * @return the total cost for the request
     */
    double computeTotalCost(RequestSensors sensors);

    /**
     * Computes all costs for the registered cost sensors in {@code sensors}:
     * <ol>
     *   <li>For each context that has a {@link Type#READ_COST} sensor, invokes
     *       {@link #computeReadCost(RequestSensors, Context)} and increments that sensor.</li>
     *   <li>For each context that has a {@link Type#WRITE_COST} sensor, invokes
     *       {@link #computeWriteCost(RequestSensors, Context)} and increments that sensor.</li>
     *   <li>If a {@link Type#TOTAL_COST} sensor is registered on {@link Context#request()},
     *       invokes {@link #computeTotalCost(RequestSensors)} once and increments that single
     *       request-level sensor. READ_COST and WRITE_COST are fully populated before this step.</li>
     * </ol>
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
        if (sensors.getSensor(Context.request(), Type.TOTAL_COST).isPresent())
        {
            sensors.incrementSensor(Context.request(), Type.TOTAL_COST, INSTANCE.computeTotalCost(sensors));
            hasCost = true;
        }
        if (hasCost)
            sensors.syncAllSensors();
    }
}
