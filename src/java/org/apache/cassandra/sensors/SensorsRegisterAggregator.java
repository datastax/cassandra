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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An aggregator function that can be used to aggregate sensors of a given type. The type of the sensor is
 * specified at {@link SensorsRegistry#registerSensorAggregator(SensorsRegisterAggregator, Type)}.
 */
public interface SensorsRegisterAggregator
{
    SensorsRegisterAggregator DEFAULT =  new SensorsRegisterAggregator() {};
    /**
     * Specifies how a sensor will be aggregated. For example, to simply sum up all sensors values, s -> s.getValue() would suffice.
     * Defaults to a function that returns the sensor value.
     */
    default Function<Sensor, Double> aggregatorFn()
    {
        return Sensor::getValue;
    }

    /**
     * Specifies a filter to be applied to the sensors of a given type. For example, to only aggregate sensors for a given keyspace,
     * one can specify s -> s.getKeyspace().equals("myKeyspace"). Defaults to a filter that accepts all sensors.
     */
    default Predicate<Sensor> aggregatorFilter()
    {
        return s -> true;
    }
}

