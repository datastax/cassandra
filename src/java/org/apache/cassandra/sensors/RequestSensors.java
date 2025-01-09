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

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Groups {@link Sensor}s associated to a given request/response and related {@link Context}: this is the main entry
 * point to create and modify sensors. Actual implementations can be created via {@link SensorsFactory}.
 */
public interface RequestSensors
{
    /**
     * Register a new sensor associated to the given context and type. It is up to the implementation to decide the
     * idempotency of this operation.
     *
     * @param context the sensor context associated with the request/response
     * @param type    the type of the sensor
     */
    void registerSensor(Context context, Type type);

    /**
     * Returns the sensor associated to the given context and type, if any.
     *
     * @param context the sensor context associated with the request/response
     * @param type    the type of the sensor
     * @return the sensor associated to the given context and type, if any
     */
    Optional<Sensor> getSensor(Context context, Type type);

    /**
     * Returns all the sensors that match the given filter
     *
     * @param filter a predicate applied to each sensor to decide if it should be included in the returned collection
     * @return a collection of sensors matching the given predicate
     */
    Collection<Sensor> getSensors(Predicate<Sensor> filter);

    /**
     * Increment the sensor value associated to the given context and type by the given value.
     *
     * @param context the sensor context associated with the request/response
     * @param type    the type of the sensor
     * @param value   the value to increment the sensor by
     */
    void incrementSensor(Context context, Type type, double value);

    /**
     * Sync all the sensors values tracked for this request to the global {@link SensorsRegistry}. This method
     * will be called at least once per request/response so it is recommended to make the implementation idempotent.
     */
    void syncAllSensors();
}
