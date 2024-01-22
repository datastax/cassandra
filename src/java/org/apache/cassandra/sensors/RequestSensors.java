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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Groups {@link Sensor}s associated to a given request/response and related {@link Context}: this is the main entry
 * point to create and modify sensors. More specifically:
 * <ul>
 *     <li>Create a new sensor associated to the request/response via {@link #registerSensor(Type)}.</li>
 *     <li>Increment the sensor value for the request/response via {@link #incrementSensor(Type, double)}.</li>
 *     <li>Sync this request/response sensor value to the {@link SensorsRegistry} via {@link #syncAllSensors()}.</li>
 * </ul>
 * Sensor values related to a given request/response are isolated from other sensors, and the "same" sensor
 * (for a given context and type) registered to different requests/responses will have a different value: in other words,
 * there is no automatic synchronization or coordination across sensor values belonging to different
 * {@link RequestSensors} objects, hence {@link #syncAllSensors()} MUST be invoked to propagate the sensors values
 * at a global level to the {@link SensorsRegistry}.
 */
public class RequestSensors
{
    private final Supplier<SensorsRegistry> sensorsRegistry;
    private final Context context;
    private final ConcurrentMap<Type, Sensor> sensors = new ConcurrentHashMap<>();

    public RequestSensors(Context context)
    {
        this(() -> SensorsRegistry.instance, context);
    }

    public RequestSensors(Supplier<SensorsRegistry> sensorsRegistry, Context context)
    {
        this.sensorsRegistry = sensorsRegistry;
        this.context = context;
    }

    public void registerSensor(Type type)
    {
        sensors.putIfAbsent(type, new Sensor(context, type));
    }

    public Optional<Sensor> getSensor(Type type)
    {
        return Optional.ofNullable(sensors.get(type));
    }

    public void incrementSensor(Type type, double value)
    {
        Optional.ofNullable(sensors.get(type)).ifPresent(s -> s.increment(value));
    }

    public void syncAllSensors()
    {
        sensors.values().forEach(s -> sensorsRegistry.get().updateSensor(s.getContext(), s.getType(), s.getValue()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestSensors sensors1 = (RequestSensors) o;
        return Objects.equals(context, sensors1.context) && Objects.equals(sensors, sensors1.sensors);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(context, sensors);
    }

    @Override
    public String toString()
    {
        return "RequestSensors{" +
               "context=" + context +
               ", sensors=" + sensors +
               '}';
    }
}
