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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.utils.Pair;

/**
 * Groups {@link Sensor}s associated to a given request/response and related {@link Context}: this is the main entry
 * point to create and modify sensors. More specifically:
 * <ul>
 *     <li>Create a new sensor associated to the request/response via {@link #registerSensor(Context, Type)}.</li>
 *     <li>Increment the sensor value for the request/response and sync it to {@link SensorsRegistry} via {@link #incrementThenSyncSensor(Context, Type, double)}.</li>
 * </ul>
 * Sensor values related to a given request/response are isolated from other sensors, and the "same" sensor
 * (for a given context and type) registered to different requests/responses will have a different value. However,
 * the sensor values are automatically propagated to the global {@link SensorsRegistry} via {@link #incrementThenSyncSensor(Context, Type, double)}.
 * to alleviate the overhead of synchronization at the global to the {@link SensorsRegistry} in a separate call.
 */
public class RequestSensors
{
    private final Supplier<SensorsRegistry> sensorsRegistry;
    private final ConcurrentMap<Pair<Context, Type>, Sensor> sensors = new ConcurrentHashMap<>();

    public RequestSensors()
    {
        this(() -> SensorsRegistry.instance);
    }

    public RequestSensors(Supplier<SensorsRegistry> sensorsRegistry)
    {
        this.sensorsRegistry = sensorsRegistry;
    }

    public void registerSensor(Context context, Type type)
    {
        sensors.putIfAbsent(Pair.create(context, type), new Sensor(context, type));
    }

    public Optional<Sensor> getSensor(Context context, Type type)
    {
        return Optional.ofNullable(sensors.get(Pair.create(context, type)));
    }

    public Set<Sensor> getSensors(Type type)
    {
        return sensors.values().stream().filter(s -> s.getType() == type).collect(Collectors.toSet());
    }

    public void incrementThenSyncSensor(Context context, Type type, double value)
    {
        Optional.ofNullable(sensors.get(Pair.create(context, type))).ifPresent(s -> {
            s.increment(value);
            // automatically sync the sensor value to the global registry
            sensorsRegistry.get().incrementSensor(s.getContext(), s.getType(), value);
        });
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestSensors sensors1 = (RequestSensors) o;
        return Objects.equals(sensors, sensors1.sensors);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sensors);
    }

    @Override
    public String toString()
    {
        return "RequestSensors{" +
               "sensors=" + sensors +
               '}';
    }
}
