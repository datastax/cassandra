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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Groups {@link Sensor}s associated to a given request/response and related {@link Context}: this is the main entry
 * point to create and modify sensors. More specifically:
 * <ul>
 *     <li>Create a new sensor associated to the request/response via {@link #registerSensor(Context, Type)}.</li>
 *     <li>Increment the sensor value for the request/response via {@link #incrementSensor(Context, Type, double)}.</li>
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

    // Using Map of array values for performance reasons to avoid wrapping key into another Object (.eg. Pair(context,type)).
    // Note that array values can contain NULL so be careful to filter NULLs when iterating over array
    private final HashMap<Context, Sensor[]> sensors = new LinkedHashMap<>();

    private final Map<Sensor, Double> latestSyncedValuePerSensor = new HashMap<>();

    public RequestSensors()
    {
        this(() -> SensorsRegistry.instance);
    }

    public RequestSensors(Supplier<SensorsRegistry> sensorsRegistry)
    {
        this.sensorsRegistry = sensorsRegistry;
    }

    public synchronized void registerSensor(Context context, Type type)
    {
        Sensor[] typeSensors = sensors.computeIfAbsent(context, key ->
        {
            Sensor[] newTypeSensors = new Sensor[Type.values().length];
            newTypeSensors[type.ordinal()] = new Sensor(context, type);
            return newTypeSensors;
        });
        if (typeSensors[type.ordinal()] == null)
            typeSensors[type.ordinal()] = new Sensor(context, type);
    }

    public synchronized Optional<Sensor> getSensor(Context context, Type type)
    {
        return Optional.ofNullable(getSensorFast(context, type));
    }

    public synchronized Set<Sensor> getSensors(Type type)
    {
        return sensors.values().stream().flatMap(Arrays::stream).filter(Objects::nonNull).filter(s -> s.getType() == type).collect(Collectors.toSet());
    }

    public synchronized void incrementSensor(Context context, Type type, double value)
    {
        Sensor sensor = getSensorFast(context, type);
        if (sensor != null)
            sensor.increment(value);
    }

    public synchronized void syncAllSensors()
    {
        sensors.values().forEach(types -> {
            for (int i = 0; i < types.length; i++)
            {
                if (types[i] != null)
                {
                    Sensor sensor = types[i];
                    double current = latestSyncedValuePerSensor.getOrDefault(sensor, 0d);
                    double update = sensor.getValue() - current;
                    if (update == 0d)
                        continue;

                    latestSyncedValuePerSensor.put(sensor, sensor.getValue());
                    sensorsRegistry.get().incrementSensor(sensor.getContext(), sensor.getType(), update);
                }
            }
        });
    }

    /**
     * To get best perfromance we are not returning Optional here
     */
    @Nullable
    private Sensor getSensorFast(Context context, Type type)
    {
        Sensor[] typeSensors = sensors.get(context);
        if (typeSensors != null)
            return typeSensors[type.ordinal()];
        return null;
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
