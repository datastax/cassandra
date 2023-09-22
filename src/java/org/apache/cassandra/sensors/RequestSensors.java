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

public class RequestSensors
{
    private final Context context;
    private final ConcurrentMap<Type, Sensor> sensors = new ConcurrentHashMap<>();

    public RequestSensors(Context context)
    {
        this.context = context;
    }

    public boolean registerSensor(Type type)
    {
        return sensors.putIfAbsent(type, new Sensor(context, type)) == null;
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
        sensors.values().forEach(s -> SensorsRegistry.instance.updateSensor(s.getContext(), s.getType(), s.getValue()));
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
