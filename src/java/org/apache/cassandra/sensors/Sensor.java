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
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Tracks the {@link #value} for a given measurement of a given {@link Type} and {@link Context}, during any
 * request/response cycle.
 * <br/><br/>
 * Sensors can be read (via {@link #getValue()}) but cannot be directly created or incremented, because their lifecycle
 * and values are managed by the {@link RequestSensors} and {@link SensorsRegistry} classes, more specifically:
 * <ul>
 *     <li>In order to track a given measurement for a given request/response, register a sensor of the related type via
 *     {@link RequestSensors#registerSensor(Type)}.</li>
 *     <li>Once registered, the sensor lifecycle spans across multiple request/response cycles, and its "global"
 *     value can be accessed via {@link SensorsRegistry}.</li>
 * </ul>
 */
public class Sensor
{
    private final Context context;
    private final Type type;
    private final AtomicDouble value;
    private long lastSnapshotTime;
    private double lastSnapshotValue;

    protected Sensor(Context context, Type type)
    {
        this.context = context;
        this.type = type;
        this.value = new AtomicDouble();
    }

    protected void increment(double value)
    {
        this.increment(value, (ignored) -> false, 0L);
    }

    protected void increment(double value, Predicate<Sensor> snapshotSensorValue, long now)
    {
        double oldValue = this.value.getAndAdd(value);
        if (snapshotSensorValue.test(this)) {
            this.lastSnapshotValue = oldValue;
            this.lastSnapshotTime = now;
        }
    }

    public Context getContext()
    {
        return context;
    }

    public Type getType()
    {
        return type;
    }

    public double getValue()
    {
        return value.doubleValue();
    }

    public long getLastSnapshotTime()
    {
        return lastSnapshotTime;
    }

    public double getLastSnapshotValue()
    {
        return lastSnapshotValue;
    }

    @VisibleForTesting
    public void reset()
    {
        value.set(0);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sensor sensor = (Sensor) o;
        return Objects.equals(context, sensor.context) && type == sensor.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(context, type);
    }

    @Override
    public String toString()
    {
        return "Sensor{" +
               "context=" + context +
               ", type=" + type +
               ", value=" + value +
               '}';
    }
}
