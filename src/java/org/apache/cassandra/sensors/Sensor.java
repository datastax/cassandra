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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;

import org.apache.cassandra.utils.DoubleBinaryPredicate;

/**
 * Tracks the {@link #value} for a given measurement of a given {@link Type} and {@link Context}, during any
 * request/response cycle.
 * <br/><br/>
 * Sensors can be read (via {@link #getValue()}) but cannot be directly created or incremented, because their lifecycle
 * and values are managed by the {@link RequestSensors} and {@link SensorsRegistry} classes, more specifically:
 * <ul>
 *     <li>In order to track a given measurement for a given request/response, register a sensor of the related type via
 *     {@link RequestSensors#registerSensor(Context context, Type)}.</li>
 *     <li>Once registered, the sensor lifecycle spans across multiple request/response cycles, and its "global"
 *     value can be accessed via {@link SensorsRegistry}.</li>
 * </ul>
 */
public class Sensor
{
    private final Context context;
    private final Type type;
    private final AtomicDouble value;

    private final int hashCode;

    protected Sensor(Context context, Type type)
    {
        this.context = context;
        this.type = type;
        this.value = new AtomicDouble();
        this.hashCode = Objects.hash(context, type);
    }

    /**
     * Atomically adds the specified amount to the current measured value of this sensor.
     *
     * @param value The amount to increment by.
     */
    @VisibleForTesting
    public void increment(double value)
    {
        this.value.addAndGet(value);
    }

    /**
     * Atomically sets the current value to {@code candidate} if
     * {@code predicate.test(current, candidate)} returns {@code true}.
     * Retries transparently on CAS contention until either the predicate
     * rejects the update or the swap succeeds.
     *
     * @param candidate the value to conditionally store
     * @param predicate a function of {@code (currentValue, candidate)} returning
     *                  {@code true} if the update should occur
     */
    @VisibleForTesting
    public void setIf(double candidate, DoubleBinaryPredicate predicate)
    {
        double current;
        do
        {
            current = value.get();
            if (!predicate.test(current, candidate))
                return;
        }
        while (!value.compareAndSet(current, candidate));
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
        return hashCode;
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
