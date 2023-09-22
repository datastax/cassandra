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

public class Sensor
{
    private final Context context;
    private final Type type;
    private final AtomicDouble value;

    protected Sensor(Context context, Type type)
    {
        this.context = context;
        this.type = type;
        this.value = new AtomicDouble();
    }

    protected void increment(double value)
    {
        this.value.addAndGet(value);
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
        return Objects.hash(context, type);
    }

    @Override
    public String toString()
    {
        return "Sensor{" +
               "context=" + context +
               ", value=" + value +
               '}';
    }
}
