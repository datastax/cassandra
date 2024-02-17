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

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.cassandra.utils.Pair;

/**
 * A table scoped sensor that facilitates registering the sensor to the provided {@link SensorsRegistry} and syncing the sensor value.
 * This sensor is not linked with a specific request/response cycle, and its value is ever-increasing.
 */
public class TableSensor extends Sensor
{
    private final Supplier<SensorsRegistry> sensorsRegistry;

    public TableSensor(Context context, Type type)
    {
        this(() -> SensorsRegistry.instance, context, type);
    }

    protected TableSensor(Supplier<SensorsRegistry> sensorsRegistry, Context context, Type type)
    {
        super(context, type);
        this.sensorsRegistry = sensorsRegistry;
    }

    public void incrementSensor(double value)
    {
        super.increment(value);
    }


    /**
     * Syncs the sensor value to the global {@link SensorsRegistry}.
     */
    public void syncSensor()
    {
        sensorsRegistry.get().updateSensor(this.getContext(), this.getType(), this.getValue());
        this.reset();
    }
}
