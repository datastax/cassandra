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

/**
 * Implementation of the {@link RequestSensorsFactory} that creates a new instance of {@link ActiveRequestSensors}
 * enabled for all keyspaces.
 */
public class ActiveRequestSensorsFactory implements RequestSensorsFactory
{
    private static final Function<Sensor, String> REQUEST_SENSOR_ENCODER = sensor -> sensor.getType() + "_REQUEST." + sensor.getContext().getTable();
    private static final Function<Sensor, String> REGISTRY_SENSOR_ENCODER = sensor -> sensor.getType() + "_TABLE." + sensor.getContext().getTable();

    @Override
    public RequestSensors create(String keyspace)
    {
        return new ActiveRequestSensors();
    }

    @Override
    public Function<Sensor, String> requestSensorEncoder()
    {
        return REQUEST_SENSOR_ENCODER;
    }

    @Override
    public Function<Sensor, String> registrySensorEncoder()
    {
        return REGISTRY_SENSOR_ENCODER;
    }
}
