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

/**
 * Implementation of the {@link SensorsFactory} that creates:
 * <ul>
 *   <li> a new {@link ActiveRequestSensors} instance for all keyspaces.</li>
 *   <li> a singleton {@link SensorEncoder} implementation that encodes the sensor name as "<SENSOR_TYPE>_REQUEST.<TABLE_NAME>" for request sensors and "<SENSOR_TYPE>_GLOBAL.<TABLE_NAME>" for global sensors.</li>
 * </ul>
 */
public class ActiveSensorsFactory implements SensorsFactory
{
    private static final SensorEncoder SENSOR_ENCODER = new SensorEncoder()
    {
        @Override
        public Optional<String> encodeRequestSensorName(Sensor sensor)
        {
            return Optional.of(sensor.getType() + "_REQUEST." + sensor.getContext().getKeyspace() + '.' + sensor.getContext().getTable());
        }

        @Override
        public Optional<String> encodeGlobalSensorName(Sensor sensor)
        {
            return Optional.of(sensor.getType() + "_GLOBAL." + sensor.getContext().getKeyspace() + '.' + sensor.getContext().getTable());
        }
    };

    @Override
    public RequestSensors createRequestSensors(String... keyspaces)
    {
        return new ActiveRequestSensors();
    }

    @Override
    public SensorEncoder createSensorEncoder()
    {
        return SENSOR_ENCODER;
    }
}
