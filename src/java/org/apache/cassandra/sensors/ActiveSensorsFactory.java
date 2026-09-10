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
 *   <li>a new {@link ActiveRequestSensors} instance for all keyspaces.</li>
 *   <li>a singleton {@link SensorEncoder} that encodes sensor names on the wire as follows:
 *     <ul>
 *       <li><b>Table-context sensors</b> — {@code <TYPE>_REQUEST.<keyspace>.<table>} for request sensors
 *           and {@code <TYPE>_GLOBAL.<keyspace>.<table>} for global sensors.</li>
 *       <li><b>Request-context sensors</b> (i.e. {@link Type#TOTAL_COST} keyed on {@link Context#request()}) —
 *           {@code <TYPE>_REQUEST} and {@code <TYPE>_GLOBAL}, with no keyspace or table suffix, because
 *           the sensor aggregates cost across the whole request rather than a single table.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class ActiveSensorsFactory implements SensorsFactory
{
    private static final SensorEncoder SENSOR_ENCODER = new SensorEncoder()
    {
        @Override
        public Optional<String> encodeRequestSensorName(Sensor sensor)
        {
            Context ctx = sensor.getContext();
            if (ctx.isRequestContext())
                return Optional.of(sensor.getType() + "_REQUEST");
            return Optional.of(sensor.getType() + "_REQUEST." + ctx.getKeyspace().get() + '.' + ctx.getTable().get());
        }

        @Override
        public Optional<String> encodeGlobalSensorName(Sensor sensor)
        {
            Context ctx = sensor.getContext();
            if (ctx.isRequestContext())
                return Optional.of(sensor.getType() + "_GLOBAL");
            return Optional.of(sensor.getType() + "_GLOBAL." + ctx.getKeyspace().get() + '.' + ctx.getTable().get());
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
