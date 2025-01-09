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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.SENSORS_FACTORY;

/**
 * Provides a factory to customize the behaviour of sensors tracking in CNDB by providing two factory methods:
 * <ul>
 *   <li>{@link SensorsFactory#createRequestSensors} provides a {@link RequestSensors} implementation to track sensors per keyspace.</li>
 *   <li>{@link SensorsFactory#createSensorEncoder} provides a {@link SensorEncoder} implementation to control how sensors are encoded as string on the wire.</li>
 * </ul>
 * The concrete implementation of this factory is configured by the {@link CassandraRelevantProperties#SENSORS_FACTORY} system property.
 */
public interface SensorsFactory
{
    SensorsFactory instance = SENSORS_FACTORY.getString() == null ?
                              new SensorsFactory() {} :
                              FBUtilities.construct(CassandraRelevantProperties.SENSORS_FACTORY.getString(), "sensors factory");

    SensorEncoder NOOP_SENSOR_ENCODER = new SensorEncoder()
    {
        @Override
        public Optional<String> encodeRequestSensorName(Sensor sensor)
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> encodeGlobalSensorName(Sensor sensor)
        {
            return Optional.empty();
        }
    };

    /**
     * Creates {@link RequestSensors} for the given keyspaces. This method is invoked by coordinators and replicas when
     * handling requests at various stages/thread pools (e.g. when processing CQL queries or when applying verbs).
     * Consequently, implementations should be very efficient.
     *
     * @param keyspaces the keyspaces associated with the request.
     * @return a {@link RequestSensors} instance. The default implementation returns a singleton no-op instance.
     */
    default RequestSensors createRequestSensors(String... keyspaces)
    {
        return NoOpRequestSensors.instance;
    }

    /**
     * Create a {@link SensorEncoder} that will be invoked when encoding the sensor on the wire. The default implementation returns a noop encoder.
     */
    default SensorEncoder createSensorEncoder()
    {
        return NOOP_SENSOR_ENCODER;
    }
}
