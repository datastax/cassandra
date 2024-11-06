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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.SENSORS_FACTORY;

/**
 * Provides a factory to customize the behaviour of sensors tracking in CNDB by providing to factory methods:
 * <li> {@link SensorsFactory#createRequestSensors} provides {@link RequestSensors} implementation to active or deactivate sensors per keyspace</li>
 * <li> {@link SensorsFactory#createSensorEncoder} provides {@link SensorEncoder} implementations control how a sensors are encoded as string on the wire</li>
 * <p>
 * The concrete implementation of this factory is configured by the {@link CassandraRelevantProperties#SENSORS_FACTORY} system property.
 */
public interface SensorsFactory
{
    SensorsFactory instance = SENSORS_FACTORY.getString() == null ?
                              new SensorsFactory() {} :
                              FBUtilities.construct(CassandraRelevantProperties.SENSORS_FACTORY.getString(), "sensors factory");

    SensorEncoder DEFAULT_SENSOR_ENCODER = new SensorEncoder()
    {
        @Override
        public String encodeRequestSensor(Sensor sensor)
        {
            return "";
        }

        @Override
        public String encodeGlobalSensor(Sensor sensor)
        {
            return "";
        }
    };

    /**
     * Creates a {@link RequestSensors} for the given keyspace. Implementations should be very efficient because this method is potentially invoked on each verb handler serving a user request.
     *
     * @param keyspace the keyspace of the request
     * @return a {@link RequestSensors} instance. The default implementation returns a singleton no-op instance.
     */
    default RequestSensors createRequestSensors(String keyspace)
    {
        return NoOpRequestSensors.instance;
    }

    /**
     * Create a {@link SensorEncoder} that will be invoked when encoding the sensor on the wire. The default implementation returns an encode that always return an empty string.
     */
    default SensorEncoder createSensorEncoder()
    {
        return DEFAULT_SENSOR_ENCODER;
    }
}
