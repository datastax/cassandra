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

package org.apache.cassandra.net;

import java.nio.ByteBuffer;

import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegisterAggregator;
import org.apache.cassandra.sensors.Type;

/**
 * A utility class that contains the definition of custom params added to the {@link Message} header to propagate {@link Sensor} values from
 * writer to coordinator and necessary methods to encode sensor values as appropriate for the internode message format.
 */
public final class SensorsCustomParams
{
    /**
     * The per-request read bytes value for a given keyspace and table.
     */
    public static final String READ_BYTES_REQUEST = "READ_BYTES_REQUEST";

    /**
     * The total read bytes value for a given keyspace and table, across all requests. This is a monotonically increasing value.
     */
    public static final String READ_BYTES_TABLE = "READ_BYTES_TABLE";

    /**
     * The rate of change of read bytes value, across all requests over a given time window
     * configured by the {@link org.apache.cassandra.sensors.SensorsRegistry.SENSORS_RATE_WINDOW_IN_SECONDS_SYSTEM_PROPERTY) system property.
     * The subset of sensors for a given type consiider for rate calculation is configured is application specefic.
     * See {@link org.apache.cassandra.sensors.SensorsRegistry#registerSensorAggregator(SensorsRegisterAggregator, Type)}. for details/.
     */
    public static final String READ_BYTES_RATE = "READ_BYTES_RATE";

    private SensorsCustomParams()
    {
    }

    /**
     * Utility method to encode senors value as byte buffer in the big endian order.
     */
    public static byte[] sensorValueAsBytes(Sensor sensor)
    {
        return sensorValueAsByteBuffer(sensor).array();
    }

    public static ByteBuffer sensorValueAsByteBuffer(Sensor sensor)
    {
        return sensorValueAsByteBuffer(sensor.getValue());
    }

    public static ByteBuffer sensorValueAsByteBuffer(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);

        return buffer;
    }

    public static double sensorValueFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getDouble();
    }
}
