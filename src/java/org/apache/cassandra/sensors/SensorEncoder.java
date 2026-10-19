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
 * Encodes sensor name as string to be used on the wire (let it be in internode messages as custom params or in native protocol
 * messages as custom payloads). Note that the sensor value itself will always be encoded as bytes in the big endian order
 * (see {@link SensorsCustomParams#sensorValueAsBytes(double)} and {@link SensorsCustomParams#sensorValueAsByteBuffer(double)}).
 * Implementations should be very efficient as sensor names are potentially encoded with each request. They should also encode
 * enough information to differentiate between sensors of the same type that belong to the same request but different
 * keyspaces and/or tables.
 */
public interface SensorEncoder
{
    /**
     * Encodes request sensor name as a string to be used on the wire. A request sensor tracks usage per request. See {@link RequestSensors}.
     *
     * @param sensor the sensor to encode
     * @return the encoded sensor as a string. If the optional is empty, the sensor will not be encoded.
     */
    Optional<String> encodeRequestSensorName(Sensor sensor);

    /**
     * Encodes global sensor name as a string to be used on the wire. A global sensor tracks usage globally across different requests. See {@link SensorsRegistry}.
     *
     * @param sensor the sensor to encode
     * @return the encoded sensor as a string. If the optional is empty, the sensor will not be encoded.
     */
    Optional<String> encodeGlobalSensorName(Sensor sensor);
}
