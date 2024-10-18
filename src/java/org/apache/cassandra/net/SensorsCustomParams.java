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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestSensorsFactory;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A utility class that contains the definition of custom params added to the {@link Message} header to propagate {@link Sensor} values from
 * writer to coordinator and necessary methods to encode sensor values as appropriate for the internode message format.
 */
public final class SensorsCustomParams
{
    private SensorsCustomParams()
    {
    }

    /**
     * Utility method to encode sensor value as byte buffer in the big endian order.
     */
    public static byte[] sensorValueAsBytes(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);

        return buffer.array();
    }

    public static ByteBuffer sensorValueAsByteBuffer(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);
        buffer.flip();
        return buffer;
    }

    public static double sensorValueFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getDouble();
    }

    /**
     * AIterate over all sensors in the {@link RequestSensors} and encodes each sensor values in the internode response message
     * as custom parameters.
     *
     * @param sensors  the collection of sensors to encode in the response
     * @param response the response message builder to add the sensors to
     * @param <T>      the response message builder type
     */
    public static <T> void addSensorsToResponse(RequestSensors sensors, Message.Builder<T> response)
    {
        Preconditions.checkNotNull(sensors);
        Preconditions.checkNotNull(response);

        for (Sensor sensor : sensors.getSensors(ignored -> true))
        {
            addSensorToResponse(response, sensor);
        }
    }

    /**
     * Reads the sensor value from the message header.
     *
     * @param message the message to read the sensor value from
     * @param param   the name of the header to read the sensor value from
     * @param <T>     the message type
     * @return the sensor value
     */
    public static <T> double sensorValueFromCustomParam(Message<T> message, String param)
    {
        if (param == null)
            return 0.0;

        Map<String, byte[]> customParams = message.header.customParams();
        if (customParams == null)
            return 0.0;

        byte[] readBytes = message.header.customParams().get(param);
        if (readBytes == null)
            return 0.0;

        return sensorValueFromBytes(readBytes);
    }



    /**
     * Adds the sensors to the native protocol response message as a custom payload.
     *
     * @param response        the response message to add the sensors to
     * @param protocolVersion the protocol version as custom pauloads are only supported for protocol version >= 4
     * @param sensors         the requests sensor tracker to get the sensor values from
     * @param context         the context of the sensor
     * @param type            the type of the sensor
     */
    public static void addSensorToMessageResponse(org.apache.cassandra.transport.Message.Response response,
                                                  ProtocolVersion protocolVersion,
                                                  RequestSensors sensors,
                                                  Context context,
                                                  Type type)
    {
        // Custom payload is not supported for protocol versions < 4
        if (protocolVersion.isSmallerThan(ProtocolVersion.V4))
        {
            return;
        }

        if (response == null || sensors == null)
        {
            return;
        }

        Optional<Sensor> writeRequestSensor = sensors.getSensor(context, type);
        writeRequestSensor.ifPresent(sensor -> {
            ByteBuffer bytes = SensorsCustomParams.sensorValueAsByteBuffer(sensor.getValue());
            String headerName = RequestSensorsFactory.instance.requestSensorEncoder().apply(sensor);
            Map<String, ByteBuffer> sensorHeader = ImmutableMap.of(headerName, bytes);
            response.setCustomPayload(sensorHeader);
        });
    }

    private static <T> void addSensorToResponse(Message.Builder<T> response, Sensor sensor)
    {
        byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(sensor.getValue());
        String requestParam = RequestSensorsFactory.instance.requestSensorEncoder().apply(sensor);
        response.withCustomParam(requestParam, requestBytes);

        Optional<Sensor> registrySensor = SensorsRegistry.instance.getSensor(sensor.getContext(), sensor.getType());
        registrySensor.ifPresent(registry -> {
            byte[] tableBytes = SensorsCustomParams.sensorValueAsBytes(registry.getValue());
            String tableParam = RequestSensorsFactory.instance.registrySensorEncoder().apply(sensor);
            response.withCustomParam(tableParam, tableBytes);
        });
    }

    public static String requestParamForSensor(Sensor sensor)
    {
        return RequestSensorsFactory.instance.requestSensorEncoder().apply(sensor);
    }

    public static String tableParamForSensor(Sensor sensor)
    {
        return RequestSensorsFactory.instance.registrySensorEncoder().apply(sensor);
    }
}