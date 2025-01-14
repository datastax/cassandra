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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A utility class that groups methods to facilitate encoding sensors in native or internode protocol messages:
 * <ul>
 *   <li>Sensors in internode messages: used to communicate sensors values from replicas to coordinators in the internode
 *   message response {@link Message.Header#customParams()} bytes map.
 *   See {@link SensorsCustomParams#addSensorsToInternodeResponse(RequestSensors, Message.Builder)} and
 *   {@link SensorsCustomParams#sensorValueFromInternodeResponse(Message, String)}.</li>
 *   <li>Sensors in native protocol messages: used to communicate sensors values from coordinator to upstream callers via the native protocol
 *   response {@link org.apache.cassandra.transport.Message#getCustomPayload()} bytes map.
 *   See {@link SensorsCustomParams#addSensorToCQLResponse(org.apache.cassandra.transport.Message.Response, ProtocolVersion, RequestSensors, Context, Type)}.</li
 * </ul>
 */
public final class SensorsCustomParams
{
    private static final SensorEncoder SENSOR_ENCODER = SensorsFactory.instance.createSensorEncoder();

    private SensorsCustomParams()
    {
    }

    /**
     * Utility method to encode sensor value as byte[] in the big endian order.
     */
    public static byte[] sensorValueAsBytes(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);

        return buffer.array();
    }

    /**
     * Utility method to encode sensor value as ByteBuffer in the big endian order.
     */
    public static ByteBuffer sensorValueAsByteBuffer(double value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);
        buffer.flip();
        return buffer;
    }

    public static double sensorValueFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getDouble();
    }

    /**
     * Iterate over all sensors in the {@link RequestSensors} and encodes each sensor value by applying the given
     * {@param valueFunction} in the internode response message as custom parameters.
     *
     * @param sensors the collection of sensors to encode in the response
     * @param valueFunction the function to get the sensor value
     * @param response the response message builder to add the sensors to
     * @param <T> the response message builder type
     */
    public static <T> void addSensorsToInternodeResponse(RequestSensors sensors, Function<Sensor, Double> valueFunction, Message.Builder<T> response)
    {
        Preconditions.checkNotNull(sensors);
        Preconditions.checkNotNull(response);

        for (Sensor sensor : sensors.getSensors(ignored -> true))
            addSensorToInternodeResponse(response, sensor, valueFunction);
    }

    /**
     * Iterate over all sensors in the {@link RequestSensors} and encodes each sensor values in the internode response
     * message as custom parameters.
     *
     * @param sensors the collection of sensors to encode in the response
     * @param response the response message builder to add the sensors to
     * @param <T> the response message builder type
     */
    public static <T> void addSensorsToInternodeResponse(RequestSensors sensors, Message.Builder<T> response)
    {
        addSensorsToInternodeResponse(sensors, Sensor::getValue, response);
    }

    /**
     * Reads the sensor value encoded in the response message header as {@link Message.Header#customParams()} bytes map.
     *
     * @param message the message to read the sensor value from
     * @param customParam the name of the header in custom params to read the sensor value from
     * @param <T> the message type
     * @return the sensor value
     */
    public static <T> double sensorValueFromInternodeResponse(Message<T> message, String customParam)
    {
        if (customParam == null)
            return 0.0;

        Map<String, byte[]> customParams = message.header.customParams();
        if (customParams == null)
            return 0.0;

        byte[] readBytes = message.header.customParams().get(customParam);
        if (readBytes == null)
            return 0.0;

        return sensorValueFromBytes(readBytes);
    }

    /**
     * Adds a sensor of a given type and context to the native protocol response message encoded in the custom payload bytes map.
     * If the sensor is already present in the custom payload, it will be overwritten.
     *
     * @param response the response message to add the sensors to
     * @param protocolVersion the protocol version specified in query options to determine if custom payload is supported (should be V4 or later).
     * @param sensors the requests sensors associated with the request to get the sensor values from.
     * @param context the context of the sensor to add to the response
     * @param type the type of the sensor to add to the response
     */
    public static void addSensorToCQLResponse(org.apache.cassandra.transport.Message.Response response,
                                              ProtocolVersion protocolVersion,
                                              RequestSensors sensors,
                                              Context context,
                                              Type type)
    {
        if (!CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.getBoolean())
            return;

        // Custom payload is not supported for protocol versions < 4
        if (protocolVersion.isSmallerThan(ProtocolVersion.V4))
            return;

        if (response == null || sensors == null)
            return;

        Optional<Sensor> requestSensor = sensors.getSensor(context, type);
        if (requestSensor.isEmpty())
            return;

        Optional<String> headerName = SENSOR_ENCODER.encodeRequestSensorName(requestSensor.get());
        if (headerName.isEmpty())
            return;

        Map<String, ByteBuffer> customPayload = response.getCustomPayload() == null ? new HashMap<>() : response.getCustomPayload();
        ByteBuffer bytes = SensorsCustomParams.sensorValueAsByteBuffer(requestSensor.get().getValue());
        customPayload.put(headerName.get(), bytes);
        response.setCustomPayload(customPayload);
    }

    private static <T> void addSensorToInternodeResponse(Message.Builder<T> response, Sensor requestSensor, Function<Sensor, Double> valueFunction)
    {
        Optional<String> requestParam = paramForRequestSensor(requestSensor);
        if (requestParam.isEmpty())
            return;

        byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(valueFunction.apply(requestSensor));
        response.withCustomParam(requestParam.get(), requestBytes);

        Optional<Sensor> globalSensor = SensorsRegistry.instance.getSensor(requestSensor.getContext(), requestSensor.getType());
        if (globalSensor.isEmpty())
            return;

        Optional<String> globalParam = paramForGlobalSensor(globalSensor.get());
        if (globalParam.isEmpty())
            return;

        byte[] globalBytes = SensorsCustomParams.sensorValueAsBytes(valueFunction.apply(globalSensor.get()));
        response.withCustomParam(globalParam.get(), globalBytes);
    }

    public static Optional<String> paramForRequestSensor(Sensor sensor)
    {
        return SENSOR_ENCODER.encodeRequestSensorName(sensor);
    }

    public static Optional<String> paramForGlobalSensor(Sensor sensor)
    {
        return SENSOR_ENCODER.encodeGlobalSensorName(sensor);
    }
}