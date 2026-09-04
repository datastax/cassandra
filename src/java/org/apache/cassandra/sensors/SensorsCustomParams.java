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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(SensorsCustomParams.class);

    private static final SensorEncoder SENSOR_ENCODER = SensorsFactory.instance.createSensorEncoder();

    private static final MUCalculator MU_CALCULATOR = initMUCalculator();

    private static MUCalculator initMUCalculator()
    {
        MUCalculator calculator = SensorsFactory.instance.getMUCalculator();
        logger.info("MUCalculator loaded: {}", calculator != null ? calculator.getClass().getName() : "null");
        return calculator;
    }

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
     * Computes the RMU value for every RMU sensor registered in {@code sensors} and increments each sensor by the
     * computed value. The execution time is read from the {@link Type#READ_EXECUTION_TIME} sensor already
     * recorded in {@code sensors}. Must be called <em>after</em> all other sensor increments for the request are
     * complete and <em>before</em> the final {@link RequestSensors#syncAllSensors()} call. Because intermediate
     * {@code syncAllSensors()} calls earlier in the request path skip RMU (its value is 0 until this method runs,
     * so the delta is 0 and the registry is not touched), the final sync after this call is the one that delivers
     * the correct RMU value to the global {@link SensorsRegistry}.
     * Must also be called before {@link #addSensorsToInternodeResponse} so the response message carries the
     * correct RMU value.
     *
     * @param sensors the request sensors for the current request
     */
    public static void computeRMU(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        if (MU_CALCULATOR == null)
            return;

        for (Sensor rmuSensor : sensors.getSensors(s -> s.getType() == Type.RMU))
        {
            Context context = rmuSensor.getContext();
            double rmuValue = MU_CALCULATOR.computeRMU(sensors, context);
            sensors.incrementSensor(context, Type.RMU, rmuValue);
        }
    }

    /**
     * Computes the WMU value for every WMU sensor registered in {@code sensors} and increments each sensor by the
     * computed value. The execution time is read from the {@link Type#WRITE_EXECUTION_TIME} sensor already
     * recorded in {@code sensors}. Must be called <em>after</em> all other sensor increments for the request are
     * complete and <em>before</em> the final {@link RequestSensors#syncAllSensors()} call. Because intermediate
     * {@code syncAllSensors()} calls earlier in the request path skip WMU (its value is 0 until this method runs,
     * so the delta is 0 and the registry is not touched), the final sync after this call is the one that delivers
     * the correct WMU value to the global {@link SensorsRegistry}.
     * Must also be called before {@link #addSensorsToInternodeResponse} so the response message carries the
     * correct WMU value.
     *
     * @param sensors the request sensors for the current request
     */
    public static void computeWMU(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        if (MU_CALCULATOR == null)
            return;

        for (Sensor wmuSensor : sensors.getSensors(s -> s.getType() == Type.WMU))
        {
            Context context = wmuSensor.getContext();
            double wmuValue = MU_CALCULATOR.computeWMU(sensors, context);
            sensors.incrementSensor(context, Type.WMU, wmuValue);
        }
    }

    /**
     * Computes the TMU (Total Measure Units) value for every TMU sensor registered in {@code sensors} and increments
     * each sensor by the computed value. TMU is defined as the sum of WMU and RMU for the same context.
     * Must be called <em>after</em> both {@link #computeRMU} and {@link #computeWMU} so that WMU and RMU values
     * are already populated. TMU is synced to the global {@link SensorsRegistry} via the normal
     * {@link RequestSensors#syncAllSensors()} call but is <em>never</em> included in CQL responses.
     *
     * @param sensors the request sensors for the current request
     */
    public static void computeTMU(RequestSensors sensors)
    {
        Preconditions.checkNotNull(sensors);

        for (Sensor tmuSensor : sensors.getSensors(s -> s.getType() == Type.TMU))
        {
            Context context = tmuSensor.getContext();
            double wmu = sensors.getSensor(context, Type.WMU).map(Sensor::getValue).orElse(0.0);
            double rmu = sensors.getSensor(context, Type.RMU).map(Sensor::getValue).orElse(0.0);
            sensors.incrementSensor(context, Type.TMU, wmu + rmu);
        }
    }

    /**
     * Iterate over all sensors in the {@link RequestSensors} and encodes each sensor value in the internode
     * response message as custom parameters.
     *
     * @param sensors the collection of sensors to encode in the response
     * @param response the response message builder to add the sensors to
     * @param <T> the response message builder type
     */
    public static <T> void addSensorsToInternodeResponse(RequestSensors sensors, Message.Builder<T> response)
    {
        Preconditions.checkNotNull(sensors);
        Preconditions.checkNotNull(response);

        for (Sensor sensor : sensors.getSensors(ignored -> true))
            addSensorToInternodeResponse(response, sensor, Sensor::getValue);
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

        // Skip zero-valued sensors — they carry no useful information and needlessly inflate the custom payload.
        if (requestSensor.get().getValue() == 0D)
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