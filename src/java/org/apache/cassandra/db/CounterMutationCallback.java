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

package org.apache.cassandra.db;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.CounterWriteReponseHandler;

/**
 * A {@link AbstractWriteResponseHandler} awarte callback to be used when we need to send a response to a counter mutation.
 */
public class CounterMutationCallback implements Runnable
{
    private final Message<CounterMutation> respondTo;
    private final InetAddressAndPort respondToAddress;
    private final RequestSensors requestSensors;
    @Nullable private CounterWriteReponseHandler<IMutation> responseHandler;

    public CounterMutationCallback(Message<CounterMutation> respondTo, InetAddressAndPort respondToAddress, RequestSensors requestSensors)
    {
        this.respondTo = respondTo;
        this.respondToAddress = respondToAddress;
        this.requestSensors = requestSensors;
    }

    public void attachHandler(CounterWriteReponseHandler<IMutation> responseHandler)
    {
        this.responseHandler = responseHandler;
    }

    @Override
    public void run()
    {
        Message.Builder<NoPayload> response = respondTo.emptyResponseBuilder();
        if (this.responseHandler != null)
        {
            addSensorsToResponse(response, this.responseHandler.replicaSensors());
        }

        MessagingService.instance().send(response.build(), respondToAddress);
    }

    private void addSensorsToResponse(Message.Builder<NoPayload> response, Map<String, Double> replicaSensors)
    {
        // Add write bytes sensors to the response
        Function<String, String> requestParam = SensorsCustomParams::encodeTableInWriteByteRequestParam;
        Function<String, String> tableParam = SensorsCustomParams::encodeTableInWriteByteTableParam;

        Collection<Sensor> sensors = this.requestSensors.getSensors(Type.WRITE_BYTES);
        addSensorsToResponse(sensors, requestParam, tableParam, response, replicaSensors);
    }

    private void addSensorsToResponse(Collection<Sensor> sensors,
                                      Function<String, String> requestParamSupplier,
                                      Function<String, String> tableParamSupplier,
                                      Message.Builder<NoPayload> response,
                                      Map<String, Double> replicaSensors)
    {
        for (Sensor requestSensor : sensors)
        {
            String requestBytesParam = requestParamSupplier.apply(requestSensor.getContext().getTable());
            double replicaSensorsValue = replicaSensors.getOrDefault(requestBytesParam, 0.0);
            byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(requestSensor.getValue() + replicaSensorsValue);
            response.withCustomParam(requestBytesParam, requestBytes);

            // for each table in the mutation, send the global per table counter write bytes as recorded by the registry
            Optional<Sensor> registrySensor = SensorsRegistry.instance.getSensor(requestSensor.getContext(), requestSensor.getType());
            registrySensor.ifPresent(sensor -> {
                String tableBytesParam = tableParamSupplier.apply(sensor.getContext().getTable());
                byte[] tableBytes = SensorsCustomParams.sensorValueAsBytes(sensor.getValue() + replicaSensorsValue);
                response.withCustomParam(tableBytesParam, tableBytes);
            });
        }
    }
}
