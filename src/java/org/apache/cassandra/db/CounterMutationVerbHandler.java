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
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.StorageProxy;

public class CounterMutationVerbHandler implements IVerbHandler<CounterMutation>
{
    public static final CounterMutationVerbHandler instance = new CounterMutationVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(CounterMutationVerbHandler.class);

    public void doVerb(final Message<CounterMutation> message)
    {
        long queryStartNanoTime = System.nanoTime();
        final CounterMutation cm = message.payload;
        logger.trace("Applying forwarded {}", cm);

        // Initialize the sensor and set ExecutorLocals
        RequestSensors sensors = new RequestSensors();
        ExecutorLocals locals = ExecutorLocals.create(sensors);
        ExecutorLocals.set(locals);

        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        // We should not wait for the result of the write in this thread,
        // otherwise we could have a distributed deadlock between replicas
        // running this VerbHandler (see #4578).
        // Instead, we use a callback to send the response. Note that the callback
        // will not be called if the request timeout, but this is ok
        // because the coordinator of the counter mutation will timeout on
        // it's own in that case.
        StorageProxy.applyCounterMutationOnLeader(cm,
                                                  localDataCenter,
                                                  () -> respond(message, message.from()),
                                                  queryStartNanoTime);
    }

    private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress)
    {
        Message.Builder<NoPayload> response = respondTo.emptyResponseBuilder();
        addSensorsToResponse(response);

        MessagingService.instance().send(response.build(), respondToAddress);
    }

    private void addSensorsToResponse(Message.Builder<NoPayload> response) {
        // Add write bytes sensors to the response
        Function<String, String> requestParam = SensorsCustomParams::encodeTableInWriteByteRequestParam;
        Function<String, String> tableParam = SensorsCustomParams::encodeTableInWriteByteTableParam;
        RequestSensors requestSensors = RequestTracker.instance.get();
        if (requestSensors != null)
        {
            Collection<Sensor> sensors = requestSensors.getSensors(Type.WRITE_BYTES);
            addSensorsToResponse(sensors, requestParam, tableParam, response);
        }
    }

    private void addSensorsToResponse(Collection<Sensor> sensors, Function<String, String> requestParamSupplier, Function<String, String> tableParamSupplier, Message.Builder<NoPayload> response) {
        for (Sensor requestSensor : sensors)
        {
            String requestBytesParam = requestParamSupplier.apply(requestSensor.getContext().getTable());
            byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(requestSensor.getValue());
            response.withCustomParam(requestBytesParam, requestBytes);

            // for each table in the mutation, send the global per table counter write bytes as recorded by the registry
            Optional<Sensor> registrySensor = SensorsRegistry.instance.getSensor(requestSensor.getContext(), requestSensor.getType());
            registrySensor.ifPresent(sensor -> {
                String tableBytesParam = tableParamSupplier.apply(sensor.getContext().getTable());
                byte[] tableBytes = SensorsCustomParams.sensorValueAsBytes(sensor.getValue());
                response.withCustomParam(tableBytesParam, tableBytes);
            });
        }
    }
}
