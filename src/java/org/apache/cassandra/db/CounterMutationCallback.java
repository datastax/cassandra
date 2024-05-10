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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;

/**
 * A counter mutation callback that encapsulates {@link RequestSensors} and replica count
 */
public class CounterMutationCallback implements Runnable
{
    private final Message<CounterMutation> respondTo;
    private final InetAddressAndPort respondToAddress;
    private final RequestSensors requestSensors;
    private int replicaCount = 0;

    public CounterMutationCallback(Message<CounterMutation> respondTo, InetAddressAndPort respondToAddress, RequestSensors requestSensors)
    {
        this.respondTo = respondTo;
        this.respondToAddress = respondToAddress;
        this.requestSensors = requestSensors;
    }

    /**
     * Sets replica count including the local one.
     */
    public void setReplicaCount(Integer replicaCount)
    {
        this.replicaCount = replicaCount;
    }

    @Override
    public void run()
    {
        Message.Builder<NoPayload> response = respondTo.emptyResponseBuilder();
        int replicaMultiplier = replicaCount == 0 ?
                                1 : // replica count was not explicitly set (default). At the bare minimum, we should send the response accomodating for the local replica (aka. mutation leader) sensor values
                                replicaCount;
        addSensorsToResponse(response, requestSensors, replicaMultiplier);
        MessagingService.instance().send(response.build(), respondToAddress);
    }

    private static void addSensorsToResponse(Message.Builder<NoPayload> response, RequestSensors requestSensors, int replicaMultiplier)
    {
        // Add write bytes sensors to the response
        Function<String, String> requestParam = SensorsCustomParams::encodeTableInWriteBytesRequestParam;
        Function<String, String> tableParam = SensorsCustomParams::encodeTableInWriteBytesTableParam;

        Collection<Sensor> sensors = requestSensors.getSensors(Type.WRITE_BYTES);
        addSensorsToResponse(sensors, requestParam, tableParam, response, replicaMultiplier);
    }

    private static void addSensorsToResponse(Collection<Sensor> sensors,
                                             Function<String, String> requestParamSupplier,
                                             Function<String, String> tableParamSupplier,
                                             Message.Builder<NoPayload> response,
                                             int replicaMultiplier)
    {
        for (Sensor requestSensor : sensors)
        {
            String requestBytesParam = requestParamSupplier.apply(requestSensor.getContext().getTable());
            byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(requestSensor.getValue() * replicaMultiplier);
            response.withCustomParam(requestBytesParam, requestBytes);

            // for each table in the mutation, send the global per table counter write bytes as recorded by the registry
            Optional<Sensor> registrySensor = SensorsRegistry.instance.getSensor(requestSensor.getContext(), requestSensor.getType());
            registrySensor.ifPresent(sensor -> {
                String tableBytesParam = tableParamSupplier.apply(sensor.getContext().getTable());
                byte[] tableBytes = SensorsCustomParams.sensorValueAsBytes(sensor.getValue() * replicaMultiplier);
                response.withCustomParam(tableBytesParam, tableBytes);
            });
        }
    }
}
