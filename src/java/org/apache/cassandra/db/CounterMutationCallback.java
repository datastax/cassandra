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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;

/**
 * A counter mutation callback that encapsulates {@link RequestSensors} and replica count
 */
public class CounterMutationCallback implements Runnable
{
    private final Message<CounterMutation> requestMessage;
    private final InetAddressAndPort respondToAddress;
    private final RequestSensors requestSensors;
    private int replicaCount = 0;

    public CounterMutationCallback(Message<CounterMutation> requestMessage, InetAddressAndPort respondToAddress, RequestSensors requestSensors)
    {
        this.requestMessage = requestMessage;
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
        Message.Builder<NoPayload> responseBuilder = requestMessage.emptyResponseBuilder();
        int replicaMultiplier = replicaCount == 0 ?
                                1 : // replica count was not explicitly set (default). At the bare minimum, we should send the response accomodating for the local replica (aka. mutation leader) sensor values
                                replicaCount;
        addSensorsToResponse(responseBuilder, replicaMultiplier);
        MessagingService.instance().send(responseBuilder.build(), respondToAddress);
    }

    private void addSensorsToResponse(Message.Builder<NoPayload> response, int replicaMultiplier)
    {
        // There is no need to increment INTERNODE_BYTES to accommodate for outbound bytes because the response payload
        // is of type NoPayload which has zero size
        Collection<Sensor> sensors = this.requestSensors.getSensors(s -> s.getType() == Type.INTERNODE_BYTES);
        addSensorsToResponse(sensors, response, replicaMultiplier);

        // Add write bytes sensors to the response
        sensors = this.requestSensors.getSensors(s -> s.getType() == Type.WRITE_BYTES);
        addSensorsToResponse(sensors, response, replicaMultiplier);
    }

    private static void addSensorsToResponse(Collection<Sensor> sensors,
                                             Message.Builder<NoPayload> response,
                                             int replicaMultiplier)
    {
        for (Sensor sensor : sensors)
        {
            String requestBytesParam = SensorsCustomParams.paramForRequestSensor(sensor);
            byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(sensor.getValue() * replicaMultiplier);
            response.withCustomParam(requestBytesParam, requestBytes);

            // for each table in the mutation, send the global per table counter write bytes as recorded by the registry
            Optional<Sensor> registrySensor = SensorsRegistry.instance.getSensor(sensor.getContext(), sensor.getType());
            registrySensor.ifPresent(s -> {
                String globalParam = SensorsCustomParams.paramForGlobalSensor(s);
                byte[] globalBytes = SensorsCustomParams.sensorValueAsBytes(s.getValue() * replicaMultiplier);
                response.withCustomParam(globalParam, globalBytes);
            });
        }
    }
}