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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.SensorsCustomParams;

/**
 * A counter mutation callback that encapsulates {@link RequestSensors} and replica count
 */
public class CounterMutationCallback implements Runnable
{
    private final Message<CounterMutation> requestMessage;
    private final InetAddressAndPort respondToAddress;
    private final RequestSensors sensors;
    private int replicaCount = 0;

    public CounterMutationCallback(Message<CounterMutation> requestMessage, InetAddressAndPort respondToAddress, RequestSensors sensors)
    {
        this.requestMessage = requestMessage;
        this.respondToAddress = respondToAddress;
        this.sensors = sensors;
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
        SensorsCustomParams.addSensorsToInternodeResponse(sensors, s -> s.getValue() * replicaMultiplier, responseBuilder);
        MessagingService.instance().send(responseBuilder.build(), respondToAddress);
    }
}
