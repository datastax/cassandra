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
import java.util.stream.Collectors;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.sensors.Type;

/**
 * Callback fired by the (non-coordinator) counter mutation leader once all sub-replica ACKs have
 * been received. Accumulates {@link Type#INTERNODE_BYTES} from the inbound request message and
 * forwards the accumulated sensor values back to the original coordinator.
 *
 * <p>{@link Type#WRITE_EXECUTION_TIME} by the time this callback fires reflects both the leader
 * apply time (added by {@code counterWriteTask} via {@code incrementSensor} before
 * {@code responseHandler.onResponse(null)}) and the max sub-replica execution time (added by
 * {@link org.apache.cassandra.net.ResponseVerbHandler} via the
 * {@link org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator} when quorum sub-replica
 * ACKs are received). The combined value is encoded into the {@code COUNTER_MUTATION_RSP} here
 * and picked up by the coordinator's own accumulator.</p>
 */
public class CounterMutationCallback implements Runnable
{
    private final Message<CounterMutation> requestMessage;
    private final InetAddressAndPort respondToAddress;
    private final RequestSensors sensors;

    public CounterMutationCallback(Message<CounterMutation> requestMessage,
                                   InetAddressAndPort respondToAddress,
                                   RequestSensors sensors)
    {
        this.requestMessage = requestMessage;
        this.respondToAddress = respondToAddress;
        this.sensors = sensors;
    }

    @Override
    public void run()
    {
        Collection<TableMetadata> allTables = requestMessage.payload.getPartitionUpdates().stream()
                                                                    .map(pu -> pu.metadata())
                                                                    .collect(Collectors.toList());

        double internodeBytesPerTable = (double) requestMessage.payloadSize(MessagingService.current_version) / allTables.size();
        for (TableMetadata tm : allTables)
            sensors.incrementSensor(Context.from(tm), Type.INTERNODE_BYTES, internodeBytesPerTable);

        sensors.syncAllSensors();

        Message.Builder<NoPayload> responseBuilder = requestMessage.emptyResponseBuilder();
        SensorsCustomParams.addSensorsToInternodeResponse(sensors, responseBuilder);
        MessagingService.instance().send(responseBuilder.build(), respondToAddress);
    }
}
