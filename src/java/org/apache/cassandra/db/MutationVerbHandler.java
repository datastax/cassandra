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

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestSensorsFactory;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    public static final MutationVerbHandler instance = new MutationVerbHandler();

    private void respond(RequestSensors requestSensors, Message<Mutation> respondToMessage, InetAddressAndPort respondToAddress)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);

        Message.Builder<NoPayload> response = respondToMessage.emptyResponseBuilder();
        // no need to calculate outbound internode bytes because the response is NoPayload
        SensorsCustomParams.addSensorsToResponse(requestSensors, response);
        MessagingService.instance().send(response.build(), respondToAddress);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(Message<Mutation> message)
    {
        if (approxTime.now() > message.expiresAtNanos())
        {
            Tracing.trace("Discarding mutation from {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            return;
        }

        // Check if there were any forwarding headers in this message
        InetAddressAndPort from = message.respondTo();
        InetAddressAndPort respondToAddress;
        if (from == null)
        {
            respondToAddress = message.from();
            ForwardingInfo forwardTo = message.forwardTo();
            if (forwardTo != null) forwardToLocalNodes(message, forwardTo);
        }
        else
        {
            respondToAddress = from;
        }

        try
        {
            // Initialize the sensor and set ExecutorLocals
            RequestSensors requestSensors = RequestSensorsFactory.instance.create(message.payload.getKeyspaceName());
            ExecutorLocals locals = ExecutorLocals.create(requestSensors);
            ExecutorLocals.set(locals);

            // Initialize internode bytes with the inbound message size:
            Collection<TableMetadata> tables = message.payload.getPartitionUpdates().stream().map(PartitionUpdate::metadata).collect(Collectors.toList());
            for (TableMetadata tm : tables)
            {
                Context context = Context.from(tm);
                requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
                requestSensors.incrementSensor(context, Type.INTERNODE_BYTES, message.payloadSize(MessagingService.current_version) / tables.size());
            }

            message.payload.applyFuture(WriteOptions.DEFAULT).thenAccept(o -> respond(requestSensors, message, respondToAddress)).exceptionally(wto -> {
                failed();
                return null;
            });
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    private static void forwardToLocalNodes(Message<Mutation> originalMessage, ForwardingInfo forwardTo)
    {
        Message.Builder<Mutation> builder =
        Message.builder(originalMessage)
               .withParam(ParamType.RESPOND_TO, originalMessage.from())
               .withoutParam(ParamType.FORWARD_TO);

        boolean useSameMessageID = forwardTo.useSameMessageID(originalMessage.id());
        // reuse the same Message if all ids are identical (as they will be for 4.0+ node originated messages)
        Message<Mutation> message = useSameMessageID ? builder.build() : null;

        forwardTo.forEach((id, target) ->
                          {
                              Tracing.trace("Enqueuing forwarded write to {}", target);
                              MessagingService.instance().send(useSameMessageID ? message : builder.withId(id).build(), target);
                          });
    }
}
