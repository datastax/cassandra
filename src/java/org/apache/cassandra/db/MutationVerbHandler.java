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

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.SensorsFactory;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MonotonicClock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class MutationVerbHandler extends AbstractMutationVerbHandler<Mutation>
{
    public static final MutationVerbHandler instance = new MutationVerbHandler();

    private void respond(RequestSensors requestSensors, Message<Mutation> respondToMessage, InetAddressAndPort respondToAddress)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);

        Message.Builder<NoPayload> response = respondToMessage.emptyResponseBuilder();
        // no need to calculate outbound internode bytes because the response is NoPayload
        SensorsCustomParams.addSensorsToInternodeResponse(requestSensors, response);
        MessagingService.instance().send(response.build(), respondToAddress);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    @Override
    public void doVerb(Message<Mutation> message)
    {
        if (approxTime.now() > message.expiresAtNanos())
        {
            Tracing.trace("Discarding mutation from {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            return;
        }

        message.payload.validateSize(MessagingService.current_version, ENTRY_OVERHEAD_SIZE);
        if (MonotonicClock.Global.approxTime.now() > message.expiresAtNanos())
        {
            Tracing.trace("Discarding mutation from {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            return;
        }

        // Record where the write came from before anything else touches the payload: an inbound mutation is
        // applied with the same WriteOptions as a locally coordinated one, so this is the only point at which
        // the replica can still tell that the coordinator sits in another datacenter. See WriteOrigin.
        //
        // Stamped BEFORE forwarding on purpose. forwardToLocalNodes hands this very Mutation instance to the
        // outbound connections, which serialize it on their own threads; writing to it after that would
        // race with them and break the "never modify a mutation while it is being serialized" rule this
        // class's payload lives under (see Mutation). Doing it here means the write happens-before the
        // handoff. The origin itself is never serialized, so the forwarded copies carry nothing extra --
        // each recipient stamps its own from its own message.
        message.payload.withOrigin(WriteOrigin.fromMessage(message));

        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null)
            forwardToLocalNodes(message, forwardTo);

        InetAddressAndPort respondToAddress = message.respondTo();
        try
        {
            processMessage(message, respondToAddress);
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    @Override
    protected void applyMutation(Message<Mutation> message, InetAddressAndPort respondToAddress)
    {
        // Initialize the sensor and set ExecutorLocals
        RequestSensors requestSensors = SensorsFactory.instance.createRequestSensors(message.payload.getKeyspaceName());
        RequestTracker.instance.set(requestSensors);

        // Initialize internode bytes with the inbound message size:
        Collection<TableMetadata> tables = message.payload.getPartitionUpdates().stream().map(PartitionUpdate::metadata).collect(Collectors.toList());
        for (TableMetadata tm : tables)
        {
            Context context = Context.from(tm);
            requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
            requestSensors.incrementSensor(context, Type.INTERNODE_BYTES, message.payloadSize(MessagingService.current_version) / tables.size());
        }

        // The origin was stamped in doVerb, before the payload could be handed to the forwarding path.
        message.payload.applyFuture(WriteOptions.DEFAULT)
                       .addCallback(o -> respond(requestSensors, message, respondToAddress), wto -> failed());
    }

    private static void forwardToLocalNodes(Message<Mutation> originalMessage, ForwardingInfo forwardTo)
    {
        Message.Builder<Mutation> builder =
        Message.builder(originalMessage)
               .withParam(ParamType.RESPOND_TO, originalMessage.from())
               .withoutParam(ParamType.FORWARD_TO);

        // reuse the same Message if all ids are identical (as they will be for 4.0+ node originated messages)
        Message<Mutation> message = builder.build();

        forwardTo.forEach((id, target) ->
                          {
                              Tracing.trace("Enqueuing forwarded write to {}", target);
                              MessagingService.instance().send(message, target);
                          });
    }
}
