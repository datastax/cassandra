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
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.tracing.Tracing;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    public static final MutationVerbHandler instance = new MutationVerbHandler();

    private void respond(Message<Mutation> respondTo, InetAddressAndPort respondToAddress)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);

        Message.Builder<NoPayload> response = respondTo.emptyResponseBuilder();
        addSensorsToResponse(response, respondTo.payload);

        MessagingService.instance().send(response.build(), respondToAddress);
    }

    private void addSensorsToResponse(Message.Builder<NoPayload> response, Mutation mutation)
    {
        int tables = mutation.getTableIds().size();

        // Add write bytes sensors to the response
        Function<String, String> requestParam = SensorsCustomParams::encodeTableInWriteBytesRequestParam;
        Function<String, String> tableParam = SensorsCustomParams::encodeTableInWriteBytesTableParam;
        Collection<Sensor> requestSensors = RequestTracker.instance.get().getSensors(Type.WRITE_BYTES);
        addSensorsToResponse(requestSensors, requestParam, tableParam, response);

        // Add index write bytes sensors to the response
        requestParam = SensorsCustomParams::encodeTableInIndexWriteBytesRequestParam;
        tableParam = SensorsCustomParams::encodeTableInIndexWriteBytesTableParam;
        requestSensors = RequestTracker.instance.get().getSensors(Type.INDEX_WRITE_BYTES);
        addSensorsToResponse(requestSensors, requestParam, tableParam, response);

        // Add internode bytes sensors to the response after updating each per-table sensor with the current response
        // message size: this is missing the sensor values, but it's a good enough approximation
        int perSensorSize = response.currentPayloadSize(MessagingService.current_version) / tables;
        requestSensors = RequestTracker.instance.get().getSensors(Type.INTERNODE_BYTES);
        requestSensors.forEach(sensor -> RequestTracker.instance.get().incrementSensor(sensor.getContext(), sensor.getType(), perSensorSize));
        RequestTracker.instance.get().syncAllSensors();
        requestParam = SensorsCustomParams::encodeTableInInternodeBytesRequestParam;
        tableParam = SensorsCustomParams::encodeTableInInternodeBytesTableParam;
        addSensorsToResponse(requestSensors, requestParam, tableParam, response);
    }

    private void addSensorsToResponse(Collection<Sensor> requestSensors, Function<String, String> requestParamSupplier, Function<String, String> tableParamSupplier, Message.Builder<NoPayload> response)
    {
        for (Sensor requestSensor : requestSensors)
        {
            String requestBytesParam = requestParamSupplier.apply(requestSensor.getContext().getTable());
            byte[] requestBytes = SensorsCustomParams.sensorValueAsBytes(requestSensor.getValue());
            response.withCustomParam(requestBytesParam, requestBytes);

            // for each table in the mutation, send the global per table write/index bytes as observed by the registry
            Optional<Sensor> registrySensor = SensorsRegistry.instance.getOrCreateSensor(requestSensor.getContext(), requestSensor.getType());
            registrySensor.ifPresent(sensor -> {
                String tableBytesParam = tableParamSupplier.apply(sensor.getContext().getTable());
                byte[] tableBytes = SensorsCustomParams.sensorValueAsBytes(sensor.getValue());
                response.withCustomParam(tableBytesParam, tableBytes);
            });
        }
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(Message<Mutation> message)
    {
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
            Collection<TableMetadata> tables = message.payload.getPartitionUpdates().stream().map(PartitionUpdate::metadata).collect(Collectors.toList());
            RequestSensors requestSensors = new RequestSensors();
            ExecutorLocals locals = ExecutorLocals.create(requestSensors);
            ExecutorLocals.set(locals);

            // Initialize internode bytes with the inbound message size:
            tables.forEach(tm -> {
                Context context = Context.from(tm);
                requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
                requestSensors.incrementSensor(context, Type.INTERNODE_BYTES, message.payloadSize(MessagingService.current_version) / tables.size());
            });

            message.payload.applyFuture(WriteOptions.DEFAULT).thenAccept(o -> respond(message, respondToAddress)).exceptionally(wto -> {
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
