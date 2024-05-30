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

import java.util.concurrent.TimeUnit;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.net.SensorsCustomParams;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);
    private static final String logMessageTemplate = "Receiving read(s) for token(s) neither owned nor pending. Example: from {} for token {} in {}.{}";
    private static final String exceptionMessageTemplate = "Exception thrown checking if token {} outside valid range for keyspace {} - permitting";

    public void doVerb(Message<ReadCommand> message)
    {
        TableMetadata metadata = message.payload.metadata();
        if (metadata.isVirtual())
        {
            if (StorageService.instance.isBootstrapMode())
            {
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
        }
        else
        {
            ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
            if (!cfs.isReadyToServeData())
            {
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
        }

        ReadCommand command = message.payload;

        // no out of token range checking for partition range reads yet
        if (command.isSinglePartitionRead())
        {
            boolean outOfRangeTokenLogging = DatabaseDescriptor.getLogOutOfTokenRangeRequests();
            boolean outOfRangeTokenRejection = DatabaseDescriptor.getRejectOutOfTokenRangeRequests();

            DecoratedKey key = ((SinglePartitionReadCommand)command).partitionKey();
            if (!command.metadata().isVirtual() && isOutOfRangeRead(command.metadata().keyspace, key))
            {
                StorageService.instance.incOutOfRangeOperationCount();
                Keyspace.open(command.metadata().keyspace).metric.outOfRangeTokenReads.inc();

                // Log at most 1 message per second
                if (outOfRangeTokenLogging)
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, logMessageTemplate,
                                     message.from(), key.getToken(), command.metadata().keyspace,  command.metadata().name);

                if (outOfRangeTokenRejection)
                    // no need to respond, just drop the request
                    return;
            }
        }

        validateTransientStatus(message);
        MessageParams.reset();

        // Initialize the sensor and set ExecutorLocals
        Context context = Context.from(command);
        ImmutableSet<TableMetadata> tables = ImmutableSet.of(command.metadata());
        RequestSensors requestSensors = new RequestSensors();
        requestSensors.registerSensor(context, Type.READ_BYTES);
        RequestTracker.instance.set(requestSensors);

        // Initialize internode bytes with the inbound message size:
        tables.forEach(tm -> {
            requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
            requestSensors.incrementSensor(context, Type.INTERNODE_BYTES, message.payloadSize(MessagingService.current_version) / tables.size());
        });

        long timeout = message.expiresAtNanos() - message.createdAtNanos();
        command.setMonitoringTime(message.createdAtNanos(), message.isCrossNode(), timeout, DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

        if (message.trackWarnings())
            command.trackWarnings();

        ReadResponse response;
        try (ReadExecutionController controller = command.executionController(message.trackRepairedData());
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            response = command.createResponse(iterator, controller.getRepairedDataInfo());
        }
        catch (RejectException e)
        {
            if (!command.isTrackingWarnings())
                throw e;

            // make sure to log as the exception is swallowed
            logger.error(e.getMessage());

            response = command.createEmptyResponse();
            Message<ReadResponse> reply = message.responseWith(response);
            reply = MessageParams.addToMessage(reply);

            MessagingService.instance().send(reply, message.from());
            return;
        }
        catch (AssertionError t)
        {
            throw new AssertionError(String.format("Caught an error while trying to process the command: %s", command.toCQLString()), t);
        }
        catch (QueryCancelledException e)
        {
            logger.debug("Query cancelled (timeout)", e);
            response = null;
            assert !command.isCompleted() : "Read marked as completed despite being aborted by timeout to table " + command.metadata();
        }

        if (command.complete())
        {
            Message.Builder<ReadResponse> replyBuilder = message.responseWithBuilder(response);
            int size = replyBuilder.currentPayloadSize(MessagingService.current_version);
            RequestTracker.instance.get().incrementSensor(context, Type.INTERNODE_BYTES, size);
            RequestTracker.instance.get().syncAllSensors();

            addInternodeSensorToResponse(replyBuilder, context);
            addReadBytesSensorToResponse(replyBuilder, context);

            Tracing.trace("Enqueuing response to {}", message.from());
            Message<ReadResponse> reply = replyBuilder.build();
            reply = MessageParams.addToMessage(reply);
            MessagingService.instance().send(reply, message.from());
        }
        else
        {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
        }
    }

    private void addInternodeSensorToResponse(Message.Builder<ReadResponse> reply, Context context)
    {
        Optional<Sensor> requestSensor = RequestTracker.instance.get().getSensor(context, Type.INTERNODE_BYTES);
        requestSensor.map(s -> SensorsCustomParams.sensorValueAsBytes(s.getValue())).ifPresent(bytes -> {
            reply.withCustomParam(SensorsCustomParams.encodeTableInInternodeBytesRequestParam(context.getTable()),
                                     bytes);
        });

        Optional<Sensor> tableSensor = SensorsRegistry.instance.getOrCreateSensor(context, Type.INTERNODE_BYTES);
        tableSensor.map(s -> SensorsCustomParams.sensorValueAsBytes(s.getValue())).ifPresent(bytes -> {
            reply.withCustomParam(SensorsCustomParams.encodeTableInInternodeBytesTableParam(context.getTable()),
                                  bytes);
        });
    }

    private void addReadBytesSensorToResponse(Message.Builder<ReadResponse> reply, Context context)
    {
        addSensorDataToResponse(reply, context, Type.READ_BYTES, SensorsCustomParams.READ_BYTES_REQUEST, SensorsCustomParams.READ_BYTES_TABLE);
    }

    /**
     * TODO: Refactor on the basis of https://github.com/datastax/cassandra/pull/1074/files#diff-2d48c168d5192fe4989baf96618533f271cafab1db3f212f6a0ff1b3dff3d606R88-R124 once CNDB-8772 is merged.
     */
    private void addSensorDataToResponse(Message.Builder<ReadResponse> reply, Context context, Type type, String requestBytesParam, String tableBytesParam)
    {
        Optional<Sensor> readRequestSensor = RequestTracker.instance.get().getSensor(context, type);
        readRequestSensor.map(s -> SensorsCustomParams.sensorValueAsBytes(s.getValue()))
                         .ifPresent(bytes -> reply.withCustomParam(requestBytesParam, bytes));

        Optional<Sensor> readTableSensor = SensorsRegistry.instance.getOrCreateSensor(context, type);
        readTableSensor.map(s -> SensorsCustomParams.sensorValueAsBytes(s.getValue()))
                       .ifPresent(bytes -> reply.withCustomParam(tableBytesParam, bytes));
    }

    private void validateTransientStatus(Message<ReadCommand> message)
    {
        ReadCommand command = message.payload;
        if (command.metadata().isVirtual())
            return;
        Token token;

        if (command instanceof SinglePartitionReadCommand)
            token = ((SinglePartitionReadCommand) command).partitionKey().getToken();
        else if (command instanceof PartitionRangeReadCommand)
            token = ((PartitionRangeReadCommand) command).dataRange().keyRange().right.getToken();
        else
            return;

        Replica replica = Keyspace.open(command.metadata().keyspace)
                                  .getReplicationStrategy()
                                  .getLocalReplicaFor(token);

        if (replica == null)
        {
            // it's fine for serverless which unloads stale sstables, SEE VECTOR-30
            if (command.isTopK())
                return;

            logger.warn("Received a read request from {} for a range that is not owned by the current replica {}.",
                        message.from(),
                        command);
            return;
        }

        if (!command.acceptsTransient() && replica.isTransient())
        {
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            throw new InvalidRequestException(String.format("Attempted to serve %s data request from %s node in %s",
                                                            command.acceptsTransient() ? "transient" : "full",
                                                            replica.isTransient() ? "transient" : "full",
                                                            this));
        }
    }

    private boolean isOutOfRangeRead(String keyspace, DecoratedKey key)
    {
        try
        {
            return Keyspace.open(keyspace)
                           .getReplicationStrategy()
                           .getNaturalReplicas(key)
                           .selfIfPresent() == null;
        }
        catch (Throwable tr)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, exceptionMessageTemplate, keyspace, key.getKey(), tr);
            return false;
        }
    }
}
