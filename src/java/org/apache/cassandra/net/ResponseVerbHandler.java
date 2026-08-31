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
package org.apache.cassandra.net;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

public class ResponseVerbHandler implements IVerbHandler
{
    public static final ResponseVerbHandler instance = new ResponseVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ResponseVerbHandler.class);

    @Override
    public void doVerb(Message message)
    {
        RequestCallbacks.CallbackInfo callbackInfo = MessagingService.instance().callbacks.remove(message.id(), message.from());
        if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, message.id(), message.from());
            Tracing.trace(msg, message.id(), message.from());
            return;
        }

        long latencyNanos = approxTime.now() - callbackInfo.createdAtNanos;
        Tracing.trace("Processing response from {}", message.from());

        RequestCallback<?> cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            cb.onFailure(message.from(), (RequestFailureReason) message.payload);
        }
        else
        {
            MessagingService.instance().latencySubscribers.maybeAdd(cb, message.verb(), message.from(), latencyNanos, NANOSECONDS, false);
            trackReplicaSensors(callbackInfo, message);
            cb.onResponse(message);
        }
    }

    /**
     * Accumulates sensors from a replica internode response into the coordinator's {@link RequestSensors}.
     *
     * <p><em>Bytes</em> sensors ({@link Type#WRITE_BYTES}, {@link Type#READ_BYTES},
     * {@link Type#INDEX_WRITE_BYTES}, {@link Type#INTERNODE_BYTES}) are accumulated by addition
     * because replicas execute sequentially from the coordinator's perspective and their byte
     * contributions are independent.</p>
     *
     * <p><em>Execution-time</em> sensors ({@link Type#READ_EXECUTION_TIME},
     * {@link Type#WRITE_EXECUTION_TIME}) are accumulated by {@code max} because replicas execute
     * in parallel — only the slowest replica's time is relevant. The coordinator later adds its
     * own local work on top (result processing for reads, post-mutate work for writes) via a
     * regular {@code incrementSensor} at the CQL statement layer.</p>
     *
     * <p>Please note {@link RequestSensors#syncAllSensors()} is not invoked here, but at the CQL statement layer:
     * this is to reduce number of calls, and because local-only requests would not go through this handler.</p>
     */
    private void trackReplicaSensors(RequestCallbacks.CallbackInfo callbackInfo, Message<?> message)
    {
        RequestSensors sensors = callbackInfo.callback.getRequestSensors();
        if (sensors == null)
            return;

        if (callbackInfo instanceof RequestCallbacks.WriteCallbackInfo)
        {
            RequestCallbacks.WriteCallbackInfo writerInfo = (RequestCallbacks.WriteCallbackInfo) callbackInfo;
            IMutation mutation = writerInfo.iMutation();
            if (mutation == null)
                return;

            Collection<PartitionUpdate> nonIndexUpdates = mutation.getPartitionUpdates().stream()
                                                                  .filter(pu -> !pu.metadata().isIndex())
                                                                  .collect(Collectors.toList());
            int allTablesCount = mutation.getPartitionUpdates().size();
            double internodeBytesPerTable = allTablesCount == 0 ? 0
                                                                : (double) writerInfo.sentPayloadSize / allTablesCount;
            for (PartitionUpdate pu : nonIndexUpdates)
            {
                Context context = Context.from(pu.metadata());
                incrementSensor(sensors, context, Type.WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INDEX_WRITE_BYTES, message);
                sensors.incrementSensor(context, Type.INTERNODE_BYTES, internodeBytesPerTable);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
            }
        }
        else if (callbackInfo.callback instanceof ReadCallback)
        {
            ReadCallback<?, ?> readCallback = (ReadCallback<?, ?>) callbackInfo.callback;
            Context context = Context.from(readCallback.command());
            incrementSensor(sensors, context, Type.READ_BYTES, message);
            incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
            accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.READ_EXECUTION_TIME, message);
        }
        // Covers Paxos Prepare and Propose callbacks. Paxos Commit callback is a regular WriteCallbackInfo.
        // INDEX_WRITE_BYTES is not tracked here: prepare/propose only write to system.paxos, which has no indexes.
        else if (callbackInfo.callback instanceof AbstractPaxosCallback)
        {
            AbstractPaxosCallback<?> paxosCallback = (AbstractPaxosCallback<?>) callbackInfo.callback;
            Context context = Context.from(paxosCallback.getMetadata());
            incrementSensor(sensors, context, Type.READ_BYTES, message);
            incrementSensor(sensors, context, Type.WRITE_BYTES, message);
            incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
            accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
        }
    }

    /**
     * Increments the sensor for the given context and type by adding the value encoded in the replica response message.
     */
    private void incrementSensor(RequestSensors sensors, Context context, Type type, Message<?> message)
    {
        Optional<Sensor> sensor = sensors.getSensor(context, type);
        if (sensor.isEmpty())
            return;

        Optional<String> customParam = SensorsCustomParams.paramForRequestSensor(sensor.get());
        if (customParam.isEmpty())
            return;

        double sensorValue = SensorsCustomParams.sensorValueFromInternodeResponse(message, customParam.get());
        sensors.incrementSensor(context, type, sensorValue);
    }

    /**
     * Reads the execution-time value from the replica response message and feeds it into the
     * callback's per-context running max. The response count is incremented separately inside
     * the callback's {@code onResponse} implementation.
     */
    private void accumulateExecutionTimeSensor(RequestCallback callback, RequestSensors sensors, Context context, Type executionType, Message<?> message)
    {
        Optional<Sensor> sensor = sensors.getSensor(context, executionType);
        if (sensor.isEmpty())
            return;

        Optional<String> customParam = SensorsCustomParams.paramForRequestSensor(sensor.get());
        if (customParam.isEmpty())
            return;

        double sensorValue = SensorsCustomParams.sensorValueFromInternodeResponse(message, customParam.get());
        callback.accumulateExecutionTimeSensor(context, executionType, sensorValue);
    }
}
