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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.sensors.Type;
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

        RequestCallback cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            cb.onFailure(message.from(), (RequestFailureReason) message.payload);
        }
        else
        {
            MessagingService.instance().latencySubscribers.maybeAdd(cb, message.from(), latencyNanos, NANOSECONDS);
            cb.onResponse(message);
            trackReplicaSensors(callbackInfo, message);
        }
    }

    private void trackReplicaSensors(RequestCallbacks.CallbackInfo callbackInfo, Message message)
    {
        RequestSensors sensors = callbackInfo.callback.getRequestSensors();
        if (sensors == null)
            return;

        if (callbackInfo instanceof RequestCallbacks.WriteCallbackInfo)
        {
            RequestCallbacks.WriteCallbackInfo writerInfo = (RequestCallbacks.WriteCallbackInfo) callbackInfo;
            Mutation mutation = writerInfo.mutation();
            if (mutation == null)
                return;

            for (PartitionUpdate pu : mutation.getPartitionUpdates())
            {
                Context context = Context.from(pu.metadata());
                if (pu.metadata().isIndex()) continue;
                incrementSensor(sensors, context, Type.WRITE_BYTES, message);
            }
        }
        else if (callbackInfo.callback instanceof ReadCallback)
        {
            ReadCallback readCallback = (ReadCallback) callbackInfo.callback;
            Context context = Context.from(readCallback.command());
            incrementSensor(sensors, context, Type.READ_BYTES, message);
        }
    }

    /**
     * Increments the sensor for the given context and type based on the value encoded in the replica response message.
     */
    private void incrementSensor(RequestSensors sensors, Context context, Type type, Message<?> message)
    {
        Optional<Sensor> sensor = sensors.getSensor(context, type);
        sensor.ifPresent(s -> {
            String customParam = SensorsCustomParams.requestParamForSensor(s);
            double sensorValue = SensorsCustomParams.sensorValueFromInternodeResponse(message, customParam);
            sensors.incrementSensor(context, type, sensorValue);
        });
    }
}
