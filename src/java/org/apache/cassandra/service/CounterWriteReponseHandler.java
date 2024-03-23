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

package org.apache.cassandra.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.SensorsCustomParams;

/**
 * A {@link AbstractWriteResponseHandler} aware callback to be used when we need to send a response to a counter mutation.
 */
public class CounterWriteReponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    private final AbstractWriteResponseHandler<T> wrapped;

    // A map of sensor name to value as received from replicas. Values are commulative.
    private final Map<String, Double> replicaSensors;

    protected CounterWriteReponseHandler(AbstractWriteResponseHandler<T> wrapped, long queryStartNanoTime, Runnable callback)
    {
        super(wrapped.replicaPlan, wrapped.callback, wrapped.writeType, queryStartNanoTime);
        this.wrapped = wrapped;
        this.replicaSensors = new ConcurrentHashMap<>();
    }

    @Override
    public int ackCount()
    {
        return wrapped.ackCount();
    }

    @Override
    public void onResponse(Message<T> msg)
    {
        // message is null when the response is local
        if (msg != null)
        {
            Map<String, byte[]> customParams = msg.header.customParams();
            if (customParams != null)
            {
                customParams.entrySet().stream().filter(e -> e.getKey().startsWith("WRITE_BYTES_REQUEST.")).forEach(
                e -> {
                    double value = SensorsCustomParams.sensorValueFromBytes(e.getValue());
                    replicaSensors.compute(e.getKey(), (ignored, v) -> v == null ? value : v + value);
                });
            }
        }
        // must happen after the sensor values are updated to guarntee values are updated before the callback is called
        wrapped.onResponse(msg);
    }

    public Map<String, Double> replicaSensors()
    {
        return replicaSensors;
    }
}
