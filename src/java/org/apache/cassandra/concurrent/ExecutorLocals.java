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

package org.apache.cassandra.concurrent;

import java.util.Arrays;

import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/*
 * This class only knows about fixed locals, so if any different executor locals are added, it must be
 * updated.
 *
 * We don't enumerate the ExecutorLocal.all array each time because it would be much slower.
 */
public class ExecutorLocals
{
    private static final ExecutorLocal<TraceState> tracing = Tracing.instance;
    private static final ExecutorLocal<ClientWarn.State> clientWarn = ClientWarn.instance;
    private static final ExecutorLocal<RequestSensors> requestTracker = RequestTracker.instance;

    public final TraceState traceState;
    public final ClientWarn.State clientWarnState;
    public final RequestSensors sensors;

    private ExecutorLocals(TraceState traceState, ClientWarn.State clientWarnState, RequestSensors sensors)
    {
        this.traceState = traceState;
        this.clientWarnState = clientWarnState;
        this.sensors = sensors;
    }

    static
    {
        assert Arrays.equals(ExecutorLocal.all, new ExecutorLocal[]{ tracing, clientWarn, requestTracker })
        : "ExecutorLocals has not been updated to reflect new ExecutorLocal.all";
    }

    /**
     * This creates a new ExecutorLocals object based on what is already set.
     *
     * @return an ExecutorLocals object which has the trace state and client warn state captured if either has been set,
     *         or null if both are unset. The null result short-circuits logic in
     *         {@link AbstractLocalAwareExecutorService#newTaskFor(Runnable, Object, ExecutorLocals)}, preventing
     *         unnecessarily calling {@link ExecutorLocals#set(ExecutorLocals)}.
     */
    public static ExecutorLocals create()
    {
        TraceState traceState = tracing.get();
        ClientWarn.State clientWarnState = clientWarn.get();
        RequestSensors sensors = requestTracker.get();
        if (traceState == null && clientWarnState == null && sensors == null)
            return null;
        else
            return new ExecutorLocals(traceState, clientWarnState, sensors);
    }

    public static ExecutorLocals create(TraceState traceState, ClientWarn.State clientWarnState, RequestSensors sensors)
    {
        return new ExecutorLocals(traceState, clientWarnState, sensors);
    }

    public static ExecutorLocals create(TraceState traceState)
    {
        ClientWarn.State clientWarnState = clientWarn.get();
        RequestSensors sensors = requestTracker.get();
        return new ExecutorLocals(traceState, clientWarnState, sensors);
    }

    public static ExecutorLocals create(RequestSensors sensors)
    {
        TraceState traceState = tracing.get();
        ClientWarn.State clientWarnState = clientWarn.get();
        return new ExecutorLocals(traceState, clientWarnState, sensors);
    }

    public static void set(ExecutorLocals locals)
    {
        TraceState traceState = locals == null ? null : locals.traceState;
        ClientWarn.State clientWarnState = locals == null ? null : locals.clientWarnState;
        RequestSensors sensors = locals == null ? null : locals.sensors;
        tracing.set(traceState);
        clientWarn.set(clientWarnState);
        requestTracker.set(sensors);
    }
}
