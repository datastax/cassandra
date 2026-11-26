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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceStateImpl extends TraceState
{
    private static final Logger logger = LoggerFactory.getLogger(TraceStateImpl.class);

    @VisibleForTesting
    public static int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS =
      Integer.parseInt(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "0"));

    private final Set<Future<?>> pendingFutures = ConcurrentHashMap.newKeySet();
    private final TraceStorage storage;

    public TraceStateImpl(ClientState state,
                          InetAddressAndPort coordinator,
                          UUID sessionId,
                          Tracing.TraceType traceType,
                          boolean isProbabilistic,
                          TraceStorage storage)
    {
        super(state, coordinator, sessionId, traceType, isProbabilistic);
        this.storage = storage;
    }

    @Override
    public TraceStorage getStorage()
    {
        return storage;
    }

    @Override
    public void stopSession()
    {
        if(!isStopped())
            pendingFutures.add(storage.stopSession(this));
    }

    @Override
    public void begin(InetAddress client, String request, Map<String, String> parameters)
    {
        Future<Void> e = storage.startSession(this, client, request, parameters, System.currentTimeMillis());
        pendingFutures.add(e);
    }

    protected void traceImpl(String message)
    {
        final String threadName = Thread.currentThread().getName();

        if (!pendingFutures.add(storage.recordEvent(this, threadName, message)))
            logger.warn("Failed to insert pending future, tracing synchronization may not work");

        if (logger.isTraceEnabled())
            logger.trace("Adding <{}> to trace events", message);
    }

    /**
     * Wait on submitted futures
     */
    protected void waitForPendingEvents()
    {
        if (WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS <= 0)
            return;

        try
        {
            if (logger.isTraceEnabled())
                logger.trace("Waiting for up to {} seconds for {} trace events to complete",
                             +WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, pendingFutures.size());

            CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture<?>[pendingFutures.size()]))
                             .get(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, TimeUnit.SECONDS);
        }
        catch (TimeoutException ex)
        {
            if (logger.isTraceEnabled())
                logger.trace("Failed to wait for tracing events to complete in {} seconds",
                             WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Got exception whilst waiting for tracing events to complete", t);
        }
    }
}
