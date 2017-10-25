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

import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;

import org.apache.cassandra.tracing.Tracing;

/**
 * Represents the state related to a given query.
 */
public class QueryState
{
    private final ClientState clientState;
    private volatile UUID preparedTracingSession;
    private final Map<QueryTimingType, QueryTiming> queryTimings = new ConcurrentHashMap<>();

    public QueryState(ClientState clientState)
    {
        this.clientState = clientState;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls());
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    /**
     * This clock guarantees that updates for the same QueryState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        return clientState.getTimestamp();
    }

    public boolean traceNextQuery()
    {
        if (preparedTracingSession != null)
        {
            return true;
        }

        double traceProbability = StorageService.instance.getTraceProbability();
        return traceProbability != 0 && ThreadLocalRandom.current().nextDouble() < traceProbability;
    }

    public void prepareTracingSession(UUID sessionId)
    {
        this.preparedTracingSession = sessionId;
    }

    public void createTracingSession()
    {
        UUID session = this.preparedTracingSession;
        if (session == null)
        {
            Tracing.instance.newSession();
        }
        else
        {
            Tracing.instance.newSession(session);
            this.preparedTracingSession = null;
        }
    }

    public InetAddress getClientAddress()
    {
        return clientState.isInternal
             ? null
             : clientState.getRemoteAddress().getAddress();
    }

    public void addTiming(long t0, QueryTimingType type)
    {
        long t = System.nanoTime() - t0;
        QueryTiming prev = queryTimings.putIfAbsent(type, new QueryTiming(type, t));
        if (prev != null)
            prev.add(t);
    }

    public void maybeLogTimings(Logger logger, String format, Object... params)
    {
        if (queryTimings.isEmpty())
            return;
        String msg = String.format(format, params);
        StringBuilder sb = new StringBuilder();
        for (QueryTiming t : queryTimings.values())
        {
            sb.append(t.type.name()).append(',').append(t.elapsedNanos).append(',');
        }
        logger.debug("{}\n    FINE_GRANULAR_TIMINGS: {}", msg, sb);
    }

    public enum QueryTimingType
    {
        modificationGetMutations,
        modificationAssureSufficientLiveNodes,
        modificationWrapBatchResponseHandler,
        modificationSyncWriteToBatchlog,
        modificationSyncWriteBatchedMutations,
        performWrite,
        mutateBlockingWait,
        getNaturalEndpoints,
        getPendingEndpoints,
        mutateWithTriggers,
        mutateAtomically,
        checkAccess
    }

    public static final class QueryTiming
    {
        public final QueryTimingType type;
        public long elapsedNanos;

        public QueryTiming(QueryTimingType type, long elapsedNanos)
        {
            this.type = type;
            this.elapsedNanos = elapsedNanos;
        }

        public void add(long t)
        {
            this.elapsedNanos += t;
        }
    }
}
