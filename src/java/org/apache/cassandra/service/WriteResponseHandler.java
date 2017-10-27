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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.SECONDS);

    protected volatile int responses;
    private static final AtomicIntegerFieldUpdater<WriteResponseHandler> responsesUpdater
            = AtomicIntegerFieldUpdater.newUpdater(WriteResponseHandler.class, "responses");

    public WriteResponseHandler(Collection<InetAddress> writeEndpoints,
                                Collection<InetAddress> pendingEndpoints,
                                ConsistencyLevel consistencyLevel,
                                Keyspace keyspace,
                                Runnable callback,
                                WriteType writeType)
    {
        super(keyspace, writeEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);
        responses = totalBlockFor();
        int clBlockFor = consistencyLevel != null ? consistencyLevel.blockFor(keyspace) : -1;
        int write = writeEndpoints.size();
        int pending = pendingEndpoints.size();
        noSpamLogger.debug("{} type:{} ks:{} cl:{} total-blockFor:{} cl-blockFor:{} pending:{} ({}) endpoints:{} ({})",
                           getClass().getSimpleName(), writeType, keyspace, consistencyLevel, clBlockFor + pending, clBlockFor,
                           pending, perDC(pendingEndpoints), write, perDC(writeEndpoints));
    }

    private String perDC(Collection<InetAddress> endpoints)
    {
        int local = 0;
        int localWaiting = 0;
        Map<String, Pair<Integer, Integer>> remote = new HashMap<>();
        String localDC = DatabaseDescriptor.getLocalDataCenter();
        for (InetAddress ep : endpoints)
        {
            boolean waitingFor = waitingFor(ep);
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(ep);
            if (localDC.equals(dc))
            {
                local++;
                if (waitingFor)
                    localWaiting++;
            }
            else
            {
                Pair<Integer, Integer> ex = remote.get(dc);
                remote.put(dc, ex != null
                               ? Pair.create(ex.left + 1, ex.right + (waitingFor ? 1 : 0))
                               : Pair.create(1, waitingFor ? 1 : 0));
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("local:").append(localWaiting).append('/').append(local);
        for (Map.Entry<String, Pair<Integer, Integer>> e : remote.entrySet())
            sb.append(' ').append(e.getKey()).append(':').append(e.getValue().right).append('/').append(e.getValue().left);
        return sb.toString();
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType, Runnable callback)
    {
        this(Arrays.asList(endpoint), Collections.<InetAddress>emptyList(), ConsistencyLevel.ONE, null, callback, writeType);
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType)
    {
        this(endpoint, writeType, null);
    }

    public void response(MessageIn<T> m)
    {
        if (responsesUpdater.decrementAndGet(this) == 0)
            signal();
    }

    protected int ackCount()
    {
        return totalBlockFor() - responses;
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
