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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.locator.ReplicaPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.metrics.ReplicaResponseSizeMetrics;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    private static final boolean useDynamicSnitchForCounterLeader = CassandraRelevantProperties.USE_DYNAMIC_SNITCH_FOR_COUNTER_LEADER.getBoolean();

    protected volatile int responses;
    private static final AtomicIntegerFieldUpdater<WriteResponseHandler> responsesUpdater
            = AtomicIntegerFieldUpdater.newUpdater(WriteResponseHandler.class, "responses");

    public WriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan,
                                Runnable callback,
                                WriteType writeType,
                                long queryStartNanoTime)
    {
        super(replicaPlan, callback, writeType, queryStartNanoTime);
        responses = blockFor();
    }

    public WriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan, WriteType writeType, long queryStartNanoTime)
    {
        this(replicaPlan, null, writeType, queryStartNanoTime);
    }

    @Override
    public boolean trackLatencyForSnitch(Verb responseVerb, boolean isTimeout)
    {
        return useDynamicSnitchForCounterLeader && responseVerb == Verb.COUNTER_MUTATION_RSP && isTimeout;
    }

    @Override
    public void onResponse(Message<T> m)
    {
        trackReplicaResponseSize(m);
        if (responsesUpdater.decrementAndGet(this) == 0)
            signal();
        //Must be last after all subclass processing
        //The two current subclasses both assume logResponseToIdealCLDelegate is called
        //here.
        logResponseToIdealCLDelegate(m);
    }

    public int ackCount()
    {
        return blockFor() - responses;
    }
}
