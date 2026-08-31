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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.SensorsFactory;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.StorageProxy;

public class CounterMutationVerbHandler implements IVerbHandler<CounterMutation>
{
    public static final CounterMutationVerbHandler instance = new CounterMutationVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(CounterMutationVerbHandler.class);

    public void doVerb(final Message<CounterMutation> message)
    {
        long queryStartNanoTime = System.nanoTime();
        final CounterMutation cm = message.payload;
        logger.trace("Applying forwarded {}", cm);

        RequestSensors requestSensors = SensorsFactory.instance.createRequestSensors(message.payload.getKeyspaceName());
        ExecutorLocals.set(ExecutorLocals.create(requestSensors));

        // Register sensors that need to exist before applyCounterMutationOnLeader constructs the
        // WriteResponseHandler (which captures the RequestSensors reference at construction time).
        // WRITE_EXECUTION_TIME is built up in two steps on the leader:
        //   1. counterWriteTask increments it with the leader apply time before onResponse(null).
        //   2. ResponseVerbHandler increments it with max(sub-replica times) via the accumulator
        //      as sub-replica MUTATION_RSP ACKs arrive and the inner WriteResponseHandler reaches quorum.
        // The combined value is then encoded into the COUNTER_MUTATION_RSP by CounterMutationCallback
        // and picked up by the coordinator's ResponseVerbHandler accumulator (threshold=1).
        // INTERNODE_BYTES is incremented in CounterMutationCallback once the response is ready to send.
        Collection<TableMetadata> tables = message.payload.getPartitionUpdates().stream().map(PartitionUpdate::metadata).collect(Collectors.toSet());
        for (TableMetadata tm : tables)
        {
            requestSensors.registerSensor(Context.from(tm), Type.INTERNODE_BYTES);
            if (!tm.isIndex())
                requestSensors.registerSensor(Context.from(tm), Type.WRITE_EXECUTION_TIME);
        }

        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        // We should not wait for the result of the write in this thread,
        // otherwise we could have a distributed deadlock between replicas
        // running this VerbHandler (see #4578).
        // Instead, we use a callback to send the response. Note that the callback
        // will not be called if the request timeout, but this is ok
        // because the coordinator of the counter mutation will timeout on
        // it's own in that case.
        StorageProxy.applyCounterMutationOnLeader(cm,
                                                  localDataCenter,
                                                  new CounterMutationCallback(message, message.from(), requestSensors),
                                                  queryStartNanoTime);
    }
}
