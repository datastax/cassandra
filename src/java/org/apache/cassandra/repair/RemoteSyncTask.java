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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RangeHash;

/**
 * RemoteSyncTask sends {@link SyncRequest} to remote(non-coordinator) node
 * to repair(stream) data with other replica.
 *
 * When RemoteSyncTask receives SyncComplete from remote node, task completes.
 */
public class RemoteSyncTask extends SyncTask
{
    private static final Logger logger = LoggerFactory.getLogger(RemoteSyncTask.class);
    private final RepairSession session;

    public RemoteSyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, RepairSession session,
                          Executor taskExecutor, SyncTask next, Map<InetAddress, Set<RangeHash>> receivedRangeCache)
    {
        super(desc, r1, r2, taskExecutor, next, receivedRangeCache);
        this.session = session;
    }

    protected void startSync(List<Range<Token>> transferToLeft, List<Range<Token>> transferToRight)
    {
        // RemoteSyncTask expects SyncComplete message sent back.
        // Register task to RepairSession to receive response.
        session.waitForSync(Pair.create(desc, new NodePair(endpoint1, endpoint2)), this);
        InetAddress local = FBUtilities.getBroadcastAddress();

        List<Range<Token>> ranges = new ArrayList<>(transferToLeft.size() + transferToRight.size() + 1);
        ranges.addAll(transferToLeft);
        ranges.add(StreamingRepairTask.RANGE_SEPARATOR);
        ranges.addAll(transferToRight);

        SyncRequest request = new SyncRequest(desc, local, endpoint1, endpoint2, ranges);
        String message = String.format("Forwarding streaming repair of %d ranges to %s%s (to be streamed with %s)",
                                       transferToLeft.size(), request.src, transferToLeft.size() != transferToRight.size()?
                                                                           String.format(" and %d ranges from", transferToRight.size()) : "",
                                       request.dst);
        logger.info("[repair #{}] {}", desc.sessionId, message);
        Tracing.traceRepair(message);
        MessagingService.instance().sendOneWay(request.createMessage(), request.src);
    }

    public void syncComplete(boolean success)
    {
        if (success)
        {
            set(stat);
        }
        else
        {
            setException(new RepairException(desc, String.format("Sync failed between %s and %s", endpoint1, endpoint2)));
        }
    }
}
