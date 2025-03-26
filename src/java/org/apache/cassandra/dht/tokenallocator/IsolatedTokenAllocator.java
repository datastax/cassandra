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

package org.apache.cassandra.dht.tokenallocator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * A utility class that allocates additional tokens for a given {@link AbstractReplicationStrategy} by creating mock
 * nodes and then allocating tokens for them. The source metadata and replication strategy are not modified. This
 * class relies on the detail that the allocation of new tokens for bootstrapping nodes is deterministic.
 */
public class IsolatedTokenAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(IsolatedTokenAllocator.class);

    public static List<Token> allocateTokens(int additionalSplits, AbstractReplicationStrategy source)
    {
        Preconditions.checkArgument(additionalSplits > 0, "additionalSplits must be greater than zero");
        Preconditions.checkNotNull(source);

        List<Token> allocatedTokens = new ArrayList<>();
        QuietAllocator allocator = new QuietAllocator(source);

        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        var localDc = source.snitch.getLocalDatacenter();
        // Get a list to consistently iterate over the racks as we allocate nodes. Need to clone the map in
        // order to retreive the topology.
        var localRacks = source.getTokenMetadata().cloneOnlyTokenMap().getTopology().getDatacenterRacks().get(localDc);
        assert localRacks != null && !localRacks.isEmpty() : "No racks found for local datacenter " + localDc;
        // Because we have RF=racks, we do not need to worry about the order of the racks. If we wnat to make anything
        // else work, we probably need to know the order of the racks here to ensure we do it the same way each time.
        // Issues could arise where we allocate a token for one rack, but that isn't the rack
        // that bootstraps a new node next.
        var racks = Iterators.cycle(localRacks.keySet());
        int nodeId = 0;
        while (allocatedTokens.size() < additionalSplits)
        {
            // Allocate tokens for current node, distributing tokens round-robin among the racks.
            var newTokens = allocator.allocateTokensForNode(nodeId, racks.next());
            int remainingTokensNeeded = additionalSplits - allocatedTokens.size();
            if (newTokens.size() > remainingTokensNeeded)
            {
                var iter = newTokens.iterator();
                for (int i = 0; i < remainingTokensNeeded; i++)
                    allocatedTokens.add(iter.next());
                return allocatedTokens;
            }
            else
            {
                allocatedTokens.addAll(newTokens);
            }
            nodeId++;
        }
        return allocatedTokens;
    }

    /**
     * A token allocator that takes a source token metadata and replication strategy, but clones the source token
     * metadata with a quiet snitch, so that added nodes are not communicated to the rest of the system.
     */
    private static class QuietAllocator
    {
        private final QuietSnitch quietSnitch;
        private final TokenMetadata quietTokenMetadata;
        private final TokenAllocation allocation;
        private final Map<String, SummaryStatistics> lastCheckPoint = Maps.newHashMap();

        private QuietAllocator(AbstractReplicationStrategy rs)
        {
            // Wrap the replication strategy's snitch with a quiet snitch
            this.quietSnitch = new QuietSnitch(rs.snitch);
            this.quietTokenMetadata = rs.getTokenMetadata().cloneWithNewSnitch(quietSnitch);
            var numTokens = DatabaseDescriptor.getNumTokens();
            this.allocation = TokenAllocation.create(quietTokenMetadata, rs, quietSnitch, numTokens);
        }

        private Collection<Token> allocateTokensForNode(int nodeId, String rackId)
        {
            // Update snitch and token metadata info to inform token allocation
            InetAddressAndPort fakeNodeAddressAndPort = getLoopbackAddressWithPort(nodeId);
            quietSnitch.nodeByRack.put(fakeNodeAddressAndPort, rackId);
            quietTokenMetadata.updateTopology(fakeNodeAddressAndPort);

            // Allocate tokens
            Collection<Token> tokens = allocation.allocate(fakeNodeAddressAndPort);

            // Validate ownership stats
            validateAllocation(nodeId, rackId);

            return tokens;
        }

        private void validateAllocation(int nodeId, String rackId)
        {
            SummaryStatistics newOwnership = allocation.getAllocationRingOwnership(SimpleSnitch.DATA_CENTER_NAME, rackId);
            SummaryStatistics oldOwnership = lastCheckPoint.put(rackId, newOwnership);
            if (oldOwnership != null)
                logger.debug(String.format("Replicated node load in rack=%s before allocating node %d: %s.", rackId, nodeId,
                                           TokenAllocation.statToString(oldOwnership)));
            logger.debug(String.format("Replicated node load in rack=%s after allocating node %d: %s.", rackId, nodeId,
                                       TokenAllocation.statToString(newOwnership)));
            if (oldOwnership != null && oldOwnership.getStandardDeviation() != 0.0)
            {
                double stdDevGrowth = newOwnership.getStandardDeviation() - oldOwnership.getStandardDeviation();
                if (stdDevGrowth > TokenAllocation.WARN_STDEV_GROWTH)
                {
                    logger.warn(String.format("Growth of %.2f%% in token ownership standard deviation after allocating node %d on rack %s above warning threshold of %d%%",
                                              stdDevGrowth * 100, nodeId, rackId, (int)(TokenAllocation.WARN_STDEV_GROWTH * 100)));
                }
            }
        }
    }

    /**
     * A snitch that doesn't gossip.
     */
    private static class QuietSnitch implements IEndpointSnitch
    {
        private final Map<InetAddressAndPort, String> nodeByRack = new HashMap<>();
        private final IEndpointSnitch fallbackSnitch;

        QuietSnitch(IEndpointSnitch fallbackSnitch)
        {
            this.fallbackSnitch = fallbackSnitch;
        }

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            String result = nodeByRack.get(endpoint);
            return result != null ? result : fallbackSnitch.getRack(endpoint);
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            // For our mocked endpoints, we return the local datacenter, otherwise we return the real datacenter
            return nodeByRack.containsKey(endpoint)
                   ? fallbackSnitch.getLocalDatacenter()
                   : fallbackSnitch.getDatacenter(endpoint);
        }

        @Override
        public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C addresses)
        {
            throw new NotImplementedException("sortedByProximity not implemented in QuietSnitch");
        }

        @Override
        public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
        {
            throw new NotImplementedException("compareEndpoints not implemented in QuietSnitch");
        }

        @Override
        public void gossiperStarting()
        {
            // This snitch doesn't gossip.
        }

        @Override
        public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
        {
            throw new NotImplementedException("isWorthMergingForRangeQuery not implemented in QuietSnitch");
        }
    }

    private static InetAddressAndPort getLoopbackAddressWithPort(int port)
    {
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByName("127.0.0.1"), port);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalStateException("Unexpected UnknownHostException", e);
        }
    }
}
