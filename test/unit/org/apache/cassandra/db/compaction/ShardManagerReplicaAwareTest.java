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

package org.apache.cassandra.db.compaction;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShardManagerReplicaAwareTest
{

    @Test
    public void testRangeEndsForShardCountEqualtToNumTokensPlusOne() throws UnknownHostException
    {
        for (int numTokens = 1; numTokens < 32; numTokens++)
        {
            var rs = buildStrategy(numTokens, 1, 1, 1);
            var expectedTokens = rs.getTokenMetadata().sortedTokens();
            var shardManager = new ShardManagerReplicaAware(rs, 1L<<16);

            var shardCount = numTokens + 1;
            var iterator = shardManager.boundaries(shardCount);
            assertEquals(Murmur3Partitioner.instance.getMinimumToken(), iterator.shardStart());
            var actualTokens = new ArrayList<Token>();
            for (Token end = iterator.shardEnd(); end != null; end = iterator.shardEnd())
            {
                assertFalse(iterator.advanceTo(end));
                assertTrue(iterator.advanceTo(end.nextValidToken()));
                actualTokens.add(end);
            }
            assertEquals(expectedTokens, actualTokens);
        }
    }

    @Test
    public void testRangeEndsAreFromTokenListAndContainLowerRangeEnds() throws UnknownHostException
    {
        for (int nodeCount = 1; nodeCount <= 6; nodeCount++)
        {
            for (int numTokensPerNode = 1; numTokensPerNode < 16; numTokensPerNode++)
            {
                // Confirm it works for multiple base shard counts.
                for (int baseShardCount = 1; baseShardCount <= 3; baseShardCount++)
                {
                    // Testing with 1 rack, nodeCount nodes, and rf 1.
                    var rs = buildStrategy(numTokensPerNode, 1, nodeCount, 1);
                    var initialSplitPoints = rs.getTokenMetadata().sortedTokens();
                    // Confirm test set up is correct.
                    assertEquals(numTokensPerNode * nodeCount, initialSplitPoints.size());
                    // Use a shared instance to
                    var shardManager = new ShardManagerReplicaAware(rs, 1L<<16);

                    // The tokens for one level lower.
                    var lowerTokens = new ArrayList<Token>();
                    var tokenLimit = numTokensPerNode * nodeCount * 8;
                    for (int shardExponent = 0; baseShardCount * Math.pow(2, shardExponent) <= tokenLimit; shardExponent++)
                    {
                        var shardCount = baseShardCount * (int) Math.pow(2, shardExponent);
                        var iterator = shardManager.boundaries(shardCount);
                        assertEquals(Murmur3Partitioner.instance.getMinimumToken(), iterator.shardStart());
                        assertEquals(shardCount, iterator.count());
                        var actualSplitPoints = new ArrayList<Token>();
                        var shardSpanSize = 0d;
                        var index = -1;
                        for (Token end = iterator.shardEnd(); end != null; end = iterator.shardEnd())
                        {
                            shardSpanSize += iterator.shardSpanSize();
                            assertEquals(index++, iterator.shardIndex());
                            assertFalse(iterator.advanceTo(end));
                            assertTrue(iterator.advanceTo(end.nextValidToken()));
                            actualSplitPoints.add(end);
                        }
                        // Need to add the last shard span size because we exit the above loop before adding it.
                        shardSpanSize += iterator.shardSpanSize();
                        // Confirm the shard span size adds to about 1
                        assertEquals(1d, shardSpanSize, 0.001);

                        // If we have more split points than the initialSplitPoints, we had to compute additional
                        // tokens, so the best we can do is confirm containment.
                        if (actualSplitPoints.size() >= initialSplitPoints.size())
                            assertTrue(actualSplitPoints + " does not contain " + initialSplitPoints,
                                       actualSplitPoints.containsAll(initialSplitPoints));
                        else
                            assertTrue(initialSplitPoints + " does not contain " + actualSplitPoints,
                                       initialSplitPoints.containsAll(actualSplitPoints));

                        // Higher tokens must always contain lower tokens.
                        assertTrue(actualSplitPoints + " does not contain " + lowerTokens,
                                   actualSplitPoints.containsAll(lowerTokens));
                        lowerTokens = actualSplitPoints;
                    }
                }
            }
        }
    }


    private AbstractReplicationStrategy buildStrategy(int numTokens, int numRacks, int numNodes, int rf) throws UnknownHostException
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
        var config = new Config();
        config.num_tokens = numTokens;
        DatabaseDescriptor.setConfig(config);
        var tokenMetadata = new TokenMetadata();
        var snitch = DatabaseDescriptor.getEndpointSnitch();
        var dc = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        // Configure rf
        var options = Map.of(dc, Integer.toString(rf));
        var networkTopology = new NetworkTopologyStrategy("0", tokenMetadata, snitch, options);

        for (int i = 0; i < numRacks; i++)
            generateFakeEndpoints(tokenMetadata, networkTopology, 1, numNodes, numTokens, dc, Integer.toString(i));

        return networkTopology;
    }

    // Generates endpoints and adds them to the tmd and the rs.
    private List<Token> generateFakeEndpoints(TokenMetadata tmd, AbstractReplicationStrategy rs, int firstNodeId, int lastNodId, int vnodes, String dc, String rack) throws UnknownHostException
    {
        System.out.printf("Adding nodes %d through %d to dc=%s, rack=%s.%n", firstNodeId, lastNodId, dc, rack);
        var result = new ArrayList<Token>();
        for (int i = firstNodeId; i <= lastNodId; i++)
        {
            // leave .1 for myEndpoint
            InetAddressAndPort addr = InetAddressAndPort.getByName("127." + dc + '.' + rack + '.' + (i + 1));
            var tokens = TokenAllocation.allocateTokens(tmd, rs, addr, vnodes);
            // TODO why don't we need addBootstrapTokens here? The test only passes with updateNormalTokens.
            // tmd.addBootstrapTokens(tokens, addr);
            tmd.updateNormalTokens(tokens, addr);
            result.addAll(tokens);
        }
        return result;
    }
}
