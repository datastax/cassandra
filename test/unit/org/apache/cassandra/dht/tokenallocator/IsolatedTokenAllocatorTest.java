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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IsolatedTokenAllocatorTest
{
    // This test ensures that as we increase shards, we maintain the invariant that lower level splits are
    // always higher level splits.
    @Test
    public void testTokenAllocationForSingleNode() throws UnknownHostException
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
        var config = new Config();
        config.num_tokens = 8;
        DatabaseDescriptor.setConfig(config);
        var tokenMetadata = new TokenMetadata();
        var snitch = DatabaseDescriptor.getEndpointSnitch();
        var networkTopology = new NetworkTopologyStrategy("0", tokenMetadata, snitch, Map.of());

        var dc = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        // Initialize the token metadata with 8 nodes
        var tokens = generateFakeEndpoints(tokenMetadata, networkTopology, 1, 1, 8, dc, "0");
        tokens.sort(Token::compareTo);

        var nodeTokens = tokenMetadata.sortedTokens();
        assertEquals(8, nodeTokens.size());
        assertArrayEquals(nodeTokens.toArray(), tokens.toArray());

        // Inductively demonstrate that each higher level split contains all the previous splits.
        List<Token> previousTokens = List.of();
        for (int i = 1; i < 256; i++)
        {
            var newTokens = IsolatedTokenAllocator.allocateTokens(i, networkTopology);
            assertEquals(i, newTokens.size());
            assertTrue(newTokens.containsAll(previousTokens));
            // The original 8 tokens must not be in the "new" tokens
            assertTrue(newTokens.stream().noneMatch(nodeTokens::contains));
            previousTokens = newTokens;
        }
    }

    // Test confirms that the IsolatedTokenAllocator generates token splits in the same way that we generate new
    // tokens for added nodes.
    @Test
    public void testTokenAllocationForMultipleNodesOneRack() throws UnknownHostException
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
        var config = new Config();
        config.num_tokens = 8;
        DatabaseDescriptor.setConfig(config);
        var tokenMetadata = new TokenMetadata();
        var snitch = DatabaseDescriptor.getEndpointSnitch();
        var networkTopology = new NetworkTopologyStrategy("0", tokenMetadata, snitch, Map.of());

        var dc = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();

        // Add the first node
        var tokens1 = generateFakeEndpoints(tokenMetadata, networkTopology, 1, 1, 8, dc, "0");
        // Generate the next set of tokens
        var nextTokens = IsolatedTokenAllocator.allocateTokens(8, networkTopology);
        // The newTokens should not include any tokens from tokens1
        assertTrue(nextTokens.stream().noneMatch(tokens1::contains));
        // We expect the newly added tokens to contain the splits defined in nextTokens
        var tokens2 = generateFakeEndpoints(tokenMetadata, networkTopology, 2, 2, 8, dc, "0");
        // tokens2 should contain nextTokens
        assertTrue(tokens2.containsAll(nextTokens));

        // Confirm for next 24 tokens (3 nodes worth)
        nextTokens = IsolatedTokenAllocator.allocateTokens(24, networkTopology);
        assertEquals(24, nextTokens.size());
        var tokens345 = generateFakeEndpoints(tokenMetadata, networkTopology, 3, 5, 8, dc, "0");
        assertTrue(tokens345.containsAll(nextTokens));
    }

    // Test confirms that the IsolatedTokenAllocator generates token splits in the same way that we generate new
    // tokens for added nodes.
    @Test
    public void testTokenAllocationForMultiNodeMultiRack() throws UnknownHostException
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(new RackInferringSnitch());
        var config = new Config();
        config.num_tokens = 8;
        DatabaseDescriptor.setConfig(config);
        var tokenMetadata = new TokenMetadata();
        var snitch = DatabaseDescriptor.getEndpointSnitch();
        var dc = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        var rf = Map.of(dc, "3");
        var networkTopology = new NetworkTopologyStrategy("0", tokenMetadata, snitch, rf);


        var existingTokens = new ArrayList<>();
        // Set up 1 node in each of 3 racks
        for (int i = 0; i < 3; i++)
           existingTokens.addAll(generateFakeEndpoints(tokenMetadata, networkTopology, 1, 1, 8, dc, Integer.toString(i)));

        // Generate the next set of tokens
        var nextTokens = IsolatedTokenAllocator.allocateTokens(16, networkTopology);
        // The nextTokens should not include any tokens from existingTokens
        assertTrue(nextTokens.stream().noneMatch(existingTokens::contains));
        var newlyAddedTokens = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            newlyAddedTokens.addAll(generateFakeEndpoints(tokenMetadata, networkTopology, 2, 3, 8, dc, Integer.toString(i)));
        assertTrue(newlyAddedTokens.containsAll(nextTokens));
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
