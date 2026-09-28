/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.OutputHandler;

import static org.assertj.core.api.Assertions.assertThat;

public class TokenReductionPlannerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TokenReductionPlannerTest.class);
    private static final double EPSILON = 1e-9;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static List<TokenReductionPlanner.Node> randomCluster(Random random, Map<String, Integer> nodesPerDc, int racks, int tokensPerNode, boolean variableTokens)
    {
        Set<Token> used = new HashSet<>();
        List<TokenReductionPlanner.Node> nodes = new ArrayList<>();
        int id = 1;
        for (Map.Entry<String, Integer> dc : nodesPerDc.entrySet())
        {
            for (int i = 0; i < dc.getValue(); i++, id++)
            {
                int count = variableTokens ? 1 + random.nextInt(tokensPerNode) : tokensPerNode;
                List<Token> tokens = new ArrayList<>();
                while (tokens.size() < count)
                {
                    Token token = Murmur3Partitioner.instance.getRandomToken(random);
                    if (used.add(token))
                        tokens.add(token);
                }
                nodes.add(new TokenReductionPlanner.Node("127.0." + (id / 250) + '.' + (id % 250 + 1), dc.getKey(), "rack" + (i % racks), tokens));
            }
        }
        return nodes;
    }

    /**
     * Replicated ownership of every endpoint computed with the real NetworkTopologyStrategy.
     */
    private static Map<String, Double> realOwnership(List<TokenReductionPlanner.Node> nodes, Map<String, Integer> rfs) throws UnknownHostException
    {
        Map<InetAddressAndPort, TokenReductionPlanner.Node> byAddress = new HashMap<>();
        for (TokenReductionPlanner.Node node : nodes)
            byAddress.put(InetAddressAndPort.getByName(node.endpoint), node);
        AbstractNetworkTopologySnitch snitch = new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return byAddress.get(endpoint).rack;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return byAddress.get(endpoint).datacenter;
            }
        };
        TokenMetadata metadata = new TokenMetadata(snitch);
        for (Map.Entry<InetAddressAndPort, TokenReductionPlanner.Node> e : byAddress.entrySet())
            metadata.updateNormalTokens(e.getValue().tokens, e.getKey());
        Map<String, String> options = new HashMap<>();
        rfs.forEach((dc, rf) -> options.put(dc, Integer.toString(rf)));
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy("ks", metadata, snitch, options);

        Map<String, Double> ownership = new TreeMap<>();
        for (TokenReductionPlanner.Node node : nodes)
            ownership.put(node.endpoint, 0.0);
        for (Token token : metadata.sortedTokens())
        {
            for (Replica replica : strategy.calculateNaturalReplicas(token, metadata))
            {
                String endpoint = replica.endpoint().getHostAddress(false);
                ownership.merge(endpoint, replica.range().left.size(replica.range().right), Double::sum);
            }
        }
        return ownership;
    }

    private static Map<String, Double> modelOwnership(List<TokenReductionPlanner.Node> nodes, Map<String, Integer> rfs)
    {
        Map<String, Double> ownership = new TreeMap<>();
        for (String dc : rfs.keySet())
        {
            List<TokenReductionPlanner.Node> dcNodes = new ArrayList<>();
            for (TokenReductionPlanner.Node node : nodes)
                if (node.datacenter.equals(dc))
                    dcNodes.add(node);
            DatacenterRing ring = ring(dcNodes, rfs.get(dc));
            for (int i = 0; i < dcNodes.size(); i++)
                ownership.put(dcNodes.get(i).endpoint, ring.ownership(i));
        }
        return ownership;
    }

    private static DatacenterRing ring(List<TokenReductionPlanner.Node> nodes, int rf)
    {
        Map<String, Integer> rackIds = new HashMap<>();
        int[] racks = new int[nodes.size()];
        List<List<Token>> tokens = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++)
        {
            racks[i] = rackIds.computeIfAbsent(nodes.get(i).rack, r -> rackIds.size());
            tokens.add(nodes.get(i).tokens);
        }
        return new DatacenterRing(rf, racks, tokens);
    }

    private static void assertSameOwnership(Map<String, Double> expected, Map<String, Double> actual)
    {
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        expected.forEach((endpoint, owned) -> assertThat(actual.get(endpoint)).as(endpoint).isCloseTo(owned, org.assertj.core.data.Offset.offset(EPSILON)));
    }

    @Test
    public void testRingModelMatchesNetworkTopologyStrategy() throws UnknownHostException
    {
        Random random = new Random(1);
        for (int iteration = 0; iteration < 60; iteration++)
        {
            Map<String, Integer> nodesPerDc = new TreeMap<>();
            Map<String, Integer> rfs = new TreeMap<>();
            int dcs = 1 + random.nextInt(2);
            for (int dc = 1; dc <= dcs; dc++)
            {
                nodesPerDc.put("dc" + dc, 1 + random.nextInt(10));
                rfs.put("dc" + dc, 1 + random.nextInt(5));
            }
            int racks = 1 + random.nextInt(4);
            List<TokenReductionPlanner.Node> nodes = randomCluster(random, nodesPerDc, racks, 1 + random.nextInt(16), random.nextBoolean());
            String description = String.format("nodes %s, rf %s, racks %d", nodesPerDc, rfs, racks);
            assertSameOwnership(realOwnership(nodes, rfs), modelOwnership(nodes, rfs));

            // remove tokens one by one: the predicted change must match the change, and the model must keep matching
            // the real strategy
            for (String dc : rfs.keySet())
            {
                List<TokenReductionPlanner.Node> dcNodes = new ArrayList<>();
                for (TokenReductionPlanner.Node node : nodes)
                    if (node.datacenter.equals(dc))
                        dcNodes.add(node);
                DatacenterRing ring = ring(dcNodes, rfs.get(dc));
                List<List<Token>> remaining = new ArrayList<>();
                for (TokenReductionPlanner.Node node : dcNodes)
                    remaining.add(new ArrayList<>(node.tokens));
                for (int removal = 0; removal < 20; removal++)
                {
                    List<Integer> candidates = new ArrayList<>();
                    for (int i = 0; i < dcNodes.size(); i++)
                        if (remaining.get(i).size() > 1)
                            candidates.add(i);
                    if (candidates.isEmpty())
                        break;
                    int node = candidates.get(random.nextInt(candidates.size()));
                    Token token = remaining.get(node).remove(random.nextInt(remaining.get(node).size()));

                    double[] before = ring.ownership();
                    double[] predicted = ring.ownershipChangeIfRemoved(token);
                    ring.remove(token);
                    for (int i = 0; i < dcNodes.size(); i++)
                        assertThat(ring.ownership(i) - before[i]).as(description).isCloseTo(predicted[i], org.assertj.core.data.Offset.offset(EPSILON));

                    List<TokenReductionPlanner.Node> updated = new ArrayList<>();
                    for (int i = 0; i < dcNodes.size(); i++)
                        updated.add(new TokenReductionPlanner.Node(dcNodes.get(i).endpoint, dc, dcNodes.get(i).rack, remaining.get(i)));
                    Map<String, Double> expected = realOwnership(updated, Collections.singletonMap(dc, rfs.get(dc)));
                    Map<String, Double> actual = new TreeMap<>();
                    for (int i = 0; i < dcNodes.size(); i++)
                        actual.put(dcNodes.get(i).endpoint, ring.ownership(i));
                    assertSameOwnership(expected, actual);
                }
            }
        }
    }

    @Test
    public void testRounds()
    {
        assertThat(TokenReductionPlanner.rounds(256, 16, 2)).containsExactly(128, 64, 32, 16);
        assertThat(TokenReductionPlanner.rounds(256, 16, 1.5)).containsExactly(171, 114, 76, 51, 34, 23, 16);
        assertThat(TokenReductionPlanner.rounds(256, 200, 2)).containsExactly(200);
        assertThat(TokenReductionPlanner.rounds(3, 1, 1.1)).containsExactly(2, 1);
    }

    @Test
    public void testPlanKeepsSubsetsAndEndsWithTargetCount() throws UnknownHostException
    {
        Random random = new Random(2);
        Map<String, Integer> nodesPerDc = new TreeMap<>();
        nodesPerDc.put("dc1", 6);
        nodesPerDc.put("dc2", 4);
        Map<String, Integer> rfs = new TreeMap<>();
        rfs.put("dc1", 3);
        rfs.put("dc2", 2);
        List<TokenReductionPlanner.Node> nodes = randomCluster(random, nodesPerDc, 3, 64, false);
        TokenReductionPlanner.Plan plan = TokenReductionPlanner.plan(nodes, rfs, TokenReductionPlanner.rounds(64, 8, 2));

        Map<String, Set<Token>> current = new HashMap<>();
        for (TokenReductionPlanner.Node node : nodes)
            current.put(node.endpoint, new HashSet<>(node.tokens));
        for (TokenReductionPlanner.Round round : plan.rounds)
        {
            assertThat(round.steps).hasSize(nodes.size());
            for (TokenReductionPlanner.Step step : round.steps)
            {
                assertThat(step.keep).hasSize(round.targetTokens);
                assertThat(current.get(step.endpoint)).containsAll(step.keep);
                current.put(step.endpoint, new HashSet<>(step.keep));
            }
        }

        // the ownership the plan reports at the end is the real one
        List<TokenReductionPlanner.Node> result = new ArrayList<>();
        for (TokenReductionPlanner.Node node : nodes)
            result.add(new TokenReductionPlanner.Node(node.endpoint, node.datacenter, node.rack, new ArrayList<>(current.get(node.endpoint))));
        Map<String, Double> expected = realOwnership(result, rfs);
        Map<String, Double> reported = new TreeMap<>();
        for (String dc : rfs.keySet())
            reported.putAll(plan.finalOwnership(dc));
        assertSameOwnership(expected, reported);
    }

    /**
     * Balance reached by shrinking random 256-token rings to 16 tokens. The allocator building a new datacenter with 16
     * tokens per node is logged for comparison.
     */
    @Test
    public void testPlanBalanceComparedWithNewDatacenter()
    {
        int[][] configurations = { { 6, 1 }, { 12, 1 }, { 12, 3 }, { 24, 3 }, { 48, 1 } }; // nodes, racks
        for (int[] configuration : configurations)
        {
            int nodeCount = configuration[0];
            int racks = configuration[1];
            Random random = new Random(nodeCount * 31L + racks);
            List<TokenReductionPlanner.Node> nodes = randomCluster(random, Collections.singletonMap("dc1", nodeCount), racks, 256, false);
            Map<String, Integer> rfs = Collections.singletonMap("dc1", 3);
            TokenReductionPlanner.Plan plan = TokenReductionPlanner.plan(nodes, rfs, TokenReductionPlanner.rounds(256, 16, 2));

            double mean = 3.0 / nodeCount;
            double initialMax = Collections.max(plan.initialOwnership.get("dc1").values()) / mean;
            double finalMax = Collections.max(plan.finalOwnership("dc1").values()) / mean;
            double finalMin = Collections.min(plan.finalOwnership("dc1").values()) / mean;
            double peak = plan.peakOwnership("dc1") / mean;
            double streamed = plan.streamedOwnership("dc1") / 3.0; // in copies of the data set

            int[] nodesPerRack = new int[racks];
            for (int i = 0; i < nodeCount; i++)
                nodesPerRack[i % racks]++;
            List<OfflineTokenAllocator.FakeNode> fresh = OfflineTokenAllocator.allocate(3, 16, nodesPerRack, new OutputHandler.LogOutput(), Murmur3Partitioner.instance);
            List<TokenReductionPlanner.Node> freshNodes = new ArrayList<>();
            for (OfflineTokenAllocator.FakeNode node : fresh)
                freshNodes.add(new TokenReductionPlanner.Node("127.1.0." + (node.nodeId() + 1), "dc1", "rack" + node.rackId(), new ArrayList<>(node.tokens())));
            Map<String, Double> freshOwnership = modelOwnership(freshNodes, rfs);
            try
            {
                assertSameOwnership(realOwnership(freshNodes, rfs), freshOwnership);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
            double freshMax = Collections.max(freshOwnership.values()) / mean;
            double freshMin = Collections.min(freshOwnership.values()) / mean;

            logger.info("{} nodes, {} racks, RF 3, 256 -> 16 by halving: initial max {}, final max {} min {}, peak {}, streamed {} copies; new DC with 16 tokens: max {} min {}",
                        nodeCount, racks, fmt(initialMax), fmt(finalMax), fmt(finalMin), fmt(peak), fmt(streamed), fmt(freshMax), fmt(freshMin));

            // at least as balanced as the initial random ring, and within 8% of the fair share (a new datacenter
            // allocated with 16 tokens reaches ~2% with one rack)
            assertThat(finalMax).isLessThan(Math.min(initialMax, 1.08));
            assertThat(finalMin).isGreaterThan(0.92);
            // halving: the last node of a round owns at most twice its share
            assertThat(peak).isLessThan(2.0 * initialMax);
        }
    }

    private static String fmt(double value)
    {
        return String.format("%.3f", value);
    }
}
