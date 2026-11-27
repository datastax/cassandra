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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Token;

/**
 * Plans the in-place reduction of the number of tokens of the nodes of a cluster, in rounds, see
 * docs/operations/num-tokens-reduction-design.md.
 * <p>
 * In every round each node of a datacenter keeps a subset of its tokens. The tokens to drop are chosen greedily, one
 * token per node in turn (the node with the highest ownership first), always dropping the token whose removal most
 * reduces the squared difference between the replicated ownership of the nodes and their target share (proportional
 * to the number of tokens they keep at the end of the round). The steps of a round, one node at a time, are then
 * simulated in execution order to report the ownership of every node after every step and the data streamed.
 * <p>
 * Datacenters are planned independently: with NetworkTopologyStrategy the replicas of a datacenter only depend on its
 * own tokens.
 */
public class TokenReductionPlanner
{
    public static final class Node
    {
        public final String endpoint;
        public final String datacenter;
        public final String rack;
        public final List<Token> tokens;

        public Node(String endpoint, String datacenter, String rack, List<Token> tokens)
        {
            this.endpoint = endpoint;
            this.datacenter = datacenter;
            this.rack = rack;
            this.tokens = Collections.unmodifiableList(new ArrayList<>(tokens));
        }
    }

    /** One node shrinking to a subset of its tokens. */
    public static final class Step
    {
        public final String datacenter;
        public final String endpoint;
        public final List<Token> keep;
        public final int dropped;
        /** Sum of the replicated ownership gained by the other nodes, i.e. the ring fraction this node streams. */
        public final double streamedOwnership;
        /** Replicated ownership of every node of the datacenter after this step. */
        public final Map<String, Double> ownershipAfter;

        Step(String datacenter, String endpoint, List<Token> keep, int dropped, double streamedOwnership, Map<String, Double> ownershipAfter)
        {
            this.datacenter = datacenter;
            this.endpoint = endpoint;
            this.keep = keep;
            this.dropped = dropped;
            this.streamedOwnership = streamedOwnership;
            this.ownershipAfter = ownershipAfter;
        }

        public double maxOwnershipAfter()
        {
            return Collections.max(ownershipAfter.values());
        }
    }

    public static final class Round
    {
        public final int targetTokens;
        public final List<Step> steps;

        Round(int targetTokens, List<Step> steps)
        {
            this.targetTokens = targetTokens;
            this.steps = steps;
        }
    }

    public static final class Plan
    {
        public final Map<String, Map<String, Double>> initialOwnership;
        public final List<Round> rounds;

        Plan(Map<String, Map<String, Double>> initialOwnership, List<Round> rounds)
        {
            this.initialOwnership = initialOwnership;
            this.rounds = rounds;
        }

        /** Ownership of every node of the datacenter at the end of the plan. */
        public Map<String, Double> finalOwnership(String datacenter)
        {
            Map<String, Double> ownership = initialOwnership.get(datacenter);
            for (Round round : rounds)
                for (Step step : round.steps)
                    if (step.datacenter.equals(datacenter))
                        ownership = step.ownershipAfter;
            return ownership;
        }

        /** Ownership of every node of the datacenter at the start of the round (0-based). */
        public Map<String, Double> ownershipBefore(int round, String datacenter)
        {
            Map<String, Double> ownership = initialOwnership.get(datacenter);
            for (int r = 0; r < round; r++)
                for (Step step : rounds.get(r).steps)
                    if (step.datacenter.equals(datacenter))
                        ownership = step.ownershipAfter;
            return ownership;
        }

        /** Highest ownership of any node of the datacenter at any point of the plan. */
        public double peakOwnership(String datacenter)
        {
            double peak = Collections.max(initialOwnership.get(datacenter).values());
            for (Round round : rounds)
                for (Step step : round.steps)
                    if (step.datacenter.equals(datacenter))
                        peak = Math.max(peak, step.maxOwnershipAfter());
            return peak;
        }

        /** Sum of the ring fractions streamed by all the steps of the datacenter. */
        public double streamedOwnership(String datacenter)
        {
            double streamed = 0;
            for (Round round : rounds)
                for (Step step : round.steps)
                    if (step.datacenter.equals(datacenter))
                        streamed += step.streamedOwnership;
            return streamed;
        }
    }

    /**
     * @return the target token counts of the rounds going from {@code current} to {@code target} tokens, dividing the
     * count by {@code factor} at every round
     */
    public static List<Integer> rounds(int current, int target, double factor)
    {
        Preconditions.checkArgument(target > 0, "the target number of tokens must be positive");
        Preconditions.checkArgument(target < current, "the target number of tokens (%s) must be lower than the current one (%s)", target, current);
        Preconditions.checkArgument(factor > 1, "the reduction factor must be greater than 1");
        List<Integer> rounds = new ArrayList<>();
        double count = current;
        int previous = current;
        while (previous > target)
        {
            count /= factor;
            int next = Math.max(target, (int) Math.ceil(count));
            if (next >= previous)
                next = previous - 1;
            rounds.add(next);
            previous = next;
        }
        return rounds;
    }

    /**
     * @param nodes the nodes of the cluster with their current tokens
     * @param replicationFactors replication factor to balance for, per datacenter; every datacenter of {@code nodes}
     *                           must be present; a datacenter with RF 0 is balanced as if it had RF 1
     * @param rounds target number of tokens of every round, strictly decreasing; a node that has at most the target
     *               of a round is left unchanged in that round
     */
    public static Plan plan(List<Node> nodes, Map<String, Integer> replicationFactors, List<Integer> rounds)
    {
        Preconditions.checkArgument(!nodes.isEmpty(), "no nodes");
        Preconditions.checkArgument(!rounds.isEmpty(), "no rounds");
        for (int i = 0; i < rounds.size(); i++)
        {
            Preconditions.checkArgument(rounds.get(i) > 0, "round targets must be positive");
            Preconditions.checkArgument(i == 0 || rounds.get(i) < rounds.get(i - 1), "round targets must be strictly decreasing: %s", rounds);
        }

        Map<String, List<Node>> byDatacenter = new TreeMap<>();
        Set<String> endpoints = new HashSet<>();
        for (Node node : nodes)
        {
            Preconditions.checkArgument(endpoints.add(node.endpoint), "duplicate endpoint %s", node.endpoint);
            byDatacenter.computeIfAbsent(node.datacenter, dc -> new ArrayList<>()).add(node);
        }
        for (String dc : byDatacenter.keySet())
        {
            Preconditions.checkArgument(replicationFactors.containsKey(dc), "no replication factor for datacenter %s", dc);
            Preconditions.checkArgument(replicationFactors.get(dc) >= 0, "negative replication factor for datacenter %s", dc);
        }

        Map<String, List<List<Token>>> tokens = new HashMap<>();
        Map<String, Map<String, Double>> initialOwnership = new TreeMap<>();
        for (Map.Entry<String, List<Node>> dc : byDatacenter.entrySet())
        {
            List<List<Token>> dcTokens = new ArrayList<>();
            for (Node node : dc.getValue())
                dcTokens.add(new ArrayList<>(node.tokens));
            tokens.put(dc.getKey(), dcTokens);
            initialOwnership.put(dc.getKey(), ownershipByEndpoint(dc.getValue(), ring(dc.getValue(), dcTokens, balancingFactor(replicationFactors.get(dc.getKey())))));
        }

        List<Round> plannedRounds = new ArrayList<>();
        for (int target : rounds)
        {
            List<Step> steps = new ArrayList<>();
            for (Map.Entry<String, List<Node>> dc : byDatacenter.entrySet())
            {
                List<Node> dcNodes = dc.getValue();
                List<List<Token>> dcTokens = tokens.get(dc.getKey());
                Map<String, Integer> index = new HashMap<>();
                for (int i = 0; i < dcNodes.size(); i++)
                    index.put(dcNodes.get(i).endpoint, i);
                List<Step> dcSteps = planRound(dc.getKey(), dcNodes, dcTokens, balancingFactor(replicationFactors.get(dc.getKey())), target);
                for (Step step : dcSteps)
                    dcTokens.set(index.get(step.endpoint), new ArrayList<>(step.keep));
                steps.addAll(dcSteps);
            }
            plannedRounds.add(new Round(target, steps));
        }
        return new Plan(initialOwnership, plannedRounds);
    }

    private static List<Step> planRound(String datacenter, List<Node> nodes, List<List<Token>> tokens, int rf, int target)
    {
        // 1. choose the tokens to drop, on a copy of the ring
        DatacenterRing ring = ring(nodes, tokens, rf);
        int n = nodes.size();
        int[] finalCount = new int[n];
        int totalFinal = 0;
        for (int node = 0; node < n; node++)
        {
            finalCount[node] = Math.min(target, tokens.get(node).size());
            totalFinal += finalCount[node];
        }
        double totalOwnership = 0;
        for (double owned : ring.ownership())
            totalOwnership += owned;
        double[] targetOwnership = new double[n];
        for (int node = 0; node < n; node++)
            targetOwnership[node] = totalOwnership * finalCount[node] / totalFinal;

        List<Set<Token>> dropped = new ArrayList<>();
        for (int node = 0; node < n; node++)
            dropped.add(new HashSet<>());
        while (true)
        {
            List<Integer> pass = new ArrayList<>();
            for (int node = 0; node < n; node++)
                if (ring.tokenCount(node) > finalCount[node])
                    pass.add(node);
            if (pass.isEmpty())
                break;
            double[] owned = ring.ownership();
            pass.sort(Comparator.comparingDouble((Integer node) -> owned[node] - targetOwnership[node]).reversed());
            for (int node : pass)
            {
                Token best = null;
                double bestChange = Double.POSITIVE_INFINITY;
                for (int i = 0; i < ring.tokenCount(node); i++)
                {
                    Token token = ring.token(node, i);
                    double change = ring.balanceChangeIfRemoved(token, targetOwnership);
                    if (change < bestChange)
                    {
                        bestChange = change;
                        best = token;
                    }
                }
                ring.remove(best);
                dropped.get(node).add(best);
            }
        }

        // 2. simulate the execution, one node at a time, the most loaded node relative to its target first
        DatacenterRing execution = ring(nodes, tokens, rf);
        Set<Integer> remaining = new HashSet<>();
        for (int node = 0; node < n; node++)
            if (!dropped.get(node).isEmpty())
                remaining.add(node);
        List<Step> steps = new ArrayList<>();
        while (!remaining.isEmpty())
        {
            double[] before = execution.ownership();
            int next = Collections.max(remaining, Comparator.comparingDouble((Integer node) -> before[node] - targetOwnership[node]));
            remaining.remove(next);
            for (Token token : dropped.get(next))
                execution.remove(token);
            double[] after = execution.ownership();
            double streamed = 0;
            for (int node = 0; node < n; node++)
                if (after[node] > before[node])
                    streamed += after[node] - before[node];
            List<Token> keep = execution.tokens(next);
            Collections.sort(keep);
            steps.add(new Step(datacenter, nodes.get(next).endpoint, keep, dropped.get(next).size(), streamed,
                               ownershipByEndpoint(nodes, execution)));
        }
        return steps;
    }

    /**
     * A datacenter that replicates nothing (RF 0) still has to shrink: balance it as if it had RF 1, so that it is
     * ready to replicate data later.
     */
    private static int balancingFactor(int replicationFactor)
    {
        return Math.max(1, replicationFactor);
    }

    private static DatacenterRing ring(List<Node> nodes, List<List<Token>> tokens, int rf)
    {
        Map<String, Integer> rackIds = new HashMap<>();
        int[] racks = new int[nodes.size()];
        for (int i = 0; i < nodes.size(); i++)
            racks[i] = rackIds.computeIfAbsent(nodes.get(i).rack, r -> rackIds.size());
        return new DatacenterRing(rf, racks, tokens);
    }

    private static Map<String, Double> ownershipByEndpoint(List<Node> nodes, DatacenterRing ring)
    {
        Map<String, Double> ownership = new LinkedHashMap<>();
        for (int i = 0; i < nodes.size(); i++)
            ownership.put(nodes.get(i).endpoint, ring.ownership(i));
        return ownership;
    }
}
