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

package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pending ranges of a node shrinking to a subset of its tokens, checked against the replicas before and after the
 * shrink computed by the replication strategy.
 */
public class ShrinkingEndpointPendingRangesTest
{
    private static final String KEYSPACE = "ks";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final class Cluster
    {
        final Map<InetAddressAndPort, String> dcs = new HashMap<>();
        final Map<InetAddressAndPort, String> racks = new HashMap<>();
        final Map<InetAddressAndPort, List<Token>> tokens = new HashMap<>();
        final IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return racks.get(endpoint);
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return dcs.get(endpoint);
            }
        };

        TokenMetadata metadata()
        {
            TokenMetadata metadata = new TokenMetadata(snitch);
            tokens.forEach((endpoint, endpointTokens) -> metadata.updateNormalTokens(endpointTokens, endpoint));
            return metadata;
        }
    }

    private static Cluster randomCluster(Random random, int dcCount) throws UnknownHostException
    {
        Cluster cluster = new Cluster();
        Set<Token> used = new HashSet<>();
        int id = 1;
        for (int dc = 1; dc <= dcCount; dc++)
        {
            int nodes = 1 + random.nextInt(8);
            int racks = 1 + random.nextInt(4);
            for (int i = 0; i < nodes; i++, id++)
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + id);
                cluster.dcs.put(endpoint, "dc" + dc);
                cluster.racks.put(endpoint, "rack" + random.nextInt(racks));
                List<Token> endpointTokens = new ArrayList<>();
                int count = 1 + random.nextInt(8);
                while (endpointTokens.size() < count)
                {
                    Token token = Murmur3Partitioner.instance.getRandomToken(random);
                    if (used.add(token))
                        endpointTokens.add(token);
                }
                cluster.tokens.put(endpoint, endpointTokens);
            }
        }
        return cluster;
    }

    private static AbstractReplicationStrategy strategy(Random random, Cluster cluster, TokenMetadata metadata)
    {
        Set<String> dcs = new HashSet<>(cluster.dcs.values());
        if (dcs.size() == 1 && random.nextInt(3) == 0)
            return new SimpleStrategy(KEYSPACE, metadata, cluster.snitch, Collections.singletonMap("replication_factor", Integer.toString(1 + random.nextInt(4))));
        Map<String, String> options = new HashMap<>();
        for (String dc : dcs)
            options.put(dc, Integer.toString(1 + random.nextInt(4)));
        return new NetworkTopologyStrategy(KEYSPACE, metadata, cluster.snitch, options);
    }

    @Test
    public void testPendingRangesOfShrinkingEndpoint() throws UnknownHostException
    {
        Random random = new Random(11);
        int checked = 0;
        for (int iteration = 0; iteration < 300; iteration++)
        {
            Cluster cluster = randomCluster(random, 1 + random.nextInt(2));
            List<InetAddressAndPort> candidates = new ArrayList<>();
            cluster.tokens.forEach((endpoint, tokens) -> {
                if (tokens.size() > 1)
                    candidates.add(endpoint);
            });
            if (candidates.isEmpty())
                continue;
            InetAddressAndPort shrinking = candidates.get(random.nextInt(candidates.size()));
            List<Token> current = new ArrayList<>(cluster.tokens.get(shrinking));
            Collections.shuffle(current, random);
            List<Token> kept = current.subList(0, 1 + random.nextInt(current.size() - 1));

            TokenMetadata metadata = cluster.metadata();
            AbstractReplicationStrategy strategy = strategy(random, cluster, metadata);
            TokenMetadata before = metadata.cloneOnlyTokenMap();
            metadata.addShrinkingEndpoint(kept, shrinking);
            metadata.calculatePendingRanges(strategy, KEYSPACE);

            TokenMetadata after = metadata.cloneAfterAllSettled();
            assertThat(after.getTokens(shrinking)).containsExactlyInAnyOrderElementsOf(kept);

            // every range of the ring before the shrink: the pending endpoints are exactly the new replicas
            for (Token token : before.sortedTokens())
            {
                Set<InetAddressAndPort> oldReplicas = strategy.calculateNaturalReplicas(token, before).endpoints();
                Set<InetAddressAndPort> newReplicas = strategy.calculateNaturalReplicas(token, after).endpoints();
                Set<InetAddressAndPort> pending = metadata.pendingEndpointsForToken(token, KEYSPACE).endpoints();
                String description = String.format("%s shrinking to %s, token %s, %s, before %s after %s", shrinking, kept, token, strategy, oldReplicas, newReplicas);
                assertThat(pending).as(description).isEqualTo(Sets.difference(newReplicas, oldReplicas));
                // the shrinking node never gains a range, and never makes another node leave a replica set
                assertThat(pending).as(description).doesNotContain(shrinking);
                assertThat(newReplicas).as(description).containsAll(Sets.difference(oldReplicas, Collections.singleton(shrinking)));
                checked++;
            }

            // the shrink completes (new tokens) or is aborted (same tokens): nothing is pending any more
            if (random.nextBoolean())
                metadata.updateNormalTokens(kept, shrinking);
            else
                metadata.removeFromShrinking(shrinking);
            assertThat(metadata.isShrinking(shrinking)).isFalse();
            metadata.calculatePendingRanges(strategy, KEYSPACE);
            for (Token token : before.sortedTokens())
                assertThat(metadata.pendingEndpointsForToken(token, KEYSPACE)).isEmpty();
        }
        assertThat(checked).isGreaterThan(1000);
    }

    /**
     * Several nodes shrinking at the same time (e.g. a peer that hasn't seen the end of a shrink yet when the next one
     * starts): the pending endpoints of every token are the new replicas of the ring with every shrink done, without
     * duplicates (which would make the writes fail).
     */
    @Test
    public void testSeveralShrinkingEndpoints() throws UnknownHostException
    {
        Random random = new Random(13);
        for (int iteration = 0; iteration < 200; iteration++)
        {
            Cluster cluster = randomCluster(random, 1 + random.nextInt(2));
            TokenMetadata metadata = cluster.metadata();
            AbstractReplicationStrategy strategy = strategy(random, cluster, metadata);
            TokenMetadata before = metadata.cloneOnlyTokenMap();
            for (Map.Entry<InetAddressAndPort, List<Token>> entry : cluster.tokens.entrySet())
            {
                if (entry.getValue().size() > 1 && random.nextBoolean())
                {
                    List<Token> current = new ArrayList<>(entry.getValue());
                    Collections.shuffle(current, random);
                    metadata.addShrinkingEndpoint(current.subList(0, 1 + random.nextInt(current.size() - 1)), entry.getKey());
                }
            }
            metadata.calculatePendingRanges(strategy, KEYSPACE);
            TokenMetadata after = metadata.cloneAfterAllSettled();
            for (Token token : before.sortedTokens())
            {
                Set<InetAddressAndPort> oldReplicas = strategy.calculateNaturalReplicas(token, before).endpoints();
                Set<InetAddressAndPort> newReplicas = strategy.calculateNaturalReplicas(token, after).endpoints();
                // pendingEndpointsForToken throws on duplicate endpoints
                assertThat(metadata.pendingEndpointsForToken(token, KEYSPACE).endpoints()).isEqualTo(Sets.difference(newReplicas, oldReplicas));
            }
        }
    }

    /**
     * The pending ranges of a shrink are computed on every node, for every keyspace: a 256-token ring must be quick.
     */
    @Test
    public void testPendingRangesOfLargeRing() throws UnknownHostException
    {
        Random random = new Random(14);
        Cluster cluster = new Cluster();
        Set<Token> used = new HashSet<>();
        for (int i = 1; i <= 24; i++)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.1." + i);
            cluster.dcs.put(endpoint, "dc1");
            cluster.racks.put(endpoint, "rack" + (i % 3));
            List<Token> tokens = new ArrayList<>();
            while (tokens.size() < 256)
            {
                Token token = Murmur3Partitioner.instance.getRandomToken(random);
                if (used.add(token))
                    tokens.add(token);
            }
            cluster.tokens.put(endpoint, tokens);
        }
        TokenMetadata metadata = cluster.metadata();
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy(KEYSPACE, metadata, cluster.snitch, Collections.singletonMap("dc1", "3"));
        InetAddressAndPort shrinking = InetAddressAndPort.getByName("127.0.1.1");
        metadata.addShrinkingEndpoint(cluster.tokens.get(shrinking).subList(0, 128), shrinking);
        long start = System.nanoTime();
        metadata.calculatePendingRanges(strategy, KEYSPACE);
        long millis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(millis).as("pending ranges of a 24 x 256 token ring in %s ms", millis).isLessThan(5000);
        int pending = 0;
        for (InetAddressAndPort endpoint : cluster.tokens.keySet())
            pending += metadata.getPendingRanges(KEYSPACE, endpoint).size();
        assertThat(pending).isGreaterThan(0);
        assertThat(metadata.getPendingRanges(KEYSPACE, shrinking)).isEmpty();
    }

    @Test
    public void testShrinkingEndpointBookkeeping() throws UnknownHostException
    {
        Cluster cluster = randomCluster(new Random(12), 1);
        InetAddressAndPort endpoint = cluster.tokens.keySet().iterator().next();
        TokenMetadata metadata = cluster.metadata();
        long version = metadata.getRingVersion();
        List<Token> kept = cluster.tokens.get(endpoint).subList(0, 1);

        metadata.addShrinkingEndpoint(kept, endpoint);
        assertThat(metadata.getRingVersion()).isGreaterThan(version);
        assertThat(metadata.isShrinking(endpoint)).isTrue();
        assertThat(metadata.getSizeOfShrinkingEndpoints()).isEqualTo(1);
        assertThat(metadata.getShrinkingEndpoints()).containsOnlyKeys(endpoint);
        assertThat(metadata.toString()).contains("Shrinking Endpoints");
        // a node that is removed is not shrinking any more
        metadata.removeEndpoint(endpoint);
        assertThat(metadata.isShrinking(endpoint)).isFalse();

        metadata.addShrinkingEndpoint(kept, endpoint);
        metadata.clearUnsafe();
        assertThat(metadata.getSizeOfShrinkingEndpoints()).isZero();
    }
}
