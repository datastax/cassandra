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

package org.apache.cassandra.service;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link RangeRelocator#notCovered}, used to compute what a shrinking node streams, against the general pairwise
 * computation of {@link RangeRelocator#calculateStreamAndFetchRanges}.
 */
public class RangeRelocatorShrinkTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final class Ring
    {
        final TokenMetadata metadata;
        final NetworkTopologyStrategy strategy;
        final Map<InetAddressAndPort, List<Token>> tokens = new HashMap<>();

        Ring(Random random, int nodes, int racks, int tokensPerNode, int rf) throws UnknownHostException
        {
            Map<InetAddressAndPort, String> rackOf = new HashMap<>();
            AbstractNetworkTopologySnitch snitch = new AbstractNetworkTopologySnitch()
            {
                public String getRack(InetAddressAndPort endpoint)
                {
                    return rackOf.get(endpoint);
                }

                public String getDatacenter(InetAddressAndPort endpoint)
                {
                    return "dc1";
                }
            };
            metadata = new TokenMetadata(snitch);
            Set<Token> used = new HashSet<>();
            for (int i = 1; i <= nodes; i++)
            {
                InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.2." + i);
                rackOf.put(endpoint, "rack" + (i % racks));
                List<Token> nodeTokens = new ArrayList<>();
                while (nodeTokens.size() < tokensPerNode)
                {
                    Token token = Murmur3Partitioner.instance.getRandomToken(random);
                    if (used.add(token))
                        nodeTokens.add(token);
                }
                tokens.put(endpoint, nodeTokens);
                metadata.updateNormalTokens(nodeTokens, endpoint);
            }
            strategy = new NetworkTopologyStrategy("ks", metadata, snitch, Collections.singletonMap("dc1", Integer.toString(rf)));
        }
    }

    private static List<Range<Token>> normalized(RangesAtEndpoint ranges)
    {
        return Range.normalize(ranges.ranges());
    }

    @Test
    public void testSameAsPairwiseComputation() throws UnknownHostException
    {
        Random random = new Random(31);
        for (int iteration = 0; iteration < 200; iteration++)
        {
            Ring ring = new Ring(random, 2 + random.nextInt(6), 1 + random.nextInt(3), 1 + random.nextInt(12), 1 + random.nextInt(3));
            List<InetAddressAndPort> endpoints = new ArrayList<>(ring.tokens.keySet());
            InetAddressAndPort node = endpoints.get(random.nextInt(endpoints.size()));
            List<Token> current = new ArrayList<>(ring.tokens.get(node));
            Collections.shuffle(current, random);
            List<Token> kept = current.subList(0, 1 + random.nextInt(current.size()));

            RangesAtEndpoint before = ring.strategy.getAddressReplicas(ring.metadata, node);
            RangesAtEndpoint after = ring.strategy.getPendingAddressRanges(ring.metadata, kept, node);
            Pair<RangesAtEndpoint, RangesAtEndpoint> expected = RangeRelocator.calculateStreamAndFetchRanges(before, after);
            assertThat(normalized(RangeRelocator.notCovered(before, after))).isEqualTo(normalized(expected.left));
            assertThat(normalized(RangeRelocator.notCovered(after, before))).isEqualTo(normalized(expected.right));
            // a shrink never fetches
            assertThat(RangeRelocator.notCovered(after, before)).isEmpty();
        }
    }

    @Test
    public void testPartialOverlaps() throws UnknownHostException
    {
        // arbitrary ranges, not only merges: the covered parts are subtracted
        Random random = new Random(32);
        for (int iteration = 0; iteration < 200; iteration++)
        {
            Ring a = new Ring(random, 2, 1, 1 + random.nextInt(10), 1);
            Ring b = new Ring(random, 2, 1, 1 + random.nextInt(10), 1);
            InetAddressAndPort node = a.tokens.keySet().iterator().next();
            RangesAtEndpoint src = a.strategy.getAddressReplicas(a.metadata, node);
            RangesAtEndpoint dst = b.strategy.getAddressReplicas(b.metadata, node);
            Pair<RangesAtEndpoint, RangesAtEndpoint> expected = RangeRelocator.calculateStreamAndFetchRanges(src, dst);
            assertThat(normalized(RangeRelocator.notCovered(src, dst))).isEqualTo(normalized(expected.left));
            assertThat(normalized(RangeRelocator.notCovered(dst, src))).isEqualTo(normalized(expected.right));
        }
    }

    /** The lab ring that made the pairwise computation take minutes per keyspace. */
    @Test
    public void testLargeRing() throws UnknownHostException
    {
        Ring ring = new Ring(new Random(33), 3, 3, 256, 2);
        InetAddressAndPort node = ring.tokens.keySet().iterator().next();
        List<Token> kept = ring.tokens.get(node).subList(0, 128);
        RangesAtEndpoint before = ring.strategy.getAddressReplicas(ring.metadata, node);
        RangesAtEndpoint after = ring.strategy.getPendingAddressRanges(ring.metadata, kept, node);
        long start = System.nanoTime();
        RangesAtEndpoint toStream = RangeRelocator.notCovered(before, after);
        RangesAtEndpoint toFetch = RangeRelocator.notCovered(after, before);
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(millis).as("%s ms", millis).isLessThan(1000);
        assertThat(toStream).isNotEmpty();
        assertThat(toFetch).isEmpty();
    }
}
