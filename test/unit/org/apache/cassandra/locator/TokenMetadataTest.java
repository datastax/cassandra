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
package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.Util.token;


public class TokenMetadataTest
{
    public final static String ONE = "1";
    public final static String SIX = "6";

    static TokenMetadata tmd;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token(ONE), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(token(SIX), InetAddressAndPort.getByName("127.0.0.6"));
    }

    private static void testRingIterator(ArrayList<Token> ring, String start, boolean includeMin, String... expected)
    {
        ArrayList<Token> actual = new ArrayList<>();
        Iterators.addAll(actual, TokenMetadata.ringIterator(ring, token(start), includeMin));
        assertEquals(actual.toString(), expected.length, actual.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch at index " + i + ": " + actual, token(expected[i]), actual.get(i));
    }

    /**
     * This test is very likely (but not guaranteed) to fail if ring invalidations are ever allowed to interleave.
     */
    @Test
    public void testConcurrentInvalidation() throws InterruptedException
    {
        long startVersion = tmd.getRingVersion();

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
        
        int invalidations = 1024;
        
        for (int i = 0; i < invalidations; i++)
            pool.execute(() -> tmd.invalidateCachedRings());

        pool.shutdown();
        
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(invalidations + startVersion, tmd.getRingVersion());
    }

    @Test
    public void testRingIterator()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", false, "6", "1");
        testRingIterator(ring, "7", false, "1", "6");
        testRingIterator(ring, "0", false, "1", "6");
        testRingIterator(ring, "", false, "1", "6");
    }

    @Test
    public void testRingIteratorIncludeMin()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", true, "6", "", "1");
        testRingIterator(ring, "7", true, "", "1", "6");
        testRingIterator(ring, "0", true, "1", "6", "");
        testRingIterator(ring, "", true, "1", "6", "");
    }

    @Test
    public void testRingIteratorEmptyRing()
    {
        testRingIterator(new ArrayList<Token>(), "2", false);
    }

    @Test
    public void testTopologyUpdate_RackConsolidation() throws UnknownHostException
    {
        final InetAddressAndPort first = InetAddressAndPort.getByName("127.0.0.1");
        final InetAddressAndPort second = InetAddressAndPort.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddressAndPort> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, ImmutableMultimap<String, InetAddressAndPort>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }
        });

        tokenMetadata.updateTopology(first);
        topology = tokenMetadata.updateTopology(second);

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));
    }

    @Test
    public void testTopologyUpdate_RackExpansion() throws UnknownHostException
    {
        final InetAddressAndPort first = InetAddressAndPort.getByName("127.0.0.1");
        final InetAddressAndPort second = InetAddressAndPort.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddressAndPort> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, ImmutableMultimap<String, InetAddressAndPort>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }
        });

        topology = tokenMetadata.updateTopology();

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));
    }

    @Test
    public void testEndpointSizes() throws UnknownHostException
    {
        final InetAddressAndPort first = InetAddressAndPort.getByName("127.0.0.1");
        final InetAddressAndPort second = InetAddressAndPort.getByName("127.0.0.6");

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        tokenMetadata.updateHostId(UUID.randomUUID(), first);
        tokenMetadata.updateHostId(UUID.randomUUID(), second);

        assertEquals(2, tokenMetadata.getSizeOfAllEndpoints());
        assertEquals(0, tokenMetadata.getSizeOfLeavingEndpoints());
        assertEquals(0, tokenMetadata.getSizeOfMovingEndpoints());

        tokenMetadata.addLeavingEndpoint(first);
        assertEquals(1, tokenMetadata.getSizeOfLeavingEndpoints());

        tokenMetadata.removeEndpoint(first);
        assertEquals(0, tokenMetadata.getSizeOfLeavingEndpoints());
        assertEquals(1, tokenMetadata.getSizeOfAllEndpoints());

        tokenMetadata.addMovingEndpoint(token(SIX), second);
        assertEquals(1, tokenMetadata.getSizeOfMovingEndpoints());

        tokenMetadata.removeFromMoving(second);
        assertEquals(0, tokenMetadata.getSizeOfMovingEndpoints());

        tokenMetadata.removeEndpoint(second);
        assertEquals(0, tokenMetadata.getSizeOfAllEndpoints());
        assertEquals(0, tokenMetadata.getSizeOfLeavingEndpoints());
        assertEquals(0, tokenMetadata.getSizeOfMovingEndpoints());
    }

    @Test
    public void testUpdateAddressForNormalTokens() throws UnknownHostException
    {
        final InetAddressAndPort first = InetAddressAndPort.getByName("127.0.0.1");
        final InetAddressAndPort second = InetAddressAndPort.getByName("127.0.0.6");

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        assertEquals(tmd.getTokens(first).size(), 1);
        assertEquals(tmd.getTokens(first).iterator().next(), token(ONE));

        assertEquals(tmd.getTokens(second).size(), 1);
        assertEquals(tmd.getTokens(second).iterator().next(), token(SIX));

        InetAddressAndPort updatedNode = InetAddressAndPort.getByName("127.0.0.10");
        assertThatThrownBy(() -> tmd.updateAddressForNormalTokens(Collections.singleton(token(SIX)), first, updatedNode))
                          .hasMessageContaining("different set of tokens");
        tmd.updateAddressForNormalTokens(Collections.singleton(token(ONE)), first, updatedNode);

        assertEquals(tmd.getTokens(updatedNode).size(), 1);
        assertEquals(tmd.getTokens(updatedNode).iterator().next(), token(ONE));

        assertEquals(tmd.getTokens(second).size(), 1);
        assertEquals(tmd.getTokens(second).iterator().next(), token(SIX));
    }

    @Test
    public void testUpdateAddressForHostId() throws UnknownHostException
    {
        final InetAddressAndPort first = InetAddressAndPort.getByName("127.0.0.1");
        UUID firstId = UUID.randomUUID();

        final InetAddressAndPort second = InetAddressAndPort.getByName("127.0.0.6");
        UUID secondId = UUID.randomUUID();

        tmd.updateHostId(firstId, first);
        tmd.updateHostId(secondId, second);

        assertEquals(tmd.getHostId(first), firstId);
        assertEquals(tmd.getHostId(second), secondId);

        InetAddressAndPort updatedNode = InetAddressAndPort.getByName("127.0.0.10");
        tmd.updateAddressForHostId(secondId, second, updatedNode);

        assertEquals(tmd.getHostId(first), firstId);
        assertEquals(tmd.getHostId(updatedNode), secondId);
    }
}
