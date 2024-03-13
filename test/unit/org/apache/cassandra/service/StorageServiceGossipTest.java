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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.SystemReplicas;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService.LeavingReplica;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// checkstyle: suppress below 'blockSystemPropertyUsage'

/*
 * This class contains tests that were originally added to StorageServiceGossipTest in CC tickets. In C* 5.0 
 * testScheduledExecutorsShutdownOnDrain was added that calls StorageService.instance.drain(), which also shuts
 * down the executor used by the gossip stage and is needed for some of these CC tests. Not finding a way to
 * make these tests work after drain is called, it was easier to move them to a separate class.
 */
public class StorageServiceGossipTest
{
    public static final String KEYSPACE = "StorageServiceGossipTest";
    public static final String COLUMN_FAMILY = "StorageServiceGossipTestColumnFamily";
    static InetAddressAndPort aAddress;
    static InetAddressAndPort bAddress;
    static InetAddressAndPort cAddress;
    static InetAddressAndPort dAddress;
    static InetAddressAndPort eAddress;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        aAddress = InetAddressAndPort.getByName("127.0.0.1");
        bAddress = InetAddressAndPort.getByName("127.0.0.2");
        cAddress = InetAddressAndPort.getByName("127.0.0.3");
        dAddress = InetAddressAndPort.getByName("127.0.0.4");
        eAddress = InetAddressAndPort.getByName("127.0.0.5");

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.local(),
                                    SchemaLoader.standardCFMD(KEYSPACE, COLUMN_FAMILY));
    }

    private static final Token threeToken = new RandomPartitioner.BigIntegerToken("3");
    private static final Token sixToken = new RandomPartitioner.BigIntegerToken("6");
    private static final Token nineToken = new RandomPartitioner.BigIntegerToken("9");
    private static final Token elevenToken = new RandomPartitioner.BigIntegerToken("11");
    private static final Token oneToken = new RandomPartitioner.BigIntegerToken("1");

    Range<Token> aRange = new Range<>(oneToken, threeToken);
    Range<Token> bRange = new Range<>(threeToken, sixToken);
    Range<Token> cRange = new Range<>(sixToken, nineToken);
    Range<Token> dRange = new Range<>(nineToken, elevenToken);
    Range<Token> eRange = new Range<>(elevenToken, oneToken);

    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                return "R1";
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "DC1";
            }
        };

        DatabaseDescriptor.setEndpointSnitch(snitch);

        CommitLog.instance.start();
        Nodes.peers().get().forEach(p -> Nodes.peers().remove(p.getPeerAddressAndPort(), false, true));
    }

    private AbstractReplicationStrategy simpleStrategy(TokenMetadata tmd)
    {
        return new SimpleStrategy("MoveTransientTest",
                                  tmd,
                                  DatabaseDescriptor.getEndpointSnitch(),
                                  com.google.common.collect.ImmutableMap.of("replication_factor", "3/1"));
    }

    @Test
    public void testSourceReplicasIsEmptyWithDeadNodes()
    {
        RandomPartitioner partitioner = new RandomPartitioner();
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(threeToken, aAddress);
        Util.joinNodeToRing(aAddress, threeToken, partitioner);
        tmd.updateNormalToken(sixToken, bAddress);
        Util.joinNodeToRing(bAddress, sixToken, partitioner);
        tmd.updateNormalToken(nineToken, cAddress);
        Util.joinNodeToRing(cAddress, nineToken, partitioner);
        tmd.updateNormalToken(elevenToken, dAddress);
        Util.joinNodeToRing(dAddress, elevenToken, partitioner);
        tmd.updateNormalToken(oneToken, eAddress);
        Util.joinNodeToRing(eAddress, oneToken, partitioner);

        AbstractReplicationStrategy strat = simpleStrategy(tmd);
        EndpointsByRange rangeReplicas = strat.getRangeAddresses(tmd);

        Replica leaving = new Replica(aAddress, aRange, true);
        Replica ourReplica = new Replica(cAddress, cRange, true);
        Set<LeavingReplica> leavingReplicas = Stream.of(new LeavingReplica(leaving, ourReplica)).collect(Collectors.toCollection(HashSet::new));

        // Mark the leaving replica as dead as well as the potential replica
        Util.markNodeAsDead(aAddress);
        Util.markNodeAsDead(bAddress);

        Multimap<InetAddressAndPort, RangeStreamer.FetchReplica> result = StorageService.instance.findLiveReplicasForRanges(leavingReplicas, rangeReplicas, cAddress);
        assertTrue("Replica set should be empty since replicas are dead", result.isEmpty());
    }

    @Test
    public void testStreamCandidatesDontIncludeDeadNodes()
    {
        List<InetAddressAndPort> endpoints = Arrays.asList(aAddress, bAddress);

        RandomPartitioner partitioner = new RandomPartitioner();
        Util.joinNodeToRing(aAddress, threeToken, partitioner);
        Util.joinNodeToRing(bAddress, sixToken, partitioner);

        Replica liveReplica = SystemReplicas.getSystemReplica(aAddress);
        Replica deadReplica = SystemReplicas.getSystemReplica(bAddress);
        Util.markNodeAsDead(bAddress);

        EndpointsForRange result = StorageService.getStreamCandidates(endpoints);
        assertTrue("Live node should be in replica list", result.contains(liveReplica));
        assertFalse("Dead node should not be in replica list", result.contains(deadReplica));
    }
}
