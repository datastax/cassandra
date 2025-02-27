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

package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.mockito.Mockito;

import static org.apache.cassandra.db.SystemKeyspace.CURRENT_VERSION;
import static org.apache.cassandra.gms.ApplicationState.RELEASE_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class GossiperTest
{
    static
    {
        CassandraRelevantProperties.CLUSTER_VERSION_PROVIDER_MIN_STABLE_DURATION.setLong(30000);
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    static final IPartitioner partitioner = new RandomPartitioner();
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = StorageService.instance.getTokenMetadata();
    ArrayList<Token> endpointTokens = new ArrayList<>();
    ArrayList<Token> keyTokens = new ArrayList<>();
    List<InetAddressAndPort> hosts = new ArrayList<>();
    List<UUID> hostIds = new ArrayList<>();

    private SeedProvider originalSeedProvider;

    final VersionedValueFactory factory = new VersionedValueFactory(null);


    @Before
    public void setup()
    {
        tmd.clearUnsafe();
        originalSeedProvider = DatabaseDescriptor.getSeedProvider();
        if (Gossiper.instance.isEnabled())
            Gossiper.instance.stop();
        Gossiper.instance.liveEndpoints.clear();
        Gossiper.instance.endpointStateMap.clear();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setSeedProvider(originalSeedProvider);
    }

    @AfterClass
    public static void afterClass()
    {
        Gossiper.instance.stop();
    }

    @Test
    public void testPaddingIntact() throws Exception
    {
        // sanity check that all 10 pads still exist
        assert ApplicationState.X1 == ApplicationState.X1;
        assert ApplicationState.X2 == ApplicationState.X2;
        assert ApplicationState.X3 == ApplicationState.X3;
        assert ApplicationState.X4 == ApplicationState.X4;
        assert ApplicationState.X5 == ApplicationState.X5;
        assert ApplicationState.X6 == ApplicationState.X6;
        assert ApplicationState.X7 == ApplicationState.X7;
        assert ApplicationState.X8 == ApplicationState.X8;
        assert ApplicationState.X9 == ApplicationState.X9;
        assert ApplicationState.X10 == ApplicationState.X10;
    }

    private void setLiveEndpoint(String address, String version) throws UnknownHostException
    {
        if (version != null)
        {
            EndpointState es = new EndpointState((HeartBeatState) null);
            es.addApplicationState(RELEASE_VERSION, factory.releaseVersion(version));
            Gossiper.instance.endpointStateMap.put(InetAddressAndPort.getByName(address), es);
        }
        else
        {
            Gossiper.instance.endpointStateMap.remove(InetAddressAndPort.getByName(address));
        }
        Gossiper.instance.liveEndpoints.add(InetAddressAndPort.getByName(address));
    }

    private void removeEndpoint(String address) throws UnknownHostException
    {
        Gossiper.instance.endpointStateMap.remove(InetAddressAndPort.getByName(address));
        Gossiper.instance.liveEndpoints.remove(InetAddressAndPort.getByName(address));
    }

    @Test
    public void testHasVersion3Nodes()
    {
        IClusterVersionProvider cvp = Mockito.mock(IClusterVersionProvider.class);
        Gossiper g = new Gossiper(false, cvp);

        when(cvp.isUpgradeInProgress()).thenReturn(false);
        when(cvp.getMinClusterVersion()).thenReturn(new CassandraVersion("3.0.0"));
        assertThat(g.getMinVersion()).isEqualTo(new CassandraVersion("3.0.0"));
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.11.0"))).isTrue();
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.0.0"))).isFalse();
        assertThat(g.hasMajorVersion3Nodes()).isTrue();

        when(cvp.isUpgradeInProgress()).thenReturn(true);
        when(cvp.getMinClusterVersion()).thenReturn(new CassandraVersion("3.0.0"));
        assertThat(g.getMinVersion()).isEqualTo(new CassandraVersion("3.0.0"));
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.11.0"))).isTrue();
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.0.0"))).isFalse();
        assertThat(g.hasMajorVersion3Nodes()).isTrue();

        when(cvp.isUpgradeInProgress()).thenReturn(true);
        when(cvp.getMinClusterVersion()).thenReturn(new CassandraVersion("3.11.0"));
        assertThat(g.getMinVersion()).isEqualTo(new CassandraVersion("3.11.0"));
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.11.0"))).isFalse();
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.0.0"))).isFalse();
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("4.0.0"))).isTrue();
        assertThat(g.hasMajorVersion3Nodes()).isTrue();

        when(cvp.isUpgradeInProgress()).thenReturn(true);
        when(cvp.getMinClusterVersion()).thenReturn(new CassandraVersion(CURRENT_VERSION.toString()));
        assertThat(g.getMinVersion()).isEqualTo(CassandraVersion.NULL_VERSION);
        assertThat(g.isUpgradingFromVersionLowerThan(new CassandraVersion("3.0.0"))).isTrue();
        assertThat(g.hasMajorVersion3Nodes()).isTrue();
    }

    @Test
    public void testDefaultClusterVersionProvider() throws UnknownHostException
    {
        Gossiper g = Gossiper.instance;
        g.stop();
        g.liveEndpoints.clear();
        g.endpointStateMap.clear();
        IClusterVersionProvider cvp = g.clusterVersionProvider;

        cvp.reset();
        assertThat(cvp.getMinClusterVersion()).isEqualTo(CURRENT_VERSION);
        assertThat(cvp.isUpgradeInProgress()).isTrue();

        g.start(0);

        cvp.reset();
        assertThat(cvp.getMinClusterVersion()).isEqualTo(CURRENT_VERSION);
        assertThat(cvp.isUpgradeInProgress()).isTrue();

        g.setNotUpgradingSinceMillisUnsafe(System.currentTimeMillis() - CassandraRelevantProperties.CLUSTER_VERSION_PROVIDER_MIN_STABLE_DURATION.getLong() - 1);
        assertThat(cvp.getMinClusterVersion()).isEqualTo(CURRENT_VERSION);
        assertThat(cvp.isUpgradeInProgress()).isFalse();

        // set one version missing
        cvp.reset();
        setLiveEndpoint("127.0.0.1", CURRENT_VERSION.toString());
        setLiveEndpoint("127.0.0.2", null);
        assertThat(cvp.getMinClusterVersion()).isEqualTo(CURRENT_VERSION);
        assertThat(cvp.isUpgradeInProgress()).isTrue();

        // set one version lower
        cvp.reset();
        setLiveEndpoint("127.0.0.1", CURRENT_VERSION.toString());
        setLiveEndpoint("127.0.0.2", "3.0.0");
        assertThat(cvp.getMinClusterVersion()).isEqualTo(new CassandraVersion("3.0.0"));
        assertThat(cvp.isUpgradeInProgress()).isTrue();
    }

    @Test
    public void testLargeGenerationJump() throws UnknownHostException, InterruptedException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);

            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

            //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration() + Gossiper.MAX_GENERATION_DIFFERENCE + 1);
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

            //The generation should have been updated because it isn't over Gossiper.MAX_GENERATION_DIFFERENCE in the future
            HeartBeatState actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());

            //Propose a generation 10 years in the future - this should be rejected.
            HeartBeatState badProposedRemoteHeartBeat = new HeartBeatState((int) (System.currentTimeMillis() / 1000) + Gossiper.MAX_GENERATION_DIFFERENCE * 10);
            EndpointState badProposedRemoteState = new EndpointState(badProposedRemoteHeartBeat);

            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, badProposedRemoteState));

            actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();

            //The generation should not have been updated because it is over Gossiper.MAX_GENERATION_DIFFERENCE in the future
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
        }
    }

    int stateChangedNum = 0;

    @Test
    public void testDuplicatedStateUpdate() throws Exception
    {
        VersionedValueFactory valueFactory =
        new VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        SimpleStateChangeListener stateChangeListener = null;
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);

            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

            //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration());
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

            final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
            VersionedValue tokensValue = valueFactory.tokens(Collections.singletonList(token));
            proposedRemoteState.addApplicationState(ApplicationState.TOKENS, tokensValue);

            stateChangeListener = new SimpleStateChangeListener();
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                assertEquals(ApplicationState.TOKENS, onChangeParams.state);
                stateChangedNum++;
            });
            Gossiper.instance.register(stateChangeListener);

            stateChangedNum = 0;
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));
            assertEquals(1, stateChangedNum);

            HeartBeatState actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());

            // Clone a new HeartBeatState
            proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration(), proposedRemoteHeartBeat.getHeartBeatVersion());
            proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

            // Bump the heartbeat version and use the same TOKENS state
            proposedRemoteHeartBeat.updateHeartBeat();
            proposedRemoteState.addApplicationState(ApplicationState.TOKENS, tokensValue);

            // The following state change should only update heartbeat without updating the TOKENS state
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));
            assertEquals(1, stateChangedNum);

            actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
            assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
            if (stateChangeListener != null)
                Gossiper.instance.unregister(stateChangeListener);
        }
    }

    // Note: This test might fail if for some reason the node broadcast address is in 127.99.0.0/16
    @Test
    public void testReloadSeeds() throws UnknownHostException
    {
        Gossiper gossiper = new Gossiper(false);
        List<String> loadedList;

        // Initialize the seed list directly to a known set to start with
        gossiper.seeds.clear();
        InetAddressAndPort addr = InetAddressAndPort.getByAddress(InetAddress.getByName("127.99.1.1"));
        int nextSize = 4;
        List<InetAddressAndPort> nextSeeds = new ArrayList<>(nextSize);
        for (int i = 0; i < nextSize; i ++)
        {
            gossiper.seeds.add(addr);
            nextSeeds.add(addr);
            addr = InetAddressAndPort.getByAddress(InetAddresses.increment(addr.address));
        }
        Assert.assertEquals(nextSize, gossiper.seeds.size());

        // Add another unique address to the list
        addr = InetAddressAndPort.getByAddress(InetAddresses.increment(addr.address));
        nextSeeds.add(addr);
        nextSize++;
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(nextSeeds));
        loadedList = gossiper.reloadSeeds();

        // Check that the new entry was added
        Assert.assertEquals(nextSize, loadedList.size());
        for (InetAddressAndPort a : nextSeeds)
            assertTrue(loadedList.contains(a.toString()));

        // Check that the return value of the reloadSeeds matches the content of the getSeeds call
        // and that they both match the internal contents of the Gossiper seeds list
        Assert.assertEquals(loadedList.size(), gossiper.getSeeds().size());
        for (InetAddressAndPort a : gossiper.seeds)
        {
            assertTrue(loadedList.contains(a.toString()));
            assertTrue(gossiper.getSeeds().contains(a.toString()));
        }

        // Add a duplicate of the last address to the seed provider list
        int uniqueSize = nextSize;
        nextSeeds.add(addr);
        nextSize++;
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(nextSeeds));
        loadedList = gossiper.reloadSeeds();

        // Check that the number of seed nodes reported hasn't increased
        Assert.assertEquals(uniqueSize, loadedList.size());
        for (InetAddressAndPort a : nextSeeds)
            assertTrue(loadedList.contains(a.toString()));

        // Create a new list that has no overlaps with the previous list
        addr = InetAddressAndPort.getByAddress(InetAddress.getByName("127.99.2.1"));
        int disjointSize = 3;
        List<InetAddressAndPort> disjointSeeds = new ArrayList<>(disjointSize);
        for (int i = 0; i < disjointSize; i ++)
        {
            disjointSeeds.add(addr);
            addr = InetAddressAndPort.getByAddress(InetAddresses.increment(addr.address));
        }
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(disjointSeeds));
        loadedList = gossiper.reloadSeeds();

        // Check that the list now contains exactly the new other list.
        Assert.assertEquals(disjointSize, gossiper.getSeeds().size());
        Assert.assertEquals(disjointSize, loadedList.size());
        for (InetAddressAndPort a : disjointSeeds)
        {
            assertTrue(gossiper.getSeeds().contains(a.toString()));
            assertTrue(loadedList.contains(a.toString()));
        }

        // Set the seed node provider to return an empty list
        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(new ArrayList<InetAddressAndPort>()));
        loadedList = gossiper.reloadSeeds();

        // Check that the in memory seed node list was not modified
        Assert.assertEquals(disjointSize, loadedList.size());
        for (InetAddressAndPort a : disjointSeeds)
            assertTrue(loadedList.contains(a.toString()));

        // Change the seed provider to one that throws an unchecked exception
        DatabaseDescriptor.setSeedProvider(new ErrorSeedProvider());
        loadedList = gossiper.reloadSeeds();

        // Check for the expected null response from a reload error
        assertNull(loadedList);

        // Check that the in memory seed node list was not modified and the exception was caught
        Assert.assertEquals(disjointSize, gossiper.getSeeds().size());
        for (InetAddressAndPort a : disjointSeeds)
            assertTrue(gossiper.getSeeds().contains(a.toString()));
    }

    @Test
    public void testNotFireDuplicatedNotificationsWithUpdateContainsOldAndNewState() throws UnknownHostException
    {
        VersionedValueFactory valueFactory =
        new VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        SimpleStateChangeListener stateChangeListener = null;
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);
            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();
            //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            // Test begins
            AtomicInteger notificationCount = new AtomicInteger(0);
            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration());
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);
            final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
            proposedRemoteState.addApplicationState(ApplicationState.STATUS, valueFactory.normal(Collections.singletonList(token)));

            stateChangeListener = new SimpleStateChangeListener();
            Gossiper.instance.register(stateChangeListener);

            // STEP 1. register verifier and apply state with just STATUS
            // simulate applying gossip state from a v3 peer
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                notificationCount.getAndIncrement();
                assertEquals("It should fire notification for STATUS when gossiper local state not yet has STATUS_WITH_PORT",
                             ApplicationState.STATUS, onChangeParams.state);
            });
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

            // STEP 2. includes both STATUS and STATUS_WITH_PORT. The gossiper knows that the remote peer is now in v4
            // update verifier and apply state again
            proposedRemoteState.addApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(Collections.singletonList(token)));
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                notificationCount.getAndIncrement();
                assertEquals("It should only fire notification for STATUS_WITH_PORT",
                             ApplicationState.STATUS_WITH_PORT, onChangeParams.state);
            });
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

            // STEP 3. somehow, the peer send only the STATUS in the update.
            proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);
            proposedRemoteState.addApplicationState(ApplicationState.STATUS, valueFactory.normal(Collections.singletonList(token)));
            stateChangeListener.setOnChangeVerifier(onChangeParams -> {
                notificationCount.getAndIncrement();
                fail("It should not fire notification for STATUS");
            });

            assertEquals("Expect exact 2 notifications with the test setup",
                         2, notificationCount.get());
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
            if (stateChangeListener != null)
                Gossiper.instance.unregister(stateChangeListener);
        }
    }

    static class SimpleStateChangeListener implements IEndpointStateChangeSubscriber
    {
        static class OnChangeParams
        {
            InetAddressAndPort endpoint;
            ApplicationState state;
            VersionedValue value;

            OnChangeParams(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
            {
                this.endpoint = endpoint;
                this.state = state;
                this.value = value;
            }
        }

        private volatile Consumer<OnChangeParams> onChangeVerifier;

        public void setOnChangeVerifier(Consumer<OnChangeParams> verifier)
        {
            onChangeVerifier = verifier;
        }

        public void onJoin(InetAddressAndPort endpoint, EndpointState epState) {}
        public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
        public void onAlive(InetAddressAndPort endpoint, EndpointState state) {}
        public void onDead(InetAddressAndPort endpoint, EndpointState state) {}
        public void onRemove(InetAddressAndPort endpoint) {}
        public void onRestart(InetAddressAndPort endpoint, EndpointState state) {}

        public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
        {
            onChangeVerifier.accept(new OnChangeParams(endpoint, state, value));
        }
    }

    static class TestSeedProvider implements SeedProvider
    {
        private List<InetAddressAndPort> seeds;

        TestSeedProvider(List<InetAddressAndPort> seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public List<InetAddressAndPort> getSeeds()
        {
            return seeds;
        }
    }

    // A seed provider for testing which throws assertion errors when queried
    static class ErrorSeedProvider implements SeedProvider
    {
        @Override
        public List<InetAddressAndPort> getSeeds()
        {
            assert(false);
            return new ArrayList<>();
        }
    }

    @Test
    public void testShutdownMarksNodeAsDead() throws UnknownHostException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        try
        {
            InetAddressAndPort remoteHostAddress = hosts.get(1);

            EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
            HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

            // Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
            assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

            VersionedValueFactory factory = new VersionedValueFactory(null);
            HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration() + Gossiper.MAX_GENERATION_DIFFERENCE + 1);
            EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);
            proposedRemoteState.addApplicationState(ApplicationState.STATUS_WITH_PORT, factory.shutdown(true));

            assertTrue(Gossiper.instance.isAlive(remoteHostAddress));
            Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));
            assertFalse(Gossiper.instance.isAlive(remoteHostAddress));
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.endpointStateMap.clear();
        }

    }
}
