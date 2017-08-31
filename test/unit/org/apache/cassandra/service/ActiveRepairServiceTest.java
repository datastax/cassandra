/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.LocalAntiCompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.Util.getFullRange;
import static org.apache.cassandra.Util.getRange;
import static org.apache.cassandra.Util.writeSSTable;
import static org.apache.cassandra.Util.writeSSTableWithTombstones;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ActiveRepairServiceTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    public InetAddress LOCAL, REMOTE;

    private boolean initialized;
    private Set<SSTableReader> toCleanup = new HashSet<>();
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE5,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_COUNTER),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_STANDARD1));
    }

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            SchemaLoader.startGossiper();
            initialized = true;

            LOCAL = FBUtilities.getBroadcastAddress();
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddress.getByName("127.0.0.2");
        }

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setTokens(Collections.singleton(tmd.partitioner.getRandomToken()));
        tmd.updateNormalToken(tmd.partitioner.getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);
        StorageService.instance.forceTerminateAllRepairSessions();
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        // Make sure to wipe all sstables forcefully removed from the tracker
        store.addSSTables(Sets.difference(toCleanup, store.getLiveSSTables()));
        toCleanup.clear();
        store.truncateBlocking();
        store = keyspace.getColumnFamilyStore(CF_COUNTER);
        store.truncateBlocking();
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsPlusOneInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInSpecifiedHosts() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the hosts are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        List<InetAddress> expected = new ArrayList<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }

        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<String> hosts = Arrays.asList(FBUtilities.getBroadcastAddress().getCanonicalHostName(), expected.get(0).getCanonicalHostName());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);

        assertEquals(expected.get(0), ActiveRepairService.getNeighbors(KEYSPACE5, ranges,
                                                                       ranges.iterator().next(),
                                                                       null, hosts).iterator().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborsSpecifiedHostsWithNoLocalHost() throws Throwable
    {
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        //Dont give local endpoint
        Collection<String> hosts = Arrays.asList("127.0.0.3");
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        ActiveRepairService.getNeighbors(KEYSPACE5, ranges, ranges.iterator().next(), null, hosts);
    }

    @Test
    public void testBuildAntiCompactionTask()
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = store.getLiveSSTables();

        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, 0, false);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);
        prs.markRepairing(store.metadata.cfId);

        //retrieve all sstable references from parent repair sessions
        LifecycleTransaction txn = prs.buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction();
        Set<SSTableReader> retrieved = Sets.newHashSet(txn.originals());
        assertEquals(original, retrieved);
        txn.abort();

        //remove 1 sstable from data data tracker
        Set<SSTableReader> newLiveSet = new HashSet<>(original);
        Iterator<SSTableReader> it = newLiveSet.iterator();
        final SSTableReader removed = it.next();
        it.remove();
        store.getTracker().dropSSTables(reader -> removed.equals(reader), OperationType.COMPACTION, null);

        //retrieve sstable references from parent repair session again - removed sstable must not be present
        txn = prs.buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction();
        retrieved = Sets.newHashSet(txn.originals());
        assertEquals(newLiveSet, retrieved);
        assertFalse(retrieved.contains(removed));
        txn.abort();
    }

    @Test
    public void testAddingMoreSSTables()
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, System.currentTimeMillis(), true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);
        prs.markRepairing(store.metadata.cfId);
        try (LifecycleTransaction txn = prs.buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction())
        {
            Set<SSTableReader> retrieved = Sets.newHashSet(txn.originals());
            assertEquals(original, retrieved);
        }
        createSSTables(store, 2);
        boolean exception = false;
        try
        {
            UUID newPrsId = UUID.randomUUID();
            ActiveRepairService.instance.registerParentRepairSession(newPrsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, System.currentTimeMillis(), true);
            ActiveRepairService.instance.getParentRepairSession(newPrsId).markRepairing(store.metadata.cfId);
        }
        catch (Throwable t)
        {
            exception = true;
        }
        assertTrue(exception);

        try (LifecycleTransaction txn = prs.buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction())
        {
            Set<SSTableReader> retrieved = Sets.newHashSet(txn.originals());
            assertEquals(original, retrieved);
        }
    }

    @Test
    public void testSnapshotAddSSTables() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        UUID prsId = UUID.randomUUID();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), true);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.cfId, prsId);

        UUID prsId2 = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId2, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), true);
        createSSTables(store, 2);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.cfId, prsId);
        try (LifecycleTransaction txn = ActiveRepairService.instance.getParentRepairSession(prsId).buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction())
        {
            assertEquals(original, Sets.newHashSet(txn.originals()));
        }
        store.forceMajorCompaction();
        // after a major compaction the original sstables will be gone and we will have no sstables to anticompact:
        try (LifecycleTransaction txn = ActiveRepairService.instance.getParentRepairSession(prsId).buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction())
        {
            assertEquals(0, txn.originals().size());
        }
    }

    @Test
    public void testSnapshotMultipleRepairs()
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), true);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.cfId, prsId);

        UUID prsId2 = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId2, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), true);
        boolean exception = false;
        try
        {
            ActiveRepairService.instance.getParentRepairSession(prsId2).maybeSnapshot(store.metadata.cfId, prsId2);
        }
        catch (Throwable t)
        {
            exception = true;
        }
        assertTrue(exception);
        try (LifecycleTransaction txn = ActiveRepairService.instance.getParentRepairSession(prsId).buildAntiCompactionTask(store, getFullRange(store)).getLocalSSTablesTransaction())
        {
            assertEquals(original, Sets.newHashSet(txn.originals()));
        }
    }

    /**
     * Test for APOLLO-688
     */
    @Test
    public void testAntiCompactionTaskIsBuiltCorrectly() throws Exception
    {
        //Avoid taking snapshot multiple times on repair session creation
        boolean incrementalBackupsEnabled = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);

        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.disableAutoCompaction();

        SSTableReader localA = writeSSTable(cfs, cfs.metadata, 0, 3, 0, 1, 0);
        SSTableReader localB = writeSSTable(cfs, cfs.metadata, 3, 6, 0, 1, 1);
        SSTableReader localC = writeSSTable(cfs, cfs.metadata, 6, 9, 0, 1, 2);
        SSTableReader localDShadowsLocalAnB = writeSSTableWithTombstones(cfs, cfs.metadata, 0, 6, 0, 1,3 );
        SSTableReader localENonShadowableByRange = writeSSTable(cfs, cfs.metadata, 12, 15, 0, 1, 4);
        SSTableReader localFNonShadowableByTimestamp = writeSSTable(cfs, cfs.metadata, 0, 9, 0, 1, 10);
        SSTableReader remoteAShadowsLocalAnD = writeSSTableWithTombstones(cfs, cfs.metadata, 0, 3, 0, 1, 5);
        SSTableReader remoteBShadowsLocalBnD = writeSSTableWithTombstones(cfs, cfs.metadata, 3, 6, 0, 1, 6);
        SSTableReader remoteCShadowsLocalC = writeSSTableWithTombstones(cfs, cfs.metadata, 6, 12, 0, 1, 7);
        SSTableReader remoteDWithoutOverlappingTombstone = writeSSTableWithTombstones(cfs, cfs.metadata, 15, 20, 0, 1, 8);
        SSTableReader remoteEWithoutTombstone = writeSSTable(cfs, cfs.metadata, 0, 9, 0, 3, 9);

        Set<SSTableReader> localSSTables = Sets.newHashSet(localA, //1
                                                           localB, //2
                                                           localC, //3
                                                           localDShadowsLocalAnB, //4
                                                           localENonShadowableByRange, //5
                                                           localFNonShadowableByTimestamp); //6

        Set<SSTableReader> remoteWithTombstones = Sets.newHashSet(remoteAShadowsLocalAnD, //7
                                                                 remoteBShadowsLocalBnD, //8
                                                                 remoteCShadowsLocalC, //9
                                                                 remoteDWithoutOverlappingTombstone); //10
        Set<SSTableReader> remoteWithoutTombstone = Sets.newHashSet(remoteEWithoutTombstone); //11
        Collection<Range<Token>> FULL_RANGE = getFullRange(cfs);

        // Case 1: no SSTables received or compacted during repair
        ActiveRepairService.ParentRepairSession prs = setupRepairSession(cfs, localSSTables, 1);
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        // Case 1: Check all SSTables should be repaired
        LocalAntiCompactionTask antiCompactionTask = prs.buildAntiCompactionTask(cfs, FULL_RANGE);
        verifyAntiCompactionTask(antiCompactionTask, localSSTables, Collections.emptySet(), Collections.emptySet(), FULL_RANGE);
        cleanup(antiCompactionTask, cfs, prs);

        // Case 2: no SSTables received - local A compacted during repair
        prs = setupRepairSession(cfs, localSSTables, 2);
        simulateCompaction(cfs, localA);
        // Case 2: Check all SSTables should be repaired, except D which shadows A
        antiCompactionTask = prs.buildAntiCompactionTask(cfs, FULL_RANGE);
        verifyAntiCompactionTask(antiCompactionTask, Sets.difference(localSSTables, Collections.singleton(localA)),
                                 Collections.emptySet(), Collections.singleton(localDShadowsLocalAnB), FULL_RANGE);
        cleanup(antiCompactionTask, cfs, prs);

        // Case 3: remote SSTables received and no local SSTables compacted
        prs = setupRepairSession(cfs, localSSTables, 3);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstone, prs.repairedAt);
        // Case 3: Check all SSTables should be repaired, including remote with tombstones
        antiCompactionTask = prs.buildAntiCompactionTask(cfs, FULL_RANGE);
        verifyAntiCompactionTask(antiCompactionTask, localSSTables, remoteWithTombstones, Collections.emptySet(), FULL_RANGE);
        cleanup(antiCompactionTask, cfs, prs);

        // Case 4: remote SSTables received and localE and localF non shadowable SSTables compacted
        // Local: Anti-compact all non-compacted
        // Remote: Mark all overlapping with tombstone as repaired
        prs = setupRepairSession(cfs, localSSTables, 4);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstone, prs.repairedAt);
        simulateCompaction(cfs, localENonShadowableByRange);
        simulateCompaction(cfs, localFNonShadowableByTimestamp);
        // Case 4: Check all SSTables should be repaired, including remote with tombstones
        antiCompactionTask = prs.buildAntiCompactionTask(cfs, FULL_RANGE);
        verifyAntiCompactionTask(antiCompactionTask, Sets.difference(localSSTables, Sets.newHashSet(localENonShadowableByRange, localFNonShadowableByTimestamp)),
                                 remoteWithTombstones, Collections.emptySet(), FULL_RANGE);
        cleanup(antiCompactionTask, cfs, prs);

        // Case 5: remote SSTables received and local SSTable D is compacted
        prs = setupRepairSession(cfs, localSSTables, 5);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstone, prs.repairedAt);
        simulateCompaction(cfs, localDShadowsLocalAnB);
        // Case 5: verify that remote A and remote B are on unrepaired set, since they shadow local D
        antiCompactionTask = prs.buildAntiCompactionTask(cfs, FULL_RANGE);
        verifyAntiCompactionTask(antiCompactionTask, Sets.difference(localSSTables, Collections.singleton(localDShadowsLocalAnB)), remoteWithTombstones, Sets.newHashSet(remoteAShadowsLocalAnD, remoteBShadowsLocalBnD), FULL_RANGE);
        cleanup(antiCompactionTask, cfs, prs);

        // Case 6: remote SSTables received and local SSTable B is compacted
        // Local: Do not anti-compact local D which shadows local B
        // Remote: Do not mark remote A and B as repaired, since both shadow D
        prs = setupRepairSession(cfs, localSSTables, 6);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstone, prs.repairedAt);
        simulateCompaction(cfs, localB);
        // Case 6: verify that localD is not repaired because it shadows localB, and remoteA and remoteB because they shadow localD
        antiCompactionTask = prs.buildAntiCompactionTask(cfs, FULL_RANGE);
        verifyAntiCompactionTask(antiCompactionTask, Sets.difference(localSSTables, Collections.singleton(localB)), remoteWithTombstones, Sets.newHashSet(localDShadowsLocalAnB, remoteAShadowsLocalAnD, remoteBShadowsLocalBnD), FULL_RANGE);
        cleanup(antiCompactionTask, cfs, prs);

        // Case 7: remote SSTables received and local SSTable C is compacted
        List<Range<Token>> ranges = Lists.newArrayList(getRange(0,1), getRange(1,2));
        SSTableReader remoteEShadowsRemoteC = writeSSTableWithTombstones(cfs, cfs.metadata, 9, 12, 0, 1, 10);
        prs = setupRepairSession(cfs, localSSTables, 7);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, Collections.singleton(remoteEShadowsRemoteC), Collections.emptySet(), prs.repairedAt);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstone, prs.repairedAt);
        remoteWithTombstones.add(remoteEShadowsRemoteC);
        simulateCompaction(cfs, localC);
        // Case 7: verify that remoteC is not repaired because it shadows localC, and remoteE because it shadows remoteC
        antiCompactionTask = prs.buildAntiCompactionTask(cfs, ranges);
        verifyAntiCompactionTask(antiCompactionTask, Sets.difference(localSSTables, Collections.singleton(localC)),
                                 remoteWithTombstones, Sets.newHashSet(remoteCShadowsLocalC, remoteEShadowsRemoteC), ranges);
        cleanup(antiCompactionTask, cfs, prs);

        DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackupsEnabled);
    }

    /**
     * Test for APOLLO-688
     */
    @Test
    public void testResourcesAreReleasedAfterFinishedRepairSession() throws Exception
    {
        boolean incrementalBackupsEnabled = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);

        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.disableAutoCompaction();

        SSTableReader localA = writeSSTable(cfs, cfs.metadata, 2, 5, 0, 1, 0);
        SSTableReader localB = writeSSTable(cfs, cfs.metadata, 3, 6, 0, 1, 1);
        SSTableReader localC = writeSSTable(cfs, cfs.metadata, 2, 4, 0, 1, 2);
        SSTableReader localD = writeSSTable(cfs, cfs.metadata, 0, 2, 0, 1, 3);
        SSTableReader remoteA = writeSSTableWithTombstones(cfs, cfs.metadata, 1, 4, 0, 1, 4);
        SSTableReader remoteB = writeSSTableWithTombstones(cfs, cfs.metadata, 0, 2, 0, 1, 5);
        SSTableReader remoteC = writeSSTableWithTombstones(cfs, cfs.metadata, 7, 9, 0, 1,6 );
        SSTableReader remoteD = writeSSTable(cfs, cfs.metadata, 0, 6, 0, 3, 7);

        Set<SSTableReader> localSSTables = Sets.newHashSet(localA,
                                                           localB,
                                                           localC,
                                                           localD);

        Set<SSTableReader> remoteWithTombstones = Sets.newHashSet(remoteA,
                                                                  remoteB,
                                                                  remoteC);

        Set<SSTableReader> remoteWithoutTombstones = Sets.newHashSet(remoteD);

        // Case 1 - check resources are released on aborted repair session

        // Create PRS with remote SStables
        final ActiveRepairService.ParentRepairSession prs1 = setupRepairSession(cfs, localSSTables, 1);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstones, prs1.repairedAt);
        Set<LifecycleTransaction> remoteTxns = prs1.remoteCompactingSSTables.get(cfs.metadata.cfId);

        // Abort parent repair session
        ActiveRepairService.instance.terminateSessions();

        // Check remote transactions were aborted and sstables are no longer marked as compacting
        assertTrue(remoteTxns.stream().allMatch(s -> s.state() == Transactional.AbstractTransactional.State.ABORTED));
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(prs1.remoteCompactingSSTables.isEmpty());

        // Check remote sstables are still present on tracker
        assertTrue(cfs.getLiveSSTables().containsAll(remoteWithTombstones));

        // Check that local and remote with tombstones were not marked repaired
        assertRepaired(Sets.union(localSSTables, remoteWithTombstones), ActiveRepairService.UNREPAIRED_SSTABLE);

        // Check PRS unsubscribed from tracker
        assertFalse(cfs.getTracker().getSubscribers().contains(prs1));
        // Check PRS is no longer present
        try
        {
            ActiveRepairService.instance.getParentRepairSession(prs1.parentSessionId);
            fail("should have thrown RuntimeException");
        } catch (RuntimeException e)
        {
            //Expected
        }

        // Cleanup
        removeUnsafe(cfs);

        // Case 2 - check resources are released after successful anti-compaction
        ActiveRepairService.ParentRepairSession prs2 = setupRepairSession(cfs, localSSTables, 2);
        simulateReceivedRepairedSSTablesFromStreaming(cfs, remoteWithTombstones, remoteWithoutTombstones, prs2.repairedAt);
        remoteTxns = prs2.remoteCompactingSSTables.get(cfs.metadata.cfId);

        ListenableFuture<List<Object>> future = prs2.doAntiCompaction(getFullRange(cfs));

        // Wait for anti-compaction to finish (get is not sufficient because session is cleared as a callback)
        CountDownLatch countDownLatch = new CountDownLatch(1);
        future.addListener(() -> countDownLatch.countDown(), MoreExecutors.directExecutor());
        countDownLatch.await();

        //Check remote transactions were aborted and sstables are no longer marked as compacting
        assertTrue(remoteTxns.stream().allMatch(s -> s.state() == Transactional.AbstractTransactional.State.ABORTED));
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(prs1.remoteCompactingSSTables.isEmpty());

        // Check remote sstables are still present on tracker
        assertTrue(cfs.getLiveSSTables().containsAll(remoteWithTombstones));
        // Check all sstables are marked repaired after anti-compaction
        assertRepaired(Sets.union(Sets.union(localSSTables, remoteWithTombstones), remoteWithoutTombstones), prs2.repairedAt);

        // Check PRS unsubscribed from tracker
        assertFalse(cfs.getTracker().getSubscribers().contains(prs1));
        // Check PRS is no longer present
        try
        {
            ActiveRepairService.instance.getParentRepairSession(prs1.parentSessionId);
            fail("should have thrown RuntimeException");
        } catch (RuntimeException e)
        {
            //expected
        }

        DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackupsEnabled);
    }

    private ActiveRepairService.ParentRepairSession setupRepairSession(ColumnFamilyStore cfs, Set<SSTableReader> localSSTables, long repairedAt) throws IOException
    {
        return setupRepairSession(cfs, localSSTables, repairedAt, getFullRange(cfs));
    }

    private ActiveRepairService.ParentRepairSession setupRepairSession(ColumnFamilyStore cfs, Set<SSTableReader> localSSTables, long repairedAt,
                                                                       Collection<Range<Token>> ranges) throws IOException
    {
        //make sure all SSTables are unrepaired before adding them to tracker
        cfs.mutateRepairedAt(localSSTables, ActiveRepairService.UNREPAIRED_SSTABLE, false);
        cfs.addSSTables(localSSTables);
        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(cfs),
                                                                 ranges,true, repairedAt, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);

        //Setup repair with original SSTables
        UUID cfId = cfs.metadata.cfId;
        prs.markRepairing(cfId);
        assertEquals(localSSTables.stream().map(s -> new ActiveRepairService.SSTableInfo(s)).collect(Collectors.toSet()), prs.validatedSSTables.get(cfId));
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        return prs;
    }

    private void cleanup(LocalAntiCompactionTask antiCompactionTask, ColumnFamilyStore cfs, ActiveRepairService.ParentRepairSession prs)
    {
        antiCompactionTask.getLocalSSTablesTransaction().abort(); //abort local transaction
        ActiveRepairService.instance.removeParentRepairSession(prs.parentSessionId); //this should abort remote transactions

        // Check PRS is no longer subscribed to tracker and compacting set is empty
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(prs.remoteCompactingSSTables.isEmpty());
        assertFalse(cfs.getTracker().getSubscribers().contains(prs));

        removeUnsafe(cfs);
    }

    private void simulateReceivedRepairedSSTablesFromStreaming(ColumnFamilyStore cfs, Set<SSTableReader> receivedWithTombstones,
                                                               Set<SSTableReader> receivedWithoutTombstones, long repairedAt) throws IOException
    {
        Set<SSTableReader> compactingBefore = Sets.newHashSet(cfs.getTracker().getCompacting());
        Set<SSTableReader> receivedSSTables = Sets.union(receivedWithoutTombstones, receivedWithTombstones);

        //mark received SSTables as repaired
        cfs.mutateRepairedAt(receivedSSTables, repairedAt);

        //Make sure notifySSTablesReceivedFromStreaming cannot be called *after* tracker.addSSTables
        try
        {
            cfs.getTracker().addSSTables(receivedSSTables);
            ActiveRepairService.instance.receiveStreamedSSTables(receivedSSTables);
            fail("Should have thrown exception");
        }
        catch (AssertionError e)
        {
            removeUnsafe(cfs, receivedSSTables);
        }

        ActiveRepairService.instance.receiveStreamedSSTables(receivedSSTables);
        //Check that only overlapping was marked as unrepaired
        assertRepaired(receivedWithTombstones, ActiveRepairService.UNREPAIRED_SSTABLE);
        assertRepaired(receivedWithoutTombstones, repairedAt);
        assertEquals(compactingBefore, cfs.getTracker().getCompacting());

        cfs.getTracker().addSSTables(receivedSSTables);
        //Check that only received with tombstones was marked as compacting
        assertEquals(Sets.union(compactingBefore, receivedWithTombstones), cfs.getTracker().getCompacting());
        assertTrue(cfs.getLiveSSTables().containsAll(receivedSSTables));
    }

    private void assertRepaired(Set<SSTableReader> repaired, long repairedAt)
    {
        for (SSTableReader reader : repaired)
        {
            assertEquals(repairedAt, reader.getSSTableMetadata().repairedAt);
        }
    }

    private void simulateCompaction(ColumnFamilyStore cfs, SSTableReader toBeCompacted)
    {
        removeUnsafe(cfs, Sets.newHashSet(toBeCompacted));
    }

    private void verifyAntiCompactionTask(LocalAntiCompactionTask antiCompactionTask, Set<SSTableReader> localNonCompacted,
                                          Set<SSTableReader> remoteWithTombstones, Set<SSTableReader> unrepaired, Collection<Range<Token>> ranges)
    {
        Set<SSTableReader> remoteToAntiCompact = antiCompactionTask.getRemoteSSTablesTransactions().stream().flatMap(s -> s.originals().stream()).collect(Collectors.toSet());
        assertEquals(remoteWithTombstones, remoteToAntiCompact);
        assertEquals(unrepaired, antiCompactionTask.getUnrepairedSet());
        assertEquals(localNonCompacted, antiCompactionTask.getLocalSSTablesTransaction().originals());
        assertEquals(ranges, antiCompactionTask.getSuccessfulRanges());
    }

    private Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<>();
        for (int i = 1; i <= max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(tmd.partitioner.getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.truncateBlocking();
        store.disableAutoCompaction();
        createSSTables(store, 10);
        return store;
    }

    private void createSSTables(ColumnFamilyStore cfs, int count)
    {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < count; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                new RowUpdateBuilder(cfs.metadata, timestamp, Integer.toString(j))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }
    }

    private void removeUnsafe(ColumnFamilyStore cfs)
    {
        removeUnsafe(cfs, cfs.getLiveSSTables());
    }

    private void removeUnsafe(ColumnFamilyStore cfs, Set<SSTableReader> sstables)
    {
        // Save sstable set for after-test cleanup
        toCleanup.addAll(sstables);
        // Remove SSTables from tracker
        cfs.getTracker().removeUnsafe(sstables);
    }
}
