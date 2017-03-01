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
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ActiveRepairServiceTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    public String cfname;
    public InetAddress LOCAL, REMOTE;

    private boolean initialized;

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
        Collection<String> hosts = Arrays.asList(FBUtilities.getBroadcastAddress().getCanonicalHostName(),expected.get(0).getCanonicalHostName());
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

    Set<InetAddress> addTokens(int max) throws Throwable
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

    @Test(expected = RuntimeException.class)
    public void testNonGlobalRepairShouldThrowExceptionWhenTryingToFetchReferencedSSTables() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = store.getLiveSSTables();

        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, 0, false);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);

        Set<SSTableReader> uncompacting = Sets.newHashSet(store.getUncompactingSSTables());
        assertEquals(original, uncompacting);

        prs.getReferencedSSTables(store.metadata.cfId);
    }

    @Test
    public void testFinishIncrementalRepairFullRange() throws Exception
    {
        Set<Range<Token>> rangeToRepair = Collections.singleton(getRange(25, 50));
        testIncrementalRepair(rangeToRepair, rangeToRepair);
    }

    @Test
    public void testFinishIncrementalRepairPartialRange() throws Exception
    {
        Set<Range<Token>> rangesToRepair = Sets.newHashSet(getRange(0, 5), getRange(22, 27), getRange(35, 60));
        Set<Range<Token>> successfulRanges = Collections.singleton(getRange(0, 5));
        testIncrementalRepair(rangesToRepair, successfulRanges);
    }

    @Test
    public void testFinishIncrementalRepairNoSuccessfulRange() throws Exception
    {
        Set<Range<Token>> rangesToRepair = Sets.newHashSet(getRange(0, 5), getRange(22, 27), getRange(35, 60));
        testIncrementalRepair(rangesToRepair, Collections.EMPTY_SET);
    }

    private void testIncrementalRepair(Set<Range<Token>> rangesToRepair, Set<Range<Token>> successfullRanges) throws Exception
    {
        UUID sessionId = UUID.randomUUID();
        ColumnFamilyStore store = prepareColumnFamilyStore(false);
        long timestamp = System.currentTimeMillis();

        //this loop will populate the table with the following sstables/ranges
        //1: [00,10]
        //2: [10,20]
        //3: [20,30]
        //4: [30,40]
        //5: [40,50]
        for (int i = 0; i < 50; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                new RowUpdateBuilder(store.metadata, timestamp, String.format("%2d", i))
                .clustering(Integer.toString(j))
                .add("val", "val")
                .build()
                .applyUnsafe();
            }
            //Flush every 10 keys, so we have 5 sstables total
            if ((i+1) % 10 == 0)
            {
                store.forceBlockingFlush();
            }
        }

        int initialCount = store.getLiveSSTables().size();
        ActiveRepairService.instance.registerParentRepairSession(sessionId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store),
                                                                 rangesToRepair, true, timestamp, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionId);

        //retrieve all sstable references from parent repair sessions
        Refs<SSTableReader> refs = prs.getReferencedSSTables(store.metadata.cfId);
        LifecycleTransaction txn = prs.getTransaction(store.metadata.cfId);
        Set<SSTableReader> referenced = Sets.newHashSet(refs.iterator());

        long sstablesToRepair = store.getLiveSSTables().stream().filter(s -> s.intersects(rangesToRepair)).count();
        long repairedSSTables = store.getLiveSSTables().stream().filter(s -> s.intersects(successfullRanges)).count();

        assertEquals(sstablesToRepair, referenced.size());
        assertEquals(referenced, txn.originals());
        assertEquals(Transactional.AbstractTransactional.State.IN_PROGRESS, txn.state());

        //only half of the sstable are unavailable for compaction
        Set<SSTableReader> uncompacting = Sets.newHashSet(store.getUncompactingSSTables());
        assertEquals(initialCount - sstablesToRepair, uncompacting.size());
        for (SSTableReader reader : uncompacting)
        {
            assertFalse(reader.intersects(rangesToRepair));
        }

        ListenableFuture listenableFuture = ActiveRepairService.instance.finishParentSession(sessionId, Collections.EMPTY_SET,
                                                                                             successfullRanges);
        listenableFuture.get();

        //make sure transaction is finished and references are released
        assertEquals(successfullRanges.isEmpty()? Transactional.AbstractTransactional.State.ABORTED : Transactional.AbstractTransactional.State.COMMITTED, txn.state());
        assertEquals(0, refs.size());

        // check that the correct number of sstables were anticompacted
        assertEquals(repairedSSTables, store.getLiveSSTables().stream().mapToInt(s -> s.isRepaired() ? 1 : 0).sum());

        //all sstables must be available for compaction
        assertEquals(Sets.newHashSet(store.getLiveSSTables()), Sets.newHashSet(store.getUncompactingSSTables()));

        //read data just to make sure all rows are present
        for (int i = 0; i < 50; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                List<FilteredPartition> all = Util.getAll(Util.cmd(store, String.format("%2d", i)).includeRow(Integer.toString(j)).build());
                assertEquals(1, all.size());
                for (FilteredPartition partition : all)
                {
                    assertEquals(1, Sets.newHashSet(partition.iterator()).size());
                }
            }
        }
    }

    @Test
    public void testAddingMoreSSTables() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);
        prs.markSSTablesRepairing(store.metadata.cfId, prsId);
        try (Refs<SSTableReader> refs = prs.getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            Set<SSTableReader> retrieved = Sets.newHashSet(refs.iterator());
            assertEquals(original, retrieved);
        }
        createSSTables(store, 2);
        boolean exception = false;
        try
        {
            UUID newPrsId = UUID.randomUUID();
            ActiveRepairService.instance.registerParentRepairSession(newPrsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), null, true, System.currentTimeMillis(), true);
            ActiveRepairService.instance.getParentRepairSession(newPrsId).markSSTablesRepairing(store.metadata.cfId, newPrsId);
        }
        catch (Throwable t)
        {
            exception = true;
        }
        assertTrue(exception);

        try (Refs<SSTableReader> refs = prs.getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            Set<SSTableReader> retrieved = Sets.newHashSet(refs.iterator());
            assertEquals(original, retrieved);
        }
    }

    @Test
    public void testSnapshotAddSSTables() throws ExecutionException, InterruptedException, IOException
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        UUID prsId = UUID.randomUUID();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), false);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.cfId, prsId);

        UUID prsId2 = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId2, FBUtilities.getBroadcastAddress(), Collections.singletonList(store), getFullRange(store), true, System.currentTimeMillis(), false);
        createSSTables(store, 2);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.cfId, prsId);
        try (Refs<SSTableReader> refs = ActiveRepairService.instance.getParentRepairSession(prsId).getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            assertEquals(original, Sets.newHashSet(refs.iterator()));
        }
        store.forceMajorCompaction();
        // after a major compaction the original sstables will be gone and we will have no sstables to anticompact:
        try (Refs<SSTableReader> refs = ActiveRepairService.instance.getParentRepairSession(prsId).getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            assertEquals(0, refs.size());
        }
    }

    private Set<Range<Token>> getFullRange(ColumnFamilyStore store)
    {
        return Collections.singleton(new Range<>(store.getPartitioner().getMinimumToken(), store.getPartitioner().getMinimumToken()));
    }

    private Range<Token> getRange(Integer start, Integer end)
    {
        return new Range<>(new ByteOrderedPartitioner.BytesToken(String.format("%2d", start).getBytes()),
                           new ByteOrderedPartitioner.BytesToken(String.format("%2d", end).getBytes()));
    }

    @Test
    public void testSnapshotMultipleRepairs() throws Exception
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
        try (Refs<SSTableReader> refs = ActiveRepairService.instance.getParentRepairSession(prsId).getActiveRepairedSSTableRefsForAntiCompaction(store.metadata.cfId, prsId))
        {
            assertEquals(original, Sets.newHashSet(refs.iterator()));
        }
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        return prepareColumnFamilyStore(true);
    }

    private ColumnFamilyStore prepareColumnFamilyStore(boolean createSSTables)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.truncateBlocking();
        store.disableAutoCompaction();
        if (createSSTables)
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
                new RowUpdateBuilder(cfs.metadata, timestamp, Integer.toString(i))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }
    }
}
