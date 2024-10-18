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
package org.apache.cassandra.nodes.virtual;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.BootstrapState;
import org.apache.cassandra.nodes.LocalInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.nodes.PeerInfo;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.nodes.virtual.NodeConstants.LEGACY_PEERS;
import static org.apache.cassandra.nodes.virtual.NodeConstants.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LegacySystemKeyspaceToNodesTest extends CQLTester
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static InetAddress localAdr;
    private static InetAddress localBroadcastAdr;
    private static InetAddress localNativeAdr;
    private static InetAddress localListenAdr;

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

        localAdr = InetAddress.getByName("127.0.1.1");
        localBroadcastAdr = InetAddress.getByName("127.0.1.2");
        localNativeAdr = InetAddress.getByName("127.0.1.3");
        localListenAdr = InetAddress.getByName("127.0.1.4");

        VirtualKeyspaceRegistry.instance.register(VirtualSchemaKeyspace.instance);
        VirtualKeyspaceRegistry.instance.register(SystemViewsKeyspace.instance);
    }

    @Before
    public void clearLocalAndPeersAndNodes()
    {
        Nodes.nodes().resetUnsafe();

        // Just do "something" with Schema...
        Schema.instance.loadFromDisk();

        Keyspace.setInitialized();

        clearTableUnsafe(LOCAL);
        clearTableUnsafe(LEGACY_PEERS);

        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                .getColumnFamilyStores()
                .forEach(cfs -> cfs.getSnapshotDetails().keySet().forEach(cfs::clearSnapshot));
        Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                .getColumnFamilyStores()
                .forEach(cfs -> cfs.getSnapshotDetails().keySet().forEach(cfs::clearSnapshot));
    }

    private void clearTableUnsafe(String tableName)
    {
        ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                        .getColumnFamilyStore(tableName);
        cfs.truncateBlockingWithoutSnapshot();
        LifecycleTransaction.waitForDeletions();
        cfs.clearUnsafe();
        cfs.getDirectories().getCFDirectories().forEach(d -> {
            File[] c = d.toJavaIOFile().listFiles();;
            if (c == null)
                return;
            for (File f : c)
            {
                if (f.isDirectory())
                    FileUtils.deleteRecursive(f.toPath());
                assertFalse("Pre-check failure: found file" + f, f.isFile());
            }
        });
    }

    @Test
    public void verifyLocalOnlyUpgrade() throws Exception
    {
        Nodes nodes = new Nodes(folder.newFolder().toPath());

        try
        {
            //TODO
            //assertEquals(0L, nodes.getLocal().getLastSaveTimeNanos());
            //assertEquals(0L, nodes.getPeers().getLastSaveTimeNanos());

            Collection<Token> tokens = persistLocalMetadata();

            markTruncated(SystemKeyspace.BATCHES);
            LocalInfo.TruncationRecord trAvailableRanges = markTruncated(SystemKeyspace.LEGACY_AVAILABLE_RANGES);
            LocalInfo.TruncationRecord trPreparedStatements = markTruncated(SystemKeyspace.PREPARED_STATEMENTS);
            removeTruncated(SystemKeyspace.BATCHES);

            umappedViews(() -> {
                assertEquals(1, executeInternal("SELECT * FROM system.local").size());
                assertTrue(executeInternal("SELECT * FROM system.peers").isEmpty());
            });

            LegacySystemKeyspaceToNodes legacySystemKeyspaceToNodes = new LegacySystemKeyspaceToNodes(nodes);
            legacySystemKeyspaceToNodes.convertToNodesFiles();
            assertTrue(legacySystemKeyspaceToNodes.localMigrated);
            assertEquals(0, legacySystemKeyspaceToNodes.peersMigrated);

            // a following upgrade must not do anything
            LegacySystemKeyspaceToNodes legacySystemKeyspaceToNodes2 = new LegacySystemKeyspaceToNodes(nodes);
            legacySystemKeyspaceToNodes2.convertToNodesFiles();
            assertFalse(legacySystemKeyspaceToNodes2.localMigrated);
            assertEquals(0, legacySystemKeyspaceToNodes2.peersMigrated);

            umappedViews(() -> {
                assertTrue(executeInternal("SELECT * FROM system.local").isEmpty());
                assertTrue(executeInternal("SELECT * FROM system.peers").isEmpty());
            });

            assertEquals(1, Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                    .getColumnFamilyStore(LOCAL)
                                    .getSnapshotDetails()
                                    .size());
            assertTrue(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                               .getColumnFamilyStore(LEGACY_PEERS)
                               .getSnapshotDetails()
                               .isEmpty());

            assertNotEquals(0L, nodes.getLocal().getLastSaveTimeNanos());
            assertEquals(0L, nodes.getPeers().getLastSaveTimeNanos());

            LocalInfo local = nodes.getLocal().get();
            assertSame(local, nodes.get(FBUtilities.getBroadcastAddressAndPort()));

            // LocalInfo specific attributes
            assertEquals(localBroadcastAdr, local.getBroadcastAddressAndPort().address);
            assertEquals(BootstrapState.IN_PROGRESS, local.getBootstrapState());
            assertEquals(DatabaseDescriptor.getClusterName(), local.getClusterName());
            assertEquals(QueryProcessor.CQL_VERSION, local.getCqlVersion());
            assertEquals(Integer.valueOf(666), local.getGossipGeneration());
            assertEquals(localListenAdr, local.getListenAddressAndPort().address);
            assertEquals(String.valueOf(ProtocolVersion.CURRENT.asInt()), local.getNativeProtocolVersion());
            assertEquals(DatabaseDescriptor.getPartitioner().getClass().getName(), local.getPartitioner());
            Map<UUID, LocalInfo.TruncationRecord> tr = local.getTruncationRecords();
            assertEquals(2, tr.size());
            assertEquals(trAvailableRanges, tr.get(tableId(SystemKeyspace.LEGACY_AVAILABLE_RANGES)));
            assertEquals(trPreparedStatements, tr.get(tableId(SystemKeyspace.PREPARED_STATEMENTS)));
            assertFalse(tr.containsKey(tableId(SystemKeyspace.BATCHES)));

            // NodeInfo attributes
            assertEquals(new CassandraVersion(FBUtilities.getReleaseVersionString()), local.getReleaseVersion());
            assertEquals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort()), local.getDataCenter());
            assertEquals(DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddressAndPort()), local.getRack());
            assertEquals(localNativeAdr, local.getNativeTransportAddressAndPort().address);
            assertEquals(DatabaseDescriptor.getNativeTransportPort(), local.getNativeTransportAddressAndPort().port);
            assertEquals(UUID.nameUUIDFromBytes(localBroadcastAdr.getAddress()), local.getSchemaVersion());
            assertEquals(new ArrayList<>(tokens), new ArrayList<>(local.getTokens()));
        }
        finally
        {
            nodes.shutdown();
        }
    }

    @Test
    public void verifyLocalAndPeersUpgrade() throws Exception
    {
        Nodes nodes = new Nodes(folder.newFolder().toPath());

        int nPeers = 1000;

        try
        {
            persistLocalMetadata();

            Collection[] tokens = new Collection[nPeers];
            for (int i = 0; i < nPeers; i++)
                tokens[i] = persistPeerMetadata(i);

            umappedViews(() -> {
                assertEquals(1, executeInternal("SELECT * FROM system.local").size());
                assertEquals(nPeers, executeInternal("SELECT * FROM system.peers").size());
            });

            LegacySystemKeyspaceToNodes legacySystemKeyspaceToNodes = new LegacySystemKeyspaceToNodes(nodes);
            legacySystemKeyspaceToNodes.convertToNodesFiles();
            assertTrue(legacySystemKeyspaceToNodes.localMigrated);
            assertEquals(1000, legacySystemKeyspaceToNodes.peersMigrated);

            umappedViews(() -> {
                assertTrue(executeInternal("SELECT * FROM system.local").isEmpty());
                assertTrue(executeInternal("SELECT * FROM system.peers").isEmpty());
            });

            // a following upgrade must not do anything
            LegacySystemKeyspaceToNodes legacySystemKeyspaceToNodes2 = new LegacySystemKeyspaceToNodes(nodes);
            legacySystemKeyspaceToNodes2.convertToNodesFiles();
            assertFalse(legacySystemKeyspaceToNodes2.localMigrated);
            assertEquals(0, legacySystemKeyspaceToNodes2.peersMigrated);

            assertEquals(1, Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                    .getColumnFamilyStore(LOCAL)
                                    .getSnapshotDetails()
                                    .size());
            assertEquals(1, Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                    .getColumnFamilyStore(LEGACY_PEERS)
                                    .getSnapshotDetails()
                                    .size());

            assertNotEquals(0L, nodes.getLocal().getLastSaveTimeNanos());
            assertNotEquals(0L, nodes.getPeers().getLastSaveTimeNanos());

            for (int i = 0; i < nPeers; i++)
            {
                InetAddressAndPort adrPeer = InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByName("127.99." + (i / 256) + '.' + (i & 0xff)), DatabaseDescriptor.getStoragePort());
                InetAddressAndPort adrPreferred = InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByName("127.199." + (i / 256) + '.' + (i & 0xff)), DatabaseDescriptor.getStoragePort());
                InetAddressAndPort adrNative = InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByName("127.42." + (i / 256) + '.' + (i & 0xff)), DatabaseDescriptor.getNativeTransportPort());

                PeerInfo peer = nodes.getPeers().get(adrPeer);
                assertNotNull(peer);

                // PeerInfo specific attributes
                assertSame(peer, nodes.get(adrPeer));
                assertEquals(adrPreferred, peer.getPreferred());

                // NodeInfo attributes
                assertEquals(new CassandraVersion("3.4.11"), peer.getReleaseVersion());
                assertEquals("DC" + (i / 100), peer.getDataCenter());
                assertEquals("RAC" + (i % 100), peer.getRack());
                assertEquals(adrNative, peer.getNativeTransportAddressAndPort());
                assertEquals(UUID.nameUUIDFromBytes(adrPeer.address.getAddress()), peer.getHostId());
                assertEquals(UUID.nameUUIDFromBytes(adrPreferred.address.getAddress()), peer.getSchemaVersion());
                assertEquals(new ArrayList<Token>(tokens[i]), new ArrayList<>(peer.getTokens()));
            }
        }
        finally
        {
            nodes.shutdown();
        }
    }


    private static Collection<Token> persistPeerMetadata(int i) throws Exception
    {
        String req = "INSERT INTO system.%s (" +
                     "peer," +
                     "release_version," +
                     "rpc_address," +
                     "data_center," +
                     "rack," +
                     "preferred_ip," +
                     "host_id," +
                     "schema_version," +
                     "tokens" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        InetAddress peer = InetAddress.getByName("127.99." + (i / 256) + '.' + (i & 0xff));
        InetAddress adrPreferred = InetAddress.getByName("127.199." + (i / 256) + '.' + (i & 0xff));
        InetAddress adrNative = InetAddress.getByName("127.42." + (i / 256) + '.' + (i & 0xff));
        Collection<Token> tokens = makeTokens();
        umappedViews(() -> executeOnceInternal(format(req, LEGACY_PEERS),
                                                     peer,
                                                     "3.4.11",
                                                     adrNative,
                                                     "DC" + (i / 100),
                                                     "RAC" + (i % 100),
                                                     adrPreferred,
                                                     UUID.nameUUIDFromBytes(peer.getAddress()),
                                                     UUID.nameUUIDFromBytes(adrPreferred.getAddress()),
                                                     tokens.stream().map(t -> DatabaseDescriptor.getPartitioner().getTokenFactory().toString(t)).collect(Collectors.toSet())));
        return tokens;
    }


    private static Collection<Token> persistLocalMetadata()
    {
        return persistLocalMetadata(FBUtilities.getReleaseVersionString());
    }

    private static Collection<Token> persistLocalMetadata(String releaseVersion)
    {
        String req = "INSERT INTO system.%s (" +
                     "key," +
                     "cluster_name," +
                     "release_version," +
                     "cql_version," +
                     "native_protocol_version," +
                     "data_center," +
                     "rack," +
                     "partitioner," +
                     "rpc_address," +
                     "broadcast_address," +
                     "listen_address," +
                     "bootstrapped," +
                     "gossip_generation," +
                     "host_id," +
                     "schema_version," +
                     "tokens" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        Collection<Token> tokens = makeTokens();
        umappedViews(() -> executeInternal(format(req, LOCAL),
                                                     LOCAL,
                                                     DatabaseDescriptor.getClusterName(),
                                                     releaseVersion,
                                                     QueryProcessor.CQL_VERSION.toString(),
                                                     String.valueOf(ProtocolVersion.CURRENT.asInt()),
                                                     snitch.getDatacenter(FBUtilities.getBroadcastAddressAndPort()),
                                                     snitch.getRack(FBUtilities.getBroadcastAddressAndPort()),
                                                     DatabaseDescriptor.getPartitioner().getClass().getName(),
                                                     localNativeAdr,
                                                     localBroadcastAdr,
                                                     localListenAdr,
                                                     BootstrapState.IN_PROGRESS.name(),
                                                     666,
                                                     UUID.nameUUIDFromBytes(localAdr.getAddress()),
                                                     UUID.nameUUIDFromBytes(localBroadcastAdr.getAddress()),
                                                     tokens.stream().map(t -> DatabaseDescriptor.getPartitioner().getTokenFactory().toString(t)).collect(Collectors.toSet())));
        return tokens;
    }

    private static void umappedViews(Runnable r)
    {
        try
        {
            Schema.unmapSystemTable("local");
            Schema.unmapSystemTable("peers");
            r.run();
        }
        finally
        {
            Schema.mapSystemTable("local");
            Schema.mapSystemTable("peers");
        }
    }

    private static Collection<Token> makeTokens()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Set<Token> tokens = new TreeSet<>();
        for (int i = 0; i < 8; i++)
            tokens.add(partitioner.getRandomToken());
        return tokens;
    }

    private static void removeTruncated(String table)
    {
        UUID tableId = tableId(table);

        String req = "DELETE truncated_at[?] from system.%s WHERE key = '%s'";
        umappedViews(() -> executeInternal(format(req, LOCAL, LOCAL), tableId));
    }

    private static UUID tableId(String table)
    {
        return Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                       .getMetadata()
                       .getTableOrViewNullable(table)
                       .id.asUUID();
    }

    private static LocalInfo.TruncationRecord markTruncated(String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                        .getColumnFamilyStore(table);

        String req = "UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'";
        Random r = new Random();
        CommitLogPosition position = new CommitLogPosition(r.nextLong(), r.nextInt(Integer.MAX_VALUE - 1) + 1);
        long truncatedAt = System.currentTimeMillis();
        umappedViews(() -> executeInternal(format(req, LOCAL, LOCAL), truncationAsMapEntry(cfs, truncatedAt, position)));
        return new LocalInfo.TruncationRecord(position, truncatedAt);
    }

    private static Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
            return singletonMap(cfs.metadata.id.asUUID(), out.asNewBuffer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldTakeSnapshotOfSystemKeyspacesOnUpgradeFromLegacySystemLocalAndPeers() throws Exception
    {
        // persist local metadata in system.local
        persistLocalMetadata("3.0.123");
        persistPeerMetadata(11);

        // check that there are no snapshots (test sanity check)
        assertSnapshotCount(0, 0, 0);

        assertTrue(LegacySystemKeyspaceToNodes.convertToNodesFilesIfNecessary());

        assertSnapshotCount(0, 1, 0);

        StorageService.snapshotOnVersionChange();

        // snapshots must exist
        assertSnapshotCount(1, 2, 1);
    }

    @Test
    public void shouldTakeSnapshotOfSystemKeyspacesOnUpgradeUsingNodesFiles() throws Exception
    {
        // persist local metadata in Nodes.Instance.nodes.
        Nodes.local().update(l -> {
            l.setReleaseVersion(new CassandraVersion("3.0.123"));
        }, true);

        // check that there are no snapshots (test sanity check)
        assertSnapshotCount(0, 0, 0);

        assertFalse(LegacySystemKeyspaceToNodes.convertToNodesFilesIfNecessary());

        // check that there are no snapshots
        assertSnapshotCount(0, 0, 0);

        //assertTrue(StorageService.snapshotOnVersionChange());
        StorageService.snapshotOnVersionChange();

        // snapshots must exist
        assertSnapshotCount(1, 1, 1);
    }

    private static void assertSnapshotCount(int expectedSnapshots, int expectedLocalPeersSnapshots, int expectedNodesSnapshots)
    {
        // check number of snapshots tables in system and system_schema keyspaces, except for system.local + system.peers tables
        for (String ks : SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES)
        {
            Keyspace.open(ks)
                    .getColumnFamilyStores()
                    .stream()
                    .filter(cfs -> !LOCAL.equals(cfs.name) && !LEGACY_PEERS.equals(cfs.name))
                    .forEach(cfs -> assertEquals(cfs.keyspace.getName() + '.' + cfs.name, expectedSnapshots, cfs.getSnapshotDetails().size()));
        }

        // check number of snapshots for system.local + system.peers tables
        Stream.of(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(LOCAL),
                  Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(LEGACY_PEERS))
              .forEach(cfs -> assertEquals(cfs.keyspace.getName() + '.' + cfs.name, expectedLocalPeersSnapshots, cfs.getSnapshotDetails().size()));

        // check number of snapshots for Nodes
        assertEquals(expectedNodesSnapshots, Nodes.nodes().listSnapshots().size());
    }
}
