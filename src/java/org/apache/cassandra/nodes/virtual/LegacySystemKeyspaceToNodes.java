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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.BootstrapState;
import org.apache.cassandra.nodes.LocalInfo;
import org.apache.cassandra.nodes.NodeInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.nodes.virtual.NodeConstants.LEGACY_PEERS;
import static org.apache.cassandra.nodes.virtual.NodeConstants.LOCAL;

/**
 * Upgrade code to migrate from {@code system.local} and {@code system.peers} to {@link Nodes}.
 * This migration exists to enable the upgrade path from older DSE versions that store the local and peers data in
 * sstables to flat files that are used within CC.
 * Without it the node isn't able (among others) to read its host id after upgrade.
 */
public final class LegacySystemKeyspaceToNodes
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySystemKeyspaceToNodes.class);

    public int peersMigrated;
    public boolean localMigrated;
    private final Nodes nodes;

    /**
     * Executes the system.local and system.peers tables migration to flat files
     * @return true if the flat files didn't exist and the tables contained data to be migrated, false otherwise
     */
    public static boolean convertToNodesFilesIfNecessary()
    {
        if (Nodes.local().localInfoWasPresent())
            // nothing to do
            return false;

        try
        {
            return new LegacySystemKeyspaceToNodes(Nodes.nodes()).convertToNodesFiles();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to migrate system.peers/local", e);
        }
    }

    public LegacySystemKeyspaceToNodes(Nodes nodes)
    {
        this.nodes = nodes;
    }

    @VisibleForTesting
    boolean convertToNodesFiles() throws Exception
    {
        Schema.unmapSystemTable(LOCAL);
        Schema.unmapSystemTable(LEGACY_PEERS);
        boolean upgradeExecuted = false;
        try
        {
            Keyspace systemKs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            ColumnFamilyStore cfs = systemKs.getColumnFamilyStore(SystemKeyspace.LEGACY_PEERS);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.STARTUP);
            if (!cfs.getLiveSSTables().isEmpty())
            {
                logger.info("Migrating system.peers ...");

                UntypedResultSet rows = executeInternal(format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_PEERS));
                if (!rows.isEmpty())
                    migratePeers(nodes, rows);
                else
                    logger.info("Not migrated anything from system.peers because the table is empty...");

                nodes.syncToDisk();

                logger.info("{}.{} has been migrated and truncated.", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_PEERS);

                upgradeExecuted = true;
            }

            cfs = systemKs.getColumnFamilyStore(SystemKeyspace.LOCAL);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.STARTUP);
            if (!cfs.getLiveSSTables().isEmpty())
            {
                logger.info("Migrating system.local ...");

                UntypedResultSet rows = executeInternal(format("SELECT * FROM %s.%s WHERE key = '%s'", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
                if (!rows.isEmpty())
                    migrateLocal(nodes, rows);
                else
                    logger.info("Not migrated anything from system.local because the table is empty...");

                nodes.syncToDisk();

                logger.info("{}.{} has been migrated and truncated.", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LOCAL);

                upgradeExecuted = true;
            }

            return upgradeExecuted;
        }
        finally
        {
            Schema.mapSystemTable(LOCAL);
            Schema.mapSystemTable(LEGACY_PEERS);
        }
    }

    private void migrateLocal(Nodes nodes, UntypedResultSet rows) throws Exception
    {
        logger.info("Migrating system.local...");
        rows.forEach(row -> nodes.getLocal().update(l -> {
        /*
        LOCAL:

        + "broadcast_address inet,"
        + "bootstrapped text,"
        + "cluster_name text,"
        + "cql_version text,"
        + "gossip_generation int,"
        + "listen_address inet,"
        + "native_protocol_version text,"
        + "partitioner text,"
        + "truncated_at map<uuid, blob>,"
        */

            if (row.has("broadcast_address"))
                l.setBroadcastAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(row.getInetAddress("broadcast_address"), DatabaseDescriptor.getStoragePort()));
            if (row.has("bootstrapped"))
                l.setBootstrapState(BootstrapState.valueOf(row.getString("bootstrapped")));
            if (row.has("cluster_name"))
                l.setClusterName(row.getString("cluster_name"));
            if (row.has("cql_version"))
                l.setCqlVersion(new CassandraVersion(row.getString("cql_version")));
            if (row.has("gossip_generation"))
                l.setGossipGeneration(row.getInt("gossip_generation"));
            if (row.has("listen_address"))
                l.setListenAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(row.getInetAddress("listen_address"), DatabaseDescriptor.getStoragePort()));
            if (row.has("native_protocol_version"))
                l.setNativeProtocolVersion(row.getString("native_protocol_version"));
            if (row.has("partitioner"))
                l.setPartitioner(row.getString("partitioner"));
            if (row.has("truncated_at"))
                l.setTruncationRecords(readTruncationRecords(row));

            migrateNodeInfo(row, l);
        }, true));
        localMigrated = true;
        Schema.instance.getKeyspaceInstance(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                .getColumnFamilyStore(SystemKeyspace.LOCAL)
                .truncateBlocking();
        logger.info("Migrated system.local and table truncated with snapshot...");
    }

    private void migratePeers(Nodes nodes, UntypedResultSet rows) throws Exception
    {
        logger.info("Migrating system.peers...");
        rows.forEach(row -> {
            InetAddressAndPort peer = InetAddressAndPort.getByAddress(row.getInetAddress("peer"));
            nodes.getPeers().update(peer, p -> {
            /*
            PEERS:

            + "peer inet,"
            + "preferred_ip inet,"
            */

                if (row.has("preferred_ip"))
                    p.setPreferred(InetAddressAndPort.getByAddress(row.getInetAddress("preferred_ip")));

                migrateNodeInfo(row, p);
            });
            peersMigrated++;
        });
        Schema.instance.getKeyspaceInstance(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                .getColumnFamilyStore(SystemKeyspace.LEGACY_PEERS)
                .truncateBlocking();
        logger.info("Migrated {} rows from system.peers and table truncated with snapshot...", peersMigrated);
    }

    private static void migrateNodeInfo(UntypedResultSet.Row row, NodeInfo n)
    {
        /*
        COMMON COLUMNS IN system.local + system.peers

        + "host_id uuid,"
        + "data_center text,"
        + "rack text,"
        + "release_version text,"
        + "schema_version uuid,"
        + "tokens set<varchar>,"
        + "rpc_address inet,"
        */
        if (row.has("host_id"))
            n.setHostId(row.getUUID("host_id"));
        if (row.has("data_center"))
            n.setDataCenter(row.getString("data_center"));
        if (row.has("rack"))
            n.setRack(row.getString("rack"));
        if (row.has("release_version"))
            n.setReleaseVersion(new CassandraVersion(row.getString("release_version")));
        if (row.has("schema_version"))
            n.setSchemaVersion(row.getUUID("schema_version"));
        if (row.has("tokens"))
            n.setTokens(deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
        if (row.has("rpc_address"))
        {
            InetAddress rpcAddress = row.getInetAddress("rpc_address");
            logger.debug("rpc_address: {}", rpcAddress);
            n.setNativeTransportAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(rpcAddress, DatabaseDescriptor.getNativeTransportPort()));
        }
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        tokens.sort(Comparator.naturalOrder());
        return tokens;
    }

    private static Map<UUID, LocalInfo.TruncationRecord> readTruncationRecords(UntypedResultSet.Row row)
    {
        Map<UUID, LocalInfo.TruncationRecord> records = new HashMap<>();

        Map<UUID, ByteBuffer> map = row.getMap("truncated_at", UUIDType.instance, BytesType.instance);
        for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
            records.put(entry.getKey(), truncationRecordFromBlob(entry.getValue()));

        return records;
    }

    private static LocalInfo.TruncationRecord truncationRecordFromBlob(ByteBuffer bytes)
    {
        try (RebufferingInputStream in = new DataInputBuffer(bytes, true))
        {
            return new LocalInfo.TruncationRecord(CommitLogPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
