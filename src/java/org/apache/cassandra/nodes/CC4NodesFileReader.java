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

package org.apache.cassandra.nodes;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;
import org.msgpack.jackson.dataformat.MessagePackFactory;

/**
 * Reads CC4's file-based node metadata (msgpack format) and converts
 * to CC5 {@link LocalInfo} and {@link PeerInfo} objects.
 *
 * CC4 stores node metadata in file-based MessagePack format at
 * {@code {metadata_dir}/nodes/local} and {@code {metadata_dir}/nodes/peers}.
 * During a CC4-to-CC5 upgrade, this reader provides a migration path
 * by reading those files and producing CC5-compatible objects.
 *
 * DSE 6.8 and 6.9 write the same two files with the same encoding but partly different keys: plain
 * addresses ({@code broadcast_address}, {@code listen_address}, {@code native_transport_address},
 * {@code rpc_address}, {@code peer}, {@code preferred_ip}) with the ports in their own keys
 * ({@code storage_port}, {@code native_transport_port}), and a 6.9 node without peers writes an empty map
 * instead of an empty array. Both layouts are read; the DSE keys never occur in a CC4 file.
 */
final class CC4NodesFileReader
{
    private static final Logger logger = LoggerFactory.getLogger(CC4NodesFileReader.class);

    private CC4NodesFileReader()
    {
    }

    /**
     * @return true if a CC4 nodes directory exists in the metadata directory
     */
    static boolean hasCC4NodesDirectory()
    {
        return getCC4NodesDirectory() != null;
    }

    /**
     * Try to read local node info from CC4's file-based store.
     *
     * @return a CC5 {@link LocalInfo} populated from CC4 data, or null if the CC4 nodes directory or local file doesn't exist
     * @throws RuntimeException if the CC4 local file exists but cannot be read or parsed
     */
    static LocalInfo tryReadLocalInfo()
    {
        Path nodesDir = getCC4NodesDirectory();
        if (nodesDir == null)
            return null;

        Path localPath = nodesDir.resolve("local");
        Path backupPath = nodesDir.resolve("local.old");
        Path tempPath = nodesDir.resolve("local.txn");

        maybeRecoverTransaction(localPath, backupPath, tempPath);

        if (!Files.exists(localPath))
        {
            logger.debug("CC4 local metadata file not found at {}", localPath);
            return null;
        }

        try
        {
            ObjectMapper mapper = createCC4ObjectMapper();
            ObjectReader reader = mapper.readerFor(CC4LocalInfo.class);

            CC4LocalInfo cc4Local;
            try (InputStream in = new BufferedInputStream(Files.newInputStream(localPath, StandardOpenOption.READ)))
            {
                cc4Local = reader.readValue(in);
            }

            LocalInfo info = convertLocalInfo(cc4Local);
            logger.info("Successfully read CC4 local node metadata from {} (host_id={})", localPath, info.getHostId());
            return info;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to read CC4 local metadata from " + localPath +
                                       "; aborting upgrade to prevent generating a new node identity", e);
        }
    }

    /**
     * Reads only the host id of {@code nodes/local}, without the transaction recovery of
     * {@link #tryReadLocalInfo()}: used to check the import marker against the files it covers.
     *
     * @return the host id, or null if the nodes directory, the local file or its host id is absent
     * @throws RuntimeException if the local file exists but cannot be read or parsed
     */
    static UUID readLocalHostId()
    {
        return readLocalWithoutRecovery("host id", CC4LocalInfo.class, cc4Local -> cc4Local.hostId);
    }

    /**
     * Reads only the release version of {@code nodes/local}, without the transaction recovery of
     * {@link #tryReadLocalInfo()}: in DSE 6 compatibility mode DSE's own files stay in place, and their release
     * names the DSE version this node ran.
     *
     * @return the release version, or null if the nodes directory, the local file or its release version is absent
     * @throws RuntimeException if the local file exists but cannot be read or parsed
     */
    static CassandraVersion readLocalReleaseVersion()
    {
        return readLocalWithoutRecovery("release version", CC4LocalInfo.class, cc4Local -> cc4Local.releaseVersion);
    }

    /**
     * Reads only the DSE workload fields of {@code nodes/local}, without the transaction recovery of
     * {@link #tryReadLocalInfo()}: in DSE 6 compatibility mode DSE's own files stay in place, and they name the DSE
     * workloads this node ran.
     *
     * @return the fields, or null if the nodes directory or the local file is absent
     * @throws RuntimeException if the local file exists but cannot be read or parsed
     */
    static DseNodeWorkloads readLocalWorkloads()
    {
        return readLocalWithoutRecovery("DSE workloads", DseWorkloadFields.class, DseWorkloadFields::toWorkloads);
    }

    /**
     * Reads only the addresses and the DSE workload fields of the entries of {@code nodes/peers}, without the
     * transaction recovery of {@link #tryReadPeers()}.
     *
     * @return the fields of every entry, empty if the nodes directory or the peers file is absent or holds no peer
     * @throws RuntimeException if the peers file exists but cannot be read or parsed
     */
    static List<DseNodeWorkloads> readPeerWorkloads()
    {
        Path nodesDir = getCC4NodesDirectory();
        if (nodesDir == null)
            return Collections.emptyList();
        Path peersPath = nodesDir.resolve("peers");
        if (!Files.exists(peersPath))
            return Collections.emptyList();
        try (InputStream in = new BufferedInputStream(Files.newInputStream(peersPath, StandardOpenOption.READ)))
        {
            if (holdsNoPeer(in))
                return Collections.emptyList();
            Collection<DseWorkloadFields> entries = createCC4ObjectMapper().readerFor(new TypeReference<Collection<DseWorkloadFields>>() {}).readValue(in);
            List<DseNodeWorkloads> workloads = new ArrayList<>(entries.size());
            for (DseWorkloadFields entry : entries)
                workloads.add(entry.toWorkloads());
            return workloads;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to read the DSE workloads from the node metadata file " + peersPath, e);
        }
    }

    /**
     * {@code nodes/local} read as {@code type}, then {@code field} of it, without the transaction recovery of
     * {@link #tryReadLocalInfo()}; null if the nodes directory or the local file is absent.
     *
     * @throws RuntimeException if the local file exists but cannot be read or parsed; the message names {@code what}
     */
    private static <R, T> T readLocalWithoutRecovery(String what, Class<R> type, Function<R, T> field)
    {
        Path nodesDir = getCC4NodesDirectory();
        if (nodesDir == null)
            return null;
        Path localPath = nodesDir.resolve("local");
        if (!Files.exists(localPath))
            return null;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(localPath, StandardOpenOption.READ)))
        {
            R read = createCC4ObjectMapper().readerFor(type).readValue(in);
            return field.apply(read);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to read the " + what + " from the node metadata file " + localPath, e);
        }
    }

    /**
     * Try to read peer info from CC4's file-based store.
     *
     * @return a stream of CC5 {@link PeerInfo} objects, or an empty stream if the CC4 nodes directory or peers file doesn't exist
     * @throws RuntimeException if the CC4 peers file exists but cannot be read or parsed
     */
    static Stream<PeerInfo> tryReadPeers()
    {
        Path nodesDir = getCC4NodesDirectory();
        if (nodesDir == null)
            return Stream.empty();

        Path peersPath = nodesDir.resolve("peers");
        Path backupPath = nodesDir.resolve("peers.old");
        Path tempPath = nodesDir.resolve("peers.txn");

        maybeRecoverTransaction(peersPath, backupPath, tempPath);

        if (!Files.exists(peersPath))
        {
            logger.debug("CC4 peers metadata file not found at {}", peersPath);
            return Stream.empty();
        }

        try
        {
            ObjectMapper mapper = createCC4ObjectMapper();
            ObjectReader reader = mapper.readerFor(new TypeReference<Collection<CC4PeerInfo>>() {});

            Collection<CC4PeerInfo> cc4Peers;
            try (InputStream in = new BufferedInputStream(Files.newInputStream(peersPath, StandardOpenOption.READ)))
            {
                if (holdsNoPeer(in))
                {
                    logger.info("The peers metadata file {} holds the empty map DSE writes before a node has peers: no peers to migrate", peersPath);
                    return Stream.empty();
                }
                cc4Peers = reader.readValue(in);
            }

            logger.info("Successfully read {} CC4 peer entries from {}", cc4Peers.size(), peersPath);
            return cc4Peers.stream().map(CC4NodesFileReader::convertPeerInfo);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to read CC4 peers metadata from " + peersPath +
                                       "; aborting upgrade", e);
        }
    }

    /**
     * Renames the CC4 nodes directory to {@code nodes.migrated} so subsequent restarts
     * do not attempt migration again. In {@code internode_compatibility_mode: DSE_6} the directory is left
     * in place instead: a node rolled back to DSE restarts from these files (the import is then recorded by
     * the marker of {@link CC4UpgradeNodesPersistence}). Its only caller already skips it in that mode; the check
     * is a defensive guard for any future caller.
     */
    static void archiveCC4NodesDirectory()
    {
        Path nodesDir = getCC4NodesDirectory();
        if (nodesDir == null)
            return;
        if (DatabaseDescriptor.isDse6CompatibilityMode())
        {
            // a rolled-back DSE node restarts from these files: leave them exactly where they are
            logger.info("Leaving the DSE node metadata directory {} in place (internode_compatibility_mode DSE_6)", nodesDir);
            return;
        }

        Path archivedDir = nodesDir.resolveSibling("nodes.migrated");
        try
        {
            Files.move(nodesDir, archivedDir, StandardCopyOption.ATOMIC_MOVE);
            logger.info("Archived CC4 nodes directory from {} to {}", nodesDir, archivedDir);
        }
        catch (IOException e)
        {
            logger.warn("Failed to archive CC4 nodes directory from {} to {}; " +
                         "migration will be attempted again on next restart but is idempotent",
                         nodesDir, archivedDir, e);
        }
    }

    private static Path getCC4NodesDirectory()
    {
        try
        {
            Path metadataDir = DatabaseDescriptor.getMetadataDirectory().toPath();
            Path nodesDir = metadataDir.resolve("nodes");
            if (!Files.isDirectory(nodesDir))
            {
                logger.debug("CC4 nodes directory not found at {}", nodesDir);
                return null;
            }
            return nodesDir;
        }
        catch (Exception e)
        {
            logger.debug("Could not determine CC4 nodes directory", e);
            return null;
        }
    }

    /**
     * Handle CC4's transactional file recovery: if a .old backup exists,
     * it means a transaction was interrupted. Restore from .old.
     * DSE 6 recovers its own files the same way when it reads them, so a DSE file is restored to what DSE itself
     * would start from.
     */
    private static void maybeRecoverTransaction(Path originalPath, Path backupPath, Path tempPath)
    {
        try
        {
            if (Files.exists(backupPath))
            {
                logger.warn("CC4 backup file {} exists, recovering from interrupted transaction", backupPath);
                Files.deleteIfExists(originalPath);
                Files.move(backupPath, originalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                Files.deleteIfExists(tempPath);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to recover CC4 transaction from " + backupPath +
                                       "; aborting upgrade because node metadata may be in an inconsistent state", e);
        }
    }

    static ObjectMapper createCC4ObjectMapper()
    {
        MessagePackFactory messagePackFactory = new MessagePackFactory();
        messagePackFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        ObjectMapper objectMapper = new ObjectMapper(messagePackFactory); // checkstyle: permit this instantiation
        objectMapper.registerModule(createMsgpackModule());
        return objectMapper;
    }

    /**
     * Creates a Jackson module with custom deserializers matching CC4's SerHelper.
     * These handle CC4's binary serialization format for UUIDs, InetAddressAndPort,
     * Tokens, CassandraVersion, and TruncationRecord.
     */
    private static SimpleModule createMsgpackModule()
    {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(CassandraVersion.class, new CassandraVersionDeserializer());
        module.addDeserializer(Token.class, new TokenDeserializer());
        module.addDeserializer(InetAddressAndPort.class, new InetAddressAndPortDeserializer());
        module.addDeserializer(InetAddress.class, new InetAddressDeserializer());
        module.addDeserializer(UUID.class, new UUIDDeserializer());
        module.addDeserializer(CC4TruncationRecord.class, new TruncationRecordDeserializer());
        return module;
    }

    // ---- Conversion methods ----

    private static LocalInfo convertLocalInfo(CC4LocalInfo cc4)
    {
        LocalInfo info = new LocalInfo();
        // Common fields from NodeInfo
        if (cc4.hostId != null)
            info.setHostId(cc4.hostId);
        if (cc4.dataCenter != null)
            info.setDataCenter(cc4.dataCenter);
        if (cc4.rack != null)
            info.setRack(cc4.rack);
        if (cc4.releaseVersion != null)
            info.setReleaseVersion(cc4.releaseVersion);
        if (cc4.schemaVersion != null)
            info.setSchemaVersion(cc4.schemaVersion);
        if (cc4.tokens != null && !cc4.tokens.isEmpty())
            info.setTokens(cc4.tokens);
        if (cc4.nativeTransportAddressAndPort != null)
            info.setNativeTransportAddressAndPort(cc4.nativeTransportAddressAndPort);
        else if (cc4.nativeTransportAddress != null || cc4.rpcAddress != null)
            info.setNativeTransportAddressAndPort(dseNativeAddress(cc4.nativeTransportAddress, cc4.rpcAddress, cc4.nativeTransportPort));

        // LocalInfo-specific fields
        if (cc4.broadcastAddressAndPort != null)
            info.setBroadcastAddressAndPort(cc4.broadcastAddressAndPort);
        else if (cc4.broadcastAddress != null)
            info.setBroadcastAddressAndPort(withPort(cc4.broadcastAddress, cc4.storagePort, DatabaseDescriptor.getStoragePort()));
        if (cc4.listenAddressAndPort != null)
            info.setListenAddressAndPort(cc4.listenAddressAndPort);
        else if (cc4.listenAddress != null)
            info.setListenAddressAndPort(withPort(cc4.listenAddress, cc4.storagePort, DatabaseDescriptor.getStoragePort()));
        if (cc4.clusterName != null)
            info.setClusterName(cc4.clusterName);
        if (cc4.bootstrapped != null)
        {
            try
            {
                info.setBootstrapState(SystemKeyspace.BootstrapState.valueOf(cc4.bootstrapped.name()));
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Unknown CC4 bootstrap state: {}", cc4.bootstrapped);
            }
        }
        if (cc4.truncatedAt != null && !cc4.truncatedAt.isEmpty())
        {
            ImmutableMap.Builder<UUID, TruncationRecord> builder = ImmutableMap.builder();
            for (Map.Entry<UUID, CC4TruncationRecord> entry : cc4.truncatedAt.entrySet())
            {
                CC4TruncationRecord cc4Rec = entry.getValue();
                builder.put(entry.getKey(), new TruncationRecord(cc4Rec.position, cc4Rec.truncatedAt));
            }
            info.setTruncationRecords(builder.build());
        }
        if (cc4.partitioner != null)
        {
            try
            {
                @SuppressWarnings("unchecked")
                Class<? extends org.apache.cassandra.dht.IPartitioner> partitionerClass =
                    (Class<? extends org.apache.cassandra.dht.IPartitioner>) Class.forName(cc4.partitioner);
                info.setPartitionerClass(partitionerClass);
            }
            catch (ClassNotFoundException e)
            {
                logger.error("Unknown CC4 partitioner class '{}', falling back to configured partitioner '{}'",
                             cc4.partitioner, DatabaseDescriptor.getPartitioner().getClass().getName());
                info.setPartitionerClass(DatabaseDescriptor.getPartitioner().getClass());
            }
        }
        return info;
    }

    private static PeerInfo convertPeerInfo(CC4PeerInfo cc4)
    {
        PeerInfo info = new PeerInfo();
        // Common fields from NodeInfo
        if (cc4.hostId != null)
            info.setHostId(cc4.hostId);
        if (cc4.dataCenter != null)
            info.setDataCenter(cc4.dataCenter);
        if (cc4.rack != null)
            info.setRack(cc4.rack);
        if (cc4.releaseVersion != null)
            info.setReleaseVersion(cc4.releaseVersion);
        if (cc4.schemaVersion != null)
            info.setSchemaVersion(cc4.schemaVersion);
        if (cc4.tokens != null && !cc4.tokens.isEmpty())
            info.setTokens(cc4.tokens);
        if (cc4.nativeTransportAddressAndPort != null)
            info.setNativeTransportAddressAndPort(cc4.nativeTransportAddressAndPort);
        else if (cc4.nativeTransportAddress != null || cc4.rpcAddress != null)
            info.setNativeTransportAddressAndPort(dseNativeAddress(cc4.nativeTransportAddress, cc4.rpcAddress, cc4.nativeTransportPort));

        // PeerInfo-specific fields
        if (cc4.peer != null)
            info.setPeerAddressAndPort(cc4.isDseLayout() ? withPort(cc4.peer.getAddress(), cc4.storagePort, DatabaseDescriptor.getStoragePort()) : cc4.peer);
        if (cc4.preferred != null)
            info.setPreferredAddressAndPort(cc4.isDseLayout() ? withPort(cc4.preferred.getAddress(), cc4.storagePort, DatabaseDescriptor.getStoragePort()) : cc4.preferred);

        return info;
    }

    private static final int MSGPACK_EMPTY_FIXMAP = 0x80;

    /**
     * Whether the peers file read by {@code in}, positioned at its start and supporting mark, holds DSE 6.9's empty
     * map, which it writes instead of an empty array before the node has any peer; the stream is left at its start.
     *
     * @throws IllegalStateException if the file holds any other map: an array of peer entries is expected
     */
    static boolean holdsNoPeer(InputStream in) throws IOException
    {
        in.mark(1);
        int first = in.read();
        in.reset();
        if (first == MSGPACK_EMPTY_FIXMAP)
            return true;
        if (isMsgpackMap(first))
            throw new IllegalStateException("unexpected layout: a msgpack map other than DSE's empty map; an array of peer entries was expected");
        return false;
    }

    private static boolean isMsgpackMap(int firstByte)
    {
        return (firstByte >= 0x80 && firstByte <= 0x8f) || firstByte == 0xde || firstByte == 0xdf;
    }

    /** DSE stores addresses and ports in separate keys; CC4 stored them together. */
    private static InetAddressAndPort withPort(InetAddress address, Integer port, int defaultPort)
    {
        return InetAddressAndPort.getByAddressOverrideDefaults(address, port != null ? port : defaultPort);
    }

    /**
     * The native transport address of a DSE entry: {@code native_transport_address}, else the deprecated
     * {@code rpc_address} DSE writes next to it, with {@code native_transport_port} or the configured
     * native port.
     */
    private static InetAddressAndPort dseNativeAddress(InetAddress nativeTransportAddress, InetAddress rpcAddress, Integer nativeTransportPort)
    {
        InetAddress address = nativeTransportAddress != null ? nativeTransportAddress : rpcAddress;
        return withPort(address, nativeTransportPort, DatabaseDescriptor.getNativeTransportPort());
    }

    // ---- CC4 deserialization target classes ----

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CC4LocalInfo
    {
        @JsonProperty("host_id")
        UUID hostId;
        @JsonProperty("data_center")
        String dataCenter;
        @JsonProperty("rack")
        String rack;
        @JsonProperty("release_version")
        CassandraVersion releaseVersion;
        @JsonProperty("schema_version")
        UUID schemaVersion;
        @JsonProperty("tokens")
        Collection<Token> tokens;
        @JsonProperty("native_transport_address_and_port")
        InetAddressAndPort nativeTransportAddressAndPort;
        @JsonProperty("broadcast_address_and_port")
        InetAddressAndPort broadcastAddressAndPort;
        @JsonProperty("listen_address_and_port")
        InetAddressAndPort listenAddressAndPort;
        // DSE 6 keys: plain addresses, ports in their own keys.
        // DSE keys deliberately ignored (recomputed at startup, DSE-only, or without an HCD 2.0 counterpart):
        // key, native_transport_port_ssl, storage_port_ssl, jmx_port, dse_version, graph, server_id,
        // workload, workloads, cql_version, gossip_generation, native_protocol_version,
        // last_nodesync_checkpoint_time. rpc_address is read only as the last fallback for the native address.
        // graph, workload and workloads are not imported; readLocalWorkloads reads them on their own.
        @JsonProperty("native_transport_address")
        InetAddress nativeTransportAddress;
        @JsonProperty("rpc_address")
        InetAddress rpcAddress;
        @JsonProperty("native_transport_port")
        Integer nativeTransportPort;
        @JsonProperty("broadcast_address")
        InetAddress broadcastAddress;
        @JsonProperty("listen_address")
        InetAddress listenAddress;
        @JsonProperty("storage_port")
        Integer storagePort;
        @JsonProperty("cluster_name")
        String clusterName;
        @JsonProperty("bootstrapped")
        CC4BootstrapState bootstrapped;
        @JsonProperty("truncated_at")
        Map<UUID, CC4TruncationRecord> truncatedAt;
        @JsonProperty("partitioner")
        String partitioner;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CC4PeerInfo
    {
        @JsonProperty("host_id")
        UUID hostId;
        @JsonProperty("data_center")
        String dataCenter;
        @JsonProperty("rack")
        String rack;
        @JsonProperty("release_version")
        CassandraVersion releaseVersion;
        @JsonProperty("schema_version")
        UUID schemaVersion;
        @JsonProperty("tokens")
        Collection<Token> tokens;
        @JsonProperty("native_transport_address_and_port")
        InetAddressAndPort nativeTransportAddressAndPort;
        // DSE 6 keys: peer and preferred_ip are plain addresses, their port is storage_port.
        // DSE keys deliberately ignored: native_transport_port_ssl, storage_port_ssl, jmx_port, dse_version,
        // graph, server_id, workload, workloads. rpc_address is read only as the last fallback for the native address.
        // graph, workload and workloads are not imported; readPeerWorkloads reads them on their own.
        @JsonProperty("native_transport_address")
        InetAddress nativeTransportAddress;
        @JsonProperty("rpc_address")
        InetAddress rpcAddress;
        @JsonProperty("native_transport_port")
        Integer nativeTransportPort;
        @JsonProperty("storage_port")
        Integer storagePort;
        @JsonProperty("peer")
        InetAddressAndPort peer;
        @JsonProperty("preferred_ip")
        InetAddressAndPort preferred;

        @JsonCreator
        CC4PeerInfo(@JsonProperty("peer") InetAddressAndPort peer)
        {
            this.peer = peer;
        }

        /**
         * An entry written by DSE: it carries keys a CC4 peer entry never has. Its {@code peer} and
         * {@code preferred_ip} take {@code storage_port}, or the configured storage port without that key; a port
         * written in a textual address of such an entry is overridden (DSE writes these addresses as raw bytes).
         */
        boolean isDseLayout()
        {
            return storagePort != null || nativeTransportAddress != null || rpcAddress != null;
        }
    }

    /**
     * The DSE workload keys of an entry of {@code nodes/local} or {@code nodes/peers}, and the address of a peer
     * entry; a CC4 file has none of the workload keys.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class DseWorkloadFields
    {
        @JsonProperty("peer")
        InetAddressAndPort peer;
        @JsonProperty("workloads")
        List<String> workloads;
        @JsonProperty("graph")
        Boolean graph;
        @JsonProperty("workload")
        String workload;

        DseNodeWorkloads toWorkloads()
        {
            return new DseNodeWorkloads(peer == null ? null : peer.getAddress(), workloads, graph, workload);
        }
    }

    enum CC4BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS,
        DECOMMISSIONED
    }

    static class CC4TruncationRecord
    {
        final CommitLogPosition position;
        final long truncatedAt;

        CC4TruncationRecord(CommitLogPosition position, long truncatedAt)
        {
            this.position = position;
            this.truncatedAt = truncatedAt;
        }
    }

    // ---- Custom deserializers matching CC4's SerHelper ----

    private static final class CassandraVersionDeserializer extends JsonDeserializer<CassandraVersion>
    {
        @Override
        public CassandraVersion deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
        {
            return new CassandraVersion(jsonParser.getText());
        }
    }

    private static final class TokenDeserializer extends JsonDeserializer<Token>
    {
        @Override
        public Token deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
        {
            return DatabaseDescriptor.getPartitioner().getTokenFactory().fromByteArray(ByteBuffer.wrap(jsonParser.getBinaryValue()));
        }
    }

    private static final class InetAddressAndPortDeserializer extends JsonDeserializer<InetAddressAndPort>
    {
        @Override
        public InetAddressAndPort deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
        {
            if (jsonParser.isExpectedStartArrayToken())
            {
                jsonParser.nextToken();
                InetAddress address = InetAddress.getByAddress(jsonParser.getBinaryValue());
                jsonParser.nextToken();
                int port = jsonParser.getIntValue();
                jsonParser.nextToken();
                return InetAddressAndPort.getByAddressOverrideDefaults(address, port);
            }
            // the msgpack parser turns a string into its UTF-8 bytes in getBinaryValue(), so branch on the token type
            if (jsonParser.currentToken() == JsonToken.VALUE_STRING)
                return textualAddressAndPort(jsonParser);
            try
            {
                return InetAddressAndPort.getByAddress(jsonParser.getBinaryValue());
            }
            catch (JsonParseException e)
            {
                jsonParser.nextToken();
                return InetAddressAndPort.getByName(jsonParser.getText());
            }
        }
    }

    /**
     * DSE writes a plain address as its raw bytes (4 for IPv4, 16 for IPv6); an IP literal written as a msgpack string,
     * without a port, is accepted too.
     */
    private static final class InetAddressDeserializer extends JsonDeserializer<InetAddress>
    {
        @Override
        public InetAddress deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
        {
            // the msgpack parser turns a string into its UTF-8 bytes in getBinaryValue(), so branch on the token type
            if (jsonParser.currentToken() == JsonToken.VALUE_STRING)
                return ipLiteral(jsonParser, jsonParser.getText());
            return InetAddress.getByAddress(jsonParser.getBinaryValue());
        }
    }

    /**
     * An address written as a msgpack string: an IP literal with an optional port ({@code 10.0.0.1:7000},
     * {@code [2001:db8::1]:7000}; an IPv6 literal takes a port only in brackets, a bare one is an address without a
     * port). A host name is refused rather than resolved, so that a string value never waits on name resolution.
     */
    private static InetAddressAndPort textualAddressAndPort(JsonParser jsonParser) throws IOException
    {
        String text = jsonParser.getText();
        HostAndPort hostAndPort;
        try
        {
            hostAndPort = HostAndPort.fromString(text);
        }
        catch (IllegalArgumentException e)
        {
            throw JsonMappingException.from(jsonParser, "not an IP address with an optional port: '" + text + "'", e);
        }
        InetAddress address = ipLiteral(jsonParser, hostAndPort.getHost());
        return InetAddressAndPort.getByAddressOverrideDefaults(address, hostAndPort.hasPort() ? hostAndPort.getPort() : null);
    }

    private static InetAddress ipLiteral(JsonParser jsonParser, String text) throws JsonMappingException
    {
        try
        {
            return InetAddresses.forString(text);
        }
        catch (IllegalArgumentException e)
        {
            throw JsonMappingException.from(jsonParser, "not an IP address: '" + text + "'", e);
        }
    }

    private static final class UUIDDeserializer extends JsonDeserializer<UUID>
    {
        @Override
        public UUID deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
        {
            jsonParser.isExpectedStartArrayToken();
            jsonParser.nextToken();
            long msb = jsonParser.getLongValue();
            jsonParser.nextToken();
            long lsb = jsonParser.getLongValue();
            jsonParser.nextToken();
            return new UUID(msb, lsb);
        }
    }

    private static final class TruncationRecordDeserializer extends JsonDeserializer<CC4TruncationRecord>
    {
        @Override
        public CC4TruncationRecord deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException
        {
            jsonParser.isExpectedStartArrayToken();
            jsonParser.nextToken();
            long truncatedAt = jsonParser.getLongValue();
            jsonParser.nextToken();
            long segmentId = jsonParser.getLongValue();
            jsonParser.nextToken();
            int position = jsonParser.getIntValue();
            jsonParser.nextToken();
            return new CC4TruncationRecord(new CommitLogPosition(segmentId, position), truncatedAt);
        }
    }
}
