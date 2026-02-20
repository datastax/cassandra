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
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
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
     * do not attempt migration again.
     */
    static void archiveCC4NodesDirectory()
    {
        Path nodesDir = getCC4NodesDirectory();
        if (nodesDir == null)
            return;

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

    private static ObjectMapper createCC4ObjectMapper()
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

        // LocalInfo-specific fields
        if (cc4.broadcastAddressAndPort != null)
            info.setBroadcastAddressAndPort(cc4.broadcastAddressAndPort);
        if (cc4.listenAddressAndPort != null)
            info.setListenAddressAndPort(cc4.listenAddressAndPort);
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

        // PeerInfo-specific fields
        if (cc4.peer != null)
            info.setPeerAddressAndPort(cc4.peer);
        if (cc4.preferred != null)
            info.setPreferredAddressAndPort(cc4.preferred);

        return info;
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
        @JsonProperty("peer")
        InetAddressAndPort peer;
        @JsonProperty("preferred_ip")
        InetAddressAndPort preferred;

        @JsonCreator
        CC4PeerInfo(@JsonProperty("peer") InetAddressAndPort peer)
        {
            this.peer = peer;
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
