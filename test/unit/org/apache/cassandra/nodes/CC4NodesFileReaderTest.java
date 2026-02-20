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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CC4NodesFileReaderTest extends CQLTester
{
    private Path tempDir;
    private Path nodesDir;
    private File originalMetadataDir;

    @Before
    public void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory("cc4-test");
        nodesDir = Files.createDirectories(tempDir.resolve("nodes"));
        originalMetadataDir = DatabaseDescriptor.getMetadataDirectory();
        DatabaseDescriptor.setMetadataDirectory(new File(tempDir));
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setMetadataDirectory(originalMetadataDir);
    }

    @Test
    public void testHasCC4NodesDirectoryWhenPresent()
    {
        assertThat(CC4NodesFileReader.hasCC4NodesDirectory()).isTrue();
    }

    @Test
    public void testHasCC4NodesDirectoryWhenAbsent() throws Exception
    {
        Files.delete(nodesDir);
        assertThat(CC4NodesFileReader.hasCC4NodesDirectory()).isFalse();
    }

    @Test
    public void testTryReadLocalInfo() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        UUID schemaVersion = UUID.randomUUID();
        InetAddressAndPort broadcast = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 7000);
        InetAddressAndPort listen = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 7000);
        InetAddressAndPort nativeTransport = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.3", 9042);
        Token.TokenFactory tf = StorageService.instance.getTokenFactory();
        List<Token> tokens = Arrays.asList(tf.fromString("-9223372036854775808"), tf.fromString("0"));

        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");
        cc4Local.put("release_version", "4.0.11");
        cc4Local.put("schema_version", serializeUUID(schemaVersion));
        cc4Local.put("tokens", serializeTokens(tokens));
        cc4Local.put("broadcast_address_and_port", serializeAddress(broadcast));
        cc4Local.put("listen_address_and_port", serializeAddress(listen));
        cc4Local.put("native_transport_address_and_port", serializeAddress(nativeTransport));
        cc4Local.put("cluster_name", "TestCluster");
        cc4Local.put("bootstrapped", "COMPLETED");
        cc4Local.put("partitioner", Murmur3Partitioner.class.getName());

        writeMsgpack(nodesDir.resolve("local"), cc4Local);

        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();
        assertThat(info.getHostId()).isEqualTo(hostId);
        assertThat(info.getDataCenter()).isEqualTo("dc1");
        assertThat(info.getRack()).isEqualTo("rack1");
        assertThat(info.getReleaseVersion()).isEqualTo(new CassandraVersion("4.0.11"));
        assertThat(info.getSchemaVersion()).isEqualTo(schemaVersion);
        assertThat(info.getTokens()).containsExactlyInAnyOrderElementsOf(tokens);
        assertThat(info.getBroadcastAddressAndPort()).isEqualTo(broadcast);
        assertThat(info.getListenAddressAndPort()).isEqualTo(listen);
        assertThat(info.getNativeTransportAddressAndPort()).isEqualTo(nativeTransport);
        assertThat(info.getClusterName()).isEqualTo("TestCluster");
        assertThat(info.getBootstrapState()).isEqualTo(SystemKeyspace.BootstrapState.COMPLETED);
        assertThat(info.getPartitionerClass()).isEqualTo(Murmur3Partitioner.class);
    }

    @Test
    public void testTryReadLocalInfoMissingFile()
    {
        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNull();
    }

    @Test
    public void testTryReadPeers() throws Exception
    {
        UUID hostId1 = UUID.randomUUID();
        UUID hostId2 = UUID.randomUUID();
        InetAddressAndPort peer1 = InetAddressAndPort.getByNameOverrideDefaults("10.0.0.1", 7000);
        InetAddressAndPort peer2 = InetAddressAndPort.getByNameOverrideDefaults("10.0.0.2", 7000);

        Map<String, Object> cc4Peer1 = new LinkedHashMap<>();
        cc4Peer1.put("host_id", serializeUUID(hostId1));
        cc4Peer1.put("peer", serializeAddress(peer1));
        cc4Peer1.put("data_center", "dc1");
        cc4Peer1.put("rack", "rack1");
        cc4Peer1.put("release_version", "4.0.11");

        Map<String, Object> cc4Peer2 = new LinkedHashMap<>();
        cc4Peer2.put("host_id", serializeUUID(hostId2));
        cc4Peer2.put("peer", serializeAddress(peer2));
        cc4Peer2.put("data_center", "dc2");
        cc4Peer2.put("rack", "rack2");
        cc4Peer2.put("release_version", "4.0.11");

        writeMsgpack(nodesDir.resolve("peers"), Arrays.asList(cc4Peer1, cc4Peer2));

        List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        assertThat(peers).hasSize(2);

        PeerInfo p1 = peers.stream().filter(p -> p.getHostId().equals(hostId1)).findFirst().orElse(null);
        assertThat(p1).isNotNull();
        assertThat(p1.getPeerAddressAndPort()).isEqualTo(peer1);
        assertThat(p1.getDataCenter()).isEqualTo("dc1");
        assertThat(p1.getRack()).isEqualTo("rack1");

        PeerInfo p2 = peers.stream().filter(p -> p.getHostId().equals(hostId2)).findFirst().orElse(null);
        assertThat(p2).isNotNull();
        assertThat(p2.getPeerAddressAndPort()).isEqualTo(peer2);
        assertThat(p2.getDataCenter()).isEqualTo("dc2");
        assertThat(p2.getRack()).isEqualTo("rack2");
    }

    @Test
    public void testTryReadPeersMissingFile()
    {
        List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        assertThat(peers).isEmpty();
    }

    @Test
    public void testTryReadLocalInfoCorruptFile() throws Exception
    {
        Files.write(nodesDir.resolve("local"), new byte[]{ 0x00, 0x01, 0x02 });
        assertThatThrownBy(CC4NodesFileReader::tryReadLocalInfo)
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to read CC4 local metadata");
    }

    @Test
    public void testTryReadPeersCorruptFile() throws Exception
    {
        Files.write(nodesDir.resolve("peers"), new byte[]{ 0x00, 0x01, 0x02 });
        assertThatThrownBy(CC4NodesFileReader::tryReadPeers)
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to read CC4 peers metadata");
    }

    @Test
    public void testTransactionRecovery() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");

        // Write the good data to the .old backup (simulating interrupted transaction)
        writeMsgpack(nodesDir.resolve("local.old"), cc4Local);
        // Write corrupt data to the main file
        Files.write(nodesDir.resolve("local"), new byte[]{ 0x00, 0x01 });
        // Create a .txn temp file
        Files.write(nodesDir.resolve("local.txn"), new byte[]{ 0x00 });

        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();
        assertThat(info.getHostId()).isEqualTo(hostId);
        // .txn file should be cleaned up
        assertThat(Files.exists(nodesDir.resolve("local.txn"))).isFalse();
        assertThat(Files.exists(nodesDir.resolve("local.old"))).isFalse();
    }

    @Test
    public void testTryReadLocalInfoWithTruncationRecords() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        UUID tableId1 = UUID.randomUUID();
        UUID tableId2 = UUID.randomUUID();
        long segmentId1 = 42L;
        int position1 = 1024;
        long truncatedAt1 = 1700000000000L;
        long segmentId2 = 99L;
        int position2 = 0;
        long truncatedAt2 = 1700000001000L;

        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");
        cc4Local.put("truncated_at", serializeTruncatedAt(
            tableId1, truncatedAt1, segmentId1, position1,
            tableId2, truncatedAt2, segmentId2, position2));

        writeMsgpack(nodesDir.resolve("local"), cc4Local);

        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();
        assertThat(info.getTruncationRecords()).hasSize(2);

        TruncationRecord rec1 = info.getTruncationRecords().get(tableId1);
        assertThat(rec1).isNotNull();
        assertThat(rec1.position).isEqualTo(new CommitLogPosition(segmentId1, position1));
        assertThat(rec1.truncatedAt).isEqualTo(truncatedAt1);

        TruncationRecord rec2 = info.getTruncationRecords().get(tableId2);
        assertThat(rec2).isNotNull();
        assertThat(rec2.position).isEqualTo(new CommitLogPosition(segmentId2, position2));
        assertThat(rec2.truncatedAt).isEqualTo(truncatedAt2);
    }

    @Test
    public void testTryReadLocalInfoWithEmptyTruncatedAt() throws Exception
    {
        UUID hostId = UUID.randomUUID();

        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");
        cc4Local.put("truncated_at", new LinkedHashMap<>());

        writeMsgpack(nodesDir.resolve("local"), cc4Local);

        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();
        assertThat(info.getHostId()).isEqualTo(hostId);
        assertThat(info.getTruncationRecords()).isEmpty();
    }

    @Test
    public void testTryReadLocalInfoWithLargeSegmentId() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        UUID tableId = UUID.randomUUID();
        long segmentId = Long.MAX_VALUE - 1;
        int position = Integer.MAX_VALUE;
        long truncatedAt = Long.MAX_VALUE;

        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");
        cc4Local.put("truncated_at", serializeTruncatedAt(
            tableId, truncatedAt, segmentId, position));

        writeMsgpack(nodesDir.resolve("local"), cc4Local);

        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();

        TruncationRecord rec = info.getTruncationRecords().get(tableId);
        assertThat(rec).isNotNull();
        assertThat(rec.position).isEqualTo(new CommitLogPosition(segmentId, position));
        assertThat(rec.truncatedAt).isEqualTo(truncatedAt);
    }

    @Test
    public void testArchiveCC4NodesDirectory() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");
        writeMsgpack(nodesDir.resolve("local"), cc4Local);

        assertThat(CC4NodesFileReader.hasCC4NodesDirectory()).isTrue();

        CC4NodesFileReader.archiveCC4NodesDirectory();

        assertThat(CC4NodesFileReader.hasCC4NodesDirectory()).isFalse();
        assertThat(Files.isDirectory(tempDir.resolve("nodes.migrated"))).isTrue();
        assertThat(Files.exists(tempDir.resolve("nodes.migrated").resolve("local"))).isTrue();
    }

    // ---- Helpers to write CC4-format msgpack data ----

    private static ObjectMapper createWriteMapper()
    {
        MessagePackFactory factory = new MessagePackFactory();
        factory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return new ObjectMapper(factory); // checkstyle: permit this instantiation
    }

    private static void writeMsgpack(Path path, Object data) throws IOException
    {
        ObjectMapper mapper = createWriteMapper();
        try (OutputStream out = Files.newOutputStream(path))
        {
            mapper.writeValue(out, data);
        }
    }

    private static long[] serializeUUID(UUID uuid)
    {
        return new long[]{ uuid.getMostSignificantBits(), uuid.getLeastSignificantBits() };
    }

    private static Object serializeAddress(InetAddressAndPort address)
    {
        InetAddress inet = address.getAddress();
        return new Object[]{ inet.getAddress(), address.getPort() };
    }

    /**
     * Serialize truncation records in CC4 format: map of UUID string keys to [truncatedAt, segmentId, position] arrays.
     * Accepts varargs of (tableId, truncatedAt, segmentId, position) groups.
     */
    private static Map<String, Object> serializeTruncatedAt(Object... args)
    {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 4)
        {
            UUID tableId = (UUID) args[i];
            long truncatedAt = (long) args[i + 1];
            long segmentId = (long) args[i + 2];
            int position = (int) args[i + 3];
            result.put(tableId.toString(), new Object[]{ truncatedAt, segmentId, position });
        }
        return result;
    }

    private static Collection<byte[]> serializeTokens(Collection<Token> tokens)
    {
        return tokens.stream()
                     .map(t -> {
                         ByteBuffer bb = t.getPartitioner().getTokenFactory().toByteArray(t);
                         byte[] bytes = new byte[bb.remaining()];
                         bb.get(bytes);
                         return bytes;
                     })
                     .collect(Collectors.toList());
    }
}
