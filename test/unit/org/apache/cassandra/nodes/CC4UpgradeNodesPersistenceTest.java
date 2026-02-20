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
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class CC4UpgradeNodesPersistenceTest extends CQLTester
{
    private Path tempDir;
    private Path nodesDir;
    private File originalMetadataDir;
    private CC4UpgradeNodesPersistence persistence;

    @Before
    public void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory("cc4-upgrade-test");
        nodesDir = Files.createDirectories(tempDir.resolve("nodes"));
        originalMetadataDir = DatabaseDescriptor.getMetadataDirectory();
        DatabaseDescriptor.setMetadataDirectory(new File(tempDir));
        persistence = new CC4UpgradeNodesPersistence();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setMetadataDirectory(originalMetadataDir);
    }

    @Test
    public void testLoadLocalMigratesFromCC4() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        Token.TokenFactory tf = StorageService.instance.getTokenFactory();
        List<Token> tokens = Arrays.asList(tf.fromString("-100"), tf.fromString("100"));
        InetAddressAndPort broadcast = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 7000);

        Map<String, Object> cc4Local = new LinkedHashMap<>();
        cc4Local.put("host_id", serializeUUID(hostId));
        cc4Local.put("data_center", "dc1");
        cc4Local.put("rack", "rack1");
        cc4Local.put("release_version", "4.0.11");
        cc4Local.put("tokens", serializeTokens(tokens));
        cc4Local.put("broadcast_address_and_port", serializeAddress(broadcast));
        cc4Local.put("cluster_name", "MigrateCluster");

        writeMsgpack(nodesDir.resolve("local"), cc4Local);

        LocalInfo loaded = persistence.loadLocal();
        assertThat(loaded).isNotNull();
        assertThat(loaded.getHostId()).isEqualTo(hostId);
        assertThat(loaded.getDataCenter()).isEqualTo("dc1");
        assertThat(loaded.getRack()).isEqualTo("rack1");
        assertThat(loaded.getReleaseVersion()).isEqualTo(new CassandraVersion("4.0.11"));
        assertThat(loaded.getTokens()).containsExactlyInAnyOrderElementsOf(tokens);
        assertThat(loaded.getBroadcastAddressAndPort()).isEqualTo(broadcast);
        assertThat(loaded.getClusterName()).isEqualTo("MigrateCluster");

        // Verify data was persisted to system tables by reading with base NodesPersistence
        NodesPersistence basePersistence = new NodesPersistence();
        LocalInfo fromSystemTable = basePersistence.loadLocal();
        assertThat(fromSystemTable).isNotNull();
        assertThat(fromSystemTable.getHostId()).isEqualTo(hostId);
        assertThat(fromSystemTable.getDataCenter()).isEqualTo("dc1");
        assertThat(fromSystemTable.getClusterName()).isEqualTo("MigrateCluster");
    }

    @Test
    public void testLoadLocalReturnsNullWhenNoCC4Files() throws Exception
    {
        Files.delete(nodesDir);
        LocalInfo loaded = persistence.loadLocal();
        assertThat(loaded).isNull();
    }

    @Test
    public void testLoadPeersMigratesFromCC4() throws Exception
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

        List<PeerInfo> loaded = persistence.loadPeers().collect(Collectors.toList());
        assertThat(loaded).hasSize(2);

        PeerInfo p1 = loaded.stream().filter(p -> p.getHostId().equals(hostId1)).findFirst().orElse(null);
        assertThat(p1).isNotNull();
        assertThat(p1.getPeerAddressAndPort()).isEqualTo(peer1);
        assertThat(p1.getDataCenter()).isEqualTo("dc1");

        PeerInfo p2 = loaded.stream().filter(p -> p.getHostId().equals(hostId2)).findFirst().orElse(null);
        assertThat(p2).isNotNull();
        assertThat(p2.getPeerAddressAndPort()).isEqualTo(peer2);
        assertThat(p2.getDataCenter()).isEqualTo("dc2");

        // Verify data was persisted to system tables by reading with base NodesPersistence
        NodesPersistence basePersistence = new NodesPersistence();
        List<PeerInfo> fromSystemTable = basePersistence.loadPeers().collect(Collectors.toList());
        assertThat(fromSystemTable).hasSize(2);
        assertThat(fromSystemTable.stream().map(PeerInfo::getHostId).collect(Collectors.toSet()))
            .containsExactlyInAnyOrder(hostId1, hostId2);
    }

    @Test
    public void testLoadPeersReturnsEmptyWhenNoCC4Files() throws Exception
    {
        Files.delete(nodesDir);
        List<PeerInfo> loaded = persistence.loadPeers().collect(Collectors.toList());
        assertThat(loaded).isEmpty();
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
