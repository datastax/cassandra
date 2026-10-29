/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.nodes;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DseDialectVersions;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence.ImportState;
import org.apache.cassandra.utils.CassandraVersion;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The CC4 node-metadata reader against files written by real DSE 6.8.65 and 6.9.26 nodes (among them
 * the files of every node of a DSE 6.9.26 cluster of two data centers), and the import behaviour in
 * {@code internode_compatibility_mode: DSE_6}: DSE's files stay in place for a rollback, the import happens
 * once per version of DSE's files, and the marker recording it is bound to the host id and to the size,
 * modification time and CRC32 of the files it covers.
 */
public class CC4NodesFileReaderDseTest extends CQLTester
{
    private static final UUID HOST_6926 = UUID.fromString("55771b40-b5c1-4123-94e5-8af8ce644811");
    private static final UUID HOST_6865 = UUID.fromString("0c646ff5-7983-460a-9aad-b0cdd24df296");

    private Path tempDir;
    private File originalMetadataDir;
    private InternodeCompatibilityMode modeBefore;

    @Before
    public void setUp() throws Exception
    {
        tempDir = Files.createTempDirectory("dse-nodes-test");
        originalMetadataDir = DatabaseDescriptor.getMetadataDirectory();
        DatabaseDescriptor.setMetadataDirectory(new File(tempDir));
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // the fixtures' entries carry the storage port 7000: the tests tell it from the configured one
        assertThat(DatabaseDescriptor.getStoragePort()).as("configured storage port").isNotEqualTo(7000);
        // the system tables are shared by the tests of this class: start every test without peers
        NodesPersistence tables = new NodesPersistence();
        for (PeerInfo peer : tables.loadPeers().collect(Collectors.toList()))
            tables.deletePeer(peer.getPeerAddressAndPort());
    }

    @After
    public void tearDown()
    {
        try
        {
            // the marker of an import is written asynchronously into tempDir: let it end before the directory goes
            CC4UpgradeNodesPersistence.awaitPendingCompletion();
        }
        finally
        {
            CC4UpgradeNodesPersistence.resetPendingCompletion();
            // DseDialectVersions.seed presumes the peers it seeds to be DSE nodes: forget them
            DsePeers.reset();
            DatabaseDescriptor.setMetadataDirectory(originalMetadataDir);
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            new File(tempDir).deleteRecursive();
        }
    }

    private static InetAddressAndPort address(String host, int port) throws Exception
    {
        return InetAddressAndPort.getByNameOverrideDefaults(host, port);
    }

    @Test
    public void readsLocalOf6926() throws Exception
    {
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();
        assertThat(info.getHostId()).isEqualTo(HOST_6926);
        assertThat(info.getDataCenter()).isEqualTo("Cassandra");
        assertThat(info.getRack()).isEqualTo("rack1");
        assertThat(info.getReleaseVersion()).isEqualTo(new CassandraVersion("4.0.0.6926"));
        // the files of a node of a two-node cluster, not of a single node
        assertThat(info.getSchemaVersion()).isEqualTo(UUID.fromString("1a50fe72-f814-3069-b1f3-46399971119e"));
        assertThat(info.getTokens()).hasSize(8).contains(new LongToken(0x03a792a7aae0319fL));
        assertThat(info.getClusterName()).isEqualTo("Test Cluster");
        assertThat(info.getBootstrapState()).isEqualTo(SystemKeyspace.BootstrapState.COMPLETED);
        assertThat(info.getPartitionerClass()).isEqualTo(org.apache.cassandra.dht.Murmur3Partitioner.class);
        // DSE keys: addresses with their own port keys
        assertThat(info.getBroadcastAddressAndPort()).isEqualTo(address("127.0.0.1", 7000));
        assertThat(info.getListenAddressAndPort()).isEqualTo(address("127.0.0.1", 7000));
        // the fixture's native_transport_port, 9042, is also the configured one: readsSixteenByteIPv6Address shows the
        // key is read
        assertThat(info.getNativeTransportAddressAndPort()).isEqualTo(address("127.0.0.1", 9042));
        assertThat(CC4NodesFileReader.readLocalHostId()).isEqualTo(HOST_6926);
    }

    @Test
    public void readsLocalOf6865() throws Exception
    {
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_8_65, false);
        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info).isNotNull();
        assertThat(info.getHostId()).isEqualTo(HOST_6865);
        assertThat(info.getDataCenter()).isEqualTo("dc1");
        assertThat(info.getRack()).isEqualTo("rack1");
        assertThat(info.getReleaseVersion()).isEqualTo(new CassandraVersion("4.0.0.6865"));
        assertThat(info.getClusterName()).isEqualTo("dse-lab");
        assertThat(info.getTokens()).hasSize(8).contains(new LongToken(0xd1007c20b359779dL));
        assertThat(info.getBroadcastAddressAndPort()).isEqualTo(address("192.168.57.11", 7000));
        assertThat(info.getListenAddressAndPort()).isEqualTo(address("192.168.57.11", 7000));
        // as for 6.9.26, the fixture's native port is also the configured one
        assertThat(info.getNativeTransportAddressAndPort()).isEqualTo(address("192.168.57.11", 9042));
    }

    /**
     * The peers imported from DSE's {@code nodes/peers} carry DSE's release ({@code 4.0.0.<build>}): all of them are
     * first connected to in the pre-4.0 dialect ({@code DseDialectVersions}), so that no DSE node gets the 4.0-style
     * probe from an upgraded node.
     */
    @Test
    public void importedDsePeersAreSeededAtTheDialect() throws Exception
    {
        for (DseVersion version : DseVersion.values())
        {
            DseFixtures.installNodesDirectory(tempDir, version, false);
            List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
            assertThat(peers).as(version.name()).isNotEmpty();
            EndpointMessagingVersions versions = new EndpointMessagingVersions();
            assertThat(DseDialectVersions.seed(peers.stream(), versions))
            .as(version.name())
            .containsExactlyInAnyOrderElementsOf(peers.stream().map(PeerInfo::getPeerAddressAndPort).collect(Collectors.toList()));
            for (PeerInfo peer : peers)
            {
                assertThat(peer.getReleaseVersion()).isEqualTo(new CassandraVersion(version.releaseVersion));
                assertThat(versions.getRaw(peer.getPeerAddressAndPort())).isEqualTo(DseCompat.DSE_DIALECT);
            }
        }
    }

    @Test
    public void readsPeersOf6865() throws Exception
    {
        // the peer address takes the storage_port of the DSE entry (7000), not the configured storage port (setUp)
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_8_65, false);
        List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        assertThat(peers).hasSize(2);
        assertThat(peers).extracting(PeerInfo::getPeerAddressAndPort)
                         .containsExactlyInAnyOrder(address("192.168.57.12", 7000), address("192.168.57.13", 7000));
        Map<InetAddressAndPort, String> hostIdAndRack = Map.of(address("192.168.57.12", 7000), "9833dda0-6508-4036-aec2-2813215da12f rack2",
                                                               address("192.168.57.13", 7000), "ed39c7b2-7f97-4a0a-91fc-71398afe1d12 rack3");
        for (PeerInfo peer : peers)
        {
            assertThat(peer.getHostId() + " " + peer.getRack()).isEqualTo(hostIdAndRack.get(peer.getPeerAddressAndPort()));
            assertThat(peer.getDataCenter()).isEqualTo("dc1");
            assertThat(peer.getReleaseVersion()).isEqualTo(new CassandraVersion("4.0.0.6865"));
            assertThat(peer.getTokens()).hasSize(8);
            assertThat(peer.getPreferredAddressAndPort()).isNull(); // nil in the file
            assertThat(peer.getNativeTransportAddressAndPort().getPort()).isEqualTo(9042);
            assertThat(peer.getNativeTransportAddressAndPort().getAddress()).isEqualTo(peer.getPeerAddressAndPort().getAddress());
            LongToken first = peer.getPeerAddressAndPort().equals(address("192.168.57.12", 7000))
                              ? new LongToken(0x63402ca0cd42d970L)
                              : new LongToken(0xde12b3ab406fd975L);
            assertThat(peer.getTokens()).contains(first);
        }
    }

    @Test
    public void readsPeersOf6926() throws Exception
    {
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        assertThat(peers).hasSize(1);
        assertThat(peers.get(0).getPeerAddressAndPort()).isEqualTo(address("127.0.0.2", 7000));
        assertThat(peers.get(0).getHostId()).isEqualTo(UUID.fromString("c7285630-7be5-42e6-b839-1e318e32db74"));
        assertThat(peers.get(0).getPreferredAddressAndPort()).isNull(); // nil in the file
        assertThat(peers.get(0).getTokens()).hasSize(8).contains(new LongToken(0xf7afd9899d503f91L));
        // the second node of that cluster listened on 9043
        assertThat(peers.get(0).getNativeTransportAddressAndPort()).isEqualTo(address("127.0.0.2", 9043));
    }

    @Test
    public void emptyPeersMapMeansNoPeers() throws Exception
    {
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, true);
        try (Stream<PeerInfo> peers = CC4NodesFileReader.tryReadPeers())
        {
            assertThat(peers.collect(Collectors.toList())).isEmpty();
        }
    }

    @Test
    public void peersMapWithEntriesIsRefusedClearly() throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, true);
        // fixmap with one entry: key "a" -> nil
        Files.write(nodes.resolve("peers"), new byte[]{ (byte) 0x81, (byte) 0xa1, 'a', (byte) 0xc0 });
        assertThatThrownBy(CC4NodesFileReader::tryReadPeers)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to read CC4 peers metadata")
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("unexpected layout: a msgpack map other than DSE's empty map; an array of peer entries was expected");
    }

    @Test
    public void emptyMapsOfTheWiderMsgpackHeadersAreRefusedToo() throws Exception
    {
        // DSE writes its empty peers map as a fixmap (0x80); an empty map16 or map32 is not DSE's and is refused
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, true);
        for (byte[] map : new byte[][]{ { (byte) 0xde, 0, 0 }, { (byte) 0xdf, 0, 0, 0, 0 } })
        {
            Files.write(nodes.resolve("peers"), map);
            assertThatThrownBy(CC4NodesFileReader::tryReadPeers)
            .hasMessageContaining("Failed to read CC4 peers metadata")
            .cause()
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("unexpected layout: a msgpack map other than DSE's empty map; an array of peer entries was expected");
        }
    }

    @Test
    public void readsTheReleaseAndTheWorkloadsOfBothVersions() throws Exception
    {
        Map<DseVersion, List<InetAddress>> peerAddresses = Map.of(DseVersion.DSE_6_8_65, List.of(InetAddress.getByName("192.168.57.12"),
                                                                                                   InetAddress.getByName("192.168.57.13")),
                                                                  DseVersion.DSE_6_9_26, List.of(InetAddress.getByName("127.0.0.2")));
        for (DseVersion version : DseVersion.values())
        {
            DseFixtures.installNodesDirectory(tempDir, version, false);
            assertThat(CC4UpgradeNodesPersistence.localReleaseVersionInNodesDirectory())
            .as(version.name()).isEqualTo(new CassandraVersion(version.releaseVersion));
            DseNodeWorkloads local = CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory();
            assertThat(local.peer).as(version.name()).isNull();
            assertThat(local.workloads).as(version.name()).containsExactly(DseNodeWorkloads.CASSANDRA);
            assertThat(local.graph).as(version.name()).isFalse();
            assertThat(local.workload).as(version.name()).isEqualTo("Cassandra");

            List<DseNodeWorkloads> peers = CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory();
            assertThat(peers).extracting(p -> p.peer).as(version.name()).containsExactlyInAnyOrderElementsOf(peerAddresses.get(version));
            for (DseNodeWorkloads peer : peers)
            {
                assertThat(peer.workloads).as(version.name()).containsExactly(DseNodeWorkloads.CASSANDRA);
                assertThat(peer.graph).as(version.name()).isFalse();
                assertThat(peer.workload).as(version.name()).isEqualTo("Cassandra");
            }
        }
    }

    @Test
    public void readsNoPeerWorkloadsFromAnEmptyPeersMap() throws Exception
    {
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, true);
        assertThat(CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory()).isEmpty();
        assertThat(CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory().workloads).containsExactly(DseNodeWorkloads.CASSANDRA);
    }

    @Test
    public void readsNothingWithoutANodesDirectory()
    {
        assertThat(Files.exists(tempDir.resolve("nodes"))).isFalse();
        assertThat(CC4UpgradeNodesPersistence.localReleaseVersionInNodesDirectory()).isNull();
        assertThat(CC4UpgradeNodesPersistence.localWorkloadsInNodesDirectory()).isNull();
        assertThat(CC4UpgradeNodesPersistence.peerWorkloadsInNodesDirectory()).isEmpty();
        assertThat(CC4NodesFileReader.readLocalHostId()).isNull();
    }

    @Test
    public void aDamagedFileFailsTheReadersWithItsPath() throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        byte[] damaged = { (byte) 0xc1 }; // a msgpack byte that is never used
        Files.write(nodes.resolve("local"), damaged);
        Files.write(nodes.resolve("peers"), damaged);
        assertThatThrownBy(CC4UpgradeNodesPersistence::localReleaseVersionInNodesDirectory)
        .hasMessage("Failed to read the release version from the node metadata file " + nodes.resolve("local"));
        assertThatThrownBy(CC4NodesFileReader::readLocalHostId)
        .hasMessage("Failed to read the host id from the node metadata file " + nodes.resolve("local"));
        assertThatThrownBy(CC4UpgradeNodesPersistence::localWorkloadsInNodesDirectory)
        .hasMessage("Failed to read the DSE workloads from the node metadata file " + nodes.resolve("local"));
        assertThatThrownBy(CC4UpgradeNodesPersistence::peerWorkloadsInNodesDirectory)
        .hasMessage("Failed to read the DSE workloads from the node metadata file " + nodes.resolve("peers"));
    }

    @Test
    public void readsTruncationRecordOf6926() throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, true);
        Files.copy(DseFixtures.nodesLocalAfterTruncate(), nodes.resolve("local"), StandardCopyOption.REPLACE_EXISTING);
        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        UUID tableId = UUID.fromString("467ee541-b828-11f1-9f9f-754297a7ef99");
        assertThat(info.getTruncationRecords()).containsKey(tableId);
        TruncationRecord record = info.getTruncationRecords().get(tableId);
        assertThat(record.position).isEqualTo(new CommitLogPosition(1790256233261L, 253237));
        assertThat(record.truncatedAt).isEqualTo(1790262194516L);
    }

    // ---- DSE address keys not covered by the fixtures ----

    @Test
    public void nativeAddressWithoutPortTakesTheConfiguredPort() throws Exception
    {
        writeLocal(Collections.singletonMap("native_transport_address", new byte[]{ 10, 0, 0, 1 }));
        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info.getNativeTransportAddressAndPort()).isEqualTo(address("10.0.0.1", DatabaseDescriptor.getNativeTransportPort()));
    }

    @Test
    public void readsSixteenByteIPv6Address() throws Exception
    {
        Map<String, Object> local = new LinkedHashMap<>();
        local.put("native_transport_address", InetAddress.getByName("2001:db8::1").getAddress());
        local.put("native_transport_port", 9142);
        local.put("broadcast_address", InetAddress.getByName("2001:db8::2").getAddress());
        local.put("storage_port", 7000);
        writeLocal(local);
        LocalInfo info = CC4NodesFileReader.tryReadLocalInfo();
        assertThat(info.getNativeTransportAddressAndPort()).isEqualTo(address("2001:db8::1", 9142));
        assertThat(info.getBroadcastAddressAndPort()).isEqualTo(address("2001:db8::2", 7000));
    }

    @Test
    public void readsTextualAddress() throws Exception
    {
        Map<String, Object> local = new LinkedHashMap<>();
        local.put("native_transport_address", "10.0.0.2");
        local.put("native_transport_port", 9142);
        writeLocal(local);
        assertThat(CC4NodesFileReader.tryReadLocalInfo().getNativeTransportAddressAndPort()).isEqualTo(address("10.0.0.2", 9142));

        // a textual peer address goes through the address-and-port deserializer
        Map<String, Object> peer = new LinkedHashMap<>();
        peer.put("peer", "10.0.0.6");
        peer.put("storage_port", 7000);
        writeMsgpack(tempDir.resolve("nodes").resolve("peers"), Collections.singletonList(peer));
        assertThat(CC4NodesFileReader.tryReadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("10.0.0.6", 7000));

        // an IPv6 literal with its port, in an entry without DSE keys: the port is the one of the text
        writeMsgpack(tempDir.resolve("nodes").resolve("peers"), Collections.singletonList(Collections.singletonMap("peer", "[2001:db8::6]:7001")));
        assertThat(CC4NodesFileReader.tryReadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("2001:db8::6", 7001));

        // a DSE entry (with storage_port) overrides the port of a textual address
        Map<String, Object> dsePeer = new LinkedHashMap<>();
        dsePeer.put("peer", "10.0.0.6:7001");
        dsePeer.put("storage_port", 7000);
        writeMsgpack(tempDir.resolve("nodes").resolve("peers"), Collections.singletonList(dsePeer));
        assertThat(CC4NodesFileReader.tryReadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("10.0.0.6", 7000));

        // a bare IPv6 literal has no port: its last group is part of the address, the port is the configured one
        writeMsgpack(tempDir.resolve("nodes").resolve("peers"), Collections.singletonList(Collections.singletonMap("peer", "2001:db8::1:7000")));
        assertThat(CC4NodesFileReader.tryReadPeers().map(PeerInfo::getPeerAddressAndPort))
        .containsExactly(address("2001:db8::1:7000", DatabaseDescriptor.getStoragePort()));
    }

    @Test
    public void aHostNameIsRefusedRatherThanResolved() throws Exception
    {
        writeLocal(Collections.singletonMap("native_transport_address", "localhost"));
        assertThatThrownBy(CC4NodesFileReader::tryReadLocalInfo)
        .hasMessageContaining("Failed to read CC4 local metadata")
        .hasStackTraceContaining("not an IP address: 'localhost'");

        Map<String, Object> peer = new LinkedHashMap<>();
        peer.put("peer", "localhost:7000");
        peer.put("storage_port", 7000);
        writeMsgpack(tempDir.resolve("nodes").resolve("peers"), Collections.singletonList(peer));
        assertThatThrownBy(CC4NodesFileReader::tryReadPeers)
        .hasMessageContaining("Failed to read CC4 peers metadata")
        .hasStackTraceContaining("not an IP address: 'localhost'");

        // a plain-address key takes neither a port nor a binary value of a length other than 4 or 16
        writeLocal(Collections.singletonMap("broadcast_address", "10.0.0.1:7000"));
        assertThatThrownBy(CC4NodesFileReader::tryReadLocalInfo)
        .hasMessageContaining("Failed to read CC4 local metadata")
        .hasStackTraceContaining("not an IP address: '10.0.0.1:7000'");
        writeLocal(Collections.singletonMap("broadcast_address", new byte[]{ 10, 0, 0, 1, 5 }));
        assertThatThrownBy(CC4NodesFileReader::tryReadLocalInfo)
        .hasMessageContaining("Failed to read CC4 local metadata")
        .hasRootCauseInstanceOf(UnknownHostException.class);

        // neither a port out of range nor an unclosed IPv6 literal is read
        for (String text : new String[]{ "10.0.0.1:99999", "[2001:db8::1" })
        {
            writeMsgpack(tempDir.resolve("nodes").resolve("peers"), Collections.singletonList(Collections.singletonMap("peer", text)));
            assertThatThrownBy(CC4NodesFileReader::tryReadPeers)
            .hasMessageContaining("Failed to read CC4 peers metadata")
            .hasStackTraceContaining("not an IP address with an optional port: '" + text + "'");
        }
    }

    @Test
    public void aDirectoryInPlaceOfTheMarkerIsNoMarker() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        Files.createDirectory(nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6865)));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.NOT_IMPORTED);
        assertThat(importBoth().getHostId()).isEqualTo(HOST_6926);
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
        // ignored, not removed
        assertThat(Files.isDirectory(nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6865)))).isTrue();
    }

    @Test
    public void aDirectoryUnderTheExpectedMarkerNameFailsTheReload() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        Path marker = nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926));
        Files.createDirectory(marker);
        // a temporary file left by an earlier failed write: the failed write removes it too
        Files.write(nodes.resolve(".hcd-import-marker.tmp"), new byte[]{ 'x' });
        importBothWithoutWaiting();

        assertThatThrownBy(() -> new CC4UpgradeNodesPersistence().loadLocal())
        .hasMessage("Failed to complete the node metadata import: flushing the system tables, then writing the import marker " + marker)
        .hasRootCauseMessage(marker + " is not a regular file, so it cannot be the import marker: remove it");
        assertThat(Files.isDirectory(marker)).isTrue();
        assertThat(Files.exists(nodes.resolve(".hcd-import-marker.tmp"))).isFalse();
        CC4UpgradeNodesPersistence.resetPendingCompletion();
    }

    @Test
    public void aDamagedMarkerImportsAgain() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        Path marker = nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926));
        Files.write(marker, new byte[]{ 'l', 'o', 'c', (byte) 0xff, '\n' });
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        importBoth();
        assertThat(new String(Files.readAllBytes(marker), StandardCharsets.UTF_8)).contains("local=739:", "peers=483:");
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void rpcAddressIsTheLastFallbackForTheNativeAddress() throws Exception
    {
        Map<String, Object> local = new LinkedHashMap<>();
        local.put("rpc_address", new byte[]{ 10, 0, 0, 3 });
        local.put("native_transport_port", 9142);
        writeLocal(local);
        assertThat(CC4NodesFileReader.tryReadLocalInfo().getNativeTransportAddressAndPort()).isEqualTo(address("10.0.0.3", 9142));

        local.put("native_transport_address", new byte[]{ 10, 0, 0, 4 });
        writeLocal(local);
        assertThat(CC4NodesFileReader.tryReadLocalInfo().getNativeTransportAddressAndPort()).isEqualTo(address("10.0.0.4", 9142));
    }

    @Test
    public void dsePeerWithoutStoragePortTakesTheConfiguredPort() throws Exception
    {
        Map<String, Object> peer = new LinkedHashMap<>();
        peer.put("peer", new byte[]{ 10, 0, 0, 5 });
        peer.put("native_transport_address", new byte[]{ 10, 0, 0, 5 });
        peer.put("preferred_ip", new byte[]{ 10, 1, 0, 5 });
        Path nodes = Files.createDirectories(tempDir.resolve("nodes"));
        writeMsgpack(nodes.resolve("peers"), Collections.singletonList(peer));
        List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        int storagePort = DatabaseDescriptor.getStoragePort();
        assertThat(peers).hasSize(1);
        assertThat(peers.get(0).getPeerAddressAndPort()).isEqualTo(address("10.0.0.5", storagePort));
        assertThat(peers.get(0).getPreferredAddressAndPort()).isEqualTo(address("10.1.0.5", storagePort));

        // with storage_port, the preferred address takes it too
        peer.put("storage_port", 7000);
        writeMsgpack(nodes.resolve("peers"), Collections.singletonList(peer));
        peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
        assertThat(peers.get(0).getPeerAddressAndPort()).isEqualTo(address("10.0.0.5", 7000));
        assertThat(peers.get(0).getPreferredAddressAndPort()).isEqualTo(address("10.1.0.5", 7000));
    }

    // ---- the import in DSE_6 mode ----

    @Test
    public void dse6ModeKeepsTheFilesAndImportsOnce() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        Map<String, FileTime> modified = new HashMap<>();
        for (String name : new String[]{ "local", "peers" })
            modified.put(name, Files.getLastModifiedTime(nodes.resolve(name)));

        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        LocalInfo local = persistence.loadLocal();
        assertThat(local.getHostId()).isEqualTo(HOST_6926);
        List<PeerInfo> peers = persistence.loadPeers().collect(Collectors.toList());
        assertThat(peers).extracting(PeerInfo::getPeerAddressAndPort).containsExactly(address("127.0.0.2", 7000));
        CC4UpgradeNodesPersistence.awaitPendingCompletion();

        // the DSE files are untouched, no nodes.migrated, and a marker records the import and the files it covers
        assertThat(Files.isDirectory(nodes)).isTrue();
        assertThat(Files.readAllBytes(nodes.resolve("local"))).isEqualTo(Files.readAllBytes(DseFixtures.nodesLocal(DseVersion.DSE_6_9_26)));
        assertThat(Files.readAllBytes(nodes.resolve("peers"))).isEqualTo(Files.readAllBytes(DseFixtures.nodesPeers(DseVersion.DSE_6_9_26)));
        for (String name : new String[]{ "local", "peers" })
            assertThat(Files.getLastModifiedTime(nodes.resolve(name))).as(name).isEqualTo(modified.get(name));
        assertThat(Files.exists(tempDir.resolve("nodes.migrated"))).isFalse();
        Path marker = nodes.resolve(CC4UpgradeNodesPersistence.IMPORT_MARKER_PREFIX + HOST_6926);
        assertThat(Files.exists(marker)).isTrue();
        assertThat(new String(Files.readAllBytes(marker), StandardCharsets.UTF_8)).contains("local=739:", "peers=483:");
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);

        // what this node learns afterwards lands in the system tables only
        persistence.saveLocal(local.setRack("rack-hcd"));
        persistence.savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        // a second start (and Nodes.reload()): the files are still there but the import is skipped
        CC4UpgradeNodesPersistence again = new CC4UpgradeNodesPersistence();
        assertThat(again.loadLocal().getRack()).isEqualTo("rack-hcd");
        assertThat(again.loadPeers().map(PeerInfo::getPeerAddressAndPort))
        .containsExactlyInAnyOrder(address("127.0.0.2", 7000), address("127.0.0.9", 7000));
        assertThat(CC4NodesFileReader.hasCC4NodesDirectory()).isTrue();
    }

    @Test
    public void dseRunningAfterTheImportMakesItRunAgain() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        persistence.saveLocal(persistence.loadLocal().setRack("rack-hcd"));
        persistence.savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        // rollback: DSE rewrote nodes/local with the same content (new modification time only)
        Path local = nodes.resolve("local");
        Files.setLastModifiedTime(local, FileTime.fromMillis(Files.getLastModifiedTime(local).toMillis() + 10_000));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        CC4UpgradeNodesPersistence second = new CC4UpgradeNodesPersistence();
        assertThat(second.loadLocal().getRack()).isEqualTo("rack1");
        // DSE's peers file is authoritative: the peer learnt after the import and absent from it is gone
        assertThat(second.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(new NodesPersistence().loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);

        // rollback again: DSE truncated a table meanwhile (a different nodes/local)
        Files.copy(DseFixtures.nodesLocalAfterTruncate(), local, StandardCopyOption.REPLACE_EXISTING);
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        LocalInfo third = new CC4UpgradeNodesPersistence().loadLocal();
        assertThat(third.getHostId()).isEqualTo(HOST_6926);
        assertThat(third.getTruncationRecords()).containsKey(UUID.fromString("467ee541-b828-11f1-9f9f-754297a7ef99"));
        new CC4UpgradeNodesPersistence().loadPeers().collect(Collectors.toList());
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(new String(Files.readAllBytes(nodes.resolve(CC4UpgradeNodesPersistence.IMPORT_MARKER_PREFIX + HOST_6926)), StandardCharsets.UTF_8))
        .contains("local=801:");
        assertThat(new NodesPersistence().loadLocal().getTruncationRecords()).containsKey(UUID.fromString("467ee541-b828-11f1-9f9f-754297a7ef99"));
    }

    @Test
    public void validMarkerWithLostSystemTablesImportsAgain() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);

        // the system keyspace is lost while metadata_directory survives: no new identity, DSE's is imported again
        QueryProcessor.executeInternal("DELETE FROM system.local WHERE key = 'local'");
        assertThat(new NodesPersistence().loadLocal()).isNull();
        CC4UpgradeNodesPersistence restart = new CC4UpgradeNodesPersistence();
        assertThat(restart.loadLocal().getHostId()).isEqualTo(HOST_6926);
        assertThat(restart.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(new NodesPersistence().loadLocal().getHostId()).isEqualTo(HOST_6926);

        // the tables hold another identity (another node's system keyspace): same outcome, and that node's
        // truncation records, whose positions refer to its own commit log, are not merged in
        NodesPersistence tables = new NodesPersistence();
        UUID foreignTable = UUID.randomUUID();
        TruncationRecord foreignRecord = new TruncationRecord(new CommitLogPosition(1790300000000L, 42), 1790300000123L);
        tables.saveLocal(tables.loadLocal().setHostId(UUID.randomUUID()).setTruncationRecords(Collections.singletonMap(foreignTable, foreignRecord)));
        LocalInfo reimported = new CC4UpgradeNodesPersistence().loadLocal();
        assertThat(reimported.getHostId()).isEqualTo(HOST_6926);
        assertThat(reimported.getTruncationRecords()).doesNotContainKey(foreignTable);
        assertThat(new NodesPersistence().loadLocal().getHostId()).isEqualTo(HOST_6926);
        assertThat(new NodesPersistence().loadLocal().getTruncationRecords()).doesNotContainKey(foreignTable);
    }

    @Test
    public void staleImportWithoutPeersFileKeepsThePeers() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        new NodesPersistence().savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        Files.delete(nodes.resolve("peers"));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        importBoth();
        assertThat(new NodesPersistence().loadPeers().map(PeerInfo::getPeerAddressAndPort))
        .containsExactlyInAnyOrder(address("127.0.0.2", 7000), address("127.0.0.9", 7000));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void staleImportKeepsTheLaterTruncationRecord() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        UUID truncatedByDse = UUID.fromString("467ee541-b828-11f1-9f9f-754297a7ef99");
        UUID truncatedOnHcd = UUID.randomUUID();
        TruncationRecord laterOnHcd = new TruncationRecord(new CommitLogPosition(1790300000000L, 42), 1790300000123L);
        TruncationRecord onlyOnHcd = new TruncationRecord(new CommitLogPosition(1790300000000L, 7), 1790300000100L);
        Map<UUID, TruncationRecord> records = new HashMap<>();
        records.put(truncatedByDse, laterOnHcd);
        records.put(truncatedOnHcd, onlyOnHcd);
        NodesPersistence tables = new NodesPersistence();
        tables.saveLocal(tables.loadLocal().setTruncationRecords(records));

        // rollback: DSE's nodes/local now holds its own, earlier, record for the first table
        Files.copy(DseFixtures.nodesLocalAfterTruncate(), nodes.resolve("local"), StandardCopyOption.REPLACE_EXISTING);
        LocalInfo local = new CC4UpgradeNodesPersistence().loadLocal();
        assertThat(local.getTruncationRecords()).containsEntry(truncatedByDse, laterOnHcd).containsEntry(truncatedOnHcd, onlyOnHcd);
        assertThat(new NodesPersistence().loadLocal().getTruncationRecords())
        .containsEntry(truncatedByDse, laterOnHcd).containsEntry(truncatedOnHcd, onlyOnHcd);

        // a record of DSE later than the one of the tables wins
        TruncationRecord earlierOnHcd = new TruncationRecord(new CommitLogPosition(1790200000000L, 1), 1790200000000L);
        tables.saveLocal(tables.loadLocal().setTruncationRecords(Collections.singletonMap(truncatedByDse, earlierOnHcd)));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        assertThat(new CC4UpgradeNodesPersistence().loadLocal().getTruncationRecords().get(truncatedByDse))
        .isEqualTo(new TruncationRecord(new CommitLogPosition(1790256233261L, 253237), 1790262194516L));
    }

    @Test
    public void markerOfAnotherHostIdIsNotTrusted() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_8_65, false);
        // a nodes/ directory restored from another node's backup: its marker even fingerprints these files
        Path foreign = nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926));
        Files.write(foreign, CC4UpgradeNodesPersistence.fingerprint(nodes).getBytes(StandardCharsets.UTF_8));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);

        assertThat(importBoth().getHostId()).isEqualTo(HOST_6865);
        assertThat(Files.exists(foreign)).isFalse();
        assertThat(Files.exists(nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6865)))).isTrue();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void crashBeforeTheMarkerMeansAnIdempotentReimport() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        // the first start dies after the local import, before the peers and the marker
        new CC4UpgradeNodesPersistence().loadLocal();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.NOT_IMPORTED);
        assertThat(new NodesPersistence().loadLocal().getHostId()).isEqualTo(HOST_6926);

        CC4UpgradeNodesPersistence restart = new CC4UpgradeNodesPersistence();
        assertThat(restart.loadLocal().getHostId()).isEqualTo(HOST_6926);
        assertThat(restart.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(new NodesPersistence().loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(Files.exists(nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926)))).isTrue();
    }

    @Test
    public void theLoadsNeverFlushAndTheMarkerFollowsTheFlush() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        Path marker = nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926));
        // the loads run while Nodes is initialised, when no flush can end: they must return without one
        DeferredSyncPersistence persistence = new DeferredSyncPersistence(() -> Files.exists(marker));
        try
        {
            assertThat(persistence.loadLocal().getHostId()).isEqualTo(HOST_6926);
            assertThat(persistence.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));

            // the flush waits for the loads to return: until then no marker, and a crash here means an idempotent re-import
            assertThat(Files.exists(marker)).isFalse();
            assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.NOT_IMPORTED);
        }
        finally
        {
            persistence.loadsReturned();
        }
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(persistence.flushes).containsExactly("local+peers flushed before the certification");
        assertThat(Files.exists(marker)).isTrue();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void aFailedFlushWritesNoMarkerAndFailsTheReload() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        CC4UpgradeNodesPersistence failing = new CC4UpgradeNodesPersistence()
        {
            @Override
            protected void flushImported(boolean local, boolean peers)
            {
                throw new IllegalStateException("flush refused by the test");
            }
        };
        assertThat(failing.loadLocal().getHostId()).isEqualTo(HOST_6926);
        assertThat(failing.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));

        // the reload of the start waits for the completion and fails with it
        assertThatThrownBy(() -> new CC4UpgradeNodesPersistence().loadLocal())
        .hasMessage("Failed to complete the node metadata import: flushing the system tables, then writing the import marker " +
                    nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926)))
        .hasRootCauseMessage("flush refused by the test");
        assertThat(Files.exists(nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926)))).isFalse();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.NOT_IMPORTED);
        CC4UpgradeNodesPersistence.resetPendingCompletion();
    }

    @Test
    public void aFailedMarkerWriteFailsTheReload() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        // a directory where the temporary marker goes: opening it for writing fails whoever runs the test
        Files.createDirectory(nodes.resolve(".hcd-import-marker.tmp"));
        Path marker = nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926));
        importBothWithoutWaiting();

        assertThatThrownBy(() -> new CC4UpgradeNodesPersistence().loadLocal())
        .hasMessage("Failed to complete the node metadata import: flushing the system tables, then writing the import marker " + marker)
        .hasCauseInstanceOf(RuntimeException.class)
        .cause().hasMessage("Failed to write the import marker " + marker);
        assertThat(Files.exists(marker)).isFalse();
        assertThat(Files.exists(nodes.resolve(".hcd-import-marker.tmp"))).isFalse();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.NOT_IMPORTED);
        CC4UpgradeNodesPersistence.resetPendingCompletion();
    }

    @Test
    public void markerWithoutLocalFileNamesUnknown() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        Files.delete(nodes.resolve("local"));
        importBoth();
        Path marker = nodes.resolve(CC4UpgradeNodesPersistence.IMPORT_MARKER_PREFIX + "unknown");
        assertThat(Files.exists(marker)).isTrue();
        assertThat(new String(Files.readAllBytes(marker), StandardCharsets.UTF_8)).contains("local=absent", "peers=483:");
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void noneModeArchivesAsBefore() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_8_65, false);
        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        persistence.loadLocal();
        persistence.loadPeers().collect(Collectors.toList());
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(Files.exists(nodes)).isFalse();
        assertThat(Files.isDirectory(tempDir.resolve("nodes.migrated"))).isTrue();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.NOT_IMPORTED);
        try (Stream<Path> migrated = Files.list(tempDir.resolve("nodes.migrated")))
        {
            assertThat(migrated.map(p -> p.getFileName().toString())).containsExactlyInAnyOrder("local", "peers");
        }
    }

    @Test
    public void noneModeReadsDse69sEmptyPeersMapAndArchives() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, true);
        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        assertThat(persistence.loadLocal().getHostId()).isEqualTo(HOST_6926);
        // no peer in the file: the system tables, emptied by setUp, hold none either
        assertThat(persistence.loadPeers()).isEmpty();
        assertThat(Files.exists(nodes)).isFalse();
        try (Stream<Path> migrated = Files.list(tempDir.resolve("nodes.migrated")))
        {
            assertThat(migrated.map(p -> p.getFileName().toString())).containsExactlyInAnyOrder("local", "peers");
        }
    }

    @Test
    public void droppingTheFlagArchivesAnImportedDirectory() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        persistence.saveLocal(persistence.loadLocal().setRack("rack-hcd"));

        // the flag is dropped: the import is not repeated, the directory moves to nodes.migrated with its marker
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        CC4UpgradeNodesPersistence flagDropped = new CC4UpgradeNodesPersistence();
        assertThat(flagDropped.loadLocal().getRack()).isEqualTo("rack-hcd");
        assertThat(flagDropped.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(Files.exists(tempDir.resolve("nodes"))).isFalse();
        Path migrated = tempDir.resolve("nodes.migrated");
        assertThat(Files.exists(migrated.resolve("local"))).isTrue();
        assertThat(Files.exists(migrated.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926)))).isTrue();
        assertThat(CC4NodesFileReader.hasCC4NodesDirectory()).isFalse();
    }

    @Test
    public void droppingTheFlagAfterARollbackImportsAgainThenArchives() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        new NodesPersistence().savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        // rollback: DSE truncated a table and rewrote nodes/local; then the flag is dropped
        Files.copy(DseFixtures.nodesLocalAfterTruncate(), nodes.resolve("local"), StandardCopyOption.REPLACE_EXISTING);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        CC4UpgradeNodesPersistence flagDropped = new CC4UpgradeNodesPersistence();
        assertThat(flagDropped.loadLocal().getTruncationRecords()).containsKey(UUID.fromString("467ee541-b828-11f1-9f9f-754297a7ef99"));
        // DSE's files are authoritative again: the peer learnt after the import is gone, and the directory is archived
        assertThat(flagDropped.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(new NodesPersistence().loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(Files.exists(nodes)).isFalse();
        assertThat(Files.exists(tempDir.resolve("nodes.migrated").resolve("local"))).isTrue();
    }

    /**
     * DSE writes a nodes file in four steps: the new content to {@code .txn}, the file renamed to {@code .old}, the
     * {@code .txn} renamed to the file, the {@code .old} deleted. A crash after the second step leaves no file and the
     * backup of the imported one, with its modification time: the recovery restores it, so that after it the files
     * match the marker again, and the import still runs whole (local and peers, then the flush and the marker).
     */
    @Test
    public void aDseWriteInterruptedBeforeItsRenameImportsTheRestoredFileAgain() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
        new NodesPersistence().savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        Files.copy(DseFixtures.nodesLocalAfterTruncate(), nodes.resolve("local.txn"));
        Files.move(nodes.resolve("local"), nodes.resolve("local.old"));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);

        DeferredSyncPersistence restart = new DeferredSyncPersistence(() -> false);
        try
        {
            LocalInfo local = restart.loadLocal();
            assertThat(local.getHostId()).isEqualTo(HOST_6926);
            // the content of the interrupted write is gone, as DSE itself would have discarded it
            assertThat(local.getTruncationRecords()).doesNotContainKey(UUID.fromString("467ee541-b828-11f1-9f9f-754297a7ef99"));
            assertThat(Files.exists(nodes.resolve("local.old"))).isFalse();
            assertThat(Files.exists(nodes.resolve("local.txn"))).isFalse();
            assertThat(restart.loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        }
        finally
        {
            restart.loadsReturned();
        }
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        assertThat(restart.flushes).containsExactly("local+peers flushed before the certification");
        // the peers are imported again too: the peer learnt after the import is gone
        assertThat(new NodesPersistence().loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void aStaleImportOfAnEmptyPeersMapRemovesEveryPeer() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        new NodesPersistence().savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        // rollback: DSE now lists no peer
        Files.copy(DseFixtures.nodesPeersEmpty(), nodes.resolve("peers"), StandardCopyOption.REPLACE_EXISTING);
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        importBoth();
        assertThat(new NodesPersistence().loadPeers()).isEmpty();
        assertThat(new String(Files.readAllBytes(nodes.resolve(CC4UpgradeNodesPersistence.markerName(HOST_6926))), StandardCharsets.UTF_8))
        .contains("peers=1:");
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    /**
     * A crash after the third step of DSE's write of {@code nodes/peers}: the new file is complete and the backup of
     * the imported one is still there. The recovery restores the backup, as DSE's own read does.
     */
    @Test
    public void aDseWriteInterruptedAfterItsRenameImportsTheRestoredPeersAgain() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        new NodesPersistence().savePeer(new PeerInfo().setPeerAddressAndPort(address("127.0.0.9", 7000)).setHostId(UUID.randomUUID()));

        Files.move(nodes.resolve("peers"), nodes.resolve("peers.old"));
        Files.copy(DseFixtures.nodesPeersEmpty(), nodes.resolve("peers"));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
        importBoth();
        assertThat(Files.exists(nodes.resolve("peers.old"))).isFalse();
        // DSE's restored peers file is authoritative: the peer learnt after the import is gone
        assertThat(new NodesPersistence().loadPeers().map(PeerInfo::getPeerAddressAndPort)).containsExactly(address("127.0.0.2", 7000));
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);
    }

    @Test
    public void aDamagedLocalFileWithAMarkerFailsTheLoad() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();

        // no local.old to recover from: the marker cannot be checked, and the start fails rather than guess
        Files.write(nodes.resolve("local"), new byte[]{ (byte) 0xc1 });
        assertThatThrownBy(() -> new CC4UpgradeNodesPersistence().loadLocal())
        .hasMessage("Failed to read the host id from the node metadata file " + nodes.resolve("local"));
    }

    @Test
    public void aRewriteOfTheSameSizeAndTimeIsSeenByTheChecksum() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path nodes = DseFixtures.installNodesDirectory(tempDir, DseVersion.DSE_6_9_26, false);
        importBoth();
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.DONE);

        // one byte of nodes/peers changes, its size and modification time stay
        Path peers = nodes.resolve("peers");
        FileTime modified = Files.getLastModifiedTime(peers);
        byte[] content = Files.readAllBytes(peers);
        content[content.length - 1] ^= 1;
        Files.write(peers, content);
        Files.setLastModifiedTime(peers, modified);
        assertThat(CC4UpgradeNodesPersistence.importState()).isEqualTo(ImportState.STALE);
    }

    /**
     * The files of the second node of a mixed DSE 6.8.65 cluster: one DSE peer, and one recorded with the release of
     * this software. Both are imported with their release, and the files stay in place.
     */
    @Test
    public void dse6ModeImportsTheMixedPhaseFilesOfOneDataCenter() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        UUID hostId = UUID.fromString("9833dda0-6508-4036-aec2-2813215da12f");
        importIntoEmptyTables(tempDir.resolve("mixed"), DseFixtures.nodesDirectoryMixed(), hostId);

        NodesPersistence tables = new NodesPersistence();
        LocalInfo local = tables.loadLocal();
        assertThat(local.getHostId()).isEqualTo(hostId);
        assertThat(local.getBroadcastAddressAndPort()).isEqualTo(address("192.168.57.12", 7000));
        assertThat(local.getDataCenter()).isEqualTo("dc1");
        assertThat(local.getRack()).isEqualTo("rack2");
        assertThat(local.getReleaseVersion()).isEqualTo(new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion));
        Map<InetAddressAndPort, UUID> hostIds = Map.of(address("192.168.57.11", 7000), HOST_6865,
                                                       address("192.168.57.13", 7000), UUID.fromString("ed39c7b2-7f97-4a0a-91fc-71398afe1d12"));
        Map<InetAddressAndPort, CassandraVersion> releases = new HashMap<>();
        for (PeerInfo peer : tables.loadPeers().collect(Collectors.toList()))
        {
            assertThat(peer.getHostId()).as(peer.getPeerAddressAndPort().toString()).isEqualTo(hostIds.get(peer.getPeerAddressAndPort()));
            assertThat(peer.getDataCenter()).as(peer.getPeerAddressAndPort().toString()).isEqualTo("dc1");
            releases.put(peer.getPeerAddressAndPort(), peer.getReleaseVersion());
        }
        assertThat(releases).isEqualTo(Map.of(address("192.168.57.11", 7000), new CassandraVersion(DseVersion.DSE_6_8_65.releaseVersion),
                                              address("192.168.57.13", 7000), new CassandraVersion("5.0.7.0-SNAPSHOT")));
    }

    /**
     * The nodes files of a DSE 6.9.26 cluster of two data centers, each with a rack of two nodes and a rack of one:
     * every node's own data center, rack and host id, and those of its five peers, as {@code nodetool status} showed them
     * on the cluster; every node's files give the same topology.
     */
    @Test
    public void readsTheDataCenterAndRackOfEveryNodeOfTwoDataCenters() throws Exception
    {
        for (int node = 1; node <= DseFixtures.TWO_DC_NODES; node++)
        {
            DseFixtures.installNodesDirectory(tempDir, DseFixtures.nodesDirectoryTwoDataCenters(node));
            LocalInfo local = CC4NodesFileReader.tryReadLocalInfo();
            assertThat(local).as("node %s", node).isNotNull();
            TwoDcNode self = TwoDcNode.of(node);
            assertThat(local.getBroadcastAddressAndPort()).as("node %s", node).isEqualTo(self.address());
            assertThat(local.getHostId()).as("node %s", node).isEqualTo(self.hostId);
            assertThat(local.getDataCenter()).as("node %s", node).isEqualTo(self.dataCenter);
            assertThat(local.getRack()).as("node %s", node).isEqualTo(self.rack);
            assertThat(local.getReleaseVersion()).as("node %s", node).isEqualTo(new CassandraVersion(DseVersion.DSE_6_9_26.releaseVersion));
            assertThat(local.getClusterName()).as("node %s", node).isEqualTo("dse-lab");
            assertThat(local.getTokens()).as("node %s", node).containsExactlyInAnyOrderElementsOf(self.tokens);

            List<PeerInfo> peers = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
            assertThat(peers).as("peers of node %s", node).hasSize(DseFixtures.TWO_DC_NODES - 1);
            for (PeerInfo peer : peers)
                TwoDcNode.assertPeer(node, peer, DseVersion.DSE_6_9_26.releaseVersion);

            // the topology as this node's files give it: per data center, a rack of two nodes and a rack of one
            Map<InetAddressAndPort, String> placement = new HashMap<>();
            placement.put(local.getBroadcastAddressAndPort(), local.getDataCenter() + '/' + local.getRack());
            for (PeerInfo peer : peers)
                placement.put(peer.getPeerAddressAndPort(), peer.getDataCenter() + '/' + peer.getRack());
            assertThat(placement).as("topology in the files of node %s", node).isEqualTo(TwoDcNode.placement());
            assertThat(placement.values().stream().collect(Collectors.groupingBy(s -> s, Collectors.counting())))
            .as("nodes per rack in the files of node %s", node)
            .isEqualTo(Map.of("dc1/rack1", 2L, "dc1/rack2", 1L, "dc2/rack1", 2L, "dc2/rack2", 1L));
        }
    }

    /**
     * The import in {@code DSE_6} of the files of every node of the two-data-center cluster: the system tables get
     * the node's own data center and rack and those of each peer, and the files stay in place.
     */
    @Test
    public void dse6ModeImportsTheDataCenterAndRackOfTwoDataCenters() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        for (int node = 1; node <= DseFixtures.TWO_DC_NODES; node++)
        {
            TwoDcNode self = TwoDcNode.of(node);
            importIntoEmptyTables(tempDir.resolve("node" + node), DseFixtures.nodesDirectoryTwoDataCenters(node), self.hostId);

            NodesPersistence tables = new NodesPersistence();
            LocalInfo local = tables.loadLocal();
            assertThat(local.getHostId()).as("node %s", node).isEqualTo(self.hostId);
            assertThat(local.getDataCenter()).as("node %s", node).isEqualTo(self.dataCenter);
            assertThat(local.getRack()).as("node %s", node).isEqualTo(self.rack);
            assertThat(local.getTokens()).as("node %s", node).containsExactlyInAnyOrderElementsOf(self.tokens);
            List<PeerInfo> peers = tables.loadPeers().collect(Collectors.toList());
            assertThat(peers).as("peers of node %s", node).hasSize(DseFixtures.TWO_DC_NODES - 1);
            for (PeerInfo peer : peers)
                TwoDcNode.assertPeer(node, peer, DseVersion.DSE_6_9_26.releaseVersion);
        }
    }

    /**
     * The import of the files DSE wrote in the mixed phase of the two-data-center cluster, once {@code dc2} ran this software:
     * the data center and rack of the peers of both releases are imported; the seeding of the dialect from the imported
     * peers (what {@code DseDialectVersions.seedFromSavedPeers} does at startup with the saved peers) assumes it only for
     * the peers stored with a DSE 6 release (on node 1 the two other nodes of {@code dc1}, on node 4 the three nodes of
     * {@code dc1}).
     */
    @Test
    public void dse6ModeImportsTheMixedPhaseFilesOfTwoDataCenters() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Map<Integer, List<Integer>> dsePeers = Map.of(1, List.of(2, 3), 4, List.of(1, 2, 3));
        Map<Integer, List<Integer>> upgradedPeers = Map.of(1, List.of(4, 5, 6), 4, List.of(5, 6));
        for (int node : new int[]{ 1, 4 })
        {
            TwoDcNode self = TwoDcNode.of(node);
            importIntoEmptyTables(tempDir.resolve("mixed-node" + node), DseFixtures.nodesDirectoryTwoDataCentersMixed(node), self.hostId);

            NodesPersistence tables = new NodesPersistence();
            LocalInfo local = tables.loadLocal();
            assertThat(local.getDataCenter()).as("node %s", node).isEqualTo(self.dataCenter);
            assertThat(local.getRack()).as("node %s", node).isEqualTo(self.rack);
            List<PeerInfo> peers = tables.loadPeers().collect(Collectors.toList());
            assertThat(peers).as("peers of node %s", node).hasSize(DseFixtures.TWO_DC_NODES - 1);
            for (PeerInfo peer : peers)
            {
                int peerNode = TwoDcNode.of(peer.getPeerAddressAndPort()).node;
                boolean upgraded = upgradedPeers.get(node).contains(peerNode);
                TwoDcNode.assertPeer(node, peer, upgraded ? "5.0.7.0-SNAPSHOT" : DseVersion.DSE_6_9_26.releaseVersion);
            }

            EndpointMessagingVersions versions = new EndpointMessagingVersions();
            assertThat(DseDialectVersions.seed(peers.stream(), versions))
            .as("peers of node %s assumed at the dialect", node)
            .containsExactlyInAnyOrderElementsOf(dsePeers.get(node).stream().map(n -> TwoDcNode.of(n).address()).collect(Collectors.toList()));
            for (int dse : dsePeers.get(node))
                assertThat(versions.getRaw(TwoDcNode.of(dse).address())).as("node %s about DSE node %s", node, dse).isEqualTo(DseCompat.DSE_DIALECT);
            for (int upgraded : upgradedPeers.get(node))
                assertThat(versions.knows(TwoDcNode.of(upgraded).address())).as("node %s about upgraded node %s", node, upgraded).isFalse();
        }
    }

    /**
     * Imports {@code fixture} in a metadata directory of its own: the system tables start without peers (the peers of an
     * earlier import of this test would otherwise stay), and the import completes (flush and marker) before returning.
     */
    private static void importIntoEmptyTables(Path metadataDir, Path fixture, UUID hostId) throws Exception
    {
        Path nodes = DseFixtures.installNodesDirectory(metadataDir, fixture);
        DatabaseDescriptor.setMetadataDirectory(new File(metadataDir));
        NodesPersistence tables = new NodesPersistence();
        for (PeerInfo peer : tables.loadPeers().collect(Collectors.toList()))
            tables.deletePeer(peer.getPeerAddressAndPort());
        importBoth();
        assertThat(Files.exists(nodes.resolve("local"))).isTrue();
        assertThat(Files.exists(nodes.resolve("peers"))).isTrue();
        assertThat(Files.exists(nodes.resolve(CC4UpgradeNodesPersistence.markerName(hostId)))).isTrue();
    }

    /**
     * A node of the two-data-center DSE 6.9.26 cluster, as {@code nodetool status} and {@code system.peers} list it
     * there.
     */
    private static final class TwoDcNode
    {
        private static final List<TwoDcNode> NODES =
        List.of(new TwoDcNode(1, "dc1", "rack1", "519bda59-070b-4e10-b6f5-c5ea3f53208a",
                              -8810301412129867072L, -6974778074775089262L, -4083733693391014424L, -1872849444631886952L,
                              -282844892117988314L, 2314323856095169058L, 5189000057153694475L, 7124673815337068492L),
                new TwoDcNode(2, "dc1", "rack1", "cf32b1f0-a357-4dbd-98b3-2ac5f1130827",
                              -7793816178483365996L, -5921487043724228803L, -5218308913267160133L, -2768857436371320769L,
                              1191752632016034824L, 4032096029335362516L, 6363019207058630219L, 8665486272675390827L),
                new TwoDcNode(3, "dc1", "rack2", "2e6cba03-2570-4c06-983e-cac8637531d6",
                              -4531186455684457724L, -3290403690733641585L, -930973104345963152L, 536047973114541044L,
                              3358242324213500183L, 4682804617734730429L, 5887892766905108872L, 8035657311466094646L),
                new TwoDcNode(4, "dc2", "rack1", "4edea6c5-e95e-49ea-8d05-ddf09fddc10f",
                              -7547514599920958809L, -4672838398862433391L, -2737164640679059375L, -225395794436443323L,
                              1610127542918334486L, 4501171924302409324L, 6712056173061536795L, 8302060725575435433L),
                new TwoDcNode(5, "dc2", "rack1", "dc59d445-10df-4270-acf0-00b534b86a24",
                              -8670085824000093043L, -5829742426680765350L, -3498819248957497646L, -1196352183340737038L,
                              791089439210057752L, 2663418573969194943L, 3366596704426263614L, 5816048181322102977L),
                new TwoDcNode(6, "dc2", "rack2", "80f593ae-296a-4114-8aab-61487b6e03a3",
                              -6503596131802627682L, -5179033838281397436L, -3973945689111018993L, -1826181144550033219L,
                              4053719162008966023L, 5294501926959782161L, 7653932513347460594L, 9120953590807964791L));

        final int node;
        final String dataCenter;
        final String rack;
        final UUID hostId;
        /** Its eight tokens. */
        final List<LongToken> tokens;

        private TwoDcNode(int node, String dataCenter, String rack, String hostId, long... tokens)
        {
            this.node = node;
            this.dataCenter = dataCenter;
            this.rack = rack;
            this.hostId = UUID.fromString(hostId);
            this.tokens = Arrays.stream(tokens).mapToObj(LongToken::new).collect(Collectors.toList());
        }

        static TwoDcNode of(int node)
        {
            return NODES.get(node - 1);
        }

        static TwoDcNode of(InetAddressAndPort address)
        {
            for (TwoDcNode candidate : NODES)
                if (candidate.address().equals(address))
                    return candidate;
            throw new AssertionError("no node of the two-data-center fixture at " + address);
        }

        /** The storage address: DSE's entries carry the storage port 7000. */
        InetAddressAndPort address()
        {
            try
            {
                return CC4NodesFileReaderDseTest.address("192.168.57.1" + node, 7000);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }

        /** Every node's data center and rack, as {@code <data center>/<rack>} by address. */
        static Map<InetAddressAndPort, String> placement()
        {
            Map<InetAddressAndPort, String> placement = new HashMap<>();
            for (TwoDcNode node : NODES)
                placement.put(node.address(), node.dataCenter + '/' + node.rack);
            return placement;
        }

        /** A peer in the files of {@code node}: another node of the cluster, with its data center, rack, host id and release. */
        static void assertPeer(int node, PeerInfo peer, String releaseVersion)
        {
            TwoDcNode expected = of(peer.getPeerAddressAndPort());
            assertThat(expected.node).as("peer of node %s", node).isNotEqualTo(node);
            assertThat(peer.getHostId()).as("node %s about node %s", node, expected.node).isEqualTo(expected.hostId);
            assertThat(peer.getDataCenter()).as("node %s about node %s", node, expected.node).isEqualTo(expected.dataCenter);
            assertThat(peer.getRack()).as("node %s about node %s", node, expected.node).isEqualTo(expected.rack);
            assertThat(peer.getReleaseVersion()).as("node %s about node %s", node, expected.node).isEqualTo(new CassandraVersion(releaseVersion));
            assertThat(peer.getTokens()).as("node %s about node %s", node, expected.node).containsExactlyInAnyOrderElementsOf(expected.tokens);
        }
    }

    // ---- helpers ----

    private static void importBothWithoutWaiting()
    {
        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        persistence.loadLocal();
        persistence.loadPeers().collect(Collectors.toList());
    }

    private static LocalInfo importBoth()
    {
        CC4UpgradeNodesPersistence persistence = new CC4UpgradeNodesPersistence();
        LocalInfo local = persistence.loadLocal();
        persistence.loadPeers().collect(Collectors.toList());
        CC4UpgradeNodesPersistence.awaitPendingCompletion();
        return local;
    }

    private void writeLocal(Map<String, Object> local) throws Exception
    {
        Path nodes = Files.createDirectories(tempDir.resolve("nodes"));
        writeMsgpack(nodes.resolve("local"), local);
    }

    private static void writeMsgpack(Path path, Object value) throws Exception
    {
        MessagePackFactory factory = new MessagePackFactory();
        factory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        ObjectMapper mapper = new ObjectMapper(factory); // checkstyle: permit this instantiation
        try (OutputStream out = Files.newOutputStream(path))
        {
            mapper.writeValue(out, value);
        }
    }
}
