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

package org.apache.cassandra.dsecompat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.dsecompat.DseFixtures.Handshake;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Sanity checks of the DSE artefacts under {@code test/data/dse-compat}: every later compatibility
 * test relies on these files being what the README says they are.
 */
public class DseFixturesTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final ObjectMapper MSGPACK = new ObjectMapper(new MessagePackFactory()); // checkstyle: permit this instantiation

    private static final byte[] HANDSHAKE_MAGIC = { (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA };

    /** The table truncated in {@code local-after-truncate}, and its record [timestamp ms, segment id, position]. */
    private static final String TRUNCATED_TABLE_ID = "467ee541-b828-11f1-9f9f-754297a7ef99";
    private static final byte[] TRUNCATION_RECORD = { (byte) 0x93,
                                                      (byte) 0xCF, 0, 0, 0x01, (byte) 0xA0, (byte) 0xD3, (byte) 0xF1, 0x31, 0x54,
                                                      (byte) 0xCF, 0, 0, 0x01, (byte) 0xA0, (byte) 0xD3, (byte) 0x96, 0x3B, 0x2D,
                                                      (byte) 0xCE, 0, 0x03, (byte) 0xDD, 0x35 };

    /** The 6.8.65 hints file name: target host id (peer 192.168.57.13) and timestamp, repeated in its header. */
    private static final UUID HINTS_HOST_ID = UUID.fromString("ed39c7b2-7f97-4a0a-91fc-71398afe1d12");
    private static final long HINTS_TIMESTAMP = 1790335633208L;
    /**
     * Same for 6.9.26: the host id of the third node of the three-node 6.9.26 cluster that wrote the file; not in the
     * 6.9.26 peers file, which comes from a two-node cluster.
     */
    private static final UUID HINTS_HOST_ID_6_9_26 = UUID.fromString("ac3923c3-6b54-4752-af03-ff2831b62388");
    private static final long HINTS_TIMESTAMP_6_9_26 = 1790587137402L;
    /** A DSE hints file header: version, timestamp, host id, then the parameters length, a crc, "{}" and a crc. */
    private static final int HINTS_HEADER_SIZE = 4 + 8 + 16 + 4 + 4 + 2 + 4;

    /** The BASE_TIMESTAMP parameter of msg3 in the captures, by "&lt;version dir&gt; &lt;connecting node&gt;". */
    private static final Map<String, Long> BASE_TIMESTAMPS = ImmutableMap.of("dse-6.8.65 192.168.57.11", 1790330248545L,
                                                                             "dse-6.8.65 192.168.57.13", 1790330248556L,
                                                                             "dse-6.9.26 192.168.57.11", 1790332178054L,
                                                                             "dse-6.9.26 192.168.57.13", 1790332178066L);

    @Test
    public void nodeFilesAreMsgpackMaps() throws IOException
    {
        assertEquals(728, Files.size(DseFixtures.nodesLocal(DseVersion.DSE_6_8_65)));
        assertEquals(953, Files.size(DseFixtures.nodesPeers(DseVersion.DSE_6_8_65)));
        assertEquals(739, Files.size(DseFixtures.nodesLocal(DseVersion.DSE_6_9_26)));
        assertEquals(483, Files.size(DseFixtures.nodesPeers(DseVersion.DSE_6_9_26)));
        for (DseVersion v : DseVersion.values())
        {
            byte[] local = Files.readAllBytes(DseFixtures.nodesLocal(v));
            // map16 with 29 entries, as both versions write it
            assertEquals("map16 marker in " + v, (byte) 0xDE, local[0]);
            assertEquals(29, ((local[1] & 0xFF) << 8) | (local[2] & 0xFF));
            assertTrue(new String(local, StandardCharsets.ISO_8859_1).contains(v.releaseVersion));

            byte[] peers = Files.readAllBytes(DseFixtures.nodesPeers(v));
            // a fixarray of map16 entries with 20 keys: two peers (6.8.65, three-node cluster) or one (6.9.26, two-node cluster)
            assertEquals("fixarray marker in " + v, 0x90 | (v == DseVersion.DSE_6_8_65 ? 2 : 1), peers[0] & 0xFF);
            assertEquals((byte) 0xDE, peers[1]);
            assertEquals(20, ((peers[2] & 0xFF) << 8) | (peers[3] & 0xFF));
        }
        assertLocal(DseFixtures.nodesLocal(DseVersion.DSE_6_8_65), "0c646ff5-7983-460a-9aad-b0cdd24df296", "dc1", "rack1", "dse-lab");
        assertLocal(DseFixtures.nodesLocal(DseVersion.DSE_6_9_26), "55771b40-b5c1-4123-94e5-8af8ce644811", "Cassandra", "rack1", "Test Cluster");

        byte[] empty = Files.readAllBytes(DseFixtures.nodesPeersEmpty());
        assertArrayEquals(new byte[]{ (byte) 0x80 }, empty);

        // the truncation record: the table id string as key, [1790262194516, 1790256233261, 253237] as value
        String afterTruncate = new String(Files.readAllBytes(DseFixtures.nodesLocalAfterTruncate()), StandardCharsets.ISO_8859_1);
        String plain = new String(Files.readAllBytes(DseFixtures.nodesLocal(DseVersion.DSE_6_9_26)), StandardCharsets.ISO_8859_1);
        byte[] key = TRUNCATED_TABLE_ID.getBytes(StandardCharsets.ISO_8859_1);
        byte[] str8Key = new byte[2 + key.length];
        str8Key[0] = (byte) 0xD9;
        str8Key[1] = (byte) key.length;
        System.arraycopy(key, 0, str8Key, 2, key.length);
        String entry = new String(str8Key, StandardCharsets.ISO_8859_1) + new String(TRUNCATION_RECORD, StandardCharsets.ISO_8859_1);
        assertTrue(afterTruncate.contains(entry));
        assertFalse(plain.contains(TRUNCATED_TABLE_ID));
        // in the plain file the key (fixstr of 12 characters) is followed by the empty map
        assertTrue(plain.contains("\u00ACtruncated_at\u0080"));
    }

    /** Host id, data center, rack, cluster name and the 8 tokens of a {@code local} file. */
    private static void assertLocal(Path local, String hostId, String dataCenter, String rack, String clusterName) throws IOException
    {
        JsonNode map = MSGPACK.readTree(Files.readAllBytes(local));
        assertEquals("host id in " + local, UUID.fromString(hostId), hostId(map));
        assertEquals("data center in " + local, dataCenter, map.get("data_center").asText());
        assertEquals("rack in " + local, rack, map.get("rack").asText());
        assertEquals("cluster in " + local, clusterName, map.get("cluster_name").asText());
        assertEquals("tokens in " + local, 8, map.get("tokens").size());
    }

    /** A host id as DSE writes it in the nodes files: an array [int64 msb, int64 lsb]. */
    private static UUID hostId(JsonNode entry)
    {
        JsonNode hostId = entry.get("host_id");
        return new UUID(hostId.get(0).asLong(), hostId.get(1).asLong());
    }

    /** The last byte of the IPv4 address under {@code key} of a nodes file entry, an address of 192.168.57.0/24. */
    private static int lastAddressByte(JsonNode entry, String key) throws IOException
    {
        byte[] address = entry.get(key).binaryValue();
        assertArrayEquals(key + " of " + entry.get("host_id") + " in 192.168.57.0/24", new byte[]{ (byte) 192, (byte) 168, 57 },
                          Arrays.copyOf(address, 3));
        assertEquals(key + " of " + entry.get("host_id"), 4, address.length);
        return address[3] & 0xFF;
    }

    /** The data center, rack and host id of each node of the two-data-center cluster, as the README lists them. */
    private static final String[][] TWO_DC_TOPOLOGY = { { "dc1", "rack1", "519bda59-070b-4e10-b6f5-c5ea3f53208a" },
                                                        { "dc1", "rack1", "cf32b1f0-a357-4dbd-98b3-2ac5f1130827" },
                                                        { "dc1", "rack2", "2e6cba03-2570-4c06-983e-cac8637531d6" },
                                                        { "dc2", "rack1", "4edea6c5-e95e-49ea-8d05-ddf09fddc10f" },
                                                        { "dc2", "rack1", "dc59d445-10df-4270-acf0-00b534b86a24" },
                                                        { "dc2", "rack2", "80f593ae-296a-4114-8aab-61487b6e03a3" } };

    /**
     * The nodes files of the two-data-center cluster: sizes as the README gives them, a 29-key map in {@code local}
     * with the node's address, data center, rack and host id, an array of five 20-key maps in {@code peers}, and the
     * local files of the mixed phase byte for byte those taken while every node ran DSE.
     */
    @Test
    public void twoDataCenterNodeFilesAreWhatTheReadmeSays() throws IOException
    {
        for (int node = 1; node <= DseFixtures.TWO_DC_NODES; node++)
        {
            Path directory = DseFixtures.nodesDirectoryTwoDataCenters(node);
            assertTwoDcNodeFiles(directory, 2381);
            String[] topology = TWO_DC_TOPOLOGY[node - 1];
            assertLocal(directory.resolve("local"), topology[2], topology[0], topology[1], "dse-lab");
            JsonNode local = MSGPACK.readTree(Files.readAllBytes(directory.resolve("local")));
            assertEquals("release of node " + node, DseVersion.DSE_6_9_26.releaseVersion, local.get("release_version").asText());
            assertArrayEquals("address of node " + node, new byte[]{ (byte) 192, (byte) 168, 57, (byte) (10 + node) },
                              local.get("broadcast_address").binaryValue());
            assertTwoDcPeers(directory, node, ImmutableSet.of());
        }
        // in the mixed phase the nodes of dc2 (192.168.57.14 to 16) ran this software; node 4 itself was switched after
        // DSE wrote its peers file
        assertTwoDcPeers(DseFixtures.nodesDirectoryTwoDataCentersMixed(1), 1, ImmutableSet.of(4, 5, 6));
        assertTwoDcPeers(DseFixtures.nodesDirectoryTwoDataCentersMixed(4), 4, ImmutableSet.of(5, 6));
        assertTwoDcNodeFiles(DseFixtures.nodesDirectoryTwoDataCentersMixed(1), 2396);
        assertTwoDcNodeFiles(DseFixtures.nodesDirectoryTwoDataCentersMixed(4), 2391);
        for (int node : new int[]{ 1, 4 })
            assertArrayEquals("local of node " + node,
                              Files.readAllBytes(DseFixtures.nodesDirectoryTwoDataCenters(node).resolve("local")),
                              Files.readAllBytes(DseFixtures.nodesDirectoryTwoDataCentersMixed(node).resolve("local")));
    }

    /**
     * The peers file of node {@code node} of the two-data-center cluster lists the five other nodes with their data
     * center, rack and host id; the {@code upgraded} ones with the release of this software and the DSE version it
     * gossips to DSE nodes, the others with DSE 6.9.26's.
     */
    private static void assertTwoDcPeers(Path directory, int node, Set<Integer> upgraded) throws IOException
    {
        JsonNode peers = MSGPACK.readTree(Files.readAllBytes(directory.resolve("peers")));
        Set<Integer> listed = new HashSet<>();
        for (JsonNode peer : peers)
        {
            int other = lastAddressByte(peer, "peer") - 10;
            assertTrue("peer of node " + node + " outside the cluster: node " + other + " in " + directory, other >= 1 && other <= DseFixtures.TWO_DC_NODES);
            assertTrue("node " + other + " listed twice in " + directory, listed.add(other));
            String[] topology = TWO_DC_TOPOLOGY[other - 1];
            String what = "node " + other + " in the peers of node " + node + " in " + directory;
            assertEquals("data center of " + what, topology[0], peer.get("data_center").asText());
            assertEquals("rack of " + what, topology[1], peer.get("rack").asText());
            assertEquals("host id of " + what, UUID.fromString(topology[2]), hostId(peer));
            boolean isUpgraded = upgraded.contains(other);
            assertEquals("release of " + what, isUpgraded ? "5.0.7.0-SNAPSHOT" : DseVersion.DSE_6_9_26.releaseVersion, peer.get("release_version").asText());
            assertEquals("DSE version of " + what, isUpgraded ? "6.8.1" : "6.9.26", peer.get("dse_version").asText());
        }
        Set<Integer> others = new HashSet<>();
        for (int n = 1; n <= DseFixtures.TWO_DC_NODES; n++)
            if (n != node)
                others.add(n);
        assertEquals("peers of node " + node + " in " + directory, others, listed);
    }

    private static void assertTwoDcNodeFiles(Path directory, int peersSize) throws IOException
    {
        byte[] local = Files.readAllBytes(directory.resolve("local"));
        assertEquals("size of local in " + directory, 728, local.length);
        assertEquals("map16 marker in " + directory, (byte) 0xDE, local[0]);
        JsonNode localMap = MSGPACK.readTree(local);
        assertTrue(localMap.isObject());
        assertEquals("keys of local in " + directory, 29, localMap.size());
        byte[] peers = Files.readAllBytes(directory.resolve("peers"));
        assertEquals("size of peers in " + directory, peersSize, peers.length);
        assertEquals("fixarray of five peers in " + directory, 0x95, peers[0] & 0xFF);
        JsonNode peersArray = MSGPACK.readTree(peers);
        assertTrue(peersArray.isArray());
        assertEquals("peers in " + directory, 5, peersArray.size());
        for (JsonNode peer : peersArray)
            assertEquals("keys of a peer in " + directory, 20, peer.size());
    }

    /**
     * The nodes files of the second node (192.168.57.12) of the mixed DSE 6.8.65 cluster: one DSE peer (192.168.57.11),
     * and one (192.168.57.13) recorded with the release of this software and the DSE version upgraded nodes gossip to
     * DSE nodes.
     */
    @Test
    public void mixedNodeFilesRecordAnUpgradedPeer() throws IOException
    {
        Path directory = DseFixtures.nodesDirectoryMixed();
        assertLocal(directory.resolve("local"), "9833dda0-6508-4036-aec2-2813215da12f", "dc1", "rack2", "dse-lab");
        assertEquals(12, lastAddressByte(MSGPACK.readTree(Files.readAllBytes(directory.resolve("local"))), "broadcast_address"));

        assertEquals(728, Files.size(directory.resolve("local")));
        assertEquals(958, Files.size(directory.resolve("peers")));

        JsonNode peers = MSGPACK.readTree(Files.readAllBytes(directory.resolve("peers")));
        Map<Integer, String> versions = new HashMap<>();
        for (JsonNode peer : peers)
            assertNull("peer listed twice: " + peer.get("peer"),
                       versions.put(lastAddressByte(peer, "peer"), peer.get("release_version").asText() + ' ' + peer.get("dse_version").asText()));
        assertEquals(ImmutableMap.of(11, DseVersion.DSE_6_8_65.releaseVersion + " 6.8.65", 13, "5.0.7.0-SNAPSHOT 6.8.1"), versions);
    }

    /**
     * The schema data directory holds only DSE's {@code system_schema}, DSE's own tables included, and
     * {@code system_auth/role_permissions}; the configuration of the same node holds the four files the README names,
     * with the node's data center and rack.
     */
    @Test
    public void schemaDataDirectoryAndConfigurationAreWhatTheReadmeSays() throws IOException
    {
        Path data = DseFixtures.schemaDataDirectory();
        assertEquals(ImmutableSet.of("system_auth", "system_schema"), fileNames(data));
        assertEquals(ImmutableSet.of("role_permissions-3afbe79f219431a7add7f5ab90d8ec9c"), fileNames(data.resolve("system_auth")));
        Set<String> schemaTables = fileNames(data.resolve("system_schema"));
        for (String dseTable : new String[]{ "hidden_columns-", "vertices-", "edges-" })
            assertTrue("system_schema lacks DSE's " + dseTable + "*: " + schemaTables, schemaTables.stream().anyMatch(n -> n.startsWith(dseTable)));

        Path conf = DseFixtures.confDirectory();
        assertEquals(ImmutableSet.of("cassandra.yaml", "dse.yaml", "cassandra-rackdc.properties", "ds_version.txt"), fileNames(conf));
        assertEquals("DSVersion=6.8.65-4a7b9eb", new String(Files.readAllBytes(conf.resolve("ds_version.txt")), StandardCharsets.US_ASCII).trim());
        // the data center and rack of the same node as its nodes/local file (nodes/dse-6.8.65-mixed)
        List<String> rackdc = Files.readAllLines(conf.resolve("cassandra-rackdc.properties"));
        assertTrue(rackdc.toString(), rackdc.contains("dc=dc1") && rackdc.contains("rack=rack2"));
    }

    /** The schema of DSE 6.9.26 creates DSE's local keyspaces with {@code LocalStrategy}. */
    @Test
    public void describeSchemaHasTheLocalKeyspacesOfDse() throws IOException
    {
        List<String> lines = Files.readAllLines(DseFixtures.describeSchema(DseVersion.DSE_6_9_26));
        for (String keyspace : new String[]{ "dse_system_local", "dse_insights_local" })
            assertTrue(keyspace + " with LocalStrategy",
                       lines.contains("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;"));
    }

    /**
     * The {@code system.paxos} rows of DSE 6.8.65: 100 of {@code lab.lwt} and 18 of {@code bbtest.t}, every stored
     * version DSE's 1024; the ballots moved an hour back keep their spacing.
     */
    @Test
    public void paxosRowsAreWhatTheReadmeSays() throws IOException
    {
        List<DseFixtures.PaxosRow> rows = DseFixtures.paxosRows();
        assertEquals(118, rows.size());
        assertEquals(100, rows.stream().filter(r -> r.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID)).count());
        assertEquals(18, rows.stream().filter(r -> r.cfId.equals(DseFixtures.PAXOS_COLLECTION_TABLE_ID)).count());
        // lab.lwt: the commits of DSE coordinators (flag digit 0), no proposal; bbtest.t: 9 committed batches, 6 of DSE
        // coordinators (digit 0) and 3 of the upgraded node (digit 2), and 9 rounds that changed nothing
        int collectionRowsWithProposal = 0;
        int collectionCommitsOfTheUpgradedNode = 0;
        for (DseFixtures.PaxosRow row : rows)
        {
            assertNotNull(row.mostRecentCommitAt);
            assertNotNull(row.inProgressBallot);
            assertEquals(Integer.valueOf(DseFixtures.PAXOS_DSE_6_8_65_VERSION), row.mostRecentCommitVersion);
            assertEquals(Integer.valueOf(DseFixtures.PAXOS_DSE_6_8_65_VERSION), row.proposalVersion);
            assertEquals("proposal and its ballot", row.proposal == null, row.proposalBallot == null);
            long commitDigit = Ballot.fromUuid(row.mostRecentCommitAt).uuidTimestamp() % 10;
            if (row.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID))
            {
                assertNull("proposal of a lab.lwt row", row.proposal);
                assertEquals("commit ballot of a lab.lwt row", 0, commitDigit);
            }
            else if (row.proposal != null)
            {
                // a round that changed nothing: the empty commit of a DSE coordinator, then the empty proposal a serial
                // read of the upgraded node left
                collectionRowsWithProposal++;
                assertEquals("commit ballot of an empty bbtest.t round", 0, commitDigit);
                assertEquals("proposal ballot of an empty bbtest.t round", 2, Ballot.fromUuid(row.proposalBallot).uuidTimestamp() % 10);
                assertEquals("in progress ballot of an empty bbtest.t round", row.proposalBallot, row.inProgressBallot);
            }
            else if (commitDigit == 2)
            {
                collectionCommitsOfTheUpgradedNode++;
            }
            else
            {
                assertEquals("commit ballot of a bbtest.t batch", 0, commitDigit);
            }
        }
        assertEquals(9, collectionRowsWithProposal);
        assertEquals(3, collectionCommitsOfTheUpgradedNode);

        List<DseFixtures.PaxosRow> shifted = DseFixtures.paxosRowsOfAnHourAgo();
        assertEquals(rows.size(), shifted.size());
        long shift = Ballot.fromUuid(shifted.get(0).mostRecentCommitAt).unixMicros() - Ballot.fromUuid(rows.get(0).mostRecentCommitAt).unixMicros();
        long latest = Long.MIN_VALUE;
        for (int i = 0; i < rows.size(); i++)
        {
            DseFixtures.PaxosRow row = rows.get(i);
            DseFixtures.PaxosRow moved = shifted.get(i);
            assertBallotShifted("commit of row " + i, row.mostRecentCommitAt, moved.mostRecentCommitAt, shift);
            assertBallotShifted("in progress ballot of row " + i, row.inProgressBallot, moved.inProgressBallot, shift);
            assertBallotShifted("proposal ballot of row " + i, row.proposalBallot, moved.proposalBallot, shift);
            assertEquals(row.mostRecentCommit, moved.mostRecentCommit);
            assertEquals(row.proposal, moved.proposal);
            latest = Math.max(latest, Ballot.fromUuid(moved.mostRecentCommitAt).unixMicros());
            for (UUID ballot : Arrays.asList(moved.inProgressBallot, moved.proposalBallot))
                if (ballot != null)
                    latest = Math.max(latest, Ballot.fromUuid(ballot).unixMicros());
        }
        long age = FBUtilities.timestampMicros() - latest;
        assertTrue("latest commit " + age + " micros old", age >= TimeUnit.HOURS.toMicros(1) && age < TimeUnit.HOURS.toMicros(1) + TimeUnit.MINUTES.toMicros(10));
    }

    /**
     * {@link DseFixtures.PaxosRow#withoutMaxTimestamp} drops the maximum timestamp that follows the table id of every
     * update (the micros of its ballot, or {@code Long.MIN_VALUE} for the 18 empty updates), and nothing else;
     * {@link DseFixtures#paxosRowDeletion} deletes the one row above the latest of the given write times.
     */
    @Test
    public void paxosRowHelpersRewriteWhatTheySay() throws IOException
    {
        int emptyUpdates = 0;
        for (DseFixtures.PaxosRow row : DseFixtures.paxosRows())
        {
            DseFixtures.PaxosRow stripped = row.withoutMaxTimestamp();
            emptyUpdates += assertWithoutMaxTimestamp(row, row.mostRecentCommit, row.mostRecentCommitAt, stripped.mostRecentCommit);
            if (row.proposal == null)
                assertNull(stripped.proposal);
            else
                emptyUpdates += assertWithoutMaxTimestamp(row, row.proposal, row.proposalBallot, stripped.proposal);
            assertEquals(row.mostRecentCommitAt, stripped.mostRecentCommitAt);
            assertEquals(row.proposalBallot, stripped.proposalBallot);
        }
        assertEquals("updates with no maximum timestamp (9 empty commits, 9 empty proposals)", 18, emptyUpdates);

        DseFixtures.PaxosRow row = DseFixtures.paxosRows().get(0);
        DseFixtures.Statement deletion = DseFixtures.paxosRowDeletion(row, 5L, 12L, 7L);
        assertEquals("DELETE FROM system.paxos USING TIMESTAMP ? WHERE row_key = ? AND cf_id = ?", deletion.cql);
        assertArrayEquals(new Object[]{ 13L, row.rowKey, row.cfId }, deletion.values);
    }

    /**
     * {@code stripped} is {@code update} without the 8 bytes past its table id, which hold the micros of the update's
     * ballot or, for an empty update, {@code Long.MIN_VALUE}; returns 1 for an empty update, 0 otherwise.
     */
    private static int assertWithoutMaxTimestamp(DseFixtures.PaxosRow row, ByteBuffer update, UUID ballot, ByteBuffer stripped)
    {
        byte[] bytes = new byte[update.remaining()];
        update.duplicate().get(bytes);
        ByteBuffer layout = ByteBuffer.wrap(bytes);
        assertEquals("table id of an update of row " + row.key(), row.cfId, new UUID(layout.getLong(0), layout.getLong(8)));
        long maxTimestamp = layout.getLong(16);
        // only the 9 empty rounds of bbtest.t, the rows with a proposal, have updates without a maximum timestamp
        assertEquals("update of row " + row.key() + " without a maximum timestamp", row.proposal != null, maxTimestamp == Long.MIN_VALUE);
        if (maxTimestamp != Long.MIN_VALUE)
            assertEquals("maximum timestamp of an update of row " + row.key(), Ballot.fromUuid(ballot).unixMicros(), maxTimestamp);
        byte[] expected = new byte[bytes.length - 8];
        System.arraycopy(bytes, 0, expected, 0, 16);
        System.arraycopy(bytes, 24, expected, 16, bytes.length - 24);
        byte[] actual = new byte[stripped.remaining()];
        stripped.duplicate().get(actual);
        assertArrayEquals(expected, actual);
        return maxTimestamp == Long.MIN_VALUE ? 1 : 0;
    }

    /** A ballot moved by {@code shift} microseconds, with its flag and its unique part kept; null stays null. */
    private static void assertBallotShifted(String what, UUID original, UUID shifted, long shift)
    {
        if (original == null)
        {
            assertNull(what, shifted);
            return;
        }
        Ballot before = Ballot.fromUuid(original);
        Ballot after = Ballot.fromUuid(shifted);
        assertEquals(what, before.unixMicros() + shift, after.unixMicros());
        assertEquals(what, before.flag(), after.flag());
        assertEquals(what, before.lsb(), after.lsb());
    }

    /**
     * The dump of the rollback holds three sstables, with 122, 3 and 103 rows of every table, of which it prints those of
     * {@code m5.px} only: 16, 3 and 3.
     */
    @Test
    public void paxosRollbackDumpHasItsThreeSSTables() throws IOException
    {
        String m5Px = "0f59f853-bf41-11f1-9b7d-b57cc343566d";
        List<String> counts = new ArrayList<>();
        List<Integer> printed = new ArrayList<>();
        for (String line : Files.readAllLines(DseFixtures.paxosRollbackDump()))
        {
            if (line.startsWith("### "))
                printed.add(0);
            else if (line.startsWith("-- rows: "))
                counts.add(line);
            else if (line.startsWith("["))
            {
                assertFalse("a row before any sstable line: " + line, printed.isEmpty());
                assertTrue("a row of another table than m5.px: " + line, line.contains("]: " + m5Px + " | "));
                printed.set(printed.size() - 1, printed.get(printed.size() - 1) + 1);
            }
            else
            {
                assertTrue("unexpected line in the dump: " + line, line.isEmpty() || line.startsWith("# "));
            }
        }
        assertEquals(Arrays.asList("-- rows: 122", "-- rows: 3", "-- rows: 103"), counts);
        assertEquals(Arrays.asList(16, 3, 3), printed);
    }

    private static Set<String> fileNames(Path directory) throws IOException
    {
        try (Stream<Path> files = Files.list(directory))
        {
            return files.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    @Test
    public void sstablesAreCompleteTrieBbSets() throws IOException
    {
        for (DseVersion v : DseVersion.values())
        {
            List<Path> components = DseFixtures.sstableComponents(v);
            List<String> names = components.stream().map(p -> p.getFileName().toString()).collect(Collectors.toList());
            for (String required : new String[]{ "Data.db", "Partitions.db", "Rows.db", "Statistics.db", "Filter.db", "CompressionInfo.db", "Digest.crc32", "TOC.txt" })
                assertTrue(v + " lacks " + required, names.contains("bb-1-bti-" + required));
            // the SAI components: aa naming on 6.8 (SAI_...), ca naming on 6.9 (SAI+ca+...)
            String saiMarker = v == DseVersion.DSE_6_8_65 ? "bb-1-bti-SAI_t_v_sai_" : "bb-1-bti-SAI+ca+t_v_sai+";
            assertTrue(v + " lacks SAI components " + saiMarker, names.stream().anyMatch(n -> n.startsWith(saiMarker)));
            // the TOC lists exactly the components present
            List<String> toc = Files.readAllLines(DseFixtures.sstableDirectory(v).resolve("bb-1-bti-TOC.txt"));
            for (String n : names)
                assertTrue(v + " TOC lacks " + n, toc.contains(n.substring("bb-1-bti-".length())));
            for (String entry : toc)
                assertTrue(v + " TOC lists missing component " + entry, names.contains("bb-1-bti-" + entry));
        }
    }

    @Test
    public void viewFixtureIsWhatTheReadmeSays() throws IOException
    {
        for (List<Path> components : Arrays.asList(DseFixtures.viewSSTableComponents(), DseFixtures.viewBaseSSTableComponents()))
        {
            Path dir = components.get(0).getParent();
            List<String> names = components.stream().map(p -> p.getFileName().toString().substring("bb-1-bti-".length())).collect(Collectors.toList());
            List<String> toc = Files.readAllLines(dir.resolve("bb-1-bti-TOC.txt"));
            assertEquals(dir + ": TOC and files", names.stream().sorted().collect(Collectors.toList()), toc.stream().sorted().collect(Collectors.toList()));
        }

        // DSE's own dump of the view sstable: every row has a primary key liveness and one cell of the hidden column
        // .v, empty, with the row's timestamp (sstabledump omits a cell timestamp equal to the row's) and no TTL or
        // deletion; the rows are the ones, in the order, DSE answered for SELECT *
        JsonNode dump;
        try (InputStream in = new GZIPInputStream(Files.newInputStream(DseFixtures.viewSSTableDump())))
        {
            dump = JsonUtils.JSON_OBJECT_MAPPER.readTree(in);
        }
        List<String> dumped = new ArrayList<>();
        for (JsonNode partition : dump)
        {
            String v = partition.get("partition").get("key").get(0).asText();
            for (JsonNode row : partition.get("rows"))
            {
                assertEquals("row", row.get("type").asText());
                assertTrue(row.toString(), row.get("liveness_info").has("tstamp"));
                assertFalse(row.toString(), row.get("liveness_info").has("ttl"));
                assertFalse(row.toString(), row.has("deletion_info"));
                JsonNode cells = row.get("cells");
                assertEquals(row.toString(), 1, cells.size());
                JsonNode cell = cells.get(0);
                assertEquals("\".v\"", cell.get("name").asText());
                assertEquals("", cell.get("value").asText());
                assertEquals(row.toString(), 2, cell.size());
                dumped.add(v + ',' + row.get("clustering").get(0).asInt() + ',' + row.get("clustering").get(1).asInt());
            }
        }
        List<String> answered = DseFixtures.viewSelectStar().stream().map(r -> r[0] + "," + r[1] + ',' + r[2]).collect(Collectors.toList());
        assertEquals(5000, answered.size());
        assertEquals(answered, dumped);
    }

    @Test
    public void dse6926PaxosSSTableIsWhatTheReadmeSays() throws IOException
    {
        List<Path> components = DseFixtures.dse6926PaxosSSTableComponents();
        Path dir = DseFixtures.dse6926PaxosSSTableDirectory();
        List<String> names = components.stream().map(p -> p.getFileName().toString().substring("bb-4-bti-".length())).sorted().collect(Collectors.toList());
        List<String> toc = Files.readAllLines(dir.resolve("bb-4-bti-TOC.txt")).stream().sorted().collect(Collectors.toList());
        assertEquals(dir + ": TOC and files", names, toc);
        // Digest.crc32 holds the CRC32 of Data.db in decimal
        CRC32 crc = new CRC32();
        crc.update(Files.readAllBytes(dir.resolve("bb-4-bti-Data.db")));
        String digest = new String(Files.readAllBytes(dir.resolve("bb-4-bti-Digest.crc32")), StandardCharsets.US_ASCII).trim();
        assertEquals("1808493471", digest);
        assertEquals(Long.parseLong(digest), crc.getValue());
    }

    @Test
    public void handshakesCarryTheDseMagicAndRelativeVersion() throws IOException
    {
        byte[] baseTimestamp = "BASE_TIMESTAMP".getBytes(StandardCharsets.US_ASCII);
        for (DseVersion v : DseVersion.values())
        {
            List<Handshake> handshakes = DseFixtures.handshakes(v);
            // a peer connecting to the restarted node, then the restarted node connecting to that peer
            assertEquals(Arrays.asList("192.168.57.11 -> 192.168.57.13", "192.168.57.13 -> 192.168.57.11"),
                         handshakes.stream().map(h -> h.client + " -> " + h.server).collect(Collectors.toList()));
            for (Handshake h : handshakes)
            {
                // the magic, then the flags: relative version in bits 24-31, 0xFF in bits 8-15, nothing else
                assertArrayEquals(new byte[]{ HANDSHAKE_MAGIC[0], HANDSHAKE_MAGIC[1], HANDSHAKE_MAGIC[2], HANDSHAKE_MAGIC[3],
                                              (byte) v.relativeMessagingVersion, 0, (byte) 0xFF, 0 },
                                  h.msg1);
                // msg2: relative << 8 as a 4-byte int
                assertEquals(4, h.msg2.length);
                assertEquals(v.relativeMessagingVersion << 8, ByteBuffer.wrap(h.msg2).getInt());
                // msg3 repeats the version, then a 1-byte-length IPv4 address of the client
                assertEquals(v.relativeMessagingVersion << 8, ByteBuffer.wrap(h.msg3, 0, 4).getInt());
                assertEquals(4, h.msg3[4]);
                String address = (h.msg3[5] & 0xFF) + "." + (h.msg3[6] & 0xFF) + "." + (h.msg3[7] & 0xFF) + "." + (h.msg3[8] & 0xFF);
                assertEquals(h.client, address);
                // then the parameter block: 02, 000e "BASE_TIMESTAMP", 10, an 8-byte millisecond timestamp
                assertEquals(9 + 1 + 2 + baseTimestamp.length + 1 + 8, h.msg3.length);
                ByteBuffer params = ByteBuffer.wrap(h.msg3, 9, h.msg3.length - 9);
                assertEquals(0x02, params.get());
                assertEquals(baseTimestamp.length, params.getShort());
                byte[] name = new byte[baseTimestamp.length];
                params.get(name);
                assertArrayEquals(baseTimestamp, name);
                assertEquals(0x10, params.get());
                assertEquals("BASE_TIMESTAMP of " + h.client + " in " + v, (long) BASE_TIMESTAMPS.get(v.dir + ' ' + h.client), params.getLong());
                // frame1 is exactly one frame: a 4-byte length and that many bytes, a gossip SYN of the test cluster
                assertEquals(4 + ByteBuffer.wrap(h.frame1).getInt(), h.frame1.length);
                String frame = new String(h.frame1, StandardCharsets.ISO_8859_1);
                assertTrue(frame.contains("\u0000\u0007dse-lab"));
                assertTrue(frame.contains("org.apache.cassandra.dht.Murmur3Partitioner"));
            }
        }
    }

    @Test
    public void commitLogSegmentOf6865HasDseVersion680() throws IOException
    {
        assertCommitLogVersion680(DseVersion.DSE_6_8_65);
    }

    /** {@link DseFixtures#unpackCommitLogSegment} writes the uncompressed segment under its name without {@code .gz}. */
    @Test
    public void unpackedCommitLogSegmentIsTheUncompressedFixture() throws IOException
    {
        for (DseVersion v : DseVersion.values())
        {
            Path unpacked = DseFixtures.unpackCommitLogSegment(v, folder.newFolder().toPath());
            Path gz = DseFixtures.commitLogSegmentGz(v);
            String name = gz.getFileName().toString();
            assertEquals(name.substring(0, name.length() - ".gz".length()), unpacked.getFileName().toString());
            // the size the gzip trailer records (ISIZE, little endian), and the version and segment id of the header
            byte[] compressed = Files.readAllBytes(gz);
            long isize = ByteBuffer.wrap(compressed, compressed.length - 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
            assertEquals("size of the segment of " + v, isize, Files.size(unpacked));
            try (InputStream in = Files.newInputStream(unpacked))
            {
                assertCommitLogHeader(v, in);
            }
        }
    }

    /** {@link DseFixtures#installNodesDirectory} copies the files of a version, or of a fixture directory, unchanged. */
    @Test
    public void installedNodesDirectoryHoldsTheFixtureFiles() throws IOException
    {
        Path nodes = DseFixtures.installNodesDirectory(folder.newFolder().toPath(), DseVersion.DSE_6_8_65, true);
        assertArrayEquals(Files.readAllBytes(DseFixtures.nodesLocal(DseVersion.DSE_6_8_65)), Files.readAllBytes(nodes.resolve("local")));
        assertArrayEquals(Files.readAllBytes(DseFixtures.nodesPeersEmpty()), Files.readAllBytes(nodes.resolve("peers")));

        Path fixture = DseFixtures.nodesDirectoryTwoDataCenters(4);
        nodes = DseFixtures.installNodesDirectory(folder.newFolder().toPath(), fixture);
        assertArrayEquals(Files.readAllBytes(fixture.resolve("local")), Files.readAllBytes(nodes.resolve("local")));
        assertArrayEquals(Files.readAllBytes(fixture.resolve("peers")), Files.readAllBytes(nodes.resolve("peers")));
    }

    @Test
    public void gossipStatesOfTheCapturedNode() throws IOException
    {
        List<DseFixtures.GossipState> states = DseFixtures.gossipStates(DseVersion.DSE_6_9_26);
        assertEquals(18, states.size());
        assertTrue(states.stream().anyMatch(s -> s.name.equals("NET_VERSION") && s.value.equals("1536")));
        assertTrue(states.stream().anyMatch(s -> s.name.equals("RELEASE_VERSION") && s.value.equals(DseVersion.DSE_6_9_26.releaseVersion)));
        assertTrue(states.stream().anyMatch(s -> s.name.equals("DSE_GOSSIP_STATE") && s.value.contains("\"dse_version\":\"6.9.26\"")));
    }

    @Test
    public void commitLogSegmentOf6926HasDseVersion680() throws IOException
    {
        assertCommitLogVersion680(DseVersion.DSE_6_9_26);
    }

    private static void assertCommitLogVersion680(DseVersion v) throws IOException
    {
        // the version and the segment id that start the uncompressed segment, read straight from the gzip stream
        try (InputStream in = DseFixtures.openCommitLogSegment(v))
        {
            assertCommitLogHeader(v, in);
        }
    }

    /** The commit log version 680 and the segment id of the file name, at the start of the uncompressed segment. */
    private static void assertCommitLogHeader(DseVersion v, InputStream in) throws IOException
    {
        byte[] head = new byte[12];
        assertEquals(12, in.readNBytes(head, 0, 12));
        ByteBuffer header = ByteBuffer.wrap(head);
        assertEquals("commit log version of " + v, 680, header.getInt());
        // CommitLog-680-<id>.log.gz
        String name = DseFixtures.commitLogSegmentGz(v).getFileName().toString();
        long id = Long.parseLong(name.substring("CommitLog-680-".length(), name.length() - ".log.gz".length()));
        assertEquals("segment id of " + v, id, header.getLong());
    }

    @Test
    public void hintsFileHeaderMatchesItsName() throws IOException
    {
        assertHintsHeader(DseVersion.DSE_6_8_65, HINTS_HOST_ID, HINTS_TIMESTAMP);

        // the host id is the one of a peer in the 6.8.65 peers file (msgpack [int64 msb, int64 lsb])
        ByteBuffer hostId = ByteBuffer.allocate(19);
        hostId.put((byte) 0x92).put((byte) 0xD3).putLong(HINTS_HOST_ID.getMostSignificantBits())
              .put((byte) 0xD3).putLong(HINTS_HOST_ID.getLeastSignificantBits());
        String peers = new String(Files.readAllBytes(DseFixtures.nodesPeers(DseVersion.DSE_6_8_65)), StandardCharsets.ISO_8859_1);
        assertTrue(peers.contains(new String(hostId.array(), StandardCharsets.ISO_8859_1)));
    }

    @Test
    public void hintsFileHeaderOf6926MatchesItsName() throws IOException
    {
        assertHintsHeader(DseVersion.DSE_6_9_26, HINTS_HOST_ID_6_9_26, HINTS_TIMESTAMP_6_9_26);
    }

    private static void assertHintsHeader(DseVersion v, UUID hostId, long timestamp) throws IOException
    {
        Path hints = DseFixtures.hintsFile(v);
        assertEquals(hostId + "-" + timestamp + "-v1.hints", hints.getFileName().toString());
        ByteBuffer header = ByteBuffer.wrap(Files.readAllBytes(hints), 0, HINTS_HEADER_SIZE);
        // 0xFFFFFFFF, not a Cassandra hints descriptor version
        assertEquals("hints descriptor version of " + v, 0xFFFFFFFF, header.getInt());
        // the timestamp and the host id of the file name
        assertEquals(timestamp, header.getLong());
        assertEquals(hostId, new UUID(header.getLong(), header.getLong()));
        // the parameters: length 2, crc, "{}", crc
        assertEquals(2, header.getInt());
        // the two crcs are those of Cassandra's HintsDescriptor: CRC32 over the fields above, each long fed as its low
        // then its high 32 bits, then continued over the parameters
        CRC32 crc = new CRC32();
        updateChecksumInt(crc, 0xFFFFFFFF);
        for (long l : new long[]{ timestamp, hostId.getMostSignificantBits(), hostId.getLeastSignificantBits() })
        {
            updateChecksumInt(crc, (int) l);
            updateChecksumInt(crc, (int) (l >>> 32));
        }
        updateChecksumInt(crc, 2);
        assertEquals("crc of the fixed fields of the hints header of " + v, (int) crc.getValue(), header.getInt());
        assertEquals('{', header.get());
        assertEquals('}', header.get());
        crc.update(new byte[]{ '{', '}' });
        assertEquals("crc of the hints header of " + v, (int) crc.getValue(), header.getInt());
        assertFalse(header.hasRemaining());
    }

    /**
     * The records of both hints files check out against their CRC32s to the last byte of the file (see
     * {@link DseFixtures#hintsRecords}), and the hints are for the fixture table and for the permissions DSE grants on
     * it (and, on 6.8.65, for DSE's nodesync table). The hints of the fixture table are those of one run of the fixture
     * workload on 6.8.65; on 6.9.26, whose file holds the hints of two runs, only a whole number of runs is checked.
     */
    @Test
    public void hintsRecordsAreFramedWithCrc32() throws IOException
    {
        for (DseVersion v : DseVersion.values())
        {
            Map<UUID, List<byte[]>> byTable = hintsByFirstTable(v);
            UUID fixtureTable = DseFixtures.fixtureTableId(v);
            Set<UUID> expected = v == DseVersion.DSE_6_8_65
                                 ? ImmutableSet.of(fixtureTable, DseFixtures.ROLE_PERMISSIONS_TABLE_ID, DseFixtures.NODESYNC_STATUS_TABLE_ID)
                                 : ImmutableSet.of(fixtureTable, DseFixtures.ROLE_PERMISSIONS_TABLE_ID);
            assertEquals("tables hinted in " + v, expected, byTable.keySet());

            // values of partition 25 (row ck2) and of the two rows of partition 100, as the fixture workload wrote them
            String fixtureHints = byTable.get(fixtureTable).stream()
                                         .map(b -> new String(b, StandardCharsets.ISO_8859_1))
                                         .collect(Collectors.joining());
            for (String value : new String[]{ "Street25", "Avenue25", "tuple25", "val50", "val199", "val200" })
                assertTrue(v + " hints lack " + value, fixtureHints.contains(value));
            // the hints of one run of the fixture workload on 6.8.65; of whole runs on 6.9.26
            int fixtureMutations = DseFixtures.fixtureHintMutations(v).size();
            if (v == DseVersion.DSE_6_8_65)
                assertEquals(DseFixtures.FIXTURE_MUTATIONS_PER_RUN, fixtureMutations);
            else
                assertTrue(v + ": " + fixtureMutations + " hints of the fixture table",
                           fixtureMutations >= DseFixtures.FIXTURE_MUTATIONS_PER_RUN && fixtureMutations % DseFixtures.FIXTURE_MUTATIONS_PER_RUN == 0);
        }
    }

    /**
     * {@link DseFixtures#writeDseHintsFile} with the host id and timestamp of a fixture file gives back its bytes, but for
     * the creation time of every hint and the two CRC32s of every record: the header (with its two CRC32s) is DSE's own.
     */
    @Test
    public void writtenDseHintsFileIsTheFixtureButForTheCreationTimes() throws IOException
    {
        long creationTime = 1790400000000L;
        for (DseVersion v : DseVersion.values())
        {
            UUID hostId = v == DseVersion.DSE_6_8_65 ? HINTS_HOST_ID : HINTS_HOST_ID_6_9_26;
            long timestamp = v == DseVersion.DSE_6_8_65 ? HINTS_TIMESTAMP : HINTS_TIMESTAMP_6_9_26;
            Path written = DseFixtures.writeDseHintsFile(v, folder.newFolder().toPath(), hostId, timestamp, creationTime);
            assertEquals(DseFixtures.hintsFile(v).getFileName(), written.getFileName());

            byte[] fixture = Files.readAllBytes(DseFixtures.hintsFile(v));
            byte[] bytes = Files.readAllBytes(written);
            assertEquals("size of the file written for " + v, fixture.length, bytes.length);
            assertArrayEquals("header of the file written for " + v, Arrays.copyOf(fixture, HINTS_HEADER_SIZE), Arrays.copyOf(bytes, HINTS_HEADER_SIZE));

            // hintsRecords checks both CRC32s of every record
            List<byte[]> original = DseFixtures.hintsRecords(v);
            List<byte[]> rewritten = DseFixtures.hintsRecords(written);
            assertEquals(original.size(), rewritten.size());
            for (int i = 0; i < original.size(); i++)
            {
                assertEquals(creationTime, ByteBuffer.wrap(rewritten.get(i)).getLong(0));
                assertArrayEquals("hint " + i + " of " + v + " past its creation time",
                                  Arrays.copyOfRange(original.get(i), Long.BYTES, original.get(i).length),
                                  Arrays.copyOfRange(rewritten.get(i), Long.BYTES, rewritten.get(i).length));
            }
        }
    }

    /** Groups the hints of a DSE hints file by the table id of their partition update. */
    private static Map<UUID, List<byte[]>> hintsByFirstTable(DseVersion v) throws IOException
    {
        Map<UUID, List<byte[]>> byTable = new HashMap<>();
        for (byte[] hint : DseFixtures.hintsRecords(v))
            byTable.computeIfAbsent(DseFixtures.hintTableId(hint), k -> new ArrayList<>()).add(hint);
        return byTable;
    }

    /** A hints file cut inside a record fails with the offset of that record, whether cut in its size or in its hint. */
    @Test
    public void aCutHintsFileFailsWithTheOffsetOfTheRecord() throws IOException
    {
        byte[] bytes = Files.readAllBytes(DseFixtures.hintsFile(DseVersion.DSE_6_9_26));
        // the second record starts after the 42-byte header and the first record (size, crc, hint, crc)
        int second = HINTS_HEADER_SIZE + 4 + 4 + DseFixtures.hintsRecords(DseVersion.DSE_6_9_26).get(0).length + 4;
        assertHintsReadFails(Arrays.copyOf(bytes, second + 2), "hints record at " + second + " in ", " truncated in its size");
        assertHintsReadFails(Arrays.copyOf(bytes, second + 8 + 10), "hints record at " + second + " in ", " truncated in its hint");
    }

    /**
     * A flipped byte in a hint or in the CRC32 of its size fails with the offset of its record; a header cut before its
     * parameters fails too.
     */
    @Test
    public void aCorruptHintsFileFails() throws IOException
    {
        byte[] bytes = Files.readAllBytes(DseFixtures.hintsFile(DseVersion.DSE_6_9_26));
        int second = HINTS_HEADER_SIZE + 4 + 4 + DseFixtures.hintsRecords(DseVersion.DSE_6_9_26).get(0).length + 4;
        byte[] corrupt = bytes.clone();
        corrupt[second + 8 + 10] ^= 0x01;
        assertHintsReadFails(corrupt, "crc of the hints record at " + second + " in ", "");
        byte[] corruptSize = bytes.clone();
        corruptSize[second + 4] ^= 0x01;
        assertHintsReadFails(corruptSize, "size crc of the hints record at " + second + " in ", "");
        assertHintsReadFails(Arrays.copyOf(bytes, 20), "hints header of ", " truncated before its parameters length");
    }

    private void assertHintsReadFails(byte[] cut, String prefix, String suffix) throws IOException
    {
        Path file = folder.newFile().toPath();
        Files.write(file, cut);
        AssertionError failure = null;
        try
        {
            DseFixtures.hintsRecords(file);
        }
        catch (AssertionError e)
        {
            failure = e;
        }
        assertNotNull("a damaged hints file of " + cut.length + " bytes was read", failure);
        assertTrue(failure.getMessage(), failure.getMessage().startsWith(prefix) && failure.getMessage().contains(suffix));
    }
}
