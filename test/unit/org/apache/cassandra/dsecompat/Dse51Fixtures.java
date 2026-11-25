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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.cassandra.dsecompat.DseFixtures.GossipState;
import org.apache.cassandra.utils.Hex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Access to the artefacts of real DataStax Enterprise 5.1.51 nodes, reached through {@link DseFixtures#dse51()}:
 * {@code system.local} and {@code system.peers} sstables after a flush and after a compaction (of a node with the
 * Cassandra workload only and of a node with Search and Graph), user sstables (the collection table, a materialized
 * view, a SASI index, two compact tables), {@code system_schema} and {@code system_auth} with DSE 5.1 grants and a
 * row-level access control restriction, commit log segments, a hints file, {@code system.batches} and
 * {@code system.paxos} sstables, transaction logs, gossip states, internode handshake and message bytes, and the
 * configuration as installed and as running. The files live under {@code test/data/dse-compat}, in the directories
 * whose name starts with {@code dse-5.1.51}, and their provenance is in the 5.1.51 section of that directory's README.
 * DSE 5.1.51 is not a value of {@link DseFixtures.DseVersion}: the DSE 6 tests iterate that enum.
 */
public final class Dse51Fixtures
{
    static final Dse51Fixtures INSTANCE = new Dse51Fixtures();

    /** The directory name of the fixtures of the three-node cluster with the Cassandra workload. */
    public static final String DIR = "dse-5.1.51";
    /** The directory name of the fixtures of the one-node cluster started with Search and Graph. */
    public static final String SEARCH_GRAPH_DIR = DIR + "-search-graph";
    /** What a DSE 5.1.51 node gossips as {@code RELEASE_VERSION} and stores as {@code release_version}. */
    public static final String RELEASE_VERSION = "3.11.3.5151";
    /** What a DSE 5.1.51 node gossips as {@code dse_version} and stores in the {@code dse_version} columns. */
    public static final String DSE_VERSION = "5.1.51";
    /** The messaging version of DSE 5.1.51: in its handshake, its {@code NET_VERSION} and its stored rows. */
    public static final int MESSAGING_VERSION = 11;
    /** The version of the commit log segments DSE 5.1.51 writes. */
    public static final int COMMIT_LOG_VERSION = 6;
    /** The version of the hints files DSE 5.1.51 writes. */
    public static final int HINTS_VERSION = 1;
    /** The sstable version DSE 5.1.51 writes, in the big format. */
    public static final String SSTABLE_VERSION = "me";

    /** The nodes of the three-node cluster: their number is the last digit of their addresses. */
    public static final int CLUSTER_NODES = 3;

    /** The host ids of nodes 1 to 3 of the three-node cluster, in node order. */
    public static final List<UUID> HOST_IDS =
        Collections.unmodifiableList(Arrays.asList(UUID.fromString("c8896567-fb1e-4ce9-bb92-c8a31586e679"),
                                                   UUID.fromString("a7314433-9ca0-4bbc-9d7a-6163c4ce7ce9"),
                                                   UUID.fromString("1971c79a-2f22-4458-8f99-61dab1d68f7f")));

    /** The host id of the node of the one-node Search and Graph cluster. */
    public static final UUID SEARCH_GRAPH_HOST_ID = UUID.fromString("5f5e669a-efde-460d-bba4-5a89e25f26c3");

    /** The id of {@code test_ks_fixture.t}, the collection table of the commit log and hints fixtures. */
    public static final UUID FIXTURE_TABLE_ID = UUID.fromString("b6aa4f10-d9c7-11f1-9bf3-35f92a6d7d8d");

    /** The hints in the hints file: the 203 mutations of the collection table, written with its target down. */
    public static final int FIXTURE_HINTS = 203;

    /** The id of {@code test_ks.rlac}, the table restricted by row-level access control. */
    public static final UUID RLAC_TABLE_ID = UUID.fromString("a592c7f0-d9c9-11f1-9381-4ff58d61cdfc");

    /** The id of {@code test_ks.rlac_by_v}, the materialized view restricted and then unrestricted. */
    public static final UUID RLAC_VIEW_ID = UUID.fromString("a6158c80-d9c9-11f1-9381-4ff58d61cdfc");

    /** The id of {@code test_ks.lwt}, the table of the {@code system.paxos} rows. */
    public static final UUID LWT_TABLE_ID = UUID.fromString("ff56c801-d9c8-11f1-9d05-ff904c15f3ab");

    /** The id of {@code test_ks.bt}, the table of the {@code system.batches} rows. */
    public static final UUID BATCH_TABLE_ID = UUID.fromString("feac0910-d9c8-11f1-9d05-ff904c15f3ab");

    /** The names of the message files under {@code messages/dse-5.1.51/}. */
    public static final List<String> MESSAGE_FILES =
        Collections.unmodifiableList(Arrays.asList("repair-incremental", "repair-full-sequential", "rlac-read",
                                                   "gossip-digest-ack"));

    private Dse51Fixtures()
    {
    }

    /**
     * Whether {@code path}, a path under {@link DseFixtures#ROOT} as {@code Files.walk} of that directory gives it, is
     * one of these fixtures: whether one of its name elements below the root, a directory or the file itself, starts
     * with {@code dse-5.1.51}.
     */
    public static boolean isDse51Fixture(Path path)
    {
        if (!path.startsWith(DseFixtures.ROOT))
            throw new IllegalArgumentException(path + " is not under " + DseFixtures.ROOT);
        for (Path part : DseFixtures.ROOT.relativize(path))
            if (part.toString().startsWith(DIR))
                return true;
        return false;
    }

    /**
     * The CQL creating {@code test_ks_fixture.t}, the collection table of the commit log and hints fixtures, as on the
     * DSE 5.1.51 cluster, with its id there ({@link #FIXTURE_TABLE_ID}; replication factor 1 for a single test node):
     * a replay or a decoding of its mutations needs that id.
     */
    public List<String> fixtureSchema()
    {
        return DseFixtures.fixtureSchema(FIXTURE_TABLE_ID);
    }

    /** The {@code nodetool gossipinfo} of node {@code node} (1 to 3) of the three-node cluster. */
    public Path gossipInfo(int node)
    {
        return existing(DseFixtures.ROOT.resolve("gossip").resolve(DIR)
                                        .resolve("gossipinfo-node" + checkNode(node) + ".txt"));
    }

    /** The {@code nodetool gossipinfo} of the Search and Graph node. */
    public Path searchGraphGossipInfo()
    {
        return existing(DseFixtures.ROOT.resolve("gossip").resolve(SEARCH_GRAPH_DIR).resolve("gossipinfo.txt"));
    }

    /**
     * The application states of the endpoint {@code endpoint} (as {@code nodetool gossipinfo} prints it, such as
     * {@code /127.0.51.11}) in a gossipinfo file, in the order printed; {@code TOKENS} is {@code <hidden>} there.
     * Nodetool of DSE 5.1.51 prints the DSE state under the Apache name of its ordinal, {@code X_11_PADDING}.
     */
    public List<GossipState> gossipStates(Path gossipInfo, String endpoint) throws IOException
    {
        List<GossipState> states = new ArrayList<>();
        boolean in = false;
        for (String line : Files.readAllLines(gossipInfo))
        {
            if (!line.startsWith(" "))
            {
                in = line.trim().equals(endpoint);
                continue;
            }
            String[] parts = line.trim().split(":", 3);
            if (in && parts.length == 3)
                states.add(new GossipState(parts[0], Integer.parseInt(parts[1]), parts[2]));
        }
        assertFalse("no application state of " + endpoint + " in " + gossipInfo, states.isEmpty());
        return states;
    }

    /**
     * The {@code system.local} and {@code system.peers} sstables of node 1 of the three-node cluster: with
     * {@code compacted} false, the table directories as they were after {@code nodetool flush system}, with true,
     * after {@code nodetool compact system local} and {@code nodetool compact system peers}. Each holds the table
     * directories ({@code local-<id>}, {@code peers-<id>}) as in the node's data directory.
     */
    public Path systemSSTables(boolean compacted)
    {
        return existing(DseFixtures.ROOT.resolve("system").resolve(DIR).resolve(compacted ? "compacted" : "flushed"));
    }

    /**
     * As {@link #systemSSTables}, of the Search and Graph node: it has no peer, its flushed peers sstable holds a
     * partition deletion of its own address only.
     */
    public Path searchGraphSystemSSTables(boolean compacted)
    {
        return existing(DseFixtures.ROOT.resolve("system").resolve(SEARCH_GRAPH_DIR)
                                        .resolve(compacted ? "compacted" : "flushed"));
    }

    /** What cqlsh answered on node {@code node} for {@code SELECT * FROM system.local} and {@code system.peers}. */
    public Path systemSelect(int node)
    {
        return existing(DseFixtures.ROOT.resolve("system").resolve(DIR)
                                        .resolve("select-local-peers-node" + checkNode(node) + ".txt"));
    }

    /** What cqlsh answered on the Search and Graph node for {@code SELECT *} from its local and peers tables. */
    public Path searchGraphSystemSelect()
    {
        return existing(DseFixtures.ROOT.resolve("system").resolve(SEARCH_GRAPH_DIR).resolve("select-local-peers.txt"));
    }

    /**
     * The user sstables of node 1, one flush each: the directory holds {@code test_ks/<table>-<id>/} for the
     * collection table {@code t}, {@code wide} and its view {@code wide_by_v}, {@code sasi_t} with its SASI index,
     * and the compact tables {@code compact_dense} and {@code compact_static}.
     */
    public Path userSSTables()
    {
        return existing(DseFixtures.ROOT.resolve("sstables").resolve(DIR + "-user"));
    }

    /**
     * A data directory of node 1 holding DSE's {@code system_schema} (every table) and
     * {@code system_auth/role_permissions} after the grants, the restrictions and the {@code UNRESTRICT ROWS} on the
     * view of the README ({@code system_auth.roles}, which holds password hashes, is left out).
     */
    public Path schemaDataDirectory()
    {
        return existing(DseFixtures.ROOT.resolve("schema").resolve(DIR).resolve("sstables"));
    }

    /**
     * DSE's {@code sstablemetadata} of the sstables of {@code dir}, one of {@link #systemSSTables},
     * {@link #searchGraphSystemSSTables} (both flushed and compacted, in one file) and {@link #userSSTables()}.
     */
    public Path sstableMetadataDump(Path dir)
    {
        if (dir.equals(userSSTables()))
            return existing(dir.resolve("sstablemetadata.txt"));
        if (dir.equals(searchGraphSystemSSTables(false)) || dir.equals(searchGraphSystemSSTables(true)))
            return existing(dir.getParent().resolve("sstablemetadata.txt"));
        if (dir.equals(systemSSTables(false)) || dir.equals(systemSSTables(true)))
            return existing(dir.getParent().resolve("sstablemetadata-" + dir.getFileName() + ".txt"));
        throw new IllegalArgumentException(dir + " has no sstablemetadata dump in the DSE 5.1.51 fixtures");
    }

    /**
     * A text file of the schema fixture: {@code extensions-restricted.txt}, {@code extensions-after-unrestrict.txt},
     * {@code extensions-per-node.txt} or {@code select-system-auth.txt}.
     */
    public Path schemaFile(String name)
    {
        return existing(DseFixtures.ROOT.resolve("schema").resolve(DIR).resolve(name));
    }

    /**
     * The table directory {@code <table>-<id>} of {@code table} under a directory of table directories, such as
     * {@link #systemSSTables} or {@code userSSTables().resolve("test_ks")}.
     */
    public Path tableDirectory(Path parent, String table) throws IOException
    {
        try (Stream<Path> s = Files.list(existing(parent)))
        {
            List<Path> matches = s.filter(p -> p.getFileName().toString().matches(table + "-[0-9a-f]{32}"))
                                  .collect(Collectors.toList());
            assertEquals("table directories of " + table + " under " + parent + ": " + matches, 1, matches.size());
            return matches.get(0);
        }
    }

    /** Every sstable component file ({@code <version>-<generation>-big-<component>}) under {@code dir}, sorted. */
    public List<Path> sstableComponents(Path dir) throws IOException
    {
        try (Stream<Path> s = Files.walk(existing(dir)))
        {
            return s.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().matches("[a-z]{2}-.*-big-.*"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /** The active commit log segment of node 1, gzipped, taken after the hints of {@link #hintsFile()} were written. */
    public Path commitLogSegmentGz() throws IOException
    {
        return single(DseFixtures.ROOT.resolve("commitlog").resolve(DIR), "CommitLog-6-", ".log.gz");
    }

    /**
     * The commit log directory of node 1 killed without a drain after a peer's restart, its two segments gzipped:
     * the older one, the segment of {@link #commitLogSegmentGz()} continued, also holds the writes DSE's endpoint
     * state persister made to {@code system.peers} for the restarted peer; the newer one is the next segment, its
     * header only.
     */
    public Path killedCommitLogDirectory()
    {
        return existing(DseFixtures.ROOT.resolve("commitlog").resolve(DIR + "-killed"));
    }

    /** Unpacks every {@code CommitLog-*.log.gz} of {@code gzDirectory} into {@code targetDir} under its own name. */
    public List<Path> unpackCommitLogSegments(Path gzDirectory, Path targetDir) throws IOException
    {
        List<Path> unpacked = new ArrayList<>();
        Files.createDirectories(targetDir);
        try (Stream<Path> s = Files.list(existing(gzDirectory)))
        {
            List<Path> segments = s.filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                                   .sorted()
                                   .collect(Collectors.toList());
            for (Path gz : segments)
            {
                String name = gz.getFileName().toString();
                Path target = targetDir.resolve(name.substring(0, name.length() - ".gz".length()));
                try (InputStream in = new GZIPInputStream(Files.newInputStream(gz)))
                {
                    Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
                }
                unpacked.add(target);
            }
        }
        assertFalse("no commit log segment in " + gzDirectory, unpacked.isEmpty());
        return unpacked;
    }

    /** The hints file node 1 wrote for node 3 while node 3 was down ({@code <host id>-<timestamp>-1.hints}). */
    public Path hintsFile() throws IOException
    {
        return single(DseFixtures.ROOT.resolve("hints").resolve(DIR), HOST_IDS.get(2) + "-", "-1.hints");
    }

    /** The {@code system.batches} table directory of node 2, holding the two batches left there, one sstable. */
    public Path batchesSSTableDirectory() throws IOException
    {
        return tableDirectory(DseFixtures.ROOT.resolve("batches").resolve(DIR), "batches");
    }

    /** The {@code system.paxos} table directory of node 1, holding the rows of two committed rounds, one sstable. */
    public Path paxosSSTableDirectory() throws IOException
    {
        return tableDirectory(DseFixtures.ROOT.resolve("paxos").resolve(DIR), "paxos");
    }

    /** What cqlsh answered on node 2 for the {@code system.batches} rows of {@link #batchesSSTableDirectory()}. */
    public Path batchesSelect()
    {
        return existing(DseFixtures.ROOT.resolve("batches").resolve(DIR).resolve("select-batches.txt"));
    }

    /** What cqlsh answered on node 1 for the {@code system.paxos} rows of {@link #paxosSSTableDirectory()}. */
    public Path paxosSelect()
    {
        return existing(DseFixtures.ROOT.resolve("paxos").resolve(DIR).resolve("select-paxos.txt"));
    }

    /**
     * The transaction log of a compaction of four sstables: with {@code committed} false, as it was while the
     * compaction ran (its {@code ADD} record only), with true, as it was once committed, just before DSE deleted it.
     */
    public Path transactionLog(boolean committed) throws IOException
    {
        return single(DseFixtures.ROOT.resolve("txn").resolve(DIR).resolve(committed ? "committed" : "in-progress"),
                      "me_txn_compaction_", ".log");
    }

    /** The configuration directory as installed ({@code installed}) or as node 1 ran it ({@code running}). */
    public Path confDirectory(boolean running)
    {
        return existing(DseFixtures.ROOT.resolve("conf").resolve(DIR).resolve(running ? "running" : "installed"));
    }

    /** One storage-port connection of DSE 5.1.51: its handshake messages and the first message after them. */
    public static final class Handshake
    {
        /** The broadcast address of the connecting node, as its {@code msg3} names it. */
        public final String client;
        /** The broadcast address the connection was opened to. */
        public final String server;
        /** Message 1, sent by the connecting node: {@code CA 55 2D FA}, then the header {@code version << 8}. */
        public final byte[] msg1;
        /** Message 2, the 4-byte messaging version the accepting node answers with. */
        public final byte[] msg2;
        /** Message 3, sent by the connecting node: its 4-byte messaging version and its address. */
        public final byte[] msg3;
        /** The first message the connecting node sends after the handshake, magic to the end of its payload. */
        public final byte[] message1;

        Handshake(String client, String server, Map<String, byte[]> parts, Path file)
        {
            this.client = client;
            this.server = server;
            this.msg1 = part(parts, "msg1", file);
            this.msg2 = part(parts, "msg2", file);
            this.msg3 = part(parts, "msg3", file);
            this.message1 = part(parts, "message1", file);
        }

        private byte[] part(Map<String, byte[]> parts, String label, Path file)
        {
            byte[] bytes = parts.get(label);
            assertNotNull("connection " + client + " -> " + server + " in " + file + " lacks " + label, bytes);
            return bytes;
        }
    }

    private static final List<String> HANDSHAKE_PARTS = Arrays.asList("msg1", "msg2", "msg3", "message1");

    /** The two handshakes captured while node 3 restarted: node 1 connecting to it, then it connecting to node 1. */
    public List<Handshake> handshakes() throws IOException
    {
        Path file = existing(DseFixtures.ROOT.resolve("handshake").resolve(DIR + ".txt"));
        List<Handshake> result = new ArrayList<>();
        String client = null;
        String server = null;
        Map<String, byte[]> parts = new LinkedHashMap<>();
        for (String line : Files.readAllLines(file))
        {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#"))
                continue;
            String[] tokens = line.split("\\s+");
            if (tokens[0].equals("connection"))
            {
                assertTrue("malformed line in " + file + ": " + line, tokens.length == 4 && tokens[2].equals("->"));
                if (client != null)
                    result.add(new Handshake(client, server, parts, file));
                client = tokens[1];
                server = tokens[3];
                parts = new LinkedHashMap<>();
            }
            else
            {
                assertTrue("malformed line in " + file + ": " + line, tokens.length == 2);
                assertTrue("unknown label " + tokens[0] + " in " + file, HANDSHAKE_PARTS.contains(tokens[0]));
                assertNotNull(tokens[0] + " before any connection line in " + file, client);
                assertNull("duplicate " + tokens[0] + " in " + file, parts.put(tokens[0], hex(tokens[1])));
            }
        }
        if (client != null)
            result.add(new Handshake(client, server, parts, file));
        assertFalse("no handshake in " + file, result.isEmpty());
        return result;
    }

    /** One internode message captured between two DSE 5.1.51 nodes. */
    public static final class Message
    {
        /** The name the message file gives it, such as the repair message type or {@code partition-read}. */
        public final String name;
        /** The verb, as DSE 5.1.51 names the ordinal the message carries. */
        public final String verb;
        /** The broadcast address of the sender. */
        public final String from;
        /** The broadcast address of the receiver. */
        public final String to;
        /** The whole message: magic, id, timestamp, sender address, verb, parameters, payload size and payload. */
        public final byte[] bytes;

        Message(String name, String verb, String from, String to, byte[] bytes)
        {
            this.name = name;
            this.verb = verb;
            this.from = from;
            this.to = to;
            this.bytes = bytes;
        }
    }

    /** The messages of {@code messages/dse-5.1.51/<name>.txt} ({@link #MESSAGE_FILES}), in send order. */
    public List<Message> messages(String name) throws IOException
    {
        assertTrue("not a message file of the fixtures: " + name, MESSAGE_FILES.contains(name));
        Path file = existing(DseFixtures.ROOT.resolve("messages").resolve(DIR).resolve(name + ".txt"));
        List<Message> result = new ArrayList<>();
        for (String line : Files.readAllLines(file))
        {
            if (line.isEmpty() || line.startsWith("#"))
                continue;
            String[] tokens = line.split(" ");
            assertEquals("malformed line in " + file + ": " + line, 5, tokens.length);
            result.add(new Message(tokens[0], tokens[1], tokens[2], tokens[3], hex(tokens[4])));
        }
        assertFalse("no message in " + file, result.isEmpty());
        return result;
    }

    private static int checkNode(int node)
    {
        if (node < 1 || node > CLUSTER_NODES)
            throw new IllegalArgumentException("node " + node + " is not one of the 1 to " + CLUSTER_NODES
                                               + " of the DSE 5.1.51 cluster");
        return node;
    }

    private static Path single(Path dir, String prefix, String suffix) throws IOException
    {
        try (Stream<Path> s = Files.list(existing(dir)))
        {
            List<Path> matches = s.filter(p -> p.getFileName().toString().startsWith(prefix)
                                               && p.getFileName().toString().endsWith(suffix))
                                  .collect(Collectors.toList());
            assertEquals("files " + prefix + '*' + suffix + " under " + dir + ": " + matches, 1, matches.size());
            return matches.get(0);
        }
    }

    private static Path existing(Path path)
    {
        assertTrue("missing fixture " + path.toAbsolutePath(), Files.exists(path));
        return path;
    }

    private static byte[] hex(String s)
    {
        assertTrue("not an even number of hex digits: " + s, s.matches("([0-9a-fA-F]{2})+"));
        return Hex.hexToBytes(s);
    }
}
