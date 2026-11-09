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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Access to the artefacts of real DataStax Enterprise 6.8.65 and 6.9.26 nodes that the DSE 6 compatibility
 * work is tested against: node metadata files (also of a two-data-center cluster and of mixed clusters),
 * sstables with SAI components, the sstables of a materialized view and its base table, a schema data
 * directory and a schema dump, a commit log segment and a hints file of each version, {@code system.paxos}
 * rows and sstables, gossip states, internode handshake bytes, and the configuration the test setup rendered
 * for a DSE node. The files live under {@code test/data/dse-compat} and their provenance is documented in the
 * README of that directory.
 */
public final class DseFixtures
{
    public static final Path ROOT = Paths.get("test", "data", "dse-compat");

    /** The size of DSE's hints file header without its parameters: version, timestamp, host id, parameters length, two CRC32s. */
    private static final int HINTS_HEADER_SIZE = 4 + 8 + 16 + 4 + 4 + 4;

    /** The two DSE versions the fixtures were produced with. */
    public enum DseVersion
    {
        DSE_6_8_65("dse-6.8.65", "4.0.0.6865", 4),
        DSE_6_9_26("dse-6.9.26", "4.0.0.6926", 6);

        /** Directory name under the fixture root. */
        public final String dir;
        /** What the node gossips as RELEASE_VERSION. */
        public final String releaseVersion;
        /** DSE's "relative" messaging version, the byte it puts in the first handshake message. */
        public final int relativeMessagingVersion;

        DseVersion(String dir, String releaseVersion, int relativeMessagingVersion)
        {
            this.dir = dir;
            this.releaseVersion = releaseVersion;
            this.relativeMessagingVersion = relativeMessagingVersion;
        }
    }

    private DseFixtures()
    {
    }

    /** {@code nodes/local} of a node of the given version (a msgpack map). */
    public static Path nodesLocal(DseVersion version)
    {
        return existing(ROOT.resolve("nodes").resolve(version.dir).resolve("local"));
    }

    /** {@code nodes/peers} of a node of the given version with at least one peer (a msgpack array). */
    public static Path nodesPeers(DseVersion version)
    {
        return existing(ROOT.resolve("nodes").resolve(version.dir).resolve("peers"));
    }

    /**
     * {@code nodes/peers} as DSE 6.9.26 writes it on a single node: an empty msgpack map. The file is
     * version-independent: {@link #installNodesDirectory} uses it for either version.
     */
    public static Path nodesPeersEmpty()
    {
        return existing(ROOT.resolve("nodes").resolve(DseVersion.DSE_6_9_26.dir).resolve("peers-empty"));
    }

    /** {@code nodes/local} of DSE 6.9.26 after a {@code TRUNCATE}: carries one truncation record. */
    public static Path nodesLocalAfterTruncate()
    {
        return existing(ROOT.resolve("nodes").resolve(DseVersion.DSE_6_9_26.dir).resolve("local-after-truncate"));
    }

    /**
     * Copies the node files of a version into {@code <metadataDir>/nodes/} as a DSE node leaves them,
     * so that a reader can be pointed at {@code metadataDir}. With {@code emptyPeers} the peers file is
     * {@link #nodesPeersEmpty()}, the same version-independent empty map whatever the version.
     */
    public static Path installNodesDirectory(Path metadataDir, DseVersion version, boolean emptyPeers) throws IOException
    {
        Path nodes = Files.createDirectories(metadataDir.resolve("nodes"));
        Files.copy(nodesLocal(version), nodes.resolve("local"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(emptyPeers ? nodesPeersEmpty() : nodesPeers(version), nodes.resolve("peers"), StandardCopyOption.REPLACE_EXISTING);
        return nodes;
    }

    /**
     * Directory holding one complete trie ({@code bb}) sstable of the collection table
     * {@code t (id int, ck text, fu frozen<addr>, l list<int>, m map<text,int>, n bigint, s int static,
     * tp frozen<tuple<int,text>>, u addr, v text, PRIMARY KEY (id, ck))} with an SAI index on {@code v}:
     * 100 partitions of two rows (200 rows), then {@code DELETE FROM t WHERE id = 1},
     * {@code DELETE v FROM t WHERE id = 2 AND ck = 'ck1'} and
     * {@code DELETE FROM t WHERE id = 3 AND ck >= 'ck1' AND ck <= 'ck2'}: 196 live regular rows, plus the
     * static row of partition 3 that survives the range deletion, so {@code SELECT count(*)} answers 197;
     * {@code v = 'val50'} is row (25, ck2).
     */
    public static Path sstableDirectory(DseVersion version)
    {
        return existing(ROOT.resolve("sstables").resolve(version.dir + "-t"));
    }

    /** Every component file of {@link #sstableDirectory(DseVersion)}. */
    public static List<Path> sstableComponents(DseVersion version) throws IOException
    {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(sstableDirectory(version)))
        {
            s.filter(p -> p.getFileName().toString().startsWith("bb-1-bti-")).sorted().forEach(files::add);
        }
        assertTrue("expected the 8 sstable components and the SAI ones under " + sstableDirectory(version) + ", found " + files,
                   files.size() > 8);
        return files;
    }

    /**
     * Directory holding one complete trie ({@code bb}) sstable of the DSE 6.8.65 materialized view
     * {@code lab.wide_by_v (v text, pk int, ck int, PRIMARY KEY (v, pk, ck))} over
     * {@code lab.wide (pk int, ck int, v text, PRIMARY KEY (pk, ck))}: 5000 rows, each with the primary key
     * liveness of its base row and a cell of DSE's hidden column {@code .v} (empty value, same timestamp),
     * which the header lists as the only regular column. {@link #viewSelectStar()} is what DSE answers for
     * {@code SELECT * FROM lab.wide_by_v}.
     */
    public static Path viewSSTableDirectory()
    {
        return existing(viewFixtureRoot().resolve("wide_by_v"));
    }

    /**
     * Directory holding the sstable of the base table {@code lab.wide} flushed by the same node at the same time as
     * {@link #viewSSTableDirectory()}: 10 partitions ({@code pk} 0 to 9) of 500 rows ({@code ck} 0 to 499) with
     * {@code v = 'v<ck % 7>'}.
     */
    public static Path viewBaseSSTableDirectory()
    {
        return existing(viewFixtureRoot().resolve("wide"));
    }

    /** DSE's {@code sstabledump} of {@link #viewSSTableDirectory()}, gzipped JSON. */
    public static Path viewSSTableDump()
    {
        return existing(viewFixtureRoot().resolve("sstabledump-wide_by_v.json.gz"));
    }

    /** Every component file of {@link #viewSSTableDirectory()}. */
    public static List<Path> viewSSTableComponents() throws IOException
    {
        return components(viewSSTableDirectory());
    }

    /** Every component file of {@link #viewBaseSSTableDirectory()}. */
    public static List<Path> viewBaseSSTableComponents() throws IOException
    {
        return components(viewBaseSSTableDirectory());
    }

    /** The directory of the DSE 6.8.65 materialized view fixture: the view's sstable, the base table's, and what DSE answered. */
    private static Path viewFixtureRoot()
    {
        return ROOT.resolve("sstables").resolve(DseVersion.DSE_6_8_65.dir + "-mv");
    }

    /** The 8 components of the sstable {@code bb-1-bti} under {@code dir}. */
    private static List<Path> components(Path dir) throws IOException
    {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(dir))
        {
            s.filter(p -> p.getFileName().toString().startsWith("bb-1-bti-")).sorted().forEach(files::add);
        }
        assertEquals("components of the sstable under " + dir + ": " + files, 8, files.size());
        return files;
    }

    /**
     * The rows DSE 6.8.65 answered for {@code SELECT * FROM lab.wide_by_v} (consistency ALL, paging off) over the
     * data of {@link #viewSSTableDirectory()}, in the order cqlsh printed them, as {@code [v, pk, ck]} triples.
     */
    public static List<Object[]> viewSelectStar() throws IOException
    {
        Path file = existing(viewFixtureRoot().resolve("select-star.txt"));
        Pattern row = Pattern.compile("^\\s*(v\\d+)\\s*\\|\\s*(\\d+)\\s*\\|\\s*(\\d+)\\s*$");
        Pattern count = Pattern.compile("^\\((\\d+) rows\\)$");
        List<Object[]> rows = new ArrayList<>();
        int announced = -1;
        for (String line : Files.readAllLines(file))
        {
            Matcher m = row.matcher(line);
            if (m.matches())
                rows.add(new Object[]{ m.group(1), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)) });
            Matcher c = count.matcher(line.trim());
            if (c.matches())
                announced = Integer.parseInt(c.group(1));
        }
        assertEquals("rows cqlsh announced in " + file + " against the rows parsed", announced, rows.size());
        return rows;
    }

    /**
     * What {@code SELECT count(*)} answers on the collection table (see {@link #sstableDirectory(DseVersion)}):
     * the {@link #COLLECTION_TABLE_LIVE_REGULAR_ROWS} plus one row for each of the
     * {@link #COLLECTION_TABLE_STATIC_ONLY_PARTITIONS}.
     */
    public static final int COLLECTION_TABLE_COUNT_STAR = 197;

    /** Live regular (clustered) rows of the collection table: 200 inserted, the two of partition 1 and the two of partition 3 deleted. */
    public static final int COLLECTION_TABLE_LIVE_REGULAR_ROWS = 196;

    /** Partitions of the collection table left with only their static row: partition 3 ({@code s = 30}). */
    public static final int COLLECTION_TABLE_STATIC_ONLY_PARTITIONS = 1;

    /**
     * One captured storage-port handshake: the three messages and the first frame after them,
     * as sent between two DSE nodes of the same version. Every part is present in the fixture files.
     */
    public static final class Handshake
    {
        public final String client;
        public final String server;
        /** Message 1, sent by the connecting node: {@code CA 55 2D FA <relative> 00 FF 00}. */
        public final byte[] msg1;
        /** Message 2, the 4-byte version the accepting node answers with ({@code relative << 8}). */
        public final byte[] msg2;
        /** Message 3, sent by the connecting node: version, address, DSE parameters. */
        public final byte[] msg3;
        /**
         * The first frame the connecting node sends after the handshake (DSE's own framing, a gossip SYN):
         * a 4-byte big-endian length and that many bytes. It is cut out of the TCP segment that follows
         * message 3, which can carry more than one frame and was itself cut by the capture's 256-byte snaplen.
         */
        public final byte[] frame1;

        Handshake(String client, String server, Map<String, byte[]> parts, Path file)
        {
            this.client = client;
            this.server = server;
            this.msg1 = part(parts, "msg1", file);
            this.msg2 = part(parts, "msg2", file);
            this.msg3 = part(parts, "msg3", file);
            this.frame1 = part(parts, "frame1", file);
        }

        private byte[] part(Map<String, byte[]> parts, String label, Path file)
        {
            byte[] bytes = parts.get(label);
            assertNotNull("connection " + client + " -> " + server + " in " + file + " lacks " + label, bytes);
            return bytes;
        }
    }

    private static final List<String> HANDSHAKE_PARTS = Arrays.asList("msg1", "msg2", "msg3", "frame1");

    /** The handshakes captured for a version, in capture order (two per file). */
    public static List<Handshake> handshakes(DseVersion version) throws IOException
    {
        Path file = existing(ROOT.resolve("handshake").resolve(version.dir + ".txt"));
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

    /**
     * The whole schema of a DSE cluster as cqlsh describes it: the system keyspaces and every keyspace DSE creates
     * (with their replication, types and tables, and table options such as {@code nodesync}) plus the user keyspace
     * {@code test_ks}, then the virtual keyspaces as comments. Only 6.9.26 so far.
     */
    public static Path describeSchema(DseVersion version)
    {
        return existing(ROOT.resolve("schema").resolve(version.dir).resolve("describe-schema.txt"));
    }

    /**
     * A data directory of a DSE 6.8.65 node holding only DSE's {@code system_schema} (every table, as the node
     * left it) and {@code system_auth/role_permissions} sstables, copied from the second node of the three-node test
     * cluster {@code dse-lab} on 2026-10-02, while nodes 1 and 2 ran DSE and node 3 already ran this software: the
     * keyspaces of the test data set, the materialized view {@code lab.wide_by_v}, DSE core's own keyspaces, and the
     * grants DSE and the test setup made.
     */
    public static Path schemaDataDirectory()
    {
        return existing(ROOT.resolve("schema").resolve(DseVersion.DSE_6_8_65.dir).resolve("sstables"));
    }

    /**
     * The configuration of the same DSE 6.8.65 node as {@link #schemaDataDirectory()}: {@code cassandra.yaml},
     * {@code dse.yaml} and {@code cassandra-rackdc.properties} as the test setup rendered them (the two YAML files
     * without comments, {@code cassandra-rackdc.properties} with the setup's header comment), and the
     * {@code ds_version.txt} of its DSE installation.
     */
    public static Path confDirectory()
    {
        return existing(ROOT.resolve("conf").resolve(DseVersion.DSE_6_8_65.dir));
    }

    /**
     * {@code nodes/local} and {@code nodes/peers} of the same DSE 6.8.65 node as {@link #schemaDataDirectory()},
     * taken at the same time: one DSE peer (192.168.57.11) and one peer already running this software
     * (192.168.57.13, release {@code 5.0.7.0-SNAPSHOT}).
     */
    public static Path nodesDirectoryMixed()
    {
        return existing(ROOT.resolve("nodes").resolve(DseVersion.DSE_6_8_65.dir + "-mixed"));
    }

    /** The number of nodes of the two-data-center DSE 6.9.26 test cluster of {@link #nodesDirectoryTwoDataCenters}. */
    public static final int TWO_DC_NODES = 6;

    /**
     * {@code nodes/local} and {@code nodes/peers} of node {@code node} (1 to {@link #TWO_DC_NODES}) of the two-data-center
     * DSE 6.9.26 test cluster {@code dse-lab}, taken while every node ran DSE: 192.168.57.1&lt;node&gt;,
     * data center {@code dc1} for nodes 1 to 3 and {@code dc2} for nodes 4 to 6, {@code rack1} for the first two nodes of each
     * data center and {@code rack2} for the third; every file lists the five other nodes.
     */
    public static Path nodesDirectoryTwoDataCenters(int node)
    {
        if (node < 1 || node > TWO_DC_NODES)
            throw new IllegalArgumentException("node " + node + " is not one of the 1 to " + TWO_DC_NODES + " of the two-data-center fixture");
        return existing(ROOT.resolve("nodes").resolve(DseVersion.DSE_6_9_26.dir + "-2dc").resolve("node" + node));
    }

    /**
     * {@code nodes/local} and {@code nodes/peers} of node 1 or node 4 of the same cluster as
     * {@link #nodesDirectoryTwoDataCenters}, taken in the mixed phase, once nodes 4 to 6 ({@code dc2}) ran this software:
     * DSE's peers file of node 1 lists the three nodes of {@code dc2} with the release {@code 5.0.7.0-SNAPSHOT} and the DSE
     * version {@code 6.8.1} upgraded nodes gossip to DSE nodes; the one of node 4, as DSE wrote it before node 4 was switched,
     * lists nodes 5 and 6 with that release and DSE version. The local files are byte for byte those of
     * {@link #nodesDirectoryTwoDataCenters}.
     */
    public static Path nodesDirectoryTwoDataCentersMixed(int node)
    {
        if (node != 1 && node != 4)
            throw new IllegalArgumentException("the mixed two-data-center fixture holds nodes 1 and 4, not " + node);
        return existing(ROOT.resolve("nodes").resolve(DseVersion.DSE_6_9_26.dir + "-2dc-mixed").resolve("node" + node));
    }

    /**
     * Copies {@code local} and {@code peers} of a fixture directory (such as {@link #nodesDirectoryTwoDataCenters}) into
     * {@code <metadataDir>/nodes/} as a DSE node leaves them, so that a reader can be pointed at {@code metadataDir}.
     */
    public static Path installNodesDirectory(Path metadataDir, Path fixture) throws IOException
    {
        Path nodes = Files.createDirectories(metadataDir.resolve("nodes"));
        Files.copy(existing(fixture.resolve("local")), nodes.resolve("local"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(existing(fixture.resolve("peers")), nodes.resolve("peers"), StandardCopyOption.REPLACE_EXISTING);
        return nodes;
    }

    /** The directory holding the commit log segment of a version. */
    public static Path commitLogDirectory(DseVersion version)
    {
        return existing(ROOT.resolve("commitlog").resolve(version.dir));
    }

    /** The gzipped commit log segment (version 680) written by a node of the given version. */
    public static Path commitLogSegmentGz(DseVersion version) throws IOException
    {
        return single(commitLogDirectory(version), "CommitLog-680-", ".log.gz");
    }

    /** Opens {@link #commitLogSegmentGz(DseVersion)} for reading the uncompressed segment. */
    public static InputStream openCommitLogSegment(DseVersion version) throws IOException
    {
        InputStream raw = Files.newInputStream(commitLogSegmentGz(version));
        try
        {
            return new GZIPInputStream(raw);
        }
        catch (IOException | RuntimeException e)
        {
            raw.close();
            throw e;
        }
    }

    /** Unpacks {@link #commitLogSegmentGz(DseVersion)} into {@code targetDir} under its original name. */
    public static Path unpackCommitLogSegment(DseVersion version, Path targetDir) throws IOException
    {
        String name = commitLogSegmentGz(version).getFileName().toString();
        Path target = Files.createDirectories(targetDir).resolve(name.substring(0, name.length() - ".gz".length()));
        try (InputStream in = openCommitLogSegment(version))
        {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    /** The directory holding the hints file of a version. */
    public static Path hintsDirectory(DseVersion version)
    {
        return existing(ROOT.resolve("hints").resolve(version.dir));
    }

    /** The hints file written by a node of the given version for a peer that was down. */
    public static Path hintsFile(DseVersion version) throws IOException
    {
        return single(hintsDirectory(version), "", ".hints");
    }

    /**
     * The hints of {@link #hintsFile(DseVersion)}, each checked against the two CRC32s of its record. Past DSE's
     * header (version, timestamp, host id, parameters length, crc, parameters, crc), the records are framed as this
     * software frames its own: the size of the hint, the CRC32 of the size, the hint, the CRC32 of the size and the hint.
     */
    public static List<byte[]> hintsRecords(DseVersion version) throws IOException
    {
        return hintsRecords(hintsFile(version));
    }

    /**
     * The hints of a DSE hints file, as {@link #hintsRecords(DseVersion)}; a cut header fails, a cut or corrupt record
     * fails with its offset.
     */
    public static List<byte[]> hintsRecords(Path hintsFile) throws IOException
    {
        byte[] bytes = Files.readAllBytes(hintsFile);
        ByteBuffer file = ByteBuffer.wrap(bytes);
        assertTrue("hints header of " + hintsFile.getFileName() + " truncated before its parameters length", file.remaining() >= 4 + 8 + 16 + 4);
        file.position(4 + 8 + 16);
        int parametersLength = file.getInt();
        assertTrue("hints header of " + hintsFile.getFileName() + " truncated in its parameters (length " + parametersLength + ')',
                   parametersLength >= 0 && parametersLength <= file.remaining() - 8);
        file.position(file.position() + 4 + parametersLength + 4);
        List<byte[]> records = new ArrayList<>();
        while (file.hasRemaining())
        {
            int start = file.position();
            String record = "hints record at " + start + " in " + hintsFile.getFileName();
            assertTrue(record + " truncated in its size", file.remaining() >= 8);
            int size = file.getInt();
            CRC32 crc = new CRC32();
            crc.update(bytes, start, Integer.BYTES);
            assertEquals("size crc of the " + record, (int) crc.getValue(), file.getInt());
            assertTrue(record + " truncated in its hint (size " + size + ')', size >= 0 && size <= file.remaining() - 4);
            byte[] hint = new byte[size];
            file.get(hint);
            crc.update(hint);
            assertEquals("crc of the " + record, (int) crc.getValue(), file.getInt());
            records.add(hint);
        }
        return records;
    }

    /**
     * Writes a hints file as DataStax Enterprise 6 writes one, {@code <host id>-<timestamp>-v1.hints} in the directory:
     * DSE's header (version -1, the timestamp, the host id, the parameters {@code {}}, each part followed by the CRC32
     * Cassandra's {@code HintsDescriptor} computes, every long fed low half first), then the hints of the fixture file of
     * a version, framed as there, each with its creation time replaced by the given one (its gc grace seconds and its
     * mutation unchanged). Tests use it to send the fixture hints to a host of their own, and to keep them live: the
     * hints of the fixture files expire ten days after DSE wrote them.
     */
    public static Path writeDseHintsFile(DseVersion version, Path directory, UUID hostId, long timestamp, long creationTime) throws IOException
    {
        return writeDseHintsFile(version, directory, hostId, timestamp, creationTime, -1, "{}");
    }

    /**
     * As {@link #writeDseHintsFile(DseVersion, Path, UUID, long, long)}, with the given descriptor version and parameters
     * (JSON) in the header instead of DSE's -1 and {@code {}}: a file named as DSE names its files that DSE did not write.
     */
    public static Path writeDseHintsFile(DseVersion version, Path directory, UUID hostId, long timestamp, long creationTime,
                                         int descriptorVersion, String parametersJson) throws IOException
    {
        List<byte[]> hints = new ArrayList<>();
        for (byte[] record : hintsRecords(version))
        {
            byte[] hint = record.clone();
            ByteBuffer.wrap(hint).putLong(0, creationTime);
            hints.add(hint);
        }
        return writeDseHintsFile(directory, hostId, timestamp, descriptorVersion, parametersJson, hints);
    }

    /**
     * As {@link #writeDseHintsFile(DseVersion, Path, UUID, long, long)}, with the given hints (see {@link #dseHint}) instead
     * of those of a fixture file: a hints file as DSE writes one, holding hints a test built.
     */
    public static Path writeDseHintsFile(Path directory, UUID hostId, long timestamp, List<byte[]> hints) throws IOException
    {
        return writeDseHintsFile(directory, hostId, timestamp, -1, "{}", hints);
    }

    private static Path writeDseHintsFile(Path directory, UUID hostId, long timestamp, int descriptorVersion, String parametersJson,
                                          List<byte[]> hints) throws IOException
    {
        byte[] parameters = parametersJson.getBytes(StandardCharsets.UTF_8);
        int size = HINTS_HEADER_SIZE + parameters.length;
        for (byte[] hint : hints)
            size += 3 * Integer.BYTES + hint.length;
        ByteBuffer out = ByteBuffer.allocate(size);
        CRC32 crc = new CRC32();
        out.putInt(descriptorVersion);
        updateChecksumInt(crc, descriptorVersion);
        out.putLong(timestamp);
        updateChecksumLong(crc, timestamp);
        out.putLong(hostId.getMostSignificantBits());
        updateChecksumLong(crc, hostId.getMostSignificantBits());
        out.putLong(hostId.getLeastSignificantBits());
        updateChecksumLong(crc, hostId.getLeastSignificantBits());
        out.putInt(parameters.length);
        updateChecksumInt(crc, parameters.length);
        out.putInt((int) crc.getValue());
        out.put(parameters);
        crc.update(parameters);
        out.putInt((int) crc.getValue());

        for (byte[] hint : hints)
        {
            crc.reset();
            updateChecksumInt(crc, hint.length);
            out.putInt(hint.length);
            out.putInt((int) crc.getValue());
            out.put(hint);
            crc.update(hint);
            out.putInt((int) crc.getValue());
        }
        Path file = Files.createDirectories(directory).resolve(hostId + "-" + timestamp + "-v1.hints");
        Files.write(file, Arrays.copyOf(out.array(), out.position()));
        return file;
    }

    /**
     * A hint as DSE serializes it in its hints files: its creation time (8 bytes), its gc grace seconds (unsigned vint),
     * then its mutation in DSE's layout ({@link #dse68Mutation}, or {@link #hintMutation} of a fixture hint).
     */
    public static byte[] dseHint(long creationTime, int gcgs, ByteBuffer mutation)
    {
        ByteBuffer hint = ByteBuffer.allocate(Long.BYTES + VIntCoding.computeUnsignedVIntSize(gcgs) + mutation.remaining());
        hint.putLong(creationTime);
        VIntCoding.writeUnsignedVInt32(gcgs, hint);
        hint.put(mutation.duplicate());
        return hint.array();
    }

    /**
     * {@code mutation}, of one partition update, in the layout DSE 6.8 and 6.9 store it with in their batchlog, hints files
     * and commit log segments, the one this software decodes at {@link MessagingService#VERSION_DSE_68}: the layout of
     * {@link MessagingService#VERSION_40} with DSE's maximum timestamp of the update (8 bytes) after its table id, as in
     * the hints of the fixtures. A mutation of a table whose metadata has a DSE hidden column among its regular columns
     * is written with its cells, as DSE writes the update of a materialized view.
     */
    public static ByteBuffer dse68Mutation(Mutation mutation) throws IOException
    {
        assertEquals("partition updates of the mutation", 1, mutation.getPartitionUpdates().size());
        long maxTimestamp = mutation.getPartitionUpdates().iterator().next().maxTimestamp();
        byte[] bytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(mutation, out, MessagingService.VERSION_40);
            bytes = out.toByteArray();
        }
        assertEquals("partition updates of the mutation", 1, bytes[0]);
        // the number of partition updates (1 byte) and the table id (16 bytes), then the maximum timestamp
        ByteBuffer dse68 = ByteBuffer.allocate(bytes.length + Long.BYTES);
        dse68.put(bytes, 0, 1 + 16).putLong(maxTimestamp).put(bytes, 1 + 16, bytes.length - (1 + 16));
        dse68.flip();
        return dse68;
    }

    /**
     * The update DSE writes into the partition {@code v} of {@code view}, a view of the fixture table
     * ({@link #FIXTURE_KEYSPACE}{@code .t}) keyed by {@code (v, id, ck)}, on the view as DSE knows it, with its hidden
     * column {@code .v}: the live entry {@code (1, 'a')}, and the entry {@code (2, 'b')} DSE holds dead after its base cell
     * changed, a primary key liveness next to a hidden tombstone.
     */
    public static Mutation dseViewUpdate(TableMetadata view, String v)
    {
        TableMetadata dseView = view.unbuild().addRegularColumn(".v", UTF8Type.instance).build();
        ColumnMetadata hidden = dseView.getColumn(ByteBufferUtil.bytes(".v"));
        long timestamp = FBUtilities.timestampMicros();
        long nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate.Builder builder = PartitionUpdate.builder(dseView, dseView.partitioner.decorateKey(ByteBufferUtil.bytes(v)),
                                                                  dseView.regularAndStaticColumns(), 2);
        builder.add(dseViewRow(1, "a", LivenessInfo.create(timestamp, nowInSec), BufferCell.live(hidden, timestamp, ByteBufferUtil.EMPTY_BYTE_BUFFER)));
        builder.add(dseViewRow(2, "b", LivenessInfo.create(timestamp, nowInSec), BufferCell.tombstone(hidden, timestamp, nowInSec)));
        return new Mutation(builder.build());
    }

    private static Row dseViewRow(int id, String ck, LivenessInfo liveness, Cell<?> hidden)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(Int32Type.instance.decompose(id), UTF8Type.instance.decompose(ck)));
        builder.addPrimaryKeyLivenessInfo(liveness);
        builder.addCell(hidden);
        return builder.build();
    }

    // the checksum helpers are deliberately independent of FBUtilities and HintsDescriptor: DseFixturesTest checks the
    // header they write against the bytes DSE wrote, so the fixture writer does not rely on the code under test
    private static void updateChecksumInt(CRC32 crc, int value)
    {
        crc.update((value >>> 24) & 0xFF);
        crc.update((value >>> 16) & 0xFF);
        crc.update((value >>> 8) & 0xFF);
        crc.update(value & 0xFF);
    }

    private static void updateChecksumLong(CRC32 crc, long value)
    {
        updateChecksumInt(crc, (int) (value & 0xFFFFFFFFL));
        updateChecksumInt(crc, (int) (value >>> 32));
    }

    /**
     * The table id of the one partition update of a DSE hint, read ahead of the mutation: a hint is its creation time
     * (8 bytes), its gc grace seconds (unsigned vint), then the mutation, the number of partition updates (unsigned
     * vint, always 1 in the fixtures) followed by the id of the first update's table.
     */
    public static UUID hintTableId(byte[] hint)
    {
        ByteBuffer body = ByteBuffer.wrap(hint);
        body.position(8 + VIntCoding.computeUnsignedVIntSize(VIntCoding.getUnsignedVInt(body, 8)));
        assertEquals("partition updates in a hint", 1, body.get());
        return new UUID(body.getLong(), body.getLong());
    }

    /**
     * The mutation of a DSE hint, as DSE serialized it at its WRITES version (the layout of
     * {@link org.apache.cassandra.net.MessagingService#VERSION_DSE_68}, as in its commit log): the bytes past the
     * creation time (8 bytes) and the gc grace seconds (unsigned vint) of the hint.
     */
    public static ByteBuffer hintMutation(byte[] hint)
    {
        int start = 8 + VIntCoding.computeUnsignedVIntSize(VIntCoding.getUnsignedVInt(ByteBuffer.wrap(hint), 8));
        return ByteBuffer.wrap(Arrays.copyOfRange(hint, start, hint.length));
    }

    /** The mutations of the hints of {@code version} for {@code test_ks_fixture.t}, as {@link #hintMutation} returns them. */
    public static List<ByteBuffer> fixtureHintMutations(DseVersion version) throws IOException
    {
        List<ByteBuffer> mutations = new ArrayList<>();
        for (byte[] hint : hintsRecords(version))
            if (hintTableId(hint).equals(fixtureTableId(version)))
                mutations.add(hintMutation(hint));
        assertFalse("no hint of test_ks_fixture.t in the " + version + " fixture", mutations.isEmpty());
        return mutations;
    }

    /** The keyspace of the collection table in the commit log segments and the hints files. */
    public static final String FIXTURE_KEYSPACE = "test_ks_fixture";

    /** The mutations of one run of the fixture workload on the collection table: 200 inserts and three deletions. */
    public static final int FIXTURE_MUTATIONS_PER_RUN = 203;

    /** {@code system_auth.role_permissions}: DSE hints the permissions it grants on the fixture keyspace and table. */
    public static final UUID ROLE_PERMISSIONS_TABLE_ID = UUID.fromString("3afbe79f-2194-31a7-add7-f5ab90d8ec9c");

    /** DSE's {@code system_distributed.nodesync_status}, a table this software does not have. */
    public static final UUID NODESYNC_STATUS_TABLE_ID = UUID.fromString("c0e1b5f4-c731-3317-a5a2-2587bbef62f8");

    /** DSE's {@code system.nodesync_checkpoints}, a table this software does not have. */
    public static final UUID NODESYNC_CHECKPOINTS_TABLE_ID = UUID.fromString("2459e663-3235-3e9d-b072-add3d46ca545");

    /** The id of {@code test_ks_fixture.t} on the DSE node that wrote the commit log segment and the hints file. */
    public static UUID fixtureTableId(DseVersion version)
    {
        switch (version)
        {
            case DSE_6_8_65:
                return UUID.fromString("269b3740-b8d4-11f1-acb3-9d12a1a4d70a");
            case DSE_6_9_26:
                return UUID.fromString("bd1016f0-bb1d-11f1-afdb-fb0517c9d423");
            default:
                throw new AssertionError("no fixture table id recorded for " + version);
        }
    }

    /**
     * The CQL creating {@code test_ks_fixture.t} as on the DSE node of a version, with its id there (replication
     * factor 1 for a single test node, no SAI index): a replay or a decoding of its mutations needs that id.
     */
    public static List<String> fixtureSchema(DseVersion version)
    {
        return Arrays.asList("CREATE KEYSPACE " + FIXTURE_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                             "CREATE TYPE " + FIXTURE_KEYSPACE + ".addr (street text, zip int)",
                             "CREATE TABLE " + FIXTURE_KEYSPACE + ".t (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, " +
                             "n bigint, s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck)) WITH ID = " +
                             fixtureTableId(version));
    }

    /** One application state of a DSE node as {@code nodetool gossipinfo} prints it: {@code NAME:version:value}. */
    public static final class GossipState
    {
        public final String name;
        public final int version;
        public final String value;

        GossipState(String name, int version, String value)
        {
            this.name = name;
            this.version = version;
            this.value = value;
        }
    }

    /**
     * The application states of the first node of the given version, in the order {@code nodetool gossipinfo}
     * printed them ({@code gossip/<version>/gossipinfo.txt}); {@code TOKENS} is {@code <hidden>} in that output.
     * Only 6.9.26 so far.
     */
    public static List<GossipState> gossipStates(DseVersion version) throws IOException
    {
        Path file = existing(ROOT.resolve("gossip").resolve(version.dir).resolve("gossipinfo.txt"));
        List<GossipState> states = new ArrayList<>();
        for (String line : Files.readAllLines(file))
        {
            if (!line.startsWith("  "))
                continue; // the endpoint line
            String[] parts = line.trim().split(":", 3);
            if (parts.length < 3)
                continue; // generation and heartbeat
            states.add(new GossipState(parts[0], Integer.parseInt(parts[1]), parts[2]));
        }
        assertFalse("no application state in " + file, states.isEmpty());
        return states;
    }

    /** The id of {@code lab.lwt} ({@code id int PRIMARY KEY, v int}) on the DSE 6.8.65 test cluster of {@link #paxosRows()}. */
    public static final UUID PAXOS_LWT_TABLE_ID = UUID.fromString("a77c0571-b80b-11f1-acb3-9d12a1a4d70a");

    /** The id of {@code bbtest.t} (the collection table) on the DSE 6.8.65 test cluster of {@link #paxosRows()}. */
    public static final UUID PAXOS_COLLECTION_TABLE_ID = UUID.fromString("9f201230-b811-11f1-acb3-9d12a1a4d70a");

    /** The messaging version DSE 6.8.65 stores in {@code proposal_version} and {@code most_recent_commit_version}. */
    public static final int PAXOS_DSE_6_8_65_VERSION = 1024;

    /** A messaging version of 256 or more that is not one of DSE 6, for the tests of a stored version this software refuses. */
    public static final int UNKNOWN_DSE_VERSION = 2304;

    /** {@link #UNKNOWN_DSE_VERSION} in {@code system.paxos}, for the tests of the refusal and of its remedy. */
    public static final int PAXOS_UNKNOWN_DSE_VERSION = UNKNOWN_DSE_VERSION;

    /**
     * The TTL DSE wrote the rows of {@link #paxosRows()} and of {@link #dse6926PaxosSSTableDirectory()} with: the
     * gc_grace_seconds of their tables on the two test clusters.
     */
    public static final int PAXOS_DSE_TTL_SECONDS = 864000;

    /**
     * The id of {@code lab.lwt} ({@code id int PRIMARY KEY, v int}) on the DSE 6.9.26 test cluster of
     * {@link #dse6926PaxosSSTableDirectory()}, another cluster than the one of {@link #paxosRows()}: not
     * {@link #PAXOS_LWT_TABLE_ID}.
     */
    public static final UUID PAXOS_DSE_6_9_26_LWT_TABLE_ID = UUID.fromString("1fab9c81-bb0f-11f1-91d0-b3b5fc586fca");

    /** The messaging version DSE 6.9.26 stores in {@code proposal_version} and {@code most_recent_commit_version}. */
    public static final int PAXOS_DSE_6_9_26_VERSION = 1536;

    /**
     * The rows of the sstable of {@link #dse6926PaxosSSTableDirectory()}: 100 left by the test data set, 3 by later
     * lightweight transactions through the same node (ids 9001 to 9003).
     */
    public static final int PAXOS_DSE_6_9_26_ROWS = 103;

    /** The earliest local deletion time of the cells of {@link #dse6926PaxosSSTableDirectory()} (its Statistics.db): 2026-10-08T07:35:22Z. */
    public static final long PAXOS_DSE_6_9_26_FIRST_EXPIRY = 1791444922L;

    /** The latest local deletion time of the cells of {@link #dse6926PaxosSSTableDirectory()} (its Statistics.db): 2026-10-13T11:32:03Z. */
    public static final long PAXOS_DSE_6_9_26_LAST_EXPIRY = 1791891123L;

    /**
     * Directory holding the {@code system.paxos} sstable DSE 6.9.26 left on the first node of its test cluster (README,
     * {@code paxos/dse-6.9.26/}), as DSE wrote it: trie {@code bb}, generation 4, {@link #PAXOS_DSE_6_9_26_ROWS}
     * committed rows of {@link #PAXOS_DSE_6_9_26_LWT_TABLE_ID} tagged {@link #PAXOS_DSE_6_9_26_VERSION}, every cell with
     * the TTL {@link #PAXOS_DSE_TTL_SECONDS} from the second of its ballot (or the next), so that the cells expire between
     * {@link #PAXOS_DSE_6_9_26_FIRST_EXPIRY} and {@link #PAXOS_DSE_6_9_26_LAST_EXPIRY}.
     */
    public static Path dse6926PaxosSSTableDirectory()
    {
        return existing(ROOT.resolve("paxos").resolve(DseVersion.DSE_6_9_26.dir));
    }

    /** Every component file of {@link #dse6926PaxosSSTableDirectory()}. */
    public static List<Path> dse6926PaxosSSTableComponents() throws IOException
    {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(dse6926PaxosSSTableDirectory()))
        {
            s.filter(p -> p.getFileName().toString().startsWith("bb-4-bti-")).sorted().forEach(files::add);
        }
        assertEquals("components under " + dse6926PaxosSSTableDirectory() + ": " + files, 8, files.size());
        return files;
    }

    /**
     * The CQL creating the two tables of {@link #paxosRows()} with their ids on the DSE 6.8.65 test cluster, keyspaces
     * {@code lab} and {@code bbtest} with {@code SimpleStrategy} and the given replication factor.
     */
    public static List<String> paxosSchema(int replicationFactor)
    {
        String replication = " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + '}';
        return Arrays.asList("CREATE KEYSPACE lab" + replication,
                             "CREATE TABLE lab.lwt (id int PRIMARY KEY, v int) WITH ID = " + PAXOS_LWT_TABLE_ID,
                             "CREATE KEYSPACE bbtest" + replication,
                             "CREATE TYPE bbtest.addr (street text, zip int)",
                             "CREATE TABLE bbtest.t (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, " +
                             "n bigint, s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck)) WITH ID = " +
                             PAXOS_COLLECTION_TABLE_ID);
    }

    /** A CQL statement and its bound values. */
    public static final class Statement
    {
        public final String cql;
        public final Object[] values;

        private Statement(String cql, Object... values)
        {
            this.cql = cql;
            this.values = values;
        }
    }

    /** A row of DSE 6.8.65's {@code system.paxos}; a null field is a column DSE left null. */
    public static final class PaxosRow
    {
        public final ByteBuffer rowKey;
        public final UUID cfId;
        public final UUID inProgressBallot;
        public final UUID proposalBallot;
        public final ByteBuffer proposal;
        public final Integer proposalVersion;
        public final UUID mostRecentCommitAt;
        public final ByteBuffer mostRecentCommit;
        public final Integer mostRecentCommitVersion;

        private PaxosRow(String[] f)
        {
            rowKey = ByteBuffer.wrap(hex(f[0]));
            cfId = UUID.fromString(f[1]);
            inProgressBallot = uuidOrNull(f[2]);
            proposalBallot = uuidOrNull(f[3]);
            proposal = blobOrNull(f[4]);
            proposalVersion = "null".equals(f[5]) ? null : Integer.valueOf(f[5]);
            mostRecentCommitAt = uuidOrNull(f[6]);
            mostRecentCommit = blobOrNull(f[7]);
            mostRecentCommitVersion = "null".equals(f[8]) ? null : Integer.valueOf(f[8]);
        }

        private PaxosRow(PaxosRow row, UUID inProgressBallot, UUID proposalBallot, ByteBuffer proposal, UUID mostRecentCommitAt,
                         ByteBuffer mostRecentCommit)
        {
            rowKey = row.rowKey;
            cfId = row.cfId;
            this.inProgressBallot = inProgressBallot;
            this.proposalBallot = proposalBallot;
            this.proposal = proposal;
            proposalVersion = row.proposalVersion;
            this.mostRecentCommitAt = mostRecentCommitAt;
            this.mostRecentCommit = mostRecentCommit;
            mostRecentCommitVersion = row.mostRecentCommitVersion;
        }

        private static UUID uuidOrNull(String s)
        {
            return "null".equals(s) ? null : UUID.fromString(s);
        }

        private static ByteBuffer blobOrNull(String s)
        {
            return "null".equals(s) ? null : ByteBuffer.wrap(hex(s));
        }

        /**
         * This row with its updates as DSE 6.0 serializes them (WRITES version OSS_30): without the maximum timestamp
         * that follows the table id from DSE 6.8 on.
         */
        public PaxosRow withoutMaxTimestamp()
        {
            return new PaxosRow(this, inProgressBallot, proposalBallot, withoutMaxTimestamp(proposal), mostRecentCommitAt,
                                withoutMaxTimestamp(mostRecentCommit));
        }

        private static ByteBuffer withoutMaxTimestamp(ByteBuffer update)
        {
            if (update == null)
                return null;
            byte[] bytes = new byte[update.remaining()];
            update.duplicate().get(bytes);
            assertTrue("an update of " + bytes.length + " bytes, shorter than its table id and maximum timestamp", bytes.length >= 24);
            byte[] stripped = new byte[bytes.length - 8];
            System.arraycopy(bytes, 0, stripped, 0, 16);
            System.arraycopy(bytes, 24, stripped, 16, bytes.length - 24);
            return ByteBuffer.wrap(stripped);
        }

        /**
         * This row with its ballots moved by {@code micros}, keeping their flag and their unique part: the update bytes do
         * not depend on the ballots, and a test that runs the Paxos of this software over the row needs ballots within the
         * gc_grace_seconds of the table, since older state is purged.
         */
        public PaxosRow withBallotsShiftedBy(long micros)
        {
            return new PaxosRow(this, shift(inProgressBallot, micros), shift(proposalBallot, micros), proposal,
                                shift(mostRecentCommitAt, micros), mostRecentCommit);
        }

        private static UUID shift(UUID ballot, long micros)
        {
            if (ballot == null)
                return null;
            Ballot b = Ballot.fromUuid(ballot);
            return Ballot.atUnixMicrosWithLsb(b.unixMicros() + micros, b.lsb(), b.flag()).asUUID();
        }

        /**
         * For a committed conditional batch of {@code bbtest.t}, the value it wrote in {@code v} of the rows {@code u1} and
         * {@code u3}: {@code cas-<coordinator>}.
         */
        public String casBatchValue()
        {
            assertEquals(PAXOS_COLLECTION_TABLE_ID, cfId);
            byte[] bytes = new byte[mostRecentCommit.remaining()];
            mostRecentCommit.duplicate().get(bytes);
            Matcher m = Pattern.compile("cas-dse-node-[123]\\.lab").matcher(new String(bytes, StandardCharsets.ISO_8859_1));
            assertTrue("no conditional batch in the commit of " + Hex.bytesToHex(bytes), m.find());
            return m.group();
        }

        /** The partition key of the row in the user table, an {@code int} in both tables. */
        public int key()
        {
            return rowKey.getInt(rowKey.position());
        }

        /**
         * The statements writing this row to {@code system.paxos} as DSE wrote it, its versions replaced by
         * {@code storedVersion}: each column with the timestamp of its ballot and the TTL {@link #PAXOS_DSE_TTL_SECONDS}.
         * DSE nulls the proposal at commit but leaves its version, written with the commit.
         */
        public List<Statement> saveStatements(int storedVersion)
        {
            String update = "UPDATE system.paxos USING TIMESTAMP ? AND TTL ? SET ";
            String where = " WHERE row_key = ? AND cf_id = ?";
            long commitMicros = Ballot.fromUuid(mostRecentCommitAt).unixMicros();
            List<Statement> statements = new ArrayList<>();
            statements.add(new Statement(update + "most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ?" + where,
                                         commitMicros, PAXOS_DSE_TTL_SECONDS, mostRecentCommitAt, mostRecentCommit, storedVersion, rowKey, cfId));
            if (inProgressBallot != null)
                statements.add(new Statement(update + "in_progress_ballot = ?" + where,
                                             Ballot.fromUuid(inProgressBallot).unixMicros(), PAXOS_DSE_TTL_SECONDS, inProgressBallot, rowKey, cfId));
            if (proposalBallot != null)
                statements.add(new Statement(update + "proposal_ballot = ?, proposal = ?, proposal_version = ?" + where,
                                             Ballot.fromUuid(proposalBallot).unixMicros(), PAXOS_DSE_TTL_SECONDS, proposalBallot, proposal,
                                             storedVersion, rowKey, cfId));
            else if (proposalVersion != null)
                statements.add(new Statement(update + "proposal_version = ?" + where,
                                             commitMicros, PAXOS_DSE_TTL_SECONDS, storedVersion, rowKey, cfId));
            return statements;
        }

        /**
         * The statement writing this row's committed update to {@code system.paxos} as a proposal DSE accepted but did not
         * commit, at its commit ballot, with no commit: DSE nulls the proposal at commit, so the fixture has none.
         */
        public Statement saveCommitAsAcceptedProposal(int storedVersion)
        {
            return new Statement("UPDATE system.paxos USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ?, proposal_ballot = ?, " +
                                 "proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?",
                                 Ballot.fromUuid(mostRecentCommitAt).unixMicros(), PAXOS_DSE_TTL_SECONDS, mostRecentCommitAt,
                                 mostRecentCommitAt, mostRecentCommit, storedVersion, rowKey, cfId);
        }
    }

    /**
     * The remedy for a row of {@code system.paxos} this software cannot decode: delete that one row (never a
     * range of {@code cf_id}) at a timestamp above every cell of it, so that the deletion covers the row whatever the
     * clocks and shadows nothing the next rounds write.
     */
    public static Statement paxosRowDeletion(PaxosRow row, long... cellWritetimes)
    {
        long timestamp = Long.MIN_VALUE;
        for (long writetime : cellWritetimes)
            timestamp = Math.max(timestamp, writetime);
        assertTrue("no live cell in the row to delete", timestamp > Long.MIN_VALUE);
        return new Statement("DELETE FROM system.paxos USING TIMESTAMP ? WHERE row_key = ? AND cf_id = ?", timestamp + 1, row.rowKey, row.cfId);
    }

    /**
     * The rows tagged with {@link #PAXOS_DSE_6_8_65_VERSION} in {@code system.paxos} of the first node of the DSE 6.8.65
     * test cluster after its upgrade to this software, as {@code cqlsh} printed them (README, {@code paxos/}): 100 of
     * {@code lab.lwt} and 18 of {@code bbtest.t}.
     */
    public static List<PaxosRow> paxosRows() throws IOException
    {
        Path file = existing(ROOT.resolve("paxos").resolve(DseVersion.DSE_6_8_65.dir).resolve("system-paxos.tsv"));
        List<String> lines = Files.readAllLines(file);
        assertFalse("empty " + file, lines.isEmpty());
        assertEquals("row_key\tcf_id\tin_progress_ballot\tproposal_ballot\tproposal\tproposal_version\t" +
                     "most_recent_commit_at\tmost_recent_commit\tmost_recent_commit_version", lines.get(0));
        List<PaxosRow> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size()))
        {
            String[] f = line.split("\t");
            assertEquals("expected 9 columns in " + line, 9, f.length);
            rows.add(new PaxosRow(f));
        }
        return rows;
    }

    /**
     * The {@code system.paxos} sstables of the third node of the DSE 6.8.65 test cluster of {@link #paxosRows()} during
     * a rollback to DSE, as this software's {@code sstabledump -d} printed them, rows of {@code m5.px} only (README,
     * {@code paxos/dse-6.8.65-m5/}).
     */
    public static Path paxosRollbackDump()
    {
        return existing(ROOT.resolve("paxos").resolve("dse-6.8.65-m5").resolve("node3-paxos-sstables-during-hold.txt"));
    }

    /**
     * {@link #paxosRows()} with their ballots moved together so that the latest is an hour old: the state DSE would have
     * left on a node upgraded an hour after the last of those lightweight transactions.
     */
    public static List<PaxosRow> paxosRowsOfAnHourAgo() throws IOException
    {
        List<PaxosRow> rows = paxosRows();
        long latest = Long.MIN_VALUE;
        for (PaxosRow row : rows)
            for (UUID ballot : Arrays.asList(row.inProgressBallot, row.proposalBallot, row.mostRecentCommitAt))
                if (ballot != null)
                    latest = Math.max(latest, Ballot.fromUuid(ballot).unixMicros());
        long shift = FBUtilities.timestampMicros() - TimeUnit.HOURS.toMicros(1) - latest;
        List<PaxosRow> shifted = new ArrayList<>(rows.size());
        for (PaxosRow row : rows)
            shifted.add(row.withBallotsShiftedBy(shift));
        return shifted;
    }

    private static Path single(Path dir, String prefix, String suffix) throws IOException
    {
        existing(dir);
        try (Stream<Path> s = Files.list(dir))
        {
            List<Path> matches = new ArrayList<>();
            s.filter(p -> p.getFileName().toString().startsWith(prefix) && p.getFileName().toString().endsWith(suffix))
             .forEach(matches::add);
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
