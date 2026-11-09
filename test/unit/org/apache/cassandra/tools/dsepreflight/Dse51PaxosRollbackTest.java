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

package org.apache.cassandra.tools.dsepreflight;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Paxos rollback mode of {@code dsepreflight} ({@code --paxos-rollback}) on a node to roll back to DSE 5.1: node 1
 * of the DSE 5.1.51 cluster of the fixtures ({@link Dse51NodeFixture}), with a {@code pre-hcd-<timestamp>} snapshot of
 * its {@code system.local}, {@code system.peers}, {@code system_schema} and {@code system.paxos}, the last the sstable
 * DSE 5.1.51 wrote ({@link Dse51Fixtures#paxosSSTableDirectory()}). That sstable holds two committed rounds only, so
 * the open rounds are written by this JVM, a client in the DSE 5.1 compatibility mode, as big {@code me} sstables: in
 * the snapshot, proposals DSE 5.1 left at messaging version 11 (one with the update of DSE 5.1.51's captured commit,
 * {@link Dse51Fixtures#paxosSelect()}) and at 10, and in the data directory a proposal of HCD 2.0 at
 * {@link MessagingService#VERSION_40}, the version it stores in {@code DSE_5_1} mode. The table is
 * {@code test_ks.lwt} of the captured schema.
 */
public class Dse51PaxosRollbackTest
{
    static final String TAG = "pre-hcd-20261107T100000Z";
    private static final TableId PAXOS_ID = TableId.fromString("b7b7f0c2-fd0a-3410-8c05-3ef614bb7c2d");
    /** The offset between the epochs of time UUIDs (1582-10-15) and of Unix, in 100 ns units. */
    private static final long UUID_EPOCH_OFFSET = 0x01b21dd213814000L;
    private static final int TTL = (int) TimeUnit.DAYS.toSeconds(10);
    private static final UUID LWT = Dse51Fixtures.LWT_TABLE_ID;
    private static final ObjectReader REPORT_READER = JsonUtils.JSON_OBJECT_MAPPER.readerFor(PaxosRollbackReport.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private int generation = 800000;
    private final long base = timeUuidTimestamp(FBUtilities.timestampMicros() - TimeUnit.HOURS.toMicros(1));

    @BeforeClass
    public static void initialise()
    {
        // the sstables written here are big me, as a client in the DSE 5.1 mode writes them
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(INTERNODE_COMPATIBILITY_MODE, "DSE_5_1"))
        {
            DatabaseDescriptor.clientInitialization();
        }
        assertThat(BigFormat.getInstance().getLatestVersion().version)
        .isEqualTo(InternodeCompatibilityMode.DSE_51_BIG_VERSION);
    }

    @Before
    public void scratchInAFolderOfTheTest() throws IOException
    {
        DseTableReader.scratchParent = temporaryFolder.newFolder("scratch").toPath();
    }

    @After
    public void scratchInTheTemporaryDirectory()
    {
        DseTableReader.scratchParent = null;
    }

    /** One run of the tool. */
    private static final class Run
    {
        final int exitCode;
        final String out;
        final String err;
        final PaxosRollbackReport report;

        Run(int exitCode, String out, String err, PaxosRollbackReport report)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
            this.report = report;
        }

        PaxosRollbackReport.OpenRow open(ByteBuffer rowKey)
        {
            String hex = ByteBufferUtil.bytesToHex(rowKey);
            PaxosRollbackReport.Partition partition =
                report.partitions.stream()
                                 .filter(p -> p.cfId.equals(LWT.toString()) && p.rowKey.equals(hex))
                                 .findFirst()
                                 .orElseThrow(() -> new AssertionError("no partition 0x" + hex + " in\n" + out));
            assertThat(partition.openRows).hasSize(1);
            return partition.openRows.get(0);
        }
    }

    private Run run(Dse51NodeFixture node, String... extra) throws IOException
    {
        return run(node.args("--paxos-rollback"), extra);
    }

    private Run run(String[] nodeArgs, String... extra) throws IOException
    {
        Path json = temporaryFolder.getRoot().toPath().resolve("paxos-" + UUID.randomUUID() + ".json");
        List<String> args = new ArrayList<>(Arrays.asList(nodeArgs));
        args.add("--json");
        args.add(json.toString());
        args.addAll(Arrays.asList(extra));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
             PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(args.toArray(new String[0]), outStream, errStream);
        }
        String output = out.toString(StandardCharsets.UTF_8);
        String error = err.toString(StandardCharsets.UTF_8);
        DsePreflightTest.assertNoPlanDecisionIds(output, error);
        if (!Files.exists(json))
            return new Run(exitCode, output, error, null);
        byte[] report = Files.readAllBytes(json);
        DsePreflightTest.assertNoPlanDecisionIds(new String(report, StandardCharsets.UTF_8));
        return new Run(exitCode, output, error, REPORT_READER.readValue(report));
    }

    // ---- the node ----

    /**
     * Node 1 with the snapshot of its system tables, of system_schema and of DSE 5.1.51's system.paxos sstable, and a
     * data directory of system.paxos without sstables.
     */
    private Dse51NodeFixture node() throws IOException
    {
        Dse51NodeFixture node = Dse51NodeFixture.create(temporaryFolder.newFolder().toPath().toRealPath());
        Path paxos = node.data.resolve("system").resolve(DseFixtures.dse51().paxosSSTableDirectory().getFileName());
        Files.createDirectories(paxos);
        Path snapshot = Files.createDirectories(paxosSnapshot(node));
        try (Stream<Path> files = Files.list(DseFixtures.dse51().paxosSSTableDirectory()))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, snapshot.resolve(file.getFileName()));
        }
        for (String keyspace : List.of("system", "system_schema"))
            snapshotKeyspace(node.data.resolve(keyspace), TAG);
        return node;
    }

    /** Copies the sstables of the tables of a keyspace directory into their snapshot {@code tag}. */
    static void snapshotKeyspace(Path keyspace, String tag) throws IOException
    {
        try (Stream<Path> tables = Files.list(keyspace))
        {
            for (Path table : (Iterable<Path>) tables::iterator)
            {
                Path snapshot = Files.createDirectories(table.resolve("snapshots").resolve(tag));
                try (Stream<Path> files = Files.list(table))
                {
                    for (Path file : (Iterable<Path>) files::iterator)
                        if (Files.isRegularFile(file))
                            Files.copy(file, snapshot.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static Path paxosDirectory(Dse51NodeFixture node) throws IOException
    {
        return node.data.resolve("system").resolve(DseFixtures.dse51().paxosSSTableDirectory().getFileName());
    }

    private static Path paxosSnapshot(Dse51NodeFixture node) throws IOException
    {
        return paxosDirectory(node).resolve("snapshots").resolve(TAG);
    }

    // ---- system.paxos rows ----

    /** system.paxos as HCD 2.0 has it ({@code in_progress_read_ballot} included) or as DSE 5.1 has it. */
    private static TableMetadata paxosMetadata(boolean hcd)
    {
        TableMetadata.Builder builder = TableMetadata.builder("system", "paxos", PAXOS_ID)
                                                     .partitioner(Murmur3Partitioner.instance)
                                                     .addPartitionKeyColumn("row_key", BytesType.instance)
                                                     .addClusteringColumn("cf_id", UUIDType.instance)
                                                     .addRegularColumn("in_progress_ballot", TimeUUIDType.instance);
        if (hcd)
            builder.addRegularColumn("in_progress_read_ballot", TimeUUIDType.instance);
        return builder.addRegularColumn("most_recent_commit", BytesType.instance)
                      .addRegularColumn("most_recent_commit_at", TimeUUIDType.instance)
                      .addRegularColumn("most_recent_commit_version", Int32Type.instance)
                      .addRegularColumn("proposal", BytesType.instance)
                      .addRegularColumn("proposal_ballot", TimeUUIDType.instance)
                      .addRegularColumn("proposal_version", Int32Type.instance)
                      .build();
    }

    private static long timeUuidTimestamp(long unixMicros)
    {
        return unixMicros * 10 + UUID_EPOCH_OFFSET;
    }

    /** A version 1 UUID with the 60-bit timestamp given. */
    private static UUID timeUuid(long timestamp)
    {
        long msb = ((timestamp & 0xFFFFFFFFL) << 32) | (((timestamp >>> 32) & 0xFFFFL) << 16) | 0x1000L |
                   ((timestamp >>> 48) & 0x0FFFL);
        UUID uuid = new UUID(msb, 0x9a8b7c6d5e4f3021L);
        assertThat(uuid.timestamp()).isEqualTo(timestamp);
        return uuid;
    }

    /** An accepted proposal of {@code test_ks.lwt}, as Paxos v1 saves it, with the timestamp of its ballot. */
    private static PartitionUpdate accept(TableMetadata paxos, int id, UUID ballot, ByteBuffer update, int version)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(paxos, ByteBufferUtil.bytes(id))
                                                               .timestamp((ballot.timestamp() - UUID_EPOCH_OFFSET) / 10)
                                                               .ttl(TTL);
        Row.SimpleBuilder row = builder.row(ByteBufferUtil.bytes(LWT));
        row.add("proposal_ballot", ByteBufferUtil.bytes(ballot))
           .add("proposal", update)
           .add("proposal_version", version);
        return builder.build();
    }

    /**
     * An update of {@code test_ks.lwt} with no row, serialized at {@link MessagingService#VERSION_40}, whose layout is
     * the 3.0 one DSE 5.1 writes at 10 and 11 ({@link DseCompat#storedDecodingVersion}).
     */
    private static ByteBuffer emptyUpdate(int id)
    {
        TableMetadata table = TableMetadata.builder("test_ks", "lwt", TableId.fromUUID(LWT))
                                           .partitioner(Murmur3Partitioner.instance)
                                           .addPartitionKeyColumn("id", Int32Type.instance)
                                           .addRegularColumn("v", Int32Type.instance)
                                           .build();
        PartitionUpdate update = PartitionUpdate.emptyUpdate(table,
                                                             table.partitioner.decorateKey(ByteBufferUtil.bytes(id)));
        return PartitionUpdate.toBytes(update, MessagingService.VERSION_40);
    }

    /** The update of the first commit of {@code select-paxos.txt}: DSE 5.1.51's bytes at version 11. */
    private static ByteBuffer capturedCommit() throws IOException
    {
        String select = new String(Files.readAllBytes(DseFixtures.dse51().paxosSelect()), StandardCharsets.UTF_8);
        Matcher matcher = Pattern.compile("most_recent_commit +\\| 0x([0-9a-f]+)").matcher(select);
        assertThat(matcher.find()).as("a commit in %s", select).isTrue();
        assertThat(select).contains("most_recent_commit_version       | " + Dse51Fixtures.MESSAGING_VERSION);
        return ByteBufferUtil.hexToBytes(matcher.group(1));
    }

    /** Writes the updates, merged by partition, as one big sstable of system.paxos in {@code directory}. */
    private void writeSSTable(Path directory, TableMetadata paxos, List<PartitionUpdate> updates) throws IOException
    {
        Map<ByteBuffer, List<PartitionUpdate>> byKey = new LinkedHashMap<>();
        for (PartitionUpdate update : updates)
            byKey.computeIfAbsent(update.partitionKey().getKey(), k -> new ArrayList<>()).add(update);
        List<PartitionUpdate> merged = new ArrayList<>();
        byKey.values().forEach(list -> merged.add(PartitionUpdate.merge(list)));
        merged.sort((a, b) -> a.partitionKey().compareTo(b.partitionKey()));
        Descriptor descriptor = new Descriptor(new File(directory), "system", "paxos",
                                               new SequenceBasedSSTableId(generation++), BigFormat.getInstance());
        SerializationHeader header = new SerializationHeader(true, paxos, paxos.regularAndStaticColumns(),
                                                             EncodingStats.NO_STATS);
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(TableMetadataRef.forOfflineTools(paxos), descriptor,
                                                               merged.size(), 0, null, false, header,
                                                               Collections.emptyList(), null))
        {
            for (PartitionUpdate update : merged)
                writer.append(update.unfilteredIterator());
            writer.finish(false, null);
        }
    }

    // ---- the tests ----

    /**
     * The rows DSE 5.1 stored at 11 and 10 are decoded through the stored-version mapping, HCD 2.0's at its own
     * version; the data centers come from system.local and system.peers of the snapshot, and the serial reads are
     * listed as for DSE 6.
     */
    @Test
    public void decodesTheRowsDse51StoredAndReadsTheSnapshotsSystemTables() throws IOException
    {
        Dse51NodeFixture node = node();
        TableMetadata dse = paxosMetadata(false);
        TableMetadata hcd = paxosMetadata(true);
        writeSSTable(paxosSnapshot(node), dse,
                     List.of(accept(dse, 10, timeUuid(base), capturedCommit(), MessagingService.VERSION_3014),
                             accept(dse, 11, timeUuid(base + 10), emptyUpdate(11), MessagingService.VERSION_3014),
                             accept(dse, 12, timeUuid(base + 20), emptyUpdate(12), MessagingService.VERSION_30)));
        writeSSTable(paxosDirectory(node), hcd,
                     List.of(accept(hcd, 13, timeUuid(base + 30), emptyUpdate(13), MessagingService.VERSION_40),
                             accept(hcd, 14, timeUuid(base + 40), emptyUpdate(14), DseCompat.DSE_68_VERSION)));
        Map<String, String> before = node.fingerprint();

        Run run = run(node);
        assertThat(run.exitCode).as("%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(DseTableReader.scratchParent.toFile().list()).as("scratch copies left").isEmpty();
        assertThat(run.report.rollbackTo).isEqualTo(PaxosRollback.DSE_51);
        assertThat(run.report.problems).isEmpty();
        assertThat(run.report.result).isEqualTo(PaxosRollbackReport.READS_LISTED);
        assertThat(run.report.dataCenter).isEqualTo("Cassandra");
        assertThat(run.report.dataCenterSource).isEqualTo("system.local of the snapshot " + TAG);
        assertThat(run.report.singleDataCenter).isTrue();
        assertThat(run.report.schemas).containsExactly("data directories", "snapshot " + TAG);
        // DSE 5.1.51's sstable, whose rounds are all committed, and the two written here
        assertThat(run.report.sstables).extracting(s -> s.format).containsOnly("big me");
        assertThat(run.report.sstables).filteredOn(s -> s.path.endsWith("me-1-big"))
                                       .singleElement()
                                       .satisfies(s -> {
                                           assertThat(s.error).isNull();
                                           assertThat(s.openRows).isZero();
                                       });

        PaxosRollbackReport.OpenRow captured = run.open(ByteBufferUtil.bytes(10));
        assertThat(captured.writtenBy).isEqualTo("DSE 5.1");
        assertThat(captured.version).isEqualTo(MessagingService.VERSION_3014);
        assertThat(captured.emptyProposal).isFalse();
        assertThat(captured.note).isNull();
        PaxosRollbackReport.OpenRow empty = run.open(ByteBufferUtil.bytes(11));
        assertThat(empty.writtenBy).isEqualTo("DSE 5.1");
        assertThat(empty.emptyProposal).isTrue();
        PaxosRollbackReport.OpenRow atTen = run.open(ByteBufferUtil.bytes(12));
        assertThat(atTen.writtenBy).isEqualTo("DSE 5.1");
        assertThat(atTen.version).isEqualTo(MessagingService.VERSION_30);
        assertThat(atTen.emptyProposal).isTrue();
        PaxosRollbackReport.OpenRow ofHcd = run.open(ByteBufferUtil.bytes(13));
        assertThat(ofHcd.writtenBy).isEqualTo("HCD 2.0");
        assertThat(ofHcd.source).isEqualTo("data directory");
        assertThat(ofHcd.emptyProposal).isTrue();
        // a DSE 6 version, which neither release of the cluster writes: listed, not decoded
        PaxosRollbackReport.OpenRow ofDse6 = run.open(ByteBufferUtil.bytes(14));
        assertThat(ofDse6.writtenBy).isEqualTo("unknown");
        assertThat(ofDse6.emptyProposal).isNull();
        assertThat(ofDse6.note).isEqualTo("proposal_version " + DseCompat.DSE_68_VERSION + " is a DSE 6 version, " +
                                          "which neither DSE 5.1 nor HCD 2.0 in DSE_5_1 mode writes: whether the " +
                                          "proposal is empty is unknown");

        assertThat(run.report.partitions).hasSize(5).allSatisfy(p -> {
            assertThat(p.keyspace).isEqualTo("test_ks");
            assertThat(p.table).isEqualTo("lwt");
            assertThat(p.reads).singleElement().satisfies(r -> {
                assertThat(r.consistency).isEqualTo(PaxosRollback.SERIAL);
                assertThat(r.action).isEqualTo(PaxosRollbackReport.Action.READ);
            });
        });
        assertThat(PaxosRollback.cql(run.report, PaxosRollback.SERIAL))
        .endsWith("CONSISTENCY SERIAL;\n" +
                  "SELECT id FROM test_ks.lwt WHERE id = 10 LIMIT 1;\n" +
                  "SELECT id FROM test_ks.lwt WHERE id = 11 LIMIT 1;\n" +
                  "SELECT id FROM test_ks.lwt WHERE id = 12 LIMIT 1;\n" +
                  "SELECT id FROM test_ks.lwt WHERE id = 13 LIMIT 1;\n" +
                  "SELECT id FROM test_ks.lwt WHERE id = 14 LIMIT 1;\n");
        assertThat(run.out).startsWith("DSE 5.1 rollback: open Paxos rounds of this node")
                           .contains("; data center: Cassandra (system.local of the snapshot " + TAG + "); one data " +
                                     "center in system.local and system.peers of the snapshot")
                           .contains(", written by DSE 5.1 (version 11): proposal ")
                           .contains(", written by HCD 2.0 (version 12): empty proposal ");
    }

    /** Without the snapshot's system.peers the data centers are unknown, and the problem says where it looked. */
    @Test
    public void reportsTheSnapshotsSystemTablesItCannotRead() throws IOException
    {
        Dse51NodeFixture node = node();
        TableMetadata dse = paxosMetadata(false);
        writeSSTable(paxosSnapshot(node), dse,
                     List.of(accept(dse, 10, timeUuid(base), capturedCommit(), MessagingService.VERSION_3014)));
        try (Stream<Path> tables = Files.list(node.data.resolve("system")))
        {
            for (Path table : (Iterable<Path>) tables::iterator)
                if (table.getFileName().toString().startsWith("peers-"))
                    deleteTree(table.resolve("snapshots"));
        }

        // the data directory holds no system.paxos sstable: HCD 2.0 wrote none, and system is not restored yet
        Run run = run(node, "--not-restored");
        assertThat(run.exitCode).as("%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.problems).containsExactly("no directory of system.peers of the snapshot " + TAG +
                                                        ": the data centers of the cluster are unknown, so the reads " +
                                                        "of the keyspaces whose replicas may span data centers are " +
                                                        "left to choose: name their consistency with " +
                                                        "--serial-consistency <keyspace>=<level>; do not start DSE " +
                                                        "before every read has succeeded");
        assertThat(run.report.singleDataCenter).isNull();
        // the data center of this node is still known, from the snapshot's system.local
        assertThat(run.report.dataCenter).isEqualTo("Cassandra");
        assertThat(run.out).contains("system.local and system.peers of the snapshot do not tell whether the cluster " +
                                     "has one data center");
        PaxosRollbackReport.Partition partition = run.report.partitions.get(0);
        assertThat(run.report.partitions).hasSize(1);
        assertThat(partition.reads).extracting(r -> r.action).containsExactly(PaxosRollbackReport.Action.CHOOSE);
        assertThat(partition.reads.get(0).reason)
        .startsWith("keyspace test_ks: SimpleStrategy, replication factor 3: SERIAL and LOCAL_SERIAL may be " +
                    "different quorums (system.local and system.peers of the snapshot do not say that the cluster " +
                    "has a single data center, and SimpleStrategy places replicas regardless of data centers, so " +
                    "the replicas in data center Cassandra are unknown)");
    }

    /**
     * The data center given with --dc is checked against the snapshot's system.local; without that table it comes
     * from cassandra-rackdc.properties, with the problem of the table not read; a proposal below 3.0 is listed with
     * its note.
     */
    @Test
    public void takesTheDataCenterFromTheOptionsOrTheRackdcFile() throws IOException
    {
        Dse51NodeFixture node = node();
        TableMetadata dse = paxosMetadata(false);
        int belowThree = MessagingService.VERSION_30 - 1;
        writeSSTable(paxosSnapshot(node), dse, List.of(accept(dse, 10, timeUuid(base), emptyUpdate(10), belowThree)));
        Run other = run(node, "--not-restored", "--dc", "dc9");
        assertThat(other.exitCode).as("%s%s", other.out, other.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(other.report.dataCenter).isEqualTo("dc9");
        assertThat(other.report.problems)
        .contains("--dc dc9 but system.local of the snapshot " + TAG + " records data center Cassandra: check --dc");
        PaxosRollbackReport.OpenRow old = other.open(ByteBufferUtil.bytes(10));
        assertThat(old.writtenBy).isEqualTo("unknown");
        assertThat(old.emptyProposal).isNull();
        assertThat(old.note).isEqualTo("proposal_version " + belowThree + " is below 3.0, neither DSE 5.1 nor HCD " +
                                       "2.0: whether the proposal is empty is unknown");

        try (Stream<Path> tables = Files.list(node.data.resolve("system")))
        {
            for (Path table : (Iterable<Path>) tables::iterator)
                if (table.getFileName().toString().startsWith("local-"))
                    deleteTree(table.resolve("snapshots"));
        }
        Run rackdc = run(node, "--not-restored");
        assertThat(rackdc.exitCode).as("%s%s", rackdc.out, rackdc.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(rackdc.report.dataCenter).isEqualTo("dc1");
        assertThat(rackdc.report.dataCenterSource).isEqualTo("cassandra-rackdc.properties");
        assertThat(rackdc.report.problems)
        .contains("no row in system.local of the snapshot " + TAG + " (it has no directory): the data centers of " +
                  "the cluster are unknown, so the reads of the keyspaces whose replicas may span data centers are " +
                  "left to choose: name their consistency with --serial-consistency <keyspace>=<level>; do not start " +
                  "DSE before every read has succeeded");
        assertThat(rackdc.report.singleDataCenter).isNull();
    }

    /**
     * A keyspace on DSE's EverywhereStrategy: as many replicas as the snapshot's system.local and system.peers list
     * nodes, named as the source of the count.
     */
    @Test
    public void countsTheReplicasOfEverywhereStrategyFromTheSnapshot() throws IOException
    {
        Dse51NodeFixture node = node();
        UUID everywhere = UUID.fromString("ffffffff-d9c8-11f1-9d05-ff904c15f3ab");
        List<String> keyspaceKey = List.of("keyspace_name");
        node.writeRows("system_schema", "keyspaces", keyspaceKey, List.of(),
                       DseNodeFixture.row("everywhere")
                                     .with("durable_writes", true)
                                     .with("replication", Map.of("class",
                                                                 "org.apache.cassandra.locator.EverywhereStrategy")));
        node.writeRows("system_schema", "tables", keyspaceKey, List.of("table_name"),
                       DseNodeFixture.row("everywhere", "t").with("id", everywhere)
                                     .with("flags", Set.of("compound")));
        node.writeRows("system_schema", "columns", keyspaceKey, List.of("table_name", "column_name"),
                       DseNodeFixture.row("everywhere", "t", "id").with("clustering_order", "none")
                                     .with("column_name_bytes", ByteBufferUtil.bytes("id"))
                                     .with("kind", "partition_key")
                                     .with("position", 0)
                                     .with("type", "int"));
        TableMetadata hcd = paxosMetadata(true);
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(hcd, ByteBufferUtil.bytes(7))
                                                               .timestamp(FBUtilities.timestampMicros()).ttl(TTL);
        builder.row(ByteBufferUtil.bytes(everywhere))
               .add("proposal_ballot", ByteBufferUtil.bytes(timeUuid(base)))
               .add("proposal", emptyUpdate(7))
               .add("proposal_version", MessagingService.VERSION_40);
        writeSSTable(paxosDirectory(node), hcd, List.of(builder.build()));

        Run run = run(node);
        assertThat(run.exitCode).as("%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_READY);
        PaxosRollbackReport.Partition partition = run.report.partitions.get(0);
        assertThat(partition.keyspace).isEqualTo("everywhere");
        assertThat(partition.reads).singleElement().satisfies(r -> {
            assertThat(r.action).isEqualTo(PaxosRollbackReport.Action.READ);
            assertThat(r.reason).startsWith("keyspace everywhere: EverywhereStrategy (3 nodes in system.local and " +
                                            "system.peers of the snapshot, 3 in data center Cassandra)");
        });
    }

    /**
     * A node rolled back to DSE 6 keeps the DSE 6 path: its DSE installation is DSE 6's, even when its nodes/local is
     * missing (the rollback puts it back before this mode runs), and its report has no rollback_to.
     */
    @Test
    public void aDse6NodeKeepsTheDse6Path() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath().toRealPath());
        snapshotKeyspace(node.data.resolve("system_schema"), TAG);
        Files.delete(node.metadata.resolve("nodes").resolve("local"));
        Run run = run(node.args("--paxos-rollback"), "--not-restored");
        assertThat(run.report).as("%s%s", run.out, run.err).isNotNull();
        assertThat(run.report.rollbackTo).isNull();
        // the data centers are looked for in the nodes files, never in the snapshot's system tables
        assertThat(run.report.problems).noneMatch(p -> p.contains("of the snapshot " + TAG));
        assertThat(String.valueOf(run.report.dataCenterSource)).doesNotContain("of the snapshot");
        assertThat(run.out).startsWith("DSE 6 rollback: open Paxos rounds of this node");
    }

    private static void deleteTree(Path root) throws IOException
    {
        try (Stream<Path> paths = Files.walk(root))
        {
            for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator)
                Files.delete(path);
        }
    }
}
