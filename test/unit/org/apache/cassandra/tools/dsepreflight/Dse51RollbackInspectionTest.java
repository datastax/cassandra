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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.tools.dsepreflight.DseNodeFixture.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rollback check of {@code dsepreflight} ({@code --rollback-check}, {@link Dse51RollbackInspection}), run through
 * its command line on node 1 of the DSE 5.1.51 cluster of the fixtures ({@link Dse51NodeFixture}) laid out as a node
 * of HCD 2.0 in {@code DSE_5_1} mode leaves it at its drain: DSE 5.1.51's sstables and hints file (for node 3, copied
 * while DSE had it open, so without its {@code .crc32} and statistics companions), a {@code pre-hcd-<timestamp>}
 * snapshot of {@code system} and {@code system_schema}, an sstable this JVM writes as a client in the DSE 5.1
 * compatibility mode (big {@code me}, as HCD 2.0 writes it in the mode), an empty commit log segment of HCD 2.0's
 * version (its header only), a saved cache, and, in the metadata directory, the DSE 5.1 upgrade marker, the record of
 * DSE 5.1 peers and the record of the values of DSE 5.1's columns, which every node of HCD 2.0 in the mode holds. Each
 * refusal is made by one change to that node: the captured DSE 5.1.51 commit log segment (it holds mutations) and
 * transaction log, and, generated here since the capture holds none, sstables of other versions or with a
 * non-numeric identifier, hints files of version 2 or with a damaged descriptor, an incomplete snapshot or several,
 * and directories that are missing or cannot be read. What a real drain leaves in the commit log is checked by
 * {@link Dse51RollbackCommitLogTest}.
 */
public class Dse51RollbackInspectionTest
{
    private static final ObjectReader REPORT_READER =
        JsonUtils.JSON_OBJECT_MAPPER.readerFor(Dse51RollbackInspection.Report.class);
    private static final List<String> KEYSPACES_KEY = List.of("keyspace_name");
    private static final UUID NODE_2 = Dse51Fixtures.HOST_IDS.get(1);
    private static final UUID NODE_3 = Dse51Fixtures.HOST_IDS.get(2);
    private static final String SEGMENT = "CommitLog-7-1793990000000.log";
    /** The address of node 3 in node 1's system.peers. */
    private static String node3Address;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void initialise() throws IOException
    {
        // the sstables written here are big me, as a client in the DSE 5.1 mode writes them
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(INTERNODE_COMPATIBILITY_MODE, "DSE_5_1"))
        {
            DatabaseDescriptor.clientInitialization();
        }
        assertThat(BigFormat.getInstance().getLatestVersion().version)
        .isEqualTo(InternodeCompatibilityMode.DSE_51_BIG_VERSION);
        Path systemTables = DseFixtures.dse51().systemSSTables(true);
        DseTableReader.TableRows peers = DseTableReader.readBig(List.of(new File(systemTables.getParent())),
                                                                systemTables.getFileName().toString(), "peers",
                                                                List.of("peer"), List.of());
        for (UntypedResultSet.Row row : peers.rows)
            if (row.has("host_id") && row.getUUID("host_id").equals(NODE_3))
                node3Address = row.getInetAddress("peer").getHostAddress();
        assertThat(node3Address).as("node 3 in node 1's system.peers").isNotNull();
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

    // ---- runs ----

    /** One run of the check. */
    private static final class Run
    {
        final int exitCode;
        final String out;
        final String err;
        final Dse51RollbackInspection.Report report;

        Run(int exitCode, String out, String err, Dse51RollbackInspection.Report report)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
            this.report = report;
        }

        List<String> codes(Finding.Severity severity)
        {
            return report.findings.stream().filter(f -> f.severity == severity).map(f -> f.code)
                                  .collect(Collectors.toList());
        }

        Finding finding(String code)
        {
            List<Finding> found = report.findings.stream().filter(f -> f.code.equals(code))
                                                 .collect(Collectors.toList());
            assertThat(found).as("%s in\n%s", code, out).hasSize(1);
            return found.get(0);
        }
    }

    private Run run(Dse51NodeFixture node, String... extra) throws IOException
    {
        Path json = temporaryFolder.getRoot().toPath().resolve("rollback-" + UUID.randomUUID() + ".json");
        String[] args = node.rollbackCheckArgs(Stream.concat(Stream.of("--json", json.toString()), Stream.of(extra))
                                                     .toArray(String[]::new));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
             PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(args, outStream, errStream);
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

    private Run assertReady(Dse51NodeFixture node, String... extra) throws IOException
    {
        Run run = run(node, extra);
        assertThat(run.exitCode).as("%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(run.report.result).isEqualTo(NodeReport.READY);
        assertThat(run.codes(Finding.Severity.BLOCKING)).isEmpty();
        return run;
    }

    /** A run that blocks with {@code code} alone. */
    private Run assertBlocked(Dse51NodeFixture node, String code, String... extra) throws IOException
    {
        Run run = run(node, extra);
        assertThat(run.exitCode).as("%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.result).isEqualTo(NodeReport.BLOCKED);
        assertThat(run.codes(Finding.Severity.BLOCKING)).containsExactly(code);
        return run;
    }

    // ---- the node ----

    /** Node 1 laid out as HCD 2.0 in DSE_5_1 mode leaves it at its drain, ready for its rollback. */
    private Dse51NodeFixture node() throws IOException
    {
        Dse51NodeFixture node = Dse51NodeFixture.create(temporaryFolder.newFolder().toPath().toRealPath());
        for (String keyspace : List.of("system", "system_schema"))
            Dse51PaxosRollbackTest.snapshotKeyspace(node.data.resolve(keyspace), Dse51PaxosRollbackTest.TAG);
        // an sstable HCD 2.0 wrote in DSE_5_1 mode, after the snapshot
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("hcd_ks").with("durable_writes", true)
                                    .with("replication", Map.of("class", "org.apache.cassandra.locator.SimpleStrategy",
                                                                "replication_factor", "3")));
        // the commit log a drain leaves: an empty segment of HCD 2.0's version, the captured one of DSE 5.1 replayed
        try (Stream<Path> segments = Files.list(node.commitLog))
        {
            for (Path segment : (Iterable<Path>) segments::iterator)
                Files.delete(segment);
        }
        emptySegment(node.commitLog, CommitLogDescriptor.VERSION_40, 1793990000000L);
        Files.write(Files.createDirectories(savedCaches(node)).resolve("KeyCache-g.db"), new byte[]{ 1, 2, 3 });
        Files.write(node.metadata.resolve(DseUpgradeMarker.DSE_51_FILE_NAME), new byte[0]);
        Files.write(node.metadata.resolve(DsePeersRecord.DSE_51_FILE_NAME), new byte[0]);
        Files.write(node.metadata.resolve(Dse51SystemTables.RECORD_FILE_NAME), new byte[0]);
        return node;
    }

    private static Path savedCaches(Dse51NodeFixture node)
    {
        // the saved_caches_directory of the captured cassandra.yaml, under the node's directory
        return node.root.resolve("var").resolve("saved_caches");
    }

    /** A commit log segment holding its header only, then zeros: no sync section, no mutation. */
    private static void emptySegment(Path directory, int version, long id) throws IOException
    {
        CommitLogDescriptor descriptor = new CommitLogDescriptor(version, id, null, new EncryptionContext());
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        Files.write(directory.resolve(descriptor.fileName()), buffer.array());
    }

    /** A hints file of the version given, its descriptor only, as {@code HintsDescriptor} lays it out. */
    private static void hintsFile(Path file, int version, UUID hostId) throws IOException
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes))
        {
            out.writeInt(version);
            FBUtilities.updateChecksumInt(crc, version);
            long[] values = { 1793990000000L, hostId.getMostSignificantBits(), hostId.getLeastSignificantBits() };
            for (long value : values)
            {
                out.writeLong(value);
                FBUtilities.updateChecksumInt(crc, (int) (value & 0xFFFFFFFFL));
                FBUtilities.updateChecksumInt(crc, (int) (value >>> 32));
            }
            out.writeInt(json.length);
            FBUtilities.updateChecksumInt(crc, json.length);
            out.writeInt((int) crc.getValue());
            out.write(json);
            crc.update(json, 0, json.length);
            out.writeInt((int) crc.getValue());
        }
        Files.write(file, bytes.toByteArray());
    }

    private static Path tableDirectory(Dse51NodeFixture node, String keyspace, String table) throws IOException
    {
        return DseFixtures.dse51().tableDirectory(node.data.resolve(keyspace), table);
    }

    private static void deleteTree(Path root) throws IOException
    {
        try (Stream<Path> paths = Files.walk(root))
        {
            for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator)
                Files.delete(path);
        }
    }

    /** Runs {@code check} with {@code directory} at {@code permissions}; skipped where they do not apply. */
    private static void withPermissions(Path directory, String permissions, IORunnable check) throws IOException
    {
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString(permissions));
        try
        {
            Assume.assumeFalse("the permissions do not apply to this user", Files.isExecutable(directory));
            check.run();
        }
        finally
        {
            Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        }
    }

    private interface IORunnable
    {
        void run() throws IOException;
    }

    // ---- the tests ----

    /**
     * The node as HCD 2.0 in DSE_5_1 mode leaves it at its drain can go back to DSE 5.1: DSE 5.1.51's hints file,
     * without its companions, is accepted and kept; the marker and the record of DSE 5.1 peers are kept by their
     * names; the commit log, the saved caches and the record of DSE 5.1's columns are listed for removal; nothing of
     * the node is written.
     */
    @Test
    public void aNodeWrittenByHcdInDse51ModeIsReady() throws IOException
    {
        Dse51NodeFixture node = node();
        Path hints = node.hints.resolve(DseFixtures.dse51().hintsFile().getFileName());
        try (Stream<Path> files = Files.list(node.hints))
        {
            assertThat(files.map(p -> p.getFileName().toString())).containsExactly(hints.getFileName().toString());
        }
        Map<String, String> before = node.fingerprint();
        Run run = assertReady(node);
        assertThat(node.fingerprint()).as("the node's files after the run").isEqualTo(before);
        assertThat(DseTableReader.scratchParent.toFile().list()).as("scratch copies left").isEmpty();
        assertThat(run.report.kind).isEqualTo(Dse51RollbackInspection.KIND);
        assertThat(run.codes(Finding.Severity.WARNING)).isEmpty();
        assertThat(run.report.inputs).containsEntry("dse_home", node.dseHome.toString());

        String target = NODE_3 + " (" + node3Address + ", release " + Dse51Fixtures.RELEASE_VERSION + ')';
        assertThat(run.report.hintsByTarget).containsOnlyKeys(target);
        assertThat(run.report.hintsByTarget.get(target)).containsExactly(hints.getFileName().toString());
        assertThat(run.finding("HINTS").message)
        .isEqualTo("hints files of version 1, which DSE 5.1.51 reads and delivers, kept by the rollback: " + target +
                   ": 1 file(s)");
        Path marker = node.metadata.resolve(DseUpgradeMarker.DSE_51_FILE_NAME);
        Path record = node.metadata.resolve(Dse51SystemTables.RECORD_FILE_NAME);
        assertThat(run.finding("MIXED_PHASE_MARKER").message)
        .startsWith("the DSE 5.1 upgrade marker " + marker + ", kept by the rollback");
        assertThat(run.finding("DSE51_SYSTEM_COLUMNS").severity).isEqualTo(Finding.Severity.INFO);
        assertThat(run.finding("DSE51_SYSTEM_COLUMNS").message)
        .isEqualTo(record + ", the record of the values of DSE 5.1's columns of system.local and system.peers that " +
                   "HCD 2.0 read at its first start in DSE_5_1 mode: the rollback deletes it, and " +
                   "dse51-system-columns.tmp if there, before DSE 5.1 starts, and keeps dse51-upgrade-in-progress " +
                   "and dse51-peers-seen; with the record and the marker both kept, the next start of HCD 2.0 on " +
                   "this node would load this record, with the values of before the rollback, instead of reading " +
                   "those DSE 5.1 writes after it");
        assertThat(run.finding("SNAPSHOT").message)
        .isEqualTo("the restore of system and system_schema takes the snapshot " + Dse51PaxosRollbackTest.TAG);
        assertThat(run.report.kept).containsExactly("the snapshot " + Dse51PaxosRollbackTest.TAG +
                                                    " of system and system_schema",
                                                    hints.toString(),
                                                    node.metadata.resolve(DsePeersRecord.DSE_51_FILE_NAME).toString(),
                                                    marker.toString());
        assertThat(run.report.toRemove).containsExactly(node.commitLog.resolve(SEGMENT).toString(),
                                                        savedCaches(node).resolve("KeyCache-g.db").toString(),
                                                        record.toString());
        assertThat(run.report.mayRemove).isEmpty();
        assertThat(run.out).startsWith("DSE 5.1 rollback check of this node")
                           .contains("Result: READY (0 blocking, 0 warnings, ")
                           .contains("Files the rollback removes before DSE 5.1 starts:\n  - " +
                                     node.commitLog.resolve(SEGMENT))
                           .contains("Kept by the rollback:\n  - the snapshot " + Dse51PaxosRollbackTest.TAG);
    }

    /** An sstable DSE 5.1.51 cannot start on refuses the rollback, by kind. */
    @Test
    public void refusesTheSSTablesDse51CannotRead() throws IOException
    {
        Dse51NodeFixture node = node();
        Path table = tableDirectory(node, "system_schema", "keyspaces");
        Files.write(table.resolve("nb-900100-big-Data.db"), new byte[0]);
        Files.write(table.resolve("nb-900100-big-TOC.txt"), new byte[0]);
        Run newer = assertBlocked(node, "SSTABLE_FORMAT");
        assertThat(newer.finding("SSTABLE_FORMAT").message)
        .isEqualTo(Dse51NodeInspector.SSTABLE_BIG_NEWER + ": system_schema.keyspaces: 1 (nb-900100-big-Data.db); " +
                   "DSE 5.1.51 refuses to start on them");
        assertThat(newer.finding("SSTABLE_FORMAT").remedy).startsWith("find out what wrote them: HCD 2.0 in DSE_5_1 " +
                                                                      "mode writes only big me sstables");
        Files.delete(table.resolve("nb-900100-big-Data.db"));
        Files.delete(table.resolve("nb-900100-big-TOC.txt"));

        String id = new UUIDBasedSSTableId(TimeUUID.Generator.nextTimeUUID()).toString();
        Files.write(table.resolve("me-" + id + "-big-Data.db"), new byte[0]);
        Files.write(table.resolve("me-" + id + "-big-TOC.txt"), new byte[0]);
        assertThat(assertBlocked(node, "SSTABLE_FORMAT").finding("SSTABLE_FORMAT").message)
        .startsWith(Dse51NodeInspector.SSTABLE_NON_NUMERIC_ID + ": system_schema.keyspaces: 1 (me-" + id);
    }

    /** The captured DSE 5.1.51 transaction log of a compaction, left in a table directory, refuses the rollback. */
    @Test
    public void refusesATransactionLog() throws IOException
    {
        Dse51NodeFixture node = node();
        Path log = DseFixtures.dse51().transactionLog(false);
        Path copy = tableDirectory(node, "system_schema", "tables").resolve(log.getFileName());
        Files.copy(log, copy);
        Run run = assertBlocked(node, "TRANSACTION_LOG");
        assertThat(run.finding("TRANSACTION_LOG").message)
        .isEqualTo("transaction logs left in the data directories, of an operation HCD 2.0 did not finish: " + copy +
                   "; DSE 5.1.51 would complete or undo the operation at its start, from a log it may not read as " +
                   "HCD 2.0 wrote it");
    }

    /**
     * A commit log segment that holds mutations (the captured segment of DSE 5.1.51, as a node killed without a
     * drain leaves it) refuses the rollback; the mutations are counted, never decoded. A segment HCD 2.0 replayed and
     * set aside is only listed for removal.
     */
    @Test
    public void refusesACommitLogSegmentThatHoldsMutations() throws IOException
    {
        Dse51NodeFixture node = node();
        Path replayed = Files.write(node.commitLog.resolve("CommitLog-6-1793958236000.log.replayed"), new byte[]{ 1 });
        assertThat(assertReady(node).report.toRemove).contains(replayed.toString());

        Dse51Fixtures fixtures = DseFixtures.dse51();
        List<Path> segments = fixtures.unpackCommitLogSegments(fixtures.commitLogSegmentGz().getParent(),
                                                               node.commitLog);
        Run run = assertBlocked(node, "COMMITLOG_UNFLUSHED");
        String name = segments.get(0).getFileName().toString();
        assertThat(run.finding("COMMITLOG_UNFLUSHED").message)
        .matches("commit log segments that hold mutations: " + name.replace(".", "\\.") + " \\([1-9][0-9]* " +
                 "mutations?\\); a drain flushes every table and leaves no mutation in the commit log, so this node " +
                 "was not drained after they were written, and they may hold writes not yet in an sstable, which " +
                 "the rollback, removing the commit log, would lose");
        assertThat(run.finding("COMMITLOG_UNFLUSHED").remedy)
        .isEqualTo("start HCD 2.0 again on this node, in DSE_5_1 mode, drain it (nodetool drain exits 0 and logs " +
                   "DRAINED), stop it, and run this check again");
        // every segment is removed by the rollback, the empty one of the drain too
        assertThat(run.report.toRemove).contains(segments.get(0).toString(),
                                                 node.commitLog.resolve(SEGMENT).toString(), replayed.toString());
    }

    /** A segment cut in its header cannot be told empty; a missing commit log directory may not be HCD 2.0's. */
    @Test
    public void refusesACommitLogSegmentItCannotRead() throws IOException
    {
        Dse51NodeFixture node = node();
        Files.write(node.commitLog.resolve("CommitLog-7-1793990000001.log"), new byte[]{ 0, 0, 0, 7 });
        assertThat(assertBlocked(node, "COMMITLOG_UNREADABLE").finding("COMMITLOG_UNREADABLE").message)
        .startsWith("commit log segments that cannot be read to the end: CommitLog-7-1793990000001.log (");
        deleteTree(node.commitLog);
        Run missing = assertReady(node);
        assertThat(missing.codes(Finding.Severity.WARNING)).containsExactly("COMMITLOG_DIRECTORY_MISSING");
        assertThat(missing.finding("COMMITLOG_DIRECTORY_MISSING").message)
        .isEqualTo("no commit log directory at " + node.commitLog + ": HCD 2.0 always has one, so this may not be " +
                   "its commitlog_directory, and its segments are not checked");
    }

    /** A commit log directory that cannot be listed hides its segments. */
    @Test
    public void refusesACommitLogDirectoryItCannotList() throws IOException
    {
        Dse51NodeFixture node = node();
        withPermissions(node.commitLog, "---------", () -> {
            Run run = assertBlocked(node, "COMMITLOG_UNREADABLE");
            assertThat(run.finding("COMMITLOG_UNREADABLE").message)
            .isEqualTo("cannot list the commit log directory " + node.commitLog + ", so whether it holds writes not " +
                       "yet in an sstable cannot be told");
            assertThat(run.finding("COMMITLOG_UNREADABLE").remedy)
            .isEqualTo("run the tool as the user HCD 2.0 runs as (or as root), or point it at the directory " +
                       "(--commitlog-dir)");
        });
    }

    /**
     * A hints file of another version than 1, by its name or by its descriptor, refuses the rollback; so does one whose
     * descriptor is empty or damaged, which neither release delivers. The files are taken by the host their
     * descriptor names.
     */
    @Test
    public void refusesHintsFilesOfAnotherVersionOrADamagedDescriptor() throws IOException
    {
        Dse51NodeFixture node = node();
        Path byName = node.hints.resolve(NODE_2 + "-1793990000000-2.hints");
        hintsFile(byName, 2, NODE_2);
        Run run = assertBlocked(node, "HINTS_VERSION");
        assertThat(run.finding("HINTS_VERSION").message)
        .isEqualTo("hints files that are not of version 1, the only one DSE 5.1.51 delivers: " + byName.getFileName() +
                   " (version 2 by its name); DSE 5.1.51 loads them and never delivers them");
        Files.delete(byName);
        Path dse6 = node.hints.resolve(NODE_2 + "-1793990000000-v1.hints");
        hintsFile(dse6, 1, NODE_2);
        assertThat(assertBlocked(node, "HINTS_VERSION").finding("HINTS_VERSION").message)
        .contains(dse6.getFileName() + " (version v1 by its name)");
        Files.delete(dse6);

        Path byDescriptor = node.hints.resolve(NODE_2 + "-1793990000000-1.hints");
        hintsFile(byDescriptor, 2, NODE_2);
        assertThat(assertBlocked(node, "HINTS_VERSION").finding("HINTS_VERSION").message)
        .contains(byDescriptor.getFileName() + " (version 2 by its descriptor)");

        Files.write(byDescriptor, new byte[0]);
        Run empty = assertBlocked(node, "HINTS_DESCRIPTOR_UNREADABLE");
        assertThat(empty.finding("HINTS_DESCRIPTOR_UNREADABLE").message)
        .isEqualTo("hints files whose descriptor is empty or cannot be read: " + byDescriptor.getFileName() +
                   " (empty); their hints cannot be delivered: HCD 2.0 deletes an empty one and sets a damaged one " +
                   "aside, or does not start on it, and what DSE 5.1.51 does with such a file is not known");
        assertThat(empty.finding("HINTS_DESCRIPTOR_UNREADABLE").remedy)
        .isEqualTo("move them out of the hints directory before DSE 5.1 starts, and repair the tables they may hold " +
                   "once the cluster runs one release");
        hintsFile(byDescriptor, 1, NODE_2);
        byte[] damaged = Files.readAllBytes(byDescriptor);
        damaged[30] ^= 1;
        Files.write(byDescriptor, damaged);
        assertThat(assertBlocked(node, "HINTS_DESCRIPTOR_UNREADABLE").finding("HINTS_DESCRIPTOR_UNREADABLE").message)
        .contains(byDescriptor.getFileName() + " (IOException: the checksum of its header does not match)");

        // a file named for node 2 whose descriptor names node 3 is node 3's
        hintsFile(byDescriptor, 1, NODE_3);
        Run byHost = assertReady(node);
        String target = NODE_3 + " (" + node3Address + ", release " + Dse51Fixtures.RELEASE_VERSION + ')';
        assertThat(byHost.report.hintsByTarget).containsOnlyKeys(target);
        assertThat(byHost.report.hintsByTarget.get(target)).contains(byDescriptor.getFileName().toString());
    }

    /** A hints directory that is missing may not be HCD 2.0's. */
    @Test
    public void warnsWithoutAHintsDirectory() throws IOException
    {
        Dse51NodeFixture node = node();
        deleteTree(node.hints);
        Run run = assertReady(node);
        assertThat(run.codes(Finding.Severity.WARNING)).containsExactly("HINTS_DIRECTORY_MISSING");
        assertThat(run.finding("HINTS_DIRECTORY_MISSING").remedy)
        .isEqualTo("pass --hints-dir with the hints_directory of HCD 2.0's cassandra.yaml; if it is that directory, " +
                   "the rollback goes on");
    }

    /** A hints directory that cannot be listed hides its files. */
    @Test
    public void refusesAHintsDirectoryItCannotList() throws IOException
    {
        Dse51NodeFixture node = node();
        withPermissions(node.hints, "---------", () -> {
            assertThat(assertBlocked(node, "HINTS_UNREADABLE").finding("HINTS_UNREADABLE").message)
            .isEqualTo("cannot list the hints directory " + node.hints + ", so the versions of its hints files " +
                       "cannot be checked");
        });
    }

    /** The companions of a hints file of version 1 are kept with it. */
    @Test
    public void keepsTheCompanionsOfTheHintsFiles() throws IOException
    {
        Dse51NodeFixture node = node();
        Path hints = node.hints.resolve(DseFixtures.dse51().hintsFile().getFileName());
        String base = hints.getFileName().toString().replace(".hints", "");
        Path crc = Files.write(node.hints.resolve(base + ".crc32"), "0".getBytes(StandardCharsets.UTF_8));
        Path statistics = Files.write(node.hints.resolve(base + "-Statistics.hints"), new byte[8]);
        Run run = assertReady(node);
        assertThat(run.report.kept).containsSubsequence(hints.toString(), crc.toString(), statistics.toString());
        assertThat(run.out).contains("Kept by the rollback:\n")
                           .contains("  - " + hints + "\n  - " + crc + "\n  - " + statistics + '\n');
    }

    /**
     * Without a complete snapshot of system and system_schema there is nothing to restore; with several, the one the
     * restore takes is named with --snapshot.
     */
    @Test
    public void checksTheSnapshotOfTheSystemTables() throws IOException
    {
        Dse51NodeFixture node = node();
        String later = "pre-hcd-20261108T100000Z";
        for (String keyspace : List.of("system", "system_schema"))
            Dse51PaxosRollbackTest.snapshotKeyspace(node.data.resolve(keyspace), later);
        Run several = assertBlocked(node, "SNAPSHOT_AMBIGUOUS");
        assertThat(several.finding("SNAPSHOT_AMBIGUOUS").message)
        .isEqualTo("several pre-hcd-<timestamp> snapshots hold system.local, system.peers and the tables of DSE " +
                   "5.1's system_schema, with sstables of system.local and system_schema.keyspaces: " +
                   Dse51PaxosRollbackTest.TAG + ", " + later);
        assertThat(assertReady(node, "--snapshot", later).finding("SNAPSHOT").message)
        .isEqualTo("the restore of system and system_schema takes the snapshot " + later);
        String what = "system.local, system.peers and the tables of DSE 5.1's system_schema, with sstables of " +
                      "system.local and system_schema.keyspaces";
        String completeOnes = "complete pre-hcd-<timestamp> snapshots: " + Dse51PaxosRollbackTest.TAG + ", " + later;
        assertThat(assertBlocked(node, "SNAPSHOT_MISSING", "--snapshot", "pre-hcd-other").finding("SNAPSHOT_MISSING")
                                                                                          .message)
        .isEqualTo("the snapshot pre-hcd-other given with --snapshot does not hold " + what + " (not found; " +
                   completeOnes + ')');
        Run path = run(node, "--snapshot", "a/b");
        assertThat(path.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(path.err).isEqualTo("--snapshot takes the tag of a snapshot, not a path: a/b\n");
        // a tag of another form, as the Paxos rollback mode takes it
        for (String keyspace : List.of("system", "system_schema"))
            Dse51PaxosRollbackTest.snapshotKeyspace(node.data.resolve(keyspace), "before-upgrade");
        assertThat(assertReady(node, "--snapshot", "before-upgrade").finding("SNAPSHOT").message)
        .isEqualTo("the restore of system and system_schema takes the snapshot before-upgrade");
        // a copy of system.local without its sstables is no snapshot of it
        try (Stream<Path> files = Files.list(tableDirectory(node, "system", "local").resolve("snapshots")
                                                                                   .resolve("before-upgrade")))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.delete(file);
        }
        assertThat(assertBlocked(node, "SNAPSHOT_MISSING", "--snapshot", "before-upgrade").finding("SNAPSHOT_MISSING")
                                                                                           .message)
        .isEqualTo("the snapshot before-upgrade given with --snapshot does not hold " + what + " (without an " +
                   "sstable: system.local; " + completeOnes + ')');
        // nor is a copy of system_schema.keyspaces without its sstables, the snapshot found without --snapshot
        try (Stream<Path> files = Files.list(tableDirectory(node, "system_schema", "keyspaces").resolve("snapshots")
                                                                                              .resolve(later)))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.delete(file);
        }
        assertThat(assertReady(node).finding("SNAPSHOT").message)
        .isEqualTo("the restore of system and system_schema takes the snapshot " + Dse51PaxosRollbackTest.TAG);

        deleteTree(tableDirectory(node, "system_schema", "views").resolve("snapshots").resolve(later));
        deleteTree(tableDirectory(node, "system_schema", "views").resolve("snapshots")
                                                                 .resolve(Dse51PaxosRollbackTest.TAG));
        Run partial = assertBlocked(node, "SNAPSHOT_MISSING");
        assertThat(partial.finding("SNAPSHOT_MISSING").message)
        .isEqualTo("no pre-hcd-<timestamp> snapshot in the data directories holds " + what + " (found: " +
                   Dse51PaxosRollbackTest.TAG + " (missing: system_schema.views), " + later + " (missing: " +
                   "system_schema.views; without an sstable: system_schema.keyspaces)); the rollback restores system " +
                   "and system_schema from the snapshot taken on DSE 5.1 before the upgrade");
        assertThat(partial.report.inputs.get("snapshots").toString())
        .isEqualTo("[before-upgrade, " + Dse51PaxosRollbackTest.TAG + ", " + later + ']');
        for (String keyspace : List.of("system", "system_schema"))
            try (Stream<Path> tables = Files.list(node.data.resolve(keyspace)))
            {
                for (Path table : (Iterable<Path>) tables::iterator)
                    if (Files.exists(table.resolve("snapshots")))
                        deleteTree(table.resolve("snapshots"));
            }
        assertThat(assertBlocked(node, "SNAPSHOT_MISSING").finding("SNAPSHOT_MISSING").message)
        .isEqualTo("no pre-hcd-<timestamp> snapshot in the data directories holds " + what + " (none found); the " +
                   "rollback restores system and system_schema from the snapshot taken on DSE 5.1 before the " +
                   "upgrade");
    }

    /**
     * With two data directories, a table of the snapshot held with sstables in one and as an empty copy in the other
     * is held: only what the snapshot lacks is named.
     */
    @Test
    public void readsTheSnapshotOfSeveralDataDirectories() throws IOException
    {
        Dse51NodeFixture node = node();
        Path second = node.root.resolve("var").resolve("data2");
        Path local = tableDirectory(node, "system", "local");
        Files.createDirectories(second.resolve("system").resolve(local.getFileName()).resolve("snapshots")
                                      .resolve(Dse51PaxosRollbackTest.TAG));
        deleteTree(tableDirectory(node, "system_schema", "views").resolve("snapshots"));
        Run run = assertBlocked(node, "SNAPSHOT_MISSING", "--data-dir", node.data.toString(), "--data-dir",
                                second.toString());
        assertThat(run.finding("SNAPSHOT_MISSING").message)
        .isEqualTo("no pre-hcd-<timestamp> snapshot in the data directories holds system.local, system.peers and the " +
                   "tables of DSE 5.1's system_schema, with sstables of system.local and system_schema.keyspaces " +
                   "(found: " + Dse51PaxosRollbackTest.TAG + " (missing: system_schema.views)); the rollback " +
                   "restores system and system_schema from the snapshot taken on DSE 5.1 before the upgrade");
    }

    /**
     * The rollback keeps the DSE 5.1 upgrade marker and removes the record of the values of DSE 5.1's columns and its
     * temporary file; the copies of unreadable records are kept; each by its name.
     */
    @Test
    public void listsTheFilesOfTheUpgradeByTheirNames() throws IOException
    {
        Dse51NodeFixture node = node();
        String record = Dse51SystemTables.RECORD_FILE_NAME;
        Path recordPath = node.metadata.resolve(record);
        Path temporary = node.metadata.resolve(record + Dse51SystemTables.TEMPORARY_SUFFIX);
        Path copy = node.metadata.resolve(record + Dse51SystemTables.UNREADABLE_SUFFIX + "-1793958236846");
        for (Path file : List.of(temporary, copy))
            Files.write(file, new byte[0]);
        Run run = assertReady(node);
        assertThat(run.report.toRemove).contains(recordPath.toString(), temporary.toString());
        assertThat(run.report.kept).contains(copy.toString(),
                                             node.metadata.resolve(DseUpgradeMarker.DSE_51_FILE_NAME).toString(),
                                             node.metadata.resolve(DsePeersRecord.DSE_51_FILE_NAME).toString());
        assertThat(run.finding("DSE51_SYSTEM_COLUMNS_COPIES").message).endsWith(": " + copy);
        assertThat(run.finding("ROLLBACK_REMOVES").message).contains(recordPath.toString(), temporary.toString());

        // the temporary file alone is removed too, and refuses nothing
        Files.delete(recordPath);
        Run temporaryOnly = assertReady(node);
        assertThat(temporaryOnly.report.toRemove).contains(temporary.toString()).doesNotContain(recordPath.toString());
        assertThat(temporaryOnly.report.findings).noneMatch(f -> f.code.equals("DSE51_SYSTEM_COLUMNS"));
    }

    /** A metadata directory that can be listed but not searched: the names decide. */
    @Test
    public void readsTheMetadataDirectoryByTheNamesListed() throws IOException
    {
        Dse51NodeFixture node = node();
        withPermissions(node.metadata, "r--------", () -> {
            Run listed = assertReady(node);
            assertThat(listed.report.toRemove).contains(node.metadata.resolve(Dse51SystemTables.RECORD_FILE_NAME)
                                                                     .toString());
            assertThat(listed.report.kept).contains(node.metadata.resolve(DseUpgradeMarker.DSE_51_FILE_NAME)
                                                                 .toString());
        });
    }

    /** A metadata directory that cannot be listed hides the files of the upgrade. */
    @Test
    public void refusesAMetadataDirectoryItCannotList() throws IOException
    {
        Dse51NodeFixture node = node();
        withPermissions(node.metadata, "---------", () -> {
            assertThat(assertBlocked(node, "METADATA_UNREADABLE").finding("METADATA_UNREADABLE").message)
            .isEqualTo("cannot list the metadata directory " + node.metadata + ", so whether it holds the DSE 5.1 " +
                       "upgrade marker dse51-upgrade-in-progress and the record dse51-system-columns, which the " +
                       "rollback removes, cannot be checked");
        });
    }

    /** Without the marker the metadata directory read may not be HCD 2.0's: refused, as the files go unchecked. */
    @Test
    public void refusesWithoutTheMarker() throws IOException
    {
        Dse51NodeFixture node = node();
        Files.delete(node.metadata.resolve(DseUpgradeMarker.DSE_51_FILE_NAME));
        Run run = assertBlocked(node, "MARKER_NOT_FOUND");
        assertThat(run.finding("MARKER_NOT_FOUND").message)
        .isEqualTo("the metadata directory " + node.metadata + " holds no dse51-upgrade-in-progress, which HCD 2.0 " +
                   "writes at its first start in DSE_5_1 mode: it is not HCD 2.0's metadata directory on this node, " +
                   "or HCD 2.0 never completed a start in DSE_5_1 mode here, so the files of the upgrade listed from " +
                   "it may not be this node's");
        assertThat(run.finding("MARKER_NOT_FOUND").remedy)
        .startsWith("pass --metadata-dir with the metadata_directory of HCD 2.0's cassandra.yaml; ");
        deleteTree(node.metadata);
        assertThat(assertBlocked(node, "MARKER_NOT_FOUND").finding("MARKER_NOT_FOUND").message)
        .isEqualTo("the metadata directory " + node.metadata + " does not exist or is not a directory: HCD 2.0 " +
                   "creates its metadata directory at its first start, and writes there dse51-upgrade-in-progress at " +
                   "its first start in DSE_5_1 mode, so this is not HCD 2.0's metadata directory on this node, or " +
                   "HCD 2.0 never started here");
    }

    /**
     * The inputs: DSE's cassandra.yaml that cannot be read, a data directory that does not exist (the remedies worded
     * for this node, not for a DSE node), and the default of the saved caches directory.
     */
    @Test
    public void checksTheInputs() throws IOException
    {
        Dse51NodeFixture node = node();
        Path missing = node.root.resolve("no-such-data");
        Run noData = run(node, "--data-dir", missing.toString(), "--data-dir", node.data.toString());
        assertThat(noData.exitCode).as("%s%s", noData.out, noData.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(noData.codes(Finding.Severity.BLOCKING)).containsExactly("UNREADABLE_INPUT");
        assertThat(noData.finding("UNREADABLE_INPUT").message)
        .isEqualTo("the data directory " + missing + " does not exist or is not a directory");
        assertThat(noData.finding("UNREADABLE_INPUT").remedy)
        .isEqualTo("point the tool at the data directories of this node (--data-dir)");

        node.replace(NodeLayout.CASSANDRA_YAML, "saved_caches_directory: ", "# saved_caches_directory: ");
        Run defaults = assertReady(node);
        assertThat(defaults.finding("DEFAULTS_ASSUMED").message)
        .isEqualTo("DSE's cassandra.yaml does not set these directories, DSE's defaults are assumed: " +
                   "saved_caches_directory: " + Dse51RollbackInspection.DEFAULT_SAVED_CACHES);
        // the default directory of this machine is listed, never written: its files would only be named for removal
        assertThat(defaults.report.inputs)
        .containsEntry("saved_caches_directory", Dse51RollbackInspection.DEFAULT_SAVED_CACHES.toString());
        assertThat(defaults.report.toRemove).doesNotContain(savedCaches(node).resolve("KeyCache-g.db").toString());

        Files.write(node.conf.resolve(NodeLayout.CASSANDRA_YAML), "[not, a, map".getBytes(StandardCharsets.UTF_8));
        Run yaml = run(node, "--data-dir", node.data.toString(), "--commitlog-dir", node.commitLog.toString(),
                       "--hints-dir", node.hints.toString());
        assertThat(yaml.exitCode).as("%s%s", yaml.out, yaml.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(yaml.codes(Finding.Severity.BLOCKING)).containsExactly("UNREADABLE_INPUT");
        assertThat(yaml.report.findings).filteredOn(f -> f.code.equals("UNREADABLE_INPUT"))
                                        .singleElement()
                                        .satisfies(f -> assertThat(f.message)
                                                        .startsWith("cannot read cassandra.yaml: " +
                                                                    node.conf.resolve(NodeLayout.CASSANDRA_YAML)));
    }

    /** A table directory that cannot be listed refuses, with the remedy worded for the files of HCD 2.0. */
    @Test
    public void refusesATableDirectoryItCannotList() throws IOException
    {
        Dse51NodeFixture node = node();
        Path table = tableDirectory(node, "system_auth", "role_permissions");
        withPermissions(table, "---------", () -> {
            Run run = assertBlocked(node, "UNREADABLE_INPUT");
            assertThat(run.finding("UNREADABLE_INPUT").message).startsWith("cannot list the table directory " + table +
                                                                           " (")
                                                               .endsWith(")");
            assertThat(run.finding("UNREADABLE_INPUT").remedy)
            .isEqualTo("run the tool as the user HCD 2.0 runs as (or as root)");
        });
    }

    /** A saved caches directory that cannot be listed is said, not refused: the rollback empties it anyway. */
    @Test
    public void warnsAboutASavedCachesDirectoryItCannotList() throws IOException
    {
        Dse51NodeFixture node = node();
        withPermissions(savedCaches(node), "---------", () -> {
            Run run = assertReady(node);
            assertThat(run.codes(Finding.Severity.WARNING)).containsExactly("SAVED_CACHES_UNREADABLE");
            assertThat(run.finding("SAVED_CACHES_UNREADABLE").message)
            .isEqualTo("cannot list the saved caches directory " + savedCaches(node));
        });
    }

    /** The warnings, none of which refuses, each with its own text. */
    @Test
    public void warnsAboutSaiFilesAndHintsDse51DoesNotDeliver() throws IOException
    {
        Dse51NodeFixture node = node();
        Path sai = tableDirectory(node, "system_schema", "tables").resolve("me-1-big-SAI+ca+t_v+TermsData.db");
        Path saiAa = tableDirectory(node, "system_schema", "tables").resolve("me-1-big-SAI_t_v_Meta.db");
        Files.write(sai, new byte[0]);
        Files.write(saiAa, new byte[0]);
        Run withSai = assertReady(node);
        assertThat(withSai.codes(Finding.Severity.WARNING)).containsExactly("SAI_FILES");
        assertThat(withSai.finding("SAI_FILES").message)
        .isEqualTo("SAI component files left in the data directories: " + sai + ", " + saiAa + "; DSE 5.1.51 has no " +
                   "SAI, ignores these files and removes them with their sstable");
        assertThat(withSai.report.mayRemove).containsExactly(sai.toString(), saiAa.toString());
        Files.delete(sai);
        Files.delete(saiAa);

        // node 3, the target of the hints file, now runs HCD 2.0
        String release = FBUtilities.getReleaseVersionString();
        node.writeRows("system", "peers", List.of("peer"), List.of(),
                       row(InetAddress.getByName(node3Address)).with("release_version", release));
        Run forHcd = assertReady(node);
        assertThat(forHcd.codes(Finding.Severity.WARNING)).containsExactly("HINTS_FOR_HCD");
        assertThat(forHcd.finding("HINTS_FOR_HCD").message)
        .isEqualTo("hints files of version 1 for targets that system.peers does not show running DSE 5.1 (HCD 2.0, " +
                   "or no release recorded): " + NODE_3 + " (" + node3Address + ", release " + release + "): 1 " +
                   "file(s); DSE 5.1.51 delivers a hints file only to a peer that gossips its own schema version, " +
                   "which a node of HCD 2.0 does not, so these hints wait for the target's own rollback to DSE 5.1, " +
                   "or for this node's next upgrade");

        // node 3 left: no row of it
        node.deleteRows("system", "peers", List.of("peer"), List.of(), row(InetAddress.getByName(node3Address)));
        Run unknown = assertReady(node);
        assertThat(unknown.codes(Finding.Severity.WARNING)).containsExactly("HINTS_FOR_UNKNOWN_TARGET");
        assertThat(unknown.finding("HINTS_FOR_UNKNOWN_TARGET").message)
        .isEqualTo("hints files of version 1 for host ids in neither system.local nor system.peers (a host that " +
                   "left the cluster, or one never saved): " + NODE_3 + " (unknown): 1 file(s); DSE 5.1.51 deletes " +
                   "the files of a host removed from the ring, and writes again for the current replicas a file " +
                   "whose host id it cannot map to an address, so these hints may reach other replicas or none");

        // system.peers cannot be read: the targets are not checked
        try (Stream<Path> files = Files.list(tableDirectory(node, "system", "peers")))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                if (file.getFileName().toString().endsWith("-Statistics.db"))
                    Files.write(file, new byte[]{ 1, 2, 3 });
        }
        Run notChecked = assertReady(node);
        assertThat(notChecked.codes(Finding.Severity.WARNING)).containsExactly("HINTS_TARGETS_UNKNOWN");
        assertThat(notChecked.finding("HINTS_TARGETS_UNKNOWN").message)
        .startsWith("cannot read system.local and system.peers of the data directories (");
        assertThat(notChecked.report.hintsByTarget).containsOnlyKeys(NODE_3 + " (not checked)");
    }

    /** The aggregate mode skips the report of the check, which is no node report, with a note. */
    @Test
    public void theAggregateModeSkipsTheReportOfTheCheck() throws IOException
    {
        Dse51NodeFixture node = node();
        Path reports = temporaryFolder.newFolder("reports").toPath();
        String[] args = node.rollbackCheckArgs("--json", reports.resolve("rollback.json").toString());
        assertThat(DsePreflight.run(args, new PrintStream(new ByteArrayOutputStream()),
                                    new PrintStream(new ByteArrayOutputStream())))
        .isEqualTo(DsePreflight.EXIT_READY);
        ClusterAggregate.AggregateReport aggregate = ClusterAggregate.aggregate(reports);
        assertThat(aggregate.findings).filteredOn(f -> f.code.equals("ROLLBACK_CHECK_REPORT_SKIPPED"))
                                      .singleElement()
                                      .satisfies(f -> {
                                          assertThat(f.severity).isEqualTo(Finding.Severity.INFO);
                                          assertThat(f.message).isEqualTo("reports of the rollback check " +
                                                                          "(--rollback-check) in the directory, " +
                                                                          "skipped: rollback.json");
                                      });
        assertThat(aggregate.findings).noneMatch(f -> f.code.equals("UNREADABLE_REPORT"));
        assertThat(aggregate.reportsRead).isZero();
    }

    /** The check is for a node going back to DSE 5.1: on a DSE 6 node it is a usage error. */
    @Test
    public void refusesADse6Node() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
             PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(new String[]{ "--rollback-check", "--dse-home", node.dseHome.toString(),
                                                      "--metadata-dir", node.metadata.toString(),
                                                      "--data-dir", node.data.toString() },
                                        outStream, errStream);
        }
        assertThat(exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(err.toString(StandardCharsets.UTF_8))
        .isEqualTo("--rollback-check applies to nodes rolled back to DSE 5.1 only, and this node is inspected as a " +
                   "DSE 6 node\n");
        assertThat(out.toString(StandardCharsets.UTF_8)).isEmpty();
    }
}
