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
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.tools.dsepreflight.DseNodeFixture.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The preflight inspection of a DSE 5.1.51 node ({@link Dse51NodeInspector}), run through its command line on node 1
 * of the three-node cluster of the fixtures, laid out by {@link Dse51NodeFixture}: its configuration as it ran and as
 * installed, its {@code system.local} and {@code system.peers} sstables (compacted and flushed, and those of the node
 * of the Search and Graph cluster), its schema, hints and commit log. The node as DSE 5.1.51 ran it is ready once
 * Thrift is off; each problem added to a copy makes the tool name it. The schema rows a test adds are written as big
 * {@code me} sstables by this JVM, a client in the DSE 5.1 compatibility mode, since the fixtures hold no table on a
 * compaction strategy this software lacks, and no other release.
 */
public class Dse51PreflightTest
{
    private static final List<String> KEYSPACES_KEY = List.of("keyspace_name");
    private static final String DTCS = "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy";
    private static final String TIERED = "org.apache.cassandra.db.compaction.TieredCompactionStrategy";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void initialise()
    {
        // the sstables the tests add next to DSE's are written at big me, as a client in the DSE 5.1 mode writes them
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(INTERNODE_COMPATIBILITY_MODE, "DSE_5_1"))
        {
            DatabaseDescriptor.clientInitialization();
        }
        assertThat(BigFormat.getInstance().getLatestVersion().version)
        .isEqualTo(InternodeCompatibilityMode.DSE_51_BIG_VERSION);
    }

    private Dse51NodeFixture node() throws IOException
    {
        return Dse51NodeFixture.create(temporaryFolder.newFolder().toPath());
    }

    private Dse51NodeFixture node(boolean running, Path systemTables) throws IOException
    {
        return Dse51NodeFixture.create(temporaryFolder.newFolder().toPath(), running, systemTables);
    }

    /** The node as it ran, with Thrift turned off in its cassandra.yaml. */
    private Dse51NodeFixture thriftOff() throws IOException
    {
        Dse51NodeFixture node = node();
        node.replace(NodeLayout.CASSANDRA_YAML, "start_rpc: true", "start_rpc: false");
        return node;
    }

    /** The scratch directories of {@link DseTableReader#readBig} the runs of a test have left. */
    private Set<String> scratchDirectories() throws IOException
    {
        try (Stream<Path> entries = Files.list(DseTableReader.scratchParent))
        {
            return entries.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
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

    private DsePreflightTest.Run run(Dse51NodeFixture node, String... extra) throws IOException
    {
        Path json = temporaryFolder.getRoot().toPath().resolve("report-" + UUID.randomUUID() + ".json");
        return DsePreflightTest.run(json, node.args(extra));
    }

    private static DsePreflightTest.Run assertBlocked(DsePreflightTest.Run run, String code)
    {
        assertThat(run.exitCode).as("exit code; output:\n%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.result).isEqualTo(NodeReport.BLOCKED);
        assertThat(run.codes(Finding.Severity.BLOCKING)).as("blocking findings; output:\n%s", run.out).contains(code);
        assertThat(run.out).contains("Result: BLOCKED").contains('[' + code + ']');
        return run;
    }

    private static DsePreflightTest.Run assertReady(DsePreflightTest.Run run)
    {
        assertThat(run.exitCode).as("exit code; output:\n%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(run.report.result).isEqualTo(NodeReport.READY);
        assertThat(run.codes(Finding.Severity.BLOCKING)).isEmpty();
        return run;
    }

    @Test
    public void theNodeAsItRanIsReadyOnceThriftIsOffAndItsFilesAreUnchanged() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Map<String, String> before = node.fingerprint();
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(node.fingerprint()).isEqualTo(before);
        // the scratch copies of the big sstables read are gone
        assertThat(scratchDirectories()).isEmpty();

        assertThat(run.out).startsWith("DSE 5.1 preflight inspection for the upgrade to HCD 2.0")
                           .contains("DSE release: DSE 5.1.51 (release 3.11.3.5151, installation 5.1.51)")
                           .contains("Peers in system.peers: 2")
                           .doesNotContain("SAI on-disk version");
        NodeReport report = run.report;
        assertThat(report.dse51.source).isEqualTo(Dse51NodeInspector.SOURCE);
        assertThat(report.node.hostId).isEqualTo(Dse51Fixtures.HOST_IDS.get(0).toString());
        assertThat(report.node.broadcastAddress).isEqualTo("127.0.51.11");
        assertThat(report.node.listenAddress).isEqualTo("127.0.51.1");
        assertThat(report.node.clusterName).isEqualTo("Test Cluster");
        assertThat(report.node.dataCenter).isEqualTo("Cassandra");
        assertThat(report.node.rack).isEqualTo("rack1");
        assertThat(report.node.releaseVersion).isEqualTo(Dse51Fixtures.RELEASE_VERSION);
        assertThat(report.node.dseVersion).isEqualTo(Dse51Fixtures.DSE_VERSION);
        assertThat(report.peers).extracting(p -> p.address).containsExactlyInAnyOrder("127.0.51.12", "127.0.51.13");
        assertThat(report.peers).allMatch(p -> Dse51Fixtures.DSE_VERSION.equals(p.dseVersion) && !p.upgraded);
        assertThat(report.peers).extracting(p -> p.hostId)
                                .containsExactlyInAnyOrder(Dse51Fixtures.HOST_IDS.get(1).toString(),
                                                           Dse51Fixtures.HOST_IDS.get(2).toString());

        assertThat(run.messages("DSE_RELEASE")).contains("DSE 5.1.51 (release 3.11.3.5151) is accepted");
        assertThat(run.messages("THRIFT"))
        .startsWith("Thrift is off when DSE starts (start_rpc: false in DSE's cassandra.yaml)");
        assertThat(report.dse51.thriftOn).isFalse();
        assertThat(report.dse51.nativeTransportPort).isEqualTo("9042");
        assertThat(report.dse51.nativeTransportPortSsl).isNull();
        assertThat(run.messages("NATIVE_PORTS"))
        .startsWith("native_transport_port 9042: HCD 2.0 keeps it on every node");
        // the node ran with DseAuthorizer, authorization on and allow_row_level_security true
        assertThat(report.dse51.rowLevelAccessControlEnforced).isTrue();
        assertThat(run.findings("ROW_LEVEL_ACCESS").get(0).remedy)
        .contains("UNRESTRICT ROWS on every restricted table, REVOKE the rows/ grants, then set " +
                  "allow_row_level_security: false; for a materialized view restricted to rows, ask the upgrade " +
                  "owner, since its remedy is not validated yet (never UNRESTRICT ROWS on a view");
        Set<String> warnings = new HashSet<>(run.codes(Finding.Severity.WARNING));
        warnings.removeAll(List.of("JAVA", "LOW_DISK_SPACE")); // they depend on the machine
        assertThat(warnings).containsExactlyInAnyOrder("ROW_LEVEL_ACCESS", "SNITCH", "PENDING_HINTS",
                                                       "AUTH_CONFIG_CHANGE");
        assertThat(run.findings("NO_PEERS")).isEmpty();
        assertThat(run.findings("SNITCH").get(0).remedy).contains("endpoint_snitch: GossipingPropertyFileSnitch")
                                                       .contains("dc=Cassandra and rack=rack1 (from system.local)");
        // the hints file node 1 wrote for node 3
        assertThat(run.messages("PENDING_HINTS"))
        .contains(Dse51Fixtures.HOST_IDS.get(2) + " (127.0.51.13, DSE 5.1): 1 file(s)");
        assertThat(report.inputs.get("commitlog_segments_by_version")).isEqualTo(Map.of("6", 1));
        assertThat(report.findings).noneMatch(f -> List.of("SSTABLE_FORMAT", "UNREADABLE_SCHEMA", "MIXED_PHASE_MARKER",
                                                           "NODE_RAN_HCD").contains(f.code));

        Map<String, String> settings = report.upgradePath.settings;
        assertThat(settings).containsEntry("internode_compatibility_mode", "DSE_5_1")
                            .containsEntry("storage_compatibility_mode", "DSE_5_1")
                            .containsEntry("paxos_variant", "v1")
                            .containsEntry("sstable.selected_format", "big")
                            .containsEntry("uuid_sstable_identifiers_enabled", "false")
                            .containsEntry("num_tokens", "1 (DSE's cassandra.yaml sets none: the number of tokens " +
                                                         "of this node in system.local)")
                            .containsEntry("endpoint_snitch", "GossipingPropertyFileSnitch (was " +
                                                              "com.datastax.bdp.snitch.DseSimpleSnitch)")
                            .containsEntry("cassandra-rackdc.properties", "dc=Cassandra, rack=rack1")
                            .containsEntry("native_transport_port", "9042 (required: the port of every DSE 5.1 " +
                                                                    "node, until the last is upgraded)")
                            .containsEntry("authorizer", "CassandraAuthorizer (was " +
                                                         "com.datastax.bdp.cassandra.auth.DseAuthorizer)");
        assertThat(report.upgradePath.upgradeSteps)
        .contains("Thrift: off", "Sstable formats: every sstable is big me",
                  "DSE core only: no trace of DSE Search, Graph, Analytics, DSEFS, CFS, Advanced Replication, tiered " +
                  "storage or in-memory tables, nor a compaction strategy HCD 2.0 no longer has, was found");
    }

    @Test
    public void thriftAsInstalledBlocks() throws IOException
    {
        Dse51NodeFixture node = node(false, DseFixtures.dse51().systemSSTables(true));
        DsePreflightTest.Run run = assertBlocked(run(node), "THRIFT");
        assertThat(run.codes(Finding.Severity.BLOCKING)).containsExactly("THRIFT");
        assertThat(run.messages("THRIFT"))
        .startsWith("Thrift is on when DSE starts (start_rpc: true in DSE's cassandra.yaml): HCD 2.0 has no Thrift")
        .contains(Dse51NodeInspector.CONNECTED_THRIFT_CLIENTS);
        assertThat(run.findings("THRIFT").get(0).remedy)
        .startsWith("move every Thrift client to CQL; then set start_rpc: false");
        assertThat(run.report.dse51.thriftOn).isTrue();
        assertThat(run.report.dse51.startRpc).isEqualTo("true");
        // as installed, dse.yaml leaves authorization off: row-level access control is not enforced
        assertThat(run.report.dse51.rowLevelAccessControlEnforced).isFalse();
        assertThat(run.findings("ROW_LEVEL_ACCESS")).isEmpty();
    }

    @Test
    public void theJvmOptionOfThriftOverridesTheYaml() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.append(NodeLayout.JVM_OPTIONS, "-Dcassandra.start_rpc=true");
        DsePreflightTest.Run run = assertBlocked(run(node), "THRIFT");
        assertThat(run.messages("THRIFT")).startsWith("Thrift is on when DSE starts (-Dcassandra.start_rpc=true in " +
                                                      node.conf.resolve("jvm.options") + ')');
        assertThat(run.report.dse51.startRpcJvmOption).isEqualTo("true");

        Dse51NodeFixture other = node();
        other.append(NodeLayout.JVM_OPTIONS, "-Dcassandra.start_rpc=false");
        DsePreflightTest.Run off = assertReady(run(other));
        assertThat(off.messages("THRIFT")).startsWith("Thrift is off when DSE starts (-Dcassandra.start_rpc=false in ");
    }

    @Test
    public void theJvmOptionsHcd20RefusesBlock() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.append(NodeLayout.JVM_OPTIONS, "-Dcassandra.force_3_0_protocol_version=true",
                    "-Denable-old-dse-state=true");
        DsePreflightTest.Run run = assertBlocked(run(node), "JVM_OPTIONS");
        assertThat(run.messages("JVM_OPTIONS"))
        .contains("set -Dcassandra.force_3_0_protocol_version=true and -Denable-old-dse-state=true");

        Dse51NodeFixture missing = thriftOff();
        Files.delete(missing.conf.resolve(NodeLayout.JVM_OPTIONS));
        DsePreflightTest.Run unreadable = assertBlocked(run(missing), "UNREADABLE_INPUT");
        assertThat(unreadable.messages("UNREADABLE_INPUT")).startsWith("cannot read jvm.options: ");
        assertThat(unreadable.findings("THRIFT")).isEmpty();
        assertThat(unreadable.report.upgradePath.upgradeSteps)
        .contains("JVM options: not checked (jvm.options could not be read)");
    }

    /** The flushed sstables, with the peers partition deletion of the node's own address, give the same identity. */
    @Test
    public void theFlushedSystemTablesGiveTheSameIdentity() throws IOException
    {
        Dse51NodeFixture node = node(true, DseFixtures.dse51().systemSSTables(false));
        node.replace(NodeLayout.CASSANDRA_YAML, "start_rpc: true", "start_rpc: false");
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(run.report.node.hostId).isEqualTo(Dse51Fixtures.HOST_IDS.get(0).toString());
        assertThat(run.report.node.dseVersion).isEqualTo(Dse51Fixtures.DSE_VERSION);
        assertThat(run.report.peers).extracting(p -> p.address).containsExactlyInAnyOrder("127.0.51.12", "127.0.51.13");
    }

    /** The node of the Search and Graph cluster: the markers of the start check report its workloads. */
    @Test
    public void theWorkloadsOfASearchAndGraphNodeAreReported() throws IOException
    {
        // that cluster's schema was not captured: node 1's stands in for it, the workloads come from system.local
        Dse51NodeFixture node = node(true, DseFixtures.dse51().searchGraphSystemSSTables(true));
        node.replace(NodeLayout.CASSANDRA_YAML, "start_rpc: true", "start_rpc: false");
        DsePreflightTest.Run run = assertBlocked(run(node), "NON_CORE_WORKLOAD");
        assertThat(run.report.node.hostId).isEqualTo(Dse51Fixtures.SEARCH_GRAPH_HOST_ID.toString());
        assertThat(run.messages("NON_CORE_WORKLOAD"))
        .startsWith("DSE workloads other than Cassandra: this node (system.local): workloads [")
        .contains("graph true");
        assertThat(run.findings("NON_CORE_WORKLOAD").get(0).remedy)
        .startsWith("restart each node listed as a plain Cassandra node");
        assertThat(run.codes(Finding.Severity.INFO)).contains("NO_PEERS");
        assertThat(run.report.upgradePath.upgradeSteps)
        .anyMatch(s -> s.startsWith("DSE core only: remove the traces listed above"));
    }

    /**
     * A table on DateTieredCompactionStrategy, which DSE 5.1.51 has and HCD 2.0 does not, is reported among the
     * markers in a group of its own, and a view on DSE's TieredCompactionStrategy with DSE's features; the rows are
     * written as one more big me sstable of system_schema.tables and of system_schema.views.
     */
    @Test
    public void aTableOnDateTieredCompactionIsReportedAmongTheMarkers() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.writeRows("system_schema", "tables", KEYSPACES_KEY, List.of("table_name"),
                       row("test_ks", "dtcs").with("compaction", Map.of("class", DTCS)));
        node.writeRows("system_schema", "views", KEYSPACES_KEY, List.of("view_name"),
                       row("test_ks", "tiered_view").with("compaction", Map.of("class", TIERED)));
        Map<String, String> before = node.fingerprint();
        DsePreflightTest.Run run = assertBlocked(run(node), "REMOVED_COMPACTION");
        assertThat(node.fingerprint()).isEqualTo(before);
        assertThat(run.messages("REMOVED_COMPACTION"))
        .isEqualTo("tables or materialized views on a compaction strategy this software no longer has: table " +
                   "test_ks.dtcs (compaction " + DTCS + "); a node in DSE_5_1 mode refuses to start while it finds " +
                   "them, and a change made on DSE 5.1 once the upgrade has started does not reach the upgraded nodes");
        assertThat(run.findings("REMOVED_COMPACTION").get(0).remedy)
        .contains("compaction = {'class': 'TimeWindowCompactionStrategy', ...}");
        assertThat(run.messages("TIERED_STORAGE"))
        .startsWith("DSE tiered storage and in-memory tables: materialized view test_ks.tiered_view (compaction " +
                    TIERED + ')');
        // the sstables the test wrote are big me with numeric generations, as DSE 5.1's
        assertThat(run.findings("SSTABLE_FORMAT")).isEmpty();
    }

    @Test
    public void everyFeatureOfTheMarkersHasAFinding()
    {
        Map<Dse6NonCoreTraces.Feature, String> codes = Map.of(Dse6NonCoreTraces.Feature.WORKLOADS, "NON_CORE_WORKLOAD",
                                                              Dse6NonCoreTraces.Feature.SEARCH, "SEARCH_INDEX",
                                                              Dse6NonCoreTraces.Feature.GRAPH, "GRAPH_KEYSPACE",
                                                              Dse6NonCoreTraces.Feature.ANALYTICS, "NON_CORE_KEYSPACE",
                                                              Dse6NonCoreTraces.Feature.ADVANCED_REPLICATION,
                                                              "ADVANCED_REPLICATION",
                                                              Dse6NonCoreTraces.Feature.TIERED_AND_IN_MEMORY,
                                                              "TIERED_STORAGE",
                                                              Dse6NonCoreTraces.Feature.REMOVED_COMPACTION,
                                                              "REMOVED_COMPACTION");
        Set<String> seen = new HashSet<>();
        for (Dse6NonCoreTraces.Feature feature : Dse6NonCoreTraces.Feature.values())
        {
            if (feature == Dse6NonCoreTraces.Feature.UNREADABLE)
                continue; // read failures of DSE 6's nodes files only
            Finding finding = Dse51NodeInspector.markerFinding(feature, List.of("table ks.t (compaction X)"));
            assertThat(finding.severity).isEqualTo(Finding.Severity.BLOCKING);
            assertThat(finding.code).as("%s", feature).isEqualTo(codes.get(feature));
            assertThat(finding.message).contains(feature.displayName() + ": table ks.t (compaction X)");
            assertThat(finding.remedy).isNotEmpty();
            seen.add(finding.code);
        }
        assertThat(seen).containsExactlyInAnyOrderElementsOf(Dse51NodeInspector.MARKER_CODES);
    }

    @Test
    public void anotherReleaseBlocks() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.writeRows("system", "local", List.of("key"), List.of(),
                       row("local").with("release_version", "3.11.3.5150").with("dse_version", "5.1.50"));
        DsePreflightTest.Run run = assertBlocked(run(node), "DSE_RELEASE");
        assertThat(run.messages("DSE_RELEASE")).isEqualTo("this node runs the release 3.11.3.5150 and dse_version " +
                                                          "5.1.50; DSE 5.1.51 is the only DSE 5.1 release upgraded " +
                                                          "in place to HCD 2.0");
        assertBlocked(run, "INSTALLED_RELEASE_MISMATCH");

        assertThat(Dse51NodeInspector.isDse51551("3.11.3.5151", "5.1.51")).isTrue();
        assertThat(Dse51NodeInspector.isDse51551(null, "5.1.51")).isTrue();
        assertThat(Dse51NodeInspector.isDse51551("3.11.3.5151", null)).isTrue();
        assertThat(Dse51NodeInspector.isDse51551(null, null)).isFalse();
        assertThat(Dse51NodeInspector.isDse51551("3.11.3.5151", "5.1.50")).isFalse();
        assertThat(Dse51NodeInspector.isDse51551("4.0.0.6926", "6.9.26")).isFalse();
    }

    @Test
    public void everySSTableMustBeBigMeWithANumericGeneration() throws IOException
    {
        assertThat(Dse51NodeInspector.sstableProblem("me-29-big-Data.db")).isNull();
        assertThat(Dse51NodeInspector.sstableProblem("mc-3-big-Data.db"))
        .isEqualTo(Dse51NodeInspector.SSTABLE_BIG_OLDER);
        assertThat(Dse51NodeInspector.sstableProblem("nb-3-big-Data.db"))
        .isEqualTo(Dse51NodeInspector.SSTABLE_BIG_NEWER);
        assertThat(Dse51NodeInspector.sstableProblem("bb-3-bti-Data.db")).isEqualTo(Dse51NodeInspector.SSTABLE_TRIE);
        assertThat(Dse51NodeInspector.sstableProblem("me-3gf0_0q1c_2l1jk2dt7w2ag7m4o9-big-Data.db"))
        .isEqualTo(Dse51NodeInspector.SSTABLE_NON_NUMERIC_ID);
        assertThat(Dse51NodeInspector.sstableProblem("Data.db")).isEqualTo(Dse51NodeInspector.SSTABLE_UNKNOWN);
        // the names before Cassandra 2.2, which DSE 5.1 reads
        assertThat(Dse51NodeInspector.sstableProblem("ks-t-ka-12-Data.db"))
        .isEqualTo(Dse51NodeInspector.SSTABLE_BIG_OLDER);
        assertThat(Dse51NodeInspector.sstableProblem("ks-t-me-12-Data.db"))
        .isEqualTo(Dse51NodeInspector.SSTABLE_UNKNOWN);

        Dse51NodeFixture node = thriftOff();
        // a table this tool does not read: only its file names are checked
        Path table = node.data.resolve("system_auth").resolve("role_permissions-3afbe79f219431a7add7f5ab90d8ec9c");
        Files.write(table.resolve("mc-800-big-Data.db"), new byte[0]);
        Map<String, String> before = node.fingerprint();
        DsePreflightTest.Run run = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(node.fingerprint()).isEqualTo(before);
        assertThat(run.codes(Finding.Severity.BLOCKING)).containsExactly("SSTABLE_FORMAT");
        assertThat(run.messages("SSTABLE_FORMAT"))
        .isEqualTo(Dse51NodeInspector.SSTABLE_BIG_OLDER + ": system_auth.role_permissions: 1 (mc-800-big-Data.db)");
        assertThat(run.findings("SSTABLE_FORMAT").get(0).remedy)
        .startsWith("run nodetool upgradesstables on DSE 5.1 first");
        assertThat(run.messages("SSTABLE_WITHOUT_TOC")).contains("mc-800-big-Data.db");
        assertThat(run.report.upgradePath.upgradeSteps)
        .contains("Sstable formats: run nodetool upgradesstables on DSE 5.1 first");
    }

    @Test
    public void aSecondNativePortBlocks() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.append(NodeLayout.CASSANDRA_YAML, "native_transport_port_ssl: 9142");
        DsePreflightTest.Run run = assertBlocked(run(node), "NATIVE_PORTS");
        assertThat(run.messages("NATIVE_PORTS"))
        .startsWith("DSE's cassandra.yaml sets native_transport_port 9042 and native_transport_port_ssl 9142");
        assertThat(run.report.dse51.nativeTransportPortSsl).isEqualTo("9142");
    }

    @Test
    public void systemInfoEncryptionBlocks() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.replace(NodeLayout.DSE_YAML, "system_info_encryption:\n    enabled: false",
                     "system_info_encryption:\n    enabled: true");
        DsePreflightTest.Run run = assertBlocked(run(node), "TDE_TABLE");
        assertThat(run.messages("TDE_TABLE")).contains("encrypts the tables system.batches and system.paxos, its " +
                                                       "commit log, its saved caches and its hints files");
        assertThat(run.findings("TDE_TABLE").get(0).remedy)
        .contains("nodetool upgradesstables -a system batches paxos on every node");
    }

    @Test
    public void theTlsSettingsAreReported() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.replace(NodeLayout.CASSANDRA_YAML, "    internode_encryption: none",
                     "    internode_encryption: all\n    protocol: TLSv1.2");
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(run.messages("TLS")).startsWith("DSE's cassandra.yaml sets internode_encryption: all, with the " +
                                                   "TLS settings {server_encryption_options.protocol=TLSv1.2}");
        assertThat(run.report.dse51.tls).containsEntry("server_encryption_options.protocol", "TLSv1.2");
    }

    /** The files HCD 2.0 writes in DSE_5_1 mode are reported by their names; DSE 6's are not DSE 5.1's. */
    @Test
    public void theFilesOfAnEarlierRunOfHcdAreReportedByTheirNames() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        // an empty record of DSE 6 peers refuses no start
        Files.write(node.metadata.resolve(DsePeersRecord.FILE_NAME), "# peers\n".getBytes(StandardCharsets.UTF_8));
        assertThat(assertReady(run(node)).findings("DSE6_UPGRADE_FILES")).isEmpty();
        // DSE 6's marker, and the record listing a peer, refuse a start in DSE_5_1 mode
        Files.write(node.metadata.resolve(DseUpgradeMarker.FILE_NAME), new byte[0]);
        Files.write(node.metadata.resolve(DsePeersRecord.FILE_NAME),
                    "# peers\n10.0.0.2:7000\n".getBytes(StandardCharsets.UTF_8));
        DsePreflightTest.Run dse6Files = assertBlocked(run(node), "DSE6_UPGRADE_FILES");
        assertThat(dse6Files.report.findings)
        .noneMatch(f -> List.of("MIXED_PHASE_MARKER", "NODE_RAN_HCD", "DSE51_SYSTEM_COLUMNS").contains(f.code));
        assertThat(dse6Files.messages("DSE6_UPGRADE_FILES"))
        .contains("holds the DSE 6 upgrade marker dse-upgrade-in-progress and the record of DSE 6 peers " +
                  "dse-peers-seen listing 1 peer, files of the upgrade from DSE 6")
        .endsWith("HCD 2.0 refuses to start on this node in DSE_5_1 mode while they are there");
        Files.delete(node.metadata.resolve(DseUpgradeMarker.FILE_NAME));
        Files.delete(node.metadata.resolve(DsePeersRecord.FILE_NAME));

        Files.write(node.metadata.resolve(DseUpgradeMarker.DSE_51_FILE_NAME), new byte[0]);
        Files.write(node.metadata.resolve(DsePeersRecord.DSE_51_FILE_NAME), new byte[0]);
        String record = Dse51SystemTables.RECORD_FILE_NAME;
        for (String name : List.of(record, record + Dse51SystemTables.TEMPORARY_SUFFIX,
                                   record + Dse51SystemTables.UNREADABLE_SUFFIX + "-1793958236846"))
            Files.write(node.metadata.resolve(name), new byte[0]);
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(run.messages("MIXED_PHASE_MARKER"))
        .startsWith("the DSE 5.1 upgrade marker " + node.metadata.resolve("dse51-upgrade-in-progress") + " exists " +
                    "(DSE 5.1 has no metadata directory: " + node.metadata + " is the one read for HCD 2.0's files, " +
                    "given by --metadata-dir)");
        assertThat(run.findings("DSE6_UPGRADE_FILES")).isEmpty();
        assertThat(run.messages("NODE_RAN_HCD")).contains("holds dse51-peers-seen");
        assertThat(run.messages("DSE51_SYSTEM_COLUMNS"))
        .contains("holds dse51-system-columns, dse51-system-columns.tmp, " +
                  "dse51-system-columns.unreadable-1793958236846: ");
    }

    /** A metadata directory that can be listed but not searched: DSE 6's files may be there, as the start says. */
    @Test
    public void theFilesOfDse6ThatMayBeThereBlock() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Files.write(node.metadata.resolve(DseUpgradeMarker.FILE_NAME), new byte[0]);
        Files.setPosixFilePermissions(node.metadata, PosixFilePermissions.fromString("r--------"));
        try
        {
            Assume.assumeFalse("the permissions do not apply to this user", Files.isExecutable(node.metadata));
            DsePreflightTest.Run run = assertBlocked(run(node), "DSE6_UPGRADE_FILES");
            assertThat(run.messages("DSE6_UPGRADE_FILES"))
            .contains("holds the DSE 6 upgrade marker dse-upgrade-in-progress (or may hold it: its presence cannot " +
                      "be checked) and the record of DSE 6 peers dse-peers-seen (or may hold it: its presence " +
                      "cannot be checked), files of the upgrade from DSE 6");
        }
        finally
        {
            Files.setPosixFilePermissions(node.metadata, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void theLiveStateOfADse51NodeIsChecked() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        Dse51NodeFixture node = thriftOff();
        Path arguments = node.root.resolve("nodetool-arguments");
        Path nodetool = node.root.resolve("nodetool");
        // statusthrift before status, which its pattern would match too
        String body = "case \"$*\" in\n" +
                      "  *statusthrift*) echo 'running' ;;\n" +
                      "  *status*) printf 'Datacenter: Cassandra\\n--  Address  Load\\nUN  127.0.51.11  1 MiB\\n" +
                      "DN  127.0.51.13  1 MiB\\n' ;;\n" +
                      "  *version*) echo 'ReleaseVersion: 3.11.3.5151' ;;\n" +
                      "  *listendpointspendinghints*) printf 'Host ID  Address  Rack  DC  Status  Total hints\\n" +
                      "x  127.0.51.13  rack1  Cassandra  DOWN  203\\n' ;;\n" +
                      "  *netstats*) echo 'Mode: NORMAL'; echo 'Not sending any streams.' ;;\n" +
                      "esac\n";
        String script = "#!/bin/sh\necho \"$*\" >> '" + arguments + "'\n" + body + "exit 0\n";
        Files.write(nodetool, script.getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(nodetool, PosixFilePermissions.fromString("rwx------"));
        DsePreflightTest.Run run = assertReady(run(node, "--nodetool", nodetool.toString()));
        assertThat(run.report.live).containsOnlyKeys(LiveState.DSE51_COMMANDS.toArray(new String[0]));
        assertThat(Files.readAllLines(arguments)).contains("statusthrift", "listendpointspendinghints")
                                                 .doesNotContain("nodesyncservice status");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("LIVE_STATUS", "LIVE_HINTS", "THRIFT")
                                                       .doesNotContain("LIVE_STREAMS", "LIVE_UNAVAILABLE");
        assertThat(run.report.dse51.thriftLive).isEqualTo("running");
        assertThat(run.messages("THRIFT"))
        .isEqualTo("Thrift is off when DSE starts (start_rpc: false in DSE's cassandra.yaml); nodetool statusthrift " +
                   "says running: it runs now, started since by nodetool enablethrift or JMX, and Thrift clients " +
                   "connected to it break when the node is upgraded");
        assertThat(run.findings("THRIFT").get(0).remedy)
        .endsWith("then run nodetool disablethrift on this node before it is upgraded");
    }

    /**
     * A big sstable whose Summary.db cannot be loaded gets it deleted when it is opened in place: the scratch copy is
     * what is opened, so the node's files stay as they are, and the rows are read all the same, without its TOC.txt
     * too.
     */
    @Test
    public void aBigSSTableWithADamagedSummaryIsReadFromItsScratchCopy() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Path local = node.data.resolve("system").resolve("local-7ad54392bcdd35a684174e047860b377");
        Files.write(local.resolve("me-29-big-Summary.db"), new byte[]{ 0, 0, 0, 1 });
        Files.delete(local.resolve("me-29-big-TOC.txt"));
        Map<String, String> before = node.fingerprint();
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(node.fingerprint()).isEqualTo(before);
        assertThat(run.report.node.hostId).isEqualTo(Dse51Fixtures.HOST_IDS.get(0).toString());
        assertThat(run.messages("SSTABLE_WITHOUT_TOC")).contains("system.local: 1 (me-29-big-Data.db)");
    }

    /** The other traces of the start check, each through the tool: a search index, a graph, Analytics, solr.data. */
    @Test
    public void theOtherTracesAreReportedThroughTheMarkers() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Map<String, String> simple = Map.of("class", "org.apache.cassandra.locator.SimpleStrategy",
                                            "replication_factor", "1");
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("dse_analytics").with("replication", simple), row("g").with("replication", simple),
                       row("g_system").with("replication", simple), row("g_pvt").with("replication", simple));
        String solr = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex";
        node.writeRows("system_schema", "indexes", KEYSPACES_KEY, List.of("table_name", "index_name"),
                       row("test_ks", "t", "solr_idx").with("options", Map.of("class_name", solr, "target", "x")));
        Files.createDirectories(node.data.resolve("solr.data"));
        DsePreflightTest.Run run = assertBlocked(run(node), "SEARCH_INDEX");
        assertThat(run.messages("SEARCH_INDEX"))
        .startsWith("DSE Search: index test_ks.t.solr_idx (" + solr + "), directory " + node.data.resolve("solr.data"));
        assertThat(run.messages("GRAPH_KEYSPACE")).startsWith("DSE Graph: keyspace g (a keyspace of the graph g), " +
                                                              "keyspace g_pvt (a keyspace of the graph g), keyspace " +
                                                              "g_system (a keyspace of the graph g)");
        assertThat(run.messages("NON_CORE_KEYSPACE"))
        .startsWith("DSE Analytics, DSEFS and CFS: keyspace dse_analytics");
    }

    @Test
    public void thePeersOfOtherReleasesAreReported() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        String release = FBUtilities.getReleaseVersionString();
        node.writeRows("system", "peers", List.of("peer"), List.of(),
                       row(InetAddress.getByName("127.0.51.12")).with("release_version", "3.11.3.5150")
                                                                .with("dse_version", "5.1.50"),
                       row(InetAddress.getByName("127.0.51.13")).with("release_version", "4.0.0.6926")
                                                                .with("dse_version", "6.9.26"),
                       row(InetAddress.getByName("127.0.51.14")).with("release_version", release));
        DsePreflightTest.Run run = assertBlocked(run(node), "MIXED_RELEASES");
        assertThat(run.messages("MIXED_RELEASES"))
        .isEqualTo("peers (system.peers) run another DSE 5.1 release than DSE 5.1.51: 127.0.51.12 (release " +
                   "3.11.3.5150, DSE 5.1.50)");
        assertThat(run.messages("PEER_RELEASE"))
        .startsWith("peers recorded with a release that is neither DSE 5.1 nor HCD 2.0: 127.0.51.13 (release " +
                    "4.0.0.6926, DSE 6.9.26)");
        assertThat(run.messages("UPGRADED_PEERS")).contains("127.0.51.14 (release " + release + ')');
        assertThat(run.report.peers).filteredOn(p -> p.upgraded)
                                    .extracting(p -> p.address)
                                    .containsExactly("127.0.51.14");
    }

    @Test
    public void whatCannotBeReadIsReported() throws IOException
    {
        // no system.local: not a DSE node as far as the tool can tell
        Dse51NodeFixture noLocal = thriftOff();
        Path local = noLocal.data.resolve("system").resolve("local-7ad54392bcdd35a684174e047860b377");
        new File(local).deleteRecursive();
        DsePreflightTest.Run run = assertBlocked(run(noLocal), "NOT_A_DSE_NODE");
        assertThat(run.messages("NOT_A_DSE_NODE"))
        .endsWith("(no sstable of the table): the DSE release, identity and workloads of this node cannot be read");
        assertThat(run.findings("NOT_A_DSE_NODE").get(0).remedy).startsWith("point the tool at DSE's data directories");

        // no system_schema.keyspaces: the traces cannot all be checked
        Dse51NodeFixture noKeyspaces = thriftOff();
        new File(noKeyspaces.data.resolve("system_schema").resolve("keyspaces-abac5682dea631c5b535b3d6cffd0fb6"))
        .deleteRecursive();
        DsePreflightTest.Run schema = assertBlocked(run(noKeyspaces), "UNREADABLE_SCHEMA");
        assertThat(schema.messages("UNREADABLE_SCHEMA"))
        .startsWith("cannot read system_schema.keyspaces (no sstable of the table), so the traces");
        assertThat(schema.report.upgradePath.upgradeSteps)
        .contains("DSE core only: not checked completely (not read: system_schema.keyspaces (no sstable of the " +
                  "table))");

        // a trie sstable in system.local, which DSE 5.1 does not write, is not read
        Dse51NodeFixture trie = thriftOff();
        Path trieLocal = trie.data.resolve("system").resolve("local-7ad54392bcdd35a684174e047860b377");
        Files.write(trieLocal.resolve("bb-900-bti-Data.db"), new byte[0]);
        Map<String, String> before = trie.fingerprint();
        DsePreflightTest.Run refused = assertBlocked(run(trie), "UNREADABLE_INPUT");
        assertThat(trie.fingerprint()).isEqualTo(before);
        assertThat(refused.messages("UNREADABLE_INPUT"))
        .contains("cannot read system.local: IOException: " + trieLocal.resolve("bb-900-bti") + " is in the bti " +
                  "format, which DSE 5.1 does not write");
        assertThat(refused.messages("SSTABLE_FORMAT")).contains(Dse51NodeInspector.SSTABLE_TRIE);
    }

    @Test
    public void theJvmOptionsAreReadAsTheStartScriptReadsThem() throws IOException
    {
        // an indented line is not an option of the start script, and a line holds as many options as words
        Dse51NodeFixture node = thriftOff();
        node.append(NodeLayout.JVM_OPTIONS, "  -Dcassandra.start_rpc=true",
                    "-Dcassandra.force_3_0_protocol_version=false -Denable-old-dse-state=false");
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(run.messages("THRIFT")).startsWith("Thrift is off when DSE starts (start_rpc: false in DSE's " +
                                                      "cassandra.yaml)");
        assertThat(run.messages("JVM_OPTIONS_READ"))
        .endsWith("; -Dcassandra.force_3_0_protocol_version=false and -Denable-old-dse-state=false are set, which " +
                  "have no effect (only true has one)");

        // the JVM options given on the command line
        Dse51NodeFixture other = thriftOff();
        Path elsewhere = other.root.resolve("jvm.options");
        Files.move(other.conf.resolve(NodeLayout.JVM_OPTIONS), elsewhere);
        Files.write(elsewhere, "-Dcassandra.start_rpc=true\n".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);
        DsePreflightTest.Run given = assertBlocked(run(other, "--jvm-options", elsewhere.toString()), "THRIFT");
        assertThat(given.messages("THRIFT")).startsWith("Thrift is on when DSE starts (-Dcassandra.start_rpc=true in " +
                                                        elsewhere + ')');
        assertThat(given.report.inputs).contains(entry("jvm_options", elsewhere.toString()));
    }

    /** The checks the two inspections share: DSE's authentication schemes and a DSE Search data directory. */
    @Test
    public void theChecksSharedWithDse6Apply() throws IOException
    {
        Dse51NodeFixture ldap = thriftOff();
        ldap.replace(NodeLayout.DSE_YAML, "    default_scheme: internal", "    default_scheme: ldap");
        DsePreflightTest.Run run = assertBlocked(run(ldap), "AUTH_SCHEME");
        assertThat(run.messages("AUTH_SCHEME")).contains("authentication_options in dse.yaml uses the schemes [ldap]");

        Dse51NodeFixture solr = thriftOff();
        Path directory = Files.createDirectories(solr.root.resolve("solr"));
        solr.append(NodeLayout.JVM_OPTIONS, "-Ddse.solr.data.dir=" + directory);
        DsePreflightTest.Run search = assertBlocked(run(solr), "SOLR_DATA");
        assertThat(search.messages("SOLR_DATA")).isEqualTo("the DSE Search data directory " + directory + " " +
                                                           "(-Ddse.solr.data.dir in DSE's JVM options) exists");
    }

    /** The aggregate of this release does not check the reports of DSE 5.1 nodes, and says so. */
    @Test
    public void theAggregateRefusesTheReportsOfDse51Nodes() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Path reports = temporaryFolder.newFolder("reports").toPath();
        assertReady(DsePreflightTest.run(reports.resolve("node1.json"), node.args()));
        ClusterAggregate.AggregateReport aggregate = ClusterAggregate.aggregate(reports);
        assertThat(aggregate.result).isEqualTo(NodeReport.BLOCKED);
        assertThat(aggregate.findings).anyMatch(f -> f.code.equals("DSE51_REPORT") &&
                                                     f.severity == Finding.Severity.BLOCKING &&
                                                     f.message.endsWith(": node1.json"));
        assertThat(aggregate.findings).noneMatch(f -> f.code.equals("NO_REPORT"));
    }

    /** A header of a DSE 5.1 hints file, laid out as {@code HintsDescriptor} writes it, with its parameters. */
    private static byte[] hintsHeader(UUID hostId, long timestamp, Map<String, Object> parameters) throws IOException
    {
        byte[] json = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsBytes(parameters);
        CRC32 crc = new CRC32();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes))
        {
            out.writeInt(Dse51NodeInspector.HINTS_VERSION);
            FBUtilities.updateChecksumInt(crc, Dse51NodeInspector.HINTS_VERSION);
            long[] values = { timestamp, hostId.getMostSignificantBits(), hostId.getLeastSignificantBits() };
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
        return bytes.toByteArray();
    }

    private static Map<String, Object> compression(String className)
    {
        return Map.of("compression", Map.of("class_name", className, "parameters", Map.of()));
    }

    @Test
    public void theHintsFilesAreChecked() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        UUID node3 = Dse51Fixtures.HOST_IDS.get(2);
        String encrypting = "org.apache.cassandra.io.compress.EncryptingLZ4Compressor";
        Files.write(node.hints.resolve(node3 + "-1793958749100-1.hints"), hintsHeader(node3, 1793958749100L,
                                                                                      compression(encrypting)));
        Files.write(node.hints.resolve(node3 + "-1793958749200-2.hints"), new byte[0]);
        Files.write(node.hints.resolve(node3 + "-1793958749300-1.hints"), new byte[]{ 1, 2, 3 });
        // named before the others of their kind: the findings list the files by name, whatever the listing order
        Files.write(node.hints.resolve(node3 + "-1793958749150-2.hints"), new byte[0]);
        Files.write(node.hints.resolve(node3 + "-1793958749250-1.hints"), new byte[]{ 4, 5, 6 });
        UUID unknown = UUID.fromString("00000000-0000-4000-8000-000000000001");
        Files.write(node.hints.resolve(unknown + "-1793958749400-1.hints"), new byte[0]);
        DsePreflightTest.Run run = assertBlocked(run(node), "HINTS_COMPRESSION");
        assertThat(run.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE 5.1 hints files whose header records a compression, an encryption or another parameter: an " +
                   "encryption (system_info_encryption): " + node3 + "-1793958749100-1.hints (compression); HCD 2.0 " +
                   "has neither DSE's encrypting compressors nor its keys");
        assertThat(run.findings("HINTS_COMPRESSION").get(0).remedy)
        .startsWith("set enabled: false under system_info_encryption in dse.yaml, and restart DSE on this node");
        assertBlocked(run, "NON_DSE_HINTS");
        assertThat(run.messages("NON_DSE_HINTS")).contains(node3 + "-1793958749150-2.hints, " + node3 +
                                                           "-1793958749200-2.hints");
        assertThat(run.messages("HINTS_UNREADABLE")).containsPattern(node3 + "-1793958749250-1.hints \\([^)]*\\), " +
                                                                     node3 + "-1793958749300-1.hints \\(");
        assertThat(run.messages("PENDING_HINTS"))
        .contains("DSE 5.1 hints files for host ids neither in system.local nor in system.peers: " + unknown +
                  ": 1 file(s)");
        assertThat(run.report.upgradePath.upgradeSteps)
        .anyMatch(step -> step.startsWith("Hints: 1 target(s) with DSE 5.1 hints files pending; 1 host id(s) of no " +
                                          "node recorded with DSE 5.1 hints files"));

        assertThat(Dse51NodeInspector.hintsHeader(compression("EncryptingSnappyCompressor")))
        .containsExactly(Dse51NodeInspector.HintsHeader.ENCRYPTED_BY_SYSTEM_INFO);
        assertThat(Dse51NodeInspector.hintsHeader(compression("com.example.NoSuchCompressor")))
        .containsExactly(Dse51NodeInspector.HintsHeader.UNKNOWN_COMPRESSOR);
        assertThat(Dse51NodeInspector.hintsHeader(Map.of("encryption", Map.of())))
        .containsExactly(Dse51NodeInspector.HintsHeader.ENCRYPTED_BY_TDE);
        assertThat(Dse51NodeInspector.hintsHeader(compression("LZ4Compressor")))
        .containsExactly(Dse51NodeInspector.HintsHeader.COMPRESSED);
        assertThat(Dse51NodeInspector.hintsHeader(Map.of())).containsExactly(Dse51NodeInspector.HintsHeader.PLAIN);
        assertThat(Dse51NodeInspector.hintsHeader(Map.of("other", "x")))
        .containsExactly(Dse51NodeInspector.HintsHeader.UNKNOWN_PARAMETER);
        // a class that is no compressor
        assertThat(Dse51NodeInspector.hintsHeader(compression("java.lang.String")))
        .containsExactly(Dse51NodeInspector.HintsHeader.UNKNOWN_COMPRESSOR);
        // another parameter beside a compression: both
        Map<String, Object> mixed = Map.of("compression", Map.of("class_name", "LZ4Compressor"), "other", "x");
        assertThat(Dse51NodeInspector.hintsHeader(mixed))
        .containsExactlyInAnyOrder(Dse51NodeInspector.HintsHeader.COMPRESSED,
                                   Dse51NodeInspector.HintsHeader.UNKNOWN_PARAMETER);

        // encrypted by transparent_data_encryption_options: its setting is named
        Dse51NodeFixture tde = thriftOff();
        // the section as Apache Cassandra 3.11's hints descriptor writes it, its IV left out of every text
        String iv = "00112233445566778899aabbccddeeff";
        Map<String, Object> section = Map.of(EncryptionContext.ENCRYPTION_CIPHER, "AES/CBC/PKCS5Padding",
                                             EncryptionContext.ENCRYPTION_KEY_ALIAS, "testing:1",
                                             EncryptionContext.ENCRYPTION_IV, iv);
        Files.write(tde.hints.resolve(node3 + "-1793958749100-1.hints"),
                    hintsHeader(node3, 1793958749100L, Map.of("encryption", section)));
        Path tdeJson = temporaryFolder.getRoot().toPath().resolve("tde.json");
        DsePreflightTest.Run encrypted = assertBlocked(DsePreflightTest.run(tdeJson, tde.args()), "HINTS_COMPRESSION");
        String json = new String(Files.readAllBytes(tdeJson), StandardCharsets.UTF_8);
        for (String text : List.of(encrypted.out, encrypted.err, json))
            assertThat(text).doesNotContain(iv).doesNotContain("testing:1");
        assertThat(encrypted.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE 5.1 hints files whose header records a compression, an encryption or another parameter: an " +
                   "encryption (transparent_data_encryption_options): " + node3 + "-1793958749100-1.hints " +
                   "(encryption); HCD 2.0 has neither DSE's encrypting compressors nor its keys");
        assertThat(encrypted.findings("HINTS_COMPRESSION").get(0).remedy)
        .startsWith("set enabled: false under transparent_data_encryption_options in DSE's cassandra.yaml, and " +
                    "restart DSE on this node");

        // a compressor HCD 2.0 does not have: what wrote it is to be found out, no setting is named
        Dse51NodeFixture unknownClass = thriftOff();
        Files.write(unknownClass.hints.resolve(node3 + "-1793958749100-1.hints"),
                    hintsHeader(node3, 1793958749100L, compression("com.example.NoSuchCompressor")));
        DsePreflightTest.Run other = assertBlocked(run(unknownClass), "HINTS_COMPRESSION");
        assertThat(other.messages("HINTS_COMPRESSION")).contains("a compressor HCD 2.0 does not have: ");
        assertThat(other.findings("HINTS_COMPRESSION").get(0).remedy)
        .startsWith("find out what wrote the files with a compressor or a parameter HCD 2.0 does not know; " +
                    "restart DSE");

        // a header parameter HCD 2.0 does not know, alone: the tool cannot tell what it does, and blocks
        Dse51NodeFixture unknownKey = thriftOff();
        Files.write(unknownKey.hints.resolve(node3 + "-1793958749100-1.hints"),
                    hintsHeader(node3, 1793958749100L, Map.of("other", "x")));
        DsePreflightTest.Run key = assertBlocked(run(unknownKey), "HINTS_COMPRESSION");
        assertThat(key.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE 5.1 hints files whose header records a compression, an encryption or another parameter: a " +
                   "header parameter HCD 2.0 does not know: " + node3 + "-1793958749100-1.hints (other); what a " +
                   "compressor or a parameter HCD 2.0 does not know does to a file is not known, so the file is not " +
                   "taken for a plain one");
        assertThat(key.findings("HINTS_COMPRESSION").get(0).remedy)
        .startsWith("find out what wrote the files with a compressor or a parameter HCD 2.0 does not know; restart " +
                    "DSE on this node, so that the hints files it writes from then on record none of these");

        // compressed with a compressor of Apache Cassandra: blocking too, with hints_compression
        Dse51NodeFixture compressed = thriftOff();
        Files.write(compressed.hints.resolve(node3 + "-1793958749100-1.hints"),
                    hintsHeader(node3, 1793958749100L, compression("org.apache.cassandra.io.compress.LZ4Compressor")));
        compressed.append(NodeLayout.CASSANDRA_YAML, "hints_compression:", "  - class_name: LZ4Compressor",
                          "    parameters:", "      -");
        DsePreflightTest.Run lz4 = assertBlocked(run(compressed), "HINTS_COMPRESSION");
        assertThat(lz4.messages("HINTS_COMPRESSION"))
        .contains("DSE's cassandra.yaml sets hints_compression (class_name LZ4Compressor)")
        .contains("DSE 5.1 hints files whose header records a compression, an encryption or another parameter: a " +
                  "compression (hints_compression): " + node3 + "-1793958749100-1.hints (compression); HCD 2.0 was " +
                  "not validated with compressed DSE 5.1 hints files")
        .doesNotContain("encrypting compressors");

        // a compression with another parameter beside it: both reasons, and both remedies
        Dse51NodeFixture mixedNode = thriftOff();
        Map<String, Object> parameters = Map.of("compression", Map.of("class_name", "LZ4Compressor", "parameters",
                                                                      Map.of()),
                                                "other", "x");
        Files.write(mixedNode.hints.resolve(node3 + "-1793958749100-1.hints"),
                    hintsHeader(node3, 1793958749100L, parameters));
        DsePreflightTest.Run both = assertBlocked(run(mixedNode), "HINTS_COMPRESSION");
        String file = node3 + "-1793958749100-1.hints (compression, other)";
        assertThat(both.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE 5.1 hints files whose header records a compression, an encryption or another parameter: a " +
                   "header parameter HCD 2.0 does not know: " + file + "; a compression (hints_compression): " + file +
                   "; HCD 2.0 was not validated with compressed DSE 5.1 hints files, and what a compressor or a " +
                   "parameter HCD 2.0 does not know does to a file is not known, so the file is not taken for a " +
                   "plain one");
        assertThat(both.findings("HINTS_COMPRESSION").get(0).remedy)
        .startsWith("find out what wrote the files with a compressor or a parameter HCD 2.0 does not know; remove " +
                    "hints_compression from DSE's cassandra.yaml, and restart DSE on this node");
    }

    @Test
    public void theCommitLogSegmentsAreChecked() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Files.write(node.commitLog.resolve("CommitLog-7-100.log"), new byte[0]);
        Files.write(node.commitLog.resolve("CommitLog-6-99.log.replayed"), new byte[0]);
        DsePreflightTest.Run run = assertBlocked(run(node), "COMMITLOG_VERSION");
        assertThat(run.codes(Finding.Severity.BLOCKING)).containsExactly("COMMITLOG_VERSION");
        assertThat(run.messages("COMMITLOG_VERSION")).startsWith("commit log segments of versions other than DSE " +
                                                                 "5.1's 6 (version: count) {7=1}");
        assertThat(run.messages("COMMITLOG")).contains("already replayed by HCD 2.0")
                                             .contains("CommitLog-6-99.log.replayed");

        // a version beyond an int, alone and beside another version
        Files.write(node.commitLog.resolve("CommitLog-99999999999-1.log"), new byte[0]);
        DsePreflightTest.Run both = assertBlocked(run(node), "COMMITLOG_VERSION");
        assertThat(both.messages("COMMITLOG_VERSION"))
        .isEqualTo("commit log segments of versions other than DSE 5.1's 6 (version: count) {7=1}; commit log " +
                   "segments whose version cannot be read: CommitLog-99999999999-1.log; DSE 5.1 refuses to replay " +
                   "them (written by another software)");
        assertThat(both.report.inputs.get("commitlog_segments_version_unreadable"))
        .isEqualTo(List.of("CommitLog-99999999999-1.log"));
        Files.delete(node.commitLog.resolve("CommitLog-7-100.log"));
        DsePreflightTest.Run alone = assertBlocked(run(node), "COMMITLOG_VERSION");
        assertThat(alone.messages("COMMITLOG_VERSION"))
        .isEqualTo("commit log segments whose version cannot be read: CommitLog-99999999999-1.log; DSE 5.1 refuses " +
                   "to replay them (written by another software)");
    }

    @Test
    public void theIdentityOfTheNodeIsChecked() throws IOException
    {
        // GossipingPropertyFileSnitch with another data center than system.local records, another cluster name,
        // another number of tokens
        Dse51NodeFixture node = thriftOff();
        node.replace(NodeLayout.CASSANDRA_YAML, "endpoint_snitch: com.datastax.bdp.snitch.DseSimpleSnitch",
                     "endpoint_snitch: GossipingPropertyFileSnitch");
        node.replace(NodeLayout.CASSANDRA_YAML, "cluster_name: 'Test Cluster'", "cluster_name: 'Other Cluster'");
        node.append(NodeLayout.CASSANDRA_YAML, "num_tokens: 4");
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(run.messages("IDENTITY_MISMATCH"))
        .contains("cassandra-rackdc.properties says dc=dc1, rack=rack1 but system.local records Cassandra/rack1")
        .contains("DSE's cassandra.yaml says cluster_name Other Cluster but system.local records Test Cluster")
        .contains("DSE's cassandra.yaml says num_tokens 4 but system.local records 1 tokens for this node");
        assertThat(run.findings("SNITCH")).isEmpty();
        assertThat(run.report.upgradePath.settings).containsEntry("endpoint_snitch", "GossipingPropertyFileSnitch");

        Dse51NodeFixture ec2 = thriftOff();
        ec2.replace(NodeLayout.CASSANDRA_YAML, "endpoint_snitch: com.datastax.bdp.snitch.DseSimpleSnitch",
                    "endpoint_snitch: Ec2Snitch");
        DsePreflightTest.Run legacy = assertReady(run(ec2));
        assertThat(legacy.messages("EC2_NAMING")).startsWith("DSE 5.1 derives the data center and rack of Ec2Snitch " +
                                                             "with the legacy EC2 names only");
        assertThat(legacy.report.upgradePath.settings).containsEntry("cassandra-rackdc.properties",
                                                                     "ec2_naming_scheme=legacy");
    }

    @Test
    public void theJavaAndTheInstallationAreReported() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        Files.delete(node.dseHome.resolve("ds_version.txt"));
        DsePreflightTest.Run run = assertReady(run(node));
        assertThat(run.messages("INSTALLED_RELEASE_UNKNOWN")).isEqualTo("no ds_version.txt in " + node.dseHome + ", " +
                                                                        "so the installed DSE release is not " +
                                                                        "compared with the running one");
        String feature = Integer.toString(Runtime.version().feature());
        assertThat(run.report.dse51.toolJavaVersion).isEqualTo(feature);
        assertThat(run.messages("JAVA")).startsWith("this tool runs on Java " + feature);
        Assume.assumeTrue("runs on JDK 17", Runtime.version().feature() == Dse51NodeInspector.HCD_20_JAVA);
        assertThat(run.findings("JAVA")).extracting(f -> f.severity).containsExactly(Finding.Severity.INFO);
        assertThat(run.messages("JAVA")).startsWith("this tool runs on Java 17, the JDK HCD 2.0 is to run on; DSE " +
                                                    "5.1.51 runs on Java 8 update 40 or later only");
    }

    @Test
    public void theEncryptedCommitLogAndTheFeaturesOfDseYamlBlock() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        node.replace(NodeLayout.CASSANDRA_YAML, "transparent_data_encryption_options:\n    enabled: false",
                     "transparent_data_encryption_options:\n    enabled: true");
        node.append(NodeLayout.DSE_YAML, "dsefs_options:", "    enabled: true");
        DsePreflightTest.Run run = assertBlocked(run(node), "TDE_COMMITLOG");
        assertThat(run.messages("TDE_COMMITLOG")).contains("DSE 5.1 encrypts the commit log and the hints files it " +
                                                           "writes");
        assertBlocked(run, "DSEFS");
        assertThat(run.report.upgradePath.upgradeSteps)
        .anyMatch(step -> step.startsWith("DSE core only: remove the traces listed above"));
    }

    @Test
    public void noPendingHintIsQuiet() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        Dse51NodeFixture node = thriftOff();
        Path nodetool = node.root.resolve("nodetool");
        String script = "#!/bin/sh\ncase \"$*\" in\n" +
                        "  *listendpointspendinghints*) echo 'This node does not have hints for other endpoints' ;;\n" +
                        "  *statusthrift*) echo 'not running' ;;\n" +
                        "esac\nexit 0\n";
        Files.write(nodetool, script.getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(nodetool, PosixFilePermissions.fromString("rwx------"));
        DsePreflightTest.Run run = assertReady(run(node, "--nodetool", nodetool.toString()));
        assertThat(run.findings("LIVE_HINTS")).isEmpty();
        assertThat(run.findings("THRIFT")).extracting(f -> f.severity).containsExactly(Finding.Severity.INFO);
    }

    @Test
    public void theJvmOptionsDoNotApplyToThePaxosRollback() throws IOException
    {
        Dse51NodeFixture node = thriftOff();
        DsePreflightTest.Run run = DsePreflightTest.run(node.root.resolve("paxos.json"), "--paxos-rollback",
                                                        "--jvm-options", node.conf.resolve("jvm.options").toString());
        assertThat(run.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(run.err).contains("--jvm-options does not apply to --paxos-rollback");
    }

    /**
     * The rows this JVM wrote (a client in the DSE 5.1 mode) read by the tool as the operator runs it, through its
     * launcher in a JVM of its own without the mode.
     */
    @Test
    public void theLauncherReadsTheRowsWrittenHere() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        Dse51NodeFixture node = thriftOff();
        node.writeRows("system_schema", "tables", KEYSPACES_KEY, List.of("table_name"),
                       row("test_ks", "dtcs").with("compaction", Map.of("class", DTCS)));
        Path json = temporaryFolder.getRoot().toPath().resolve("launcher.json");
        List<String> command = new ArrayList<>(List.of("tools/bin/dsepreflight"));
        command.addAll(List.of(node.args("--json", json.toString())));
        ToolRunner.ToolResult tool = ToolRunner.invoke(command);
        assertThat(tool.getExitCode()).as("%s%s", tool.getStdout(), tool.getStderr())
                                      .isEqualTo(DsePreflight.EXIT_BLOCKED);
        NodeReport report = ClusterAggregate.READER.readValue(Files.readAllBytes(json));
        assertThat(report.findings).filteredOn(f -> f.severity == Finding.Severity.BLOCKING)
                                   .extracting(f -> f.code)
                                   .containsExactly("REMOVED_COMPACTION");
    }

    /**
     * A flush of DSE 5.1 in progress, its transaction log listing a new sstable: the sstable is not read, and the log
     * is no finding, in this JVM and through the launcher, in a JVM without the mode; the node's files stay as they
     * are.
     */
    @Test
    public void aTransactionInProgressIsLeftOut() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        Dse51NodeFixture node = thriftOff();
        DseNodeFixture.SchemaRow dtcs = row("test_ks", "dtcs").with("compaction", Map.of("class", DTCS));
        // closed unfinished, the writer aborts its transaction
        try (SSTableTxnWriter ignored = node.startWriting("system_schema", "tables", KEYSPACES_KEY,
                                                          List.of("table_name"), dtcs))
        {
            Path tables = node.data.resolve("system_schema").resolve("tables-afddfb9dbc1e30688056eed6c302ba09");
            try (Stream<Path> files = Files.list(tables))
            {
                assertThat(files.map(p -> p.getFileName().toString())).anyMatch(n -> n.endsWith(".log"));
            }
            Map<String, String> before = node.fingerprint();
            DsePreflightTest.Run run = assertReady(run(node));
            assertThat(run.findings("REMOVED_COMPACTION")).isEmpty();
            assertThat(run.findings("TXN_LOG_UNREADABLE")).isEmpty();
            assertThat(run.findings("SSTABLE_WITHOUT_TOC")).isEmpty();

            Path json = temporaryFolder.getRoot().toPath().resolve("in-progress.json");
            List<String> command = new ArrayList<>(List.of("tools/bin/dsepreflight"));
            command.addAll(List.of(node.args("--json", json.toString())));
            ToolRunner.ToolResult tool = ToolRunner.invoke(command);
            assertThat(tool.getExitCode()).as("%s%s", tool.getStdout(), tool.getStderr())
                                          .isEqualTo(DsePreflight.EXIT_READY);
            NodeReport report = ClusterAggregate.READER.readValue(Files.readAllBytes(json));
            assertThat(report.findings).noneMatch(f -> List.of("REMOVED_COMPACTION", "TXN_LOG_UNREADABLE",
                                                               "SSTABLE_WITHOUT_TOC").contains(f.code));
            assertThat(node.fingerprint()).isEqualTo(before);
        }
    }
}
