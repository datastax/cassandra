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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.hints.HintsDescriptorBytes;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.tools.dsepreflight.DseNodeFixture.row;
import static org.apache.cassandra.tools.dsepreflight.DseNodeFixture.section;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The preflight inspection run through its command line ({@link DsePreflight#run}) on a DSE 6.8.65 node laid out from
 * the fixtures ({@link DseNodeFixture}): the node as DSE left it is ready to upgrade, and each forbidden feature added to
 * a copy of it makes the tool exit with a blocking finding naming it. The JSON report written with {@code --json} is
 * what the assertions read.
 */
public class DsePreflightTest
{
    private static final List<String> KEYSPACES_KEY = List.of("keyspace_name");
    private static final List<String> TABLES_KEY = List.of("table_name");
    private static final List<String> COLUMNS_KEY = List.of("table_name", "column_name");
    private static final List<String> INDEXES_KEY = List.of("table_name", "index_name");
    private static final List<String> FUNCTIONS_KEY = List.of("function_name", "argument_types");
    private static final List<String> ROLE_KEY = List.of("role");
    private static final List<String> RESOURCE_KEY = List.of("resource");
    private static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    /** The end of every warning INTERNODE_TLS message of a node report: where internode TLS with DSE is validated. */
    private static final String TLS_VALIDATED = "internode TLS between DSE and HCD 2.0 is validated on DSE 6.9.26 with all, " +
                                                "rack and dc, in one data center and between two, and on DSE 6.8.65 with all in " +
                                                "one data center only";
    /** What every INTERNODE_TLS remedy says of HCD 2.0, up to the keys of the fixture's server_encryption_options. */
    private static final String TLS_CARRY = "; on HCD 2.0, carry the keys DSE sets in server_encryption_options into the cassandra.yaml of HCD 2.0";
    /** The message of TDE_TABLE for system_info_encryption enabled in dse.yaml. */
    private static final String SYSTEM_INFO_ENCRYPTION_MESSAGE = "system_info_encryption.enabled is true in dse.yaml: DSE encrypts the tables " +
                                                                 "system.batches, system.paxos, system_backups.backup_configurations and " +
                                                                 "system_backups.backup_stores, its commit log, its saved caches and its " +
                                                                 "hints files; encryption is out of scope of this upgrade";
    /** What the remedies of the encryption findings ask to keep while encrypted data is left. */
    private static final String SYSTEM_KEY_DIRECTORY = "the whole directory that system_key_directory names in dse.yaml (by default " +
                                                       "/etc/dse/conf), its system/system_table_keytab included,";
    /** How the remedies of the encryption findings end: what happens without the keytab. */
    private static final String SYSTEM_KEY_LOSS = "without the original system/system_table_keytab of that directory, DSE cannot read " +
                                                  "what it encrypted before (DSE 6.8.65, started without it, made a new one with no warning, " +
                                                  "failed on its encrypted hints files and, under disk_failure_policy: stop, stopped serving)";
    /** The remedy of TDE_TABLE for system_info_encryption enabled in dse.yaml. */
    private static final String SYSTEM_INFO_ENCRYPTION_REMEDY = "set enabled: false under system_info_encryption in dse.yaml; restart DSE on every " +
                                                                "node, with " + SYSTEM_KEY_DIRECTORY + " still in place, so that " +
                                                                "DSE replays the commit log segments written before and writes its " +
                                                                "commit log, saved caches and hints files " +
                                                                "unencrypted from then on (HCD 2.0 skips a saved cache it cannot read); then run " +
                                                                "nodetool upgradesstables -a system batches paxos and nodetool upgradesstables -a " +
                                                                "system_backups on every node, so that those tables are rewritten unencrypted (this " +
                                                                "tool does not check that the rewrite is done); and keep that directory in place " +
                                                                "until the rewrite is done and, with every node up, DSE has delivered the hints " +
                                                                "files written before, which stay encrypted (this tool names those left, under " +
                                                                "HINTS_COMPRESSION); all of it before the first node of the cluster is upgraded; " +
                                                                SYSTEM_KEY_LOSS;
    /**
     * An id of the project's plan an operator cannot look up, which no text of the tool may hold: a decision ("decision
     * 3i", "decisions 3i and 3l"), a collection answer or question ("A58", "A06.01", "Q48"), a proposed runbook change
     * ("R10", "R-e") or a local run ("L04").
     */
    private static final Pattern PLAN_DECISION_ID = Pattern.compile("(?i:\\bdecisions? [0-9]+[a-z]*\\b)|\\b(A[0-9]{2,3}(\\.[0-9]+)?|Q[0-9]{2,3}|R[0-9]+|R-[a-z]|L[0-9]{2})\\b");
    /** An absolute path, as the texts quote the node's directories and files: a slash at the start of a word. */
    private static final Pattern MACHINE_PATH = Pattern.compile("(?<=^|[\\s(\"'=:])(/[^/\\s\"',;:()\\[\\]]+)+/?");
    /** This machine's host name as the tool records it, as a whole name; null when the tool records none or "unknown". */
    private static final Pattern HOST_NAME = hostNamePattern(DsePreflight.hostname());

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void initialise()
    {
        // the sstables the tests add next to DSE's are written in DSE 6's trie version, as an upgraded node writes them
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, "bb"))
        {
            DatabaseDescriptor.clientInitialization();
            assertThat(BtiFormat.getInstance().getLatestVersion().version).isEqualTo("bb");
        }
    }

    /** One run of the tool. */
    static final class Run
    {
        final int exitCode;
        final String out;
        final String err;
        final NodeReport report;

        Run(int exitCode, String out, String err, NodeReport report)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
            this.report = report;
        }

        Set<String> codes(Finding.Severity severity)
        {
            return report.findings.stream().filter(f -> f.severity == severity).map(f -> f.code).collect(Collectors.toSet());
        }

        List<Finding> findings(String code)
        {
            return report.findings.stream().filter(f -> f.code.equals(code)).collect(Collectors.toList());
        }

        String messages(String code)
        {
            return findings(code).stream().map(f -> f.message).collect(Collectors.joining("\n"));
        }
    }

    private static Run run(Path json, String... args) throws IOException
    {
        List<String> all = new ArrayList<>(Arrays.asList(args));
        all.add("--json");
        all.add(json.toString());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
             PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(all.toArray(new String[0]), outStream, errStream);
        }
        byte[] bytes = Files.exists(json) ? Files.readAllBytes(json) : null;
        if (bytes != null)
            assertNoPlanDecisionIds(new String(bytes, StandardCharsets.UTF_8));
        NodeReport report = bytes == null ? null : JsonUtils.JSON_OBJECT_MAPPER.readValue(bytes, NodeReport.class);
        Run run = new Run(exitCode, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8), report);
        assertNoPlanDecisionIds(run);
        return run;
    }

    private Run run(DseNodeFixture node, String... extra) throws IOException
    {
        return run(temporaryFolder.getRoot().toPath().resolve("report-" + UUID.randomUUID() + ".json"), node.args(extra));
    }

    private DseNodeFixture node() throws IOException
    {
        return DseNodeFixture.create(temporaryFolder.newFolder().toPath());
    }

    private static Run assertBlocked(Run run, String code)
    {
        assertThat(run.exitCode).as("exit code; output:\n%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(run.report.result).isEqualTo(NodeReport.BLOCKED);
        assertThat(run.codes(Finding.Severity.BLOCKING)).as("blocking findings; output:\n%s", run.out).contains(code);
        assertThat(run.out).contains("Result: BLOCKED").contains('[' + code + ']');
        return run;
    }

    private static Run assertReady(Run run)
    {
        assertThat(run.exitCode).as("exit code; output:\n%s%s", run.out, run.err).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(run.report.result).isEqualTo(NodeReport.READY);
        assertThat(run.codes(Finding.Severity.BLOCKING)).isEmpty();
        return run;
    }

    /** The texts the tool prints say what to do, never an id of the project's plan. */
    private static void assertNoPlanDecisionIds(Run run)
    {
        assertNoPlanDecisionIds(run.out, run.err);
        if (run.report == null)
            return;
        for (Finding finding : run.report.findings)
            assertNoPlanDecisionIds(finding.message, finding.remedy == null ? "" : finding.remedy);
    }

    private static Pattern hostNamePattern(String hostname)
    {
        if (hostname.isBlank() || "unknown".equals(hostname))
            return null;
        return Pattern.compile("(?<![\\w.-])" + Pattern.quote(hostname) + "(?![\\w.-])");
    }

    /**
     * What a run of the tool printed or wrote (its output, its error output, its JSON) holds no id of the plan; the
     * paths and this machine's host name, which the texts quote and which are not the tool's words, are left out.
     */
    private static void assertNoPlanDecisionIds(String... texts)
    {
        for (String text : texts)
        {
            String own = MACHINE_PATH.matcher(text).replaceAll("<path>");
            if (HOST_NAME != null)
                own = HOST_NAME.matcher(own).replaceAll("<host>");
            assertThat(own).doesNotContainPattern(PLAN_DECISION_ID);
        }
    }

    @Test
    public void noTextOfTheToolHoldsAPlanDecisionId() throws IOException
    {
        // the string literals of the tool's sources, comments left out: the texts of findings no test reaches included;
        // each literal on its own, so an id split across two literals of one text would escape (the runs above see it),
        // and a char literal holding a quote ('"') pairs with the next quote of its line (harmless: it only adds text)
        Pattern literal = Pattern.compile("\"(?:[^\"\\\\]|\\\\.)*\"");
        List<String> found = new ArrayList<>();
        int literals = 0;
        try (Stream<Path> sources = Files.list(Path.of("src", "java", "org", "apache", "cassandra", "tools", "dsepreflight")))
        {
            for (Path source : sources.filter(p -> p.toString().endsWith(".java")).sorted().collect(Collectors.toList()))
            {
                List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);
                for (int i = 0; i < lines.size(); i++)
                {
                    String line = lines.get(i).trim();
                    if (line.startsWith("*") || line.startsWith("/*") || line.startsWith("//"))
                        continue;
                    Matcher matcher = literal.matcher(line);
                    while (matcher.find())
                    {
                        literals++;
                        if (PLAN_DECISION_ID.matcher(matcher.group()).find())
                            found.add(source.getFileName() + ":" + (i + 1) + ": " + matcher.group());
                    }
                }
            }
        }
        assertThat(literals).isGreaterThan(500);
        assertThat(found).isEmpty();
    }

    // ---- the nodes as DSE left them ----

    @Test
    public void theDse6865FixtureNodeIsReadyAndLeftUntouched() throws IOException
    {
        DseNodeFixture node = node();
        Map<String, String> before = node.fingerprint();
        Run run = assertReady(run(node));
        assertThat(node.fingerprint()).as("the tool wrote to the node's files").isEqualTo(before);

        NodeReport report = run.report;
        assertThat(report.tool).isEqualTo(NodeReport.TOOL);
        assertThat(report.formatVersion).isEqualTo(NodeReport.FORMAT_VERSION);
        assertThat(report.node.releaseVersion).isEqualTo(DseVersion.DSE_6_8_65.releaseVersion);
        assertThat(report.node.dseVersion).isEqualTo("6.8.65");
        assertThat(report.node.installedDseVersion).isEqualTo("6.8.65-4a7b9eb");
        assertThat(report.node.clusterName).isEqualTo("dse-lab");
        assertThat(report.node.dataCenter).isEqualTo("dc1");
        assertThat(report.node.hostId).isNotNull();
        assertThat(report.peers).hasSize(2).allMatch(p -> !p.upgraded && DseVersion.DSE_6_8_65.releaseVersion.equals(p.releaseVersion));
        assertThat(report.upgradePath.saiVersion).isEqualTo("aa");
        assertThat(report.upgradePath.dseRelease).isEqualTo("DSE 6.8.65 (release 4.0.0.6865, installation 6.8.65-4a7b9eb)");
        assertThat(report.upgradePath.settings).containsEntry("internode_compatibility_mode", "DSE_6")
                                               // the only variant the DSE_6 mode accepts
                                               .containsEntry("paxos_variant", "v1")
                                               .containsEntry("jvm-server.options", "-Dcassandra.sai.latest.version=aa")
                                               .containsEntry("cluster_name", "dse-lab")
                                               .containsEntry("authenticator", "PasswordAuthenticator (was com.datastax.bdp.cassandra.auth.DseAuthenticator)")
                                               .containsEntry("role_manager", "CassandraRoleManager (was com.datastax.bdp.cassandra.auth.DseRoleManager)")
                                               .containsEntry("authorizer", "CassandraAuthorizer (was com.datastax.bdp.cassandra.auth.DseAuthorizer)")
                                               .containsEntry("internode_compression", "all");
        // the printed variant is the one the DSE_6 mode accepts
        assertThat(DatabaseDescriptor.isDse6PaxosVariant(Config.PaxosVariant.valueOf(report.upgradePath.settings.get("paxos_variant")))).isTrue();
        assertThat(report.upgradePath.upgradeSteps).anyMatch(s -> s.contains("-Dcassandra.sai.latest.version=aa"))
                                                   .contains("Materialized views: 1 view(s), none to drop")
                                                   // DSE grants the proxy permissions to the creator of every role
                                                   .contains("Security: review the access control warnings above")
                                                   .contains("DSE core only: no trace of DSE Search, Graph, Analytics, DSEFS or Advanced " +
                                                             "Replication was found");
        // DSE core's own keyspaces, solr_admin and dse_leases included, are no trace of a forbidden feature
        // LOW_DISK_SPACE depends on the file system of the temporary directory, so it is not asserted either way
        assertThat(run.codes(Finding.Severity.WARNING)).contains("AUTH_CONFIG_CHANGE", "PROXY_GRANT")
                                                       .doesNotContain("NODESYNC", "SNITCH", "IDENTITY_MISMATCH", "PENDING_HINTS",
                                                                       "LONE_WRITE_GRANT", "DSE_ONLY_PERMISSION", "DSE_ONLY_RESOURCE", "DELEGATION",
                                                                       "SSTABLE_WITHOUT_TOC", "NO_ROLE_PERMISSIONS", "TRIGGER");
        assertThat(run.messages("SSTABLES_ONLY")).endsWith("run nodetool flush system_schema && nodetool flush system_auth " +
                                                           "(two commands, one keyspace each) right before the tool");
        assertThat(report.accessControls.authorizationEnabled).isTrue();
        assertThat(report.accessControls.rolePermissionsRows).isEqualTo(25);
        assertThat(run.messages("PROXY_GRANT")).contains("role 'cassandra' on roles/dseadmin: [PROXY.EXECUTE, PROXY.LOGIN]");
        assertThat(run.messages("AUTH_REPLICA")).contains("read from this node's replica (25 rows)")
                                                .endsWith("so check the reports of every node");
        assertThat(run.messages("VIEWS_HANDLED")).contains("lab.wide_by_v (hidden column .v)");
        assertThat(run.messages("DSE_RELEASE")).isEqualTo("DSE 6.8 (6.8.65) is accepted; the upgrade requires the latest DSE 6.8 or 6.9 patch " +
                                                          "release, which this tool does not check; DSE 6.9 is preferred");
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = (Map<String, Object>) report.inputs.get("schema");
        assertThat((Integer) schema.get("keyspaces")).isGreaterThan(10);
        assertThat(schema).containsEntry("views", 1).doesNotContainKey("role_permissions_rows");
        assertThat(report.upgradePath.upgradeSteps).contains("Sstable formats: every sstable is in DSE 6's trie format");
        assertThat((List<String>) schema.get("keyspace_names")).contains("lab", "solr_admin", "dse_leases", "dse_system", "system_auth");
        assertThat(run.out).contains("Result: READY").contains("SAI on-disk version to pin: aa").contains("Upgrade steps for this node:");
    }

    @Test
    public void aPeerAlreadyUpgradedIsReportedNotRefused() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        Run run = assertReady(run(node));
        assertThat(run.report.node.broadcastAddress).isEqualTo("192.168.57.12");
        assertThat(run.report.peers).anyMatch(p -> p.upgraded && "192.168.57.13".equals(p.address) && "5.0.7.0-SNAPSHOT".equals(p.releaseVersion));
        assertThat(run.messages("UPGRADED_PEERS")).contains("192.168.57.13 (release 5.0.7.0-SNAPSHOT)");
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("MIXED_RELEASES");
    }

    @Test
    public void aDse6926NodePinsSaiVersionCa() throws IOException
    {
        Path nodes = Files.createDirectories(temporaryFolder.newFolder().toPath().resolve("nodes"));
        Files.copy(DseFixtures.nodesLocal(DseVersion.DSE_6_9_26), nodes.resolve("local"));
        Files.copy(DseFixtures.nodesPeers(DseVersion.DSE_6_9_26), nodes.resolve("peers"));
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), nodes);
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        Run run = assertReady(run(node));
        assertThat(run.report.node.releaseVersion).isEqualTo(DseVersion.DSE_6_9_26.releaseVersion);
        assertThat(run.report.upgradePath.saiVersion).isEqualTo("ca");
        assertThat(run.report.upgradePath.settings).containsEntry("jvm-server.options", "-Dcassandra.sai.latest.version=ca");
        assertThat(run.messages("DSE_RELEASE")).isEqualTo("DSE 6.9 (6.9.26) is accepted; the upgrade requires the latest DSE 6.9 patch release, " +
                                                          "which this tool does not check");
        // that node ran in another cluster, which the configuration of the 6.8.65 node does not describe
        assertThat(run.messages("IDENTITY_MISMATCH")).contains("Test Cluster");
    }

    @Test
    public void theKeyspacesDse6926CreatesAreNoTrace() throws IOException
    {
        DseNodeFixture node = node();
        Pattern keyspace = Pattern.compile("CREATE KEYSPACE (\\w+) WITH replication = \\{'class': '(\\w+)'([^}]*)\\}");
        Matcher matcher = keyspace.matcher(new String(Files.readAllBytes(DseFixtures.describeSchema(DseVersion.DSE_6_9_26)), StandardCharsets.UTF_8));
        List<DseNodeFixture.SchemaRow> rows = new ArrayList<>();
        Pattern option = Pattern.compile("'(\\w+)': '([^']*)'");
        while (matcher.find())
        {
            Map<String, String> replication = new LinkedHashMap<>();
            replication.put("class", "org.apache.cassandra.locator." + matcher.group(2));
            Matcher options = option.matcher(matcher.group(3));
            while (options.find())
                replication.put(options.group(1), options.group(2));
            rows.add(row(matcher.group(1)).with("durable_writes", true).with("replication", replication));
        }
        assertThat(rows).extracting(r -> r.partitionKey[0]).contains("solr_admin", "dse_leases", "dse_system_local", "dse_insights", "system_backups");
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), rows.toArray(new DseNodeFixture.SchemaRow[0]));
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), row("preflight_sentinel").with("durable_writes", true).with("replication", simpleStrategy()));
        Run run = assertReady(run(node));
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("NON_CORE_KEYSPACE", "GRAPH_KEYSPACE", "UNLOADABLE_KEYSPACE");
        @SuppressWarnings("unchecked")
        List<String> names = (List<String>) ((Map<String, Object>) run.report.inputs.get("schema")).get("keyspace_names");
        assertThat(names).contains("preflight_sentinel").containsAll(rows.stream().map(r -> (String) r.partitionKey[0]).collect(Collectors.toList()));
    }

    // ---- workloads ----

    @Test
    public void aSearchWorkloadOfThisNodeBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editLocal(local -> local.put("workloads", List.of("Search", "Cassandra")));
        Run run = assertBlocked(run(node), "NON_CORE_WORKLOAD");
        assertThat(run.messages("NON_CORE_WORKLOAD")).contains("this node (nodes/local)").contains("workloads [Search, Cassandra]");
        assertThat(run.findings("NON_CORE_WORKLOAD").get(0).remedy).contains("without -s, -k or -g");
    }

    @Test
    public void aGraphPeerBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> peers.get(0).put("graph", true));
        Run run = assertBlocked(run(node), "NON_CORE_WORKLOAD");
        assertThat(run.messages("NON_CORE_WORKLOAD")).contains("(nodes/peers)").contains("graph true");
    }

    @Test
    public void theUnknownWorkloadIsNoTrace() throws IOException
    {
        DseNodeFixture node = node();
        node.editLocal(local -> {
            local.put("workloads", List.of("Unknown"));
            local.put("workload", "Unknown");
        });
        node.editPeers(peers -> peers.forEach(peer -> peer.put("workloads", List.of("Unknown"))));
        Run run = assertReady(run(node));
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("NON_CORE_WORKLOAD");
    }

    // ---- DSE Search, Graph, Analytics, DSEFS, Advanced Replication ----

    @Test
    public void aSearchIndexBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "indexes", KEYSPACES_KEY, INDEXES_KEY,
                       row("lab", "wide", "wide_solr").with("kind", "CUSTOM")
                                                      .with("options", Map.of("class_name", "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex", "target", "v")));
        Run run = assertBlocked(run(node), "SEARCH_INDEX");
        assertThat(run.messages("SEARCH_INDEX")).contains("lab.wide.wide_solr (com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex)");
    }

    @Test
    public void aSolrDataDirectoryBlocks() throws IOException
    {
        DseNodeFixture node = node();
        Files.createDirectories(node.data.resolve("solr.data").resolve("lab.wide"));
        Path relocated = Files.createDirectories(node.root.resolve("solr-elsewhere"));
        node.editYaml("dse.yaml", yaml -> yaml.put("solr_data_dir", relocated.toString()));
        Run run = assertBlocked(run(node), "SOLR_DATA");
        assertThat(run.messages("SOLR_DATA")).contains(node.data.resolve("solr.data").toString()).contains(relocated + " (solr_data_dir in dse.yaml)");
    }

    @Test
    public void aGraphKeyspaceBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("people").with("durable_writes", true).with("replication", simpleStrategy()).with("graph_engine", "Core"));
        node.writeRows("system_schema", "vertices", KEYSPACES_KEY, List.of("table_name", "label_name"), row("classic_graph", "person", "person"));
        Run run = assertBlocked(run(node), "GRAPH_KEYSPACE");
        assertThat(run.messages("GRAPH_KEYSPACE")).contains("people (graph_engine Core)").contains("classic_graph (rows in system_schema.vertices or system_schema.edges)");
    }

    @Test
    public void analyticsDsefsAndAdvancedReplicationKeyspacesBlockEvenEmpty() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("dse.yaml", yaml -> section(yaml, "dsefs_options").put("keyspace_name", "files"));
        List<DseNodeFixture.SchemaRow> rows = new ArrayList<>();
        for (String keyspace : List.of("dse_analytics", "HiveMetaStore", "dsefs", "cfs", "cfs_archive", "dse_advrep", "files"))
            rows.add(row(keyspace).with("durable_writes", true).with("replication", simpleStrategy()));
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), rows.toArray(new DseNodeFixture.SchemaRow[0]));
        Run run = assertBlocked(run(node), "NON_CORE_KEYSPACE");
        assertThat(run.messages("NON_CORE_KEYSPACE")).contains("DSE Analytics, DSEFS and CFS keyspaces: HiveMetaStore, cfs, cfs_archive, dse_analytics, dsefs, files")
                                                     .contains("DSE Advanced Replication keyspaces: dse_advrep");
    }

    @Test
    public void advancedReplicationAndDsefsSettingsBlock() throws IOException
    {
        DseNodeFixture node = node();
        Path advrep = Files.createDirectories(node.root.resolve("advrep"));
        Files.write(advrep.resolve("channel.log"), new byte[]{ 1 });
        node.editYaml("dse.yaml", yaml -> {
            Map<String, Object> options = section(yaml, "advanced_replication_options");
            options.put("enabled", true);
            options.put("advanced_replication_directory", advrep.toString());
            section(yaml, "dsefs_options").put("enabled", true);
        });
        Run run = assertBlocked(run(node), "ADVANCED_REPLICATION");
        assertThat(run.findings("ADVANCED_REPLICATION")).hasSize(2);
        assertThat(run.codes(Finding.Severity.BLOCKING)).contains("DSEFS");
    }

    // ---- classes, types and table options ----

    @Test
    public void tieredStorageBlocks() throws IOException
    {
        DseNodeFixture node = node();
        addTable(node, "lab", "tiered", Map.of("class", SchemaInspector.TIERED_COMPACTION_STRATEGY, "tiering_strategy", "TimeWindowStorageStrategy"),
                 lz4(), Set.of("compound"), Map.of());
        node.editYaml("dse.yaml", yaml -> section(yaml, "tiered_storage_options").put("strategy1", Map.of("tiers", List.of(Map.of("paths", List.of("/mnt1"))))));
        Run run = assertBlocked(run(node), "TIERED_STORAGE");
        assertThat(run.messages("TIERED_STORAGE")).contains("lab.tiered (compaction class " + SchemaInspector.TIERED_COMPACTION_STRATEGY + ')')
                                                  .contains("tiered_storage_options [strategy1]");
        // a compaction strategy both products have (UnifiedCompactionStrategy is not checked on DSE 6.8)
        assertThat(run.findings("TIERED_STORAGE")).anyMatch(f -> f.remedy.contains("{'class': 'SizeTieredCompactionStrategy'} or {'class': " +
                                                                                 "'LeveledCompactionStrategy'} (both strategies exist in DSE and " +
                                                                                 "in HCD 2.0)"))
                                                  .noneMatch(f -> f.remedy.contains("Unified"));
    }

    @Test
    public void classesHcd20DoesNotHaveBlock() throws IOException
    {
        DseNodeFixture node = node();
        addTable(node, "lab", "custom_compaction", Map.of("class", "com.example.NoSuchCompactionStrategy"), lz4(), Set.of("compound"), Map.of());
        addTable(node, "lab", "custom_compression", stcs(), Map.of("class", "com.example.NoSuchCompressor", "chunk_length_in_kb", "64"), Set.of("compound"), Map.of());
        node.writeRows("system_schema", "indexes", KEYSPACES_KEY, INDEXES_KEY,
                       row("lab", "wide", "wide_custom").with("kind", "CUSTOM").with("options", Map.of("class_name", "com.example.NoSuchIndex", "target", "v")));
        Run run = assertBlocked(run(node), "UNLOADABLE_CLASS");
        assertThat(run.messages("UNLOADABLE_CLASS")).contains("lab.custom_compaction: compaction class com.example.NoSuchCompactionStrategy")
                                                    .contains("lab.custom_compression: compression")
                                                    .contains("lab.wide.wide_custom (com.example.NoSuchIndex)");
        assertThat(run.findings("UNLOADABLE_CLASS").get(0).remedy).contains("SizeTieredCompactionStrategy or LeveledCompactionStrategy")
                                                                  .doesNotContain("Unified");
    }

    /**
     * A non-finite {@code min_compress_ratio}, which older releases accepted and stored: the schema load of HCD 2.0
     * keeps it ({@link CompressionParams#fromStoredMap}), so the tool does not flag the table, although the parse of a
     * new definition refuses it.
     */
    @Test
    public void aStoredNonFiniteMinCompressRatioDoesNotBlock() throws IOException
    {
        DseNodeFixture node = node();
        for (String ratio : new String[]{ "NaN", "Infinity" })
        {
            Map<String, String> compression = Map.of("class", "org.apache.cassandra.io.compress.LZ4Compressor", "chunk_length_in_kb", "64",
                                                     "min_compress_ratio", ratio);
            addTable(node, "lab", "ratio_" + ratio.toLowerCase(), stcs(), compression, Set.of("compound"), Map.of());
            assertThatThrownBy(() -> CompressionParams.fromMap(compression)).isInstanceOf(ConfigurationException.class)
                                                                            .hasMessage("Invalid value for min_compress_ratio: " + ratio);
        }
        assertReady(run(node));
    }

    @Test
    public void aColumnTypeHcd20DoesNotHaveBlocks() throws IOException
    {
        DseNodeFixture node = node();
        addTable(node, "lab", "places", stcs(), lz4(), Set.of("compound"), Map.of("location", "'com.example.marshal.NoSuchType'"));
        Run run = assertBlocked(run(node), "UNLOADABLE_TABLE");
        assertThat(run.messages("UNLOADABLE_TABLE")).contains("lab.places").contains("com.example.marshal.NoSuchType");
    }

    @Test
    public void encryptedTablesBlock() throws IOException
    {
        DseNodeFixture node = node();
        addTable(node, "lab", "secret", stcs(), Map.of("class", "org.apache.cassandra.io.compress.Encryptor", "cipher_algorithm", "AES/ECB/PKCS5Padding"),
                 Set.of("compound"), Map.of());
        node.editYaml("dse.yaml", yaml -> section(yaml, "system_info_encryption").put("enabled", true));
        Run run = assertBlocked(run(node), "TDE_TABLE");
        assertThat(run.findings("TDE_TABLE")).hasSize(2);
        assertThat(run.messages("TDE_TABLE")).contains("lab.secret (compression class org.apache.cassandra.io.compress.Encryptor)")
                                             .contains("system_info_encryption.enabled is true");
        assertThat(run.messages("TDE_TABLE")).contains("encrypted (TDE) tables: lab.secret (compression class org.apache.cassandra.io.compress.Encryptor); " +
                                                       "the upgrade applies to clusters without encrypted tables or views")
                                             .contains(SYSTEM_INFO_ENCRYPTION_MESSAGE);
        assertThat(run.findings("TDE_TABLE")).filteredOn(f -> f.message.equals(SYSTEM_INFO_ENCRYPTION_MESSAGE)).singleElement()
                                             .extracting(f -> f.remedy).isEqualTo(SYSTEM_INFO_ENCRYPTION_REMEDY);
    }

    @Test
    public void compactStorageAndLegacyAuthTablesBlock() throws IOException
    {
        DseNodeFixture node = node();
        addTable(node, "lab", "thrift_era", stcs(), lz4(), Set.of("dense"), Map.of());
        addTable(node, "system_auth", "users", stcs(), lz4(), Set.of("compound"), Map.of());
        Run run = assertBlocked(run(node), "COMPACT_STORAGE");
        assertThat(run.messages("COMPACT_STORAGE")).contains("lab.thrift_era (flags [dense])");
        assertThat(run.messages("LEGACY_AUTH_TABLES")).contains("system_auth.users");
    }

    @Test
    public void javascriptFunctionsBlockWhateverTheCase() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "functions", KEYSPACES_KEY, FUNCTIONS_KEY,
                       function("lab", "plus_one", "JavaScript"), function("lab", "minus_one", "javascript"), function("lab", "java_ok", "java"));
        Run run = assertBlocked(run(node), "JAVASCRIPT_UDF");
        assertThat(run.messages("JAVASCRIPT_UDF")).contains("lab.plus_one(int)").contains("lab.minus_one(int)").doesNotContain("java_ok");
    }

    @Test
    public void viewHiddenColumnsOtherThanTheRegularKeyColumnBlock() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", SchemaInspector.HIDDEN_COLUMNS, KEYSPACES_KEY, COLUMNS_KEY,
                       row("lab", "wide_by_v", ".extra").with("kind", "regular").with("required_for_liveness", false).with("type", "int")
                                                       .with("position", -1).with("clustering_order", "none")
                                                       .with("column_name_bytes", ByteBufferUtil.bytes(".extra")));
        Run run = assertBlocked(run(node), "VIEW_HIDDEN_COLUMNS");
        assertThat(run.messages("VIEW_HIDDEN_COLUMNS")).contains("lab.wide_by_v: DSE keeps the hidden columns [");
        assertThat(run.report.upgradePath.upgradeSteps).anyMatch(s -> s.startsWith("Materialized views: drop lab.wide_by_v"));
    }

    // ---- access controls and authentication ----

    @Test
    public void rowLevelAccessControlInDseYamlBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("dse.yaml", yaml -> section(yaml, "authorization_options").put("allow_row_level_security", true));
        Run run = assertBlocked(run(node), "ROW_LEVEL_ACCESS");
        assertThat(run.messages("ROW_LEVEL_ACCESS")).startsWith("authorization_options.allow_row_level_security is true in dse.yaml");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Security: resolve the blocking access control findings above");
    }

    @Test
    public void restrictAndRowLevelAccessControlBlock() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_auth", "role_permissions", ROLE_KEY, RESOURCE_KEY,
                       row("analyst", "data/lab/wide").with("restricted", Set.of("SELECT")),
                       row("analyst", "rows/tenant1/data/lab/wide").with("permissions", Set.of("SELECT")));
        node.writeRows("system_schema", "tables", KEYSPACES_KEY, TABLES_KEY,
                       row("lab", "wide").with("extensions", Map.of("DSE_RLACA", ByteBufferUtil.bytes("pk"), "other", ByteBufferUtil.bytes("x"))));
        node.editYaml("dse.yaml", yaml -> section(yaml, "authorization_options").put("allow_row_level_security", true));
        Run run = assertBlocked(run(node), "RESTRICT");
        assertThat(run.messages("RESTRICT")).contains("role 'analyst' on data/lab/wide: [SELECT]");
        assertThat(run.findings("ROW_LEVEL_ACCESS")).hasSize(3);
        assertThat(run.messages("ROW_LEVEL_ACCESS")).contains("role 'analyst' on rows/tenant1/data/lab/wide: [SELECT]")
                                                    .contains("lab.wide (tenant column pk)")
                                                    .contains("allow_row_level_security is true");
        assertThat(run.messages("TABLE_EXTENSIONS")).contains("lab.wide: [other]");
    }

    @Test
    public void ldapKerberosAndLdapRolesBlock() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("dse.yaml", yaml -> {
            Map<String, Object> authentication = section(yaml, "authentication_options");
            authentication.put("default_scheme", "ldap");
            authentication.put("other_schemes", List.of("internal", "kerberos"));
            section(yaml, "role_management_options").put("mode", "ldap");
        });
        Run run = assertBlocked(run(node), "AUTH_SCHEME");
        assertThat(run.messages("AUTH_SCHEME")).contains("[kerberos, ldap]").contains("role_management_options.mode is ldap");
        assertThat(run.codes(Finding.Severity.WARNING)).doesNotContain("AUTH_CONFIG_CHANGE");
    }

    @Test
    public void loneWriteGrantsDsePermissionsAndDelegationsWarn() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_auth", "role_permissions", ROLE_KEY, RESOURCE_KEY,
                       row("app", "data/lab/wide").with("permissions", Set.of("UPDATE", "SELECT")),
                       row("app", "data/lab").with("permissions", Set.of("SELECT", "BACKUP")).with("grantables", Set.of("SELECT")),
                       row("app", "search/lab/wide").with("permissions", Set.of("SEARCH.UPDATE")),
                       row("writer", "data/lab/wide").with("permissions", Set.of("UPDATE", "TRUNCATE")),
                       row("bell\u0007role", "data/lab/wide").with("permissions", Set.of("TRUNCATE")));
        Run run = assertReady(run(node));
        assertThat(run.messages("LONE_WRITE_GRANT")).startsWith("DSE UPDATE or TRUNCATE granted alone: ")
                                                    .contains("role 'app' on data/lab/wide: [UPDATE]").doesNotContain("writer")
                                                    .contains("on data/lab/wide: [TRUNCATE]").doesNotContain("\u0007");
        assertThat(run.findings("LONE_WRITE_GRANT").get(0).remedy).isEqualTo("GRANT MODIFY where the role should also have the other permission, or " +
                                                                             "accept the loss");
        assertThat(run.messages("DSE_ONLY_PERMISSION")).contains("role 'app' on data/lab: [BACKUP]");
        assertThat(run.messages("DSE_ONLY_RESOURCE")).contains("role 'app' on search/lab/wide: [SEARCH.UPDATE]");
        assertThat(run.messages("DELEGATION")).contains("role 'app' on data/lab: [SELECT]");
    }

    // ---- releases ----

    @Test
    public void aDseReleaseOtherThan68Or69Blocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editLocal(local -> {
            local.put("release_version", "4.0.0.678");
            local.put("dse_version", "6.7.8");
        });
        node.editPeers(peers -> peers.forEach(peer -> peer.put("release_version", "4.0.0.678")));
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.7.8-0123abc\n".getBytes(StandardCharsets.UTF_8));
        Run run = assertBlocked(run(node), "DSE_RELEASE");
        assertThat(run.messages("DSE_RELEASE")).isEqualTo("this node runs DSE 6.7.8 (release 4.0.0.678); only DSE 6.8 and 6.9 are upgraded in place " +
                                                          "to HCD 2.0 (6.9 preferred)");
        assertThat(run.findings("DSE_RELEASE").get(0).remedy).isEqualTo("upgrade every node to the latest DSE 6.9 (or 6.8) patch release first, " +
                                                                        "following DataStax's procedure");
        assertThat(run.report.upgradePath.saiVersion).isNull();
    }

    @Test
    public void aNodeThatDoesNotRunDse6Blocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editLocal(local -> local.put("release_version", "5.0.7.0"));
        Run run = assertBlocked(run(node), "DSE_RELEASE");
        assertThat(run.messages("DSE_RELEASE")).contains("not a DSE 6 release");
    }

    @Test
    public void mixedDseReleasesBlock() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> {
            peers.get(0).put("release_version", "4.0.0.6926");
            peers.get(0).put("dse_version", "6.9.26");
        });
        Run run = assertBlocked(run(node), "MIXED_RELEASES");
        assertThat(run.messages("MIXED_RELEASES")).contains("4.0.0.6926 (DSE 6.9.26)");
    }

    @Test
    public void anInstalledReleaseOtherThanTheRunningOneBlocks() throws IOException
    {
        DseNodeFixture node = node();
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        Run run = assertBlocked(run(node), "INSTALLED_RELEASE_MISMATCH");
        assertThat(run.messages("INSTALLED_RELEASE_MISMATCH")).contains("is 6.9.26-0c77b67 (ds_version.txt) but this node last ran DSE 6.8.65");
    }

    // ---- files on disk ----

    @Test
    public void sstablesDse6DoesNotWriteBlock() throws IOException
    {
        DseNodeFixture node = node();
        Path table = Files.createDirectories(node.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        String uuid = new UUIDBasedSSTableId(TimeUUID.Generator.nextTimeUUID()).toString();
        for (String name : List.of("mc-1-big-Data.db", "ca-2-bti-Data.db", "bb-" + uuid + "-bti-Data.db", "lab-wide-ka-4-Data.db", "bb-5-bti-Data.db"))
            Files.write(table.resolve(name), new byte[0]);
        Run run = assertBlocked(run(node), "SSTABLE_FORMAT");
        String messages = run.messages("SSTABLE_FORMAT");
        assertThat(run.findings("SSTABLE_FORMAT")).hasSize(4);
        assertThat(messages).contains(NodeInspector.BIG_FORMAT + ": lab.wide: 1 (mc-1-big-Data.db)")
                            .contains(NodeInspector.NEWER_TRIE + ": lab.wide: 1 (ca-2-bti-Data.db)")
                            .contains(NodeInspector.NON_NUMERIC_ID)
                            .contains(NodeInspector.UNKNOWN_NAME + ": lab.wide: 1 (lab-wide-ka-4-Data.db)")
                            .doesNotContain("bb-5-bti");
        assertThat(run.report.upgradePath.upgradeSteps)
            .anyMatch(s -> s.equals("Sstable formats: run nodetool upgradesstables on DSE first, and move the files DSE 6 did not write " +
                                    "out of the data directories (see SSTABLE_FORMAT)"));
    }

    @Test
    public void pendingHintsAndForeignCommitLogSegmentsWarn() throws IOException
    {
        DseNodeFixture node = node();
        Path dseHints = DseFixtures.hintsFile(DseVersion.DSE_6_8_65);
        Files.copy(dseHints, node.hints.resolve(dseHints.getFileName()));
        Files.write(node.hints.resolve(UUID.randomUUID() + "-1790335633208-v1.hints"), new byte[0]);
        Files.write(node.hints.resolve(UUID.randomUUID() + "-1790335633208-2.hints"), new byte[0]);
        DseFixtures.unpackCommitLogSegment(DseVersion.DSE_6_8_65, node.commitLog);
        Files.write(node.commitLog.resolve("CommitLog-7-1790335633208.log"), new byte[0]);
        Run run = assertReady(run(node));
        assertThat(run.messages("PENDING_HINTS")).contains("ed39c7b2-7f97-4a0a-91fc-71398afe1d12 (192.168.57.13, DSE): 1 file(s)")
                                                 .contains("for host ids neither in nodes/local nor in nodes/peers");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("NON_DSE_HINTS").doesNotContain("HINTS_UNREADABLE");
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("HINTS_COMPRESSION");
        assertThat(run.messages("COMMITLOG_VERSION")).contains("{7=1}");
        assertThat(run.report.upgradePath.upgradeSteps).anyMatch(s -> s.startsWith("Hints: 1 target(s)"));
    }

    @Test
    public void nodesyncNotDisabledInTheYamlWarns() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> yaml.remove("nodesync"));
        Run run = assertReady(run(node));
        assertThat(run.messages("NODESYNC")).contains("absent, NodeSync starts at every DSE start");
        assertThat(run.report.upgradePath.upgradeSteps).anyMatch(s -> s.startsWith("NodeSync: add enabled: false"));
    }

    @Test
    public void aKeyspaceDseDroppedIsNoTrace() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), row("dse_analytics").with("durable_writes", true).with("replication", simpleStrategy()));
        assertBlocked(run(node), "NON_CORE_KEYSPACE");
        node.deletePartitions("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), "dse_analytics");
        assertReady(run(node));
    }

    @Test
    public void filesTheToolCannotReadBlock() throws IOException
    {
        DseNodeFixture node = node();
        Files.delete(node.metadata.resolve("nodes").resolve("local"));
        Files.delete(node.conf.resolve("dse.yaml"));
        Run run = assertBlocked(run(node), "NOT_A_DSE_NODE");
        assertThat(run.messages("UNREADABLE_INPUT")).contains("cannot read dse.yaml");
        // DseAuthorizer without dse.yaml: whether DSE checks permissions is not known
        assertThat(run.report.accessControls.authorizationEnabled).isNull();

        DseNodeFixture noSchema = node();
        try (Stream<Path> paths = Files.walk(noSchema.data.resolve("system_schema")))
        {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
                Files.delete(path);
        }
        Run schemaRun = assertBlocked(run(noSchema), "UNREADABLE_SCHEMA");
        assertThat(schemaRun.messages("UNREADABLE_SCHEMA")).contains("no data directory holds system_schema.keyspaces");
        assertThat(schemaRun.report.upgradePath.upgradeSteps)
        .contains("Security: review the access control warnings above; not checked completely (not read: the schema)",
                  "DSE core only: not checked completely (not read: the schema)",
                  "Materialized views: not checked completely (not read: the schema)");
    }

    // ---- live state ----

    @Test
    public void theLiveStateOfARunningNodeIsChecked() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        DseNodeFixture node = node();
        Path arguments = node.root.resolve("nodetool-arguments");
        Path nodetool = fakeNodetool(node.root, arguments, 0,
                                     "case \"$*\" in\n" +
                                     "  *nodesyncservice*) echo 'The NodeSync service is running' ;;\n" +
                                     "  *describecluster*) printf 'Cluster Information:\\n\\tSchema versions:\\n\\t\\t78b1789d-6787-353b-9443-b33b3c0d7120: [192.168.57.11]\\n" +
                                     "\\t\\t42be62c3-6cac-39ac-8412-42c8cbd3485c: [192.168.57.13]\\n' ;;\n" +
                                     "  *status*) printf 'Datacenter: dc1\\n--  Address  Load\\nUN  192.168.57.11  1 MiB\\nDN  192.168.57.13  1 MiB\\n' ;;\n" +
                                     "  *version*) echo 'ReleaseVersion: 4.0.0.6865' ;;\n" +
                                     "  *compactionstats*) printf 'pending tasks: 3\\nid  compaction type  keyspace\\n1  Validation  lab\\n' ;;\n" +
                                     "  *netstats*) printf 'Mode: NORMAL\\nRebuild 1\\n' ;;\n" +
                                     "  *listpendinghints*) printf 'Host ID  Address  Rack  DC  Status  Total files\\nx  192.168.57.13  rack3  dc1  DOWN  1\\n' ;;\n" +
                                     "esac\n");
        Run run = assertReady(run(node, "--nodetool", nodetool.toString(), "--nodetool-arg=-h", "--nodetool-arg=127.0.0.1"));
        assertThat(run.codes(Finding.Severity.WARNING)).contains("LIVE_STATUS", "LIVE_SCHEMA", "LIVE_NODESYNC", "LIVE_COMPACTIONS", "LIVE_REPAIR",
                                                                 "LIVE_STREAMS", "LIVE_HINTS")
                                                       .doesNotContain("LIVE_UNAVAILABLE");
        assertThat(run.messages("LIVE_STATUS")).contains("DN 192.168.57.13");
        assertThat(run.report.live).containsOnlyKeys(LiveState.COMMANDS.toArray(new String[0]));
        assertThat(LiveState.COMMANDS).contains("gossipinfo", "tpstats");
        assertThat(run.report.live.get("version").output).contains("ReleaseVersion: 4.0.0.6865");
        assertThat(Files.readAllLines(arguments)).contains("-h 127.0.0.1 version", "-h 127.0.0.1 nodesyncservice status");
    }

    @Test
    public void aNodetoolCommandThatDoesNotFinishIsStopped() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        DseNodeFixture node = node();
        Path nodetool = fakeNodetool(node.root, node.root.resolve("arguments"), 0, "case \"$*\" in\n  *version*) sleep 30 ;;\nesac\n");
        long start = System.nanoTime();
        Run run = assertReady(run(node, "--nodetool", nodetool.toString(), "--nodetool-timeout", "1"));
        assertThat(System.nanoTime() - start).as("the command was stopped").isLessThan(TimeUnit.SECONDS.toNanos(25));
        assertThat(run.report.live.get("version").error).isEqualTo("nodetool version did not finish within 1 s");
        assertThat(run.messages("LIVE_UNAVAILABLE")).contains("nodetool version: nodetool version did not finish within 1 s");
        assertThat(run.findings("LIVE_UNAVAILABLE").get(0).remedy).contains("-pwf <file> rather than -pw <password>");
    }

    @Test
    public void aNodetoolThatFailsIsReported() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        DseNodeFixture node = node();
        Path nodetool = fakeNodetool(node.root, node.root.resolve("arguments"), 1, "echo 'nodetool: Failed to connect to 127.0.0.1:7199'\n");
        Run run = assertReady(run(node, "--nodetool", nodetool.toString()));
        assertThat(run.messages("LIVE_UNAVAILABLE")).contains("nodetool status: exit code 1 nodetool: Failed to connect");
        Run missing = assertReady(run(node, "--nodetool", node.root.resolve("no-such-nodetool").toString()));
        assertThat(missing.messages("LIVE_UNAVAILABLE")).contains("cannot run");
        // the nodetool of DSE 6.8.65 and 6.9.26 has no listpendinghints
        Path older = fakeNodetool(node.root, node.root.resolve("arguments"), 0,
                                 "case \"$*\" in\n" +
                                 "  *listpendinghints*) echo 'nodetool: Found unexpected parameters: [listpendinghints]'; exit 1 ;;\n" +
                                 "  *netstats*) echo 'Not sending any streams.' ;;\n" +
                                 "esac\n");
        Run unknown = assertReady(run(node, "--nodetool", older.toString()));
        assertThat(unknown.codes(Finding.Severity.WARNING)).doesNotContain("LIVE_UNAVAILABLE", "LIVE_STREAMS", "LIVE_HINTS");
        assertThat(unknown.messages("LIVE_COMMAND_UNKNOWN")).contains("nodetool listpendinghints");
    }

    // ---- more blocking findings, one modified copy each ----

    @Test
    public void aReplicationHcd20DoesNotHaveBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("custom_replication").with("durable_writes", true).with("replication", Map.of("class", "com.example.NoSuchStrategy", "dc1", "3")));
        Run run = assertBlocked(run(node), "UNLOADABLE_KEYSPACE");
        assertThat(run.messages("UNLOADABLE_KEYSPACE")).contains("custom_replication: ").contains("com.example.NoSuchStrategy");
    }

    @Test
    public void aUserTypeHcd20CannotLoadBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "types", KEYSPACES_KEY, List.of("type_name"),
                       row("lab", "geo").with("field_names", List.of("place")).with("field_types", List.of("'com.example.marshal.NoSuchType'")));
        Run run = assertBlocked(run(node), "UNLOADABLE_TYPE");
        assertThat(run.messages("UNLOADABLE_TYPE")).contains("lab: ").contains("com.example.marshal.NoSuchType");
    }

    @Test
    public void aDroppedColumnOfATypeHcd20CannotLoadBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "dropped_columns", KEYSPACES_KEY, COLUMNS_KEY,
                       row("lab", "wide", "old").with("dropped_time", new Date(1)).with("kind", "regular")
                                                .with("type", "'com.example.marshal.NoSuchType'"));
        Run run = assertBlocked(run(node), "UNLOADABLE_TABLE");
        assertThat(run.messages("UNLOADABLE_TABLE")).contains("dropped columns of a type HCD 2.0 cannot load").contains("lab.wide.old");
    }

    @Test
    public void aFunctionInAnotherLanguageBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "functions", KEYSPACES_KEY, FUNCTIONS_KEY, function("lab", "py", "python"));
        Run run = assertBlocked(run(node), "UDF_LANGUAGE");
        assertThat(run.messages("UDF_LANGUAGE")).contains("lab.py(int) (language python)");
    }

    @Test
    public void aPartitionerHcd20DoesNotHaveBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> yaml.put("partitioner", "com.example.NoSuchPartitioner"));
        Run run = assertBlocked(run(node), "UNLOADABLE_CLASS");
        assertThat(run.messages("UNLOADABLE_CLASS")).contains("the partitioner com.example.NoSuchPartitioner");
    }

    @Test
    public void otherDseSecurityClassesBlock() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> yaml.put("authorizer", "com.datastax.bdp.cassandra.auth.CustomAuthorizer"));
        Run run = assertBlocked(run(node), "AUTH_SCHEME");
        assertThat(run.messages("AUTH_SCHEME")).contains("authorizer com.datastax.bdp.cassandra.auth.CustomAuthorizer");
    }

    @Test
    public void schemePermissionsBlock() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("dse.yaml", yaml -> section(yaml, "authentication_options").put("scheme_permissions", true));
        Run run = assertBlocked(run(node), "SCHEME_PERMISSIONS");
        assertThat(run.findings("SCHEME_PERMISSIONS").get(0).remedy).contains("set scheme_permissions: false");
        // DSE's resources of the authentication schemes
        assertThat(run.messages("SCHEME_PERMISSIONS")).contains("EXECUTE on (resources authentication_schemes/...)");
    }

    @Test
    public void anEncryptedCommitLogBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> section(yaml, "transparent_data_encryption_options").put("enabled", true));
        Run run = assertBlocked(run(node), "TDE_COMMITLOG");
        assertThat(run.findings("TDE_COMMITLOG")).hasSize(1);
        assertThat(run.messages("TDE_COMMITLOG")).isEqualTo("transparent_data_encryption_options.enabled is true in DSE's cassandra.yaml: the commit " +
                                                            "log DSE writes is encrypted; encryption is out of scope of this upgrade, and HCD 2.0's " +
                                                            "replay of DSE's commit log was not validated with encrypted segments");
        assertThat(run.findings("TDE_COMMITLOG").get(0).remedy)
        .isEqualTo("set enabled: false under transparent_data_encryption_options and restart DSE on every node, with the keystore that section " +
                   "names still in place, so that DSE replays the commit log segments written before and writes its segments unencrypted from " +
                   "then on, before the upgrade");
    }

    /**
     * HINTS_COMPRESSION: DSE's cassandra.yaml sets hints_compression, so DSE writes compressed hints files, which HCD 2.0
     * cannot convert; the key written with no value, an empty list or an empty string is no compression.
     */
    @Test
    public void hintsCompressionBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> yaml.put("hints_compression", List.of(Map.of("class_name", "LZ4Compressor", "parameters", List.of(Map.of())))));
        Run run = assertBlocked(run(node), "HINTS_COMPRESSION");
        assertThat(run.findings("HINTS_COMPRESSION")).hasSize(1);
        assertThat(run.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE's cassandra.yaml sets hints_compression (class_name LZ4Compressor): DSE writes compressed hints files, and HCD 2.0 " +
                   "cannot convert a compressed DSE hints file at its start: it keeps the file and does not deliver its hints");
        assertThat(run.findings("HINTS_COMPRESSION").get(0).remedy)
        .isEqualTo("remove hints_compression from DSE's cassandra.yaml and restart DSE on this node, so that the hints files it writes from then " +
                   "on are not compressed; the files written before keep their compression: with every node up, wait until DSE has delivered " +
                   "them and they are gone from its hints directory, before the first node of the cluster is upgraded (this tool names the " +
                   "compressed or encrypted files left)");

        DseNodeFixture other = node();
        other.editYaml("cassandra.yaml", yaml -> yaml.put("hints_compression", "SnappyCompressor"));
        assertThat(assertBlocked(run(other), "HINTS_COMPRESSION").messages("HINTS_COMPRESSION"))
        .startsWith("DSE's cassandra.yaml sets hints_compression (SnappyCompressor): ");

        for (Object unset : new Object[]{ null, List.of(), "" })
        {
            DseNodeFixture plain = node();
            plain.editYaml("cassandra.yaml", yaml -> yaml.put("hints_compression", unset));
            assertThat(assertReady(run(plain)).findings("HINTS_COMPRESSION")).as(String.valueOf(unset)).isEmpty();
        }
        assertThat(NodeInspector.isSetValue(Map.of())).isFalse();
        assertThat(NodeInspector.isSetValue(" ")).isFalse();
        assertThat(NodeInspector.isSetValue(Map.of("class_name", "LZ4Compressor"))).isTrue();
        assertThat(run.report.upgradePath.upgradeSteps).anyMatch(s -> s.startsWith("Hints: compressed or encrypted DSE hints files, or hints_compression set: " +
                                                                                    "see HINTS_COMPRESSION; "));
    }

    /**
     * The four tables system_info_encryption encrypts: sorted, as the schema inspection lists them, and so named; the
     * names joined as a sentence joins them.
     */
    @Test
    public void systemInfoEncryptedTablesAreSortedAndNamed()
    {
        assertThat(NodeInspector.SYSTEM_INFO_ENCRYPTED_TABLE_NAMES).isSorted()
                                                                   .containsExactly("system.batches", "system.paxos",
                                                                                    "system_backups.backup_configurations",
                                                                                    "system_backups.backup_stores");
        assertThat(NodeInspector.SYSTEM_INFO_ENCRYPTED_TABLES).isEqualTo("the tables system.batches, system.paxos, " +
                                                                        "system_backups.backup_configurations and " +
                                                                        "system_backups.backup_stores");
        assertThat(NodeInspector.namesWithAnd(List.of("a"))).isEqualTo("a");
        assertThat(NodeInspector.namesWithAnd(List.of("a", "b"))).isEqualTo("a and b");
    }

    /**
     * HINTS_COMPRESSION from the hints files themselves: a DSE hints file whose header records a compression blocks, so
     * that the files written before hints_compression was removed are found, and so does a file DSE encrypted while
     * system_info_encryption was enabled in dse.yaml, whose header records the encryption as a compression (TDE_TABLE blocks
     * too while the setting is on); the parameters are named, never their values. A header that cannot be read is a warning.
     */
    @Test
    public void compressedDseHintsFilesBlock() throws IOException
    {
        DseNodeFixture node = node();
        String compressed = UUID.randomUUID() + "-1790335633208-v1.hints";
        writeDseHintsHeader(node.hints.resolve(compressed), "{\"compression\":{\"class_name\":\"LZ4Compressor\",\"parameters\":{\"secret\":\"s3cr3t\"}}}");
        String plain = UUID.randomUUID() + "-1790335633209-v1.hints";
        writeDseHintsHeader(node.hints.resolve(plain), "{}");
        Run run = assertBlocked(run(node), "HINTS_COMPRESSION");
        assertThat(run.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE hints files written compressed or encrypted (the parameters their header records): " + compressed + " (compression); HCD " +
                   "2.0 cannot convert such a file at its start: it keeps the file and does not deliver its hints");
        assertThat(run.findings("HINTS_COMPRESSION").get(0).remedy)
        .isEqualTo("DSE writes such files while its cassandra.yaml sets hints_compression, or while dse.yaml sets enabled: true under " +
                   "system_info_encryption, with which DSE encrypts its hints files through their compression (the header records the " +
                   "encryption as a compression): remove hints_compression from DSE's cassandra.yaml and set enabled: false under " +
                   "system_info_encryption in dse.yaml, each one that is set, and restart DSE on this node, so that the hints files it " +
                   "writes from then on are neither compressed nor encrypted; the files written before keep their compression or " +
                   "encryption: with every node up, and for encrypted files " + SYSTEM_KEY_DIRECTORY + " still in place, wait " +
                   "until DSE has delivered them and they are gone, before the first node of the cluster is upgraded, then run " +
                   "this tool again; if " +
                   "system_info_encryption was enabled, it also encrypted the tables system.batches, system.paxos, " +
                   "system_backups.backup_configurations and system_backups.backup_stores: once DSE runs with it off on every node, run " +
                   "nodetool upgradesstables -a system batches paxos and nodetool upgradesstables -a system_backups on every node, with " +
                   "that directory still in place, before the first node of the cluster is upgraded (this tool does not check that the " +
                   "rewrite is done); for encrypted files, " + SYSTEM_KEY_LOSS);
        assertThat(run.out).doesNotContain("s3cr3t");
        assertThat(run.codes(Finding.Severity.WARNING)).doesNotContain("HINTS_UNREADABLE");
        assertThat(NodeInspector.hintsFileParameters(node.hints.resolve(plain))).isEmpty();
        // the header as this software's own HintsDescriptor writes it, with DSE's version, and DSE's own file
        Path written = node.hints.resolve(UUID.randomUUID() + "-1790335633211-v1.hints");
        Files.write(written, HintsDescriptorBytes.header(UUID.randomUUID(), -1, 1790335633211L, "LZ4Compressor"));
        assertThat(NodeInspector.hintsFileParameters(written)).containsOnlyKeys("compression");
        assertThat(NodeInspector.hintsFileParameters(DseFixtures.hintsFile(DseVersion.DSE_6_8_65))).isEmpty();

        // a file DSE encrypted while system_info_encryption was enabled, the setting off since: its header records DSE's
        // encrypting compressor
        DseNodeFixture encryptedNode = node();
        String encrypted = UUID.randomUUID() + "-1790335633212-v1.hints";
        writeDseHintsHeader(encryptedNode.hints.resolve(encrypted),
                            "{\"compression\":{\"class_name\":\"org.apache.cassandra.io.compress.EncryptingLZ4Compressor\"," +
                            "\"parameters\":{\"cipher_algorithm\":\"AES\",\"secret_key_strength\":\"128\"," +
                            "\"key_provider\":\"com.datastax.bdp.cassandra.crypto.LocalFileSystemKeyProviderFactory\"," +
                            "\"secret_key_file\":\"/etc/dse/conf/system/k3y-s3cr3t\"}}}");
        Run encryptedRun = assertBlocked(run(encryptedNode), "HINTS_COMPRESSION");
        assertThat(encryptedRun.messages("HINTS_COMPRESSION"))
        .isEqualTo("DSE hints files written compressed or encrypted (the parameters their header records): " + encrypted + " (compression); HCD " +
                   "2.0 cannot convert such a file at its start: it keeps the file and does not deliver its hints");
        assertThat(encryptedRun.findings("HINTS_COMPRESSION").get(0).remedy).isEqualTo(run.findings("HINTS_COMPRESSION").get(0).remedy);
        assertThat(encryptedRun.codes(Finding.Severity.BLOCKING)).containsExactly("HINTS_COMPRESSION");
        assertThat(encryptedRun.report.upgradePath.upgradeSteps).anyMatch(s -> s.startsWith("Hints: compressed or encrypted DSE hints files, or " +
                                                                                            "hints_compression set: see HINTS_COMPRESSION; "));
        assertThat(encryptedRun.out).doesNotContain("EncryptingLZ4Compressor").doesNotContain("LocalFileSystemKeyProviderFactory")
                                     .doesNotContain("/etc/dse/conf/system").doesNotContain("k3y-s3cr3t");
        // the same file with system_info_encryption still on: the setting blocks too, as TDE_TABLE
        DseNodeFixture stillEncrypting = node();
        stillEncrypting.editYaml("dse.yaml", yaml -> section(yaml, "system_info_encryption").put("enabled", true));
        Files.copy(encryptedNode.hints.resolve(encrypted), stillEncrypting.hints.resolve(encrypted));
        Run stillEncryptingRun = assertBlocked(run(stillEncrypting), "HINTS_COMPRESSION");
        assertThat(stillEncryptingRun.messages("HINTS_COMPRESSION")).isEqualTo(encryptedRun.messages("HINTS_COMPRESSION"));
        assertThat(stillEncryptingRun.findings("HINTS_COMPRESSION").get(0).remedy).isEqualTo(run.findings("HINTS_COMPRESSION").get(0).remedy);
        assertThat(stillEncryptingRun.codes(Finding.Severity.BLOCKING)).containsExactlyInAnyOrder("TDE_TABLE", "HINTS_COMPRESSION");
        assertThat(stillEncryptingRun.messages("TDE_TABLE")).isEqualTo(SYSTEM_INFO_ENCRYPTION_MESSAGE);
        assertThat(stillEncryptingRun.findings("TDE_TABLE").get(0).remedy).isEqualTo(SYSTEM_INFO_ENCRYPTION_REMEDY);
        assertThat(stillEncryptingRun.out).doesNotContain("EncryptingLZ4Compressor").doesNotContain("LocalFileSystemKeyProviderFactory")
                                          .doesNotContain("/etc/dse/conf/system").doesNotContain("k3y-s3cr3t");

        // a header cut short, or damaged: not known, a warning
        DseNodeFixture cut = node();
        String damaged = UUID.randomUUID() + "-1790335633210-v1.hints";
        Files.write(cut.hints.resolve(damaged), new byte[]{ (byte) 0xff, (byte) 0xff, (byte) 0xff });
        Run cutRun = assertReady(run(cut));
        assertThat(cutRun.messages("HINTS_UNREADABLE")).contains(damaged + " (EOFException");
        Path flipped = cut.hints.resolve(damaged);
        writeDseHintsHeader(flipped, "{}");
        byte[] bytes = Files.readAllBytes(flipped);
        bytes[5] ^= 1;
        Files.write(flipped, bytes);
        assertThat(assertReady(run(cut)).messages("HINTS_UNREADABLE")).contains(damaged + " (IOException: the checksum of its header does not match)");
    }

    /** The header of a DSE hints file, as DSE writes it: version -1, a timestamp, a host id and the parameters given as JSON. */
    private static void writeDseHintsHeader(Path file, String parametersJson) throws IOException
    {
        byte[] parameters = parametersJson.getBytes(StandardCharsets.UTF_8);
        UUID hostId = UUID.randomUUID();
        CRC32 crc = new CRC32();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes))
        {
            out.writeInt(-1);
            FBUtilities.updateChecksumInt(crc, -1);
            for (long value : new long[]{ 1790335633208L, hostId.getMostSignificantBits(), hostId.getLeastSignificantBits() })
            {
                out.writeLong(value);
                FBUtilities.updateChecksumInt(crc, (int) (value & 0xFFFFFFFFL));
                FBUtilities.updateChecksumInt(crc, (int) (value >>> 32));
            }
            out.writeInt(parameters.length);
            FBUtilities.updateChecksumInt(crc, parameters.length);
            out.writeInt((int) crc.getValue());
            out.write(parameters);
            crc.update(parameters, 0, parameters.length);
            out.writeInt((int) crc.getValue());
        }
        Files.write(file, bytes.toByteArray());
    }

    /** Neither a password of DSE's configuration nor a malformed line holding one is printed or written to the report. */
    @Test
    @SuppressWarnings("unchecked")
    public void noSecretOfTheConfigurationIsReported() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> {
            Map<String, Object> encryption = (Map<String, Object>) yaml.get("server_encryption_options");
            encryption.put("internode_encryption", "all");
            encryption.put("keystore_password", "keystore-s3cr3t");
            encryption.put("truststore_password", "truststore-s3cr3t");
            section(yaml, "client_encryption_options").put("keystore_password", "client-s3cr3t");
        });
        Path json = temporaryFolder.getRoot().toPath().resolve("secrets.json");
        Run run = assertReady(run(json, node.args()));
        String report = new String(Files.readAllBytes(json), StandardCharsets.UTF_8);
        for (String secret : List.of("keystore-s3cr3t", "truststore-s3cr3t", "client-s3cr3t"))
            assertThat(run.out + run.err + report).as(secret).doesNotContain(secret);

        // a dse.yaml the YAML parser refuses on the line of a password: where, not the line
        DseNodeFixture malformed = node();
        Files.write(malformed.conf.resolve("dse.yaml"), "ldap_options:\n  search_password: \"ldap-s3cr3t\n  server_host: [\n".getBytes(StandardCharsets.UTF_8));
        Path malformedJson = temporaryFolder.getRoot().toPath().resolve("malformed.json");
        Run malformedRun = assertBlocked(run(malformedJson, malformed.args()), "UNREADABLE_INPUT");
        assertThat(malformedRun.messages("UNREADABLE_INPUT")).contains("cannot read dse.yaml").contains("not valid YAML").contains("at line ");
        String malformedReport = new String(Files.readAllBytes(malformedJson), StandardCharsets.UTF_8);
        assertThat(malformedRun.out + malformedRun.err + malformedReport).doesNotContain("ldap-s3cr3t");
        assertThat(malformedRun.report.upgradePath.upgradeSteps).contains("Security: review the access control warnings above; not checked " +
                                                                          "completely (not read: dse.yaml)");
    }

    /** The JSON report is written through a temporary file of its own, which does not stay; a report that cannot be written is an error. */
    @Test
    public void theReportIsWrittenWholeOrNotAtAll() throws IOException
    {
        DseNodeFixture node = node();
        Path directory = temporaryFolder.newFolder().toPath();
        Path json = directory.resolve("report.json");
        assertReady(run(json, node.args()));
        try (Stream<Path> files = Files.list(directory))
        {
            assertThat(files.map(p -> p.getFileName().toString()).collect(Collectors.toList())).containsExactly("report.json");
        }
        Run missing = run(directory.resolve("no-such-directory").resolve("report.json"), node.args());
        assertThat(missing.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(missing.err).contains("the inspection failed: ");
        // the findings are printed all the same
        assertThat(missing.out).contains("Result: READY");
        assertThat(directory.resolve("no-such-directory")).doesNotExist();
    }

    /** DSE's YAML 1.1 booleans: a quoted yes is true for DSE, so a setting the tool blocks on is not missed. */
    @Test
    public void aSettingDseReadsAsTrueBlocksWhateverItsSpelling() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("dse.yaml", yaml -> section(yaml, "authorization_options").put("allow_row_level_security", "yes"));
        assertBlocked(run(node), "ROW_LEVEL_ACCESS");
        assertThat(NodeLayout.isTrue(" On ")).isTrue();
        assertThat(NodeLayout.isTrue("1")).isFalse();
        assertThat(NodeLayout.isFalse("Off")).isTrue();
        assertThat(NodeLayout.isFalse(null)).isFalse();
    }

    @Test
    public void theOlderSearchIndexClassBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "indexes", KEYSPACES_KEY, INDEXES_KEY,
                       row("lab", "wide", "old_solr").with("kind", "CUSTOM")
                                                     .with("options", Map.of("class_name", "com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex", "target", "v")));
        Run run = assertBlocked(run(node), "SEARCH_INDEX");
        assertThat(run.messages("SEARCH_INDEX")).contains("lab.wide.old_solr (com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex)");
    }

    @Test
    public void aViewOnTieredStorageBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "views", KEYSPACES_KEY, List.of("view_name"),
                       row("lab", "wide_by_v").with("compaction", Map.of("class", SchemaInspector.TIERED_COMPACTION_STRATEGY)));
        Run run = assertBlocked(run(node), "TIERED_STORAGE");
        assertThat(run.messages("TIERED_STORAGE")).contains("materialized views using DSE tiered storage: lab.wide_by_v");
        assertThat(run.findings("TIERED_STORAGE").get(0).remedy).contains("ALTER MATERIALIZED VIEW ... WITH compaction = {'class': " +
                                                                          "'SizeTieredCompactionStrategy'}").doesNotContain("Unified");
    }

    @Test
    public void anEncryptedViewBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "views", KEYSPACES_KEY, List.of("view_name"),
                       row("lab", "wide_by_v").with("compression", Map.of("class", "org.apache.cassandra.io.compress.Encryptor",
                                                                          "cipher_algorithm", "AES/ECB/PKCS5Padding")));
        Run run = assertBlocked(run(node), "TDE_TABLE");
        assertThat(run.messages("TDE_TABLE"))
        .isEqualTo("encrypted (TDE) materialized views: lab.wide_by_v (compression class org.apache.cassandra.io.compress.Encryptor); the " +
                   "upgrade applies to clusters without encrypted tables or views");
        assertThat(run.findings("TDE_TABLE").get(0).remedy).startsWith("decrypt them on DSE (ALTER MATERIALIZED VIEW ... WITH compression = ");
    }

    @Test
    public void peersOfAReleaseThatIsNeitherDse6NorHcd20Block() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> {
            peers.get(0).put("release_version", "3.11.17");
            peers.get(1).put("release_version", "4.0.11.0");
        });
        Run run = assertBlocked(run(node), "PEER_RELEASE");
        assertThat(run.messages("PEER_RELEASE")).contains("(release 3.11.17)").contains("(release 4.0.11.0)");
        assertThat(run.report.peers).noneMatch(p -> p.upgraded);
        assertThat(run.codes(Finding.Severity.INFO)).doesNotContain("UPGRADED_PEERS");
    }

    @Test
    public void sstablesInATableDirectoryWithoutIdAndNewerBigVersionsBlock() throws IOException
    {
        DseNodeFixture node = node();
        Path legacy = Files.createDirectories(node.data.resolve("lab").resolve("legacy"));
        Files.write(legacy.resolve("mc-1-big-Data.db"), new byte[0]);
        Path table = Files.createDirectories(node.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        Files.write(table.resolve("nb-2-big-Data.db"), new byte[0]);
        Files.write(table.resolve("ad-3-bti-Data.db"), new byte[0]);
        Run run = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(run.messages("SSTABLE_FORMAT")).contains(NodeInspector.BIG_FORMAT + ": lab.legacy: 1 (mc-1-big-Data.db)")
                                                  .contains(NodeInspector.NEWER_BIG + ": lab.wide: 1 (nb-2-big-Data.db)");
        assertThat(run.messages("OLDER_TRIE")).contains("lab.wide: 1 (ad-3-bti-Data.db)");
        // the empty files have no TOC.txt either
        assertThat(run.messages("SSTABLE_WITHOUT_TOC")).contains("lab.legacy: 1 (mc-1-big-Data.db)");

        DseNodeFixture foreign = node();
        Path foreignTable = Files.createDirectories(foreign.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        Files.write(foreignTable.resolve("oa-1-big-Data.db"), new byte[0]);
        Run foreignRun = assertBlocked(run(foreign), "SSTABLE_FORMAT");
        assertThat(foreignRun.report.upgradePath.upgradeSteps)
            .anyMatch(s -> s.equals("Sstable formats: move the files DSE 6 did not write out of the data directories once their data is " +
                                    "safe (see SSTABLE_FORMAT)"));
    }

    @Test
    public void theRemedyOfAnSSTableWithoutTocNamesZeroCopyStreamingForBaAndBbSSTablesOnly() throws IOException
    {
        // big sstables only: an incomplete copy or a damaged disk, not DSE's zero-copy streaming
        DseNodeFixture node = node();
        Path legacy = Files.createDirectories(node.data.resolve("lab").resolve("legacy"));
        Files.write(legacy.resolve("mc-1-big-Data.db"), new byte[0]);
        Run bigOnly = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(bigOnly.messages("SSTABLE_WITHOUT_TOC")).isEqualTo(NodeInspector.WITHOUT_TOC + ": lab.legacy: 1 (mc-1-big-Data.db)");
        assertThat(bigOnly.findings("SSTABLE_WITHOUT_TOC").get(0).remedy)
        .isEqualTo("find out why they have none (an incomplete copy or restore, a damaged disk) before the upgrade");

        // a bb sstable among them, as DSE's zero-copy streaming writes it (no TOC.txt): the streaming is named
        Path table = Files.createDirectories(node.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        Files.write(table.resolve("bb-3-bti-Data.db"), new byte[0]);
        Files.write(table.resolve("bb-6-bti-Data.db"), new byte[0]);
        Run mixed = assertBlocked(run(node), "SSTABLE_FORMAT");
        // each one is named, to be matched with DSE's log line
        assertThat(mixed.messages("SSTABLE_WITHOUT_TOC")).contains("lab.legacy: 1 (mc-1-big-Data.db)")
                                                         .contains("lab.wide: 2 (bb-3-bti-Data.db, bb-6-bti-Data.db)");
        assertThat(mixed.findings("SSTABLE_WITHOUT_TOC").get(0).remedy)
        .startsWith("find out why they have none before the upgrade; a likely cause right after a repair or another streaming operation is ")
        .contains("DSE's zero-copy streaming");

        // up to WITHOUT_TOC_LISTED names per table, then how many more
        Path many = Files.createDirectories(node.data.resolve("lab").resolve("many-b6472ef0b80b11f1acb39d12a1a4d70a"));
        List<String> names = new ArrayList<>();
        for (int generation = 1; generation <= NodeInspector.WITHOUT_TOC_LISTED + 2; generation++)
        {
            Files.write(many.resolve("bb-" + generation + "-bti-Data.db"), new byte[0]);
            names.add("bb-" + generation + "-bti-Data.db");
        }
        // by generation: bb-1 to bb-20 named, the last two counted
        assertThat(assertBlocked(run(node), "SSTABLE_FORMAT").messages("SSTABLE_WITHOUT_TOC"))
        .contains("lab.many: " + (NodeInspector.WITHOUT_TOC_LISTED + 2) + " (" + String.join(", ", names.subList(0, NodeInspector.WITHOUT_TOC_LISTED)) +
                  " and 2 more)");
        try (Stream<Path> files = Files.list(many))
        {
            for (Path file : files.collect(Collectors.toList()))
                Files.delete(file);
        }
        Files.delete(many);

        // ba, which DSE also streams with zero-copy (from b, before c): DSE's streaming is named
        Files.delete(table.resolve("bb-3-bti-Data.db"));
        Files.delete(table.resolve("bb-6-bti-Data.db"));
        Files.write(table.resolve("ba-4-bti-Data.db"), new byte[0]);
        assertThat(assertBlocked(run(node), "SSTABLE_FORMAT").findings("SSTABLE_WITHOUT_TOC").get(0).remedy).contains("DSE's zero-copy streaming");

        // aa, which DSE does not stream with zero-copy: not named
        Files.delete(table.resolve("ba-4-bti-Data.db"));
        Files.write(table.resolve("aa-7-bti-Data.db"), new byte[0]);
        assertThat(assertBlocked(run(node), "SSTABLE_FORMAT").findings("SSTABLE_WITHOUT_TOC").get(0).remedy)
        .isEqualTo("find out why they have none (an incomplete copy or restore, a damaged disk) before the upgrade");

        // trie sstables DSE 6 does not write, a newer version or a non-numeric identifier: not DSE's streaming
        Files.delete(table.resolve("aa-7-bti-Data.db"));
        Files.write(table.resolve("ca-5-bti-Data.db"), new byte[0]);
        Run newer = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(newer.messages("SSTABLE_WITHOUT_TOC")).contains("lab.wide: 1 (ca-5-bti-Data.db)");
        assertThat(newer.findings("SSTABLE_WITHOUT_TOC").get(0).remedy)
        .isEqualTo("find out why they have none (an incomplete copy or restore, a damaged disk) before the upgrade");
        Files.delete(table.resolve("ca-5-bti-Data.db"));
        String uuid = new UUIDBasedSSTableId(TimeUUID.Generator.nextTimeUUID()).toString();
        Files.write(table.resolve("bb-" + uuid + "-bti-Data.db"), new byte[0]);
        Run nonNumeric = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(nonNumeric.messages("SSTABLE_WITHOUT_TOC")).contains("lab.wide: 1 (bb-" + uuid + "-bti-Data.db)");
        assertThat(nonNumeric.findings("SSTABLE_WITHOUT_TOC").get(0).remedy)
        .isEqualTo("find out why they have none (an incomplete copy or restore, a damaged disk) before the upgrade");
    }

    @Test
    public void unreadableInputsBlock() throws IOException
    {
        DseNodeFixture node = node();
        Files.write(node.metadata.resolve("nodes").resolve("peers"), new byte[]{ (byte) 0x92, (byte) 0xc1 });
        Run run = assertBlocked(run(node, "--data-dir", node.data.toString(), "--data-dir", node.root.resolve("no-such-directory").toString()),
                                "UNREADABLE_INPUT");
        assertThat(run.messages("UNREADABLE_INPUT")).contains("cannot read nodes/peers").contains("no-such-directory does not exist");
    }

    @Test
    public void aNodesDirectoryTheToolMayNotReadIsUnreadableNotAbsent() throws IOException
    {
        DseNodeFixture node = node();
        Path nodes = node.metadata.resolve("nodes");
        Files.setPosixFilePermissions(nodes, PosixFilePermissions.fromString("-w-------"));
        try
        {
            Assume.assumeFalse("the directory is still readable (the tests run as root)", Files.isReadable(nodes));
            Run run = assertBlocked(run(node), "INSPECTION_ERROR");
            assertThat(run.messages("INSPECTION_ERROR")).contains("the inspection of the metadata directory failed");
            assertThat(run.messages("UNREADABLE_INPUT")).contains("cannot read nodes/local").contains("cannot read nodes/peers");
            assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("NOT_A_DSE_NODE");
            // the other parts still ran
            assertThat(run.report.inputs).containsKey("live_sstable_bytes");
            assertThat(run.messages("VIEWS_HANDLED")).contains("lab.wide_by_v");
        }
        finally
        {
            Files.setPosixFilePermissions(nodes, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void schemaTheToolCannotReadBlocks() throws IOException
    {
        DseNodeFixture node = node();
        try (Stream<Path> tables = Files.list(node.data.resolve("system_schema")))
        {
            for (Path directory : tables.filter(p -> p.getFileName().toString().startsWith(SchemaInspector.HIDDEN_COLUMNS + '-')).collect(Collectors.toList()))
                deleteTree(directory);
        }
        Run noHiddenColumns = assertBlocked(run(node), "UNREADABLE_SCHEMA");
        assertThat(noHiddenColumns.messages("UNREADABLE_SCHEMA")).contains("materialized views but no data directory holds DSE's system_schema.hidden_columns");
        assertThat(noHiddenColumns.report.upgradePath.upgradeSteps).contains("Materialized views: not checked completely (not read: system_schema.hidden_columns)");

        DseNodeFixture corrupt = node();
        try (Stream<Path> files = Files.walk(corrupt.data.resolve("system_schema")))
        {
            for (Path data : files.filter(p -> p.toString().contains("/tables-") && p.getFileName().toString().endsWith("-Data.db")).collect(Collectors.toList()))
                Files.write(data, new byte[]{ 0, 1, 2, 3 });
        }
        Run corrupted = assertBlocked(run(corrupt), "UNREADABLE_SCHEMA");
        assertThat(corrupted.messages("UNREADABLE_SCHEMA")).contains("cannot read system_schema.tables");
        assertThat(corrupted.findings("UNREADABLE_SCHEMA").get(0).remedy).contains("if DSE is running, an sstable may have been compacted away");
    }

    @Test
    public void anSSTableWithoutTocIsReadWithoutWritingOne() throws IOException
    {
        DseNodeFixture node = node();
        Path toc;
        try (Stream<Path> files = Files.walk(node.data.resolve("system_schema")))
        {
            toc = files.filter(p -> p.toString().contains("/keyspaces-") && p.getFileName().toString().endsWith("-TOC.txt")).findFirst().orElseThrow();
        }
        Files.delete(toc);
        Map<String, String> before = node.fingerprint();
        Run run = assertReady(run(node));
        assertThat(node.fingerprint()).as("the tool wrote to the node's files").isEqualTo(before);
        assertThat(toc).doesNotExist();
        assertThat(run.messages("SSTABLE_WITHOUT_TOC")).contains(toc.getFileName().toString().replace("-TOC.txt", ""))
                                                       .startsWith("sstables without a TOC.txt (HCD 2.0 writes the missing one when it opens them; " +
                                                                   "neither DSE 6.8 nor DSE 6.9 writes it at its start): ");
        assertThat(run.findings("SSTABLE_WITHOUT_TOC").get(0).remedy)
        .isEqualTo("find out why they have none before the upgrade; a likely cause right after a repair or another streaming operation is " +
                   "DSE's zero-copy streaming, which writes an sstable without TOC.txt (DSE 6.8 and 6.9 alike; a partially streamed one also " +
                   "without Filter.db and Digest.crc32): if DSE's system.log or its archives have the line ZeroCopySSTableWriter ... Writing " +
                   "partial component DATA to <the sstable's Data.db file> (or, for an sstable streamed whole, ... Writing entire component " +
                   "DATA to <the sstable's Data.db file>) for every such sstable of the table (the finding names the first 20 of each table), " +
                   "the warning can be accepted (HCD 2.0 reads such an sstable); otherwise look for an incomplete copy or restore, or a " +
                   "damaged disk");
    }

    @Test
    public void theUpgradeMarkerIsANoteThatHoldsAfterTheLastRollbackToo() throws IOException
    {
        // the marker a rolled-back node keeps, whether other nodes still run HCD 2.0 or not: a note, not a finding that
        // changes the result, and its text does not say that the cluster is mixed
        DseNodeFixture node = node();
        Path marker = node.metadata.resolve(DseUpgradeMarker.FILE_NAME);
        Files.write(marker, "DSE 6 in-place upgrade in progress on this node since 2026-10-20T20:06:13.281Z\n".getBytes(StandardCharsets.UTF_8));
        Run run = assertReady(run(node));
        String text = "the upgrade marker " + marker + " exists: HCD 2.0 writes it at its first start in DSE_6 mode (or someone created it by " +
                      "hand), and a rollback leaves it, in the mixed phase and after the last rollback alike; keep it while any node runs HCD " +
                      "2.0 and, after the last rollback, while the node may be upgraded again; delete it once every node of the cluster runs " +
                      "HCD 2.0, before internode_compatibility_mode is set to NONE, or when the upgrade is abandoned for good; while it exists, " +
                      "HCD 2.0 refuses to start on this node with internode_compatibility_mode NONE";
        assertThat(run.findings("MIXED_PHASE_MARKER")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.INFO);
        assertThat(run.messages("MIXED_PHASE_MARKER")).isEqualTo(text + "; DSE does not read it");
        assertThat(run.codes(Finding.Severity.INFO)).doesNotContain("UPGRADED_PEERS");

        // an empty marker (made by hand, or left by a start killed between creating and writing it): the way back of a
        // failed drain is named
        Files.write(marker, new byte[0]);
        assertThat(assertReady(run(node)).messages("MIXED_PHASE_MARKER"))
        .isEqualTo(text + "; this one is empty: if HCD 2.0 has not started on this node since its drain, remove it only if this node never " +
                   "ran HCD 2.0 before (an empty marker may also be one made by hand at an earlier upgrade); DSE does not read it");

        Files.delete(marker);
        assertThat(assertReady(run(node)).codes(Finding.Severity.INFO)).doesNotContain("MIXED_PHASE_MARKER");
    }

    @Test
    public void anInterruptedRewriteOfNodesLocalBlocks() throws IOException
    {
        DseNodeFixture node = node();
        Path nodes = node.metadata.resolve("nodes");
        Files.move(nodes.resolve("local"), nodes.resolve("local.old"));
        Run run = assertBlocked(run(node), "NOT_A_DSE_NODE");
        assertThat(run.messages("NOT_A_DSE_NODE")).contains("but a nodes/local.old");
        assertThat(run.findings("NOT_A_DSE_NODE").get(0).remedy).contains("start DSE on this node once");
    }

    // ---- more warnings ----

    @Test
    public void aRowLevelResourceWithoutPermissionsIsNoGrant() throws IOException
    {
        // as the startup check reads it: a rows/ row with no permissions grants nothing; its delegation is still reported
        DseNodeFixture node = node();
        node.writeRows("system_auth", "role_permissions", ROLE_KEY, RESOURCE_KEY,
                       row("analyst", "rows/tenant1/data/lab/wide").with("grantables", Set.of("SELECT")));
        Run run = assertReady(run(node));
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("ROW_LEVEL_ACCESS");
        assertThat(run.messages("DELEGATION")).contains("role 'analyst' on rows/tenant1/data/lab/wide: [SELECT]");
    }

    @Test
    public void withoutAnyGrantTheSecurityStepNamesWhatWasRead() throws IOException
    {
        DseNodeFixture node = node();
        List<Object> roles = new ArrayList<>();
        for (UntypedResultSet.Row row : DseTableReader.read(List.of(new File(node.data)), "system_auth", "role_permissions",
                                                            ROLE_KEY, RESOURCE_KEY).rows)
            if (!roles.contains(row.getString("role")))
                roles.add(row.getString("role"));
        node.deletePartitions("system_auth", "role_permissions", ROLE_KEY, RESOURCE_KEY, roles.toArray());
        Run run = assertReady(run(node));
        assertThat(run.report.accessControls.rolePermissionsRows).isZero();
        assertThat(run.report.upgradePath.upgradeSteps).contains("Security: no DSE access control this software ignores was found in dse.yaml, the " +
                                                                 "schema and this node's replica of system_auth.role_permissions");
    }

    @Test
    public void anUnreadableRolePermissionsTableBlocks() throws IOException
    {
        DseNodeFixture node = node();
        corruptData(rolePermissionsDirectory(node));
        Run run = assertBlocked(run(node), "UNREADABLE_SCHEMA");
        assertThat(run.messages("UNREADABLE_SCHEMA")).contains("cannot read system_auth.role_permissions");
        assertThat(run.report.accessControls.rolePermissionsRows).isNull();
        assertThat(run.report.upgradePath.upgradeSteps).contains("Security: not checked completely (not read: system_auth.role_permissions)");
    }

    @Test
    public void anUnreadableOptionalSchemaTableLeavesTheStepsIncomplete() throws IOException
    {
        DseNodeFixture indexes = node();
        corruptData(schemaTableDirectory(indexes, "indexes"));
        Run run = assertBlocked(run(indexes), "UNREADABLE_SCHEMA");
        assertThat(run.messages("UNREADABLE_SCHEMA")).contains("cannot read system_schema.indexes");
        assertThat(run.report.upgradePath.upgradeSteps).contains("DSE core only: not checked completely (not read: system_schema.indexes)");

        DseNodeFixture hidden = node();
        corruptData(schemaTableDirectory(hidden, SchemaInspector.HIDDEN_COLUMNS));
        Run hiddenRun = assertBlocked(run(hidden), "UNREADABLE_SCHEMA");
        assertThat(hiddenRun.findings("UNREADABLE_SCHEMA")).hasSize(1);
        assertThat(hiddenRun.messages("UNREADABLE_SCHEMA")).contains("cannot read system_schema." + SchemaInspector.HIDDEN_COLUMNS)
                                                           .doesNotContain("no data directory holds");
        assertThat(hiddenRun.report.upgradePath.upgradeSteps).contains("Materialized views: not checked completely (not read: system_schema." +
                                                                       SchemaInspector.HIDDEN_COLUMNS + ')');
    }

    @Test
    public void anSSTableOfTheBigFormatIsNotOpened() throws IOException
    {
        // opening a big sstable loads its summary, which may delete Summary.db or rewrite TOC.txt: the tool refuses it
        DseNodeFixture node = node();
        node.writeRows(BigFormat.getInstance(), "system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("big_written").with("durable_writes", true).with("replication", simpleStrategy()));
        Map<String, String> before = node.fingerprint();
        Run run = assertBlocked(run(node), "UNREADABLE_SCHEMA");
        assertThat(node.fingerprint()).as("the tool wrote to the node's files").isEqualTo(before);
        assertThat(run.messages("UNREADABLE_SCHEMA")).contains("format, which this tool does not read: run nodetool upgradesstables on DSE first");
        assertThat(run.codes(Finding.Severity.BLOCKING)).contains("SSTABLE_FORMAT");
    }

    @Test
    public void aViewDseListsNoHiddenColumnForIsANote() throws IOException
    {
        DseNodeFixture node = node();
        node.deletePartitions("system_schema", SchemaInspector.HIDDEN_COLUMNS, KEYSPACES_KEY, COLUMNS_KEY, "lab");
        Run run = assertReady(run(node));
        assertThat(run.messages("VIEW_OLD_FORMAT")).contains("lab.wide_by_v");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Materialized views: 1 view(s), none to drop");
    }

    @Test
    public void viewsHcd20CannotLoadAreToDrop() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "views", KEYSPACES_KEY, List.of("view_name"),
                       view("lab", "no_columns", "wide"), view("lab", "orphan", "gone"));
        node.writeRows("system_schema", "columns", KEYSPACES_KEY, COLUMNS_KEY,
                       column("lab", "orphan", "pk", "partition_key", 0, "int"), column("lab", "orphan", "v", "clustering", 0, "text"));
        Run run = assertBlocked(run(node), "UNLOADABLE_TABLE");
        assertThat(run.messages("UNLOADABLE_TABLE")).contains("lab.no_columns: no column in system_schema.columns")
                                                    .contains("lab.orphan: its base table lab.gone cannot be loaded");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Materialized views: drop lab.no_columns, lab.orphan before the upgrade");
    }

    @Test
    public void anIndexClassThatIsNoIndexBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "indexes", KEYSPACES_KEY, INDEXES_KEY,
                       row("lab", "wide", "not_an_index").with("kind", "CUSTOM").with("options", Map.of("class_name", "java.lang.String", "target", "v")));
        Run run = assertBlocked(run(node), "UNLOADABLE_CLASS");
        assertThat(run.messages("UNLOADABLE_CLASS")).contains("lab.wide.not_an_index (java.lang.String is not an index class)");
    }

    @Test
    public void triggersWarn() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "triggers", KEYSPACES_KEY, List.of("table_name", "trigger_name"),
                       row("lab", "wide", "audit\u0007log").with("options", Map.of("class", "com.example.AuditTrigger")));
        Run run = assertReady(run(node));
        assertThat(run.messages("TRIGGER")).isEqualTo("triggers: lab.wide.audit\\u0007log (class com.example.AuditTrigger)");
        // a character beyond the basic multilingual plane takes eight digits, so that no digit after it reads as its own
        assertThat(SchemaInspector.safe("tag\uDB40\uDC01" + "1")).isEqualTo("tag\\U000e00011");
    }

    @Test
    public void theToolAsksForAFlushOfTheTablesItReadsWhateverTheCommitLog() throws IOException
    {
        // a running DSE node always has a commit log segment, and so has a drained one: the segments tell nothing
        DseNodeFixture node = node();
        DseFixtures.unpackCommitLogSegment(DseVersion.DSE_6_8_65, node.commitLog);
        Run run = assertReady(run(node));
        assertThat(run.messages("SSTABLES_ONLY")).endsWith("run nodetool flush system_schema && nodetool flush system_auth " +
                                                           "(two commands, one keyspace each) right before the tool");
        assertThat(run.codes(Finding.Severity.WARNING)).doesNotContain("COMMITLOG_VERSION");
    }

    @Test
    public void aSchemaWithoutKeyspaceBlocks() throws IOException
    {
        DseNodeFixture node = node();
        @SuppressWarnings("unchecked")
        List<String> keyspaces = (List<String>) ((Map<String, Object>) assertReady(run(node)).report.inputs.get("schema")).get("keyspace_names");
        node.deletePartitions("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), keyspaces.toArray());
        Run run = assertBlocked(run(node), "UNREADABLE_SCHEMA");
        assertThat(run.messages("UNREADABLE_SCHEMA")).contains("system_schema.keyspaces holds no row");
    }

    @Test
    public void aMissingRolePermissionsTableWarnsAndAnEmptyOneIsANote() throws IOException
    {
        DseNodeFixture missing = node();
        deleteTree(rolePermissionsDirectory(missing));
        Run run = assertReady(run(missing));
        assertThat(run.messages("NO_ROLE_PERMISSIONS")).contains("no data directory holds system_auth.role_permissions");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("NO_ROLE_PERMISSIONS");

        DseNodeFixture empty = node();
        Path directory = rolePermissionsDirectory(empty);
        try (Stream<Path> files = Files.list(directory))
        {
            for (Path file : files.collect(Collectors.toList()))
                Files.delete(file);
        }
        Run emptyRun = assertReady(run(empty));
        assertThat(emptyRun.codes(Finding.Severity.INFO)).contains("NO_ROLE_PERMISSIONS");
        assertThat(emptyRun.codes(Finding.Severity.WARNING)).doesNotContain("NO_ROLE_PERMISSIONS");
        assertThat(emptyRun.report.accessControls.rolePermissionsRows).isZero();
    }

    @Test
    public void aSchemaTableInADirectoryWithoutIdIsRead() throws IOException
    {
        DseNodeFixture node = node();
        Path keyspaces;
        try (Stream<Path> tables = Files.list(node.data.resolve("system_schema")))
        {
            keyspaces = tables.filter(p -> p.getFileName().toString().startsWith("keyspaces-")).findFirst().orElseThrow();
        }
        Files.move(keyspaces, keyspaces.resolveSibling("keyspaces"));
        Run run = assertReady(run(node));
        @SuppressWarnings("unchecked")
        List<String> names = (List<String>) ((Map<String, Object>) run.report.inputs.get("schema")).get("keyspace_names");
        assertThat(names).contains("lab", "system_auth", "solr_admin");
    }

    @Test
    public void aTransitionalAuthenticationModeWarns() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("dse.yaml", yaml -> section(yaml, "authentication_options").put("transitional_mode", "permissive"));
        Run run = assertReady(run(node));
        assertThat(run.messages("TRANSITIONAL_MODE")).contains("transitional_mode is permissive");
    }

    @Test
    public void moreSnitchesAreMapped() throws IOException
    {
        DseNodeFixture noDelegate = node();
        noDelegate.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
        Run unknown = assertReady(run(noDelegate));
        assertThat(unknown.messages("SNITCH")).contains("dse.yaml names no delegated_snitch").doesNotContain("null");

        DseNodeFixture gossiping = node();
        gossiping.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
        gossiping.editYaml("dse.yaml", yaml -> yaml.put("delegated_snitch", "org.apache.cassandra.locator.GossipingPropertyFileSnitch"));
        Files.write(gossiping.conf.resolve("cassandra-rackdc.properties"), "dc=dc1\nrack=rack9\n".getBytes(StandardCharsets.UTF_8));
        Run delegated = assertReady(run(gossiping));
        assertThat(delegated.messages("IDENTITY_MISMATCH")).contains("rack=rack9 but nodes/local records dc1/rack1");

        DseNodeFixture custom = node();
        custom.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.example.NoSuchSnitch"));
        Run customRun = assertReady(run(custom));
        assertThat(customRun.messages("SNITCH")).contains("the snitch com.example.NoSuchSnitch of DSE's cassandra.yaml is not in HCD 2.0");
    }

    @Test
    public void aDelegatedSnitchHcd20DoesNotHaveWarns() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
        node.editYaml("dse.yaml", yaml -> yaml.put("delegated_snitch", "com.example.NoSuchSnitch"));
        Run run = assertReady(run(node));
        assertThat(run.messages("SNITCH")).contains("it delegates to com.example.NoSuchSnitch").contains("which HCD 2.0 does not have either");
        assertThat(run.findings("SNITCH").get(0).remedy).contains("put the jar of com.example.NoSuchSnitch");
    }

    @Test
    public void aMissingRackdcFileIsANoteForASnitchThatDoesNotReadIt() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> yaml.remove("endpoint_snitch"));
        Files.delete(node.conf.resolve("cassandra-rackdc.properties"));
        Run run = assertReady(run(node));
        assertThat(run.findings("UNREADABLE_INPUT")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.INFO);
        assertThat(run.messages("UNREADABLE_INPUT")).contains("(DSE runs with its DseSimpleSnitch, which does not read it)").doesNotContain("null");

        DseNodeFixture gossiping = node();
        Files.delete(gossiping.conf.resolve("cassandra-rackdc.properties"));
        Run gossipingRun = assertReady(run(gossiping));
        assertThat(gossipingRun.findings("UNREADABLE_INPUT")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
    }

    @Test
    public void onlyBigSstablesAskForUpgradesstablesAlone() throws IOException
    {
        DseNodeFixture node = node();
        Path table = Files.createDirectories(node.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        Files.write(table.resolve("mc-1-big-Data.db"), new byte[0]);
        Files.write(table.resolve("mc-1-big-TOC.txt"), new byte[0]);
        Run run = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Sstable formats: run nodetool upgradesstables on DSE first");
        assertThat(run.codes(Finding.Severity.WARNING)).doesNotContain("SSTABLE_WITHOUT_TOC");
    }

    @Test
    public void anSSTableBeingWrittenIsNoFinding() throws IOException
    {
        DseNodeFixture node = node();
        try (SSTableTxnWriter writer = node.startWriting("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                                                         row("being_written").with("durable_writes", true).with("replication", simpleStrategy())))
        {
            List<Path> inProgress;
            try (Stream<Path> files = Files.walk(node.data.resolve("system_schema")))
            {
                inProgress = files.filter(p -> p.getFileName().toString().contains("-900000-")).collect(Collectors.toList());
            }
            assertThat(inProgress).as("the sstable being written").anyMatch(p -> p.toString().endsWith("-Data.db"))
                                  .noneMatch(p -> p.toString().endsWith("-TOC.txt"));
            Run run = assertReady(run(node));
            assertThat(run.codes(Finding.Severity.WARNING)).doesNotContain("SSTABLE_WITHOUT_TOC");
            @SuppressWarnings("unchecked")
            List<String> names = (List<String>) ((Map<String, Object>) run.report.inputs.get("schema")).get("keyspace_names");
            assertThat(names).doesNotContain("being_written");
            writer.abort();
        }
    }

    @Test
    public void aSolrDataDirectoryIsNotCountedAsSstables() throws IOException
    {
        DseNodeFixture node = node();
        @SuppressWarnings("unchecked")
        Map<String, Object> before = (Map<String, Object>) assertReady(run(node)).report.inputs.get("live_sstable_bytes");
        // directly in the directory of the core: walked as a table directory, its files would count
        Path core = Files.createDirectories(node.data.resolve("solr.data").resolve("lab.wide"));
        Files.write(core.resolve("_0.cfs"), new byte[1 << 20]);
        Run run = assertBlocked(run(node), "SOLR_DATA");
        assertThat(run.report.inputs.get("live_sstable_bytes")).isEqualTo(before);
    }

    @Test
    public void transactionLogsTheToolCannotReadFallBackToEverySstable() throws IOException
    {
        DseNodeFixture node = node();
        Path table = Files.createDirectories(node.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        Files.write(table.resolve("mc-1-big-Data.db"), new byte[0]);
        Files.write(table.resolve("mc-1-big-TOC.txt"), new byte[0]);
        // an operation this build does not know
        Files.write(table.resolve("bb_txn_dsenodesync_" + TimeUUID.Generator.nextTimeUUID() + ".log"), "ADD:[x,0,1][2]\n".getBytes(StandardCharsets.UTF_8));
        Run run = assertBlocked(run(node), "SSTABLE_FORMAT");
        assertThat(run.messages("SSTABLE_FORMAT")).contains("lab.wide: 1 (mc-1-big-Data.db)");
        assertThat(run.messages("TXN_LOG_UNREADABLE")).contains(table.toString()).contains("dsenodesync");
        assertThat(run.report.upgradePath.upgradeSteps).noneMatch(s -> s.contains("not checked"));
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("INSPECTION_ERROR", "UNREADABLE_INPUT");

        // a log of a known operation with a damaged record: its parts are ignored, the scan goes on
        DseNodeFixture damaged = node();
        Path damagedTable = Files.createDirectories(damaged.data.resolve("lab").resolve("wide-a6472ef0b80b11f1acb39d12a1a4d70a"));
        Files.write(damagedTable.resolve("mc-1-big-Data.db"), new byte[0]);
        Files.write(damagedTable.resolve("mc-1-big-TOC.txt"), new byte[0]);
        Files.write(damagedTable.resolve("bb_txn_compaction_" + TimeUUID.Generator.nextTimeUUID() + ".log"), "not a record\n".getBytes(StandardCharsets.UTF_8));
        Run damagedRun = assertBlocked(run(damaged), "SSTABLE_FORMAT");
        // the damaged record is ignored (OnTxnErr.IGNORE): no fallback
        assertThat(damagedRun.findings("TXN_LOG_UNREADABLE")).isEmpty();
        assertThat(damagedRun.messages("SSTABLE_FORMAT")).contains("lab.wide: 1 (mc-1-big-Data.db)");
        assertThat(damagedRun.codes(Finding.Severity.BLOCKING)).doesNotContain("INSPECTION_ERROR", "UNREADABLE_INPUT");
        assertThat(damagedRun.report.upgradePath.upgradeSteps).noneMatch(s -> s.contains("not checked"));
    }

    @Test
    public void dataDirectoriesNotInspectedCompletelyLeaveTheFormatsUnchecked() throws IOException
    {
        DseNodeFixture node = node();
        Run run = assertBlocked(run(node, "--data-dir", node.root.resolve("no-such-directory").toString()), "UNREADABLE_INPUT");
        assertThat(run.report.upgradePath.upgradeSteps)
            .contains("Sstable formats: not checked (the data directories could not be inspected completely)");
    }

    @Test
    public void theRackdcNoteTellsWhyTheFileDoesNotMatter() throws IOException
    {
        DseNodeFixture unreadableYaml = node();
        Files.delete(unreadableYaml.conf.resolve("cassandra.yaml"));
        Files.delete(unreadableYaml.conf.resolve("cassandra-rackdc.properties"));
        // without cassandra.yaml the directories are given, so that nothing of this machine is read
        Run noYaml = run(unreadableYaml, "--data-dir", unreadableYaml.data.toString(), "--metadata-dir", unreadableYaml.metadata.toString(),
                         "--commitlog-dir", unreadableYaml.commitLog.toString(), "--hints-dir", unreadableYaml.hints.toString());
        assertThat(noYaml.findings("UNREADABLE_INPUT")).anyMatch(f -> f.severity == Finding.Severity.INFO &&
                                                                        f.message.contains("(the snitch DSE runs with is not known: cassandra.yaml cannot be read)"));

        DseNodeFixture noDelegate = node();
        noDelegate.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
        Files.delete(noDelegate.conf.resolve("cassandra-rackdc.properties"));
        Run unknown = assertReady(run(noDelegate));
        assertThat(unknown.messages("UNREADABLE_INPUT")).contains("(the snitch DSE delegates to is not known: dse.yaml names no delegated_snitch)");

        DseNodeFixture noDseYaml = node();
        noDseYaml.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
        Files.delete(noDseYaml.conf.resolve("dse.yaml"));
        Files.delete(noDseYaml.conf.resolve("cassandra-rackdc.properties"));
        Run unreadableDse = run(noDseYaml);
        assertThat(unreadableDse.messages("SNITCH")).contains("and dse.yaml cannot be read, so the snitch DSE delegates to is not known");
        assertThat(unreadableDse.messages("UNREADABLE_INPUT")).contains("(the snitch DSE delegates to is not known: dse.yaml cannot be read)");

        DseNodeFixture propertyFile = node();
        propertyFile.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "PropertyFileSnitch"));
        Files.delete(propertyFile.conf.resolve("cassandra-rackdc.properties"));
        Run notUsed = assertReady(run(propertyFile));
        assertThat(notUsed.messages("UNREADABLE_INPUT")).contains("(not used by the snitch PropertyFileSnitch)");
    }

    @Test
    public void anEmptyDseYamlIsReadNotUnreadable() throws IOException
    {
        DseNodeFixture node = node();
        Files.write(node.conf.resolve("dse.yaml"), new byte[0]);
        Run run = run(node);
        // DseAuthorizer without authorization_options: DSE checks no permission
        assertThat(run.report.accessControls.authorizationEnabled).isFalse();
        assertThat(run.findings("UNREADABLE_INPUT")).noneMatch(f -> f.message.contains("dse.yaml"));
    }

    @Test
    public void theMajorReleaseOfUpgradedPeersIsThatOfThisBuild()
    {
        assertThat(NodeInspector.upgradedMajor("6.1.0")).isEqualTo(6);
        assertThat(NodeInspector.upgradedMajor("Unknown")).as("a build that does not know its release").isEqualTo(NodeInspector.HCD_20_MAJOR);
        assertThat(NodeInspector.upgradedMajor()).isEqualTo(NodeInspector.upgradedMajor(FBUtilities.getReleaseVersionString()));
    }

    @Test
    public void aWrongNodetoolTimeoutIsRefused() throws IOException
    {
        DseNodeFixture node = node();
        for (String timeout : List.of("0", "-1", "abc"))
        {
            Run run = run(node, "--nodetool", "/bin/false", "--nodetool-timeout", timeout);
            assertThat(run.exitCode).as(timeout).isEqualTo(DsePreflight.EXIT_ERROR);
            assertThat(run.err).contains("--nodetool-timeout must be a number of seconds, 1 or more: " + timeout);
        }
    }

    private static Path rolePermissionsDirectory(DseNodeFixture node) throws IOException
    {
        try (Stream<Path> tables = Files.list(node.data.resolve("system_auth")))
        {
            return tables.filter(p -> p.getFileName().toString().startsWith("role_permissions-")).findFirst().orElseThrow();
        }
    }

    @Test
    public void snitchesHcd20DoesNotHaveAreMapped() throws IOException
    {
        DseNodeFixture absent = node();
        absent.editYaml("cassandra.yaml", yaml -> yaml.remove("endpoint_snitch"));
        Run noSnitch = assertReady(run(absent));
        assertThat(noSnitch.messages("SNITCH")).contains("sets no endpoint_snitch");
        assertThat(noSnitch.report.upgradePath.settings).containsEntry("endpoint_snitch", "GossipingPropertyFileSnitch (was not set, DseSimpleSnitch)");

        DseNodeFixture delegate = node();
        delegate.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
        delegate.editYaml("dse.yaml", yaml -> yaml.put("delegated_snitch", "org.apache.cassandra.locator.Ec2Snitch"));
        Run delegated = assertReady(run(delegate));
        assertThat(delegated.messages("SNITCH")).contains("it delegates to org.apache.cassandra.locator.Ec2Snitch");
        assertThat(delegated.report.upgradePath.settings)
            .containsEntry("endpoint_snitch", "org.apache.cassandra.locator.Ec2Snitch (was com.datastax.bdp.snitch.DseDelegateSnitch)");
        // Ec2Snitch does not read cassandra-rackdc.properties' dc and rack: no comparison with nodes/local
        assertThat(delegated.codes(Finding.Severity.WARNING)).doesNotContain("IDENTITY_MISMATCH");
    }

    /**
     * EC2_NAMING: DSE names data centers and racks with the legacy EC2 names only, so every Ec2Snitch and
     * Ec2MultiRegionSnitch node is warned, whatever its names, the snitch named directly or delegated to, with the
     * dc_suffix of DSE's cassandra-rackdc.properties carried when it sets one.
     */
    @Test
    public void ec2SnitchesWarnOfTheLegacyNames() throws IOException
    {
        for (String snitch : List.of("Ec2Snitch", "org.apache.cassandra.locator.Ec2MultiRegionSnitch"))
        {
            DseNodeFixture direct = node();
            direct.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", snitch));
            Files.write(direct.conf.resolve("cassandra-rackdc.properties"), "dc_suffix=_live\n".getBytes(StandardCharsets.UTF_8));
            Run directRun = assertReady(run(direct));
            String names = "here dc=" + directRun.report.node.dataCenter + ", rack=" + directRun.report.node.rack + " from nodes/local";
            assertThat(directRun.findings("EC2_NAMING")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
            assertThat(directRun.messages("EC2_NAMING"))
            .isEqualTo("DSE derives the data center and rack of " + snitch + " with the legacy EC2 names only (" + names + "); HCD 2.0 defaults " +
                       "to ec2_naming_scheme=standard and refuses to start on a ring with legacy names");
            assertThat(directRun.findings("EC2_NAMING").get(0).remedy)
            .isEqualTo("set ec2_naming_scheme=legacy in the cassandra-rackdc.properties of HCD 2.0 on every node, and carry dc_suffix=_live " +
                       "(DSE's value)");
            assertThat(directRun.report.upgradePath.settings.get("cassandra-rackdc.properties"))
            .isEqualTo("ec2_naming_scheme=legacy, dc_suffix=_live (DSE's names here: dc=" + directRun.report.node.dataCenter + ", rack=" +
                       directRun.report.node.rack + ')');
            assertThat(directRun.report.upgradePath.upgradeSteps)
            .contains("EC2 names: set ec2_naming_scheme=legacy, dc_suffix=_live in the cassandra-rackdc.properties of HCD 2.0 " +
                      "on every node (see EC2_NAMING)");

            DseNodeFixture delegate = node();
            delegate.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", "com.datastax.bdp.snitch.DseDelegateSnitch"));
            delegate.editYaml("dse.yaml", yaml -> yaml.put("delegated_snitch", snitch));
            Files.write(delegate.conf.resolve("cassandra-rackdc.properties"), "dc=dc1\nrack=rack1\n".getBytes(StandardCharsets.UTF_8));
            Run delegated = assertReady(run(delegate));
            assertThat(delegated.messages("EC2_NAMING")).startsWith("DSE derives the data center and rack of " + snitch + " with the legacy EC2 names only");
            assertThat(delegated.findings("EC2_NAMING").get(0).remedy)
            .isEqualTo("set ec2_naming_scheme=legacy in the cassandra-rackdc.properties of HCD 2.0 on every node, and carry dc_suffix if DSE's " +
                       "cassandra-rackdc.properties sets one (it sets none here)");
            assertThat(delegated.report.upgradePath.settings.get("cassandra-rackdc.properties")).startsWith("ec2_naming_scheme=legacy (DSE's names here: ");

            // a dc_suffix ending with a space: java.util.Properties keeps it, and so does the data center name, so it is shown quoted
            DseNodeFixture spaced = node();
            spaced.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", snitch));
            Files.write(spaced.conf.resolve("cassandra-rackdc.properties"), "dc_suffix= _live \n".getBytes(StandardCharsets.UTF_8));
            Run spacedRun = assertReady(run(spaced));
            assertThat(spacedRun.findings("EC2_NAMING").get(0).remedy)
            .contains("and carry dc_suffix='_live ' (DSE's value; quoted here because its spaces are part of the data center name: copy DSE's " +
                      "dc_suffix line as it is)");
            assertThat(spacedRun.report.upgradePath.settings.get("cassandra-rackdc.properties"))
            .startsWith("ec2_naming_scheme=legacy, dc_suffix='_live ' (quoted: copy DSE's dc_suffix line as it is; DSE's names here: ");
            assertThat(spacedRun.report.upgradePath.upgradeSteps)
            .contains("EC2 names: set ec2_naming_scheme=legacy, dc_suffix='_live ' in the cassandra-rackdc.properties of " +
                      "HCD 2.0 on every node (quoted: copy DSE's dc_suffix line as it is; see EC2_NAMING)");

            // without the file, dc_suffix is not known: a warning, and the remedy says so
            DseNodeFixture unreadable = node();
            unreadable.editYaml("cassandra.yaml", yaml -> yaml.put("endpoint_snitch", snitch));
            Files.delete(unreadable.conf.resolve("cassandra-rackdc.properties"));
            Run unreadableRun = assertReady(run(unreadable));
            assertThat(unreadableRun.findings("UNREADABLE_INPUT")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
            assertThat(unreadableRun.messages("UNREADABLE_INPUT")).contains("the dc_suffix " + snitch + " adds to the data center is not known");
            assertThat(unreadableRun.findings("EC2_NAMING").get(0).remedy)
            .contains("and carry dc_suffix if DSE's cassandra-rackdc.properties sets one (it cannot be read here: see UNREADABLE_INPUT)");
        }
        // any other snitch: no warning
        assertThat(assertReady(run(node())).codes(Finding.Severity.WARNING)).doesNotContain("EC2_NAMING");
    }

    /**
     * INTERNODE_TLS on DSE 6.8.65 (the release of the fixture node, which does not have enable_legacy_ssl_storage_port):
     * whenever DSE encrypts internode traffic, a warning with the settings to carry and no change asked of DSE; nothing
     * with internode_encryption none (the fixture node as DSE left it).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void internodeTlsIsWarnedWithTheSettingsToCarry() throws IOException
    {
        Run plain = assertReady(run(node()));
        assertThat(plain.findings("INTERNODE_TLS")).isEmpty();
        assertThat(plain.report.upgradePath.settings).containsEntry("server_encryption_options.internode_encryption", "none")
                                                     .doesNotContainKey("server_encryption_options.legacy_ssl_storage_port_enabled");
        assertThat(plain.report.upgradePath.upgradeSteps).noneMatch(s -> s.contains("INTERNODE_TLS") || s.contains("internode TLS"));

        DseNodeFixture all = node();
        all.editYaml("cassandra.yaml", yaml -> {
            ((Map<String, Object>) yaml.get("server_encryption_options")).put("internode_encryption", "all");
            yaml.put("ssl_storage_port", 7443);
        });
        Run allRun = assertReady(run(all));
        assertThat(allRun.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
        assertThat(allRun.messages("INTERNODE_TLS"))
        .isEqualTo("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options) on DSE 6.8.65; " + TLS_VALIDATED);
        assertThat(allRun.findings("INTERNODE_TLS").get(0).remedy)
        .isEqualTo("DSE 6.8.65 does not have enable_legacy_ssl_storage_port and always listens with TLS on ssl_storage_port, so DSE 6.8 " +
                   "needs no TLS change for the upgrade; on HCD 2.0, carry the keys DSE sets in server_encryption_options into the " +
                   "cassandra.yaml of HCD 2.0 under the names it accepts (internode_encryption: all, optional, require_client_auth, " +
                   "require_endpoint_verification, keystore, keystore_password, truststore, truststore_password, store_type, protocol, " +
                   "accepted_protocols, cipher_suites, algorithm); give keystore: resources/dse/conf/.keystore and truststore: " +
                   "resources/dse/conf/.truststore absolute paths (a relative path is resolved from the directory the process starts in, " +
                   "which need not be the same for DSE and HCD 2.0); carry ssl_storage_port: 7443 (check DSE's JVM options for a " +
                   "-Dcassandra.ssl_storage_port, which must be absent or give that same ssl_storage_port (required: DSE honours it over " +
                   "cassandra.yaml, and this tool reads cassandra.yaml only; check it by hand: the command line of the running DSE process " +
                   "lists the -D options it was started with)); leave legacy_ssl_storage_port_enabled false and do not carry " +
                   "enable_legacy_ssl_storage_port, the old name of that HCD 2.0 setting: in DSE_6 mode HCD 2.0 listens with TLS on " +
                   "ssl_storage_port for the DSE nodes and dials each one on the port its release listens on");
        assertThat(allRun.report.upgradePath.settings).containsEntry("ssl_storage_port", "7443")
                                                      .containsEntry("server_encryption_options.internode_encryption", "all")
                                                      .containsEntry("server_encryption_options.legacy_ssl_storage_port_enabled",
                                                                     "false (in DSE_6 mode HCD 2.0 listens on ssl_storage_port for DSE without it)");
        assertThat(allRun.report.upgradePath.upgradeSteps)
        .contains("Internode TLS: carry the keys of server_encryption_options HCD 2.0 accepts and " +
                  "ssl_storage_port, leave legacy_ssl_storage_port_enabled false (see INTERNODE_TLS)")
        .noneMatch(s -> s.startsWith("Internode TLS on DSE 6.9"));

        // DSE's store types, absolute paths, and no ssl_storage_port in DSE's cassandra.yaml: the default of both
        DseNodeFixture dc = node();
        dc.editYaml("cassandra.yaml", yaml -> {
            Map<String, Object> encryption = (Map<String, Object>) yaml.get("server_encryption_options");
            encryption.put("internode_encryption", "dc");
            encryption.put("keystore_type", "JKS");
            encryption.put("truststore_type", "JKS");
            encryption.put("keystore", "/etc/dse/keystore");
            encryption.put("truststore", "/etc/dse/truststore");
            yaml.remove("ssl_storage_port");
        });
        Run dcRun = assertReady(run(dc));
        assertThat(dcRun.messages("INTERNODE_TLS")).startsWith("DSE's cassandra.yaml sets internode_encryption: dc (server_encryption_options)");
        assertThat(dcRun.findings("INTERNODE_TLS").get(0).remedy)
        .contains("; leave out keystore_type, truststore_type, which HCD 2.0 does not have and refuses to start on (HCD 2.0 has one " +
                  "store_type for both stores: set store_type: JKS); carry ssl_storage_port: 7001 (check DSE's JVM options for a " +
                  "-Dcassandra.ssl_storage_port, which must be absent or give that same ssl_storage_port")
        .doesNotContain("absolute path");
        assertThat(dcRun.report.upgradePath.settings).containsEntry("ssl_storage_port", "7001 (DSE's cassandra.yaml sets none: the default of both products)");
    }

    /**
     * INTERNODE_TLS by DSE release: on DSE 6.9.7 and later with internode encryption on, DSE's
     * enable_legacy_ssl_storage_port must be true, so false or absent blocks; true, or a release
     * without the setting (DSE 6.8, 6.9.0 to 6.9.6), is a warning with the settings to carry; no encryption, nothing.
     */
    @Test
    public void internodeTlsOnDse697AndLaterRequiresDsesLegacyTlsPort() throws IOException
    {
        // DSE 6.9.26 with the setting true: a warning, not blocking
        Run withPort = assertReady(run(nodeWithInternodeTls("6.9.26", "rack", true)));
        assertThat(withPort.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
        assertThat(withPort.messages("INTERNODE_TLS"))
        .isEqualTo("DSE's cassandra.yaml sets internode_encryption: rack (server_encryption_options) on DSE 6.9.26, with " +
                   "enable_legacy_ssl_storage_port: true, as required; " + TLS_VALIDATED);
        assertThat(withPort.findings("INTERNODE_TLS").get(0).remedy)
        .startsWith("keep enable_legacy_ssl_storage_port: true on every DSE node (required), and check that DSE was " +
                    "restarted since it was set: ss -tln lists ssl_storage_port as well as storage_port" + TLS_CARRY)
        .doesNotContain("change nothing on DSE");
        assertThat(withPort.report.upgradePath.upgradeSteps)
        .contains("Internode TLS on DSE 6.9: enable_legacy_ssl_storage_port: true is set in this node's cassandra.yaml; check that " +
                  "DSE was restarted since (ss -tln lists ssl_storage_port); the report of every DSE node checks its own file, and every " +
                  "one of them must pass");

        // DSE 6.9.26 with the setting false, DSE's shipped value: blocking
        Run falsePort = assertBlocked(run(nodeWithInternodeTls("6.9.26", "all", false)), "INTERNODE_TLS");
        assertThat(falsePort.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.BLOCKING);
        assertThat(falsePort.messages("INTERNODE_TLS"))
        .isEqualTo("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options) on DSE 6.9.26, and sets " +
                   "enable_legacy_ssl_storage_port: false: with internode TLS on DSE 6.9.7 and later, that setting must be true on every " +
                   "DSE node, or a node rolled back to DSE listens with TLS on storage_port only while the other DSE nodes go on dialing " +
                   "its ssl_storage_port, and they can see it DN until each of them is restarted");
        String eachNode = "restart each DSE node whose cassandra.yaml did not set it to true (this tool's report of each node says), one at a " +
                          "time, each drained before it stops, with every other node up and every node up and normal (UN in nodetool status) " +
                          "before the next one, and check with ss -tln on each that DSE listens on ssl_storage_port";
        String restart = eachNode + ", before the first upgrade (in a rolling restart of DSE that is still to come before the upgrade, else in a " +
                         "rolling restart of its own)";
        assertThat(falsePort.findings("INTERNODE_TLS").get(0).remedy)
        .startsWith("set enable_legacy_ssl_storage_port: true on every DSE node (required): " +
                    "change its line under server_encryption_options in DSE's cassandra.yaml to true (do not add a second one), then " +
                    restart + ", and run this tool again" + TLS_CARRY)
        .contains("(internode_encryption: all, optional,").doesNotContain("change nothing on DSE");
        assertThat(falsePort.report.upgradePath.upgradeSteps)
        .contains("Internode TLS on DSE 6.9: set enable_legacy_ssl_storage_port: true on every DSE node, then " + restart +
                  " (see INTERNODE_TLS)");

        // DSE 6.9.26 without the setting: blocking too, the line to add
        Run absentPort = assertBlocked(run(nodeWithInternodeTls("6.9.26", "dc", null)), "INTERNODE_TLS");
        assertThat(absentPort.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.BLOCKING);
        assertThat(absentPort.messages("INTERNODE_TLS"))
        .startsWith("DSE's cassandra.yaml sets internode_encryption: dc (server_encryption_options) on DSE 6.9.26, and does not set " +
                    "enable_legacy_ssl_storage_port (its default: false): with internode TLS on DSE 6.9.7 and later, that setting must be true");
        assertThat(absentPort.findings("INTERNODE_TLS").get(0).remedy)
        .startsWith("set enable_legacy_ssl_storage_port: true on every DSE node (required): " +
                    "add it under server_encryption_options in DSE's cassandra.yaml, then " + restart + ", and run this tool again" + TLS_CARRY);

        // the key written with no value: the line to change, not a second one
        DseNodeFixture emptyKey = nodeWithInternodeTls("6.9.26", "all", null);
        emptyKey.editYaml("cassandra.yaml", yaml -> serverEncryptionOptions(yaml).put("enable_legacy_ssl_storage_port", null));
        Run emptyRun = assertBlocked(run(emptyKey), "INTERNODE_TLS");
        assertThat(emptyRun.messages("INTERNODE_TLS"))
        .startsWith("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options) on DSE 6.9.26, and sets " +
                    "enable_legacy_ssl_storage_port to no value: with internode TLS on DSE 6.9.7 and later");
        assertThat(emptyRun.findings("INTERNODE_TLS").get(0).remedy)
        .startsWith("set enable_legacy_ssl_storage_port: true on every DSE node (required): " +
                    "change its line under server_encryption_options in DSE's cassandra.yaml to true (do not add a second one), then ");

        // in the mixed phase, a restart of each DSE node that lacked it
        DseNodeFixture mixed = nodeWithInternodeTls("6.9.26", "all", null);
        mixed.editPeers(peers -> peers.get(0).put("release_version", "5.0.7.0-SNAPSHOT"));
        Run mixedRun = assertBlocked(run(mixed), "INTERNODE_TLS");
        String mixedRestart = "since the upgrade has started (UPGRADED_PEERS), report to the upgrade owner, then " + eachNode;
        assertThat(mixedRun.codes(Finding.Severity.INFO)).contains("UPGRADED_PEERS");
        assertThat(mixedRun.findings("INTERNODE_TLS").get(0).remedy).contains(", then " + mixedRestart + ", and run this tool again;")
                                                                    .doesNotContain("before the first upgrade");
        assertThat(mixedRun.report.upgradePath.upgradeSteps)
        .contains("Internode TLS on DSE 6.9: set enable_legacy_ssl_storage_port: true on every DSE node, then " + mixedRestart +
                  " (see INTERNODE_TLS)");

        // DSE 6.9.7, the first release with the setting
        Run first = assertReady(run(nodeWithInternodeTls("6.9.7", "all", true)));
        assertThat(first.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
        assertThat(first.messages("INTERNODE_TLS")).contains(" on DSE 6.9.7, with enable_legacy_ssl_storage_port: true, as required");
        assertBlocked(run(nodeWithInternodeTls("6.9.7", "all", null)), "INTERNODE_TLS");

        // string values DSE reads as true (its loader takes YAML 1.1's true, yes and on, in any case, even quoted), and one it
        // does not
        for (String value : new String[]{ "TRUE", "yes", "On" })
        {
            DseNodeFixture node = nodeWithInternodeTls("6.9.26", "all", null);
            node.editYaml("cassandra.yaml", yaml -> serverEncryptionOptions(yaml).put("enable_legacy_ssl_storage_port", value));
            Run run = assertReady(run(node));
            assertThat(run.findings("INTERNODE_TLS")).as(value).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
            assertThat(run.messages("INTERNODE_TLS")).as(value).contains("with enable_legacy_ssl_storage_port: \"" + value + "\" (true for DSE), as required");
        }
        DseNodeFixture off = nodeWithInternodeTls("6.9.26", "all", null);
        off.editYaml("cassandra.yaml", yaml -> serverEncryptionOptions(yaml).put("enable_legacy_ssl_storage_port", "off"));
        assertThat(assertBlocked(run(off), "INTERNODE_TLS").messages("INTERNODE_TLS")).contains("and sets enable_legacy_ssl_storage_port: \"off\":");
        // a quoted value with a space is no boolean for DSE: blocking, the value shown as written
        DseNodeFixture spaced = nodeWithInternodeTls("6.9.26", "all", null);
        spaced.editYaml("cassandra.yaml", yaml -> serverEncryptionOptions(yaml).put("enable_legacy_ssl_storage_port", " true"));
        assertThat(assertBlocked(run(spaced), "INTERNODE_TLS").messages("INTERNODE_TLS"))
        .contains("and sets enable_legacy_ssl_storage_port: \" true\": with internode TLS on DSE 6.9.7 and later");

        // DSE 6.9.26 without internode encryption: nothing, whatever the setting
        for (Boolean legacy : new Boolean[]{ null, false, true })
        {
            Run none = assertReady(run(nodeWithInternodeTls("6.9.26", "none", legacy)));
            assertThat(none.findings("INTERNODE_TLS")).as(String.valueOf(legacy)).isEmpty();
            assertThat(none.report.upgradePath.upgradeSteps).noneMatch(s -> s.contains("internode TLS"));
        }

        // DSE 6.8.65 and 6.9.6 have no such setting: a warning only, whatever the file says, which asks to remove the key
        // when the file has it (DSE refuses a key it does not have)
        for (Boolean legacy : new Boolean[]{ null, false, true })
        {
            String remove68 = legacy == null ? "" : "; remove the enable_legacy_ssl_storage_port line from DSE's cassandra.yaml until DSE " +
                                                    "runs 6.9.7 or later: DSE 6.8.65 does not have that setting, and DSE refuses a " +
                                                    "cassandra.yaml with a key it does not have";
            String remove696 = remove68.replace("DSE 6.8.65", "DSE 6.9.6");
            Run dse68 = assertReady(run(nodeWithInternodeTls("6.8.65", "all", legacy)));
            assertThat(dse68.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
            assertThat(dse68.findings("INTERNODE_TLS").get(0).remedy)
            .startsWith("DSE 6.8.65 does not have enable_legacy_ssl_storage_port and always listens with TLS on ssl_storage_port, so DSE " +
                        "6.8 needs no TLS change for the upgrade" + remove68 + TLS_CARRY);
            assertThat(dse68.report.upgradePath.upgradeSteps).noneMatch(s -> s.startsWith("Internode TLS on DSE 6.9"));

            Run dse696 = assertReady(run(nodeWithInternodeTls("6.9.6", "all", legacy)));
            assertThat(dse696.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
            assertThat(dse696.messages("INTERNODE_TLS"))
            .isEqualTo("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options) on DSE 6.9.6; " + TLS_VALIDATED);
            assertThat(dse696.findings("INTERNODE_TLS").get(0).remedy)
            .startsWith("DSE 6.9.6 predates enable_legacy_ssl_storage_port (DSE 6.9.7 is the first release with it) and the required 6.9 " +
                        "release (the latest DSE 6.9 patch release): after DSE's own upgrade to it, set enable_legacy_ssl_storage_port: " +
                        "true on every DSE node before the first upgrade (required); until then DSE always listens with TLS on " +
                        "ssl_storage_port" + remove696 + TLS_CARRY);
            assertThat(dse696.report.upgradePath.upgradeSteps).noneMatch(s -> s.startsWith("Internode TLS on DSE 6.9"));
        }

        // a release whose TLS ports the tool does not know (a build number it cannot read): a warning; DSE_RELEASE blocks
        for (Boolean legacy : new Boolean[]{ null, false, true })
        {
            Run unknown = assertBlocked(run(nodeWithInternodeTls("6.9.261", "all", legacy)), "DSE_RELEASE");
            assertThat(unknown.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
            assertThat(unknown.messages("INTERNODE_TLS"))
            .isEqualTo("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options), and the TLS ports of this node's DSE " +
                       "release are not known (nodes/local records the release 4.0.0.69261); " + TLS_VALIDATED);
            assertThat(unknown.findings("INTERNODE_TLS").get(0).remedy)
            .startsWith((legacy == null ? "if this node runs DSE 6.9.7 or later, set enable_legacy_ssl_storage_port: true on every DSE node " +
                                          "(required): add it under server_encryption_options in DSE's cassandra.yaml, then " + restart
                                        : !legacy ? "if this node runs DSE 6.9.7 or later, set enable_legacy_ssl_storage_port: true on every DSE " +
                                                    "node (required): change its line under server_encryption_options in DSE's cassandra.yaml to " +
                                                    "true (do not add a second one), then " + restart
                                                  : "this cassandra.yaml sets enable_legacy_ssl_storage_port: true: keep it on every DSE node if " +
                                                    "this node runs DSE 6.9.7 or later (required), and check that DSE was restarted since it was " +
                                                    "set: ss -tln lists ssl_storage_port as well as storage_port") +
                        (legacy == null ? "" : "; if it runs DSE 6.8 or 6.9.0 to 6.9.6, remove the enable_legacy_ssl_storage_port line " +
                                               "instead (DSE refuses a cassandra.yaml with a key it does not have)") + TLS_CARRY);
            assertThat(unknown.report.upgradePath.upgradeSteps).noneMatch(s -> s.startsWith("Internode TLS on DSE 6.9"));
        }

        // a quoted value DSE reads as true, shown as written
        DseNodeFixture unknownYes = nodeWithInternodeTls("6.9.261", "all", null);
        unknownYes.editYaml("cassandra.yaml", yaml -> serverEncryptionOptions(yaml).put("enable_legacy_ssl_storage_port", "yes"));
        assertThat(run(unknownYes).findings("INTERNODE_TLS").get(0).remedy)
        .startsWith("this cassandra.yaml sets enable_legacy_ssl_storage_port: \"yes\" (true for DSE): keep it on every DSE node if this node " +
                    "runs DSE 6.9.7 or later");
        DseNodeFixture yesStep = nodeWithInternodeTls("6.9.26", "all", null);
        yesStep.editYaml("cassandra.yaml", yaml -> serverEncryptionOptions(yaml).put("enable_legacy_ssl_storage_port", "yes"));
        assertThat(assertReady(run(yesStep)).report.upgradePath.upgradeSteps)
        .anyMatch(s -> s.startsWith("Internode TLS on DSE 6.9: enable_legacy_ssl_storage_port: \"yes\" (true for DSE) is set in " +
                                    "this node's cassandra.yaml; check that DSE was restarted since"));

        // no release recorded in nodes/local, and no nodes/local at all
        DseNodeFixture noRelease = nodeWithInternodeTls("6.9.26", "all", null);
        noRelease.editLocal(local -> local.remove("release_version"));
        assertThat(run(noRelease).messages("INTERNODE_TLS"))
        .isEqualTo("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options), and the TLS ports of this node's DSE " +
                   "release are not known (nodes/local records no release); " + TLS_VALIDATED);
        DseNodeFixture noLocal = nodeWithInternodeTls("6.9.26", "all", null);
        Files.delete(noLocal.metadata.resolve("nodes").resolve("local"));
        Run noLocalRun = assertBlocked(run(noLocal), "NOT_A_DSE_NODE");
        assertThat(noLocalRun.findings("INTERNODE_TLS")).hasSize(1).allMatch(f -> f.severity == Finding.Severity.WARNING);
        assertThat(noLocalRun.messages("INTERNODE_TLS"))
        .isEqualTo("DSE's cassandra.yaml sets internode_encryption: all (server_encryption_options), and the TLS ports of this node's DSE " +
                   "release are not known (no nodes/local was read); " + TLS_VALIDATED);
    }

    /** The releases whose internode TLS needs DSE's legacy TLS port, read with the one parser of DSE's build numbers. */
    @Test
    public void theReleasesThatRequireDsesLegacyTlsPort()
    {
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.6926"))).isTrue();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.697"))).isTrue();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.6910"))).isTrue();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.696"))).isFalse();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.690"))).isFalse();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.6865"))).isFalse();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.680"))).isFalse();
        // DSE 6.0 to 6.7 (ports not read), a build number of five digits, not a DSE 6 release, or none
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.678"))).isNull();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.6012"))).isNull();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.0.69261"))).isNull();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("5.0.7.0"))).isNull();
        assertThat(NodeInspector.requiresLegacySslStoragePort(new CassandraVersion("4.0.11.0"))).isNull();
        assertThat(NodeInspector.requiresLegacySslStoragePort(null)).isNull();
    }

    /** DSE's reading of a boolean setting: YAML 1.1's true, yes and on in any case, as a Boolean or a string. */
    @Test
    public void theBooleansDseReadsAsTrue()
    {
        for (Object value : new Object[]{ Boolean.TRUE, "true", "TRUE", "True", "yes", "Yes", "YES", "on", "On", "ON" })
            assertThat(NodeInspector.isDseTrue(value)).as(String.valueOf(value)).isTrue();
        for (Object value : new Object[]{ null, Boolean.FALSE, "false", "no", "off", "", "1", "y", "enabled", " true", "on " })
            assertThat(NodeInspector.isDseTrue(value)).as(String.valueOf(value)).isFalse();
    }

    private DseNodeFixture nodeWithInternodeTls(String dseVersion, String internodeEncryption, @Nullable Boolean legacySslStoragePort) throws IOException
    {
        return withInternodeTls(node(), dseVersion, internodeEncryption, legacySslStoragePort);
    }

    /**
     * {@code node} made to run DSE {@code dseVersion} ({@code 6.<minor>.<patch>}, with its peers and its installation),
     * with {@code internodeEncryption} and, unless null, {@code enable_legacy_ssl_storage_port} in DSE's cassandra.yaml.
     */
    private static DseNodeFixture withInternodeTls(DseNodeFixture node, String dseVersion, String internodeEncryption, @Nullable Boolean legacySslStoragePort)
    throws IOException
    {
        String release = "4.0.0." + dseVersion.replace(".", "");
        node.editLocal(local -> {
            local.put("release_version", release);
            local.put("dse_version", dseVersion);
        });
        node.editPeers(peers -> peers.forEach(peer -> {
            peer.put("release_version", release);
            peer.put("dse_version", dseVersion);
        }));
        Files.write(node.dseHome.resolve("ds_version.txt"), ("DSVersion=" + dseVersion + "-0c77b67\n").getBytes(StandardCharsets.UTF_8));
        node.editYaml("cassandra.yaml", yaml -> {
            Map<String, Object> encryption = serverEncryptionOptions(yaml);
            encryption.put("internode_encryption", internodeEncryption);
            encryption.remove("enable_legacy_ssl_storage_port");
            if (legacySslStoragePort != null)
                encryption.put("enable_legacy_ssl_storage_port", legacySslStoragePort);
            encryption.put("keystore", "/etc/dse/keystore");
            encryption.put("truststore", "/etc/dse/truststore");
        });
        return node;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> serverEncryptionOptions(Map<String, Object> yaml)
    {
        return (Map<String, Object>) yaml.get("server_encryption_options");
    }

    /** The keys of DSE's server_encryption_options HCD 2.0 refuses: only those it does not have, whatever the values. */
    @Test
    public void serverEncryptionKeysHcd20DoesNotHave()
    {
        Map<Object, Object> block = new LinkedHashMap<>();
        block.put("internode_encryption", "all");
        block.put("keystore", "/etc/dse/keystore");
        block.put("keystore_password", "secret");
        block.put("truststore", "/etc/dse/truststore");
        block.put("truststore_password", "secret");
        block.put("outbound_keystore", "/etc/dse/outbound");
        block.put("store_type", "JKS");
        block.put("require_client_auth", true);
        block.put("require_endpoint_verification", false);
        block.put("cipher_suites", List.of("TLS_AES_128_GCM_SHA256"));
        block.put("protocol", "TLS");
        // a name from a camel case setter, and one HCD 2.0 loads in server_encryption_options but ignores there
        block.put("accepted_protocols", List.of("TLSv1.2"));
        block.put("enabled", true);
        // the old name of legacy_ssl_storage_port_enabled: HCD 2.0 accepts it
        block.put("enable_legacy_ssl_storage_port", true);
        // a dotted name of a nested property HCD 2.0 has
        block.put("ssl_context_factory.class_name", "org.apache.cassandra.security.DefaultSslContextFactory");
        block.put("truststore_type", "JKS");
        block.put("keystore_type", "PKCS12");
        // the values are not looked at: a known key with a value HCD 2.0 cannot read, or none, is not listed; an unknown one is
        block.put("optional", "not a boolean");
        block.put("algorithm", null);
        block.put("require_dse_magic", null);
        block.put("dse_only.nested", "x");
        // a nested name HCD 2.0 does not resolve to the end, and a name of dots only
        block.put("ssl_context_factory.bogus", "x");
        block.put(".", "x");
        // keys YAML 1.1 reads as a boolean or as null: named as written
        block.put(Boolean.TRUE, "x");
        block.put(null, "x");
        assertThat(NodeInspector.keysHcd20DoesNotHave(block))
        .containsExactly(".", "dse_only.nested", "keystore_type", "null (written ~, null, Null, NULL or nothing in DSE's file)", "require_dse_magic",
                         "ssl_context_factory.bogus", "true (written on, yes or true, in lower case, capitalised or upper case, in DSE's file)",
                         "truststore_type");
        assertThat(NodeInspector.keysHcd20DoesNotHave(Map.of())).isEmpty();
    }

    /**
     * The keys DSE's file sets to no value that HCD 2.0 refuses: not @Nullable, with a default, or one that cannot be read
     * before the options are applied (optional, enabled); not the others.
     */
    @Test
    public void emptyServerEncryptionKeysHcd20Refuses()
    {
        Map<Object, Object> block = new LinkedHashMap<>();
        for (String key : new String[]{ "keystore", "truststore", "store_type", "require_client_auth", "internode_encryption",
                                        "enable_legacy_ssl_storage_port", "algorithm", "protocol", "accepted_protocols", "cipher_suites",
                                        "keystore_password", "truststore_password", "optional", "keystore_type" })
            block.put(key, null);
        block.put("truststore_type", "JKS");
        assertThat(NodeInspector.emptyKeysHcd20Refuses(block))
        .containsExactly("enable_legacy_ssl_storage_port", "internode_encryption", "keystore", "optional", "require_client_auth", "store_type",
                         "truststore");
        // for every name HCD 2.0 takes, the tool names it empty exactly when HCD 2.0's loader refuses it with no value, for
        // one of these reasons: an object-typed setting with a default (the loader's null check: "are not valid"), a default
        // that cannot be read before the options are applied (optional, enabled), or a primitive boolean under any name,
        // which cannot hold no value, so that the loader fails while setting it, before its null check reports
        for (String key : NodeInspector.hcd20ServerEncryptionKeys())
        {
            Map<Object, Object> empty = new LinkedHashMap<>();
            empty.put(key, null);
            boolean named = !NodeInspector.emptyKeysHcd20Refuses(empty).isEmpty();
            Map<String, Object> single = new LinkedHashMap<>();
            single.put(key, null);
            Throwable refusal = null;
            try
            {
                YamlConfigurationLoader.fromMap(single, true, EncryptionOptions.ServerEncryptionOptions.class);
            }
            catch (RuntimeException e)
            {
                refusal = e;
            }
            assertThat(named).as(key).isEqualTo(refusal != null);
            if (refusal != null)
            {
                String messages = String.valueOf(refusal.getMessage()) + ' ' + String.valueOf(Throwables.getRootCause(refusal).getMessage());
                assertThat(messages).as(key).doesNotContain("Please remove properties");
                assertThat(messages.contains("Those properties [" + key + "] are not valid") || messages.contains("applyConfig must be called first")
                           || messages.contains("to null value"))
                .as(key + ": " + messages).isTrue();
            }
        }
    }

    /** Every name the tool takes as known is one HCD 2.0's configuration loader accepts in server_encryption_options. */
    @Test
    public void theKnownServerEncryptionKeysAreTheOnesHcd20Loads()
    {
        Set<String> known = NodeInspector.hcd20ServerEncryptionKeys();
        assertThat(known).contains("internode_encryption", "keystore", "store_type", "optional", "protocol", "accepted_protocols",
                                   "legacy_ssl_storage_port_enabled", "enable_legacy_ssl_storage_port");
        for (String name : known)
        {
            Map<String, Object> single = new LinkedHashMap<>();
            single.put(name, null);
            try
            {
                YamlConfigurationLoader.fromMap(single, true, EncryptionOptions.ServerEncryptionOptions.class);
            }
            catch (RuntimeException e)
            {
                // a refused value is fine here; a refused name is not
                assertThat(String.valueOf(e.getMessage())).as(name).doesNotContain("Please remove properties");
            }
        }
        Map<String, Object> unknown = new LinkedHashMap<>();
        unknown.put("keystore_type", "JKS");
        assertThatThrownBy(() -> YamlConfigurationLoader.fromMap(unknown, true, EncryptionOptions.ServerEncryptionOptions.class))
        .hasMessageContaining("Please remove properties [keystore_type]");
    }

    /** INTERNODE_TLS says what to do with DSE's two store types: the value when there is one, a conversion when they differ. */
    @Test
    @SuppressWarnings("unchecked")
    public void internodeTlsTellsWhatToDoWithDsesStoreTypes() throws IOException
    {
        DseNodeFixture differing = node();
        differing.editYaml("cassandra.yaml", yaml -> {
            Map<String, Object> encryption = (Map<String, Object>) yaml.get("server_encryption_options");
            encryption.put("internode_encryption", "all");
            encryption.put("keystore_type", "PKCS12");
            encryption.put("truststore_type", "JKS");
        });
        assertThat(assertReady(run(differing)).findings("INTERNODE_TLS").get(0).remedy)
        .contains("; leave out keystore_type, truststore_type, which HCD 2.0 does not have and refuses to start on (HCD 2.0 has one store_type " +
                  "for both stores, and DSE's keystore_type PKCS12 and truststore_type JKS differ: convert one store to the type of the other " +
                  "and set store_type to it);");

        // one store type only, or two that differ in case only
        for (String[] types : new String[][]{ { "PKCS12", null }, { null, "JKS" }, { "jks", "JKS" } })
        {
            DseNodeFixture node = node();
            node.editYaml("cassandra.yaml", yaml -> {
                Map<String, Object> encryption = (Map<String, Object>) yaml.get("server_encryption_options");
                encryption.put("internode_encryption", "all");
                if (types[0] != null)
                    encryption.put("keystore_type", types[0]);
                if (types[1] != null)
                    encryption.put("truststore_type", types[1]);
            });
            String remedy = assertReady(run(node)).findings("INTERNODE_TLS").get(0).remedy;
            if (types[0] == null)
                assertThat(remedy).contains("(HCD 2.0 has one store_type for both stores: set store_type: JKS if DSE's keystore is of that type too, " +
                                            "else convert the keystore to it; DSE's truststore_type names only its truststore)");
            else if (types[1] == null)
                assertThat(remedy).contains("(HCD 2.0 has one store_type for both stores: set store_type: PKCS12 if DSE's truststore is of that type " +
                                            "too, else convert the truststore to it; DSE's keystore_type names only its keystore)");
            else
                assertThat(remedy).contains("(HCD 2.0 has one store_type for both stores: set store_type: jks)").doesNotContain("differ");
        }

        // an unknown key that is no store type, and a key DSE's file sets to no value
        DseNodeFixture other = node();
        other.editYaml("cassandra.yaml", yaml -> {
            Map<String, Object> encryption = (Map<String, Object>) yaml.get("server_encryption_options");
            encryption.put("internode_encryption", "all");
            encryption.put("require_dse_magic", true);
            encryption.put("algorithm", null);
            encryption.put("keystore", null);
        });
        assertThat(assertReady(run(other)).findings("INTERNODE_TLS").get(0).remedy)
        .contains("; leave out require_dse_magic, which HCD 2.0 does not have and refuses to start on; leave out keystore, which DSE's file " +
                  "sets to no value: HCD 2.0 refuses no value for these settings;")
        .doesNotContain("store_type for both")
        .doesNotContain("leave out algorithm");
    }

    @Test
    public void ec2SnitchesAreRecognisedInTheLocatorPackageOnly()
    {
        assertThat(NodeInspector.isEc2Snitch("Ec2Snitch")).isTrue();
        assertThat(NodeInspector.isEc2Snitch(" org.apache.cassandra.locator.Ec2MultiRegionSnitch ")).isTrue();
        assertThat(NodeInspector.isEc2Snitch("org.apache.cassandra.locator.Ec2Snitch")).isTrue();
        // a class of that name in another package: the tool cannot tell whether it names data centers as Ec2Snitch does
        assertThat(NodeInspector.isEc2Snitch("com.example.Ec2Snitch")).isFalse();
        assertThat(NodeInspector.isEc2Snitch("com.datastax.bdp.snitch.Ec2Snitch")).isFalse();
        assertThat(NodeInspector.isEc2Snitch("org.apache.cassandra.locator.sub.Ec2Snitch")).isFalse();
        assertThat(NodeInspector.isEc2Snitch("Ec2SnitchExtended")).isFalse();
        assertThat(NodeInspector.isEc2Snitch("GoogleCloudSnitch")).isFalse();
        assertThat(NodeInspector.isEc2Snitch("org.apache.cassandra.locator.GossipingPropertyFileSnitch")).isFalse();
        assertThat(NodeInspector.isEc2Snitch(null)).isFalse();
    }

    @Test
    public void tokensAreCarried() throws IOException
    {
        DseNodeFixture node = node();
        node.editYaml("cassandra.yaml", yaml -> {
            yaml.put("num_tokens", 16);
            yaml.put("initial_token", "-9000,0,9000");
        });
        Run run = assertReady(run(node));
        assertThat(run.messages("IDENTITY_MISMATCH")).contains("num_tokens 16 but nodes/local records 8 tokens");
        assertThat(run.report.upgradePath.settings).containsEntry("initial_token", "-9000,0,9000");

        DseNodeFixture absent = node();
        absent.editYaml("cassandra.yaml", yaml -> yaml.remove("num_tokens"));
        assertThat(assertReady(run(absent)).report.upgradePath.settings).containsEntry("num_tokens", "8 (DSE's cassandra.yaml sets none: the number of tokens of this node in nodes/local)");
    }

    // ---- command line ----

    @Test
    public void wrongUsageExitsWithTwo() throws IOException
    {
        Path json = temporaryFolder.getRoot().toPath().resolve("unused.json");
        Run unknown = run(json, "--no-such-option");
        assertThat(unknown.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(unknown.err).contains("no-such-option");
        Run arguments = run(json, "unexpected");
        assertThat(arguments.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(arguments.err).contains("unexpected arguments: [unexpected]");
        Run help = run(json, "--help");
        assertThat(help.exitCode).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(help.out).contains("--nodetool").contains("Exit codes");
        assertThat(json).doesNotExist();
    }

    // ---- helpers ----

    private static void deleteTree(Path root) throws IOException
    {
        try (Stream<Path> paths = Files.walk(root))
        {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
                Files.delete(path);
        }
    }

    private static Map<String, String> simpleStrategy()
    {
        return Map.of("class", SIMPLE_STRATEGY, "replication_factor", "1");
    }

    private static Map<String, String> stcs()
    {
        return Map.of("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold", "32", "min_threshold", "4");
    }

    private static Map<String, String> lz4()
    {
        return Map.of("class", "org.apache.cassandra.io.compress.LZ4Compressor", "chunk_length_in_kb", "64");
    }

    /**
     * Adds a table {@code (pk int PRIMARY KEY, v text, <extra columns>)} with DSE's table options to the schema of the
     * node, in the keyspace given, which must exist ({@code lab} or {@code system_auth}, for instance): no keyspace row is
     * written.
     */
    private static void addTable(DseNodeFixture node, String keyspace, String table, Map<String, String> compaction, Map<String, String> compression,
                                 Set<String> flags, Map<String, String> extraColumns) throws IOException
    {
        node.writeRows("system_schema", "tables", KEYSPACES_KEY, TABLES_KEY,
                       withTableOptions(row(keyspace, table), compaction, compression).with("flags", flags));
        List<DseNodeFixture.SchemaRow> columns = new ArrayList<>();
        columns.add(column(keyspace, table, "pk", "partition_key", 0, "int"));
        columns.add(column(keyspace, table, "v", "regular", -1, "text"));
        extraColumns.forEach((name, type) -> columns.add(column(keyspace, table, name, "regular", -1, type)));
        node.writeRows("system_schema", "columns", KEYSPACES_KEY, COLUMNS_KEY, columns.toArray(new DseNodeFixture.SchemaRow[0]));
    }

    /** {@code row} with a new id and DSE's table options, those of a table and of a view alike. */
    private static DseNodeFixture.SchemaRow withTableOptions(DseNodeFixture.SchemaRow row, Map<String, String> compaction, Map<String, String> compression)
    {
        return row.with("id", UUID.randomUUID())
                                           .with("bloom_filter_fp_chance", 0.01)
                                           .with("caching", Map.of("keys", "ALL", "rows_per_partition", "NONE"))
                                           .with("comment", "")
                                           .with("compaction", compaction)
                                           .with("compression", compression)
                                           .with("crc_check_chance", 1.0)
                                           .with("dclocal_read_repair_chance", 0.0)
                                           .with("default_time_to_live", 0)
                                           .with("extensions", Map.of())
                                           .with("gc_grace_seconds", 864000)
                                           .with("max_index_interval", 2048)
                                           .with("memtable_flush_period_in_ms", 0)
                                           .with("min_index_interval", 128)
                                           .with("nodesync", Map.of("enabled", "true", "incremental", "true"))
                                           .with("read_repair", "BLOCKING")
                                           .with("read_repair_chance", 0.0)
                                           .with("speculative_retry", "99PERCENTILE");
    }

    /** A row of {@code system_schema.views}: the view {@code keyspace.view} of {@code baseTable}, with DSE's table options. */
    private static DseNodeFixture.SchemaRow view(String keyspace, String view, String baseTable)
    {
        return withTableOptions(row(keyspace, view), stcs(), lz4()).with("base_table_id", UUID.randomUUID())
                                                                    .with("base_table_name", baseTable)
                                                                    .with("include_all_columns", false)
                                                                    .with("where_clause", "v IS NOT NULL AND pk IS NOT NULL");
    }

    private static Path schemaTableDirectory(DseNodeFixture node, String table) throws IOException
    {
        try (Stream<Path> tables = Files.list(node.data.resolve("system_schema")))
        {
            return tables.filter(p -> p.getFileName().toString().startsWith(table + '-')).findFirst().orElseThrow();
        }
    }

    /** Overwrites the {@code Data.db} files of a table directory with bytes no sstable reader can decode. */
    private static void corruptData(Path tableDirectory) throws IOException
    {
        try (Stream<Path> files = Files.list(tableDirectory))
        {
            for (Path data : files.filter(p -> p.getFileName().toString().endsWith("-Data.db")).collect(Collectors.toList()))
                Files.write(data, new byte[]{ 0, 1, 2, 3 });
        }
    }

    private static DseNodeFixture.SchemaRow column(String keyspace, String table, String name, String kind, int position, String type)
    {
        return row(keyspace, table, name).with("clustering_order", "none")
                                         .with("column_name_bytes", ByteBufferUtil.bytes(name))
                                         .with("kind", kind)
                                         .with("position", position)
                                         .with("type", type);
    }

    private static DseNodeFixture.SchemaRow function(String keyspace, String name, String language)
    {
        return row(keyspace, name, List.of("int")).with("argument_names", List.of("x"))
                                                  .with("body", "return x + 1;")
                                                  .with("called_on_null_input", true)
                                                  .with("language", language)
                                                  .with("return_type", "int");
    }

    /** A {@code nodetool} that records its arguments, one line per call, runs {@code body} and exits with {@code exitCode}. */
    private static Path fakeNodetool(Path directory, Path arguments, int exitCode, String body) throws IOException
    {
        Path script = directory.resolve("nodetool");
        Files.write(script, ("#!/bin/sh\necho \"$*\" >> '" + arguments + "'\n" + body + "exit " + exitCode + '\n').getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"));
        return script;
    }
}
