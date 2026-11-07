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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import org.apache.commons.cli.Option;
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
    private static final String NETWORK_TOPOLOGY_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    /** What the remedies of SYSTEM_AUTH_REPLICATION add when the nodes counted are a lower bound. */
    private static final String LOWER_TO_THE_NODES = ", and lower the 3 replicas of a data center of fewer nodes to its nodes";
    /** The recommendation the remedies of SYSTEM_AUTH_REPLICATION give outside the mixed phase. */
    private static final String AUTH_REPLICATION_RECOMMENDED = "3 replicas, or as many as its nodes if fewer, are recommended: in a data center " +
                                                               "of 3 nodes or more they let the upgrade go on while a node is down";
    /** The check by hand SYSTEM_AUTH_REPLICATION asks for when the data centers of some nodes are not known. */
    private static final String AUTH_REPLICATION_CHECK = "that system_auth has replicas in every data center of the cluster (nodetool status " +
                                                         "names them; with several data centers, NetworkTopologyStrategy listing each), no " +
                                                         "more in a data center than its nodes can make a LOCAL_QUORUM of";
    /**
     * The remedy of SYSTEM_AUTH_REPLICATION when the data centers of some nodes are not known, outside the mixed phase.
     */
    private static final String AUTH_REPLICATION_BY_HAND = "check by hand, before the first node of the cluster is upgraded, " + AUTH_REPLICATION_CHECK +
                                                           "; " + AUTH_REPLICATION_RECOMMENDED;
    /**
     * How the remedies of SYSTEM_AUTH_REPLICATION end when the data centers of the nodes are not all known and nodes/local
     * and nodes/peers name one at most, so that the other replicated system keyspaces are not checked.
     */
    private static final String AUTH_REPLICATION_OTHER_KEYSPACES = "; with several data centers, the other replicated system keyspaces need " +
                                                                   "every data center too, which this tool checks only when nodes/local and " +
                                                                   "nodes/peers name several";
    /**
     * How the remedies of SYSTEM_AUTH_REPLICATION end when the data centers of the nodes are not all known and nodes/local
     * and nodes/peers name several, so that the other replicated system keyspaces are checked against the nodes counted.
     */
    private static final String AUTH_REPLICATION_OTHERS_BY_HAND = "; the other replicated system keyspaces were checked against the nodes whose " +
                                                                  "data center is known only: check by hand that each has replicas in every data " +
                                                                  "center too";
    /**
     * Why the nodes counted are a lower bound, when nodes/local is missing, a nodes file cannot be read, or nodes/peers is
     * being rewritten.
     */
    private static final String NODES_FILES_NOT_READ = "nodes/local is missing, or nodes/local or nodes/peers could not be read, or nodes/peers " +
                                                       "was being rewritten by DSE, and the data centers of the nodes are not all known";
    /** The UNREADABLE_INPUT of a nodes/peers.old left in the nodes directory. */
    private static final String PEERS_OLD_LEFT = "cannot read nodes/peers: a nodes/peers.old is left, which DSE puts back in place of nodes/peers " +
                                                 "at its next start (DSE is rewriting the file, or was interrupted while it did), so the peers of " +
                                                 "this node are not known; the releases and workloads of the peers cannot be checked";
    /** The remedy of {@link #PEERS_OLD_LEFT}. */
    private static final String PEERS_OLD_LEFT_REMEDY = "run the tool again; if it persists with DSE stopped, start DSE on this node once (it puts " +
                                                        "nodes/peers.old back), then run the tool again";
    /**
     * What the blocking remedy of SYSTEM_AUTH_REPLICATION, its check by hand and the remedy of KEYSPACE_REPLICATION add when
     * nodes/peers cannot be read or trusted.
     */
    private static final String MIXED_PHASE_UNKNOWN = "; only if no peer already runs HCD 2.0, which this tool cannot tell while " +
                                                      "nodes/peers cannot be read or trusted (in the mixed phase, no schema change and no " +
                                                      "repair; if one does, upgrade no further node and report to the upgrade owner)";
    /** What every KEYSPACE_REPLICATION message says after what HCD 2.0 does with the keyspace. */
    private static final String KEYSPACE_REPLICATION_NOT_BLOCKING = ", so this does not stop the upgrade";
    /** What a KEYSPACE_REPLICATION message says after SimpleStrategy over several data centers, before the keyspace. */
    private static final String NO_REPLICA_OF_PART = ", so a data center of the nodes may hold no replica of part of ";
    /** The upgrade step of the other keyspaces with one data center known at most and the nodes not all counted. */
    private static final String OTHER_KEYSPACES_NOT_CHECKED_STEP = "Replication of the other system keyspaces: not checked (" + NODES_FILES_NOT_READ +
                                                                   "); with several data centers, each needs replicas in every data center, 3 " +
                                                                   "recommended, or as many as its nodes if fewer";
    /** The start of the PENDING_HINTS info when nodes/local or nodes/peers could not be read or trusted. */
    private static final String HINTS_NOT_CHECKED = "DSE hints files for host ids not checked against nodes/local and nodes/peers, which could " +
                                                    "not both be read and trusted: ";
    /** The end of the PENDING_HINTS info when nodes/local or nodes/peers could not be read or trusted. */
    private static final String HINTS_NOT_CHECKED_END = "; some may be pending for live DSE peers: this tool checks them once it reads both";
    /** What HCD 2.0 does with system_traces, as KEYSPACE_REPLICATION says it. */
    private static final String TRACES_USE = "HCD 2.0 writes the traces of queries to system_traces at ANY (and, once repairs are allowed again " +
                                             "after the upgrade, those of repairs, which it reads back at ONE)";
    /** What HCD 2.0 does with system_distributed, as KEYSPACE_REPLICATION says it. */
    private static final String DISTRIBUTED_USE = "HCD 2.0 writes the build status of materialized views to system_distributed at ONE and reads it " +
                                                  "at ONE (and, once repairs are allowed again after the upgrade, writes the repair history there " +
                                                  "at ANY)";
    /** How a KEYSPACE_REPLICATION message ends outside the mixed phase. */
    private static final String KEYSPACE_REPLICATION_RECOMMENDED = "; replicas in every data center, 3 (or as many as its nodes if fewer) recommended, " +
                                                                   "keep it available in each";
    /** Why SYSTEM_AUTH_REPLICATION matters, in each of its messages. */
    private static final String AUTH_REPLICATION_READ = "; an upgraded node reads DSE's access controls from system_auth at LOCAL_QUORUM (HCD " +
                                                        "2.0's default auth_read_consistency_level) when it starts and refuses to start " +
                                                        "when it cannot";
    /** How a blocking SYSTEM_AUTH_REPLICATION message ends, the data centers of every node known. */
    private static final String AUTH_REPLICATION_BLOCKING = AUTH_REPLICATION_READ + ", so every data center of nodes/local and nodes/peers " +
                                                            "needs replicas of system_auth (with several data centers, NetworkTopologyStrategy " +
                                                            "listing each), no more in a data center than its nodes can make a LOCAL_QUORUM " +
                                                            "of, or an upgraded node there cannot start, even with every node up";
    /**
     * How a warning SYSTEM_AUTH_REPLICATION message ends when a data center has too few replicas, outside the mixed
     * phase.
     */
    private static final String AUTH_REPLICATION_SHORT = AUTH_REPLICATION_READ + "; more replicas (3, or up to the nodes of the data center) let " +
                                                         "the upgrade go on while a node is down";
    /**
     * How a warning SYSTEM_AUTH_REPLICATION message ends when a data center has too many replicas, outside the mixed
     * phase.
     */
    private static final String AUTH_REPLICATION_ABOVE = AUTH_REPLICATION_READ + "; as many replicas as the nodes of the data center let the " +
                                                         "upgrade go on while a node is down";
    /** The remedy of a blocking SYSTEM_AUTH_REPLICATION in the mixed phase. */
    private static final String AUTH_REPLICATION_MIXED_PHASE_REMEDY = "the upgrade has started (UPGRADED_PEERS), and the replication cannot be " +
                                                                      "changed in the mixed phase (no schema change and no repair then, and the " +
                                                                      "upgraded nodes do not receive DSE's schema changes): upgrade no further " +
                                                                      "node and report to the upgrade owner; until it is changed, an upgraded " +
                                                                      "node cannot start while too few of the replicas in its data center are " +
                                                                      "up for LOCAL_QUORUM, and never where its data center lacks replicas (not " +
                                                                      "listed, at 0, or SimpleStrategy over several data centers) or has fewer " +
                                                                      "nodes than its LOCAL_QUORUM needs (with EverywhereStrategy, the quorum of " +
                                                                      "the whole ring), nor with a replication HCD 2.0 cannot use (no class or " +
                                                                      "another class, replicas that are no number, or no replication_factor)";
    /** What the remedies of SYSTEM_AUTH_REPLICATION say of the replicas of EverywhereStrategy. */
    private static final String EVERYWHERE_NOT_CARRIED = "; the replicas of EverywhereStrategy are those of the whole ring, so they are not carried over";
    /** What the problems and shortfalls of EverywhereStrategy say of its replicas. */
    private static final String EVERYWHERE = "(EverywhereStrategy: LOCAL_QUORUM counts the replicas of the whole ring)";
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
    /**
     * A reference to a document of the project, which no text of the tool may hold: the texts state the problem and the
     * fix themselves (the upgrade runbook, its sections and appendices, SECURITY.md).
     */
    private static final Pattern DOCUMENT_REFERENCE = Pattern.compile("(?i)\\brunbook|\\bappendix|SECURITY\\.md|\\bsections? [0-9]");
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

    static Run run(Path json, String... args) throws IOException
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
        NodeReport report = bytes == null ? null : ClusterAggregate.READER.readValue(bytes);
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

    /** The texts the tool prints say what to do, never an id of the project's plan nor a document of the project. */
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
     * What a run of the tool printed or wrote (its output, its error output, its JSON) holds no id of the plan and no
     * reference to a document of the project; the paths and this machine's host name, which the texts quote and which are
     * not the tool's words, are left out.
     */
    static void assertNoPlanDecisionIds(String... texts)
    {
        for (String text : texts)
        {
            String own = MACHINE_PATH.matcher(text).replaceAll("<path>");
            if (HOST_NAME != null)
                own = HOST_NAME.matcher(own).replaceAll("<host>");
            assertThat(own).doesNotContainPattern(PLAN_DECISION_ID).doesNotContainPattern(DOCUMENT_REFERENCE);
        }
    }

    @Test
    public void noTextOfTheToolHoldsAPlanDecisionId() throws IOException
    {
        // the string literals of the tool's sources, comments left out: the texts of findings no test reaches included;
        // each literal on its own, so an id split across two literals of one text would escape (the runs above see it),
        // and a char literal holding a quote ('"') pairs with the next quote of its line (harmless: it only adds text);
        // nor a reference to a document of the project
        Pattern literal = Pattern.compile("\"(?:[^\"\\\\]|\\\\.)*\"");
        List<String> found = new ArrayList<>();
        List<String> documents = new ArrayList<>();
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
                        if (DOCUMENT_REFERENCE.matcher(matcher.group()).find())
                            documents.add(source.getFileName() + ":" + (i + 1) + ": " + matcher.group());
                    }
                }
            }
        }
        assertThat(literals).isGreaterThan(500);
        assertThat(found).isEmpty();
        assertThat(documents).isEmpty();
        assertThat(DOCUMENT_REFERENCE.matcher("see section 4 of the runbook").find()).isTrue();
        assertThat(DOCUMENT_REFERENCE.matcher("the table's sections").find()).isFalse();
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
                                                .endsWith("so check the reports of every node (the aggregate mode does)");
        assertThat(run.messages("VIEWS_HANDLED")).contains("lab.wide_by_v (hidden column .v)");
        assertThat(run.messages("DSE_RELEASE")).isEqualTo("DSE 6.8 (6.8.65) is accepted; the upgrade requires the latest DSE 6.8 or 6.9 patch " +
                                                          "release, which this tool does not check; DSE 6.9 is preferred");
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = (Map<String, Object>) report.inputs.get("schema");
        assertThat((Integer) schema.get("keyspaces")).isGreaterThan(10);
        assertThat(schema).containsEntry("views", 1).doesNotContainKey("role_permissions_rows");
        assertThat(report.upgradePath.upgradeSteps).contains("Sstable formats: every sstable is in DSE 6's trie format");
        // the lab cluster the schema was taken from replicates system_auth in its one data center, on its three nodes
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '3'}, enough " +
                                                             "in every data center of nodes/local and nodes/peers");
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
        // the schema fixture (from the 6.8.65 lab node) replicates system_auth in dc1; the 6.9.26 nodes files put both nodes in Cassandra
        writeAuthReplication(node, networkTopologyStrategy("Cassandra", "2"));
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
        // DSE's default replication of system_auth, SimpleStrategy with one replica, is a warning in the data center of
        // 3 nodes
        Run run = assertReady(run(node));
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '1'}: " +
                                                                      "dc1 has 1 replica and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_SHORT);
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

    /**
     * The tables system_info_encryption encrypts, encrypted in the schema: with the setting on, only the finding of the
     * setting names them, next to the finding of a user table; with it off in dse.yaml and DSE not restarted since, or
     * with dse.yaml unreadable, a finding of their own points to the restart and the rewrite, and the finding of the
     * other encrypted tables, with its ALTER TABLE, leaves them out.
     */
    @Test
    public void tablesEncryptedBySystemInfoEncryption() throws IOException
    {
        String encryptingClass = " (compression class org.apache.cassandra.io.compress.EncryptingLZ4Compressor)";
        String userTableMessage = "encrypted (TDE) tables: lab.secret" + encryptingClass + "; the upgrade applies to clusters without " +
                                  "encrypted tables or views";
        String userTableRemedy = "decrypt them on DSE (ALTER TABLE ... WITH compression = {'class': 'LZ4Compressor'}, then nodetool " +
                                 "upgradesstables -a on every node) before the upgrade";
        String offMessage = "tables that system_info_encryption of dse.yaml encrypted, whose schema still names an encrypting compression " +
                            "although dse.yaml does not enable it here, or could not be read: system.batches" + encryptingClass +
                            ", system.paxos" + encryptingClass + ", system_backups.backup_configurations" + encryptingClass +
                            ", system_backups.backup_stores" + encryptingClass + "; DSE sets their compression back to LZ4Compressor at " +
                            "its first start with the setting off (not done yet on this node, or, for system_backups, another node " +
                            "started with it on since then; or system_schema not flushed since), and their sstables stay encrypted " +
                            "until they are rewritten";
        String offRemedy = "do not ALTER these system tables: set enabled: false under system_info_encryption in dse.yaml and restart DSE on " +
                           "every node that has not run with it off yet, with " + SYSTEM_KEY_DIRECTORY + " still in place, so that DSE " +
                           "replays the commit log segments written before (if DSE already runs with it off on every node, run nodetool " +
                           "flush system_schema on this node and this tool again); then run nodetool upgradesstables -a system batches " +
                           "paxos and nodetool upgradesstables -a system_backups on every node (this tool does not check that the rewrite " +
                           "is done), and keep that directory in place until the rewrite is done and, with every node up, DSE has " +
                           "delivered the hints files written before, which stay encrypted (this tool names those left, under " +
                           "HINTS_COMPRESSION); all of it before the first node of the cluster is upgraded; " + SYSTEM_KEY_LOSS;

        // the setting on: its own finding only for the four tables, and the ALTER TABLE finding for a user table
        DseNodeFixture on = systemInfoEncryptedNode();
        on.editYaml("dse.yaml", yaml -> section(yaml, "system_info_encryption").put("enabled", true));
        Run onRun = assertBlocked(run(on), "TDE_TABLE");
        assertThat(onRun.codes(Finding.Severity.BLOCKING)).containsExactly("TDE_TABLE");
        assertThat(onRun.findings("TDE_TABLE")).extracting(f -> f.message).containsExactlyInAnyOrder(SYSTEM_INFO_ENCRYPTION_MESSAGE, userTableMessage);
        assertThat(onRun.findings("TDE_TABLE")).filteredOn(f -> f.message.equals(SYSTEM_INFO_ENCRYPTION_MESSAGE)).singleElement()
                                               .extracting(f -> f.remedy).isEqualTo(SYSTEM_INFO_ENCRYPTION_REMEDY);
        assertThat(onRun.findings("TDE_TABLE")).filteredOn(f -> f.message.equals(userTableMessage)).singleElement()
                                               .extracting(f -> f.remedy).isEqualTo(userTableRemedy);

        // the setting off: a finding of their own, and the ALTER TABLE finding for the user table only
        DseNodeFixture off = systemInfoEncryptedNode();
        Run offRun = assertBlocked(run(off), "TDE_TABLE");
        assertThat(offRun.codes(Finding.Severity.BLOCKING)).containsExactly("TDE_TABLE");
        assertThat(offRun.findings("TDE_TABLE")).extracting(f -> f.message).containsExactlyInAnyOrder(offMessage, userTableMessage);
        assertThat(offRun.findings("TDE_TABLE")).filteredOn(f -> f.message.equals(offMessage)).singleElement()
                                                .extracting(f -> f.remedy).isEqualTo(offRemedy);
        assertThat(offRun.findings("TDE_TABLE")).filteredOn(f -> f.message.equals(userTableMessage)).singleElement()
                                                .extracting(f -> f.remedy).isEqualTo(userTableRemedy);
        // the parameters of the encrypting compression are not printed
        assertThat(offRun.out).doesNotContain("/etc/dse/conf/system/system_table_keytab").doesNotContain("LocalFileSystemKeyProviderFactory");

        // dse.yaml unreadable: the same finding, next to UNREADABLE_INPUT
        DseNodeFixture unread = systemInfoEncryptedNode();
        Files.write(unread.conf.resolve("dse.yaml"), "system_info_encryption: [\n".getBytes(StandardCharsets.UTF_8));
        Run unreadRun = assertBlocked(run(unread), "UNREADABLE_INPUT");
        assertThat(unreadRun.codes(Finding.Severity.BLOCKING)).containsExactlyInAnyOrder("TDE_TABLE", "UNREADABLE_INPUT");
        assertThat(unreadRun.findings("UNREADABLE_INPUT")).singleElement().satisfies(f -> {
            assertThat(f.message).startsWith("cannot read dse.yaml").endsWith(", so the checks that depend on it cannot be made");
            assertThat(f.remedy).isEqualTo("run the tool as the user DSE runs as (or as root), or point it at the file (--cassandra-yaml, " +
                                           "--dse-yaml, --conf-dir, --dse-home)");
        });
        assertThat(unreadRun.findings("TDE_TABLE")).extracting(f -> f.message).containsExactlyInAnyOrder(offMessage, userTableMessage);
        assertThat(unreadRun.findings("TDE_TABLE")).filteredOn(f -> f.message.equals(offMessage)).singleElement()
                                                   .extracting(f -> f.remedy).isEqualTo(offRemedy);
    }

    /**
     * A node whose schema records the four tables system_info_encryption encrypts with DSE's encrypting compression and
     * its parameters (their table rows written again, their columns as the fixture has them), and the user table
     * lab.secret encrypted the same way.
     */
    private DseNodeFixture systemInfoEncryptedNode() throws IOException
    {
        Map<String, String> encrypting = Map.of("chunk_length_in_kb", "64",
                                                "class", "org.apache.cassandra.io.compress.EncryptingLZ4Compressor",
                                                "cipher_algorithm", "AES",
                                                "key_provider", "com.datastax.bdp.cassandra.crypto.LocalFileSystemKeyProviderFactory",
                                                "secret_key_file", "/etc/dse/conf/system/system_table_keytab",
                                                "secret_key_strength", "128");
        DseNodeFixture node = node();
        for (String table : List.of("system.batches", "system.paxos", "system_backups.backup_configurations", "system_backups.backup_stores"))
        {
            int dot = table.indexOf('.');
            node.writeRows("system_schema", "tables", KEYSPACES_KEY, TABLES_KEY,
                           withTableOptions(row(table.substring(0, dot), table.substring(dot + 1)), stcs(), encrypting).with("flags", Set.of("compound")));
        }
        addTable(node, "lab", "secret", stcs(), encrypting, Set.of("compound"), Map.of());
        return node;
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
                  "Materialized views: not checked completely (not read: the schema)",
                  "Replication of system_auth: not checked (the schema could not be read, or its inspection failed)");
    }

    // ---- the source release ----

    /** The layout the tool reads with --dse-home, --conf-dir, --dse-yaml (its dse.yaml) and --metadata-dir. */
    private static NodeLayout layoutOf(Path dseHome, Path conf, Path metadata)
    {
        NodeLayout.Options options = new NodeLayout.Options();
        options.dseHome = dseHome;
        options.confDir = conf;
        options.dseYaml = conf.resolve("dse.yaml");
        options.metadataDirectory = metadata;
        return new NodeLayout(options);
    }

    /**
     * A DSE 6 node (its nodes/local) is inspected as before; a DSE 5.1 node, told by its ds_version.txt, or without one
     * by its system.local in big 3.x sstables, by the inspection of DSE 5.1, whose report has its own section.
     */
    @Test
    public void theInspectionIsChosenFromTheReleaseOfTheNode() throws IOException
    {
        DseNodeFixture dse6 = node();
        assertThat(DsePreflight.isDse51Node(layoutOf(dse6.dseHome, dse6.conf, dse6.metadata))).isFalse();
        Run run = assertReady(run(dse6));
        assertThat(run.report.dse51).isNull();
        assertThat(run.out).startsWith("DSE 6 preflight inspection");
        // the JVM options of DSE 5.1 do not apply to a DSE 6 node
        Run jvmOptions = run(dse6, "--jvm-options", dse6.conf.resolve("jvm.options").toString());
        assertThat(jvmOptions.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(jvmOptions.err).contains("--jvm-options applies to DSE 5.1 nodes only, and this node is inspected " +
                                            "as a DSE 6 node");

        Dse51NodeFixture dse51 = Dse51NodeFixture.create(temporaryFolder.newFolder().toPath());
        assertThat(DsePreflight.isDse51Node(layoutOf(dse51.dseHome, dse51.conf, dse51.metadata))).isTrue();
        Run report = run(temporaryFolder.getRoot().toPath().resolve("report-dse51.json"), dse51.args());
        assertThat(report.report.dse51.source).isEqualTo("DSE 5.1");
        assertThat(report.out).startsWith("DSE 5.1 preflight inspection");

        // without ds_version.txt, system.local in big me sstables tells a DSE 5.1 node
        Files.delete(dse51.dseHome.resolve("ds_version.txt"));
        assertThat(DsePreflight.isDse51Node(layoutOf(dse51.dseHome, dse51.conf, dse51.metadata))).isTrue();
        // a release before DSE 6 is inspected as DSE 5.1, whose inspection refuses it
        Files.write(dse51.dseHome.resolve("ds_version.txt"), "DSVersion=5.0.15\n".getBytes(StandardCharsets.UTF_8));
        assertThat(DsePreflight.isDse51Node(layoutOf(dse51.dseHome, dse51.conf, dse51.metadata))).isTrue();
        Run older = run(temporaryFolder.getRoot().toPath().resolve("report-dse50.json"), dse51.args());
        assertThat(older.report.dse51.source).isEqualTo("DSE 5.1");
        assertThat(older.messages("INSTALLED_RELEASE_MISMATCH"))
        .contains("is 5.0.15 (ds_version.txt) but this node last ran DSE 5.1.51 (system.local)");
        assertThat(older.findings("INSTALLED_RELEASE_MISMATCH")).extracting(f -> f.severity)
                                                                .containsExactly(Finding.Severity.BLOCKING);
        // a ds_version.txt of DSE 6 decides, and so do DSE 6's nodes files
        Files.write(dse51.dseHome.resolve("ds_version.txt"),
                    "DSVersion=6.8.65-4a7b9eb\n".getBytes(StandardCharsets.UTF_8));
        assertThat(DsePreflight.isDse51Node(layoutOf(dse51.dseHome, dse51.conf, dse51.metadata))).isFalse();
        Files.delete(dse51.dseHome.resolve("ds_version.txt"));
        Files.createDirectories(dse51.metadata.resolve("nodes"));
        Files.write(dse51.metadata.resolve("nodes").resolve("local"), new byte[0]);
        assertThat(DsePreflight.isDse51Node(layoutOf(dse51.dseHome, dse51.conf, dse51.metadata))).isFalse();
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
        assertThat(run.findings("UNLOADABLE_KEYSPACE").get(0).remedy).isEqualTo("ALTER KEYSPACE on DSE to a replication strategy HCD 2.0 has " +
                                                                               "(NetworkTopologyStrategy, SimpleStrategy, EverywhereStrategy)");
    }

    @Test
    public void systemAuthOnASingleNodeIsReadyWithOneReplica() throws IOException
    {
        // a single-node installation on DSE's default replication of system_auth, SimpleStrategy with one replica
        DseNodeFixture node = node();
        node.editPeers(List::clear);
        writeAuthReplication(node, simpleStrategy());
        Run run = assertReady(run(node));
        assertThat(run.report.peers).isEmpty();
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'SimpleStrategy', 'replication_factor': '1'}, " +
                                                                 "enough in every data center of nodes/local and nodes/peers");

        // NetworkTopologyStrategy with one replica in the one data center of one node
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();

        // more replicas than the one node can make a LOCAL_QUORUM of: the upgraded node can never start
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "3"));
        Run simple = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(simple.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', " +
                                                                         "'replication_factor': '3'}: dc1 has 3 replicas and only 1 node, so " +
                                                                         "LOCAL_QUORUM needs 2 replicas up there, more than its nodes" +
                                                                         AUTH_REPLICATION_BLOCKING);
        assertThat(simple.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 1", "", false));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "2"));
        Run two = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(two.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '2'}: " +
                                                                      "dc1 has 2 replicas and only 1 node, so LOCAL_QUORUM needs 2 replicas up there, " +
                                                                      "more than its nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(two.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 1", "", false));
    }

    @Test
    public void systemAuthOnSimpleStrategyWithOneReplicaInADataCenterOfThreeNodesWarns() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, simpleStrategy());
        Run run = assertReady(run(node));
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).hasSize(1);
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '1'}: " +
                                                                      "dc1 has 1 replica and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_SHORT);
        // the data center has 3 nodes: the remedy says nothing of 2 nodes, nor of several data centers
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: see SYSTEM_AUTH_REPLICATION");

        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "2"));
        Run two = assertReady(run(node));
        assertThat(two.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '2'}: " +
                                                                      "dc1 has 2 replicas and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_SHORT);
        assertThat(two.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));

        // with as many replicas as nodes, an upgraded node starts while another node is down
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "3"));
        Run three = assertReady(run(node));
        assertThat(three.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(three.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'SimpleStrategy', 'replication_factor': '3'}, " +
                                                                   "enough in every data center of nodes/local and nodes/peers");

        // 5 replicas over 3 nodes make LOCAL_QUORUM need every node: the factor is not carried over above the nodes
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "5"));
        Run five = assertReady(run(node));
        assertThat(five.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '5'}: " +
                                                                       "dc1 has 5 replicas and only 3 nodes, so LOCAL_QUORUM needs 3 replicas up there: " +
                                                                       "an upgraded node there cannot start while another node of dc1 is down" +
                                                                       AUTH_REPLICATION_ABOVE);
        assertThat(five.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));

        // with the nodes of the data center not all known, the check is by hand
        node.editPeers(peers -> peers.forEach(peer -> peer.remove("data_center")));
        Run notKnown = assertReady(run(node));
        assertThat(notKnown.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(notKnown.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data centers of 2 nodes are not known from nodes/local and nodes/peers, so the " +
                                                                           "replication of system_auth, {'class': 'SimpleStrategy', 'replication_factor': " +
                                                                           "'5'}, was checked against the nodes whose data center is known only");
        assertThat(notKnown.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);
    }

    @Test
    public void systemAuthOnSimpleStrategyWithNoReplicaBlocks() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "0"));
        Run none = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(none.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '0'}: " +
                                                                       "its replication_factor is 0" + AUTH_REPLICATION_BLOCKING);
        assertThat(none.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        // a negative factor is no number of replicas
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "-1"));
        Run negative = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(negative.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', " +
                                                                           "'replication_factor': '-1'}: its replication_factor is '-1', not a " +
                                                                           "number of replicas" + AUTH_REPLICATION_BLOCKING);
        assertThat(negative.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY));
        Run missing = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(missing.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy'}: it has no " +
                                                                          "replication_factor" + AUTH_REPLICATION_BLOCKING);
        assertThat(missing.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));
    }

    @Test
    public void systemAuthOnSimpleStrategyWithSeveralDataCentersBlocksWithEachDataCenter() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> peers.get(1).put("data_center", "dc2"));
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "3"));
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '3'}: " +
                                                                      "SimpleStrategy does not place replicas in each data center, and nodes/local and " +
                                                                      "nodes/peers name 2 (dc1, dc2), so LOCAL_QUORUM cannot be met in each" +
                                                                      AUTH_REPLICATION_BLOCKING);
        // the replication factor of SimpleStrategy spans the data centers, so it gives no data center its replicas
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedy("'dc1': 2, 'dc2': 1", "; the replication factor of SimpleStrategy spans the data centers, so it is not carried " +
                                                                   "over", true));

        // DSE's default blocks too
        writeAuthReplication(node, simpleStrategy());
        Run byDefault = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(byDefault.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', " +
                                                                            "'replication_factor': '1'}: SimpleStrategy does not place replicas in " +
                                                                            "each data center, and nodes/local and nodes/peers name 2 (dc1, dc2), so " +
                                                                            "LOCAL_QUORUM cannot be met in each" + AUTH_REPLICATION_BLOCKING);
        assertThat(byDefault.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedy("'dc1': 2, 'dc2': 1", "; the replication factor of SimpleStrategy spans the data centers, so it is not carried " +
                                                                   "over", true));
    }

    @Test
    public void systemAuthWithNoReplicationClassOrAnotherOneBlocks() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, Map.of());
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is not known: it has no replication class" +
                                                                      AUTH_REPLICATION_BLOCKING);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        // a class HCD 2.0 does not have (removed in Cassandra 4.0), which UNLOADABLE_KEYSPACE reports too
        writeAuthReplication(node, Map.of("class", "org.apache.cassandra.locator.OldNetworkTopologyStrategy"));
        Run other = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(other.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'OldNetworkTopologyStrategy'}: " +
                                                                        "its class is OldNetworkTopologyStrategy (none of NetworkTopologyStrategy, " +
                                                                        "SimpleStrategy and EverywhereStrategy)" + AUTH_REPLICATION_BLOCKING);
        assertThat(other.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));
        assertThat(other.messages("UNLOADABLE_KEYSPACE")).contains("system_auth: ").contains("OldNetworkTopologyStrategy");
    }

    @Test
    public void systemAuthOnEverywhereStrategyByTheQuorumOfTheRing() throws IOException
    {
        // DSE's EverywhereStrategy, which HCD 2.0 has: in one data center, every node holds a replica
        DseNodeFixture node = node();
        writeAuthReplication(node, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run one = assertReady(run(node));
        assertThat(one.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(one.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'EverywhereStrategy'}, enough in every data " +
                                                                 "center of nodes/local and nodes/peers");

        // over several data centers the LOCAL_QUORUM of the 3 replicas of the ring is 2, which the data center of 1
        // node lacks
        node.editPeers(peers -> peers.get(1).put("data_center", "dc2"));
        Run several = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(several.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'EverywhereStrategy'}: dc2 holds 1 " +
                                                                          "of the 3 replicas of the ring " + EVERYWHERE + ", so LOCAL_QUORUM needs 2 " +
                                                                          "replicas up there, more than its nodes" + AUTH_REPLICATION_BLOCKING);
        // the replicas of the ring are not carried over
        assertThat(several.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 2, 'dc2': 1", EVERYWHERE_NOT_CARRIED, true));

        // the two data centers of 3 nodes: 4 replicas up of the 6 of the ring, in each
        DseNodeFixture twoDataCenters = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCenters(1));
        Files.write(twoDataCenters.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        writeAuthReplication(twoDataCenters, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run six = assertBlocked(run(twoDataCenters), "SYSTEM_AUTH_REPLICATION");
        String sixOfTheRing = "dc1 holds 3 of the 6 replicas of the ring " + EVERYWHERE + ", so LOCAL_QUORUM needs 4 replicas up there, more than " +
                              "its nodes; dc2 holds 3 of the 6 replicas of the ring " + EVERYWHERE + ", so LOCAL_QUORUM needs 4 replicas up " +
                              "there, more than its nodes";
        assertThat(six.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'EverywhereStrategy'}: " + sixOfTheRing +
                                                                      AUTH_REPLICATION_BLOCKING);
        assertThat(six.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3, 'dc2': 3", EVERYWHERE_NOT_CARRIED,
                                                                                                           false));

        // with a node of no data center, the two data centers known still cannot both meet the quorum of the ring
        addPeer(twoDataCenters, 199, false);
        Run uncounted = assertBlocked(run(twoDataCenters), "SYSTEM_AUTH_REPLICATION");
        assertThat(uncounted.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'EverywhereStrategy'}: " +
                                                                            "with EverywhereStrategy, LOCAL_QUORUM counts the replicas of the whole " +
                                                                            "ring, which only a data center of more than half of the nodes meets, and " +
                                                                            "nodes/local and nodes/peers name 2 (dc1, dc2), so LOCAL_QUORUM cannot be " +
                                                                            "met in each" + AUTH_REPLICATION_BLOCKING);
        assertThat(uncounted.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 3, 'dc2': 3", EVERYWHERE_NOT_CARRIED + "; the data center of 1 node is not known from " +
                                                                           "nodes/local and nodes/peers: check by hand the data center of each node " +
                                                                           "and its replicas" + LOWER_TO_THE_NODES, AUTH_REPLICATION_OTHERS_BY_HAND));

        // the data centers of the peers not known: only this node's data center known, so the class only
        DseNodeFixture noPeers = node();
        noPeers.editPeers(peers -> peers.forEach(peer -> peer.remove("data_center")));
        writeAuthReplication(noPeers, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run withoutPeers = run(noPeers);
        assertThat(withoutPeers.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(withoutPeers.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(withoutPeers.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data centers of 2 nodes are not known from nodes/local and nodes/peers, " +
                                                                               "so the replication of system_auth, {'class': 'EverywhereStrategy'}, was " +
                                                                               "checked for its class only, not for the replicas of each data center");
        assertThat(withoutPeers.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("check by hand, before the first node of the cluster is upgraded, " + AUTH_REPLICATION_CHECK + " (EverywhereStrategy: " +
                       "LOCAL_QUORUM counts the replicas of the whole ring, which only a data center of more than half of the nodes meets); " +
                       AUTH_REPLICATION_RECOMMENDED + AUTH_REPLICATION_OTHER_KEYSPACES);

        // the mixed phase of the two-data-center cluster: blocking, with the remedy to upgrade no further node
        DseNodeFixture mixed = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCentersMixed(1));
        Files.write(mixed.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        writeAuthReplication(mixed, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run mixedRun = assertBlocked(run(mixed), "SYSTEM_AUTH_REPLICATION");
        assertThat(mixedRun.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'EverywhereStrategy'}: " +
                                                                           sixOfTheRing + AUTH_REPLICATION_BLOCKING);
        assertThat(mixedRun.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY);

        // dc1 of 3 nodes and dc2 of 1: LOCAL_QUORUM of the ring of 4 is 3, which dc2 lacks and which needs every node of dc1
        DseNodeFixture fourNodes = node();
        addPeer(fourNodes, 199, true);
        fourNodes.editPeers(peers -> peers.get(1).put("data_center", "dc2"));
        writeAuthReplication(fourNodes, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run besides = assertBlocked(run(fourNodes), "SYSTEM_AUTH_REPLICATION");
        assertThat(besides.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'EverywhereStrategy'}: dc2 holds 1 " +
                                                                          "of the 4 replicas of the ring " + EVERYWHERE + ", so LOCAL_QUORUM needs 3 " +
                                                                          "replicas up there, more than its nodes" + AUTH_REPLICATION_BLOCKING +
                                                                          "; besides, dc1 holds 3 of the 4 replicas of the ring " + EVERYWHERE + ", so " +
                                                                          "LOCAL_QUORUM needs 3 replicas up there: an upgraded node there cannot start " +
                                                                          "while another node of dc1 is down");
        assertThat(besides.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3, 'dc2': 1", EVERYWHERE_NOT_CARRIED,
                                                                                                               false));

        // with a data center of more than 3 nodes, the ALTER does not keep the replica on each of its nodes: dc1 of 4 nodes, dc2 of 1
        addPeer(fourNodes, 198, true);
        Run fiveNodes = assertBlocked(run(fourNodes), "SYSTEM_AUTH_REPLICATION");
        assertThat(fiveNodes.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'EverywhereStrategy'}: dc2 holds " +
                                                                            "1 of the 5 replicas of the ring " + EVERYWHERE + ", so LOCAL_QUORUM needs 3 " +
                                                                            "replicas up there, more than its nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(fiveNodes.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3, 'dc2': 1", EVERYWHERE_NOT_CARRIED,
                                                                                                                 false));

        // in one data center known, with a node of no data center: the class only, and the check by hand says how it
        // counts
        DseNodeFixture oneKnown = node();
        addPeer(oneKnown, 199, false);
        writeAuthReplication(oneKnown, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run classOnly = run(oneKnown);
        assertThat(classOnly.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(classOnly.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(classOnly.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so " +
                                                                            "the replication of system_auth, {'class': 'EverywhereStrategy'}, was " +
                                                                            "checked for its class only, not for the replicas of each data center");
        assertThat(classOnly.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("check by hand, before the first node of the cluster is upgraded, " + AUTH_REPLICATION_CHECK + " (EverywhereStrategy: " +
                       "LOCAL_QUORUM counts the replicas of the whole ring, which only a data center of more than half of the nodes meets); " +
                       AUTH_REPLICATION_RECOMMENDED + AUTH_REPLICATION_OTHER_KEYSPACES);
    }

    @Test
    public void systemAuthOnNetworkTopologyStrategyWithThreeReplicasIsReady() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Run run = assertReady(run(node));
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.report.peers).hasSize(2).allMatch(p -> "dc1".equals(p.dataCenter));
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '3'}, enough in " +
                                                                 "every data center of nodes/local and nodes/peers");
    }

    @Test
    public void systemAuthWithFewerReplicasThanThreeInADataCenterOfThreeNodesWarns() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1"));
        Run run = assertReady(run(node));
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '1'}: " +
                                                                      "dc1 has 1 replica and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_SHORT);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));

        writeAuthReplication(node, networkTopologyStrategy("dc1", "2"));
        Run two = assertReady(run(node));
        assertThat(two.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '2'}: " +
                                                                      "dc1 has 2 replicas and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_SHORT);
        assertThat(two.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));
    }

    @Test
    public void systemAuthWithNoReplicaOrNoNumberInADataCenterBlocks() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "0"));
        Run none = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(none.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '0'}: " +
                                                                       "dc1 has 0 replicas and 3 nodes" +
                                                                       AUTH_REPLICATION_BLOCKING);
        assertThat(none.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        writeAuthReplication(node, networkTopologyStrategy("dc1", "three"));
        Run noNumber = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(noNumber.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                           "'dc1': 'three'}: dc1 is listed with 'three', not a number of replicas" +
                                                                           AUTH_REPLICATION_BLOCKING);
        assertThat(noNumber.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        // a data center this node does not know of, with no replica, is no problem; with no number, it is left out of
        // the ALTER KEYSPACE
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "old", "0"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "old", "x"));
        Run unknownNoNumber = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(unknownNoNumber.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': " +
                                                                                  "'NetworkTopologyStrategy', 'dc1': '3', 'old': 'x'}: old is " +
                                                                                  "listed with 'x', not a number of replicas" + AUTH_REPLICATION_BLOCKING);
        assertThat(unknownNoNumber.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedy("'dc1': 3", "; old, listed with no number of replicas and with no node in nodes/local and nodes/peers, " +
                                                         "left out", false));

        // a negative number is no number of replicas
        writeAuthReplication(node, networkTopologyStrategy("dc1", "-1"));
        Run negative = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(negative.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': " +
                                                                           "'-1'}: dc1 is listed with '-1', not a number of replicas" +
                                                                           AUTH_REPLICATION_BLOCKING);
        assertThat(negative.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        // replication_factor is no data center
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "replication_factor", "3"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
    }

    @Test
    public void systemAuthInADataCenterOfTwoNodesByItsQuorum() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> peers.remove(1));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "2"));
        Run run = assertReady(run(node));
        assertThat(run.report.peers).hasSize(1);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();

        // with 2 nodes, LOCAL_QUORUM needs both whatever the replicas: one replica is no warning
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        writeAuthReplication(node, simpleStrategy());
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        // 3 replicas have the LOCAL_QUORUM of 2, which the 2 nodes meet
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();

        // 4 replicas have the LOCAL_QUORUM of 3, which the 2 nodes never meet
        writeAuthReplication(node, networkTopologyStrategy("dc1", "4"));
        Run four = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(four.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '4'}: " +
                                                                       "dc1 has 4 replicas and only 2 nodes, so LOCAL_QUORUM needs 3 replicas up there, " +
                                                                       "more than its nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(four.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 2", "", true));
    }

    @Test
    public void systemAuthWithADataCenterOfTheTwoDataCenterClusterNotListedBlocks() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCenters(1));
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.report.peers).hasSize(DseFixtures.TWO_DC_NODES - 1);
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '3'}: " +
                                                                      "dc2 (3 nodes) is not listed" + AUTH_REPLICATION_BLOCKING);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3, 'dc2': 3", "", false));

        // a shortfall where something else blocks is named after it, since the ALTER raises that data center too
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1"));
        Run both = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(both.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '1'}: " +
                                                                       "dc2 (3 nodes) is not listed" + AUTH_REPLICATION_BLOCKING + "; besides, dc1 has " +
                                                                       "1 replica and 3 nodes: an upgraded node there cannot start while another node " +
                                                                       "of dc1 is down");
        assertThat(both.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3, 'dc2': 3", "", false));

        // too few replicas in one data center and too many in the other: both said
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1", "dc2", "4"));
        Run fewerAndAbove = assertReady(run(node));
        assertThat(fewerAndAbove.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                                "'dc1': '1', 'dc2': '4'}: dc1 has 1 replica and 3 nodes: an upgraded " +
                                                                                "node there cannot start while another node of dc1 is down; dc2 has 4 " +
                                                                                "replicas and only 3 nodes, so LOCAL_QUORUM needs 3 replicas up there: an " +
                                                                                "upgraded node there cannot start while another node of dc2 is down" +
                                                                                AUTH_REPLICATION_READ + "; more replicas (3 where a data center has " +
                                                                                "fewer), and as many as its nodes where it has more replicas than nodes, let " +
                                                                                "the upgrade go on while a node is down");
        assertThat(fewerAndAbove.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationWarningRemedy("'dc1': 3, 'dc2': 3", "", false));

        // a shortfall in the other data center is a warning once nothing blocks
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "1"));
        Run shortfall = assertReady(run(node));
        assertThat(shortfall.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                            "'dc1': '3', 'dc2': '1'}: dc2 has 1 replica and 3 nodes: an upgraded node " +
                                                                            "there cannot start while another node of dc2 is down" + AUTH_REPLICATION_SHORT);
        assertThat(shortfall.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3, 'dc2': 3", "", false));

        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        Run ready = assertReady(run(node));
        assertThat(ready.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(ready.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3'}, " +
                                                                   "enough in every data center of nodes/local and nodes/peers");
    }

    @Test
    public void systemAuthInADataCenterOfOneNodeNeedsOneReplica() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> peers.get(1).put("data_center", "dc2"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                      "'dc1': '3'}: dc2 (1 node) is not listed" + AUTH_REPLICATION_BLOCKING);
        // 3 replicas in the 2 nodes of dc1 have the quorum of 2, so they are no problem, but the remedy keeps them within the nodes
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 2, 'dc2': 1", "", true));

        // every data center listed, one replica in the data center of one node: no finding, no warning
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "1"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1", "dc2", "1"));
        assertThat(assertReady(run(node)).findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
    }

    @Test
    public void systemAuthWithMoreReplicasThanNodesWarnsOrBlocksByItsQuorum() throws IOException
    {
        // 4 or 5 replicas over 3 nodes make LOCAL_QUORUM need every node: a warning
        DseNodeFixture node = node();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "5"));
        Run five = assertReady(run(node));
        assertThat(five.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '5'}: " +
                                                                       "dc1 has 5 replicas and only 3 nodes, so LOCAL_QUORUM needs 3 replicas up there: an " +
                                                                       "upgraded node there cannot start while another node of dc1 is down" +
                                                                       AUTH_REPLICATION_ABOVE);
        assertThat(five.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "4"));
        Run four = assertReady(run(node));
        assertThat(four.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(four.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '4'}: " +
                                                                       "dc1 has 4 replicas and only 3 nodes, so LOCAL_QUORUM needs 3 replicas up there: an " +
                                                                       "upgraded node there cannot start while another node of dc1 is down" +
                                                                       AUTH_REPLICATION_ABOVE);
        assertThat(four.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3", "", false));
        // 6 replicas over 3 nodes make LOCAL_QUORUM need 4: blocking
        writeAuthReplication(node, networkTopologyStrategy("dc1", "6"));
        Run six = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(six.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '6'}: " +
                                                                      "dc1 has 6 replicas and only 3 nodes, so LOCAL_QUORUM needs 4 replicas up there, " +
                                                                      "more than its nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(six.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 3", "", false));

        // 3 replicas in a data center of one node need 2 replicas up there, which it never has
        node.editPeers(peers -> peers.get(1).put("data_center", "o'dc"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "2", "o'dc", "3"));
        Run one = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(one.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '2', " +
                                                                      "'o''dc': '3'}: o'dc has 3 replicas and only 1 node, so LOCAL_QUORUM needs 2 " +
                                                                      "replicas up there, more than its nodes" + AUTH_REPLICATION_BLOCKING);
        // a data center name is quoted as CQL quotes it
        assertThat(one.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 2, 'o''dc': 1", "", true));
    }

    @Test
    public void systemAuthInADataCenterOfFourNodesByItsQuorum() throws IOException
    {
        DseNodeFixture node = node();
        addPeer(node, 199, true);
        // 5 replicas over 4 nodes have the LOCAL_QUORUM of 3: one node can be down
        writeAuthReplication(node, networkTopologyStrategy("dc1", "5"));
        Run five = assertReady(run(node));
        assertThat(five.report.peers).hasSize(3);
        assertThat(five.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        // 6 replicas have the LOCAL_QUORUM of 4, every node
        writeAuthReplication(node, networkTopologyStrategy("dc1", "6"));
        Run six = assertReady(run(node));
        assertThat(six.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '6'}: " +
                                                                      "dc1 has 6 replicas and only 4 nodes, so LOCAL_QUORUM needs 4 replicas up there: an " +
                                                                      "upgraded node there cannot start while another node of dc1 is down" +
                                                                      AUTH_REPLICATION_ABOVE);
        assertThat(six.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 4", "", false));
        // 8 replicas have the LOCAL_QUORUM of 5, more than the nodes
        writeAuthReplication(node, networkTopologyStrategy("dc1", "8"));
        Run eight = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(eight.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '8'}: " +
                                                                        "dc1 has 8 replicas and only 4 nodes, so LOCAL_QUORUM needs 5 replicas up there, " +
                                                                        "more than its nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(eight.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 4", "", false));
        // 2 replicas: too few, a warning
        writeAuthReplication(node, networkTopologyStrategy("dc1", "2"));
        assertThat(assertReady(run(node)).messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': " +
                                                                                         "'NetworkTopologyStrategy', 'dc1': '2'}: dc1 has 2 replicas " +
                                                                                         "and 4 nodes: an upgraded node there cannot start while " +
                                                                                         "another node of dc1 is down" + AUTH_REPLICATION_SHORT);
    }

    @Test
    public void theOptionalRemedyOfSystemAuthNamesADataCenterOfTwoNodes() throws IOException
    {
        // dc1 of 3 nodes, dc2 of 2 nodes
        DseNodeFixture node = node();
        addPeer(node, 198, true);
        addPeer(node, 199, true);
        node.editPeers(peers -> peers.subList(2, 4).forEach(peer -> peer.put("data_center", "dc2")));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1", "dc2", "1"));
        Run run = assertReady(run(node));
        assertThat(run.report.peers).hasSize(4);
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '1', " +
                                                                      "'dc2': '1'}: dc1 has 1 replica and 3 nodes: an upgraded node there cannot start " +
                                                                      "while another node of dc1 is down" + AUTH_REPLICATION_SHORT);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationWarningRemedy("'dc1': 3, 'dc2': 2", "", true));
    }

    @Test
    public void theRemedyOfSystemAuthLeavesOutTheDataCentersOfNoNode() throws IOException
    {
        DseNodeFixture node = node();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "0", "old", "3"));
        Run unknown = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(unknown.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                          "'dc1': '0', 'old': '3'}: dc1 has 0 replicas and 3 nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(unknown.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedy("'dc1': 3", "; old, listed but with no node in nodes/local and nodes/peers, left out", false));

        // and so does the remedy of the warning
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1", "old", "3"));
        Run shortfall = assertReady(run(node));
        assertThat(shortfall.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': " +
                                                                            "'NetworkTopologyStrategy', 'dc1': '1', 'old': '3'}: dc1 has 1 replica " +
                                                                            "and 3 nodes: an upgraded node there cannot start while another node " +
                                                                            "of dc1 is down" + AUTH_REPLICATION_SHORT);
        assertThat(shortfall.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationWarningRemedy("'dc1': 3", "; old, listed but with no node in nodes/local and nodes/peers, left out", false));
    }

    @Test
    public void systemAuthWithNoDataCenterKnownIsCheckedForItsClassOnly() throws IOException
    {
        DseNodeFixture node = node();
        node.editLocal(local -> local.remove("data_center"));
        node.editPeers(peers -> peers.forEach(peer -> peer.remove("data_center")));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1"));
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("no data center is known from nodes/local and nodes/peers, so the replication " +
                                                                      "of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '1'}, was checked " +
                                                                      "for its class only, not for the replicas of each data center");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '1'}, checked " +
                                                                 "for its class only (no data center is known from nodes/local and nodes/peers)");

        // SimpleStrategy may be the replication of a single data center: checked by hand too
        writeAuthReplication(node, simpleStrategy());
        Run simple = run(node);
        assertThat(simple.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(simple.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(simple.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("no data center is known from nodes/local and nodes/peers, so the replication " +
                                                                         "of system_auth, {'class': 'SimpleStrategy', 'replication_factor': '1'}, was " +
                                                                         "checked for its class only, not for the replicas of each data center");
        assertThat(simple.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);

        // a replication with no replica anywhere still blocks, and the remedy names no data center
        String noDataCenter = "; no data center is known from nodes/local and nodes/peers: list each data center nodetool status shows" +
                              LOWER_TO_THE_NODES;
        writeAuthReplication(node, networkTopologyStrategy());
        Run nowhere = run(node);
        assertThat(nowhere.codes(Finding.Severity.BLOCKING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(nowhere.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy'}: it " +
                                                                          "lists no data center with a replica" + AUTH_REPLICATION_READ + ", so every " +
                                                                          "data center of the cluster (nodetool status names them) needs replicas of " +
                                                                          "system_auth (with several data centers, NetworkTopologyStrategy listing " +
                                                                          "each), no more in a data center than its nodes can make a LOCAL_QUORUM of, " +
                                                                          "or an upgraded node there cannot start, even with every node up");
        assertThat(nowhere.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'<dc>': 3", noDataCenter, AUTH_REPLICATION_OTHER_KEYSPACES));
    }

    @Test
    public void systemAuthWithANodeOfNoDataCenterIsCheckedAgainstTheOthers() throws IOException
    {
        DseNodeFixture node = node();
        node.editPeers(peers -> peers.get(1).remove("data_center"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "2"));
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                      "replication of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '2'}, " +
                                                                      "was checked against the nodes whose data center is known only");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '2'}, checked " +
                                                                 "against the nodes whose data center is known only (the data center of 1 node is " +
                                                                 "not known from nodes/local and nodes/peers)");

        // with a problem, the remedy asks for the check by hand of that node, and keeps every data center listed
        String uncounted = "; the data center of 1 node is not known from nodes/local and nodes/peers: check by hand the data center of each " +
                           "node and its replicas" + LOWER_TO_THE_NODES;
        writeAuthReplication(node, networkTopologyStrategy("dc1", "0", "dc2", "3"));
        Run problem = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(problem.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                          "'dc1': '0', 'dc2': '3'}: dc1 has 0 replicas and 2 nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(problem.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 3, 'dc2': 3", uncounted, AUTH_REPLICATION_OTHER_KEYSPACES));

        // a data center listed with no number that no node counted is in is named, and makes the replication one of several data centers
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "x"));
        Run unnumbered = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(unnumbered.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': " +
                                                                             "'NetworkTopologyStrategy', 'dc1': '3', 'dc2': 'x'}: dc2 is listed " +
                                                                             "with 'x', not a number of replicas" + AUTH_REPLICATION_BLOCKING);
        assertThat(unnumbered.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 3", uncounted + "; dc2, listed with no number of replicas, left out: set it by hand if that " +
                                                                 "data center has nodes", AUTH_REPLICATION_OTHER_KEYSPACES));

        // SimpleStrategy over the two data centers known still blocks, whatever the data center of the other node
        node.editPeers(peers -> peers.get(0).put("data_center", "dc2"));
        writeAuthReplication(node, simpleStrategy());
        Run simple = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(simple.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', " +
                                                                         "'replication_factor': '1'}: SimpleStrategy does not place replicas in " +
                                                                         "each data center, and nodes/local and nodes/peers name 2 (dc1, dc2), so " +
                                                                         "LOCAL_QUORUM cannot be met in each" + AUTH_REPLICATION_BLOCKING);
        node.editPeers(peers -> peers.get(0).put("data_center", "dc1"));

        // a data center listed that no node counted is in is raised to 3 replicas too, the unknown node perhaps in it
        writeAuthReplication(node, networkTopologyStrategy("dc1", "0", "dc2", "1"));
        Run raised = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(raised.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 3, 'dc2': 3", uncounted, AUTH_REPLICATION_OTHER_KEYSPACES));

        // with only this node counted, a replication listing another data center alone names both, and the other keyspaces
        node.editPeers(peers -> peers.forEach(peer -> peer.remove("data_center")));
        writeAuthReplication(node, networkTopologyStrategy("dc2", "3"));
        Run other = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(other.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                        "'dc2': '3'}: dc1 (1 node) is not listed" + AUTH_REPLICATION_BLOCKING);
        assertThat(other.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 3, 'dc2': 3", "; the data centers of 2 nodes are not known from nodes/local and nodes/peers: " +
                                                                           "check by hand the data center of each node and its replicas" +
                                                                           LOWER_TO_THE_NODES, AUTH_REPLICATION_OTHER_KEYSPACES));
    }

    @Test
    public void systemAuthWithAShortfallAmongTheNodesCountedIsNamedInTheCheckByHand() throws IOException
    {
        // 3 nodes of dc1 counted, a fourth of no data center: one replica is too few whatever that node's data center
        DseNodeFixture node = node();
        addPeer(node, 199, false);
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1"));
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                      "replication of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '1'}, " +
                                                                      "was checked against the nodes whose data center is known only; dc1 has 1 " +
                                                                      "replica and at least 3 nodes: an upgraded node there cannot start while " +
                                                                      "another node of dc1 is down" + AUTH_REPLICATION_READ);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);

        // the nodes counted a lower bound, more replicas than them are no shortfall: the node of no data center may be
        // in dc1
        writeAuthReplication(node, networkTopologyStrategy("dc1", "5"));
        Run above = run(node);
        assertThat(above.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(above.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(above.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                        "replication of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '5'}, " +
                                                                        "was checked against the nodes whose data center is known only");
        assertThat(above.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);
    }

    @Test
    public void systemAuthWithMoreReplicasThanThreeIsKeptWhereTheDataCenterHasTheNodes() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCenters(1));
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        // two of the three nodes of dc2 moved to dc1: dc1 has 5 nodes, dc2 one
        node.editPeers(peers -> peers.stream().filter(peer -> "dc2".equals(peer.get("data_center"))).limit(2).forEach(peer -> peer.put("data_center", "dc1")));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "5"));
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                      "'dc1': '5'}: dc2 (1 node) is not listed" + AUTH_REPLICATION_BLOCKING);
        // no data center of 2 nodes: the remedy says nothing of one
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(authReplicationRemedy("'dc1': 5, 'dc2': 1", "", false));
    }

    @Test
    public void systemAuthWithAReplicationThatCannotBeReadIsLeftToUnloadableKeyspace() throws IOException
    {
        DseNodeFixture node = node();
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("system_auth").with("durable_writes", true).with("replication", ByteBuffer.wrap(new byte[]{ 0, 0, 0, 9 })));
        Run run = assertBlocked(run(node), "UNLOADABLE_KEYSPACE");
        assertThat(run.messages("UNLOADABLE_KEYSPACE")).contains("system_auth: ");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("INSPECTION_ERROR");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: not checked (its replication cannot be read, see " +
                                                                 "UNLOADABLE_KEYSPACE)");
    }

    @Test
    public void systemAuthReplicationIsNotCappedByTheNodesCountedWhenSomeAreNotKnown() throws IOException
    {
        // dc1 has this node and the peer of no data center, perhaps: 3 replicas there are no problem
        DseNodeFixture node = node();
        node.editPeers(peers -> {
            peers.get(0).put("data_center", "dc2");
            peers.get(1).remove("data_center");
        });
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "1"));
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        // no quorum problem nor shortfall: the nodes counted are a lower bound
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                      "replication of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '3', " +
                                                                      "'dc2': '1'}, was checked against the nodes whose data center is known only");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHERS_BY_HAND);

        // with a problem, the replicas listed are kept as they are, above the nodes counted and above 3
        writeAuthReplication(node, networkTopologyStrategy("dc1", "5"));
        Run problem = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(problem.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                          "'dc1': '5'}: dc2 (1 node) is not listed" + AUTH_REPLICATION_BLOCKING);
        assertThat(problem.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 5, 'dc2': 3", "; the data center of 1 node is not known from nodes/local and nodes/peers: " +
                                                                           "check by hand the data center of each node and its replicas" +
                                                                           LOWER_TO_THE_NODES, AUTH_REPLICATION_OTHERS_BY_HAND));

        // so is a data center listed above 3 that no node counted is in
        writeAuthReplication(node, networkTopologyStrategy("dc1", "5", "old", "4"));
        Run old = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(old.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo(authReplicationRemedyNotExact("'dc1': 5, 'dc2': 3, 'old': 4", "; the data center of 1 node is not known from nodes/local and " +
                                                                                     "nodes/peers: check by hand the data center of each node and its " +
                                                                                     "replicas" + LOWER_TO_THE_NODES, AUTH_REPLICATION_OTHERS_BY_HAND));
    }

    @Test
    public void systemAuthReplicationWithNodesFilesThatCannotBeReadIsAWarning() throws IOException
    {
        DseNodeFixture node = node();
        Files.write(node.metadata.resolve("nodes").resolve("peers"), new byte[]{ (byte) 0xc1, 0, 1, 2 });
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        String hostId = UUID.randomUUID().toString();
        Files.write(node.hints.resolve(hostId + "-1790335633208-v1.hints"), new byte[0]);
        Run run = assertBlocked(run(node), "UNREADABLE_INPUT");
        assertThat(run.messages("UNREADABLE_INPUT")).startsWith("cannot read nodes/peers: cannot decode ")
                                                    .endsWith("; the releases and workloads of the peers cannot be checked");
        assertThat(run.findings("UNREADABLE_INPUT").get(0).remedy).isEqualTo("run the tool as the user DSE runs as (or as root); a damaged nodes/peers " +
                                                                             "must be repaired on DSE first");
        // a DSE hints file for a host id of no known node is only said, nodes/peers not being read
        assertThat(run.findings("PENDING_HINTS")).hasSize(1);
        assertThat(run.findings("PENDING_HINTS").get(0).severity).isEqualTo(Finding.Severity.INFO);
        assertThat(run.messages("PENDING_HINTS")).isEqualTo(HINTS_NOT_CHECKED + hostId + ": 1 file(s)" + HINTS_NOT_CHECKED_END);
        assertThat(run.report.upgradePath.upgradeSteps).contains("Hints: 1 host id(s) with DSE hints files, not checked against nodes/local and " +
                                                                 "nodes/peers (not both read and trusted); see PENDING_HINTS");
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo(NODES_FILES_NOT_READ + ", so the replication of system_auth, {'class': " +
                                                                      "'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3'}, was checked against the " +
                                                                      "nodes whose data center is known only");
        // the nodes counted are a lower bound, so the remedy names the other keyspaces too
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES +
                                                                                    MIXED_PHASE_UNKNOWN);

        // with a file for this node too, a target known: both said
        Files.write(node.hints.resolve(run.report.node.hostId + "-1790335633208-v1.hints"), new byte[0]);
        assertThat(assertBlocked(run(node), "UNREADABLE_INPUT").report.upgradePath.upgradeSteps)
            .contains("Hints: 1 target(s) with DSE hints files pending; see PENDING_HINTS; 1 host id(s) with DSE hints files, not checked " +
                      "against nodes/local and nodes/peers (not both read and trusted); see PENDING_HINTS");

        // nodes/local missing too: no data center is known
        Files.delete(node.metadata.resolve("nodes").resolve("local"));
        Run noLocal = assertBlocked(run(node), "NOT_A_DSE_NODE");
        assertThat(noLocal.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(noLocal.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(noLocal.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo(NODES_FILES_NOT_READ + ", so the replication of system_auth, " +
                                                                          "{'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3'}, was checked for " +
                                                                          "its class only, not for the replicas of each data center");
        assertThat(noLocal.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES +
                                                                                        MIXED_PHASE_UNKNOWN);
    }

    @Test
    public void systemAuthReplicationWithNodesLocalMissingIsAWarning() throws IOException
    {
        // nodes/local missing, nodes/peers read: the two peers of dc1 are counted, as a lower bound
        DseNodeFixture noLocalNode = node();
        Files.delete(noLocalNode.metadata.resolve("nodes").resolve("local"));
        writeAuthReplication(noLocalNode, networkTopologyStrategy("dc1", "3"));
        Run noLocal = assertBlocked(run(noLocalNode), "NOT_A_DSE_NODE");
        assertThat(noLocal.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(noLocal.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(noLocal.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo(NODES_FILES_NOT_READ + ", so the replication of system_auth, " +
                                                                          "{'class': 'NetworkTopologyStrategy', 'dc1': '3'}, was checked against the nodes " +
                                                                          "whose data center is known only");
        assertThat(noLocal.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);
    }

    @Test
    public void aSingleNodeWithoutNodesPeersIsASingleNode() throws IOException
    {
        // a single DSE 6.9.26 node that never had a peer wrote no nodes/peers: nodes/local alone is a single node, said by
        // an info, as a lost file would count this node alone wrongly
        DseNodeFixture node = node();
        Path nodes = node.metadata.resolve("nodes");
        Files.delete(nodes.resolve("peers"));
        writeAuthReplication(node, simpleStrategy());
        writeDseDefaultReplications(node);
        Run run = assertReady(run(node));
        assertThat(run.report.peers).isEmpty();
        assertThat(run.out).contains("Peers in nodes/peers: none (no nodes/peers file, see NO_PEERS_FILE)");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(run.findings("NO_PEERS_FILE")).hasSize(1);
        assertThat(run.findings("NO_PEERS_FILE").get(0).severity).isEqualTo(Finding.Severity.INFO);
        assertThat(run.messages("NO_PEERS_FILE")).isEqualTo("no nodes/peers in " + nodes + ": taken for a node that never had a peer (a single DSE " +
                                                            "6.9.26 node writes none), so the replication checks count this node alone and no " +
                                                            "other node's release, workloads or upgrade state is checked; if nodetool status " +
                                                            "lists other nodes, the file is lost: apply no remedy of SYSTEM_AUTH_REPLICATION " +
                                                            "or KEYSPACE_REPLICATION and report to the upgrade owner");
        assertThat(run.codes(Finding.Severity.WARNING)).doesNotContain("NO_PEERS_FILE", "SYSTEM_AUTH_REPLICATION", "KEYSPACE_REPLICATION");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'SimpleStrategy', 'replication_factor': '1'}, " +
                                                                 "enough in every data center of nodes/local and nodes/peers (no nodes/peers: this " +
                                                                 "node counted alone, see NO_PEERS_FILE)",
                                                                 "Replication of the other system keyspaces: with one data center they stay as they " +
                                                                 "are (no nodes/peers: this node counted alone, see NO_PEERS_FILE)");

        // the node's data center counts one node: 3 replicas there are more than it can make a LOCAL_QUORUM of
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Run three = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(three.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '3'}: " +
                                                                        "dc1 has 3 replicas and only 1 node, so LOCAL_QUORUM needs 2 replicas up there, " +
                                                                        "more than its nodes" + AUTH_REPLICATION_BLOCKING);
        // its remedy, which would lower the replicas, applies only if the node has no peer indeed
        assertThat(three.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .endsWith("; nodes/peers is missing, so this node was counted alone: apply this only if nodetool status lists no other node, otherwise " +
                      "report to the upgrade owner (see NO_PEERS_FILE)");
        // the same caveat on a keyspace of which no node holds a replica, whose remedy would drop the data center listed
        writeAuthReplication(node, simpleStrategy());
        writeKeyspaceReplication(node, "system_distributed", networkTopologyStrategy("dc2", "3"));
        Run lost = assertReady(run(node));
        assertThat(keyspaceReplication(lost, "system_distributed").remedy)
            .isEqualTo(keyspaceReplicationRemedy("system_distributed", "'dc1': 1", "; dc2, listed but with no node in nodes/local and nodes/peers, " +
                                                                                   "left out") +
                       "; nodes/peers is missing, so this node was counted alone: apply this only if nodetool status lists no other node, " +
                       "otherwise report to the upgrade owner (see NO_PEERS_FILE)");
        assertThat(lost.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: see KEYSPACE_REPLICATION " +
                                                                  "(system_distributed; no nodes/peers: this node counted alone, see NO_PEERS_FILE)");
        // hints for other nodes on a node with no nodes/peers suggest that the file was lost
        Files.write(node.hints.resolve(UUID.randomUUID() + "-1790335633208-v1.hints"), new byte[0]);
        assertThat(assertReady(run(node)).messages("PENDING_HINTS")).endsWith("; with nodes/peers missing, hints for other nodes suggest that the " +
                                                                              "file was lost: see NO_PEERS_FILE");
        writeKeyspaceReplication(node, "system_distributed", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "3"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));

        // DSE's own empty nodes/peers map: no peer, and no such caveat
        Files.copy(DseFixtures.nodesPeersEmpty(), nodes.resolve("peers"));
        Run empty = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(empty.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(empty.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).doesNotContain("nodes/peers is missing");
        assertThat(empty.messages("PENDING_HINTS")).contains("neither in nodes/local nor in nodes/peers").doesNotContain("with nodes/peers missing");

        // nodes/local missing too: no node is counted, so nothing is said of this one
        Files.delete(nodes.resolve("peers"));
        Files.delete(nodes.resolve("local"));
        writeAuthReplication(node, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "0"));
        Run neither = assertBlocked(run(node), "NOT_A_DSE_NODE");
        assertThat(neither.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(neither.codes(Finding.Severity.BLOCKING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(neither.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).doesNotContain("nodes/peers is missing");
    }

    @Test
    public void aMissingNodesPeersWithOtherNodesInNodetoolStatusBlocks() throws IOException
    {
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        DseNodeFixture node = node();
        Path nodes = node.metadata.resolve("nodes");
        Files.delete(nodes.resolve("peers"));
        writeAuthReplication(node, simpleStrategy());
        writeDseDefaultReplications(node);
        Path arguments = node.root.resolve("nodetool-arguments");
        Path nodetool = fakeNodetool(node.root, arguments, 0,
                                     "case \"$*\" in\n" +
                                     "  *status*) printf 'Datacenter: dc1\\n--  Address  Load\\nUN  192.168.57.11  1 MiB\\nUN  192.168.57.12  1 MiB\\n" +
                                     "UN  192.168.57.13  1 MiB\\n' ;;\n" +
                                     "esac\n");
        Run lost = assertBlocked(run(node, "--nodetool", nodetool.toString()), "UNREADABLE_INPUT");
        assertThat(lost.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(lost.out).contains("Peers in nodes/peers: not used (see UNREADABLE_INPUT)");
        assertThat(lost.messages("UNREADABLE_INPUT")).isEqualTo("cannot read nodes/peers: it is missing, but nodetool status lists 3 nodes, so the " +
                                                                "file is lost and the peers of this node are not known; the releases and workloads " +
                                                                "of the peers cannot be checked");
        assertThat(lost.findings("UNREADABLE_INPUT").get(0).remedy)
            .isEqualTo("restart DSE on this node and wait until nodetool status shows its peers (DSE then writes nodes/peers anew), then run the " +
                       "tool again; if nodes/peers is still missing, report to the upgrade owner");
        // its peers not known, the nodes counted are a lower bound, and the remedies cannot tell the mixed phase
        assertThat(lost.findings("SYSTEM_AUTH_REPLICATION")).hasSize(1);
        assertThat(lost.findings("SYSTEM_AUTH_REPLICATION").get(0).severity).isEqualTo(Finding.Severity.WARNING);
        assertThat(lost.messages("SYSTEM_AUTH_REPLICATION")).startsWith(NODES_FILES_NOT_READ);
        assertThat(lost.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).endsWith(MIXED_PHASE_UNKNOWN).doesNotContain("nodes/peers is missing");
        // the other keyspaces, on DSE's defaults, are not checked with one data center known and the peers not known
        assertThat(lost.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(lost.report.upgradePath.upgradeSteps).contains(OTHER_KEYSPACES_NOT_CHECKED_STEP, "Hints: no DSE hints file pending");
        // hints for another node are not checked against nodes files not both trusted
        Path hint = Files.write(node.hints.resolve(UUID.randomUUID() + "-1790335633208-v1.hints"), new byte[0]);
        Run lostHints = assertBlocked(run(node, "--nodetool", nodetool.toString()), "UNREADABLE_INPUT");
        assertThat(lostHints.findings("PENDING_HINTS")).hasSize(1);
        assertThat(lostHints.findings("PENDING_HINTS").get(0).severity).isEqualTo(Finding.Severity.INFO);
        assertThat(lostHints.messages("PENDING_HINTS")).startsWith(HINTS_NOT_CHECKED);
        Files.delete(hint);

        // nodes/local recording no data center: the file is lost all the same
        node.editLocal(local -> local.remove("data_center"));
        Run noDataCenter = assertBlocked(run(node, "--nodetool", nodetool.toString()), "UNREADABLE_INPUT");
        assertThat(noDataCenter.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(noDataCenter.messages("UNREADABLE_INPUT")).isEqualTo(lost.messages("UNREADABLE_INPUT"));
        assertThat(noDataCenter.findings("UNREADABLE_INPUT").get(0).remedy).isEqualTo(lost.findings("UNREADABLE_INPUT").get(0).remedy);
        node.editLocal(local -> local.put("data_center", "dc1"));

        // nodes/local missing too: the tool may not look at DSE's metadata directory at all, so only NOT_A_DSE_NODE
        Path local = nodes.resolve("local");
        Path localCopy = node.root.resolve("local-copy");
        Files.move(local, localCopy);
        Run noLocal = assertBlocked(run(node, "--nodetool", nodetool.toString()), "NOT_A_DSE_NODE");
        assertThat(noLocal.findings("UNREADABLE_INPUT")).isEmpty();
        assertThat(noLocal.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(noLocal.report.live.get("status").exitCode).isZero();
        // nodes/local that cannot be read: only its own UNREADABLE_INPUT
        Files.write(local, "not a nodes file".getBytes(StandardCharsets.UTF_8));
        Run damagedLocal = assertBlocked(run(node, "--nodetool", nodetool.toString()), "UNREADABLE_INPUT");
        assertThat(damagedLocal.findings("UNREADABLE_INPUT")).hasSize(1);
        assertThat(damagedLocal.findings("UNREADABLE_INPUT").get(0).message).startsWith("cannot read nodes/local: ");
        assertThat(damagedLocal.findings("UNREADABLE_INPUT").get(0).remedy)
            .isEqualTo("run the tool as the user DSE runs as (or as root); a damaged nodes/local must be repaired on DSE first");
        assertThat(damagedLocal.report.live.get("status").exitCode).isZero();
        assertThat(damagedLocal.findings("NO_PEERS_FILE")).isEmpty();
        Files.move(localCopy, local, StandardCopyOption.REPLACE_EXISTING);

        // DSE's empty map while nodetool status lists 3 nodes: the file is stale
        Files.copy(DseFixtures.nodesPeersEmpty(), nodes.resolve("peers"));
        Run stale = assertBlocked(run(node, "--nodetool", nodetool.toString()), "UNREADABLE_INPUT");
        assertThat(stale.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(stale.messages("UNREADABLE_INPUT")).isEqualTo("cannot read nodes/peers: it records no peer, but nodetool status lists 3 nodes, so the " +
                                                                 "file is stale and the peers of this node are not known; the releases and workloads " +
                                                                 "of the peers cannot be checked");
        assertThat(stale.findings("UNREADABLE_INPUT").get(0).remedy)
            .isEqualTo("restart DSE on this node and wait until nodetool status shows its peers (DSE then writes nodes/peers anew), then run the " +
                       "tool again; if nodes/peers still records no peer, report to the upgrade owner");
        Files.delete(nodes.resolve("peers"));

        // nodetool status listing this node alone: a single node
        fakeNodetool(node.root, arguments, 0,
                     "case \"$*\" in\n" +
                     "  *status*) printf 'Datacenter: dc1\\n--  Address  Load\\nUN  192.168.57.11  1 MiB\\n' ;;\n" +
                     "esac\n");
        assertSingleNodeWithoutPeers(run(node, "--nodetool", nodetool.toString()));
        // nodetool status listing no node it can parse: the same
        fakeNodetool(node.root, arguments, 0,
                     "case \"$*\" in\n" +
                     "  *status*) echo 'nodetool: Failed to connect' ;;\n" +
                     "esac\n");
        assertSingleNodeWithoutPeers(run(node, "--nodetool", nodetool.toString()));
        // nodetool status failing: nothing to compare, the same
        fakeNodetool(node.root, arguments, 1, "");
        Run failing = run(node, "--nodetool", nodetool.toString());
        assertThat(failing.codes(Finding.Severity.WARNING)).contains("LIVE_UNAVAILABLE");
        assertSingleNodeWithoutPeers(failing);
    }

    @Test
    public void peersRecordedAreNotComparedWithTheNodesOfNodetoolStatus() throws IOException
    {
        // with peers recorded, nodes joining or leaving make nodetool status list another count for a while: not compared
        Assume.assumeTrue("needs /bin/sh", Files.isExecutable(Path.of("/bin/sh")));
        DseNodeFixture node = node();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Path nodetool = fakeNodetool(node.root, node.root.resolve("nodetool-arguments"), 0,
                                     "case \"$*\" in\n" +
                                     "  *status*) printf 'Datacenter: dc1\\n--  Address  Load\\nUN  192.168.57.11  1 MiB\\nUN  192.168.57.12  1 MiB\\n" +
                                     "UN  192.168.57.13  1 MiB\\nUN  192.168.57.14  1 MiB\\nUN  192.168.57.15  1 MiB\\n' ;;\n" +
                                     "esac\n");
        Run run = run(node, "--nodetool", nodetool.toString());
        // nodetool status was read: 5 nodes against the 2 peers recorded and this node
        assertThat(Files.readAllLines(node.root.resolve("nodetool-arguments"))).contains("status");
        assertThat(run.report.live.get("status").exitCode).isZero();
        assertThat(run.report.peers).hasSize(2);
        assertThat(run.out).contains("Peers in nodes/peers: 2" + System.lineSeparator());
        assertThat(run.findings("UNREADABLE_INPUT")).isEmpty();
        assertThat(run.findings("NO_PEERS_FILE")).isEmpty();
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
    }

    /** What a node with no nodes/peers, on DSE's defaults, gives when it is taken for a single node. */
    private static void assertSingleNodeWithoutPeers(Run run)
    {
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("UNREADABLE_INPUT");
        assertThat(run.findings("NO_PEERS_FILE")).hasSize(1);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
    }

    @Test
    public void nodesPeersBeingRewrittenByDseBlocks() throws IOException
    {
        // DSE rewriting nodes/peers, or interrupted while it did: peers.old holds the peers, which the tool does not take for none
        DseNodeFixture node = node();
        Path nodes = node.metadata.resolve("nodes");
        Files.move(nodes.resolve("peers"), nodes.resolve("peers.old"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Run run = assertBlocked(run(node), "UNREADABLE_INPUT");
        assertThat(run.messages("UNREADABLE_INPUT")).isEqualTo(PEERS_OLD_LEFT);
        assertThat(run.findings("UNREADABLE_INPUT").get(0).remedy).isEqualTo(PEERS_OLD_LEFT_REMEDY);
        // not a single node: the nodes counted are a lower bound, so 3 replicas in dc1 are no problem
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION", "NOT_A_DSE_NODE");
        // the peers of peers.old are not used either, and the other keyspaces are not checked, one data center known
        assertThat(run.report.peers).isEmpty();
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(run.report.upgradePath.upgradeSteps).contains(OTHER_KEYSPACES_NOT_CHECKED_STEP);
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo(NODES_FILES_NOT_READ + ", so the replication of system_auth, {'class': " +
                                                                      "'NetworkTopologyStrategy', 'dc1': '3'}, was checked against the nodes whose " +
                                                                      "data center is known only");
        // the check by hand cannot tell the mixed phase either
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES +
                                                                                    MIXED_PHASE_UNKNOWN);
        // a replication that gives no node a replica is said, its remedy unable to tell the mixed phase either
        writeKeyspaceReplication(node, "dse_security", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "0"));
        Finding security = keyspaceReplication(assertBlocked(run(node), "UNREADABLE_INPUT"), "dse_security");
        assertThat(security.message).isEqualTo("the replication of dse_security is {'class': 'SimpleStrategy', 'replication_factor': '0'}: its " +
                                               "replication_factor is 0, so no node holds a replica of dse_security; " + NODES_FILES_NOT_READ +
                                               ", so it was checked against the nodes whose data center is known only; HCD 2.0 does not read " +
                                               "or write dse_security, which only DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING +
                                               KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(security.remedy).isEqualTo(keyspaceReplicationRemedy("dse_security", "'dc1': 3", "; " + NODES_FILES_NOT_READ + ": check by " +
                                                                        "hand the data center of each node and its replicas" + LOWER_TO_THE_NODES)
                                                  .replace("kept up to the nodes of the data center", "kept as listed") + MIXED_PHASE_UNKNOWN);
        node.deletePartitions("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), "dse_security");
        // a replication that blocks: its remedy says that it applies only outside the mixed phase, which the tool cannot tell
        writeAuthReplication(node, networkTopologyStrategy("dc1", "0"));
        Run zero = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(zero.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).endsWith(AUTH_REPLICATION_OTHER_KEYSPACES + MIXED_PHASE_UNKNOWN);
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));

        // peers.txn alone: DSE writing the file for the first time, or interrupted while it did, which nothing recovers
        Files.move(nodes.resolve("peers.old"), nodes.resolve("peers.txn"));
        Run txn = assertBlocked(run(node), "UNREADABLE_INPUT");
        assertThat(txn.messages("UNREADABLE_INPUT")).isEqualTo("cannot read nodes/peers: it is missing and a nodes/peers.txn is left (DSE is " +
                                                               "writing the file for the first time, or was interrupted while it did), so the " +
                                                               "peers of this node are not known; the releases and workloads of the peers cannot " +
                                                               "be checked");
        assertThat(txn.findings("UNREADABLE_INPUT").get(0).remedy).isEqualTo("run the tool again; if it persists with DSE stopped, start DSE once " +
                                                                             "and wait until nodetool status shows its peers (DSE then writes " +
                                                                             "nodes/peers anew), then run the tool again; if this node has no peer, " +
                                                                             "delete nodes/peers.txn");
        assertThat(txn.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(txn.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo(run.messages("SYSTEM_AUTH_REPLICATION"));

        // nodes/peers there with a peers.txn left beside it: DSE puts nothing back, so the peers are read, and counted exactly
        Files.copy(nodes.resolve("peers.txn"), nodes.resolve("peers"));
        Run txnLeft = assertReady(run(node));
        assertThat(txnLeft.findings("UNREADABLE_INPUT")).isEmpty();
        assertThat(txnLeft.report.peers).hasSize(2);
        assertThat(txnLeft.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(txnLeft.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '3'}, " +
                                                                     "enough in every data center of nodes/local and nodes/peers");

        // nodes/peers there with a peers.old left beside it: DSE, and the import of HCD 2.0, put peers.old back at the next start
        Files.copy(nodes.resolve("peers"), nodes.resolve("peers.old"));
        Run oldLeft = assertBlocked(run(node), "UNREADABLE_INPUT");
        assertThat(oldLeft.messages("UNREADABLE_INPUT")).isEqualTo(PEERS_OLD_LEFT);
        // the peers of nodes/peers, which DSE replaces at its next start, are not used: no peer, no check of their releases
        assertThat(oldLeft.report.peers).isEmpty();

        // peers recorded as upgraded, or of another release, in a nodes/peers that peers.old will replace: no mixed phase, no
        // mixed releases; and a DSE hints file for a host id of no known node is only said, the peers not being known
        String hostId = UUID.randomUUID().toString();
        Files.write(node.hints.resolve(hostId + "-1790335633208-v1.hints"), new byte[0]);
        node.editPeers(peers -> {
            peers.get(0).put("release_version", "5.0.7.0-SNAPSHOT");
            peers.get(1).put("release_version", "4.0.0.6926");
        });
        Run stale = assertBlocked(run(node), "UNREADABLE_INPUT");
        assertThat(stale.report.peers).isEmpty();
        assertThat(stale.findings("UPGRADED_PEERS")).isEmpty();
        assertThat(stale.findings("MIXED_RELEASES")).isEmpty();
        assertThat(stale.findings("PENDING_HINTS")).hasSize(1);
        assertThat(stale.findings("PENDING_HINTS").get(0).severity).isEqualTo(Finding.Severity.INFO);
        assertThat(stale.messages("PENDING_HINTS")).isEqualTo(HINTS_NOT_CHECKED + hostId + ": 1 file(s)" + HINTS_NOT_CHECKED_END);
        assertThat(stale.report.upgradePath.upgradeSteps).contains("Hints: 1 host id(s) with DSE hints files, not checked against nodes/local and nodes/peers " +
                                                                   "(not both read and trusted); see PENDING_HINTS");
        // the same nodes/peers, trusted once peers.old is gone, gives both
        Files.delete(nodes.resolve("peers.old"));
        Run trusted = run(node);
        assertThat(trusted.report.peers).hasSize(2);
        assertThat(trusted.findings("UPGRADED_PEERS")).isNotEmpty();
        assertThat(trusted.findings("MIXED_RELEASES")).isNotEmpty();
        assertThat(trusted.findings("PENDING_HINTS")).hasSize(1);
        assertThat(trusted.findings("PENDING_HINTS").get(0).severity).isEqualTo(Finding.Severity.WARNING);
        assertThat(trusted.messages("PENDING_HINTS")).startsWith("DSE hints files for host ids neither in nodes/local nor in nodes/peers: " +
                                                                 hostId + ": 1 file(s)");
        assertThat(trusted.report.upgradePath.upgradeSteps).contains("Hints: 1 host id(s) of no known node with DSE hints files; see PENDING_HINTS");
    }

    @Test
    public void peersFileProblemAndPeersReadFailure() throws IOException
    {
        // as looked at on disk: a missing file, and a file looked at twice, unchanged
        Path folder = temporaryFolder.newFolder().toPath();
        NodeInspector.PeersFileState absent = NodeInspector.PeersFileState.of(folder.resolve("peers"));
        assertThat(absent.exists).isFalse();
        assertThat(absent.error).isNull();
        Path file = Files.write(folder.resolve("peers"), new byte[]{ 1, 2, 3 });
        NodeInspector.PeersFileState present = NodeInspector.PeersFileState.of(file);
        assertThat(present.exists).isTrue();
        assertThat(present.error).isNull();
        assertThat(present.size).isEqualTo(3);
        assertThat(present.sameAs(NodeInspector.PeersFileState.of(file))).isTrue();

        NodeInspector.PeersFileState there = new NodeInspector.PeersFileState(true, 7L, 100, 1000, null);
        NodeInspector.PeersFileState missing = new NodeInspector.PeersFileState(false, null, 0, 0, null);
        String changed = "cannot read nodes/peers: it changed while it was read (DSE wrote or rewrote it), so the peers of this node are not known; the " +
                         "releases and workloads of the peers cannot be checked";
        // appeared, disappeared, or rewritten in place: another file (moved away and back between the two looks), another
        // size, another modification time
        List<NodeInspector.PeersFileState> others = List.of(new NodeInspector.PeersFileState(true, 8L, 100, 1000, null),
                                                            new NodeInspector.PeersFileState(true, 7L, 120, 1000, null),
                                                            new NodeInspector.PeersFileState(true, 7L, 100, 2000, null));
        List<NodeInspector.PeersFileState[]> pairs = new ArrayList<>();
        pairs.add(new NodeInspector.PeersFileState[]{ missing, there });
        pairs.add(new NodeInspector.PeersFileState[]{ there, missing });
        others.forEach(other -> pairs.add(new NodeInspector.PeersFileState[]{ there, other }));
        for (NodeInspector.PeersFileState[] states : pairs)
        {
            Finding finding = NodeInspector.peersFileProblem(states[0], states[1], false, false);
            assertThat(finding).isNotNull();
            assertThat(finding.code).isEqualTo("UNREADABLE_INPUT");
            assertThat(finding.severity).isEqualTo(Finding.Severity.BLOCKING);
            assertThat(finding.message).isEqualTo(changed);
            assertThat(finding.remedy).isEqualTo("run the tool again");
        }
        // attributes that cannot be read are no missing file
        Finding error = NodeInspector.peersFileProblem(there, new NodeInspector.PeersFileState(true, null, 0, 0, "java.io.IOException: denied"),
                                                       false, false);
        assertThat(error.code).isEqualTo("UNREADABLE_INPUT");
        assertThat(error.severity).isEqualTo(Finding.Severity.BLOCKING);
        assertThat(error.message).isEqualTo("cannot read nodes/peers: its attributes cannot be read (java.io.IOException: denied), so the peers of " +
                                            "this node are not known; the releases and workloads of the peers cannot be checked");
        assertThat(error.remedy).isEqualTo("run the tool as the user DSE runs as (or as root)");
        // the error of the first look wins
        Finding first = NodeInspector.peersFileProblem(new NodeInspector.PeersFileState(true, null, 0, 0, "java.io.IOException: first"),
                                                       new NodeInspector.PeersFileState(true, null, 0, 0, "java.io.IOException: second"), false, false);
        assertThat(first.message).contains("(java.io.IOException: first)");
        // unchanged: read (or missing, no peer), and trusted
        assertThat(NodeInspector.peersFileProblem(there, new NodeInspector.PeersFileState(true, 7L, 100, 1000, null), false, false)).isNull();
        assertThat(NodeInspector.peersFileProblem(missing, missing, false, false)).isNull();
        assertThat(NodeInspector.peersFileProblem(there, there, false, true)).isNull();
        assertThat(NodeInspector.peersFileProblem(there, there, true, false).message).isEqualTo(PEERS_OLD_LEFT);

        // a read that failed: the file gone since (moved away by DSE), a peers.old left beside it, or a damaged file
        assertThat(NodeInspector.peersReadFailure("boom", there, missing, true).message).isEqualTo(changed);
        assertThat(NodeInspector.peersReadFailure("boom", there, there, true).message).isEqualTo(PEERS_OLD_LEFT);
        NodeInspector.PeersFileState denied = new NodeInspector.PeersFileState(true, null, 0, 0, "java.io.IOException: denied");
        assertThat(NodeInspector.peersReadFailure("boom", denied, denied, true).message).isEqualTo(PEERS_OLD_LEFT);
        // a file replaced while it was read changed, damaged or not
        assertThat(NodeInspector.peersReadFailure("boom", there, new NodeInspector.PeersFileState(true, 8L, 100, 1000, null), false).message)
            .isEqualTo(changed);
        Finding damaged = NodeInspector.peersReadFailure("boom", there, there, false);
        assertThat(damaged.severity).isEqualTo(Finding.Severity.BLOCKING);
        assertThat(damaged.message).isEqualTo("cannot read nodes/peers: boom; the releases and workloads of the peers cannot be checked");
        assertThat(damaged.remedy).isEqualTo("run the tool as the user DSE runs as (or as root); a damaged nodes/peers must be repaired on DSE first");
    }

    @Test
    public void systemAuthReplicationNotFullyCheckedInTheMixedPhaseAsksForTheUpgradeOwner() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        node.editPeers(peers -> peers.get(0).remove("data_center"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                      "replication of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '3'}, " +
                                                                      "was checked against the nodes whose data center is known only");
        // no recommendation, no change: the replication cannot be changed in the mixed phase
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("check by hand " + AUTH_REPLICATION_CHECK + "; if it does not, upgrade no further node and report to the upgrade owner, since the " +
                       "replication cannot be changed in the mixed phase");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '3'}, checked " +
                                                                 "against the nodes whose data center is known only (the data center of 1 node is " +
                                                                 "not known from nodes/local and nodes/peers)");
    }

    @Test
    public void systemAuthReplicationShortInTheMixedPhaseOnlyStatesTheRisk() throws IOException
    {
        // DSE's default in the one data center of the mixed cluster: a warning, which asks for no change in the mixed
        // phase
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        writeAuthReplication(node, simpleStrategy());
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        // no "more replicas" either: the replication stays as it is
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '1'}: " +
                                                                      "dc1 has 1 replica and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_READ);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("the upgrade has started (UPGRADED_PEERS), so the replication stays as it is (no schema change and no repair in the mixed " +
                       "phase): keep every node of dc1 up while an upgraded node there starts")
            .doesNotContain("ALTER KEYSPACE").doesNotContain("nodetool repair");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: see SYSTEM_AUTH_REPLICATION");

        // short in both data centers of the two-data-center cluster
        DseNodeFixture twoDataCenters = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCentersMixed(1));
        Files.write(twoDataCenters.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        writeAuthReplication(twoDataCenters, networkTopologyStrategy("dc1", "1", "dc2", "2"));
        Run both = run(twoDataCenters);
        assertThat(both.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(both.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(both.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '1', " +
                                                                       "'dc2': '2'}: dc1 has 1 replica and 3 nodes: an upgraded node there cannot start " +
                                                                       "while another node of dc1 is down; dc2 has 2 replicas and 3 nodes: an upgraded " +
                                                                       "node there cannot start while another node of dc2 is down" + AUTH_REPLICATION_READ);
        assertThat(both.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("the upgrade has started (UPGRADED_PEERS), so the replication stays as it is (no schema change and no repair in the mixed " +
                       "phase): keep every node of dc1 and dc2 up while an upgraded node there starts");

        // too many replicas: the warning does not say how many would do either
        DseNodeFixture above = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        writeAuthReplication(above, networkTopologyStrategy("dc1", "5"));
        Run five = run(above);
        assertThat(five.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(five.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(five.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '5'}: " +
                                                                       "dc1 has 5 replicas and only 3 nodes, so LOCAL_QUORUM needs 3 replicas up there: an " +
                                                                       "upgraded node there cannot start while another node of dc1 is down" +
                                                                       AUTH_REPLICATION_READ);
        assertThat(five.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("the upgrade has started (UPGRADED_PEERS), so the replication stays as it is (no schema change and no repair in the mixed " +
                       "phase): keep every node of dc1 up while an upgraded node there starts");

        // EverywhereStrategy with a node of no data center: its check by hand, and no change in the mixed phase
        DseNodeFixture everywhere = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        addPeer(everywhere, 199, false);
        writeAuthReplication(everywhere, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run ring = run(everywhere);
        assertThat(ring.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(ring.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(ring.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                       "replication of system_auth, {'class': 'EverywhereStrategy'}, was checked for " +
                                                                       "its class only, not for the replicas of each data center");
        assertThat(ring.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("check by hand " + AUTH_REPLICATION_CHECK + " (EverywhereStrategy: LOCAL_QUORUM counts the replicas of the whole ring, which " +
                       "only a data center of more than half of the nodes meets); if it does not, upgrade no further node and report to the " +
                       "upgrade owner, since the replication cannot be changed in the mixed phase");

        // with a node of no data center, the check by hand keeps the risk of the shortfall among the nodes counted
        DseNodeFixture uncounted = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        addPeer(uncounted, 199, false);
        writeAuthReplication(uncounted, networkTopologyStrategy("dc1", "1"));
        Run byHand = run(uncounted);
        assertThat(byHand.codes(Finding.Severity.BLOCKING)).doesNotContain("SYSTEM_AUTH_REPLICATION");
        assertThat(byHand.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(byHand.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the data center of 1 node is not known from nodes/local and nodes/peers, so the " +
                                                                         "replication of system_auth, {'class': 'NetworkTopologyStrategy', 'dc1': '1'}, " +
                                                                         "was checked against the nodes whose data center is known only; dc1 has 1 " +
                                                                         "replica and at least 3 nodes: an upgraded node there cannot start while " +
                                                                         "another node of dc1 is down" + AUTH_REPLICATION_READ);
        assertThat(byHand.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy)
            .isEqualTo("check by hand " + AUTH_REPLICATION_CHECK + "; if it does not, upgrade no further node and report to the upgrade owner, since the " +
                       "replication cannot be changed in the mixed phase; keep every node of dc1 up while an upgraded node there starts");
    }

    @Test
    public void systemAuthReplicationInTheMixedPhaseBlocksAndAsksForTheUpgradeOwner() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        writeAuthReplication(node, networkTopologyStrategy("dc1", "0"));
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                      "'dc1': '0'}: dc1 has 0 replicas and 3 nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY).doesNotContain("ALTER KEYSPACE");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: see SYSTEM_AUTH_REPLICATION");

        // more replicas than the 3 nodes can make a LOCAL_QUORUM of
        writeAuthReplication(node, networkTopologyStrategy("dc1", "6"));
        Run above = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(above.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                        "'dc1': '6'}: dc1 has 6 replicas and only 3 nodes, so LOCAL_QUORUM needs 4 " +
                                                                        "replicas up there, more than its nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(above.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY);

        // with a node of no data center: what blocks whatever its data center still blocks
        DseNodeFixture uncounted = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        addPeer(uncounted, 199, false);
        writeAuthReplication(uncounted, networkTopologyStrategy("dc1", "0"));
        Run lowerBound = assertBlocked(run(uncounted), "SYSTEM_AUTH_REPLICATION");
        assertThat(lowerBound.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': " +
                                                                             "'NetworkTopologyStrategy', 'dc1': '0'}: dc1 has 0 replicas and 3 " +
                                                                             "nodes" + AUTH_REPLICATION_BLOCKING);
        assertThat(lowerBound.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY);

        // the mixed phase of the two-data-center cluster, the data center of the upgraded nodes not listed
        DseNodeFixture twoDataCenters = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCentersMixed(1));
        Files.write(twoDataCenters.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        writeAuthReplication(twoDataCenters, networkTopologyStrategy("dc1", "3"));
        Run mixed = assertBlocked(run(twoDataCenters), "SYSTEM_AUTH_REPLICATION");
        assertThat(mixed.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', " +
                                                                        "'dc1': '3'}: dc2 (3 nodes) is not listed" + AUTH_REPLICATION_BLOCKING);
        assertThat(mixed.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY);
        assertThat(mixed.report.upgradePath.upgradeSteps).contains("Replication of system_auth: see SYSTEM_AUTH_REPLICATION");

        // and with a shortfall in the data center listed, named after what blocks
        writeAuthReplication(twoDataCenters, networkTopologyStrategy("dc1", "1"));
        Run both = assertBlocked(run(twoDataCenters), "SYSTEM_AUTH_REPLICATION");
        assertThat(both.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'NetworkTopologyStrategy', 'dc1': '1'}: " +
                                                                       "dc2 (3 nodes) is not listed" + AUTH_REPLICATION_BLOCKING + "; besides, dc1 has " +
                                                                       "1 replica and 3 nodes: an upgraded node there cannot start while another node " +
                                                                       "of dc1 is down");
        assertThat(both.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY);

        // SimpleStrategy over its two data centers
        writeAuthReplication(twoDataCenters, Map.of("class", SIMPLE_STRATEGY, "replication_factor", "3"));
        Run simple = assertBlocked(run(twoDataCenters), "SYSTEM_AUTH_REPLICATION");
        assertThat(simple.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', " +
                                                                         "'replication_factor': '3'}: SimpleStrategy does not place replicas in " +
                                                                         "each data center, and nodes/local and nodes/peers name 2 (dc1, dc2), so " +
                                                                         "LOCAL_QUORUM cannot be met in each" + AUTH_REPLICATION_BLOCKING);
        assertThat(simple.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_MIXED_PHASE_REMEDY);
    }

    @Test
    public void otherSystemKeyspacesOnASingleNodeOnDseDefaultsGiveNothing() throws IOException
    {
        // a single-node installation on DSE's defaults: system_auth with one replica, system_distributed with 3, system_traces with 2,
        // DSE's empty nodes/peers map
        DseNodeFixture node = node();
        Files.copy(DseFixtures.nodesPeersEmpty(), node.metadata.resolve("nodes").resolve("peers"), StandardCopyOption.REPLACE_EXISTING);
        writeAuthReplication(node, simpleStrategy());
        writeDseDefaultReplications(node);
        Run run = assertReady(run(node));
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: with one data center they stay as they are");
    }

    @Test
    public void otherSystemKeyspacesInOneDataCenterOfThreeNodesStayAsTheyAre() throws IOException
    {
        // DSE's defaults on the 3 nodes of dc1: system_auth warns, as graded on its own; the others are not checked with one data center
        DseNodeFixture node = node();
        writeAuthReplication(node, simpleStrategy());
        writeDseDefaultReplications(node);
        Run run = assertReady(run(node));
        assertThat(run.codes(Finding.Severity.WARNING)).contains("SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the replication of system_auth is {'class': 'SimpleStrategy', 'replication_factor': '1'}: " +
                                                                      "dc1 has 1 replica and 3 nodes: an upgraded node there cannot start while another " +
                                                                      "node of dc1 is down" + AUTH_REPLICATION_SHORT);
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: with one data center they stay as they are");

        // nor with a replication that would be short with several data centers
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "1"));
        assertThat(assertReady(run(node)).findings("KEYSPACE_REPLICATION")).isEmpty();

        // but a replication that gives no node a replica is said, with one data center too
        writeKeyspaceReplication(node, "system_distributed", networkTopologyStrategy("old", "3"));
        Run old = assertReady(run(node));
        assertThat(old.findings("KEYSPACE_REPLICATION")).hasSize(1);
        assertThat(keyspaceReplication(old, "system_distributed").message)
            .isEqualTo("the replication of system_distributed is {'class': 'NetworkTopologyStrategy', 'old': '3'}: dc1 (3 nodes) is not listed, so " +
                       "no node holds a replica of system_distributed; " + DISTRIBUTED_USE + KEYSPACE_REPLICATION_NOT_BLOCKING +
                       KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(old, "system_distributed").remedy)
            .isEqualTo(keyspaceReplicationRemedy("system_distributed", "'dc1': 3", "; old, listed but with no node in nodes/local and nodes/peers, " +
                                                                                   "left out"));
        assertThat(old.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: see KEYSPACE_REPLICATION " +
                                                                 "(system_distributed)");
        writeKeyspaceReplication(node, "system_traces", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "0"));
        assertThat(keyspaceReplication(assertReady(run(node)), "system_traces").message)
            .isEqualTo("the replication of system_traces is {'class': 'SimpleStrategy', 'replication_factor': '0'}: its replication_factor is 0, " +
                       "so no node holds a replica of system_traces; " + TRACES_USE + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
    }

    @Test
    public void systemDistributedOnSimpleStrategyWithTwoDataCentersWarns() throws IOException
    {
        DseNodeFixture node = twoDataCenterNode();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        writeDseDefaultReplications(node);
        Run run = assertReady(run(node));
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.codes(Finding.Severity.WARNING)).contains("KEYSPACE_REPLICATION");
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("KEYSPACE_REPLICATION");
        // one warning per keyspace on SimpleStrategy; those on EverywhereStrategy stay as they are
        assertThat(run.findings("KEYSPACE_REPLICATION")).extracting(f -> f.message.substring(0, f.message.indexOf(" is ")))
                                                        .containsExactly("the replication of dse_leases", "the replication of dse_perf",
                                                                         "the replication of dse_security", "the replication of system_distributed",
                                                                         "the replication of system_traces");
        Finding distributed = keyspaceReplication(run, "system_distributed");
        assertThat(distributed.message).isEqualTo("the replication of system_distributed is {'class': 'SimpleStrategy', 'replication_factor': '3'}: " +
                                                  "SimpleStrategy does not place replicas in each data center, and nodes/local and nodes/peers name " +
                                                  "2 (dc1, dc2)" + NO_REPLICA_OF_PART + "system_distributed; " + DISTRIBUTED_USE +
                                                  KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(distributed.remedy).isEqualTo(keyspaceReplicationRemedy("system_distributed", "'dc1': 3, 'dc2': 3",
                                                                           "; the replication factor of SimpleStrategy spans the data centers, so it is " +
                                                                           "not carried over"));
        assertThat(keyspaceReplication(run, "system_traces").message)
            .isEqualTo("the replication of system_traces is {'class': 'SimpleStrategy', 'replication_factor': '2'}: SimpleStrategy does not place " +
                       "replicas in each data center, and nodes/local and nodes/peers name 2 (dc1, dc2)" + NO_REPLICA_OF_PART + "system_traces; " +
                       TRACES_USE + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        // a DSE keyspace, which HCD 2.0 does not use
        Finding leases = keyspaceReplication(run, "dse_leases");
        assertThat(leases.message).isEqualTo("the replication of dse_leases is {'class': 'SimpleStrategy', 'replication_factor': '1'}: " +
                                             "SimpleStrategy does not place replicas in each data center, and nodes/local and nodes/peers " +
                                             "name 2 (dc1, dc2)" + NO_REPLICA_OF_PART + "dse_leases; HCD 2.0 does not read or write dse_leases, " +
                                             "which only DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(leases.remedy).isEqualTo(keyspaceReplicationRemedy("dse_leases", "'dc1': 3, 'dc2': 3",
                                                                      "; the replication factor of SimpleStrategy spans the data centers, so it is not " +
                                                                      "carried over"));
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3'}, " +
                                                                 "enough in every data center of nodes/local and nodes/peers",
                                                                 "Replication of the other system keyspaces: see KEYSPACE_REPLICATION (dse_leases, dse_perf, " +
                                                                 "dse_security, system_distributed, system_traces)");

        // with 3 replicas in each data center, nothing is left to say
        for (String keyspace : List.of("system_distributed", "system_traces", "dse_security", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        Run ready = assertReady(run(node));
        assertThat(ready.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(ready.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: dse_leases, dse_perf, dse_security, " +
                                                                   "system_distributed, system_traces, enough in every data center of nodes/local " +
                                                                   "and nodes/peers");
    }

    @Test
    public void dseKeyspaceOnNetworkTopologyStrategyMissingADataCenterWarns() throws IOException
    {
        DseNodeFixture node = twoDataCenterNode();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        for (String keyspace : List.of("system_distributed", "system_traces", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3"));
        Run run = assertReady(run(node));
        assertThat(run.findings("KEYSPACE_REPLICATION")).hasSize(1);
        assertThat(run.codes(Finding.Severity.WARNING)).contains("KEYSPACE_REPLICATION");
        Finding security = keyspaceReplication(run, "dse_security");
        assertThat(security.message).isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '3'}: dc2 (3 nodes) is " +
                                               "not listed, so dc2 holds no replica of dse_security; HCD 2.0 does not read or write dse_security, " +
                                               "which only DSE uses" +
                                               KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(security.remedy).isEqualTo(keyspaceReplicationRemedy("dse_security", "'dc1': 3, 'dc2': 3", ""));
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: see KEYSPACE_REPLICATION (dse_security)");

        // at 0 in a data center with nodes
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3", "dc2", "0"));
        assertThat(keyspaceReplication(assertReady(run(node)), "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '0'}: dc2 has 0 replicas and 3 " +
                       "nodes, so dc2 holds no replica of dse_security; HCD 2.0 does not read or write dse_security, which only DSE " +
                       "uses" + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);

        // no replica in any data center of the nodes: no node holds a replica, whatever a data center of no node holds
        String dseOnly = "; HCD 2.0 does not read or write dse_security, which only DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING +
                         KEYSPACE_REPLICATION_RECOMMENDED;
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy());
        assertThat(keyspaceReplication(assertReady(run(node)), "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy'}: dc1 (3 nodes) is not listed; dc2 (3 nodes) is not " +
                       "listed, so no node holds a replica of dse_security" + dseOnly);
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "0", "dc2", "0"));
        assertThat(keyspaceReplication(assertReady(run(node)), "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '0', 'dc2': '0'}: dc1 has 0 replicas and 3 " +
                       "nodes; dc2 has 0 replicas and 3 nodes, so no node holds a replica of dse_security" + dseOnly);
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "0", "dc2", "0", "old", "3"));
        Run old = assertReady(run(node));
        assertThat(keyspaceReplication(old, "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '0', 'dc2': '0', 'old': '3'}: dc1 has 0 " +
                       "replicas and 3 nodes; dc2 has 0 replicas and 3 nodes, so no node holds a replica of dse_security" + dseOnly);
        assertThat(keyspaceReplication(old, "dse_security").remedy)
            .isEqualTo(keyspaceReplicationRemedy("dse_security", "'dc1': 3, 'dc2': 3", "; old, listed but with no node in nodes/local and nodes/peers, " +
                                                                                        "left out"));

        // with the data center of a node not known: no data center listed, so no node holds a replica; a data center of no
        // node listed with replicas, which the unknown node may be in, so only the data centers known hold none
        addPeer(node, 198, false);
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy());
        assertThat(keyspaceReplication(assertReady(run(node)), "dse_security").message)
            .startsWith("the replication of dse_security is {'class': 'NetworkTopologyStrategy'}: dc1 (3 nodes) is not listed; dc2 (3 nodes) is not " +
                        "listed, so no node holds a replica of dse_security; the data center of 1 node is not known");
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("old", "3"));
        assertThat(keyspaceReplication(assertReady(run(node)), "dse_security").message)
            .startsWith("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'old': '3'}: dc1 (3 nodes) is not listed; dc2 (3 " +
                        "nodes) is not listed, so dc1 and dc2 hold no replica of dse_security; the data center of 1 node is not known");
        node.editPeers(peers -> peers.remove(peers.size() - 1));

        // a data center without replicas and a low count in another: the remedy is not optional
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "1"));
        Run both = assertReady(run(node));
        assertThat(keyspaceReplication(both, "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '1'}: dc2 (3 nodes) is not listed, so dc2 " +
                       "holds no replica of dse_security; dc1 has 1 replica and 3 nodes, fewer than the 3 recommended" + dseOnly);
        assertThat(keyspaceReplication(both, "dse_security").remedy).isEqualTo(keyspaceReplicationRemedy("dse_security", "'dc1': 3, 'dc2': 3", ""));

        // fewer than the 3 replicas recommended in a data center of 3 nodes: 1, or 2
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "1", "dc2", "3"));
        Run fewer = assertReady(run(node));
        assertThat(keyspaceReplication(fewer, "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '1', 'dc2': '3'}: dc1 has 1 replica and 3 " +
                       "nodes, fewer than the 3 recommended; HCD 2.0 does not read or write dse_security, which only DSE uses" +
                       KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(fewer, "dse_security").remedy).isEqualTo(keyspaceReplicationOptionalRemedy("dse_security", "'dc1': 3, 'dc2': 3", ""));
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3", "dc2", "2"));
        Run two = assertReady(run(node));
        assertThat(keyspaceReplication(two, "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'}: dc2 has 2 replicas and 3 " +
                       "nodes, fewer than the 3 recommended; HCD 2.0 does not read or write dse_security, which only DSE uses" +
                       KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(two, "dse_security").remedy).isEqualTo(keyspaceReplicationOptionalRemedy("dse_security", "'dc1': 3, 'dc2': 3", ""));

        // no shortfall in a data center of 2 nodes with 1 replica, nor in one of 1 node, and nothing for more replicas than nodes
        // (6 on 3 nodes, 4 on 2 nodes, 2 on 1 node, which LOCAL_QUORUM would never meet, or 5 on 3 nodes, which it would only
        // with every node up)
        node.editPeers(peers -> peers.stream().filter(peer -> "dc2".equals(peer.get("data_center"))).findFirst().get().put("data_center", "dc3"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "2", "dc3", "1"));
        for (String keyspace : List.of("system_distributed", "system_traces", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, networkTopologyStrategy("dc1", "3", "dc2", "2", "dc3", "1"));
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3", "dc2", "1", "dc3", "1"));
        writeKeyspaceReplication(node, "dse_leases", networkTopologyStrategy("dc1", "6", "dc2", "4", "dc3", "2"));
        writeKeyspaceReplication(node, "dse_perf", networkTopologyStrategy("dc1", "5", "dc2", "2", "dc3", "1"));
        Run small = assertReady(run(node));
        assertThat(small.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(small.findings("KEYSPACE_REPLICATION")).isEmpty();

        // with the data center of a node not known, the data centers known are checked against the nodes counted
        addPeer(node, 199, false);
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3", "dc2", "2"));
        Run uncounted = assertReady(run(node));
        assertThat(keyspaceReplication(uncounted, "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'}: dc3 (1 node) is not listed" +
                       ", so dc3 holds no replica of dse_security; " +
                       "the data center of 1 node is not known from nodes/local and nodes/peers, so it was checked against the nodes whose data " +
                       "center is known only; HCD 2.0 does not read or write dse_security, which only DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING +
                       KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(uncounted, "dse_security").remedy)
            .isEqualTo(keyspaceReplicationRemedy("dse_security", "'dc1': 3, 'dc2': 3, 'dc3': 3",
                                                 "; the data center of 1 node is not known from nodes/local and nodes/peers: check by hand the data " +
                                                 "center of each node and its replicas" + LOWER_TO_THE_NODES)
                           .replace("kept up to the nodes of the data center", "kept as listed"));

        // nothing short among the nodes counted: no finding, the upgrade step says what was checked, and the remedy of
        // system_auth asks for the check by hand of these keyspaces too
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3", "dc2", "2", "dc3", "1"));
        Run enough = assertReady(run(node));
        assertThat(enough.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(enough.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: dse_leases, dse_perf, dse_security, " +
                                                                    "system_distributed, system_traces, enough in every data center of nodes/local " +
                                                                    "and nodes/peers, checked against the nodes whose data center is known only (the " +
                                                                    "data center of 1 node is not known from nodes/local and nodes/peers)");
        assertThat(enough.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHERS_BY_HAND);
    }

    @Test
    public void otherSystemKeyspacesNotCheckedWithOneDataCenterKnownAndANodeOfNoDataCenter() throws IOException
    {
        // dc1 known, one node of no data center: there may be several data centers, which the tool cannot tell
        DseNodeFixture node = node();
        node.editPeers(peers -> peers.get(1).remove("data_center"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3"));
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "1"));
        Run run = assertReady(run(node));
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHER_KEYSPACES);
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: not checked (the data center of 1 node is " +
                                                                 "not known from nodes/local and nodes/peers); with several data centers, each needs " +
                                                                 "replicas in every data center, 3 recommended, or as many as its nodes if fewer");

        // a keyspace whose replication cannot be read is named
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("dse_perf").with("durable_writes", true).with("replication", ByteBuffer.wrap(new byte[]{ 0, 0, 0, 9 })));
        assertThat(assertBlocked(run(node), "UNLOADABLE_KEYSPACE").report.upgradePath.upgradeSteps)
            .contains("Replication of the other system keyspaces: not checked (the data center of 1 node is not known from nodes/local and " +
                      "nodes/peers); with several data centers, each needs replicas in every data center, 3 recommended, or as many as its nodes " +
                      "if fewer; dse_perf not checked (its replication cannot be read, see UNLOADABLE_KEYSPACE)");
        writeKeyspaceReplication(node, "dse_perf", simpleStrategy());

        // a replication that gives no node a replica is said all the same
        writeKeyspaceReplication(node, "dse_security", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "0"));
        Run zero = assertReady(run(node));
        assertThat(keyspaceReplication(zero, "dse_security").message)
            .isEqualTo("the replication of dse_security is {'class': 'SimpleStrategy', 'replication_factor': '0'}: its replication_factor is 0, so " +
                       "no node holds a replica of dse_security; the data center of 1 node is not known from nodes/local and nodes/peers, so it " +
                       "was checked against the nodes whose data center is known only; HCD 2.0 does not read or write dse_security, which only " +
                       "DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(zero.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: see KEYSPACE_REPLICATION (dse_security); " +
                                                                  "otherwise not checked (the data center of 1 node is not known from nodes/local and " +
                                                                  "nodes/peers); with several data centers, each needs replicas in every data center, " +
                                                                  "3 recommended, or as many as its nodes if fewer");
    }

    @Test
    public void aHintsDirectoryThatCannotBeListedIsNotCheckedInTheUpgradeStep() throws IOException
    {
        DseNodeFixture node = node();
        Files.setPosixFilePermissions(node.hints, PosixFilePermissions.fromString("-w-------"));
        try
        {
            Assume.assumeFalse("the directory is still readable (the tests run as root)", Files.isReadable(node.hints));
            Run run = run(node);
            assertThat(run.messages("PENDING_HINTS")).contains("cannot list the hints directory");
            assertThat(run.report.upgradePath.upgradeSteps).contains("Hints: not checked (the hints directory cannot be listed); see PENDING_HINTS");
        }
        finally
        {
            Files.setPosixFilePermissions(node.hints, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void otherSystemKeyspacesChosenByNameAndClass() throws IOException
    {
        DseNodeFixture node = twoDataCenterNode();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        for (String keyspace : List.of("system_distributed", "dse_security", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        // solr_admin and any other system_* keyspace are checked, on SimpleStrategy or NetworkTopologyStrategy
        writeKeyspaceReplication(node, "solr_admin", simpleStrategy());
        writeKeyspaceReplication(node, "system_backups", networkTopologyStrategy("dc1", "3"));
        // a keyspace named system_* that is not DSE's: checked, without saying that only DSE uses it
        writeKeyspaceReplication(node, "system_custom", networkTopologyStrategy("dc1", "3", "dc2", "1"));
        // a SimpleStrategy with no replication_factor
        writeKeyspaceReplication(node, "system_traces", Map.of("class", SIMPLE_STRATEGY));
        // a user keyspace, and a DSE Graph keyspace named dse_*, which GRAPH_KEYSPACE blocks on, are not checked
        writeKeyspaceReplication(node, "test_ks", simpleStrategy());
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("dse_graphs").with("durable_writes", true).with("replication", simpleStrategy()).with("graph_engine", "Core"));
        // the keyspaces of the DSE features HCD 2.0 does not support, which NON_CORE_KEYSPACE blocks on, are not: DSE
        // Analytics' dse_analytics and a DSEFS keyspace named in dse.yaml
        writeKeyspaceReplication(node, "dse_analytics", simpleStrategy());
        node.editYaml("dse.yaml", yaml -> section(yaml, "dsefs_options").put("keyspace_name", "dse_files"));
        writeKeyspaceReplication(node, "dse_files", simpleStrategy());
        Run run = assertBlocked(run(node), "NON_CORE_KEYSPACE");
        assertThat(run.codes(Finding.Severity.BLOCKING)).contains("GRAPH_KEYSPACE");
        assertThat(run.findings("KEYSPACE_REPLICATION")).extracting(f -> f.message.substring(0, f.message.indexOf(" is ")))
                                                        .containsExactly("the replication of solr_admin", "the replication of system_backups",
                                                                         "the replication of system_custom", "the replication of system_traces");
        assertThat(keyspaceReplication(run, "system_custom").message)
            .isEqualTo("the replication of system_custom is {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '1'}: dc2 has 1 replica and 3 " +
                       "nodes, fewer than the 3 recommended; HCD 2.0 itself does not use system_custom (its clients may)" +
                       KEYSPACE_REPLICATION_NOT_BLOCKING +
                       KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(run, "solr_admin").message)
            .isEqualTo("the replication of solr_admin is {'class': 'SimpleStrategy', 'replication_factor': '1'}: SimpleStrategy does not place " +
                       "replicas in each data center, and nodes/local and nodes/peers name 2 (dc1, dc2)" + NO_REPLICA_OF_PART + "solr_admin; " +
                       "HCD 2.0 does not read or write solr_admin, which only DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING +
                       KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(run, "system_backups").message)
            .isEqualTo("the replication of system_backups is {'class': 'NetworkTopologyStrategy', 'dc1': '3'}: dc2 (3 nodes) is not listed, " +
                       "so dc2 holds no replica of system_backups; HCD 2.0 does not read or write system_backups, which only DSE uses" +
                       KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(run, "system_backups").remedy).isEqualTo(keyspaceReplicationRemedy("system_backups", "'dc1': 3, 'dc2': 3", ""));
        assertThat(keyspaceReplication(run, "system_traces").message)
            .isEqualTo("the replication of system_traces is {'class': 'SimpleStrategy'}: it has no replication_factor; SimpleStrategy does not " +
                       "place replicas in each data center, and nodes/local and nodes/peers name 2 (dc1, dc2), so no node holds a replica of " +
                       "system_traces; " + TRACES_USE + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(run, "system_traces").remedy).isEqualTo(keyspaceReplicationRemedy("system_traces", "'dc1': 3, 'dc2': 3", ""));

        // a value that is no number
        writeKeyspaceReplication(node, "system_backups", networkTopologyStrategy("dc1", "3", "dc2", "x"));
        assertThat(keyspaceReplication(run(node), "system_backups").message)
            .isEqualTo("the replication of system_backups is {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': 'x'}: dc2 is listed with 'x', " +
                       "not a number of replicas, so dc2 holds no replica of system_backups; HCD 2.0 does not read or write system_backups, which only " +
                       "DSE uses" +
                       KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);

        // a data center of no node listed with a value that is no number: wrong, but no data center of the nodes lacks replicas
        writeKeyspaceReplication(node, "system_backups", networkTopologyStrategy("dc1", "3", "dc2", "3", "old", "x"));
        assertThat(keyspaceReplication(run(node), "system_backups").message)
            .isEqualTo("the replication of system_backups is {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3', 'old': 'x'}: old is " +
                       "listed with 'x', not a number of replicas; HCD 2.0 does not read or write system_backups, which only DSE uses" +
                       KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        assertThat(keyspaceReplication(run(node), "system_backups").remedy)
            .isEqualTo(keyspaceReplicationRemedy("system_backups", "'dc1': 3, 'dc2': 3", "; old, listed with no number of replicas and with no node in " +
                                                                                          "nodes/local and nodes/peers, left out"));

        // a replication factor of 0
        writeKeyspaceReplication(node, "system_traces", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "0"));
        assertThat(keyspaceReplication(run(node), "system_traces").message)
            .isEqualTo("the replication of system_traces is {'class': 'SimpleStrategy', 'replication_factor': '0'}: its replication_factor is 0; " +
                       "SimpleStrategy does not place replicas in each data center, and nodes/local and nodes/peers name 2 (dc1, dc2), so no node " +
                       "holds a replica of system_traces; " + TRACES_USE + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
    }

    @Test
    public void otherSystemKeyspacesOnSimpleStrategyWithAReplicaOnEveryNodeAreNotWarnedAbout() throws IOException
    {
        // two data centers of one node on DSE's defaults: system_distributed (3 replicas) and system_traces (2) have a
        // replica on each node, so in each data center; DSE's keyspaces, at 1, do not
        DseNodeFixture node = node();
        node.editPeers(peers -> {
            peers.remove(1);
            peers.get(0).put("data_center", "dc2");
        });
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1", "dc2", "1"));
        writeDseDefaultReplications(node);
        Run run = assertReady(run(node));
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION")).isEmpty();
        assertThat(run.findings("KEYSPACE_REPLICATION")).extracting(f -> f.message.substring(0, f.message.indexOf(" is ")))
                                                        .containsExactly("the replication of dse_leases", "the replication of dse_perf",
                                                                         "the replication of dse_security");
        // system_auth, on SimpleStrategy over the two data centers, is SYSTEM_AUTH_REPLICATION's only
        writeAuthReplication(node, simpleStrategy());
        Run auth = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(auth.findings("KEYSPACE_REPLICATION")).noneMatch(f -> f.message.startsWith("the replication of system_auth "));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "1", "dc2", "1"));

        // with a node of no data center, the nodes counted are a lower bound: no replica on every node is known
        addPeer(node, 199, false);
        Run uncounted = run(node);
        assertThat(uncounted.findings("KEYSPACE_REPLICATION")).extracting(f -> f.message.substring(0, f.message.indexOf(" is ")))
                                                              .containsExactly("the replication of dse_leases", "the replication of dse_perf",
                                                                               "the replication of dse_security",
                                                                               "the replication of system_distributed",
                                                                               "the replication of system_traces");
        assertThat(keyspaceReplication(uncounted, "system_distributed").message)
            .isEqualTo("the replication of system_distributed is {'class': 'SimpleStrategy', 'replication_factor': '3'}: SimpleStrategy does not " +
                       "place replicas in each data center, and nodes/local and nodes/peers name 2 (dc1, dc2)" + NO_REPLICA_OF_PART +
                       "system_distributed; the data center of 1 node is not known from nodes/local and nodes/peers, so it was checked against " +
                       "the nodes whose data center is known only; " + DISTRIBUTED_USE + KEYSPACE_REPLICATION_NOT_BLOCKING +
                       KEYSPACE_REPLICATION_RECOMMENDED);
    }

    @Test
    public void otherSystemKeyspaceWithAReplicationThatCannotBeReadIsLeftToUnloadableKeyspace() throws IOException
    {
        DseNodeFixture node = twoDataCenterNode();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        for (String keyspace : List.of("system_distributed", "system_traces", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(),
                       row("dse_security").with("durable_writes", true).with("replication", ByteBuffer.wrap(new byte[]{ 0, 0, 0, 9 })));
        Run run = assertBlocked(run(node), "UNLOADABLE_KEYSPACE");
        assertThat(run.messages("UNLOADABLE_KEYSPACE")).contains("dse_security: ");
        assertThat(run.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(run.findings("INSPECTION_ERROR")).isEmpty();
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: dse_leases, dse_perf, system_distributed, " +
                                                                 "system_traces, enough in every data center of nodes/local and nodes/peers; " +
                                                                 "dse_security not checked (its replication cannot be read, see " +
                                                                 "UNLOADABLE_KEYSPACE)");
    }

    @Test
    public void otherSystemKeyspacesWithNodesLocalMissing() throws IOException
    {
        // nodes/peers names dc1 (2 nodes) and dc2 (3): the other keyspaces are checked against the nodes counted
        DseNodeFixture node = twoDataCenterNode();
        Files.delete(node.metadata.resolve("nodes").resolve("local"));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        writeDseDefaultReplications(node);
        Run run = assertBlocked(run(node), "NOT_A_DSE_NODE");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo(AUTH_REPLICATION_BY_HAND + AUTH_REPLICATION_OTHERS_BY_HAND);
        Finding leases = keyspaceReplication(run, "dse_leases");
        assertThat(leases.message).isEqualTo("the replication of dse_leases is {'class': 'SimpleStrategy', 'replication_factor': '1'}: " +
                                             "SimpleStrategy does not place replicas in each data center, and nodes/local and nodes/peers " +
                                             "name 2 (dc1, dc2)" + NO_REPLICA_OF_PART + "dse_leases; " + NODES_FILES_NOT_READ + ", so it was " +
                                             "checked against the nodes whose data center is known only; HCD 2.0 does not read or write " +
                                             "dse_leases, which only DSE uses" + KEYSPACE_REPLICATION_NOT_BLOCKING + KEYSPACE_REPLICATION_RECOMMENDED);
        // nodes/peers read: the mixed phase is known, so the remedy has no caveat on it
        assertThat(leases.remedy).isEqualTo(keyspaceReplicationRemedy("dse_leases", "'dc1': 3, 'dc2': 3",
                                                                      "; the replication factor of SimpleStrategy spans the data centers, so it is not " +
                                                                      "carried over; " + NODES_FILES_NOT_READ + ": check by hand the data center " +
                                                                      "of each node and its replicas" + LOWER_TO_THE_NODES)
                                                .replace("kept up to the nodes of the data center", "kept as listed"));
        // a DSE hints file for a host id of no known node, which may be this node's, is only said
        String hostId = UUID.randomUUID().toString();
        Files.write(node.hints.resolve(hostId + "-1790335633208-v1.hints"), new byte[0]);
        Run hints = assertBlocked(run(node), "NOT_A_DSE_NODE");
        assertThat(hints.messages("PENDING_HINTS")).isEqualTo(HINTS_NOT_CHECKED + hostId + ": 1 file(s)" + HINTS_NOT_CHECKED_END);
    }

    @Test
    public void otherSystemKeyspaceAbsentFromTheSchemaGivesNothing() throws IOException
    {
        DseNodeFixture node = twoDataCenterNode();
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        writeDseDefaultReplications(node);
        // as on a cluster that never had system_traces
        node.deletePartitions("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), "system_traces");
        Run run = assertReady(run(node));
        assertThat(run.findings("KEYSPACE_REPLICATION")).hasSize(4).noneMatch(f -> f.message.contains("system_traces"));
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: see KEYSPACE_REPLICATION (dse_leases, dse_perf, " +
                                                                 "dse_security, system_distributed)");

        // EverywhereStrategy, a replica on every node, stays as it is: with the others on it, none is checked
        for (String keyspace : List.of("system_distributed", "dse_security", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        Run everywhere = assertReady(run(node));
        assertThat(everywhere.findings("KEYSPACE_REPLICATION")).isEmpty();
        assertThat(everywhere.report.upgradePath.upgradeSteps).contains("Replication of the other system keyspaces: none on SimpleStrategy or " +
                                                                        "NetworkTopologyStrategy");
    }

    @Test
    public void otherSystemKeyspacesInTheMixedPhaseAskForNoChange() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCentersMixed(1));
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        writeAuthReplication(node, networkTopologyStrategy("dc1", "3", "dc2", "3"));
        writeDseDefaultReplications(node);
        writeKeyspaceReplication(node, "dse_security", networkTopologyStrategy("dc1", "3"));
        Run run = run(node);
        assertThat(run.codes(Finding.Severity.BLOCKING)).doesNotContain("KEYSPACE_REPLICATION", "SYSTEM_AUTH_REPLICATION");
        assertThat(run.codes(Finding.Severity.WARNING)).contains("KEYSPACE_REPLICATION");
        Finding distributed = keyspaceReplication(run, "system_distributed");
        assertThat(distributed.message).isEqualTo("the replication of system_distributed is {'class': 'SimpleStrategy', 'replication_factor': '3'}: " +
                                                  "SimpleStrategy does not place replicas in each data center, and nodes/local and nodes/peers name " +
                                                  "2 (dc1, dc2)" + NO_REPLICA_OF_PART + "system_distributed; " + DISTRIBUTED_USE +
                                                  KEYSPACE_REPLICATION_NOT_BLOCKING);
        assertThat(distributed.remedy).isEqualTo("the upgrade has started (UPGRADED_PEERS), so the replication stays as it is (no schema change and no " +
                                                 "repair in the mixed phase)");
        Finding security = keyspaceReplication(run, "dse_security");
        assertThat(security.message).isEqualTo("the replication of dse_security is {'class': 'NetworkTopologyStrategy', 'dc1': '3'}: dc2 (3 nodes) is " +
                                               "not listed, so dc2 holds no replica of dse_security; HCD 2.0 does not read or write dse_security, " +
                                               "which only DSE uses" +
                                               KEYSPACE_REPLICATION_NOT_BLOCKING);
        assertThat(security.remedy).isEqualTo(distributed.remedy);
        assertThat(run.findings("KEYSPACE_REPLICATION")).allMatch(f -> !f.remedy.contains("ALTER KEYSPACE") && !f.remedy.contains("nodetool repair"));
    }

    @Test
    public void systemAuthMissingFromTheSchemaBlocks() throws IOException
    {
        DseNodeFixture node = node();
        node.deletePartitions("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), "system_auth");
        Run run = assertBlocked(run(node), "SYSTEM_AUTH_REPLICATION");
        assertThat(run.messages("SYSTEM_AUTH_REPLICATION")).isEqualTo("the schema read from this node's sstables has no system_auth keyspace, which DSE " +
                                                                      "creates at its first start, so its replication cannot be checked");
        assertThat(run.findings("SYSTEM_AUTH_REPLICATION").get(0).remedy).isEqualTo("run nodetool flush system_schema on this node and this tool again; if " +
                                                                                    "system_auth is still missing, point the tool at the data directories " +
                                                                                    "of this DSE node (data_file_directories of DSE's cassandra.yaml, or " +
                                                                                    "--data-dir)");
        assertThat(run.report.upgradePath.upgradeSteps).contains("Replication of system_auth: not checked (the schema has no system_auth keyspace)");
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
        assertThat(run.messages("UNREADABLE_SCHEMA")).contains("format, which this tool does not read: opening it could delete or rewrite some of its files");
        assertThat(run.findings("UNREADABLE_SCHEMA").get(0).remedy).contains("if an sstable is not in DSE 6's trie format, run nodetool upgradesstables on DSE first");
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
                  "DSE was restarted since (ss -tln lists ssl_storage_port); the report of every DSE node checks its own file, and the " +
                  "aggregate is READY only if all of them pass");

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
    static DseNodeFixture withInternodeTls(DseNodeFixture node, String dseVersion, String internodeEncryption, @Nullable Boolean legacySslStoragePort)
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
        Run notADirectory = run(json, "--aggregate", json.resolveSibling("missing").toString());
        assertThat(notADirectory.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(notADirectory.err).contains("--aggregate needs a directory of JSON reports: " + json.resolveSibling("missing"));
        // every other option is refused with --aggregate, which would ignore it
        List<String> refused = new ArrayList<>();
        for (Option option : DsePreflight.options().getOptions())
        {
            String name = option.getLongOpt();
            if (DsePreflight.AGGREGATE_OPTIONS.contains(name))
                continue;
            refused.add(name);
            Run misuse = run(json, "--aggregate", temporaryFolder.getRoot().toString(), option.hasArg() ? "--" + name + "=x" : "--" + name);
            assertThat(misuse.exitCode).as(name).isEqualTo(DsePreflight.EXIT_ERROR);
            // the Paxos rollback mode refuses --aggregate first
            assertThat(misuse.err).as(name).startsWith(name.equals("paxos-rollback") ? "--aggregate does not apply to --paxos-rollback"
                                                                                     : "--" + name + " does not apply to --aggregate");
        }
        assertThat(refused).contains("dse-home", "data-dir", "nodetool", "nodetool-timeout");
        Run help = run(json, "--help");
        assertThat(help.exitCode).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(help.out).contains("--aggregate").contains("--nodetool").contains("--jvm-options");
        assertThat(help.out.replaceAll("\\s+", " "))
        .contains("Inspects a DSE 6.8/6.9 or DSE 5.1.51 node, read-only")
        .contains("With --aggregate, checks the JSON reports of the")
        .contains("Exit codes: 0 ready (warnings allowed), 1 blocking findings or cluster not ready");
        // the lines of the Paxos rollback mode that the procedure looks for in the help
        assertThat(help.out.lines()).anyMatch(l -> l.matches("^ +--paxos-rollback .*"));
        assertThat(help.out.replaceAll("\\s+", " ")).contains("Only the directory given is read, not its subdirectories");
        assertThat(json).doesNotExist();
    }

    /**
     * The rollback check of a node going back to DSE 5.1 takes the options of its inputs and --json only, refuses the
     * others, so that none is silently ignored, and is named in the help with its exit codes.
     */
    @Test
    public void theRollbackCheckTakesOnlyItsOptions() throws IOException
    {
        Path json = temporaryFolder.getRoot().toPath().resolve("unused.json");
        List<String> refused = new ArrayList<>();
        for (Option option : DsePreflight.options().getOptions())
        {
            String name = option.getLongOpt();
            if (DsePreflight.ROLLBACK_CHECK_OPTIONS.contains(name))
                continue;
            refused.add(name);
            Run misuse = run(json, "--rollback-check", option.hasArg() ? "--" + name + "=x" : "--" + name);
            assertThat(misuse.exitCode).as(name).isEqualTo(DsePreflight.EXIT_ERROR);
            // the Paxos rollback mode and the aggregate mode refuse it first
            String refusal = name.equals("paxos-rollback") ? "--rollback-check does not apply to --paxos-rollback"
                             : name.equals("aggregate") ? "--rollback-check does not apply to --aggregate"
                             : "--" + name + " does not apply to --rollback-check";
            assertThat(misuse.err).as(name).startsWith(refusal);
        }
        assertThat(refused).contains("dse-yaml", "rackdc", "jvm-options", "nodetool", "cql", "dc")
                           .doesNotContain("snapshot");
        // --snapshot, which both rollback modes take, is refused alone with both named
        Run snapshotAlone = run(json, "--snapshot", "pre-hcd-x");
        assertThat(snapshotAlone.exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(snapshotAlone.err).startsWith("--snapshot applies only to --paxos-rollback and --rollback-check");
        Run help = run(json, "--help");
        assertThat(help.exitCode).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(help.out.lines()).anyMatch(l -> l.matches("^ +--rollback-check .*"));
        assertThat(help.out.replaceAll("\\s+", " "))
        .contains("dsepreflight --rollback-check [options] check a node to roll back to DSE 5.1")
        .contains("With --rollback-check, checks a node of HCD 2.0 to roll back to DSE 5.1.")
        .contains("With --rollback-check: 0 the node can go back to DSE 5.1 (warnings allowed), 1 blocking findings, " +
                  "2 usage or inspection error.");
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

    /** A NetworkTopologyStrategy replication with the data centers and replicas given in pairs. */
    private static Map<String, String> networkTopologyStrategy(String... dataCentersAndReplicas)
    {
        Map<String, String> replication = new LinkedHashMap<>();
        replication.put("class", NETWORK_TOPOLOGY_STRATEGY);
        for (int i = 0; i < dataCentersAndReplicas.length; i += 2)
            replication.put(dataCentersAndReplicas[i], dataCentersAndReplicas[i + 1]);
        return replication;
    }

    /**
     * Adds to nodes/peers a copy of its first peer with its own address, 192.168.57.{@code lastOctet} (as DSE writes an
     * address: its bytes), and its own host id, in the data center of the first peer or in none. Its tokens, release
     * and schema version are those of the first peer: the tool only counts the tokens, so sharing them changes nothing
     * it checks.
     */
    private static void addPeer(DseNodeFixture node, int lastOctet, boolean withDataCenter) throws IOException
    {
        byte[] address = { (byte) 192, (byte) 168, 57, (byte) lastOctet };
        node.editPeers(peers -> {
            Map<String, Object> peer = new LinkedHashMap<>(peers.get(0));
            for (String key : List.of("peer", "broadcast_address", "listen_address"))
                if (peer.containsKey(key))
                    peer.put(key, address);
            // DSE writes a host id as its two longs
            peer.put("host_id", List.of(0L, (long) lastOctet));
            if (!withDataCenter)
                peer.remove("data_center");
            peers.add(peer);
        });
    }

    /** Writes the replication of system_auth, over the one DSE stored. */
    private static void writeAuthReplication(DseNodeFixture node, Map<String, String> replication) throws IOException
    {
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), row("system_auth").with("durable_writes", true).with("replication", replication));
    }

    /** Writes the replication of {@code keyspace}, over the one DSE stored. */
    private static void writeKeyspaceReplication(DseNodeFixture node, String keyspace, Map<String, String> replication) throws IOException
    {
        node.writeRows("system_schema", "keyspaces", KEYSPACES_KEY, List.of(), row(keyspace).with("durable_writes", true).with("replication", replication));
    }

    /**
     * Writes the replications DSE 6.9.26 gives its replicated keyspaces other than system_auth, as the fixture
     * {@code schema/dse-6.9.26/describe-schema.txt} shows them.
     */
    private static void writeDseDefaultReplications(DseNodeFixture node) throws IOException
    {
        writeKeyspaceReplication(node, "system_distributed", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "3"));
        writeKeyspaceReplication(node, "system_traces", Map.of("class", SIMPLE_STRATEGY, "replication_factor", "2"));
        for (String keyspace : List.of("dse_security", "dse_leases", "dse_perf"))
            writeKeyspaceReplication(node, keyspace, simpleStrategy());
        for (String keyspace : List.of("dse_system", "dse_insights", "system_backups", "solr_admin"))
            writeKeyspaceReplication(node, keyspace, Map.of("class", "org.apache.cassandra.locator.EverywhereStrategy"));
        for (String keyspace : List.of("dse_system_local", "dse_insights_local"))
            writeKeyspaceReplication(node, keyspace, Map.of("class", "org.apache.cassandra.locator.LocalStrategy"));
    }

    /** Node 1 of the two-data-center DSE 6.9.26 test cluster, 3 nodes in dc1 and 3 in dc2, with the schema of the fixtures. */
    private DseNodeFixture twoDataCenterNode() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryTwoDataCenters(1));
        Files.write(node.dseHome.resolve("ds_version.txt"), "DSVersion=6.9.26-0c77b67\n".getBytes(StandardCharsets.UTF_8));
        return node;
    }

    /** The one KEYSPACE_REPLICATION finding of {@code keyspace}. */
    private static Finding keyspaceReplication(Run run, String keyspace)
    {
        List<Finding> findings = run.findings("KEYSPACE_REPLICATION").stream()
                                    .filter(f -> f.message.startsWith("the replication of " + keyspace + " is "))
                                    .collect(Collectors.toList());
        assertThat(findings).as("KEYSPACE_REPLICATION of %s; output:\n%s", keyspace, run.out).hasSize(1);
        return findings.get(0);
    }

    /**
     * The remedy of KEYSPACE_REPLICATION on a low count only, optional, before the first node is upgraded, the data centers
     * of the nodes all known: the replicas per data center its ALTER KEYSPACE lists and what it says of the data centers
     * after them.
     */
    private static String keyspaceReplicationOptionalRemedy(String keyspace, String replicas, String dataCenters)
    {
        return "optional, " + keyspaceReplicationRemedy(keyspace, replicas, dataCenters);
    }

    /**
     * The remedy of KEYSPACE_REPLICATION with a data center without replicas, or a data center listed with a value that is
     * no number, not optional though it does not block: see
     * {@link #keyspaceReplicationOptionalRemedy}.
     */
    private static String keyspaceReplicationRemedy(String keyspace, String replicas, String dataCenters)
    {
        return "before the first node of the cluster is upgraded, with the NodeSync flag off on the tables of " + keyspace + " (DSE's repair " +
               "skips a table whose flag is on), through one DSE node: ALTER KEYSPACE " + keyspace + " WITH replication = {'class': " +
               "'NetworkTopologyStrategy', " + replicas + "}; (every data center, its exact name as nodetool status shows it; 3 replicas, or as many " +
               "as its nodes if fewer, are recommended; a higher replication factor is kept up to the nodes of the data center" + dataCenters + "); " +
               "DESCRIBE KEYSPACE " + keyspace + " then shows the new map and nodetool describecluster one schema version; right after, nodetool " +
               "repair -full " + keyspace + " on every node, one at a time, each exiting 0 and printing no Skipping anti-entropy repair line, a " +
               "failed one run again, whole, on its node (the ALTER and the repair in a quiet period, without a pause between them); then nodetool " +
               "flush system_schema on this node and run this tool again";
    }

    /**
     * The change the remedies of SYSTEM_AUTH_REPLICATION give before the first node is upgraded: the replicas per data
     * center its ALTER KEYSPACE lists, what it says of the data centers after them, and whether it ends with the clause
     * on data centers of 2 nodes, and the clause on the other keyspaces it ends with, if any.
     */
    private static String authReplicationChange(String replicas, String dataCenters, boolean twoNodes, String others)
    {
        return "with the NodeSync flag off on the tables of system_auth (DSE's repair skips a table whose flag is on), through one DSE node: " +
               "ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', " + replicas + "}; (every data center, its " +
               "exact name as nodetool status shows it; " + AUTH_REPLICATION_RECOMMENDED + "; a higher replication factor is kept up to the " +
               "nodes of the data center" + dataCenters + "); DESCRIBE KEYSPACE system_auth then shows the new map and nodetool " +
               "describecluster one schema version; right after, nodetool repair -full system_auth on every node, one at a time, each " +
               "exiting 0 and printing no Skipping anti-entropy repair line, a failed one run again, whole, on its node (the ALTER and the " +
               "repair in a quiet period, without a pause between them); then nodetool flush system_schema on this node and run this tool " +
               "again" +
               (twoNodes ? "; in a data center of 2 nodes, an upgraded node still cannot start while the other one is down" : "") +
               others;
    }

    /**
     * The remedy of a blocking SYSTEM_AUTH_REPLICATION before the first node is upgraded:
     * {@link #authReplicationChange}, the data centers of the nodes all known.
     */
    private static String authReplicationRemedy(String replicas, String dataCenters, boolean twoNodes)
    {
        return "before the first node of the cluster is upgraded, " + authReplicationChange(replicas, dataCenters, twoNodes, "");
    }

    /**
     * The remedy of a warning SYSTEM_AUTH_REPLICATION before the first node is upgraded:
     * {@link #authReplicationChange}, optional, the data centers of the nodes all known.
     */
    private static String authReplicationWarningRemedy(String replicas, String dataCenters, boolean twoNodes)
    {
        return "optional, before the first node of the cluster is upgraded, " + authReplicationChange(replicas, dataCenters, twoNodes, "");
    }

    /**
     * {@link #authReplicationRemedy} when the nodes counted are a lower bound: the replicas are raised to 3 and kept when
     * higher (the remedy says "kept as listed" instead of "kept up to the nodes of the data center"), and the remedy ends
     * with a clause on the other keyspaces: {@link #AUTH_REPLICATION_OTHER_KEYSPACES} when nodes/local and nodes/peers
     * name one data center at most, {@link #AUTH_REPLICATION_OTHERS_BY_HAND} when they name several.
     */
    private static String authReplicationRemedyNotExact(String replicas, String dataCenters, String others)
    {
        return ("before the first node of the cluster is upgraded, " + authReplicationChange(replicas, dataCenters, false, others))
               .replace("kept up to the nodes of the data center", "kept as listed");
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
