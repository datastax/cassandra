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
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The aggregate mode of the preflight inspection ({@code --aggregate}): on reports of fixture nodes produced by the tool
 * itself, and on reports built here, one per rule of the cluster-wide check.
 */
public class ClusterAggregateTest
{
    private static final String RELEASE = "4.0.0.6865";
    private static final String SCHEMA = "78b1789d-6787-353b-9443-b33b3c0d7120";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void initialise()
    {
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, "bb"))
        {
            DatabaseDescriptor.clientInitialization();
        }
    }

    /** The result of {@code dsepreflight --aggregate <directory> --json <file>}. */
    private static final class Aggregate
    {
        final int exitCode;
        final String out;
        final Map<String, Object> json;

        Aggregate(int exitCode, String out, Map<String, Object> json)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.json = json;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> findings()
        {
            return (List<Map<String, Object>>) json.get("findings");
        }

        Set<String> codes(String severity)
        {
            return findings().stream().filter(f -> severity.equals(f.get("severity"))).map(f -> (String) f.get("code")).collect(Collectors.toSet());
        }

        String messages(String code)
        {
            return findings().stream().filter(f -> code.equals(f.get("code"))).map(f -> (String) f.get("message")).collect(Collectors.joining("\n"));
        }
    }

    private Aggregate aggregate(Path directory) throws IOException
    {
        return aggregate(directory, temporaryFolder.newFolder().toPath().resolve("aggregate.json"));
    }

    private static Aggregate aggregate(Path directory, Path json) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream stream = new PrintStream(out, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(new String[]{ "--aggregate", directory.toString(), "--json", json.toString() }, stream, stream);
        }
        byte[] report = Files.readAllBytes(json);
        DsePreflightTest.assertNoPlanDecisionIds(out.toString(StandardCharsets.UTF_8), new String(report, StandardCharsets.UTF_8));
        Map<String, Object> parsed = JsonUtils.JSON_OBJECT_MAPPER.readValue(report, new TypeReference<Map<String, Object>>() {});
        return new Aggregate(exitCode, out.toString(StandardCharsets.UTF_8), parsed);
    }

    // ---- reports of the fixture nodes ----

    @Test
    public void reportsOfTheFixtureNodesAgreeOnceTheyDescribeTheSameCluster() throws IOException
    {
        Path reports = temporaryFolder.newFolder("reports").toPath();
        DseNodeFixture first = DseNodeFixture.create(temporaryFolder.newFolder().toPath());
        DseNodeFixture second = DseNodeFixture.create(temporaryFolder.newFolder().toPath(), DseFixtures.nodesDirectoryMixed());
        assertThat(DsePreflightTest.run(reports.resolve("node2.json"), second.args()).exitCode).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(DsePreflightTest.run(reports.resolve("node1.json"), first.args()).exitCode).isEqualTo(DsePreflight.EXIT_READY);

        // node 1's files were taken a week before node 2's: another schema version, and node 3 still on DSE
        Aggregate stale = aggregate(reports);
        assertThat(stale.exitCode).as(stale.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(stale.codes("BLOCKING")).containsExactlyInAnyOrder("SCHEMA_DISAGREEMENT", "MISSING_REPORT");
        assertThat(stale.messages("MISSING_REPORT")).contains("192.168.57.13 (host id ed39c7b2-7f97-4a0a-91fc-71398afe1d12) listed by [192.168.57.11]");
        assertThat(stale.messages("UPGRADED_PEERS")).contains("192.168.57.13");

        // node 1 as it would be after node 3's upgrade, at node 2's schema version
        NodeReport secondReport = ClusterAggregate.READER.readValue(Files.readAllBytes(reports.resolve("node2.json")));
        first.editLocal(local -> local.put("schema_version", uuidAsDseWritesIt(secondReport.node.schemaVersion)));
        first.editPeers(peers -> peers.forEach(peer -> {
            if (secondReport.peers.stream().anyMatch(p -> p.upgraded && p.hostId.equals(hostId(peer))))
                peer.put("release_version", "5.0.7.0-SNAPSHOT");
            else
                peer.put("schema_version", uuidAsDseWritesIt(secondReport.node.schemaVersion));
        }));
        assertThat(DsePreflightTest.run(reports.resolve("node1.json"), first.args()).exitCode).isEqualTo(DsePreflight.EXIT_READY);
        Aggregate ready = aggregate(reports);
        assertThat(ready.exitCode).as(ready.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(ready.json).containsEntry("result", NodeReport.READY).containsEntry("reports_read", 2);
        assertThat(ready.codes("BLOCKING")).isEmpty();
        assertThat(ready.messages("UPGRADED_PEERS")).contains("192.168.57.13");
        assertThat(ready.out).contains("Result: READY").contains("AUTH_CONFIG_CHANGE: 192.168.57.11, 192.168.57.12");
    }

    /** A UUID as DSE writes it in its nodes files: an array of two longs. */
    private static List<Long> uuidAsDseWritesIt(String uuid)
    {
        UUID parsed = UUID.fromString(uuid);
        return List.of(parsed.getMostSignificantBits(), parsed.getLeastSignificantBits());
    }

    private static String hostId(Map<String, Object> entry)
    {
        List<?> parts = (List<?>) entry.get("host_id");
        return new UUID(((Number) parts.get(0)).longValue(), ((Number) parts.get(1)).longValue()).toString();
    }

    // ---- one rule at a time ----

    @Test
    public void aClusterOfReadyNodesThatListEachOtherIsReady() throws IOException
    {
        Path reports = cluster(3);
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).as(aggregate.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(aggregate.codes("BLOCKING")).isEmpty();
        assertThat(aggregate.json).containsEntry("reports_read", 3);
    }

    @Test
    public void aBlockedNodeBlocksTheCluster() throws IOException
    {
        Path reports = cluster(3);
        rewrite(reports.resolve("node2.json"), report -> {
            report.result = NodeReport.BLOCKED;
            report.findings.add(Finding.blocking("SEARCH_INDEX", "DSE Search indexes: ks.t.i", "drop it"));
        });
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.messages("NODE_BLOCKED")).contains("10.0.0.2 [SEARCH_INDEX]");
    }

    @Test
    public void releasesSchemaVersionsAndClusterNamesMustAgree() throws IOException
    {
        Path reports = cluster(3);
        rewrite(reports.resolve("node1.json"), report -> report.node.releaseVersion = "4.0.0.6926");
        rewrite(reports.resolve("node2.json"), report -> report.node.schemaVersion = "42be62c3-6cac-39ac-8412-42c8cbd3485c");
        rewrite(reports.resolve("node3.json"), report -> report.node.clusterName = "other");
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.codes("BLOCKING")).containsExactlyInAnyOrder("MIXED_RELEASES", "SCHEMA_DISAGREEMENT", "CLUSTER_NAMES");
        assertThat(aggregate.messages("MIXED_RELEASES")).contains("4.0.0.6926 on 10.0.0.1").contains(RELEASE + " on 10.0.0.2, 10.0.0.3");
    }

    @Test
    public void everyDsePeerNeedsAReportButUpgradedOnesDoNot() throws IOException
    {
        Path reports = cluster(3);
        Files.delete(reports.resolve("node3.json"));
        Aggregate missing = aggregate(reports);
        assertThat(missing.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(missing.messages("MISSING_REPORT")).contains("10.0.0.3 (host id 00000000-0000-0000-0000-000000000003) listed by [10.0.0.1, 10.0.0.2]");

        for (String file : List.of("node1.json", "node2.json"))
            rewrite(reports.resolve(file), report -> report.peers.stream().filter(p -> "10.0.0.3".equals(p.address)).forEach(p -> {
                p.upgraded = true;
                p.releaseVersion = "5.0.7.0";
            }));
        Aggregate upgraded = aggregate(reports);
        assertThat(upgraded.exitCode).as(upgraded.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(upgraded.messages("UPGRADED_PEERS")).contains("10.0.0.3");
    }

    @Test
    public void reportsThatCannotBeUsedBlock() throws IOException
    {
        Path reports = cluster(2);
        Files.copy(reports.resolve("node1.json"), reports.resolve("node1-again.json"));
        Files.write(reports.resolve("broken.json"), "{ not json".getBytes(StandardCharsets.UTF_8));
        Files.write(reports.resolve("other-tool.json"), "{\"tool\": \"something else\"}".getBytes(StandardCharsets.UTF_8));
        rewrite(reports.resolve("node2.json"), report -> report.formatVersion = NodeReport.FORMAT_VERSION + 1);
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.codes("BLOCKING")).contains("UNREADABLE_REPORT", "DUPLICATE_REPORT", "MISSING_REPORT");
        assertThat(aggregate.messages("UNREADABLE_REPORT")).contains("broken.json").contains("other-tool.json (not a report of dsepreflight)")
                                                          .contains("node2.json (report format " + (NodeReport.FORMAT_VERSION + 1));
        assertThat(aggregate.messages("DUPLICATE_REPORT")).contains("00000000-0000-0000-0000-000000000001: [10.0.0.1 (node1-again.json), " +
                                                                    "10.0.0.1 (node1.json)]");

        Aggregate empty = aggregate(temporaryFolder.newFolder().toPath());
        assertThat(empty.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(empty.codes("BLOCKING")).containsExactly("NO_REPORT");
    }

    @Test
    public void reportsOfDifferentBuildsBlockAndAggregateReportsAreSkipped() throws IOException
    {
        Path reports = cluster(2);
        rewrite(reports.resolve("node1.json"), report -> report.softwareVersion = "0.0.1-a");
        rewrite(reports.resolve("node2.json"), report -> report.softwareVersion = "0.0.2-b");
        // the aggregate report written into the directory of the node reports is skipped by the next run
        Aggregate first = aggregate(reports, reports.resolve("aggregate.json"));
        assertThat(first.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(first.messages("MIXED_TOOL_BUILDS")).contains("0.0.1-a on 10.0.0.1").contains("0.0.2-b on 10.0.0.2")
                                                       .contains("; this aggregate runs the release " + FBUtilities.getReleaseVersionString());
        assertThat(first.json).containsEntry("kind", NodeReport.AGGREGATE);
        Aggregate second = aggregate(reports, reports.resolve("aggregate.json"));
        assertThat(second.codes("BLOCKING")).containsExactly("MIXED_TOOL_BUILDS");
        assertThat(second.messages("AGGREGATE_REPORT_SKIPPED")).contains("aggregate.json");
        assertThat(second.json).containsEntry("reports_read", 2);

        // the aggregating build among the builds of the reports: not named again
        rewrite(reports.resolve("node2.json"), report -> report.softwareVersion = FBUtilities.getReleaseVersionString());
        Aggregate third = aggregate(reports);
        assertThat(third.messages("MIXED_TOOL_BUILDS")).contains("0.0.1-a on 10.0.0.1").doesNotContain("this aggregate runs the release");
    }

    /**
     * INTERNODE_TLS: DSE nodes that differ in internode_encryption (absent: none; case ignored) or, with encryption, in
     * ssl_storage_port (absent: 7001) are named, with a remedy that says which port each product dials; without encryption
     * the port is not compared.
     */
    @Test
    public void internodeTlsSettingsMustAgree() throws IOException
    {
        Path reports = cluster(3);
        rewrite(reports.resolve("node1.json"), report -> {
            report.upgradePath.settings.put("server_encryption_options.internode_encryption", "none");
            report.upgradePath.settings.put("ssl_storage_port", "7001");
        });
        // the note a node report adds to a port DSE's cassandra.yaml does not set is not a difference
        rewrite(reports.resolve("node2.json"), report -> report.upgradePath.settings.put("ssl_storage_port", "7001 (DSE's cassandra.yaml sets none: the default of both products)"));
        // without encryption ssl_storage_port is not used, so it may differ
        rewrite(reports.resolve("node3.json"), report -> report.upgradePath.settings.put("ssl_storage_port", "7011"));
        Aggregate agreeing = aggregate(reports);
        assertThat(agreeing.exitCode).as(agreeing.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(agreeing.codes("WARNING")).doesNotContain("INTERNODE_TLS");
        assertThat(agreeing.codes("BLOCKING")).doesNotContain("MIXED_STORAGE_PORTS");

        // the same mode written in another case
        for (String file : List.of("node1.json", "node2.json", "node3.json"))
            rewrite(reports.resolve(file), report -> report.upgradePath.settings.put("ssl_storage_port", "7001"));
        rewrite(reports.resolve("node1.json"), report -> report.upgradePath.settings.put("server_encryption_options.internode_encryption", "ALL"));
        rewrite(reports.resolve("node2.json"), report -> report.upgradePath.settings.put("server_encryption_options.internode_encryption", "all"));
        rewrite(reports.resolve("node3.json"), report -> report.upgradePath.settings.put("server_encryption_options.internode_encryption", " all"));
        assertThat(aggregate(reports).codes("WARNING")).doesNotContain("INTERNODE_TLS");
        rewrite(reports.resolve("node1.json"), report -> report.upgradePath.settings.put("server_encryption_options.internode_encryption", "none"));

        rewrite(reports.resolve("node2.json"), report -> report.upgradePath.settings.put("server_encryption_options.internode_encryption", "all"));
        rewrite(reports.resolve("node3.json"), report -> {
            report.upgradePath.settings.put("server_encryption_options.internode_encryption", "all");
            report.upgradePath.settings.put("ssl_storage_port", "7443");
        });
        Aggregate differing = aggregate(reports);
        assertThat(differing.exitCode).as(differing.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(differing.codes("WARNING")).containsExactly("INTERNODE_TLS");
        assertThat(differing.messages("INTERNODE_TLS"))
        .isEqualTo("the DSE nodes differ in their internode TLS settings: internode_encryption all, ssl_storage_port 7001 on 10.0.0.2; " +
                   "internode_encryption all, ssl_storage_port 7443 on 10.0.0.3; internode_encryption none on 10.0.0.1");
        assertThat(differing.findings().stream().filter(f -> "INTERNODE_TLS".equals(f.get("code"))).map(f -> (String) f.get("remedy")))
        .containsExactly("give every DSE node the same internode_encryption and ssl_storage_port before the upgrade: for the connections they " +
                         "encrypt, HCD 2.0 dials its own ssl_storage_port on every DSE node below 6.9.7, and on one of unknown version when " +
                         "legacy_ssl_storage_port_enabled is on; DSE dials its own on the upgraded nodes (read in DSE's code for 6.8.65, 6.9.0 to " +
                         "6.9.6, the port choice of 6.9.7, and 6.9.26; the other releases inferred from them): DSE 6.8 and 6.9 before 6.9.7 " +
                         "always, DSE 6.9.7+ once it has stored a messaging version below DSE 6.9.7's for them (12 from gossip, 11 from a " +
                         "handshake); and check DSE's JVM options for a -Dcassandra.ssl_storage_port, which must be absent or give that same " +
                         "ssl_storage_port (required: DSE honours it over cassandra.yaml, and this tool reads cassandra.yaml only; check it by " +
                         "hand: the command line of the running DSE process lists the -D options it was started with)");
    }

    /**
     * INTERNODE_TLS on a node of DSE 6.9.7+ without DSE's legacy TLS port is a blocking finding of that node, so the cluster
     * is not ready (NODE_BLOCKED names it); the same node with the setting true, a warning, leaves the cluster ready.
     */
    @Test
    public void aNodeWithoutDsesLegacyTlsPortBlocksTheCluster() throws IOException
    {
        Path nodeReports = temporaryFolder.newFolder().toPath();
        Path without = nodeReports.resolve("without.json");
        Path with = nodeReports.resolve("with.json");
        DseNodeFixture withoutPort = DsePreflightTest.withInternodeTls(DseNodeFixture.create(temporaryFolder.newFolder().toPath()), "6.9.26", "all", null);
        DseNodeFixture withPort = DsePreflightTest.withInternodeTls(DseNodeFixture.create(temporaryFolder.newFolder().toPath()), "6.9.26", "all", true);
        assertThat(DsePreflightTest.run(without, withoutPort.args()).exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(DsePreflightTest.run(with, withPort.args()).exitCode).isEqualTo(DsePreflight.EXIT_READY);
        NodeReport blocked = ClusterAggregate.READER.readValue(Files.readAllBytes(without));
        NodeReport ready = ClusterAggregate.READER.readValue(Files.readAllBytes(with));
        assertThat(blocked.result).isEqualTo(NodeReport.BLOCKED);
        assertThat(ready.result).isEqualTo(NodeReport.READY);

        // node 2 of a three-node cluster takes the result and the INTERNODE_TLS finding of each run in turn
        Path reports = cluster(3);
        rewrite(reports.resolve("node2.json"), report -> {
            report.result = blocked.result;
            blocked.findings.stream().filter(f -> f.code.equals("INTERNODE_TLS")).forEach(report.findings::add);
        });
        Aggregate notReady = aggregate(reports);
        assertThat(notReady.exitCode).as(notReady.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(notReady.codes("BLOCKING")).containsExactly("NODE_BLOCKED");
        assertThat(notReady.messages("NODE_BLOCKED")).isEqualTo("nodes not ready: 10.0.0.2 [INTERNODE_TLS]");

        Path readyReports = cluster(3);
        rewrite(readyReports.resolve("node2.json"), report -> {
            report.result = ready.result;
            ready.findings.stream().filter(f -> f.code.equals("INTERNODE_TLS")).forEach(report.findings::add);
        });
        Aggregate readyCluster = aggregate(readyReports);
        assertThat(readyCluster.exitCode).as(readyCluster.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(readyCluster.codes("BLOCKING")).isEmpty();
    }

    /**
     * MIXED_STORAGE_PORTS: DSE nodes that differ in storage_port (absent: 7000, the default of both products; read as a
     * typed YAML loader reads it) block, with the value of each node, with or without internode encryption; a report
     * whose upgrade_path is null, or that could not read cassandra.yaml, is not compared.
     */
    @Test
    public void storagePortsMustAgree() throws IOException
    {
        Path reports = cluster(3);
        // the default written out on one node and left out on the others is no difference
        rewrite(reports.resolve("node1.json"), report -> report.upgradePath.settings.put("storage_port", "7000"));
        Aggregate agreeing = aggregate(reports);
        assertThat(agreeing.exitCode).as(agreeing.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(agreeing.codes("BLOCKING")).isEmpty();

        rewrite(reports.resolve("node3.json"), report -> report.upgradePath.settings.put("storage_port", "7010"));
        Aggregate differing = aggregate(reports);
        assertThat(differing.exitCode).as(differing.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(differing.codes("BLOCKING")).containsExactly("MIXED_STORAGE_PORTS");
        assertThat(differing.messages("MIXED_STORAGE_PORTS"))
        .isEqualTo("the DSE nodes differ in their storage_port: storage_port 7000 on 10.0.0.1, 10.0.0.2; storage_port 7010 on 10.0.0.3 " +
                   "(a report without storage_port counts as 7000: its cassandra.yaml sets none, and that is the default of both products)");
        assertThat(differing.findings().stream().filter(f -> "MIXED_STORAGE_PORTS".equals(f.get("code"))).map(f -> (String) f.get("remedy")))
        .containsExactly("make the cassandra.yaml of every DSE node set the storage_port DSE runs with, the same on every node, and check DSE's " +
                         "JVM options for a -Dcassandra.storage_port, which must be absent or give that same port, and, with internode encryption, " +
                         "for a -Dcassandra.ssl_storage_port, which must be absent or give the ssl_storage_port of the node's cassandra.yaml, the " +
                         "port HCD 2.0 carries (required: DSE honours both over cassandra.yaml, and this tool reads cassandra.yaml only; check " +
                         "them by hand: the command line of the running DSE process lists the -D options it was started with), and run the tool " +
                         "again on those nodes; then carry that storage_port into HCD 2.0 unchanged: DSE's pre-4.0 messages give their addresses " +
                         "without a port, the coordinator of a forwarded write included, and HCD 2.0 gives each of them its own storage_port");

        // a quoted 07000 (the report's loader keeps it a string) is octal for a typed loader, 3584: not 7000
        rewrite(reports.resolve("node3.json"), report -> report.upgradePath.settings.put("storage_port", "07000"));
        assertThat(aggregate(reports).messages("MIXED_STORAGE_PORTS"))
        .isEqualTo("the DSE nodes differ in their storage_port: storage_port 3584 on 10.0.0.3; storage_port 7000 on 10.0.0.1, 10.0.0.2 " +
                   "(a report without storage_port counts as 7000: its cassandra.yaml sets none, and that is the default of both products)");

        // a quoted value with spaces around it is no integer for a typed loader: shown as written
        rewrite(reports.resolve("node3.json"), report -> report.upgradePath.settings.put("storage_port", " 7000 "));
        assertThat(aggregate(reports).messages("MIXED_STORAGE_PORTS"))
        .isEqualTo("the DSE nodes differ in their storage_port: storage_port \" 7000 \" on 10.0.0.3; storage_port 7000 on 10.0.0.1, 10.0.0.2 " +
                   "(a report without storage_port counts as 7000: its cassandra.yaml sets none, and that is the default of both products)");

        // the same port other than the default everywhere, written in hexadecimal on one node, and with internode
        // encryption on every node
        for (String file : List.of("node1.json", "node2.json", "node3.json"))
            rewrite(reports.resolve(file), report -> {
                report.upgradePath.settings.put("server_encryption_options.internode_encryption", "all");
                report.upgradePath.settings.put("storage_port", "7010");
            });
        rewrite(reports.resolve("node2.json"), report -> report.upgradePath.settings.put("storage_port", "0x1B62"));
        Aggregate sameOtherPort = aggregate(reports);
        assertThat(sameOtherPort.exitCode).as(sameOtherPort.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(sameOtherPort.codes("BLOCKING")).isEmpty();
        assertThat(sameOtherPort.codes("WARNING")).doesNotContain("INTERNODE_TLS");

        // one report without the key, the others on 7010: the default differs, and the message says why it is 7000
        rewrite(reports.resolve("node1.json"), report -> report.upgradePath.settings.remove("storage_port"));
        Aggregate oneDefault = aggregate(reports);
        assertThat(oneDefault.exitCode).as(oneDefault.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(oneDefault.codes("BLOCKING")).containsExactly("MIXED_STORAGE_PORTS");
        assertThat(oneDefault.messages("MIXED_STORAGE_PORTS"))
        .isEqualTo("the DSE nodes differ in their storage_port: storage_port 7000 on 10.0.0.1; storage_port 7010 on 10.0.0.2, 10.0.0.3 " +
                   "(a report without storage_port counts as 7000: its cassandra.yaml sets none, and that is the default of both products)");

        // a value that reads as no integer differs from every port, and is shown as written
        rewrite(reports.resolve("node1.json"), report -> report.upgradePath.settings.put("storage_port", "seven"));
        assertThat(aggregate(reports).messages("MIXED_STORAGE_PORTS"))
        .isEqualTo("the DSE nodes differ in their storage_port: storage_port \"seven\" on 10.0.0.1; storage_port 7010 on 10.0.0.2, 10.0.0.3");

        // a report that could not read cassandra.yaml (blocked for that already, with the finding the tool writes) is not
        // compared: its missing storage_port is not 7000, nor its missing internode_encryption none
        Path inspected = temporaryFolder.newFolder().toPath().resolve("unread.json");
        DseNodeFixture unreadable = DseNodeFixture.create(temporaryFolder.newFolder().toPath());
        Files.delete(unreadable.conf.resolve(NodeLayout.CASSANDRA_YAML));
        List<Finding> unreadInput = DsePreflightTest.run(inspected, unreadable.args()).report.findings.stream()
                                                    .filter(f -> f.code.equals("UNREADABLE_INPUT") && f.message.startsWith("cannot read "))
                                                    .collect(Collectors.toList());
        assertThat(unreadInput).hasSize(1).allMatch(f -> f.message.startsWith("cannot read cassandra.yaml: ") && f.severity == Finding.Severity.BLOCKING);
        rewrite(reports.resolve("node1.json"), report -> {
            report.upgradePath.settings.remove("storage_port");
            report.upgradePath.settings.remove("server_encryption_options.internode_encryption");
            report.result = NodeReport.BLOCKED;
            report.findings.addAll(unreadInput);
        });
        Aggregate unread = aggregate(reports);
        assertThat(unread.exitCode).as(unread.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(unread.codes("BLOCKING")).containsExactly("NODE_BLOCKED");
        assertThat(unread.codes("WARNING")).doesNotContain("INTERNODE_TLS");

        // a report whose upgrade_path is null cannot be used: its ports are neither taken for the defaults nor left out
        rewrite(reports.resolve("node1.json"), report -> {
            report.findings.removeIf(f -> f.code.equals("UNREADABLE_INPUT"));
            report.result = NodeReport.READY;
        });
        editJson(reports.resolve("node1.json"), json -> json.put("upgrade_path", null));
        Aggregate withoutSettings = aggregate(reports);
        assertThat(withoutSettings.exitCode).as(withoutSettings.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(withoutSettings.messages("UNREADABLE_REPORT")).isEqualTo("reports that cannot be read: node1.json (without upgrade_path)");
    }

    @Test
    public void noGrantReadAnywhereWhileDseChecksPermissionsWarns() throws IOException
    {
        Path reports = cluster(2);
        for (String file : List.of("node1.json", "node2.json"))
            rewrite(reports.resolve(file), report -> {
                report.accessControls.authorizationEnabled = true;
                report.accessControls.rolePermissionsRows = 0;
            });
        Aggregate none = aggregate(reports);
        assertThat(none.exitCode).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(none.codes("WARNING")).containsExactly("NO_GRANTS_READ");

        rewrite(reports.resolve("node2.json"), report -> report.accessControls.rolePermissionsRows = 3);
        assertThat(aggregate(reports).codes("WARNING")).isEmpty();

        // permissions not checked, or not known to be: nothing to warn about
        for (Boolean authorization : new Boolean[]{ false, null })
        {
            for (String file : List.of("node1.json", "node2.json"))
                rewrite(reports.resolve(file), report -> {
                    report.accessControls.authorizationEnabled = authorization;
                    report.accessControls.rolePermissionsRows = 0;
                });
            assertThat(aggregate(reports).codes("WARNING")).as("authorization %s", authorization).isEmpty();
        }
    }

    @Test
    public void reportsOfAnotherBuildThanTheAggregatingOneBlock() throws IOException
    {
        Path reports = cluster(2);
        for (String file : List.of("node1.json", "node2.json"))
            rewrite(reports.resolve(file), report -> report.softwareVersion = "4.9.9");
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.messages("MIXED_TOOL_BUILDS")).contains("the reports were written by the release 4.9.9 of the tool, and this aggregate runs the release " +
                                                                     FBUtilities.getReleaseVersionString());
    }

    // ---- several data centers ----

    /**
     * Two data centers of three nodes, one rack per node: the cluster-wide checks hold across the data centers, and the
     * nodes are listed by data center and rack, in the JSON and in the text.
     */
    @Test
    public void nodesAreListedByDataCenterAndTheChecksSpanDataCenters() throws IOException
    {
        Path reports = twoDataCenters();
        Aggregate ready = aggregate(reports);
        assertThat(ready.exitCode).as(ready.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(ready.codes("BLOCKING")).isEmpty();
        assertThat(ready.codes("WARNING")).doesNotContain("TOPOLOGY_MISMATCH");
        assertThat(ready.json.get("data_centers"))
        .isEqualTo(Map.of("dc1", Map.of("r1", List.of("10.0.0.1"), "r2", List.of("10.0.0.2"), "r3", List.of("10.0.0.3")),
                          "dc2", Map.of("r1", List.of("10.0.0.4"), "r2", List.of("10.0.0.5"), "r3", List.of("10.0.0.6"))));
        assertThat(nodeLine(ready, "10.0.0.5")).containsEntry("data_center", "dc2").containsEntry("rack", "r2");
        assertThat(ready.out).contains("Reports read: 6, in 2 data centers\n" +
                                       "  Data center dc1:\n" +
                                       "    10.0.0.1 (node1.json), rack r1: READY, 0 blocking, 1 warnings\n" +
                                       "    10.0.0.2 (node2.json), rack r2: READY, 0 blocking, 1 warnings\n" +
                                       "    10.0.0.3 (node3.json), rack r3: READY, 0 blocking, 1 warnings\n" +
                                       "  Data center dc2:\n" +
                                       "    10.0.0.4 (node4.json), rack r1: READY, 0 blocking, 1 warnings\n");

        // a node of dc2 at another schema version, and the report of another node of dc2 missing: both block, as in one
        // data center
        rewrite(reports.resolve("node4.json"), report -> report.node.schemaVersion = "42be62c3-6cac-39ac-8412-42c8cbd3485c");
        Files.delete(reports.resolve("node6.json"));
        Aggregate blocked = aggregate(reports);
        assertThat(blocked.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(blocked.codes("BLOCKING")).containsExactlyInAnyOrder("SCHEMA_DISAGREEMENT", "MISSING_REPORT");
        assertThat(blocked.messages("MISSING_REPORT")).contains("10.0.0.6 (host id 00000000-0000-0000-0000-000000000006) listed by " +
                                                                "[10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4, 10.0.0.5]");
    }

    /** The line of the reports read counts the data centers, and the nodes whose nodes/local records none. */
    @Test
    public void theReportsReadLineCountsTheDataCenters() throws IOException
    {
        Path reports = cluster(3);
        assertThat(aggregate(reports).out).contains("Reports read: 3, none with a data center recorded\n");
        for (String file : List.of("node1.json", "node2.json", "node3.json"))
            rewrite(reports.resolve(file), report -> report.node.dataCenter = "dc1");
        assertThat(aggregate(reports).out).contains("Reports read: 3, in 1 data center\n");
        assertThat(aggregate(temporaryFolder.newFolder().toPath()).out).contains("Reports read: 0\n");
    }

    /**
     * TOPOLOGY_MISMATCH: a node recorded with more than one data center or rack, by its own nodes/local and the nodes/peers
     * of the others, a node without a report included (the records compared among themselves); a record without a data
     * center or rack is not compared on it, and a node whose nodes/local has none is listed as unknown.
     */
    @Test
    public void aNodeRecordedWithAnotherDataCenterOrRackWarns() throws IOException
    {
        Path reports = twoDataCenters();
        // node 5 recorded in dc1 by node 1, in rack r3 by node 2; node 4 recorded with no data center or rack by node 3
        rewrite(reports.resolve("node1.json"), report -> peer(report, "10.0.0.5").dataCenter = "dc1");
        rewrite(reports.resolve("node2.json"), report -> peer(report, "10.0.0.5").rack = "r3");
        rewrite(reports.resolve("node3.json"), report -> {
            peer(report, "10.0.0.4").dataCenter = null;
            peer(report, "10.0.0.4").rack = null;
        });
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).as(aggregate.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(aggregate.codes("WARNING")).containsExactly("TOPOLOGY_MISMATCH");
        assertThat(aggregate.messages("TOPOLOGY_MISMATCH"))
        .isEqualTo("the reports record nodes with more than one data center or rack: 10.0.0.5 (data center dc2, rack r2 in its " +
                   "nodes/local and in the nodes/peers of [10.0.0.3, 10.0.0.4, 10.0.0.6]; data center dc1, rack r2 in the nodes/peers " +
                   "of [10.0.0.1]; data center dc2, rack r3 in the nodes/peers of [10.0.0.2])");

        // a node without a report, recorded in two data centers by the others: the records are compared among themselves
        Files.delete(reports.resolve("node6.json"));
        rewrite(reports.resolve("node4.json"), report -> peer(report, "10.0.0.6").dataCenter = "dc1");
        Aggregate unreported = aggregate(reports);
        assertThat(unreported.messages("TOPOLOGY_MISMATCH"))
        .contains("10.0.0.6 (host id 00000000-0000-0000-0000-000000000006) (no report) (data center dc2, rack r3 in the " +
                  "nodes/peers of [10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.5]; data center dc1, rack r3 in the nodes/peers of [10.0.0.4])");

        // a node whose nodes/local records no data center or rack: listed as unknown, compared on the records of the others
        rewrite(reports.resolve("node5.json"), report -> {
            report.node.dataCenter = null;
            report.node.rack = null;
        });
        Aggregate unknown = aggregate(reports);
        assertThat(dataCenters(unknown)).containsEntry(ClusterAggregate.UNKNOWN, Map.of(ClusterAggregate.UNKNOWN, List.of("10.0.0.5")));
        assertThat(unknown.out).contains("  Data center (unknown):\n    10.0.0.5 (node5.json), rack (unknown): READY, 0 blocking, 1 warnings\n");
        assertThat(unknown.messages("TOPOLOGY_MISMATCH")).contains("10.0.0.5 (data center (unknown), rack (unknown) in its nodes/local; " +
                                                                   "data center dc2, rack r2 in the nodes/peers of [10.0.0.3, 10.0.0.4]; " +
                                                                   "data center dc1, rack r2 in the nodes/peers of [10.0.0.1]");
        assertThat(unknown.out).contains("Reports read: 5, in 2 data centers, 1 with no data center recorded\n");
    }

    /** A node reported twice is listed once in TOPOLOGY_MISMATCH, and a peer record whose host id has no report is matched by address. */
    @Test
    public void topologyMismatchOfADuplicateReportAndOfAPeerMatchedByAddress() throws IOException
    {
        Path reports = twoDataCenters();
        Files.copy(reports.resolve("node5.json"), reports.resolve("node5-again.json"));
        rewrite(reports.resolve("node1.json"), report -> peer(report, "10.0.0.5").dataCenter = "dc1");
        // node 2 records node 4 with a host id no report has, and another rack
        rewrite(reports.resolve("node2.json"), report -> {
            peer(report, "10.0.0.4").hostId = "00000000-0000-0000-0000-000000000099";
            peer(report, "10.0.0.4").rack = "r2";
        });
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.codes("BLOCKING")).contains("DUPLICATE_REPORT");
        String message = aggregate.messages("TOPOLOGY_MISMATCH");
        assertThat(message.split("10\\.0\\.0\\.5 \\(", -1)).as(message).hasSize(2);
        assertThat(message).contains("10.0.0.4 (data center dc2, rack r1 in its nodes/local and in the nodes/peers of [10.0.0.1, 10.0.0.3, " +
                                     "10.0.0.5, 10.0.0.6]; data center dc2, rack r2 in the nodes/peers of [10.0.0.2])");
    }

    /**
     * In a large cluster the largest group of nodes recording one data center and rack is counted, so that the message
     * names the node that differs.
     */
    @Test
    public void aLargeGroupOfRecordersIsCounted() throws IOException
    {
        int size = ClusterAggregate.NAMED_RECORDERS + 5;
        Path reports = sameTopology(size);
        // node 1's own nodes/local says dc2, every other node records it in dc1; node 2 also records node 3 in rack r2
        rewrite(reports.resolve("node1.json"), report -> report.node.dataCenter = "dc2");
        rewrite(reports.resolve("node2.json"), report -> peer(report, "10.0.0.3").rack = "r2");
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.messages("TOPOLOGY_MISMATCH"))
        .isEqualTo("the reports record nodes with more than one data center or rack: 10.0.0.1 (data center dc2, rack r1 in its " +
                   "nodes/local; data center dc1, rack r1 in the nodes/peers of " + (size - 1) + " nodes), 10.0.0.3 (data center dc1, " +
                   "rack r1 in its nodes/local and in the nodes/peers of " + (size - 2) + " nodes; data center dc1, rack r2 in the " +
                   "nodes/peers of [10.0.0.2])");
    }

    /**
     * A node without a report is one node whether a record names it by host id or, without one, by its address; and two
     * reports without a host id at the same address are a duplicate.
     */
    @Test
    public void aNodeIsOneNodeWhateverItsRecordsNameItBy() throws IOException
    {
        Path reports = twoDataCenters();
        Files.delete(reports.resolve("node6.json"));
        rewrite(reports.resolve("node4.json"), report -> peer(report, "10.0.0.6").dataCenter = "dc1");
        rewrite(reports.resolve("node3.json"), report -> {
            peer(report, "10.0.0.6").hostId = null;
            peer(report, "10.0.0.6").dataCenter = "dc3";
        });
        String message = aggregate(reports).messages("TOPOLOGY_MISMATCH");
        assertThat(message.split("10\\.0\\.0\\.6 \\(", -1)).as(message).hasSize(2);
        assertThat(message).contains("10.0.0.6 (host id 00000000-0000-0000-0000-000000000006) (no report) (data center dc2, rack r3 in " +
                                     "the nodes/peers of [10.0.0.1, 10.0.0.2, 10.0.0.5]; data center dc1, rack r3 in the nodes/peers " +
                                     "of [10.0.0.4]; data center dc3, rack r3 in the nodes/peers of [10.0.0.3])");

        rewrite(reports.resolve("node5.json"), ClusterAggregateTest::withoutHostId);
        Files.copy(reports.resolve("node5.json"), reports.resolve("node5-again.json"));
        Aggregate duplicate = aggregate(reports);
        assertThat(duplicate.messages("DUPLICATE_REPORT")).contains("address 10.0.0.5: [10.0.0.5 (node5-again.json), 10.0.0.5 (node5.json)]");
    }

    /** A group of exactly NAMED_RECORDERS nodes is named; one node more and it is counted. */
    @Test
    public void aGroupOfNamedRecordersSizeIsNamed() throws IOException
    {
        int size = ClusterAggregate.NAMED_RECORDERS + 2;
        Path reports = sameTopology(size);
        rewrite(reports.resolve("node1.json"), report -> report.node.dataCenter = "dc2");
        // node 1 is recorded in dc1 by the NAMED_RECORDERS + 1 other nodes: counted
        assertThat(aggregate(reports).messages("TOPOLOGY_MISMATCH")).contains("data center dc1, rack r1 in the nodes/peers of " + (size - 1) + " nodes");
        // one report fewer: NAMED_RECORDERS nodes record node 1 in dc1, named
        Files.delete(reports.resolve("node" + size + ".json"));
        String named = aggregate(reports).messages("TOPOLOGY_MISMATCH");
        assertThat(named).contains("data center dc1, rack r1 in the nodes/peers of [10.0.0.10, 10.0.0.11, 10.0.0.2, 10.0.0.3, " +
                                   "10.0.0.4, 10.0.0.5, 10.0.0.6, 10.0.0.7, 10.0.0.8, 10.0.0.9]");
    }

    /**
     * Two reports without a host id at different addresses are not a duplicate, whatever their listen addresses; one
     * without a host id at the broadcast address of a report with one is, and is left out of TOPOLOGY_MISMATCH.
     */
    @Test
    public void reportsWithoutAHostIdAreToldApartByAddress() throws IOException
    {
        Path reports = twoDataCenters();
        rewrite(reports.resolve("node1.json"), ClusterAggregateTest::withoutHostId);
        rewrite(reports.resolve("node2.json"), ClusterAggregateTest::withoutHostId);
        assertThat(aggregate(reports).codes("BLOCKING")).doesNotContain("DUPLICATE_REPORT");

        // the duplicate rule compares broadcast addresses only, in both checks: node 2's report is kept although node 4
        // listens on its address, and what node 2 records is compared
        rewrite(reports.resolve("node4.json"), report -> report.node.listenAddress = "10.0.0.2");
        rewrite(reports.resolve("node2.json"), report -> peer(report, "10.0.0.6").dataCenter = "dc1");
        Aggregate listening = aggregate(reports);
        assertThat(listening.codes("BLOCKING")).doesNotContain("DUPLICATE_REPORT");
        assertThat(listening.messages("TOPOLOGY_MISMATCH")).contains("data center dc1, rack r3 in the nodes/peers of [10.0.0.2]")
                                                           .doesNotContain("10.0.0.4 (");
        rewrite(reports.resolve("node4.json"), report -> report.node.listenAddress = "10.0.0.4");
        rewrite(reports.resolve("node2.json"), report -> peer(report, "10.0.0.6").dataCenter = "dc2");

        // a copy of node 3's report without its host id, which also records node 4 in another data center
        Path copy = reports.resolve("node3-copy.json");
        Files.copy(reports.resolve("node3.json"), copy);
        rewrite(copy, report -> {
            withoutHostId(report);
            peer(report, "10.0.0.4").dataCenter = "dc1";
        });
        Aggregate duplicate = aggregate(reports);
        assertThat(duplicate.messages("DUPLICATE_REPORT")).contains("address 10.0.0.3: [10.0.0.3 (node3-copy.json), 10.0.0.3 (node3.json)]");
        assertThat(duplicate.codes("WARNING")).doesNotContain("TOPOLOGY_MISMATCH");
    }

    /** A record with a data center and no rack is compared on its data center only, and one with a rack only on its rack. */
    @Test
    public void aRecordIsComparedOnWhatItHas() throws IOException
    {
        Path reports = twoDataCenters();
        rewrite(reports.resolve("node1.json"), report -> peer(report, "10.0.0.5").rack = null);
        rewrite(reports.resolve("node2.json"), report -> peer(report, "10.0.0.5").dataCenter = null);
        assertThat(aggregate(reports).codes("WARNING")).doesNotContain("TOPOLOGY_MISMATCH");

        rewrite(reports.resolve("node2.json"), report -> peer(report, "10.0.0.5").rack = "r3");
        assertThat(aggregate(reports).messages("TOPOLOGY_MISMATCH"))
        .isEqualTo("the reports record nodes with more than one data center or rack: 10.0.0.5 (data center dc2, rack r2 in its " +
                   "nodes/local and in the nodes/peers of [10.0.0.3, 10.0.0.4, 10.0.0.6]; data center (unknown), rack r3 in the " +
                   "nodes/peers of [10.0.0.2]; data center dc2, rack (unknown) in the nodes/peers of [10.0.0.1])");
    }

    /**
     * Three hundred reports, one cluster: the aggregate lists them all and stays READY; a node whose nodes/local is wrong is
     * named once with the others counted, and a node that records every other wrongly gives one entry per node, the list
     * cut after a hundred.
     */
    @Test
    public void threeHundredReports() throws IOException
    {
        int size = 300;
        Path reports = sameTopology(size);
        Aggregate ready = aggregate(reports);
        assertThat(ready.exitCode).as(ready.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(ready.json).containsEntry("reports_read", size);
        assertThat(ready.codes("WARNING")).doesNotContain("TOPOLOGY_MISMATCH");

        rewrite(reports.resolve("node1.json"), report -> report.node.rack = "r2");
        assertThat(aggregate(reports).messages("TOPOLOGY_MISMATCH"))
        .isEqualTo("the reports record nodes with more than one data center or rack: 10.0.0.1 (data center dc1, rack r2 in its " +
                   "nodes/local; data center dc1, rack r1 in the nodes/peers of " + (size - 1) + " nodes)");

        rewrite(reports.resolve("node1.json"), report -> {
            report.node.rack = "r1";
            report.peers.forEach(peer -> peer.dataCenter = "dc2");
        });
        String message = aggregate(reports).messages("TOPOLOGY_MISMATCH");
        assertThat(message).endsWith(" and " + (size - 1 - 100) + " more")
                           .contains("data center dc2, rack r1 in the nodes/peers of [10.0.0.1]")
                           .hasSizeLessThan(25_000);

        // half the nodes record the racks of the other half otherwise: one entry per node of the second half, each naming
        // ten of the recorders of the other rack, the list cut after a hundred
        Path halves = sameTopology(size);
        for (int i = 1; i <= size / 2; i++)
            rewrite(halves.resolve("node" + i + ".json"), report -> report.peers.stream()
                                                                         .filter(peer -> Integer.parseInt(peer.hostId.substring(24)) > size / 2)
                                                                         .forEach(peer -> peer.rack = "r2"));
        String halvesMessage = aggregate(halves).messages("TOPOLOGY_MISMATCH");
        assertThat(halvesMessage).endsWith(" and " + (size / 2 - 100) + " more")
                                 .contains("data center dc1, rack r1 in its nodes/local and in the nodes/peers of [10.0.0.152, ")
                                 .contains(" and " + (size / 2 - 1 - ClusterAggregate.NAMED_RECORDERS) + " more; data center dc1, rack r2 in the " +
                                           "nodes/peers of " + size / 2 + " nodes)")
                                 .hasSizeLessThan(60_000);
    }

    /**
     * A report whose nodes/local could not be read (no host id, no address) is blocked and listed under an unknown data
     * center when it records none, and two of them are two nodes: neither a duplicate nor merged in TOPOLOGY_MISMATCH,
     * where one node's own data center and rack would be compared with the other's.
     */
    @Test
    public void reportsWithoutHostIdOrAddress() throws IOException
    {
        Path reports = twoDataCenters();
        for (String file : List.of("node5.json", "node6.json"))
            rewrite(reports.resolve(file), report -> {
                report.result = NodeReport.BLOCKED;
                report.findings.add(Finding.blocking("UNREADABLE_INPUT", "cannot read nodes/local", "run the tool as the user of DSE"));
                report.node.hostId = null;
                report.node.broadcastAddress = null;
                report.node.listenAddress = null;
                report.node.dataCenter = null;
                report.node.rack = null;
            });
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).as(aggregate.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.codes("BLOCKING")).contains("NODE_BLOCKED").doesNotContain("DUPLICATE_REPORT");
        assertThat(dataCenters(aggregate)).containsEntry(ClusterAggregate.UNKNOWN, Map.of(ClusterAggregate.UNKNOWN, List.of("host5", "host6")));
        assertThat(aggregate.out).contains("Reports read: 6, in 2 data centers, 2 with no data center recorded\n");

        // nor, their nodes/local unread, their release, schema version and cluster name: not compared, NODE_BLOCKED names them
        for (String file : List.of("node5.json", "node6.json"))
            rewrite(reports.resolve(file), report -> {
                report.node.releaseVersion = null;
                report.node.schemaVersion = null;
                report.node.clusterName = null;
            });
        assertThat(aggregate(reports).codes("BLOCKING")).contains("NODE_BLOCKED").doesNotContain("MIXED_RELEASES", "SCHEMA_DISAGREEMENT", "CLUSTER_NAMES");

        // their own data center and racks differ: merged into one node, they would be a mismatch
        rewrite(reports.resolve("node5.json"), report -> {
            report.node.dataCenter = "dc2";
            report.node.rack = "r2";
        });
        rewrite(reports.resolve("node6.json"), report -> {
            report.node.dataCenter = "dc2";
            report.node.rack = "r3";
        });
        Aggregate separate = aggregate(reports);
        assertThat(separate.codes("WARNING")).doesNotContain("TOPOLOGY_MISMATCH");
    }

    /**
     * Two reports with different host ids at one broadcast address are of one node: a duplicate, the first file kept; and
     * a report that duplicates a left-out one leaves the comparison too, as DUPLICATE_REPORT names them.
     */
    @Test
    public void twoHostIdsAtOneAddressAreADuplicate() throws IOException
    {
        Path reports = twoDataCenters();
        Path stale = reports.resolve("node3-stale.json");
        Files.copy(reports.resolve("node3.json"), stale);
        rewrite(stale, report -> {
            report.node.hostId = "00000000-0000-0000-0000-000000000033";
            report.node.rack = "r1";
        });
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.messages("DUPLICATE_REPORT")).contains("address 10.0.0.3: [10.0.0.3 (node3-stale.json), 10.0.0.3 (node3.json)]");
        // node3-stale.json sorts first and is kept: its rack r1 against the r3 the others record
        assertThat(aggregate.messages("TOPOLOGY_MISMATCH")).contains("10.0.0.3 (data center dc1, rack r1 in its nodes/local; data center dc1, " +
                                                                     "rack r3 in the nodes/peers of [10.0.0.1, 10.0.0.2, 10.0.0.4, 10.0.0.5, 10.0.0.6])");

        // R1 (node1.json: host id 1 at 10.0.0.1), R2 (host id 1 at 10.0.0.9) and R3 (host id 9 at 10.0.0.9), in that order
        // of the file names: R1 is kept, R2 is left out by its host id and R3 by R2's address, although R2 was not kept, so
        // R3, which records node 4 in another rack, is not compared
        Files.delete(stale);
        Path r2 = reports.resolve("node1x-r2.json");
        Path r3 = reports.resolve("node1x-r3.json");
        Files.copy(reports.resolve("node1.json"), r2);
        Files.copy(reports.resolve("node1.json"), r3);
        rewrite(r2, report -> report.node.broadcastAddress = "10.0.0.9");
        rewrite(r3, report -> {
            report.node.broadcastAddress = "10.0.0.9";
            report.node.hostId = "00000000-0000-0000-0000-000000000009";
            peer(report, "10.0.0.4").rack = "r3";
        });
        // nodes 2 and 3 record R3's node (host id 9, whose only report is left out) in two racks, and nodes 5 and 6 a node of
        // host id 7 at the same address, with no report anywhere, in two racks too
        rewrite(reports.resolve("node2.json"), report -> report.peers.add(peer("10.0.0.9", "00000000-0000-0000-0000-000000000009", "r1")));
        rewrite(reports.resolve("node3.json"), report -> report.peers.add(peer("10.0.0.9", "00000000-0000-0000-0000-000000000009", "r2")));
        rewrite(reports.resolve("node5.json"), report -> report.peers.add(peer("10.0.0.9", "00000000-0000-0000-0000-000000000007", "r1")));
        rewrite(reports.resolve("node6.json"), report -> report.peers.add(peer("10.0.0.9", "00000000-0000-0000-0000-000000000007", "r2")));
        Aggregate transitive = aggregate(reports);
        assertThat(transitive.messages("DUPLICATE_REPORT")).contains("00000000-0000-0000-0000-000000000001: [10.0.0.1 (node1.json), " +
                                                                     "10.0.0.9 (node1x-r2.json)]")
                                                           .contains("address 10.0.0.9: [10.0.0.9 (node1x-r2.json), 10.0.0.9 (node1x-r3.json)]");
        // R3's record of node 4 in rack r3 is not compared: the only entries are the two nodes at 10.0.0.9
        assertThat(transitive.messages("TOPOLOGY_MISMATCH"))
        .isEqualTo("the reports record nodes with more than one data center or rack: 10.0.0.9 (host id 00000000-0000-0000-0000-000000000007) " +
                   "(no report) (data center dc1, rack r1 in the nodes/peers of [10.0.0.5]; data center dc1, rack r2 in the nodes/peers of " +
                   "[10.0.0.6]), 10.0.0.9 (host id 00000000-0000-0000-0000-000000000009) (its report left out as a duplicate) (data center " +
                   "dc1, rack r1 in the nodes/peers of [10.0.0.2]; data center dc1, rack r2 in the nodes/peers of [10.0.0.3])");
    }

    /** On a tie between the node's own group and another, both large, its own is the one counted. */
    @Test
    public void aTieGoesToTheNodesOwnDataCenterAndRack() throws IOException
    {
        int size = 2 * ClusterAggregate.NAMED_RECORDERS + 3;
        Path reports = sameTopology(size);
        // node 1 is in rack r3; eleven nodes record it so, eleven in rack r2, which sorts first
        rewrite(reports.resolve("node1.json"), report -> report.node.rack = "r3");
        for (int i = 2; i <= size; i++)
        {
            String rack = i <= ClusterAggregate.NAMED_RECORDERS + 2 ? "r3" : "r2";
            rewrite(reports.resolve("node" + i + ".json"), report -> peer(report, "10.0.0.1").rack = rack);
        }
        assertThat(aggregate(reports).messages("TOPOLOGY_MISMATCH"))
        .isEqualTo("the reports record nodes with more than one data center or rack: 10.0.0.1 (data center dc1, rack r3 in its " +
                   "nodes/local and in the nodes/peers of 11 nodes; data center dc1, rack r2 in the nodes/peers of [10.0.0.13, 10.0.0.14, " +
                   "10.0.0.15, 10.0.0.16, 10.0.0.17, 10.0.0.18, 10.0.0.19, 10.0.0.20, 10.0.0.21, 10.0.0.22] and 1 more)");
    }

    /** A node recorded with more data centers and racks than NAMED_RECORDERS lists that many, then a count. */
    @Test
    public void theDataCentersAndRacksOfANodeAreBounded() throws IOException
    {
        int size = ClusterAggregate.NAMED_RECORDERS + 5;
        Path reports = sameTopology(size);
        // every other node records node 1 in a rack of its own
        for (int i = 2; i <= size; i++)
        {
            int n = i;
            rewrite(reports.resolve("node" + i + ".json"), report -> peer(report, "10.0.0.1").rack = "q" + n);
        }
        // and two more nodes record it in rack q2, the largest group: listed second, before the cut
        for (int i = 3; i <= 4; i++)
            rewrite(reports.resolve("node" + i + ".json"), report -> peer(report, "10.0.0.1").rack = "q2");
        String message = aggregate(reports).messages("TOPOLOGY_MISMATCH");
        assertThat(message).startsWith("the reports record nodes with more than one data center or rack: 10.0.0.1 (data center dc1, rack r1 " +
                                       "in its nodes/local; data center dc1, rack q2 in the nodes/peers of [10.0.0.2, 10.0.0.3, 10.0.0.4]; ")
                           .endsWith("; and " + (size - 2 - ClusterAggregate.NAMED_RECORDERS) + " other data centers and racks)");
        assertThat(message.split("; ", -1)).hasSize(ClusterAggregate.NAMED_RECORDERS + 1);
    }

    /**
     * A report with an empty entry in its peers, findings or settings, or a finding without a code or severity, cannot be
     * used; the others are aggregated.
     */
    @Test
    public void reportsWithEmptyEntriesCannotBeUsed() throws IOException
    {
        Path reports = twoDataCenters();
        rewrite(reports.resolve("node2.json"), report -> report.peers.add(null));
        rewrite(reports.resolve("node3.json"), report -> report.findings.add(null));
        rewrite(reports.resolve("node4.json"), report -> report.findings.get(0).code = null);
        rewrite(reports.resolve("node5.json"), report -> report.findings.get(0).severity = null);
        rewrite(reports.resolve("node6.json"), report -> report.upgradePath.settings.put("ssl_storage_port", null));
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).as(aggregate.out).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.messages("UNREADABLE_REPORT")).contains("node2.json (an empty entry in peers, findings or upgrade_path.settings")
                                                          .contains("node3.json (an empty entry")
                                                          .contains("node4.json (an empty entry")
                                                          .contains("node5.json (an empty entry")
                                                          .contains("node6.json (an empty entry");
        assertThat(aggregate.json).containsEntry("reports_read", 1);
    }

    /**
     * Fail closed: a key the node inspection always writes, left out, is never read as its default (a report of this tool,
     * of a node, of this format, with empty sections); a report of another kind or format, or with another result, is
     * not taken either.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void incompleteOrForeignReportsCannotBeUsed() throws IOException
    {
        Map<String, String> messages = new TreeMap<>();
        for (String key : List.of("tool", "kind", "format_version", "software_version", "generated_at", "result", "node", "peers", "findings",
                                  "upgrade_path", "access_controls"))
        {
            Path reports = cluster(1);
            editJson(reports.resolve("node1.json"), json -> json.remove(key));
            Aggregate aggregate = aggregate(reports);
            assertThat(aggregate.exitCode).as(key).isEqualTo(DsePreflight.EXIT_BLOCKED);
            assertThat(aggregate.codes("BLOCKING")).as(key).containsExactlyInAnyOrder("UNREADABLE_REPORT", "NO_REPORT");
            messages.put(key, aggregate.messages("UNREADABLE_REPORT"));
        }
        assertThat(messages.get("tool")).endsWith("node1.json (not a report of dsepreflight)");
        assertThat(messages.get("kind")).endsWith("node1.json (no kind)");
        assertThat(messages.get("format_version")).endsWith("node1.json (no report format)");
        for (String key : List.of("software_version", "generated_at", "result", "node", "peers", "findings", "upgrade_path", "access_controls"))
            assertThat(messages.get(key)).as(key).endsWith("node1.json (without " + key + ')');

        // the smallest document that read as a ready report before every key was required
        Path minimal = temporaryFolder.newFolder().toPath();
        Files.write(minimal.resolve("node1.json"), ("{\"result\": \"READY\", \"software_version\": \"" + FBUtilities.getReleaseVersionString() + "\"}")
                                                   .getBytes(StandardCharsets.UTF_8));
        assertThat(aggregate(minimal).messages("UNREADABLE_REPORT")).endsWith("node1.json (not a report of dsepreflight)");

        Path reports = cluster(2);
        editJson(reports.resolve("node1.json"), json -> json.put("kind", "something"));
        editJson(reports.resolve("node2.json"), json -> json.put("format_version", 0));
        Aggregate others = aggregate(reports);
        assertThat(others.codes("BLOCKING")).containsExactlyInAnyOrder("UNREADABLE_REPORT", "NO_REPORT");
        assertThat(others.messages("UNREADABLE_REPORT")).contains("node1.json (kind \"something\", not a node report)")
                                                       .contains("node2.json (report format 0, not this tool's " + NodeReport.FORMAT_VERSION + ')');

        reports = cluster(4);
        editJson(reports.resolve("node1.json"), json -> json.put("result", null));
        editJson(reports.resolve("node2.json"), json -> json.put("result", "OK"));
        editJson(reports.resolve("node3.json"), json -> json.put("generated_at", "yesterday"));
        editJson(reports.resolve("node4.json"), json -> ((Map<String, Object>) json.get("upgrade_path")).remove("settings"));
        Aggregate results = aggregate(reports);
        assertThat(results.messages("UNREADABLE_REPORT")).contains("node1.json (without result)")
                                                        .contains("node2.json (result \"OK\", neither READY nor BLOCKED)")
                                                        .contains("node3.json (generated_at \"yesterday\" is not a time)")
                                                        .contains("node4.json (without upgrade_path.settings)");
        assertThat(results.json).containsEntry("reports_read", 0);

        // valid JSON, but not a node report: a value of the wrong type, a key a finding does not have, a format that is no
        // integer, a key written twice
        reports = cluster(4);
        editJson(reports.resolve("node1.json"), json -> json.put("peers", "x"));
        editJson(reports.resolve("node2.json"), json -> ((List<Map<String, Object>>) json.get("findings")).get(0).put("extra", 1));
        editJson(reports.resolve("node3.json"), json -> json.put("format_version", 1.0));
        String node4 = new String(Files.readAllBytes(reports.resolve("node4.json")), StandardCharsets.UTF_8);
        Files.write(reports.resolve("node4.json"), node4.replaceFirst("\\{", "{\"result\": \"READY\",").getBytes(StandardCharsets.UTF_8));
        Aggregate typed = aggregate(reports);
        assertThat(typed.messages("UNREADABLE_REPORT")).contains("node1.json (not a node report: ")
                                                      .contains("node2.json (not a node report: ")
                                                      .contains("node3.json (report format 1.0 is not an integer)")
                                                      .contains("node4.json (a key written twice: Duplicate field 'result'");
        assertThat(typed.json).containsEntry("reports_read", 0);
    }

    /** A ready report names its node fully; a blocked one may not, its nodes/local unread. */
    @Test
    public void aReadyReportMustNameItsNode() throws IOException
    {
        Path reports = cluster(5);
        rewrite(reports.resolve("node1.json"), report -> report.node.hostId = null);
        rewrite(reports.resolve("node2.json"), report -> {
            report.node.broadcastAddress = null;
            report.node.listenAddress = null;
        });
        rewrite(reports.resolve("node3.json"), report -> report.node.clusterName = null);
        rewrite(reports.resolve("node4.json"), report -> report.node.releaseVersion = null);
        rewrite(reports.resolve("node5.json"), report -> report.node.schemaVersion = null);
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        for (int i = 1; i <= 5; i++)
            assertThat(aggregate.messages("UNREADABLE_REPORT")).contains("node" + i + ".json (ready, but its node section lacks the host id, the " +
                                                                         "addresses, the cluster name, the release or the schema version)");

        // only the broadcast address missing: the listen address names the node
        reports = cluster(2);
        rewrite(reports.resolve("node1.json"), report -> report.node.broadcastAddress = null);
        Aggregate listen = aggregate(reports);
        assertThat(listen.exitCode).as(listen.out).isEqualTo(DsePreflight.EXIT_READY);
    }

    /** A ready result with a blocking finding, or a blocked one without, is a node that is not ready. */
    @Test
    public void theResultAndTheFindingsMustAgree() throws IOException
    {
        Path reports = cluster(3);
        rewrite(reports.resolve("node1.json"), report -> report.findings.add(Finding.blocking("SEARCH_INDEX", "DSE Search indexes: ks.t.i", "drop it")));
        rewrite(reports.resolve("node2.json"), report -> report.result = NodeReport.BLOCKED);
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(aggregate.messages("NODE_BLOCKED")).isEqualTo("nodes not ready: 10.0.0.1 [SEARCH_INDEX], 10.0.0.2 (BLOCKED, without a blocking finding)");
    }

    /**
     * Only the regular files named *.json of the directory are read: a subdirectory, a link (even to a report) and a file
     * of another name are not; a file too large for a report, or a JSON document that is no object, cannot be used.
     */
    @Test
    public void onlyTheRegularJsonFilesOfTheDirectoryAreRead() throws IOException
    {
        Path reports = cluster(2);
        Path outside = temporaryFolder.newFolder().toPath().resolve("node3.json");
        Files.copy(reports.resolve("node2.json"), outside);
        Files.createSymbolicLink(reports.resolve("link.json"), outside);
        Files.createDirectory(reports.resolve("sub.json"));
        Files.copy(reports.resolve("node2.json"), reports.resolve("sub.json").resolve("node2-again.json"));
        Files.write(reports.resolve("notes.txt"), "not a report".getBytes(StandardCharsets.UTF_8));
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.exitCode).as(aggregate.out).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(aggregate.json).containsEntry("reports_read", 2);
        assertThat(aggregate.messages("NOT_A_FILE_SKIPPED")).isEqualTo("entries of the directory named *.json that are not regular files " +
                                                                        "(directories, links), skipped: link.json, sub.json");

        Files.write(reports.resolve("null.json"), "null".getBytes(StandardCharsets.UTF_8));
        Files.write(reports.resolve("array.json"), "[1, 2]".getBytes(StandardCharsets.UTF_8));
        Files.write(reports.resolve("empty.json"), new byte[0]);
        try (RandomAccessFile big = new RandomAccessFile(reports.resolve("big.json").toFile(), "rw"))
        {
            big.setLength(ClusterAggregate.MAX_REPORT_BYTES + 1);
        }
        Aggregate unusable = aggregate(reports);
        assertThat(unusable.exitCode).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(unusable.messages("UNREADABLE_REPORT")).contains("null.json (not a JSON object)")
                                                         .contains("array.json (not a JSON object)")
                                                         .contains("empty.json (not a JSON object)")
                                                         .contains("big.json (" + (ClusterAggregate.MAX_REPORT_BYTES + 1) + " bytes, more than the " +
                                                                   ClusterAggregate.MAX_REPORT_BYTES + " a report may have)");
        assertThat(unusable.json).containsEntry("reports_read", 2);
    }

    /** A directory holding only aggregate reports holds no report. */
    @Test
    public void aDirectoryOfAggregateReportsOnlyHasNoReport() throws IOException
    {
        Path reports = cluster(1);
        aggregate(reports, reports.resolve("aggregate.json"));
        Files.delete(reports.resolve("node1.json"));
        Aggregate aggregate = aggregate(reports);
        assertThat(aggregate.codes("BLOCKING")).containsExactly("NO_REPORT");
        assertThat(aggregate.messages("AGGREGATE_REPORT_SKIPPED")).endsWith("skipped: aggregate.json");
    }

    /** A report written more than a day before the aggregate, or after it beyond the clock skew allowed, blocks. */
    @Test
    public void staleReportsBlock() throws IOException
    {
        Path reports = cluster(3);
        Instant now = FBUtilities.now();
        rewrite(reports.resolve("node1.json"), report -> report.generatedAt = now.minus(ClusterAggregate.MAX_REPORT_AGE).minusSeconds(1).toString());
        rewrite(reports.resolve("node2.json"), report -> report.generatedAt = now.plus(ClusterAggregate.MAX_CLOCK_SKEW).plusSeconds(1).toString());
        rewrite(reports.resolve("node3.json"), report -> report.generatedAt = now.minus(ClusterAggregate.MAX_REPORT_AGE).plusSeconds(1).toString());
        ClusterAggregate.AggregateReport aggregate = ClusterAggregate.aggregate(reports, FBUtilities.getReleaseVersionString(), now);
        assertThat(aggregate.result).isEqualTo(NodeReport.BLOCKED);
        assertThat(aggregate.reportsRead).isEqualTo(3);
        Finding stale = aggregate.findings.stream().filter(f -> f.code.equals("STALE_REPORT")).findFirst().orElseThrow(AssertionError::new);
        assertThat(stale.severity).isEqualTo(Finding.Severity.BLOCKING);
        assertThat(stale.message).startsWith("reports written more than 24 hours before this aggregate, or later than its time: node1.json (written at ")
                                 .contains("node2.json (written at ")
                                 .doesNotContain("node3.json")
                                 .endsWith("; this aggregate runs at " + now);
        assertThat(aggregate.findings).noneMatch(f -> f.severity == Finding.Severity.BLOCKING && !f.code.equals("STALE_REPORT"));
    }

    /** Without --json the aggregate is printed only; --json never replaces a file that is not an aggregate report. */
    @Test
    public void theJsonOptionWritesOnlyAnAggregateReport() throws IOException
    {
        Path reports = cluster(2);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int exitCode;
        try (PrintStream stream = new PrintStream(out, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(new String[]{ "--aggregate", reports.toString() }, stream, stream);
        }
        assertThat(exitCode).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(out.toString(StandardCharsets.UTF_8)).contains("Reports read: 2").contains("Result: READY");
        try (Stream<Path> files = Files.list(reports))
        {
            assertThat(files.map(f -> f.getFileName().toString())).containsExactlyInAnyOrder("node1.json", "node2.json");
        }

        byte[] node1 = Files.readAllBytes(reports.resolve("node1.json"));
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try (PrintStream stream = new PrintStream(err, true, StandardCharsets.UTF_8))
        {
            exitCode = DsePreflight.run(new String[]{ "--aggregate", reports.toString(), "--json", reports.resolve("node1.json").toString() }, stream, stream);
        }
        assertThat(exitCode).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(err.toString(StandardCharsets.UTF_8)).contains("--json names an existing file that is not an aggregate report of dsepreflight, " +
                                                                  "which it would replace: " + reports.resolve("node1.json"));
        assertThat(Files.readAllBytes(reports.resolve("node1.json"))).isEqualTo(node1);

        // an earlier aggregate report is replaced
        Path json = temporaryFolder.newFolder().toPath().resolve("aggregate.json");
        aggregate(reports, json);
        assertThat(aggregate(reports, json).exitCode).isEqualTo(DsePreflight.EXIT_READY);
    }

    // ---- helpers ----

    /** The reports of {@code size} nodes all in data center dc1 and rack r1, recording each other so. */
    private Path sameTopology(int size) throws IOException
    {
        Path reports = cluster(size);
        for (int i = 1; i <= size; i++)
            rewrite(reports.resolve("node" + i + ".json"), report -> {
                report.node.dataCenter = "dc1";
                report.node.rack = "r1";
                for (NodeReport.Peer peer : report.peers)
                {
                    peer.dataCenter = "dc1";
                    peer.rack = "r1";
                }
            });
        return reports;
    }

    /**
     * The reports of a ready cluster of two data centers: {@code dc1} with nodes 10.0.0.1 to 10.0.0.3, {@code dc2} with
     * 10.0.0.4 to 10.0.0.6, in racks r1 to r3 of each, every node recording the others with their data center and rack.
     */
    private Path twoDataCenters() throws IOException
    {
        Path reports = cluster(6);
        for (int i = 1; i <= 6; i++)
            rewrite(reports.resolve("node" + i + ".json"), report -> {
                report.node.dataCenter = dataCenter(report.node.broadcastAddress);
                report.node.rack = rack(report.node.broadcastAddress);
                for (NodeReport.Peer peer : report.peers)
                {
                    peer.dataCenter = dataCenter(peer.address);
                    peer.rack = rack(peer.address);
                }
            });
        return reports;
    }

    private static String dataCenter(String address)
    {
        return lastByte(address) <= 3 ? "dc1" : "dc2";
    }

    private static String rack(String address)
    {
        return "r" + ((lastByte(address) - 1) % 3 + 1);
    }

    private static int lastByte(String address)
    {
        return Integer.parseInt(address.substring(address.lastIndexOf('.') + 1));
    }

    /** A record of a node in data center dc1. */
    private static NodeReport.Peer peer(String address, String hostId, String rack)
    {
        NodeReport.Peer peer = new NodeReport.Peer();
        peer.address = address;
        peer.hostId = hostId;
        peer.dataCenter = "dc1";
        peer.rack = rack;
        peer.releaseVersion = RELEASE;
        peer.schemaVersion = SCHEMA;
        return peer;
    }

    private static NodeReport.Peer peer(NodeReport report, String address)
    {
        return report.peers.stream().filter(p -> address.equals(p.address)).findFirst().orElseThrow(AssertionError::new);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> dataCenters(Aggregate aggregate)
    {
        return (Map<String, Object>) aggregate.json.get("data_centers");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nodeLine(Aggregate aggregate, String node)
    {
        return ((List<Map<String, Object>>) aggregate.json.get("nodes")).stream()
                                                                        .filter(line -> node.equals(line.get("node")))
                                                                        .findFirst()
                                                                        .orElseThrow(AssertionError::new);
    }

    /** The reports of a ready cluster of {@code size} DSE nodes 10.0.0.1, 10.0.0.2, ..., each listing the others as DSE peers. */
    private Path cluster(int size) throws IOException
    {
        Path directory = temporaryFolder.newFolder().toPath();
        for (int i = 1; i <= size; i++)
        {
            NodeReport report = new NodeReport();
            report.result = NodeReport.READY;
            report.softwareVersion = FBUtilities.getReleaseVersionString();
            report.generatedAt = FBUtilities.now().toString();
            report.hostname = "host" + i;
            report.node.hostId = hostId(i);
            report.node.broadcastAddress = address(i);
            report.node.listenAddress = address(i);
            report.node.clusterName = "prod";
            report.node.releaseVersion = RELEASE;
            report.node.schemaVersion = SCHEMA;
            report.findings.add(Finding.warning("AUTH_CONFIG_CHANGE", "DSE security classes", "set them"));
            for (int j = 1; j <= size; j++)
            {
                if (j == i)
                    continue;
                NodeReport.Peer peer = new NodeReport.Peer();
                peer.address = address(j);
                peer.hostId = hostId(j);
                peer.releaseVersion = RELEASE;
                peer.schemaVersion = SCHEMA;
                report.peers.add(peer);
            }
            write(directory.resolve("node" + i + ".json"), report);
        }
        return directory;
    }

    /** The address of node {@code i}: 10.0.0.1 to 10.0.0.250, then 10.0.1.1 on. */
    private static String address(int i)
    {
        return i <= 250 ? "10.0.0." + i : "10.0.1." + (i - 250);
    }

    private static String hostId(int i)
    {
        return String.format("00000000-0000-0000-0000-%012d", i);
    }

    private static void write(Path file, NodeReport report) throws IOException
    {
        Files.write(file, JsonUtils.JSON_OBJECT_PRETTY_WRITER.writeValueAsBytes(report));
    }

    /**
     * The report of a node whose nodes/local records no host id: blocked, as a ready report must give one, and still
     * aggregated.
     */
    private static void withoutHostId(NodeReport report)
    {
        report.node.hostId = null;
        report.result = NodeReport.BLOCKED;
        report.findings.add(Finding.blocking("UNREADABLE_INPUT", "nodes/local records no host id", "restart DSE on this node"));
    }

    /** Edits a report as a JSON map, to write what the tool never writes. */
    private static void editJson(Path file, Consumer<Map<String, Object>> edit) throws IOException
    {
        Map<String, Object> json = JsonUtils.JSON_OBJECT_MAPPER.readValue(Files.readAllBytes(file), new TypeReference<Map<String, Object>>() {});
        edit.accept(json);
        Files.write(file, JsonUtils.JSON_OBJECT_PRETTY_WRITER.writeValueAsBytes(json));
    }

    private static void rewrite(Path file, Consumer<NodeReport> edit) throws IOException
    {
        NodeReport report = ClusterAggregate.READER.readValue(Files.readAllBytes(file));
        edit.accept(report);
        write(file, report);
    }
}
