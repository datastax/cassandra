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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.tools.dsepreflight.Finding.blocking;
import static org.apache.cassandra.tools.dsepreflight.Finding.info;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.list;

/**
 * The aggregate mode of the preflight inspection: reads the JSON reports of the nodes of a cluster (every
 * {@code *.json} file of one directory, as the operator collected them) and checks the cluster as a whole. It reads
 * nothing but the reports, so it runs anywhere, in time linear in the total size of the reports (each lists the peers
 * of its node), and it fails closed: a file of the directory that is not a complete node report of this tool, of this
 * report format, written by the release of the tool that aggregates, blocks the cluster, as does a report written more
 * than {@link #MAX_REPORT_AGE} before the aggregate. Aggregate reports in the directory are skipped, and so are the
 * entries that are not regular files (links are not followed). The cluster is ready when: every report reads and comes
 * from this tool; no report is blocked; every node runs the same DSE release, with one schema version and one cluster
 * name; no node is reported twice (one host id or one broadcast address); every node a report lists as a DSE peer has a
 * report of its own (a peer recorded with a release of this software is an upgraded node and needs none); and every
 * node has the same {@code storage_port} ({@code MIXED_STORAGE_PORTS}: DSE's pre-4.0 messages carry addresses without a
 * port, which HCD 2.0 completes with its own {@code storage_port}, so the port must be one and the same on every node).
 * It warns when the DSE nodes differ in their internode TLS settings ({@code internode_encryption},
 * {@code ssl_storage_port}), and when the reports record a node with more than one data center or rack, in its own
 * {@code nodes/local} and in the {@code nodes/peers} of the others ({@code TOPOLOGY_MISMATCH}). A node report whose
 * settings lack a port compares as the default of both products (its {@code cassandra.yaml} sets none), while one that
 * could not read {@code cassandra.yaml} is not compared on the internode settings. The tool reads
 * {@code cassandra.yaml} only, not a {@code -Dcassandra.storage_port} or {@code -Dcassandra.ssl_storage_port} in DSE's
 * JVM options, which DSE honours over the file (observed with DSE 6.8.65 and 6.9.26 for the first, read in DSE's code
 * for both): each must be absent or give the file's port (the second with internode encryption, as HCD 2.0 carries that
 * port), which the remedies of {@code MIXED_STORAGE_PORTS} and {@code INTERNODE_TLS}, and the node report's
 * {@code INTERNODE_TLS}, say. Every check is cluster-wide, whatever the number of data centers; the nodes are listed by
 * data center, with the rack of each (the JSON groups them by rack too).
 */
final class ClusterAggregate
{
    /** The data center or rack of a node whose nodes/local records none (or could not be read). */
    static final String UNKNOWN = "(unknown)";
    /**
     * In TOPOLOGY_MISMATCH, the largest group of nodes recording one data center and rack is counted when it has more than
     * this many nodes, and named otherwise.
     */
    static final int NAMED_RECORDERS = 10;

    /** The size above which a file is not taken for a node report: a report of a node with a thousand peers is far smaller. */
    static final long MAX_REPORT_BYTES = 16L * 1024 * 1024;
    /** How long before the aggregate a report may have been written: an older one may no longer describe its node. */
    static final Duration MAX_REPORT_AGE = Duration.ofHours(24);
    /** How far after the aggregate a report may have been written, for the clocks of the nodes. */
    static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(10);
    /** The keys every node report has, with a value that is not null. */
    private static final List<String> REQUIRED_KEYS = List.of("software_version", "generated_at", "result", "node", "peers", "findings",
                                                              "upgrade_path", "access_controls");

    static final ObjectReader READER = JsonUtils.JSON_OBJECT_MAPPER.readerFor(NodeReport.class);
    /** Reads a report as a tree, refusing a key written twice, of which the last would otherwise be taken. */
    private static final ObjectReader TREE_READER = JsonUtils.JSON_OBJECT_MAPPER.reader().with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    /** The aggregate report, written as JSON with {@code --json}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class AggregateReport
    {
        @JsonProperty("tool")
        public String tool = NodeReport.TOOL;
        @JsonProperty("format_version")
        public int formatVersion = NodeReport.FORMAT_VERSION;
        @JsonProperty("kind")
        public String kind = NodeReport.AGGREGATE;
        @JsonProperty("software_version")
        public String softwareVersion;
        @JsonProperty("generated_at")
        public String generatedAt;
        /** {@link NodeReport#READY} or {@link NodeReport#BLOCKED} */
        @JsonProperty("result")
        public String result;
        @JsonProperty("reports_read")
        public int reportsRead;
        @JsonProperty("findings")
        public List<Finding> findings = new ArrayList<>();
        /** one line per node report: file, node, data center, rack, result, counts */
        @JsonProperty("nodes")
        public List<Map<String, Object>> nodes = new ArrayList<>();
        /** the nodes of the reports by data center, then by rack, as each node's {@code nodes/local} records them */
        @JsonProperty("data_centers")
        public Map<String, Map<String, List<String>>> dataCenters = new TreeMap<>();
        /** the warnings of the node reports, by code, with the nodes that have them */
        @JsonProperty("warnings_by_code")
        public Map<String, List<String>> warningsByCode = new TreeMap<>();
    }

    private ClusterAggregate()
    {
    }

    static AggregateReport aggregate(Path directory) throws IOException
    {
        return aggregate(directory, FBUtilities.getReleaseVersionString(), FBUtilities.now());
    }

    /**
     * @param build the release of the build that aggregates, which the releases of the builds that wrote the reports
     *              must match
     * @param now   the time of the aggregate, which every report must have been written close to
     */
    static AggregateReport aggregate(Path directory, String build, Instant now) throws IOException
    {
        AggregateReport aggregate = new AggregateReport();
        Map<Path, NodeReport> reports = new TreeMap<>();
        List<String> unreadable = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<String> skippedPaxos = new ArrayList<>();
        List<String> notFiles = new ArrayList<>();
        List<String> stale = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, "*.json"))
        {
            for (Path file : files)
            {
                String name = file.getFileName().toString();
                try
                {
                    // a link is not followed, so that nothing outside the directory is read
                    BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (!attributes.isRegularFile())
                    {
                        notFiles.add(name);
                        continue;
                    }
                    if (attributes.size() > MAX_REPORT_BYTES)
                    {
                        unreadable.add(name + " (" + attributes.size() + " bytes, more than the " + MAX_REPORT_BYTES + " a report may have)");
                        continue;
                    }
                    JsonNode tree = TREE_READER.readTree(Files.readAllBytes(file));
                    if (tree != null && tree.isObject() && NodeReport.TOOL.equals(tree.path("tool").textValue()) &&
                        NodeReport.AGGREGATE.equals(tree.path("kind").textValue()))
                    {
                        skipped.add(name);
                        continue;
                    }
                    if (tree != null && tree.isObject() && NodeReport.TOOL.equals(tree.path("tool").textValue()) &&
                        PaxosRollbackReport.KIND.equals(tree.path("kind").textValue()))
                    {
                        skippedPaxos.add(name);
                        continue;
                    }
                    String problem = problem(tree);
                    NodeReport report = null;
                    if (problem == null)
                    {
                        try
                        {
                            report = READER.readValue(tree);
                            problem = problem(report);
                        }
                        catch (JsonProcessingException e)
                        {
                            // valid JSON, but a value of the wrong type, or a key a section does not have
                            problem = "not a node report: " + e.getOriginalMessage();
                        }
                    }
                    if (problem != null)
                    {
                        unreadable.add(name + " (" + problem + ')');
                        continue;
                    }
                    Instant generatedAt = Instant.parse(report.generatedAt);
                    if (generatedAt.isBefore(now.minus(MAX_REPORT_AGE)) || generatedAt.isAfter(now.plus(MAX_CLOCK_SKEW)))
                        stale.add(name + " (written at " + report.generatedAt + ')');
                    reports.put(file, report);
                }
                catch (JsonProcessingException e)
                {
                    JsonLocation location = e.getLocation();
                    // the strict reader also refuses a key written twice, in a document that is JSON otherwise
                    String what = e.getOriginalMessage() != null && e.getOriginalMessage().startsWith("Duplicate field") ? "a key written twice" : "not JSON";
                    unreadable.add(name + " (" + what + ": " + e.getOriginalMessage() +
                                   (location == null ? "" : ", at line " + location.getLineNr() + ", column " + location.getColumnNr()) + ')');
                }
                catch (IOException | RuntimeException e)
                {
                    unreadable.add(name + " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ')');
                }
            }
        }
        aggregate.reportsRead = reports.size();
        List<Finding> findings = aggregate.findings;
        if (!unreadable.isEmpty())
            findings.add(blocking("UNREADABLE_REPORT", "reports that cannot be read: " + list(unreadable),
                                  "run the tool again on those nodes with --json, with the same release of HCD 2.0, and remove from the " +
                                  "directory any file that is not a node report of this tool"));
        if (reports.isEmpty())
            findings.add(blocking("NO_REPORT", "no report in " + directory, "collect the --json report of every DSE node into one directory"));
        if (!stale.isEmpty())
            findings.add(blocking("STALE_REPORT", "reports written more than " + MAX_REPORT_AGE.toHours() + " hours before this aggregate, or " +
                                                  "later than its time: " + list(stale) + "; this aggregate runs at " + now,
                                  "run the tool again on those nodes, then aggregate the new reports at once (check the clocks of the " +
                                  "nodes if a report was written after the aggregate)"));
        if (!notFiles.isEmpty())
            findings.add(info("NOT_A_FILE_SKIPPED", "entries of the directory named *.json that are not regular files (directories, links), " +
                                                    "skipped: " + list(notFiles)));
        if (!skipped.isEmpty())
            findings.add(info("AGGREGATE_REPORT_SKIPPED", "aggregate reports in the directory, skipped: " + list(skipped)));
        if (!skippedPaxos.isEmpty())
            findings.add(info("PAXOS_ROLLBACK_REPORT_SKIPPED", "Paxos rollback reports (--paxos-rollback) in the directory, skipped: " + list(skippedPaxos)));

        Map<String, List<String>> releases = new TreeMap<>();
        Map<String, List<String>> schemaVersions = new TreeMap<>();
        Map<String, List<String>> clusterNames = new TreeMap<>();
        Map<String, List<String>> builds = new TreeMap<>();
        Map<String, List<String>> internodeTls = new TreeMap<>();
        Map<String, List<String>> storagePorts = new TreeMap<>();
        boolean storagePortDefaulted = false;
        boolean authorizationOn = false;
        long rolePermissionsRows = 0;
        Map<String, List<String>> hostIds = new TreeMap<>();
        // the reports by broadcast address, and the host ids they give ("" for none): one address is one node
        Map<String, List<String>> byBroadcastAddress = new TreeMap<>();
        Map<String, Set<String>> hostIdsByAddress = new TreeMap<>();
        Set<String> reportedHosts = new TreeSet<>();
        Set<String> reportedAddresses = new TreeSet<>();
        List<String> blockedNodes = new ArrayList<>();
        for (Map.Entry<Path, NodeReport> entry : reports.entrySet())
        {
            NodeReport report = entry.getValue();
            String node = describe(entry.getKey(), report);
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("file", entry.getKey().getFileName().toString());
            line.put("node", node);
            line.put("data_center", dataCenterOf(report));
            line.put("rack", rackOf(report));
            line.put("result", report.result);
            line.put("blocking", report.count(Finding.Severity.BLOCKING));
            line.put("warnings", report.count(Finding.Severity.WARNING));
            aggregate.nodes.add(line);
            aggregate.dataCenters.computeIfAbsent(dataCenterOf(report), dc -> new TreeMap<>())
                                 .computeIfAbsent(rackOf(report), rack -> new ArrayList<>())
                                 .add(node);
            if (report.blocked() || !NodeReport.READY.equals(report.result))
            {
                Set<String> codes = new TreeSet<>();
                report.findings.stream().filter(f -> f.severity == Finding.Severity.BLOCKING).forEach(f -> codes.add(f.code));
                blockedNodes.add(node + (codes.isEmpty() ? " (" + report.result + ", without a blocking finding)" : " " + codes));
            }
            for (Finding finding : report.findings)
                if (finding.severity == Finding.Severity.WARNING)
                    aggregate.warningsByCode.computeIfAbsent(finding.code, c -> new ArrayList<>()).add(node);
            // a blocked report may name none of them (its nodes/local unread): NODE_BLOCKED names it already
            if (report.node.releaseVersion != null)
                releases.computeIfAbsent(report.node.releaseVersion, r -> new ArrayList<>()).add(node);
            if (report.node.schemaVersion != null)
                schemaVersions.computeIfAbsent(report.node.schemaVersion, s -> new ArrayList<>()).add(node);
            if (report.node.clusterName != null)
                clusterNames.computeIfAbsent(report.node.clusterName, c -> new ArrayList<>()).add(node);
            builds.computeIfAbsent(String.valueOf(report.softwareVersion), b -> new ArrayList<>()).add(node);
            // a node whose cassandra.yaml could not be read is blocked for that already (NODE_BLOCKED): its settings say
            // nothing of the internode settings DSE runs with
            if (!cassandraYamlUnread(report))
            {
                internodeTls.computeIfAbsent(internodeTls(report.upgradePath.settings), t -> new ArrayList<>()).add(node);
                String storagePort = report.upgradePath.settings.get("storage_port");
                storagePortDefaulted |= storagePort == null;
                storagePorts.computeIfAbsent("storage_port " + (storagePort == null ? NodeInspector.DEFAULT_STORAGE_PORT : portNumber(storagePort)),
                                             p -> new ArrayList<>()).add(node);
            }
            if (Boolean.TRUE.equals(report.accessControls.authorizationEnabled))
                authorizationOn = true;
            if (report.accessControls.rolePermissionsRows != null)
                rolePermissionsRows += report.accessControls.rolePermissionsRows;
            // the reports of one node, named with their files for DUPLICATE_REPORT
            String nodeAndFile = node + " (" + entry.getKey().getFileName() + ')';
            if (report.node.hostId != null)
            {
                hostIds.computeIfAbsent(report.node.hostId, h -> new ArrayList<>()).add(nodeAndFile);
                reportedHosts.add(report.node.hostId);
            }
            if (report.node.broadcastAddress != null)
            {
                byBroadcastAddress.computeIfAbsent(report.node.broadcastAddress, a -> new ArrayList<>()).add(nodeAndFile);
                hostIdsByAddress.computeIfAbsent(report.node.broadcastAddress, a -> new TreeSet<>())
                                .add(report.node.hostId == null ? "" : report.node.hostId);
                reportedAddresses.add(report.node.broadcastAddress);
            }
            if (report.node.listenAddress != null)
                reportedAddresses.add(report.node.listenAddress);
        }
        aggregate.warningsByCode.values().forEach(nodes -> nodes.sort(null));
        aggregate.dataCenters.values().forEach(racks -> racks.values().forEach(nodes -> nodes.sort(null)));

        if (!blockedNodes.isEmpty())
            findings.add(blocking("NODE_BLOCKED", "nodes not ready: " + list(blockedNodes),
                                  "resolve the blocking findings of each node report, then run the tool on those nodes again"));
        if (releases.size() > 1)
            findings.add(blocking("MIXED_RELEASES", "the nodes run different DSE releases: " + groups(releases),
                                  "bring every DSE node to the same release (6.9 preferred) before the upgrade"));
        if (schemaVersions.size() > 1)
            findings.add(blocking("SCHEMA_DISAGREEMENT", "the nodes record different schema versions in nodes/local: " + groups(schemaVersions),
                                  "wait for schema agreement (nodetool describecluster shows one version), then run the tool on every node again"));
        if (authorizationOn && rolePermissionsRows == 0)
            findings.add(Finding.warning("NO_GRANTS_READ",
                                         "DSE checks permissions, but no report read a row of system_auth.role_permissions: the checks of the DSE access " +
                                         "controls (RESTRICT, row-level access control, lone grants) had nothing to check on any node",
                                         "check that every node was inspected with all its data directories, after nodetool flush system_auth"));
        if (builds.size() > 1)
            findings.add(blocking("MIXED_TOOL_BUILDS", "the reports come from different releases of the tool, whose rules may differ: " + groups(builds) +
                                                       (builds.containsKey(build) ? "" : "; this aggregate runs the release " + build),
                                  "run the tool of the HCD 2.0 release to be installed on every node again, and aggregate with that release"));
        else if (builds.size() == 1 && !builds.containsKey(build))
            findings.add(blocking("MIXED_TOOL_BUILDS", "the reports were written by the release " + builds.keySet().iterator().next() + " of the tool, and this " +
                                                       "aggregate runs the release " + build + ", whose rules may differ",
                                  "aggregate with the release that wrote the reports, or run the tool of this release on every node again"));
        if (internodeTls.size() > 1)
            findings.add(Finding.warning("INTERNODE_TLS", "the DSE nodes differ in their internode TLS settings: " + groups(internodeTls),
                                         "give every DSE node the same internode_encryption and ssl_storage_port before the upgrade: for the " +
                                         "connections they encrypt, HCD 2.0 dials its own ssl_storage_port on every DSE node below 6.9.7, and on " +
                                         "one of unknown version when legacy_ssl_storage_port_enabled is on; DSE dials its own on the upgraded " +
                                         "nodes (read in DSE's code for 6.8.65, 6.9.0 to 6.9.6, the port choice of 6.9.7, and 6.9.26; the other " +
                                         "releases inferred from them): DSE 6.8 and 6.9 before 6.9.7 always, DSE 6.9.7+ once it has stored a " +
                                         "messaging version below DSE 6.9.7's for them (12 from gossip, 11 from a handshake); and " + NodeInspector.CHECK_SSL_STORAGE_PORT_OPTION));
        if (storagePorts.size() > 1)
            findings.add(blocking("MIXED_STORAGE_PORTS", "the DSE nodes differ in their storage_port: " + groups(storagePorts) +
                                                         (storagePortDefaulted ? " (a report without storage_port counts as " + NodeInspector.DEFAULT_STORAGE_PORT + ": its " +
                                                                                 "cassandra.yaml sets none, and that is the default of both products)" : ""),
                                  "make the cassandra.yaml of every DSE node set the storage_port DSE runs with, the same on every node, and check " +
                                  "DSE's JVM options for a -Dcassandra.storage_port, which must be absent or give that same port, and, with " +
                                  "internode encryption, for a -Dcassandra.ssl_storage_port, which must be absent or give the ssl_storage_port of " +
                                  "the node's cassandra.yaml, the port HCD 2.0 carries (required: DSE honours both over cassandra.yaml, and this " +
                                  "tool reads cassandra.yaml only; check them by hand: the command line of the running DSE process lists the -D " +
                                  "options it was started with), and run the tool again on those nodes; then carry that storage_port into HCD 2.0 " +
                                  "unchanged: DSE's pre-4.0 messages give their addresses without a port, the coordinator of a forwarded write " +
                                  "included, and HCD 2.0 gives each of them its own storage_port"));
        if (clusterNames.size() > 1)
            findings.add(blocking("CLUSTER_NAMES", "the reports come from more than one cluster: " + groups(clusterNames),
                                  "aggregate the reports of one cluster at a time"));
        List<String> duplicates = new ArrayList<>();
        hostIds.forEach((hostId, nodes) -> {
            if (nodes.size() > 1)
                duplicates.add(hostId + ": " + nodes);
        });
        // two reports of one broadcast address are of one node, unless they are already named above (one host id)
        byBroadcastAddress.forEach((address, nodes) -> {
            Set<String> ids = hostIdsByAddress.get(address);
            if (nodes.size() > 1 && !(ids.size() == 1 && !ids.contains("")))
                duplicates.add("address " + address + ": " + nodes);
        });
        if (!duplicates.isEmpty())
            findings.add(blocking("DUPLICATE_REPORT", "more than one report for the same node (host id or broadcast address): " + list(duplicates),
                                  "keep the latest report of each node"));

        List<String> mismatches = topologyMismatches(reports);
        if (!mismatches.isEmpty())
            findings.add(Finding.warning("TOPOLOGY_MISMATCH", "the reports record nodes with more than one data center or rack: " + list(mismatches),
                                         "a node should be recorded with the same data center and rack everywhere: check its snitch settings and " +
                                         "nodetool status on the nodes that record it otherwise, then run the tool again on those nodes (with a " +
                                         "snitch that learns its peers' data center and rack from gossip, such as GossipingPropertyFileSnitch, an " +
                                         "upgraded node takes them from DSE's nodes/peers until gossip gives them)"));

        Map<String, Set<String>> missing = new TreeMap<>();
        Map<String, Set<String>> upgraded = new TreeMap<>();
        for (Map.Entry<Path, NodeReport> entry : reports.entrySet())
        {
            String node = describe(entry.getKey(), entry.getValue());
            for (NodeReport.Peer peer : entry.getValue().peers)
            {
                String key = (peer.address == null ? "(no address)" : peer.address) + (peer.hostId == null ? "" : " (host id " + peer.hostId + ')');
                boolean reported = (peer.hostId != null && reportedHosts.contains(peer.hostId)) ||
                                   (peer.hostId == null && peer.address != null && reportedAddresses.contains(peer.address));
                if (reported)
                    continue;
                if (peer.upgraded)
                    upgraded.computeIfAbsent(key, k -> new TreeSet<>()).add(node);
                else
                    missing.computeIfAbsent(key, k -> new TreeSet<>()).add(node);
            }
        }
        if (!missing.isEmpty())
        {
            List<String> items = new ArrayList<>();
            missing.forEach((peer, by) -> items.add(peer + " listed by " + by));
            findings.add(blocking("MISSING_REPORT", "DSE nodes without a report: " + list(items),
                                  "run the tool on each of them and add its report; an entry of a node that has left the cluster is stale: restart DSE " +
                                  "on the nodes listing it to refresh their nodes/peers"));
        }
        if (!upgraded.isEmpty())
            findings.add(info("UPGRADED_PEERS", "nodes already running HCD 2.0 (no report needed): " + list(upgraded.keySet())));

        findings.sort(Comparator.comparing(f -> f.severity));
        aggregate.result = findings.stream().anyMatch(f -> f.severity == Finding.Severity.BLOCKING) ? NodeReport.BLOCKED : NodeReport.READY;
        return aggregate;
    }

    /**
     * Why a JSON document is not a node report of this tool and format, before it is read as one; null when it is one.
     * Every key the node inspection writes is required, so that a key left out is never read as its default.
     */
    private static String problem(JsonNode tree)
    {
        if (tree == null || !tree.isObject())
            return "not a JSON object";
        if (!NodeReport.TOOL.equals(tree.path("tool").textValue()))
            return "not a report of " + NodeReport.TOOL;
        JsonNode kind = tree.get("kind");
        if (!NodeReport.NODE.equals(tree.path("kind").textValue()))
            return kind == null ? "no kind" : "kind " + kind + ", not a node report";
        JsonNode format = tree.get("format_version");
        if (format == null || format.isNull())
            return "no report format";
        if (!format.isInt())
            return "report format " + format + " is not an integer";
        if (format.intValue() > NodeReport.FORMAT_VERSION)
            return "report format " + format.intValue() + ", newer than this tool's " + NodeReport.FORMAT_VERSION;
        if (format.intValue() != NodeReport.FORMAT_VERSION)
            return "report format " + format.intValue() + ", not this tool's " + NodeReport.FORMAT_VERSION;
        List<String> missing = new ArrayList<>();
        for (String key : REQUIRED_KEYS)
            if (!tree.hasNonNull(key))
                missing.add(key);
        if (tree.path("upgrade_path").isObject() && !tree.path("upgrade_path").hasNonNull("settings"))
            missing.add("upgrade_path.settings");
        if (!missing.isEmpty())
            return "without " + String.join(", ", missing);
        JsonNode result = tree.get("result");
        if (!NodeReport.READY.equals(result.textValue()) && !NodeReport.BLOCKED.equals(result.textValue()))
            return "result " + result + ", neither " + NodeReport.READY + " nor " + NodeReport.BLOCKED;
        JsonNode generatedAt = tree.get("generated_at");
        try
        {
            if (!generatedAt.isTextual())
                return "generated_at " + generatedAt + " is not a time";
            Instant.parse(generatedAt.textValue());
        }
        catch (DateTimeParseException e)
        {
            return "generated_at " + generatedAt + " is not a time";
        }
        return null;
    }

    /**
     * Why a node report cannot be aggregated; null when it can. A ready report must name its node fully, as the node
     * inspection of a node whose {@code nodes/local} it read does: host id, an address, cluster name, release and schema
     * version.
     */
    private static String problem(NodeReport report)
    {
        if (report.peers.contains(null) || report.findings.stream().anyMatch(f -> f == null || f.code == null || f.severity == null)
            || report.upgradePath.settings.containsValue(null))
            return "an empty entry in peers, findings or upgrade_path.settings, or a finding without a code or severity";
        NodeReport.Node node = report.node;
        if (NodeReport.READY.equals(report.result) &&
            (node.hostId == null || (node.broadcastAddress == null && node.listenAddress == null) || node.clusterName == null ||
             node.releaseVersion == null || node.schemaVersion == null))
            return "ready, but its node section lacks the host id, the addresses, the cluster name, the release or the schema version";
        return null;
    }

    /**
     * The internode TLS settings of a node report that must be equal on every DSE node, as one value to group by:
     * {@code internode_encryption} (lower case) and, unless it is {@code none}, {@code ssl_storage_port}, which nothing
     * uses without encryption.
     */
    private static String internodeTls(Map<String, String> settings)
    {
        String encryption = settings.getOrDefault("server_encryption_options.internode_encryption", "none").trim().toLowerCase(Locale.ROOT);
        if (encryption.equals("none"))
            return "internode_encryption none";
        return "internode_encryption " + encryption + ", ssl_storage_port " + port(settings.getOrDefault("ssl_storage_port", NodeInspector.DEFAULT_SSL_STORAGE_PORT));
    }

    /** A port of a node report's settings, without the note the node report adds to a default port. */
    private static String port(String setting)
    {
        String port = setting.trim();
        int note = port.indexOf(' ');
        return note < 0 ? port : port.substring(0, note);
    }

    /**
     * A {@code storage_port} of a node report's settings (as DSE's {@code cassandra.yaml} gives it, with no note) as
     * the number a typed YAML loader, HCD 2.0's and DSE's (SnakeYAML's integer construct, 2.0 in DSE and 2.4 here, the
     * same code, which DSE applies to a quoted value too, as observed with DSE 6.8.65 and 6.9.26 and read in the
     * SnakeYAML classes DSE ships), reads into an {@code int} setting: an unquoted {@code 07000} is octal, 3584, and
     * {@code 0x1B62} hexadecimal, 7010 (the loader of the node report already read an unquoted one so), and a quoted
     * one is read the same way. A value that this tool does not read as an integer, a quoted one with spaces around it
     * included, is given as written, in double quotes, and so differs from every port. Known differences: this tool
     * lets YAML 1.1's integer pattern decide whether the integer construct runs, while DSE's typed loader runs it on
     * any value of an {@code int} setting, so a value the construct reads but the pattern rejects (a leading
     * underscore, {@code _7000}, or a base-60 group above 59, {@code 1:6940}) is an integer for DSE and not here; this
     * can only make MIXED_STORAGE_PORTS block a cluster it need not, never miss a difference (values DSE refuses at
     * start, such as a quoted one carrying a YAML comment, tag or anchor, which this tool reads as a YAML document,
     * aside).
     */
    private static String portNumber(String setting)
    {
        if (setting.equals(setting.trim()))
        {
            try
            {
                Object value = new Yaml(new SafeConstructor(new LoaderOptions())).load(setting);
                if (value instanceof Integer)
                    return value.toString();
            }
            catch (RuntimeException e)
            {
                // not YAML: shown as written
            }
        }
        return '"' + setting + '"';
    }

    /**
     * Whether the node report says that DSE's {@code cassandra.yaml} could not be read (its blocking
     * {@code UNREADABLE_INPUT}), so that its settings do not give the internode settings DSE runs with.
     */
    private static boolean cassandraYamlUnread(NodeReport report)
    {
        String prefix = NodeInspector.cannotRead(NodeLayout.CASSANDRA_YAML);
        return report.findings.stream().anyMatch(f -> "UNREADABLE_INPUT".equals(f.code) && f.severity == Finding.Severity.BLOCKING &&
                                                      f.message != null && f.message.startsWith(prefix));
    }

    /**
     * The nodes the reports record with more than one data center or more than one rack: in the node's own
     * {@code nodes/local}, when it has a report, and in the {@code nodes/peers} of the others, also for a node without a
     * report. For each, every data center and rack recorded, with where.
     * <p>
     * A peer record names its node by host id, or by address when it has no host id or names one no report has (a node
     * without a report is the same whether a record names it by host id or only by address, when another record gives
     * both; an address that the records give more than one host id, a node replaced at that address, leads to no host
     * id); a record without a data center or rack is not compared on it. A second report of a node (DUPLICATE_REPORT: one
     * host id, or one broadcast address) is left out: the reports with a host id are taken first, each in the order of
     * the file names, and a report is left out when an earlier one, kept or not, has its host id or its broadcast address;
     * a report with neither is a node of its own. A node whose only reports were left out is named as such ("its report
     * left out as a duplicate").
     * <p>
     * For each node its own data center and rack come first, then those of the largest group of the others (its own on a
     * tie), then the rest. The largest group of nodes recording one data center and rack is given as a count when it has
     * more than {@link #NAMED_RECORDERS} nodes, any other group names its first {@link #NAMED_RECORDERS} nodes only, a
     * node lists {@link #NAMED_RECORDERS} data centers and racks at most, and the message {@code NodeInspector.list}'s
     * number of nodes, so that it names the few that differ in a large cluster and stays bounded.
     * <p>
     * MISSING_REPORT matches a peer record that has a host id by its host id only; here a record whose host id has no
     * report is matched by address too (the broadcast addresses of the reports first, then their listen addresses), so a
     * stale record of a node replaced at the same address shows in both: as missing, and as recording that address's node
     * with the old node's data center and rack.
     */
    private static List<String> topologyMismatches(Map<Path, NodeReport> reports)
    {
        Map<String, NodeReport> byHostId = new TreeMap<>();
        // a peer record's address is looked up among the broadcast addresses of the reports first, then their listen ones
        Map<String, NodeReport> byAddress = new TreeMap<>();
        Map<String, NodeReport> byListenAddress = new TreeMap<>();
        Map<NodeReport, String> names = new IdentityHashMap<>();
        Map<NodeReport, String> files = new IdentityHashMap<>();
        List<NodeReport> distinct = new ArrayList<>();
        // the host ids and broadcast addresses of every report examined, kept or not, so that the reports left out are the
        // ones DUPLICATE_REPORT names, transitively
        Set<String> seenHostIds = new TreeSet<>();
        Set<String> seenBroadcastAddresses = new TreeSet<>();
        // the reports with a host id first, so that one without a host id at the same address is the one left out
        for (boolean withHostId : new boolean[]{ true, false })
        {
            for (Map.Entry<Path, NodeReport> entry : reports.entrySet())
            {
                NodeReport report = entry.getValue();
                if ((report.node.hostId != null) != withHostId)
                    continue;
                String name = describe(entry.getKey(), report);
                String file = entry.getKey().getFileName().toString();
                // both are recorded whatever the other says: a report left out still makes the next one with its
                // broadcast address a duplicate (DUPLICATE_REPORT names them all)
                boolean seenHostId = report.node.hostId != null && !seenHostIds.add(report.node.hostId);
                boolean seenAddress = report.node.broadcastAddress != null && !seenBroadcastAddresses.add(report.node.broadcastAddress);
                if (seenHostId || seenAddress)
                    continue;
                distinct.add(report);
                names.put(report, name);
                files.put(report, file);
                if (report.node.hostId != null)
                    byHostId.put(report.node.hostId, report);
                if (report.node.broadcastAddress != null)
                    byAddress.putIfAbsent(report.node.broadcastAddress, report);
                if (report.node.listenAddress != null)
                    byListenAddress.putIfAbsent(report.node.listenAddress, report);
            }
        }

        // by node: its name, its own data center and rack, and the data centers and racks the others record, with the recorders
        Map<String, String> subjects = new TreeMap<>();
        Map<String, String> own = new TreeMap<>();
        Map<String, Map<String, Set<String>>> recorded = new TreeMap<>();
        Map<String, Set<String>> dataCenters = new TreeMap<>();
        Map<String, Set<String>> racks = new TreeMap<>();
        for (NodeReport report : distinct)
        {
            String key = key(report, files.get(report));
            subjects.put(key, names.get(report));
            own.put(key, topology(report.node.dataCenter, report.node.rack));
            collect(key, report.node.dataCenter, report.node.rack, dataCenters, racks);
        }
        // the host id the records give an address, for a record of a node without a report that names no host id; an
        // address given more than one host id (a node replaced at that address) leads to none
        Map<String, Set<String>> hostIdsOfAddress = new TreeMap<>();
        for (NodeReport report : distinct)
            for (NodeReport.Peer peer : report.peers)
                if (peer.hostId != null && peer.address != null)
                    hostIdsOfAddress.computeIfAbsent(peer.address, a -> new TreeSet<>()).add(peer.hostId);
        Map<String, String> hostIdOfAddress = new TreeMap<>();
        hostIdsOfAddress.forEach((address, hostIds) -> {
            if (hostIds.size() == 1)
                hostIdOfAddress.put(address, hostIds.iterator().next());
        });
        for (NodeReport report : distinct)
        {
            for (NodeReport.Peer peer : report.peers)
            {
                NodeReport subject = peer.hostId == null ? null : byHostId.get(peer.hostId);
                if (subject == null && peer.address != null)
                    subject = byAddress.containsKey(peer.address) ? byAddress.get(peer.address) : byListenAddress.get(peer.address);
                if (subject == report)
                    continue;
                String hostId = peer.hostId != null ? peer.hostId : peer.address == null ? null : hostIdOfAddress.get(peer.address);
                String key;
                if (subject != null)
                    key = key(subject, files.get(subject));
                else if (hostId != null || peer.address != null)
                    key = hostId != null ? "host " + hostId : "address " + peer.address;
                else
                    continue;
                // as the key: by host id when the record gives one, else by address
                boolean leftOut = hostId != null ? seenHostIds.contains(hostId) : peer.address != null && seenBroadcastAddresses.contains(peer.address);
                subjects.putIfAbsent(key, (peer.address == null ? "(no address)" : peer.address) +
                                          (hostId == null ? "" : " (host id " + hostId + ')') +
                                          (leftOut ? " (its report left out as a duplicate)" : " (no report)"));
                recorded.computeIfAbsent(key, k -> new TreeMap<>())
                        .computeIfAbsent(topology(peer.dataCenter, peer.rack), t -> new TreeSet<>())
                        .add(names.get(report));
                collect(key, peer.dataCenter, peer.rack, dataCenters, racks);
            }
        }

        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, String> subject : subjects.entrySet())
        {
            String key = subject.getKey();
            if (dataCenters.getOrDefault(key, Collections.emptySet()).size() < 2 && racks.getOrDefault(key, Collections.emptySet()).size() < 2)
                continue;
            Map<String, Set<String>> byTopology = recorded.getOrDefault(key, Collections.emptyMap());
            String ownTopology = own.get(key);
            // the largest group of recorders (the node's own data center and rack on a tie) is given as a count when it is
            // large: the others are what to look at
            String largest = byTopology.entrySet().stream()
                                       .max(Comparator.<Map.Entry<String, Set<String>>>comparingInt(e -> e.getValue().size())
                                                      .thenComparing(e -> e.getKey().equals(ownTopology)))
                                       .map(Map.Entry::getKey).orElse(null);
            // its own data center and rack first, then the largest group, then the others: a cut keeps the two views to compare
            List<String> items = new ArrayList<>();
            if (ownTopology != null)
                items.add(ownTopology + " in its nodes/local" +
                          (byTopology.containsKey(ownTopology) ? " and in the nodes/peers of " + nodes(byTopology.get(ownTopology), ownTopology.equals(largest)) : ""));
            if (largest != null && !largest.equals(ownTopology))
                items.add(largest + " in the nodes/peers of " + nodes(byTopology.get(largest), true));
            byTopology.forEach((topology, by) -> {
                if (!topology.equals(ownTopology) && !topology.equals(largest))
                    items.add(topology + " in the nodes/peers of " + nodes(by, false));
            });
            List<String> shown = items;
            if (items.size() > NAMED_RECORDERS)
            {
                shown = new ArrayList<>(items.subList(0, NAMED_RECORDERS));
                shown.add("and " + (items.size() - NAMED_RECORDERS) + " other data centers and racks");
            }
            mismatches.add(subject.getValue() + " (" + String.join("; ", shown) + ')');
        }
        mismatches.sort(null);
        return mismatches;
    }

    /**
     * How a node with a report is told from the others: its host id, else its broadcast address (the rules of
     * DUPLICATE_REPORT), else the report's file, so that two reports with neither are two nodes.
     */
    private static String key(NodeReport report, String file)
    {
        if (report.node.hostId != null)
            return "host " + report.node.hostId;
        return report.node.broadcastAddress != null ? "address " + report.node.broadcastAddress : "file " + file;
    }

    /**
     * The nodes recording a data center and rack: counted when they are the largest group and more than
     * {@link #NAMED_RECORDERS}, otherwise named, the first {@link #NAMED_RECORDERS} only when there are more.
     */
    private static String nodes(Set<String> nodes, boolean largest)
    {
        if (nodes.size() <= NAMED_RECORDERS)
            return "[" + String.join(", ", nodes) + ']';
        if (largest)
            return nodes.size() + " nodes";
        return "[" + String.join(", ", new ArrayList<>(nodes).subList(0, NAMED_RECORDERS)) + "] and " + (nodes.size() - NAMED_RECORDERS) + " more";
    }

    private static void collect(String key, String dataCenter, String rack, Map<String, Set<String>> dataCenters, Map<String, Set<String>> racks)
    {
        if (dataCenter != null)
            dataCenters.computeIfAbsent(key, k -> new TreeSet<>()).add(dataCenter);
        if (rack != null)
            racks.computeIfAbsent(key, k -> new TreeSet<>()).add(rack);
    }

    private static String topology(String dataCenter, String rack)
    {
        return "data center " + (dataCenter == null ? UNKNOWN : dataCenter) + ", rack " + (rack == null ? UNKNOWN : rack);
    }

    /** The data center a report's {@code nodes/local} records, or {@link #UNKNOWN}. */
    private static String dataCenterOf(NodeReport report)
    {
        return report.node.dataCenter == null ? UNKNOWN : report.node.dataCenter;
    }

    /** The rack a report's {@code nodes/local} records, or {@link #UNKNOWN}. */
    private static String rackOf(NodeReport report)
    {
        return report.node.rack == null ? UNKNOWN : report.node.rack;
    }

    private static String describe(Path file, NodeReport report)
    {
        String address = report.node.broadcastAddress != null ? report.node.broadcastAddress : report.node.listenAddress;
        if (address == null)
            address = report.hostname != null ? report.hostname : file.getFileName().toString();
        return address;
    }

    private static String groups(Map<String, List<String>> byValue)
    {
        List<String> items = new ArrayList<>();
        byValue.forEach((value, nodes) -> items.add(value + " on " + list(nodes)));
        return String.join("; ", items);
    }
}
