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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import org.yaml.snakeyaml.introspector.Property;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.Properties;
import org.apache.cassandra.config.Replacement;
import org.apache.cassandra.config.Replacements;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence;
import org.apache.cassandra.nodes.DseNodeRecord;
import org.apache.cassandra.nodes.DseNodesFiles;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.service.Dse6SaiVersionPin;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.tools.dsepreflight.Finding.blocking;
import static org.apache.cassandra.tools.dsepreflight.Finding.info;
import static org.apache.cassandra.tools.dsepreflight.Finding.warning;
import static org.apache.cassandra.tools.dsepreflight.NodeLayout.get;
import static org.apache.cassandra.tools.dsepreflight.NodeLayout.getString;
import static org.apache.cassandra.tools.dsepreflight.NodeLayout.isTrue;

/**
 * The preflight inspection of one DSE node: reads its files ({@link NodeLayout}), runs every check and fills the
 * {@link NodeReport}. Nothing is written: the files are opened for reading only, the sstables without validation (see
 * {@link DseTableReader}), and the optional {@code nodetool} commands are read-only ones ({@link LiveState}).
 */
final class NodeInspector
{
    /** The most items a finding lists; the others are counted. */
    static final int LISTED = 100;
    /** DSE's commit log version, the one an upgraded node replays (6.8 and 6.9 write the same). */
    static final int DSE_COMMITLOG_VERSION = CommitLogDescriptor.VERSION_DSE_68;
    /** A hints file: {@code <host id>-<timestamp>-<version>.hints}; DSE writes {@code v1}, this software a number. */
    static final Pattern HINTS_FILE = Pattern.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-(\\d+)-(v?)(\\d+)\\.hints");
    static final Pattern COMMITLOG_FILE = Pattern.compile("CommitLog-(\\d+)-(\\d+)\\.log(\\.replayed)?");
    /** The DSE authentication schemes HCD 2.0 does not have. */
    static final Set<String> UNSUPPORTED_SCHEMES = Set.of("ldap", "kerberos");
    static final String DSE_CLASS_PREFIX = "com.datastax.bdp.";
    static final String GOSSIPING_PROPERTY_FILE_SNITCH = "GossipingPropertyFileSnitch";
    static final String DSE_SIMPLE_SNITCH = "DseSimpleSnitch";
    static final String DSE_DELEGATE_SNITCH = "DseDelegateSnitch";
    /** The EC2 snitches: DSE has no {@code ec2_naming_scheme} and derives their data center and rack with the legacy names. */
    static final Set<String> EC2_SNITCHES = Set.of("Ec2Snitch", "Ec2MultiRegionSnitch");
    static final String LOCATOR_PACKAGE = "org.apache.cassandra.locator.";
    /** The major version of HCD 2.0's releases, for a build whose release is not known (no version.properties). */
    static final int HCD_20_MAJOR = 5;
    /** The names of the tables DSE 6.8.65 and 6.9.26 encrypt while system_info_encryption is enabled in dse.yaml, sorted. */
    static final List<String> SYSTEM_INFO_ENCRYPTED_TABLE_NAMES = List.of("system.batches", "system.paxos", "system_backups.backup_configurations",
                                                                          "system_backups.backup_stores");
    /** {@link #SYSTEM_INFO_ENCRYPTED_TABLE_NAMES}, as the texts name them. */
    static final String SYSTEM_INFO_ENCRYPTED_TABLES = "the tables " + namesWithAnd(SYSTEM_INFO_ENCRYPTED_TABLE_NAMES);
    /** The rewrite of {@link #SYSTEM_INFO_ENCRYPTED_TABLES} once DSE runs with system_info_encryption off. */
    static final String SYSTEM_INFO_REWRITE = "nodetool upgradesstables -a system batches paxos and nodetool upgradesstables -a system_backups " +
                                              "on every node";
    /**
     * What to keep while DSE still has data that system_info_encryption encrypted: the whole key directory, as the table
     * key is its system/system_table_keytab.
     */
    static final String SYSTEM_KEY_DIRECTORY = "the whole directory that system_key_directory names in dse.yaml (by default /etc/dse/conf), " +
                                               "its system/system_table_keytab included,";
    /** What happens without that keytab, as DSE 6.8.65 showed when started with the directory moved aside. */
    static final String SYSTEM_KEY_LOSS = "without the original system/system_table_keytab of that directory, DSE cannot read what it encrypted " +
                                          "before (DSE 6.8.65, started without it, made a new one with no warning, failed on its encrypted hints " +
                                          "files and, under disk_failure_policy: stop, stopped serving)";
    /**
     * How the remedies of the findings on system_info_encryption end: the key directory kept until the rewrite of
     * {@link #SYSTEM_INFO_ENCRYPTED_TABLES} and the delivery of the encrypted hints files, before the first upgrade, and
     * {@link #SYSTEM_KEY_LOSS}.
     */
    static final String SYSTEM_INFO_KEEP_UNTIL = "keep that directory in place until the rewrite is done and, with every node up, DSE has " +
                                                 "delivered the hints files written before, which stay encrypted (this tool names those left, " +
                                                 "under HINTS_COMPRESSION); all of it before the first node of the cluster is upgraded; " +
                                                 SYSTEM_KEY_LOSS;
    /** What DSE's nodetool answers for a command it does not have (DSE 6.8.65 and 6.9.26 have no listpendinghints). */
    static final String UNKNOWN_COMMAND = "Found unexpected parameters";
    /** The longest parameters the header of a DSE hints file is taken to hold; a longer length is a damaged header. */
    static final int MAX_HINTS_PARAMETERS_BYTES = 64 * 1024;
    /** The default {@code ssl_storage_port} of DSE 6 and of HCD 2.0. */
    static final String DEFAULT_SSL_STORAGE_PORT = "7001";
    /**
     * The default {@code storage_port} of DSE 6 and of HCD 2.0; the node report carries {@code storage_port} only when DSE's
     * {@code cassandra.yaml} sets it.
     */
    static final String DEFAULT_STORAGE_PORT = "7000";
    /**
     * What a remedy that has {@code ssl_storage_port} carried asks of DSE's JVM options: DSE honours a
     * {@code -Dcassandra.ssl_storage_port} over its {@code cassandra.yaml}, which this tool does not read.
     */
    static final String CHECK_SSL_STORAGE_PORT_OPTION = "check DSE's JVM options for a -Dcassandra.ssl_storage_port, which must be absent " +
                                                        "or give that same ssl_storage_port (required: DSE honours it over cassandra.yaml, " +
                                                        "and this tool reads cassandra.yaml only; check it by hand: the command line of the " +
                                                        "running DSE process lists the -D options it was started with)";

    private final NodeLayout layout;
    @Nullable
    private final Map<String, NodeReport.LiveCommand> live;
    private final NodeReport report = new NodeReport();
    private final List<Finding> findings = report.findings;

    @Nullable
    private DseNodeRecord local;
    /**
     * whether internode TLS on this node's DSE release needs DSE's legacy TLS port; set only by {@link #checkInternodeTls}
     * with internode encryption on, null when the release is unknown or the check did not run
     */
    @Nullable
    private Boolean legacyPortRequired;
    /**
     * whether DSE's cassandra.yaml sets {@code enable_legacy_ssl_storage_port} to a value DSE reads as true; set only by
     * {@link #checkInternodeTls} with internode encryption on, false otherwise
     */
    private boolean legacyPortOn;
    /** DSE's {@code enable_legacy_ssl_storage_port} as the loader gives it, with {@link #legacyPortOn} */
    @Nullable
    private Object legacyPortValue;
    private List<DseNodeRecord> peers = List.of();
    @Nullable
    private CassandraVersion localRelease;
    private final Map<String, Long> liveBytesByDataDirectory = new LinkedHashMap<>();
    private final Map<String, Integer> pendingHintsByTarget = new TreeMap<>();
    private boolean nodesyncDisabledInYaml;
    /** the blocking kinds of sstable problem found in the data directories */
    private final Set<String> formatProblems = new TreeSet<>();
    /** whether every data directory was listed completely */
    private boolean dataDirectoriesScanned;
    /** the table directories whose transaction logs could not be read */
    private final List<String> unreadableTransactionLogs = new ArrayList<>();
    private SchemaInspector schema;

    NodeInspector(NodeLayout layout, @Nullable Map<String, NodeReport.LiveCommand> live)
    {
        this.layout = layout;
        this.live = live;
    }

    NodeReport inspect(String hostname)
    {
        report.softwareVersion = FBUtilities.getReleaseVersionString();
        report.generatedAt = FBUtilities.now().toString();
        report.hostname = hostname;
        guarded("the inputs", this::recordInputs);
        String snitch = dseSnitch();
        boolean rackdcUsed = snitch != null && snitch.endsWith(GOSSIPING_PROPERTY_FILE_SNITCH);
        boolean ec2 = isEc2Snitch(snitch);
        String configuredSnitch = getString(layout.cassandraConf, "endpoint_snitch");
        String snitchNote;
        if (layout.unreadable.containsKey(NodeLayout.CASSANDRA_YAML))
            snitchNote = " (the snitch DSE runs with is not known: cassandra.yaml cannot be read)";
        else if (configuredSnitch == null)
            snitchNote = " (DSE runs with its DseSimpleSnitch, which does not read it)";
        else if (snitch == null && layout.unreadable.containsKey(NodeLayout.DSE_YAML))
            snitchNote = " (the snitch DSE delegates to is not known: dse.yaml cannot be read)";
        else if (snitch == null)
            snitchNote = " (the snitch DSE delegates to is not known: dse.yaml names no delegated_snitch)";
        else
            snitchNote = " (not used by the snitch " + snitch + ')';
        layout.unreadable.forEach((what, why) -> {
            if (what.equals(NodeLayout.RACKDC) && rackdcUsed)
                findings.add(warning("UNREADABLE_INPUT", cannotRead(what) + why + "; the data center and rack are taken from nodes/local",
                                     "pass --rackdc <file>: with GossipingPropertyFileSnitch the data center and rack of HCD 2.0 come from it"));
            else if (what.equals(NodeLayout.RACKDC) && ec2)
                findings.add(warning("UNREADABLE_INPUT", cannotRead(what) + why + "; the dc_suffix " + snitch + " adds to the data center " +
                                                         "is not known",
                                     "pass --rackdc <file>: with " + snitch + " HCD 2.0 needs the dc_suffix of DSE's cassandra-rackdc.properties (EC2_NAMING)"));
            else if (what.equals(NodeLayout.RACKDC))
                findings.add(info("UNREADABLE_INPUT", cannotRead(what) + why + snitchNote));
            else
                findings.add(blocking("UNREADABLE_INPUT", cannotRead(what) + why + ", so the checks that depend on it cannot be made",
                                      "run the tool as the user DSE runs as (or as root), or point it at the file (--cassandra-yaml, --dse-yaml, --conf-dir, --dse-home)"));
        });
        if (!layout.defaultsAssumed.isEmpty())
            findings.add(info("DEFAULTS_ASSUMED", "DSE's cassandra.yaml does not set these directories, DSE's defaults are assumed: " + list(layout.defaultsAssumed)));

        guarded("nodes/local and nodes/peers", this::inspectNodesFiles);
        guarded("cassandra.yaml and dse.yaml", this::inspectConfiguration);
        schema = new SchemaInspector(layout.dataDirectories, dsefsKeyspace(), partitioner(),
                                     isTrue(get(layout.dseConf, "system_info_encryption", "enabled")), findings);
        guarded("the schema and the access controls", schema::inspect);
        report.accessControls.rolePermissionsRows = schema.rolePermissionsRead ? schema.rolePermissionsRows : null;
        report.accessControls.authorizationEnabled = authorizationEnabled();
        if (schema.schemaRead)
        {
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("keyspaces", schema.keyspaceCount);
            summary.put("tables", schema.tableCount);
            summary.put("views", schema.viewCount);
            summary.put("keyspace_names", schema.keyspaceNames());
            report.inputs.put("schema", summary);
        }

        guarded("the data directories", this::inspectDataDirectories);
        guarded("the hints directory", this::inspectHints);
        guarded("the commit log directory", this::inspectCommitLog);
        findings.add(info("SSTABLES_ONLY",
                          "system_schema and system_auth are read from their sstables, so changes a running DSE node has not flushed yet (a GRANT, a " +
                          "RESTRICT, a schema change) are not seen: run nodetool flush system_schema && nodetool flush system_auth (two " +
                          "commands, one keyspace each) right before the tool"));
        guarded("the metadata directory", this::inspectMetadataDirectory);
        if (live != null)
            guarded("the live state", this::inspectLive);
        guarded("the upgrade path", this::buildUpgradePath);

        findings.sort(Comparator.comparing(f -> f.severity));
        report.result = report.blocked() ? NodeReport.BLOCKED : NodeReport.READY;
        return report;
    }

    /**
     * Runs one part of the inspection; an unexpected error (a file DSE wrote in a form the tool does not expect, for
     * instance) is a blocking finding rather than the end of the run, so that the other parts are still reported and the
     * node is never declared ready unchecked.
     */
    private void guarded(String what, Runnable part)
    {
        try
        {
            part.run();
        }
        catch (RuntimeException | AssertionError e)
        {
            findings.add(blocking("INSPECTION_ERROR", "the inspection of " + what + " failed (" + e + "), so it is incomplete",
                                  "report the error with the output of the tool; the node cannot be declared ready until the inspection completes"));
        }
    }

    /**
     * The start of the message of an {@code UNREADABLE_INPUT} finding for the input {@code what} ({@link NodeLayout}'s
     * names, such as {@link NodeLayout#CASSANDRA_YAML}), which the aggregate mode reads back ({@code ClusterAggregate}).
     */
    static String cannotRead(String what)
    {
        return "cannot read " + what + ": ";
    }

    private void recordInputs()
    {
        report.inputs.put("dse_home", layout.dseHome == null ? null : layout.dseHome.toString());
        report.inputs.put("cassandra_yaml", layout.cassandraYaml.toString());
        report.inputs.put("dse_yaml", layout.dseYaml.toString());
        report.inputs.put("cassandra_rackdc_properties", layout.rackdc.toString());
        List<String> data = new ArrayList<>();
        layout.dataDirectories.forEach(d -> data.add(d.toString()));
        report.inputs.put("data_file_directories", data);
        report.inputs.put("metadata_directory", layout.metadataDirectory.toString());
        report.inputs.put("commitlog_directory", layout.commitLogDirectory.toString());
        report.inputs.put("hints_directory", layout.hintsDirectory.toString());
    }

    // ---- nodes/local, nodes/peers, releases, workloads ----

    private void inspectNodesFiles()
    {
        Path nodes = layout.metadataDirectory.toPath().resolve("nodes");
        try
        {
            local = DseNodesFiles.readLocal(nodes);
            if (local == null && Files.exists(nodes.resolve("local.old")))
                findings.add(blocking("NOT_A_DSE_NODE",
                                      "no nodes/local in " + nodes + " but a nodes/local.old: DSE was interrupted while it rewrote the file, so the DSE release, " +
                                      "identity and workloads of this node cannot be read",
                                      "start DSE on this node once (it recovers nodes/local from local.old), then run the tool again"));
            else if (local == null)
                findings.add(blocking("NOT_A_DSE_NODE",
                                      "no nodes/local in " + nodes + ": the DSE release, identity and workloads of this node cannot be read",
                                      "point the tool at DSE's metadata directory (metadata_directory of DSE's cassandra.yaml, or --metadata-dir)"));
        }
        catch (RuntimeException e)
        {
            findings.add(blocking("UNREADABLE_INPUT", cannotRead("nodes/local") + e.getMessage(),
                                  "run the tool as the user DSE runs as (or as root); a damaged nodes/local must be repaired on DSE first"));
        }
        try
        {
            peers = DseNodesFiles.readPeers(nodes);
        }
        catch (RuntimeException e)
        {
            findings.add(blocking("UNREADABLE_INPUT", cannotRead("nodes/peers") + e.getMessage() + "; the releases and workloads of the peers cannot be checked",
                                  "run the tool as the user DSE runs as (or as root)"));
        }

        String installed = null;
        try
        {
            installed = layout.installedDseVersion();
        }
        catch (IOException e)
        {
            findings.add(warning("INSTALLED_RELEASE_UNKNOWN", "cannot read ds_version.txt: " + e.getMessage(), "check the release with dse -v"));
        }
        if (installed == null && layout.dseHome != null && local != null)
            findings.add(warning("INSTALLED_RELEASE_UNKNOWN", "no ds_version.txt in " + layout.dseHome + ", so the installed DSE release is not compared with the running one",
                                 "pass --dse-home <DSE installation directory>, or check with dse -v that every node runs the same release"));
        else if (installed == null && local != null)
            findings.add(warning("INSTALLED_RELEASE_UNKNOWN", "no DSE installation found (/opt/dse, /usr/share/dse, DSE_HOME), so the installed DSE release is not compared with the running one",
                                 "pass --dse-home <DSE installation directory>"));
        report.node.installedDseVersion = installed;

        if (local != null)
        {
            report.node.hostId = string(local.hostId);
            report.node.broadcastAddress = address(local.broadcastAddress);
            report.node.listenAddress = address(local.listenAddress);
            report.node.clusterName = local.clusterName;
            report.node.dataCenter = local.dataCenter;
            report.node.rack = local.rack;
            report.node.releaseVersion = local.releaseVersion;
            report.node.dseVersion = local.dseVersion;
            report.node.schemaVersion = string(local.schemaVersion);
            checkLocalRelease(installed);
            List<String> nonCore = Dse6NonCoreTraces.nonCore(local.workloads);
            if (!nonCore.isEmpty())
                findings.add(blocking("NON_CORE_WORKLOAD",
                                      "this node (nodes/local) records DSE workloads other than Cassandra: " + String.join(", ", nonCore) +
                                      "; HCD 2.0 supports only DSE core, and an upgraded node refuses to start with them",
                                      restartAsCassandra("this node")));
        }
        checkPeers();
    }

    private void checkLocalRelease(@Nullable String installed)
    {
        localRelease = parse(local.releaseVersion);
        String sai = Dse6SaiVersionPin.saiVersionOf(localRelease);
        if (!DseCompat.isDse6ReleaseVersion(localRelease))
            findings.add(blocking("DSE_RELEASE",
                                  "nodes/local records the release " + local.releaseVersion + ", not a DSE 6 release (4.0.0.<build>): this node does not run DSE 6",
                                  "run the tool on DSE 6.8 or 6.9 nodes only; a node already upgraded to HCD 2.0 needs no preflight"));
        else if (sai == null)
            findings.add(blocking("DSE_RELEASE",
                                  "this node runs DSE " + local.dseVersion + " (release " + local.releaseVersion + "); only DSE 6.8 and 6.9 are upgraded in place " +
                                  "to HCD 2.0 (6.9 preferred)",
                                  "upgrade every node to the latest DSE 6.9 (or 6.8) patch release first, following DataStax's procedure"));
        else if ("aa".equals(sai))
            findings.add(info("DSE_RELEASE", "DSE 6.8 (" + dseVersionOrRelease() + ") is accepted; the upgrade requires the latest DSE 6.8 or 6.9 " +
                              "patch release, which this tool does not check; DSE 6.9 is preferred"));
        else
            findings.add(info("DSE_RELEASE", "DSE 6.9 (" + dseVersionOrRelease() + ") is accepted; the upgrade requires the latest DSE 6.9 patch " +
                              "release, which this tool does not check"));

        if (installed != null && local.dseVersion != null)
        {
            String installedRelease = installed.contains("-") ? installed.substring(0, installed.indexOf('-')) : installed;
            if (!installedRelease.equals(local.dseVersion))
                findings.add(blocking("INSTALLED_RELEASE_MISMATCH",
                                      "the DSE installation " + layout.dseHome + " is " + installed + " (ds_version.txt) but this node last ran DSE " + local.dseVersion +
                                      " (nodes/local)",
                                      "restart DSE so that the node runs the installed release (or reinstall the release it runs); every node must run one " +
                                      "release before the upgrade"));
        }
    }

    /** The DSE version {@code nodes/local} records, or its release when it records no DSE version. */
    private String dseVersionOrRelease()
    {
        return local.dseVersion != null ? local.dseVersion : "release " + local.releaseVersion;
    }

    private void checkPeers()
    {
        List<String> otherReleases = new ArrayList<>();
        List<String> upgraded = new ArrayList<>();
        List<String> notDse6 = new ArrayList<>();
        List<String> unknown = new ArrayList<>();
        for (DseNodeRecord peer : peers)
        {
            String address = peer.peer == null ? "(no address)" : peer.peer.getHostAddress();
            NodeReport.Peer entry = new NodeReport.Peer();
            entry.address = peer.peer == null ? null : peer.peer.getHostAddress();
            entry.hostId = string(peer.hostId);
            entry.dataCenter = peer.dataCenter;
            entry.rack = peer.rack;
            entry.releaseVersion = peer.releaseVersion;
            entry.dseVersion = peer.dseVersion;
            entry.schemaVersion = string(peer.schemaVersion);
            CassandraVersion release = parse(peer.releaseVersion);
            if (release == null)
            {
                unknown.add(address + " (release " + peer.releaseVersion + ')');
            }
            else if (isUpgradedRelease(release))
            {
                entry.upgraded = true;
                upgraded.add(address + " (release " + peer.releaseVersion + ')');
            }
            else if (!DseCompat.isDse6ReleaseVersion(release))
            {
                notDse6.add(address + " (release " + peer.releaseVersion + ')');
            }
            else if (localRelease != null && !release.equals(localRelease))
            {
                otherReleases.add(address + ' ' + peer.releaseVersion + (peer.dseVersion == null ? "" : " (DSE " + peer.dseVersion + ')'));
            }
            report.peers.add(entry);
            List<String> nonCore = Dse6NonCoreTraces.nonCore(peer.workloads);
            if (!nonCore.isEmpty())
                findings.add(blocking("NON_CORE_WORKLOAD",
                                      "peer " + address + " (nodes/peers) records DSE workloads other than Cassandra: " + String.join(", ", nonCore) +
                                      "; HCD 2.0 supports only DSE core, and an upgraded node refuses to start with them",
                                      restartAsCassandra("peer " + address) + "; if that node has left the cluster or was already restarted as a Cassandra-only " +
                                      "node while this one was down, the entry is stale: restart DSE on this node to refresh nodes/peers"));
        }
        if (!otherReleases.isEmpty())
            findings.add(blocking("MIXED_RELEASES",
                                  "DSE nodes run different releases: this node " + local.releaseVersion + ", peers (nodes/peers) " + list(otherReleases),
                                  "bring every DSE node to the same release (6.9 preferred) before the upgrade"));
        if (!notDse6.isEmpty())
            findings.add(blocking("PEER_RELEASE",
                                  "peers recorded with a release that is neither DSE 6 nor HCD 2.0: " + list(notDse6) + "; every node must run DSE 6.8 or 6.9 " +
                                  "before the upgrade (or already run HCD 2.0)",
                                  "upgrade those nodes to the DSE release of the cluster; if an entry is stale (the node has left the cluster), remove the " +
                                  "node from the cluster on DSE (nodetool removenode) so that DSE drops its entry"));
        if (!upgraded.isEmpty())
            findings.add(info("UPGRADED_PEERS", "peers already running HCD 2.0 (the cluster is in the mixed phase): " + list(upgraded)));
        if (!unknown.isEmpty())
            findings.add(warning("PEER_RELEASE_UNKNOWN", "peers whose release nodes/peers does not record: " + list(unknown),
                                 "check their release with nodetool version on each, or with this tool's report of each"));
    }

    private static String restartAsCassandra(String who)
    {
        return "restart " + who + " as a plain Cassandra node (dse cassandra without -s, -k or -g; SOLR_ENABLED, SPARK_ENABLED and GRAPH_ENABLED set to " +
               "0 in /etc/default/dse for a package install) while every other node is up";
    }

    // ---- cassandra.yaml and dse.yaml ----

    private void inspectConfiguration()
    {
        Map<String, Object> yaml = layout.cassandraConf;
        Map<String, Object> dse = layout.dseConf;

        Object nodesync = get(yaml, "nodesync", "enabled");
        nodesyncDisabledInYaml = nodesync != null && NodeLayout.isFalse(nodesync);
        if (!yaml.isEmpty() && !nodesyncDisabledInYaml)
            findings.add(warning("NODESYNC",
                                 "DSE's cassandra.yaml does not set enabled: false under nodesync: (" + (nodesync == null ? "absent, NodeSync starts at every DSE start" : "enabled: " + nodesync) +
                                 "); while NodeSync runs, DSE closes its connections to upgraded nodes",
                                 "add enabled: false under nodesync: in DSE's cassandra.yaml and run nodetool nodesyncservice disable on every DSE node"));

        if (!yaml.isEmpty())
            checkAuthentication(yaml, dse);
        if (isTrue(get(dse, "authorization_options", "allow_row_level_security")))
            findings.add(blocking("ROW_LEVEL_ACCESS",
                                  "authorization_options.allow_row_level_security is true in dse.yaml: DSE row-level access control is on, and HCD 2.0 does not " +
                                  "enforce it",
                                  "UNRESTRICT ROWS on every restricted table, REVOKE the row-level grants, then set allow_row_level_security: false"));
        checkSnitch(yaml, dse);
        checkInternodeTls(yaml);
        String partitioner = getString(yaml, "partitioner");
        if (partitioner != null)
        {
            try
            {
                FBUtilities.newPartitioner(partitioner);
            }
            catch (RuntimeException e)
            {
                findings.add(blocking("UNLOADABLE_CLASS", "the partitioner " + partitioner + " of DSE's cassandra.yaml is not in HCD 2.0 (" + e.getMessage() + ')',
                                      "the partitioner of a cluster cannot change: this cluster cannot be upgraded in place"));
            }
        }
        if (isTrue(get(yaml, "transparent_data_encryption_options", "enabled")))
            findings.add(blocking("TDE_COMMITLOG",
                                  "transparent_data_encryption_options.enabled is true in DSE's cassandra.yaml: the commit log DSE writes is encrypted; " +
                                  "encryption is out of scope of this upgrade, and HCD 2.0's replay of DSE's commit log was not validated with " +
                                  "encrypted segments",
                                  "set enabled: false under transparent_data_encryption_options and restart DSE on every node, with the keystore that " +
                                  "section names still in place, so that DSE replays the commit log segments written before and writes its segments " +
                                  "unencrypted from then on, before the upgrade"));
        Object hintsCompression = yaml.get("hints_compression");
        if (isSetValue(hintsCompression))
            findings.add(blocking("HINTS_COMPRESSION",
                                  "DSE's cassandra.yaml sets hints_compression (" + compressorOf(hintsCompression) + "): DSE writes compressed hints files, " +
                                  "and HCD 2.0 cannot convert a compressed DSE hints file at its start: it keeps the file and does not deliver its hints",
                                  "remove hints_compression from DSE's cassandra.yaml and restart DSE on this node, so that the hints files it writes from " +
                                  "then on are not compressed; the files written before keep their compression: with every node up, wait until DSE has " +
                                  "delivered them and they are gone from its hints directory, before the first node of the cluster is upgraded (this tool " +
                                  "names the compressed or encrypted files left)"));

        if (dse.isEmpty())
            return;
        if (isTrue(get(dse, "system_info_encryption", "enabled")))
            findings.add(blocking("TDE_TABLE",
                                  "system_info_encryption.enabled is true in dse.yaml: DSE encrypts " + SYSTEM_INFO_ENCRYPTED_TABLES + ", its commit " +
                                  "log, its saved caches and its hints files; encryption is out of scope of this upgrade",
                                  "set enabled: false under system_info_encryption in dse.yaml; restart DSE on every node, with " + SYSTEM_KEY_DIRECTORY +
                                  " still in place, so that DSE replays the commit log segments written before and writes its commit log, saved " +
                                  "caches and hints files unencrypted from then on (HCD 2.0 skips a saved cache it cannot read); then run " +
                                  SYSTEM_INFO_REWRITE + ", so that those tables are rewritten unencrypted (this tool does not check that the rewrite " +
                                  "is done); and " + SYSTEM_INFO_KEEP_UNTIL));
        if (isTrue(get(dse, "config_encryption_active")))
            findings.add(warning("CONFIG_ENCRYPTION",
                                 "config_encryption_active is true in dse.yaml: some values of DSE's configuration files are encrypted, and the tool sees them " +
                                 "encrypted",
                                 "carry the decrypted values into the configuration of HCD 2.0"));
        if (isTrue(get(dse, "audit_logging_options", "enabled")))
            findings.add(warning("AUDIT_LOGGING",
                                 "DSE audit logging is enabled (audit_logging_options in dse.yaml); HCD 2.0 does not read it",
                                 "configure the audit log of HCD 2.0 (audit_logging_options in its cassandra.yaml) before the node starts"));
        if (isTrue(get(dse, "advanced_replication_options", "enabled")))
            findings.add(blocking("ADVANCED_REPLICATION",
                                  "advanced_replication_options.enabled is true in dse.yaml; HCD 2.0 does not support DSE Advanced Replication",
                                  "stop Advanced Replication, set enabled: false, migrate what dse_advrep holds if it is needed and drop it"));
        String advrepDirectory = getString(dse, "advanced_replication_options", "advanced_replication_directory");
        if (advrepDirectory != null && nonEmptyDirectory(Path.of(advrepDirectory)))
            findings.add(blocking("ADVANCED_REPLICATION",
                                  "the DSE Advanced Replication directory " + advrepDirectory + " holds files",
                                  "stop Advanced Replication, then delete the directory"));
        if (isTrue(get(dse, "dsefs_options", "enabled")))
            findings.add(blocking("DSEFS",
                                  "dsefs_options.enabled is true in dse.yaml; HCD 2.0 does not support DSEFS",
                                  "disable DSEFS, migrate what its keyspace holds if it is needed and drop it"));
        Object tiered = get(dse, "tiered_storage_options");
        if (tiered instanceof Map && !((Map<?, ?>) tiered).isEmpty())
            findings.add(blocking("TIERED_STORAGE",
                                  "dse.yaml configures DSE tiered storage (tiered_storage_options " + ((Map<?, ?>) tiered).keySet() + "); HCD 2.0 does not have it",
                                  "move every table off tiered storage on DSE, then remove tiered_storage_options from dse.yaml"));
        String solrDataDir = getString(dse, "solr_data_dir");
        if (solrDataDir != null && Files.exists(Path.of(solrDataDir)))
            findings.add(blocking("SOLR_DATA",
                                  "the DSE Search data directory " + solrDataDir + " (solr_data_dir in dse.yaml) exists",
                                  "drop every search index on DSE, then delete the directory"));
    }

    /**
     * The partitioner of the cluster, as DSE's {@code cassandra.yaml} names it (DSE's default, {@code Murmur3Partitioner},
     * when it does not), for the table metadata the schema is checked with; {@code Murmur3Partitioner} when the one named
     * is not in this software, which the configuration check reports.
     */
    private IPartitioner partitioner()
    {
        String name = getString(layout.cassandraConf, "partitioner");
        try
        {
            return name == null ? Murmur3Partitioner.instance : FBUtilities.newPartitioner(name);
        }
        catch (RuntimeException e)
        {
            return Murmur3Partitioner.instance;
        }
    }

    /**
     * Whether DSE checks permissions on this node: {@code DseAuthorizer} with {@code authorization_options.enabled}, or
     * any authorizer but {@code AllowAllAuthorizer}; null when it is not known: {@code cassandra.yaml} cannot be read, or
     * the authorizer is {@code DseAuthorizer} and {@code dse.yaml} cannot be read.
     */
    @Nullable
    private Boolean authorizationEnabled()
    {
        if (layout.unreadable.containsKey(NodeLayout.CASSANDRA_YAML))
            return null;
        String authorizer = getString(layout.cassandraConf, "authorizer");
        if (authorizer == null || authorizer.endsWith("AllowAllAuthorizer"))
            return false;
        if (authorizer.endsWith(".DseAuthorizer"))
            return layout.unreadable.containsKey(NodeLayout.DSE_YAML) ? null : isTrue(get(layout.dseConf, "authorization_options", "enabled"));
        return true;
    }

    /** The DSEFS keyspace named in {@code dse.yaml} when it is not DSE's default, else null. */
    @Nullable
    private String dsefsKeyspace()
    {
        String name = getString(layout.dseConf, "dsefs_options", "keyspace_name");
        return name == null || name.equals("dsefs") ? null : name;
    }

    private void checkAuthentication(Map<String, Object> yaml, Map<String, Object> dse)
    {
        String authenticator = getString(yaml, "authenticator");
        String authorizer = getString(yaml, "authorizer");
        String roleManager = getString(yaml, "role_manager");
        List<String> changes = new ArrayList<>();
        List<String> blocked = new ArrayList<>();

        if (isDseClass(authenticator))
        {
            if (authenticator.endsWith(".DseAuthenticator"))
            {
                boolean enabled = isTrue(get(dse, "authentication_options", "enabled"));
                String defaultScheme = getString(dse, "authentication_options", "default_scheme");
                List<String> schemes = new ArrayList<>();
                if (defaultScheme != null)
                    schemes.add(defaultScheme.trim().toLowerCase());
                NodeLayout.stringList(get(dse, "authentication_options", "other_schemes")).forEach(s -> schemes.add(s.toLowerCase()));
                Set<String> unsupported = new TreeSet<>(schemes);
                unsupported.retainAll(UNSUPPORTED_SCHEMES);
                if (enabled && !unsupported.isEmpty())
                    blocked.add("authentication_options in dse.yaml uses the schemes " + unsupported + " (default_scheme " + defaultScheme + ", other_schemes " +
                                NodeLayout.stringList(get(dse, "authentication_options", "other_schemes")) + ')');
                changes.add("authenticator: " + (enabled ? "PasswordAuthenticator" : "AllowAllAuthenticator") + " (was " + authenticator +
                            (enabled ? " with the internal scheme" : " with authentication_options.enabled false") + ')');
                if (enabled && isTrue(get(dse, "authentication_options", "scheme_permissions")))
                    findings.add(blocking("SCHEME_PERMISSIONS",
                                          "authentication_options.scheme_permissions is true in dse.yaml: DSE lets a role log in only with the schemes it has " +
                                          "EXECUTE on (resources authentication_schemes/...); HCD 2.0 ignores those grants, so a role DSE keeps out could log in after the upgrade",
                                          "on DSE, give the roles that must log in EXECUTE on their scheme and make the others unable to log in (ALTER ROLE ... " +
                                          "WITH LOGIN = false), then set scheme_permissions: false, before the upgrade"));
                String transitional = getString(dse, "authentication_options", "transitional_mode");
                if (enabled && transitional != null && !transitional.trim().equalsIgnoreCase("disabled"))
                    findings.add(warning("TRANSITIONAL_MODE",
                                         "authentication_options.transitional_mode is " + transitional + " in dse.yaml: DSE lets some clients in without a valid " +
                                         "login; HCD 2.0's PasswordAuthenticator does not",
                                         "make every client log in with valid credentials, then set transitional_mode: disabled, before the upgrade"));
            }
            else
            {
                blocked.add("authenticator " + authenticator);
            }
        }
        else if (authenticator != null && !loadable(authenticator, "org.apache.cassandra.auth."))
        {
            findings.add(warning("UNSUPPORTED_AUTH_CLASS", "the authenticator " + authenticator + " is not in HCD 2.0",
                                 "put its jar (built against HCD 2.0) in the lib directory of HCD 2.0, or use PasswordAuthenticator"));
        }

        if (isDseClass(roleManager))
        {
            String mode = getString(dse, "role_management_options", "mode");
            if (roleManager.endsWith(".DseRoleManager") && (mode == null || !mode.trim().equalsIgnoreCase("ldap")))
                changes.add("role_manager: CassandraRoleManager (was " + roleManager + " in mode " + (mode == null ? "internal" : mode) + ')');
            else if (roleManager.endsWith(".DseRoleManager"))
                blocked.add("role_management_options.mode is ldap in dse.yaml");
            else
                blocked.add("role_manager " + roleManager);
        }

        if (isDseClass(authorizer))
        {
            if (authorizer.endsWith(".DseAuthorizer"))
            {
                boolean enabled = isTrue(get(dse, "authorization_options", "enabled"));
                changes.add("authorizer: " + (enabled ? "CassandraAuthorizer" : "AllowAllAuthorizer") + " (was " + authorizer +
                            (enabled ? "" : " with authorization_options.enabled false") + ')');
            }
            else
            {
                blocked.add("authorizer " + authorizer);
            }
        }
        if (!blocked.isEmpty())
            findings.add(blocking("AUTH_SCHEME",
                                  "DSE authentication or role management HCD 2.0 does not have: " + list(blocked),
                                  "move every role to internal authentication on DSE (authentication_options.default_scheme: internal without other_schemes, " +
                                  "role_management_options.mode: internal) before the upgrade; HCD 2.0 has PasswordAuthenticator, CassandraRoleManager and " +
                                  "CassandraAuthorizer"));
        else if (!changes.isEmpty())
            findings.add(warning("AUTH_CONFIG_CHANGE",
                                 "DSE security classes have no counterpart of the same name in HCD 2.0; the roles and grants stored in system_auth are kept: " +
                                 list(changes),
                                 "set these classes in the cassandra.yaml of HCD 2.0 (see the settings to carry)"));
    }

    /**
     * The snitch DSE runs with: {@code endpoint_snitch} of its {@code cassandra.yaml}, or, for {@code DseDelegateSnitch},
     * the {@code delegated_snitch} of {@code dse.yaml}; null when {@code cassandra.yaml} sets none.
     */
    @Nullable
    private String dseSnitch()
    {
        String snitch = getString(layout.cassandraConf, "endpoint_snitch");
        if (snitch != null && snitch.endsWith(DSE_DELEGATE_SNITCH))
            return getString(layout.dseConf, "delegated_snitch"); // null when dse.yaml names none
        return snitch;
    }

    /**
     * The snitch HCD 2.0 is to run with on this node: the one DSE runs with when HCD 2.0 has it, else
     * {@code GossipingPropertyFileSnitch} with the data center and rack of {@code nodes/local} (DSE's own snitches, and no
     * snitch at all, which leaves DSE on its {@code DseSimpleSnitch}).
     */
    private String targetSnitch()
    {
        String snitch = dseSnitch();
        if (snitch == null || isDseClass(snitch) || snitch.endsWith(DSE_SIMPLE_SNITCH) || snitch.endsWith(DSE_DELEGATE_SNITCH))
            return GOSSIPING_PROPERTY_FILE_SNITCH;
        return snitch;
    }

    private void checkSnitch(Map<String, Object> yaml, Map<String, Object> dse)
    {
        if (yaml.isEmpty())
            return;
        String configured = getString(yaml, "endpoint_snitch");
        String snitch = dseSnitch();
        String target = targetSnitch();
        String dcAndRack = local == null ? "the data center and rack of nodes/local" : "dc=" + local.dataCenter + " and rack=" + local.rack + " (from nodes/local)";
        if (configured == null)
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml sets no endpoint_snitch, so DSE runs with its DseSimpleSnitch, which HCD 2.0 does not have (HCD 2.0 would " +
                                 "start with SimpleSnitch and its data center datacenter1)",
                                 "set endpoint_snitch: " + target + " in the cassandra.yaml of HCD 2.0, with " + dcAndRack + " in its cassandra-rackdc.properties"));
        else if (configured.endsWith(DSE_DELEGATE_SNITCH) && snitch == null)
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml uses " + configured + ", which HCD 2.0 does not have, and " +
                                 (layout.unreadable.containsKey(NodeLayout.DSE_YAML) ? "dse.yaml cannot be read" : "dse.yaml names no delegated_snitch") +
                                 ", so the snitch DSE delegates to is not known",
                                 "set endpoint_snitch: " + target + " in the cassandra.yaml of HCD 2.0, with " + dcAndRack + " in its cassandra-rackdc.properties"));
        else if (configured.endsWith(DSE_DELEGATE_SNITCH) && target.equals(snitch) && !loadable(snitch, LOCATOR_PACKAGE))
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml uses " + configured + ", which HCD 2.0 does not have; it delegates to " + snitch + " (delegated_snitch in " +
                                 "dse.yaml), which HCD 2.0 does not have either",
                                 "put the jar of " + snitch + " (built against HCD 2.0) in the lib directory of HCD 2.0 and set endpoint_snitch: " + snitch +
                                 ", or use GossipingPropertyFileSnitch with " + dcAndRack));
        else if (configured.endsWith(DSE_DELEGATE_SNITCH))
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml uses " + configured + ", which HCD 2.0 does not have; it delegates to " + snitch + " (delegated_snitch in dse.yaml)",
                                 "set endpoint_snitch: " + target + " in the cassandra.yaml of HCD 2.0, keeping " + dcAndRack));
        else if (target.equals(snitch) && !loadable(snitch, LOCATOR_PACKAGE))
            findings.add(warning("SNITCH",
                                 "the snitch " + snitch + " of DSE's cassandra.yaml is not in HCD 2.0",
                                 "put its jar (built against HCD 2.0) in the lib directory of HCD 2.0, or use GossipingPropertyFileSnitch with " + dcAndRack));
        else if (!target.equals(snitch))
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml uses " + configured + ", which HCD 2.0 does not have",
                                 "set endpoint_snitch: " + target + " in the cassandra.yaml of HCD 2.0, with " + dcAndRack + " in its cassandra-rackdc.properties"));
        if (snitch != null && snitch.endsWith(GOSSIPING_PROPERTY_FILE_SNITCH) && local != null)
        {
            String dc = layout.rackdcProperties.getProperty("dc");
            String rack = layout.rackdcProperties.getProperty("rack");
            if ((dc != null && !dc.trim().equals(local.dataCenter)) || (rack != null && !rack.trim().equals(local.rack)))
                findings.add(warning("IDENTITY_MISMATCH",
                                     "cassandra-rackdc.properties says dc=" + dc + ", rack=" + rack + " but nodes/local records " + local.dataCenter + '/' + local.rack,
                                     "carry the data center and rack this node runs with into HCD 2.0 (check nodetool status); a different one moves its data"));
        }
        if (isEc2Snitch(snitch))
        {
            String names = local == null ? "the data center and rack of nodes/local" : "here dc=" + local.dataCenter + ", rack=" + local.rack + " from nodes/local";
            findings.add(warning("EC2_NAMING",
                                 "DSE derives the data center and rack of " + snitch + " with the legacy EC2 names only (" + names + "); HCD 2.0 " +
                                 "defaults to ec2_naming_scheme=standard and refuses to start on a ring with legacy names",
                                 "set ec2_naming_scheme=legacy in the cassandra-rackdc.properties of HCD 2.0 on every node, and carry dc_suffix" +
                                 dcSuffixNote()));
        }
        String clusterName = getString(yaml, "cluster_name");
        if (local != null && clusterName != null && local.clusterName != null && !clusterName.equals(local.clusterName))
            findings.add(warning("IDENTITY_MISMATCH",
                                 "DSE's cassandra.yaml says cluster_name " + clusterName + " but nodes/local records " + local.clusterName,
                                 "carry the cluster name the node runs with (nodes/local) into HCD 2.0"));
        Object numTokens = yaml.get("num_tokens");
        if (local != null && local.tokenCount != null && numTokens != null && !numTokens.toString().trim().equals(Integer.toString(local.tokenCount)))
            findings.add(warning("IDENTITY_MISMATCH",
                                 "DSE's cassandra.yaml says num_tokens " + numTokens + " but nodes/local records " + local.tokenCount + " tokens for this node",
                                 "carry num_tokens " + local.tokenCount + " into HCD 2.0: a node does not start with another number of tokens than it owns"));
    }

    /**
     * DSE's internode encryption ({@code server_encryption_options.internode_encryption} of its {@code cassandra.yaml}),
     * {@code none} when it sets none.
     */
    private static String internodeEncryption(Map<String, Object> yaml)
    {
        String value = getString(yaml, "server_encryption_options", "internode_encryption");
        return value == null ? "none" : value.trim();
    }

    /** DSE's {@code ssl_storage_port}, or its default (the default of HCD 2.0 too) when its {@code cassandra.yaml} sets none. */
    private static String sslStoragePort(Map<String, Object> yaml)
    {
        Object value = yaml.get("ssl_storage_port");
        return value == null ? DEFAULT_SSL_STORAGE_PORT : value.toString().trim();
    }

    /**
     * Internode TLS with DSE: in {@code DSE_6} mode HCD 2.0 listens with TLS on {@code ssl_storage_port} for the DSE nodes
     * and dials each one on the port its release listens on (DSE 6.8 and 6.9 before 6.9.7: {@code ssl_storage_port}; 6.9.7
     * and later: {@code storage_port}), so HCD 2.0's {@code legacy_ssl_storage_port_enabled} stays false and the operator
     * carries the TLS settings and {@code ssl_storage_port}, and checks that DSE's JVM options set no other
     * {@code -Dcassandra.ssl_storage_port} ({@link #CHECK_SSL_STORAGE_PORT_OPTION}). On DSE 6.9.7 and later, DSE's own
     * {@code enable_legacy_ssl_storage_port} must be true on every DSE node before the first upgrade: the other DSE
     * nodes go on dialing a node rolled back to DSE on its {@code ssl_storage_port}, where it listens only with that
     * setting. Blocking there when the setting is false or absent; a warning with the settings to carry in every other
     * case with internode encryption on (DSE 6.8 and 6.9.0 to 6.9.6 do not have the setting and always listen with TLS on
     * {@code ssl_storage_port}). The release is the one {@code nodes/local} records ({@link #checkLocalRelease}).
     */
    private void checkInternodeTls(Map<String, Object> yaml)
    {
        String encryption = internodeEncryption(yaml);
        if (yaml.isEmpty() || encryption.equalsIgnoreCase("none"))
            return;
        Object legacy = get(yaml, "server_encryption_options", "enable_legacy_ssl_storage_port");
        Object options = yaml.get("server_encryption_options");
        Map<?, ?> block = options instanceof Map ? (Map<?, ?>) options : Map.of();
        List<String> unknown = keysHcd20DoesNotHave(block);
        List<String> empty = emptyKeysHcd20Refuses(block);
        List<String> relative = new ArrayList<>();
        for (String store : List.of("keystore", "truststore"))
        {
            Object path = block.get(store);
            if (path != null && !path.toString().trim().startsWith("/"))
                relative.add(store + ": " + path.toString().trim());
        }
        String carry = "carry the keys DSE sets in server_encryption_options into the cassandra.yaml of HCD 2.0 under the names it accepts " +
                       "(internode_encryption: " + encryption + ", optional, require_client_auth, require_endpoint_verification, keystore, " +
                       "keystore_password, truststore, truststore_password, store_type, protocol, accepted_protocols, cipher_suites, " +
                       "algorithm)" +
                       (unknown.isEmpty() ? "" : "; leave out " + String.join(", ", unknown) + ", which HCD 2.0 does not have and refuses " +
                                                 "to start on" + storeTypeNote(block)) +
                       (empty.isEmpty() ? "" : "; leave out " + String.join(", ", empty) + ", which DSE's file sets to no value: HCD " +
                                               "2.0 refuses no value for these settings") +
                       (relative.isEmpty() ? "" : "; give " + String.join(" and ", relative) + (relative.size() == 1 ? " an absolute path" : " absolute paths") +
                                                  " (a relative path is resolved from the directory the process starts in, which need not " +
                                                  "be the same for DSE and HCD 2.0)") +
                       "; carry ssl_storage_port: " + sslStoragePort(yaml) + " (" + CHECK_SSL_STORAGE_PORT_OPTION + "); leave " +
                       "legacy_ssl_storage_port_enabled false and do not " +
                       "carry enable_legacy_ssl_storage_port, the old name of that HCD 2.0 setting: in DSE_6 mode HCD 2.0 listens with TLS " +
                       "on ssl_storage_port for the DSE nodes and dials each one on the port its release listens on";
        String sets = "DSE's cassandra.yaml sets internode_encryption: " + encryption + " (server_encryption_options)";
        String validated = "internode TLS between DSE and HCD 2.0 is validated on DSE 6.9.26 with all, rack and dc, in one data center " +
                           "and between two, and on DSE 6.8.65 with all in one data center only";
        boolean keyPresent = block.containsKey("enable_legacy_ssl_storage_port");
        legacyPortOn = isDseTrue(legacy);
        legacyPortValue = legacy;
        String line = keyPresent ? "change its line under server_encryption_options in DSE's cassandra.yaml to true (do not add a second one)"
                                 : "add it under server_encryption_options in DSE's cassandra.yaml";
        int[] minorAndPatch = DseCompat.dse6MinorAndPatch(localRelease);
        legacyPortRequired = requiresLegacySslStoragePort(minorAndPatch);
        if (legacyPortRequired == null)
        {
            String release = local == null ? "no nodes/local was read"
                                           : local.releaseVersion == null ? "nodes/local records no release"
                                                                          : "nodes/local records the release " + local.releaseVersion;
            findings.add(warning("INTERNODE_TLS",
                                 sets + ", and the TLS ports of this node's DSE release are not known (" + release + "); " + validated,
                                 (legacyPortOn ? "this cassandra.yaml sets enable_legacy_ssl_storage_port: " + shownTrueValue(legacy) + ": keep " +
                                                 "it on every DSE node if this node runs DSE 6.9.7 or later (required), and " +
                                                 "check that DSE was restarted since it was set: ss -tln lists ssl_storage_port as well as " +
                                                 "storage_port"
                                               : "if this node runs DSE 6.9.7 or later, set enable_legacy_ssl_storage_port: true on every DSE " +
                                                 "node (required): " + line + ", then " + legacyPortRestart()) +
                                 (keyPresent ? "; if it runs DSE 6.8 or 6.9.0 to 6.9.6, remove the enable_legacy_ssl_storage_port line instead " +
                                               "(DSE refuses a cassandra.yaml with a key it does not have)"
                                             : "") +
                                 "; on HCD 2.0, " + carry));
            return;
        }
        String dse = "DSE 6." + minorAndPatch[0] + '.' + minorAndPatch[1];
        if (!legacyPortRequired)
        {
            String dseSide = minorAndPatch[0] == 8
                             ? dse + " does not have enable_legacy_ssl_storage_port and always listens with TLS on ssl_storage_port, so DSE 6.8 " +
                               "needs no TLS change for the upgrade"
                             : dse + " predates enable_legacy_ssl_storage_port (DSE 6.9.7 is the first release with it) and the required 6.9 " +
                               "release (the latest DSE 6.9 patch release): after DSE's own upgrade to it, set enable_legacy_ssl_storage_port: " +
                               "true on every DSE node before the first upgrade (required); until then DSE always listens with TLS on " +
                               "ssl_storage_port";
            String remove = keyPresent ? "; remove the enable_legacy_ssl_storage_port line from DSE's cassandra.yaml until DSE runs 6.9.7 or " +
                                         "later: " + dse + " does not have that setting, and DSE refuses a cassandra.yaml with a key it does not have"
                                       : "";
            findings.add(warning("INTERNODE_TLS", sets + " on " + dse + "; " + validated, dseSide + remove + "; on HCD 2.0, " + carry));
            return;
        }
        if (legacyPortOn)
        {
            findings.add(warning("INTERNODE_TLS",
                                 sets + " on " + dse + ", with enable_legacy_ssl_storage_port: " + shownTrueValue(legacy) + ", as required; " +
                                 validated,
                                 "keep enable_legacy_ssl_storage_port: true on every DSE node (required), and check that DSE was restarted since it " +
                                 "was set: ss -tln lists ssl_storage_port as well as storage_port; on HCD 2.0, " + carry));
            return;
        }
        findings.add(blocking("INTERNODE_TLS",
                              sets + " on " + dse + ", and " +
                              (!keyPresent ? "does not set enable_legacy_ssl_storage_port (its default: false)"
                                           : legacy == null ? "sets enable_legacy_ssl_storage_port to no value"
                                                            : "sets enable_legacy_ssl_storage_port: " + shownYamlValue(legacy)) +
                              ": with internode TLS on DSE 6.9.7 and later, that setting must be true on every DSE node, or a node rolled " +
                              "back to DSE listens with TLS on storage_port only while the other DSE nodes go on dialing its " +
                              "ssl_storage_port, and they can see it DN until each of them is restarted",
                              "set enable_legacy_ssl_storage_port: true on every DSE node (required): " + line + ", then " + legacyPortRestart() +
                              ", and run this tool again; on HCD 2.0, " + carry));
    }

    /**
     * How DSE's {@code enable_legacy_ssl_storage_port} takes effect: a restart of each DSE node that did not run with it,
     * one at a time, before the first upgrade, or, once a peer runs HCD 2.0 ({@code UPGRADED_PEERS}), after reporting to
     * the upgrade owner.
     */
    private String legacyPortRestart()
    {
        String restart = "restart each DSE node whose cassandra.yaml did not set it to true (this tool's report of each node says), one at a " +
                         "time, each drained before it stops, with every other node up and every node up and normal (UN in nodetool status) " +
                         "before the next one, and check with ss -tln on each that DSE listens on ssl_storage_port";
        if (report.peers.stream().anyMatch(peer -> peer.upgraded))
            return "since the upgrade has started (UPGRADED_PEERS), report to the upgrade owner, then " + restart;
        return restart + ", before the first upgrade (in a rolling restart of DSE that is still to come before the upgrade, else in a " +
               "rolling restart of its own)";
    }

    /**
     * A value of DSE's YAML as the operator wrote it: a string in double quotes, exactly (spaces included), since DSE does
     * not read a string the way the file's loader here reads an unquoted value; anything else as the loader gives it.
     */
    private static String shownYamlValue(Object value)
    {
        return value instanceof String ? '"' + (String) value + '"' : String.valueOf(value);
    }

    /** {@link #shownYamlValue} of a value DSE reads as true, which says so when it is a string. */
    private static String shownTrueValue(Object value)
    {
        return shownYamlValue(value) + (value instanceof String ? " (true for DSE)" : "");
    }

    /**
     * Whether DSE reads a boolean setting as true: {@code true}, {@code yes} or {@code on} in any case, as SnakeYAML's
     * YAML 1.1 boolean reading ({@code ConstructYamlBool}), which a loader of Java bean properties such as DSE's applies to
     * a boolean property even when the value is quoted; the file's loader here gives the unquoted ones as a
     * {@link Boolean}.
     */
    @VisibleForTesting
    static boolean isDseTrue(@Nullable Object value)
    {
        if (value == null)
            return false;
        // no trim: ConstructYamlBool looks the value up as written, so a quoted value with spaces is no boolean for DSE
        String text = value.toString().toLowerCase(Locale.ROOT);
        return text.equals("true") || text.equals("yes") || text.equals("on");
    }

    /**
     * Whether internode TLS on a DSE release needs DSE's {@code enable_legacy_ssl_storage_port: true}:
     * true from DSE 6.9.7, the first release with that setting and with the port rule of {@code DseTlsPorts}; false for
     * DSE 6.8 and 6.9.0 to 6.9.6, which always listen with TLS on {@code ssl_storage_port}; null for any other release
     * (not a DSE 6 release, DSE 6.0 to 6.7, whose ports were not read, or a build number
     * {@link DseCompat#dse6MinorAndPatch}, which reads the release, cannot read).
     */
    @VisibleForTesting
    @Nullable
    static Boolean requiresLegacySslStoragePort(@Nullable CassandraVersion release)
    {
        return requiresLegacySslStoragePort(DseCompat.dse6MinorAndPatch(release));
    }

    @Nullable
    private static Boolean requiresLegacySslStoragePort(@Nullable int[] minorAndPatch)
    {
        if (minorAndPatch == null || (minorAndPatch[0] != 8 && minorAndPatch[0] != 9))
            return null;
        return minorAndPatch[0] == 9 && minorAndPatch[1] >= 7;
    }

    /**
     * What the remedy of {@code INTERNODE_TLS} says of DSE's {@code keystore_type} and {@code truststore_type}, which
     * HCD 2.0 replaces with one {@code store_type}: the value to set, or what to do when they differ; empty when DSE sets
     * neither.
     */
    private static String storeTypeNote(Map<?, ?> block)
    {
        Object keystoreType = block.get("keystore_type");
        Object truststoreType = block.get("truststore_type");
        if (keystoreType == null && truststoreType == null)
            return "";
        if (keystoreType != null && truststoreType != null && !keystoreType.toString().trim().equalsIgnoreCase(truststoreType.toString().trim()))
            return " (HCD 2.0 has one store_type for both stores, and DSE's keystore_type " + keystoreType.toString().trim() + " and truststore_type " +
                   truststoreType.toString().trim() + " differ: convert one store to the type of the other and set store_type to it)";
        if (keystoreType != null && truststoreType != null)
            return " (HCD 2.0 has one store_type for both stores: set store_type: " + keystoreType.toString().trim() + ')';
        // DSE's default type of the store it does not name is not recorded: the operator checks it
        String set = keystoreType != null ? "keystore" : "truststore";
        String other = keystoreType != null ? "truststore" : "keystore";
        Object type = keystoreType != null ? keystoreType : truststoreType;
        return " (HCD 2.0 has one store_type for both stores: set store_type: " + type.toString().trim() + " if DSE's " + other + " is of that " +
               "type too, else convert the " + other + " to it; DSE's " + set + "_type names only its " + set + ')';
    }

    /**
     * The keys of DSE's {@code server_encryption_options} that HCD 2.0's {@code cassandra.yaml} does not have, such as
     * DSE's {@code keystore_type} and {@code truststore_type}: HCD 2.0 refuses to start on them.
     * A key is known when it is one of {@link #hcd20ServerEncryptionKeys}, or a dotted name of nested properties whose
     * every part HCD 2.0's loader resolves, as it does; the values are not looked at.
     */
    @VisibleForTesting
    static List<String> keysHcd20DoesNotHave(Map<?, ?> serverEncryptionOptions)
    {
        Set<String> known = hcd20ServerEncryptionKeys();
        Set<String> unknown = new TreeSet<>();
        for (Object key : serverEncryptionOptions.keySet())
        {
            boolean isKnown = key instanceof String && (((String) key).contains(".") ? isNestedProperty((String) key) : known.contains(key));
            if (!isKnown)
                unknown.add(describeKey(key));
        }
        return new ArrayList<>(unknown);
    }

    /** Whether HCD 2.0's loader resolves every part of a dotted name from {@code ServerEncryptionOptions}, as it does. */
    private static boolean isNestedProperty(String name)
    {
        Class<?> type = EncryptionOptions.ServerEncryptionOptions.class;
        String[] parts = name.split("\\.");
        // a name of dots only has no part, and the loader refuses it
        if (parts.length == 0)
            return false;
        for (String part : parts)
        {
            Property property = Properties.defaultLoader().getProperties(type).get(part);
            if (property == null)
                return false;
            type = property.getType();
        }
        return true;
    }

    /**
     * The keys of DSE's {@code server_encryption_options} that are set to no value and that HCD 2.0's loader refuses so:
     * a property it has (under that name or as an old name), not marked {@code @Nullable}, whose default is not null
     * ({@code keystore}, {@code truststore}, {@code store_type}, the booleans, {@code internode_encryption}), or whose
     * default cannot even be read before the options are applied ({@code optional}, {@code enabled}): the same rule as
     * its loader, which fails the same way on those. An empty key HCD 2.0 accepts (a password, {@code algorithm}) is the
     * same as no key. Dotted names of nested properties are not examined (DSE writes none).
     */
    @VisibleForTesting
    static List<String> emptyKeysHcd20Refuses(Map<?, ?> serverEncryptionOptions)
    {
        Map<String, Property> properties = Properties.defaultLoader().getProperties(EncryptionOptions.ServerEncryptionOptions.class);
        Map<String, Replacement> oldNames = Replacements.getNameReplacements(EncryptionOptions.ServerEncryptionOptions.class)
                                                        .getOrDefault(EncryptionOptions.ServerEncryptionOptions.class, Map.of());
        EncryptionOptions.ServerEncryptionOptions defaults = new EncryptionOptions.ServerEncryptionOptions();
        Set<String> refused = new TreeSet<>();
        serverEncryptionOptions.forEach((key, value) -> {
            if (value != null || !(key instanceof String))
                return;
            String name = oldNames.containsKey(key) ? oldNames.get(key).newName : (String) key;
            Property property = properties.get(name);
            if (property == null || property.getAnnotation(Nullable.class) != null)
                return;
            Object defaultValue;
            try
            {
                defaultValue = property.get(defaults);
            }
            catch (RuntimeException e)
            {
                // the loader reads the default the same way before it sets no value, and fails the same way
                refused.add((String) key);
                return;
            }
            if (defaultValue != null)
                refused.add((String) key);
        });
        return new ArrayList<>(refused);
    }

    /**
     * The names HCD 2.0's configuration loader takes in {@code server_encryption_options}: the properties of
     * {@code ServerEncryptionOptions} its loader has (public fields and setters), and the old names it still reads
     * ({@code @Replaces}, such as {@code enable_legacy_ssl_storage_port}). The same sources as
     * {@code YamlConfigurationLoader}'s check, which refuses any other name.
     */
    @VisibleForTesting
    static Set<String> hcd20ServerEncryptionKeys()
    {
        Set<String> known = new TreeSet<>(Properties.defaultLoader().getProperties(EncryptionOptions.ServerEncryptionOptions.class).keySet());
        known.addAll(Replacements.getNameReplacements(EncryptionOptions.ServerEncryptionOptions.class)
                                 .getOrDefault(EncryptionOptions.ServerEncryptionOptions.class, Map.of()).keySet());
        return known;
    }

    /**
     * A key of DSE's YAML as the report names it: the YAML reader the tool uses (SnakeYAML's YAML 1.1 rules) reads some
     * unquoted keys as a boolean or as null, which HCD 2.0's loader takes as the text written (so it refuses {@code on},
     * not {@code true}): those are named with the ways they can be written. Any other key is named as the reader read it.
     */
    private static String describeKey(@Nullable Object key)
    {
        if (key == null)
            return "null (written ~, null, Null, NULL or nothing in DSE's file)";
        if (key instanceof Boolean)
            return key + ((Boolean) key ? " (written on, yes or true, in lower case, capitalised or upper case, in DSE's file)"
                                        : " (written off, no or false, in lower case, capitalised or upper case, in DSE's file)");
        return String.valueOf(key);
    }

    /** Whether a YAML value is set to something: neither absent, nor written with no value, nor an empty list, map or string. */
    @VisibleForTesting
    static boolean isSetValue(@Nullable Object value)
    {
        if (value == null)
            return false;
        if (value instanceof Collection)
            return !((Collection<?>) value).isEmpty();
        if (value instanceof Map)
            return !((Map<?, ?>) value).isEmpty();
        return !value.toString().trim().isEmpty();
    }

    /**
     * The compressor of a {@code hints_compression} setting, a list of one {@code class_name} with its {@code parameters}
     * in a {@code cassandra.yaml}: its {@code class_name}, or the value as written when it holds none.
     */
    private static String compressorOf(Object hintsCompression)
    {
        if (hintsCompression instanceof List && !((List<?>) hintsCompression).isEmpty() && ((List<?>) hintsCompression).get(0) instanceof Map)
        {
            Object className = ((Map<?, ?>) ((List<?>) hintsCompression).get(0)).get("class_name");
            if (className != null)
                return "class_name " + className;
        }
        return String.valueOf(hintsCompression);
    }

    private static boolean isDseClass(@Nullable String className)
    {
        return className != null && className.startsWith(DSE_CLASS_PREFIX);
    }

    /**
     * Whether a snitch class is one of the {@link #EC2_SNITCHES}, named by its simple name or in full in
     * {@code org.apache.cassandra.locator}. A class of that name in another package (DSE's own, or a third party's) is
     * not taken for it: the tool cannot tell whether it derives its names as {@code Ec2Snitch} does.
     */
    @VisibleForTesting
    static boolean isEc2Snitch(@Nullable String className)
    {
        if (className == null)
            return false;
        String trimmed = className.trim();
        String simpleName = trimmed.startsWith(LOCATOR_PACKAGE) ? trimmed.substring(LOCATOR_PACKAGE.length()) : trimmed;
        return EC2_SNITCHES.contains(simpleName);
    }

    /**
     * What the remedy of {@code EC2_NAMING} says of {@code dc_suffix}: its value when DSE's {@code cassandra-rackdc.properties}
     * sets one, that it sets none, or that the file cannot be read.
     */
    private String dcSuffixNote()
    {
        if (layout.unreadable.containsKey(NodeLayout.RACKDC))
            return " if DSE's cassandra-rackdc.properties sets one (it cannot be read here: see UNREADABLE_INPUT)";
        String suffix = layout.rackdcProperties.getProperty("dc_suffix");
        if (suffix == null)
            return " if DSE's cassandra-rackdc.properties sets one (it sets none here)";
        return "=" + shownDcSuffix(suffix) + " (DSE's value" +
               (needsQuotes(suffix) ? "; quoted here because its spaces are part of the data center name: copy DSE's dc_suffix line as it is)" : ")");
    }

    /**
     * DSE's {@code dc_suffix} as the report shows it: as it is, or quoted when it begins or ends with spaces, which
     * {@code java.util.Properties} keeps at the end of a value and HCD 2.0 adds to the data center name as they are.
     */
    private static String shownDcSuffix(String suffix)
    {
        return needsQuotes(suffix) ? "'" + suffix + "'" : suffix;
    }

    /** Whether a {@code dc_suffix} begins or ends with spaces, so that the report shows it quoted. */
    private static boolean needsQuotes(String suffix)
    {
        return !suffix.equals(suffix.strip());
    }

    /** Whether a class named in a configuration, possibly by its simple name in {@code defaultPackage}, is in this software. */
    private static boolean loadable(String className, String defaultPackage)
    {
        try
        {
            FBUtilities.classForName(className.contains(".") ? className : defaultPackage + className, "class");
            return true;
        }
        catch (RuntimeException e)
        {
            return false;
        }
    }

    // ---- data directories ----

    private void inspectDataDirectories()
    {
        Map<String, Map<String, List<String>>> formatProblemsFound = new TreeMap<>();
        boolean complete = true;
        for (File dataDirectory : layout.dataDirectories)
        {
            if (!dataDirectory.isDirectory())
            {
                findings.add(blocking("UNREADABLE_INPUT", "the data directory " + dataDirectory + " does not exist or is not a directory",
                                      "point the tool at the data directories of this DSE node (--data-dir)"));
                complete = false;
                continue;
            }
            File solrData = new File(dataDirectory, Dse6NonCoreTraces.SOLR_DATA);
            if (solrData.exists())
                findings.add(blocking("SOLR_DATA",
                                      "the DSE Search data directory " + solrData + " exists; an upgraded node refuses to start with it",
                                      "drop every search index on DSE, then delete it"));
            long bytes = 0;
            try
            {
                for (File keyspace : dataDirectory.listUnchecked(File::isDirectory))
                {
                    // every directory of a keyspace directory is a table directory, named <table>-<id>, or <table> for a table
                    // created before Cassandra 2.1; snapshots and backups live inside table directories
                    if (keyspace.name().equals(Dse6NonCoreTraces.SOLR_DATA))
                        continue; // reported above, and its index files are no sstables
                    for (File table : keyspace.listUnchecked(File::isDirectory))
                    {
                        try
                        {
                            bytes += inspectTableDirectory(keyspace.name() + '.' + tableName(table.name()), table, formatProblemsFound);
                        }
                        catch (RuntimeException e)
                        {
                            // a table DSE drops during the scan takes its directory away: nothing left to check
                            if (table.exists())
                            {
                                findings.add(blocking("UNREADABLE_INPUT", "cannot list the table directory " + table + " (" + rootCause(e) + ')',
                                                      "run the tool as the user DSE runs as (or as root)"));
                                complete = false;
                            }
                        }
                    }
                }
            }
            catch (RuntimeException e)
            {
                findings.add(blocking("UNREADABLE_INPUT", "cannot list the data directory " + dataDirectory + " (" + e.getMessage() + ')',
                                      "run the tool as the user DSE runs as (or as root)"));
                complete = false;
            }
            liveBytesByDataDirectory.put(dataDirectory.toString(), bytes);
        }
        for (Map.Entry<String, Map<String, List<String>>> problem : formatProblemsFound.entrySet())
        {
            List<String> items = new ArrayList<>();
            // the sstables without a TOC.txt of a table are named, up to WITHOUT_TOC_LISTED of them, so that each can be
            // matched with DSE's log line the remedy cites (list() names the first LISTED tables)
            boolean named = problem.getKey().equals(WITHOUT_TOC);
            problem.getValue().forEach((table, files) -> items.add(table + ": " + files.size() + " (" + (named ? withoutTocNames(files)
                                                                                                             : files.get(0) + (files.size() > 1 ? ", ..." : "")) + ')'));
            if (problem.getKey().equals(OLDER_TRIE))
            {
                findings.add(info("OLDER_TRIE", problem.getKey() + ": " + list(items)));
            }
            else if (problem.getKey().equals(WITHOUT_TOC))
            {
                findings.add(warning("SSTABLE_WITHOUT_TOC", problem.getKey() + ": " + list(items), withoutTocRemedy(problem.getValue())));
            }
            else
            {
                formatProblems.add(problem.getKey());
                findings.add(blocking("SSTABLE_FORMAT", problem.getKey() + ": " + list(items), formatRemedy(problem.getKey())));
            }
        }
        dataDirectoriesScanned = complete;
        if (!unreadableTransactionLogs.isEmpty())
            findings.add(warning("TXN_LOG_UNREADABLE",
                                 "transaction logs that cannot be read: " + list(unreadableTransactionLogs) + "; every sstable of these table directories " +
                                 "was checked instead, so a blocking format finding there may come from an sstable a DSE operation has already replaced, and " +
                                 "HCD 2.0 refuses to start while such a log remains",
                                 "run the tool again once DSE has finished the operation in progress; a log that remains must be gone once DSE is drained and " +
                                 "stopped (restart DSE, which completes or rolls back its transactions, if it is not) before the node is upgraded"));
        report.inputs.put("live_sstable_bytes", new LinkedHashMap<>(liveBytesByDataDirectory));
        checkDiskSpace();
    }

    /**
     * The remedy of {@code SSTABLE_WITHOUT_TOC}. With an sstable among them in a version DSE streams with zero-copy
     * ({@code ba} or {@code bb}: from {@code b}, before {@code c}), it names DSE's zero-copy streaming first, which writes
     * no TOC.txt for an sstable it receives, whole or in part, and DSE's start does not add the file (the same code in
     * DSE 6.8.65 and 6.9.26; observed with DSE 6.8.65 after a repair, and at its restart): a partially streamed sstable
     * is logged by {@code ZeroCopySSTableWriter} as "Writing partial component DATA" and also lacks its filter and
     * digest, a whole one as "Writing entire component DATA". In every DSE release 6.8.0 to 6.9.26 both lines are logged
     * at INFO by the class logger (with no level guard, as read in 6.8.65 and 6.9.26), and DSE's shipped logback.xml
     * sends INFO and above to system.log and DEBUG and above to debug.log, so the remedy sends the operator to
     * system.log or its archives only. Then it names an incomplete copy or restore, or a damaged disk; otherwise
     * ({@code aa}, which DSE does not stream with zero-copy, {@code big}, newer or unknown sstables) only these two
     * causes.
     */
    private static String withoutTocRemedy(Map<String, List<String>> byTable)
    {
        boolean zeroCopy = byTable.values().stream().flatMap(List::stream).anyMatch(NodeInspector::isZeroCopyVersion);
        if (!zeroCopy)
            return "find out why they have none (an incomplete copy or restore, a damaged disk) before the upgrade";
        return "find out why they have none before the upgrade; a likely cause right after a repair or another streaming operation is " +
               "DSE's zero-copy streaming, " +
               "which writes an sstable without TOC.txt (DSE 6.8 and 6.9 alike; a partially streamed one also without Filter.db and " +
               "Digest.crc32): if DSE's system.log or its archives have the line ZeroCopySSTableWriter ... Writing partial component " +
               "DATA to <the sstable's Data.db file> (or, for an sstable streamed whole, ... Writing entire component DATA to <the " +
               "sstable's Data.db file>) for every such sstable of the table (the finding names the first " + WITHOUT_TOC_LISTED + " of " +
               "each table), the warning can be accepted (HCD 2.0 reads such an sstable); otherwise look " +
               "for an incomplete copy or restore, or a damaged disk";
    }

    /** How many sstables without a TOC.txt {@code SSTABLE_WITHOUT_TOC} names per table. */
    static final int WITHOUT_TOC_LISTED = 20;

    /**
     * The first {@link #WITHOUT_TOC_LISTED} of the sstables without a TOC.txt of a table, by generation (numeric ones
     * first), then by name, and how many more there are.
     */
    private static String withoutTocNames(List<String> files)
    {
        List<String> sorted = new ArrayList<>(files);
        sorted.sort(Comparator.comparingLong(NodeInspector::numericGeneration).thenComparing(Comparator.naturalOrder()));
        if (sorted.size() <= WITHOUT_TOC_LISTED)
            return String.join(", ", sorted);
        return String.join(", ", sorted.subList(0, WITHOUT_TOC_LISTED)) + " and " + (sorted.size() - WITHOUT_TOC_LISTED) + " more";
    }

    /** The numeric generation of a {@code -Data.db} file name, or {@link Long#MAX_VALUE} when it has none. */
    private static long numericGeneration(String fileName)
    {
        List<String> tokens = Splitter.on('-').splitToList(fileName);
        if (tokens.size() != 4)
            return Long.MAX_VALUE;
        try
        {
            return Long.parseLong(tokens.get(1));
        }
        catch (NumberFormatException e)
        {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Whether a {@code -Data.db} file name is a trie sstable of DSE 6 in a version DSE streams with zero-copy: {@code bb},
     * or an older trie version from {@code b} on ({@code ba}), the versions from {@code b} and before {@code c}.
     */
    private static boolean isZeroCopyVersion(String fileName)
    {
        String problem = sstableProblem(fileName);
        // sstableProblem has checked the name: its first token is the version
        return problem == null || (problem.equals(OLDER_TRIE) && fileName.startsWith("b"));
    }

    /** The table of a table directory: {@code <table>} of {@code <table>-<id>}, or the name of a directory without an id. */
    private static String tableName(String directoryName)
    {
        int dash = directoryName.lastIndexOf('-');
        return dash > 0 && DseTableReader.isDirectoryOf(directoryName, directoryName.substring(0, dash)) ? directoryName.substring(0, dash) : directoryName;
    }

    /** Checks the sstables of a table directory (and of its secondary index directories); returns their size in bytes. */
    private long inspectTableDirectory(String table, File directory, Map<String, Map<String, List<String>>> problems)
    {
        long bytes = 0;
        for (File file : directory.listUnchecked(f -> true))
        {
            if (file.isDirectory())
            {
                // .<index name> holds the sstables of a legacy secondary index; snapshots and backups are not live
                if (file.name().startsWith("."))
                    bytes += inspectTableDirectory(table + " (index " + file.name().substring(1) + ')', file, problems);
                continue;
            }
            bytes += file.length();
        }
        // only the sstables of committed transactions: a running DSE writes the TOC.txt of an sstable last, and a flush
        // or a compaction in progress is no finding. This scan is advisory, so a transaction log it cannot read (a DSE
        // operation this build does not know, a race with a commit) falls back to every Data.db file of the directory:
        // files in progress (DSE 6 writes bb sstables with numeric generations) can then only add TOC warnings, but an
        // sstable an operation already replaced and not yet deleted is checked too, which the TXN_LOG_UNREADABLE warning
        // says. The schema read, which must be exact, refuses such a log instead (DseTableReader), as the startup of HCD 2.0
        // would.
        List<File> committed;
        try
        {
            committed = LifecycleTransaction.getFiles(directory.toPath(),
                                                      (file, type) -> type == Directories.FileType.FINAL && file.name().endsWith("-Data.db"),
                                                      Directories.OnTxnErr.IGNORE);
        }
        catch (RuntimeException e)
        {
            unreadableTransactionLogs.add(directory + " (" + rootCause(e) + ')');
            committed = List.of(directory.listUnchecked(f -> f.isFile() && f.name().endsWith("-Data.db")));
        }
        for (File file : committed)
        {
            String name = file.name();
            String problem = sstableProblem(name);
            if (problem != null)
                problems.computeIfAbsent(problem, p -> new TreeMap<>()).computeIfAbsent(table, t -> new ArrayList<>()).add(name);
            if (!new File(directory, name.substring(0, name.length() - "Data.db".length()) + "TOC.txt").exists())
                problems.computeIfAbsent(WITHOUT_TOC, p -> new TreeMap<>()).computeIfAbsent(table, t -> new ArrayList<>()).add(name);
        }
        return bytes;
    }

    static final String BIG_FORMAT = "sstables in the big format, which HCD 2.0 does not take from DSE 6 (DSE 6 writes the trie format bb)";
    static final String NEWER_BIG = "big sstables newer than me, which DSE 6 neither reads nor writes (written by another software into DSE's data directories)";
    static final String NEWER_TRIE = "trie sstables newer than bb, which DSE 6 does not write (written by another software into DSE's data directories)";
    static final String OLDER_TRIE = "trie sstables older than bb (written by DSE 6.0 to 6.7), which HCD 2.0 reads as they are";
    /**
     * Neither DSE 6.8 nor DSE 6.9 writes a missing TOC.txt at its start (the same code in DSE 6.8.65 and 6.9.26):
     * observed with DSE 6.8.65, whose restart left an sstable it had received by zero-copy streaming without a TOC.txt;
     * DSE 6.9 from its code only. Not covered: DSE's registration of extra components (an SAI index build) writes a
     * TOC.txt listing only those when there is none, a path not traced for a received sstable, and the sstable observed
     * had no SAI index.
     */
    static final String WITHOUT_TOC = "sstables without a TOC.txt (HCD 2.0 writes the missing one when it opens them; neither DSE 6.8 nor " +
                                      "DSE 6.9 writes it at its start)";
    /** The newest big version DSE 6 reads (the startup checks of {@code DSE_6} use the same). */
    static final String DSE_6_MAX_BIG_VERSION = "me";
    static final String NON_NUMERIC_ID = "sstables with a non-numeric identifier, which DSE 6 does not write";
    static final String UNKNOWN_NAME = "sstable files of an unknown format or name";

    /**
     * What is wrong with a {@code -Data.db} file name of a DSE node's data directory, or null when it is a {@code bb}
     * sstable; {@link #OLDER_TRIE} is a note, not a problem.
     */
    @Nullable
    static String sstableProblem(String fileName)
    {
        List<String> tokens = Splitter.on('-').splitToList(fileName);
        if (tokens.size() != 4)
            return UNKNOWN_NAME;
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSSTableFormats().get(tokens.get(2));
        if (format == null || !Version.validate(tokens.get(0)))
            return UNKNOWN_NAME;
        if (format instanceof BigFormat)
            return tokens.get(0).compareTo(DSE_6_MAX_BIG_VERSION) > 0 ? NEWER_BIG : BIG_FORMAT;
        if (!(format instanceof BtiFormat))
            return UNKNOWN_NAME;
        SSTableId id;
        try
        {
            id = SSTableIdFactory.instance.fromString(tokens.get(1));
        }
        catch (RuntimeException e)
        {
            return UNKNOWN_NAME;
        }
        if (!(id instanceof SequenceBasedSSTableId))
            return NON_NUMERIC_ID;
        if (tokens.get(0).compareTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION) > 0)
            return NEWER_TRIE;
        if (tokens.get(0).compareTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION) < 0)
            return OLDER_TRIE;
        return null;
    }

    private static String formatRemedy(String problem)
    {
        if (problem.equals(BIG_FORMAT))
            return "run nodetool upgradesstables on DSE first, so that every sstable is rewritten in DSE 6's trie format";
        return "find out what wrote these files: DSE cannot read them either; move them out of the data directories once their data is safe";
    }

    private void checkDiskSpace()
    {
        Map<FileStore, Long> liveBytes = new LinkedHashMap<>();
        Map<FileStore, List<String>> directories = new LinkedHashMap<>();
        for (File dataDirectory : layout.dataDirectories)
        {
            Long bytes = liveBytesByDataDirectory.get(dataDirectory.toString());
            if (bytes == null || !dataDirectory.isDirectory())
                continue;
            try
            {
                FileStore store = Files.getFileStore(dataDirectory.toPath());
                liveBytes.merge(store, bytes, Long::sum);
                directories.computeIfAbsent(store, s -> new ArrayList<>()).add(dataDirectory.toString());
            }
            catch (IOException e)
            {
                findings.add(warning("LOW_DISK_SPACE", "cannot read the free space of " + dataDirectory + " (" + e.getMessage() + ')', "check it with df"));
            }
        }
        Map<String, String> space = new LinkedHashMap<>();
        for (Map.Entry<FileStore, Long> entry : liveBytes.entrySet())
        {
            try
            {
                long usable = entry.getKey().getUsableSpace();
                space.put(String.join(",", directories.get(entry.getKey())), "sstables " + entry.getValue() + " bytes, usable " + usable + " bytes");
                if (usable < entry.getValue())
                    findings.add(warning("LOW_DISK_SPACE",
                                         "the file system of " + directories.get(entry.getKey()) + " has " + usable + " bytes free for " + entry.getValue() +
                                         " bytes of sstables: the upgrade snapshot keeps every current sstable on disk until it is cleared, so as compaction " +
                                         "rewrites them after the upgrade up to that much more space is needed",
                                         "free space, or plan to clear the snapshot (nodetool clearsnapshot -t pre-hcd-<timestamp>) as soon as a rollback " +
                                         "of this node is no longer needed"));
            }
            catch (IOException e)
            {
                findings.add(warning("LOW_DISK_SPACE", "cannot read the free space of " + directories.get(entry.getKey()) + " (" + e.getMessage() + ')', "check it with df"));
            }
        }
        report.inputs.put("disk_space", space);
    }

    // ---- hints, commit log, metadata ----

    private void inspectHints()
    {
        File directory = layout.hintsDirectory;
        if (!directory.isDirectory())
        {
            findings.add(info("HINTS", "no hints directory at " + directory));
            return;
        }
        Map<String, String> targets = new LinkedHashMap<>();
        for (DseNodeRecord peer : peers)
            if (peer.hostId != null)
                targets.put(peer.hostId.toString(), (peer.peer == null ? "(no address)" : peer.peer.getHostAddress()) +
                                                    (isUpgraded(peer) ? ", upgraded to HCD 2.0" : ", DSE"));
        if (local != null && local.hostId != null)
            targets.put(local.hostId.toString(), "this node");
        Map<String, Integer> notInRing = new TreeMap<>();
        List<String> notDse = new ArrayList<>();
        List<String> compressed = new ArrayList<>();
        List<String> unreadableHeaders = new ArrayList<>();
        try
        {
            for (File file : directory.listUnchecked(File::isFile))
            {
                Matcher matcher = HINTS_FILE.matcher(file.name());
                if (!matcher.matches())
                    continue;
                String hostId = matcher.group(1);
                if (matcher.group(3).isEmpty())
                {
                    notDse.add(file.name());
                    continue;
                }
                try
                {
                    // the names of the parameters only: the values of an encryption may hold key material
                    Map<String, Object> parameters = hintsFileParameters(file.toPath());
                    if (!parameters.isEmpty())
                        compressed.add(file.name() + " (" + String.join(", ", new TreeSet<>(parameters.keySet())) + ')');
                }
                catch (IOException | RuntimeException e)
                {
                    unreadableHeaders.add(file.name() + " (" + rootCause(e) + ')');
                }
                String target = targets.get(hostId);
                if (target == null)
                    notInRing.merge(hostId, 1, Integer::sum);
                else
                    pendingHintsByTarget.merge(hostId + " (" + target + ')', 1, Integer::sum);
            }
        }
        catch (RuntimeException e)
        {
            findings.add(warning("PENDING_HINTS", "cannot list the hints directory " + directory + " (" + e.getMessage() + ')',
                                 "check it by hand before the node is upgraded"));
            return;
        }
        if (!pendingHintsByTarget.isEmpty())
        {
            List<String> items = new ArrayList<>();
            pendingHintsByTarget.forEach((target, files) -> items.add(target + ": " + files + " file(s)"));
            findings.add(warning("PENDING_HINTS",
                                 "DSE hints files not delivered yet: " + list(items),
                                 "before this node is upgraded, wait until the files for DSE nodes are gone; HCD 2.0 converts and delivers the others at " +
                                 "its first start"));
        }
        if (!notInRing.isEmpty())
        {
            List<String> items = new ArrayList<>();
            notInRing.forEach((hostId, files) -> items.add(hostId + ": " + files + " file(s)"));
            findings.add(warning("PENDING_HINTS",
                                 "DSE hints files for host ids neither in nodes/local nor in nodes/peers: " + list(items) + "; they are dropped when HCD 2.0 " +
                                 "converts them, and their writes come back only with a repair",
                                 "find out which nodes they were for (nodetool status) before the upgrade"));
        }
        if (!compressed.isEmpty())
            findings.add(blocking("HINTS_COMPRESSION",
                                  "DSE hints files written compressed or encrypted (the parameters their header records): " + list(compressed) +
                                  "; HCD 2.0 cannot convert such a file at its start: it keeps the file and does not deliver its hints",
                                  "DSE writes such files while its cassandra.yaml sets hints_compression, or while dse.yaml sets enabled: true under " +
                                  "system_info_encryption, with which DSE encrypts its hints files through their compression (the header records the " +
                                  "encryption as a compression): remove hints_compression from DSE's cassandra.yaml and set enabled: false under " +
                                  "system_info_encryption in dse.yaml, each one that is set, and restart DSE on this node, so that the hints files it " +
                                  "writes from then on are neither compressed nor encrypted; the files written before keep their compression or " +
                                  "encryption: with every node up, and for encrypted files " + SYSTEM_KEY_DIRECTORY + " still in place, wait until DSE " +
                                  "has delivered them and they are gone, before the first node of the cluster is upgraded, then run this tool again; if " +
                                  "system_info_encryption was enabled, it also encrypted " + SYSTEM_INFO_ENCRYPTED_TABLES + ": once DSE runs with it " +
                                  "off on every node, run " + SYSTEM_INFO_REWRITE + ", with that directory still in place, before the first node of " +
                                  "the cluster is upgraded (this tool does not check that the rewrite is done); for encrypted files, " + SYSTEM_KEY_LOSS));
        if (!unreadableHeaders.isEmpty())
            findings.add(warning("HINTS_UNREADABLE",
                                 "DSE hints files whose header cannot be read, so whether they are compressed or encrypted is not known: " + list(unreadableHeaders),
                                 "run this tool again (DSE may have been writing them); if a file stays unreadable, find out why before the first node " +
                                 "of the cluster is upgraded: HCD 2.0 cannot convert a damaged hints file either"));
        if (!notDse.isEmpty())
            findings.add(warning("NON_DSE_HINTS",
                                 "hints files that DSE did not write (left by an earlier run of HCD 2.0 on this node): " + list(notDse) + "; DSE ignores them",
                                 "remove them, as the rollback of a node to DSE does: DSE never delivers them"));
    }

    /**
     * The parameters of the descriptor at the start of a DSE hints file, where DSE records its compression or encryption
     * (none for a file it neither compresses nor encrypts), read as {@code HintsDescriptor} lays it out: the version, the
     * timestamp, the host id, the length of the parameters and the CRC32 so far, the parameters as JSON and the CRC32 of
     * the whole; empty for an empty file (one DSE created and never wrote). Read here rather than through
     * {@code HintsDescriptor.deserialize}, which is not public: the tool leaves the server's hints code as it is.
     *
     * @throws IOException when the descriptor cannot be read whole or does not match its checksums
     */
    @VisibleForTesting
    static Map<String, Object> hintsFileParameters(Path file) throws IOException
    {
        if (Files.size(file) == 0)
            return Map.of();
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ))))
        {
            CRC32 crc = new CRC32();
            FBUtilities.updateChecksumInt(crc, in.readInt());
            for (int i = 0; i < 3; i++)
            {
                // the timestamp and the two halves of the host id, each checksummed low half first
                long value = in.readLong();
                FBUtilities.updateChecksumInt(crc, (int) (value & 0xFFFFFFFFL));
                FBUtilities.updateChecksumInt(crc, (int) (value >>> 32));
            }
            int length = in.readInt();
            FBUtilities.updateChecksumInt(crc, length);
            if (in.readInt() != (int) crc.getValue())
                throw new IOException("the checksum of its header does not match");
            if (length < 0 || length > MAX_HINTS_PARAMETERS_BYTES)
                throw new IOException("its header gives parameters of " + length + " bytes");
            byte[] parameters = new byte[length];
            in.readFully(parameters);
            crc.update(parameters, 0, length);
            if (in.readInt() != (int) crc.getValue())
                throw new IOException("the checksum of its parameters does not match");
            return JsonUtils.fromJsonMap(parameters);
        }
    }

    private static boolean isUpgraded(DseNodeRecord peer)
    {
        CassandraVersion release = parse(peer.releaseVersion);
        return release != null && isUpgradedRelease(release);
    }

    /**
     * Whether a release recorded for a peer has the major version of this build ({@link #upgradedMajor}): a node
     * already upgraded. Any other release that is not a DSE 6 one (DSE 5.1's {@code 3.11.x}, HCD 1.2's {@code 4.0.11.x},
     * ...) is a node that cannot be in the cluster at the upgrade.
     */
    static boolean isUpgradedRelease(CassandraVersion release)
    {
        return release.major == upgradedMajor();
    }

    /** The major version of the release of this build, or {@link #HCD_20_MAJOR} when the build does not know its release. */
    static int upgradedMajor()
    {
        return upgradedMajor(FBUtilities.getReleaseVersionString());
    }

    /** {@link #upgradedMajor()} for the release {@code buildRelease} of a build, null or not a version when it is not known. */
    @VisibleForTesting
    static int upgradedMajor(@Nullable String buildRelease)
    {
        CassandraVersion build = parse(buildRelease);
        return build == null ? HCD_20_MAJOR : build.major;
    }

    private void inspectCommitLog()
    {
        File directory = layout.commitLogDirectory;
        if (!directory.isDirectory())
        {
            findings.add(info("COMMITLOG", "no commit log directory at " + directory));
            return;
        }
        Map<Integer, Integer> versions = new TreeMap<>();
        List<String> replayed = new ArrayList<>();
        try
        {
            for (File file : directory.listUnchecked(File::isFile))
            {
                Matcher matcher = COMMITLOG_FILE.matcher(file.name());
                if (!matcher.matches())
                    continue;
                if (matcher.group(3) != null)
                {
                    replayed.add(file.name());
                    continue;
                }
                try
                {
                    versions.merge(Integer.parseInt(matcher.group(1)), 1, Integer::sum);
                }
                catch (NumberFormatException e)
                {
                    versions.merge(-1, 1, Integer::sum);
                }
            }
        }
        catch (RuntimeException e)
        {
            findings.add(warning("COMMITLOG_VERSION", "cannot list the commit log directory " + directory + " (" + e.getMessage() + ')', "check it by hand"));
            return;
        }
        report.inputs.put("commitlog_segments_by_version", versions);
        Map<Integer, Integer> others = new TreeMap<>(versions);
        others.remove(DSE_COMMITLOG_VERSION);
        if (!others.isEmpty())
            findings.add(warning("COMMITLOG_VERSION",
                                 "commit log segments of versions other than DSE's " + DSE_COMMITLOG_VERSION + " (version: count) " + others + "; DSE refuses to replay " +
                                 "them (written by another software)",
                                 "find out what wrote them; drain DSE before the upgrade"));
        if (!replayed.isEmpty())
            findings.add(info("COMMITLOG", "commit log segments already replayed by HCD 2.0 (left by an earlier run of HCD 2.0 on this node): " + list(replayed)));
    }

    private void inspectMetadataDirectory()
    {
        File directory = layout.metadataDirectory;
        File marker = new File(directory, DseUpgradeMarker.FILE_NAME);
        if (marker.exists())
            findings.add(info("MIXED_PHASE_MARKER", "the upgrade marker " + marker + " exists: HCD 2.0 writes it at its first start in DSE_6 mode (or " +
                                                    "someone created it by hand), and a rollback leaves it, in the mixed phase and after the last " +
                                                    "rollback alike; keep it while any node runs HCD 2.0 and, after the last rollback, while the " +
                                                    "node may be upgraded again; delete it once every node of the cluster runs HCD 2.0, before " +
                                                    "internode_compatibility_mode is set to NONE, or when the upgrade is abandoned for good; while it exists, " +
                                                    "HCD 2.0 refuses to start on this node with internode_compatibility_mode NONE" +
                                                    (marker.length() == 0 ? "; this one is empty: if HCD 2.0 has not started on this node since its " +
                                                                            "drain, remove it only if this node never ran HCD 2.0 before (an empty " +
                                                                            "marker may also be one made by hand at an earlier upgrade)" : "") +
                                                    "; DSE does not read it"));
        File nodes = new File(directory, "nodes");
        boolean ranHcd = new File(directory, DsePeersRecord.FILE_NAME).exists();
        if (nodes.isDirectory())
            ranHcd |= nodes.listUnchecked(f -> f.name().startsWith(CC4UpgradeNodesPersistence.IMPORT_MARKER_PREFIX)).length > 0;
        if (ranHcd)
            findings.add(info("NODE_RAN_HCD", "the metadata directory holds files of an earlier run of HCD 2.0 on this node (it was rolled back to DSE)"));
    }

    // ---- live state ----

    private void inspectLive()
    {
        report.live.putAll(live);
        List<String> unavailable = new ArrayList<>();
        List<String> unknownCommands = new ArrayList<>();
        live.forEach((command, result) -> {
            if (result.error == null && result.exitCode != null && result.exitCode != 0 && result.output != null && result.output.contains(UNKNOWN_COMMAND))
                unknownCommands.add("nodetool " + command);
            else if (result.error != null || result.exitCode == null || result.exitCode != 0)
                unavailable.add("nodetool " + command + ": " + (result.error != null ? result.error : "exit code " + result.exitCode + ' ' + firstLine(result.output)));
        });
        if (!unknownCommands.isEmpty())
            findings.add(info("LIVE_COMMAND_UNKNOWN", "commands the nodetool of this DSE release does not have, skipped: " + list(unknownCommands) +
                                                      " (the hints directory is checked on disk)"));
        if (!unavailable.isEmpty())
            findings.add(warning("LIVE_UNAVAILABLE", "the live state could not be read completely: " + list(unavailable),
                                 "check that DSE runs and that nodetool reaches it (host, port, user and password file: --nodetool-arg, with -pwf " +
                                 "<file> rather than -pw <password>, which the process list shows)"));

        String status = output("status");
        if (status != null)
        {
            List<String> notUp = new ArrayList<>();
            for (String line : status.split("\n"))
            {
                String trimmed = line.trim();
                if (trimmed.length() > 3 && trimmed.matches("^[UD][NLJM]\\s+\\S+.*") && !trimmed.startsWith("UN"))
                    notUp.add(trimmed.split("\\s+")[0] + ' ' + trimmed.split("\\s+")[1]);
            }
            if (!notUp.isEmpty())
                findings.add(warning("LIVE_STATUS", "nodes not up and normal (nodetool status): " + list(notUp),
                                     "every node must be UN before a node is upgraded"));
        }
        String describe = output("describecluster");
        if (describe != null)
        {
            Set<String> versions = new TreeSet<>();
            Matcher matcher = Pattern.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\\s*:").matcher(describe);
            while (matcher.find())
                versions.add(matcher.group(1));
            if (versions.size() > 1)
                findings.add(warning("LIVE_SCHEMA", "the cluster has " + versions.size() + " schema versions (nodetool describecluster): " + versions,
                                     "one schema version before the first upgrade; exactly two (DSE nodes, upgraded nodes) during the mixed phase"));
        }
        String nodesync = output("nodesyncservice status");
        if (nodesync != null && nodesync.toLowerCase().contains("running") && !nodesync.toLowerCase().contains("not running"))
            findings.add(warning("LIVE_NODESYNC", "the NodeSync service is running (nodetool nodesyncservice status)",
                                 "run nodetool nodesyncservice disable on every DSE node"));
        String compactions = output("compactionstats");
        if (compactions != null)
        {
            Matcher pending = Pattern.compile("pending tasks:\\s*(\\d+)").matcher(compactions);
            if (pending.find() && Long.parseLong(pending.group(1)) > 0)
                findings.add(warning("LIVE_COMPACTIONS", "compactions are pending (nodetool compactionstats: pending tasks " + pending.group(1) + ')',
                                     "let them finish before the node is upgraded"));
            if (compactions.contains("Validation") || compactions.contains("Anticompaction"))
                findings.add(warning("LIVE_REPAIR", "a repair is running (Validation or Anticompaction in nodetool compactionstats)",
                                     "let it finish; no repair may run during the upgrade"));
        }
        String netstats = output("netstats");
        if (netstats != null && !netstats.contains("Not sending any streams"))
            findings.add(warning("LIVE_STREAMS", "streams are running (nodetool netstats)",
                                 "let them finish before the node is upgraded"));
        String hints = output("listpendinghints");
        if (hints != null && !hints.contains("does not have any pending hints") && !hints.trim().isEmpty())
            findings.add(warning("LIVE_HINTS", "the node has pending hints (nodetool listpendinghints)",
                                 "wait until the hints for DSE nodes are delivered before the node is upgraded"));
    }

    /** The output of a live command that succeeded; null otherwise. */
    @Nullable
    private String output(String command)
    {
        NodeReport.LiveCommand result = live.get(command);
        return result == null || result.error != null || !Objects.equals(result.exitCode, 0) ? null : result.output;
    }

    private static String firstLine(@Nullable String text)
    {
        if (text == null)
            return "";
        String trimmed = text.trim();
        int newline = trimmed.indexOf('\n');
        return newline < 0 ? trimmed : trimmed.substring(0, newline);
    }

    // ---- upgrade path ----

    private void buildUpgradePath()
    {
        NodeReport.UpgradePath path = report.upgradePath;
        if (local != null)
            path.dseRelease = "DSE " + local.dseVersion + " (release " + local.releaseVersion +
                              (report.node.installedDseVersion == null ? "" : ", installation " + report.node.installedDseVersion) + ')';
        path.saiVersion = Dse6SaiVersionPin.saiVersionOf(localRelease);

        Map<String, Object> yaml = layout.cassandraConf;
        Map<String, String> settings = path.settings;
        settings.put("internode_compatibility_mode", InternodeCompatibilityMode.DSE_6.name());
        settings.put("storage_compatibility_mode", "HCD_1");
        settings.put("paxos_variant", "v1");
        settings.put("sstable.selected_format", BtiFormat.NAME);
        settings.put("uuid_sstable_identifiers_enabled", "false");
        if (path.saiVersion != null)
            settings.put("jvm-server.options", "-Dcassandra.sai.latest.version=" + path.saiVersion);
        carry(settings, yaml, "cluster_name");
        Object numTokens = yaml.get("num_tokens");
        Integer tokenCount = local == null ? null : local.tokenCount;
        if (numTokens != null)
            settings.put("num_tokens", numTokens.toString());
        else if (tokenCount != null)
            settings.put("num_tokens", tokenCount + " (DSE's cassandra.yaml sets none: the number of tokens of this node in nodes/local)");
        carry(settings, yaml, "initial_token");
        carry(settings, yaml, "allocate_tokens_for_local_replication_factor");
        carry(settings, yaml, "partitioner");
        String snitch = getString(yaml, "endpoint_snitch");
        String target = targetSnitch();
        settings.put("endpoint_snitch", target.equals(snitch) ? target : target + " (was " + (snitch == null ? "not set, DseSimpleSnitch" : snitch) + ')');
        if (isEc2Snitch(dseSnitch()))
        {
            List<String> notes = new ArrayList<>();
            String quoteNote = ec2DcSuffixQuoteNote();
            if (!quoteNote.isEmpty())
                notes.add(quoteNote);
            if (local != null)
                notes.add("DSE's names here: dc=" + local.dataCenter + ", rack=" + local.rack);
            settings.put("cassandra-rackdc.properties", "ec2_naming_scheme=legacy" + ec2DcSuffixSetting() +
                                                        (notes.isEmpty() ? "" : " (" + String.join("; ", notes) + ')'));
        }
        else if (local != null)
            settings.put("cassandra-rackdc.properties", "dc=" + local.dataCenter + ", rack=" + local.rack);
        Object seeds = get(yaml, "seed_provider");
        if (seeds instanceof List && !((List<?>) seeds).isEmpty())
            settings.put("seed_provider", seedsOf((List<?>) seeds));
        carry(settings, yaml, "listen_address");
        carry(settings, yaml, "listen_interface");
        carry(settings, yaml, "broadcast_address");
        if (get(yaml, "native_transport_address") != null)
            settings.put("rpc_address", getString(yaml, "native_transport_address") + " (native_transport_address of DSE)");
        carry(settings, yaml, "rpc_address");
        if (get(yaml, "native_transport_broadcast_address") != null)
            settings.put("broadcast_rpc_address", getString(yaml, "native_transport_broadcast_address") + " (native_transport_broadcast_address of DSE)");
        carry(settings, yaml, "storage_port");
        carry(settings, yaml, "ssl_storage_port");
        carry(settings, yaml, "native_transport_port");
        settings.put("data_file_directories", layout.dataDirectories.toString());
        settings.put("commitlog_directory", layout.commitLogDirectory.toString());
        settings.put("hints_directory", layout.hintsDirectory.toString());
        settings.put("metadata_directory", layout.metadataDirectory.toString());
        carry(settings, yaml, "saved_caches_directory");
        carry(settings, yaml, "cdc_raw_directory");
        carry(settings, yaml, "internode_compression");
        if (get(yaml, "server_encryption_options", "internode_encryption") != null)
            settings.put("server_encryption_options.internode_encryption", getString(yaml, "server_encryption_options", "internode_encryption"));
        if (!yaml.isEmpty() && !internodeEncryption(yaml).equalsIgnoreCase("none"))
        {
            settings.putIfAbsent("ssl_storage_port", DEFAULT_SSL_STORAGE_PORT + " (DSE's cassandra.yaml sets none: the default of both products)");
            settings.put("server_encryption_options.legacy_ssl_storage_port_enabled", "false (in DSE_6 mode HCD 2.0 listens on ssl_storage_port for DSE without it)");
        }
        if (get(yaml, "client_encryption_options", "enabled") != null)
            settings.put("client_encryption_options.enabled", getString(yaml, "client_encryption_options", "enabled"));
        String authenticator = getString(yaml, "authenticator");
        if (authenticator != null)
            settings.put("authenticator", mapped(authenticator, "DseAuthenticator", isTrue(get(layout.dseConf, "authentication_options", "enabled")) ? "PasswordAuthenticator" : "AllowAllAuthenticator"));
        String authorizer = getString(yaml, "authorizer");
        if (authorizer != null)
            settings.put("authorizer", mapped(authorizer, "DseAuthorizer", isTrue(get(layout.dseConf, "authorization_options", "enabled")) ? "CassandraAuthorizer" : "AllowAllAuthorizer"));
        String roleManager = getString(yaml, "role_manager");
        if (roleManager != null)
            settings.put("role_manager", mapped(roleManager, "DseRoleManager", "CassandraRoleManager"));

        List<String> steps = path.upgradeSteps;
        steps.add("Preflight: run this tool on every DSE node, collect the JSON reports and check them with dsepreflight --aggregate <directory>");
        if (isEc2Snitch(dseSnitch()))
        {
            String quoteNote = ec2DcSuffixQuoteNote();
            steps.add("EC2 names: set ec2_naming_scheme=legacy" + ec2DcSuffixSetting() + " in the cassandra-rackdc.properties of HCD 2.0 " +
                      "on every node (" + (quoteNote.isEmpty() ? "" : quoteNote + "; ") + "see EC2_NAMING)");
        }
        boolean internodeTls = !yaml.isEmpty() && !internodeEncryption(yaml).equalsIgnoreCase("none");
        if (internodeTls && Boolean.TRUE.equals(legacyPortRequired))
            steps.add("Internode TLS on DSE 6.9: " +
                      (legacyPortOn
                       ? "enable_legacy_ssl_storage_port: " + shownTrueValue(legacyPortValue) + " is set in this node's cassandra.yaml; " +
                         "check that DSE was restarted since " +
                         "(ss -tln lists ssl_storage_port); the report of every DSE node checks its own file, and the aggregate is READY " +
                         "only if all of them pass"
                       : "set enable_legacy_ssl_storage_port: true on every DSE node, then " + legacyPortRestart() + " (see INTERNODE_TLS)"));
        boolean dseYamlRead = !layout.unreadable.containsKey(NodeLayout.DSE_YAML);
        List<String> securityNotRead = new ArrayList<>();
        if (!dseYamlRead)
            securityNotRead.add("dse.yaml");
        securityNotRead.addAll(schema == null ? List.of("the schema", "system_auth.role_permissions") : schema.accessControlInputsNotRead());
        String securityIncomplete = securityNotRead.isEmpty() ? "" : "not checked completely (not read: " + String.join(", ", securityNotRead) + ')';
        if (has("RESTRICT", "ROW_LEVEL_ACCESS", "SCHEME_PERMISSIONS"))
            steps.add("Security: resolve the blocking access control findings above" + (securityIncomplete.isEmpty() ? "" : "; " + securityIncomplete));
        else if (warns("PROXY_GRANT", "DELEGATION", "LONE_WRITE_GRANT", "DSE_ONLY_PERMISSION", "DSE_ONLY_RESOURCE"))
            steps.add("Security: review the access control warnings above" + (securityIncomplete.isEmpty() ? "" : "; " + securityIncomplete));
        else if (!securityIncomplete.isEmpty())
            steps.add("Security: " + securityIncomplete);
        else
            steps.add("Security: no DSE access control this software ignores was found in dse.yaml, the schema and this node's replica of " +
                      "system_auth.role_permissions");
        List<String> notRead = new ArrayList<>();
        if (local == null)
            notRead.add("nodes/local");
        if (!dseYamlRead)
            notRead.add("dse.yaml");
        if (!dataDirectoriesScanned)
            notRead.add("every data directory");
        notRead.addAll(schema == null ? List.of("the schema") : schema.nonCoreInputsNotRead());
        if (has("NON_CORE_WORKLOAD", "NON_CORE_KEYSPACE", "GRAPH_KEYSPACE", "SEARCH_INDEX", "SOLR_DATA", "ADVANCED_REPLICATION", "DSEFS"))
            steps.add("DSE core only: remove the traces of DSE Search, Graph, Analytics, DSEFS and Advanced Replication listed above");
        else if (notRead.isEmpty())
            steps.add("DSE core only: no trace of DSE Search, Graph, Analytics, DSEFS or Advanced Replication was found");
        else
            steps.add("DSE core only: not checked completely (not read: " + String.join(", ", notRead) + ')');
        List<String> viewsNotRead = schema == null ? List.of("the schema") : schema.viewInputsNotRead();
        if (schema != null && !schema.viewsToDrop.isEmpty())
            steps.add("Materialized views: drop " + list(schema.viewsToDrop) + " before the upgrade" +
                      (viewsNotRead.isEmpty() ? "" : "; not checked completely (not read: " + String.join(", ", viewsNotRead) + ')'));
        else if (!viewsNotRead.isEmpty())
            steps.add("Materialized views: not checked completely (not read: " + String.join(", ", viewsNotRead) + ')');
        else
            steps.add("Materialized views: " + schema.viewCount + " view(s), none to drop");
        steps.add("NodeSync: " + (nodesyncDisabledInYaml ? "enabled: false is set under nodesync: in DSE's cassandra.yaml; check nodetool nodesyncservice status"
                                                         : "add enabled: false under nodesync: in DSE's cassandra.yaml and run nodetool nodesyncservice disable"));
        steps.add("SAI version pin: " + (path.saiVersion == null ? "no SAI version can be chosen for this release"
                                                                 : "add -Dcassandra.sai.latest.version=" + path.saiVersion + " to conf/jvm-server.options of HCD 2.0 before its first start"));
        steps.add("During the mixed phase: no topology or schema change, no repair and no upgradesstables");
        String p5;
        if (!dataDirectoriesScanned)
            p5 = "not checked (the data directories could not be inspected completely)";
        else if (formatProblems.isEmpty())
            p5 = "every sstable is in DSE 6's trie format";
        else if (formatProblems.equals(Set.of(BIG_FORMAT)))
            p5 = "run nodetool upgradesstables on DSE first";
        else if (formatProblems.contains(BIG_FORMAT))
            p5 = "run nodetool upgradesstables on DSE first, and move the files DSE 6 did not write out of the data directories (see SSTABLE_FORMAT)";
        else
            p5 = "move the files DSE 6 did not write out of the data directories once their data is safe (see SSTABLE_FORMAT)";
        steps.add("Sstable formats: " + p5);
        steps.add("Drivers: every driver pins native protocol V4; DSE continuous paging leaves no trace on disk, so check the drivers' settings " +
                  "(continuous paging is not available once a coordinator runs HCD 2.0)");
        steps.add("Hints: " + (has("HINTS_COMPRESSION") ? "compressed or encrypted DSE hints files, or hints_compression set: see HINTS_COMPRESSION; " : "") +
                  (pendingHintsByTarget.isEmpty() ? "no DSE hints file pending" : pendingHintsByTarget.size() + " target(s) with DSE hints files pending; see the warning above"));
        steps.add("Snapshot: nodetool flush, then nodetool snapshot -t pre-hcd-<UTC timestamp>; copy cassandra.yaml, dse.yaml and cassandra-rackdc.properties aside");
        steps.add("Install: install HCD 2.0 with the settings to carry listed here");
        if (internodeTls)
            steps.add("Internode TLS: carry the keys of server_encryption_options HCD 2.0 accepts and " +
                      "ssl_storage_port, leave legacy_ssl_storage_port_enabled false (see INTERNODE_TLS)");
    }

    /** The {@code dc_suffix} entry of the settings to carry for an EC2 snitch. */
    private String ec2DcSuffixSetting()
    {
        if (layout.unreadable.containsKey(NodeLayout.RACKDC))
            return ", dc_suffix as in DSE's cassandra-rackdc.properties (not read)";
        String suffix = layout.rackdcProperties.getProperty("dc_suffix");
        if (suffix == null)
            return "";
        return ", dc_suffix=" + shownDcSuffix(suffix);
    }

    /** What the settings and the upgrade step add when DSE's {@code dc_suffix} is shown quoted; empty otherwise. */
    private String ec2DcSuffixQuoteNote()
    {
        String suffix = layout.unreadable.containsKey(NodeLayout.RACKDC) ? null : layout.rackdcProperties.getProperty("dc_suffix");
        return suffix != null && needsQuotes(suffix) ? "quoted: copy DSE's dc_suffix line as it is" : "";
    }

    private boolean has(String... codes)
    {
        Set<String> wanted = Set.of(codes);
        return findings.stream().anyMatch(f -> f.severity == Finding.Severity.BLOCKING && wanted.contains(f.code));
    }

    /** Whether a warning with one of {@code codes} was found. */
    private boolean warns(String... codes)
    {
        Set<String> wanted = Set.of(codes);
        return findings.stream().anyMatch(f -> f.severity == Finding.Severity.WARNING && wanted.contains(f.code));
    }

    private static String mapped(String className, String dseSimpleName, String replacement)
    {
        return className.endsWith('.' + dseSimpleName) ? replacement + " (was " + className + ')' : className;
    }

    private static void carry(Map<String, String> settings, Map<String, Object> yaml, String key)
    {
        Object value = yaml.get(key);
        if (value != null)
            settings.put(key, value.toString());
    }

    private static String seedsOf(List<?> seedProviders)
    {
        Object first = seedProviders.get(0);
        if (first instanceof Map)
        {
            Object parameters = ((Map<?, ?>) first).get("parameters");
            if (parameters instanceof List && !((List<?>) parameters).isEmpty() && ((List<?>) parameters).get(0) instanceof Map)
            {
                Object seeds = ((Map<?, ?>) ((List<?>) parameters).get(0)).get("seeds");
                if (seeds != null)
                    return ((Map<?, ?>) first).get("class_name") + " seeds: " + seeds;
            }
        }
        return seedProviders.toString();
    }

    // ---- helpers ----

    /** The items in the order given, cut at {@link #LISTED}. */
    static String list(Collection<String> items)
    {
        if (items.size() <= LISTED)
            return String.join(", ", items);
        List<String> shown = new ArrayList<>(items).subList(0, LISTED);
        return String.join(", ", shown) + " and " + (items.size() - LISTED) + " more";
    }

    /** One or more names as a sentence names them: "a", "a and b", "a, b and c". */
    static String namesWithAnd(List<String> names)
    {
        int last = names.size() - 1;
        return last == 0 ? names.get(0) : String.join(", ", names.subList(0, last)) + " and " + names.get(last);
    }

    /** The message of the innermost cause of an error, which names what went wrong rather than what failed with it. */
    private static String rootCause(Throwable e)
    {
        Throwable root = Throwables.getRootCause(e);
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    @Nullable
    static CassandraVersion parse(@Nullable String release)
    {
        if (release == null)
            return null;
        try
        {
            return new CassandraVersion(release);
        }
        catch (RuntimeException e)
        {
            return null;
        }
    }

    private static boolean nonEmptyDirectory(Path path)
    {
        try (Stream<Path> entries = Files.list(path))
        {
            return entries.findAny().isPresent();
        }
        catch (IOException | RuntimeException e)
        {
            return false;
        }
    }

    @Nullable
    private static String string(@Nullable UUID uuid)
    {
        return uuid == null ? null : uuid.toString();
    }

    @Nullable
    private static String address(@Nullable InetAddress address)
    {
        return address == null ? null : address.getHostAddress();
    }
}
