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
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.tools.dsepreflight.Finding.blocking;
import static org.apache.cassandra.tools.dsepreflight.Finding.info;
import static org.apache.cassandra.tools.dsepreflight.Finding.warning;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.cannotRead;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.list;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.parse;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.rootCause;
import static org.apache.cassandra.tools.dsepreflight.NodeLayout.get;
import static org.apache.cassandra.tools.dsepreflight.NodeLayout.getString;
import static org.apache.cassandra.tools.dsepreflight.NodeLayout.isTrue;

/**
 * The preflight inspection of one DataStax Enterprise 5.1.51 node, the sibling of DSE 6's {@link NodeInspector}: DSE
 * 5.1 keeps what DSE 6 keeps in {@code nodes/local} and {@code nodes/peers} in the tables {@code system.local} and
 * {@code system.peers}, read here from their big sstables ({@link DseTableReader#readBig}); it has no metadata
 * directory, no NodeSync and no SAI, still runs Thrift, and writes big {@code me} sstables, commit log segments of
 * version {@value #COMMITLOG_VERSION} and hints files of version {@value #HINTS_VERSION}. It reads the node's files and
 * never writes to them; the optional {@code nodetool} commands are read-only ones ({@link LiveState#DSE51_COMMANDS}).
 * The checks both inspections make are package-private helpers of {@link NodeInspector}; the traces of the features
 * this software does not support are gathered with the rules of the node's own start check
 * ({@link Dse6NonCoreTraces.Dse51Markers}), and the schema and the access controls are checked by
 * {@link SchemaInspector#forDse51}. The report has the fields of a DSE 6 node's, and a section of its own
 * ({@link NodeReport.Dse51}).
 */
final class Dse51NodeInspector
{
    /** The release the texts and the report name. */
    static final String SOURCE = "DSE 5.1";
    /** The DSE version of the only DSE 5.1 release upgraded in place. */
    static final String DSE_VERSION = DseCompat.DSE51_GOSSIPED_DSE_VERSION;
    /**
     * The release DSE 5.1.51 records in {@code system.local}, {@code 3.11.3.5151}: the version of its Cassandra module
     * and the DSE version without its dots ({@link DseCompat#isDse51ReleaseVersion}).
     */
    static final String RELEASE_VERSION = "3.11.3." + DSE_VERSION.replace(".", "");
    /** The commit log version DSE 5.1.51 writes, the one a node in {@code DSE_5_1} mode replays. */
    static final int COMMITLOG_VERSION = CommitLogDescriptor.VERSION_30;
    /** The hints file version DSE 5.1.51 writes, in a file named {@code <host id>-<timestamp>-1.hints}. */
    static final int HINTS_VERSION = 1;
    /** {@code native_transport_port} when DSE's {@code cassandra.yaml} sets none: the default of both products. */
    static final String DEFAULT_NATIVE_TRANSPORT_PORT = "9042";
    /** The JVM option that decides whether DSE 5.1 starts Thrift, over {@code start_rpc} of {@code cassandra.yaml}. */
    static final String START_RPC_PROPERTY = "cassandra.start_rpc";
    /**
     * The JVM options a DSE 5.1 node must not run with set to true, as DSE reads them ({@code Boolean.parseBoolean}):
     * HCD 2.0 refuses the dialect of the first, and does not read the gossip state of the second.
     */
    static final List<String> REFUSED_PROPERTIES = List.of("cassandra.force_3_0_protocol_version",
                                                           "enable-old-dse-state");
    /** The JVM option that moves DSE Search's data directory, over {@code solr_data_dir} of {@code dse.yaml}. */
    static final String SOLR_DATA_DIR_PROPERTY = "dse.solr.data.dir";
    /** The JMX name of the gauge of the Thrift clients connected to a DSE 5.1 node, which this tool does not read. */
    static final String CONNECTED_THRIFT_CLIENTS =
        "org.apache.cassandra.metrics:type=Client,name=connectedThriftClients";
    /** The Java feature release HCD 2.0 is validated with for this upgrade. */
    static final int HCD_20_JAVA = 17;
    /** The tables DSE 5.1.51 encrypts while system_info_encryption is enabled in dse.yaml, sorted. */
    static final List<String> SYSTEM_INFO_ENCRYPTED_TABLE_NAMES = List.of("system.batches", "system.paxos");
    /** The encrypting compressors of DSE 5.1 ({@code org.apache.cassandra.io.compress} in DSE), which HCD 2.0 lacks. */
    static final Set<String> ENCRYPTING_COMPRESSORS = Set.of("EncryptingLZ4Compressor", "EncryptingSnappyCompressor",
                                                            "EncryptingDeflateCompressor");
    /** The codes of the findings of the traces of the features HCD 2.0 does not support ({@link #markerFinding}). */
    static final List<String> MARKER_CODES = List.of("NON_CORE_WORKLOAD", "SEARCH_INDEX", "GRAPH_KEYSPACE",
                                                     "NON_CORE_KEYSPACE", "ADVANCED_REPLICATION", "TIERED_STORAGE",
                                                     "REMOVED_COMPACTION");
    /** The codes of the blocking findings of the schema of DSE 5.1 a step of the upgrade path names. */
    static final List<String> SCHEMA_CODES = List.of("COMPACT_STORAGE", "SASI_INDEX", "UNLOADABLE_KEYSPACE",
                                                     "UNLOADABLE_TYPE", "UNLOADABLE_TABLE", "UNLOADABLE_CLASS",
                                                     "LEGACY_AUTH_TABLES", "JAVASCRIPT_UDF", "UDF_LANGUAGE");

    static final String SSTABLE_BIG_OLDER = "big sstables older than me (written by an earlier DSE 5.1 release, by " +
                                            "DSE 5.0 or earlier, or by Apache Cassandra 3.x), which the upgrade " +
                                            "requires rewritten at me on DSE 5.1 first";
    static final String SSTABLE_BIG_NEWER = "big sstables newer than me, which DSE 5.1 neither reads nor writes " +
                                            "(written by another software into DSE's data directories)";
    static final String SSTABLE_TRIE = "sstables in the trie format, which DSE 5.1 neither reads nor writes " +
                                       "(written by another software into DSE's data directories)";
    static final String SSTABLE_NON_NUMERIC_ID = "sstables with a non-numeric identifier, which DSE 5.1 does not " +
                                                 "write";
    static final String SSTABLE_UNKNOWN = NodeInspector.UNKNOWN_NAME;
    static final String SSTABLE_WITHOUT_TOC = "sstables without a TOC.txt (HCD 2.0 writes the missing one when it " +
                                              "opens them)";
    /** The version of an sstable named as before Cassandra 2.2, such as {@code ka}. */
    private static final Pattern LEGACY_VERSION = Pattern.compile("[a-z]{2}");

    private final NodeLayout layout;
    @Nullable
    private final Map<String, NodeReport.LiveCommand> live;
    private final NodeReport report = new NodeReport();
    private final NodeReport.Dse51 dse51 = new NodeReport.Dse51();
    private final List<Finding> findings = report.findings;

    /** the row of {@code system.local}; null when it could not be read */
    @Nullable
    private UntypedResultSet.Row local;
    /** the rows of {@code system.peers}, empty when none could be read */
    private List<UntypedResultSet.Row> peers = List.of();
    /** whether {@code system.peers} was read, with or without rows */
    private boolean peersRead;
    /** the options of DSE's {@code jvm.options}; null when it could not be read */
    @Nullable
    private List<String> jvmOptions;
    /** the inspection of the schema and the access controls, which reads the rows of system_schema the traces need */
    private SchemaInspector schema;
    /** whether the inspection of the schema completed, without an error {@link #guarded} reported */
    private boolean schemaInspected;
    private final Map<String, Long> liveBytesByDataDirectory = new LinkedHashMap<>();
    private final List<String> unreadableTransactionLogs = new ArrayList<>();
    /** the blocking kinds of sstable problem found in the data directories */
    private final Set<String> formatProblems = new TreeSet<>();
    /** whether every data directory was listed completely */
    private boolean dataDirectoriesScanned;
    private final Map<String, Integer> pendingHintsByTarget = new TreeMap<>();
    /** whether the hints directory could not be listed */
    private boolean hintsNotListed;
    /** the host ids with DSE 5.1 hints files that neither system.local nor system.peers records */
    private int hintHostIdsNotInRing;

    Dse51NodeInspector(NodeLayout layout, @Nullable Map<String, NodeReport.LiveCommand> live)
    {
        this.layout = layout;
        this.live = live;
    }

    NodeReport inspect(String hostname)
    {
        report.softwareVersion = FBUtilities.getReleaseVersionString();
        report.generatedAt = FBUtilities.now().toString();
        report.hostname = hostname;
        report.dse51 = dse51;
        dse51.source = SOURCE;
        guarded("the inputs", this::recordInputs);
        guarded("system.local and system.peers", this::inspectSystemTables);
        guarded("cassandra.yaml, dse.yaml and jvm.options", this::inspectConfiguration);
        // after the configuration, whose settings tell whether DSE enforces row-level access control
        schema = SchemaInspector.forDse51(layout.dataDirectories, NodeInspector.dsefsKeyspace(layout),
                                          NodeInspector.partitioner(layout), dse51.rowLevelAccessControlEnforced,
                                          findings);
        guarded("the schema and the access controls", this::inspectSchema);
        guarded("the traces of the features HCD 2.0 does not support", this::inspectMarkers);
        guarded("the data directories", this::inspectDataDirectories);
        guarded("the hints directory", this::inspectHints);
        guarded("the commit log directory", this::inspectCommitLog);
        findings.add(info("SSTABLES_ONLY",
                          "system.local, system.peers, system_schema and system_auth are read from their sstables, " +
                          "so changes a running DSE node has not flushed yet (a workload recorded, a schema change, " +
                          "a GRANT) are not seen: run nodetool flush system && nodetool flush system_schema && " +
                          "nodetool flush system_auth (three commands, one keyspace each) right before the tool"));
        guarded("the metadata directory", this::inspectMetadataDirectory);
        if (live != null)
            guarded("the live state", this::inspectLive);
        guarded("the upgrade path", this::buildUpgradePath);

        findings.sort(Comparator.comparing(f -> f.severity));
        report.result = report.blocked() ? NodeReport.BLOCKED : NodeReport.READY;
        return report;
    }

    /** Runs one part of the inspection, as {@link NodeInspector} does: an unexpected error is a blocking finding. */
    private void guarded(String what, Runnable part)
    {
        try
        {
            part.run();
        }
        catch (RuntimeException | AssertionError | FSError e)
        {
            findings.add(blocking("INSPECTION_ERROR",
                                  "the inspection of " + what + " failed (" + e + "), so it is incomplete",
                                  "report the error with the output of the tool; the node cannot be declared ready " +
                                  "until the inspection completes"));
        }
    }

    private void recordInputs()
    {
        report.inputs.put("dse_home", layout.dseHome == null ? null : layout.dseHome.toString());
        report.inputs.put("cassandra_yaml", layout.cassandraYaml.toString());
        report.inputs.put("dse_yaml", layout.dseYaml.toString());
        report.inputs.put("cassandra_rackdc_properties", layout.rackdc.toString());
        report.inputs.put("jvm_options", layout.jvmOptions.toString());
        List<String> data = new ArrayList<>();
        layout.dataDirectories.forEach(d -> data.add(d.toString()));
        report.inputs.put("data_file_directories", data);
        report.inputs.put("metadata_directory", layout.metadataDirectory.toString());
        report.inputs.put("commitlog_directory", layout.commitLogDirectory.toString());
        report.inputs.put("hints_directory", layout.hintsDirectory.toString());

        try
        {
            jvmOptions = layout.readJvmOptions();
        }
        catch (IOException | RuntimeException e)
        {
            findings.add(blocking("UNREADABLE_INPUT",
                                  cannotRead(NodeLayout.JVM_OPTIONS) + layout.jvmOptions + " (" + NodeLayout.reason(e) +
                                  "), so the checks of DSE's JVM options (Thrift, the protocol and gossip options) " +
                                  "cannot be made",
                                  "run the tool as the user DSE runs as (or as root), or point it at the file " +
                                  "(--jvm-options)"));
        }
        String snitch = getString(layout.cassandraConf, "endpoint_snitch");
        boolean rackdcUsed = snitch != null && snitch.endsWith(NodeInspector.GOSSIPING_PROPERTY_FILE_SNITCH);
        boolean ec2 = NodeInspector.isEc2Snitch(snitch);
        layout.unreadable.forEach((what, why) -> {
            if (what.equals(NodeLayout.RACKDC) && rackdcUsed)
                findings.add(warning("UNREADABLE_INPUT",
                                     cannotRead(what) + why + "; the data center and rack are taken from system.local",
                                     "pass --rackdc <file>: with GossipingPropertyFileSnitch the data center and " +
                                     "rack of HCD 2.0 come from it"));
            else if (what.equals(NodeLayout.RACKDC) && ec2)
                findings.add(warning("UNREADABLE_INPUT",
                                     cannotRead(what) + why + "; the dc_suffix " + snitch + " adds to the data " +
                                     "center is not known",
                                     "pass --rackdc <file>: with " + snitch + " HCD 2.0 needs the dc_suffix of DSE's " +
                                     "cassandra-rackdc.properties (EC2_NAMING)"));
            else if (what.equals(NodeLayout.RACKDC))
                findings.add(info("UNREADABLE_INPUT",
                                  cannotRead(what) + why + (snitch == null ? "" : " (not used by the snitch " + snitch +
                                                                                  ')')));
            else
                findings.add(blocking("UNREADABLE_INPUT",
                                      cannotRead(what) + why + ", so the checks that depend on it cannot be made",
                                      "run the tool as the user DSE runs as (or as root), or point it at the file " +
                                      "(--cassandra-yaml, --dse-yaml, --conf-dir, --dse-home)"));
        });
        // DSE 5.1 has no metadata directory: the one read is where HCD 2.0 keeps its files on this node
        List<String> assumed = new ArrayList<>();
        for (String directory : layout.defaultsAssumed)
            if (!directory.startsWith("metadata_directory:"))
                assumed.add(directory);
        if (!assumed.isEmpty())
            findings.add(info("DEFAULTS_ASSUMED",
                              "DSE's cassandra.yaml does not set these directories, DSE's defaults are assumed: " +
                              list(assumed)));
    }

    // ---- system.local and system.peers ----

    private void inspectSystemTables()
    {
        DseTableReader.TableRows localRows;
        try
        {
            localRows = DseTableReader.readBig(layout.dataDirectories, SchemaConstants.SYSTEM_KEYSPACE_NAME, "local",
                                               List.of("key"), List.of());
        }
        catch (IOException | RuntimeException e)
        {
            findings.add(blocking("UNREADABLE_INPUT",
                                  cannotRead("system.local") + rootCause(e) + ", so the DSE release, identity and " +
                                  "workloads of this node cannot be read",
                                  "run the tool again (DSE may have been compacting the table); if it persists, run " +
                                  "it as the user DSE runs as (or as root); a damaged system.local must be repaired " +
                                  "on DSE first"));
            return;
        }
        for (UntypedResultSet.Row row : localRows.rows)
            if ("local".equals(row.getString("key")))
                local = row;
        if (local == null)
        {
            findings.add(blocking("NOT_A_DSE_NODE",
                                  "no row of system.local in the data directories " + layout.dataDirectories +
                                  (localRows.found ? "" : " (no sstable of the table)") + ": the DSE release, " +
                                  "identity and workloads of this node cannot be read",
                                  localRows.found
                                  ? "run nodetool flush system on DSE (the row may be in its commit log only), then " +
                                    "run the tool again"
                                  : "point the tool at DSE's data directories (data_file_directories of DSE's " +
                                    "cassandra.yaml, or --data-dir)"));
        }
        else
        {
            NodeReport.Node node = report.node;
            node.hostId = NodeInspector.string(uuid(local, "host_id"));
            node.broadcastAddress = NodeInspector.address(inet(local, "broadcast_address"));
            node.listenAddress = NodeInspector.address(inet(local, "listen_address"));
            node.clusterName = text(local, "cluster_name");
            node.dataCenter = text(local, "data_center");
            node.rack = text(local, "rack");
            node.releaseVersion = text(local, "release_version");
            node.dseVersion = text(local, Dse51SystemTables.DSE_VERSION);
            node.schemaVersion = NodeInspector.string(uuid(local, "schema_version"));
            checkLocalRelease();
        }

        try
        {
            DseTableReader.TableRows peerRows = DseTableReader.readBig(layout.dataDirectories,
                                                                       SchemaConstants.SYSTEM_KEYSPACE_NAME, "peers",
                                                                       List.of("peer"), List.of());
            peers = peerRows.rows;
            peersRead = true;
            if (peers.isEmpty() && local != null)
                findings.add(info("NO_PEERS",
                                  "system.peers records no peer" +
                                  (peerRows.found ? "" : " (no sstable of the table)") + ": taken for a node " +
                                  "without peers, so no other node's release or workloads is checked; if " +
                                  "nodetool status lists other nodes, run nodetool flush system on this node and " +
                                  "run the tool again"));
        }
        catch (IOException | RuntimeException e)
        {
            findings.add(blocking("UNREADABLE_INPUT",
                                  cannotRead("system.peers") + rootCause(e) + "; the releases and workloads of the " +
                                  "peers cannot be checked",
                                  "run the tool again (DSE may have been compacting the table); if it persists, run " +
                                  "it as the user DSE runs as (or as root); a damaged system.peers must be repaired " +
                                  "on DSE first"));
        }
        checkPeers();
    }

    /** The release check: DSE 5.1.51 only, from {@code release_version} and {@code dse_version} of system.local. */
    private void checkLocalRelease()
    {
        String release = report.node.releaseVersion;
        String dseVersion = report.node.dseVersion;
        String installed = null;
        boolean unreadable = false;
        try
        {
            installed = layout.installedDseVersion();
        }
        catch (IOException | RuntimeException e)
        {
            unreadable = true;
            findings.add(warning("INSTALLED_RELEASE_UNKNOWN", "cannot read ds_version.txt: " + e.getMessage(),
                                 "check the release with dse -v"));
        }
        report.node.installedDseVersion = installed;
        if (isDse51551(release, dseVersion))
            findings.add(info("DSE_RELEASE",
                              "DSE " + DSE_VERSION + " (release " + release + ") is accepted, the only DSE 5.1 " +
                              "release upgraded in place to HCD 2.0"));
        else if (isDse51(release, dseVersion))
            findings.add(blocking("DSE_RELEASE",
                                  "this node runs " + describe(release, dseVersion) + "; DSE " + DSE_VERSION + " is " +
                                  "the only DSE 5.1 release upgraded in place to HCD 2.0",
                                  "upgrade every node to DSE " + DSE_VERSION + " first, following DataStax's " +
                                  "procedure"));
        else
            findings.add(blocking("DSE_RELEASE",
                                  "system.local records " + describe(release, dseVersion) + ", not DSE 5.1 (release " +
                                  "3.11.3.<build>, dse_version 5.1.<patch>): this node does not run DSE 5.1",
                                  "run the tool on DSE 5.1.51 nodes; a node already upgraded to HCD 2.0 needs no " +
                                  "preflight"));

        // an unreadable ds_version.txt is said above
        if (!unreadable && installed == null && layout.dseHome != null)
            findings.add(warning("INSTALLED_RELEASE_UNKNOWN",
                                 "no ds_version.txt in " + layout.dseHome + ", so the installed DSE release is not " +
                                 "compared with the running one",
                                 "pass --dse-home <DSE installation directory>, or check with dse -v that every node " +
                                 "runs the same release"));
        else if (!unreadable && installed == null)
            findings.add(warning("INSTALLED_RELEASE_UNKNOWN",
                                 "no DSE installation found (/opt/dse, /usr/share/dse, DSE_HOME), so the installed " +
                                 "DSE release is not compared with the running one",
                                 "pass --dse-home <DSE installation directory>"));
        if (installed != null && dseVersion != null)
        {
            String installedRelease = installed.contains("-") ? installed.substring(0, installed.indexOf('-'))
                                                              : installed;
            if (!installedRelease.equals(dseVersion))
                findings.add(blocking("INSTALLED_RELEASE_MISMATCH",
                                      "the DSE installation " + layout.dseHome + " is " + installed + " " +
                                      "(ds_version.txt) but this node last ran DSE " + dseVersion + " (system.local)",
                                      "restart DSE so that the node runs the installed release (or reinstall the " +
                                      "release it runs); every node must run DSE " + DSE_VERSION + " before the " +
                                      "upgrade"));
        }
    }

    /** Whether a release and a {@code dse_version}, either null, both say DSE 5.1.51, and at least one says it. */
    @VisibleForTesting
    static boolean isDse51551(@Nullable String release, @Nullable String dseVersion)
    {
        if (release == null && dseVersion == null)
            return false;
        return (release == null || RELEASE_VERSION.equals(release)) &&
               (dseVersion == null || DSE_VERSION.equals(dseVersion));
    }

    /** Whether a release or a {@code dse_version} is DSE 5.1's ({@link DseCompat#isDse51Peer}'s rule). */
    private static boolean isDse51(@Nullable String release, @Nullable String dseVersion)
    {
        return DseCompat.isDse51Peer(null, parse(release), dseVersion);
    }

    private static String describe(@Nullable String release, @Nullable String dseVersion)
    {
        return "the release " + release + (dseVersion == null ? " and no dse_version" : " and dse_version " +
                                                                                          dseVersion);
    }

    private void checkPeers()
    {
        List<String> otherReleases = new ArrayList<>();
        List<String> upgraded = new ArrayList<>();
        List<String> notDse51 = new ArrayList<>();
        List<String> unknown = new ArrayList<>();
        for (UntypedResultSet.Row row : peers)
        {
            InetAddress peer = inet(row, "peer");
            NodeReport.Peer entry = new NodeReport.Peer();
            entry.address = NodeInspector.address(peer);
            entry.hostId = NodeInspector.string(uuid(row, "host_id"));
            entry.dataCenter = text(row, "data_center");
            entry.rack = text(row, "rack");
            entry.releaseVersion = text(row, "release_version");
            entry.dseVersion = text(row, Dse51SystemTables.DSE_VERSION);
            entry.schemaVersion = NodeInspector.string(uuid(row, "schema_version"));
            CassandraVersion release = parse(entry.releaseVersion);
            entry.upgraded = release != null && NodeInspector.isUpgradedRelease(release);
            String shown = (peer == null ? "(no address)" : peer.getHostAddress()) + " (release " +
                           entry.releaseVersion + (entry.dseVersion == null ? "" : ", DSE " + entry.dseVersion) + ')';
            if (release == null && entry.dseVersion == null)
                unknown.add(entry.releaseVersion == null ? shown : shown + " in a form not understood");
            else if (entry.upgraded)
                upgraded.add(shown);
            else if (!isDse51(entry.releaseVersion, entry.dseVersion))
                notDse51.add(shown);
            else if (!isDse51551(entry.releaseVersion, entry.dseVersion))
                otherReleases.add(shown);
            report.peers.add(entry);
        }
        if (!otherReleases.isEmpty())
            findings.add(blocking("MIXED_RELEASES",
                                  "peers (system.peers) run another DSE 5.1 release than DSE " + DSE_VERSION + ": " +
                                  list(otherReleases),
                                  "upgrade every node to DSE " + DSE_VERSION + " before the upgrade"));
        if (!notDse51.isEmpty())
            findings.add(blocking("PEER_RELEASE",
                                  "peers recorded with a release that is neither DSE 5.1 nor HCD 2.0: " +
                                  list(notDse51) + "; every node must run DSE " + DSE_VERSION + " before the " +
                                  "upgrade (or already run HCD 2.0)",
                                  "upgrade those nodes to DSE " + DSE_VERSION + "; if an entry is stale (the node " +
                                  "has left the cluster), remove the node from the cluster on DSE (nodetool " +
                                  "removenode) so that DSE drops its row"));
        if (!upgraded.isEmpty())
            findings.add(info("UPGRADED_PEERS",
                              "peers already running HCD 2.0 (the cluster is in the mixed phase): " + list(upgraded)));
        if (!unknown.isEmpty())
            findings.add(warning("PEER_RELEASE_UNKNOWN",
                                 "peers whose release system.peers does not record, or records in a form not " +
                                 "understood: " + list(unknown),
                                 "check their release with nodetool version on each, or with this tool's report of " +
                                 "each"));
    }

    // ---- the schema and the access controls ----

    /**
     * The checks of the schema and of {@code system_auth.role_permissions} ({@link SchemaInspector#forDse51}), with
     * what the report and the aggregate take from them: the rows of {@code role_permissions} read, whether DSE checks
     * permissions, the views restricted to rows in this node's schema and the rows a failed {@code UNRESTRICT ROWS}
     * on a view left (both null when the schema or its views could not be read, or its inspection failed).
     */
    private void inspectSchema()
    {
        // from the yaml files only: known whether the inspection of the schema completes or not
        report.accessControls.authorizationEnabled = NodeInspector.authorizationEnabled(layout);
        schema.inspect();
        schemaInspected = true;
        report.accessControls.rolePermissionsRows = schema.rolePermissionsRead ? schema.rolePermissionsRows : null;
        if (!schema.schemaRead)
            return;
        // without system_schema.views neither is known, which the aggregate must not take for none
        if (schema.viewInputsNotRead().isEmpty())
        {
            dse51.restrictedViews = new ArrayList<>(schema.restrictedViews);
            dse51.viewTableRows = new ArrayList<>(schema.viewTableRows);
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("keyspaces", schema.keyspaceCount);
        summary.put("tables", schema.tableCount);
        summary.put("views", schema.viewCount);
        summary.put("keyspace_names", schema.keyspaceNames());
        // DSE 5.1 lists system and system_schema in its schema; the keyspaces of the traces are reported apart
        summary.put("keyspaces_left_out", schema.keyspacesLeftOut());
        report.inputs.put("schema", summary);
    }

    // ---- the traces of the features HCD 2.0 does not support ----

    /**
     * The traces the start of a node in {@code DSE_5_1} mode refuses (the workloads of {@code system.local} and
     * {@code system.peers}, the keyspaces, the indexes and the compaction of the tables and views of
     * {@code system_schema}, the {@code solr.data} directories), one blocking finding per feature, so that the
     * operator removes them on DSE 5.1 before the first node is upgraded. The rows of {@code system_schema} are those
     * the inspection of the schema read ({@link SchemaInspector#rowsOf}), which reports the tables it could not read.
     */
    private void inspectMarkers()
    {
        Dse6NonCoreTraces.Dse51Markers markers = new Dse6NonCoreTraces.Dse51Markers();
        if (local != null)
            markers.workloads(workloads(null, local));
        for (UntypedResultSet.Row row : peers)
            markers.workloads(workloads(inet(row, "peer"), row));

        Map<String, String> replication = schema.replicationClasses();
        if (replication != null)
        {
            markers.keyspaces(replication);
            String dsefs = NodeInspector.dsefsKeyspace(layout);
            if (dsefs != null && replication.containsKey(dsefs))
                markers.dsefsKeyspace(dsefs);
        }
        DseTableReader.TableRows indexes = schema.rowsOf(SchemaKeyspaceTables.INDEXES);
        if (indexes != null)
            for (UntypedResultSet.Row row : indexes.rows)
                markers.index(row.getString("keyspace_name"), row.getString("table_name"), row.getString("index_name"),
                              row.has("options")
                              ? row.getFrozenTextMap("options").get(IndexTarget.CUSTOM_INDEX_OPTION_NAME)
                              : null);
        DseTableReader.TableRows tables = schema.rowsOf(SchemaKeyspaceTables.TABLES);
        if (tables != null)
            for (UntypedResultSet.Row row : tables.rows)
                markers.table(row.getString("keyspace_name"), row.getString("table_name"), compactionClass(row));
        DseTableReader.TableRows views = schema.rowsOf(SchemaKeyspaceTables.VIEWS);
        if (views != null)
            for (UntypedResultSet.Row row : views.rows)
                markers.view(row.getString("keyspace_name"), row.getString("view_name"), compactionClass(row));
        markers.solrData(layout.dataDirectories.toArray(new File[0]));

        markers.byFeature().forEach((feature, traces) -> findings.add(markerFinding(feature, traces)));
    }

    @Nullable
    private static String compactionClass(UntypedResultSet.Row row)
    {
        return row.has("compaction") ? row.getFrozenTextMap("compaction").get("class") : null;
    }

    /** The workloads DSE 5.1 wrote in a row of {@code system.local} ({@code peer} null) or {@code system.peers}. */
    private static DseNodeWorkloads workloads(@Nullable InetAddress peer, UntypedResultSet.Row row)
    {
        List<String> workloads = row.has(Dse51SystemTables.WORKLOADS)
                                 ? new ArrayList<>(row.getFrozenSet(Dse51SystemTables.WORKLOADS, UTF8Type.instance))
                                 : List.of();
        Boolean graph = row.has(Dse51SystemTables.GRAPH) ? row.getBoolean(Dse51SystemTables.GRAPH) : null;
        return new DseNodeWorkloads(peer, workloads, graph, text(row, Dse51SystemTables.WORKLOAD));
    }

    /** The blocking finding of the traces of one feature, with what removes them on DSE 5.1. */
    @VisibleForTesting
    static Finding markerFinding(Dse6NonCoreTraces.Feature feature, List<String> traces)
    {
        String found = feature.displayName() + ": " + list(traces);
        String upgraded = "; a node in DSE_5_1 mode refuses to start while it finds them, and a change made on DSE " +
                          "5.1 once the upgrade has started does not reach the upgraded nodes";
        String before = "on DSE 5.1, before the first node is upgraded: ";
        switch (feature)
        {
            case WORKLOADS:
                return blocking("NON_CORE_WORKLOAD", found + "; HCD 2.0 supports only DSE core" + upgraded,
                                "restart each node listed as a plain Cassandra node (dse cassandra without -s, -k, " +
                                "-c or -g; for a package install, every workload of /etc/default/dse set to 0) " +
                                "while every other node is up, before the first node is upgraded; with " +
                                "DseSimpleSnitch the data center of a node is named after its workloads, so move " +
                                "such a node to a snitch that keeps its data center name first " +
                                "(GossipingPropertyFileSnitch, its dc and rack in cassandra-rackdc.properties); then " +
                                "run nodetool flush system on this node and run the tool again");
            case SEARCH:
                return blocking("SEARCH_INDEX", found + "; HCD 2.0 does not support DSE Search" + upgraded,
                                before + "drop the search indexes (DROP SEARCH INDEX ON <keyspace>.<table>), " +
                                "delete the solr.data directories once no search index is left, and drop the " +
                                "keyspace solr_admin once no node runs DSE Search");
            case GRAPH:
                return blocking("GRAPH_KEYSPACE", found + "; HCD 2.0 does not support DSE Graph" + upgraded,
                                before + "migrate the data of the graph keyspaces elsewhere and drop them");
            case ANALYTICS:
                return blocking("NON_CORE_KEYSPACE",
                                found + "; HCD 2.0 does not support DSE Analytics, DSEFS or CFS" + upgraded,
                                before + "migrate the data of these keyspaces elsewhere if it is needed and drop " +
                                "them, empty or not");
            case ADVANCED_REPLICATION:
                return blocking("ADVANCED_REPLICATION",
                                found + "; HCD 2.0 does not support DSE Advanced Replication" + upgraded,
                                before + "stop Advanced Replication, migrate what its keyspace holds if it is needed " +
                                "and drop it, empty or not");
            case TIERED_AND_IN_MEMORY:
                return blocking("TIERED_STORAGE", found + "; HCD 2.0 has neither compaction class" + upgraded,
                                before + "alter the tables and materialized views listed to a compaction strategy of " +
                                "Apache Cassandra (ALTER TABLE or ALTER MATERIALIZED VIEW ... WITH compaction)");
            case REMOVED_COMPACTION:
                return blocking("REMOVED_COMPACTION", "tables or materialized views on " + found + upgraded,
                                before + "alter them to TimeWindowCompactionStrategy (ALTER TABLE or ALTER " +
                                "MATERIALIZED VIEW ... WITH compaction = {'class': " +
                                "'TimeWindowCompactionStrategy', ...})");
            default:
                // the files of DSE 6 that cannot be read: no trace of a DSE 5.1 node
                return blocking("INSPECTION_ERROR", found, "report the error with the output of the tool");
        }
    }

    // ---- cassandra.yaml, dse.yaml and jvm.options ----

    private void inspectConfiguration()
    {
        Map<String, Object> yaml = layout.cassandraConf;
        Map<String, Object> dse = layout.dseConf;
        boolean yamlRead = !layout.unreadable.containsKey(NodeLayout.CASSANDRA_YAML);
        Map<String, String> properties = jvmProperties();

        Object startRpc = yaml.get("start_rpc");
        String startRpcOption = properties.get(START_RPC_PROPERTY);
        dse51.startRpc = startRpc == null ? null : startRpc.toString();
        dse51.startRpcJvmOption = startRpcOption;
        String thriftLive = live == null ? null : NodeInspector.output(live, "statusthrift");
        dse51.thriftLive = thriftLive == null ? null : thriftLive.trim();
        // the JVM option decides alone when it is set; cassandra.yaml otherwise
        if (jvmOptions != null && (yamlRead || startRpcOption != null))
            checkThrift(startRpc, startRpcOption);

        List<String> refused = new ArrayList<>();
        List<String> noEffect = new ArrayList<>();
        for (String property : REFUSED_PROPERTIES)
        {
            if (!properties.containsKey(property))
                continue;
            String option = "-D" + property + '=' + properties.get(property);
            if (Boolean.parseBoolean(properties.get(property)))
                refused.add(option);
            else
                noEffect.add(option);
        }
        if (!refused.isEmpty())
        {
            List<String> why = new ArrayList<>();
            if (refused.stream().anyMatch(o -> o.startsWith("-D" + REFUSED_PROPERTIES.get(0) + '=')))
                why.add("with -D" + REFUSED_PROPERTIES.get(0) + " DSE 5.1 speaks a messaging version HCD 2.0 refuses");
            if (refused.stream().anyMatch(o -> o.startsWith("-D" + REFUSED_PROPERTIES.get(1) + '=')))
                why.add("with -D" + REFUSED_PROPERTIES.get(1) + " it gossips the DSE state in the form of older " +
                        "releases, which HCD 2.0 does not read");
            findings.add(blocking("JVM_OPTIONS",
                                  "DSE's JVM options (" + layout.jvmOptions + ") set " + String.join(" and ", refused) +
                                  ": " + String.join(", and ", why),
                                  "remove " + (refused.size() == 1 ? "this option" : "these options") + " from DSE's " +
                                  "JVM options and restart DSE on every node that has " +
                                  (refused.size() == 1 ? "it" : "them") + ", one at a time, before the first node is " +
                                  "upgraded"));
        }
        if (jvmOptions != null)
            findings.add(info("JVM_OPTIONS_READ",
                              "DSE's JVM options were read from " + layout.jvmOptions + " only (its lines that start " +
                              "with -); options set in cassandra-env.sh, JVM_OPTS, JVM_EXTRA_OPTS or " +
                              "/etc/default/dse are not seen: check the command line of the running DSE process " +
                              "(ps) for -D" + START_RPC_PROPERTY + " and for " +
                              String.join(" and ", prefixed(REFUSED_PROPERTIES)) +
                              (noEffect.isEmpty() ? "" : "; " + String.join(" and ", noEffect) +
                                                         (noEffect.size() == 1 ? " is set, which has" : " are set, " +
                                                                                                        "which have") +
                                                         " no effect (only true has one)")));
        String solrDataDir = properties.get(SOLR_DATA_DIR_PROPERTY);
        // the directory of solr_data_dir is reported by the checks of dse.yaml
        if (solrDataDir != null && !solrDataDir.isEmpty() && new File(solrDataDir).exists() &&
            !solrDataDir.equals(getString(dse, "solr_data_dir")))
            findings.add(blocking("SOLR_DATA",
                                  "the DSE Search data directory " + solrDataDir + " (-D" + SOLR_DATA_DIR_PROPERTY +
                                  " in DSE's JVM options) exists",
                                  "drop every search index on DSE, then delete the directory"));

        checkJava();
        if (yamlRead && !yaml.isEmpty())
            NodeInspector.checkAuthentication(yaml, dse, findings);
        if (yamlRead)
        {
            checkSnitch(yaml);
            checkNativePorts(yaml);
            checkTls(yaml);
            NodeInspector.checkPartitioner(yaml, findings);
            checkCommitLogAndHintsSettings(yaml);
        }
        checkRowLevelAccessControl(yaml, dse);
        NodeInspector.checkDseYamlFeatures(dse, findings);
        if (isTrue(get(dse, "system_info_encryption", "enabled")))
            findings.add(blocking("TDE_TABLE",
                                  "system_info_encryption.enabled is true in dse.yaml: DSE 5.1 encrypts the tables " +
                                  NodeInspector.namesWithAnd(SYSTEM_INFO_ENCRYPTED_TABLE_NAMES) + ", its commit log, " +
                                  "its saved caches and its hints files; HCD 2.0 cannot read what DSE encrypted, and " +
                                  "encryption is out of scope of this upgrade",
                                  "set enabled: false under system_info_encryption in dse.yaml; restart DSE on every " +
                                  "node, with the directory system_key_directory names in dse.yaml (by default " +
                                  "/etc/dse/conf) still in place, so that DSE replays the commit log segments " +
                                  "written before and writes its commit log, saved caches and hints files " +
                                  "unencrypted from then on (HCD 2.0 skips a saved cache it cannot read); then run " +
                                  "nodetool upgradesstables -a system batches paxos on every node, so that those " +
                                  "tables are rewritten unencrypted (this tool does not check that the rewrite is " +
                                  "done); keep that directory in place until the rewrite is done and, with every " +
                                  "node up, DSE has delivered the hints files written before, which stay encrypted " +
                                  "(this tool names those left, under HINTS_COMPRESSION); all of it before the " +
                                  "first node of the cluster is upgraded"));
    }

    /**
     * The settings of DSE 5.1's {@code cassandra.yaml} that encrypt or compress what HCD 2.0 replays or delivers:
     * {@code transparent_data_encryption_options}, which encrypts the commit log and, in DSE 5.1, the hints files, and
     * {@code hints_compression}. Blocking, as for DSE 6.
     */
    private void checkCommitLogAndHintsSettings(Map<String, Object> yaml)
    {
        if (isTrue(get(yaml, "transparent_data_encryption_options", "enabled")))
            findings.add(blocking("TDE_COMMITLOG",
                                  "transparent_data_encryption_options.enabled is true in DSE's cassandra.yaml: DSE " +
                                  "5.1 encrypts the commit log and the hints files it writes; encryption is out of " +
                                  "scope of this upgrade, and HCD 2.0 was validated with neither encrypted",
                                  "set enabled: false under transparent_data_encryption_options and restart DSE on " +
                                  "every node, with the keystore that section names still in place, so that DSE " +
                                  "replays the commit log segments written before and writes its segments and hints " +
                                  "files unencrypted from then on; with every node up, wait until DSE has delivered " +
                                  "the hints files written before and they are gone; all of it before the first node " +
                                  "is upgraded"));
        Object hintsCompression = yaml.get("hints_compression");
        if (NodeInspector.isSetValue(hintsCompression))
            findings.add(blocking("HINTS_COMPRESSION",
                                  "DSE's cassandra.yaml sets hints_compression (" +
                                  NodeInspector.compressorOf(hintsCompression) + "): DSE 5.1 writes compressed hints " +
                                  "files, and HCD 2.0 was not validated with them",
                                  "remove hints_compression from DSE's cassandra.yaml and restart DSE on this node, " +
                                  "so that the hints files it writes from then on are not compressed; with every " +
                                  "node up, wait until DSE has delivered the files written before and they are gone, " +
                                  "before the first node of the cluster is upgraded (this tool names the compressed " +
                                  "or encrypted files left)"));
    }

    /**
     * Thrift, which HCD 2.0 does not have: on when DSE starts unless {@code -Dcassandra.start_rpc} of DSE's JVM
     * options, or without it {@code start_rpc} of its {@code cassandra.yaml} (true by default), turns it off;
     * blocking while on, with what {@code nodetool statusthrift} says beside it.
     */
    private void checkThrift(@Nullable Object startRpc, @Nullable String startRpcOption)
    {
        // the option as DSE 5.1 reads it (Boolean.parseBoolean), the yaml as its loader reads a boolean
        boolean on = startRpcOption != null ? Boolean.parseBoolean(startRpcOption) : !NodeLayout.isFalse(startRpc);
        dse51.thriftOn = on;
        String source = startRpcOption != null
                        ? "-D" + START_RPC_PROPERTY + '=' + startRpcOption + " in " + layout.jvmOptions
                        : startRpc == null ? "start_rpc is not set in DSE's cassandra.yaml (default true)"
                                           : "start_rpc: " + startRpc + " in DSE's cassandra.yaml";
        String liveNote = dse51.thriftLive == null ? "" : "; nodetool statusthrift says " + dse51.thriftLive;
        if (on)
            findings.add(blocking("THRIFT",
                                  "Thrift is on when DSE starts (" + source + ")" + liveNote + ": HCD 2.0 has no " +
                                  "Thrift, so Thrift clients break against an upgraded node; the number of Thrift " +
                                  "clients connected is the JMX gauge " + CONNECTED_THRIFT_CLIENTS + ", which this " +
                                  "tool does not read (it needs a JMX client)",
                                  "move every Thrift client to CQL; then set start_rpc: false in DSE's " +
                                  "cassandra.yaml, remove any -D" + START_RPC_PROPERTY + " from DSE's JVM options, " +
                                  "and run nodetool disablethrift (or restart DSE), on every node, before the first " +
                                  "node is upgraded"));
        else if (dse51.thriftLive != null && !dse51.thriftLive.contains("not running"))
            findings.add(warning("THRIFT",
                                 "Thrift is off when DSE starts (" + source + ")" + liveNote + ": it runs now, " +
                                 "started since by nodetool enablethrift or JMX, and Thrift clients connected to it " +
                                 "break when the node is upgraded",
                                 "move every Thrift client to CQL, then run nodetool disablethrift on this node " +
                                 "before it is upgraded"));
        else
            findings.add(info("THRIFT", "Thrift is off when DSE starts (" + source + ")" + liveNote));
    }

    /** The {@code -D} options of DSE's {@code jvm.options} by name, the last of a name; empty when not read. */
    private Map<String, String> jvmProperties()
    {
        Map<String, String> properties = new LinkedHashMap<>();
        if (jvmOptions == null)
            return properties;
        for (String option : jvmOptions)
        {
            if (!option.startsWith("-D"))
                continue;
            int equals = option.indexOf('=');
            properties.put(equals < 0 ? option.substring(2) : option.substring(2, equals),
                           equals < 0 ? "" : option.substring(equals + 1));
        }
        return properties;
    }

    private static List<String> prefixed(List<String> properties)
    {
        List<String> options = new ArrayList<>(properties.size());
        properties.forEach(p -> options.add("-D" + p));
        return options;
    }

    /**
     * The Java of the upgrade: DSE 5.1.51 starts only on Java 8 update 40 or later, which its start script checks, so
     * a node that runs it has one; HCD 2.0 is to run on the JDK this tool runs on, which the tool can see.
     */
    private void checkJava()
    {
        int feature = Runtime.version().feature();
        dse51.toolJavaVersion = Integer.toString(feature);
        String dseJava = "DSE 5.1.51 runs on Java 8 update 40 or later only (its start script refuses any other), " +
                         "which this tool, run on HCD 2.0's JDK, does not see; keep it for DSE until the node is " +
                         "upgraded";
        if (feature == HCD_20_JAVA)
            findings.add(info("JAVA", "this tool runs on Java " + feature + ", the JDK HCD 2.0 is to run on; " +
                                      dseJava));
        else
            findings.add(warning("JAVA",
                                 "this tool runs on Java " + feature + ", not on the JDK " + HCD_20_JAVA + " HCD 2.0 " +
                                 "is validated with for this upgrade; " + dseJava,
                                 "install JDK " + HCD_20_JAVA + " on the node, next to DSE's Java 8, and run HCD 2.0 " +
                                 "and this tool with it"));
    }

    /**
     * The snitch: DSE 5.1 runs the one {@code endpoint_snitch} names (it wraps it in its {@code DseDelegateSnitch} and
     * ignores {@code delegated_snitch} of {@code dse.yaml}); its own {@code DseSimpleSnitch}, which names the data
     * center after the workloads and the rack {@code rack1}, maps to {@code GossipingPropertyFileSnitch} with the data
     * center and rack of {@code system.local}.
     */
    private void checkSnitch(Map<String, Object> yaml)
    {
        String snitch = getString(yaml, "endpoint_snitch");
        String dcAndRack = local == null ? "the data center and rack of system.local"
                                         : "dc=" + text(local, "data_center") + " and rack=" + text(local, "rack") +
                                           " (from system.local)";
        String target = targetSnitch(snitch);
        String set = "set endpoint_snitch: " + target + " in the cassandra.yaml of HCD 2.0, with " + dcAndRack +
                     " in its cassandra-rackdc.properties";
        if (snitch == null)
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml sets no endpoint_snitch (DSE 5.1 refuses to start without " +
                                 "one), so the snitch DSE runs with is not known",
                                 set));
        else if (snitch.endsWith(NodeInspector.DSE_SIMPLE_SNITCH))
            findings.add(warning("SNITCH",
                                 "DSE's cassandra.yaml uses " + snitch + ", which HCD 2.0 does not have; it names " +
                                 "the data center after the node's workloads and the rack rack1",
                                 set + ", the names this node has now (a different one moves its data)"));
        else if (NodeInspector.isDseClass(snitch) || snitch.endsWith(NodeInspector.DSE_DELEGATE_SNITCH))
            findings.add(warning("SNITCH", "DSE's cassandra.yaml uses " + snitch + ", which HCD 2.0 does not have",
                                 set));
        else if (!NodeInspector.loadable(snitch, NodeInspector.LOCATOR_PACKAGE))
            findings.add(warning("SNITCH", "the snitch " + snitch + " of DSE's cassandra.yaml is not in HCD 2.0",
                                 "put its jar (built against HCD 2.0) in the lib directory of HCD 2.0, or use " +
                                 "GossipingPropertyFileSnitch with " + dcAndRack));
        if (snitch != null && snitch.endsWith(NodeInspector.GOSSIPING_PROPERTY_FILE_SNITCH) && local != null)
        {
            String dc = layout.rackdcProperties.getProperty("dc");
            String rack = layout.rackdcProperties.getProperty("rack");
            String recordedDc = text(local, "data_center");
            String recordedRack = text(local, "rack");
            if ((dc != null && !dc.trim().equals(recordedDc)) || (rack != null && !rack.trim().equals(recordedRack)))
                findings.add(warning("IDENTITY_MISMATCH",
                                     "cassandra-rackdc.properties says dc=" + dc + ", rack=" + rack + " but " +
                                     "system.local records " + recordedDc + '/' + recordedRack,
                                     "carry the data center and rack this node runs with into HCD 2.0 (check " +
                                     "nodetool status); a different one moves its data"));
        }
        if (NodeInspector.isEc2Snitch(snitch))
        {
            String suffix = layout.rackdcProperties.getProperty("dc_suffix");
            findings.add(warning("EC2_NAMING",
                                 "DSE 5.1 derives the data center and rack of " + snitch + " with the legacy EC2 " +
                                 "names only (" + dcAndRack + "); HCD 2.0 defaults to ec2_naming_scheme=standard and " +
                                 "refuses to start on a ring with legacy names",
                                 "set ec2_naming_scheme=legacy in the cassandra-rackdc.properties of HCD 2.0 on " +
                                 "every node, and carry dc_suffix" +
                                 (suffix == null ? " if DSE's cassandra-rackdc.properties sets one" : "=" + suffix)));
        }
        String clusterName = getString(yaml, "cluster_name");
        String recorded = local == null ? null : text(local, "cluster_name");
        if (clusterName != null && recorded != null && !clusterName.equals(recorded))
            findings.add(warning("IDENTITY_MISMATCH",
                                 "DSE's cassandra.yaml says cluster_name " + clusterName + " but system.local " +
                                 "records " + recorded,
                                 "carry the cluster name the node runs with (system.local) into HCD 2.0"));
        Object numTokens = yaml.get("num_tokens");
        Integer tokens = tokenCount();
        if (numTokens != null && tokens != null && !numTokens.toString().trim().equals(Integer.toString(tokens)))
            findings.add(warning("IDENTITY_MISMATCH",
                                 "DSE's cassandra.yaml says num_tokens " + numTokens + " but system.local records " +
                                 tokens + " tokens for this node",
                                 "carry num_tokens " + tokens + " into HCD 2.0: a node does not start with another " +
                                 "number of tokens than it owns"));
    }

    /** The snitch HCD 2.0 is to run with: DSE 5.1's when HCD 2.0 has it, else GossipingPropertyFileSnitch. */
    private static String targetSnitch(@Nullable String snitch)
    {
        if (snitch == null || NodeInspector.isDseClass(snitch) || snitch.endsWith(NodeInspector.DSE_SIMPLE_SNITCH) ||
            snitch.endsWith(NodeInspector.DSE_DELEGATE_SNITCH))
            return NodeInspector.GOSSIPING_PROPERTY_FILE_SNITCH;
        return snitch;
    }

    /** The number of tokens {@code system.local} records for this node; null when it records none. */
    @Nullable
    private Integer tokenCount()
    {
        if (local == null || !local.has("tokens"))
            return null;
        return local.getSet("tokens", UTF8Type.instance).size();
    }

    /**
     * The native ports: DSE 5.1.51 publishes no native port of a node, and the port of each event it sends to drivers
     * is its own, so drivers use one port for every node and each HCD 2.0 node keeps {@code native_transport_port}; a
     * second port, {@code native_transport_port_ssl}, blocks until the cluster has one port.
     */
    private void checkNativePorts(Map<String, Object> yaml)
    {
        Object port = yaml.get("native_transport_port");
        Object sslPort = yaml.get("native_transport_port_ssl");
        dse51.nativeTransportPort = port == null ? DEFAULT_NATIVE_TRANSPORT_PORT : port.toString().trim();
        dse51.nativeTransportPortSsl = sslPort == null ? null : sslPort.toString().trim();
        if (sslPort != null && !dse51.nativeTransportPortSsl.equals(dse51.nativeTransportPort))
            findings.add(blocking("NATIVE_PORTS",
                                  "DSE's cassandra.yaml sets native_transport_port " + dse51.nativeTransportPort +
                                  " and native_transport_port_ssl " + dse51.nativeTransportPortSsl + ": two native " +
                                  "ports; DSE 5.1 publishes no native port of a node, so drivers use one port for " +
                                  "every node, and an upgraded node would announce its plain port to a driver " +
                                  "connected on the TLS one",
                                  "move the cluster to one native port on DSE 5.1 before the first node is upgraded: " +
                                  "client encryption with optional: true on native_transport_port while the clients " +
                                  "move, then native_transport_port_ssl removed, on every node"));
        else
            findings.add(info("NATIVE_PORTS",
                              "native_transport_port " + dse51.nativeTransportPort +
                              (sslPort == null ? "" : " (native_transport_port_ssl the same)") + ": HCD 2.0 keeps " +
                              "it on every node until the last node is upgraded, since DSE 5.1 publishes no native " +
                              "port of a node and drivers use one port for every node"));
    }

    /**
     * The TLS settings of the internode and client encryption, reported: DSE 5.1.51 pins no protocol and no cipher
     * suite (its JDK chooses), and what JDK 17 cannot negotiate with Java 8 is not known yet, so nothing is blocked.
     */
    private void checkTls(Map<String, Object> yaml)
    {
        List<String> on = new ArrayList<>();
        String internode = getString(yaml, "server_encryption_options", "internode_encryption");
        if (internode != null && !internode.trim().equalsIgnoreCase("none"))
            on.add("internode_encryption: " + internode.trim());
        if (isTrue(get(yaml, "client_encryption_options", "enabled")))
            on.add("client_encryption_options.enabled: true");
        for (String section : List.of("server_encryption_options", "client_encryption_options"))
            for (String key : List.of("protocol", "algorithm", "cipher_suites"))
            {
                Object value = get(yaml, section, key);
                if (value != null)
                    dse51.tls.put(section + '.' + key, value.toString());
            }
        if (on.isEmpty())
            return;
        findings.add(info("TLS",
                          "DSE's cassandra.yaml sets " + String.join(" and ", on) + ", with " +
                          (dse51.tls.isEmpty() ? "no protocol, algorithm or cipher_suites (DSE 5.1's Java 8 " +
                                                 "chooses them)"
                                               : "the TLS settings " + dse51.tls) +
                          "; carry the TLS settings into HCD 2.0, which runs on JDK " + HCD_20_JAVA + ": check " +
                          "that the two negotiate before the first node is upgraded (a protocol or cipher suite one " +
                          "of the JDKs no longer enables fails the handshake)"));
    }

    /**
     * Row-level access control: enforced by DSE 5.1 only with {@code DseAuthorizer},
     * {@code authorization_options.enabled} and {@code allow_row_level_security}. The restrictions themselves are in
     * the schema; this check tells only whether DSE enforced them, and so which remedy applies.
     */
    private void checkRowLevelAccessControl(Map<String, Object> yaml, Map<String, Object> dse)
    {
        if (layout.unreadable.containsKey(NodeLayout.CASSANDRA_YAML) ||
            layout.unreadable.containsKey(NodeLayout.DSE_YAML))
            return;
        String authorizer = getString(yaml, "authorizer");
        boolean dseAuthorizer = authorizer != null && authorizer.endsWith(".DseAuthorizer");
        boolean allowed = isTrue(get(dse, "authorization_options", "allow_row_level_security"));
        boolean enforced = dseAuthorizer && isTrue(get(dse, "authorization_options", "enabled")) && allowed;
        dse51.rowLevelAccessControlEnforced = enforced;
        if (enforced)
            findings.add(warning("ROW_LEVEL_ACCESS",
                                 "DSE 5.1 enforces row-level access control on this node (DseAuthorizer, " +
                                 "authorization_options.enabled and allow_row_level_security true in dse.yaml): a " +
                                 "user who holds only rows/ grants on a restricted table sees only the rows they " +
                                 "name; HCD 2.0 does not enforce it, and refuses to start while a table or view is " +
                                 "restricted or a rows/ grant exists",
                                 "on DSE 5.1, before the first node is upgraded: give the users who read restricted " +
                                 "tables the access they need without row-level access control (or none), UNRESTRICT " +
                                 "ROWS on every restricted table, REVOKE the rows/ grants, then set " +
                                 "allow_row_level_security: false; drop a materialized view restricted to rows and " +
                                 "create it again without the restriction (see RESTRICTED_VIEW where a node reports " +
                                 "it; " +
                                 SchemaInspector.VIEW_REMEDY_NOT_VALIDATED + "; " +
                                 SchemaInspector.NEVER_UNRESTRICT_VIEW + ')'));
        else if (allowed)
            findings.add(info("ROW_LEVEL_ACCESS",
                              "allow_row_level_security is true in dse.yaml, but DSE 5.1 does not enforce row-level " +
                              "access control on this node (" +
                              (dseAuthorizer ? "authorization_options.enabled is not true"
                                             : "the authorizer is not DseAuthorizer") +
                              "): restrictions and rows/ grants left in the schema still keep HCD 2.0 from starting; " +
                              "remove them before the first node is upgraded: UNRESTRICT ROWS on a restricted table " +
                              "and REVOKE of the rows/ grants; drop a materialized view restricted to rows and " +
                              "create it again without the restriction (" + SchemaInspector.VIEW_REMEDY_NOT_VALIDATED +
                              "; never UNRESTRICT ROWS on a view)"));
    }

    // ---- data directories ----

    private void inspectDataDirectories()
    {
        Map<String, Map<String, List<String>>> problems = new TreeMap<>();
        // the solr.data directories are traces of DSE Search, reported with the other traces
        boolean complete = NodeInspector.scanDataDirectories(layout.dataDirectories, d -> {},
                                                             Dse51NodeInspector::sstableProblem, SSTABLE_WITHOUT_TOC,
                                                             problems, liveBytesByDataDirectory,
                                                             unreadableTransactionLogs, findings);
        for (Map.Entry<String, Map<String, List<String>>> problem : problems.entrySet())
        {
            List<String> items = new ArrayList<>();
            boolean named = problem.getKey().equals(SSTABLE_WITHOUT_TOC);
            problem.getValue().forEach((table, files) -> {
                String shown = named ? NodeInspector.withoutTocNames(files)
                                     : files.get(0) + (files.size() > 1 ? ", ..." : "");
                items.add(table + ": " + files.size() + " (" + shown + ')');
            });
            if (named)
            {
                findings.add(warning("SSTABLE_WITHOUT_TOC", problem.getKey() + ": " + list(items),
                                     "find out why they have none (an incomplete copy or restore, a damaged disk) " +
                                     "before the upgrade"));
            }
            else
            {
                formatProblems.add(problem.getKey());
                findings.add(blocking("SSTABLE_FORMAT", problem.getKey() + ": " + list(items),
                                      formatRemedy(problem.getKey())));
            }
        }
        dataDirectoriesScanned = complete;
        if (!unreadableTransactionLogs.isEmpty())
            findings.add(NodeInspector.transactionLogsUnreadable(unreadableTransactionLogs));
        report.inputs.put("live_sstable_bytes", new LinkedHashMap<>(liveBytesByDataDirectory));
        NodeInspector.checkDiskSpace(layout.dataDirectories, liveBytesByDataDirectory, findings, report.inputs);
    }

    /**
     * What is wrong with a {@code -Data.db} file name of a DSE 5.1 node's data directory, or null when it is a big
     * {@code me} sstable with a numeric generation, the only sstables a node in {@code DSE_5_1} mode writes.
     */
    @VisibleForTesting
    @Nullable
    static String sstableProblem(String fileName)
    {
        List<String> tokens = Splitter.on('-').splitToList(fileName);
        // <keyspace>-<table>-<version>-<generation>-Data.db: the names before Cassandra 2.2 (DSE 4.8), which DSE 5.1
        // reads and upgradesstables rewrites
        if (tokens.size() == 5 && LEGACY_VERSION.matcher(tokens.get(2)).matches() && tokens.get(2).compareTo("ma") < 0)
            return SSTABLE_BIG_OLDER;
        if (tokens.size() != 4)
            return SSTABLE_UNKNOWN;
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSSTableFormats().get(tokens.get(2));
        if (format == null || !Version.validate(tokens.get(0)))
            return SSTABLE_UNKNOWN;
        if (format instanceof BtiFormat)
            return SSTABLE_TRIE;
        if (!(format instanceof BigFormat))
            return SSTABLE_UNKNOWN;
        SSTableId id;
        try
        {
            id = SSTableIdFactory.instance.fromString(tokens.get(1));
        }
        catch (RuntimeException e)
        {
            return SSTABLE_UNKNOWN;
        }
        if (!(id instanceof SequenceBasedSSTableId))
            return SSTABLE_NON_NUMERIC_ID;
        int order = tokens.get(0).compareTo(InternodeCompatibilityMode.DSE_51_BIG_VERSION);
        if (order < 0)
            return SSTABLE_BIG_OLDER;
        if (order > 0)
            return SSTABLE_BIG_NEWER;
        return null;
    }

    private static String formatRemedy(String problem)
    {
        if (problem.equals(SSTABLE_BIG_OLDER))
            return "run nodetool upgradesstables on DSE 5.1 first, on every node, so that every sstable is " +
                   "rewritten at me";
        return "find out what wrote these files: DSE 5.1 cannot read them either; move them out of the data " +
               "directories once their data is safe";
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
        for (UntypedResultSet.Row row : peers)
        {
            UUID hostId = uuid(row, "host_id");
            InetAddress peer = inet(row, "peer");
            CassandraVersion release = parse(text(row, "release_version"));
            if (hostId != null)
                targets.put(hostId.toString(), (peer == null ? "(no address)" : peer.getHostAddress()) +
                                               (release != null && NodeInspector.isUpgradedRelease(release)
                                                ? ", upgraded to HCD 2.0"
                                                : ", DSE 5.1"));
        }
        UUID localHostId = local == null ? null : uuid(local, "host_id");
        if (localHostId != null)
            targets.put(localHostId.toString(), "this node");
        Map<String, Integer> notInRing = new TreeMap<>();
        List<String> otherVersions = new ArrayList<>();
        Map<HintsHeader, List<String>> headers = new TreeMap<>();
        List<String> unreadableHeaders = new ArrayList<>();
        try
        {
            for (File file : directory.listUnchecked(File::isFile))
            {
                Matcher matcher = NodeInspector.HINTS_FILE.matcher(file.name());
                if (!matcher.matches())
                    continue;
                // DSE 5.1 names its files with the version alone, DSE 6 with v and the version
                if (!matcher.group(3).isEmpty() || !Integer.toString(HINTS_VERSION).equals(matcher.group(4)))
                {
                    otherVersions.add(file.name());
                    continue;
                }
                try
                {
                    // the names of the parameters only: the values of an encryption may hold key material
                    Map<String, Object> parameters = NodeInspector.hintsFileParameters(file.toPath());
                    String shown = file.name() + " (" + String.join(", ", new TreeSet<>(parameters.keySet())) + ')';
                    for (HintsHeader kind : hintsHeader(parameters))
                        if (kind != HintsHeader.PLAIN)
                            headers.computeIfAbsent(kind, k -> new ArrayList<>()).add(shown);
                }
                catch (IOException | RuntimeException e)
                {
                    unreadableHeaders.add(file.name() + " (" + rootCause(e) + ')');
                }
                String target = targets.get(matcher.group(1));
                if (target == null)
                    notInRing.merge(matcher.group(1), 1, Integer::sum);
                else
                    pendingHintsByTarget.merge(matcher.group(1) + " (" + target + ')', 1, Integer::sum);
            }
        }
        catch (RuntimeException e)
        {
            hintsNotListed = true;
            findings.add(warning("PENDING_HINTS",
                                 "cannot list the hints directory " + directory + " (" + e.getMessage() + ')',
                                 "check it by hand before the node is upgraded"));
            return;
        }
        // in the order of the names, whatever the order of the listing
        otherVersions.sort(Comparator.naturalOrder());
        unreadableHeaders.sort(Comparator.naturalOrder());
        headers.values().forEach(files -> files.sort(Comparator.naturalOrder()));
        if (!pendingHintsByTarget.isEmpty())
        {
            List<String> items = new ArrayList<>();
            pendingHintsByTarget.forEach((target, files) -> items.add(target + ": " + files + " file(s)"));
            findings.add(warning("PENDING_HINTS",
                                 "DSE 5.1 hints files (version " + HINTS_VERSION + ") not delivered yet: " +
                                 list(items),
                                 "before this node is upgraded, wait until DSE has delivered them and they are gone " +
                                 "from its hints directory"));
        }
        hintHostIdsNotInRing = notInRing.size();
        if (!notInRing.isEmpty())
        {
            List<String> items = new ArrayList<>();
            notInRing.forEach((hostId, files) -> items.add(hostId + ": " + files + " file(s)"));
            if (local != null && peersRead)
                findings.add(warning("PENDING_HINTS",
                                     "DSE 5.1 hints files for host ids neither in system.local nor in system.peers: " +
                                     list(items) + "; they are never delivered, and their writes come back only " +
                                     "with a repair",
                                     "find out which nodes they were for (nodetool status) before the upgrade"));
            else
                findings.add(info("PENDING_HINTS",
                                  "DSE 5.1 hints files for host ids not checked against system.local and " +
                                  "system.peers, which could not both be read: " + list(items)));
        }
        if (!headers.isEmpty())
        {
            List<String> found = new ArrayList<>();
            List<String> settings = new ArrayList<>();
            headers.forEach((kind, files) -> {
                found.add(kind.description + ": " + list(files));
                if (kind.setting != null)
                    settings.add(kind.setting);
            });
            boolean unknown = headers.containsKey(HintsHeader.UNKNOWN_COMPRESSOR) ||
                              headers.containsKey(HintsHeader.UNKNOWN_PARAMETER);
            List<String> why = new ArrayList<>(3);
            if (headers.containsKey(HintsHeader.ENCRYPTED_BY_TDE) ||
                headers.containsKey(HintsHeader.ENCRYPTED_BY_SYSTEM_INFO))
                why.add("HCD 2.0 has neither DSE's encrypting compressors nor its keys");
            if (headers.containsKey(HintsHeader.COMPRESSED))
                why.add("HCD 2.0 was not validated with compressed DSE 5.1 hints files");
            if (unknown)
                why.add("what a compressor or a parameter HCD 2.0 does not know does to a file is not known, so the " +
                        "file is not taken for a plain one");
            findings.add(blocking("HINTS_COMPRESSION",
                                  "DSE 5.1 hints files whose header records a compression, an encryption or " +
                                  "another parameter: " + String.join("; ", found) + "; " + String.join(", and ", why),
                                  (unknown ? "find out what wrote the files with a compressor or a parameter HCD 2.0 " +
                                             "does not know; " : "") +
                                  (settings.isEmpty() ? "" : String.join(" and ", settings) + ", and ") +
                                  "restart DSE on this node, so that the hints files it writes from then on record " +
                                  "none of these; with every node up, wait until DSE has delivered the files written " +
                                  "before and they are gone, before the first node of the cluster is upgraded, then " +
                                  "run this tool again"));
        }
        if (!unreadableHeaders.isEmpty())
            findings.add(warning("HINTS_UNREADABLE",
                                 "DSE 5.1 hints files whose header cannot be read, so whether they are compressed or " +
                                 "encrypted is not known: " + list(unreadableHeaders),
                                 "run this tool again (DSE may have been writing them); if a file stays unreadable, " +
                                 "find out why before the first node of the cluster is upgraded"));
        if (!otherVersions.isEmpty())
            findings.add(blocking("NON_DSE_HINTS",
                                  "hints files of another version than DSE 5.1's " + HINTS_VERSION + " (left by an " +
                                  "earlier run of HCD 2.0 on this node): " + list(otherVersions) + "; DSE 5.1 does " +
                                  "not deliver them",
                                  "remove them, as the rollback of a node to DSE 5.1 does: DSE 5.1 never delivers " +
                                  "them"));
    }

    /** What the header of a DSE 5.1 hints file records, in the order the finding lists them. */
    enum HintsHeader
    {
        /** an {@code encryption} section, which {@code transparent_data_encryption_options} writes */
        ENCRYPTED_BY_TDE("an encryption (transparent_data_encryption_options)",
                         "set enabled: false under transparent_data_encryption_options in DSE's cassandra.yaml"),
        /** a compression by one of {@link #ENCRYPTING_COMPRESSORS}, which {@code system_info_encryption} sets */
        ENCRYPTED_BY_SYSTEM_INFO("an encryption (system_info_encryption)",
                                 "set enabled: false under system_info_encryption in dse.yaml"),
        /** a compression by a class this software does not have */
        UNKNOWN_COMPRESSOR("a compressor HCD 2.0 does not have", null),
        /** a parameter other than a compression or an encryption */
        UNKNOWN_PARAMETER("a header parameter HCD 2.0 does not know", null),
        /** a compression by a compressor of Apache Cassandra, which {@code hints_compression} sets */
        COMPRESSED("a compression (hints_compression)", "remove hints_compression from DSE's cassandra.yaml"),
        /** no parameter */
        PLAIN(null, null);

        final String description;
        @Nullable
        final String setting;

        HintsHeader(String description, @Nullable String setting)
        {
            this.description = description;
            this.setting = setting;
        }
    }

    /**
     * What the parameters of a hints file header record, each kind found: an encryption,
     * {@code transparent_data_encryption_options}'s {@code encryption} section or a compression by one of DSE 5.1's
     * encrypting compressors ({@link #ENCRYPTING_COMPRESSORS}, set by {@code system_info_encryption}), which this
     * software does not have; a compression by another class this software does not have, or by a class that is no
     * {@link ICompressor}; a compression of its own; and, beside any of them, a parameter other than
     * {@code compression} and {@code encryption}. {@link HintsHeader#PLAIN} alone when there is no parameter. Only the
     * class name of a compression is read, never a value of an encryption, and the class is looked up without being
     * initialised.
     */
    @VisibleForTesting
    static Set<HintsHeader> hintsHeader(Map<String, Object> parameters)
    {
        if (parameters.isEmpty())
            return Set.of(HintsHeader.PLAIN);
        Set<HintsHeader> kinds = EnumSet.noneOf(HintsHeader.class);
        if (!Set.of("compression", "encryption").containsAll(parameters.keySet()))
            kinds.add(HintsHeader.UNKNOWN_PARAMETER);
        if (parameters.containsKey("encryption"))
            kinds.add(HintsHeader.ENCRYPTED_BY_TDE);
        else if (parameters.containsKey("compression"))
            kinds.add(compressionOf(parameters.get("compression")));
        return kinds;
    }

    /** What the {@code compression} parameter of a hints file header records. */
    private static HintsHeader compressionOf(Object compression)
    {
        Object className = compression instanceof Map ? ((Map<?, ?>) compression).get("class_name") : null;
        if (className == null)
            return HintsHeader.UNKNOWN_COMPRESSOR;
        String name = className.toString().trim();
        if (ENCRYPTING_COMPRESSORS.contains(name.substring(name.lastIndexOf('.') + 1)))
            return HintsHeader.ENCRYPTED_BY_SYSTEM_INFO;
        try
        {
            Class<?> type = Class.forName(name.contains(".") ? name : "org.apache.cassandra.io.compress." + name, false,
                                          Dse51NodeInspector.class.getClassLoader());
            return ICompressor.class.isAssignableFrom(type) ? HintsHeader.COMPRESSED : HintsHeader.UNKNOWN_COMPRESSOR;
        }
        catch (ClassNotFoundException | LinkageError e)
        {
            return HintsHeader.UNKNOWN_COMPRESSOR;
        }
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
        List<String> unparsed = new ArrayList<>();
        try
        {
            for (File file : directory.listUnchecked(File::isFile))
            {
                Matcher matcher = NodeInspector.COMMITLOG_FILE.matcher(file.name());
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
                    // digits beyond an int
                    unparsed.add(file.name());
                }
            }
        }
        catch (RuntimeException e)
        {
            findings.add(warning("COMMITLOG_VERSION",
                                 "cannot list the commit log directory " + directory + " (" + e.getMessage() + ')',
                                 "check it by hand"));
            return;
        }
        unparsed.sort(Comparator.naturalOrder());
        replayed.sort(Comparator.naturalOrder());
        report.inputs.put("commitlog_segments_by_version", versions);
        if (!unparsed.isEmpty())
            report.inputs.put("commitlog_segments_version_unreadable", unparsed);
        Map<Integer, Integer> others = new TreeMap<>(versions);
        others.remove(COMMITLOG_VERSION);
        List<String> segments = new ArrayList<>(2);
        if (!others.isEmpty())
            segments.add("commit log segments of versions other than DSE 5.1's " + COMMITLOG_VERSION + " (version: " +
                         "count) " + others);
        if (!unparsed.isEmpty())
            segments.add("commit log segments whose version cannot be read: " + list(unparsed));
        if (!segments.isEmpty())
            findings.add(blocking("COMMITLOG_VERSION",
                                  String.join("; ", segments) + "; DSE 5.1 refuses to replay them (written by " +
                                  "another software)",
                                  "find out what wrote them, and move them out of the commit log directory once " +
                                  "their data is safe (a rollback to DSE 5.1 removes HCD 2.0's segments)"));
        if (!replayed.isEmpty())
            findings.add(info("COMMITLOG",
                              "commit log segments already replayed by HCD 2.0 (left by an earlier run of HCD 2.0 on " +
                              "this node): " + list(replayed)));
    }

    /**
     * The files HCD 2.0 writes in its metadata directory in {@code DSE_5_1} mode, read by their names: the marker of
     * the upgrade from DSE 5.1, the record of the DSE 5.1 peers, and the record of the values of DSE 5.1's columns of
     * {@code system.local} and {@code system.peers}, with its temporary file and its copies set aside. Each is the
     * sign of an earlier run of HCD 2.0 on this node, rolled back; none blocks, but for the record of the values of
     * DSE 5.1's columns beside the marker, which a rollback removes while it keeps the marker: with both, the next
     * start in {@code DSE_5_1} mode would load values read before the rollback. DSE 6's marker and record block, where
     * they refuse a start in {@code DSE_5_1} mode.
     */
    private void inspectMetadataDirectory()
    {
        File directory = layout.metadataDirectory;
        boolean assumed = layout.defaultsAssumed.stream().anyMatch(d -> d.startsWith("metadata_directory:"));
        String where = " (DSE 5.1 has no metadata directory: " + directory + " is the one read for HCD 2.0's files" +
                       (assumed ? ", the default of a package install" : ", given by --metadata-dir") + ')';
        if (assumed)
            findings.add(info("METADATA_DIRECTORY_ASSUMED",
                              "DSE 5.1 has no metadata directory: the files HCD 2.0 writes in DSE_5_1 mode are " +
                              "looked for in " + directory + "; pass --metadata-dir <directory> when HCD 2.0's " +
                              "cassandra.yaml sets another metadata_directory on this node"));
        // the start in DSE_5_1 is refused while DSE 6's marker exists, or its record lists a peer or cannot be read
        List<String> dse6Files = new ArrayList<>();
        Path dse6Marker = directory.toPath().resolve(DseUpgradeMarker.FILE_NAME);
        if (Files.exists(dse6Marker))
            dse6Files.add("the DSE 6 upgrade marker " + DseUpgradeMarker.FILE_NAME);
        else if (!Files.notExists(dse6Marker))
            dse6Files.add("the DSE 6 upgrade marker " + DseUpgradeMarker.FILE_NAME + " (or may hold it: its presence " +
                          "cannot be checked)");
        String dse6Record = dse6RecordState(directory.toPath().resolve(DsePeersRecord.FILE_NAME));
        if (dse6Record != null)
            dse6Files.add("the record of DSE 6 peers " + DsePeersRecord.FILE_NAME + ' ' + dse6Record);
        if (!dse6Files.isEmpty())
            findings.add(blocking("DSE6_UPGRADE_FILES",
                                  "the metadata directory" + where + " holds " + String.join(" and ", dse6Files) +
                                  ", files of the upgrade from DSE 6 (written by a start of HCD 2.0 in DSE_6 mode): " +
                                  "HCD 2.0 refuses to start on this node in DSE_5_1 mode while they are there",
                                  "find out why they are there: a node of a DSE 5.1 cluster takes no part in an " +
                                  "upgrade from DSE 6; delete them before the first start of HCD 2.0 in DSE_5_1 mode " +
                                  "once no DSE 6 node is in the cluster"));
        String record = Dse51SystemTables.RECORD_FILE_NAME;
        String markerName = DseUpgradeMarker.DSE_51_FILE_NAME;
        String peersRecordName = DsePeersRecord.DSE_51_FILE_NAME;
        Path marker = directory.toPath().resolve(markerName);
        // listed first: in a directory that can be listed but not searched, the names tell what is there
        List<String> records = new ArrayList<>();
        boolean listed = false;
        boolean markerListed = false;
        boolean peersRecordListed = false;
        boolean recordUnchecked = false;
        if (directory.isDirectory())
        {
            try
            {
                String temporary = record + Dse51SystemTables.TEMPORARY_SUFFIX;
                String unreadable = record + Dse51SystemTables.UNREADABLE_SUFFIX + '-';
                for (File file : directory.listUnchecked(f -> f.name().equals(record) || f.name().equals(temporary) ||
                                                              f.name().startsWith(unreadable) ||
                                                              f.name().equals(markerName) ||
                                                              f.name().equals(peersRecordName)))
                {
                    if (file.name().equals(markerName))
                        markerListed = true;
                    else if (file.name().equals(peersRecordName))
                        peersRecordListed = true;
                    else
                        records.add(file.name());
                }
                listed = true;
            }
            catch (RuntimeException e)
            {
                findings.add(warning("UNREADABLE_INPUT",
                                     "cannot list the metadata directory " + directory + " (" + rootCause(e) + ')',
                                     "check by hand whether it holds " + markerName + ", " + record + ", " +
                                     DseUpgradeMarker.FILE_NAME + " or " + DsePeersRecord.FILE_NAME + ": HCD 2.0 " +
                                     "refuses to start in DSE_5_1 mode while the presence of the last two cannot be " +
                                     "checked"));
                // a directory that can be searched but not listed still answers for the record by its name; in one
                // that can be neither searched nor listed the record may be there, and counts, as the marker does
                Path recordPath = directory.toPath().resolve(record);
                if (!Files.notExists(recordPath))
                {
                    records.add(record);
                    recordUnchecked = !Files.exists(recordPath);
                }
            }
        }
        boolean markerPresent = markerListed || Files.exists(marker);
        if (markerPresent)
            findings.add(info("MIXED_PHASE_MARKER",
                              "the DSE 5.1 upgrade marker " + marker + " exists" + where + ": HCD 2.0 writes it at " +
                              "its first start in DSE_5_1 mode, and a rollback leaves it; keep it while any node " +
                              "runs HCD 2.0 and, after the last rollback, while the node may be upgraded again; " +
                              "delete it, with " + record + ", once every node of the cluster runs HCD 2.0, with the " +
                              "node stopped and set to internode_compatibility_mode NONE, or when the upgrade is " +
                              "abandoned for good; while it exists, HCD 2.0 refuses to start on this node with NONE " +
                              "or DSE_6; DSE 5.1 does not read it"));
        if (peersRecordListed || Files.exists(directory.toPath().resolve(peersRecordName)))
            findings.add(info("NODE_RAN_HCD",
                              "the metadata directory" + where + " holds " + peersRecordName + ", a " +
                              "file of an earlier run of HCD 2.0 on this node in DSE_5_1 mode (it was rolled back to " +
                              "DSE 5.1)"));
        records.sort(Comparator.naturalOrder());
        // a rollback keeps the marker and removes the record: with both, the next start in DSE_5_1 mode keeps the
        // record and loads the values it holds, read before that rollback. The marker counts as there unless it is
        // known to be absent, as the start of HCD 2.0 counts it, which cannot read a directory's names
        String unchecked = " (or may hold it: its presence cannot be checked)";
        String markerText = markerPresent ? ""
                            : listed ? " (not listed, but HCD 2.0's start cannot check it and counts it as there)"
                            : unchecked;
        // a listed marker keeps counting even if it goes away between the listing and this look
        if (records.contains(record) && (markerPresent || !Files.notExists(marker)))
            findings.add(blocking("DSE51_SYSTEM_COLUMNS",
                                  "the metadata directory" + where + " holds " + list(records) +
                                  (recordUnchecked ? unchecked : "") + " and the DSE 5.1 upgrade marker" + markerText +
                                  ": a rollback to DSE 5.1 " + (recordUnchecked || !markerPresent ? "may have kept"
                                                                                                    : "kept") +
                                  " the record and the marker, so the next start of HCD 2.0 in DSE_5_1 mode on this " +
                                  "node would load the values of DSE 5.1's columns of system.local and system.peers " +
                                  "recorded before that rollback, instead of reading those DSE 5.1 wrote since",
                                  (markerPresent ? ""
                                                 : "give the user HCD 2.0 runs as read and search permission on " +
                                                   directory + " (HCD 2.0 cannot work in it otherwise), then, if " +
                                                   markerName + " is there, ") +
                                  "delete " + directory.toPath().resolve(record) + " and its " +
                                  Dse51SystemTables.TEMPORARY_SUFFIX + " file, if there, before the upgrade; keep " +
                                  markerName + ", as the rollback to DSE 5.1 does"));
        else if (!records.isEmpty())
            findings.add(info("DSE51_SYSTEM_COLUMNS",
                              "the metadata directory" + where + " holds " + list(records) + ": the values of DSE " +
                              "5.1's columns of system.local and system.peers that HCD 2.0 read at its first start " +
                              "in DSE_5_1 mode, files of the upgrade; delete them with the DSE 5.1 upgrade marker " +
                              "once every node runs HCD 2.0, with the node stopped and set to " +
                              "internode_compatibility_mode NONE"));
    }

    /**
     * What keeps the record of DSE 6 peers {@code record} from letting a start in {@code DSE_5_1} go on, as the
     * startup check reads it, failing closed: {@code listing <n> peers}, {@code that cannot be read (<cause>)}, or
     * {@code (or may hold it: its presence cannot be checked)}; null when it is absent or lists no peer (its comment
     * lines and empty lines left out).
     */
    @Nullable
    private static String dse6RecordState(Path record)
    {
        if (Files.notExists(record))
            return null;
        if (!Files.exists(record))
            return "(or may hold it: its presence cannot be checked)";
        try
        {
            long entries = Files.readAllLines(record, StandardCharsets.UTF_8)
                                .stream()
                                .map(String::trim)
                                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                                .count();
            return entries == 0 ? null : "listing " + entries + (entries == 1 ? " peer" : " peers");
        }
        catch (IOException | RuntimeException e)
        {
            return "that cannot be read (" + rootCause(e) + ')';
        }
    }

    // ---- live state ----

    private void inspectLive()
    {
        report.live.putAll(live);
        NodeInspector.checkLiveCommands(live, findings);
        NodeInspector.checkLiveStatus(NodeInspector.output(live, "status"), findings);
        NodeInspector.checkLiveSchema(NodeInspector.output(live, "describecluster"), findings);
        NodeInspector.checkLiveCompactions(NodeInspector.output(live, "compactionstats"), findings);
        NodeInspector.checkLiveStreams(NodeInspector.output(live, "netstats"), findings);
        String hints = NodeInspector.output(live, "listendpointspendinghints");
        if (hints != null && !hints.trim().isEmpty() && !hints.contains("does not have hints for other endpoints"))
            findings.add(warning("LIVE_HINTS", "the node has pending hints (nodetool listendpointspendinghints)",
                                 "wait until the hints are delivered before the node is upgraded"));
    }

    // ---- upgrade path ----

    private void buildUpgradePath()
    {
        NodeReport.UpgradePath path = report.upgradePath;
        if (local != null)
            path.dseRelease = "DSE " + report.node.dseVersion + " (release " + report.node.releaseVersion +
                              (report.node.installedDseVersion == null
                               ? ""
                               : ", installation " + report.node.installedDseVersion) + ')';
        buildSettings(path.settings);
        buildSteps(path.upgradeSteps);
    }

    private void buildSettings(Map<String, String> settings)
    {
        Map<String, Object> yaml = layout.cassandraConf;
        settings.put("internode_compatibility_mode", InternodeCompatibilityMode.DSE_5_1.name());
        settings.put("storage_compatibility_mode", StorageCompatibilityMode.DSE_5_1.name());
        settings.put("paxos_variant", "v1");
        settings.put("sstable.selected_format", BigFormat.NAME);
        settings.put("uuid_sstable_identifiers_enabled", "false");
        carry(settings, yaml, "cluster_name");
        Object numTokens = yaml.get("num_tokens");
        Integer tokens = tokenCount();
        if (numTokens != null)
            settings.put("num_tokens", numTokens.toString());
        else if (tokens != null)
            settings.put("num_tokens", tokens + " (DSE's cassandra.yaml sets none: the number of tokens of this " +
                                       "node in system.local)");
        carry(settings, yaml, "initial_token");
        carry(settings, yaml, "allocate_tokens_for_local_replication_factor");
        carry(settings, yaml, "partitioner");
        String snitch = getString(yaml, "endpoint_snitch");
        String target = targetSnitch(snitch);
        boolean same = snitch == null || target.equals(snitch);
        settings.put("endpoint_snitch", same ? target : target + " (was " + snitch + ')');
        String suffix = layout.rackdcProperties.getProperty("dc_suffix");
        if (NodeInspector.isEc2Snitch(snitch))
            settings.put("cassandra-rackdc.properties", "ec2_naming_scheme=legacy" +
                                                        (suffix == null ? "" : ", dc_suffix=" + suffix));
        else if (local != null)
            settings.put("cassandra-rackdc.properties", "dc=" + text(local, "data_center") + ", rack=" +
                                                        text(local, "rack"));
        Object seeds = get(yaml, "seed_provider");
        if (seeds instanceof List && !((List<?>) seeds).isEmpty())
            settings.put("seed_provider", NodeInspector.seedsOf((List<?>) seeds));
        for (String key : List.of("listen_address", "listen_interface", "broadcast_address", "rpc_address",
                                  "rpc_interface", "broadcast_rpc_address", "storage_port", "ssl_storage_port"))
            carry(settings, yaml, key);
        if (dse51.nativeTransportPort != null)
            settings.put("native_transport_port", dse51.nativeTransportPort + " (required: the port of every DSE " +
                                                  "5.1 node, until the last is upgraded)");
        settings.put("data_file_directories", layout.dataDirectories.toString());
        settings.put("commitlog_directory", layout.commitLogDirectory.toString());
        settings.put("hints_directory", layout.hintsDirectory.toString());
        settings.put("metadata_directory", layout.metadataDirectory + " (DSE 5.1 has none: the directory this tool " +
                                           "read for HCD 2.0's files)");
        for (String key : List.of("saved_caches_directory", "cdc_raw_directory", "internode_compression"))
            carry(settings, yaml, key);
        if (get(yaml, "server_encryption_options", "internode_encryption") != null)
            settings.put("server_encryption_options.internode_encryption",
                         getString(yaml, "server_encryption_options", "internode_encryption"));
        if (get(yaml, "client_encryption_options", "enabled") != null)
            settings.put("client_encryption_options.enabled", getString(yaml, "client_encryption_options", "enabled"));
        Map<String, Object> dse = layout.dseConf;
        String authenticator = getString(yaml, "authenticator");
        if (authenticator != null)
            settings.put("authenticator", mapped(authenticator, "DseAuthenticator",
                                                 isTrue(get(dse, "authentication_options", "enabled"))
                                                 ? "PasswordAuthenticator"
                                                 : "AllowAllAuthenticator"));
        String authorizer = getString(yaml, "authorizer");
        if (authorizer != null)
            settings.put("authorizer", mapped(authorizer, "DseAuthorizer",
                                              isTrue(get(dse, "authorization_options", "enabled"))
                                              ? "CassandraAuthorizer"
                                              : "AllowAllAuthorizer"));
        String roleManager = getString(yaml, "role_manager");
        if (roleManager != null)
            settings.put("role_manager", mapped(roleManager, "DseRoleManager", "CassandraRoleManager"));
    }

    private void buildSteps(List<String> steps)
    {
        steps.add("Preflight: run this tool on every DSE 5.1 node and keep the JSON reports (--json)");
        steps.add("Release: every node runs DSE " + DSE_VERSION +
                  (has("DSE_RELEASE", "MIXED_RELEASES", "PEER_RELEASE", "INSTALLED_RELEASE_MISMATCH")
                   ? "; not yet (see DSE_RELEASE, MIXED_RELEASES, PEER_RELEASE and INSTALLED_RELEASE_MISMATCH)"
                   : ""));
        steps.add("Thrift: " + (dse51.thriftOn == null ? "not checked (cassandra.yaml or jvm.options could not be read)"
                                                       : dse51.thriftOn ? "turn it off on every node (see THRIFT)"
                                                                        : "off"));
        steps.add("JVM options: " + (jvmOptions == null ? "not checked (jvm.options could not be read)"
                                                        : has("JVM_OPTIONS") ? "remove the options JVM_OPTIONS names"
                                                                             : "none HCD 2.0 refuses in " +
                                                                               layout.jvmOptions));
        List<String> notRead = schema.notRead(SchemaInspector.MARKER_SCHEMA_TABLES);
        if (!schemaInspected)
            notRead.add("the inspection of the schema failed, see INSPECTION_ERROR");
        if (local == null)
            notRead.add("system.local");
        if (!peersRead)
            notRead.add("system.peers");
        String notComplete = notRead.isEmpty() ? "" : "not checked completely (not read: " + list(notRead) + ')';
        List<String> coreOnly = new ArrayList<>(MARKER_CODES);
        coreOnly.addAll(List.of("DSEFS", "SOLR_DATA"));
        if (has(coreOnly.toArray(new String[0])))
            steps.add("DSE core only: remove the traces listed above on DSE 5.1, or alter the compaction of the " +
                      "tables listed" + (notComplete.isEmpty() ? "" : "; " + notComplete));
        else if (notComplete.isEmpty())
            steps.add("DSE core only: no trace of DSE Search, Graph, Analytics, DSEFS, CFS, Advanced Replication, " +
                      "tiered storage or in-memory tables, nor a compaction strategy HCD 2.0 no longer has, was found");
        else
            steps.add("DSE core only: " + notComplete);
        buildSchemaSteps(steps);
        steps.add("Native port: " + (dse51.nativeTransportPort == null
                                     ? "not checked (cassandra.yaml could not be read)"
                                     : has("NATIVE_PORTS") ? "move to one native port first (see NATIVE_PORTS)"
                                                           : "keep native_transport_port " +
                                                             dse51.nativeTransportPort + " on every HCD 2.0 node"));
        String formats;
        if (!dataDirectoriesScanned)
            formats = "not checked (the data directories could not be inspected completely)";
        else if (formatProblems.isEmpty())
            formats = "every sstable is big me";
        else if (formatProblems.equals(Set.of(SSTABLE_BIG_OLDER)))
            formats = "run nodetool upgradesstables on DSE 5.1 first";
        else if (formatProblems.contains(SSTABLE_BIG_OLDER))
            formats = "run nodetool upgradesstables on DSE 5.1 first, and move the files DSE 5.1 did not write out " +
                      "of the data directories (see SSTABLE_FORMAT)";
        else
            formats = "move the files DSE 5.1 did not write out of the data directories once their data is safe " +
                      "(see SSTABLE_FORMAT)";
        steps.add("Sstable formats: " + formats);
        steps.add("During the mixed phase: no topology or schema change and no repair");
        List<String> hints = new ArrayList<>();
        if (!pendingHintsByTarget.isEmpty())
            hints.add(pendingHintsByTarget.size() + " target(s) with DSE 5.1 hints files pending");
        if (hintHostIdsNotInRing > 0)
            hints.add(hintHostIdsNotInRing + (local != null && peersRead
                                              ? " host id(s) of no node recorded with DSE 5.1 hints files"
                                              : " host id(s) with DSE 5.1 hints files, not checked against " +
                                                "system.local and system.peers"));
        steps.add("Hints: " + (hintsNotListed ? "not checked (the hints directory cannot be listed); see PENDING_HINTS"
                                              : hints.isEmpty() ? "no DSE 5.1 hints file pending"
                                                                : String.join("; ", hints) + "; see PENDING_HINTS"));
        steps.add("Snapshot: nodetool flush, then nodetool snapshot -t pre-hcd-<UTC timestamp>; copy cassandra.yaml, " +
                  "dse.yaml, cassandra-rackdc.properties and jvm.options aside");
        steps.add("Install: install HCD 2.0, with JDK " + HCD_20_JAVA + ", and the settings to carry listed here");
    }

    /** The steps of the schema, its materialized views and the access controls. */
    private void buildSchemaSteps(List<String> steps)
    {
        List<String> notRead = schema.notRead();
        if (!schemaInspected)
            notRead.add("the inspection of the schema failed, see INSPECTION_ERROR");
        String schemaNotRead = notRead.isEmpty() ? "" : "not checked completely (not read: " + list(notRead) + ')';
        List<String> schemaCodes = present(SCHEMA_CODES);
        // TDE_TABLE of an encrypted table or view of the schema, not of system_info_encryption
        if (schema.encryptedTablesFound)
            schemaCodes.add("TDE_TABLE");
        if (!schemaCodes.isEmpty())
            steps.add("Schema: change on DSE 5.1, before the first node is upgraded, what " +
                      String.join(", ", schemaCodes) + " name" + (schemaNotRead.isEmpty() ? "" : "; " + schemaNotRead));
        else if (schemaNotRead.isEmpty() && schema.schemaRead)
            steps.add("Schema: no table, index, type or function HCD 2.0 refuses or cannot load was found");
        else
            steps.add("Schema: " + (schemaNotRead.isEmpty() ? "not checked (the schema could not be read)"
                                                            : schemaNotRead));

        String everyNode = "a RESTRICT ROWS on a view is in the schema of the node that ran it only: aggregate the " +
                           "reports of every node";
        List<String> views = new ArrayList<>();
        if (!schema.restrictedViews.isEmpty())
            views.add("on DSE 5.1, REVOKE the rows/ grants on " + list(schema.restrictedViews) + ", then drop " +
                      (schema.restrictedViews.size() == 1 ? "it and create it" : "them and create them") +
                      " again without the restriction (see RESTRICTED_VIEW; " +
                      SchemaInspector.VIEW_REMEDY_NOT_VALIDATED + ")" +
                      (schema.restrictedViewsWithTableRow.isEmpty()
                       ? ""
                       : schema.restrictedViewsWithTableRow.size() == schema.restrictedViews.size()
                         ? ", only once the upgrade owner has chosen the repair of the row VIEW_TABLE_ROW names " +
                           "beside " + (schema.restrictedViews.size() == 1 ? "it" : "each")
                         : ", but not " + list(schema.restrictedViewsWithTableRow) + " before the upgrade owner " +
                           "has chosen the repair of the row VIEW_TABLE_ROW names beside it"));
        if (!schema.viewTableRows.isEmpty())
            views.add("this node cannot be upgraded while the rows VIEW_TABLE_ROW names are in its schema: report " +
                      "to the upgrade owner");
        if (!schema.viewsToDrop.isEmpty())
            views.add("drop " + list(schema.viewsToDrop) + " on DSE 5.1, and create them again once no node runs DSE");
        if (!schemaInspected && !schema.schemaRead)
            steps.add("Materialized views: not checked (see INSPECTION_ERROR); " + everyNode);
        else if (!schema.schemaRead)
            steps.add("Materialized views: not checked (the schema could not be read); " + everyNode);
        else if (!schemaInspected || !schema.viewInputsNotRead().isEmpty())
            steps.add("Materialized views: " + (views.isEmpty() ? "" : String.join("; ", views) + "; ") +
                      "not checked completely (" + (schemaInspected ? "not read: " +
                                                                      list(schema.viewInputsNotRead())
                                                                    : "see INSPECTION_ERROR") + "); " + everyNode);
        else if (views.isEmpty())
            steps.add("Materialized views: " + schema.viewCount + " view(s), none to change in this node's schema; " +
                      everyNode);
        else
            steps.add("Materialized views: " + String.join("; ", views) + "; " + everyNode);

        List<String> securityNotRead = new ArrayList<>();
        if (layout.unreadable.containsKey(NodeLayout.DSE_YAML))
            securityNotRead.add("dse.yaml");
        securityNotRead.addAll(schema.accessControlInputsNotRead());
        if (!schemaInspected)
            securityNotRead.add("the inspection of the schema failed, see INSPECTION_ERROR");
        String securityIncomplete = securityNotRead.isEmpty() ? "" : "not checked completely (not read: " +
                                                                     String.join(", ", securityNotRead) + ')';
        if (has("RESTRICT", "ROW_LEVEL_ACCESS", "RESTRICTED_VIEW"))
            steps.add("Security: resolve the blocking access control findings above, on DSE 5.1" +
                      (securityIncomplete.isEmpty() ? "" : "; " + securityIncomplete));
        else if (warns("ROW_LEVEL_ACCESS", "PROXY_GRANT", "DELEGATION", "LONE_WRITE_GRANT", "DSE_ONLY_PERMISSION",
                       "DSE_ONLY_RESOURCE"))
            steps.add("Security: review the access control warnings above" +
                      (securityIncomplete.isEmpty() ? "" : "; " + securityIncomplete));
        else if (!securityIncomplete.isEmpty())
            steps.add("Security: " + securityIncomplete);
        else
            steps.add("Security: no DSE access control this software ignores was found in the schema and this " +
                      "node's replica of system_auth.role_permissions");
    }

    /** Those of {@code codes} that a blocking finding has, in their order. */
    private List<String> present(List<String> codes)
    {
        List<String> present = new ArrayList<>();
        for (String code : codes)
            if (has(code))
                present.add(code);
        return present;
    }

    private boolean warns(String... codes)
    {
        Set<String> wanted = Set.of(codes);
        return findings.stream().anyMatch(f -> f.severity == Finding.Severity.WARNING && wanted.contains(f.code));
    }

    private boolean has(String... codes)
    {
        Set<String> wanted = Set.of(codes);
        return findings.stream().anyMatch(f -> f.severity == Finding.Severity.BLOCKING && wanted.contains(f.code));
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

    // ---- helpers ----

    @Nullable
    private static String text(UntypedResultSet.Row row, String column)
    {
        return row.has(column) ? row.getString(column) : null;
    }

    @Nullable
    private static UUID uuid(UntypedResultSet.Row row, String column)
    {
        return row.has(column) ? row.getUUID(column) : null;
    }

    @Nullable
    private static InetAddress inet(UntypedResultSet.Row row, String column)
    {
        return row.has(column) ? row.getInetAddress(column) : null;
    }
}
