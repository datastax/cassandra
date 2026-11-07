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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;

import static org.apache.cassandra.tools.dsepreflight.Finding.blocking;
import static org.apache.cassandra.tools.dsepreflight.Finding.info;
import static org.apache.cassandra.tools.dsepreflight.Finding.warning;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.list;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.rootCause;

/**
 * The rollback check of {@code dsepreflight} ({@code --rollback-check}): whether a node of HCD 2.0 in
 * {@code DSE_5_1} mode, drained and stopped, can go back to DSE 5.1.51 with what HCD 2.0 left on its disks. Run from
 * the unpacked HCD 2.0 tarball, before the restore of {@code system} and {@code system_schema} and before DSE 5.1
 * starts; it reads file names, the descriptors of the hints files, the entries of the commit log segments (counted,
 * never decoded) and the {@code system.local} and {@code system.peers} sstables of the data directories (from scratch
 * copies, {@link DseTableReader#readBig}), and never writes to the node's files.
 * <p>
 * It refuses the rollback (blocking findings) while DSE 5.1.51 could not start on the files left, or would lose or
 * misread what they hold, or while the check cannot tell: an sstable that is not big {@code me} with a numeric
 * generation ({@link Dse51NodeInspector#sstableProblem}), a transaction log, a commit log segment that holds a
 * mutation (a drain leaves none, so the node was not drained after it was written: the rollback removes the commit
 * log) or that cannot be read to its end, a hints file of a version other than 1 (by its name or by its descriptor),
 * one whose descriptor is empty or cannot be read, no snapshot of DSE 5.1's {@code system} and
 * {@code system_schema} tables with sstables of {@code system.local} and {@code system_schema.keyspaces} (the one
 * given with {@code --snapshot}, else the only complete {@code pre-hcd-<timestamp>} one; several refuse), a metadata
 * directory without the DSE 5.1 upgrade marker, which HCD 2.0 writes at its first start in {@code DSE_5_1} mode (the
 * directory read is then not HCD 2.0's), DSE's {@code cassandra.yaml} that cannot be read, and a data, commit log,
 * hints or metadata directory that cannot be listed.
 * <p>
 * The rollback keeps the DSE 5.1 upgrade marker, as for DSE 6, and removes the record of the values of DSE 5.1's
 * columns of {@code system.local} and {@code system.peers} ({@link Dse51SystemTables#RECORD_FILE_NAME}) and its
 * temporary file: with both kept, the next start of HCD 2.0 on the node, the marker there, would load the record of
 * the upgrade rolled back instead of reading what DSE 5.1 wrote since. The record, which every node of HCD 2.0 in the
 * mode holds, is listed for removal, as the commit log segments are; the node inspection of the next upgrade refuses
 * a node that kept both. The copies of unreadable records are kept, never loaded.
 * <p>
 * It accepts hints files of version 1, which DSE 5.1.51 reads and delivers, and lists them by the target their
 * descriptor names, with a warning for a target that the node's {@code system.peers} does not show running DSE 5.1
 * (DSE 5.1.51 dispatches a file only to a peer that gossips its own schema version, which a node of HCD 2.0 does not;
 * a peer with no release recorded is counted with them), and one for a target in neither {@code system.local} nor
 * {@code system.peers}. It warns about the SAI component files left (DSE 5.1.51 ignores them and removes them with
 * their sstable), and about a commit log or hints directory that does not exist (it may not be HCD 2.0's), and lists
 * what the rollback removes before DSE 5.1 starts (the commit log segments, the saved caches, the record and its
 * temporary file) and what it keeps (the marker, the record of DSE 5.1 peers, the copies of unreadable records, the
 * hints files of version 1 with their companions, the snapshot).
 */
final class Dse51RollbackInspection
{
    /** The {@link Report#kind} of the report of this check, which the aggregate mode skips. */
    static final String KIND = "dse51_rollback_check";
    static final int FORMAT_VERSION = 1;
    /** The prefix of the tag of the snapshot taken before the upgrade, which the rollback restores. */
    static final String SNAPSHOT_PREFIX = "pre-hcd-";
    /** The tables of DSE 5.1.51's {@code system_schema}, each of which the restore takes from the snapshot. */
    static final List<String> DSE_51_SCHEMA_TABLES = List.of("keyspaces", "tables", "columns", "dropped_columns",
                                                             "triggers", "views", "types", "functions", "aggregates",
                                                             "indexes");
    /** The hints version DSE 5.1.51 reads and delivers. */
    static final int HINTS_VERSION = Dse51NodeInspector.HINTS_VERSION;
    /** A transaction log of a table directory, {@code <version>_txn_<operation>_<id>.log}. */
    private static final Pattern TRANSACTION_LOG = Pattern.compile(".*_txn_.*\\.log");
    /** An SAI component file: the component of its name starts with {@code SAI+}, or {@code SAI_} for version aa. */
    private static final Pattern SAI_COMPONENT = Pattern.compile(".*-SAI[+_][^-]*");
    /** The default of {@code saved_caches_directory} of a package install of DSE. */
    static final Path DEFAULT_SAVED_CACHES = NodeLayout.DSE_STORAGE_DIR.resolve("saved_caches");
    private static final String RESTART_AND_DRAIN = "start HCD 2.0 again on this node, in DSE_5_1 mode, drain it " +
                                                    "(nodetool drain exits 0 and logs DRAINED), stop it, and run " +
                                                    "this check again";
    /** The remedy of a directory of HCD 2.0 that cannot be read. */
    private static final String RUN_AS_HCD_USER = "run the tool as the user HCD 2.0 runs as (or as root)";

    /** The report of the check, written as JSON with {@code --json}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    static final class Report
    {
        @JsonProperty("tool")
        public String tool = NodeReport.TOOL;
        @JsonProperty("format_version")
        public int formatVersion = FORMAT_VERSION;
        @JsonProperty("kind")
        public String kind = KIND;
        @JsonProperty("software_version")
        public String softwareVersion;
        @JsonProperty("generated_at")
        public String generatedAt;
        @JsonProperty("hostname")
        public String hostname;
        /** {@link NodeReport#READY} or {@link NodeReport#BLOCKED} */
        @JsonProperty("result")
        public String result;
        /** the files and directories read */
        @JsonProperty("inputs")
        public Map<String, Object> inputs = new LinkedHashMap<>();
        @JsonProperty("findings")
        public List<Finding> findings = new ArrayList<>();
        /** the version-1 hints files left, by target ({@code <host id> (<what the node knows of it>)}) */
        @JsonProperty("hints_by_target")
        public Map<String, List<String>> hintsByTarget = new TreeMap<>();
        /** the files and directories the rollback removes before DSE 5.1 starts */
        @JsonProperty("to_remove")
        public List<String> toRemove = new ArrayList<>();
        /** the files the rollback may remove before DSE 5.1 starts, which DSE 5.1 ignores: the SAI component files */
        @JsonProperty("may_remove")
        public List<String> mayRemove = new ArrayList<>();
        /** what the rollback keeps: files, the hints files with their companions, the snapshot it restores */
        @JsonProperty("kept")
        public List<String> kept = new ArrayList<>();

        @JsonIgnore
        boolean blocked()
        {
            return findings.stream().anyMatch(f -> f.severity == Finding.Severity.BLOCKING);
        }

        @JsonIgnore
        long count(Finding.Severity severity)
        {
            return findings.stream().filter(f -> f.severity == severity).count();
        }
    }

    private final NodeLayout layout;
    /** the tag of the snapshot the restore takes, given with {@code --snapshot}; null when not given */
    @Nullable
    private final String snapshot;
    private final Report report = new Report();
    private final List<Finding> findings = report.findings;

    Dse51RollbackInspection(NodeLayout layout, @Nullable String snapshot)
    {
        this.layout = layout;
        this.snapshot = snapshot;
    }

    Report inspect(String hostname, String softwareVersion, String generatedAt)
    {
        report.hostname = hostname;
        report.softwareVersion = softwareVersion;
        report.generatedAt = generatedAt;
        guarded("the inputs", this::recordInputs);
        guarded("the data directories", this::checkDataDirectories);
        guarded("the snapshot", this::checkSnapshot);
        guarded("the commit log directory", this::checkCommitLog);
        guarded("the hints directory", this::checkHints);
        guarded("the saved caches directory", this::listSavedCaches);
        guarded("the metadata directory", this::checkMetadataDirectory);
        if (!report.toRemove.isEmpty())
            findings.add(info("ROLLBACK_REMOVES",
                              "files the rollback removes before DSE 5.1 starts: " + list(report.toRemove)));
        if (!report.kept.isEmpty())
            findings.add(info("ROLLBACK_KEEPS", "kept by the rollback: " + list(report.kept)));
        findings.sort(Comparator.comparing(f -> f.severity));
        report.result = report.blocked() ? NodeReport.BLOCKED : NodeReport.READY;
        return report;
    }

    /** Runs one part of the check: an unexpected error is a blocking finding, as in the node inspection. */
    private void guarded(String what, Runnable part)
    {
        try
        {
            part.run();
        }
        catch (RuntimeException | AssertionError | FSError e)
        {
            findings.add(blocking("INSPECTION_ERROR",
                                  "the check of " + what + " failed (" + e + "), so it is incomplete",
                                  "report the error with the output of the tool; the node cannot be declared ready " +
                                  "for its rollback until the check completes"));
        }
    }

    // ---- inputs ----

    private void recordInputs()
    {
        if (layout.dseHome != null)
            report.inputs.put("dse_home", layout.dseHome.toString());
        report.inputs.put("cassandra_yaml", layout.cassandraYaml.toString());
        List<String> data = new ArrayList<>();
        layout.dataDirectories.forEach(d -> data.add(d.toString()));
        report.inputs.put("data_file_directories", data);
        report.inputs.put("metadata_directory", layout.metadataDirectory.toString());
        report.inputs.put("commitlog_directory", layout.commitLogDirectory.toString());
        report.inputs.put("hints_directory", layout.hintsDirectory.toString());
        report.inputs.put("saved_caches_directory", savedCachesDirectory().toString());
        String yaml = layout.unreadable.get(NodeLayout.CASSANDRA_YAML);
        if (yaml != null)
            findings.add(blocking("UNREADABLE_INPUT",
                                  NodeInspector.cannotRead(NodeLayout.CASSANDRA_YAML) + yaml + ", so the directories " +
                                  "of the node are DSE's defaults or those given, which may not be this node's",
                                  "run the tool as the user DSE runs as (or as root), or point it at the file " +
                                  "(--cassandra-yaml, --conf-dir, --dse-home), or give every directory (--data-dir, " +
                                  "--commitlog-dir, --hints-dir, --metadata-dir)"));
        List<String> assumed = new ArrayList<>(layout.defaultsAssumed);
        if (layout.cassandraConf.get("saved_caches_directory") == null)
            assumed.add("saved_caches_directory: " + DEFAULT_SAVED_CACHES);
        if (!assumed.isEmpty())
            findings.add(info("DEFAULTS_ASSUMED",
                              "DSE's cassandra.yaml does not set these directories, DSE's defaults are assumed: " +
                              list(assumed)));
    }

    /** DSE's {@code saved_caches_directory}, else its default. */
    private File savedCachesDirectory()
    {
        Object configured = layout.cassandraConf.get("saved_caches_directory");
        return new File(configured == null || configured.toString().isEmpty() ? DEFAULT_SAVED_CACHES
                                                                                : Path.of(configured.toString()));
    }

    // ---- data directories ----

    private void checkDataDirectories()
    {
        Map<String, Map<String, List<String>>> problems = new TreeMap<>();
        List<String> unreadableTransactionLogs = new ArrayList<>();
        List<Finding> scanned = new ArrayList<>();
        NodeInspector.scanDataDirectories(layout.dataDirectories, d -> {}, Dse51NodeInspector::sstableProblem,
                                          Dse51NodeInspector.SSTABLE_WITHOUT_TOC, problems, new LinkedHashMap<>(),
                                          unreadableTransactionLogs, scanned);
        // the remedies of the scan are those of a DSE node's inspection: the files are HCD 2.0's here
        for (Finding finding : scanned)
            findings.add(new Finding(finding.severity, finding.code, finding.message,
                                     finding.remedy == null ? null : dataDirectoryRemedy(finding.remedy)));
        for (Map.Entry<String, Map<String, List<String>>> problem : problems.entrySet())
        {
            List<String> items = new ArrayList<>();
            boolean withoutToc = problem.getKey().equals(Dse51NodeInspector.SSTABLE_WITHOUT_TOC);
            problem.getValue().forEach((table, files) -> {
                String shown = withoutToc ? NodeInspector.withoutTocNames(files)
                                          : files.get(0) + (files.size() > 1 ? ", ..." : "");
                items.add(table + ": " + files.size() + " (" + shown + ')');
            });
            if (withoutToc)
                findings.add(warning("SSTABLE_WITHOUT_TOC", problem.getKey() + ": " + list(items),
                                     "find out why they have none (HCD 2.0 writes it for every sstable): an " +
                                     "incomplete copy or restore, a damaged disk"));
            else
                findings.add(blocking("SSTABLE_FORMAT",
                                      problem.getKey() + ": " + list(items) + "; DSE 5.1.51 refuses to start on them",
                                      "find out what wrote them: HCD 2.0 in DSE_5_1 mode writes only big me sstables " +
                                      "with numeric identifiers, and refuses to start while others are in its data " +
                                      "directories; move them out of the data directories once their data is safe " +
                                      "(the writes they hold come back to this node with a repair on DSE 5.1), then " +
                                      "run this check again"));
        }
        // unreadable logs are listed with the others below: every transaction log refuses the rollback
        List<String> transactionLogs = new ArrayList<>();
        List<String> saiFiles = new ArrayList<>();
        for (File dataDirectory : layout.dataDirectories)
            for (File keyspace : listed(dataDirectory, File::isDirectory))
                for (File table : listed(keyspace, File::isDirectory))
                    scanTableFiles(table, transactionLogs, saiFiles);
        if (!transactionLogs.isEmpty())
            findings.add(blocking("TRANSACTION_LOG",
                                  "transaction logs left in the data directories, of an operation HCD 2.0 did not " +
                                  "finish: " + list(transactionLogs) + "; DSE 5.1.51 would complete or undo the " +
                                  "operation at its start, from a log it may not read as HCD 2.0 wrote it",
                                  "start HCD 2.0 again on this node, in DSE_5_1 mode, which completes or undoes the " +
                                  "operation at its start, then drain it (nodetool drain exits 0 and logs DRAINED), " +
                                  "stop it, and run this check again"));
        if (!saiFiles.isEmpty())
        {
            report.mayRemove.addAll(saiFiles);
            findings.add(warning("SAI_FILES",
                                 "SAI component files left in the data directories: " + list(saiFiles) + "; DSE " +
                                 "5.1.51 has no SAI, ignores these files and removes them with their sstable",
                                 "remove them before DSE 5.1 starts, if you choose to; the rollback goes on either " +
                                 "way"));
        }
    }

    /** A remedy of the shared scan of the data directories, worded for the files of HCD 2.0 of this check. */
    private static String dataDirectoryRemedy(String remedy)
    {
        if (remedy.equals(NodeInspector.POINT_AT_DATA_DIRECTORIES))
            return "point the tool at the data directories of this node (--data-dir)";
        if (remedy.equals(NodeInspector.RUN_AS_DSE_USER))
            return RUN_AS_HCD_USER;
        return remedy;
    }

    /**
     * Adds the transaction logs and the SAI component files of a table directory, and of its secondary index
     * directories ({@code .<index>}), to the lists given; snapshots and backups are not live, and are left out.
     */
    private static void scanTableFiles(File directory, List<String> transactionLogs, List<String> saiFiles)
    {
        for (File file : listed(directory, f -> true))
        {
            if (file.isDirectory())
            {
                if (file.name().startsWith("."))
                    scanTableFiles(file, transactionLogs, saiFiles);
                continue;
            }
            if (TRANSACTION_LOG.matcher(file.name()).matches())
                transactionLogs.add(file.toString());
            else if (SAI_COMPONENT.matcher(file.name()).matches())
                saiFiles.add(file.toString());
        }
    }

    /**
     * The entries of a directory accepted by {@code filter}, sorted by name; none when it cannot be listed, which the
     * scan of the data directories ({@link NodeInspector#scanDataDirectories}) reports.
     */
    private static List<File> listed(File directory, Predicate<File> filter)
    {
        File[] files = directory.tryList(filter);
        if (files == null)
            return List.of();
        List<File> sorted = new ArrayList<>(List.of(files));
        sorted.sort(Comparator.comparing(File::name));
        return sorted;
    }

    // ---- the snapshot ----

    /**
     * The {@code pre-hcd-<timestamp>} snapshot the rollback restores {@code system} and {@code system_schema} from:
     * one that holds {@code system.local}, {@code system.peers} and every table of DSE 5.1.51's {@code system_schema}
     * ({@link #DSE_51_SCHEMA_TABLES}; a snapshot holds a directory of every table, sstables or not), with an sstable
     * of {@code system.local} and of {@code system_schema.keyspaces}, which DSE 5.1 always holds data in: the one
     * given with {@code --snapshot}, whatever its tag, as the Paxos rollback mode takes it, else the only complete
     * {@code pre-hcd-<timestamp>} one; several complete snapshots refuse, as the Paxos rollback mode refuses them,
     * until {@code --snapshot} names the one the restore takes.
     */
    private void checkSnapshot()
    {
        List<String> required = new ArrayList<>();
        required.add(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LOCAL);
        required.add(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LEGACY_PEERS);
        DSE_51_SCHEMA_TABLES.forEach(t -> required.add(SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + t));
        // a copy of these without an sstable is a bad copy: DSE 5.1 always holds data in them
        Set<String> withData = Set.of(SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.LOCAL,
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME + ".keyspaces");
        // the tables each snapshot holds, by tag, whatever its name: --snapshot takes any tag
        Map<String, Set<String>> tablesByTag = new TreeMap<>();
        // the tables of withData a snapshot holds without an sstable
        Map<String, Set<String>> emptyByTag = new TreeMap<>();
        for (String qualified : required)
        {
            int dot = qualified.indexOf('.');
            String keyspace = qualified.substring(0, dot);
            String table = qualified.substring(dot + 1);
            for (File dataDirectory : layout.dataDirectories)
                for (File tableDirectory : listed(new File(dataDirectory, keyspace),
                                                  f -> f.isDirectory() && PaxosRollback.isTableDirectory(f, table)))
                    for (File snapshotDirectory : listed(new File(tableDirectory, Directories.SNAPSHOT_SUBDIR),
                                                         File::isDirectory))
                    {
                        Set<String> tables = tablesByTag.computeIfAbsent(snapshotDirectory.name(),
                                                                         t -> new TreeSet<>());
                        if (!withData.contains(qualified) ||
                            !listed(snapshotDirectory, f -> f.name().endsWith("-Data.db")).isEmpty())
                            tables.add(qualified);
                        else
                            emptyByTag.computeIfAbsent(snapshotDirectory.name(), t -> new TreeSet<>()).add(qualified);
                    }
        }
        report.inputs.put("snapshots", new ArrayList<>(tablesByTag.keySet()));
        List<String> complete = new ArrayList<>();
        List<String> partial = new ArrayList<>();
        tablesByTag.forEach((tag, tables) -> {
            if (!tag.startsWith(SNAPSHOT_PREFIX))
                return;
            if (tables.containsAll(required))
                complete.add(tag);
            else
                partial.add(tag + " (" + lacking(tables, emptyByTag.getOrDefault(tag, Set.of()), required) + ')');
        });
        String what = "system.local, system.peers and the tables of DSE 5.1's system_schema, with sstables of " +
                      "system.local and system_schema.keyspaces";
        if (snapshot != null)
        {
            Set<String> tables = tablesByTag.get(snapshot);
            if (tables == null || !tables.containsAll(required))
            {
                findings.add(blocking("SNAPSHOT_MISSING",
                                      "the snapshot " + snapshot + " given with --snapshot does not hold " + what +
                                      " (" + (tables == null ? "not found"
                                                             : lacking(tables, emptyByTag.getOrDefault(snapshot,
                                                                                                       Set.of()),
                                                                       required)) +
                                      "; complete " + SNAPSHOT_PREFIX + "<timestamp> snapshots: " +
                                      (complete.isEmpty() ? "none" : list(complete)) + ')',
                                      "pass --snapshot with the tag of the snapshot taken before the upgrade, the " +
                                      "one the restore takes"));
                return;
            }
        }
        else if (complete.isEmpty())
        {
            findings.add(blocking("SNAPSHOT_MISSING",
                                  "no " + SNAPSHOT_PREFIX + "<timestamp> snapshot in the data directories holds " +
                                  what + " (" + (partial.isEmpty() ? "none found" : "found: " + list(partial)) +
                                  "); the rollback restores system and system_schema from the snapshot taken on DSE " +
                                  "5.1 before the upgrade",
                                  "find the snapshot taken before the upgrade (pass --data-dir if the data " +
                                  "directories are not those of DSE's cassandra.yaml): without it this node cannot " +
                                  "go back to DSE 5.1"));
            return;
        }
        else if (complete.size() > 1)
        {
            findings.add(blocking("SNAPSHOT_AMBIGUOUS",
                                  "several " + SNAPSHOT_PREFIX + "<timestamp> snapshots hold " + what + ": " +
                                  list(complete),
                                  "pass --snapshot <tag>, the snapshot the restore takes (the one taken on DSE 5.1 " +
                                  "right before this upgrade)"));
            return;
        }
        String restored = snapshot != null ? snapshot : complete.get(0);
        report.kept.add("the snapshot " + restored + " of system and system_schema");
        findings.add(info("SNAPSHOT", "the restore of system and system_schema takes the snapshot " + restored));
    }

    /**
     * What a snapshot lacks of the tables of the restore: those it has no directory of, and those of
     * {@code system.local} and {@code system_schema.keyspaces} it holds without an sstable in every data directory
     * that has them ({@code empty}, less those of {@code tables}).
     */
    private static String lacking(Set<String> tables, Set<String> empty, List<String> required)
    {
        List<String> missing = new ArrayList<>(required);
        missing.removeAll(tables);
        missing.removeAll(empty);
        // a table held with an sstable in one data directory and without in another is held
        List<String> withoutSSTable = new ArrayList<>(empty);
        withoutSSTable.removeAll(tables);
        List<String> parts = new ArrayList<>(2);
        if (!missing.isEmpty())
            parts.add("missing: " + String.join(", ", missing));
        if (!withoutSSTable.isEmpty())
            parts.add("without an sstable: " + String.join(", ", withoutSSTable));
        return String.join("; ", parts);
    }

    // ---- commit log ----

    /** Counts the entries of a commit log segment, one per mutation, without decoding any. */
    private static final class EntryCounter extends CommitLogReader
    {
        int entries;

        @Override
        protected void readMutation(CommitLogReadHandler handler, byte[] inputBuffer, int size,
                                    CommitLogPosition minPosition, int entryLocation, CommitLogDescriptor desc)
        {
            // the entry passed its checksums: a mutation the segment holds
            entries++;
        }
    }

    /** Records the errors of the read of a segment; the reader decodes no mutation, so none is handled. */
    private static final class ReadErrors implements CommitLogReadHandler
    {
        final List<String> errors = new ArrayList<>();

        @Override
        public boolean shouldSkipSegmentOnError(CommitLogReadException exception)
        {
            errors.add(exception.getMessage());
            return true;
        }

        @Override
        public void handleUnrecoverableError(CommitLogReadException exception)
        {
            errors.add(exception.getMessage());
        }

        @Override
        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            throw new AssertionError("no mutation is decoded");
        }

        @Override
        public void handleInvalidMutation(TableId id)
        {
            throw new AssertionError("no mutation is decoded");
        }
    }

    /**
     * The entries of a commit log segment, one per mutation, counted once their checksums pass and never decoded; the
     * errors of the read are added to {@code errors}. The segment is read to its end, though its first entry decides
     * the refusal, so that the count the finding gives is the whole: a segment is at most
     * {@code commitlog_segment_size} long.
     */
    static int countEntries(File segment, List<String> errors)
    {
        EntryCounter counter = new EntryCounter();
        ReadErrors handler = new ReadErrors();
        try
        {
            counter.readCommitLogSegment(handler, segment, CommitLogPosition.NONE, CommitLogReader.ALL_MUTATIONS,
                                         false);
        }
        catch (IOException | RuntimeException e)
        {
            handler.errors.add(rootCause(e));
        }
        errors.addAll(handler.errors);
        return counter.entries;
    }

    private void checkCommitLog()
    {
        File directory = layout.commitLogDirectory;
        if (!directory.isDirectory())
        {
            findings.add(warning("COMMITLOG_DIRECTORY_MISSING",
                                 "no commit log directory at " + directory + ": HCD 2.0 always has one, so this may " +
                                 "not be its commitlog_directory, and its segments are not checked",
                                 "pass --commitlog-dir with the commitlog_directory of HCD 2.0's cassandra.yaml; if " +
                                 "it is that directory, the rollback goes on"));
            return;
        }
        File[] files = directory.tryList(File::isFile);
        if (files == null)
        {
            findings.add(blocking("COMMITLOG_UNREADABLE",
                                  "cannot list the commit log directory " + directory + ", so whether it holds " +
                                  "writes not yet in an sstable cannot be told",
                                  RUN_AS_HCD_USER + ", or point it at the directory (--commitlog-dir)"));
            return;
        }
        List<File> segments = new ArrayList<>(List.of(files));
        segments.sort(Comparator.comparing(File::name));
        List<String> withMutations = new ArrayList<>();
        List<String> unreadable = new ArrayList<>();
        for (File segment : segments)
        {
            Matcher matcher = NodeInspector.COMMITLOG_FILE.matcher(segment.name());
            if (!matcher.matches())
                continue;
            report.toRemove.add(segment.toString());
            // a segment HCD 2.0 replayed and set aside holds nothing to replay
            if (matcher.group(3) != null)
                continue;
            List<String> errors = new ArrayList<>();
            int entries = countEntries(segment, errors);
            if (entries > 0)
                withMutations.add(segment.name() + " (" + entries + (entries == 1 ? " mutation)" : " mutations)"));
            if (!errors.isEmpty())
                unreadable.add(segment.name() + " (" + errors.get(0) + ')');
        }
        if (!withMutations.isEmpty())
            findings.add(blocking("COMMITLOG_UNFLUSHED",
                                  "commit log segments that hold mutations: " + list(withMutations) + "; a drain " +
                                  "flushes every table and leaves no mutation in the commit log, so this node was " +
                                  "not drained after they were written, and they may hold writes not yet in an " +
                                  "sstable, which the rollback, removing the commit log, would lose",
                                  RESTART_AND_DRAIN));
        if (!unreadable.isEmpty())
            findings.add(blocking("COMMITLOG_UNREADABLE",
                                  "commit log segments that cannot be read to the end: " + list(unreadable) + ", so " +
                                  "whether they hold writes not yet in an sstable cannot be told",
                                  RESTART_AND_DRAIN + "; if a segment stays unreadable, find out why before the " +
                                  "rollback"));
    }

    // ---- hints ----

    /** What the node's {@code system.local} and {@code system.peers} know of a host. */
    private static final class Target
    {
        final String description;
        /** whether {@code system.peers} shows the host running DSE 5.1, or the host is this node */
        final boolean dse51;

        Target(String description, boolean dse51)
        {
            this.description = description;
            this.dse51 = dse51;
        }
    }

    private void checkHints()
    {
        File directory = layout.hintsDirectory;
        if (!directory.isDirectory())
        {
            findings.add(warning("HINTS_DIRECTORY_MISSING",
                                 "no hints directory at " + directory + ": HCD 2.0 always has one, so this may not " +
                                 "be its hints_directory, and its hints files are not checked",
                                 "pass --hints-dir with the hints_directory of HCD 2.0's cassandra.yaml; if it is " +
                                 "that directory, the rollback goes on"));
            return;
        }
        File[] files = directory.tryList(File::isFile);
        if (files == null)
        {
            findings.add(blocking("HINTS_UNREADABLE",
                                  "cannot list the hints directory " + directory + ", so the versions of its hints " +
                                  "files cannot be checked",
                                  RUN_AS_HCD_USER + ", or point it at the directory (--hints-dir)"));
            return;
        }
        List<File> sorted = new ArrayList<>(List.of(files));
        sorted.sort(Comparator.comparing(File::name));
        List<String> otherVersions = new ArrayList<>();
        List<String> unreadableDescriptors = new ArrayList<>();
        Map<String, List<String>> byHostId = new TreeMap<>();
        for (File file : sorted)
        {
            Matcher matcher = NodeInspector.HINTS_FILE.matcher(file.name());
            if (!matcher.matches())
                continue;
            // DSE 5.1 names its files with the version alone; HCD 2.0 does too, DSE 6 with v and the version
            if (!matcher.group(3).isEmpty() || !Integer.toString(HINTS_VERSION).equals(matcher.group(4)))
            {
                otherVersions.add(file.name() + " (version " + matcher.group(3) + matcher.group(4) + " by its name)");
                continue;
            }
            NodeInspector.HintsFileHeader header;
            try
            {
                header = NodeInspector.hintsFileHeader(file.toPath());
            }
            catch (IOException | RuntimeException e)
            {
                unreadableDescriptors.add(file.name() + " (" + rootCause(e) + ')');
                continue;
            }
            if (header == null)
            {
                unreadableDescriptors.add(file.name() + " (empty)");
                continue;
            }
            if (header.version != HINTS_VERSION)
            {
                otherVersions.add(file.name() + " (version " + header.version + " by its descriptor)");
                continue;
            }
            // DSE 5.1 dispatches a file to the host its descriptor names
            byHostId.computeIfAbsent(header.hostId.toString(), h -> new ArrayList<>()).add(file.name());
        }
        if (!unreadableDescriptors.isEmpty())
            findings.add(blocking("HINTS_DESCRIPTOR_UNREADABLE",
                                  "hints files whose descriptor is empty or cannot be read: " +
                                  list(unreadableDescriptors) + "; their hints cannot be delivered: HCD 2.0 deletes " +
                                  "an empty one and sets a damaged one aside, or does not start on it, and what DSE " +
                                  "5.1.51 does with such a file is not known",
                                  "move them out of the hints directory before DSE 5.1 starts, and repair the tables " +
                                  "they may hold once the cluster runs one release"));
        if (!otherVersions.isEmpty())
            findings.add(blocking("HINTS_VERSION",
                                  "hints files that are not of version " + HINTS_VERSION + ", the only one DSE " +
                                  "5.1.51 delivers: " + list(otherVersions) + "; DSE 5.1.51 loads them and never " +
                                  "delivers them",
                                  "start HCD 2.0 again on this node and let it deliver them (nodetool " +
                                  "listpendinghints, the targets up), or remove them and repair the tables they " +
                                  "hold once the cluster runs one release; then drain the node and run this check " +
                                  "again"));
        if (byHostId.isEmpty())
            return;
        Map<String, Target> targets = targets();
        List<String> forHcd = new ArrayList<>();
        List<String> unknown = new ArrayList<>();
        byHostId.forEach((hostId, names) -> {
            Target target = targets == null ? null : targets.get(hostId);
            String key = hostId + " (" + (target != null ? target.description
                                                         : targets == null ? "not checked" : "unknown") + ')';
            report.hintsByTarget.put(key, names);
            for (String name : names)
            {
                report.kept.add(new File(directory, name).toString());
                // the companions of a file, its checksum and its statistics, go with it
                String base = name.substring(0, name.length() - ".hints".length());
                for (String companion : List.of(base + ".crc32", base + "-Statistics.hints"))
                    if (new File(directory, companion).exists())
                        report.kept.add(new File(directory, companion).toString());
            }
            String item = key + ": " + names.size() + " file(s)";
            if (targets != null && target == null)
                unknown.add(item);
            else if (target != null && !target.dse51)
                forHcd.add(item);
        });
        List<String> items = new ArrayList<>();
        report.hintsByTarget.forEach((target, names) -> items.add(target + ": " + names.size() + " file(s)"));
        findings.add(info("HINTS",
                          "hints files of version " + HINTS_VERSION + ", which DSE 5.1.51 reads and delivers, " +
                          "kept by the rollback: " + list(items)));
        if (!forHcd.isEmpty())
            findings.add(warning("HINTS_FOR_HCD",
                                 "hints files of version " + HINTS_VERSION + " for targets that system.peers does " +
                                 "not show running DSE 5.1 (HCD 2.0, or no release recorded): " + list(forHcd) +
                                 "; DSE 5.1.51 delivers a hints file only to a peer that gossips its own schema " +
                                 "version, which a node of HCD 2.0 does not, so these hints wait for the target's " +
                                 "own rollback to DSE 5.1, or for this node's next upgrade",
                                 "before the drain, let HCD 2.0 deliver them (nodetool listpendinghints, the target " +
                                 "up); otherwise the rollback goes on, and they wait, or remove them and repair the " +
                                 "tables they hold once the cluster runs one release"));
        if (!unknown.isEmpty())
            findings.add(warning("HINTS_FOR_UNKNOWN_TARGET",
                                 "hints files of version " + HINTS_VERSION + " for host ids in neither system.local " +
                                 "nor system.peers (a host that left the cluster, or one never saved): " +
                                 list(unknown) + "; DSE 5.1.51 deletes the files of a host removed from the ring, " +
                                 "and writes again for the current replicas a file whose host id it cannot map to " +
                                 "an address, so these hints may reach other replicas or none",
                                 "find out which node each host id was (nodetool status on another node); remove " +
                                 "the files of a node that left for good, and repair the tables they hold"));
    }

    /**
     * The hosts the node knows, by host id, from the {@code system.local} and {@code system.peers} of its data
     * directories: this node, and each peer with what it was recorded running; null, with a warning, when they cannot
     * be read.
     */
    @Nullable
    private Map<String, Target> targets()
    {
        Map<String, Target> targets = new LinkedHashMap<>();
        try
        {
            String system = SchemaConstants.SYSTEM_KEYSPACE_NAME;
            DseTableReader.TableRows peers = DseTableReader.readBig(layout.dataDirectories, system,
                                                                    SystemKeyspace.LEGACY_PEERS, List.of("peer"),
                                                                    List.of());
            for (UntypedResultSet.Row row : peers.rows)
            {
                if (!row.has("host_id"))
                    continue;
                InetAddress address = row.getInetAddress("peer");
                String release = row.has("release_version") ? row.getString("release_version") : null;
                boolean dse51 = DseCompat.isDse51ReleaseVersion(NodeInspector.parse(release));
                targets.put(row.getUUID("host_id").toString(),
                            new Target(address.getHostAddress() + ", " + (release == null ? "no release recorded"
                                                                                          : "release " + release),
                                       dse51));
            }
            DseTableReader.TableRows local = DseTableReader.readBig(layout.dataDirectories, system,
                                                                    SystemKeyspace.LOCAL, List.of("key"), List.of());
            for (UntypedResultSet.Row row : local.rows)
                if (row.has("host_id"))
                    targets.put(row.getUUID("host_id").toString(), new Target("this node", true));
            return targets;
        }
        catch (IOException | RuntimeException e)
        {
            findings.add(warning("HINTS_TARGETS_UNKNOWN",
                                 "cannot read system.local and system.peers of the data directories (" +
                                 rootCause(e) + "), so the targets of the hints files of version " + HINTS_VERSION +
                                 " are not checked",
                                 "check by hand (nodetool status on another node) that each host id the file names " +
                                 "start with is a node of the cluster that runs DSE 5.1"));
            return null;
        }
    }

    // ---- saved caches ----

    private void listSavedCaches()
    {
        File directory = savedCachesDirectory();
        if (!directory.isDirectory())
            return;
        File[] files = directory.tryList(File::isFile);
        if (files == null)
        {
            findings.add(warning("SAVED_CACHES_UNREADABLE",
                                 "cannot list the saved caches directory " + directory,
                                 "check it by hand: the rollback empties it before DSE 5.1 starts"));
            return;
        }
        // DSE 5.1 ignores caches of another version, and the rollback removes them anyway
        List<File> sorted = new ArrayList<>(List.of(files));
        sorted.sort(Comparator.comparing(File::name));
        sorted.forEach(f -> report.toRemove.add(f.toString()));
    }

    // ---- metadata directory ----

    /**
     * The files of the upgrade HCD 2.0 writes in its metadata directory in {@code DSE_5_1} mode, by their names: the
     * DSE 5.1 upgrade marker and the record of DSE 5.1 peers, which the rollback keeps, as for DSE 6, while any node
     * runs HCD 2.0 and while the node may be upgraded again; the record of the values of DSE 5.1's columns of
     * {@code system.local} and {@code system.peers}, and its temporary file, which the rollback removes; the copies of
     * unreadable records, which it keeps, never loaded, deleted with the marker at the flag drop.
     */
    private void checkMetadataDirectory()
    {
        File directory = layout.metadataDirectory;
        String markerName = DseUpgradeMarker.DSE_51_FILE_NAME;
        String peersRecordName = DsePeersRecord.DSE_51_FILE_NAME;
        String record = Dse51SystemTables.RECORD_FILE_NAME;
        String temporary = record + Dse51SystemTables.TEMPORARY_SUFFIX;
        String copyPrefix = record + Dse51SystemTables.UNREADABLE_SUFFIX + '-';
        String metadataRemedy = "pass --metadata-dir with the metadata_directory of HCD 2.0's cassandra.yaml";
        if (!directory.isDirectory())
        {
            findings.add(blocking("MARKER_NOT_FOUND",
                                  "the metadata directory " + directory + " does not exist or is not a directory: " +
                                  "HCD 2.0 creates its metadata directory at its first start, and writes there " +
                                  markerName + " at its first start in DSE_5_1 mode, so this is not HCD 2.0's " +
                                  "metadata directory on this node, or HCD 2.0 never started here",
                                  metadataRemedy + "; on a node where HCD 2.0 never completed a start in DSE_5_1 " +
                                  "mode there is no file of the upgrade to check, and the rest of the rollback " +
                                  "applies without this check"));
            return;
        }
        // decided from the names listed, so that a directory that can be listed but not searched hides nothing
        File[] files = directory.tryList(f -> f.name().equals(markerName) || f.name().equals(peersRecordName) ||
                                              f.name().equals(record) || f.name().equals(temporary) ||
                                              f.name().startsWith(copyPrefix));
        if (files == null)
        {
            findings.add(blocking("METADATA_UNREADABLE",
                                  "cannot list the metadata directory " + directory + ", so whether it holds the " +
                                  "DSE 5.1 upgrade marker " + markerName + " and the record " + record + ", which " +
                                  "the rollback removes, cannot be checked",
                                  RUN_AS_HCD_USER + ", or " + metadataRemedy));
            return;
        }
        List<File> sorted = new ArrayList<>(List.of(files));
        sorted.sort(Comparator.comparing(File::name));
        if (sorted.stream().noneMatch(f -> f.name().equals(markerName)))
        {
            findings.add(blocking("MARKER_NOT_FOUND",
                                  "the metadata directory " + directory + " holds no " + markerName + ", which HCD " +
                                  "2.0 writes at its first start in DSE_5_1 mode: it is not HCD 2.0's metadata " +
                                  "directory on this node, or HCD 2.0 never completed a start in DSE_5_1 mode here, " +
                                  "so the files of the upgrade listed from it may not be this node's",
                                  metadataRemedy + "; on a node where HCD 2.0 never completed a start in DSE_5_1 " +
                                  "mode there is no file of the upgrade to check, and the rest of the rollback " +
                                  "applies without this check"));
        }
        List<String> copies = new ArrayList<>();
        File recordFile = null;
        for (File file : sorted)
        {
            if (file.name().equals(markerName))
            {
                report.kept.add(file.toString());
                findings.add(info("MIXED_PHASE_MARKER",
                                  "the DSE 5.1 upgrade marker " + file + ", kept by the rollback: keep it while any " +
                                  "node runs HCD 2.0 and while this node may be upgraded again; DSE 5.1 does not " +
                                  "read it, and HCD 2.0 refuses to start on this node with NONE or DSE_6 while it " +
                                  "exists"));
            }
            else if (file.name().equals(peersRecordName))
            {
                report.kept.add(file.toString());
            }
            else if (file.name().startsWith(copyPrefix))
            {
                copies.add(file.toString());
                report.kept.add(file.toString());
            }
            else
            {
                if (file.name().equals(record))
                    recordFile = file;
                report.toRemove.add(file.toString());
            }
        }
        if (recordFile != null)
            findings.add(info("DSE51_SYSTEM_COLUMNS",
                              recordFile + ", the record of the values of DSE 5.1's columns of system.local and " +
                              "system.peers that HCD 2.0 read at its first start in DSE_5_1 mode: the rollback " +
                              "deletes it, and " + temporary + " if there, before DSE 5.1 starts, and keeps " +
                              markerName + " and " + peersRecordName + "; with the record and the marker both " +
                              "kept, the next start of HCD 2.0 on this node would load this record, with the values " +
                              "of before the rollback, instead of reading those DSE 5.1 writes after it"));
        if (!copies.isEmpty())
            findings.add(info("DSE51_SYSTEM_COLUMNS_COPIES",
                              "copies of records of DSE 5.1's columns HCD 2.0 could not read, kept by the rollback " +
                              "(never loaded; deleted with the DSE 5.1 upgrade marker at the flag drop): " +
                              list(copies)));
    }
}
