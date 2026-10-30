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
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.DseNodeRecord;
import org.apache.cassandra.nodes.DseNodesFiles;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.tools.dsepreflight.PaxosRollbackReport.Action;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The Paxos rollback mode of {@code dsepreflight} ({@code --paxos-rollback}): the steps of the rollback of a node to DSE
 * that protect its open Paxos rounds. Run on the node to roll back, with HCD 2.0 stopped after its drain and DSE not
 * started, from the unpacked HCD 2.0 tarball; it reads the node's files offline and never writes to them: it opens only
 * sstables of the trie format, offline and without validation, which neither rebuilds nor saves any component, and
 * writes only the files given with {@code --json} and {@code --cql}.
 * <p>
 * It lists the partitions with an open Paxos round in the {@code system.paxos} sstables of the data directories (those
 * HCD 2.0 left, flushed by the drain) and in those of the {@code pre-hcd-<timestamp>} snapshot the rollback restores,
 * and writes the serial reads that complete or supersede those rounds through another node before DSE starts. Each
 * sstable is read on its own (a row that a later sstable shadows is still listed: one read too many is harmless, one too
 * few is not). A row is open when {@code proposal_ballot} is live and either {@code most_recent_commit_at} is absent or
 * the timestamp of the proposal ballot is greater than that of the commit (the 60-bit timestamps of the time UUIDs, as
 * {@code Commit.isAfter} of Paxos v1 compares them, never their text). A row with only a promise is not open; an empty
 * proposal (left by a serial read or an LWT that did not apply) is, and is marked as such. Liveness is that of the run:
 * a proposal no later than its commit is listed too when the commit expires before it within
 * {@link #COMMIT_EXPIRY_MARGIN_SECONDS}, so the tool is run again if DSE does not start within that time.
 * <p>
 * The tables are found by {@code cf_id} in {@code system_schema}, read offline from the data directories and from the
 * snapshot; the serialized partition key ({@code row_key}) is rendered as CQL literals of the partition key types, each
 * checked by parsing it back to the same bytes. Per keyspace these replication rules apply: no read where every quorum
 * includes all the other replicas (a total replication factor of 2 at {@code SERIAL}, a local one of 2 at
 * {@code LOCAL_SERIAL}), no protection where this node is the whole quorum (1).
 * <p>
 * A partition is read at the serial consistency its table is written at, never at the other: where the two are
 * different quorums, a serial read at the other one can itself complete an old proposal its quorum still holds over a
 * value a quorum of the right consistency committed later. The tool cannot know which one a table uses, so it chooses
 * only where both are the same quorum: a keyspace whose replicas are all in this node's data center (any keyspace of a
 * cluster with one data center, by the nodes files). Elsewhere it lists no read until the operator names the consistency
 * with {@code --serial-consistency <keyspace>=<level>} ({@code *} for every keyspace not named).
 * <p>
 * The sstables are streamed, but the partitions with an open round are all held, and printed without {@code --cql}: one
 * per partition that a serial read or an LWT touched within the TTL of {@code system.paxos} without a later commit
 * (empty proposals included). A cluster with very many such partitions needs a larger heap than the 512M of
 * {@code tools/bin/dsepreflight} ({@code MAX_HEAP_SIZE}) and {@code --cql}.
 */
final class PaxosRollback
{
    private static final String SNAPSHOT_PREFIX = "pre-hcd-";
    static final String SERIAL = "SERIAL";
    static final String LOCAL_SERIAL = "LOCAL_SERIAL";
    private static final String DATA_DIRECTORY = "data directory";
    private static final String HCD = "HCD 2.0";
    private static final String DSE = "DSE";
    private static final String PAXOS_DIRECTORY_OPTION = "--paxos-dir";
    /** The key of {@code --serial-consistency} that applies to every keyspace not named. */
    private static final String ALL_KEYSPACES = "*";
    private static final List<String> PAXOS_KEY = List.of("row_key");
    private static final List<String> PAXOS_CLUSTERING = List.of("cf_id");
    private static final String SIMPLE_STRATEGY = "SimpleStrategy";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
    private static final String LOCAL_STRATEGY = "LocalStrategy";
    private static final String EVERYWHERE_STRATEGY = "EverywhereStrategy";
    private static final String LOCATOR_PACKAGE = "org.apache.cassandra.locator.";
    /**
     * How long after the run a commit must stay live for the tool to count on it: a commit that expires sooner, and before
     * the proposal of its row, may be gone when DSE starts, which then reads the proposal alone as an open round.
     */
    static final long COMMIT_EXPIRY_MARGIN_SECONDS = TimeUnit.DAYS.toSeconds(1);

    /** A wrong use of the mode, or an input the rollback requires that is missing: exit code {@link DsePreflight#EXIT_ERROR}. */
    static final class UsageException extends Exception
    {
        UsageException(String message)
        {
            super(message);
        }
    }

    /** What the operator gave on the command line, and the layout of the node. */
    static final class Options
    {
        List<File> dataDirectories = new ArrayList<>();
        File metadataDirectory;
        /** DSE's {@code cassandra-rackdc.properties}, the last source of the data center */
        @Nullable
        String rackdcDataCenter;
        @Nullable
        String snapshot;
        List<File> paxosDirectories = new ArrayList<>();
        /** the operator states that system has not been restored yet ({@code --not-restored}) */
        boolean notRestored;
        @Nullable
        String dataCenter;
        /** the serial consistency by keyspace, {@link #ALL_KEYSPACES} for every keyspace not named */
        Map<String, String> serialConsistencies = new TreeMap<>();
        /** the directories taken from DSE's defaults because DSE's {@code cassandra.yaml} does not set them */
        List<String> defaultsAssumed = new ArrayList<>();
        /** why DSE's {@code cassandra.yaml} cannot be read; null when it was */
        @Nullable
        String cassandraYamlUnreadable;
        /** why DSE's {@code cassandra-rackdc.properties} cannot be read; null when it was */
        @Nullable
        String rackdcUnreadable;
        String hostname;
        String softwareVersion;
        String generatedAt;
    }

    /** A directory of {@code system.paxos} sstables, and what it is. */
    private static final class Source
    {
        final String label;
        final File directory;
        /** given with {@code --paxos-dir}: it must hold sstables of {@code system.paxos} */
        final boolean given;

        Source(String label, File directory)
        {
            this(label, directory, false);
        }

        Source(String label, File directory, boolean given)
        {
            this.label = label;
            this.directory = directory;
            this.given = given;
        }
    }

    /**
     * A directory given with {@code --paxos-dir} that holds no sstable of {@code system.paxos}, with the index of the
     * problem reserved for it.
     */
    private static final class NoPaxosSSTable
    {
        /** the index of its problem in the problems of the report */
        final int problem;
        final Source source;
        /** the number of sstables of other tables it holds */
        final int others;

        NoPaxosSSTable(int problem, Source source, int others)
        {
            this.problem = problem;
            this.source = source;
            this.others = others;
        }
    }

    /** A partition key column of a table. */
    private static final class KeyColumn
    {
        final String name;
        final AbstractType<?> type;

        KeyColumn(String name, AbstractType<?> type)
        {
            this.name = name;
            this.type = type;
        }
    }

    /** A table of the schema, by id. */
    private static final class Table
    {
        final String keyspace;
        final String name;
        /** the partition key columns in order; empty when {@link #problem} is set */
        final List<KeyColumn> partitionKey;
        @Nullable
        final String problem;

        Table(String keyspace, String name, List<KeyColumn> partitionKey, @Nullable String problem)
        {
            this.keyspace = keyspace;
            this.name = name;
            this.partitionKey = partitionKey;
            this.problem = problem;
        }
    }

    /** The replication of a keyspace, as far as the tool can tell. */
    private static final class Replication
    {
        final String description;
        /** the number of replicas of each partition; null when unknown, with {@link #unknown} */
        @Nullable
        final Integer total;
        /** the number of replicas in this node's data center; null when unknown, with {@link #unknown} */
        @Nullable
        final Integer local;
        /** why {@link #total} or {@link #local} is unknown */
        @Nullable
        final String unknown;
        /** whether every replica is in this node's data center: then SERIAL and LOCAL_SERIAL are the same quorum */
        final boolean localOnly;
        /** a keyspace whose data never leaves a node ({@code LocalStrategy}) */
        final boolean nodeLocal;

        Replication(String description, @Nullable Integer total, @Nullable Integer local, @Nullable String unknown, boolean localOnly, boolean nodeLocal)
        {
            this.description = description;
            this.total = total;
            this.local = local;
            this.unknown = unknown;
            this.localOnly = localOnly;
            this.nodeLocal = nodeLocal;
        }
    }

    private final Options options;
    private final PaxosRollbackReport report = new PaxosRollbackReport();
    private final Map<String, Map<String, String>> keyspaces = new HashMap<>();
    private final Map<UUID, Table> tables = new HashMap<>();
    /** the partitions with an open row, by cf_id then row_key in hex */
    private final Map<String, PaxosRollbackReport.Partition> partitions = new TreeMap<>();
    /** every directory read for system.paxos sstables, by real path, with the label of its source */
    private final Map<Path, String> readDirectories = new HashMap<>();
    /**
     * The labels of the directories given with {@code --paxos-dir} that hold sstables of {@code system.paxos}, in the
     * order given; complete once every directory is scanned.
     */
    private final List<String> givenWithPaxosSSTables = new ArrayList<>();
    /**
     * The directories given with {@code --paxos-dir} that hold no sstable of {@code system.paxos}: the problem of each is
     * reserved at its place when it is scanned and built once every directory is scanned ({@link #noPaxosSSTable}, which
     * may name the directories given that hold some), so that the problems keep the order of the directories read.
     */
    private final List<NoPaxosSSTable> noPaxosSSTables = new ArrayList<>();
    /** the number of nodes in each data center by the nodes files; null when they cannot tell */
    @Nullable
    private Map<String, Integer> nodesByDataCenter;
    /** the time of the run, in seconds: the expiry of a commit is compared with it */
    private final long nowInSeconds = FBUtilities.nowInSeconds();

    PaxosRollback(Options options)
    {
        this.options = options;
    }

    /**
     * Reads the node and builds the report.
     *
     * @throws UsageException when an input the rollback requires cannot be found: the snapshot, or any schema
     */
    PaxosRollbackReport run() throws UsageException
    {
        report.softwareVersion = options.softwareVersion;
        report.generatedAt = options.generatedAt;
        report.hostname = options.hostname;
        options.dataDirectories.forEach(d -> report.dataDirectories.add(d.toString()));
        report.defaultsAssumed.addAll(options.defaultsAssumed);
        report.notRestored = options.notRestored;
        report.validUntil = Instant.ofEpochSecond(nowInSeconds + COMMIT_EXPIRY_MARGIN_SECONDS).toString();
        if (options.cassandraYamlUnreadable != null)
        {
            List<String> directories = options.defaultsAssumed.stream().filter(d -> d.startsWith("data_file_directories:") || d.startsWith("metadata_directory:"))
                                                              .collect(Collectors.toList());
            if (!directories.isEmpty())
                report.problems.add("cannot read cassandra.yaml " + options.cassandraYamlUnreadable + ": DSE's defaults are assumed (" + String.join(", ", directories) +
                                    "), which may not be this node's directories: pass --conf-dir or --cassandra-yaml, or --data-dir and --metadata-dir");
        }
        String snapshot = snapshot();
        report.snapshot = snapshot;
        readDataCenters();
        readSchema(snapshot);
        checkDataCenter();
        checkSerialConsistencies();
        for (Source source : sources(snapshot))
            scan(source);
        int emptyPaxosTables = (int) noPaxosSSTables.stream().filter(d -> isEmptyPaxosTable(d.source.directory)).count();
        for (NoPaxosSSTable given : noPaxosSSTables)
            report.problems.set(given.problem, noPaxosSSTable(given.source.label, given.source.directory, given.others, emptyPaxosTables));
        for (PaxosRollbackReport.Partition partition : partitions.values())
        {
            plan(partition);
            report.partitions.add(partition);
        }
        report.partitions.sort(Comparator.comparing((PaxosRollbackReport.Partition p) -> p.keyspace == null ? "" : p.keyspace)
                                         .thenComparing(p -> p.table == null ? "" : p.table)
                                         .thenComparing(p -> p.cfId)
                                         .thenComparing(p -> p.rowKey));
        boolean attention = !report.problems.isEmpty() || report.partitions.stream().anyMatch(PaxosRollbackReport.Partition::needsAttention);
        boolean reads = !readAt(report, SERIAL).isEmpty() || !readAt(report, LOCAL_SERIAL).isEmpty();
        report.result = attention ? PaxosRollbackReport.NEEDS_ATTENTION
                                  : report.partitions.isEmpty() ? PaxosRollbackReport.NOTHING_TO_READ
                                  : reads ? PaxosRollbackReport.READS_LISTED : PaxosRollbackReport.NO_READ_NEEDED;
        return report;
    }

    // ---- inputs ----

    /** The tag of the snapshot to read: the one given, else the only {@code pre-hcd-*} snapshot of the node. */
    private String snapshot() throws UsageException
    {
        if (options.snapshot != null && (options.snapshot.isEmpty() || options.snapshot.equals(".") || options.snapshot.equals("..") ||
                                         options.snapshot.contains("/")))
            throw new UsageException("--snapshot takes the tag of a snapshot, not a path: " + options.snapshot);
        Set<String> tags = new TreeSet<>();
        for (File dataDirectory : options.dataDirectories)
            for (String keyspace : List.of(SchemaConstants.SYSTEM_KEYSPACE_NAME, SchemaConstants.SCHEMA_KEYSPACE_NAME))
                for (File tableDirectory : tableDirectories(new File(dataDirectory, keyspace), null))
                    for (File snapshotDirectory : list(new File(tableDirectory, Directories.SNAPSHOT_SUBDIR), File::isDirectory))
                        if (options.snapshot != null ? snapshotDirectory.name().equals(options.snapshot) : snapshotDirectory.name().startsWith(SNAPSHOT_PREFIX))
                            tags.add(snapshotDirectory.name());
        if (options.snapshot != null)
        {
            if (tags.isEmpty())
                throw new UsageException("no snapshot " + options.snapshot + " of the system or system_schema tables in the data directories " + options.dataDirectories +
                                         problemsSoFar());
            return options.snapshot;
        }
        if (tags.isEmpty())
            throw new UsageException("no " + SNAPSHOT_PREFIX + "<timestamp> snapshot of the system or system_schema tables in the data directories " + options.dataDirectories +
                                     ": pass --snapshot <tag>, the snapshot taken before the upgrade, or --data-dir if the data directories are not " +
                                     "those of DSE's cassandra.yaml" + problemsSoFar());
        if (tags.size() > 1)
            throw new UsageException("several " + SNAPSHOT_PREFIX + "<timestamp> snapshots in the data directories: " + String.join(", ", tags) +
                                     "; pass --snapshot <tag>, the one the rollback restores" + problemsSoFar());
        return tags.iterator().next();
    }

    /** The problems found before a usage error, which may be its cause, appended to its message; empty when there is none. */
    private String problemsSoFar()
    {
        return report.problems.isEmpty() ? "" : " (problems found before: " + String.join("; ", report.problems) + ')';
    }

    /**
     * The directories of {@code system.paxos} to read: those of the data directories, their snapshot, and those given
     * with {@code --paxos-dir}. A data directory without {@code system.paxos}, or a snapshot without it, is a problem:
     * both must be read. Without {@code --paxos-dir}, data directories that hold only sstables of the snapshot are a
     * problem too ({@link #restoreState}): the sstables HCD 2.0 wrote may be elsewhere. A directory of
     * {@code --paxos-dir} that holds no sstable of {@code system.paxos} is a problem of {@link #scan}. A snapshot directory
     * of {@code system.paxos} that holds no sstable is not: DSE had no Paxos state when the snapshot was taken. A
     * {@code --paxos-dir} is read through its normalized absolute path; one whose normalized path is not, or cannot be
     * shown to be, the directory it names (a ".." after a symbolic link) is a problem and is not read; one that is a
     * directory already read (the same real path) is a problem, and is read once; the comparison with the snapshot is
     * made when no {@code --paxos-dir} is read.
     */
    private List<Source> sources(String snapshot)
    {
        List<Source> sources = new ArrayList<>();
        List<String> withoutPaxos = new ArrayList<>();
        boolean snapshotFound = false;
        for (File dataDirectory : options.dataDirectories)
        {
            // every data directory holds a directory of each table (the node creates them at its start): one without it is
            // a wrong data directory, or one whose Paxos state cannot be read
            File systemDirectory = new File(dataDirectory, SchemaConstants.SYSTEM_KEYSPACE_NAME);
            File[] listed = systemDirectory.tryList(f -> f.isDirectory() && isTableDirectory(f, SystemKeyspace.PAXOS));
            if (listed == null || listed.length == 0)
            {
                withoutPaxos.add(dataDirectory + (!dataDirectory.isDirectory() ? " (not a directory)"
                                                  : !systemDirectory.isDirectory() ? " (no system directory)"
                                                  : listed == null ? " (its system directory cannot be listed)"
                                                  : " (no system/paxos-<id>)"));
                continue;
            }
            Arrays.sort(listed, Comparator.comparing(File::toString));
            for (File tableDirectory : listed)
            {
                sources.add(new Source(DATA_DIRECTORY, tableDirectory));
                File snapshotDirectory = new File(new File(tableDirectory, Directories.SNAPSHOT_SUBDIR), snapshot);
                if (snapshotDirectory.isDirectory())
                {
                    sources.add(new Source("snapshot " + snapshot, snapshotDirectory));
                    snapshotFound = true;
                }
            }
        }
        if (sources.isEmpty())
            report.problems.add("no directory of system.paxos (system/paxos-<id>) in the data directories " + options.dataDirectories +
                                ": nothing was read; check the data directories (--data-dir)");
        else if (!withoutPaxos.isEmpty())
            report.problems.add("no directory of system.paxos (system/paxos-<id>) in the data " + (withoutPaxos.size() == 1 ? "directory " : "directories ") +
                                String.join(", ", withoutPaxos) + ": the Paxos state of " + (withoutPaxos.size() == 1 ? "that data directory" : "those data directories") +
                                " was not read; check the data directories (--data-dir)");
        for (Source source : sources)
            readDirectories.putIfAbsent(realPath(source.directory), source.label);
        List<Source> given = new ArrayList<>();
        for (File directory : options.paxosDirectories)
        {
            // a --paxos-dir is read once: its open rows would otherwise be listed twice; the problem comes first, as dropping
            // it is the first thing to do (--not-restored, which the problems below may advise, is refused with --paxos-dir)
            String label = PAXOS_DIRECTORY_OPTION + ' ' + directory;
            // read through its normalized absolute path: the keyspace and table of an sstable are taken from the names of its
            // directory and of its parent, which a "." or ".." in the path given would hide; refused when that path is not the
            // directory given (a ".." after a symbolic link)
            Path normalized = directory.toPath().toAbsolutePath().normalize();
            String notSame = notSameFile(directory.toPath(), normalized);
            if (notSame != null)
            {
                report.problems.add(label + (notSame.isEmpty() ? " is not the directory its path names without \".\" and \"..\" (" + normalized +
                                                                 "), as a symbolic link comes before a \"..\""
                                                               : " cannot be compared with the directory its path names without \".\" and \"..\" (" +
                                                                 normalized + "): " + notSame) +
                                    ": nothing was read from it; give the directory without \".\" and \"..\"");
                continue;
            }
            String earlier = readDirectories.putIfAbsent(realPath(directory), label);
            if (earlier != null)
                report.problems.add(label + " is the directory already read as " + earlier + ", read once: drop this " + PAXOS_DIRECTORY_OPTION);
            else
                given.add(new Source(label, new File(normalized), true));
        }
        RestoreState state = snapshotFound && given.isEmpty() ? restoreState(sources) : RestoreState.NOT_RESTORED;
        if (state == RestoreState.PARTLY_RESTORED && !options.notRestored)
            report.problems.add("the system.paxos sstables of the data directories are some of those of the snapshot " + snapshot + " (by name and size; also " +
                                "when the data directories hold none), and the others are missing: a restore of system in progress or interrupted (the " +
                                "system.paxos sstables HCD 2.0 wrote would not be read: finish the restore and pass --paxos-dir with each directory they " +
                                "were moved to, such as <quarantine>/system/paxos-<id>), or snapshot sstables HCD 2.0 dropped because all their cells had " +
                                "expired, without writing any of its own (then pass --not-restored)");
        else if (state == RestoreState.LOOKS_RESTORED && !options.notRestored)
            report.problems.add("every system.paxos sstable of the data directories is also in the snapshot " + snapshot + " (by name and size; also when " +
                                "the data directories hold none): system looks already restored, so the system.paxos sstables HCD 2.0 wrote would not be " +
                                "read. If the restore ran, pass --paxos-dir with each directory they were moved to (such as " +
                                "<quarantine>/system/paxos-<id>); if it did not (HCD 2.0 wrote no system.paxos sstable on this node: no LWT and no " +
                                "compaction of system.paxos), pass --not-restored");
        if (!sources.isEmpty() && !snapshotFound)
            report.problems.add("the snapshot " + snapshot + " has no directory of system.paxos (system/paxos-<id>/snapshots/" + snapshot + "): its Paxos state was " +
                                "not read; check that the snapshot is complete before the restore");
        sources.addAll(given);
        return sources;
    }

    /** How the {@code system.paxos} sstables of the data directories compare with those of the snapshot. */
    private enum RestoreState
    {
        /** the data directories hold an sstable the snapshot does not: HCD 2.0's */
        NOT_RESTORED,
        /** the same sstables, by name and size (none in both included) */
        LOOKS_RESTORED,
        /** only sstables of the snapshot, but not all of them (none included) */
        PARTLY_RESTORED
    }

    /**
     * How the {@code system.paxos} sstables of the data directories compare with those of the snapshot, by name and size.
     * The same sstables are the layout after the restore of {@code system}, where the sstables HCD 2.0 wrote are elsewhere,
     * and also the layout of a node where HCD 2.0 wrote no {@code system.paxos} sstable, which the tool cannot tell apart
     * (DSE 6 has no {@code system.local} sstable either): the operator says which. Only some of the snapshot's is a
     * restore in progress or interrupted, or the layout of a node where HCD 2.0 dropped snapshot sstables whose cells had
     * all expired (system tables have no gc_grace_seconds, and Paxos cells carry a TTL) without writing any: the operator
     * says which too.
     */
    private static RestoreState restoreState(List<Source> sources)
    {
        Map<String, Long> live = new HashMap<>();
        Map<String, Long> snapshot = new HashMap<>();
        for (Source source : sources)
        {
            Map<String, Long> target = DATA_DIRECTORY.equals(source.label) ? live : snapshot;
            for (File file : list(source.directory, f -> f.isFile() && f.name().endsWith("-Data.db")))
                target.put(file.name(), file.length());
        }
        if (!live.entrySet().stream().allMatch(e -> e.getValue().equals(snapshot.get(e.getKey()))))
            return RestoreState.NOT_RESTORED;
        return live.size() == snapshot.size() ? RestoreState.LOOKS_RESTORED : RestoreState.PARTLY_RESTORED;
    }

    /** Null when two paths name the same file; empty when they do not; why they cannot be compared otherwise. */
    @Nullable
    private static String notSameFile(Path one, Path other)
    {
        try
        {
            return Files.isSameFile(one, other) ? null : "";
        }
        catch (IOException e)
        {
            return e.toString();
        }
    }

    /** The name of a file, the last element of its path; empty for the root. */
    static String nameOf(File file)
    {
        Path name = file.toPath().getFileName();
        return name == null ? "" : name.toString();
    }

    /** Whether the parent of a directory is named {@code system}; false for the root and the directories right under it. */
    static boolean inSystemDirectory(File directory)
    {
        File parent = directory.parent();
        return parent != null && SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(nameOf(parent));
    }

    /** The real path of a directory, symbolic links resolved; its normalized absolute path when it cannot be resolved. */
    private static Path realPath(File directory)
    {
        try
        {
            return directory.toPath().toRealPath();
        }
        catch (IOException e)
        {
            return directory.toPath().toAbsolutePath().normalize();
        }
    }

    /** The table directories of a keyspace directory: of {@code table} only when it is not null; empty when there is none. */
    private static List<File> tableDirectories(File keyspaceDirectory, @Nullable String table)
    {
        return list(keyspaceDirectory, f -> f.isDirectory() && (table == null || isTableDirectory(f, table)));
    }

    /** Whether {@code directory} is named as a table directory of {@code table}: {@code <table>-<id>}, or {@code <table>} before Cassandra 2.1. */
    static boolean isTableDirectory(File directory, String table)
    {
        return DseTableReader.isDirectoryOf(nameOf(directory), table) || nameOf(directory).equals(table);
    }

    /** The files of a directory accepted by {@code filter}, sorted by name; empty when it is not a directory or cannot be listed. */
    private static List<File> list(File directory, Predicate<File> filter)
    {
        File[] files = directory.tryList(filter);
        if (files == null)
            return List.of();
        List<File> sorted = new ArrayList<>(List.of(files));
        sorted.sort(Comparator.comparing(File::toString));
        return sorted;
    }

    /** This node's data center, and the nodes of each data center, from the options and the nodes files. */
    private void readDataCenters()
    {
        DseNodeRecord local = null;
        List<DseNodeRecord> peers = null;
        Path nodes = options.metadataDirectory.toPath().resolve("nodes");
        try
        {
            local = DseNodesFiles.readLocal(nodes);
            // an absent peers file tells nothing about the other nodes; an empty one says there is none
            if (Files.exists(nodes.resolve("peers")))
                peers = DseNodesFiles.readPeers(nodes);
        }
        catch (RuntimeException e)
        {
            report.problems.add("cannot read the nodes files of " + nodes + " (" + e.getMessage() + "): the data centers of the cluster are unknown");
        }
        if (options.dataCenter != null)
        {
            report.dataCenter = options.dataCenter;
            report.dataCenterSource = "--dc";
            if (local != null && local.dataCenter != null && !local.dataCenter.equals(options.dataCenter))
                report.problems.add("--dc " + options.dataCenter + " but " + nodes.resolve("local") + " records data center " + local.dataCenter + ": check --dc");
        }
        else if (local != null && local.dataCenter != null)
        {
            report.dataCenter = local.dataCenter;
            report.dataCenterSource = nodes.resolve("local").toString();
        }
        else if (options.rackdcDataCenter != null)
        {
            report.dataCenter = options.rackdcDataCenter;
            report.dataCenterSource = "cassandra-rackdc.properties";
        }
        if (report.dataCenter == null && options.rackdcUnreadable != null)
            report.problems.add("cannot read cassandra-rackdc.properties " + options.rackdcUnreadable + ", and the nodes files name no data center for this " +
                                "node: pass --dc, or --rackdc <file>");
        if (local != null && local.dataCenter != null && peers != null && peers.stream().allMatch(p -> p.dataCenter != null))
        {
            Map<String, Integer> counts = new TreeMap<>();
            counts.merge(local.dataCenter, 1, Integer::sum);
            for (DseNodeRecord peer : peers)
                counts.merge(peer.dataCenter, 1, Integer::sum);
            nodesByDataCenter = counts;
            report.singleDataCenter = counts.size() == 1;
        }
    }

    /** That this node's data center is one the nodes files or the replication of some keyspace name. */
    private void checkDataCenter()
    {
        if (report.dataCenter == null)
            return;
        Set<String> known = new TreeSet<>();
        if (nodesByDataCenter != null)
            known.addAll(nodesByDataCenter.keySet());
        for (Map<String, String> replication : keyspaces.values())
            if (NETWORK_TOPOLOGY_STRATEGY.equals(simpleName(replication.getOrDefault("class", ""))))
                replication.keySet().stream().filter(k -> !"class".equals(k) && !"replication_factor".equals(k)).forEach(known::add);
        if (!known.isEmpty() && !known.contains(report.dataCenter))
            report.problems.add("data center " + report.dataCenter + " (" + report.dataCenterSource + ") is neither in the nodes files nor in the replication of any " +
                                "keyspace (" + String.join(", ", known) + "): check --dc");
    }

    /** That every keyspace named with {@code --serial-consistency} is in the schema. */
    private void checkSerialConsistencies()
    {
        for (String keyspace : options.serialConsistencies.keySet())
            if (!ALL_KEYSPACES.equals(keyspace) && !keyspaces.containsKey(keyspace))
                report.problems.add("--serial-consistency names keyspace " + keyspace + ", which is in no schema read: check its name");
    }

    /**
     * Reads {@code system_schema} of the data directories, then of the snapshot for what they do not have. A schema that
     * cannot be read is a problem of the report while the other one is read, which then describes the tables alone.
     */
    private void readSchema(String snapshot) throws UsageException
    {
        List<String> absent = new ArrayList<>();
        List<String> unreadable = new ArrayList<>();
        String liveLabel = "data directories";
        String snapshotLabel = "snapshot " + snapshot;
        boolean live = readSchema(null, liveLabel, absent, unreadable);
        boolean fromSnapshot = readSchema(snapshot, snapshotLabel, absent, unreadable);
        if (!live && !fromSnapshot)
        {
            List<String> reasons = new ArrayList<>(absent);
            reasons.addAll(report.problems);
            reasons.addAll(unreadable);
            throw new UsageException("cannot read system_schema in the data directories " + options.dataDirectories + " nor in their snapshot " + snapshot + ": " +
                                     String.join("; ", reasons));
        }
        for (String problem : unreadable)
            report.problems.add(problem + ": the tables are taken from the schema of the " + (live ? liveLabel : snapshotLabel) + " alone, and a partition of " +
                                "a table not in it is listed to handle by hand; this problem needs no other action when no partition is listed to " +
                                "handle by hand");
    }

    /**
     * Reads one schema; returns whether it was read. A schema that is absent is added to {@code absent}, which is not a
     * problem while the other one is read (both describe the same tables: the schema does not change during the upgrade); a
     * schema that cannot be read is added to {@code unreadable}, a problem of the report while the other one is read.
     */
    private boolean readSchema(@Nullable String snapshot, String label, List<String> absent, List<String> unreadable)
    {
        String schema = SchemaConstants.SCHEMA_KEYSPACE_NAME;
        try
        {
            DseTableReader.TableRows keyspaceRows = DseTableReader.read(options.dataDirectories, snapshot, schema, SchemaKeyspaceTables.KEYSPACES, List.of("keyspace_name"), List.of());
            DseTableReader.TableRows tableRows = DseTableReader.read(options.dataDirectories, snapshot, schema, SchemaKeyspaceTables.TABLES, List.of("keyspace_name"), List.of("table_name"));
            DseTableReader.TableRows columnRows = DseTableReader.read(options.dataDirectories, snapshot, schema, SchemaKeyspaceTables.COLUMNS, List.of("keyspace_name"),
                                                                      List.of("table_name", "column_name"));
            DseTableReader.TableRows typeRows = DseTableReader.read(options.dataDirectories, snapshot, schema, SchemaKeyspaceTables.TYPES, List.of("keyspace_name"), List.of("type_name"));
            if (keyspaceRows.rows.isEmpty() || tableRows.rows.isEmpty() || columnRows.rows.isEmpty())
            {
                absent.add("no schema in system_schema of the " + label + " (keyspaces, tables or columns has no row)");
                return false;
            }
            Map<String, Map<String, String>> replication = new HashMap<>();
            for (UntypedResultSet.Row row : keyspaceRows.rows)
                replication.put(row.getString("keyspace_name"), row.has("replication") ? row.getFrozenTextMap("replication") : Map.of());
            Map<String, Types.RawBuilder> typeBuilders = new HashMap<>();
            for (UntypedResultSet.Row row : typeRows.rows)
                typeBuilders.computeIfAbsent(row.getString("keyspace_name"), Types::rawBuilder)
                            .add(row.getString("type_name"),
                                 row.has("field_names") ? row.getFrozenList("field_names", UTF8Type.instance) : List.of(),
                                 row.has("field_types") ? row.getFrozenList("field_types", UTF8Type.instance) : List.of());
            Map<String, Types> types = new HashMap<>();
            Map<String, String> typeProblems = new HashMap<>();
            typeBuilders.forEach((keyspace, builder) -> {
                try
                {
                    types.put(keyspace, builder.build());
                }
                catch (RuntimeException e)
                {
                    typeProblems.put(keyspace, "the user types of keyspace " + keyspace + " cannot be loaded (" + e.getMessage() + ')');
                }
            });
            Map<String, Map<String, List<UntypedResultSet.Row>>> keyColumns = new HashMap<>();
            for (UntypedResultSet.Row row : columnRows.rows)
                if ("partition_key".equals(row.getString("kind")))
                    keyColumns.computeIfAbsent(row.getString("keyspace_name"), k -> new HashMap<>()).computeIfAbsent(row.getString("table_name"), k -> new ArrayList<>()).add(row);

            // built apart and merged once the whole schema is read, so that a schema that fails half-way adds nothing
            Map<UUID, Table> read = new HashMap<>();
            for (UntypedResultSet.Row row : tableRows.rows)
            {
                UUID id = row.getUUID("id");
                String keyspace = row.getString("keyspace_name");
                String table = row.getString("table_name");
                read.putIfAbsent(id, table(keyspace, table, keyColumns.getOrDefault(keyspace, Map.of()).getOrDefault(table, List.of()),
                                           types.getOrDefault(keyspace, Types.none()), typeProblems.get(keyspace)));
            }
            replication.forEach(keyspaces::putIfAbsent);
            read.forEach(tables::putIfAbsent);
            report.schemas.add(label);
            return true;
        }
        catch (IOException | RuntimeException e)
        {
            unreadable.add("cannot read system_schema of the " + label + " (" + e + ')');
            return false;
        }
    }

    private static Table table(String keyspace, String table, List<UntypedResultSet.Row> keyColumnRows, Types types, @Nullable String typeProblem)
    {
        if (keyColumnRows.isEmpty())
            return new Table(keyspace, table, List.of(), "no partition key column in system_schema.columns");
        List<UntypedResultSet.Row> sorted = new ArrayList<>(keyColumnRows);
        sorted.sort(Comparator.comparingInt(r -> r.getInt("position")));
        List<KeyColumn> columns = new ArrayList<>();
        try
        {
            for (UntypedResultSet.Row row : sorted)
                columns.add(new KeyColumn(row.getString("column_name"), CQLTypeParser.parse(keyspace, row.getString("type"), types)));
        }
        catch (RuntimeException e)
        {
            return new Table(keyspace, table, List.of(), "the partition key types cannot be parsed (" + e.getMessage() + ')' +
                                                         (typeProblem == null ? "" : "; " + typeProblem));
        }
        return new Table(keyspace, table, columns, null);
    }

    // ---- the scan ----

    /**
     * Reads the sstables of a directory, each on its own, and records the directory in the report with the number of
     * sstables found in it, so that a directory that holds none is visible too. A directory given with
     * {@code --paxos-dir} that holds no sstable of {@code system.paxos} is a problem ({@link #noPaxosSSTable}), reserved
     * at its place here and built once every directory is scanned.
     */
    private void scan(Source source)
    {
        PaxosRollbackReport.Directory directory = new PaxosRollbackReport.Directory();
        directory.source = source.label;
        directory.path = source.directory.toString();
        report.directories.add(directory);
        List<Descriptor> descriptors;
        try
        {
            descriptors = new ArrayList<>(DseTableReader.sstablesIn(source.directory));
        }
        catch (RuntimeException e)
        {
            String reason = e.getMessage() != null ? e.getMessage() : e.toString();
            directory.error = "cannot be listed: " + reason;
            report.problems.add("cannot list the sstables of " + source.directory + " (" + reason + "): check them by hand with sstabledump on a copy of their files");
            return;
        }
        descriptors.sort(Comparator.comparing(d -> d.baseFile().toString()));
        directory.sstables = descriptors.size();
        directory.otherTableSSTables = 0;
        directory.rows = 0L;
        int paxosSSTables = 0;
        for (Descriptor descriptor : descriptors)
        {
            PaxosRollbackReport.SSTable sstable = new PaxosRollbackReport.SSTable();
            sstable.source = source.label;
            sstable.path = descriptor.baseFile().toString();
            sstable.format = descriptor.version.format.name() + ' ' + descriptor.version;
            report.sstables.add(sstable);
            if (!SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(descriptor.ksname) || !SystemKeyspace.PAXOS.equals(descriptor.cfname))
            {
                sstable.error = "not an sstable of system.paxos: its directory names " + descriptor.ksname + '.' + descriptor.cfname +
                                " (the directory of an sstable of system.paxos is laid out as <...>/system/paxos-<id>)";
                report.problems.add(sstable.path + ": " + sstable.error);
                directory.otherTableSSTables++;
                continue;
            }
            paxosSSTables++;
            try
            {
                // streamed: a system.paxos sstable of a cluster with many LWTs is large; only the open rows are kept
                DseTableReader.readSSTable(descriptor, PAXOS_KEY, PAXOS_CLUSTERING, (row, cells) -> {
                    sstable.rows++;
                    PaxosRollbackReport.OpenRow open = openRow(row, cells, nowInSeconds, source.label, descriptor.baseFile().name());
                    if (open == null)
                        return;
                    sstable.openRows++;
                    UUID cfId = row.getUUID("cf_id");
                    String rowKey = ByteBufferUtil.bytesToHex(row.getBytes("row_key"));
                    partitions.computeIfAbsent(cfId + "/" + rowKey, k -> {
                        PaxosRollbackReport.Partition p = new PaxosRollbackReport.Partition();
                        p.cfId = cfId.toString();
                        p.rowKey = rowKey;
                        return p;
                    }).openRows.add(open);
                });
            }
            catch (IOException | RuntimeException e)
            {
                sstable.error = e.toString();
                report.problems.add("cannot read " + sstable.path + " (" + e + "): check it by hand with sstabledump on a copy of its files");
            }
            report.rowsRead += sstable.rows;
            directory.rows += sstable.rows;
        }
        if (source.given && paxosSSTables != 0)
            givenWithPaxosSSTables.add(source.label);
        if (source.given && paxosSSTables == 0)
        {
            noPaxosSSTables.add(new NoPaxosSSTable(report.problems.size(), source, directory.otherTableSSTables));
            report.problems.add(null);
        }
    }

    /**
     * The problem of a directory given with {@code --paxos-dir} that holds no sstable of {@code system.paxos}, such as the
     * quarantine or its {@code system} directory instead of {@code <quarantine>/system/paxos-<id>}: nothing of HCD 2.0's
     * Paxos state was read from it. An sstable is read as one of {@code system.paxos} only from a directory laid out as
     * {@code <...>/system/paxos-<id>} ({@link Descriptor}), by the names in the path it is read through: the normalized
     * absolute path of the directory given, which is used here too, so that the directories named are read as they are
     * recognised. The table directories of {@code system.paxos} named are those of its {@code system} subdirectory, and its
     * own when it is itself named {@code system}; each is named with the sstables it holds (counted as {@link #scan} counts
     * them) and the source it is already read as, if any; those that hold sstables (or whose sstables cannot be listed) and
     * are not read yet are the directories to pass. They are not read: the operator names each directory read, so that
     * the report says exactly what was read (a quarantine may hold the Paxos state of several restores, and a table
     * directory holds snapshots and backups that are not HCD 2.0's state). A table directory in a {@code system}
     * directory, of {@code system.paxos} without its sstables or of another table named {@code <table>-<id>}, names the
     * same way the table directories of {@code system.paxos} next to it that hold sstables (or whose sstables cannot be
     * listed), read or not; a table directory of {@code system.paxos} with none such next to it
     * ({@link #isEmptyPaxosTable}) names the directories given that hold sstables of {@code system.paxos} and says to drop
     * this one, else says that it is empty only when its data directory held no Paxos state at the drain and when the
     * problem then needs no other action ({@link #appendEmptyPaxosTable}). A directory named as a table directory of
     * {@code system.paxos} whose parent is not a {@code system} directory is not laid out as one, which the problem says
     * too. Real paths are compared to tell the directories already read and the directory itself among its neighbours;
     * when the directory is reached through a symbolic link, the problem gives its real path, the layout being read from
     * the path given. Built once every directory is scanned, as the directories given that hold sstables of
     * {@code system.paxos} are named.
     *
     * @param label {@code --paxos-dir} and the directory as given
     * @param directory the directory given with {@code --paxos-dir}, normalized and absolute
     * @param others the number of sstables of other tables it holds
     * @param emptyPaxosTables the number of directories given that are empty table directories of {@code system.paxos}
     *                         ({@link #isEmptyPaxosTable})
     */
    private String noPaxosSSTable(String label, File directory, int others, int emptyPaxosTables)
    {
        StringBuilder problem = new StringBuilder();
        problem.append(label).append(" holds no system.paxos sstable (")
               .append(others == 0 ? "no *-Data.db file" : others == 1 ? "1 sstable of another table" : others + " sstables of other tables")
               .append("): nothing was read from it. ").append(PAXOS_DIRECTORY_OPTION)
               .append(" takes the table directory of system.paxos the restore moved HCD 2.0's sstables to, laid out as ")
               .append("<...>/system/paxos-<id> and holding their *-Data.db files, such as <quarantine>/system/paxos-<id>");
        Path real = realPath(directory);
        boolean inSystem = inSystemDirectory(directory);
        boolean paxosTable = isTableDirectory(directory, SystemKeyspace.PAXOS);
        Map<File, Integer> candidates = paxosTablesIn(directory);
        if (!candidates.isEmpty())
        {
            problem.append("; it holds ");
            appendCandidates(problem, candidates);
            problem.append(" (the tool reads the directories given, not their subdirectories)");
        }
        else if (inSystem && (paxosTable || DseTableReader.isTableDirectoryName(nameOf(directory))))
        {
            Map<File, Integer> siblings = paxosTablesNextTo(directory);
            if (!siblings.isEmpty())
            {
                problem.append(paxosTable ? "; it is laid out as one but holds none, and next to it " : "; it is the table directory of another table, and next to it ")
                       .append(siblings.size() == 1 ? "is " : "are ");
                appendCandidates(problem, siblings);
            }
            else if (paxosTable && !givenWithPaxosSSTables.isEmpty())
            {
                problem.append("; it is laid out as one but holds none, while ").append(givenWithPaxosSSTables())
                       .append(" sstables of system.paxos: drop this ").append(PAXOS_DIRECTORY_OPTION)
                       .append(" (pass only the directories that hold sstables of system.paxos)");
            }
            else if (paxosTable)
            {
                appendEmptyPaxosTable(problem, emptyPaxosTables);
            }
        }
        else if (paxosTable)
        {
            problem.append("; it is named as one, but its parent is not a system directory: the tool reads the sstables of system.paxos ")
                   .append("only from a directory laid out as <...>/system/paxos-<id>");
        }
        if (!real.equals(directory.toPath()))
            problem.append("; it is reached through a symbolic link, and its real path is ").append(real).append(": the layout is read from the path given");
        return problem.toString();
    }

    /**
     * The table directories of {@code system.paxos} the problem of a directory given with {@code --paxos-dir} names as in
     * it: those of its {@code system} subdirectory, and its own when it is itself named {@code system}, each with its
     * sstables as {@link #sstableCount} counts them.
     */
    private static Map<File, Integer> paxosTablesIn(File directory)
    {
        Map<File, Integer> candidates = new LinkedHashMap<>();
        if (SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(nameOf(directory)))
            tableDirectories(directory, SystemKeyspace.PAXOS).forEach(d -> candidates.put(d, sstableCount(d)));
        tableDirectories(new File(directory, SchemaConstants.SYSTEM_KEYSPACE_NAME), SystemKeyspace.PAXOS).forEach(d -> candidates.put(d, sstableCount(d)));
        return candidates;
    }

    /**
     * The table directories of {@code system.paxos} next to a directory, itself excluded by real path, that hold sstables
     * or whose sstables cannot be listed, each with its sstables as {@link #sstableCount} counts them.
     */
    private static Map<File, Integer> paxosTablesNextTo(File directory)
    {
        Path real = realPath(directory);
        Map<File, Integer> siblings = new LinkedHashMap<>();
        for (File sibling : tableDirectories(directory.parent(), SystemKeyspace.PAXOS))
        {
            int sstables = realPath(sibling).equals(real) ? 0 : sstableCount(sibling);
            if (sstables != 0)
                siblings.put(sibling, sstables);
        }
        return siblings;
    }

    /**
     * Whether a directory given with {@code --paxos-dir} that holds no sstable of {@code system.paxos} is an empty table
     * directory of {@code system.paxos} laid out as one, such as an empty {@code <quarantine>/system/paxos-<id>}, with no
     * table directory of {@code system.paxos} in it or next to it that holds sstables (or whose sstables cannot be listed).
     */
    private static boolean isEmptyPaxosTable(File directory)
    {
        return paxosTablesIn(directory).isEmpty() && inSystemDirectory(directory) && isTableDirectory(directory, SystemKeyspace.PAXOS) &&
               paxosTablesNextTo(directory).isEmpty();
    }

    /** The directories given that hold sstables of {@code system.paxos}, then "holds" or "hold". */
    private String givenWithPaxosSSTables()
    {
        return String.join(", ", givenWithPaxosSSTables) + (givenWithPaxosSSTables.size() == 1 ? " holds" : " hold");
    }

    /**
     * Appends, when no directory given holds sstables of {@code system.paxos}, when an empty table directory of
     * {@code system.paxos} ({@link #isEmptyPaxosTable}) is right: the restore moves there all the sstables of
     * {@code system.paxos} of its data directory, so it is empty only when that data directory held none at the drain.
     * When the node held none, the operator passes the one of each data directory, and the problem needs no other action
     * when the only other problems are the same problem for each other data directory: with one data directory, when it
     * is the only problem. The number of empty table directories of {@code system.paxos} given is named when it is not the
     * number of data directories, with only the one of each data directory to pass.
     *
     * @param emptyPaxosTables the number of directories given that are empty table directories of {@code system.paxos}
     */
    private void appendEmptyPaxosTable(StringBuilder problem, int emptyPaxosTables)
    {
        int dataDirectories = options.dataDirectories.size();
        problem.append("; it is laid out as one: check that it is the directory the restore moved the sstables of system.paxos to. ");
        if (dataDirectories == 1)
        {
            problem.append("The restore moves all of them there, so it is empty only when the node had none at the drain (no LWT under HCD 2.0, ")
                   .append("and none of DSE's left): then there is no Paxos state to read, and this problem needs no other action when it is ")
                   .append("the only problem in the report");
        }
        else
        {
                problem.append("The restore moves there all those of its data directory, so it is empty only when that data directory had none at ")
                   .append("the drain. When none of the ").append(dataDirectories).append(" data directories had any (no LWT under HCD 2.0, and ")
                   .append("none of DSE's left), there is no Paxos state to read: pass the quarantined system/paxos-<id> of each of them; then ")
                   .append("this problem needs no other action when ")
                   .append(dataDirectories == 2 ? "the only other problem in the report is" : "the only other problems in the report are")
                   .append(" the same problem (an empty quarantined system/paxos-<id>) for ")
                   .append(dataDirectories == 2 ? "the other data directory" : "each of the other " + (dataDirectories - 1) + " data directories");
        }
        if (emptyPaxosTables != dataDirectories)
            problem.append("; ").append(emptyPaxosTables).append(emptyPaxosTables == 1 ? " empty system/paxos-<id> is" : " empty system/paxos-<id> are")
                   .append(" passed for the ").append(dataDirectories).append(dataDirectories == 1 ? " data directory" : " data directories")
                   .append(": pass only that of ").append(dataDirectories == 1 ? "the data directory" : "each data directory");
    }

    /**
     * Appends the table directories of {@code system.paxos} named by {@link #noPaxosSSTable}, each with its sstables and
     * the source it is already read as, then what to do: pass those that hold sstables (or whose sstables cannot be listed,
     * so that the listing problem is reported) and are not read yet, or the only one when it is empty and not read yet
     * and no directory given holds sstables of {@code system.paxos} (its problem then says when an empty table directory
     * is right), else drop the {@code --paxos-dir} when some are already read or when a directory given holds sstables of
     * {@code system.paxos}, else, none holding an sstable, check where the restore moved HCD 2.0's sstables.
     *
     * @param candidates the directories, with their sstables as {@link #sstableCount} counts them
     */
    private void appendCandidates(StringBuilder problem, Map<File, Integer> candidates)
    {
        List<String> described = new ArrayList<>();
        List<File> toPass = new ArrayList<>();
        boolean anyRead = false;
        for (Map.Entry<File, Integer> candidate : candidates.entrySet())
        {
            String readAs = readDirectories.get(realPath(candidate.getKey()));
            anyRead |= readAs != null;
            int sstables = candidate.getValue();
            if (sstables != 0 && readAs == null)
                toPass.add(candidate.getKey());
            described.add(candidate.getKey() + " (" + (sstables < 0 ? "its sstables cannot be listed" : count(sstables, "sstable", "sstables")) +
                          (readAs == null ? "" : (sstables < 0 ? ", already named as " : ", already read as ") + readAs) + ')');
        }
        if (toPass.isEmpty() && !anyRead && candidates.size() == 1 && givenWithPaxosSSTables.isEmpty())
            toPass.addAll(candidates.keySet());
        problem.append(candidates.size() == 1 ? "the directory " : "the directories ").append(String.join(", ", described));
        if (toPass.size() == 1)
            problem.append(": pass ").append(toPass.get(0)).append(" instead");
        else if (toPass.size() > 1)
            problem.append(": pass instead each one of ").append(toPass.stream().map(File::toString).collect(Collectors.joining(", ")))
                   .append(" the restore moved HCD 2.0's sstables to");
        else if (anyRead)
            problem.append(": drop this ").append(PAXOS_DIRECTORY_OPTION);
        else if (!givenWithPaxosSSTables.isEmpty())
            problem.append(", none with an sstable, while ").append(givenWithPaxosSSTables()).append(" sstables of system.paxos: drop this ")
                   .append(PAXOS_DIRECTORY_OPTION);
        else
            problem.append(", none with an sstable: check where the restore moved HCD 2.0's sstables");
    }

    /** The sstables of a directory, as {@link #scan} lists them; -1 when they cannot be listed. */
    private static int sstableCount(File directory)
    {
        try
        {
            return DseTableReader.sstablesIn(directory).size();
        }
        catch (RuntimeException e)
        {
            return -1;
        }
    }

    /**
     * The open round of a row of {@code system.paxos}; null when the row has none. A row whose proposal ballot is set but
     * whose proposal or proposal version is missing is listed too: Paxos would ignore such a proposal, so the read is only
     * one too many. A proposal no later than the commit of its row is listed too when the commit expires before it and
     * within {@link #COMMIT_EXPIRY_MARGIN_SECONDS} of the run (a proposal accepted after its commit, with an older ballot):
     * once the commit has expired, DSE would read the proposal alone.
     *
     * @param cells the live cells of the row
     */
    @Nullable
    private static PaxosRollbackReport.OpenRow openRow(UntypedResultSet.Row row, Row cells, long nowInSeconds, String source, String sstable)
    {
        if (!row.has("proposal_ballot"))
            return null;
        UUID proposal = row.getUUID("proposal_ballot");
        UUID commit = row.has("most_recent_commit_at") ? row.getUUID("most_recent_commit_at") : null;
        List<String> notes = new ArrayList<>();
        if (proposal.version() != 1 || (commit != null && commit.version() != 1))
            notes.add("a ballot is not a time UUID: listed, since the order of the proposal and the commit cannot be told");
        else if (commit != null && proposal.timestamp() <= commit.timestamp())
        {
            Cell<?> commitCell = cell(cells, "most_recent_commit_at");
            Cell<?> proposalCell = cell(cells, "proposal_ballot");
            if (commitCell == null || !commitCell.isExpiring() || commitCell.localDeletionTime() >= nowInSeconds + COMMIT_EXPIRY_MARGIN_SECONDS ||
                (proposalCell != null && proposalCell.isExpiring() && proposalCell.localDeletionTime() <= commitCell.localDeletionTime()))
                return null;
            notes.add("the commit, later than the proposal, expires at " + Instant.ofEpochSecond(commitCell.localDeletionTime()) + ", within " +
                      TimeUnit.SECONDS.toHours(COMMIT_EXPIRY_MARGIN_SECONDS) + " hours of this run and before the proposal: listed, since DSE would read the " +
                      "proposal alone once the commit has expired");
        }

        PaxosRollbackReport.OpenRow open = new PaxosRollbackReport.OpenRow();
        open.source = source;
        open.sstable = sstable;
        open.proposalBallot = proposal.toString();
        open.mostRecentCommitAt = commit == null ? null : commit.toString();
        Integer proposalVersion = row.has("proposal_version") ? row.getInt("proposal_version") : null;
        open.version = proposalVersion;
        open.writtenBy = proposalVersion == null || proposalVersion < MessagingService.VERSION_40 ? "unknown"
                         : proposalVersion >= DseCompat.DSE_60_VERSION ? DSE : HCD;
        ByteBuffer update = row.has("proposal") ? row.getBytes("proposal") : null;
        if (update == null || !update.hasRemaining() || proposalVersion == null)
        {
            notes.add("the proposal or its version is missing: listed conservatively, Paxos ignores such a proposal");
        }
        else if (proposalVersion < MessagingService.VERSION_40)
        {
            notes.add("proposal_version " + proposalVersion + " is below 4.0, neither DSE 6 nor HCD 2.0: whether the proposal is empty is unknown");
        }
        else if (DseCompat.isUnknownStoredDseVersion(proposalVersion))
        {
            notes.add("proposal_version " + proposalVersion + " is not a DSE 6 version: whether the proposal is empty is unknown");
        }
        else
        {
            try
            {
                open.emptyProposal = PartitionUpdate.PartitionUpdateSerializer.isEmpty(update, DeserializationHelper.Flag.LOCAL, null,
                                                                                      DseCompat.storedDecodingVersion(proposalVersion));
            }
            catch (IOException | RuntimeException e)
            {
                notes.add("the proposal cannot be decoded (" + e + "): whether it is empty is unknown");
            }
        }
        open.note = notes.isEmpty() ? null : String.join("; ", notes);
        return open;
    }

    /** The cell of a column of a row; null when the row has none. */
    @Nullable
    private static Cell<?> cell(Row row, String column)
    {
        for (Cell<?> cell : row.cells())
            if (cell.column().name.toString().equals(column))
                return cell;
        return null;
    }

    // ---- the plan ----

    private void plan(PaxosRollbackReport.Partition partition)
    {
        Table table = tables.get(UUID.fromString(partition.cfId));
        if (table == null)
        {
            manual(partition, null, "table " + partition.cfId + " is in no schema read (" + String.join(", ", report.schemas) + "): if it was dropped, nothing is to " +
                                    "read; otherwise find it by its id (system_schema.tables.id) and read the partition of row_key 0x" + partition.rowKey + " by hand");
            return;
        }
        partition.keyspace = table.keyspace;
        partition.table = table.name;
        if (table.problem != null)
        {
            manual(partition, null, "the partition key of " + qualified(table) + " cannot be decoded: " + table.problem + "; read the partition of row_key 0x" +
                                    partition.rowKey + " by hand");
            return;
        }
        String where = where(table, partition);
        if (where == null)
            return;
        partition.where = where;
        partition.select = "SELECT " + table.partitionKey.stream().map(c -> ColumnIdentifier.maybeQuote(c.name)).collect(Collectors.joining(", ")) +
                           " FROM " + qualified(table) + " WHERE " + where + " LIMIT 1;";

        Replication replication = replication(table.keyspace);
        if (!replication.nodeLocal && (Integer.valueOf(0).equals(replication.total) || Integer.valueOf(0).equals(replication.local)))
        {
            // whatever the serial consistency: this node holds Paxos state of the keyspace, so it is one of its replicas
            manual(partition, null, replication.description + ": " +
                                    (Integer.valueOf(0).equals(replication.total) ? "the replication places no replica of the keyspace"
                                                                                  : "the replication places no replica in data center " + report.dataCenter + " (" +
                                                                                    report.dataCenterSource + ')') +
                                    ", yet this node holds Paxos state of it: the replication (the schema, or the nodes files for EverywhereStrategy) " +
                                    "or this node's data center is wrong; no read is listed, check them");
            return;
        }
        String given = options.serialConsistencies.get(table.keyspace);
        String override = given != null ? given : options.serialConsistencies.get(ALL_KEYSPACES);
        if (override != null)
        {
            partition.reads.add(read(override, replication, "--serial-consistency " + (given != null ? table.keyspace : ALL_KEYSPACES) + '=' + override));
        }
        else if (replication.localOnly)
        {
            partition.reads.add(read(SERIAL, replication, "every replica is in this node's data center, so SERIAL and LOCAL_SERIAL are the same quorum"));
        }
        else
        {
            partition.reads.add(new PaxosRollbackReport.Read(null, Action.CHOOSE,
                                                             replication.description + ": SERIAL and LOCAL_SERIAL may be different quorums" +
                                                             (replication.unknown == null ? "" : " (" + replication.unknown + ')') +
                                                             ", and a serial read at the consistency the table is not written at can complete an old proposal over a " +
                                                             "newer committed value: no read is listed; pass --serial-consistency " + table.keyspace +
                                                             "=SERIAL or " + table.keyspace + "=LOCAL_SERIAL, the serial consistency of the application"));
        }
    }

    private static void manual(PaxosRollbackReport.Partition partition, @Nullable String consistency, String reason)
    {
        partition.reads.add(new PaxosRollbackReport.Read(consistency, Action.MANUAL, reason));
    }

    /** The read of a partition at one serial consistency, by the replication rules of the class javadoc. */
    private PaxosRollbackReport.Read read(String consistency, Replication replication, String why)
    {
        String prefix = replication.description + "; " + why + ": ";
        if (replication.nodeLocal)
            return new PaxosRollbackReport.Read(consistency, Action.SKIPPED, prefix + "the keyspace is local to each node, no other node holds the partition");
        boolean serial = SERIAL.equals(consistency);
        Integer replicas = serial ? replication.total : replication.local;
        String what = serial ? "total replication factor" : "replication factor in data center " + report.dataCenter;
        if (replicas == null)
            return new PaxosRollbackReport.Read(consistency, Action.CHECK,
                                                prefix + replication.unknown + ": the read is listed, check by hand that it is needed and enough");
        if (replicas == 0)
            throw new IllegalStateException("zero replicas are handled by plan()");
        if (replicas == 1)
            return new PaxosRollbackReport.Read(consistency, Action.CANNOT_PROTECT,
                                                prefix + what + " 1: this node is the whole quorum, so the Paxos state of the upgrade is lost at the rollback " +
                                                "and an open proposal of the snapshot can replay over newer data");
        if (replicas == 2)
            return new PaxosRollbackReport.Read(consistency, Action.SKIPPED, prefix + what + " 2: every quorum includes all the other replicas");
        return new PaxosRollbackReport.Read(consistency, Action.READ, prefix + what + ' ' + replicas);
    }

    private static String simpleName(String className)
    {
        return className.startsWith(LOCATOR_PACKAGE) ? className.substring(LOCATOR_PACKAGE.length()) : className;
    }

    /** The replication of a keyspace, from its schema row, this node's data center and the nodes files. */
    private Replication replication(String keyspace)
    {
        Map<String, String> options = keyspaces.getOrDefault(keyspace, Map.of());
        String className = options.getOrDefault("class", "unknown");
        String dataCenter = report.dataCenter;
        boolean singleDataCenter = nodesByDataCenter != null && nodesByDataCenter.size() == 1;
        String dataCenterUnknown = "this node's data center is unknown, pass --dc";
        try
        {
            switch (simpleName(className))
            {
                case SIMPLE_STRATEGY:
                {
                    int replicas = ReplicationFactor.fromString(options.get("replication_factor")).allReplicas;
                    String description = "keyspace " + keyspace + ": SimpleStrategy, replication factor " + replicas;
                    if (singleDataCenter)
                        return new Replication(description, replicas, replicas, null, true, false);
                    return new Replication(description, replicas, null,
                                           dataCenter == null ? dataCenterUnknown
                                                              : "the nodes files do not say that the cluster has a single data center, and SimpleStrategy " +
                                                                "places replicas regardless of data centers, so the replicas in data center " + dataCenter + " are unknown",
                                           false, false);
                }
                case NETWORK_TOPOLOGY_STRATEGY:
                {
                    Map<String, Integer> byDataCenter = new TreeMap<>();
                    // the schema stores one entry per data center; replication_factor is only a shorthand of CREATE KEYSPACE
                    for (Map.Entry<String, String> entry : options.entrySet())
                        if (!"class".equals(entry.getKey()) && !"replication_factor".equals(entry.getKey()))
                            byDataCenter.put(entry.getKey(), ReplicationFactor.fromString(entry.getValue()).allReplicas);
                    int total = byDataCenter.values().stream().mapToInt(Integer::intValue).sum();
                    String description = "keyspace " + keyspace + ": NetworkTopologyStrategy " + byDataCenter;
                    if (dataCenter == null)
                        return new Replication(description, total, null, dataCenterUnknown, false, false);
                    int local = byDataCenter.getOrDefault(dataCenter, 0);
                    return new Replication(description, total, local, null, local > 0 && local == total, false);
                }
                case EVERYWHERE_STRATEGY:
                {
                    // DSE's strategy of a replica on every node: as many replicas as the nodes files list nodes
                    String description = "keyspace " + keyspace + ": EverywhereStrategy";
                    if (nodesByDataCenter == null || dataCenter == null)
                        return new Replication(description, null, null, "EverywhereStrategy places a replica on every node, and the nodes files do not tell " +
                                                                        "how many nodes each data center has", false, false);
                    int total = nodesByDataCenter.values().stream().mapToInt(Integer::intValue).sum();
                    int local = nodesByDataCenter.getOrDefault(dataCenter, 0);
                    return new Replication(description + " (" + total + " nodes in the nodes files, " + local + " in data center " + dataCenter + ')',
                                           total, local, null, local == total, false);
                }
                case LOCAL_STRATEGY:
                    return new Replication("keyspace " + keyspace + ": LocalStrategy", null, null, null, true, true);
                default:
                    return new Replication("keyspace " + keyspace + ": replication " + options, null, null,
                                           "the replication class " + className + " is not one the tool knows", false, false);
            }
        }
        catch (RuntimeException e)
        {
            return new Replication("keyspace " + keyspace + ": replication " + options, null, null,
                                   "the replication cannot be read (" + e.getMessage() + ')', false, false);
        }
    }

    /** The partition key of a partition as a CQL condition; null, with the partition marked for manual handling, when it cannot be rendered. */
    @Nullable
    private static String where(Table table, PaxosRollbackReport.Partition partition)
    {
        ByteBuffer key = ByteBufferUtil.hexToBytes(partition.rowKey);
        List<KeyColumn> columns = table.partitionKey;
        ByteBuffer[] components;
        if (columns.size() == 1)
        {
            components = new ByteBuffer[]{ key };
        }
        else
        {
            List<AbstractType<?>> types = columns.stream().<AbstractType<?>>map(c -> c.type).collect(Collectors.toList());
            try
            {
                components = CompositeType.getInstance(types).split(key);
                if (components.length != columns.size() || !CompositeType.build(ByteBufferAccessor.instance, components).equals(key))
                    components = null;
            }
            catch (RuntimeException e)
            {
                components = null;
            }
            if (components == null)
            {
                manual(partition, null, "row_key 0x" + partition.rowKey + " is not a composite of the " + columns.size() + " partition key columns of " +
                                        qualified(table) + "; read the partition by hand");
                return null;
            }
        }
        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++)
        {
            KeyColumn column = columns.get(i);
            String literal = literal(table.keyspace, column.name, column.type, components[i], partition);
            if (literal == null)
            {
                manual(partition, null, "the value 0x" + ByteBufferUtil.bytesToHex(components[i]) + " of partition key column " + column.name + " (" +
                                        column.type.asCQL3Type() + ") of " + qualified(table) + " cannot be written as a CQL literal; read the partition of row_key 0x" +
                                        partition.rowKey + " by hand");
                return null;
            }
            conditions.add(ColumnIdentifier.maybeQuote(column.name) + " = " + literal);
        }
        return String.join(" AND ", conditions);
    }

    /**
     * A partition key value as a CQL literal, checked by parsing it back to the same bytes; for a valid value of a native
     * type whose literal does not parse back to the same bytes, {@code blobAs<type>(0x...)}, with a note on the partition;
     * null when neither works (an invalid value is refused by the {@code blobAs} functions too).
     */
    @Nullable
    private static String literal(String keyspace, String name, AbstractType<?> type, ByteBuffer value, PaxosRollbackReport.Partition partition)
    {
        CQL3Type cqlType = type.asCQL3Type();
        try
        {
            type.validate(value);
        }
        catch (RuntimeException e)
        {
            return null;
        }
        try
        {
            String literal = cqlType.toCQLLiteral(value);
            if (Terms.asBytes(keyspace, literal, type).equals(value))
                return literal;
        }
        catch (RuntimeException e)
        {
            // rendered as a blob below, when the type is native
        }
        if (cqlType instanceof CQL3Type.Native)
        {
            String typeName = cqlType.toString();
            partition.notes.add("the value of " + name + " (" + typeName + ") is written as a blob: it does not render as a " + typeName + " literal");
            return "blobAs" + Character.toUpperCase(typeName.charAt(0)) + typeName.substring(1) + "(0x" + ByteBufferUtil.bytesToHex(value) + ')';
        }
        return null;
    }

    private static String qualified(Table table)
    {
        return ColumnIdentifier.maybeQuote(table.keyspace) + '.' + ColumnIdentifier.maybeQuote(table.name);
    }

    // ---- output ----

    /** The file of the LOCAL_SERIAL reads written next to the file of the SERIAL reads {@code cql}. */
    static Path localSerialFile(Path cql)
    {
        String name = cql.getFileName().toString();
        String local = (name.endsWith(".cql") ? name.substring(0, name.length() - 4) : name) + "-local-serial.cql";
        return cql.resolveSibling(local);
    }

    /** The partitions read at {@code consistency}: those whose read there is listed. */
    private static List<PaxosRollbackReport.Partition> readAt(PaxosRollbackReport report, String consistency)
    {
        List<PaxosRollbackReport.Partition> read = new ArrayList<>();
        for (PaxosRollbackReport.Partition partition : report.partitions)
            for (PaxosRollbackReport.Read r : partition.reads)
                if (consistency.equals(r.consistency) && (r.action == Action.READ || r.action == Action.CHECK))
                    read.add(partition);
        return read;
    }

    /**
     * The cqlsh input with the reads of the report at {@code consistency}: the SERIAL reads go through any other node, the
     * LOCAL_SERIAL ones through a node of this node's data center, hence a file each.
     */
    static String cql(PaxosRollbackReport report, String consistency)
    {
        boolean serial = SERIAL.equals(consistency);
        String dataCenter = report.dataCenter == null ? "<this node's data center>" : report.dataCenter;
        StringBuilder out = new StringBuilder();
        if (PaxosRollbackReport.NEEDS_ATTENTION.equals(report.result))
            out.append(attention(report, consistency));
        out.append("-- ").append(consistency).append(" reads completing the open Paxos rounds of ").append(report.hostname)
           .append(" before its rollback to DSE.\n");
        out.append("-- Written by ").append(NodeReport.TOOL).append(' ').append(report.softwareVersion).append(" at ").append(report.generatedAt).append(".\n");
        out.append("-- ").append(validity(report)).append('\n');
        if (serial)
            out.append("-- Run it through another node (a DSE node while one exists), as a role with SELECT on these tables, with every other replica\n")
               .append("-- of these partitions up: cqlsh <node> -f <this file>.");
        else
            out.append("-- Run it through a node of data center ").append(dataCenter).append(" (this node's) other than this node, never through a node of another\n")
               .append("-- data center, as a role with SELECT on these tables, with every replica of that data center up: cqlsh <node of ")
               .append(dataCenter).append("> -f <this file>.");
        out.append(" Run it again until no statement fails\n")
           .append("-- (Unavailable, a timeout, CasWriteUnknownResult): the reads can be repeated. Do not start DSE before every read has succeeded.\n");
        List<PaxosRollbackReport.Partition> partitions = readAt(report, consistency);
        if (partitions.isEmpty())
        {
            out.append("-- No read is needed at ").append(consistency).append(".\n");
            return out.toString();
        }
        out.append("CONSISTENCY ").append(consistency).append(";\n");
        for (PaxosRollbackReport.Partition partition : partitions)
        {
            if (partition.reads.stream().anyMatch(r -> consistency.equals(r.consistency) && r.action == Action.CHECK))
                out.append("-- CHECK by hand that this read is needed and enough (see the report):\n");
            out.append(partition.select).append('\n');
        }
        return out.toString();
    }

    /**
     * The first line of the cqlsh file of the reads at {@code consistency} of a report that needs attention: what that file
     * leaves out (the partitions to decide at that consistency or at an unknown one, its reads to check), and the problems;
     * a note when it leaves out nothing.
     */
    private static String attention(PaxosRollbackReport report, String consistency)
    {
        long undecided = report.partitions.stream().filter(p -> p.reads.stream().anyMatch(r -> (r.consistency == null || consistency.equals(r.consistency)) &&
                                                                                            (r.action == Action.CHOOSE || r.action == Action.MANUAL ||
                                                                                             r.action == Action.CANNOT_PROTECT))).count();
        long toCheck = report.partitions.stream().filter(p -> p.reads.stream().anyMatch(r -> consistency.equals(r.consistency) && r.action == Action.CHECK)).count();
        List<String> parts = new ArrayList<>();
        if (undecided > 0)
            parts.add(count(undecided, "partition needs", "partitions need") + " a decision (--serial-consistency, or by hand)");
        if (toCheck > 0)
            parts.add(count(toCheck, "partition has a read", "partitions have a read") + " to check by hand");
        if (!report.problems.isEmpty())
            parts.add("the report lists " + count(report.problems.size(), "problem", "problems"));
        if (parts.isEmpty())
            return "-- NOTE: this file leaves nothing out; the report needs attention for the other serial consistency.\n";
        return "-- INCOMPLETE: " + String.join("; ", parts) + ". See the report before running this file.\n";
    }

    /** When the reads stop covering the Paxos state read: a commit that expires within the margin is not counted on. */
    private static String validity(PaxosRollbackReport report)
    {
        return "Run the tool again if DSE does not start before " + report.validUntil + " (within " + TimeUnit.SECONDS.toHours(COMMIT_EXPIRY_MARGIN_SECONDS) +
               " hours of the run): the Paxos state is read as of the run.";
    }

    private static String count(long n, String singular, String plural)
    {
        return n + " " + (n == 1 ? singular : plural);
    }

    private static String actionName(PaxosRollbackReport.Read read)
    {
        return (read.action == Action.CANNOT_PROTECT ? "CANNOT BE PROTECTED" : read.action.name()) + (read.consistency == null ? "" : " at " + read.consistency);
    }

    private static String describe(PaxosRollbackReport.Read read)
    {
        return actionName(read) + ": " + read.reason;
    }

    private static boolean sameReads(List<PaxosRollbackReport.Read> reads, List<PaxosRollbackReport.Read> others)
    {
        if (reads.size() != others.size())
            return false;
        for (int i = 0; i < reads.size(); i++)
            if (!describe(reads.get(i)).equals(describe(others.get(i))))
                return false;
        return true;
    }

    /** What a directory of the report holds: its sstables, those of other tables, and the rows read; or why it cannot be listed. */
    private static String describe(PaxosRollbackReport.Directory directory)
    {
        if (directory.error != null)
            return directory.error;
        String others = directory.otherTableSSTables == 0 ? ""
                        : directory.otherTableSSTables == 1 ? ", 1 of another table" : ", " + directory.otherTableSSTables + " of other tables";
        return count(directory.sstables, "sstable", "sstables") + others + ", " + count(directory.rows, "row", "rows");
    }

    static void print(PaxosRollbackReport report, String serialCql, String localSerialCql, PrintStream out)
    {
        out.println("DSE 6 rollback: open Paxos rounds of this node (" + NodeReport.TOOL + ' ' + report.softwareVersion + ')');
        out.println("Precondition, not verified by the tool: HCD 2.0 stopped after a successful drain (nodetool drain exited 0, DRAINED in system.log), DSE not started.");
        out.println(validity(report));
        out.println("Host: " + report.hostname + "; data center: " + (report.dataCenter == null ? "unknown" : report.dataCenter + " (" + report.dataCenterSource + ')') +
                    (report.singleDataCenter == null ? "; the nodes files do not tell whether the cluster has one data center"
                                                     : report.singleDataCenter ? "; one data center in the nodes files" : "; several data centers in the nodes files"));
        out.println("Data directories: " + String.join(", ", report.dataDirectories) +
                    (report.defaultsAssumed.isEmpty() ? "" : " (DSE's defaults assumed: " + String.join(", ", report.defaultsAssumed) + ')'));
        out.println("Snapshot: " + report.snapshot + "; schema read from: " + String.join(", ", report.schemas));
        if (report.notRestored)
            out.println("--not-restored: the operator states that system has not been restored yet.");
        out.println("system.paxos directories read:");
        for (PaxosRollbackReport.Directory directory : report.directories)
            out.println("  " + directory.source + ": " + directory.path + " (" + describe(directory) + ')');
        if (report.directories.isEmpty())
            out.println("  none");
        out.println("system.paxos sstables read, each on its own (" + report.rowsRead + " rows):");
        for (PaxosRollbackReport.SSTable sstable : report.sstables)
            out.println("  " + sstable.source + ": " + sstable.path + " (" + sstable.format + (sstable.error != null ? ", unreadable: " + sstable.error
                                                                                                                     : ", " + sstable.rows + " rows, " + sstable.openRows + " open") + ')');
        if (report.sstables.isEmpty())
            out.println("  none");
        out.println();

        Map<Action, Integer> counts = new TreeMap<>();
        for (PaxosRollbackReport.Partition partition : report.partitions)
            for (PaxosRollbackReport.Read read : partition.reads)
                counts.merge(read.action, 1, Integer::sum);
        out.println("Result: " + report.result + " (" + report.partitions.size() + " partitions with an open round; reads listed: " + readAt(report, SERIAL).size() +
                    " at SERIAL, " + readAt(report, LOCAL_SERIAL).size() + " at LOCAL_SERIAL; " + counts.getOrDefault(Action.SKIPPED, 0) + " need no read, " +
                    counts.getOrDefault(Action.CANNOT_PROTECT, 0) + " cannot be protected, " + counts.getOrDefault(Action.CHOOSE, 0) + " need their serial consistency, " +
                    counts.getOrDefault(Action.CHECK, 0) + " to check, " + counts.getOrDefault(Action.MANUAL, 0) + " to handle by hand)");
        for (String problem : report.problems)
            out.println("  PROBLEM: " + problem);

        if (!report.partitions.isEmpty())
        {
            out.println();
            out.println("Partitions with an open round, by table (what to do, then the open rows):");
            String table = null;
            List<PaxosRollbackReport.Read> tableReads = List.of();
            for (PaxosRollbackReport.Partition partition : report.partitions)
            {
                String name = partition.keyspace == null ? "table " + partition.cfId + " (in no schema read)" : partition.keyspace + '.' + partition.table;
                if (!name.equals(table))
                {
                    // the reads of a table are the same for all its partitions, but for those to handle by hand: printed once
                    table = name;
                    out.println("  " + name);
                    tableReads = partition.reads.stream().anyMatch(r -> r.action == Action.MANUAL) ? List.of() : partition.reads;
                    for (PaxosRollbackReport.Read read : tableReads)
                        out.println("    " + describe(read));
                }
                boolean asTable = sameReads(partition.reads, tableReads);
                out.println("    " + (partition.where != null ? partition.where : "row_key 0x" + partition.rowKey) + ": " +
                            partition.reads.stream().map(r -> asTable ? actionName(r) : describe(r)).collect(Collectors.joining("; ")));
                for (String note : partition.notes)
                    out.println("      note: " + note);
                for (PaxosRollbackReport.OpenRow row : partition.openRows)
                    out.println("      open in " + row.source + ", " + row.sstable + ", written by " + row.writtenBy + (row.version == null ? "" : " (version " + row.version + ')') +
                                ": " + (Boolean.TRUE.equals(row.emptyProposal) ? "empty proposal " : "proposal ") + row.proposalBallot +
                                (row.mostRecentCommitAt == null ? ", no commit" : " after commit " + row.mostRecentCommitAt) + (row.note == null ? "" : "; " + row.note));
            }
        }
        if (counts.containsKey(Action.CANNOT_PROTECT))
        {
            out.println();
            out.println("WARNING: some open rounds cannot be protected (replication factor 1 at their serial consistency): the Paxos state of the upgrade of those");
            out.println("partitions is lost at the rollback, and an open proposal of the snapshot can replay over newer data. Decide before DSE starts.");
        }
        out.println();
        if (report.cqlFile != null)
        {
            out.println("cqlsh input written to " + report.cqlFile + " (SERIAL, through any other node) and " + report.cqlLocalSerialFile +
                        " (LOCAL_SERIAL, through another node of data center " + (report.dataCenter == null ? "<this node's>" : report.dataCenter) +
                        "): run them as their headers say, before DSE starts.");
        }
        else
        {
            out.println("cqlsh input of the SERIAL reads (--cql <file> writes it to a file):");
            out.print(serialCql);
            out.println();
            out.println("cqlsh input of the LOCAL_SERIAL reads (written to a second file with --cql):");
            out.print(localSerialCql);
        }
    }
}
