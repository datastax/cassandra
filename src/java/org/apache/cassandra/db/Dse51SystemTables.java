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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * The five columns DataStax Enterprise 5.1 adds to {@code system.local} and {@code system.peers}. DSE 5.1.51 adds them
 * to the in-memory definitions of both tables at every start, before it reads them, and its endpoint state persister
 * writes them for the local node and for every live peer: {@code workload} (text, the legacy single workload),
 * {@code workloads} (a frozen set of text), {@code graph} (boolean), {@code dse_version} (text) and
 * {@code server_id} (text). A flushed sstable of these tables names a column only if a row of its memtable wrote it, a
 * compacted one names all five: DSE 5.1 builds the header of a compaction from the table definition.
 * <p>
 * This software has no such column. The two tables record the five as dropped system columns
 * ({@link TableMetadata.Builder#recordDeprecatedSystemColumns}, as {@code thrift_version}), in every mode, so that
 * an sstable DSE 5.1 wrote is read with their cells skipped (the rows outlive the mode, until a compaction rewrites
 * them without those values). A mutation of the commit log naming them is read only in
 * {@code internode_compatibility_mode: DSE_5_1}, where the start replays the segments DSE 5.1 left
 * ({@link #readsStoredDse51Columns}); without the mode its replay fails on the unknown column, as before.
 * <p>
 * At the first start in the mode, before this software writes to {@code system.local}, the values of the columns are
 * read from the sstables of both tables and recorded in the metadata directory, from where the later starts in the
 * mode load them ({@link #loadOrReadAtStart}); they are skipped everywhere else. The values that only the commit log
 * holds (writes of the persister after the last flush, when DSE 5.1 was stopped without a drain) are not read: the
 * replay skips them.
 */
public final class Dse51SystemTables
{
    private static final Logger logger = LoggerFactory.getLogger(Dse51SystemTables.class);

    public static final String WORKLOAD = "workload";
    public static final String WORKLOADS = "workloads";
    public static final String GRAPH = "graph";
    public static final String DSE_VERSION = "dse_version";
    public static final String SERVER_ID = "server_id";

    /** The type of {@code workloads}. */
    private static final SetType<String> WORKLOADS_TYPE = SetType.getInstance(UTF8Type.instance, false);

    /**
     * The five columns and their types, in the order DSE 5.1.51 adds them; {@code workloads} is frozen, as the headers
     * of the sstables DSE 5.1.51 writes name it ({@code FrozenType(SetType(UTF8Type))}).
     */
    public static final Map<String, AbstractType<?>> COLUMNS = columns();

    private static final Set<ByteBuffer> COLUMN_NAMES = COLUMNS.keySet()
                                                               .stream()
                                                               .map(UTF8Type.instance::decompose)
                                                               .collect(Collectors.toUnmodifiableSet());

    /**
     * The name of the record of the values read at the first start in the mode, in the metadata directory; public for
     * the preflight tool, which reports it.
     */
    public static final String RECORD_FILE_NAME = "dse51-system-columns";

    @VisibleForTesting
    static final String RECORD_HEADER = "# values of DSE 5.1's columns of system.local and system.peers, " +
                                        "read at the first start in DSE_5_1 mode; one row per line: node, " +
                                        "dse_version, workload, graph, workloads; - when absent";

    private static final String ABSENT = "-";

    /**
     * The suffix, followed by a dash and the time in milliseconds, a record that cannot be read is renamed with; public
     * for the preflight tool, which reports such copies.
     */
    public static final String UNREADABLE_SUFFIX = ".unreadable";

    /** The suffix of the temporary file the record is written to before it is moved in place. */
    public static final String TEMPORARY_SUFFIX = ".tmp";

    /** What {@link #loadOrReadAtStart} found; empty until it runs. */
    private static volatile List<StoredColumns> stored = Collections.emptyList();

    /** The four of the five columns {@link StoredColumns} keeps ({@code server_id} is not kept). */
    private static final List<String> KEPT_COLUMNS = List.of(WORKLOAD, WORKLOADS, GRAPH, DSE_VERSION);

    /**
     * Set when the record of an earlier upgrade could not be deleted ({@link #deleteRecordOfAnEarlierUpgrade}): the
     * start is then refused ({@link #loadOrReadAtStart}).
     */
    private static volatile boolean earlierRecordNotDeleted;

    private Dse51SystemTables()
    {
    }

    private static Map<String, AbstractType<?>> columns()
    {
        Map<String, AbstractType<?>> columns = new LinkedHashMap<>();
        columns.put(WORKLOAD, UTF8Type.instance);
        columns.put(WORKLOADS, WORKLOADS_TYPE);
        columns.put(GRAPH, BooleanType.instance);
        columns.put(DSE_VERSION, UTF8Type.instance);
        columns.put(SERVER_ID, UTF8Type.instance);
        return Collections.unmodifiableMap(columns);
    }

    /** Whether {@code metadata} is {@code system.local} or {@code system.peers}, the tables DSE 5.1 adds columns to. */
    public static boolean hasDse51Columns(TableMetadata metadata)
    {
        return SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(metadata.keyspace)
               && (SystemKeyspace.LOCAL.equals(metadata.name) || SystemKeyspace.LEGACY_PEERS.equals(metadata.name));
    }

    /** Whether {@code name} is one of the five columns DSE 5.1 adds to {@code metadata}. */
    public static boolean isDse51Column(TableMetadata metadata, ByteBuffer name)
    {
        return hasDse51Columns(metadata) && COLUMN_NAMES.contains(name);
    }

    /**
     * Whether the columns of an update of {@code metadata} decoded at messaging {@code version} may name the five
     * columns, read as the dropped columns they are here: only in {@code DSE_5_1}, at
     * {@link MessagingService#VERSION_30}, the version of DSE 5.1's commit log segments (version 6). Those two tables
     * are local, so no peer sends them and no hint or batch holds them: such an update is one DSE 5.1 wrote to the
     * commit log of this node, which the start in the mode replays. Evaluated for every update decoded, cheapest test
     * first.
     */
    public static boolean readsStoredDse51Columns(TableMetadata metadata, int version)
    {
        return version == MessagingService.VERSION_30
               && DatabaseDescriptor.isDse51CompatibilityMode()
               && hasDse51Columns(metadata);
    }

    /** The five columns {@code update} names, read as dropped columns; empty for any other table. */
    public static List<String> dse51ColumnsOf(PartitionUpdate update)
    {
        if (!hasDse51Columns(update.metadata()))
            return Collections.emptyList();
        List<String> names = new ArrayList<>();
        for (ColumnMetadata column : update.columns())
        {
            if (column.isDropped() && COLUMN_NAMES.contains(column.name.bytes))
                names.add(column.name.toString());
        }
        return names;
    }

    /** The row of {@code system.local} first, then the peers by the bytes of their addresses. */
    private static final Comparator<StoredColumns> BY_NODE =
        Comparator.comparing((StoredColumns columns) -> columns.peer == null ? new byte[0] : columns.peer.getAddress(),
                             UnsignedBytes.lexicographicalComparator());

    /**
     * The values DSE 5.1 wrote for one node in four of the five columns, those later checks read: {@code workloads},
     * {@code workload}, {@code graph} and {@code dse_version} ({@code server_id} is not kept).
     */
    public static final class StoredColumns
    {
        /** The address of the peer; null for the row of {@code system.local}, the node's own. */
        @Nullable
        public final InetAddress peer;
        /** {@code workloads}, sorted; empty when absent. */
        public final List<String> workloads;
        /** {@code workload}, null when absent. */
        @Nullable
        public final String workload;
        /** {@code graph}, null when absent. */
        @Nullable
        public final Boolean graph;
        /** {@code dse_version}, null when absent. */
        @Nullable
        public final String dseVersion;

        StoredColumns(@Nullable InetAddress peer, List<String> workloads, @Nullable String workload,
                      @Nullable Boolean graph, @Nullable String dseVersion)
        {
            this.peer = peer;
            this.workloads = Collections.unmodifiableList(new ArrayList<>(workloads));
            this.workload = workload;
            this.graph = graph;
            this.dseVersion = dseVersion;
        }

        /** The workload fields, as the checks of the DSE workloads read them. */
        public DseNodeWorkloads nodeWorkloads()
        {
            return new DseNodeWorkloads(peer, workloads, graph, workload);
        }

        /** The address of the peer, or {@code local} for the row of {@code system.local}. */
        String node()
        {
            return peer == null ? "local" : peer.getHostAddress();
        }

        @Override
        public String toString()
        {
            return node() + " (workloads " + (workloads.isEmpty() ? "absent" : workloads) +
                   ", workload " + absentIfNull(workload) + ", graph " + absentIfNull(graph) +
                   ", dse_version " + absentIfNull(dseVersion) + ')';
        }

        private static Object absentIfNull(@Nullable Object value)
        {
            return value == null ? "absent" : value;
        }
    }

    /**
     * The values of the columns {@link StoredColumns} keeps, as DSE 5.1 last wrote them to {@code system.local} and
     * {@code system.peers} before this node left it: one entry per row that holds a value of {@code workloads},
     * {@code workload}, {@code graph} or {@code dse_version}. Set by {@link #loadOrReadAtStart} in {@code DSE_5_1}
     * (read from the sstables at the first start, then from the record of that read, {@link #recordPath}); empty in
     * the other modes.
     */
    public static List<StoredColumns> stored()
    {
        return stored;
    }

    /** The record of the values read at the first start in {@code DSE_5_1}, in the metadata directory. */
    public static Path recordPath()
    {
        return DatabaseDescriptor.getMetadataDirectory().toPath().resolve(RECORD_FILE_NAME);
    }

    /**
     * In {@code DSE_5_1}, sets {@link #stored}: from the record of an earlier start in the mode ({@link #recordPath})
     * when it exists, otherwise from the live sstables of {@code system.local} and {@code system.peers}, and then
     * writes the record (also when nothing was found), so that a later start, after a compaction of this software
     * has rewritten some of those sstables without the values, gets them as they were. INFO lines: one when the record
     * is written ("recorded the values ... in"), one saying where the values come from and how many rows hold some,
     * and one per row when they are read from the sstables (DEBUG when loaded from the record). To be called after
     * the startup checks and before the first write of this software to {@code system.local}, whose flush can start a
     * compaction of that table. In the other modes it only empties {@link #stored}.
     * <p>
     * The record belongs to one upgrade: the startup checks delete it before they write the DSE 5.1 upgrade marker
     * ({@link #deleteRecordOfAnEarlierUpgrade}), and it is deleted by hand with that marker. A record that cannot be
     * read is renamed aside ({@link #UNREADABLE_SUFFIX} and the time) with a WARN, and the values are read again from
     * the sstables, which a compaction may have rewritten, and recorded again; a record that cannot be renamed aside
     * or written is reported with an ERROR. When an sstable could not be read ({@link #read}), the values are not
     * recorded, with a WARN, so that the next start, the cause removed, reads them again; a partition whose values
     * cannot be decoded, a lasting fault, is only left out of the record. When the startup checks could not delete the
     * record of an earlier upgrade (they then do not write the marker), the start is refused, so that the next one
     * tries again.
     *
     * @throws StartupException when the record of an earlier upgrade could not be deleted
     */
    public static void loadOrReadAtStart() throws StartupException
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
        {
            stored = Collections.emptyList();
            return;
        }
        Path record = recordPath();
        if (earlierRecordNotDeleted)
        {
            earlierRecordNotDeleted = false;
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       "cannot start in DSE_5_1 compatibility mode: " + record + ", the record of " +
                                       "the values of DSE 5.1's columns of system.local and system.peers of an " +
                                       "earlier upgrade, cannot be deleted (the error is logged above); delete it by " +
                                       "hand and start the node again");
        }
        List<StoredColumns> found = null;
        boolean recordable = true;
        if (Files.exists(record))
        {
            try
            {
                found = load(record);
            }
            catch (IOException | RuntimeException e)
            {
                recordable = renameAside(record, e);
            }
        }
        boolean loaded = found != null;
        if (!loaded)
        {
            Keyspace system = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            found = new ArrayList<>();
            List<String> skipped = new ArrayList<>();
            found.addAll(read(system.getColumnFamilyStore(SystemKeyspace.LOCAL), skipped));
            found.addAll(read(system.getColumnFamilyStore(SystemKeyspace.LEGACY_PEERS), skipped));
            if (!skipped.isEmpty())
                logger.warn("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.local and " +
                            "system.peers are not recorded, since some could not be read ({}): the next start reads " +
                            "them again. Remove the cause reported above and restart the node now, before any " +
                            "compaction of system.local and system.peers", String.join(", ", skipped));
            else if (recordable)
                record(record, found);
        }
        stored = Collections.unmodifiableList(found);
        logger.info("DSE 5.1 compatibility mode: values of DSE 5.1's columns {} of system.local and system.peers {}; " +
                    "rows holding a value: {}", KEPT_COLUMNS,
                    loaded ? "loaded from " + record : "read from their sstables", found.size());
        for (StoredColumns columns : found)
        {
            if (loaded)
                logger.debug("DSE 5.1 compatibility mode: DSE 5.1's columns of system.local and system.peers for {}",
                             columns);
            else
                logger.info("DSE 5.1 compatibility mode: DSE 5.1's columns of system.local and system.peers for {}",
                            columns);
        }
    }

    /**
     * Renames the unreadable {@code record} aside, then logs a WARN saying why it cannot be read; returns whether it is
     * gone, so that a new record can be written. When the rename fails, an ERROR says the values cannot be recorded
     * until the file is removed by hand.
     */
    private static boolean renameAside(Path record, Throwable unreadable)
    {
        Path aside = record.resolveSibling(record.getFileName() + UNREADABLE_SUFFIX + '-' + currentTimeMillis());
        try
        {
            Files.move(record, aside);
        }
        catch (IOException | RuntimeException e)
        {
            e.addSuppressed(unreadable);
            logger.error("DSE 5.1 compatibility mode: the record {} of the values of DSE 5.1's columns of " +
                         "system.local and system.peers cannot be read, nor renamed to {}; the values cannot be " +
                         "recorded until it is removed by hand, and every start reads them again from the sstables " +
                         "of those tables, which a compaction of this software rewrites without them. Remove it and " +
                         "restart the node now, before any compaction of system.local and system.peers", record,
                         aside, e);
            return false;
        }
        logger.warn("DSE 5.1 compatibility mode: the record {} of the values of DSE 5.1's columns of system.local " +
                    "and system.peers cannot be read, and is renamed to {}; the values are read again from the " +
                    "sstables of those tables, which a compaction may have rewritten without some of them", record,
                    aside, unreadable);
        return true;
    }

    /** Writes the record, reporting a failure with an ERROR that says what to do. */
    private static void record(Path record, List<StoredColumns> found)
    {
        try
        {
            write(record, found);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.local and " +
                         "system.peers cannot be recorded in {}: {}; a later start reads them again from the " +
                         "sstables of those tables, which a compaction of this software rewrites without them",
                         record, e.getMessage(), e);
        }
        catch (IOException | RuntimeException e)
        {
            logger.error("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of system.local and " +
                         "system.peers cannot be recorded in {}; a later start reads them again from the " +
                         "sstables of those tables, which a compaction of this software rewrites without " +
                         "them. Remove the cause reported below (for example make the metadata directory " +
                         "writable, or remove {}.tmp) and restart the node now, before any compaction of " +
                         "system.local and system.peers", record, record, e);
        }
    }

    /**
     * Deletes the record ({@link #recordPath}) if it exists, and its temporary file: called by the startup checks
     * before they write the DSE 5.1 upgrade marker, which begins a new upgrade, so that the values a DSE 5.1 node wrote
     * after a rollback are read again rather than those of the upgrade rolled back. The copies of unreadable records
     * ({@link #UNREADABLE_SUFFIX}) are kept. Returns whether the record is gone: when it is not, an ERROR is logged,
     * the startup checks do not write the marker, and the start is refused ({@link #loadOrReadAtStart}). A temporary
     * file that cannot be deleted is only reported with a WARN: the next write replaces it.
     */
    public static boolean deleteRecordOfAnEarlierUpgrade()
    {
        Path record = recordPath();
        Path temporary = record.resolveSibling(record.getFileName() + TEMPORARY_SUFFIX);
        try
        {
            Files.deleteIfExists(temporary);
        }
        catch (IOException | RuntimeException e)
        {
            logger.warn("DSE 5.1 compatibility mode: unable to delete {}, a temporary file of the record of the " +
                        "values of DSE 5.1's columns of system.local and system.peers", temporary, e);
        }
        try
        {
            if (Files.deleteIfExists(record))
                logger.info("DSE 5.1 compatibility mode: deleted {}, the record of the values of DSE 5.1's columns " +
                            "of system.local and system.peers of an earlier upgrade", record);
            earlierRecordNotDeleted = false;
            return true;
        }
        catch (IOException | RuntimeException e)
        {
            earlierRecordNotDeleted = true;
            logger.error("DSE 5.1 compatibility mode: unable to delete {}, the record of the values of DSE 5.1's " +
                         "columns of system.local and system.peers of an earlier upgrade; the DSE 5.1 upgrade marker " +
                         "is not written and the start is refused. Delete it by hand and start the node again",
                         record, e);
            return false;
        }
    }

    /**
     * Writes {@code columns} to {@code record}, as {@link #load} reads them: a header line, then one line per row,
     * its fields separated by tabs (the node, {@code dse_version}, {@code workload}, {@code graph}, {@code workloads}
     * joined by commas), {@code -} for an absent value. A value that is empty or {@code -}, or that holds a tab, a
     * line break or a comma, is refused with an {@link IllegalArgumentException} (DSE 5.1 writes none). Written to a
     * temporary file, synced, moved in place and the directory synced; the temporary file is deleted on a failure.
     */
    @VisibleForTesting
    static void write(Path record, List<StoredColumns> columns) throws IOException
    {
        StringBuilder content = new StringBuilder(RECORD_HEADER).append('\n');
        for (StoredColumns row : columns)
        {
            List<String> workloads = new ArrayList<>();
            for (String workload : row.workloads)
                workloads.add(checked(workload));
            content.append(row.node()).append('\t')
                   .append(field(row.dseVersion)).append('\t')
                   .append(field(row.workload)).append('\t')
                   .append(row.graph == null ? ABSENT : row.graph.toString()).append('\t')
                   .append(workloads.isEmpty() ? ABSENT : String.join(",", workloads)).append('\n');
        }
        Path temporary = record.resolveSibling(record.getFileName() + TEMPORARY_SUFFIX);
        try
        {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                                                        StandardOpenOption.TRUNCATE_EXISTING))
            {
                ByteBuffer bytes = ByteBuffer.wrap(content.toString().getBytes(StandardCharsets.UTF_8));
                while (bytes.hasRemaining())
                    channel.write(bytes);
                SyncUtil.force(channel, true);
            }
            Files.move(temporary, record, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (IOException | RuntimeException e)
        {
            try
            {
                Files.deleteIfExists(temporary);
            }
            catch (IOException | RuntimeException deleteFailure)
            {
                e.addSuppressed(deleteFailure);
            }
            throw e;
        }
        SyncUtil.trySyncDir(new File(record.getParent()));
        logger.info("DSE 5.1 compatibility mode: recorded the values of DSE 5.1's columns of system.local and " +
                    "system.peers in {}", record);
    }

    /** The rows {@link #write} wrote to {@code record}. */
    @VisibleForTesting
    static List<StoredColumns> load(Path record) throws IOException
    {
        List<String> lines = Files.readAllLines(record, StandardCharsets.UTF_8);
        if (lines.isEmpty() || !RECORD_HEADER.equals(lines.get(0)))
            throw new IOException("not a record of the values of DSE 5.1's columns: its first line is not '" +
                                  RECORD_HEADER + '\'');
        List<StoredColumns> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size()))
        {
            String[] fields = line.split("\t", -1);
            if (fields.length != 5)
                throw new IOException("line '" + line + "' does not have five fields separated by tabs");
            InetAddress peer = "local".equals(fields[0]) ? null : InetAddresses.forString(fields[0]);
            List<String> workloads = new ArrayList<>();
            if (!ABSENT.equals(fields[4]))
            {
                for (String workload : fields[4].split(",", -1))
                {
                    if (workload.isEmpty())
                        throw new IOException("line '" + line + "' has an empty element in its workloads field");
                    workloads.add(workload);
                }
                Collections.sort(workloads);
            }
            Boolean graph;
            if (ABSENT.equals(fields[3]))
                graph = null;
            else if ("true".equals(fields[3]) || "false".equals(fields[3]))
                graph = Boolean.valueOf(fields[3]);
            else
                throw new IOException("line '" + line + "' has a graph field that is not true, false or " + ABSENT);
            rows.add(new StoredColumns(peer, workloads, value(fields[2]), graph, value(fields[1])));
        }
        return rows;
    }

    private static String field(@Nullable String value)
    {
        return value == null ? ABSENT : checked(value);
    }

    /** {@code value}, refused when it cannot be written as a field of the record. */
    private static String checked(String value)
    {
        if (value.isEmpty() || ABSENT.equals(value) || value.indexOf('\t') >= 0 || value.indexOf('\n') >= 0
            || value.indexOf('\r') >= 0 || value.indexOf(',') >= 0)
            throw new IllegalArgumentException("the value '" + value + "' cannot be recorded: it is empty or " +
                                               ABSENT + ", or holds a tab, a line break or a comma");
        return value;
    }

    @Nullable
    private static String value(String field)
    {
        return ABSENT.equals(field) ? null : field;
    }

    /**
     * The values of the five columns in the live sstables of {@code cfs}, {@code system.local} or {@code system.peers},
     * merged as a read merges them, each sstable read in full into the heap (both tables are small). An sstable that
     * cannot be read, or a partition whose values cannot be decoded, is skipped with a WARN: the values of the others
     * are returned, which may then be older than those it holds, or belong to a peer it deletes. A
     * {@code CorruptSSTableException} or an {@code FSError} (a checksum mismatch, a failing disk) also goes to the
     * {@code disk_failure_policy}, which with the {@code cassandra.yaml} default {@code stop} stops the start; any
     * other failure (such as a truncated statistics component) only skips.
     */
    @VisibleForTesting
    static List<StoredColumns> read(ColumnFamilyStore cfs)
    {
        return read(cfs, new ArrayList<>());
    }

    /** As {@link #read(ColumnFamilyStore)}, adding to {@code skipped} the sstables it could not read. */
    private static List<StoredColumns> read(ColumnFamilyStore cfs, List<String> skipped)
    {
        TableMetadata withDse51Columns = withDse51Columns(cfs.metadata());
        Map<DecoratedKey, List<PartitionUpdate>> partitions = new HashMap<>();
        try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.selectFunction(SSTableSet.LIVE)))
        {
            for (SSTableReader sstable : view.sstables)
            {
                try
                {
                    for (PartitionUpdate partition : partitions(sstable, withDse51Columns))
                        partitions.computeIfAbsent(partition.partitionKey(), key -> new ArrayList<>()).add(partition);
                }
                catch (IOException | RuntimeException | FSError e)
                {
                    skipped.add(sstable.descriptor.toString());
                    logger.warn("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of {} cannot be read " +
                                "from {}, which is skipped; the values read from the other sstables may be older " +
                                "than those it holds, or belong to a peer it deletes", cfs.metadata(),
                                sstable.descriptor, e);
                    JVMStabilityInspector.inspectThrowable(e);
                }
            }
        }

        boolean local = SystemKeyspace.LOCAL.equals(cfs.name);
        List<StoredColumns> found = new ArrayList<>();
        long nowInSec = FBUtilities.nowInSeconds();
        for (List<PartitionUpdate> updates : partitions.values())
        {
            try (RowIterator rows = UnfilteredRowIterators.filter(PartitionUpdate.merge(updates).unfilteredIterator(),
                                                                  nowInSec))
            {
                InetAddress peer = local ? null : InetAddressType.instance.compose(rows.partitionKey().getKey());
                while (rows.hasNext())
                {
                    StoredColumns columns = columnsOf(withDse51Columns, peer, rows.next(), nowInSec);
                    if (columns != null)
                        found.add(columns);
                }
            }
            catch (RuntimeException e)
            {
                String key = cfs.metadata().partitionKeyType.getString(updates.get(0).partitionKey().getKey());
                logger.warn("DSE 5.1 compatibility mode: the values of DSE 5.1's columns of {} in the partition {} " +
                            "cannot be decoded, and are left out, also of the record of a start in the mode",
                            cfs.metadata(), key, e);
                JVMStabilityInspector.inspectThrowable(e);
            }
        }
        found.sort(BY_NODE);
        return found;
    }

    /**
     * {@code metadata} with the five columns as regular columns and without their record as dropped columns: its rows
     * keep their cells, as in DSE 5.1's definition of the table. Its id is that of {@code metadata}. Public for the
     * tests of other packages.
     */
    public static TableMetadata withDse51Columns(TableMetadata metadata)
    {
        Map<ByteBuffer, DroppedColumn> dropped = new HashMap<>(metadata.droppedColumns);
        dropped.keySet().removeAll(COLUMN_NAMES);
        TableMetadata.Builder builder = metadata.unbuild().droppedColumns(dropped);
        for (Map.Entry<String, AbstractType<?>> column : COLUMNS.entrySet())
            builder.addRegularColumn(column.getKey(), column.getValue());
        return builder.build();
    }

    /**
     * The partitions of a big {@code sstable}, read in full with {@code metadata} (the table with the five columns),
     * in the order of the data file; none for an sstable of another format, which DSE 5.1 does not write. The sstable
     * is read through its open reader, without changing any of its files. Public for the tests of other packages.
     */
    public static List<PartitionUpdate> partitions(SSTableReader sstable, TableMetadata metadata) throws IOException
    {
        Descriptor descriptor = sstable.descriptor;
        if (!BigFormat.is(descriptor.getFormat()))
            return Collections.emptyList();
        MetadataComponent component = descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.HEADER);
        SerializationHeader.Component written = (SerializationHeader.Component) component;
        SerializationHeader header = written.toHeader(descriptor.toString(), metadata, descriptor.version, false);
        int version = descriptor.version.correspondingMessagingVersion();
        DeserializationHelper helper = new DeserializationHelper(metadata, version, DeserializationHelper.Flag.LOCAL);
        List<PartitionUpdate> partitions = new ArrayList<>();
        try (RandomAccessReader file = sstable.openDataReaderForScan())
        {
            while (!file.isEOF())
            {
                DecoratedKey key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(file));
                DeletionTime deletion = DeletionTime.getSerializer(descriptor.version).deserialize(file);
                SSTableSimpleIterator rows = SSTableSimpleIterator.create(metadata, file, header, helper, deletion);
                Row staticRow = rows.readStaticRow();
                UnfilteredRowIterator partition = new AbstractUnfilteredRowIterator(metadata, key, deletion,
                                                                                   header.columns(), staticRow,
                                                                                   false, header.stats())
                {
                    @Override
                    protected Unfiltered computeNext()
                    {
                        return rows.hasNext() ? rows.next() : endOfData();
                    }
                };
                partitions.add(PartitionUpdate.fromIterator(partition, ColumnFilter.all(metadata)));
            }
        }
        return partitions;
    }

    /** The live values of the columns {@link StoredColumns} keeps in {@code row}; null when it holds none. */
    @Nullable
    private static StoredColumns columnsOf(TableMetadata metadata, @Nullable InetAddress peer, Row row, long nowInSec)
    {
        String workload = text(row, column(metadata, WORKLOAD), nowInSec);
        String dseVersion = text(row, column(metadata, DSE_VERSION), nowInSec);
        Cell<?> graphCell = row.getCell(column(metadata, GRAPH));
        Boolean graph = graphCell == null || !graphCell.isLive(nowInSec)
                        ? null
                        : BooleanType.instance.compose(graphCell.buffer());
        Cell<?> workloadsCell = row.getCell(column(metadata, WORKLOADS));
        List<String> workloads = workloadsCell == null || !workloadsCell.isLive(nowInSec)
                                 ? Collections.emptyList()
                                 : new ArrayList<>(WORKLOADS_TYPE.compose(workloadsCell.buffer()));
        if (workload == null && dseVersion == null && graph == null && workloads.isEmpty())
            return null;
        return new StoredColumns(peer, workloads, workload, graph, dseVersion);
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(UTF8Type.instance.decompose(name));
    }

    @Nullable
    private static String text(Row row, ColumnMetadata column, long nowInSec)
    {
        Cell<?> cell = row.getCell(column);
        return cell == null || !cell.isLive(nowInSec) ? null : UTF8Type.instance.compose(cell.buffer());
    }
}
