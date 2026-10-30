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

package org.apache.cassandra.db.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;

/**
 * The hidden columns DSE 6 keeps for the materialized views of this node, as DSE lists them in its
 * {@code system_schema.hidden_columns} table, read from the sstables DSE left in this node's data directories.
 * <p>
 * This software does not define that table, so it does not load it with the rest of the schema. Nor are the hidden
 * columns of a view derived from its definition alone: a view DSE created in its "old" format has none. DSE 6.7 and
 * later create views in the "new" format, the one with hidden columns (and refuse to create one that needs them while
 * a node of the cluster runs an earlier release or is not a DSE node), unless the node runs with
 * {@code -Dcassandra.mv.create_legacy_schema=true}; a view created before DSE 6.7, or with that property, has the old
 * format. The DSE 6.8.65 fixture shows what the table holds for a view of the new format
 * ({@code system-schema-wide_by_v.txt} in {@code test/data/dse-compat/sstables/dse-6.8.65-mv}: the row of {@code .v},
 * regular, required for liveness). A view of the old format has no row there, and its sstables name no hidden column:
 * the fixtures {@code test/data/dse-compat/sstables/dse-6.8.65-mv-updates} and
 * {@code test/data/dse-compat/sstables/dse-6.9.26-mv-updates} hold one, created with that property, next to one of the
 * new format. So a view the table lists nothing for is taken to have no hidden column.
 * Its sstables are complete: a DSE node is drained before it is upgraded, which flushes every table, and this software
 * does not replay DSE's commit log for a table it does not define. The table does not change while this software runs
 * in DSE 6 compatibility mode: no DDL is expected during the upgrade; a view created meanwhile by this software has no
 * entry, and its data is exchanged as it is, with a WARN.
 * <p>
 * In DSE 6 compatibility mode the catalog is read at startup, once the schema and the sstables are loaded
 * ({@link #checkViewsAtStartup}), which refuses to start when the node cannot exchange the data of one of its views with
 * DSE 6 nodes: a view with a hidden column other than the one of its regular base column in the primary key, a view
 * whose own DSE sstables name a hidden column the table does not list, or data directories without DSE's table. For
 * each view, {@link #translatedColumn} then decides how its data is exchanged with DSE, and how the view updates DSE
 * stored on the disk of this node are decoded (see {@link DseViewWireFormat}). On a node with views, the startup reads
 * it before the commit log is replayed, the hints files DSE left are converted and the batchlog is replayed, which
 * decode those updates; otherwise it is read the first time it is needed.
 */
public final class DseViewHiddenColumnCatalog
{
    private static final Logger logger = LoggerFactory.getLogger(DseViewHiddenColumnCatalog.class);

    /** The name of DSE's table of hidden columns in {@code system_schema}. */
    public static final String HIDDEN_COLUMNS = "hidden_columns";

    /** DSE's {@code kind} of a regular column. */
    private static final String REGULAR_KIND = "regular";

    /** The id of DSE's table, the one of a system table of that name: the end of the name of its directory. */
    public static final TableId HIDDEN_COLUMNS_ID = TableId.forSystemTable(SchemaConstants.SCHEMA_KEYSPACE_NAME, HIDDEN_COLUMNS);

    /** The {@code compatstatus} refusal reason of the view data this software does not exchange with DSE 6 nodes. */
    public static final String REFUSED_VIEW = "materialized view data with a DSE hidden column this software does not reproduce";

    private static volatile DseViewHiddenColumnCatalog instance;

    /** Why the catalog of this node could not be read, if it could not. */
    private static volatile String loadFailure;

    /** The hidden columns of each view, by keyspace and view name. */
    private final Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns;

    /** What {@link #translatedColumn} decided for each view, by id: a view definition does not change in DSE 6 compatibility mode. */
    private final Map<TableId, Translation> translations = new ConcurrentHashMap<>();

    /** One row of DSE's {@code system_schema.hidden_columns}. */
    public static final class HiddenColumn
    {
        public final String name;
        public final String kind;
        public final boolean requiredForLiveness;
        /** The CQL type of the column, as DSE writes it ({@code text}). */
        public final String type;

        public HiddenColumn(String name, String kind, boolean requiredForLiveness, String type)
        {
            this.name = name;
            this.kind = kind;
            this.requiredForLiveness = requiredForLiveness;
            this.type = type;
        }

        @Override
        public String toString()
        {
            return name + ' ' + type + " (" + kind + (requiredForLiveness ? ", required for liveness)" : ")");
        }
    }

    /** The decision for one view: the column to translate (possibly none), or the error refusing the view. */
    private static final class Translation
    {
        private final ColumnMetadata column;
        private final String refusal;

        private Translation(ColumnMetadata column, String refusal)
        {
            this.column = column;
            this.refusal = refusal;
        }
    }

    @VisibleForTesting
    public DseViewHiddenColumnCatalog(Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns)
    {
        this.hiddenColumns = hiddenColumns;
    }

    /**
     * The catalog of this node, read from its data directories the first time it is needed (at startup in DSE 6
     * compatibility mode, see {@link #checkViewsAtStartup}).
     *
     * @throws IllegalStateException when DSE's table cannot be read, counted as a refusal of view data
     * ({@link #REFUSED_VIEW}): no view data is exchanged with DSE 6 nodes then, rather than exchanged without knowing
     * whether DSE keeps hidden columns for it; the error is kept until the node restarts
     */
    public static DseViewHiddenColumnCatalog instance()
    {
        DseViewHiddenColumnCatalog catalog = readableInstance();
        if (catalog == null)
        {
            DseCompatRefusals.refused(REFUSED_VIEW);
            throw new IllegalStateException(loadFailure);
        }
        return catalog;
    }

    /**
     * {@link #instance()} without counting a refusal, for the view updates DSE stored on the disk of this node, which are
     * not data exchanged with DSE 6 nodes.
     *
     * @return the catalog of this node, or {@code null} when DSE's table cannot be read ({@link #loadFailure()} says why)
     */
    static DseViewHiddenColumnCatalog readableInstance()
    {
        DseViewHiddenColumnCatalog catalog = instance;
        if (catalog == null && loadFailure == null)
        {
            synchronized (DseViewHiddenColumnCatalog.class)
            {
                if (instance == null && loadFailure == null)
                {
                    try
                    {
                        instance = load(List.of(DatabaseDescriptor.getAllDataFileLocations()));
                    }
                    catch (RuntimeException | FSError | AssertionError e)
                    {
                        // logged here with its cause; a startup refusal repeats the message, a message refused at run time does not
                        loadFailure = "DSE 6 compatibility mode: cannot read DSE's system_schema." + HIDDEN_COLUMNS + " from the data directories of this node (" +
                                      (e instanceof Error || e.getMessage() == null ? e.toString() : e.getMessage()) +
                                      "); the data of materialized views is not exchanged with DSE 6 nodes";
                        logger.error(loadFailure, e);
                        // then the disk failure policy, which may stop the node, for a file that cannot be read from disk (the
                        // reader applies it already to a component it cannot open, before this point)
                        JVMStabilityInspector.inspectThrowable(e);
                    }
                }
                catalog = instance;
            }
        }
        return catalog;
    }

    /** Why the catalog of this node could not be read, or {@code null} when it was read or not tried yet. */
    static String loadFailure()
    {
        return loadFailure;
    }

    /**
     * Replaces the catalog of this node, and forgets why it could not be read.
     *
     * @return the catalog it replaces, possibly {@code null} when none was read yet
     */
    @VisibleForTesting
    public static synchronized DseViewHiddenColumnCatalog setInstance(DseViewHiddenColumnCatalog catalog)
    {
        DseViewHiddenColumnCatalog previous = instance;
        instance = catalog;
        loadFailure = null;
        return previous;
    }

    /**
     * DSE 6 compatibility mode: reads the catalog, and refuses to start when the data of a view of this node cannot be
     * exchanged with DSE 6 nodes ({@link #startupProblems}). A no-op otherwise, and on a node without materialized views.
     */
    public static void checkViewsAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse6CompatibilityMode())
            return;
        List<TableMetadata> views = new ArrayList<>();
        for (KeyspaceMetadata keyspace : Schema.instance.distributedKeyspaces())
            for (ViewMetadata view : keyspace.views)
                views.add(view.metadata);
        if (views.isEmpty())
            return;

        DseViewHiddenColumnCatalog catalog;
        try
        {
            catalog = instance();
        }
        catch (IllegalStateException e)
        {
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE, e.getMessage());
        }
        List<StartupProblem> problems = catalog.startupProblems(views);
        if (problems.isEmpty())
            return;
        List<String> messages = new ArrayList<>(problems.size());
        boolean diskState = false;
        for (StartupProblem problem : problems)
        {
            messages.add(problem.message);
            diskState |= problem.diskState;
        }
        throw new StartupException(diskState ? StartupException.ERR_WRONG_DISK_STATE : StartupException.ERR_WRONG_CONFIG, String.join("\n", messages));
    }

    /** Why the data of a view cannot be exchanged with DSE 6 nodes. */
    public static final class StartupProblem
    {
        public final String message;
        /** Whether the data directories are at fault (DSE's table missing from them), rather than the view itself. */
        public final boolean diskState;

        private StartupProblem(String message, boolean diskState)
        {
            this.message = message;
            this.diskState = diskState;
        }

        @Override
        public String toString()
        {
            return message;
        }
    }

    /**
     * Why the data of each of {@code views} cannot be exchanged with DSE 6 nodes: {@link #translatedColumn} refuses it,
     * or DSE lists no hidden column for a view whose own DSE sstables on this node name one (the column
     * {@link DseViewHiddenColumns} reads), which means that this node's copy of DSE's table is not the one DSE wrote.
     */
    @VisibleForTesting
    public List<StartupProblem> startupProblems(Iterable<TableMetadata> views)
    {
        List<StartupProblem> problems = new ArrayList<>();
        for (TableMetadata view : views)
        {
            try
            {
                if (translatedColumn(view) != null || !hiddenColumns(view.keyspace, view.name).isEmpty())
                    continue;
            }
            catch (IllegalStateException e)
            {
                problems.add(new StartupProblem(e.getMessage(), false));
                continue;
            }
            ColumnFamilyStore store = Keyspace.openAndGetStoreIfExists(view);
            if (store == null)
                continue;
            for (SSTableReader sstable : store.getLiveSSTables())
            {
                ColumnMetadata hidden = sstable.header.dseViewLivenessColumn();
                if (hidden != null)
                {
                    problems.add(new StartupProblem(String.format("DSE 6 compatibility mode: the sstable %s of materialized view %s, written by DSE, holds the " +
                                                                  "hidden column %s, but DSE's system_schema.%s on this node lists no hidden column for the " +
                                                                  "view; the data directories of this node do not hold the table DSE wrote, and this software " +
                                                                  "cannot exchange the view's data with DSE 6 nodes",
                                                                  sstable.descriptor, view, hidden.name, HIDDEN_COLUMNS),
                                                    true));
                    break;
                }
            }
        }
        return problems;
    }

    /** The hidden columns DSE lists for the view {@code keyspace.view}; empty when it lists none. */
    public List<HiddenColumn> hiddenColumns(String keyspace, String view)
    {
        return hiddenColumns.getOrDefault(Pair.create(keyspace, view), Collections.emptyList());
    }

    /**
     * The hidden column the data of the view {@code view} carries when it is exchanged with DSE 6 nodes:
     * {@link DseViewHiddenColumns#requiredHiddenColumn}, when DSE lists exactly that column for the view, regular, required
     * for liveness and of the type of the view column; {@code null} when DSE lists no hidden column for the view, whose
     * data is then exchanged as it is, or when the view is not (or no longer) in the schema.
     *
     * @throws IllegalStateException when DSE lists any other hidden column for the view: its data cannot be exchanged
     * with DSE 6 nodes without returning or storing wrong view rows
     */
    public ColumnMetadata translatedColumn(TableMetadata view)
    {
        return translatedColumn(view, true);
    }

    /**
     * {@link #translatedColumn(TableMetadata)} without counting a refusal, for the view updates DSE stored on the disk of
     * this node, which are not data exchanged with DSE 6 nodes.
     *
     * @throws IllegalStateException as {@link #translatedColumn(TableMetadata)}
     */
    ColumnMetadata storedTranslatedColumn(TableMetadata view)
    {
        return translatedColumn(view, false);
    }

    private ColumnMetadata translatedColumn(TableMetadata view, boolean countRefusal)
    {
        Translation translation = translations.get(view.id);
        if (translation == null)
        {
            // the decision depends on the view and its base table being in the schema
            if (Schema.instance.getView(view.keyspace, view.name) == null)
                return null;
            Translation decided = translationOf(view);
            translation = translations.putIfAbsent(view.id, decided);
            if (translation == null)
            {
                translation = decided;
                logDecision(view, decided);
            }
        }
        if (translation.refusal != null)
        {
            if (countRefusal)
                DseCompatRefusals.refused(REFUSED_VIEW);
            throw new IllegalStateException(translation.refusal);
        }
        return translation.column;
    }

    private Translation translationOf(TableMetadata view)
    {
        List<HiddenColumn> hidden = hiddenColumns(view.keyspace, view.name);
        if (hidden.isEmpty())
            return new Translation(null, null);

        ColumnMetadata derived = DseViewHiddenColumns.requiredHiddenColumn(view);
        if (reproduces(hidden, derived))
            return new Translation(derived, null);
        return new Translation(null, String.format("DSE 6 compatibility mode: DSE keeps the hidden columns %s for materialized view %s; this software only reproduces " +
                                                   "the hidden column of a regular base column in the view primary key, and refuses to exchange the view's " +
                                                   "data with DSE 6 nodes rather than return or store wrong view rows. %s",
                                                   hidden, view, DseViewHiddenColumns.REMEDY));
    }

    /**
     * Whether this software reproduces the hidden columns {@code hidden} DSE lists for a view whose hidden column of its
     * regular base column in the primary key is {@code derived} ({@link DseViewHiddenColumns#requiredHiddenColumn},
     * null when the view has none): DSE lists exactly that column, regular, required for liveness and of the type of
     * the view column. The type is compared as the CQL text DSE writes ({@code text}, {@code int}): a type DSE spells
     * otherwise than this software (a user type, a tuple, a frozen collection) refuses the view rather than accept a
     * column this software may not read the same. A view DSE lists no hidden column for is not covered: its data is
     * exchanged as it is.
     */
    public static boolean reproduces(List<HiddenColumn> hidden, ColumnMetadata derived)
    {
        if (hidden.size() != 1 || derived == null)
            return false;
        HiddenColumn column = hidden.get(0);
        return column.name.equals(derived.name.toString()) && REGULAR_KIND.equals(column.kind) && column.requiredForLiveness
               && derived.type.asCQL3Type().toString().equals(column.type);
    }

    /**
     * Logs, once for each view, how its data is exchanged with DSE 6 nodes; a refused view is logged at ERROR with its own
     * line, which a startup refusal (listing every refused view) repeats.
     */
    private static void logDecision(TableMetadata view, Translation translation)
    {
        if (translation.refusal != null)
            logger.error(translation.refusal);
        else if (translation.column != null)
            logger.info("DSE 6 compatibility mode: the data of materialized view {} is exchanged with DSE 6 nodes with DSE's hidden column {}", view, translation.column.name);
        else if (DseViewHiddenColumns.requiredHiddenColumn(view) != null)
            logger.warn("DSE 6 compatibility mode: DSE's system_schema.{} on this node lists no hidden column for materialized view {}, whose data is " +
                        "exchanged with DSE 6 nodes as it is, as for a view of DSE's old format or one created by this software; if DSE created the " +
                        "view in its new format, the data directories of this node do not hold the table DSE wrote, and DSE reads the view rows of " +
                        "this node as dead",
                        HIDDEN_COLUMNS, view);
    }

    /**
     * Reads DSE's {@code system_schema.hidden_columns} from the sstables in the {@code system_schema} directory of each of
     * the given data directories.
     * <p>
     * Only the sstables of committed transactions are read (as for any table, see {@link LifecycleTransaction#getFiles});
     * a directory of the table under another table id, which is not DSE's, is skipped.
     *
     * @throws IllegalStateException when no data directory holds the directory of that table, which every node that ran
     * DSE 6 has (DSE creates the directory of each of its tables)
     * @throws RuntimeException when a directory, a transaction log or an sstable of that table cannot be read (an
     * {@link java.io.UncheckedIOException}, a {@link org.apache.cassandra.io.sstable.CorruptSSTableException}...), or an
     * {@link FSError} when a file of that table cannot be read from disk, or an {@link AssertionError} when a file is
     * shorter than its metadata says (a truncated copy)
     */
    public static DseViewHiddenColumnCatalog load(Collection<File> dataDirectories)
    {
        List<SSTableReader> readers = new ArrayList<>();
        List<UnfilteredPartitionIterator> scanners = new ArrayList<>();
        Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns = new HashMap<>();
        TableMetadata metadata = hiddenColumnsMetadata(HIDDEN_COLUMNS_ID);
        boolean tableFound = false;
        try
        {
            for (File dataDirectory : dataDirectories)
            {
                File schemaDirectory = new File(dataDirectory, SchemaConstants.SCHEMA_KEYSPACE_NAME);
                // a directory that cannot be listed fails here, rather than pass for a directory that is not there
                File[] tableDirectories = schemaDirectory.isDirectory()
                                          ? schemaDirectory.listUnchecked(file -> file.isDirectory() && file.name().startsWith(HIDDEN_COLUMNS + '-'))
                                          : null;
                if (tableDirectories == null)
                    continue;
                for (File tableDirectory : tableDirectories)
                {
                    String id = tableDirectory.name().substring(HIDDEN_COLUMNS.length() + 1);
                    if (!id.equals(HIDDEN_COLUMNS_ID.toHexString()))
                    {
                        logger.warn("DSE 6 compatibility mode: {} is not the directory of DSE's system_schema.{}, whose table id is {}, skipped",
                                    tableDirectory, HIDDEN_COLUMNS, HIDDEN_COLUMNS_ID.toHexString());
                        continue;
                    }
                    tableFound = true;
                    List<File> dataFiles = LifecycleTransaction.getFiles(tableDirectory.toPath(),
                                                                         (file, type) -> type == Directories.FileType.FINAL && file.name().endsWith("-" + SSTableFormat.Components.DATA.name),
                                                                         Directories.OnTxnErr.THROW);
                    for (File dataFile : dataFiles)
                    {
                        Descriptor descriptor = Descriptor.fromFileWithComponent(dataFile, false).left;
                        readers.add(SSTableReader.openNoValidation(null, descriptor, TableMetadataRef.forOfflineTools(metadata)));
                    }
                }
            }
            if (!tableFound)
                throw new IllegalStateException("no data directory holds " + SchemaConstants.SCHEMA_KEYSPACE_NAME + '/' + HIDDEN_COLUMNS + '-' +
                                                HIDDEN_COLUMNS_ID.toHexString() + ": " + dataDirectories +
                                                " are not the data directories of a node that ran DSE 6");
            if (readers.isEmpty())
                return loaded(hiddenColumns);

            for (SSTableReader reader : readers)
                scanners.add(reader.getScanner());
            ColumnMetadata kind = metadata.getColumn(ByteBufferUtil.bytes("kind"));
            ColumnMetadata requiredForLiveness = metadata.getColumn(ByteBufferUtil.bytes("required_for_liveness"));
            ColumnMetadata type = metadata.getColumn(ByteBufferUtil.bytes("type"));
            // from here the merge owns the scanners, and closes them
            UnfilteredPartitionIterator merged = UnfilteredPartitionIterators.merge(new ArrayList<>(scanners), UnfilteredPartitionIterators.MergeListener.NOOP);
            scanners.clear();
            try (PartitionIterator partitions = UnfilteredPartitionIterators.filter(merged, FBUtilities.nowInSeconds()))
            {
                while (partitions.hasNext())
                {
                    try (RowIterator partition = partitions.next())
                    {
                        String keyspace = UTF8Type.instance.compose(partition.partitionKey().getKey());
                        while (partition.hasNext())
                        {
                            Row row = partition.next();
                            String view = UTF8Type.instance.compose(row.clustering().bufferAt(0));
                            String column = UTF8Type.instance.compose(row.clustering().bufferAt(1));
                            hiddenColumns.computeIfAbsent(Pair.create(keyspace, view), k -> new ArrayList<>())
                                         .add(new HiddenColumn(column, text(row.getCell(kind)), bool(row.getCell(requiredForLiveness)), text(row.getCell(type))));
                        }
                    }
                }
            }
            return loaded(hiddenColumns);
        }
        finally
        {
            for (UnfilteredPartitionIterator scanner : scanners)
                FileUtils.closeQuietly(scanner);
            for (SSTableReader reader : readers)
                reader.selfRef().release();
        }
    }

    private static DseViewHiddenColumnCatalog loaded(Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns)
    {
        logger.info("DSE 6 compatibility mode: DSE's system_schema.{} lists hidden columns for {} materialized views{}",
                    HIDDEN_COLUMNS, hiddenColumns.size(), hiddenColumns.isEmpty() ? "" : ": " + hiddenColumns);
        return new DseViewHiddenColumnCatalog(hiddenColumns);
    }

    private static String text(Cell<?> cell)
    {
        return cell == null ? null : UTF8Type.instance.compose(cell.buffer());
    }

    private static boolean bool(Cell<?> cell)
    {
        return cell != null && BooleanType.instance.compose(cell.buffer());
    }

    /**
     * DSE's {@code system_schema.hidden_columns}: the columns of {@code system_schema.columns}, as the rows of the
     * DSE 6.8.65 fixture in {@code test/data/dse-compat/sstables/dse-6.8.65-mv/system-schema-wide_by_v.txt} show them,
     * with the partitioner of this node's schema tables.
     */
    private static TableMetadata hiddenColumnsMetadata(TableId id)
    {
        TableMetadata columns = Schema.instance.getTableMetadata(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.COLUMNS);
        return TableMetadata.builder(SchemaConstants.SCHEMA_KEYSPACE_NAME, HIDDEN_COLUMNS, id)
                            .partitioner(columns.partitioner)
                            .addPartitionKeyColumn("keyspace_name", UTF8Type.instance)
                            .addClusteringColumn("table_name", UTF8Type.instance)
                            .addClusteringColumn("column_name", UTF8Type.instance)
                            .addRegularColumn("clustering_order", UTF8Type.instance)
                            .addRegularColumn("column_name_bytes", BytesType.instance)
                            .addRegularColumn("kind", UTF8Type.instance)
                            .addRegularColumn("position", Int32Type.instance)
                            .addRegularColumn("type", UTF8Type.instance)
                            .addRegularColumn("required_for_liveness", BooleanType.instance)
                            .build();
    }
}
