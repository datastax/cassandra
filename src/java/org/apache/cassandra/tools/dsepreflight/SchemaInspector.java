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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog;
import org.apache.cassandra.db.view.DseViewHiddenColumnCatalog.HiddenColumn;
import org.apache.cassandra.db.view.DseViewHiddenColumns;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.Dse6AccessControlCheck;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tools.dsepreflight.Finding.blocking;
import static org.apache.cassandra.tools.dsepreflight.Finding.info;
import static org.apache.cassandra.tools.dsepreflight.Finding.warning;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.SYSTEM_INFO_ENCRYPTED_TABLE_NAMES;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.SYSTEM_INFO_KEEP_UNTIL;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.SYSTEM_INFO_REWRITE;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.SYSTEM_KEY_DIRECTORY;
import static org.apache.cassandra.tools.dsepreflight.NodeInspector.list;

/**
 * The checks of the preflight inspection on DSE's schema ({@code system_schema}) and on its access controls
 * ({@code system_auth.role_permissions}), both read offline from the sstables of this node ({@link DseTableReader}).
 * The schema rows are turned into this software's metadata with the parsers of its own schema load (table parameters by
 * {@link SchemaKeyspace#createTableParamsFromRow}, types by {@link CQLTypeParser}, replication by
 * {@link KeyspaceParams#create}), so that what an upgraded node would fail to load is found here first. The rules on
 * DSE features are those of the startup checks of {@code internode_compatibility_mode: DSE_6}
 * ({@link Dse6NonCoreTraces}, {@link Dse6AccessControlCheck}, {@link DseViewHiddenColumnCatalog}), which refuse the
 * same things on the first start of an upgraded node.
 */
final class SchemaInspector
{
    static final String SCHEMA = SchemaConstants.SCHEMA_KEYSPACE_NAME;
    /** DSE's table of the hidden columns of materialized views. */
    static final String HIDDEN_COLUMNS = DseViewHiddenColumnCatalog.HIDDEN_COLUMNS;
    /** DSE 6.8's tables of the schema of DSE Graph (Core engine) keyspaces, absent without graph keyspaces. */
    static final List<String> GRAPH_SCHEMA_TABLES = List.of("vertices", "edges");
    /** The DSE class of tiered storage, the only one of its hierarchy that a table names. */
    static final String TIERED_COMPACTION_STRATEGY = "org.apache.cassandra.db.compaction.TieredCompactionStrategy";
    static final String PROXY_PERMISSION_PREFIX = "PROXY.";
    /** The package of the replication strategies, which the class names of the schema's replication maps start with. */
    private static final String LOCATOR_PACKAGE = "org.apache.cassandra.locator.";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
    private static final String SIMPLE_STRATEGY = "SimpleStrategy";
    private static final String EVERYWHERE_STRATEGY = "EverywhereStrategy";
    /**
     * The replicas of {@code system_auth} recommended in each data center, or as many as its nodes if fewer: with
     * three, an upgraded node still reads DSE's access controls at {@code LOCAL_QUORUM} while another node of its data
     * center is down.
     */
    private static final int AUTH_REPLICAS = 3;

    private final List<File> dataDirectories;
    /** the DSEFS keyspace named in {@code dse.yaml}, when it is not DSE's default */
    @Nullable
    private final String dsefsKeyspace;
    /** the partitioner of the cluster, which every table of the schema has */
    private final IPartitioner partitioner;
    /**
     * whether dse.yaml enables system_info_encryption, whose finding then names the tables it encrypts
     * ({@link NodeInspector#SYSTEM_INFO_ENCRYPTED_TABLE_NAMES}); those are not to be decrypted by an ALTER TABLE, as DSE
     * sets their compression back itself at its first start with the setting off
     */
    private final boolean systemInfoEncryption;
    /** the nodes of each data center, this node included, as nodes/local and nodes/peers record them */
    private final Map<String, Integer> nodesByDataCenter;
    /** the nodes whose data center nodes/local and nodes/peers do not give, not counted in {@link #nodesByDataCenter} */
    private final int nodesWithoutDataCenter;
    /** whether nodes/local and nodes/peers could both be read, so that every node is either counted or without a data center */
    private final boolean nodesFilesRead;
    /** whether a peer already runs HCD 2.0 (the mixed phase), when the replication of system_auth can no longer be changed */
    private final boolean mixedPhase;
    private final List<Finding> findings;

    private final Map<String, UntypedResultSet.Row> keyspaces = new TreeMap<>();
    private final Map<String, Map<String, UntypedResultSet.Row>> tables = new TreeMap<>();
    private final Map<String, Map<String, UntypedResultSet.Row>> views = new TreeMap<>();
    private final Map<String, Map<String, List<UntypedResultSet.Row>>> columns = new HashMap<>();
    private final Map<String, List<UntypedResultSet.Row>> types = new HashMap<>();
    private final List<UntypedResultSet.Row> indexes = new ArrayList<>();
    private final List<UntypedResultSet.Row> functions = new ArrayList<>();
    private final List<UntypedResultSet.Row> triggers = new ArrayList<>();
    private final List<UntypedResultSet.Row> droppedColumns = new ArrayList<>();
    private final Map<Pair<String, String>, List<HiddenColumn>> hiddenColumns = new HashMap<>();
    private final Set<String> graphSchemaKeyspaces = new TreeSet<>();
    private final Map<String, TableMetadata> built = new HashMap<>();
    private boolean hiddenColumnsTableFound;
    /** the tables of {@code system_schema} that could not be read, or that are required and absent */
    private final Set<String> schemaTablesNotRead = new TreeSet<>();

    /** whether the schema could be read; the summary depends on it */
    boolean schemaRead;
    /** the materialized views whose data this software exchanges with DSE 6 nodes, with how */
    final List<String> viewsHandled = new ArrayList<>();
    /** the materialized views whose hidden columns HCD 2.0 does not reproduce, with those columns */
    final List<String> viewsRefused = new ArrayList<>();
    /** the names of the materialized views to drop before the upgrade: those of {@link #viewsRefused} and the unloadable ones */
    final Set<String> viewsToDrop = new TreeSet<>();
    /** the upgrade step on the replication of {@code system_auth}; null when the schema could not be read or its inspection failed */
    @Nullable
    String authReplicationStep;
    int keyspaceCount;
    int tableCount;
    int viewCount;
    /** the rows of {@code system_auth.role_permissions} in this node's replica */
    int rolePermissionsRows;
    /** whether {@code system_auth.role_permissions} could be read */
    boolean rolePermissionsRead;
    /** whether a directory of {@code system_auth.role_permissions} exists in the data directories */
    boolean rolePermissionsFound;

    SchemaInspector(List<File> dataDirectories, @Nullable String dsefsKeyspace, IPartitioner partitioner, boolean systemInfoEncryption,
                    Map<String, Integer> nodesByDataCenter, int nodesWithoutDataCenter, boolean nodesFilesRead,
                    boolean mixedPhase, List<Finding> findings)
    {
        this.dataDirectories = dataDirectories;
        this.dsefsKeyspace = dsefsKeyspace;
        this.partitioner = partitioner;
        this.systemInfoEncryption = systemInfoEncryption;
        this.nodesByDataCenter = nodesByDataCenter;
        this.nodesWithoutDataCenter = nodesWithoutDataCenter;
        this.nodesFilesRead = nodesFilesRead;
        this.mixedPhase = mixedPhase;
        this.findings = findings;
    }

    void inspect()
    {
        schemaRead = readSchema();
        if (schemaRead)
        {
            checkKeyspaces();
            Map<String, Types> typesByKeyspace = checkTypes();
            checkTables(typesByKeyspace);
            checkIndexes();
            checkViews(typesByKeyspace);
            checkDroppedColumns();
            checkFunctions();
            checkTriggers();
            try
            {
                checkAuthReplication();
            }
            catch (RuntimeException | AssertionError e)
            {
                findings.add(blocking("INSPECTION_ERROR", "the inspection of the replication of system_auth failed (" + e + "), so it is incomplete",
                                      "report the error with the output of the tool; the node cannot be declared ready until the inspection completes"));
            }
        }
        checkAccessControls();
    }

    /** The keyspaces of the schema, sorted. */
    List<String> keyspaceNames()
    {
        return new ArrayList<>(keyspaces.keySet());
    }

    /**
     * What the checks of DSE Search and Graph in the schema could not read: the schema, or the tables of it they read
     * that could not be read; empty when they read everything.
     */
    List<String> nonCoreInputsNotRead()
    {
        if (!schemaRead)
            return List.of("the schema");
        List<String> notRead = new ArrayList<>();
        for (String table : schemaTablesNotRead)
            if (table.equals(SchemaKeyspaceTables.INDEXES) || GRAPH_SCHEMA_TABLES.contains(table))
                notRead.add(SCHEMA + '.' + table);
        return notRead;
    }

    /**
     * What the check of the materialized views could not read: the schema, {@code system_schema.views}, or DSE's
     * {@code system_schema.hidden_columns} when the schema has views; empty when it read everything.
     */
    List<String> viewInputsNotRead()
    {
        if (!schemaRead)
            return List.of("the schema");
        List<String> notRead = new ArrayList<>();
        if (schemaTablesNotRead.contains(SchemaKeyspaceTables.VIEWS))
            notRead.add(SCHEMA + '.' + SchemaKeyspaceTables.VIEWS);
        if (!views.isEmpty() && !hiddenColumnsTableFound)
            notRead.add(SCHEMA + '.' + HIDDEN_COLUMNS);
        return notRead;
    }

    /**
     * What the checks of the DSE access controls in the schema and in {@code system_auth.role_permissions} could not
     * read; empty when they read everything.
     */
    List<String> accessControlInputsNotRead()
    {
        List<String> notRead = new ArrayList<>();
        if (!schemaRead)
            notRead.add("the schema");
        if (!rolePermissionsRead || !rolePermissionsFound)
            notRead.add("system_auth." + AuthKeyspace.ROLE_PERMISSIONS);
        return notRead;
    }

    // ---- reading ----

    private boolean readSchema()
    {
        List<String> unreadable = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        DseTableReader.TableRows keyspaceRows = read(SchemaKeyspaceTables.KEYSPACES, List.of("keyspace_name"), List.of(), unreadable, missing, true);
        DseTableReader.TableRows tableRows = read(SchemaKeyspaceTables.TABLES, List.of("keyspace_name"), List.of("table_name"), unreadable, missing, true);
        DseTableReader.TableRows columnRows = read(SchemaKeyspaceTables.COLUMNS, List.of("keyspace_name"), List.of("table_name", "column_name"), unreadable, missing, true);
        DseTableReader.TableRows viewRows = read(SchemaKeyspaceTables.VIEWS, List.of("keyspace_name"), List.of("view_name"), unreadable, missing, false);
        DseTableReader.TableRows typeRows = read(SchemaKeyspaceTables.TYPES, List.of("keyspace_name"), List.of("type_name"), unreadable, missing, false);
        DseTableReader.TableRows indexRows = read(SchemaKeyspaceTables.INDEXES, List.of("keyspace_name"), List.of("table_name", "index_name"), unreadable, missing, false);
        DseTableReader.TableRows functionRows = read(SchemaKeyspaceTables.FUNCTIONS, List.of("keyspace_name"), List.of("function_name", "argument_types"), unreadable, missing, false);
        DseTableReader.TableRows triggerRows = read(SchemaKeyspaceTables.TRIGGERS, List.of("keyspace_name"), List.of("table_name", "trigger_name"), unreadable, missing, false);
        DseTableReader.TableRows hiddenRows = read(HIDDEN_COLUMNS, List.of("keyspace_name"), List.of("table_name", "column_name"), unreadable, missing, false);
        DseTableReader.TableRows droppedRows = read(SchemaKeyspaceTables.DROPPED_COLUMNS, List.of("keyspace_name"), List.of("table_name", "column_name"), unreadable, missing, false);
        List<DseTableReader.TableRows> graphRows = new ArrayList<>();
        for (String graphTable : GRAPH_SCHEMA_TABLES)
            graphRows.add(read(graphTable, List.of("keyspace_name"), List.of(), unreadable, missing, false));

        if (!missing.isEmpty())
            findings.add(blocking("UNREADABLE_SCHEMA",
                                  "no data directory holds " + list(missing) + " (data directories " + dataDirectories + "), so the schema cannot be checked",
                                  "point the tool at the data directories of this DSE node (data_file_directories of DSE's cassandra.yaml, or --data-dir)"));
        if (!unreadable.isEmpty())
            findings.add(blocking("UNREADABLE_SCHEMA",
                                  "cannot read " + list(unreadable) + ", so the schema cannot be checked completely",
                                  "run the tool as the user DSE runs as (or as root); if DSE is running, an sstable may have been compacted away while it " +
                                  "was read: run the tool again; if an sstable is not in DSE 6's trie format, run nodetool upgradesstables on DSE " +
                                  "first; if an sstable is damaged, repair it on DSE first"));
        if (keyspaceRows == null || tableRows == null || columnRows == null)
            return false;
        if (keyspaceRows.rows.isEmpty())
        {
            findings.add(blocking("UNREADABLE_SCHEMA",
                                  "system_schema.keyspaces holds no row in " + dataDirectories + " (sstables " + keyspaceRows.sstables + "), which no DSE node has",
                                  "point the tool at the data directories of this DSE node; if they are the right ones, the schema tables are damaged: " +
                                  "restore them from a snapshot on DSE"));
            return false;
        }

        for (UntypedResultSet.Row row : keyspaceRows.rows)
            keyspaces.put(row.getString("keyspace_name"), row);
        for (UntypedResultSet.Row row : tableRows.rows)
            tables.computeIfAbsent(row.getString("keyspace_name"), k -> new TreeMap<>()).put(row.getString("table_name"), row);
        for (UntypedResultSet.Row row : columnRows.rows)
            columns.computeIfAbsent(row.getString("keyspace_name"), k -> new HashMap<>()).computeIfAbsent(row.getString("table_name"), k -> new ArrayList<>()).add(row);
        if (viewRows != null)
            for (UntypedResultSet.Row row : viewRows.rows)
                views.computeIfAbsent(row.getString("keyspace_name"), k -> new TreeMap<>()).put(row.getString("view_name"), row);
        if (typeRows != null)
            for (UntypedResultSet.Row row : typeRows.rows)
                types.computeIfAbsent(row.getString("keyspace_name"), k -> new ArrayList<>()).add(row);
        if (indexRows != null)
            indexes.addAll(indexRows.rows);
        if (functionRows != null)
            functions.addAll(functionRows.rows);
        if (triggerRows != null)
            triggers.addAll(triggerRows.rows);
        if (droppedRows != null)
            droppedColumns.addAll(droppedRows.rows);
        if (hiddenRows != null)
        {
            hiddenColumnsTableFound = hiddenRows.found;
            for (UntypedResultSet.Row row : hiddenRows.rows)
                hiddenColumns.computeIfAbsent(Pair.create(row.getString("keyspace_name"), row.getString("table_name")), k -> new ArrayList<>())
                             .add(new HiddenColumn(row.getString("column_name"),
                                                   row.has("kind") ? row.getString("kind") : null,
                                                   row.has("required_for_liveness") && row.getBoolean("required_for_liveness"),
                                                   row.has("type") ? row.getString("type") : null));
        }
        for (DseTableReader.TableRows rows : graphRows)
            if (rows != null)
                for (UntypedResultSet.Row row : rows.rows)
                    graphSchemaKeyspaces.add(row.getString("keyspace_name"));
        keyspaceCount = keyspaces.size();
        tableCount = tables.values().stream().mapToInt(Map::size).sum();
        viewCount = views.values().stream().mapToInt(Map::size).sum();
        return true;
    }

    /** Reads a table of {@code system_schema}; null when it cannot be read, or when it is required and absent. */
    @Nullable
    private DseTableReader.TableRows read(String table, List<String> partitionKey, List<String> clustering, List<String> unreadable, List<String> missing, boolean required)
    {
        try
        {
            DseTableReader.TableRows rows = DseTableReader.read(dataDirectories, SCHEMA, table, partitionKey, clustering);
            if (required && !rows.found)
            {
                missing.add(SCHEMA + '.' + table);
                schemaTablesNotRead.add(table);
                return null;
            }
            return rows;
        }
        catch (Exception e)
        {
            unreadable.add(SCHEMA + '.' + table + " (" + e + ')');
            schemaTablesNotRead.add(table);
            return null;
        }
    }

    // ---- keyspaces ----

    private void checkKeyspaces()
    {
        Map<String, List<String>> nonCore = new TreeMap<>();
        List<String> graph = new ArrayList<>();
        List<String> unloadable = new ArrayList<>();
        for (Map.Entry<String, UntypedResultSet.Row> entry : keyspaces.entrySet())
        {
            String name = entry.getKey();
            UntypedResultSet.Row row = entry.getValue();
            String feature = Dse6NonCoreTraces.nonCoreKeyspaceFeature(name);
            if (feature == null && name.equals(dsefsKeyspace))
                feature = Dse6NonCoreTraces.nonCoreKeyspaceFeature("dsefs");
            if (feature != null)
                nonCore.computeIfAbsent(feature, f -> new ArrayList<>()).add(name);
            if (row.has("graph_engine"))
                graph.add(name + " (graph_engine " + row.getString("graph_engine") + ')');
            try
            {
                KeyspaceParams.create(!row.has("durable_writes") || row.getBoolean("durable_writes"),
                                      row.has("replication") ? row.getFrozenTextMap("replication") : Map.of());
            }
            catch (RuntimeException e)
            {
                unloadable.add(name + ": " + message(e));
            }
        }
        for (Map.Entry<String, List<String>> entry : nonCore.entrySet())
            findings.add(blocking("NON_CORE_KEYSPACE",
                                  entry.getKey() + " keyspaces: " + list(entry.getValue()) + "; HCD 2.0 supports only DSE core (the Cassandra workload), " +
                                  "and an upgraded node refuses to start while they exist, even when empty",
                                  "migrate what they hold elsewhere if it is needed and drop them on DSE, empty or not"));
        Set<String> graphTraces = new TreeSet<>(graph);
        for (String keyspace : graphSchemaKeyspaces)
            if (!keyspaces.containsKey(keyspace) || !keyspaces.get(keyspace).has("graph_engine"))
                graphTraces.add(keyspace + " (rows in system_schema.vertices or system_schema.edges)");
        if (!graphTraces.isEmpty())
            findings.add(blocking("GRAPH_KEYSPACE",
                                  "DSE Graph keyspaces: " + list(graphTraces) + "; HCD 2.0 does not support DSE Graph, and an upgraded node refuses to start while they exist",
                                  "migrate the graph data elsewhere and drop these keyspaces on DSE"));
        if (!unloadable.isEmpty())
            findings.add(blocking("UNLOADABLE_KEYSPACE",
                                  "keyspaces whose replication HCD 2.0 cannot load: " + list(unloadable),
                                  "ALTER KEYSPACE on DSE to a replication strategy HCD 2.0 has (NetworkTopologyStrategy, SimpleStrategy, " +
                                  "EverywhereStrategy)"));
    }

    /**
     * {@code system_auth} must give every data center of {@code nodes/local} and {@code nodes/peers} replicas whose
     * {@code LOCAL_QUORUM} its nodes can meet: an upgraded node reads DSE's access controls from it at
     * {@code LOCAL_QUORUM} when it starts (the whole of {@code role_permissions}), and refuses to start when it cannot.
     * Blocking, as the upgrade cannot work then, where a data center with nodes has no replica (not listed by
     * {@code NetworkTopologyStrategy}, or at 0), where {@code NetworkTopologyStrategy} lists no data center with a
     * replica, where {@code SimpleStrategy} spans several data centers or has no {@code replication_factor}, or 0,
     * where the class is none of {@code NetworkTopologyStrategy}, {@code SimpleStrategy} and {@code EverywhereStrategy}
     * (or there is none) or the replicas are no number, or where a data center has more replicas than its nodes can
     * make a {@code LOCAL_QUORUM} of ({@code EverywhereStrategy} over several data centers counts the replicas of the
     * whole ring). A warning where the replication works with every node up but not while another node of the data
     * center is down, which {@link #AUTH_REPLICAS} replicas (or as many as its nodes if fewer) would allow: a data
     * center of 3 nodes or more with fewer than 3 replicas, or with so many more replicas than nodes that its
     * {@code LOCAL_QUORUM} needs every node; never in a data center of 1 or 2 nodes, where no replication lets a node
     * be down. With one data center, {@code SimpleStrategy} is the replication of that data center, so DSE's default
     * (one replica) passes on a single node, and {@code EverywhereStrategy} gives every node a replica. In the mixed
     * phase, when the replication can no longer be changed, the blocking remedy is to upgrade no further node and
     * report to the upgrade owner, and the warning asks for no change, only that every node of the data center stays up
     * while an upgraded node there starts. When the data centers of the nodes are not all known, or a nodes file could
     * not be read, the nodes counted are a lower bound: no upper bound and no cap then, the remedy asking for
     * {@link #AUTH_REPLICAS} replicas in each data center it names (the replicas listed kept when higher), to be
     * lowered by hand where a data center has fewer nodes, and a warning asking for the check by hand when nothing
     * blocks. A schema without {@code system_auth} blocks; a replication that cannot be read is left to
     * {@code UNLOADABLE_KEYSPACE}.
     */
    private void checkAuthReplication()
    {
        UntypedResultSet.Row row = keyspaces.get(SchemaConstants.AUTH_KEYSPACE_NAME);
        if (row == null)
        {
            authReplicationStep = "not checked (the schema has no system_auth keyspace)";
            findings.add(blocking("SYSTEM_AUTH_REPLICATION",
                                  "the schema read from this node's sstables has no system_auth keyspace, which DSE creates at its first start, so its " +
                                  "replication cannot be checked",
                                  "run nodetool flush system_schema on this node and this tool again; if system_auth is still missing, point the tool at " +
                                  "the data directories of this DSE node (data_file_directories of DSE's cassandra.yaml, or --data-dir)"));
            return;
        }
        Map<String, String> replication;
        try
        {
            replication = row.has("replication") ? row.getFrozenTextMap("replication") : Map.of();
        }
        catch (RuntimeException e)
        {
            // UNLOADABLE_KEYSPACE reports it
            authReplicationStep = "not checked (its replication cannot be read, see UNLOADABLE_KEYSPACE)";
            return;
        }
        String shown = cql(replication);
        String className = shortClassName(replication.getOrDefault("class", ""));
        boolean networkTopology = NETWORK_TOPOLOGY_STRATEGY.equals(className);
        boolean simple = SIMPLE_STRATEGY.equals(className);
        // DSE's EverywhereStrategy, which HCD 2.0 has: a replica on every node, its factor the nodes of the whole ring
        boolean everywhere = EVERYWHERE_STRATEGY.equals(className);
        // whether the nodes of each data center are all counted: the data center of every node known, from files read
        boolean countsExact = nodesFilesRead && nodesWithoutDataCenter == 0 && !nodesByDataCenter.isEmpty();
        // what stops an upgraded node from starting even with every node up: blocking
        List<String> problems = new ArrayList<>();
        // what stops an upgraded node from starting while another node of its data center is down, where the
        // recommended replicas would not: a warning
        List<String> shortfalls = new ArrayList<>();
        // the data centers of the shortfalls, whose nodes must all be up while an upgraded node of them starts
        List<String> shortOf = new ArrayList<>();
        // whether a shortfall is too few replicas, and whether one is so many above the nodes that LOCAL_QUORUM needs
        // them all
        boolean fewerThanRecommended = false;
        boolean aboveTheNodes = false;
        // the replicas the replication gives each data center of nodes/local and nodes/peers, when it gives a number
        // above 0
        Map<String, Integer> replicasByDataCenter = new TreeMap<>();
        // the replicas the remedy sets in each data center: those already there, raised to what is recommended and kept
        // within the nodes of the data center; with the nodes counted a lower bound, raised to AUTH_REPLICAS and not
        // capped
        Map<String, Integer> wanted = new TreeMap<>();
        // the data centers listed with replicas that no node of nodes/local and nodes/peers is in, left out of the
        // remedy when the nodes of each data center are all counted
        Set<String> leftOut = new TreeSet<>();
        // the data centers listed, with no number of replicas, that no node of nodes/local and nodes/peers is known in
        Set<String> unnumbered = new TreeSet<>();
        // what the remedy says of replicas it does not carry over (SimpleStrategy over several data centers,
        // EverywhereStrategy)
        String factorNotCarried = "";
        // the data centers NetworkTopologyStrategy lists with replicas
        int listedWithReplicas = 0;
        if (simple)
        {
            String factorValue = replication.get("replication_factor");
            Integer factor = factorValue == null ? null : replicas(factorValue);
            if (factorValue == null)
                problems.add("it has no replication_factor");
            else if (factor == null)
                problems.add("its replication_factor is '" + factorValue + "', not a number of replicas");
            else if (factor == 0)
                problems.add("its replication_factor is 0");
            if (nodesByDataCenter.size() > 1)
            {
                // SimpleStrategy places the replicas around the ring regardless of the data centers
                problems.add("SimpleStrategy does not place replicas in each data center, and nodes/local and nodes/peers name " +
                             nodesByDataCenter.size() + " (" + list(nodesByDataCenter.keySet()) + "), so LOCAL_QUORUM cannot be met in each");
                if (factor != null && factor > 0)
                    factorNotCarried = "; the replication factor of SimpleStrategy spans the data centers, so it is not carried over";
            }
            else if (factor != null && factor > 0)
            {
                // with one data center (none known: nothing to give), the replication factor of SimpleStrategy is the
                // replicas of that data center
                nodesByDataCenter.keySet().forEach(dataCenter -> replicasByDataCenter.put(dataCenter, factor));
            }
        }
        else if (networkTopology)
        {
            for (Map.Entry<String, String> option : new TreeMap<>(replication).entrySet())
            {
                // replication_factor is an option of NetworkTopologyStrategy, not a data center, which DSE does not
                // store
                if (option.getKey().equals("class") || option.getKey().equals("replication_factor"))
                    continue;
                Integer replicas = replicas(option.getValue());
                if (replicas == null)
                    problems.add(option.getKey() + " is listed with '" + option.getValue() + "', not a number of replicas");
                if (replicas == null && !nodesByDataCenter.containsKey(option.getKey()))
                    unnumbered.add(option.getKey());
                else if (replicas != null && replicas > 0 && nodesByDataCenter.containsKey(option.getKey()))
                    replicasByDataCenter.put(option.getKey(), replicas);
                else if (replicas != null && replicas > 0 && !countsExact)
                    wanted.put(option.getKey(), replicas);
                else if (replicas != null && replicas > 0)
                    leftOut.add(option.getKey());
                if (replicas != null && replicas > 0)
                    listedWithReplicas++;
            }
        }
        else if (everywhere)
        {
            // LOCAL_QUORUM is the quorum of the factor of the whole ring, met by the replicas in the node's data center
            // only; known only when the nodes of each data center are all counted
            int ring = nodesByDataCenter.values().stream().mapToInt(Integer::intValue).sum();
            factorNotCarried = "; the replicas of EverywhereStrategy are those of the whole ring, so they are not carried over";
            if (countsExact)
                nodesByDataCenter.keySet().forEach(dataCenter -> replicasByDataCenter.put(dataCenter, ring));
            // only a data center of more than half of the nodes meets that quorum, so at most one: with two known,
            // whatever the nodes not counted, one of them cannot
            else if (nodesByDataCenter.size() > 1)
                problems.add("with EverywhereStrategy, LOCAL_QUORUM counts the replicas of the whole ring, which only a data center of " +
                             "more than half of the nodes meets, and nodes/local and nodes/peers name " + nodesByDataCenter.size() + " (" +
                             list(nodesByDataCenter.keySet()) + "), so LOCAL_QUORUM cannot be met in each");
        }
        else
        {
            problems.add(replication.containsKey("class") ? "its class is " + className + " (none of NetworkTopologyStrategy, SimpleStrategy and " +
                                                            "EverywhereStrategy)"
                                                          : "it has no replication class");
        }
        boolean twoNodes = false;
        // whether a data center of the nodes is not listed, at 0 replicas or with no number, which says more than that
        // none is listed
        boolean dataCenterWithoutReplicas = false;
        for (Map.Entry<String, Integer> entry : nodesByDataCenter.entrySet())
        {
            String dataCenter = entry.getKey();
            int nodeCount = entry.getValue();
            int recommended = Math.min(AUTH_REPLICAS, nodeCount);
            twoNodes |= countsExact && nodeCount == 2;
            String listed = networkTopology ? replication.get(dataCenter) : null;
            Integer listedReplicas = listed == null ? null : replicas(listed);
            if (networkTopology && listed == null)
                problems.add(dataCenter + " (" + nodes(nodeCount) + ") is not listed");
            else if (networkTopology && listedReplicas != null && listedReplicas == 0)
                problems.add(dataCenter + " has 0 replicas and " + nodes(nodeCount));
            dataCenterWithoutReplicas |= networkTopology && (listed == null || listedReplicas == null || listedReplicas == 0);
            Integer replicas = replicasByDataCenter.get(dataCenter);
            if (replicas != null)
            {
                int quorum = quorum(replicas);
                String anotherDown = ": an upgraded node there cannot start while another node of " + dataCenter + " is down";
                String has = dataCenter + " has " + replicas + (replicas == 1 ? " replica" : " replicas");
                // EverywhereStrategy: the data center holds a replica on each of its nodes, of the factor of the whole
                // ring
                String above = everywhere ? dataCenter + " holds " + nodeCount + " of the " + replicas + " replicas of the ring " +
                                            "(EverywhereStrategy: LOCAL_QUORUM counts the replicas of the whole ring)"
                                          : has + " and only " + nodes(nodeCount);
                // the nodes counted are a lower bound when the data centers are not all known: no upper bound then
                if (countsExact && quorum > nodeCount)
                    problems.add(above + ", so LOCAL_QUORUM needs " + quorum + " replicas up there, more than its nodes");
                // with 1 or 2 replicas, LOCAL_QUORUM needs them all; from 3 nodes on, 3 replicas would let one node be
                // down
                else if (replicas < recommended && nodeCount >= AUTH_REPLICAS)
                {
                    shortfalls.add(has + " and " + (countsExact ? "" : "at least ") + nodes(nodeCount) + anotherDown);
                    shortOf.add(dataCenter);
                    fewerThanRecommended = true;
                }
                // so many replicas above the nodes that LOCAL_QUORUM needs every node, where as many as the nodes would
                // not (from 3 nodes on, a quorum of every node implies more replicas than nodes: said for readability)
                else if (countsExact && replicas > nodeCount && quorum == nodeCount && nodeCount >= AUTH_REPLICAS)
                {
                    shortfalls.add(above + ", so LOCAL_QUORUM needs " + quorum + " replicas up there" + anotherDown);
                    shortOf.add(dataCenter);
                    aboveTheNodes = true;
                }
            }
            // the remedy's ALTER sets every data center, those that need no change included, to the recommended
            // replicas, a higher factor kept up to its nodes (as listed when the nodes counted are a lower bound); in a
            // data center of 2 nodes, 1 replica is raised and 3 lowered to 2, which costs nothing: the scan needs both
            // nodes either way (the replicas of EverywhereStrategy are no factor chosen for the data center: not
            // carried over)
            Integer current = everywhere ? null : replicas;
            wanted.put(dataCenter, countsExact ? Math.max(recommended, current == null ? 0 : Math.min(current, nodeCount))
                                               : Math.max(recommended, current == null ? 0 : current));
        }
        if (networkTopology && listedWithReplicas == 0 && !dataCenterWithoutReplicas)
            problems.add("it lists no data center with a replica");
        // with the nodes counted a lower bound, the remedy asks for the full 3 replicas in every data center it names,
        // the listed ones that no node counted is in included, the replicas listed kept when higher
        if (!countsExact)
            wanted.replaceAll((dataCenter, replicas) -> Math.max(AUTH_REPLICAS, replicas));
        String everyDataCenter = nodesByDataCenter.isEmpty() ? "every data center of the cluster (nodetool status names them)"
                                                             : "every data center of nodes/local and nodes/peers";
        String recommendation = AUTH_REPLICAS + " replicas, or as many as its nodes if fewer, are recommended: in a data center of " + AUTH_REPLICAS +
                                " nodes or more they let the upgrade go on while a node is down";
        String otherKeyspaces = !countsExact || wanted.size() > 1
                                ? "; with several data centers, the other replicated system keyspaces need every data center too, which this " +
                                  "tool does not check"
                                : "";
        String reads = "an upgraded node reads DSE's access controls from system_auth at LOCAL_QUORUM (HCD 2.0's default " +
                       "auth_read_consistency_level) when it starts and refuses to start when it cannot";
        String keepUp = shortOf.isEmpty() ? "" : "keep every node of " + NodeInspector.namesWithAnd(shortOf) + " up while an upgraded node there starts";
        String unknown;
        if (!nodesFilesRead)
            unknown = "nodes/local or nodes/peers is missing or could not be read, and the data centers of the nodes are not all known";
        else if (nodesByDataCenter.isEmpty())
            unknown = "no data center is known from nodes/local and nodes/peers";
        else
            unknown = (nodesWithoutDataCenter == 1 ? "the data center of 1 node is" : "the data centers of " + nodesWithoutDataCenter + " nodes are") +
                      " not known from nodes/local and nodes/peers";
        if (problems.isEmpty() && !countsExact)
        {
            // EverywhereStrategy has no replicas per data center: they are those of the whole ring, not known here
            boolean classOnly = nodesByDataCenter.isEmpty() || everywhere;
            String checked = classOnly ? "for its class only" : "against the nodes whose data center is known only";
            findings.add(warning("SYSTEM_AUTH_REPLICATION",
                                 unknown + ", so the replication of system_auth, " + shown + ", was checked " + checked +
                                 (classOnly ? ", not for the replicas of each data center" : "") +
                                 (shortfalls.isEmpty() ? "" : "; " + items(shortfalls) + "; " + reads),
                                 "check by hand" + (mixedPhase ? "" : ", before the first node of the cluster is upgraded,") + " that system_auth " +
                                 "has replicas in every data center of the cluster (nodetool status names them; with several data centers, " +
                                 "NetworkTopologyStrategy listing each), no more in a data center than its nodes can make a LOCAL_QUORUM of" +
                                 (everywhere ? " (EverywhereStrategy: LOCAL_QUORUM counts the replicas of the whole ring, which only a data " +
                                               "center of more than half of the nodes meets)" : "") +
                                 (mixedPhase ? "; if it does not, upgrade no further node and report to the upgrade owner, since the replication " +
                                               "cannot be changed in the mixed phase" + (shortfalls.isEmpty() ? "" : "; " + keepUp)
                                             : "; " + recommendation + otherKeyspaces)));
            authReplicationStep = shown + ", checked " + checked + " (" + unknown + ")" + otherKeyspaces;
            return;
        }
        if (problems.isEmpty() && shortfalls.isEmpty())
        {
            authReplicationStep = shown + ", enough in every data center of nodes/local and nodes/peers" + otherKeyspaces;
            return;
        }

        StringBuilder alter = new StringBuilder("ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy'");
        if (wanted.isEmpty())
            alter.append(", '<dc>': ").append(AUTH_REPLICAS);
        wanted.forEach((dataCenter, replicas) -> alter.append(", ").append(quoted(dataCenter)).append(": ").append(replicas));
        alter.append("};");
        String dataCenters = factorNotCarried +
                             (nodesByDataCenter.isEmpty() ? "; " + unknown + ": list each data center nodetool status shows, and lower the " +
                                                            AUTH_REPLICAS + " replicas of a data center of fewer nodes to its nodes"
                                                          : countsExact ? "" : "; " + unknown + ": check by hand the data center of each node " +
                                                                               "and its replicas, and lower the " + AUTH_REPLICAS + " replicas of " +
                                                                               "a data center of fewer nodes to its nodes") +
                             (leftOut.isEmpty() ? "" : "; " + list(new ArrayList<>(leftOut)) + ", listed but with no node in nodes/local and " +
                                                       "nodes/peers, left out") +
                             (unnumbered.isEmpty() ? ""
                                                   : countsExact ? "; " + list(new ArrayList<>(unnumbered)) + ", listed with no number of replicas " +
                                                                   "and with no node in nodes/local and nodes/peers, left out"
                                                                 : "; " + list(new ArrayList<>(unnumbered)) + ", listed with no number of replicas, " +
                                                                   "left out: set it by hand if that data center has nodes");
        String change = "with the NodeSync flag off on the tables of system_auth (DSE's repair skips a table whose flag is on), through one DSE " +
                        "node: " + alter + " (every data center, its exact name as nodetool status shows it; " + recommendation +
                        "; a higher replication factor is " + (countsExact ? "kept up to the nodes of the data center" : "kept as listed") + dataCenters +
                        "); DESCRIBE KEYSPACE system_auth then shows the new map and nodetool describecluster one schema version; right after, " +
                        "nodetool repair -full system_auth on every node, one at a time, each exiting 0 and printing no Skipping anti-entropy " +
                        "repair line, a failed one run again, whole, on its node (the ALTER and the repair in a quiet period, without a pause " +
                        "between them); then nodetool flush system_schema on this node and run this tool again" +
                        (twoNodes ? "; in a data center of 2 nodes, an upgraded node still cannot start while the other one is down" : "") +
                        otherKeyspaces;
        if (problems.isEmpty())
        {
            // in the mixed phase, when the replication can no longer be changed, the warning asks for no change, only
            // that every node of the data center stays up while an upgraded node there starts
            String more = fewerThanRecommended && aboveTheNodes
                          ? "more replicas (" + AUTH_REPLICAS + " where a data center has fewer), and as many as its nodes where it has more " +
                            "replicas than nodes, let"
                          : fewerThanRecommended ? "more replicas (" + AUTH_REPLICAS + ", or up to the nodes of the data center) let"
                                                 : "as many replicas as the nodes of the data center let";
            String message = "the replication of system_auth is " + shown + ": " + items(shortfalls) + "; " + reads +
                             (mixedPhase ? "" : "; " + more + " the upgrade go on while a node is down");
            findings.add(warning("SYSTEM_AUTH_REPLICATION", message,
                                 mixedPhase ? "the upgrade has started (UPGRADED_PEERS), so the replication stays as it is (no schema change and no " +
                                              "repair in the mixed phase): " + keepUp
                                            : "optional, before the first node of the cluster is upgraded, " + change));
            authReplicationStep = "see SYSTEM_AUTH_REPLICATION";
            return;
        }
        String message = "the replication of system_auth is " + shown + ": " + items(problems) + "; " + reads + ", so " + everyDataCenter + " needs " +
                         "replicas of system_auth (with several data centers, NetworkTopologyStrategy listing each), no more in a data center " +
                         "than its nodes can make a LOCAL_QUORUM of, or an upgraded node there cannot start, even with every node up" +
                         (shortfalls.isEmpty() ? "" : "; besides, " + items(shortfalls));
        if (mixedPhase)
        {
            findings.add(blocking("SYSTEM_AUTH_REPLICATION", message,
                                  "the upgrade has started (UPGRADED_PEERS), and the replication cannot be changed in the mixed phase (no schema change " +
                                  "and no repair then, and the upgraded nodes do not receive DSE's schema changes): upgrade no further node and report " +
                                  "to the upgrade owner; until it is changed, an upgraded node cannot start while too few of the replicas in its data " +
                                  "center are up for LOCAL_QUORUM, and never where its data center lacks replicas (not listed, at 0, or " +
                                  "SimpleStrategy over several data centers) or has fewer nodes than its LOCAL_QUORUM needs (with " +
                                  "EverywhereStrategy, the quorum of the whole ring), nor with a replication HCD 2.0 cannot use (no class or " +
                                  "another class, replicas that are no number, or no replication_factor)"));
            authReplicationStep = "see SYSTEM_AUTH_REPLICATION";
            return;
        }
        findings.add(blocking("SYSTEM_AUTH_REPLICATION", message, "before the first node of the cluster is upgraded, " + change));
        authReplicationStep = "see SYSTEM_AUTH_REPLICATION";
    }

    /**
     * The problems or shortfalls of the replication of {@code system_auth}, each with commas of its own: joined with
     * semicolons.
     */
    private static String items(List<String> items)
    {
        return String.join("; ", items);
    }

    /** The replicas that make a {@code LOCAL_QUORUM} of {@code replicas}. */
    private static int quorum(int replicas)
    {
        return replicas / 2 + 1;
    }

    /** A CQL string literal: {@code value} in single quotes, its quotes doubled. */
    private static String quoted(String value)
    {
        return '\'' + value.replace("'", "''") + '\'';
    }

    /** The class name of a replication strategy without the package of this software's strategies. */
    private static String shortClassName(String className)
    {
        return className.startsWith(LOCATOR_PACKAGE) ? className.substring(LOCATOR_PACKAGE.length()) : className;
    }

    /** The replicas of a replication option; null when it is no number, or a negative one. */
    @Nullable
    private static Integer replicas(String value)
    {
        try
        {
            int replicas = Integer.parseInt(value.trim());
            return replicas < 0 ? null : replicas;
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    private static String nodes(int nodes)
    {
        return nodes + (nodes == 1 ? " node" : " nodes");
    }

    /** A replication map as cqlsh shows it, the class without the package of this software's strategies. */
    private static String cql(Map<String, String> replication)
    {
        if (replication.isEmpty())
            return "not known";
        List<String> options = new ArrayList<>();
        if (replication.containsKey("class"))
            options.add("'class': " + quoted(shortClassName(replication.get("class"))));
        new TreeMap<>(replication).forEach((option, value) -> {
            if (!option.equals("class"))
                options.add(quoted(option) + ": " + quoted(value));
        });
        return '{' + String.join(", ", options) + '}';
    }

    // ---- types and tables ----

    private Map<String, Types> checkTypes()
    {
        Map<String, Types> result = new HashMap<>();
        List<String> unloadable = new ArrayList<>();
        for (Map.Entry<String, List<UntypedResultSet.Row>> entry : types.entrySet())
        {
            Types.RawBuilder builder = Types.rawBuilder(entry.getKey());
            try
            {
                for (UntypedResultSet.Row row : entry.getValue())
                    builder.add(row.getString("type_name"),
                                row.has("field_names") ? row.getFrozenList("field_names", UTF8Type.instance) : List.of(),
                                row.has("field_types") ? row.getFrozenList("field_types", UTF8Type.instance) : List.of());
                result.put(entry.getKey(), builder.build());
            }
            catch (RuntimeException e)
            {
                unloadable.add(entry.getKey() + ": " + message(e));
            }
        }
        if (!unloadable.isEmpty())
            findings.add(blocking("UNLOADABLE_TYPE",
                                  "user types HCD 2.0 cannot load: " + list(unloadable),
                                  "change or drop them on DSE (with the tables that use them); a DSE-only field type has no counterpart in HCD 2.0"));
        return result;
    }

    private void checkTables(Map<String, Types> typesByKeyspace)
    {
        List<String> tiered = new ArrayList<>();
        List<String> encrypted = new ArrayList<>();
        List<String> systemInfoEncrypted = new ArrayList<>();
        List<String> unloadableClasses = new ArrayList<>();
        List<String> unloadableTables = new ArrayList<>();
        List<String> compact = new ArrayList<>();
        List<String> legacyAuth = new ArrayList<>();
        for (Map.Entry<String, Map<String, UntypedResultSet.Row>> keyspace : tables.entrySet())
        {
            for (Map.Entry<String, UntypedResultSet.Row> entry : keyspace.getValue().entrySet())
            {
                String name = keyspace.getKey() + '.' + entry.getKey();
                UntypedResultSet.Row row = entry.getValue();
                if (SchemaConstants.AUTH_KEYSPACE_NAME.equals(keyspace.getKey()) && SchemaConstants.LEGACY_AUTH_TABLES.contains(entry.getKey()))
                    legacyAuth.add(name);
                Set<TableMetadata.Flag> flags = Collections.emptySet();
                try
                {
                    flags = TableMetadata.Flag.fromStringSet(row.has("flags") ? row.getFrozenSet("flags", UTF8Type.instance) : Set.of());
                    if (!TableMetadata.Flag.isCQLTable(flags))
                        compact.add(name + " (flags " + new TreeSet<>(TableMetadata.Flag.toStringSet(flags)) + ')');
                }
                catch (RuntimeException e)
                {
                    unloadableTables.add(name + ": flags " + message(e));
                }
                List<String> encryptedList = SYSTEM_INFO_ENCRYPTED_TABLE_NAMES.contains(name) ? systemInfoEncrypted : encrypted;
                TableParams params = checkTableParams(keyspace.getKey(), entry.getKey(), row, tiered, encryptedList, unloadableClasses, unloadableTables);
                TableMetadata metadata = build(keyspace.getKey(), entry.getKey(), row, flags, params, typesByKeyspace, false, unloadableTables);
                if (metadata != null)
                    built.put(name, metadata);
            }
        }
        if (!tiered.isEmpty())
            findings.add(blocking("TIERED_STORAGE",
                                  "tables using DSE tiered storage: " + list(tiered) + "; HCD 2.0 does not have it",
                                  "ALTER TABLE ... WITH compaction = {'class': 'SizeTieredCompactionStrategy'} or {'class': 'LeveledCompactionStrategy'} " +
                                  "(both strategies exist in DSE and in HCD 2.0) on DSE, " +
                                  "and remove tiered_storage_options from dse.yaml"));
        if (!encrypted.isEmpty())
            findings.add(blocking("TDE_TABLE",
                                  "encrypted (TDE) tables: " + list(encrypted) + "; the upgrade applies to clusters without encrypted tables or views",
                                  "decrypt them on DSE (ALTER TABLE ... WITH compression = {'class': 'LZ4Compressor'}, then nodetool upgradesstables -a " +
                                  "on every node) before the upgrade"));
        // with the setting on, the finding of dse.yaml's system_info_encryption names these tables and their remedy
        if (!systemInfoEncrypted.isEmpty() && !systemInfoEncryption)
            findings.add(blocking("TDE_TABLE",
                                  "tables that system_info_encryption of dse.yaml encrypted, whose schema still names an encrypting compression " +
                                  "although dse.yaml does not enable it here, or could not be read: " + list(systemInfoEncrypted) + "; DSE sets " +
                                  "their compression back to LZ4Compressor at its first start with the setting off (not done yet on this node, or, " +
                                  "for system_backups, another node started with it on since then; or system_schema not flushed since), and " +
                                  "their sstables stay encrypted until they are rewritten",
                                  "do not ALTER these system tables: set enabled: false under system_info_encryption in dse.yaml and restart DSE " +
                                  "on every node that has not run with it off yet, with " + SYSTEM_KEY_DIRECTORY + " still in place, so that " +
                                  "DSE replays the commit log segments written before (if DSE already runs with it off on every node, run " +
                                  "nodetool flush system_schema on this node and this tool again); then run " + SYSTEM_INFO_REWRITE + " (this " +
                                  "tool does not check that the rewrite is done), and " + SYSTEM_INFO_KEEP_UNTIL));
        if (!unloadableClasses.isEmpty())
            findings.add(blocking("UNLOADABLE_CLASS",
                                  "tables with a compaction or compression class HCD 2.0 cannot load: " + list(unloadableClasses),
                                  "ALTER these tables on DSE to classes DSE and HCD 2.0 both have (for example SizeTieredCompactionStrategy or " +
                                  "LeveledCompactionStrategy, LZ4Compressor)"));
        if (!unloadableTables.isEmpty())
            findings.add(blocking("UNLOADABLE_TABLE",
                                  "tables HCD 2.0 cannot load: " + list(unloadableTables),
                                  "change or drop these tables on DSE; an upgraded node does not start with them"));
        if (!compact.isEmpty())
            findings.add(blocking("COMPACT_STORAGE",
                                  "tables with COMPACT STORAGE: " + list(compact),
                                  "ALTER TABLE ... DROP COMPACT STORAGE on DSE for each of them before the upgrade"));
        if (!legacyAuth.isEmpty())
            findings.add(blocking("LEGACY_AUTH_TABLES",
                                  "legacy authentication tables: " + list(legacyAuth) + "; HCD 2.0 refuses to start while they exist",
                                  "check that their content was migrated to roles, role_members and role_permissions, then drop them on DSE"));
    }

    /**
     * The parameters of a table or a view row, as the schema load of an upgraded node reads them; the compaction and
     * compression classes are checked first, so that the finding names them. Null when they cannot be loaded.
     */
    @Nullable
    private TableParams checkTableParams(String keyspaceName, String tableName, UntypedResultSet.Row row, List<String> tiered, List<String> encrypted, List<String> unloadableClasses, List<String> unloadableTables)
    {
        String name = keyspaceName + '.' + tableName;
        boolean classesLoad = true;
        Map<String, String> compaction = row.has("compaction") ? row.getFrozenTextMap("compaction") : Map.of();
        Map<String, String> compression = row.has("compression") ? row.getFrozenTextMap("compression") : Map.of();
        String compactionClass = compaction.get("class");
        String compressionClass = compression.containsKey("class") ? compression.get("class") : compression.get("sstable_compression");
        if (compressionClass != null && compressionClass.toLowerCase().contains("encrypt"))
        {
            encrypted.add(name + " (compression class " + compressionClass + ')');
            classesLoad = false;
        }
        else if (!compression.isEmpty())
        {
            try
            {
                CompressionParams.fromStoredMap(compression, keyspaceName, tableName);
            }
            catch (RuntimeException e)
            {
                unloadableClasses.add(name + ": compression " + compression + " (" + message(e) + ')');
                classesLoad = false;
            }
        }
        if (compactionClass != null && (compactionClass.equals(TIERED_COMPACTION_STRATEGY) || compactionClass.equals("TieredCompactionStrategy")))
        {
            tiered.add(name + " (compaction class " + compactionClass + ')');
            classesLoad = false;
        }
        else if (!compaction.isEmpty())
        {
            try
            {
                CompactionParams.fromMap(compaction);
            }
            catch (RuntimeException e)
            {
                unloadableClasses.add(name + ": compaction class " + compactionClass + " (" + message(e) + ')');
                classesLoad = false;
            }
        }
        if (!classesLoad)
            return null;
        try
        {
            return SchemaKeyspace.createTableParamsFromRow(row, keyspaceName, tableName);
        }
        catch (RuntimeException e)
        {
            unloadableTables.add(name + ": table options (" + e + ')');
            return null;
        }
    }

    /** The metadata of a table or a view, as the schema load builds it; null, with the reason recorded, when it cannot be built. */
    @Nullable
    private TableMetadata build(String keyspace, String table, UntypedResultSet.Row row, Set<TableMetadata.Flag> flags, @Nullable TableParams params,
                                Map<String, Types> typesByKeyspace, boolean view, List<String> unloadable)
    {
        List<UntypedResultSet.Row> columnRows = columns.getOrDefault(keyspace, Map.of()).getOrDefault(table, List.of());
        if (columnRows.isEmpty())
        {
            unloadable.add(keyspace + '.' + table + ": no column in system_schema.columns");
            return null;
        }
        try
        {
            Types keyspaceTypes = typesByKeyspace.getOrDefault(keyspace, Types.none());
            TableMetadata.Builder builder = TableMetadata.builder(keyspace, table, TableId.fromUUID(row.getUUID("id"))).partitioner(partitioner);
            if (view)
                builder.kind(TableMetadata.Kind.VIEW);
            else
                builder.flags(flags);
            if (params != null)
                builder.params(params);
            for (UntypedResultSet.Row column : columnRows)
                builder.addColumn(column(column, keyspaceTypes));
            return builder.build();
        }
        catch (RuntimeException | AssertionError e)
        {
            unloadable.add(keyspace + '.' + table + ": " + message(e));
            return null;
        }
    }

    /** A column of {@code system_schema.columns}, as the schema load reads it (without DSE's hidden columns, which are not listed there). */
    private static ColumnMetadata column(UntypedResultSet.Row row, Types types)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");
        ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());
        ColumnMetadata.ClusteringOrder order = ColumnMetadata.ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());
        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), types);
        if (order == ColumnMetadata.ClusteringOrder.DESC)
            type = ReversedType.getInstance(type);
        ColumnIdentifier name = new ColumnIdentifier(row.getBytes("column_name_bytes"), row.getString("column_name"));
        return new ColumnMetadata(keyspace, table, name, type, row.getInt("position"), kind, null);
    }

    // ---- indexes ----

    private void checkIndexes()
    {
        List<String> search = new ArrayList<>();
        List<String> unloadable = new ArrayList<>();
        for (UntypedResultSet.Row row : indexes)
        {
            String name = row.getString("keyspace_name") + '.' + row.getString("table_name") + '.' + row.getString("index_name");
            String kind = row.has("kind") ? row.getString("kind") : null;
            if (!IndexMetadata.Kind.CUSTOM.name().equals(kind))
                continue;
            Map<String, String> options = row.has("options") ? row.getFrozenTextMap("options") : Map.of();
            String className = options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
            if (className == null)
            {
                unloadable.add(name + " (no " + IndexTarget.CUSTOM_INDEX_OPTION_NAME + " option)");
                continue;
            }
            String expanded = IndexMetadata.expandAliases(className);
            if (Dse6NonCoreTraces.isSearchIndexClass(expanded))
            {
                search.add(name + " (" + expanded + ')');
                continue;
            }
            try
            {
                // loaded without being initialised: no code of the class runs in the tool
                Class<?> indexClass = Class.forName(expanded, false, SchemaInspector.class.getClassLoader());
                if (!Index.class.isAssignableFrom(indexClass))
                    unloadable.add(name + " (" + expanded + " is not an index class)");
            }
            catch (ClassNotFoundException | LinkageError | RuntimeException e)
            {
                unloadable.add(name + " (" + expanded + ')');
            }
        }
        if (!search.isEmpty())
            findings.add(blocking("SEARCH_INDEX",
                                  "DSE Search indexes: " + list(search) + "; HCD 2.0 does not support DSE Search, and an upgraded node refuses to start with them",
                                  "DROP SEARCH INDEX ON <keyspace>.<table> on DSE for each, then delete the solr.data directories"));
        if (!unloadable.isEmpty())
            findings.add(blocking("UNLOADABLE_CLASS",
                                  "indexes of a class HCD 2.0 does not have: " + list(unloadable) + "; an upgraded node would load them as indexes that do " +
                                  "nothing, and the queries using them would fail",
                                  "DROP INDEX on DSE (and recreate them as SAI indexes once the upgrade is complete, if needed)"));
    }

    // ---- views ----

    private void checkViews(Map<String, Types> typesByKeyspace)
    {
        List<String> unloadable = new ArrayList<>();
        List<String> viewsTiered = new ArrayList<>();
        List<String> viewsEncrypted = new ArrayList<>();
        List<String> oldFormat = new ArrayList<>();
        for (Map.Entry<String, Map<String, UntypedResultSet.Row>> keyspace : views.entrySet())
        {
            for (Map.Entry<String, UntypedResultSet.Row> entry : keyspace.getValue().entrySet())
            {
                String name = keyspace.getKey() + '.' + entry.getKey();
                UntypedResultSet.Row row = entry.getValue();
                int unloadableBefore = unloadable.size();
                TableParams params = checkTableParams(keyspace.getKey(), entry.getKey(), row, viewsTiered, viewsEncrypted, unloadable, unloadable);
                TableMetadata view = build(keyspace.getKey(), entry.getKey(), row, Set.of(), params, typesByKeyspace, true, unloadable);
                TableMetadata base = built.get(keyspace.getKey() + '.' + row.getString("base_table_name"));
                if (view != null && base == null)
                    unloadable.add(name + ": its base table " + keyspace.getKey() + '.' + row.getString("base_table_name") + " cannot be loaded");
                if (unloadable.size() > unloadableBefore)
                    viewsToDrop.add(name);
                if (view == null || base == null)
                    continue;
                List<HiddenColumn> hidden = hiddenColumns.getOrDefault(Pair.create(keyspace.getKey(), entry.getKey()), List.of());
                ColumnMetadata derived = DseViewHiddenColumns.requiredHiddenColumn(view, base);
                if (hidden.isEmpty())
                {
                    if (derived != null)
                        oldFormat.add(name);
                    else
                        viewsHandled.add(name + " (no hidden column)");
                }
                else if (DseViewHiddenColumnCatalog.reproduces(hidden, derived))
                {
                    viewsHandled.add(name + " (hidden column " + safe(derived.name.toString()) + ')');
                }
                else
                {
                    viewsRefused.add(name + ": DSE keeps the hidden columns " + safe(hidden.toString()));
                    viewsToDrop.add(name);
                }
            }
        }
        if (!views.isEmpty() && !hiddenColumnsTableFound && !schemaTablesNotRead.contains(HIDDEN_COLUMNS))
            findings.add(blocking("UNREADABLE_SCHEMA",
                                  "the schema has materialized views but no data directory holds DSE's system_schema." + HIDDEN_COLUMNS +
                                  ", so whether their data can be exchanged with DSE 6 nodes cannot be checked",
                                  "point the tool at every data directory of this DSE node"));
        if (!viewsRefused.isEmpty())
            findings.add(blocking("VIEW_HIDDEN_COLUMNS",
                                  "materialized views with DSE hidden columns HCD 2.0 does not reproduce (it handles only the hidden column of the one " +
                                  "regular base column in the view primary key): " + list(viewsRefused),
                                  DseViewHiddenColumns.REMEDY));
        if (!oldFormat.isEmpty())
            findings.add(info("VIEW_OLD_FORMAT",
                              "materialized views DSE lists no hidden column for (DSE's old view format): " + list(oldFormat) +
                              "; their data is exchanged with DSE 6 nodes as it is"));
        if (!viewsHandled.isEmpty())
            findings.add(info("VIEWS_HANDLED", "materialized views whose data HCD 2.0 exchanges with DSE 6 nodes: " + list(viewsHandled)));
        if (!viewsTiered.isEmpty())
            findings.add(blocking("TIERED_STORAGE",
                                  "materialized views using DSE tiered storage: " + list(viewsTiered) + "; HCD 2.0 does not have it",
                                  "ALTER MATERIALIZED VIEW ... WITH compaction = {'class': 'SizeTieredCompactionStrategy'} or {'class': " +
                                  "'LeveledCompactionStrategy'} (both strategies exist in DSE and in HCD 2.0) on DSE"));
        if (!viewsEncrypted.isEmpty())
            findings.add(blocking("TDE_TABLE",
                                  "encrypted (TDE) materialized views: " + list(viewsEncrypted) + "; the upgrade applies to clusters without encrypted " +
                                  "tables or views",
                                  "decrypt them on DSE (ALTER MATERIALIZED VIEW ... WITH compression = {'class': 'LZ4Compressor'}, then nodetool " +
                                  "upgradesstables -a on every node) before the upgrade"));
        if (!unloadable.isEmpty())
            findings.add(blocking("UNLOADABLE_TABLE",
                                  "materialized views HCD 2.0 cannot load: " + list(unloadable),
                                  "drop these views on DSE before the upgrade and create them again once no node runs DSE"));
    }

    // ---- dropped columns ----

    /** The types of the dropped columns, which the schema load of an upgraded node parses as well. */
    private void checkDroppedColumns()
    {
        List<String> unloadable = new ArrayList<>();
        for (UntypedResultSet.Row row : droppedColumns)
        {
            String keyspace = row.getString("keyspace_name");
            String name = keyspace + '.' + row.getString("table_name") + '.' + safe(row.getString("column_name"));
            try
            {
                CQLTypeParser.parseDroppedType(keyspace, row.has("type") ? row.getString("type") : null);
            }
            catch (RuntimeException e)
            {
                unloadable.add(name + " (type " + (row.has("type") ? row.getString("type") : null) + ": " + message(e) + ')');
            }
        }
        if (!unloadable.isEmpty())
            findings.add(blocking("UNLOADABLE_TABLE",
                                  "dropped columns of a type HCD 2.0 cannot load (the schema load parses them too): " + list(unloadable),
                                  "on DSE, recreate the table without the dropped column's history (copy its data to a new table), or contact support"));
    }

    // ---- functions and triggers ----

    private void checkFunctions()
    {
        List<String> javascript = new ArrayList<>();
        List<String> other = new ArrayList<>();
        for (UntypedResultSet.Row row : functions)
        {
            String language = row.has("language") ? row.getString("language") : null;
            if (language == null || "java".equalsIgnoreCase(language))
                continue;
            List<String> arguments = row.has("argument_types") ? row.getFrozenList("argument_types", UTF8Type.instance) : List.of();
            String name = row.getString("keyspace_name") + '.' + safe(row.getString("function_name")) + '(' + safe(String.join(", ", arguments)) + ')';
            if ("javascript".equalsIgnoreCase(language))
                javascript.add(name);
            else
                other.add(name + " (language " + safe(language) + ')');
        }
        if (!javascript.isEmpty())
            findings.add(blocking("JAVASCRIPT_UDF",
                                  "user-defined functions written in JavaScript: " + list(javascript) + "; HCD 2.0 runs Java functions only",
                                  "rewrite them in Java, or drop them (and the aggregates using them), on DSE"));
        if (!other.isEmpty())
            findings.add(blocking("UDF_LANGUAGE",
                                  "user-defined functions in a language HCD 2.0 does not run: " + list(other),
                                  "rewrite them in Java, or drop them (and the aggregates using them), on DSE"));
    }

    private void checkTriggers()
    {
        List<String> found = new ArrayList<>();
        for (UntypedResultSet.Row row : triggers)
        {
            Map<String, String> options = row.has("options") ? row.getFrozenTextMap("options") : Map.of();
            found.add(row.getString("keyspace_name") + '.' + row.getString("table_name") + '.' + safe(row.getString("trigger_name")) + " (class " +
                      safe(options.get("class")) + ')');
        }
        if (!found.isEmpty())
            findings.add(warning("TRIGGER",
                                 "triggers: " + list(found),
                                 "build each trigger class against HCD 2.0 and put its jar in the triggers directory of HCD 2.0 before the node starts"));
    }

    // ---- access controls ----

    private void checkAccessControls()
    {
        checkRowLevelTables();
        DseTableReader.TableRows rows;
        try
        {
            rows = DseTableReader.read(dataDirectories, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, List.of("role"), List.of("resource"));
        }
        catch (Exception e)
        {
            findings.add(blocking("UNREADABLE_SCHEMA",
                                  "cannot read system_auth." + AuthKeyspace.ROLE_PERMISSIONS + " (" + e + "), so the DSE access controls this software does not " +
                                  "enforce cannot be checked",
                                  "run the tool as the user DSE runs as (or as root); if DSE is running, an sstable may have been compacted away while it " +
                                  "was read: run the tool again; if an sstable is not in DSE 6's trie format, run nodetool upgradesstables on DSE " +
                                  "first; if an sstable is damaged, repair it on DSE first"));
            return;
        }
        rolePermissionsRows = rows.rows.size();
        rolePermissionsRead = true;
        rolePermissionsFound = rows.found;
        if (!rows.found)
            findings.add(warning("NO_ROLE_PERMISSIONS",
                                 "no data directory holds system_auth." + AuthKeyspace.ROLE_PERMISSIONS + ", which DSE creates on every node: the DSE access " +
                                 "controls cannot be checked on this node",
                                 "point the tool at every data directory of this DSE node; the reports of the other nodes cover the partitions this node " +
                                 "would hold"));
        else if (rows.sstables.isEmpty())
            findings.add(info("NO_ROLE_PERMISSIONS",
                              "system_auth." + AuthKeyspace.ROLE_PERMISSIONS + " has no sstable on this node (it holds no replica of a partition, or " +
                              "nothing was flushed yet): the reports of the other nodes cover the grants"));
        List<String> restrictions = new ArrayList<>();
        List<String> rowGrants = new ArrayList<>();
        List<String> proxy = new ArrayList<>();
        List<String> delegations = new ArrayList<>();
        List<String> lone = new ArrayList<>();
        List<String> unknownPermissions = new ArrayList<>();
        List<String> unknownResources = new ArrayList<>();
        for (UntypedResultSet.Row row : rows.rows)
        {
            String role = row.getString("role");
            String resource = row.getString("resource");
            Set<String> permissions = row.has("permissions") ? row.getSet("permissions", UTF8Type.instance) : Set.of();
            if (row.has(Dse6AccessControlCheck.RESTRICTED_COLUMN) && !row.getSet(Dse6AccessControlCheck.RESTRICTED_COLUMN, UTF8Type.instance).isEmpty())
                restrictions.add(Dse6AccessControlCheck.permissionsEntry(role, resource, row.getSet(Dse6AccessControlCheck.RESTRICTED_COLUMN, UTF8Type.instance)));
            if (row.has(Dse6AccessControlCheck.GRANTABLES_COLUMN) && !row.getSet(Dse6AccessControlCheck.GRANTABLES_COLUMN, UTF8Type.instance).isEmpty())
                delegations.add(Dse6AccessControlCheck.permissionsEntry(role, resource, row.getSet(Dse6AccessControlCheck.GRANTABLES_COLUMN, UTF8Type.instance)));
            if (resource.startsWith(Dse6AccessControlCheck.ROWS_RESOURCE_PREFIX))
            {
                // as the startup check reads them: a row of a rows/ resource with no permissions grants nothing
                if (!permissions.isEmpty())
                    rowGrants.add(Dse6AccessControlCheck.permissionsEntry(role, resource, permissions));
                continue;
            }
            if (permissions.isEmpty())
                continue;
            Set<String> proxyNames = new TreeSet<>();
            for (String permission : permissions)
                if (permission.startsWith(PROXY_PERMISSION_PREFIX))
                    proxyNames.add(permission);
            if (!proxyNames.isEmpty())
                proxy.add(Dse6AccessControlCheck.permissionsEntry(role, resource, proxyNames));
            if (!CassandraAuthorizer.isKnownResourceName(resource))
            {
                unknownResources.add(Dse6AccessControlCheck.permissionsEntry(role, resource, permissions));
                continue;
            }
            String loneName = CassandraAuthorizer.dseLoneWriteGrantName(resource, permissions);
            if (loneName != null)
                lone.add(Dse6AccessControlCheck.permissionsEntry(role, resource, Set.of(loneName)));
            List<String> unknown = CassandraAuthorizer.unknownPermissionNames(resource, permissions);
            unknown.removeAll(proxyNames);
            if (!unknown.isEmpty())
                unknownPermissions.add(Dse6AccessControlCheck.permissionsEntry(role, resource, new TreeSet<>(unknown)));
        }
        for (List<String> entries : List.of(restrictions, rowGrants, proxy, delegations, lone, unknownPermissions, unknownResources))
            Collections.sort(entries);
        if (!restrictions.isEmpty())
            findings.add(blocking("RESTRICT",
                                  "DSE RESTRICT entries in system_auth.role_permissions: " + list(restrictions) + "; HCD 2.0 does not enforce them, so ignored " +
                                  "they would grant access, and an upgraded node refuses to start while they exist",
                                  "UNRESTRICT each of them on DSE before the upgrade"));
        if (!rowGrants.isEmpty())
            findings.add(blocking("ROW_LEVEL_ACCESS",
                                  "DSE row-level access control grants in system_auth.role_permissions: " + list(rowGrants) + "; HCD 2.0 does not enforce them",
                                  "REVOKE the row-level grants and run UNRESTRICT ROWS on every restricted table on DSE before the upgrade"));
        // DSE grants every permission of a role to the role that creates it, PROXY.LOGIN and PROXY.EXECUTE included, so
        // these rows exist on most clusters whether or not an application logs in by proxy: a warning, not a refusal
        if (!proxy.isEmpty())
            findings.add(warning("PROXY_GRANT",
                                 "DSE proxy authentication grants (DSE also grants them to the creator of every role): " + list(proxy) + "; HCD 2.0 has no " +
                                 "proxy login or proxy execution, so an application relying on them cannot log in or run as the proxied role once its " +
                                 "coordinator runs HCD 2.0",
                                 "make every application that uses proxy login or execution log in as the role it needs before the upgrade; the grants " +
                                 "themselves can stay"));
        if (!delegations.isEmpty())
            findings.add(warning("DELEGATION",
                                 "DSE AUTHORIZE FOR delegations (grantables): " + list(delegations) + "; HCD 2.0 drops them, so these roles can no longer grant " +
                                 "those permissions",
                                 "grant AUTHORIZE to the roles that must keep granting, once the upgrade is complete"));
        if (!lone.isEmpty())
            findings.add(warning("LONE_WRITE_GRANT",
                                 "DSE UPDATE or TRUNCATE granted alone: " + list(lone) + "; HCD 2.0 has MODIFY only, which grants both, so it " +
                                 "drops these grants rather than widen them, and the roles lose them",
                                 "GRANT MODIFY where the role should also have the other permission, or accept the loss"));
        if (!unknownPermissions.isEmpty())
            findings.add(warning("DSE_ONLY_PERMISSION",
                                 "DSE permissions HCD 2.0 does not have, which it ignores: " + list(unknownPermissions),
                                 "check that no application relies on them; grant the closest HCD 2.0 permission where one exists"));
        if (!unknownResources.isEmpty())
            findings.add(warning("DSE_ONLY_RESOURCE",
                                 "grants on DSE resources HCD 2.0 does not have, which it ignores: " + list(unknownResources),
                                 "check that no application relies on them"));
        if (!rows.rows.isEmpty())
            findings.add(info("AUTH_REPLICA",
                              "system_auth.role_permissions was read from this node's replica (" + rows.rows.size() + " rows): a node holds only the " +
                              "partitions it replicates, so check the reports of every node"));
    }

    private void checkRowLevelTables()
    {
        List<String> restricted = new ArrayList<>();
        List<String> otherExtensions = new ArrayList<>();
        for (Map.Entry<String, Map<String, UntypedResultSet.Row>> keyspace : tables.entrySet())
        {
            for (Map.Entry<String, UntypedResultSet.Row> entry : keyspace.getValue().entrySet())
            {
                UntypedResultSet.Row row = entry.getValue();
                if (!row.has("extensions"))
                    continue;
                Map<String, ByteBuffer> extensions = row.getFrozenMap("extensions", UTF8Type.instance, BytesType.instance);
                String name = keyspace.getKey() + '.' + entry.getKey();
                ByteBuffer tenant = extensions.get(Dse6AccessControlCheck.RLAC_EXTENSION_KEY);
                if (tenant != null)
                    restricted.add(name + " (tenant column " + Dse6AccessControlCheck.tenantColumn(tenant) + ')');
                Set<String> others = new TreeSet<>(extensions.keySet());
                others.remove(Dse6AccessControlCheck.RLAC_EXTENSION_KEY);
                if (!others.isEmpty())
                    otherExtensions.add(name + ": " + safe(others.toString()));
            }
        }
        if (!restricted.isEmpty())
            findings.add(blocking("ROW_LEVEL_ACCESS",
                                  "tables restricted to rows by DSE row-level access control (" + Dse6AccessControlCheck.RLAC_EXTENSION_KEY + "): " +
                                  list(restricted) + "; HCD 2.0 does not enforce it",
                                  "UNRESTRICT ROWS ON <keyspace>.<table> for each on DSE, and REVOKE the row-level grants"));
        if (!otherExtensions.isEmpty())
            findings.add(info("TABLE_EXTENSIONS",
                              "tables with schema extensions other than " + Dse6AccessControlCheck.RLAC_EXTENSION_KEY + ", kept and not interpreted by HCD 2.0: " +
                              list(otherExtensions)));
    }

    /** The message of an error, or the error itself when it has none, with the names it quotes shown as {@link #safe} shows them. */
    private static String message(Throwable e)
    {
        return safe(e.getMessage() == null ? e.toString() : e.getMessage());
    }

    /**
     * A name of the schema as a finding shows it: as it is, but for the control, format and line or paragraph separator
     * characters a quoted identifier or an option may hold, each written as a backslash, then a u and four hexadecimal
     * digits, or a U and eight for a character beyond the basic multilingual plane. The names of
     * {@code system_auth.role_permissions} are shown as the startup check shows them instead
     * ({@link Dse6AccessControlCheck#permissionsEntry}).
     */
    @Nullable
    static String safe(@Nullable String name)
    {
        if (name == null)
            return null;
        StringBuilder shown = new StringBuilder(name.length());
        name.codePoints().forEach(c -> {
            int type = Character.getType(c);
            if (Character.isISOControl(c) || type == Character.FORMAT || type == Character.LINE_SEPARATOR || type == Character.PARAGRAPH_SEPARATOR)
                shown.append(Character.isBmpCodePoint(c) ? String.format("\\u%04x", c) : String.format("\\U%08x", c));
            else
                shown.appendCodePoint(c);
        });
        return shown.toString();
    }
}
